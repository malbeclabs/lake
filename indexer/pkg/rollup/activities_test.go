package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhousetesting "github.com/malbeclabs/lake/indexer/pkg/clickhouse/testing"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openRawConn(t *testing.T, db *clickhousetesting.DB, database string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{db.Addr()},
		Auth: clickhouse.Auth{
			Database: database,
			Username: db.Username(),
			Password: db.Password(),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func setupTestDB(t *testing.T) clickhouse.Conn {
	t.Helper()
	info := laketesting.NewClientWithInfo(t, sharedDB)
	return openRawConn(t, sharedDB, info.Database)
}

// --- Link rollup tests ---

func TestWriteLinkBuckets(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	ctx := context.Background()

	buckets := []LinkBucket{{
		BucketTS:   time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		LinkPK:     "link-1",
		IngestedAt: time.Now().Truncate(time.Millisecond),
		A: LinkLatencyStats{
			AvgRttUs: 100.5, MinRttUs: 50, P50RttUs: 90, P90RttUs: 120,
			P95RttUs: 150, P99RttUs: 200, MaxRttUs: 250,
			LossPct: 1.0, Samples: 500,
		},
		Z: LinkLatencyStats{
			AvgRttUs: 110.5, MinRttUs: 60, P50RttUs: 100, P90RttUs: 130,
			P95RttUs: 160, P99RttUs: 210, MaxRttUs: 260,
			LossPct: 1.5, Samples: 500,
		},
		Status:       "activated",
		Provisioning: false,
		ISISDown:     true,
	}}

	require.NoError(t, a.WriteLinkBuckets(ctx, buckets))

	var aAvg, aMin, aP95, aMax, aLoss float64
	var aSamples uint32
	var zAvg, zP95, zLoss float64
	var zSamples uint32
	var status string
	var provisioning, isisDown bool
	err := conn.QueryRow(ctx, `
		SELECT a_avg_rtt_us, a_min_rtt_us, a_p95_rtt_us, a_max_rtt_us, a_loss_pct, a_samples,
		       z_avg_rtt_us, z_p95_rtt_us, z_loss_pct, z_samples,
		       status, provisioning, isis_down
		FROM link_rollup_5m FINAL WHERE link_pk = 'link-1'
	`).Scan(&aAvg, &aMin, &aP95, &aMax, &aLoss, &aSamples, &zAvg, &zP95, &zLoss, &zSamples,
		&status, &provisioning, &isisDown)
	require.NoError(t, err)

	assert.InDelta(t, 100.5, aAvg, 0.01)
	assert.InDelta(t, 50.0, aMin, 0.01)
	assert.InDelta(t, 150.0, aP95, 0.01)
	assert.InDelta(t, 250.0, aMax, 0.01)
	assert.InDelta(t, 1.0, aLoss, 0.01)
	assert.Equal(t, uint32(500), aSamples)
	assert.InDelta(t, 110.5, zAvg, 0.01)
	assert.InDelta(t, 160.0, zP95, 0.01)
	assert.InDelta(t, 1.5, zLoss, 0.01)
	assert.Equal(t, uint32(500), zSamples)
	assert.Equal(t, "activated", status)
	assert.False(t, provisioning)
	assert.True(t, isisDown)
}

func TestWriteLinkBuckets_Empty(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	require.NoError(t, a.WriteLinkBuckets(context.Background(), nil))
}

func TestComputeLinkRollup_EmptyTables(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeLinkRollup(context.Background(), BackfillChunkInput{
		WindowStart: time.Now().Add(-1 * time.Hour),
		WindowEnd:   time.Now(),
	})
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

func TestComputeLinkRollup_WithData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed dim_dz_links_history (dz_links_current is a view over this)
	err := conn.Exec(ctx, `INSERT INTO dim_dz_links_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_z_pk, bandwidth_bps, committed_rtt_ns) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"link-entity-1", now, now, "00000000-0000-0000-0000-000000000001", uint8(0), "link-1", "activated", "device-a", "device-z", int64(10_000_000_000), int64(500_000))
	require.NoError(t, err)

	// Seed ISIS adjacency (link has adjacency = not ISIS down)
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-1", now, now, "00000000-0000-0000-0000-000000000002", uint8(0), "link-1", "device-a", "sys-1", "sys-2")
	require.NoError(t, err)

	// Seed latency samples for both sides within the same 5m bucket
	for i := range 20 {
		ts := bucketStart.Add(time.Duration(i) * time.Second)
		// Side A probes: RTT 100-290us
		err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ts, ts, int64(1), int32(i), "device-a", "device-z", "link-1", int64(100+i*10), false)
		require.NoError(t, err)
		// Side Z probes: RTT 200-390us (higher)
		err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ts, ts, int64(1), int32(i), "device-z", "device-a", "link-1", int64(200+i*10), false)
		require.NoError(t, err)
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeLinkRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	b := buckets[0]
	assert.Equal(t, "link-1", b.LinkPK)

	// Direction A should have lower RTT than direction Z
	assert.Greater(t, b.A.AvgRttUs, float64(0))
	assert.Greater(t, b.Z.AvgRttUs, float64(0))
	assert.Less(t, b.A.AvgRttUs, b.Z.AvgRttUs)

	// Full percentile spectrum for each direction
	assert.GreaterOrEqual(t, b.A.MaxRttUs, b.A.P99RttUs)
	assert.GreaterOrEqual(t, b.A.P99RttUs, b.A.P95RttUs)
	assert.GreaterOrEqual(t, b.A.P95RttUs, b.A.P50RttUs)
	assert.GreaterOrEqual(t, b.A.P50RttUs, b.A.MinRttUs)

	assert.GreaterOrEqual(t, b.Z.MaxRttUs, b.Z.P99RttUs)
	assert.GreaterOrEqual(t, b.Z.P99RttUs, b.Z.P95RttUs)
	assert.GreaterOrEqual(t, b.Z.P95RttUs, b.Z.P50RttUs)
	assert.GreaterOrEqual(t, b.Z.P50RttUs, b.Z.MinRttUs)

	assert.Equal(t, uint32(20), b.A.Samples)
	assert.Equal(t, uint32(20), b.Z.Samples)

	// Entity state
	assert.Equal(t, "activated", b.Status)
	assert.False(t, b.Provisioning)
	assert.False(t, b.ISISDown) // adjacency exists

	// Write back and verify
	require.NoError(t, a.WriteLinkBuckets(ctx, buckets))
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM link_rollup_5m").Scan(&count))
	assert.Equal(t, uint64(1), count)
}

func TestComputeLinkRollup_Provisioning(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed link with provisioning sentinel
	err := conn.Exec(ctx, `INSERT INTO dim_dz_links_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_z_pk, bandwidth_bps, committed_rtt_ns) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"link-entity-prov", now, now, "00000000-0000-0000-0000-000000000010", uint8(0), "link-prov", "activated", "dev-a", "dev-z", int64(10_000_000_000), provisioningSentinel)
	require.NoError(t, err)

	// Seed one probe
	err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		bucketStart, bucketStart, int64(1), int32(0), "dev-a", "dev-z", "link-prov", int64(100), false)
	require.NoError(t, err)

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeLinkRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)
	assert.True(t, buckets[0].Provisioning)
}

func TestComputeLinkRollup_ISISDown(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed link without ISIS adjacency
	err := conn.Exec(ctx, `INSERT INTO dim_dz_links_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_z_pk, bandwidth_bps) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"link-entity-noisis", now, now, "00000000-0000-0000-0000-000000000011", uint8(0), "link-noisis", "activated", "dev-a2", "dev-z2", int64(10_000_000_000))
	require.NoError(t, err)

	// No ISIS adjacency seeded for this link

	err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		bucketStart, bucketStart, int64(1), int32(0), "dev-a2", "dev-z2", "link-noisis", int64(100), false)
	require.NoError(t, err)

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeLinkRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)
	// Link with no ISIS adjacency history is not an ISIS link — ISISDown should be false
	assert.False(t, buckets[0].ISISDown)
}

// --- Device interface rollup tests ---

func TestWriteDeviceInterfaceBuckets(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	ctx := context.Background()

	tunnelID := int64(42)
	buckets := []DeviceInterfaceBucket{{
		BucketTS:           time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		DevicePK:           "device-1",
		Intf:               "Ethernet1/1",
		IngestedAt:         time.Now().Truncate(time.Millisecond),
		LinkPK:             "link-1",
		LinkSide:           "A",
		UserTunnelID:       &tunnelID,
		UserPK:             "user-1",
		InErrors:           100,
		OutErrors:          50,
		InFcsErrors:        10,
		InDiscards:         20,
		OutDiscards:        15,
		CarrierTransitions: 3,
		InBps:              InterfaceRateStats{Avg: 1_000_000, P95: 1_500_000, Max: 2_000_000},
		OutBps:             InterfaceRateStats{Avg: 500_000},
		InPps:              InterfaceRateStats{Avg: 1500},
		OutPps:             InterfaceRateStats{Avg: 750},
		Status:             "activated",
		ISISOverload:       false,
		ISISUnreachable:    true,
	}}

	require.NoError(t, a.WriteDeviceInterfaceBuckets(ctx, buckets))

	var inErr, outErr, fcsErr uint64
	var avgInBps, p95InBps, maxInBps float64
	var linkPK, linkSide, userPK, status string
	var userTunnelID *int64
	var isisOverload, isisUnreachable bool
	err := conn.QueryRow(ctx, `
		SELECT in_errors, out_errors, in_fcs_errors, avg_in_bps, p95_in_bps, max_in_bps,
		       link_pk, link_side, user_tunnel_id, user_pk,
		       status, isis_overload, isis_unreachable
		FROM device_interface_rollup_5m FINAL
		WHERE device_pk = 'device-1' AND intf = 'Ethernet1/1'
	`).Scan(&inErr, &outErr, &fcsErr, &avgInBps, &p95InBps, &maxInBps,
		&linkPK, &linkSide, &userTunnelID, &userPK,
		&status, &isisOverload, &isisUnreachable)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), inErr)
	assert.Equal(t, uint64(50), outErr)
	assert.Equal(t, uint64(10), fcsErr)
	assert.InDelta(t, 1_000_000, avgInBps, 0.01)
	assert.InDelta(t, 1_500_000, p95InBps, 0.01)
	assert.InDelta(t, 2_000_000, maxInBps, 0.01)
	assert.Equal(t, "link-1", linkPK)
	assert.Equal(t, "A", linkSide)
	require.NotNil(t, userTunnelID)
	assert.Equal(t, int64(42), *userTunnelID)
	assert.Equal(t, "user-1", userPK)
	assert.Equal(t, "activated", status)
	assert.False(t, isisOverload)
	assert.True(t, isisUnreachable)
}

func TestWriteDeviceInterfaceBuckets_Empty(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	require.NoError(t, a.WriteDeviceInterfaceBuckets(context.Background(), nil))
}

func TestComputeDeviceInterfaceRollup_EmptyTables(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeDeviceInterfaceRollup(context.Background(), BackfillChunkInput{
		WindowStart: time.Now().Add(-1 * time.Hour),
		WindowEnd:   time.Now(),
	})
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

func TestComputeDeviceInterfaceRollup_WithData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed device dimension
	err := conn.Exec(ctx, `INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, device_type, code, metro_pk) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"dev-entity-1", now, now, "00000000-0000-0000-0000-000000000020", uint8(0), "device-1", "activated", "router", "DEV1", "metro-1")
	require.NoError(t, err)

	// Seed multiple counter snapshots for the same interface
	for i := range 5 {
		ts := bucketStart.Add(time.Duration(i) * time.Minute)
		err := conn.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters (event_ts, ingested_at, device_pk, host, intf, link_pk, link_side, in_errors_delta, out_errors_delta, in_fcs_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta, delta_duration) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
			ts, ts, "device-1", "host-1", "Ethernet1/1", "link-1", "A",
			int64(10), int64(5), int64(1), int64(2), int64(1), int64(0),
			int64(125_000*(i+1)), int64(62_500*(i+1)), int64(100*(i+1)), int64(50*(i+1)), float64(1.0))
		require.NoError(t, err)
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeDeviceInterfaceRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	b := buckets[0]
	assert.Equal(t, "device-1", b.DevicePK)
	assert.Equal(t, "Ethernet1/1", b.Intf)

	// Link context from fact table
	assert.Equal(t, "link-1", b.LinkPK)
	assert.Equal(t, "A", b.LinkSide)

	// Counters are summed
	assert.Equal(t, uint64(50), b.InErrors)
	assert.Equal(t, uint64(25), b.OutErrors)

	// BPS percentiles (5 snapshots with increasing rates)
	assert.Greater(t, b.InBps.Avg, float64(0))
	assert.Greater(t, b.InBps.Min, float64(0))
	assert.Greater(t, b.InBps.Max, float64(0))
	assert.GreaterOrEqual(t, b.InBps.Max, b.InBps.P99)
	assert.GreaterOrEqual(t, b.InBps.P99, b.InBps.P95)
	assert.GreaterOrEqual(t, b.InBps.P95, b.InBps.Min)

	// PPS
	assert.Greater(t, b.InPps.Avg, float64(0))
	assert.Greater(t, b.OutPps.Avg, float64(0))

	// Device state
	assert.Equal(t, "activated", b.Status)
	assert.False(t, b.ISISOverload)
	assert.False(t, b.ISISUnreachable)

	// Write back and verify
	require.NoError(t, a.WriteDeviceInterfaceBuckets(ctx, buckets))
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM device_interface_rollup_5m").Scan(&count))
	assert.Equal(t, uint64(1), count)
}

func TestComputeDeviceInterfaceRollup_WithUserTunnel(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed device dimension
	err := conn.Exec(ctx, `INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, device_type, code, metro_pk) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"dev-entity-tun", now, now, "00000000-0000-0000-0000-000000000030", uint8(0), "device-tun", "activated", "router", "DEVTUN", "metro-1")
	require.NoError(t, err)

	// Seed user dimension
	err = conn.Exec(ctx, `INSERT INTO dim_dz_users_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, device_pk, tunnel_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"user-entity-1", now, now, "00000000-0000-0000-0000-000000000031", uint8(0), "user-pk-1", "activated", "device-tun", int32(42))
	require.NoError(t, err)

	// Seed counter with tunnel ID
	tunnelID := int64(42)
	err = conn.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters (event_ts, ingested_at, device_pk, host, intf, link_pk, link_side, user_tunnel_id, in_errors_delta, out_errors_delta, in_fcs_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta, delta_duration) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`,
		bucketStart, bucketStart, "device-tun", "host-tun", "Ethernet2/1", "", "", &tunnelID,
		int64(0), int64(0), int64(0), int64(0), int64(0), int64(0),
		int64(125_000), int64(62_500), int64(100), int64(50), float64(1.0))
	require.NoError(t, err)

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeDeviceInterfaceRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	b := buckets[0]
	assert.Equal(t, "device-tun", b.DevicePK)
	require.NotNil(t, b.UserTunnelID)
	assert.Equal(t, int64(42), *b.UserTunnelID)
	assert.Equal(t, "user-pk-1", b.UserPK)
}
