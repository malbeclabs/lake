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

	// Seed ISIS adjacencies for both sides of the link (both sides must be UP for isis_down=false)
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-1", now, now, "00000000-0000-0000-0000-000000000002", uint8(0), "link-1", "device-a", "sys-1", "sys-2")
	require.NoError(t, err)
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-2", now, now, "00000000-0000-0000-0000-000000000003", uint8(0), "link-1", "device-z", "sys-2", "sys-1")
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

// TestComputeLinkRollup_ISISHostnameRename verifies that a device hostname rename —
// which emits a simultaneous is_deleted=1 (old entity) and is_deleted=0 (new entity)
// at the same snapshot_ts — does not cause a false isis_down=true.
func TestComputeLinkRollup_ISISHostnameRename(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)
	initialTS := now.Add(-20 * time.Minute)
	renameTS := now.Add(-15 * time.Minute) // before window start, processed as carry-forward

	err := conn.Exec(ctx, `INSERT INTO dim_dz_links_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_z_pk, bandwidth_bps, committed_rtt_ns) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"link-entity-rename", now, now, "00000000-0000-0000-0000-000000000020", uint8(0), "link-rename", "activated", "device-a", "device-z", int64(10_000_000_000), int64(500_000))
	require.NoError(t, err)

	// Initial state: both sides active
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-a-old", initialTS, initialTS, "00000000-0000-0000-0000-000000000021", uint8(0), "link-rename", "device-a", "sys-a", "sys-z")
	require.NoError(t, err)
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-z", initialTS, initialTS, "00000000-0000-0000-0000-000000000022", uint8(0), "link-rename", "device-z", "sys-z", "sys-a")
	require.NoError(t, err)

	// Hostname rename: delete old entity AND create new entity at the same snapshot_ts.
	// Nondeterministic row ordering in ClickHouse previously caused this to mark the link down.
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-a-old", renameTS, renameTS, "00000000-0000-0000-0000-000000000023", uint8(1), "link-rename", "device-a", "sys-a", "sys-z")
	require.NoError(t, err)
	err = conn.Exec(ctx, `INSERT INTO dim_isis_adjacencies_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, link_pk, device_pk, system_id, neighbor_system_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		"isis-adj-a-new", renameTS, renameTS, "00000000-0000-0000-0000-000000000024", uint8(0), "link-rename", "device-a-renamed", "sys-a-new", "sys-z")
	require.NoError(t, err)

	err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		bucketStart, bucketStart, int64(1), int32(0), "device-a", "device-z", "link-rename", int64(100), false)
	require.NoError(t, err)

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeLinkRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)
	// After rename, old entity is deleted but new entity is active alongside the Z side — link is UP.
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
		InMcastPps:         InterfaceRateStats{Avg: 200, P95: 350, Max: 500},
		OutMcastPps:        InterfaceRateStats{Avg: 100, P95: 175, Max: 250},
		Status:             "activated",
		ISISOverload:       false,
		ISISUnreachable:    true,
	}}

	require.NoError(t, a.WriteDeviceInterfaceBuckets(ctx, buckets))

	var inErr, outErr, fcsErr uint64
	var avgInBps, p95InBps, maxInBps float64
	var avgInMcastPps, p95InMcastPps, maxInMcastPps float64
	var avgOutMcastPps, p95OutMcastPps, maxOutMcastPps float64
	var linkPK, linkSide, userPK, status string
	var userTunnelID *int64
	var isisOverload, isisUnreachable bool
	err := conn.QueryRow(ctx, `
		SELECT in_errors, out_errors, in_fcs_errors, avg_in_bps, p95_in_bps, max_in_bps,
		       avg_in_mcast_pps, p95_in_mcast_pps, max_in_mcast_pps,
		       avg_out_mcast_pps, p95_out_mcast_pps, max_out_mcast_pps,
		       link_pk, link_side, user_tunnel_id, user_pk,
		       status, isis_overload, isis_unreachable
		FROM device_interface_rollup_5m FINAL
		WHERE device_pk = 'device-1' AND intf = 'Ethernet1/1'
	`).Scan(&inErr, &outErr, &fcsErr, &avgInBps, &p95InBps, &maxInBps,
		&avgInMcastPps, &p95InMcastPps, &maxInMcastPps,
		&avgOutMcastPps, &p95OutMcastPps, &maxOutMcastPps,
		&linkPK, &linkSide, &userTunnelID, &userPK,
		&status, &isisOverload, &isisUnreachable)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), inErr)
	assert.Equal(t, uint64(50), outErr)
	assert.Equal(t, uint64(10), fcsErr)
	assert.InDelta(t, 1_000_000, avgInBps, 0.01)
	assert.InDelta(t, 1_500_000, p95InBps, 0.01)
	assert.InDelta(t, 2_000_000, maxInBps, 0.01)
	assert.InDelta(t, 200, avgInMcastPps, 0.01)
	assert.InDelta(t, 350, p95InMcastPps, 0.01)
	assert.InDelta(t, 500, maxInMcastPps, 0.01)
	assert.InDelta(t, 100, avgOutMcastPps, 0.01)
	assert.InDelta(t, 175, p95OutMcastPps, 0.01)
	assert.InDelta(t, 250, maxOutMcastPps, 0.01)
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
		err := conn.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters (event_ts, ingested_at, device_pk, host, intf, link_pk, link_side, in_errors_delta, out_errors_delta, in_fcs_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta, in_multicast_pkts_delta, out_multicast_pkts_delta, delta_duration) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
			ts, ts, "device-1", "host-1", "Ethernet1/1", "link-1", "A",
			int64(10), int64(5), int64(1), int64(2), int64(1), int64(0),
			int64(125_000*(i+1)), int64(62_500*(i+1)), int64(100*(i+1)), int64(50*(i+1)),
			int64(30*(i+1)), int64(15*(i+1)), float64(1.0))
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

	// Multicast PPS (5 snapshots with increasing rates: 30/s, 60/s, 90/s, 120/s, 150/s)
	assert.Greater(t, b.InMcastPps.Avg, float64(0))
	assert.Greater(t, b.InMcastPps.Min, float64(0))
	assert.Greater(t, b.InMcastPps.Max, float64(0))
	assert.GreaterOrEqual(t, b.InMcastPps.Max, b.InMcastPps.P99)
	assert.GreaterOrEqual(t, b.InMcastPps.P99, b.InMcastPps.P95)
	assert.GreaterOrEqual(t, b.InMcastPps.P95, b.InMcastPps.Min)
	assert.Greater(t, b.OutMcastPps.Avg, float64(0))
	assert.Greater(t, b.OutMcastPps.Max, float64(0))

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

// TestComputeDeviceInterfaceRollupFromGNMI_ComputesDeltas verifies the gNMI
// branch derives per-bucket deltas from interface_state's RAW CUMULATIVE counters
// (the InfluxDB fact table arrives pre-delta'd; interface_state does not).
func TestComputeDeviceInterfaceRollupFromGNMI_ComputesDeltas(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	// interface_state lives in the telemetry_* schema; create it in the test DB.
	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS interface_state (
			timestamp DateTime64(9),
			device_pubkey LowCardinality(String),
			interface_name String,
			admin_status LowCardinality(String),
			oper_status LowCardinality(String),
			ifindex UInt32, mtu UInt16, last_change Int64,
			carrier_transitions UInt64,
			in_octets UInt64, out_octets UInt64, in_pkts UInt64, out_pkts UInt64,
			in_errors UInt64, out_errors UInt64, in_discards UInt64, out_discards UInt64,
			in_fcs_errors UInt64,
			in_unicast_pkts UInt64, in_multicast_pkts UInt64, in_broadcast_pkts UInt64,
			out_unicast_pkts UInt64, out_multicast_pkts UInt64, out_broadcast_pkts UInt64
		) ENGINE = MergeTree() ORDER BY (device_pubkey, interface_name, timestamp)
	`))

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// 3 cumulative samples in one 5-min bucket -> 2 consecutive deltas.
	// per minute: in_octets +125000, in_pkts +100, in_errors +10, out_errors +5,
	// in_discards +2, in_multicast_pkts +30.
	for i := range 3 {
		ts := bucketStart.Add(time.Duration(i) * time.Minute)
		require.NoError(t, conn.Exec(ctx, `INSERT INTO interface_state
			(timestamp, device_pubkey, interface_name, admin_status, oper_status, ifindex, mtu, last_change,
			 carrier_transitions, in_octets, out_octets, in_pkts, out_pkts, in_errors, out_errors, in_discards, out_discards,
			 in_fcs_errors, in_unicast_pkts, in_multicast_pkts, in_broadcast_pkts, out_unicast_pkts, out_multicast_pkts, out_broadcast_pkts)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
			ts, "device-1", "Ethernet1/1", "UP", "UP", uint32(1), uint16(9000), int64(0),
			uint64(0),
			uint64(1000+125000*i), uint64(500+62500*i), uint64(10+100*i), uint64(5+50*i),
			uint64(10*i), uint64(5*i), uint64(2*i), uint64(1*i),
			uint64(0), uint64(0), uint64(30*i), uint64(0), uint64(0), uint64(0), uint64(0)))
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeDeviceInterfaceRollupFromGNMI(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	b := buckets[0]
	assert.Equal(t, "device-1", b.DevicePK)
	assert.Equal(t, "Ethernet1/1", b.Intf)
	// 2 deltas summed
	assert.Equal(t, uint64(20), b.InErrors)
	assert.Equal(t, uint64(10), b.OutErrors)
	assert.Equal(t, uint64(4), b.InDiscards)
	// rates derived from cumulative diffs
	assert.Greater(t, b.InBps.Avg, float64(0))
	assert.Greater(t, b.InPps.Avg, float64(0))
	assert.Greater(t, b.InMcastPps.Avg, float64(0))
}

// TestComputeDeviceInterfaceRollupFromGNMI_EnrichesAndDropsJunk verifies the gNMI
// branch enriches link/tunnel context (from dim_dz_links_history / Tunnel<N> name)
// and drops junk interfaces: not mapped to a link or tunnel and carrying no traffic.
func TestComputeDeviceInterfaceRollupFromGNMI_EnrichesAndDropsJunk(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS interface_state (
			timestamp DateTime64(9),
			device_pubkey LowCardinality(String), interface_name String,
			admin_status LowCardinality(String), oper_status LowCardinality(String),
			ifindex UInt32, mtu UInt16, last_change Int64,
			carrier_transitions UInt64,
			in_octets UInt64, out_octets UInt64, in_pkts UInt64, out_pkts UInt64,
			in_errors UInt64, out_errors UInt64, in_discards UInt64, out_discards UInt64,
			in_fcs_errors UInt64,
			in_unicast_pkts UInt64, in_multicast_pkts UInt64, in_broadcast_pkts UInt64,
			out_unicast_pkts UInt64, out_multicast_pkts UInt64, out_broadcast_pkts UInt64
		) ENGINE = MergeTree() ORDER BY (device_pubkey, interface_name, timestamp)
	`))

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// device-1/Ethernet1/1 is side A of link-1.
	require.NoError(t, conn.Exec(ctx, `INSERT INTO dim_dz_links_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_a_iface_name, side_z_pk, side_z_iface_name) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		"link-entity-1", now, now, "00000000-0000-0000-0000-000000000031", uint8(0), "link-1", "activated", "device-1", "Ethernet1/1", "device-2", "Ethernet9/9"))

	// octetsStep>0 => traffic; octetsStep==0 => flat (no traffic).
	insert := func(intf string, octetsStep uint64) {
		for i := range 3 {
			ts := bucketStart.Add(time.Duration(i) * time.Minute)
			require.NoError(t, conn.Exec(ctx, `INSERT INTO interface_state
				(timestamp, device_pubkey, interface_name, admin_status, oper_status, ifindex, mtu, last_change,
				 carrier_transitions, in_octets, out_octets, in_pkts, out_pkts, in_errors, out_errors, in_discards, out_discards,
				 in_fcs_errors, in_unicast_pkts, in_multicast_pkts, in_broadcast_pkts, out_unicast_pkts, out_multicast_pkts, out_broadcast_pkts)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
				ts, "device-1", intf, "UP", "UP", uint32(1), uint16(9000), int64(0),
				uint64(0),
				uint64(1000+octetsStep*uint64(i)), uint64(500+octetsStep*uint64(i)), uint64(10+100*i), uint64(5+50*i),
				uint64(0), uint64(0), uint64(0), uint64(0),
				uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)))
		}
	}
	insert("Ethernet1/1", 125000) // link-mapped + traffic -> keep
	insert("Tunnel500", 125000)   // user tunnel + traffic -> keep
	insert("Ethernet48", 0)       // no link/tunnel, flat octets (no traffic) -> drop

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeDeviceInterfaceRollupFromGNMI(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)

	byIntf := make(map[string]DeviceInterfaceBucket)
	for _, b := range buckets {
		byIntf[b.Intf] = b
	}

	_, hasJunk := byIntf["Ethernet48"]
	assert.False(t, hasJunk, "junk interface (no link/tunnel, no traffic) should be dropped")

	eth, ok := byIntf["Ethernet1/1"]
	require.True(t, ok, "link-mapped interface should be kept")
	assert.Equal(t, "link-1", eth.LinkPK)
	assert.Equal(t, "A", eth.LinkSide)

	tun, ok := byIntf["Tunnel500"]
	require.True(t, ok, "user-tunnel interface should be kept")
	require.NotNil(t, tun.UserTunnelID)
	assert.Equal(t, int64(500), *tun.UserTunnelID)
}

// TestCoalesceDeviceInterfaceBuckets_PrefersGNMI verifies the per-device merge: a device
// present in the gNMI buckets wins (its fact-table buckets are dropped), and a device with
// no gNMI data falls back to its fact-table buckets.
func TestCoalesceDeviceInterfaceBuckets_PrefersGNMI(t *testing.T) {
	t.Parallel()
	gnmi := []DeviceInterfaceBucket{
		{DevicePK: "d1", Intf: "Ethernet1", InErrors: 99},
	}
	fact := []DeviceInterfaceBucket{
		{DevicePK: "d1", Intf: "Ethernet1", InErrors: 1}, // superseded by gNMI
		{DevicePK: "d2", Intf: "Ethernet2", InErrors: 7}, // no gNMI for d2 -> kept
	}

	out := coalesceDeviceInterfaceBuckets(gnmi, fact)

	byDev := make(map[string]DeviceInterfaceBucket)
	for _, b := range out {
		byDev[b.DevicePK] = b
	}

	require.Len(t, out, 2)
	require.Contains(t, byDev, "d1")
	require.Contains(t, byDev, "d2")
	assert.Equal(t, uint64(99), byDev["d1"].InErrors, "d1 should come from gNMI, not fact")
	assert.Equal(t, uint64(7), byDev["d2"].InErrors, "d2 has no gNMI data -> fall back to fact")
}

// TestComputeDeviceInterfaceRollupFromGNMI_ResolvesDeviceState verifies the gNMI branch
// resolves device Status (from dim_dz_devices_history) onto its buckets, like the fact path.
func TestComputeDeviceInterfaceRollupFromGNMI_ResolvesDeviceState(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS interface_state (
			timestamp DateTime64(9),
			device_pubkey LowCardinality(String), interface_name String,
			admin_status LowCardinality(String), oper_status LowCardinality(String),
			ifindex UInt32, mtu UInt16, last_change Int64,
			carrier_transitions UInt64,
			in_octets UInt64, out_octets UInt64, in_pkts UInt64, out_pkts UInt64,
			in_errors UInt64, out_errors UInt64, in_discards UInt64, out_discards UInt64,
			in_fcs_errors UInt64,
			in_unicast_pkts UInt64, in_multicast_pkts UInt64, in_broadcast_pkts UInt64,
			out_unicast_pkts UInt64, out_multicast_pkts UInt64, out_broadcast_pkts UInt64
		) ENGINE = MergeTree() ORDER BY (device_pubkey, interface_name, timestamp)
	`))

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	require.NoError(t, conn.Exec(ctx, `INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, device_type, code, metro_pk) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"dev-entity-9", now, now, "00000000-0000-0000-0000-000000000041", uint8(0), "device-9", "soft-drained", "router", "DEV9", "metro-1"))

	for i := range 3 {
		ts := bucketStart.Add(time.Duration(i) * time.Minute)
		require.NoError(t, conn.Exec(ctx, `INSERT INTO interface_state
			(timestamp, device_pubkey, interface_name, admin_status, oper_status, ifindex, mtu, last_change,
			 carrier_transitions, in_octets, out_octets, in_pkts, out_pkts, in_errors, out_errors, in_discards, out_discards,
			 in_fcs_errors, in_unicast_pkts, in_multicast_pkts, in_broadcast_pkts, out_unicast_pkts, out_multicast_pkts, out_broadcast_pkts)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
			ts, "device-9", "Ethernet1/1", "UP", "UP", uint32(1), uint16(9000), int64(0),
			uint64(0),
			uint64(1000+125000*i), uint64(500+62500*i), uint64(10+100*i), uint64(5+50*i),
			uint64(0), uint64(0), uint64(0), uint64(0),
			uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)))
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeDeviceInterfaceRollupFromGNMI(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.NotEmpty(t, buckets)
	assert.Equal(t, "soft-drained", buckets[0].Status, "gNMI bucket should have device Status resolved")
}

// TestDeviceInterfaceRollup_GNMIMatchesInfluxForSameData seeds equivalent underlying data
// into both sources (raw cumulative counters in interface_state; the equivalent precomputed
// deltas in the fact table) and asserts the two rollups produce matching totals and average
// rates. This guards that the gNMI delta math equals the InfluxDB path given the same samples.
func TestDeviceInterfaceRollup_GNMIMatchesInfluxForSameData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS interface_state (
			timestamp DateTime64(9),
			device_pubkey LowCardinality(String), interface_name String,
			admin_status LowCardinality(String), oper_status LowCardinality(String),
			ifindex UInt32, mtu UInt16, last_change Int64,
			carrier_transitions UInt64,
			in_octets UInt64, out_octets UInt64, in_pkts UInt64, out_pkts UInt64,
			in_errors UInt64, out_errors UInt64, in_discards UInt64, out_discards UInt64,
			in_fcs_errors UInt64,
			in_unicast_pkts UInt64, in_multicast_pkts UInt64, in_broadcast_pkts UInt64,
			out_unicast_pkts UInt64, out_multicast_pkts UInt64, out_broadcast_pkts UInt64
		) ENGINE = MergeTree() ORDER BY (device_pubkey, interface_name, timestamp)
	`))

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Per minute: in_octets +125000, out_octets +62500, in_pkts +100, out_pkts +50,
	// in_errors +10, out_errors +5, in_discards +2, in_multicast_pkts +30.
	// gNMI: 3 cumulative samples (-> 2 deltas).
	for i := range 3 {
		ts := bucketStart.Add(time.Duration(i) * time.Minute)
		require.NoError(t, conn.Exec(ctx, `INSERT INTO interface_state
			(timestamp, device_pubkey, interface_name, admin_status, oper_status, ifindex, mtu, last_change,
			 carrier_transitions, in_octets, out_octets, in_pkts, out_pkts, in_errors, out_errors, in_discards, out_discards,
			 in_fcs_errors, in_unicast_pkts, in_multicast_pkts, in_broadcast_pkts, out_unicast_pkts, out_multicast_pkts, out_broadcast_pkts)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
			ts, "device-eq", "Ethernet1/1", "UP", "UP", uint32(1), uint16(9000), int64(0),
			uint64(0),
			uint64(1000+125000*i), uint64(500+62500*i), uint64(10+100*i), uint64(5+50*i),
			uint64(10*i), uint64(5*i), uint64(2*i), uint64(0),
			uint64(0), uint64(0), uint64(30*i), uint64(0), uint64(0), uint64(0), uint64(0)))
	}
	// Fact: the equivalent 2 delta rows (matching the 2 gNMI deltas).
	for i := 1; i < 3; i++ {
		ts := bucketStart.Add(time.Duration(i) * time.Minute)
		require.NoError(t, conn.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters (event_ts, ingested_at, device_pk, host, intf, link_pk, link_side, in_errors_delta, out_errors_delta, in_fcs_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta, in_multicast_pkts_delta, out_multicast_pkts_delta, delta_duration) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
			ts, ts, "device-eq", "host", "Ethernet1/1", "", "",
			int64(10), int64(5), int64(0), int64(2), int64(0), int64(0),
			int64(125000), int64(62500), int64(100), int64(50),
			int64(30), int64(0), float64(60.0)))
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	input := BackfillChunkInput{WindowStart: now.Add(-10 * time.Minute), WindowEnd: now.Add(5 * time.Minute)}

	gnmi, err := a.ComputeDeviceInterfaceRollupFromGNMI(ctx, input)
	require.NoError(t, err)
	require.Len(t, gnmi, 1)
	fact, err := a.ComputeDeviceInterfaceRollup(ctx, input)
	require.NoError(t, err)
	require.Len(t, fact, 1)

	g, f := gnmi[0], fact[0]
	// Totals match exactly.
	assert.Equal(t, f.InErrors, g.InErrors)
	assert.Equal(t, f.OutErrors, g.OutErrors)
	assert.Equal(t, f.InDiscards, g.InDiscards)
	assert.Equal(t, f.OutDiscards, g.OutDiscards)
	assert.Equal(t, f.CarrierTransitions, g.CarrierTransitions)
	// Average rates match (same samples, same aggregation).
	assert.InDelta(t, f.InBps.Avg, g.InBps.Avg, 1.0)
	assert.InDelta(t, f.OutBps.Avg, g.OutBps.Avg, 1.0)
	assert.InDelta(t, f.InPps.Avg, g.InPps.Avg, 0.01)
	assert.InDelta(t, f.InMcastPps.Avg, g.InMcastPps.Avg, 0.01)
}

// TestTelemetryDatabaseForNetwork verifies the gNMI telemetry DB name derived per network.
func TestTelemetryDatabaseForNetwork(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "telemetry_mainnet_beta", telemetryDatabaseForNetwork("mainnet-beta"))
	assert.Equal(t, "telemetry_testnet", telemetryDatabaseForNetwork("testnet"))
	assert.Equal(t, "telemetry_devnet", telemetryDatabaseForNetwork("devnet"))
	assert.Equal(t, "", telemetryDatabaseForNetwork(""), "empty network -> gNMI off (fact-only)")
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

// TestComputeDeviceInterfaceRollup_DeduplicatesStaleRows verifies that the rollup
// uses FINAL when reading the fact table, so stale duplicate rows (e.g. from a
// backfill that overwrites inflated-rate rows) are excluded. Without FINAL, maxIf
// would see both the old inflated row and the new clean row and return 74 Gbps.
func TestComputeDeviceInterfaceRollup_DeduplicatesStaleRows(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	ts := now.Add(-4 * time.Minute)
	oldIngestedAt := now.Add(-1 * time.Hour)

	insert := `INSERT INTO fact_dz_device_interface_counters
		(event_ts, ingested_at, device_pk, host, intf, link_pk, link_side,
		 in_errors_delta, out_errors_delta, in_fcs_errors_delta, in_discards_delta, out_discards_delta,
		 carrier_transitions_delta, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta,
		 in_multicast_pkts_delta, out_multicast_pkts_delta, delta_duration)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`

	// Old row: inflated rate due to tiny delta_duration (simulates pre-fix carrier-transition bug).
	// 63_745_167 bytes / 0.007s ≈ 74 Gbps.
	err := conn.Exec(ctx, insert,
		ts, oldIngestedAt, "device-dedup", "host-1", "Ethernet1/1", "", "",
		int64(0), int64(0), int64(0), int64(0), int64(0), int64(0),
		int64(63_745_167), int64(0), int64(100), int64(50), int64(0), int64(0),
		float64(0.007))
	require.NoError(t, err)

	// New row: same key (event_ts, device_pk, intf), newer ingested_at, correct delta_duration.
	// 63_745_167 bytes / 2.0s ≈ 255 Mbps.
	err = conn.Exec(ctx, insert,
		ts, now, "device-dedup", "host-1", "Ethernet1/1", "", "",
		int64(0), int64(0), int64(0), int64(0), int64(0), int64(0),
		int64(63_745_167), int64(0), int64(100), int64(50), int64(0), int64(0),
		float64(2.0))
	require.NoError(t, err)

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeDeviceInterfaceRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	// With FINAL the newer (clean) row wins: max should be ~255 Mbps, nowhere near 74 Gbps.
	assert.Less(t, buckets[0].InBps.Max, 1e9, "expected <1 Gbps but got inflated rate — FINAL missing from fact table read?")
}

// TestComputeLinkRollup_DeduplicatesStaleRows verifies that the link rollup uses
// FINAL when reading the latency fact table so duplicate rows from backfills don't
// inflate sample counts or skew percentiles.
func TestComputeLinkRollup_DeduplicatesStaleRows(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	ts := now.Add(-4 * time.Minute)
	oldIngestedAt := now.Add(-1 * time.Hour)

	// Seed required link dimension.
	err := conn.Exec(ctx, `INSERT INTO dim_dz_links_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_z_pk, bandwidth_bps, committed_rtt_ns)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"link-entity-dedup", now, now, "00000000-0000-0000-0000-000000000099", uint8(0),
		"link-dedup", "activated", "dev-dedup-a", "dev-dedup-z", int64(10_000_000_000), int64(500_000))
	require.NoError(t, err)

	insert := `INSERT INTO fact_dz_device_link_latency
		(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	// Old row: stale ingested_at, rtt=100us.
	err = conn.Exec(ctx, insert, ts, oldIngestedAt, int64(1), int32(0), "dev-dedup-a", "dev-dedup-z", "link-dedup", int64(100), false)
	require.NoError(t, err)

	// New row: same key, newer ingested_at, rtt=300us.
	err = conn.Exec(ctx, insert, ts, now, int64(1), int32(0), "dev-dedup-a", "dev-dedup-z", "link-dedup", int64(300), false)
	require.NoError(t, err)

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	buckets, err := a.ComputeLinkRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	// With FINAL only the newer row is seen: 1 sample at 300us.
	// Without FINAL both rows are seen: 2 samples averaging (100+300)/2=200us.
	b := buckets[0]
	assert.Equal(t, uint32(1), b.A.Samples, "expected 1 sample — duplicate rows not deduplicated, FINAL missing?")
	assert.InDelta(t, 300.0, b.A.AvgRttUs, 1.0)
}
