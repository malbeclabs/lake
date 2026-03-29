package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedLinkRollup(t *testing.T, api *handlers.API, bucketTS time.Time, linkPK string, aAvg, zAvg, aLoss, zLoss float64, aSamples, zSamples uint32, status string, provisioning, isisDown bool) {
	t.Helper()
	ctx := t.Context()
	err := api.DB.Exec(ctx, `INSERT INTO link_rollup_5m (
		bucket_ts, link_pk, ingested_at,
		a_avg_rtt_us, a_min_rtt_us, a_p50_rtt_us, a_p90_rtt_us, a_p95_rtt_us, a_p99_rtt_us, a_max_rtt_us, a_loss_pct, a_samples,
		z_avg_rtt_us, z_min_rtt_us, z_p50_rtt_us, z_p90_rtt_us, z_p95_rtt_us, z_p99_rtt_us, z_max_rtt_us, z_loss_pct, z_samples,
		status, provisioning, isis_down
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
		bucketTS, linkPK, time.Now(),
		aAvg, aAvg*0.5, aAvg*0.9, aAvg*1.1, aAvg*1.2, aAvg*1.3, aAvg*1.5, aLoss, aSamples,
		zAvg, zAvg*0.5, zAvg*0.9, zAvg*1.1, zAvg*1.2, zAvg*1.3, zAvg*1.5, zLoss, zSamples,
		status, provisioning, isisDown,
	)
	require.NoError(t, err)
}

func seedInterfaceRollup(t *testing.T, api *handlers.API, bucketTS time.Time, devicePK, intf, linkPK, linkSide string, inErrors, outErrors uint64, avgInBps float64, status string) {
	t.Helper()
	ctx := t.Context()
	err := api.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m (
		bucket_ts, device_pk, intf, ingested_at,
		link_pk, link_side,
		in_errors, out_errors, in_fcs_errors, in_discards, out_discards, carrier_transitions,
		avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps,
		status, isis_overload, isis_unreachable
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43)`,
		bucketTS, devicePK, intf, time.Now(),
		linkPK, linkSide,
		inErrors, outErrors, uint64(0), uint64(0), uint64(0), uint64(0),
		avgInBps, avgInBps*0.5, avgInBps*0.8, avgInBps*1.2, avgInBps*1.3, avgInBps*1.5, avgInBps*2.0,
		avgInBps*0.5, avgInBps*0.25, avgInBps*0.4, avgInBps*0.6, avgInBps*0.65, avgInBps*0.75, avgInBps,
		float64(1000), float64(500), float64(800), float64(1200), float64(1300), float64(1500), float64(2000),
		float64(500), float64(250), float64(400), float64(600), float64(650), float64(750), float64(1000),
		status, false, false,
	)
	require.NoError(t, err)
}

func seedLinkMetadata(t *testing.T, api *handlers.API, pk, code, linkType, contributorPK, sideAPK, sideZPK string, bandwidthBps, committedRttNs int64, status string) {
	t.Helper()
	seedLinkMetadataAt(t, api, pk, code, linkType, contributorPK, sideAPK, sideZPK, bandwidthBps, committedRttNs, status, time.Now())
}

func seedLinkMetadataAt(t *testing.T, api *handlers.API, pk, code, linkType, contributorPK, sideAPK, sideZPK string, bandwidthBps, committedRttNs int64, status string, snapshotTS time.Time) {
	t.Helper()
	ctx := t.Context()
	err := api.DB.Exec(ctx, `INSERT INTO dim_dz_links_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted,
		pk, code, link_type, status, contributor_pk, side_a_pk, side_z_pk,
		bandwidth_bps, committed_rtt_ns
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		pk, snapshotTS, snapshotTS, "00000000-0000-0000-0000-000000000001", uint8(0),
		pk, code, linkType, status, contributorPK, sideAPK, sideZPK,
		bandwidthBps, committedRttNs,
	)
	require.NoError(t, err)
}

func seedDeviceMetadata(t *testing.T, api *handlers.API, pk, code, deviceType, contributorPK, metroPK string, maxUsers int32, status string) {
	t.Helper()
	ctx := t.Context()
	now := time.Now()
	err := api.DB.Exec(ctx, `INSERT INTO dim_dz_devices_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted,
		pk, code, device_type, status, contributor_pk, metro_pk, max_users
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		pk, now, now, "00000000-0000-0000-0000-000000000002", uint8(0),
		pk, code, deviceType, status, contributorPK, metroPK, maxUsers,
	)
	require.NoError(t, err)
}

func seedMetro(t *testing.T, api *handlers.API, pk, code string) {
	t.Helper()
	now := time.Now()
	err := api.DB.Exec(t.Context(), `INSERT INTO dim_dz_metros_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, code
	) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		pk, now, now, "00000000-0000-0000-0000-000000000003", uint8(0), pk, code,
	)
	require.NoError(t, err)
}

func seedContributor(t *testing.T, api *handlers.API, pk, code string) {
	t.Helper()
	now := time.Now()
	err := api.DB.Exec(t.Context(), `INSERT INTO dim_dz_contributors_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, code
	) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		pk, now, now, "00000000-0000-0000-0000-000000000004", uint8(0), pk, code,
	)
	require.NoError(t, err)
}

// TestQueryLinkRollup verifies the link rollup query executes valid SQL
// and correctly re-buckets 5-minute data into display buckets.
func TestQueryLinkRollup(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Seed dimension data
	seedMetro(t, api, "metro-a", "NYC")
	seedMetro(t, api, "metro-z", "LAX")
	seedContributor(t, api, "contrib-1", "acme")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-a", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-z", 10, "activated")
	seedLinkMetadata(t, api, "link-1", "NYC-LAX-1", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")

	// Seed 12 five-minute rollup buckets (1 hour of data)
	now := time.Now().UTC().Truncate(5 * time.Minute)
	for i := 0; i < 12; i++ {
		ts := now.Add(-time.Duration(12-i) * 5 * time.Minute)
		seedLinkRollup(t, api, ts, "link-1", 100.0, 110.0, 0.5, 0.3, 90, 90, "activated", false, false)
	}

	// Query with 20-minute display buckets over 1 hour
	params := handlers.ExportParseBucketParams("1h", 3)
	result, err := handlers.ExportQueryLinkRollup(ctx, api.DB, params)
	require.NoError(t, err)

	// Should have 3 display buckets × 1 link = 3 entries
	assert.GreaterOrEqual(t, len(result), 3, "expected at least 3 re-bucketed entries")

	// Verify sample-weighted averaging works
	for _, row := range result {
		assert.Equal(t, "link-1", row.LinkPK)
		assert.Greater(t, row.ASamples, uint64(0))
		assert.Greater(t, row.ZSamples, uint64(0))
		assert.InDelta(t, 100.0, row.AAvgRttUs, 1.0)
		assert.InDelta(t, 110.0, row.ZAvgRttUs, 1.0)
		assert.Equal(t, "activated", row.Status)
		assert.False(t, row.Provisioning)
		assert.False(t, row.ISISDown)
	}
}

// TestQueryLinkRollup_Empty verifies the query works with no data.
func TestQueryLinkRollup_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	params := handlers.ExportParseBucketParams("1h", 3)
	result, err := handlers.ExportQueryLinkRollup(t.Context(), api.DB, params)
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestQueryInterfaceRollup verifies the interface rollup query executes valid SQL
// and supports different grouping modes.
func TestQueryInterfaceRollup(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	now := time.Now().UTC().Truncate(5 * time.Minute)
	for i := 0; i < 12; i++ {
		ts := now.Add(-time.Duration(12-i) * 5 * time.Minute)
		seedInterfaceRollup(t, api, ts, "dev-a", "Ethernet1/1", "link-1", "A", 10, 5, 1_000_000, "activated")
		seedInterfaceRollup(t, api, ts, "dev-z", "Ethernet1/1", "link-1", "Z", 0, 0, 500_000, "activated")
	}

	params := handlers.ExportParseBucketParams("1h", 3)

	t.Run("GroupByLinkSide", func(t *testing.T) {
		rows, err := handlers.ExportQueryInterfaceRollup(t.Context(), api.DB, params, handlers.ExportInterfaceRollupOpts{
			GroupBy: handlers.ExportGroupByLinkSide,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, rows)

		// Should have entries for both sides
		sides := map[string]bool{}
		for _, r := range rows {
			sides[r.LinkSide] = true
			assert.Equal(t, "link-1", r.LinkPK)
		}
		assert.True(t, sides["A"], "expected side A data")
		assert.True(t, sides["Z"], "expected side Z data")
	})

	t.Run("GroupByDevice", func(t *testing.T) {
		rows, err := handlers.ExportQueryInterfaceRollup(t.Context(), api.DB, params, handlers.ExportInterfaceRollupOpts{
			GroupBy: handlers.ExportGroupByDevice,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, rows)

		devices := map[string]bool{}
		for _, r := range rows {
			devices[r.DevicePK] = true
		}
		assert.True(t, devices["dev-a"])
		assert.True(t, devices["dev-z"])
	})

	t.Run("GroupByDeviceIntf", func(t *testing.T) {
		rows, err := handlers.ExportQueryInterfaceRollup(t.Context(), api.DB, params, handlers.ExportInterfaceRollupOpts{
			GroupBy:   handlers.ExportGroupByDeviceIntf,
			DevicePKs: []string{"dev-a"},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, rows)
		for _, r := range rows {
			assert.Equal(t, "dev-a", r.DevicePK)
			assert.Equal(t, "Ethernet1/1", r.Intf)
		}
	})

	t.Run("ErrorsOnly", func(t *testing.T) {
		rows, err := handlers.ExportQueryInterfaceRollup(t.Context(), api.DB, params, handlers.ExportInterfaceRollupOpts{
			GroupBy:    handlers.ExportGroupByDevice,
			ErrorsOnly: true,
		})
		require.NoError(t, err)
		// Only dev-a has errors
		for _, r := range rows {
			assert.Equal(t, "dev-a", r.DevicePK)
		}
	})
}

// TestQueryInterfaceRollup_Empty verifies the query works with no data.
func TestQueryInterfaceRollup_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	params := handlers.ExportParseBucketParams("1h", 3)
	rows, err := handlers.ExportQueryInterfaceRollup(t.Context(), api.DB, params, handlers.ExportInterfaceRollupOpts{
		GroupBy: handlers.ExportGroupByDevice,
	})
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestQueryLinkRollup_FilterByLinkPK verifies filtering by specific link PKs.
func TestQueryLinkRollup_FilterByLinkPK(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	now := time.Now().UTC().Truncate(5 * time.Minute)
	seedLinkRollup(t, api, now.Add(-5*time.Minute), "link-1", 100, 110, 0, 0, 90, 90, "activated", false, false)
	seedLinkRollup(t, api, now.Add(-5*time.Minute), "link-2", 200, 210, 0, 0, 90, 90, "activated", false, false)

	params := handlers.ExportParseBucketParams("1h", 12)

	// Filter to link-1 only
	result, err := handlers.ExportQueryLinkRollup(t.Context(), api.DB, params, "link-1")
	require.NoError(t, err)

	for _, row := range result {
		assert.Equal(t, "link-1", row.LinkPK)
	}
}

// TestQueryLinkRollup_StateColumns verifies entity state columns are returned.
func TestQueryLinkRollup_StateColumns(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	now := time.Now().UTC().Truncate(5 * time.Minute)
	seedLinkRollup(t, api, now.Add(-5*time.Minute), "link-drained", 100, 110, 0, 0, 90, 90, "soft-drained", false, false)
	seedLinkRollup(t, api, now.Add(-5*time.Minute), "link-prov", 100, 110, 0, 0, 90, 90, "activated", true, false)
	seedLinkRollup(t, api, now.Add(-5*time.Minute), "link-isis", 100, 110, 0, 0, 90, 90, "activated", false, true)

	params := handlers.ExportParseBucketParams("1h", 12)
	result, err := handlers.ExportQueryLinkRollup(t.Context(), api.DB, params)
	require.NoError(t, err)

	states := map[string]struct {
		status       string
		provisioning bool
		isisDown     bool
	}{}
	for _, row := range result {
		states[row.LinkPK] = struct {
			status       string
			provisioning bool
			isisDown     bool
		}{row.Status, row.Provisioning, row.ISISDown}
	}

	assert.Equal(t, "soft-drained", states["link-drained"].status)
	assert.True(t, states["link-prov"].provisioning)
	assert.True(t, states["link-isis"].isisDown)
}

// TestLinkRollupVsRaw seeds raw latency data and rollup data from it,
// then verifies that querying with UseRaw=true produces equivalent results
// to querying the rollup table.
func TestLinkRollupVsRaw(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Seed dimension data
	seedMetro(t, api, "metro-a", "NYC")
	seedMetro(t, api, "metro-z", "LAX")
	seedContributor(t, api, "contrib-1", "acme")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-a", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-z", 10, "activated")
	// Seed raw latency probes: 20 probes per direction in one 5-minute bucket
	now := time.Now().UTC().Truncate(5 * time.Minute)
	bucketTS := now.Add(-10 * time.Minute)

	// Seed link metadata with snapshot_ts before the latency bucket so raw state resolution works
	seedLinkMetadataAt(t, api, "link-1", "NYC-LAX-1", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated", bucketTS.Add(-time.Hour))
	for i := range 20 {
		ts := bucketTS.Add(time.Duration(i) * 10 * time.Second)
		// Direction A: dev-a → dev-z, RTT 100-290us
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_dz_device_link_latency
			(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ts, ts, int64(1), int32(i), "dev-a", "dev-z", "link-1", int64(100+i*10), false))
		// Direction Z: dev-z → dev-a, RTT 200-390us
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_dz_device_link_latency
			(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ts, ts, int64(1), int32(20+i), "dev-z", "dev-a", "link-1", int64(200+i*10), false))
	}

	// Also seed the corresponding rollup row (what the rollup worker would produce)
	seedLinkRollup(t, api, bucketTS, "link-1", 195.0, 295.0, 0, 0, 20, 20, "activated", false, false)

	// Query with rollup source (UseRaw=false)
	params := handlers.ExportParseBucketParams("1h", 12) // 5-min buckets
	rollupResult, err := handlers.ExportQueryLinkRollup(ctx, api.DB, params)
	require.NoError(t, err)

	// Query with raw source (UseRaw=true)
	paramsRaw := handlers.ExportParseBucketParams("1h", 12)
	paramsRaw.UseRaw = true
	rawResult, err := handlers.ExportQueryLinkRollup(ctx, api.DB, paramsRaw)
	require.NoError(t, err)

	// Both should have data for link-1
	require.NotEmpty(t, rollupResult, "rollup result should not be empty")
	require.NotEmpty(t, rawResult, "raw result should not be empty")

	// Find the matching bucket in each result
	var rollupRow, rawRow *handlers.ExportLinkRollupRow
	for _, r := range rollupResult {
		if r.LinkPK == "link-1" {
			rollupRow = r
			break
		}
	}
	for _, r := range rawResult {
		if r.LinkPK == "link-1" {
			rawRow = r
			break
		}
	}
	require.NotNil(t, rollupRow, "rollup should have link-1 data")
	require.NotNil(t, rawRow, "raw should have link-1 data")

	// Sample counts should match
	assert.Equal(t, rollupRow.ASamples, rawRow.ASamples, "A samples should match")
	assert.Equal(t, rollupRow.ZSamples, rawRow.ZSamples, "Z samples should match")

	// Latency should be close (rollup seeds approximate values, raw computes exact)
	assert.InDelta(t, rollupRow.AAvgRttUs, rawRow.AAvgRttUs, 5.0, "A avg RTT should be close")
	assert.InDelta(t, rollupRow.ZAvgRttUs, rawRow.ZAvgRttUs, 5.0, "Z avg RTT should be close")

	// Loss should match (both 0%)
	assert.InDelta(t, rollupRow.ALossPct, rawRow.ALossPct, 0.1, "A loss should match")
	assert.InDelta(t, rollupRow.ZLossPct, rawRow.ZLossPct, 0.1, "Z loss should match")

	// Status should match
	assert.Equal(t, rollupRow.Status, rawRow.Status, "status should match")
	assert.Equal(t, rollupRow.ISISDown, rawRow.ISISDown, "isis_down should match")
}
