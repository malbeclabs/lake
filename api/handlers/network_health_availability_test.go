package handlers_test

import (
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// findAvailability returns a pointer to the entry with the given code, or nil
// if not present.
func findAvailability(rows []handlers.NHAvailability, code string) *handlers.NHAvailability {
	for i := range rows {
		if rows[i].Code == code {
			return &rows[i]
		}
	}
	return nil
}

// insertAvailBuckets inserts n consecutive 5-minute link_rollup_5m rows for
// linkPK, all with the given status/isis_down/loss/provisioning combination,
// starting at base and stepping forward by 5 minutes per row.
func insertAvailBuckets(t *testing.T, api *handlers.API, base time.Time, linkPK string, n int, status string, isisDown, provisioning bool) time.Time {
	t.Helper()
	ctx := t.Context()
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx,
			`INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples, status, isis_down, provisioning)
			 VALUES ($1, $2, now(), 0, 0, 100, 100, $3, $4, $5)`,
			ts, linkPK, status, isisDown, provisioning))
	}
	return base.Add(time.Duration(n*5) * time.Minute)
}

// TestNetworkHealthAvailability_ThreeWaySplit verifies the time/bucket-based
// 3-way availability split (avail/drained/outage) computed by
// fetchLinkAvailability and fetchDeviceAvailability. Each 5-minute bucket is
// classified into exactly one of the three states (see the NHAvailability doc
// comment), and the percentages must sum to 100 of the classified buckets.
//
// Case "mostly_available": 4 available buckets, 3 drained, 1 outage (8 total)
// -> avail 50%, drained 37.5% (0.25h), outage 12.5% (0.08h, rounded).
// Case "mostly_drained": 1 available, 6 drained, 1 outage (8 total)
// -> avail 12.5%, drained 75% (0.5h), outage 12.5% (0.08h, rounded).
func TestNetworkHealthAvailability_ThreeWaySplit(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC()
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	cases := []struct {
		name                                              string
		linkPK, linkCode, devAPK, devZPK                  string
		avail, drained, outage                            int
		wantAvailPct, wantDrainedPct                      float64
		wantOutagePct                                     float64
		wantAvailHours, wantDrainedHours, wantOutageHours float64
	}{
		{
			name: "mostly_available", linkPK: "link-mostly-avail", linkCode: "NYC-LAX-AVAIL",
			devAPK: "dev-a-avail", devZPK: "dev-z-avail",
			avail: 4, drained: 3, outage: 1,
			wantAvailPct: 50.0, wantDrainedPct: 37.5, wantOutagePct: 12.5,
			wantAvailHours: 0.33, wantDrainedHours: 0.25, wantOutageHours: 0.08,
		},
		{
			name: "mostly_drained", linkPK: "link-mostly-drained", linkCode: "NYC-LAX-DRAINED",
			devAPK: "dev-a-drained", devZPK: "dev-z-drained",
			avail: 1, drained: 6, outage: 1,
			wantAvailPct: 12.5, wantDrainedPct: 75.0, wantOutagePct: 12.5,
			wantAvailHours: 0.08, wantDrainedHours: 0.5, wantOutageHours: 0.08,
		},
	}

	for _, c := range cases {
		require.NoError(t, api.DB.Exec(ctx,
			`INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES ($1, $2, 'router', 'metro-nyc', 'contrib-1', 'activated')`,
			c.devAPK, c.devAPK+"-CODE"))
		require.NoError(t, api.DB.Exec(ctx,
			`INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES ($1, $2, 'router', 'metro-lax', 'contrib-1', 'activated')`,
			c.devZPK, c.devZPK+"-CODE"))
		require.NoError(t, api.DB.Exec(ctx,
			`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ($1, $2, 'activated', 'WAN', $3, $4, 'contrib-1')`,
			c.linkPK, c.linkCode, c.devAPK, c.devZPK))

		base := now.Add(-90 * time.Minute)
		base = insertAvailBuckets(t, api, base, c.linkPK, c.avail, "activated", false, false)
		base = insertAvailBuckets(t, api, base, c.linkPK, c.drained, "soft-drained", false, false)
		insertAvailBuckets(t, api, base, c.linkPK, c.outage, "activated", true, false)
	}

	resp := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "")

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			link := findAvailability(resp.LinkAvailability, c.linkCode)
			require.NotNil(t, link, "link %s missing from LinkAvailability", c.linkCode)
			assert.InDelta(t, c.wantAvailPct, link.AvailPct, 0.001)
			assert.InDelta(t, c.wantDrainedPct, link.DrainedPct, 0.001)
			assert.InDelta(t, c.wantOutagePct, link.OutagePct, 0.001)
			assert.InDelta(t, c.wantAvailHours, link.AvailHours, 0.001)
			assert.InDelta(t, c.wantDrainedHours, link.DrainedHours, 0.001)
			assert.InDelta(t, c.wantOutageHours, link.OutageHours, 0.001)
			assert.Equal(t, "NYC ↔ LAX", link.Metros)
			assert.Empty(t, resp.Degraded, "a healthy window must not name a panel")
			assert.Empty(t, resp.Error)

			// The link's two endpoint devices are on no other link, so their
			// availability should match the link's exactly.
			devA := findAvailability(resp.DeviceAvailability, c.devAPK+"-CODE")
			require.NotNil(t, devA, "device %s missing from DeviceAvailability", c.devAPK)
			assert.InDelta(t, c.wantAvailPct, devA.AvailPct, 0.001)
			assert.InDelta(t, c.wantDrainedPct, devA.DrainedPct, 0.001)
			assert.InDelta(t, c.wantOutagePct, devA.OutagePct, 0.001)
			assert.InDelta(t, c.wantAvailHours, devA.AvailHours, 0.001)

			devZ := findAvailability(resp.DeviceAvailability, c.devZPK+"-CODE")
			require.NotNil(t, devZ, "device %s missing from DeviceAvailability", c.devZPK)
			assert.InDelta(t, c.wantAvailPct, devZ.AvailPct, 0.001)
		})
	}
}

// insertAvailBucketAt inserts one link_rollup_5m row with an explicit
// ingested_at, so a later row deterministically supersedes an earlier one under
// the table's ReplacingMergeTree(ingested_at) version.
func insertAvailBucketAt(t *testing.T, api *handlers.API, ts time.Time, linkPK string, isisDown bool, ingestedAt time.Time) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(),
		`INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples, status, isis_down, provisioning)
		 VALUES ($1, $2, $3, 0, 0, 100, 100, 'activated', $4, false)`,
		ts, linkPK, ingestedAt, isisDown))
}

// insertAvailLossBuckets inserts n consecutive 5-minute link_rollup_5m rows
// for linkPK with status='activated', isis_down=false, provisioning=false,
// but with high packet loss (a_loss_pct/z_loss_pct = lossPct). The shared
// nhAvailStateCountIfs classification counts these as outage via the
// "loss >= 10" branch rather than the isis_down branch, so this covers the
// other half of that OR condition.
func insertAvailLossBuckets(t *testing.T, api *handlers.API, base time.Time, linkPK string, n int, lossPct float64) time.Time {
	t.Helper()
	ctx := t.Context()
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx,
			`INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples, status, isis_down, provisioning)
			 VALUES ($1, $2, now(), $3, $3, 100, 100, 'activated', false, false)`,
			ts, linkPK, lossPct))
	}
	return base.Add(time.Duration(n*5) * time.Minute)
}

// TestNetworkHealthLinkAvailability_AllStates covers the full 3-way
// availability split for a single link through fetchLinkAvailability (the
// network-wide ranking query), exercising every branch of the shared
// nhAvailStateCountIfs classifier in one link: both outage sub-conditions
// (isis_down and loss>=10), both drained statuses, and provisioning exclusion.
// The other tests above cover isis_down-only outage and provisioning, but not
// the loss>=10 outage branch or hard-drained, so this retains that coverage
// after the per-link drill-down builder was removed.
//
// Bucket mix inserted for one link (26 total, 20 classified + 6 excluded):
//   - 8 available: status='activated', clean (isis_down=false, loss=0)
//   - 5 drained (soft-drained) + 2 drained (hard-drained) = 7 drained
//   - 3 outage via isis_down=true + 2 outage via loss>=10 = 5 outage
//   - 6 provisioning=true buckets (isis_down=true, which would otherwise
//     classify as outage) that must be excluded from the denominator entirely
//
// Expected, out of the 20 classified buckets: avail 40%, drained 35%, outage
// 25% (summing to 100). outage_hours = 5 buckets * 5/60 = 0.4166.. -> 0.42
// rounded; drained_hours = 7 buckets * 5/60 = 0.5833.. -> 0.58 rounded.
func TestNetworkHealthLinkAvailability_AllStates(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC()
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	const linkPK = "link-single"
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk, bandwidth_bps) VALUES ($1, 'NYC-LAX-SINGLE', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1', 10000000000)`,
		linkPK))

	base := now.Add(-100 * time.Minute)
	base = insertAvailBuckets(t, api, base, linkPK, 8, "activated", false, false)    // available
	base = insertAvailBuckets(t, api, base, linkPK, 5, "soft-drained", false, false) // drained
	base = insertAvailBuckets(t, api, base, linkPK, 2, "hard-drained", false, false) // drained
	base = insertAvailBuckets(t, api, base, linkPK, 3, "activated", true, false)     // outage (isis_down)
	base = insertAvailLossBuckets(t, api, base, linkPK, 2, 15.0)                     // outage (loss >= 10)
	insertAvailBuckets(t, api, base, linkPK, 6, "activated", true, true)             // provisioning, excluded

	resp := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "")

	link := findAvailability(resp.LinkAvailability, "NYC-LAX-SINGLE")
	require.NotNil(t, link, "link NYC-LAX-SINGLE missing from LinkAvailability")

	assert.InDelta(t, 40.0, link.AvailPct, 0.001)
	assert.InDelta(t, 35.0, link.DrainedPct, 0.001)
	assert.InDelta(t, 25.0, link.OutagePct, 0.001)
	assert.InDelta(t, 100.0, link.AvailPct+link.DrainedPct+link.OutagePct, 0.001, "the three shares must sum to 100")

	assert.InDelta(t, 0.42, link.OutageHours, 0.001, "5 outage buckets * 5/60")
	assert.InDelta(t, 0.58, link.DrainedHours, 0.001, "7 drained buckets * 5/60")
}

// TestNetworkHealthAvailability_ProvisioningExcluded confirms that
// provisioning buckets are excluded from the denominator entirely (not
// counted as outage, drained, or available), and that a link with only
// provisioning buckets is excluded from the ranking altogether rather than
// showing as 0%/100%.
func TestNetworkHealthAvailability_ProvisioningExcluded(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC()
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	// link-real: 2 real available buckets, plus 5 provisioning buckets that
	// would otherwise classify as outage (isis_down=true). The provisioning
	// buckets must not pull avail_pct down from 100%.
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-real', 'REAL-LINK', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))
	base := now.Add(-90 * time.Minute)
	base = insertAvailBuckets(t, api, base, "link-real", 2, "activated", false, false)
	insertAvailBuckets(t, api, base, "link-real", 5, "activated", true, true)

	// link-prov-only: every bucket is provisioning=true, so it must be
	// excluded from the ranking entirely (not shown as a gap/0%).
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES ('dev-prov-a', 'PROV-A-CODE', 'router', 'metro-nyc', 'contrib-1', 'activated'), ('dev-prov-z', 'PROV-Z-CODE', 'router', 'metro-lax', 'contrib-1', 'activated')`))
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-prov-only', 'PROV-ONLY-LINK', 'activated', 'WAN', 'dev-prov-a', 'dev-prov-z', 'contrib-1')`))
	insertAvailBuckets(t, api, base, "link-prov-only", 4, "activated", false, true)

	resp := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "")

	real := findAvailability(resp.LinkAvailability, "REAL-LINK")
	require.NotNil(t, real, "link with real (non-provisioning) buckets should appear")
	assert.InDelta(t, 100.0, real.AvailPct, 0.001, "provisioning buckets must not count as outage")
	assert.InDelta(t, 0.0, real.DrainedPct, 0.001)
	assert.InDelta(t, 0.0, real.OutagePct, 0.001)
	assert.InDelta(t, 0.17, real.AvailHours, 0.001, "2 available buckets * 5/60, provisioning buckets excluded")

	assert.Nil(t, findAvailability(resp.LinkAvailability, "PROV-ONLY-LINK"), "an all-provisioning link should be excluded, not shown as a gap")
	assert.Nil(t, findAvailability(resp.DeviceAvailability, "PROV-A-CODE"), "a device whose only link is all-provisioning should be excluded")
}

// TestNetworkHealthAvailability_DeviceReachability confirms the device
// REACHABILITY semantics of fetchDeviceAvailability: a device is available in a
// bucket whenever at least one of its links is working, even if another link is
// down. The hub device is side_a on a fully-working link and side_z on a
// fully-fault-down link over the same buckets; because one link works every
// bucket, the device reads 100% available (a single link down does not lower
// the device), which is the opposite of a link-hours average.
func TestNetworkHealthAvailability_DeviceReachability(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC()
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES
			('dev-hub', 'HUB-CODE', 'router', 'metro-nyc', 'contrib-1', 'activated'),
			('dev-x1', 'X1-CODE', 'router', 'metro-lax', 'contrib-1', 'activated'),
			('dev-x2', 'X2-CODE', 'router', 'metro-lax', 'contrib-1', 'activated')`))

	// Link 1: dev-hub is side_a, fully working (4 buckets) over the same times.
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-hub-1', 'HUB-LINK-1', 'activated', 'WAN', 'dev-hub', 'dev-x1', 'contrib-1')`))
	insertAvailBuckets(t, api, now.Add(-90*time.Minute), "link-hub-1", 4, "activated", false, false)

	// Link 2: dev-hub is side_z, fully fault-down (isis_down) over the same times.
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-hub-2', 'HUB-LINK-2', 'activated', 'WAN', 'dev-x2', 'dev-hub', 'contrib-1')`))
	insertAvailBuckets(t, api, now.Add(-90*time.Minute), "link-hub-2", 4, "activated", true, false)

	resp := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "")

	// Each link alone is 100% one state.
	link1 := findAvailability(resp.LinkAvailability, "HUB-LINK-1")
	require.NotNil(t, link1)
	assert.InDelta(t, 100.0, link1.AvailPct, 0.001)

	link2 := findAvailability(resp.LinkAvailability, "HUB-LINK-2")
	require.NotNil(t, link2)
	assert.InDelta(t, 100.0, link2.OutagePct, 0.001)

	// The device is reachable through link-hub-1 every bucket, so it is 100%
	// available despite link-hub-2 being fully down.
	hub := findAvailability(resp.DeviceAvailability, "HUB-CODE")
	require.NotNil(t, hub, "device dev-hub missing from DeviceAvailability")
	assert.InDelta(t, 100.0, hub.AvailPct, 0.001, "one working link keeps the device available")
	assert.InDelta(t, 0.0, hub.DrainedPct, 0.001)
	assert.InDelta(t, 0.0, hub.OutagePct, 0.001)
	assert.InDelta(t, 0.33, hub.AvailHours, 0.001, "4 available buckets * 5/60")

	// dev-x2 (side_a on the fully-down link only) has no working link, so it is
	// in outage the whole window.
	x2 := findAvailability(resp.DeviceAvailability, "X2-CODE")
	require.NotNil(t, x2, "device dev-x2 missing from DeviceAvailability")
	assert.InDelta(t, 100.0, x2.OutagePct, 0.001, "a device with only a fault-down link is in outage")
}

// insertIfaceBps inserts one device_interface_rollup_5m row attributing traffic
// (avg_out_bps) to linkPK at ts. Used to make an impactful-downtime link
// "attributable" with a known pre-outage bps.
func insertIfaceBps(t *testing.T, api *handlers.API, ts time.Time, linkPK, devicePK string, avgOutBps float64) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(),
		`INSERT INTO device_interface_rollup_5m (bucket_ts, device_pk, intf, link_pk, ingested_at, avg_in_bps, avg_out_bps)
		 VALUES ($1, $2, 'eth0', $3, now(), 0, $4)`,
		ts, devicePK, linkPK, avgOutBps))
}

// sumCountPoints totals the Count over an outages-over-time series.
func sumCountPoints(pts []handlers.NHCountPoint) uint64 {
	var n uint64
	for _, p := range pts {
		n += p.Count
	}
	return n
}

// hasCountAt reports whether the series carries a point whose T equals ts
// (RFC3339 UTC), i.e. an episode started in that interval.
func hasCountAt(pts []handlers.NHCountPoint, ts time.Time) bool {
	want := ts.UTC().Format(time.RFC3339)
	for _, p := range pts {
		if p.T == want {
			return true
		}
	}
	return false
}

// TestNetworkHealthOutages_EpisodeReconstruction drives the shared outage-episode
// "island" CTE through FetchNetworkHealthOutagesData over link_rollup_5m and
// pins its four rules using four separate links:
//   - blip: a single sub-10-minute down bucket (300s) is DROPPED from the
//     sustained OutageCount (>= 10 min floor) but still counted as a flap.
//   - split: two down buckets separated by one clean bucket do NOT merge into one
//     episode; they stay two 300s flaps (0 sustained), not one 600s episode.
//   - run2: two contiguous down buckets = one 600s episode, which DOES clear the
//     >= 2-bucket / 10-min floor (the inclusive boundary).
//   - run3: three contiguous down buckets = one 900s episode with the right start.
func TestNetworkHealthOutages_EpisodeReconstruction(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	tRun3 := now.Add(-90 * time.Minute)
	tRun2 := now.Add(-60 * time.Minute)
	tBlip := now.Add(-45 * time.Minute)
	tSplit := now.Add(-30 * time.Minute)

	// run3: 3 contiguous down buckets (900s, one episode).
	insertAvailBuckets(t, api, tRun3, "epi-run3", 3, "activated", true, false)
	// run2: 2 contiguous down buckets (600s, one episode, exactly at the floor).
	insertAvailBuckets(t, api, tRun2, "epi-run2", 2, "activated", true, false)
	// blip: 1 down bucket (300s, below the floor).
	insertAvailBuckets(t, api, tBlip, "epi-blip", 1, "activated", true, false)
	// split: down, gap of one clean bucket, down again -> two 300s islands.
	insertAvailBuckets(t, api, tSplit, "epi-split", 1, "activated", true, false)
	insertAvailBuckets(t, api, tSplit.Add(10*time.Minute), "epi-split", 1, "activated", true, false)

	resp := api.FetchNetworkHealthOutagesData(ctx, start, end, "")

	// Sustained OutageCount counts only the >= 2-bucket (>= 10 min) episodes:
	// run3 and run2. The blip and both split flaps are excluded.
	assert.Equal(t, uint64(2), resp.Reliability.OutageCount, "only run3 + run2 clear the 10-min floor")
	assert.Equal(t, uint64(2), resp.OutageCount, "headline OutageCount mirrors reliability")

	// The histogram counts ALL episodes: three 300s flaps (blip + two split
	// islands) and two 5-15m episodes (run2 600s, run3 900s). If the split had
	// merged, this would read one 600s episode (Short5to15m) and one flap instead.
	assert.Equal(t, uint64(3), resp.Reliability.DurationHistogram.FlapLE5m, "blip + two non-merged split flaps")
	assert.Equal(t, uint64(2), resp.Reliability.DurationHistogram.Short5to15m, "run2 (600s) + run3 (900s)")
	assert.Equal(t, uint64(0), resp.Reliability.DurationHistogram.Medium15to60m)

	// Capped downtime is the sum of the sustained episode seconds: 900 + 600.
	assert.InDelta(t, 0.4, resp.Reliability.CappedDowntimeHours, 0.001, "(900+600)/3600 rounded to 0.1")

	// outages_ts carries only the >= 2-bucket episodes (run3, run2), each starting
	// in its own 5-min interval, so the total is 2 and run3's start is present.
	assert.Equal(t, uint64(2), sumCountPoints(resp.OutagesOverTime), "only sustained episodes appear on the timeline")
	assert.True(t, hasCountAt(resp.OutagesOverTime, tRun3), "run3's episode starts at its first down bucket")
	assert.True(t, hasCountAt(resp.OutagesOverTime, tRun2), "run2's episode starts at its first down bucket")

	// The drain panel counts the same failures the headline tile does: one
	// definition, one outage_count.
	drain := api.FetchNetworkHealthDrainData(ctx, start, end, "")
	assert.Equal(t, int(resp.OutageCount), drain.DrainTiming.OutageCount,
		"the drain panel's outage_count must equal the Outages group's")

	// A healthy window names no panel and reports no error.
	assert.Empty(t, resp.Degraded)
	assert.Empty(t, resp.Error)
	assert.Empty(t, drain.Degraded)
	assert.Empty(t, drain.Error)
}

// TestNetworkHealthDrainEventsMatchOutageDefinition pins the shared outage
// definition across the two panels that publish outage_count. The drain panel
// used to reconstruct link failures from a different query: isis_down only, no
// duration floor, no provisioning filter and no FINAL. Loss-only failures were
// invisible to it while the headline tile counted them.
func TestNetworkHealthDrainEventsMatchOutageDefinition(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	base := now.Add(-90 * time.Minute)
	// Counts: 3 contiguous isis_down buckets.
	insertAvailBuckets(t, api, base, "dm-isis", 3, "activated", true, false)
	// Counts: 3 contiguous loss-only buckets (isis_down = 0, loss >= 10).
	insertAvailLossBuckets(t, api, base, "dm-loss", 3, 20.0)
	// Excluded: a single bucket is below the 10-minute sustained floor.
	insertAvailBuckets(t, api, base, "dm-blip", 1, "activated", true, false)
	// Excluded: provisioning buckets are not failures.
	insertAvailBuckets(t, api, base, "dm-prov", 3, "activated", true, true)

	outages := api.FetchNetworkHealthOutagesData(ctx, start, end, "")
	drain := api.FetchNetworkHealthDrainData(ctx, start, end, "")

	assert.Equal(t, uint64(2), outages.OutageCount, "isis-down run + loss-only run")
	assert.Equal(t, 2, drain.DrainTiming.OutageCount,
		"the drain panel must see the loss-only failure and drop the blip and the provisioning run")
	assert.Equal(t, int(outages.OutageCount), drain.DrainTiming.OutageCount)
}

// TestNetworkHealthOutageDefinitionUsesFinal guards the missing-FINAL class of
// bug for every caller of the shared episode CTE at once: link_rollup_5m is a
// replacing table, so a superseded row must not supply a stale isis_down.
func TestNetworkHealthOutageDefinitionUsesFinal(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)
	base := now.Add(-60 * time.Minute)

	// Write 3 down buckets, then supersede the same (bucket_ts, link_pk) keys with
	// clean rows carrying a later ingested_at (the table's replacing version).
	// FINAL must resolve to the later rows, leaving no episode.
	for i := range 3 {
		ts := base.Add(time.Duration(i*5) * time.Minute)
		insertAvailBucketAt(t, api, ts, "final-link", true, now.Add(-2*time.Hour))
		insertAvailBucketAt(t, api, ts, "final-link", false, now.Add(-1*time.Hour))
	}

	outages := api.FetchNetworkHealthOutagesData(ctx, start, end, "")
	assert.Equal(t, uint64(0), outages.OutageCount, "a superseded down bucket must not count")

	drain := api.FetchNetworkHealthDrainData(ctx, start, end, "")
	assert.Equal(t, 0, drain.DrainTiming.OutageCount, "the drain panel dedups the same way")
}

// TestNetworkHealthImpactful_Weighting drives FetchNetworkHealthImpactfulData and
// checks the impact weighting: only downtime on the primary path or a
// traffic-carrying link is summed. Five links, each with a >= 2-bucket outage, on
// one NYC-LAX metro pair:
//   - primary (lowest committed RTT, unattributable): COUNTS via the primary rule.
//   - backup (higher committed RTT, unattributable): EXCLUDED (not primary).
//   - hightraffic (attributable, >= 1Mbps pre-outage): COUNTS via the traffic rule.
//   - lowtraffic (attributable, < 1Mbps pre-outage): EXCLUDED (idle).
//   - sentinel (unattributable, committed_rtt_ns == 1e9 sentinel): EXCLUDED
//     (dropped from the metric graph, so never primary).
//
// Only primary (1200s) + hightraffic (600s) count -> 1800s = 0.5h.
//
// The episodes come from the shared outage definition (nhOutEpisodeCTE), so a
// sub-floor blip and a provisioning run on the counted primary link add nothing.
func TestNetworkHealthImpactful_Weighting(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	// The impactful query weights links by isis_delay_override_ns/committed_rtt_ns
	// like the topology graph; the bare rollup schema omits the override column.
	require.NoError(t, api.DB.Exec(ctx,
		`ALTER TABLE dz_links_current ADD COLUMN IF NOT EXISTS isis_delay_override_ns Int64 DEFAULT 0`))

	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	// All five links span the same NYC-LAX metro pair (dev-nyc-1 / dev-lax-1 from
	// insertBaseMetadata), so they compete for the single primary slot.
	links := []struct {
		pk, code    string
		committedNs int64
	}{
		{"imp-primary", "IMP-PRIMARY", 5_000_000},
		{"imp-backup", "IMP-BACKUP", 20_000_000},
		{"imp-hightraf", "IMP-HIGHTRAF", 8_000_000},
		{"imp-lowtraf", "IMP-LOWTRAF", 10_000_000},
		{"imp-sentinel", "IMP-SENTINEL", 1_000_000_000},
	}
	for _, l := range links {
		require.NoError(t, api.DB.Exec(ctx,
			`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk, committed_rtt_ns) VALUES ($1, $2, 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1', $3)`,
			l.pk, l.code, l.committedNs))
	}

	base := now.Add(-30 * time.Minute)
	insertAvailBuckets(t, api, base, "imp-primary", 4, "activated", true, false)  // 1200s, counts
	insertAvailBuckets(t, api, base, "imp-hightraf", 2, "activated", true, false) // 600s, counts
	insertAvailBuckets(t, api, base, "imp-backup", 2, "activated", true, false)   // excluded
	insertAvailBuckets(t, api, base, "imp-lowtraf", 2, "activated", true, false)  // excluded
	insertAvailBuckets(t, api, base, "imp-sentinel", 2, "activated", true, false) // excluded

	// Separate episodes on the primary link that the shared definition drops: one
	// bucket is below the 10-minute floor, and provisioning buckets are not failures.
	insertAvailBuckets(t, api, base.Add(-30*time.Minute), "imp-primary", 1, "activated", true, false)
	insertAvailBuckets(t, api, base.Add(-80*time.Minute), "imp-primary", 2, "activated", true, true)

	// Pre-outage traffic (60 min before the outage start). hightraffic carries
	// 5Mbps (>= 1Mbps -> impactful); lowtraffic carries 0.5Mbps (idle -> excluded).
	pre := base.Add(-15 * time.Minute)
	insertIfaceBps(t, api, pre, "imp-hightraf", "dev-nyc-1", 5_000_000)
	insertIfaceBps(t, api, pre, "imp-lowtraf", "dev-nyc-1", 500_000)

	resp := api.FetchNetworkHealthImpactfulData(ctx, start, end, "")

	assert.False(t, resp.Unavailable, "the current-window scan should succeed")
	assert.InDelta(t, 0.5, resp.ImpactfulDowntimeHours, 0.001,
		"only primary (1200s) + traffic-carrying hightraffic (600s) count: 1800s = 0.5h")
}

// TestNetworkHealthScopeIsolation confirms per-contributor scoping across the SQL
// groups. C1 owns one always-up link; C2 owns a fully-down link. A third
// contributor CEMPTY owns no links at all.
func TestNetworkHealthScopeIsolation(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_contributors_current (pk, code, name) VALUES ('cc1', 'C1', 'One'), ('cc2', 'C2', 'Two'), ('cce', 'CEMPTY', 'Empty')`))
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES
			('dev-cc1-a', 'DEV-CC1-A', 'router', 'metro-nyc', 'cc1', 'activated'),
			('dev-cc1-z', 'DEV-CC1-Z', 'router', 'metro-lax', 'cc1', 'activated'),
			('dev-cc2-a', 'DEV-CC2-A', 'router', 'metro-nyc', 'cc2', 'activated'),
			('dev-cc2-z', 'DEV-CC2-Z', 'router', 'metro-lax', 'cc2', 'activated')`))
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
			('link-cc1', 'LINK-CC1', 'activated', 'WAN', 'dev-cc1-a', 'dev-cc1-z', 'cc1'),
			('link-cc2-down', 'LINK-CC2-DOWN', 'activated', 'WAN', 'dev-cc2-a', 'dev-cc2-z', 'cc2')`))

	linkBase := now.Add(-60 * time.Minute)
	insertAvailBuckets(t, api, linkBase, "link-cc1", 6, "activated", false, false)     // C1: always up
	insertAvailBuckets(t, api, linkBase, "link-cc2-down", 6, "activated", true, false) // C2: fully down (outage)

	// A network-wide drain event so the drain group is non-empty when unscoped.
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_link_status_changes (link_pk, link_code, previous_status, new_status, changed_ts, side_a_metro, side_z_metro) VALUES ('link-cc2-down', 'LINK-CC2-DOWN', 'activated', 'soft-drained', $1, 'NYC', 'LAX')`,
		now.Add(-50*time.Minute)))

	// --- Availability group scoped to C1: only C1's link/devices, never C2's. ---
	avail := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "C1")
	require.NotNil(t, findAvailability(avail.LinkAvailability, "LINK-CC1"), "C1's link should appear")
	assert.Nil(t, findAvailability(avail.LinkAvailability, "LINK-CC2-DOWN"), "C2's down link must not leak into a C1-scoped view")
	require.NotNil(t, findAvailability(avail.DeviceAvailability, "DEV-CC1-A"), "C1's device should appear")
	assert.Nil(t, findAvailability(avail.DeviceAvailability, "DEV-CC2-A"), "C2's device must not leak into a C1-scoped view")

	// --- Outages group scoped to C1: C2's outage is excluded entirely. ---
	outC1 := api.FetchNetworkHealthOutagesData(ctx, start, end, "C1")
	assert.Equal(t, uint64(0), outC1.OutageCount, "C1 has no outage; C2's must not count")
	require.NotNil(t, outC1.OutageSummary)
	assert.Equal(t, uint64(0), outC1.OutageSummary.LinkOutages, "no C1 link outages")
	for _, r := range outC1.DowntimeLinks {
		assert.NotEqual(t, "LINK-CC2-DOWN", r.Code, "C2's down link must not appear in C1's downtime ranking")
	}
	// Unscoped, C2's outage IS counted, proving the scope filter is what removed it.
	outAll := api.FetchNetworkHealthOutagesData(ctx, start, end, "")
	assert.GreaterOrEqual(t, outAll.OutageCount, uint64(1), "C2's outage is visible network-wide")
	var sawC2 bool
	for _, r := range outAll.DowntimeLinks {
		if r.Code == "LINK-CC2-DOWN" {
			sawC2 = true
		}
	}
	assert.True(t, sawC2, "C2's down link appears in the network-wide downtime ranking")

	// --- Drain group scoped to a zero-link contributor returns empty (A3): a
	// scoped request that owns no links must NOT fall back to a network-wide scan. ---
	drainEmpty := api.FetchNetworkHealthDrainData(ctx, start, end, "CEMPTY")
	assert.Equal(t, 0, drainEmpty.DrainTiming.OutageCount, "zero-link scope must not see network outages")
	assert.Equal(t, 0, drainEmpty.DrainTiming.Drains, "zero-link scope must not see network drains")
	assert.Equal(t, 0, drainEmpty.DrainTiming.Undrains)
	assert.Equal(t, 0, drainEmpty.DrainTiming.MatchedUndrains)
	assert.Nil(t, drainEmpty.DrainTiming.TimeToDrainP50Min)
	assert.Nil(t, drainEmpty.DrainTiming.DrainWithin30mPct)
	require.NotNil(t, drainEmpty.Prev)
	// Owning no links is a real, empty answer, not a failed query: it must not be
	// reported as unavailable (which would freeze the cache and hide the panel).
	assert.Empty(t, drainEmpty.Error, "a zero-link scope is not a failure")
	assert.Empty(t, drainEmpty.Degraded)
	// Unscoped, the drain event is visible, proving the empty result is scoping.
	drainAll := api.FetchNetworkHealthDrainData(ctx, start, end, "")
	assert.GreaterOrEqual(t, drainAll.DrainTiming.Drains, 1, "the drain event is visible network-wide")
}

// TestNetworkHealthScopeCrossOwnedLink pins how the two kinds of scope differ.
// A link's contributor and its endpoint devices' contributors are independent
// (58 of 161 activated links on mainnet are cross-owned), so the link panels
// scope on link ownership and the device panels scope on DEVICE ownership.
//
// Here CX2 owns the link, but one endpoint device belongs to CX1. That device's
// fault time is CX1's, and it must appear in CX1's device panels even though CX1
// owns no link at all. This is the invariant that makes the device queries'
// outer-join scoping correct: scoping those CTEs by link ownership instead would
// silently drop the device from its owner's view.
func TestNetworkHealthScopeCrossOwnedLink(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC().Truncate(5 * time.Minute)
	start := now.Add(-2 * time.Hour)
	end := now.Add(2 * time.Hour)

	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_contributors_current (pk, code, name) VALUES ('cx1', 'CX1', 'Edge Owner'), ('cx2', 'CX2', 'Link Owner')`))
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES
			('dev-cx1-edge', 'DEV-CX1-EDGE', 'router', 'metro-nyc', 'cx1', 'activated'),
			('dev-cx2-core', 'DEV-CX2-CORE', 'router', 'metro-lax', 'cx2', 'activated')`))
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
			('link-cx2', 'LINK-CX2', 'activated', 'WAN', 'dev-cx2-core', 'dev-cx1-edge', 'cx2')`))

	// One sustained failure (4 buckets = 20 min, above the 10-minute floor).
	insertAvailBuckets(t, api, now.Add(-60*time.Minute), "link-cx2", 4, "activated", true, false)

	// CX1 owns no links: its LINK panels stay empty.
	availCX1 := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "CX1")
	assert.Nil(t, findAvailability(availCX1.LinkAvailability, "LINK-CX2"),
		"a link owned by CX2 must not appear in CX1's link availability")

	// ...but its DEVICE is unreachable for the whole window, and that is CX1's fact.
	dev := findAvailability(availCX1.DeviceAvailability, "DEV-CX1-EDGE")
	require.NotNil(t, dev, "CX1's device must appear in CX1's device availability even though CX2 owns the link")
	assert.Equal(t, float64(100), dev.OutagePct, "the device has a fault-down link and no working link")

	outCX1 := api.FetchNetworkHealthOutagesData(ctx, start, end, "CX1")
	require.NotNil(t, outCX1.OutageSummary)
	assert.Equal(t, uint64(0), outCX1.OutageSummary.LinkOutages, "CX1 owns no link, so it has no link outage")
	assert.Equal(t, uint64(1), outCX1.OutageSummary.DeviceOutages, "CX1's device outage is CX1's")
	assert.Equal(t, uint64(1), outCX1.OutageSummary.DevicesAffected)
	var sawDevice bool
	for _, r := range outCX1.DowntimeDevices {
		if r.Code == "DEV-CX1-EDGE" {
			sawDevice = true
		}
	}
	assert.True(t, sawDevice, "CX1's device must appear in CX1's device downtime ranking")
	for _, r := range outCX1.DowntimeLinks {
		assert.NotEqual(t, "LINK-CX2", r.Code, "CX2's link must not appear in CX1's link downtime ranking")
	}

	// The link owner sees the mirror image: the link, plus only its own device.
	availCX2 := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "CX2")
	require.NotNil(t, findAvailability(availCX2.LinkAvailability, "LINK-CX2"), "CX2 owns the link")
	require.NotNil(t, findAvailability(availCX2.DeviceAvailability, "DEV-CX2-CORE"), "CX2 owns this endpoint")
	assert.Nil(t, findAvailability(availCX2.DeviceAvailability, "DEV-CX1-EDGE"),
		"CX1's device must not leak into CX2's device availability")

	outCX2 := api.FetchNetworkHealthOutagesData(ctx, start, end, "CX2")
	require.NotNil(t, outCX2.OutageSummary)
	assert.Equal(t, uint64(1), outCX2.OutageSummary.LinkOutages, "CX2 owns the failed link")
	assert.Equal(t, uint64(1), outCX2.OutageSummary.DeviceOutages, "only CX2's own endpoint counts here")
}

// TestNetworkHealthDeltas pins the sign and magnitude of the current-vs-prior
// deltas so a current/prior swap fails. Current window has 3 sustained outages
// (one on the primary link) and the prior window has 1; the primary link's
// outage lasts longer in the current window than in the prior.
func TestNetworkHealthDeltas(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx,
		`ALTER TABLE dz_links_current ADD COLUMN IF NOT EXISTS isis_delay_override_ns Int64 DEFAULT 0`))

	now := time.Now().UTC().Truncate(5 * time.Minute)
	end := now
	start := now.Add(-60 * time.Minute) // 1h window
	// prior window is [now-120m, now-60m).

	// implink is the sole NYC-LAX link, so it is the impactful primary path.
	require.NoError(t, api.DB.Exec(ctx,
		`INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk, committed_rtt_ns) VALUES ('implink', 'IMPLINK', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1', 5000000)`))

	// Current window: 3 sustained outages (implink 4 buckets, two plain links 2).
	insertAvailBuckets(t, api, now.Add(-30*time.Minute), "implink", 4, "activated", true, false)
	insertAvailBuckets(t, api, now.Add(-40*time.Minute), "plain1", 2, "activated", true, false)
	insertAvailBuckets(t, api, now.Add(-40*time.Minute), "plain2", 2, "activated", true, false)
	// Prior window: 1 sustained outage (implink 2 buckets).
	insertAvailBuckets(t, api, now.Add(-90*time.Minute), "implink", 2, "activated", true, false)

	out := api.FetchNetworkHealthOutagesData(ctx, start, end, "")
	assert.Equal(t, uint64(3), out.OutageCount, "3 sustained outages in the current window")
	require.NotNil(t, out.Prev)
	assert.Equal(t, uint64(1), out.Prev.Reliability.OutageCount, "1 sustained outage in the prior window")
	require.NotNil(t, out.OutageCountDelta, "delta is defined when the prior count is non-zero")
	assert.InDelta(t, 200.0, *out.OutageCountDelta, 0.001, "(3-1)/1*100 = +200; a swap would be negative")

	imp := api.FetchNetworkHealthImpactfulData(ctx, start, end, "")
	assert.False(t, imp.Unavailable)
	assert.Empty(t, imp.Degraded, "a healthy window must not name a panel")
	assert.Empty(t, imp.Error)
	assert.Empty(t, out.Degraded)
	assert.Empty(t, out.Error)
	assert.InDelta(t, 0.3, imp.ImpactfulDowntimeHours, 0.001, "implink 1200s current -> 0.3h")
	require.NotNil(t, imp.Prev)
	assert.InDelta(t, 0.2, imp.Prev.ImpactfulDowntimeHours, 0.001, "implink 600s prior -> 0.2h")
	require.NotNil(t, imp.ImpactfulDowntimeDelta, "delta is defined when the prior value is non-zero")
	assert.InDelta(t, 50.0, *imp.ImpactfulDowntimeDelta, 0.001, "(0.3-0.2)/0.2*100 = +50; a swap would be negative")
}
