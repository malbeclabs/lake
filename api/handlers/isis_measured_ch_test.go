package handlers_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
)

// seedProbe writes one raw latency sample in one direction.
func seedProbe(t *testing.T, api *handlers.API, ts time.Time, idx int32, originPK, targetPK, linkPK string, rttUs, ipdvUs int64, loss bool) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `INSERT INTO fact_dz_device_link_latency
		(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, ipdv_us, loss)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		ts, ts, int64(1), idx, originPK, targetPK, linkPK, rttUs, ipdvUs, loss)
	require.NoError(t, err)
}

// seedProbeNoIPDV writes a probe that arrived but carries no IPDV. The first
// sample of a run has nothing to difference against, so this is ordinary.
func seedProbeNoIPDV(t *testing.T, api *handlers.API, ts time.Time, idx int32, originPK, targetPK, linkPK string, rttUs int64) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `INSERT INTO fact_dz_device_link_latency
		(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, ipdv_us, loss)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9)`,
		ts, ts, int64(1), idx, originPK, targetPK, linkPK, rttUs, false)
	require.NoError(t, err)
}

// A lost probe carries rtt_us = 0. Averaging those in deflates the measured
// figure exactly when a link is having a bad hour, and the internet column this
// page compares against has no matching discount. Production showed a link
// reporting 78.5ms against a real 88.5ms at 11% loss, so this is worth a test
// against real ClickHouse rather than a hand-built map.
func TestLinkMeasuredMapExcludesLostProbes(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-lax", "LAX")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-lax", 10, "activated")
	seedLinkMetadata(t, api, "link-1", "NYC-LAX-1", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 60_000_000, "activated")

	// Eight probes at 60ms and two lost. The true average is 60ms; counting the
	// lost pair as 0 would report 48ms.
	base := time.Now().UTC().Add(-time.Hour)
	for i := range 8 {
		seedProbe(t, api, base.Add(time.Duration(i)*time.Second), int32(i), "dev-a", "dev-z", "link-1", 60_000, 100, false)
	}
	seedProbe(t, api, base.Add(8*time.Second), 8, "dev-a", "dev-z", "link-1", 0, 0, true)
	seedProbe(t, api, base.Add(9*time.Second), 9, "dev-a", "dev-z", "link-1", 0, 0, true)

	measured, err := api.ExportLinkMeasuredMap(t.Context(), 24*time.Hour)
	require.NoError(t, err)

	got, ok := measured["dev-a:dev-z"]
	require.True(t, ok, "link missing from the map: %+v", measured)
	assert.InDelta(t, 60.0, got.AvgRttMs, 0.001, "lost probes must not be averaged in")
	assert.InDelta(t, 60.0, got.P95RttMs, 0.001)
	assert.InDelta(t, 0.1, got.AvgJitterMs, 0.001)
	assert.Equal(t, uint64(8), got.SampleCount, "sample count must be the surviving probes")

	// Both directions resolve, since a path walks device pairs in order.
	_, ok = measured["dev-z:dev-a"]
	assert.True(t, ok, "reverse direction missing")
}

// A link losing every probe reported 0ms, which made a dead hop look like a free
// one. Three links were in that state in production. It must drop out of the map
// so the path is reported as partial instead of fast.
func TestLinkMeasuredMapDropsFullyLostLink(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-lax", "LAX")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-lax", 10, "activated")
	seedLinkMetadata(t, api, "link-dead", "NYC-LAX-DEAD", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 60_000_000, "activated")

	base := time.Now().UTC().Add(-time.Hour)
	for i := range 10 {
		seedProbe(t, api, base.Add(time.Duration(i)*time.Second), int32(i), "dev-a", "dev-z", "link-dead", 0, 0, true)
	}

	measured, err := api.ExportLinkMeasuredMap(t.Context(), 24*time.Hour)
	require.NoError(t, err)

	assert.NotContains(t, measured, "dev-a:dev-z", "a link with no surviving probes must not report 0ms")
}

// ipdv_us is Nullable(Int64). avg over an all-null group returns NULL, which
// fails the scan into a float64 and takes the endpoint and the page-cache
// refresh with it. Excluding lost probes makes this easier to reach, not harder:
// a link down to its first surviving probe has nothing to difference against.
func TestLinkMeasuredMapSurvivesNullJitter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-lax", "LAX")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-lax", 10, "activated")
	seedLinkMetadata(t, api, "link-1", "NYC-LAX-1", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 60_000_000, "activated")

	base := time.Now().UTC().Add(-time.Hour)
	seedProbeNoIPDV(t, api, base, 0, "dev-a", "dev-z", "link-1", 60_000)
	seedProbe(t, api, base.Add(time.Second), 1, "dev-a", "dev-z", "link-1", 0, 0, true)

	measured, err := api.ExportLinkMeasuredMap(t.Context(), 24*time.Hour)
	require.NoError(t, err)

	got, ok := measured["dev-a:dev-z"]
	require.True(t, ok, "link missing from the map: %+v", measured)
	assert.InDelta(t, 60.0, got.AvgRttMs, 0.001)
	assert.Zero(t, got.AvgJitterMs, "no IPDV reads as zero, not as a failed scan")
}
