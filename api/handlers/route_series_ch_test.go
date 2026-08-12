package handlers_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apitesting "github.com/malbeclabs/lake/api/testing"
)

// The card and the sparkline under it describe the same route, so they must
// count the same probes. They did not: the card read the raw samples and the
// series read link_rollup_5m, which averages lost probes in as 0. On the preview
// that put a card reading 53.16ms above a line asserting 25.54ms and dipping to
// 0.13ms, which is worse than the original deflation because a reader sees both
// at once.
func TestRouteSeriesAgreesWithTheCard(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-lax", "lax")
	seedMetro(t, api, "metro-nyc", "nyc")
	seedDeviceMetadata(t, api, "dev-lax", "DEV-LAX", "router", "contrib-1", "metro-lax", 10, "activated")
	seedDeviceMetadata(t, api, "dev-nyc", "DEV-NYC", "router", "contrib-1", "metro-nyc", 10, "activated")
	seedLinkMetadata(t, api, "link-1", "LAX-NYC-1", "WAN", "contrib-1", "dev-lax", "dev-nyc", 10_000_000_000, 60_000_000, "activated")

	// Eight probes at 60ms and two lost, inside one completed hour. Counting the
	// lost pair as 0 would report 48ms.
	hour := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Hour)
	base := hour.Add(10 * time.Minute)
	for i := range 8 {
		seedProbe(t, api, base.Add(time.Duration(i)*time.Second), int32(i), "dev-lax", "dev-nyc", "link-1", 60_000, 100, false)
	}
	seedProbe(t, api, base.Add(8*time.Second), 8, "dev-lax", "dev-nyc", "link-1", 0, 0, true)
	seedProbe(t, api, base.Add(9*time.Second), 9, "dev-lax", "dev-nyc", "link-1", 0, 0, true)

	card, err := api.FetchMetroPathLatencyData(t.Context(), "latency", 0)
	require.NoError(t, err)

	var cardMs float64
	for _, p := range card.Paths {
		if p.FromMetroCode == "lax" && p.ToMetroCode == "nyc" {
			require.False(t, p.PartiallyCommitted, "the hop is measured, so the card must state a figure")
			cardMs = p.MeasuredLatencyMs
		}
	}
	require.NotZero(t, cardMs, "lax-nyc missing from the card: %+v", card.Paths)
	assert.InDelta(t, 60.0, cardMs, 0.001, "lost probes must not be averaged into the card")

	series, err := api.FetchRouteSeries(t.Context(), [][2]string{{"lax", "nyc"}})
	require.NoError(t, err)
	require.Len(t, series.Series, 1)

	var seriesMs float64
	for _, pt := range series.Series[0].Points {
		if pt.TS.Equal(hour) {
			seriesMs = pt.DZMs
		}
	}
	require.NotZero(t, seriesMs, "no series point for %s: %+v", hour, series.Series[0].Points)
	assert.InDelta(t, cardMs, seriesMs, 0.001, "the sparkline must agree with the card above it")
}
