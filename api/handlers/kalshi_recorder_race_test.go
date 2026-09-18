package handlers_test

import (
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The recorder race reads `kalshi_book_race`, a view this repository does not create — it is
// the recorder's own, three views deep over two tables in malbeclabs/kalshi. Recreating that
// CHAIN in a fixture would duplicate a schema this repo does not own and drift from it.
//
// Its OUTPUT is a different matter, and lake's query over that output is lake's to get wrong:
// two filters, two prefixes, a grouping and a window, each of which can return a plausible
// wrong number. So the fixture below is a table with the view's output columns — the same
// thing kalshi_l2_coverage_test.go does for its own out-of-band-proxied table — and the query
// runs against it.

// **An absent view is an empty payload and not an error.** Every other leg of this page falls
// back to a live query; this one has nothing to fall back to, so a hard error here would take
// the whole scoreboard down in any environment where the recorder does not write — which is
// every local one, and every test that does not create the view.
func TestFetchKalshiRecorderRaceWithoutTheViewIsEmptyAndNotAnError(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	got, err := api.FetchKalshiRecorderRace(t.Context())
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, got.Sites)
	// Stated even when empty: the consumer renders the window in its own caption, and a zero
	// there would read as "no window" rather than as "no sites".
	assert.Equal(t, 15, got.WindowMinutes)
	assert.False(t, got.GeneratedAt.IsZero())
}

// createKalshiBookRaceTable creates a table with the columns lake reads from `kalshi_book_race`.
//
// The view is wider (`env`, `recorder`, `feed`, `book_key`, `observed_by`, `last_observation`,
// the two receive stamps, `book_certain`, `exponents_agree`); those are unread here. `lead_ms`
// is Nullable in the view and is Nullable here, which is load-bearing — see the empty-side case
// below.
func createKalshiBookRaceTable(t *testing.T, api *handlers.API) {
	t.Helper()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.kalshi_book_race (
			site LowCardinality(String),
			symbol LowCardinality(String),
			bucket DateTime64(9),
			occurrence UInt64,
			observations UInt64,
			first_observation LowCardinality(String),
			lead_ms Nullable(Float64)
		) ENGINE = MergeTree
		ORDER BY (site, symbol, bucket, occurrence)
	`, db)))
}

// insertBookRace writes one row of the view's output.
func insertBookRace(t *testing.T, api *handlers.API, site, symbol, firstObservation string, agoSecs int, occurrence, observations uint64, leadMs float64) {
	t.Helper()
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO `+"`%s`"+`.kalshi_book_race
		(site, symbol, bucket, occurrence, observations, first_observation, lead_ms)
		VALUES ('%s', '%s', now64(9) - toIntervalSecond(%d), %d, %d, '%s', %f)
	`, api.FeedsDB, site, symbol, agoSecs, occurrence, observations, firstObservation, leadMs)))
}

// The query, run. Every row in this fixture is here because leaving it out of the result is a
// decision the query makes and could make wrongly.
//
// `cmh` carries the three that count and the three that must not: a lone sighting
// (`observations = 1`), a repeat of a book state (`occurrence = 2`), and a pair outside the
// fifteen-minute window. `dub` is a second site, so the grouping has something to get wrong.
func TestFetchKalshiRecorderRaceOverTheViewsOutput(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createKalshiBookRaceTable(t, api)

	// Two venue wins at the same lead, so the median is that lead and not an interpolation.
	insertBookRace(t, api, "cmh", "KXNFLGAME-A", "venue-perps-ws", 60, 1, 2, 9)
	insertBookRace(t, api, "cmh", "KXNFLGAME-B", "venue-perps-ws", 90, 1, 2, 9)
	// **Neither side's win.** A first_observation matching neither prefix stays in `pairs` and
	// is counted for nobody, so the shortfall is the signal that the naming convention moved.
	// On `LIKE 'venue%'` this row scored as a venue win and the shortfall never appeared.
	insertBookRace(t, api, "cmh", "KXNFLGAME-C", "venue_v2", 120, 1, 2, 9)
	// The three that are filtered out, each at the same site so their inclusion would show.
	insertBookRace(t, api, "cmh", "KXNFLGAME-D", "venue-perps-ws", 60, 1, 1, 0)
	insertBookRace(t, api, "cmh", "KXNFLGAME-E", "venue-perps-ws", 60, 2, 2, 500)
	insertBookRace(t, api, "cmh", "KXNFLGAME-F", "venue-perps-ws", 30*60, 1, 2, 500)
	// A site the wire wins outright, which leaves the venue side of the aggregate empty.
	insertBookRace(t, api, "dub", "KXNFLGAME-A", "edge-kalshi-perps", 60, 1, 2, 1.5)

	got, err := api.FetchKalshiRecorderRace(t.Context())
	require.NoError(t, err)
	require.True(t, got.Measured)
	require.Len(t, got.Sites, 2, "grouped per site, ordered by site")

	cmh := got.Sites[0]
	assert.Equal(t, "cmh", cmh.Site)
	assert.EqualValues(t, 3, cmh.Pairs, "the lone sighting, the repeat and the old pair are none of them races")
	assert.EqualValues(t, 3, cmh.Symbols)
	assert.EqualValues(t, 2, cmh.VenueWins, "venue_v2 is not a venue win")
	assert.EqualValues(t, 0, cmh.WireWins)
	assert.EqualValues(t, 1, cmh.Pairs-cmh.VenueWins-cmh.WireWins,
		"the shortfall the page renders: a pair attributed to neither side")
	assert.InDelta(t, 9, cmh.VenueP50Ms, 0.01, "500ms from the filtered rows would land here")
	assert.InDelta(t, 9, cmh.VenueP95Ms, 0.01)

	dub := got.Sites[1]
	assert.Equal(t, "dub", dub.Site)
	assert.EqualValues(t, 1, dub.Pairs)
	assert.EqualValues(t, 1, dub.WireWins)
	assert.EqualValues(t, 0, dub.VenueWins)
	assert.InDelta(t, 1.5, dub.WireP50Ms, 0.01)
	// **A side with no wins reads 0.00 ms and does not fail.** `lead_ms` is Nullable, so a
	// quantile over an empty condition is NULL rather than NaN; the `ifNotFinite` guard is what
	// keeps that out of the scan, where a NULL into a float64 would take the whole fetch — and
	// with it every other site's row — down.
	assert.Zero(t, dub.VenueP50Ms)
	assert.Zero(t, dub.VenueP95Ms)
}
