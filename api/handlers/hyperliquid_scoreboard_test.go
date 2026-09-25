package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createObservationsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.hyperliquid_bbo_observations (
			insert_ts DateTime64(9) DEFAULT now64(9),
			capture_run_id String,
			measurement_node_id String,
			host String,
			location_code LowCardinality(String),
			source LowCardinality(String),
			symbol LowCardinality(String),
			source_ts_ms UInt64,
			source_ts_ns UInt64,
			bbo_hash UInt64,
			recv_ts_ns UInt64,
			recv_ts_kind LowCardinality(String),
			bid_px_raw Int64,
			ask_px_raw Int64,
			price_exp Int8
		) ENGINE = MergeTree()
		ORDER BY (location_code, symbol, bbo_hash, source)
	`, db)))
}

// newObserver records "this feed saw this book state at this site, recvOffsetMs after the
// window base". The base is captured ONCE per test and written as a literal: calling
// now64(9) inside each INSERT stamps every row with its own wall clock, and the
// milliseconds between statements then dominate the offsets under test.
func newObserver(t *testing.T, api *handlers.API) func(loc, source, symbol string, hash uint64, recvOffsetMs float64) {
	t.Helper()
	at := newObserverAtTS(t, api)
	return func(loc, source, symbol string, hash uint64, recvOffsetMs float64) {
		t.Helper()
		at(loc, source, symbol, 1, hash, recvOffsetMs)
	}
}

// newObserver with the emission timestamp under the caller's control: source_ts_ms is part of
// the race key, so a test needs to say which emission each row belongs to.
func newObserverAtTS(t *testing.T, api *handlers.API) func(loc, source, symbol string, tsMs, hash uint64, recvOffsetMs float64) {
	t.Helper()
	base := time.Now().Add(-time.Hour).UnixNano()
	db := "`" + api.FeedsDB + "`"
	return func(loc, source, symbol string, tsMs, hash uint64, recvOffsetMs float64) {
		t.Helper()
		require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.hyperliquid_bbo_observations
			(capture_run_id, measurement_node_id, host, location_code, source, symbol, source_ts_ms, bbo_hash, recv_ts_ns, recv_ts_kind)
			VALUES ('run1', 'n1', 'n1', '%s', '%s', '%s', %d, %d, %d, 'kernel_udp_software')
		`, db, loc, source, symbol, tsMs, hash, base+int64(recvOffsetMs*1e6))))
	}
}

func TestGetHyperliquidScoreboard_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetHyperliquidScoreboard(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Zero(t, resp.Races)
	assert.Empty(t, resp.Feeds)
}

func TestHyperliquidScoreboard_UsesEarliestDoubleZeroPublisher(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 30)
	obs("tyo", "tob_aws_tyo_hl_mainnet", "BTC", 1, 90)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 100)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Races)
	assert.InDelta(t, 100.0, resp.All.WinPct, 0.01)
	assert.InDelta(t, 70.0, resp.All.P50Ms, 0.01, "the earliest publisher sets the arrival, not the slowest")
}

func TestHyperliquidScoreboard_LossesCountIntoThePercentiles(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	for _, hash := range []uint64{1, 2, 3} {
		obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", hash, 60)
		obs("tyo", "hydromancer_bbo", "BTC", hash, 10)
	}

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, resp.Races)
	assert.InDelta(t, 0.0, resp.All.WinPct, 0.01)
	assert.InDelta(t, -50.0, resp.All.P50Ms, 0.01, "a lost race must carry a negative margin")
}

func TestHyperliquidScoreboard_RetiredPublisherExcluded(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	// The mirror is the fastest publisher here, so excluding it has to move the arrival to
	// the next publisher and turn a +80ms win into a -10ms loss. Dropping the mirror's row
	// alone would keep the win.
	obs("tyo", "tob_aws_tyo_mirror1", "BTC", 1, 10)
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 100)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 90)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Races)
	assert.InDelta(t, 0.0, resp.All.WinPct, 0.01, "the surviving publisher lost this race")
	assert.InDelta(t, -10.0, resp.All.P50Ms, 0.01)
}

func TestHyperliquidScoreboard_StateWithOnlyRetiredPublisherIsDropped(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	obs("tyo", "tob_aws_tyo_mirror1", "BTC", 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 90)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	assert.Zero(t, resp.Races, "no surviving publisher means no arrival to report")
}

func TestHyperliquidScoreboard_PayloadCarriesNoFeedNames(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	for _, c := range []string{"hydromancer_bbo", "dwellir_l2book_bbo", "quicknode_l2book_bbo", "hyperliquid_public_bbo"} {
		obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
		obs("tyo", c, "BTC", 1, 50)
	}

	// Asserted on what the fetch serialises to, not on a handler response: the handler now
	// writes the worker's cached bytes back verbatim, so these are the bytes that reach the
	// browser.
	fetched, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	raw, err := json.Marshal(fetched)
	require.NoError(t, err)

	// This route is public. One competitor's feed id in the payload deanonymises every
	// "Competitor N" label at once, and the browser gets the whole JSON whatever the UI
	// renders.
	// Derived, not hardcoded: a fifth competitor added to hyperliquidCompetitors or
	// hyperliquidExcludedFeeds has to be covered by this test the moment it exists, or the
	// public route ships a feed id with the test still green.
	feedIDs := handlers.HyperliquidRacedFeedIDs()
	require.NotEmpty(t, feedIDs)
	body := strings.ToLower(string(raw))
	for _, id := range feedIDs {
		assert.NotContains(t, body, strings.ToLower(id), "competitor feed id leaked into the payload")
		// Also the bare vendor token, since a label could carry it without the full id.
		if base, _, ok := strings.Cut(id, "_"); ok && len(base) > 3 {
			assert.NotContains(t, body, strings.ToLower(base), "competitor name leaked into the payload")
		}
	}

	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.Len(t, resp.Feeds, 4)
	assert.Equal(t, "Public API", resp.Feeds[0].Label)
	assert.True(t, resp.Feeds[0].Venue)
	for i, f := range resp.Feeds[1:] {
		assert.Equal(t, fmt.Sprintf("Competitor %d", i+1), f.Label)
		assert.False(t, f.Venue)
	}
}

// Asserted on the serialised bytes, which is what the cache stores and the browser gets.
// Without next_refresh_at the page never shows the stale warning; without markets, a page
// loaded before By Market was removed crashes on markets.map.
func TestHyperliquidScoreboard_PayloadCarriesScheduleAndEmptyMarkets(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 50)

	fetched, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	raw, err := json.Marshal(fetched)
	require.NoError(t, err)

	var body struct {
		AsOf          time.Time         `json:"as_of"`
		NextRefreshAt *time.Time        `json:"next_refresh_at"`
		Markets       []json.RawMessage `json:"markets"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))

	require.NotNil(t, body.NextRefreshAt)
	assert.True(t, body.NextRefreshAt.After(body.AsOf))
	assert.Equal(t, handlers.HyperliquidScoreboardRefreshHourUTC,
		time.Duration(body.NextRefreshAt.Hour())*time.Hour)
	assert.Contains(t, string(raw), `"markets":[]`, "must be an empty array, not null or absent")
	assert.Empty(t, body.Markets)
}

func TestHyperliquidScoreboard_CompetitorsNumberedByMedianAscending(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
	obs("tyo", "quicknode_l2book_bbo", "BTC", 1, 30)
	obs("tyo", "dwellir_l2book_bbo", "BTC", 1, 50)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 70)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)

	byLabel := map[string]float64{}
	for _, f := range resp.Feeds {
		byLabel[f.Label] = f.P50Ms
	}
	assert.InDelta(t, 20.0, byLabel["Competitor 1"], 0.01)
	assert.InDelta(t, 40.0, byLabel["Competitor 2"], 0.01)
	assert.InDelta(t, 60.0, byLabel["Competitor 3"], 0.01)
}

func TestHyperliquidScoreboard_RecurringStateIsOneRacePerEmission(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserverAtTS(t, api)

	// The same book state published twice. Keyed on the state alone these collapse into one
	// race; keyed on the emission they are two.
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1000, 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 1000, 1, 50)
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 2000, 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 2000, 1, 50)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, resp.Races)
	assert.InDelta(t, 100.0, resp.All.WinPct, 0.01)
	assert.InDelta(t, 40.0, resp.All.P50Ms, 0.01)
}

func TestHyperliquidScoreboard_AMissedEmissionDoesNotBorrowALaterArrival(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserverAtTS(t, api)

	// DoubleZero misses the first emission and catches the second. Keyed on the state alone its
	// second arrival is compared against the competitor's FIRST, turning a 40ms win into a
	// 990ms loss.
	obs("tyo", "hydromancer_bbo", "BTC", 1000, 1, 10)
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 2000, 1, 1000)
	obs("tyo", "hydromancer_bbo", "BTC", 2000, 1, 1040)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Races, "the emission DoubleZero missed has no arrival to score")
	assert.InDelta(t, 100.0, resp.All.WinPct, 0.01)
	assert.InDelta(t, 40.0, resp.All.P50Ms, 0.01,
		"the margin is the delivery difference on the emission both feeds saw")
}

func TestHyperliquidScoreboard_SitesComeFromTheData(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	// "fra" is unnamed only in that it is absent from hyperliquidNamedSites; naming it means
	// moving this to another code. Written out of display order on purpose.
	for _, loc := range []string{"was", "fra", "tyo"} {
		obs(loc, "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
		obs(loc, "hydromancer_bbo", "BTC", 1, 50)
	}

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)

	require.Len(t, resp.Sites, 3)
	assert.Equal(t, 3, resp.SiteCount)
	assert.Equal(t, "TYO", resp.Sites[0].Code)
	assert.Equal(t, "Tokyo", resp.Sites[0].Label, "named sites come first, in their declared order")
	assert.Equal(t, "WAS", resp.Sites[1].Code)
	assert.Equal(t, "Washington DC", resp.Sites[1].Label)
	assert.Equal(t, "FRA", resp.Sites[2].Code)
	assert.Equal(t, "FRA", resp.Sites[2].Label, "an unnamed site renders under its uppercased code")

	require.NotEmpty(t, resp.Feeds)
	for _, f := range resp.Feeds {
		require.Len(t, f.Sites, 3)
		assert.Equal(t, "TYO", f.Sites[0].Code)
		assert.Equal(t, "WAS", f.Sites[1].Code)
		assert.Equal(t, "FRA", f.Sites[2].Code)
	}
}

// "Instruments carried" samples the live fleet over hyperliquidCarriedWindowMinutes rather than
// the 24h window the board races, so an instrument quiet for longer than the sample is not
// counted. That is the documented trade against a 4.25B-row scan for the same answer, and this
// pins it so the narrowness stays a decision on the record rather than a surprise.
func TestHyperliquidScoreboard_CarriedInstrumentsAreCountedFromTheLiveFleet(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)

	db := "`" + api.FeedsDB + "`"
	write := func(symbol string, recvAgo time.Duration) {
		t.Helper()
		require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.hyperliquid_bbo_observations
			(capture_run_id, measurement_node_id, host, location_code, source, symbol, source_ts_ms, bbo_hash, recv_ts_ns, recv_ts_kind)
			VALUES ('run1', 'n1', 'n1', 'tyo', 'tob_gcp_tyo_hl_mainnet1', '%s', 1, 1, %d, 'kernel_udp_software')
		`, db, symbol, time.Now().Add(-recvAgo).UnixNano())))
	}

	write("BTC", 10*time.Second)       // live
	write("xyz:SP500", 10*time.Second) // live
	write("xyz:NVDA", 30*time.Minute)  // quiet for longer than the sample

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)

	assert.Equal(t, 2, resp.Instruments,
		"an instrument quiet for longer than the sample is not counted — the known cost of the narrow window")
}

// A probe that could not run is not a table that is not there, and the difference is a day of
// wrong numbers. The worker writes this key once a day, so an empty payload written on a blip
// advances updated_at and stands until tomorrow's run — with nothing logged, because returning
// it as a success is what tells the escalator there was nothing to report. A refresh that
// cannot establish what it is querying has to fail, so nothing is written and it stays due.
func TestHyperliquidScoreboard_AFailedProbeIsAnErrorNotAnEmptyBoard(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := api.FetchHyperliquidScoreboardData(ctx)
	require.Error(t, err, "an unreadable probe must not be reported as an absent table")
}

// A CUBE cell with no DoubleZero arrival has nothing to take a quantile over, and an unguarded
// quantileTDigestIf answers NaN there. json.Marshal refuses NaN, so one such cell fails the
// WritePageCache of this key — and because the handler never computes in the request path, the
// page then serves 503 until something else fixes it. The failure is in the marshal, not in the
// fetch, so that is what this asserts.
func TestHyperliquidScoreboard_SiteWithNoDoubleZeroArrivalStaysSerialisable(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	// Tokyo races normally.
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
	obs("tyo", "hyperliquid_public_bbo", "BTC", 1, 40)
	// Frankfurt has a competitor and no DoubleZero publisher at all.
	obs("fra", "hyperliquid_public_bbo", "BTC", 2, 40)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)

	b, err := json.Marshal(resp)
	require.NoError(t, err, "a site with no DoubleZero arrival must not put NaN in the payload")
	require.NotContains(t, string(b), "NaN")

	var fra *handlers.HyperliquidScoreboardSite
	for i, s := range resp.Sites {
		if s.Code == "FRA" {
			fra = &resp.Sites[i]
		}
	}
	require.NotNil(t, fra, "the site is still reported, at zero rather than not at all")
	assert.Zero(t, fra.P50Ms)
	assert.Zero(t, fra.P95Ms)
	assert.Zero(t, fra.P99Ms)
}

// A miss must never compute. One compute is a multi-GB scan of hyperliquid_bbo_observations,
// the route is reachable by anyone, there is no singleflight and the miss path never wrote
// back — so gating the cache read on isMainnet left `X-DZ-Env: testnet` as a way for any
// caller to run it, once per request. Exercised through EnvMiddleware, which is what reads
// the header.
func TestGetHyperliquidScoreboard_NeverComputesInTheRequestPath(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 50)

	h := handlers.EnvMiddleware(http.HandlerFunc(api.GetHyperliquidScoreboard))
	for _, env := range []string{"", "mainnet-beta", "testnet", "nonsense"} {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
		if env != "" {
			req.Header.Set("X-DZ-Env", env)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)

		require.Equal(t, http.StatusServiceUnavailable, rr.Code, "env %q reached the live compute", env)
		assert.Equal(t, "30", rr.Header().Get("Retry-After"), "env %q", env)
		assert.Equal(t, "MISS", rr.Header().Get("X-Cache"), "env %q", env)
		assert.NotContains(t, rr.Body.String(), "win_pct", "env %q served a computed board", env)
	}
}

// races is distinct emissions; comparisons is the (emission, feed) count the rates are computed
// over. The page labels the headline "Updates raced", so reporting comparisons there overstated
// it by however many feeds happened to cover each update — measured at 3.9x on production.
func TestHyperliquidScoreboard_RacesCountsEmissionsNotComparisons(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	// One book state, seen by DoubleZero and three competitors.
	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 50)
	obs("tyo", "dwellir_l2book_bbo", "BTC", 1, 60)
	obs("tyo", "quicknode_l2book_bbo", "BTC", 1, 70)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 1, resp.Races, "one emission was raced, however many feeds saw it")
	assert.EqualValues(t, 3, resp.Comparisons, "three feed comparisons stand behind the rates")
}

// A feed with nothing in the window is not a competitor at 0.0%. Rendering it anyway put it at
// the TOP of the board: ranking is by median margin ascending, and a zero sorts ahead of every
// real competitor DoubleZero beats.
func TestHyperliquidScoreboard_FeedWithNoDataIsNotRankedFirst(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10)
	obs("tyo", "hydromancer_bbo", "BTC", 1, 50)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)

	require.Len(t, resp.Feeds, 1, "only the feed with observations is measured")
	assert.Equal(t, "Competitor 1", resp.Feeds[0].Label)
	assert.InDelta(t, 40.0, resp.Feeds[0].P50Ms, 0.01, "the ranked feed is the real one, not a zero")
	assert.Equal(t, 1, resp.FeedCount, "feed count is what was measured, not the configured list")
}

// The win rate is conditional on DoubleZero having delivered, so an emission it missed is in no
// rate on the page. That is defensible, but it has to be stated: without the count, a publisher
// dropping updates leaves every number on the board unchanged.
func TestHyperliquidScoreboard_EmissionsDoubleZeroMissedAreReported(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createObservationsTable(t, api)
	obs := newObserver(t, api)

	obs("tyo", "tob_gcp_tyo_hl_mainnet1", "BTC", 1, 10) // delivered, won by 40ms
	obs("tyo", "hydromancer_bbo", "BTC", 1, 50)
	obs("tyo", "hydromancer_bbo", "BTC", 2, 50) // competitor only: DoubleZero never delivered

	resp, err := api.FetchHyperliquidScoreboardData(t.Context())
	require.NoError(t, err)

	assert.EqualValues(t, 1, resp.Races, "the missed emission is not a race")
	assert.EqualValues(t, 1, resp.DZAbsent, "but it is reported")
	assert.InDelta(t, 100.0, resp.All.WinPct, 0.01, "and it does not move the rate")
}
