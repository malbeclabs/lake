package handlers_test

import (
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
	base := time.Now().Add(-time.Hour).UnixNano()
	db := "`" + api.FeedsDB + "`"
	return func(loc, source, symbol string, hash uint64, recvOffsetMs float64) {
		t.Helper()
		require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.hyperliquid_bbo_observations
			(capture_run_id, measurement_node_id, host, location_code, source, symbol, source_ts_ms, bbo_hash, recv_ts_ns, recv_ts_kind)
			VALUES ('run1', 'n1', 'n1', '%s', '%s', '%s', 1, %d, %d, 'kernel_udp_software')
		`, db, loc, source, symbol, hash, base+int64(recvOffsetMs*1e6))))
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

	req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetHyperliquidScoreboard(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// This route is public. One competitor's feed id in the payload deanonymises every
	// "Competitor N" label at once, and the browser gets the whole JSON whatever the UI
	// renders.
	body := rr.Body.String()
	for _, name := range []string{"hydromancer", "dwellir", "quicknode", "hyperpc"} {
		assert.NotContains(t, strings.ToLower(body), name, "competitor feed name leaked into the payload")
	}

	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.Feeds, 4)
	assert.Equal(t, "Public API", resp.Feeds[0].Label)
	assert.True(t, resp.Feeds[0].Venue)
	for i, f := range resp.Feeds[1:] {
		assert.Equal(t, fmt.Sprintf("Competitor %d", i+1), f.Label)
		assert.False(t, f.Venue)
	}
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
