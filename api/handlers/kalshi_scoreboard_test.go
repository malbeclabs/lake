package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	kalshiDZFeed     = "tob_lashay_1"
	kalshiPublicFeed = "kalshi_perps_public"
)

// createKalshiFeedsTable creates kalshi_bbo_feed_race_summary in the feeds DB, matching the
// capture's 20260714000001 migration: per-event lead_time_ms (not p50/p95 columns), no
// feed_type, no total_events/events_won, and capture_run_id in the sorting key.
func createKalshiFeedsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.kalshi_bbo_feed_race_summary (
			event_ts DateTime64(9, 'UTC'),
			ingested_at DateTime64(9, 'UTC') DEFAULT now64(9),
			measurement_node_id String,
			capture_run_id String,
			host String,
			location_code LowCardinality(String),
			symbol LowCardinality(String),
			source_ts_ms UInt64,
			bbo_hash UInt64,
			feed LowCardinality(String),
			loser_feed LowCardinality(String),
			lead_time_ms Float64,
			send_lead_time_ms Nullable(Float64)
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toDate(event_ts)
		ORDER BY (measurement_node_id, capture_run_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed)
	`, db)))
}

// insertKalshiRace inserts one race row: winner beat loser by leadMs.
func insertKalshiRace(t *testing.T, api *handlers.API, node, loc, symbol string, srcTs, hash uint64, winner, loser string, leadMs float64) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.kalshi_bbo_feed_race_summary
		(event_ts, measurement_node_id, capture_run_id, host, location_code, symbol, source_ts_ms, bbo_hash, feed, loser_feed, lead_time_ms)
		VALUES (now64(9), '%s', 'run1', '%s', '%s', '%s', %d, %d, '%s', '%s', %f)
	`, db, node, node, loc, symbol, srcTs, hash, winner, loser, leadMs)))
}

// seedKalshiEntry inserts a configured competing feed into the Postgres allow-list.
func seedKalshiEntry(t *testing.T, api *handlers.API, feed, label string, order int) {
	t.Helper()
	_, err := api.PgPool.Exec(t.Context(), `
		INSERT INTO kalshi_scoreboard_entry (feed, label, display_order, enabled)
		VALUES ($1, $2, $3, TRUE)
		ON CONFLICT (feed) DO UPDATE SET label = EXCLUDED.label,
			display_order = EXCLUDED.display_order, enabled = TRUE, updated_at = NOW()`,
		feed, label, order)
	require.NoError(t, err)
}

// newKalshiTestAPI builds a per-test API. ClickHouse is isolated per test, but the Postgres
// container is shared across the package — so the allow-list is truncated here, or rows seeded
// by an earlier test would leak into the ones that assert on an unconfigured scoreboard.
func newKalshiTestAPI(t *testing.T) *handlers.API {
	t.Helper()
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	_, err := api.PgPool.Exec(t.Context(), "TRUNCATE kalshi_scoreboard_entry")
	require.NoError(t, err)
	return api
}

func TestGetKalshiScoreboard_Empty(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/kalshi/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetKalshiScoreboard(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.KalshiScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "1h", resp.Window)
	assert.Equal(t, "MISS", rr.Header().Get("X-Cache"))
	assert.Empty(t, resp.Nodes)
}

func TestGetKalshiScoreboard_MissingTable(t *testing.T) {
	api := newKalshiTestAPI(t)
	// Do NOT create the table -> handler must degrade to empty 200, not 500.

	req := httptest.NewRequest(http.MethodGet, "/api/dz/kalshi/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetKalshiScoreboard(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.KalshiScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Competitors)
}

func TestFetchKalshiScoreboardData_MissingTable(t *testing.T) {
	api := newKalshiTestAPI(t)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "24h", resp.Window)
	assert.Empty(t, resp.Competitors)
	assert.Empty(t, resp.Nodes)
	assert.Empty(t, resp.RecentRaces)
}

// With no rows in the allow-list, races exist in ClickHouse but nothing is configured to be
// shown. That is the deliberate pre-seeding state, and it must render as empty rather than
// leaking unconfigured feed ids or erroring.
func TestKalshiScoreboard_NoConfiguredFeeds(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiDZFeed, kalshiPublicFeed, 2.0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.Competitors)
	assert.Empty(t, resp.Nodes)
	assert.EqualValues(t, 0, resp.TotalRaces)
}

func TestKalshiScoreboard_HeadlineAndCompetitors(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// 4 races at cmh: DZ beats the public feed three times, loses once.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 1000, 1, kalshiDZFeed, kalshiPublicFeed, 1.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 2000, 2, kalshiDZFeed, kalshiPublicFeed, 2.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 3000, 3, kalshiDZFeed, kalshiPublicFeed, 3.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 4000, 4, kalshiPublicFeed, kalshiDZFeed, 0.5)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	// DZ won 3 of 4 races = 75%.
	assert.InDelta(t, 75.0, resp.DZWinSharePct, 0.1)
	assert.EqualValues(t, 4, resp.TotalRaces)

	require.Len(t, resp.Competitors, 1)
	c := resp.Competitors[0]
	assert.Equal(t, kalshiPublicFeed, c.Feed)
	assert.Equal(t, "Public API", c.Label)
	assert.InDelta(t, 75.0, c.DZWinPct, 0.1)
	assert.EqualValues(t, 4, c.Races)
	// Lead p50 over the 3 DZ wins (1.0, 2.0, 3.0) = 2.0. The percentiles are computed from
	// per-event lead_time_ms rows here, not read from a pre-aggregated column.
	assert.InDelta(t, 2.0, c.LeadP50Ms, 0.001)
	assert.InDelta(t, 3.0, c.LeadP95Ms, 0.001)
}

func TestKalshiScoreboard_PerNode(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// cmh: DZ wins both. tyo: DZ wins 1, loses 1.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXETHPERP", 10, 1, kalshiDZFeed, kalshiPublicFeed, 2.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXETHPERP", 20, 2, kalshiDZFeed, kalshiPublicFeed, 2.0)
	insertKalshiRace(t, api, "tyo-rec1", "tyo", "KXETHPERP", 30, 3, kalshiDZFeed, kalshiPublicFeed, 1.0)
	insertKalshiRace(t, api, "tyo-rec1", "tyo", "KXETHPERP", 40, 4, kalshiPublicFeed, kalshiDZFeed, 1.0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 2)

	byNode := map[string]handlers.KalshiNode{}
	for _, n := range resp.Nodes {
		byNode[n.MeasurementNodeID] = n
	}
	assert.InDelta(t, 100.0, byNode["cmh-rec1"].DZWinSharePct, 0.1)
	assert.InDelta(t, 50.0, byNode["tyo-rec1"].DZWinSharePct, 0.1)
}

// The overlapping refresh windows of the remote materialized view write the same logical race
// repeatedly. Queries drop FINAL for cost, so the dedup has to come from counting distinct
// sorting-key tuples — and capture_run_id is part of that key on this table.
func TestKalshiScoreboard_DedupsRepeatedRows(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// The same race appended three times, as an overlapping MV refresh would.
	for i := 0; i < 3; i++ {
		insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 1000, 1, kalshiDZFeed, kalshiPublicFeed, 1.0)
	}
	// Plus one genuinely different race.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 2000, 2, kalshiDZFeed, kalshiPublicFeed, 1.0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	assert.EqualValues(t, 2, resp.TotalRaces, "repeated appends of one race must count once")
}

func TestKalshiScoreboard_RecentRaces(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// A DZ-won race and a competitor-won race, on two of the grid's symbols.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 100, 1, kalshiDZFeed, kalshiPublicFeed, 1.5)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXETHPERP", 200, 2, kalshiPublicFeed, kalshiDZFeed, 0.7)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.RecentRaces), 2)

	bySym := map[string]handlers.KalshiRace{}
	for _, r := range resp.RecentRaces {
		bySym[r.Symbol] = r
	}
	assert.True(t, bySym["KXBTCPERP"].IsDZ)
	assert.Equal(t, "DoubleZero", bySym["KXBTCPERP"].WinnerLabel)
	assert.Equal(t, "Public API", bySym["KXBTCPERP"].RunnerUpLabel)
	assert.InDelta(t, 1.5, bySym["KXBTCPERP"].LeadMs, 0.001)

	assert.False(t, bySym["KXETHPERP"].IsDZ)
	assert.Equal(t, "Public API", bySym["KXETHPERP"].WinnerLabel)
	assert.Equal(t, "DoubleZero", bySym["KXETHPERP"].RunnerUpLabel)
}

// A cell where DoubleZero won zero races makes the lead-time quantile aggregate over an empty
// predicate set, which ClickHouse returns as NaN. If that NaN reaches the float64 fields, JSON
// encoding of the whole response fails — breaking the page and poisoning the page cache.
func TestKalshiScoreboard_ZeroDZWins_Encodable(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXETHPERP", 10, 1, kalshiPublicFeed, kalshiDZFeed, 3.0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	// The entire response must be JSON-encodable — a single NaN anywhere fails encoding.
	_, err = json.Marshal(resp)
	require.NoError(t, err, "response must not contain NaN percentiles")

	require.Len(t, resp.Competitors, 1)
	assert.InDelta(t, 0.0, resp.Competitors[0].DZWinPct, 0.001)
	assert.InDelta(t, 0.0, resp.Competitors[0].LeadP50Ms, 0.001)
	assert.InDelta(t, 0.0, resp.Competitors[0].LeadP95Ms, 0.001)
}

// DZ-vs-DZ pairings (the WebSocket and FIX arms of the same publisher racing each other) are a
// transport comparison, not a race against the venue. They must not enter the win rate.
func TestKalshiScoreboard_ExcludesDZvsDZ(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiDZFeed, kalshiPublicFeed, 1.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 20, 2, "tob_lashay_1_ws", "tob_lashay_1_fix", 0.2)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	assert.EqualValues(t, 1, resp.TotalRaces, "a DZ-vs-DZ pairing must not be counted as a race")
}

// A tob_ row in the allow-list would broaden the SQL allow-list clause to match races against
// feeds nobody configured, leaking their raw ids into the payload. The loader drops it.
func TestKalshiScoreboard_RejectsDZFeedConfigRow(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiDZFeed, "Should Be Ignored", 0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiDZFeed, "some_unconfigured_feed", 1.0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	assert.Empty(t, resp.Competitors)
	assert.EqualValues(t, 0, resp.TotalRaces)
}
