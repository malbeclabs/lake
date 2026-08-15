package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	kalshiDZFeed     = "tob_edge_kalshi_perps"
	kalshiDZMbpFeed  = "mbp_edge_kalshi_perps"
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
	// The UI must be told this is an unconfigured environment rather than having to infer it
	// from empty slices, which a capture outage would produce just as well.
	assert.True(t, resp.Unconfigured)
}

// A configured environment whose window simply held no races must NOT be reported as
// unconfigured — that is the distinction the flag exists to preserve.
func TestKalshiScoreboard_ConfiguredButNoRaces(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	assert.False(t, resp.Unconfigured)
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
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 20, 2, "tob_edge_kalshi_perps_ws", "tob_edge_kalshi_perps_fix", 0.2)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	assert.EqualValues(t, 1, resp.TotalRaces, "a DZ-vs-DZ pairing must not be counted as a race")
}

// A DoubleZero row in the allow-list would broaden the SQL allow-list clause to match races
// against feeds nobody configured, leaking their raw ids into the payload. The loader drops
// it — for the market-by-price lanes as well as the top-of-book one.
func TestKalshiScoreboard_RejectsDZFeedConfigRow(t *testing.T) {
	for _, dzFeed := range []string{kalshiDZFeed, kalshiDZMbpFeed} {
		t.Run(dzFeed, func(t *testing.T) {
			api := newKalshiTestAPI(t)
			createKalshiFeedsTable(t, api)
			seedKalshiEntry(t, api, dzFeed, "Should Be Ignored", 0)
			insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, dzFeed, "some_unconfigured_feed", 1.0)

			resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
			require.NoError(t, err)
			assert.Empty(t, resp.Competitors)
			assert.EqualValues(t, 0, resp.TotalRaces)
		})
	}
}

// The market-by-price lanes are DoubleZero's too: an MBP source emits the shared BBO
// observation on every derived top-of-book change, so it races the public feed exactly as the
// top-of-book lane does. Classifying mbp_ as a competitor would both drop these races from the
// counts and invert who is credited with the win — the dashboards' dz_class is `tob_,mbp_`.
func TestKalshiScoreboard_TreatsMbpLaneAsDoubleZero(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// The MBP lane beats the public feed twice and loses once.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiDZMbpFeed, kalshiPublicFeed, 1.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 20, 2, kalshiDZMbpFeed, kalshiPublicFeed, 3.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 30, 3, kalshiPublicFeed, kalshiDZMbpFeed, 0.5)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	assert.EqualValues(t, 3, resp.TotalRaces, "mbp_ races must be counted")
	assert.InDelta(t, 100.0*2/3, resp.DZWinSharePct, 0.1)
	require.Len(t, resp.Competitors, 1)
	assert.Equal(t, kalshiPublicFeed, resp.Competitors[0].Feed, "the public feed is the competitor, not the mbp_ lane")
}

// With both DoubleZero lanes racing, a race DoubleZero LOSES writes one pairwise row per lane
// (the winner is related to each loser) while a race it WINS writes a single DZ-vs-competitor
// row (the tob_-vs-mbp_ row is dropped as DZ-vs-DZ). Counting rows rather than races therefore
// counted every loss twice and understated the win rate — one won and one lost race must read
// 50%, not 33%. See kalshiRaceKeyTuple.
func TestKalshiScoreboard_CountsDualLaneLossOnce(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// Race 1: the public feed wins it, beating both DoubleZero lanes.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiPublicFeed, kalshiDZFeed, 0.5)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiPublicFeed, kalshiDZMbpFeed, 0.6)

	// Race 2: the tob_ lane wins it, beating the public feed and the mbp_ lane.
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 20, 2, kalshiDZFeed, kalshiPublicFeed, 1.0)
	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 20, 2, kalshiDZFeed, kalshiDZMbpFeed, 0.1)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	assert.EqualValues(t, 2, resp.TotalRaces, "two races, not one row per losing lane")
	assert.InDelta(t, 50.0, resp.DZWinSharePct, 0.1)
	require.Len(t, resp.Competitors, 1)
	assert.EqualValues(t, 2, resp.Competitors[0].Races)
	assert.InDelta(t, 50.0, resp.Competitors[0].DZWinPct, 0.1)
	require.Len(t, resp.Nodes, 1)
	assert.EqualValues(t, 2, resp.Nodes[0].TotalRaces)
}

// A win by an mbp_ lane must read as DoubleZero in the live grid, not as a raw source id.
func TestKalshiScoreboard_LabelsMbpWinnerAsDoubleZero(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiFeedsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	insertKalshiRace(t, api, "cmh-rec1", "cmh", "KXBTCPERP", 10, 1, kalshiDZMbpFeed, kalshiPublicFeed, 1.0)

	resp, err := api.FetchKalshiScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.Len(t, resp.RecentRaces, 1)
	assert.True(t, resp.RecentRaces[0].IsDZ)
	assert.Equal(t, "DoubleZero", resp.RecentRaces[0].WinnerLabel)
	assert.Equal(t, "Public API", resp.RecentRaces[0].RunnerUpLabel)
}

// createKalshiObservationsTable creates the columns the path-latency query reads.
func createKalshiObservationsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.kalshi_bbo_observations (
			measurement_node_id String,
			location_code LowCardinality(String),
			source LowCardinality(String),
			symbol LowCardinality(String),
			source_ts_ms UInt64,
			recv_ts_ns UInt64,
			source_id UInt16,
			channel_id UInt8
		) ENGINE = MergeTree
		ORDER BY (measurement_node_id, symbol, source_ts_ms, source, recv_ts_ns)
	`, db)))
}

// insertObservation records `source` seeing symbol's update stamped sourceTsMs, latencyMs later.
func insertObservation(t *testing.T, api *handlers.API, source string, sourceID uint16, channelID uint8, symbol string, sourceTsMs uint64, latencyMs float64) {
	t.Helper()
	insertObservationAt(t, api, "cmh", source, sourceID, channelID, symbol, sourceTsMs, latencyMs)
}

func insertObservationAt(t *testing.T, api *handlers.API, metro, source string, sourceID uint16, channelID uint8, symbol string, sourceTsMs uint64, latencyMs float64) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.kalshi_bbo_observations
		(measurement_node_id, location_code, source, symbol, source_ts_ms, recv_ts_ns, source_id, channel_id)
		VALUES ('%s-rec1', '%s', '%s', '%s', %d, toUInt64(%d) * 1000000 + %d, %d, %d)
	`, db, metro, metro, source, symbol, sourceTsMs, sourceTsMs, int64(latencyMs*1e6), sourceID, channelID)))
}

// The headline: each feed measured against the venue's own timestamp, never against each other.
func TestKalshiPathLatency_PerFeed(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiObservationsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	// recv_ts_ns must land inside the 24h window, so anchor source_ts_ms at now.
	nowMs := uint64(time.Now().UnixMilli())
	for i, lat := range []float64{2, 4, 6} {
		insertObservation(t, api, kalshiDZFeed, 3, 101, "KXBTCPERP", nowMs+uint64(i), lat)
	}
	for i, lat := range []float64{500, 520, 540} {
		insertObservation(t, api, kalshiPublicFeed, 9, 0, "KXBTCPERP", nowMs+uint64(i), lat)
	}

	pl, err := api.FetchKalshiPathLatency(t.Context())
	require.NoError(t, err)
	require.Len(t, pl.Feeds, 2)

	// Within a vantage, DoubleZero first, then the configured competitors in order.
	assert.True(t, pl.Feeds[0].IsDZ)
	assert.Equal(t, "DoubleZero", pl.Feeds[0].Label)
	assert.Equal(t, "cmh", pl.Feeds[0].LocationCode)
	assert.InDelta(t, 4.0, pl.Feeds[0].P50Ms, 0.01)
	assert.EqualValues(t, 3, pl.Feeds[0].Samples)

	assert.False(t, pl.Feeds[1].IsDZ)
	assert.Equal(t, "Public API", pl.Feeds[1].Label)
	assert.Equal(t, "cmh", pl.Feeds[1].LocationCode)
	assert.InDelta(t, 520.0, pl.Feeds[1].P50Ms, 0.01)
}

// Latency is a property of a path, and a path ends somewhere. Without the metro in the group
// key the inner min() collapses every vantage into one row per update and reports fleet-wide
// first arrival instead — a different quantity that flatters the best-connected vantage.
func TestKalshiPathLatency_PerVantage(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiObservationsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	nowMs := uint64(time.Now().UnixMilli())
	// One update, seen by DoubleZero at three vantages with very different latencies.
	insertObservationAt(t, api, "cmh", kalshiDZFeed, 3, 101, "KXBTCPERP", nowMs, 5)
	insertObservationAt(t, api, "was", kalshiDZFeed, 3, 101, "KXBTCPERP", nowMs, 25)
	insertObservationAt(t, api, "dub", kalshiDZFeed, 3, 101, "KXBTCPERP", nowMs, 60)

	pl, err := api.FetchKalshiPathLatency(t.Context())
	require.NoError(t, err)
	require.Len(t, pl.Feeds, 3, "one row per (feed, vantage), not one collapsed row")

	byMetro := map[string]handlers.KalshiFeedLatency{}
	for _, f := range pl.Feeds {
		byMetro[f.LocationCode] = f
	}
	assert.InDelta(t, 5.0, byMetro["cmh"].P50Ms, 0.01)
	assert.InDelta(t, 25.0, byMetro["was"].P50Ms, 0.01)
	assert.InDelta(t, 60.0, byMetro["dub"].P50Ms, 0.01)
}

// The FIX arm stamps source_ts_ms from a different clock than the WS arm and the public feed,
// so averaging it into DoubleZero's latency would report a number that means nothing. The
// market-by-price lanes are excluded for the same reason, even though they count in the race.
func TestKalshiPathLatency_ExcludesIncomparableClocks(t *testing.T) {
	api := newKalshiTestAPI(t)
	createKalshiObservationsTable(t, api)
	seedKalshiEntry(t, api, kalshiPublicFeed, "Public API", 0)

	nowMs := uint64(time.Now().UnixMilli())
	insertObservation(t, api, kalshiDZFeed, 3, 101, "KXBTCPERP", nowMs, 5)   // WS arm
	insertObservation(t, api, kalshiDZFeed, 3, 1, "KXBTCPERP", nowMs+1, 900) // FIX arm
	// The MBP lane carries source_id 3 too — production does, so source_id alone cannot
	// discriminate the lanes and the tob_ prefix is what actually excludes this row.
	insertObservationAt(t, api, "cmh", kalshiDZMbpFeed, 3, 101, "KXBTCPERP", nowMs+2, 800)

	pl, err := api.FetchKalshiPathLatency(t.Context())
	require.NoError(t, err)
	require.Len(t, pl.Feeds, 1, "only the WS arm has a comparable source timestamp")
	assert.InDelta(t, 5.0, pl.Feeds[0].P50Ms, 0.01)
	assert.EqualValues(t, 1, pl.Feeds[0].Samples)
}
