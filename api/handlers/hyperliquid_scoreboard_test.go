package handlers_test

import (
	"context"
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

// createFeedsTable creates the hyperliquid_bbo_feed_race_summary table in the feeds DB.
func createFeedsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.hyperliquid_bbo_feed_race_summary (
			event_ts DateTime64(9),
			ingested_at DateTime64(9) DEFAULT now64(9),
			capture_run_id String,
			measurement_node_id String,
			host String,
			location_code LowCardinality(String),
			feed_type LowCardinality(String) DEFAULT 'bbo',
			symbol LowCardinality(String),
			source_ts_ms UInt64,
			bbo_hash UInt64,
			feed LowCardinality(String),
			loser_feed LowCardinality(String) DEFAULT '',
			total_events UInt64,
			events_won UInt64,
			lead_time_p50_ms Float64 DEFAULT 0,
			lead_time_p95_ms Float64 DEFAULT 0,
			send_lead_time_p50_ms Nullable(Float64) DEFAULT NULL,
			send_lead_time_p95_ms Nullable(Float64) DEFAULT NULL
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toDate(event_ts)
		ORDER BY (measurement_node_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed)
	`, db)))
}

// pairwiseRow inserts one pairwise race row (winner feed beat loser_feed by leadMs).
func insertPairwise(t *testing.T, api *handlers.API, node, loc, symbol string, srcTs, hash uint64, winner, loser string, leadMs float64) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.hyperliquid_bbo_feed_race_summary
		(event_ts, capture_run_id, measurement_node_id, host, location_code, symbol, source_ts_ms, bbo_hash, feed, loser_feed, total_events, events_won, lead_time_p50_ms, lead_time_p95_ms)
		VALUES (now64(9), 'run1', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', 1, 1, %f, %f)
	`, db, node, node, loc, symbol, srcTs, hash, winner, loser, leadMs, leadMs)))
}

// insertEntry adds a scoreboard feed config row. Feed names and labels here are deliberately
// neutral placeholders — this repository is public.
//
// SetupPostgresForTest gives no per-test isolation (see cleanupPublisherCheckCache in
// v1_shreds_publishers_test.go for the same issue), so this cleans up its own row after
// the test — otherwise fixtures shared across tests (e.g. "feed_a_bbo") collide on the
// table's primary key.
func insertEntry(t *testing.T, api *handlers.API, feed, label string, order int, enabled bool) {
	t.Helper()
	_, err := api.PgPool.Exec(t.Context(), `
		INSERT INTO hyperliquid_scoreboard_entry (feed, label, display_order, enabled)
		VALUES ($1, $2, $3, $4)`, feed, label, order, enabled)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = api.PgPool.Exec(context.Background(), `DELETE FROM hyperliquid_scoreboard_entry WHERE feed = $1`, feed)
	})
}

func TestGetHyperliquidScoreboard_Empty(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api) // empty table -> empty-but-valid response

	req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetHyperliquidScoreboard(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "1h", resp.Window)
	assert.Equal(t, "MISS", rr.Header().Get("X-Cache"))
}

func TestGetHyperliquidScoreboard_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	// Do NOT create the table -> handler must degrade to empty 200, not 500.

	req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetHyperliquidScoreboard(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Competitors)
}

func TestFetchHyperliquidScoreboardData_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	// Do NOT create the table -> FetchHyperliquidScoreboardData must return an
	// empty-but-valid response (nil error, non-nil resp, empty slices).

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "24h", resp.Window)
	assert.Empty(t, resp.Competitors)
	assert.Empty(t, resp.Nodes)
	assert.Empty(t, resp.RecentRaces)
}

func TestHyperliquidScoreboard_PerNode(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)

	// tyo: DZ wins both vs Feed A. nyc: DZ wins 1, loses 1 vs Feed A.
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 2.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 20, 2, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 2.0)
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 30, 3, "tob_aws_galaxy1", "feed_a_bbo", 1.0)
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 40, 4, "feed_a_bbo", "tob_aws_galaxy1", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 2)

	byNode := map[string]handlers.HyperliquidNode{}
	for _, n := range resp.Nodes {
		byNode[n.MeasurementNodeID] = n
	}
	assert.InDelta(t, 100.0, byNode["tyo-rec1"].DZWinSharePct, 0.1)
	assert.InDelta(t, 50.0, byNode["nyc-rec1"].DZWinSharePct, 0.1)
}

func TestHyperliquidScoreboard_RecentRaces(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)
	insertEntry(t, api, "feed_b_bbo", "Feed B", 2, true)

	// A DZ-won race (BTC) and a competitor-won race (ETH), both pairwise.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 100, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.5)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 200, 2, "feed_b_bbo", "tob_gcp_tyo_hl_mainnet1", 0.7)

	races, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(races.RecentRaces), 2)

	bySym := map[string]handlers.HyperliquidRace{}
	for _, r := range races.RecentRaces {
		bySym[r.Symbol] = r
	}
	assert.True(t, bySym["BTC"].IsDZ)
	assert.Equal(t, "Feed A", bySym["BTC"].RunnerUpLabel)
	assert.InDelta(t, 1.5, bySym["BTC"].LeadMs, 0.001)
	assert.False(t, bySym["ETH"].IsDZ)
	assert.Equal(t, "Feed B", bySym["ETH"].WinnerLabel)
}

// A cell where DoubleZero won zero races (a competitor swept it) makes the lead-time
// quantile aggregate over an empty predicate set, which ClickHouse returns as NaN. If that
// NaN reaches the float64 fields, json encoding of the whole response fails — breaking the
// scoreboard for everyone and poisoning the page cache. The percentiles must coalesce to 0.
func TestHyperliquidScoreboard_ZeroDZWins_Encodable(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)

	// Only a competitor-won race: DZ has zero wins vs Feed A in this window.
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 10, 1, "feed_a_bbo", "tob_aws_galaxy1", 3.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	// The entire response must be JSON-encodable — a single NaN anywhere fails encoding.
	_, err = json.Marshal(resp)
	require.NoError(t, err, "response must not contain NaN percentiles")

	var got *handlers.HyperliquidCompetitor
	for i := range resp.Competitors {
		if resp.Competitors[i].Feed == "feed_a_bbo" {
			got = &resp.Competitors[i]
		}
	}
	require.NotNil(t, got)
	assert.InDelta(t, 0.0, got.DZWinPct, 0.001)
	assert.InDelta(t, 0.0, got.LeadP50Ms, 0.001)
	assert.InDelta(t, 0.0, got.LeadP95Ms, 0.001)
}

func TestHyperliquidScoreboard_HeadlineAndCompetitors(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)

	// 4 races at node tyo: DZ (tob_*) beats Feed A three times, loses once.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 1000, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 2000, 2, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 2.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 3000, 3, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 3.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 4000, 4, "feed_a_bbo", "tob_gcp_tyo_hl_mainnet1", 0.5)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	// DZ won 3 of 4 comparable races = 75%.
	assert.InDelta(t, 75.0, resp.DZWinSharePct, 0.1)
	assert.EqualValues(t, 4, resp.TotalRaces)

	var got *handlers.HyperliquidCompetitor
	for i := range resp.Competitors {
		if resp.Competitors[i].Feed == "feed_a_bbo" {
			got = &resp.Competitors[i]
		}
	}
	require.NotNil(t, got)
	assert.Equal(t, "Feed A", got.Label)
	assert.InDelta(t, 75.0, got.DZWinPct, 0.1)
	assert.EqualValues(t, 4, got.Races)
	// Lead p50 over the 3 DZ wins (1.0, 2.0, 3.0) = 2.0 (quantileTDigest(0.5), exact at this size).
	assert.InDelta(t, 2.0, got.LeadP50Ms, 0.001)
}

// The migration must create an empty config table: rows are environment config inserted
// out of band, never seeded by the migration.
func TestHyperliquidScoreboardEntry_TableCreatedEmpty(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)

	var n int
	err := api.PgPool.QueryRow(t.Context(),
		`SELECT count(*) FROM hyperliquid_scoreboard_entry`).Scan(&n)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// Feeds are emitted in configured display_order, so reordering the scoreboard is a row edit.
func TestHyperliquidScoreboard_RespectsDisplayOrder(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	// Insert out of order; display_order decides the emitted order.
	insertEntry(t, api, "feed_b_bbo", "Feed B", 2, true)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)

	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 20, 2, "tob_gcp_tyo_hl_mainnet1", "feed_b_bbo", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.Len(t, resp.Competitors, 2)
	assert.Equal(t, "feed_a_bbo", resp.Competitors[0].Feed)
	assert.Equal(t, "feed_b_bbo", resp.Competitors[1].Feed)
}

// Disabling a row removes the feed everywhere: columns, headline totals, and recent races.
// This is the "remove a feed without a deploy" path.
func TestHyperliquidScoreboard_DisabledEntryExcludedEverywhere(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)
	insertEntry(t, api, "feed_b_bbo", "Feed B", 2, false)

	// DZ beats Feed A once and Feed B once; only the Feed A race may count.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 20, 2, "tob_gcp_tyo_hl_mainnet1", "feed_b_bbo", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	require.Len(t, resp.Competitors, 1)
	assert.Equal(t, "feed_a_bbo", resp.Competitors[0].Feed)
	assert.EqualValues(t, 1, resp.TotalRaces)
	for _, r := range resp.RecentRaces {
		assert.NotEqual(t, "feed_b_bbo", r.RunnerUpFeed)
		assert.NotEqual(t, "feed_b_bbo", r.WinnerFeed)
	}
}

// A feed with no config row at all never surfaces — including in the recent-races strip,
// which previously could show an unlisted feed under its raw name.
func TestHyperliquidScoreboard_UnconfiguredFeedNeverSurfaces(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)

	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 20, 2, "tob_gcp_tyo_hl_mainnet1", "feed_unlisted_bbo", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	require.Len(t, resp.Competitors, 1)
	assert.Equal(t, "feed_a_bbo", resp.Competitors[0].Feed)
	for _, r := range resp.RecentRaces {
		assert.NotEqual(t, "feed_unlisted_bbo", r.RunnerUpFeed)
		assert.NotEqual(t, "feed_unlisted_bbo", r.WinnerFeed)
	}
}

// A malformed feed id must be dropped rather than inlined into SQL, and must not stop the
// remaining valid rows from serving.
func TestHyperliquidScoreboard_UnsafeFeedIDSkipped(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)
	insertEntry(t, api, "bad'; DROP TABLE hyperliquid_scoreboard_entry --", "Bad", 2, true)

	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.Len(t, resp.Competitors, 1)
	assert.Equal(t, "feed_a_bbo", resp.Competitors[0].Feed)

	// The config table must still exist — proving nothing was executed.
	var n int
	require.NoError(t, api.PgPool.QueryRow(t.Context(),
		`SELECT count(*) FROM hyperliquid_scoreboard_entry`).Scan(&n))
	assert.Equal(t, 2, n)
}

// With the feeds table present but no configured rows, the scoreboard serves an
// empty-but-valid payload rather than erroring — the local-dev and unseeded-env path.
func TestHyperliquidScoreboard_NoConfiguredEntries(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)

	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "24h", resp.Window)
	assert.Empty(t, resp.Competitors)
	assert.Empty(t, resp.Nodes)
	assert.Empty(t, resp.RecentRaces)
	assert.EqualValues(t, 0, resp.TotalRaces)
}

// A genuine Postgres failure while loading the feed config must propagate as an error rather
// than degrade to the same empty-but-valid response as "zero rows configured" — otherwise the
// background refresher and page-cache worker would treat the failure as success and overwrite
// the last-good cached payload with an empty one.
func TestFetchHyperliquidScoreboardData_ConfigLoadFailure(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)

	api.PgPool.Close() // force loadHyperliquidScoreboardEntries's query to fail

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.Error(t, err)
	assert.Nil(t, resp)
}

// A tob_ feed accidentally added as a scoreboard config row must be ignored. tob_* feeds are
// DoubleZero's own; the allow-list clause assumes they are never configured entries, so a
// configured tob_ row would broaden the clause to match races against unconfigured
// competitors, leaking their raw feed ids into the public payload.
func TestHyperliquidScoreboard_TobFeedEntryIgnored(t *testing.T) {
	api := apitesting.NewTestAPIBarePg(t, testChDB, testPgDB)
	createFeedsTable(t, api)
	insertEntry(t, api, "feed_a_bbo", "Feed A", 1, true)
	insertEntry(t, api, "tob_gcp_tyo_hl_mainnet1", "Bad DZ Entry", 2, true)

	// If the tob_ row were honored, this race would slip through the allow-list purely
	// because tob_gcp_tyo_hl_mainnet1 appears in the IN(...) list, exposing the otherwise
	// unconfigured feed_unlisted_bbo.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 10, 1, "tob_gcp_tyo_hl_mainnet1", "feed_a_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 20, 2, "tob_gcp_tyo_hl_mainnet1", "feed_unlisted_bbo", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	require.Len(t, resp.Competitors, 1)
	assert.Equal(t, "feed_a_bbo", resp.Competitors[0].Feed)
	for _, r := range resp.RecentRaces {
		assert.NotEqual(t, "feed_unlisted_bbo", r.RunnerUpFeed)
		assert.NotEqual(t, "feed_unlisted_bbo", r.WinnerFeed)
	}

	// The config table must still hold the tob_ row — it's dropped from the in-memory
	// allow-list, not deleted from Postgres.
	var n int
	require.NoError(t, api.PgPool.QueryRow(t.Context(),
		`SELECT count(*) FROM hyperliquid_scoreboard_entry`).Scan(&n))
	assert.Equal(t, 2, n)
}
