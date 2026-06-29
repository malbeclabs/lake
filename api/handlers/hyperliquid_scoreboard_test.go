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

func TestGetHyperliquidScoreboard_Empty(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
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
	api := apitesting.NewTestAPIBare(t, testChDB)
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
	api := apitesting.NewTestAPIBare(t, testChDB)
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
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api)

	// tyo: DZ wins both vs QuickNode. nyc: DZ wins 1, loses 1 vs QuickNode.
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 10, 1, "tob_gcp_tyo_hl_mainnet1", "quicknode_l2book_bbo", 2.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 20, 2, "tob_gcp_tyo_hl_mainnet1", "quicknode_l2book_bbo", 2.0)
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 30, 3, "tob_aws_galaxy1", "quicknode_l2book_bbo", 1.0)
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 40, 4, "quicknode_l2book_bbo", "tob_aws_galaxy1", 1.0)

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
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api)

	// A DZ-won race (BTC) and a competitor-won race (ETH), both pairwise.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 100, 1, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 1.5)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 200, 2, "quicknode_l2book_bbo", "tob_gcp_tyo_hl_mainnet1", 0.7)

	races, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(races.RecentRaces), 2)

	bySym := map[string]handlers.HyperliquidRace{}
	for _, r := range races.RecentRaces {
		bySym[r.Symbol] = r
	}
	assert.True(t, bySym["BTC"].IsDZ)
	assert.Equal(t, "Hydromancer", bySym["BTC"].RunnerUpLabel)
	assert.InDelta(t, 1.5, bySym["BTC"].LeadMs, 0.001)
	assert.False(t, bySym["ETH"].IsDZ)
}

func TestHyperliquidScoreboard_HeadlineAndCompetitors(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api)

	// 4 races at node tyo: DZ (tob_*) beats Hydromancer three times, loses once.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 1000, 1, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 2000, 2, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 2.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 3000, 3, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 3.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 4000, 4, "hydromancer_bbo", "tob_gcp_tyo_hl_mainnet1", 0.5)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	// DZ won 3 of 4 comparable races = 75%.
	assert.InDelta(t, 75.0, resp.DZWinSharePct, 0.1)
	assert.EqualValues(t, 4, resp.TotalRaces)

	var hydro *handlers.HyperliquidCompetitor
	for i := range resp.Competitors {
		if resp.Competitors[i].Feed == "hydromancer_bbo" {
			hydro = &resp.Competitors[i]
		}
	}
	require.NotNil(t, hydro)
	assert.Equal(t, "Hydromancer", hydro.Label)
	assert.InDelta(t, 75.0, hydro.DZWinPct, 0.1)
	assert.EqualValues(t, 4, hydro.Races)
	// Lead p50 over the 3 DZ wins (1.0, 2.0, 3.0) = 2.0 (quantileExact(0.5)).
	assert.InDelta(t, 2.0, hydro.LeadP50Ms, 0.001)
}
