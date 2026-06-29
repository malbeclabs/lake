package handlers_test

import (
	"fmt"
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
