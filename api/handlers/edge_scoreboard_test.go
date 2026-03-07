package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSlotFeedRacesTable creates the slot_feed_races table in the shredder DB.
func createSlotFeedRacesTable(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	err := config.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", config.ShredderDB))
	require.NoError(t, err)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.slot_feed_races (
			event_ts DateTime64(3),
			ingested_at DateTime64(3) DEFAULT now(),
			node_id String,
			feed_type String,
			epoch UInt64,
			slot UInt64,
			feed String,
			total_shreds UInt64,
			shreds_won UInt64,
			lead_time_p50_ms Float64,
			lead_time_p95_ms Float64
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toYYYYMM(event_ts)
		ORDER BY (node_id, slot, feed)
	`, "`"+config.ShredderDB+"`"))
	require.NoError(t, err)
}

// insertEdgeScoreboardTestData inserts test metros and slot_feed_races data.
func insertEdgeScoreboardTestData(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	createSlotFeedRacesTable(t)

	// Create metros: SLC and FRA
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, code, name, latitude, longitude)
		VALUES
			('metro-slc', now(), now(), generateUUIDv4(), 0, 1,
			 'metro-slc', 'SLC', 'Salt Lake City', 40.76, -111.89),
			('metro-fra', now(), now(), generateUUIDv4(), 0, 2,
			 'metro-fra', 'FRA', 'Frankfurt', 50.11, 8.68)
	`)
	require.NoError(t, err)

	// Insert feed races for two nodes
	// Slot 100: all 3 feeds (dz, turbine, jito) for both nodes — DZ wins most shreds
	// Slot 200: only turbine + jito (no DZ) — tests completeness calculation
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.slot_feed_races
			(event_ts, node_id, feed_type, epoch, slot, feed, total_shreds, shreds_won, lead_time_p50_ms, lead_time_p95_ms)
		VALUES
			(now(), 'slc-qa-bm1', 'shred', 800, 100, 'dz',      100, 80, 1.5, 3.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 100, 'turbine',  100, 15, 5.0, 10.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 100, 'jito',     100,  5, 8.0, 15.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 200, 'turbine',  100, 60, 4.0, 9.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 200, 'jito',     100, 40, 7.0, 14.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 100, 'dz',       100, 70, 2.0, 4.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 100, 'turbine',  100, 20, 6.0, 12.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 100, 'jito',     100, 10, 9.0, 16.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 200, 'turbine',  100, 55, 5.0, 11.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 200, 'jito',     100, 45, 8.0, 15.0)
	`, "`"+config.ShredderDB+"`"))
	require.NoError(t, err)
}

func TestGetEdgeScoreboard_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createSlotFeedRacesTable(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/scoreboard", nil)
	rr := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Empty(t, resp.Nodes)
	assert.Equal(t, "24h", resp.Window)
}

func TestGetEdgeScoreboard_WithData(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertEdgeScoreboardTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/scoreboard", nil)
	rr := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, "24h", resp.Window)
	assert.Equal(t, uint64(800), resp.CurrentEpoch)
	assert.Len(t, resp.Nodes, 2)

	// Each node has 2 total slots (100 and 200) and 1 DZ slot (100)
	// Total across both nodes: 4 total slots, 2 DZ slots => 50% completeness
	assert.Equal(t, uint64(4), resp.TotalSlots)
	assert.Equal(t, uint64(2), resp.DZSlots)
	assert.Equal(t, 50.0, resp.CompletenessPct)

	// Find nodes by ID
	nodeMap := make(map[string]handlers.EdgeScoreboardNode)
	for _, n := range resp.Nodes {
		nodeMap[n.NodeID] = n
	}

	// Check SLC node
	slc, ok := nodeMap["slc-qa-bm1"]
	require.True(t, ok, "slc-qa-bm1 node should exist")
	assert.Equal(t, "SLC", slc.Location)
	assert.Equal(t, "Salt Lake City", slc.MetroName)
	assert.Equal(t, 40.76, slc.Latitude)
	assert.Equal(t, -111.89, slc.Longitude)
	assert.Equal(t, uint64(2), slc.TotalSlots)
	assert.Equal(t, uint64(1), slc.SlotsObserved)

	// Check DZ feed win rate for SLC (only slot 100 is DZ-participating)
	dzFeed, ok := slc.Feeds["dz"]
	require.True(t, ok, "dz feed should exist for slc")
	assert.Equal(t, uint64(80), dzFeed.ShredsWon)
	assert.Equal(t, uint64(100), dzFeed.TotalShreds)
	assert.Equal(t, 80.0, dzFeed.WinRatePct)

	// Check FRA node
	fra, ok := nodeMap["fra-qa-bm1"]
	require.True(t, ok, "fra-qa-bm1 node should exist")
	assert.Equal(t, "FRA", fra.Location)
	assert.Equal(t, "Frankfurt", fra.MetroName)
	assert.Equal(t, 50.11, fra.Latitude)
	assert.Equal(t, 8.68, fra.Longitude)
	assert.Equal(t, uint64(2), fra.TotalSlots)
	assert.Equal(t, uint64(1), fra.SlotsObserved)

	dzFeed, ok = fra.Feeds["dz"]
	require.True(t, ok, "dz feed should exist for fra")
	assert.Equal(t, uint64(70), dzFeed.ShredsWon)
	assert.Equal(t, uint64(100), dzFeed.TotalShreds)
	assert.Equal(t, 70.0, dzFeed.WinRatePct)
}

func TestGetEdgeScoreboard_WindowParam(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertEdgeScoreboardTestData(t)

	windows := []string{"1h", "24h", "7d", "30d", "all"}
	for _, w := range windows {
		t.Run(w, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/scoreboard?window="+w, nil)
			rr := httptest.NewRecorder()
			handlers.GetEdgeScoreboard(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)

			var resp handlers.EdgeScoreboardResponse
			err := json.NewDecoder(rr.Body).Decode(&resp)
			require.NoError(t, err)
			assert.Equal(t, w, resp.Window)
		})
	}
}

func TestGetEdgeScoreboard_InvalidWindow(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createSlotFeedRacesTable(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/scoreboard?window=bogus", nil)
	rr := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "24h", resp.Window)
}
