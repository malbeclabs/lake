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

// createShredderTables creates the slot_feed_races and publisher_shred_stats tables in the shredder DB.
func createShredderTables(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	db := "`" + config.GetShredderDB() + "`"
	err := config.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db))
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
			loser_feed String DEFAULT '',
			total_shreds UInt64,
			shreds_won UInt64,
			lead_time_p50_ms Float64 DEFAULT 0,
			lead_time_p95_ms Float64 DEFAULT 0
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toYYYYMM(event_ts)
		ORDER BY (node_id, slot, feed, loser_feed)
	`, db))
	require.NoError(t, err)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.publisher_shred_stats (
			event_ts DateTime64(3),
			ingested_at DateTime64(3) DEFAULT now(),
			host String,
			publisher_ip String,
			client_ip String,
			node_pubkey String,
			vote_pubkey String,
			activated_stake UInt64,
			dz_user_pubkey String,
			dz_device_code String,
			dz_metro_code String,
			epoch UInt64,
			slot UInt64,
			total_packets UInt64,
			unique_shreds UInt64,
			data_shreds UInt64,
			coding_shreds UInt64,
			max_data_index Int64,
			needs_repair Bool,
			first_seen_ns Int64,
			last_seen_ns Int64,
			is_scheduled_leader Bool
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(event_ts)
		ORDER BY (slot, node_pubkey)
	`, db))
	require.NoError(t, err)
}

// insertEdgeScoreboardTestData inserts test metros and slot_feed_races data.
func insertEdgeScoreboardTestData(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	createShredderTables(t)

	const epoch = 800
	const slot1 = 100 // DZ-leader slot
	const slot2 = 200 // non-DZ slot

	// Create metros: SLC and FRA
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, code, name, latitude, longitude)
		VALUES
			('metro-slc', now(), now(), generateUUIDv4(), 0, 1,
			 'metro-slc', 'slc', 'Salt Lake City', 40.76, -111.89),
			('metro-fra', now(), now(), generateUUIDv4(), 0, 2,
			 'metro-fra', 'fra', 'Frankfurt', 50.11, 8.68)
	`)
	require.NoError(t, err)

	// Mark slot1 as a DZ-leader slot via publisher_shred_stats
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.publisher_shred_stats
			(event_ts, host, publisher_ip, client_ip, node_pubkey, vote_pubkey,
			 activated_stake, dz_user_pubkey, dz_device_code, dz_metro_code,
			 epoch, slot, total_packets, unique_shreds, data_shreds, coding_shreds,
			 max_data_index, needs_repair, first_seen_ns, last_seen_ns, is_scheduled_leader)
		VALUES
			(now(), 'host-1', '1.2.3.4', '1.2.3.4', 'test-pubkey', 'test-vote',
			 1000000000, 'dz-user-1', 'slc-qa-bm1', 'slc',
			 %d, %d, 100, 80, 60, 20,
			 79, false, 0, 1000000, true)
	`, "`"+config.GetShredderDB()+"`", epoch, slot1))
	require.NoError(t, err)

	// Insert win-count rows (loser_feed = '') — per-feed summary for each slot
	// slot1: all 3 feeds (dz, turbine, jito) for both nodes — DZ wins most shreds (DZ-leader slot)
	// slot2: only turbine + jito (no DZ) — tests completeness calculation
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.slot_feed_races
			(event_ts, node_id, feed_type, epoch, slot, feed, loser_feed, total_shreds, shreds_won)
		VALUES
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[3]d, 'dz',      '', 100, 80),
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[3]d, 'turbine',  '', 100, 15),
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[3]d, 'jito',     '', 100,  5),
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[4]d, 'turbine',  '', 100, 60),
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[4]d, 'jito',     '', 100, 40),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[3]d, 'dz',       '', 100, 70),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[3]d, 'turbine',  '', 100, 20),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[3]d, 'jito',     '', 100, 10),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[4]d, 'turbine',  '', 100, 55),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[4]d, 'jito',     '', 100, 45)
	`, "`"+config.GetShredderDB()+"`", epoch, slot1, slot2))
	require.NoError(t, err)

	// Insert lead-time rows (loser_feed != '') — pairwise: winner vs specific loser
	// For slot1 on slc-qa-bm1: dz beat turbine and jito
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.slot_feed_races
			(event_ts, node_id, feed_type, epoch, slot, feed, loser_feed, lead_time_p50_ms, lead_time_p95_ms)
		VALUES
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[3]d, 'dz', 'turbine', 1.5, 3.0),
			(now(), 'slc-qa-bm1', 'shred', %[2]d, %[3]d, 'dz', 'jito',    2.0, 4.0),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[3]d, 'dz', 'turbine', 2.5, 5.0),
			(now(), 'fra-qa-bm1', 'shred', %[2]d, %[3]d, 'dz', 'jito',    3.0, 6.0)
	`, "`"+config.GetShredderDB()+"`", epoch, slot1))
	require.NoError(t, err)
}

func TestGetEdgeScoreboard_Empty(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createShredderTables(t)

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
	t.Parallel()
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

	// Check pairwise lead times for SLC DZ feed
	assert.Len(t, dzFeed.LeadTimes, 2, "slc dz should have 2 pairwise lead times")
	ltMap := make(map[string]handlers.EdgeScoreboardLeadTime)
	for _, lt := range dzFeed.LeadTimes {
		ltMap[lt.LoserFeed] = lt
	}
	assert.InDelta(t, 1.5, ltMap["turbine"].P50Ms, 0.1)
	assert.InDelta(t, 3.0, ltMap["turbine"].P95Ms, 0.1)
	assert.InDelta(t, 2.0, ltMap["jito"].P50Ms, 0.1)
	assert.InDelta(t, 4.0, ltMap["jito"].P95Ms, 0.1)

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

	// Check pairwise lead times for FRA DZ feed
	assert.Len(t, dzFeed.LeadTimes, 2, "fra dz should have 2 pairwise lead times")
}

func TestGetEdgeScoreboard_WindowParam(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertEdgeScoreboardTestData(t)

	windows := []string{"1h", "24h", "7d", "30d", "all"}
	for _, w := range windows {
		t.Run(w, func(t *testing.T) {
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createShredderTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/edge/scoreboard?window=bogus", nil)
	rr := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "24h", resp.Window)
}
