package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDZStakeAttribution_Disconnect(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	// DZ IP
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "", "")

	// Current tables (for total stake computation)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	// node-stay is always on DZ with gossip IP 1.2.3.4
	insertCurrentGossipNode(t, "node-stay", "1.2.3.4")

	// T1: validator A on DZ (gossip IP 1.2.3.4), validator stay also on DZ
	for _, v := range []struct{ vote, node, ip string }{
		{"vote-A", "node-A", "1.2.3.4"},
		{"vote-stay", "node-stay", "1.2.3.4"},
	} {
		insertVoteAccountHistory(t, v.vote, v.node, 100_000_000_000_000, t1)
		insertGossipNodeHistory(t, v.node, v.ip, t1)
	}

	// T2: validator A gossip IP changed to non-DZ, validator stay still on DZ
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "5.5.5.5", t2)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-stay", "1.2.3.4", t2)

	// Query the timeline
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Find dz_stake_attribution events
	var attrEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		if e.EventType == "left_dz" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 left_dz event")
	assert.Equal(t, "validator", attrEvents[0].EntityType)

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "left_dz", details["action"])
	assert.Equal(t, "vote-A", details["vote_pubkey"])

	// DZ total stake share should be present and correct.
	// Current state: vote-stay (200k SOL) is on DZ, total = 300k SOL, so current DZ total = 66.67%.
	// The disconnect event already happened, so the DZ total after it = current = 66.67%.
	dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
	assert.True(t, ok, "dz_total_stake_share_pct should be a float64, got %T: %v", details["dz_total_stake_share_pct"], details["dz_total_stake_share_pct"])
	t.Logf("dz_total_stake_share_pct = %v", dzTotal)
	assert.InDelta(t, 66.67, dzTotal, 1.0, "DZ total should be ~66.67%% (200k on DZ / 300k total)")
}

func TestDZStakeAttribution_Connect(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "", "")
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)

	// T1: validator B NOT on DZ
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t1)

	// T2: validator B now on DZ
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "1.2.3.4", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	var attrEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		if e.EventType == "joined_dz" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 joined_dz event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "joined_dz", details["action"])
	assert.Equal(t, "vote-B", details["vote_pubkey"])

	// DZ total stake share should be present
	dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
	assert.True(t, ok, "dz_total_stake_share_pct should be a float64, got %T: %v", details["dz_total_stake_share_pct"], details["dz_total_stake_share_pct"])
	assert.Greater(t, dzTotal, float64(0), "DZ total should be > 0")
}

func TestDZStakeAttribution_StakeChange(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "", "")
	insertCurrentVoteAccount(t, "vote-C", "node-C", 80_000_000_000_000)

	// T1: validator C on DZ, 100k SOL
	insertVoteAccountHistory(t, "vote-C", "node-C", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-C", "1.2.3.4", t1)

	// T2: validator C on DZ, 80k SOL (decreased)
	insertVoteAccountHistory(t, "vote-C", "node-C", 80_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-C", "1.2.3.4", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	var attrEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		if e.EventType == "stake_changed" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 stake_changed event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "stake_changed", details["action"])
	// Contribution change: 80k - 100k = -20k SOL in lamports
	contribChange, ok := details["contribution_change_lamports"].(float64)
	require.True(t, ok)
	assert.Less(t, contribChange, float64(0), "contribution change should be negative")

	// DZ total stake share should be present
	dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
	assert.True(t, ok, "dz_total_stake_share_pct should be a float64, got %T: %v", details["dz_total_stake_share_pct"], details["dz_total_stake_share_pct"])
	assert.Greater(t, dzTotal, float64(0), "DZ total should be > 0")
}

func TestDZStakeAttribution_ValidatorLeft(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "", "")
	insertCurrentVoteAccount(t, "vote-D", "node-D", 50_000_000_000_000)

	// T1: validator D on DZ, 50k SOL
	insertVoteAccountHistory(t, "vote-D", "node-D", 50_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-D", "1.2.3.4", t1)

	// T2: validator D not in vote accounts (left Solana) - no rows inserted for T2
	// But we need at least one row at T2 so the snapshot exists for the DZ total calculation
	// Add a different validator at T2 so the snapshot exists
	insertVoteAccountHistory(t, "vote-other", "node-other", 1_000_000_000, t2)
	insertGossipNodeHistory(t, "node-other", "9.9.9.9", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	var attrEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		if e.EventType == "left_dz" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 left_dz event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "left_dz", details["action"])
}

func TestDZStakeAttribution_NoChange(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "", "")
	insertCurrentVoteAccount(t, "vote-E", "node-E", 100_000_000_000_000)

	// T1 and T2: same validator, same stake, same DZ status - no change
	for _, ts := range []time.Time{t1, t2} {
		insertVoteAccountHistory(t, "vote-E", "node-E", 100_000_000_000_000, ts)
		insertGossipNodeHistory(t, "node-E", "1.2.3.4", ts)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// No dz_stake_attribution events should appear
	for _, e := range resp.Events {
		if e.EventType == "left_dz" || e.EventType == "joined_dz" || e.EventType == "stake_changed" {
			t.Errorf("unexpected DZ stake attribution event: %s", e.EventType)
		}
	}
}

// TestDZTotalStakeShare_OnJoinedEvent tests that validator_joined events
// (from queryVoteAccountChanges) get dz_total_stake_share_pct populated
// via queryDZTotalBySnapshot.
func TestDZTotalStakeShare_OnJoinedEvent(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	// DZ IP
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "", "")

	// Current tables: validator X (on DZ) and validator Y (not on DZ)
	insertCurrentVoteAccount(t, "vote-X", "node-X", 100_000_000_000_000)
	insertCurrentVoteAccount(t, "vote-Y", "node-Y", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-X", "1.2.3.4")
	insertCurrentGossipNode(t, "node-Y", "9.9.9.9")

	// History: validator X exists at both timestamps (on DZ), validator Y only appears at T2 (joined)
	for _, ts := range []time.Time{t1, t2} {
		insertVoteAccountHistory(t, "vote-X", "node-X", 100_000_000_000_000, ts)
		insertGossipNodeHistory(t, "node-X", "1.2.3.4", ts)
	}
	// Y appears only at T2 (joined event)
	insertVoteAccountHistory(t, "vote-Y", "node-Y", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-Y", "9.9.9.9", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Log all events for debugging
	t.Logf("total events: %d", len(resp.Events))
	for _, e := range resp.Events {
		t.Logf("event: type=%s entity=%s details=%+v", e.EventType, e.EntityType, e.Details)
	}

	// Find the validator_joined event for vote-Y (Solana join, not DZ join)
	var joinedEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		if e.EventType == "validator_joined" {
			if details, ok := e.Details.(map[string]any); ok {
				if details["vote_pubkey"] == "vote-Y" {
					joinedEvents = append(joinedEvents, e)
				}
			}
		}
	}
	require.Len(t, joinedEvents, 1, "expected 1 validator_joined event for vote-Y")

	details, ok := joinedEvents[0].Details.(map[string]any)
	require.True(t, ok)

	// DZ total stake share should be populated via queryDZTotalBySnapshot
	// At the snapshot: vote-X (100k SOL) is on DZ, total = 200k SOL, so DZ total = 50%
	dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
	assert.True(t, ok, "dz_total_stake_share_pct should be present, got %T: %v", details["dz_total_stake_share_pct"], details["dz_total_stake_share_pct"])
	t.Logf("dz_total_stake_share_pct on validator_joined = %v", dzTotal)
	assert.InDelta(t, 50.0, dzTotal, 1.0, "DZ total should be ~50%% (100k/200k)")
}

// --- Helper functions for new tests ---

// tsFormat formats a time for ClickHouse DateTime64(3) columns.
func tsFormat(ts time.Time) string {
	return ts.Format("2006-01-02 15:04:05.000")
}

func insertVoteAccountHistory(t *testing.T, votePubkey, nodePubkey string, stake int64, ts time.Time) {
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage) VALUES ('%s', '%s', '%s', '%s', 0, 0, '%s', 0, '%s', %d, 'true', 0)`,
		votePubkey, tsFormat(ts), tsFormat(ts), uuid.New().String(), votePubkey, nodePubkey, stake)))
}

func insertGossipNodeHistory(t *testing.T, pubkey, gossipIP string, ts time.Time) {
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version) VALUES ('%s', '%s', '%s', '%s', 0, 0, '%s', 0, '%s', 0, '', 0, '')`,
		pubkey, tsFormat(ts), tsFormat(ts), uuid.New().String(), pubkey, gossipIP)))
}

// insertCurrentVoteAccount inserts a vote account into the history table with a
// far-future timestamp so it appears as the "current" row via the view.
func insertCurrentVoteAccount(t *testing.T, votePubkey, nodePubkey string, stake int64) {
	futureTS := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage) VALUES ('%s', '%s', '%s', '%s', 0, 0, '%s', 0, '%s', %d, 'true', 0)`,
		votePubkey, tsFormat(futureTS), tsFormat(futureTS), uuid.New().String(), votePubkey, nodePubkey, stake)))
}

// insertCurrentGossipNode inserts a gossip node into the history table with a
// far-future timestamp so it appears as the "current" row via the view.
func insertCurrentGossipNode(t *testing.T, pubkey, gossipIP string) {
	futureTS := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version) VALUES ('%s', '%s', '%s', '%s', 0, 0, '%s', 0, '%s', 0, '', 0, '')`,
		pubkey, tsFormat(futureTS), tsFormat(futureTS), uuid.New().String(), pubkey, gossipIP)))
}

// deleteCurrentVoteAccount inserts a deleted row into the history table at the
// specified timestamp so the view excludes this entity (simulating "not in current").
// The deleteTS should be within the query range to ensure queryVoteAccountChanges
// properly detects the validator as "left".
func deleteCurrentVoteAccount(t *testing.T, votePubkey string, deleteTS time.Time) {
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage) VALUES ('%s', '%s', '%s', '%s', 1, 0, '%s', 0, '', 0, '', 0)`,
		votePubkey, tsFormat(deleteTS), tsFormat(deleteTS), uuid.New().String(), votePubkey)))
}

// deleteCurrentGossipNode inserts a deleted row into the history table at the
// specified timestamp so the view excludes this entity.
func deleteCurrentGossipNode(t *testing.T, pubkey string, deleteTS time.Time) {
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version) VALUES ('%s', '%s', '%s', '%s', 1, 0, '%s', 0, '', 0, '', 0, '')`,
		pubkey, tsFormat(deleteTS), tsFormat(deleteTS), uuid.New().String(), pubkey)))
}

func insertDZUserHistory(t *testing.T, pk, entityID, ownerPubkey, dzIP, devicePK, status string, ts time.Time) {
	// Use unique attrs_hash per row so the timeline query detects attribute changes
	attrsHash := uint64(ts.UnixMilli())
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_dz_users_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id) VALUES ('%s', '%s', '%s', '%s', 0, %d, '%s', '%s', '%s', '', '', '%s', '%s', 0)`,
		entityID, tsFormat(ts), tsFormat(ts), uuid.New().String(), attrsHash, pk, ownerPubkey, status, dzIP, devicePK)))
}

// insertDZUserCurrent inserts a DZ user into the history table with a
// far-future timestamp so it appears as the "current" row via the view.
func insertDZUserCurrent(t *testing.T, pk, dzIP, status, ownerPubkey, devicePK string) {
	futureTS := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	entityID := fmt.Sprintf("entity-%s", pk)
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_dz_users_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id) VALUES ('%s', '%s', '%s', '%s', 0, 0, '%s', '%s', '%s', '', '', '%s', '%s', 0)`,
		entityID, tsFormat(futureTS), tsFormat(futureTS), uuid.New().String(), pk, ownerPubkey, status, dzIP, devicePK)))
}

func findEventsByType(events []handlers.TimelineEvent, eventType string) []handlers.TimelineEvent {
	var result []handlers.TimelineEvent
	for _, e := range events {
		if e.EventType == eventType {
			result = append(result, e)
		}
	}
	return result
}

func getDetails(t *testing.T, event handlers.TimelineEvent) map[string]any {
	details, ok := event.Details.(map[string]any)
	require.True(t, ok, "expected map[string]any details, got %T", event.Details)
	return details
}

// --- queryVoteAccountChanges tests ---

func TestVoteAccountChanges_ValidatorLeft(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t0 := time.Date(2025, 5, 31, 22, 0, 0, 0, time.UTC) // before query range
	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-A in history but NOT in current = left
	// First appearance before query range so it doesn't also show as "joined"
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t0)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	deleteCurrentVoteAccount(t, "vote-A", t2)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t0)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)

	// Need another validator in current so total_stake > 0
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t0)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t0)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t1)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	leftEvents := findEventsByType(resp.Events, "validator_left")
	require.Len(t, leftEvents, 1, "expected 1 validator_left event")
	details := getDetails(t, leftEvents[0])
	assert.Equal(t, "left", details["action"])
	assert.Equal(t, "vote-A", details["vote_pubkey"])
	assert.Equal(t, "validator", leftEvents[0].EntityType)
}

func TestVoteAccountChanges_JoinedAndLeft(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-A: in history at t1, NOT in current = left
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)
	deleteCurrentVoteAccount(t, "vote-A", t2)

	// vote-B: first appears at t2, IS in current = joined
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "10.0.0.3", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "10.0.0.3")

	// Keep a stable validator in current for total_stake
	// First appearance must be BEFORE query range so it's not counted as "joined"
	t0 := t1.Add(-2 * time.Hour)
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t0)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t0)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t1)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t2)

	// vote-A also needs first appearance before query range so only its "left" shows
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t0)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t0)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	leftEvents := findEventsByType(resp.Events, "validator_left")
	require.GreaterOrEqual(t, len(leftEvents), 1, "expected at least 1 validator_left")

	// Find vote-A left event
	foundLeft := false
	for _, e := range leftEvents {
		details := getDetails(t, e)
		if details["vote_pubkey"] == "vote-A" {
			foundLeft = true
		}
	}
	assert.True(t, foundLeft, "vote-A should have a validator_left event")

	// Find vote-B joined event
	joinedEvents := findEventsByType(resp.Events, "validator_joined")
	foundJoined := false
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		if details["vote_pubkey"] == "vote-B" {
			foundJoined = true
		}
	}
	assert.True(t, foundJoined, "vote-B should have a validator_joined event")
}

func TestVoteAccountChanges_DZMetadata(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user with IP 1.2.3.4
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "device-A")

	// vote-A joins, node on DZ IP
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")

	// vote-B joins, node NOT on DZ
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Both vote-A and vote-B join Solana, so they get validator_joined from queryVoteAccountChanges
	// vote-A is on DZ IP, so it gets DZ metadata enrichment
	solanaJoinedEvents := findEventsByType(resp.Events, "validator_joined")
	foundA := false
	for _, e := range solanaJoinedEvents {
		details := getDetails(t, e)
		if details["vote_pubkey"] == "vote-A" {
			foundA = true
			assert.Equal(t, "ownerAAA", details["owner_pubkey"], "vote-A should have DZ owner_pubkey")
		}
	}
	assert.True(t, foundA, "vote-A should have a validator_joined event with DZ metadata")

	// vote-B is NOT on DZ
	foundB := false
	for _, e := range solanaJoinedEvents {
		details := getDetails(t, e)
		if details["vote_pubkey"] == "vote-B" {
			foundB = true
			ownerPubkey, _ := details["owner_pubkey"].(string)
			assert.Empty(t, ownerPubkey, "vote-B should have empty owner_pubkey")
		}
	}
	assert.True(t, foundB, "vote-B should have a validator_joined event")
}

// --- queryGossipNetworkChanges tests ---

func TestGossipNetworkChanges_ValidatorOffline(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// node-A in gossip history with vote account, but NOT in current gossip
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	deleteCurrentGossipNode(t, "node-A", t2)
	// But need vote account in current for stake lookup
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)

	// Another node stays online so queries don't break
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t1)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t1)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	offlineEvents := findEventsByType(resp.Events, "validator_offline")
	require.Len(t, offlineEvents, 1, "expected 1 validator_offline event")
	details := getDetails(t, offlineEvents[0])
	assert.Equal(t, "offline", details["action"])
	assert.Equal(t, "validator", offlineEvents[0].EntityType)
}

func TestGossipNetworkChanges_GossipNodeOffline(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// node-B in gossip history but NO vote account anywhere
	insertGossipNodeHistory(t, "node-B", "10.0.0.5", t1)
	deleteCurrentGossipNode(t, "node-B", t2)
	// node-B NOT in current gossip, no vote accounts for node-B

	// Need at least one validator in current for total stake
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t1)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t1)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	offlineEvents := findEventsByType(resp.Events, "gossip_node_offline")
	require.Len(t, offlineEvents, 1, "expected 1 gossip_node_offline event")
	details := getDetails(t, offlineEvents[0])
	assert.Equal(t, "offline", details["action"])
	assert.Equal(t, "gossip_node", offlineEvents[0].EntityType)
	votePubkey, _ := details["vote_pubkey"].(string)
	assert.Empty(t, votePubkey, "gossip_node_offline should have empty vote_pubkey")
}

// --- queryStakeChanges tests ---

func TestStakeChanges_Increase(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-A: 100k SOL at t1, 115k SOL at t2 (+15k, above 10k threshold)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 115_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 115_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "10.0.0.1")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	increased := findEventsByType(resp.Events, "stake_increased")
	require.Len(t, increased, 1, "expected 1 stake_increased event")
	assert.Equal(t, "validator", increased[0].EntityType)
	details := getDetails(t, increased[0])
	assert.Equal(t, "increased", details["action"])
}

func TestStakeChanges_Decrease(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-B: 200k SOL at t1, 180k SOL at t2 (-20k)
	insertVoteAccountHistory(t, "vote-B", "node-B", 200_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-B", "node-B", 180_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "10.0.0.2", t1)
	insertGossipNodeHistory(t, "node-B", "10.0.0.2", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 180_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "10.0.0.2")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	decreased := findEventsByType(resp.Events, "stake_decreased")
	require.Len(t, decreased, 1, "expected 1 stake_decreased event")
	assert.Equal(t, "warning", decreased[0].Severity)
	details := getDetails(t, decreased[0])
	assert.Equal(t, "decreased", details["action"])
}

func TestStakeChanges_BelowThreshold(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-C: 100k SOL at t1, 103k SOL at t2 (+3k = 3%, below both thresholds)
	insertVoteAccountHistory(t, "vote-C", "node-C", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-C", "node-C", 103_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-C", "10.0.0.3", t1)
	insertGossipNodeHistory(t, "node-C", "10.0.0.3", t2)
	insertCurrentVoteAccount(t, "vote-C", "node-C", 103_000_000_000_000)
	insertCurrentGossipNode(t, "node-C", "10.0.0.3")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	increased := findEventsByType(resp.Events, "stake_increased")
	decreased := findEventsByType(resp.Events, "stake_decreased")
	assert.Len(t, increased, 0, "expected 0 stake_increased events (below threshold)")
	assert.Len(t, decreased, 0, "expected 0 stake_decreased events")
}

func TestStakeChanges_PercentageThreshold(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-D: 100k SOL at t1, 106k SOL at t2 (+6k = 6%, above 5% but below 10k SOL)
	insertVoteAccountHistory(t, "vote-D", "node-D", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-D", "node-D", 106_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-D", "10.0.0.4", t1)
	insertGossipNodeHistory(t, "node-D", "10.0.0.4", t2)
	insertCurrentVoteAccount(t, "vote-D", "node-D", 106_000_000_000_000)
	insertCurrentGossipNode(t, "node-D", "10.0.0.4")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	increased := findEventsByType(resp.Events, "stake_increased")
	require.Len(t, increased, 1, "expected 1 stake_increased (6% above 5% threshold)")
}

func TestStakeChanges_OnDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "")

	// vote-A on DZ, stake increase
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 115_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t1)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 115_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	increased := findEventsByType(resp.Events, "stake_increased")
	require.Len(t, increased, 1, "expected 1 stake_increased")
	assert.Contains(t, increased[0].Title, "DZ ", "title should start with DZ prefix for on-DZ validator")
}

// --- queryValidatorEvents tests ---

func TestValidatorEvents_JoinedDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user: status pending at t1, activated at t2
	insertDZUserHistory(t, "user-A", "entity-A", "ownerAAA", "1.2.3.4", "device-A", "pending", t1)
	insertDZUserHistory(t, "user-A", "entity-A", "ownerAAA", "1.2.3.4", "device-A", "activated", t2)
	insertDZUserCurrent(t, "user-A", "1.2.3.4", "activated", "ownerAAA", "device-A")

	// Gossip node matching DZ IP with vote account (validator)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t1)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Look for validator_joined from queryValidatorEvents (DZ user status transition)
	joinedEvents := findEventsByType(resp.Events, "joined_dz")
	found := false
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		if details["action"] == "joined_dz" && details["kind"] == "validator" {
			found = true
			assert.Equal(t, "validator", e.EntityType)
			break
		}
	}
	assert.True(t, found, "expected a joined_dz event with action=joined_dz and kind=validator")
}

func TestValidatorEvents_LeftDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user: activated at t1, deactivated at t2
	insertDZUserHistory(t, "user-B", "entity-B", "ownerBBB", "1.2.3.5", "device-B", "activated", t1)
	insertDZUserHistory(t, "user-B", "entity-B", "ownerBBB", "1.2.3.5", "device-B", "deactivated", t2)
	insertDZUserCurrent(t, "user-B", "1.2.3.5", "deactivated", "ownerBBB", "device-B")

	// Gossip node with vote account
	insertGossipNodeHistory(t, "node-B", "1.2.3.5", t1)
	insertGossipNodeHistory(t, "node-B", "1.2.3.5", t2)
	insertCurrentGossipNode(t, "node-B", "1.2.3.5")
	insertVoteAccountHistory(t, "vote-B", "node-B", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-B", "node-B", 100_000_000_000_000, t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 100_000_000_000_000)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	leftEvents := findEventsByType(resp.Events, "left_dz")
	found := false
	for _, e := range leftEvents {
		details := getDetails(t, e)
		if details["action"] == "left_dz" && details["kind"] == "validator" {
			found = true
			assert.Equal(t, "warning", e.Severity)
			break
		}
	}
	assert.True(t, found, "expected a left_dz event with action=left_dz and kind=validator")
}

func TestValidatorEvents_GossipNodeJoinedDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user: pending -> activated
	insertDZUserHistory(t, "user-C", "entity-C", "ownerCCC", "1.2.3.6", "device-C", "pending", t1)
	insertDZUserHistory(t, "user-C", "entity-C", "ownerCCC", "1.2.3.6", "device-C", "activated", t2)
	insertDZUserCurrent(t, "user-C", "1.2.3.6", "activated", "ownerCCC", "device-C")

	// Gossip node with NO vote account (gossip_only)
	insertGossipNodeHistory(t, "node-C", "1.2.3.6", t1)
	insertGossipNodeHistory(t, "node-C", "1.2.3.6", t2)
	insertCurrentGossipNode(t, "node-C", "1.2.3.6")

	// Need at least one validator in current for total stake
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	joinedEvents := findEventsByType(resp.Events, "gossip_node_joined")
	require.Len(t, joinedEvents, 1, "expected 1 gossip_node_joined event")
	assert.Equal(t, "gossip_node", joinedEvents[0].EntityType)
	details := getDetails(t, joinedEvents[0])
	assert.Equal(t, "gossip_only", details["kind"])
}

func TestValidatorEvents_GossipNodeLeftDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user: activated -> deactivated
	insertDZUserHistory(t, "user-D", "entity-D", "ownerDDD", "1.2.3.7", "device-D", "activated", t1)
	insertDZUserHistory(t, "user-D", "entity-D", "ownerDDD", "1.2.3.7", "device-D", "deactivated", t2)
	insertDZUserCurrent(t, "user-D", "1.2.3.7", "deactivated", "ownerDDD", "device-D")

	// Gossip node with NO vote account
	insertGossipNodeHistory(t, "node-D", "1.2.3.7", t1)
	insertGossipNodeHistory(t, "node-D", "1.2.3.7", t2)
	insertCurrentGossipNode(t, "node-D", "1.2.3.7")

	// Need at least one validator in current
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	leftEvents := findEventsByType(resp.Events, "gossip_node_left")
	require.Len(t, leftEvents, 1, "expected 1 gossip_node_left event")
	assert.Equal(t, "gossip_node", leftEvents[0].EntityType)
	details := getDetails(t, leftEvents[0])
	assert.Equal(t, "gossip_only", details["kind"])
}

// --- Filter tests ---

func TestDZFilter_OnDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user with IP 1.2.3.4
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "device-A")

	// vote-A joins, on DZ
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")

	// vote-B joins, NOT on DZ
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&dz_filter=on_dz",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	joinedEvents := findEventsByType(resp.Events, "joined_dz")
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		assert.NotEqual(t, "vote-B", details["vote_pubkey"], "off-DZ validator should be filtered out with on_dz filter")
	}
}

func TestDZFilter_OffDZ(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "device-A")

	// vote-A joins, on DZ
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")

	// vote-B joins, NOT on DZ
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&dz_filter=off_dz",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// vote-B joins Solana (not DZ), so it produces validator_joined
	joinedEvents := findEventsByType(resp.Events, "validator_joined")
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		assert.NotEqual(t, "vote-A", details["vote_pubkey"], "on-DZ validator should be filtered out with off_dz filter")
	}
	// vote-B should be present
	found := false
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		if details["vote_pubkey"] == "vote-B" {
			found = true
		}
	}
	assert.True(t, found, "vote-B (off-DZ) should be in off_dz results")
}

func TestDZFilter_AttributionPassThrough(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "")

	// Validator on DZ at t1, switches off DZ at t2 -> produces left_dz
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "5.5.5.5") // now off DZ
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t1)
	insertGossipNodeHistory(t, "node-A", "5.5.5.5", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&dz_filter=on_dz",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	disconnected := findEventsByType(resp.Events, "left_dz")
	assert.GreaterOrEqual(t, len(disconnected), 1, "left_dz events should pass through on_dz filter")
}

func TestActionFilter_Added(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-A in history at t1, NOT in current (left)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)
	deleteCurrentVoteAccount(t, "vote-A", t2)

	// vote-B first appears at t2 (joined)
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "10.0.0.3", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "10.0.0.3")

	// Stable validator
	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t1)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&action=added",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Should only have _joined or _created or joined_dz events
	for _, e := range resp.Events {
		isAdded := e.EventType == "joined_dz" ||
			len(e.EventType) > 8 && e.EventType[len(e.EventType)-7:] == "_joined" ||
			len(e.EventType) > 8 && e.EventType[len(e.EventType)-8:] == "_created"
		assert.True(t, isAdded, "with action=added, got unexpected event type: %s", e.EventType)
	}

	// left events should NOT be present
	leftEvents := findEventsByType(resp.Events, "validator_left")
	assert.Len(t, leftEvents, 0, "validator_left should not appear with action=added")
	leftDZEvents := findEventsByType(resp.Events, "left_dz")
	assert.Len(t, leftDZEvents, 0, "left_dz should not appear with action=added")
}

func TestActionFilter_Removed(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// vote-A in history at t1, NOT in current (left)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)
	deleteCurrentVoteAccount(t, "vote-A", t2)

	// vote-B first appears at t2 (joined)
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "10.0.0.3", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "10.0.0.3")

	insertCurrentVoteAccount(t, "vote-stay", "node-stay", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-stay", "10.0.0.2")
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-stay", "node-stay", 200_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t1)
	insertGossipNodeHistory(t, "node-stay", "10.0.0.2", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&action=removed",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// validator_joined should NOT be present (these are Solana joins, not DZ)
	joinedEvents := findEventsByType(resp.Events, "validator_joined")
	assert.Len(t, joinedEvents, 0, "validator_joined should not appear with action=removed")

	// validator_left should be present (Solana leave)
	leftEvents := findEventsByType(resp.Events, "validator_left")
	assert.GreaterOrEqual(t, len(leftEvents), 1, "validator_left should appear with action=removed")
}

func TestActionFilter_Changed(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// Set up a DZ stake_changed event via attribution (validator on DZ with stake change)
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "")
	insertCurrentVoteAccount(t, "vote-A", "node-A", 80_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 80_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t1)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&action=changed",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// stake_changed from attribution should match "changed" filter
	stakeChanged := findEventsByType(resp.Events, "stake_changed")
	assert.GreaterOrEqual(t, len(stakeChanged), 1, "stake_changed should appear with action=changed")

	// stake_increased/decreased should NOT match "changed" (they match alerting/resolved)
	increased := findEventsByType(resp.Events, "stake_increased")
	decreased := findEventsByType(resp.Events, "stake_decreased")
	assert.Len(t, increased, 0, "stake_increased should not appear with action=changed")
	assert.Len(t, decreased, 0, "stake_decreased should not appear with action=changed")
}

func TestActionFilter_AlertingIncludesStakeIncrease(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// Stake increase above threshold
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 115_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t1)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 115_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "10.0.0.1")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&action=alerting",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	increased := findEventsByType(resp.Events, "stake_increased")
	assert.GreaterOrEqual(t, len(increased), 1, "stake_increased should appear with action=alerting")
}

func TestMinStakePct_FiltersValidators(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// Total stake in current: 1M SOL = 1_000_000 * 1e9 = 1_000_000_000_000_000
	// Validator A: 100k SOL (10%)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "10.0.0.1")

	// Validator B: 10k SOL (1%)
	insertVoteAccountHistory(t, "vote-B", "node-B", 10_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "10.0.0.2", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 10_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "10.0.0.2")

	// Remaining stake to make total = 1M SOL: 890k SOL
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 890_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "10.0.0.99")
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 890_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 890_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "10.0.0.99", t1)
	insertGossipNodeHistory(t, "node-rest", "10.0.0.99", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&min_stake_pct=5",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	joinedEvents := findEventsByType(resp.Events, "validator_joined")
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		vp := details["vote_pubkey"].(string)
		assert.NotEqual(t, "vote-B", vp, "vote-B (1%% stake) should be filtered out by min_stake_pct=5")
	}
}

func TestMinStakePct_NonValidatorPassThrough(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	ctx := t.Context()

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// Insert a device event via dim_dz_devices_history
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users) VALUES ('dev-entity-1', '%s', '%s', '%s', 0, 1, 'dev-1', 'pending', 'router', 'DEV-001', '10.0.0.1', '', '', 0)`,
		tsFormat(t1), tsFormat(t1), uuid.New().String())))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_dz_devices_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users) VALUES ('dev-entity-1', '%s', '%s', '%s', 0, 2, 'dev-1', 'activated', 'router', 'DEV-001', '10.0.0.1', '', '', 0)`,
		tsFormat(t2), tsFormat(t2), uuid.New().String())))

	// Small validator (1% stake)
	insertCurrentVoteAccount(t, "vote-small", "node-small", 10_000_000_000_000)
	insertCurrentGossipNode(t, "node-small", "10.0.0.3")
	insertVoteAccountHistory(t, "vote-small", "node-small", 10_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-small", "10.0.0.3", t2)

	// Remaining for total 1M SOL
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 990_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "10.0.0.99")
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 990_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 990_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "10.0.0.99", t1)
	insertGossipNodeHistory(t, "node-rest", "10.0.0.99", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&min_stake_pct=5",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Device events should pass through min_stake_pct filter
	hasDeviceEvent := false
	for _, e := range resp.Events {
		if e.EntityType == "device" {
			hasDeviceEvent = true
			break
		}
	}
	assert.True(t, hasDeviceEvent, "device events should pass through min_stake_pct filter")
}

// --- Integration / edge case tests ---

func TestDZTotal_OnAllEventTypes(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "device-A")

	// DZ validator (on DZ): exists at both t1 and t2
	insertVoteAccountHistory(t, "vote-dz", "node-dz", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-dz", "node-dz", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-dz", "1.2.3.4", t1)
	insertGossipNodeHistory(t, "node-dz", "1.2.3.4", t2)
	insertCurrentVoteAccount(t, "vote-dz", "node-dz", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-dz", "1.2.3.4")

	// Validator that joins at t2 (non-DZ)
	insertVoteAccountHistory(t, "vote-new", "node-new", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-new", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-new", "node-new", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-new", "9.9.9.9")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// All validator/gossip_node events should have dz_total_stake_share_pct > 0
	for _, e := range resp.Events {
		if e.EntityType == "validator" || e.EntityType == "gossip_node" {
			details := getDetails(t, e)
			dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
			if ok {
				assert.Greater(t, dzTotal, float64(0), "event %s should have dz_total_stake_share_pct > 0", e.EventType)
			}
		}
	}
}

func TestEdge_NoDZUsers(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// No DZ users at all

	// Validator joins
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "10.0.0.1", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "10.0.0.1")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Should still produce events, no crash
	joinedEvents := findEventsByType(resp.Events, "validator_joined")
	assert.GreaterOrEqual(t, len(joinedEvents), 1, "should produce validator_joined even without DZ users")
}

func TestEdge_ZeroStake(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// Validator with 0 stake
	insertVoteAccountHistory(t, "vote-zero", "node-zero", 0, t1)
	insertGossipNodeHistory(t, "node-zero", "10.0.0.1", t1)
	insertCurrentVoteAccount(t, "vote-zero", "node-zero", 0)
	insertCurrentGossipNode(t, "node-zero", "10.0.0.1")

	// Need another validator with stake so total > 0
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "10.0.0.2")
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", "10.0.0.2", t1)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	// Should not crash
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestCombinedFilters(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user
	insertDZUserCurrent(t, "dz-user-1", "1.2.3.4", "activated", "ownerAAA", "device-A")

	// Validator A on DZ, joins at t2, 100k SOL (10% of 1M)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", "1.2.3.4", t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", "1.2.3.4")

	// Validator B off DZ, joins at t2, 50k SOL (5%)
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9")

	// Validator C on DZ, joins at t2, 10k SOL (1% - below min_stake_pct=2)
	insertDZUserCurrent(t, "dz-user-2", "1.2.3.5", "activated", "ownerCCC", "device-C")
	insertVoteAccountHistory(t, "vote-C", "node-C", 10_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-C", "1.2.3.5", t2)
	insertCurrentVoteAccount(t, "vote-C", "node-C", 10_000_000_000_000)
	insertCurrentGossipNode(t, "node-C", "1.2.3.5")

	// Remaining stake to total 1M
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 840_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "10.0.0.99")
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 840_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 840_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "10.0.0.99", t1)
	insertGossipNodeHistory(t, "node-rest", "10.0.0.99", t2)

	// Validator D: in history at t1, NOT in current (left) - should be excluded by action=added
	insertVoteAccountHistory(t, "vote-D", "node-D", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-D", "1.2.3.4", t1)
	deleteCurrentVoteAccount(t, "vote-D", t2)
	deleteCurrentGossipNode(t, "node-D", t2)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change&dz_filter=on_dz&action=added&min_stake_pct=2&entity_type=validator",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Should only get vote-A: on DZ, joined (added), >= 2% stake, validator type
	// vote-B: off DZ (excluded by dz_filter)
	// vote-C: on DZ but 1% stake (excluded by min_stake_pct)
	// vote-D: left (excluded by action=added)
	for _, e := range resp.Events {
		assert.Equal(t, "validator", e.EntityType, "entity_type filter should restrict to validators")
		details := getDetails(t, e)
		vp, _ := details["vote_pubkey"].(string)
		assert.NotEqual(t, "vote-B", vp, "vote-B should be excluded by dz_filter=on_dz")
		assert.NotEqual(t, "vote-D", vp, "vote-D (left) should be excluded by action=added")
	}

	// Check that vote-A is present
	found := false
	for _, e := range resp.Events {
		if e.EventType == "joined_dz" {
			details := getDetails(t, e)
			if details["vote_pubkey"] == "vote-A" {
				found = true
			}
		}
	}
	assert.True(t, found, "vote-A (on DZ, joined, 10%% stake) should be in results")
}
