package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"strings"

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
		if e.EventType == "validator_left_dz" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 left_dz event")
	assert.Equal(t, "validator", attrEvents[0].EntityType)

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "validator_left_dz", details["action"])
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
		if e.EventType == "validator_joined_dz" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 joined_dz event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "validator_joined_dz", details["action"])
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
		if e.EventType == "validator_stake_changed" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 stake_changed event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "validator_stake_changed", details["action"])
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
		if e.EventType == "validator_left_dz" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 left_dz event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "validator_left_dz", details["action"])
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
		if e.EventType == "validator_left_dz" || e.EventType == "validator_joined_dz" || e.EventType == "validator_stake_changed" {
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
		if e.EventType == "validator_joined_solana" {
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

// TestDZTotalBackfillWalk_MultipleSnapshots tests that the DZ total stake share
// percentage walks back correctly across multiple attribution events over several
// days. This is the scenario that was broken in production: the total drifted from
// ~39% down to ~36% because the old dedup removed legitimate events.
func TestDZTotalBackfillWalk_MultipleSnapshots(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	// 4 timestamps over 3 days — each transition changes DZ composition
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)
	t4 := time.Date(2025, 1, 4, 0, 0, 0, 0, time.UTC)

	dzIP := "1.2.3.4"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// Two validators: A (always on DZ, 100k SOL) and B (joins DZ at t2, changes stake at t3, leaves at t4)
	// Total network stake: A(100k) + B(50k) + rest(850k) = 1M SOL

	// Current tables reflect final state (t4): A on DZ, B NOT on DZ
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", dzIP)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 80_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9") // B left DZ at t4
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 820_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	// T1: A on DZ (100k), B NOT on DZ (50k)
	// DZ total = 100k / 1M = 10%
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", dzIP, t1)
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 850_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t1)

	// T2: A on DZ (100k), B JOINS DZ (50k)
	// DZ total = 150k / 1M = 15%
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", dzIP, t2)
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", dzIP, t2) // joins DZ
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 850_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t2)

	// T3: A on DZ (100k), B on DZ, stake increased to 80k
	// DZ total = 180k / 1M = 18%
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t3)
	insertGossipNodeHistory(t, "node-A", dzIP, t3)
	insertVoteAccountHistory(t, "vote-B", "node-B", 80_000_000_000_000, t3)
	insertGossipNodeHistory(t, "node-B", dzIP, t3)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 820_000_000_000_000, t3)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t3)

	// T4: A on DZ (100k), B LEAVES DZ (80k)
	// DZ total = 100k / 1M = 10%
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t4)
	insertGossipNodeHistory(t, "node-A", dzIP, t4)
	insertVoteAccountHistory(t, "vote-B", "node-B", 80_000_000_000_000, t4)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t4) // leaves DZ
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 820_000_000_000_000, t4)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t4)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t4.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Collect all attribution events (they have contribution_change_lamports)
	var attrEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		details := getDetailsOptional(e)
		if details == nil {
			continue
		}
		if ccl, ok := details["contribution_change_lamports"].(float64); ok && ccl != 0 {
			attrEvents = append(attrEvents, e)
		}
	}
	t.Logf("Attribution events: %d", len(attrEvents))
	for _, e := range attrEvents {
		d := getDetailsOptional(e)
		t.Logf("  %s  type=%s  ccl=%.0f  dz_total=%.2f%%  vote=%s",
			e.Timestamp, e.EventType,
			d["contribution_change_lamports"],
			d["dz_total_stake_share_pct"],
			d["vote_pubkey"])
	}

	// We expect 3 attribution events for validator B:
	// t2: joined DZ (+50k), t3: stake changed (+30k), t4: left DZ (-80k)
	require.GreaterOrEqual(t, len(attrEvents), 3, "expected at least 3 attribution events for validator B")

	// Key assertion: ALL events should have dz_total_stake_share_pct > 5%.
	// The bug was that backfill drift caused old events to show ~0% or ~36%
	// when they should have been around 10-18%.
	for _, e := range resp.Events {
		details := getDetailsOptional(e)
		if details == nil {
			continue
		}
		dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
		if !ok || dzTotal == 0 {
			continue
		}
		assert.Greater(t, dzTotal, 5.0,
			"DZ total at %s (%s) should be > 5%% (was %.2f%%)", e.Timestamp, e.EventType, dzTotal)
		assert.Less(t, dzTotal, 25.0,
			"DZ total at %s (%s) should be < 25%% (was %.2f%%)", e.Timestamp, e.EventType, dzTotal)
	}
}

// TestDZTotalBackfillWalk_StableTotal tests that when no attribution events exist
// (DZ composition doesn't change), all events show the same DZ total.
func TestDZTotalBackfillWalk_StableTotal(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	dzIP := "1.2.3.4"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// Validator A always on DZ, same stake at all timestamps
	for _, ts := range []time.Time{t1, t2, t3} {
		insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, ts)
		insertGossipNodeHistory(t, "node-A", dzIP, ts)
		insertVoteAccountHistory(t, "vote-rest", "node-rest", 100_000_000_000_000, ts)
		insertGossipNodeHistory(t, "node-rest", "8.8.8.8", ts)
	}
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", dzIP)
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	// Validator B joins Solana at t2 (NOT on DZ) — generates a non-attribution event
	insertVoteAccountHistory(t, "vote-B", "node-B", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9")

	// Validator C joins Solana at t3 (NOT on DZ)
	insertVoteAccountHistory(t, "vote-C", "node-C", 30_000_000_000_000, t3)
	insertGossipNodeHistory(t, "node-C", "7.7.7.7", t3)
	insertCurrentVoteAccount(t, "vote-C", "node-C", 30_000_000_000_000)
	insertCurrentGossipNode(t, "node-C", "7.7.7.7")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t3.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// All events with dz_total should show the same value since DZ composition
	// didn't change. Current DZ total = 100k / (100k + 100k + 50k + 30k) ≈ 35.7%
	var dzTotals []float64
	for _, e := range resp.Events {
		details := getDetailsOptional(e)
		if details == nil {
			continue
		}
		dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
		if !ok || dzTotal == 0 {
			continue
		}
		dzTotals = append(dzTotals, dzTotal)
	}
	require.NotEmpty(t, dzTotals, "expected some events with dz_total_stake_share_pct")

	// All should be the same value (no drift)
	for i, pct := range dzTotals {
		assert.InDelta(t, dzTotals[0], pct, 0.01,
			"DZ total[%d] = %.2f%% should equal DZ total[0] = %.2f%% (no drift)", i, pct, dzTotals[0])
	}
}

// TestDedup_SameValidatorDifferentTimestamps tests that the dedup logic
// does NOT remove events for the same validator at different timestamps.
// This was a bug: the old dedup keyed on (vote_pubkey, event_type) globally,
// removing legitimate stake change events at different times.
func TestDedup_SameValidatorDifferentTimestamps(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC)

	dzIP := "1.2.3.4"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// Validator A on DZ with changing stake across 3 snapshots
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", dzIP, t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 120_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", dzIP, t2)
	insertVoteAccountHistory(t, "vote-A", "node-A", 80_000_000_000_000, t3)
	insertGossipNodeHistory(t, "node-A", dzIP, t3)

	insertCurrentVoteAccount(t, "vote-A", "node-A", 80_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", dzIP)

	// Need another validator so total stake > 0
	for _, ts := range []time.Time{t1, t2, t3} {
		insertVoteAccountHistory(t, "vote-rest", "node-rest", 900_000_000_000_000, ts)
		insertGossipNodeHistory(t, "node-rest", "8.8.8.8", ts)
	}
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 920_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t3.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Count validator_stake_changed events for vote-A
	var stakeChangedCount int
	for _, e := range resp.Events {
		if e.EventType != "validator_stake_changed" {
			continue
		}
		details := getDetailsOptional(e)
		if details == nil {
			continue
		}
		if details["vote_pubkey"] == "vote-A" {
			stakeChangedCount++
			t.Logf("stake_changed at %s: ccl=%.0f", e.Timestamp, details["contribution_change_lamports"])
		}
	}
	// Should have 2 stake_changed events: t1→t2 (+20k) and t2→t3 (-40k)
	assert.Equal(t, 2, stakeChangedCount,
		"same validator should have 2 stake_changed events at different timestamps (old dedup bug would leave only 1)")
}

// TestTimeline_FullResponse_JoinLeaveSequence tests the complete API response
// for a validator that joins DZ, changes stake, and leaves DZ. Asserts exact
// event types, ordering, field values, and DZ total consistency.
func TestTimeline_FullResponse_JoinLeaveSequence(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 3, 1, 6, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)
	t4 := time.Date(2025, 3, 1, 18, 0, 0, 0, time.UTC)

	dzIP := "10.0.0.1"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// "rest" validator provides background stake (always off DZ, stable)
	// "base" validator always on DZ
	// "target" validator: off DZ at t1, joins at t2, stake changes at t3, leaves at t4

	const restStake int64 = 700_000_000_000_000    // 700k SOL
	const baseStake int64 = 200_000_000_000_000    // 200k SOL
	const targetStake1 int64 = 100_000_000_000_000 // 100k SOL
	const targetStake2 int64 = 150_000_000_000_000 // 150k SOL
	// Total = 1M SOL at t1-t3, 1.05M at t4 (slight variation)

	// Current tables (final state: base on DZ, target off DZ)
	insertCurrentVoteAccount(t, "vote-base", "node-base", baseStake)
	insertCurrentGossipNode(t, "node-base", dzIP)
	insertCurrentVoteAccount(t, "vote-target", "node-target", targetStake2)
	insertCurrentGossipNode(t, "node-target", "9.9.9.9")
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", restStake)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	// T1: base on DZ (200k), target off DZ (100k), rest (700k)
	for _, v := range []struct {
		vote, node string
		stake      int64
		ip         string
	}{
		{"vote-base", "node-base", baseStake, dzIP},
		{"vote-target", "node-target", targetStake1, "9.9.9.9"},
		{"vote-rest", "node-rest", restStake, "8.8.8.8"},
	} {
		insertVoteAccountHistory(t, v.vote, v.node, v.stake, t1)
		insertGossipNodeHistory(t, v.node, v.ip, t1)
	}

	// T2: target joins DZ
	insertVoteAccountHistory(t, "vote-base", "node-base", baseStake, t2)
	insertGossipNodeHistory(t, "node-base", dzIP, t2)
	insertVoteAccountHistory(t, "vote-target", "node-target", targetStake1, t2)
	insertGossipNodeHistory(t, "node-target", dzIP, t2) // joins DZ
	insertVoteAccountHistory(t, "vote-rest", "node-rest", restStake, t2)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t2)

	// T3: target stake increases while on DZ
	insertVoteAccountHistory(t, "vote-base", "node-base", baseStake, t3)
	insertGossipNodeHistory(t, "node-base", dzIP, t3)
	insertVoteAccountHistory(t, "vote-target", "node-target", targetStake2, t3)
	insertGossipNodeHistory(t, "node-target", dzIP, t3) // still on DZ, more stake
	insertVoteAccountHistory(t, "vote-rest", "node-rest", restStake, t3)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t3)

	// T4: target leaves DZ
	insertVoteAccountHistory(t, "vote-base", "node-base", baseStake, t4)
	insertGossipNodeHistory(t, "node-base", dzIP, t4)
	insertVoteAccountHistory(t, "vote-target", "node-target", targetStake2, t4)
	insertGossipNodeHistory(t, "node-target", "9.9.9.9", t4) // leaves DZ
	insertVoteAccountHistory(t, "vote-rest", "node-rest", restStake, t4)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t4)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t4.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Response metadata
	assert.Equal(t, 500, resp.Limit)
	assert.Equal(t, 0, resp.Offset)
	assert.NotEmpty(t, resp.TimeRange.Start)
	assert.NotEmpty(t, resp.TimeRange.End)

	// Extract attribution events for vote-target, sorted newest first (API default)
	type eventSummary struct {
		eventType string
		timestamp string
		details   map[string]any
	}
	var targetEvents []eventSummary
	for _, e := range resp.Events {
		d := getDetailsOptional(e)
		if d == nil {
			continue
		}
		vp, _ := d["vote_pubkey"].(string)
		if vp != "vote-target" {
			continue
		}
		targetEvents = append(targetEvents, eventSummary{e.EventType, e.Timestamp, d})
	}

	// Log all events for debugging
	for _, e := range targetEvents {
		t.Logf("vote-target: %s  type=%-30s  ccl=%v  dz_total=%.2f%%",
			e.timestamp, e.eventType,
			e.details["contribution_change_lamports"],
			e.details["dz_total_stake_share_pct"])
	}

	// Multiple query functions can produce events for the same validator:
	// - queryDZStakeAttribution → validator_joined_dz, validator_stake_changed, validator_left_dz
	// - queryVoteAccountChanges → validator_joined_solana (if new to Solana)
	// - queryStakeChanges → validator_stake_increased/decreased
	findByType := func(eventType string) *eventSummary {
		for i, e := range targetEvents {
			if e.eventType == eventType {
				return &targetEvents[i]
			}
		}
		return nil
	}

	// Core attribution events must exist
	leftEvent := findByType("validator_left_dz")
	require.NotNil(t, leftEvent, "should have validator_left_dz at t4")
	assert.Equal(t, t4.Format(time.RFC3339), leftEvent.timestamp)
	assert.Equal(t, "validator_left_dz", leftEvent.details["action"])
	assert.Equal(t, "vote-target", leftEvent.details["vote_pubkey"])
	assert.Equal(t, "node-target", leftEvent.details["node_pubkey"])
	assert.Equal(t, "validator", leftEvent.details["kind"])
	cclLeft, _ := leftEvent.details["contribution_change_lamports"].(float64)
	assert.Less(t, cclLeft, float64(0), "left DZ should have negative contribution")

	stakeEvent := findByType("validator_stake_changed")
	require.NotNil(t, stakeEvent, "should have validator_stake_changed at t3")
	assert.Equal(t, t3.Format(time.RFC3339), stakeEvent.timestamp)
	cclStake, _ := stakeEvent.details["contribution_change_lamports"].(float64)
	assert.Equal(t, float64(targetStake2-targetStake1), cclStake,
		"stake change = 150k - 100k = 50k SOL in lamports")

	joinEvent := findByType("validator_joined_dz")
	require.NotNil(t, joinEvent, "should have validator_joined_dz at t2")
	assert.Equal(t, t2.Format(time.RFC3339), joinEvent.timestamp)
	assert.Equal(t, "validator_joined_dz", joinEvent.details["action"])
	cclJoin, _ := joinEvent.details["contribution_change_lamports"].(float64)
	assert.Equal(t, float64(targetStake1), cclJoin, "joining DZ adds full stake")

	// DZ total assertions — verify the backfill walk produces correct values.
	// Current DZ total: base(200k) / (200k + 150k + 700k) = 200k/1050k ≈ 19.05%
	// After t4 (left): same as current ≈ 19.05%
	// After t3 (stake changed): base(200k) + target(150k) = 350k/1050k ≈ 33.33%
	// After t2 (joined): base(200k) + target(100k) = 300k/1050k ≈ 28.57%
	dzLeft, _ := leftEvent.details["dz_total_stake_share_pct"].(float64)
	dzStake, _ := stakeEvent.details["dz_total_stake_share_pct"].(float64)
	dzJoin, _ := joinEvent.details["dz_total_stake_share_pct"].(float64)

	assert.InDelta(t, 19.05, dzLeft, 1.0, "DZ total after left should be ~19%%")
	assert.InDelta(t, 33.33, dzStake, 1.0, "DZ total after stake change should be ~33%%")
	assert.InDelta(t, 28.57, dzJoin, 1.5, "DZ total after join should be ~29%%")

	// DZ totals should reflect the changes correctly
	assert.Less(t, dzLeft, dzJoin, "t4 total < t2 total (target left)")
	assert.Less(t, dzJoin, dzStake, "t2 total < t3 total (stake increased)")
}

// TestTimeline_FullResponse_DZFilter tests that the dz_filter=on_dz parameter
// correctly filters events to only DZ-related validators.
func TestTimeline_FullResponse_DZFilter(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 4, 1, 6, 0, 0, 0, time.UTC)

	dzIP := "10.0.0.1"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "owner-1", "device-1")

	// Validator A: on DZ, joins at t2
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", dzIP, t2)
	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", dzIP)

	// Validator B: NOT on DZ, joins at t2
	insertVoteAccountHistory(t, "vote-B", "node-B", 200_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-B", "9.9.9.9", t2)
	insertCurrentVoteAccount(t, "vote-B", "node-B", 200_000_000_000_000)
	insertCurrentGossipNode(t, "node-B", "9.9.9.9")

	// Background validator at t1
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 700_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 700_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t2)
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 700_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	// Without DZ filter: both A and B should appear
	reqAll := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rrAll := httptest.NewRecorder()
	handlers.GetTimeline(rrAll, reqAll)
	require.Equal(t, http.StatusOK, rrAll.Code)

	var respAll handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rrAll.Body).Decode(&respAll))

	allVotePubkeys := make(map[string]bool)
	for _, e := range respAll.Events {
		d := getDetailsOptional(e)
		if d == nil {
			continue
		}
		if vp, ok := d["vote_pubkey"].(string); ok && vp != "" {
			allVotePubkeys[vp] = true
		}
	}
	assert.True(t, allVotePubkeys["vote-A"], "vote-A should appear without DZ filter")
	assert.True(t, allVotePubkeys["vote-B"], "vote-B should appear without DZ filter")

	// With DZ filter: only A should appear
	reqDZ := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&dz_filter=on_dz&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rrDZ := httptest.NewRecorder()
	handlers.GetTimeline(rrDZ, reqDZ)
	require.Equal(t, http.StatusOK, rrDZ.Code)

	var respDZ handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rrDZ.Body).Decode(&respDZ))

	for _, e := range respDZ.Events {
		d := getDetailsOptional(e)
		if d == nil {
			continue
		}
		vp, _ := d["vote_pubkey"].(string)
		assert.NotEqual(t, "vote-B", vp, "vote-B (off DZ) should be excluded by dz_filter=on_dz")
	}

	// vote-A should appear as validator_joined_dz
	foundA := false
	for _, e := range respDZ.Events {
		if e.EventType == "validator_joined_dz" {
			d := getDetailsOptional(e)
			if d != nil && d["vote_pubkey"] == "vote-A" {
				foundA = true
				// Verify DZ metadata is populated
				assert.NotEmpty(t, d["dz_ip"], "DZ validator should have dz_ip")
				dzTotal, ok := d["dz_total_stake_share_pct"].(float64)
				assert.True(t, ok && dzTotal > 0, "should have dz_total_stake_share_pct > 0")
			}
		}
	}
	assert.True(t, foundA, "vote-A should appear as validator_joined_dz with dz_filter=on_dz")
}

// TestTimeline_FullResponse_MinStakeFilter tests that min_stake_pct correctly
// excludes low-stake validators from results.
func TestTimeline_FullResponse_MinStakeFilter(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 5, 1, 6, 0, 0, 0, time.UTC)

	dzIP := "10.0.0.1"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// Validator big: 5% of total stake (50k / 1M)
	insertVoteAccountHistory(t, "vote-big", "node-big", 50_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-big", dzIP, t2)
	insertCurrentVoteAccount(t, "vote-big", "node-big", 50_000_000_000_000)
	insertCurrentGossipNode(t, "node-big", dzIP)

	// Validator small: 0.1% of total stake (1k / 1M)
	insertVoteAccountHistory(t, "vote-small", "node-small", 1_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-small", dzIP, t2)
	insertCurrentVoteAccount(t, "vote-small", "node-small", 1_000_000_000_000)
	insertCurrentGossipNode(t, "node-small", dzIP)

	// Background to make total ~1M
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 949_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 949_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t2)
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 949_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	// min_stake_pct=1 should exclude vote-small (0.1%) but include vote-big (5%)
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&min_stake_pct=1&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	foundBig := false
	for _, e := range resp.Events {
		d := getDetailsOptional(e)
		if d == nil {
			continue
		}
		vp, _ := d["vote_pubkey"].(string)
		assert.NotEqual(t, "vote-small", vp,
			"vote-small (0.1%% stake) should be excluded by min_stake_pct=1")
		if vp == "vote-big" {
			foundBig = true
			ssp, _ := d["stake_share_pct"].(float64)
			assert.InDelta(t, 5.0, ssp, 0.5, "vote-big stake_share_pct should be ~5%%")
		}
	}
	assert.True(t, foundBig, "vote-big (5%% stake) should be included with min_stake_pct=1")
}

// TestTimeline_FullResponse_Pagination tests that limit and offset work correctly.
func TestTimeline_FullResponse_Pagination(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	dzIP := "10.0.0.1"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// Create 5 validators that join at different times
	baseTime := t1
	for i := 0; i < 5; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Hour)
		vote := fmt.Sprintf("vote-%d", i)
		node := fmt.Sprintf("node-%d", i)
		insertVoteAccountHistory(t, vote, node, int64((i+1)*10_000_000_000_000), ts)
		insertGossipNodeHistory(t, node, dzIP, ts)
		insertCurrentVoteAccount(t, vote, node, int64((i+1)*10_000_000_000_000))
		insertCurrentGossipNode(t, node, dzIP)
	}
	// Background stake
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 900_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 900_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t1)

	endTime := baseTime.Add(5 * time.Hour)

	// Get all events
	reqAll := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		endTime.Format(time.RFC3339)), nil)
	rrAll := httptest.NewRecorder()
	handlers.GetTimeline(rrAll, reqAll)
	require.Equal(t, http.StatusOK, rrAll.Code)

	var respAll handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rrAll.Body).Decode(&respAll))
	totalEvents := respAll.Total

	if totalEvents < 3 {
		t.Skipf("Not enough events to test pagination (got %d)", totalEvents)
	}

	// Page 1: limit=2, offset=0
	req1 := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&limit=2&offset=0",
		t1.Add(-time.Minute).Format(time.RFC3339),
		endTime.Format(time.RFC3339)), nil)
	rr1 := httptest.NewRecorder()
	handlers.GetTimeline(rr1, req1)
	require.Equal(t, http.StatusOK, rr1.Code)

	var resp1 handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr1.Body).Decode(&resp1))
	assert.Equal(t, 2, resp1.Limit)
	assert.Equal(t, 0, resp1.Offset)
	assert.Equal(t, totalEvents, resp1.Total, "total should be same across pages")
	assert.Len(t, resp1.Events, 2, "page 1 should have 2 events")

	// Page 2: limit=2, offset=2
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&limit=2&offset=2",
		t1.Add(-time.Minute).Format(time.RFC3339),
		endTime.Format(time.RFC3339)), nil)
	rr2 := httptest.NewRecorder()
	handlers.GetTimeline(rr2, req2)
	require.Equal(t, http.StatusOK, rr2.Code)

	var resp2 handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr2.Body).Decode(&resp2))
	assert.Equal(t, 2, resp2.Limit)
	assert.Equal(t, 2, resp2.Offset)
	assert.Len(t, resp2.Events, 2, "page 2 should have 2 events")

	// Events across pages should not overlap
	page1IDs := make(map[string]bool)
	for _, e := range resp1.Events {
		page1IDs[e.ID] = true
	}
	for _, e := range resp2.Events {
		assert.False(t, page1IDs[e.ID], "page 2 event %s should not appear on page 1", e.ID)
	}

	// Events should be sorted newest first within each page
	if len(resp1.Events) >= 2 {
		assert.GreaterOrEqual(t, resp1.Events[0].Timestamp, resp1.Events[1].Timestamp,
			"events should be sorted newest first")
	}
}

// TestTimeline_FullResponse_EventFields tests that all required fields are
// present and correctly typed on validator events.
func TestTimeline_FullResponse_EventFields(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 7, 1, 6, 0, 0, 0, time.UTC)

	dzIP := "10.0.0.1"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "owner-pub-1", "device-pk-1")

	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-A", "9.9.9.9", t1)
	insertVoteAccountHistory(t, "vote-A", "node-A", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-A", dzIP, t2) // joins DZ at t2

	insertCurrentVoteAccount(t, "vote-A", "node-A", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-A", dzIP)

	// Background
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 900_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t1)
	insertVoteAccountHistory(t, "vote-rest", "node-rest", 900_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-rest", "8.8.8.8", t2)
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 900_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/api/timeline?start=%s&end=%s&category=state_change&entity_type=validator&limit=500",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Find the validator_joined_dz event
	var joinEvent *handlers.TimelineEvent
	for i, e := range resp.Events {
		if e.EventType == "validator_joined_dz" {
			joinEvent = &resp.Events[i]
			break
		}
	}
	require.NotNil(t, joinEvent, "should have a validator_joined_dz event")

	// Top-level fields
	assert.NotEmpty(t, joinEvent.ID, "id must be set")
	assert.Equal(t, "validator_joined_dz", joinEvent.EventType)
	assert.Equal(t, t2.Format(time.RFC3339), joinEvent.Timestamp)
	assert.Equal(t, "state_change", joinEvent.Category)
	assert.Equal(t, "info", joinEvent.Severity)
	assert.NotEmpty(t, joinEvent.Title)
	assert.Equal(t, "validator", joinEvent.EntityType)
	assert.Equal(t, "vote-A", joinEvent.EntityPK)
	assert.Equal(t, "vote-A", joinEvent.EntityCode)

	// Detail fields
	d := getDetailsOptional(*joinEvent)
	require.NotNil(t, d, "details should be present")

	assert.Equal(t, "vote-A", d["vote_pubkey"])
	assert.Equal(t, "node-A", d["node_pubkey"])
	assert.Equal(t, "validator_joined_dz", d["action"])
	assert.Equal(t, "validator", d["kind"])
	assert.Equal(t, dzIP, d["dz_ip"])

	// Numeric fields should be present and reasonable
	stakeLamports, ok := d["stake_lamports"].(float64) // JSON numbers are float64
	assert.True(t, ok, "stake_lamports should be a number")
	assert.Equal(t, float64(100_000_000_000_000), stakeLamports)

	stakeSol, ok := d["stake_sol"].(float64)
	assert.True(t, ok, "stake_sol should be a number")
	assert.InDelta(t, 100_000.0, stakeSol, 1.0)

	stakeSharePct, ok := d["stake_share_pct"].(float64)
	assert.True(t, ok, "stake_share_pct should be a number")
	assert.InDelta(t, 10.0, stakeSharePct, 1.0, "100k/1M = 10%%")

	dzTotal, ok := d["dz_total_stake_share_pct"].(float64)
	assert.True(t, ok, "dz_total_stake_share_pct should be a number")
	assert.Greater(t, dzTotal, 0.0)

	cclRaw, ok := d["contribution_change_lamports"].(float64)
	assert.True(t, ok, "contribution_change_lamports should be present on attribution events")
	assert.Equal(t, float64(100_000_000_000_000), cclRaw, "joining DZ adds full 100k SOL")
}

// TestTimeline_FullResponse_ActionFilter tests the action filter with exact
// expected outputs for added/removed/changed actions.
func TestTimeline_FullResponse_ActionFilter(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 8, 1, 6, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 8, 1, 12, 0, 0, 0, time.UTC)

	dzIP := "10.0.0.1"
	insertDZUserCurrent(t, "dz-user-1", dzIP, "activated", "", "")

	// Validator A: joins DZ at t2 (action=added)
	insertVoteAccountHistory(t, "vote-joiner", "node-joiner", 100_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-joiner", "9.9.9.9", t1)
	insertVoteAccountHistory(t, "vote-joiner", "node-joiner", 100_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-joiner", dzIP, t2)
	insertCurrentVoteAccount(t, "vote-joiner", "node-joiner", 100_000_000_000_000)
	insertCurrentGossipNode(t, "node-joiner", dzIP)

	// Validator B: leaves DZ at t3 (action=removed)
	insertVoteAccountHistory(t, "vote-leaver", "node-leaver", 80_000_000_000_000, t1)
	insertGossipNodeHistory(t, "node-leaver", dzIP, t1)
	insertVoteAccountHistory(t, "vote-leaver", "node-leaver", 80_000_000_000_000, t2)
	insertGossipNodeHistory(t, "node-leaver", dzIP, t2)
	insertVoteAccountHistory(t, "vote-leaver", "node-leaver", 80_000_000_000_000, t3)
	insertGossipNodeHistory(t, "node-leaver", "9.9.9.9", t3)
	insertCurrentVoteAccount(t, "vote-leaver", "node-leaver", 80_000_000_000_000)
	insertCurrentGossipNode(t, "node-leaver", "9.9.9.9")

	// Background
	for _, ts := range []time.Time{t1, t2, t3} {
		insertVoteAccountHistory(t, "vote-rest", "node-rest", 820_000_000_000_000, ts)
		insertGossipNodeHistory(t, "node-rest", "8.8.8.8", ts)
	}
	insertCurrentVoteAccount(t, "vote-rest", "node-rest", 820_000_000_000_000)
	insertCurrentGossipNode(t, "node-rest", "8.8.8.8")

	timeRange := fmt.Sprintf("start=%s&end=%s",
		t1.Add(-time.Minute).Format(time.RFC3339),
		t3.Add(time.Minute).Format(time.RFC3339))

	// action=added: should only get join events
	reqAdded := httptest.NewRequest(http.MethodGet,
		"/api/timeline?"+timeRange+"&category=state_change&entity_type=validator&action=added&limit=500", nil)
	rrAdded := httptest.NewRecorder()
	handlers.GetTimeline(rrAdded, reqAdded)
	require.Equal(t, http.StatusOK, rrAdded.Code)

	var respAdded handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rrAdded.Body).Decode(&respAdded))

	for _, e := range respAdded.Events {
		assert.True(t,
			strings.Contains(e.EventType, "_joined") || strings.Contains(e.EventType, "_created"),
			"action=added should only return joined/created events, got %s", e.EventType)
	}

	// action=removed: should only get left events
	reqRemoved := httptest.NewRequest(http.MethodGet,
		"/api/timeline?"+timeRange+"&category=state_change&entity_type=validator&action=removed&limit=500", nil)
	rrRemoved := httptest.NewRecorder()
	handlers.GetTimeline(rrRemoved, reqRemoved)
	require.Equal(t, http.StatusOK, rrRemoved.Code)

	var respRemoved handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rrRemoved.Body).Decode(&respRemoved))

	for _, e := range respRemoved.Events {
		assert.True(t,
			strings.Contains(e.EventType, "_left") || strings.Contains(e.EventType, "_deleted"),
			"action=removed should only return left/deleted events, got %s", e.EventType)
	}

	// Verify the specific validators appear in the right filter
	foundJoinerInAdded := false
	for _, e := range respAdded.Events {
		d := getDetailsOptional(e)
		if d != nil && d["vote_pubkey"] == "vote-joiner" {
			foundJoinerInAdded = true
		}
	}
	assert.True(t, foundJoinerInAdded, "vote-joiner should appear in action=added results")

	foundLeaverInRemoved := false
	for _, e := range respRemoved.Events {
		d := getDetailsOptional(e)
		if d != nil && d["vote_pubkey"] == "vote-leaver" {
			foundLeaverInRemoved = true
		}
	}
	assert.True(t, foundLeaverInRemoved, "vote-leaver should appear in action=removed results")
}

// getDetailsOptional returns event details as map or nil.
func getDetailsOptional(e handlers.TimelineEvent) map[string]any {
	details, ok := e.Details.(map[string]any)
	if !ok {
		return nil
	}
	return details
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

func insertDZUserHistoryDeleted(t *testing.T, pk, entityID, ownerPubkey, dzIP, devicePK, status string, ts time.Time) {
	attrsHash := uint64(ts.UnixMilli())
	require.NoError(t, config.DB.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO dim_dz_users_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id) VALUES ('%s', '%s', '%s', '%s', 1, %d, '%s', '%s', '%s', '', '', '%s', '%s', 0)`,
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

	leftEvents := findEventsByType(resp.Events, "validator_left_solana")
	require.Len(t, leftEvents, 1, "expected 1 validator_left event")
	details := getDetails(t, leftEvents[0])
	assert.Equal(t, "validator_left_solana", details["action"])
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

	leftEvents := findEventsByType(resp.Events, "validator_left_solana")
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
	joinedEvents := findEventsByType(resp.Events, "validator_joined_solana")
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
	solanaJoinedEvents := findEventsByType(resp.Events, "validator_joined_solana")
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

	offlineEvents := findEventsByType(resp.Events, "validator_left_solana")
	require.Len(t, offlineEvents, 1, "expected 1 validator_left_solana event")
	details := getDetails(t, offlineEvents[0])
	assert.Equal(t, "left_solana", details["action"])
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

	offlineEvents := findEventsByType(resp.Events, "gossip_node_left_solana")
	require.Len(t, offlineEvents, 1, "expected 1 gossip_node_offline event")
	details := getDetails(t, offlineEvents[0])
	assert.Equal(t, "left_solana", details["action"])
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

	increased := findEventsByType(resp.Events, "validator_stake_increased")
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

	decreased := findEventsByType(resp.Events, "validator_stake_decreased")
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

	increased := findEventsByType(resp.Events, "validator_stake_increased")
	decreased := findEventsByType(resp.Events, "validator_stake_decreased")
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

	increased := findEventsByType(resp.Events, "validator_stake_increased")
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

	increased := findEventsByType(resp.Events, "validator_stake_increased")
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
	joinedEvents := findEventsByType(resp.Events, "validator_joined_dz")
	found := false
	for _, e := range joinedEvents {
		details := getDetails(t, e)
		if details["action"] == "validator_joined_dz" && details["kind"] == "validator" {
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

	leftEvents := findEventsByType(resp.Events, "validator_left_dz")
	found := false
	for _, e := range leftEvents {
		details := getDetails(t, e)
		if details["action"] == "validator_left_dz" && details["kind"] == "validator" {
			found = true
			assert.Equal(t, "warning", e.Severity)
			break
		}
	}
	assert.True(t, found, "expected a left_dz event with action=left_dz and kind=validator")
}

func TestValidatorEvents_LeftDZ_DeletedWhileActivated(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	t1 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 1, 0, 0, 0, time.UTC)

	// DZ user: activated at t1, then deleted (is_deleted=1) at t2 while still "activated"
	insertDZUserHistory(t, "user-DEL", "entity-DEL", "ownerDEL", "1.2.3.50", "device-DEL", "activated", t1)
	insertDZUserHistoryDeleted(t, "user-DEL", "entity-DEL", "ownerDEL", "1.2.3.50", "device-DEL", "activated", t2)

	// Gossip node with vote account
	insertGossipNodeHistory(t, "node-DEL", "1.2.3.50", t1)
	insertGossipNodeHistory(t, "node-DEL", "1.2.3.50", t2)
	insertCurrentGossipNode(t, "node-DEL", "1.2.3.50")
	insertVoteAccountHistory(t, "vote-DEL", "node-DEL", 100_000_000_000_000, t1)
	insertVoteAccountHistory(t, "vote-DEL", "node-DEL", 100_000_000_000_000, t2)
	insertCurrentVoteAccount(t, "vote-DEL", "node-DEL", 100_000_000_000_000)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	leftEvents := findEventsByType(resp.Events, "validator_left_dz")
	found := false
	for _, e := range leftEvents {
		details := getDetails(t, e)
		if details["action"] == "validator_left_dz" && details["kind"] == "validator" {
			found = true
			assert.Equal(t, "warning", e.Severity)
			break
		}
	}
	assert.True(t, found, "expected a left_dz event when user is deleted while still activated")
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

	joinedEvents := findEventsByType(resp.Events, "gossip_node_joined_dz")
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

	leftEvents := findEventsByType(resp.Events, "gossip_node_left_dz")
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

	joinedEvents := findEventsByType(resp.Events, "validator_joined_dz")
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
	joinedEvents := findEventsByType(resp.Events, "validator_joined_solana")
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

	disconnected := findEventsByType(resp.Events, "validator_left_dz")
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

	// Should only have _joined or _created events
	for _, e := range resp.Events {
		isAdded := strings.Contains(e.EventType, "_joined") || strings.Contains(e.EventType, "_created")
		assert.True(t, isAdded, "with action=added, got unexpected event type: %s", e.EventType)
	}

	// left events should NOT be present
	leftEvents := findEventsByType(resp.Events, "validator_left_solana")
	assert.Len(t, leftEvents, 0, "validator_left should not appear with action=added")
	leftDZEvents := findEventsByType(resp.Events, "validator_left_dz")
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
	joinedEvents := findEventsByType(resp.Events, "validator_joined_solana")
	assert.Len(t, joinedEvents, 0, "validator_joined should not appear with action=removed")

	// validator_left should be present (Solana leave)
	leftEvents := findEventsByType(resp.Events, "validator_left_solana")
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
	stakeChanged := findEventsByType(resp.Events, "validator_stake_changed")
	assert.GreaterOrEqual(t, len(stakeChanged), 1, "stake_changed should appear with action=changed")

	// stake_increased/decreased should NOT match "changed" (they match alerting/resolved)
	increased := findEventsByType(resp.Events, "validator_stake_increased")
	decreased := findEventsByType(resp.Events, "validator_stake_decreased")
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

	increased := findEventsByType(resp.Events, "validator_stake_increased")
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

	joinedEvents := findEventsByType(resp.Events, "validator_joined_solana")
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
	joinedEvents := findEventsByType(resp.Events, "validator_joined_solana")
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
		if e.EventType == "validator_joined_dz" {
			details := getDetails(t, e)
			if details["vote_pubkey"] == "vote-A" {
				found = true
			}
		}
	}
	assert.True(t, found, "vote-A (on DZ, joined, 10%% stake) should be in results")
}
