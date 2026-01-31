package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTimelineSchema creates the minimal schema needed for timeline DZ stake attribution queries
func setupTimelineSchema(t *testing.T) {
	ctx := t.Context()

	tables := []string{
		`CREATE TABLE IF NOT EXISTS solana_vote_accounts_current (
			vote_pubkey String,
			node_pubkey String,
			activated_stake_lamports UInt64
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS solana_gossip_nodes_current (
			pubkey String,
			gossip_ip String
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_users_current (
			dz_ip String,
			status String
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dim_solana_vote_accounts_history (
			vote_pubkey String,
			node_pubkey String,
			activated_stake_lamports UInt64,
			snapshot_ts DateTime
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dim_solana_gossip_nodes_history (
			pubkey String,
			gossip_ip String,
			snapshot_ts DateTime
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String,
			code String,
			metro_pk String
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String,
			code String
		) ENGINE = Memory`,
		// Tables needed by other timeline queries that run in the handler
		`CREATE TABLE IF NOT EXISTS dim_dz_devices_history (
			pk String,
			entity_id String,
			snapshot_ts DateTime,
			status String,
			attrs_hash String,
			ingested_at DateTime,
			op_id UInt64,
			code String,
			device_type String,
			public_ip String,
			contributor_pk String,
			metro_pk String,
			max_users Int32
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dim_dz_links_history (
			pk String,
			entity_id String,
			snapshot_ts DateTime,
			status String,
			attrs_hash String,
			ingested_at DateTime,
			op_id UInt64,
			code String,
			link_type String,
			tunnel_net String,
			contributor_pk String,
			side_a_pk String,
			side_z_pk String,
			side_a_iface_name String,
			side_z_iface_name String,
			committed_rtt_ns Int64,
			committed_jitter_ns Int64,
			bandwidth_bps Int64,
			isis_delay_override_ns Int64
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dim_dz_metros_history (
			pk String,
			entity_id String,
			snapshot_ts DateTime,
			status String,
			attrs_hash String,
			ingested_at DateTime,
			op_id UInt64,
			code String,
			name String,
			longitude Float64,
			latitude Float64
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dim_dz_contributors_history (
			pk String,
			entity_id String,
			snapshot_ts DateTime,
			status String,
			attrs_hash String,
			ingested_at DateTime,
			op_id UInt64,
			code String,
			name String
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dim_dz_users_history (
			pk String,
			entity_id String,
			snapshot_ts DateTime,
			status String,
			attrs_hash String,
			ingested_at DateTime,
			op_id UInt64,
			owner_pubkey String,
			kind String,
			client_ip String,
			dz_ip String,
			device_pk String,
			tunnel_id Int32
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS fact_dz_device_link_latency (
			event_ts DateTime,
			device_pk String,
			link_pk String,
			loss_pct Float64,
			rtt_ns Int64
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String,
			code String,
			status String,
			link_type String,
			tunnel_net String,
			contributor_pk String,
			side_a_pk String,
			side_z_pk String,
			side_a_iface_name String,
			side_z_iface_name String,
			committed_rtt_ns Int64,
			committed_jitter_ns Int64,
			bandwidth_bps Int64,
			isis_delay_override_ns Int64
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_contributors_current (
			pk String,
			code String,
			name String
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS fact_dz_device_interface_counters (
			event_ts DateTime,
			device_pk String,
			intf String,
			user_tunnel_id Nullable(String),
			in_octets_delta UInt64,
			out_octets_delta UInt64,
			in_errors_delta UInt64,
			out_errors_delta UInt64,
			in_discards_delta UInt64,
			out_discards_delta UInt64,
			carrier_transitions_delta UInt64,
			delta_duration Float64
		) ENGINE = Memory`,
	}

	for _, ddl := range tables {
		err := config.DB.Exec(ctx, ddl)
		require.NoError(t, err)
	}
}

func TestDZStakeAttribution_Disconnect(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupTimelineSchema(t)
	ctx := t.Context()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	// DZ IP
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_users_current (dz_ip, status) VALUES ('1.2.3.4', 'activated')`))

	// Current tables (for total stake computation)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO solana_vote_accounts_current (vote_pubkey, node_pubkey, activated_stake_lamports) VALUES
		('vote-A', 'node-A', 100000000000000),
		('vote-stay', 'node-stay', 200000000000000)`))
	// node-stay is always on DZ with gossip IP 1.2.3.4
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO solana_gossip_nodes_current (pubkey, gossip_ip) VALUES ('node-stay', '1.2.3.4')`))

	// T1: validator A on DZ (gossip IP 1.2.3.4), validator stay also on DZ
	for _, v := range []struct{ vote, node, ip string }{
		{"vote-A", "node-A", "1.2.3.4"},
		{"vote-stay", "node-stay", "1.2.3.4"},
	} {
		require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
			`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('%s', '%s', 100000000000000, '%s')`,
			v.vote, v.node, t1.Format("2006-01-02 15:04:05"))))
		require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
			`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('%s', '%s', '%s')`,
			v.node, v.ip, t1.Format("2006-01-02 15:04:05"))))
	}

	// T2: validator A gossip IP changed to non-DZ, validator stay still on DZ
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-A', 'node-A', 100000000000000, '%s')`,
		t2.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-A', '5.5.5.5', '%s')`,
		t2.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-stay', 'node-stay', 100000000000000, '%s')`,
		t2.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-stay', '1.2.3.4', '%s')`,
		t2.Format("2006-01-02 15:04:05"))))

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
		if e.EventType == "dz_disconnected" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 dz_disconnected event")
	assert.Equal(t, "validator", attrEvents[0].EntityType)

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dz_disconnected", details["action"])
	assert.Equal(t, "vote-A", details["vote_pubkey"])

	// DZ total stake share should be present
	dzTotal, ok := details["dz_total_stake_share_pct"].(float64)
	assert.True(t, ok, "dz_total_stake_share_pct should be a float64, got %T: %v", details["dz_total_stake_share_pct"], details["dz_total_stake_share_pct"])
	t.Logf("dz_total_stake_share_pct = %v", dzTotal)
	assert.Greater(t, dzTotal, float64(0), "dz_total_stake_share_pct should be > 0")
}

func TestDZStakeAttribution_Connect(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupTimelineSchema(t)
	ctx := t.Context()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_users_current (dz_ip, status) VALUES ('1.2.3.4', 'activated')`))
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO solana_vote_accounts_current (vote_pubkey, node_pubkey, activated_stake_lamports) VALUES ('vote-B', 'node-B', 50000000000000)`))

	// T1: validator B NOT on DZ
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-B', 'node-B', 50000000000000, '%s')`,
		t1.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-B', '9.9.9.9', '%s')`,
		t1.Format("2006-01-02 15:04:05"))))

	// T2: validator B now on DZ
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-B', 'node-B', 50000000000000, '%s')`,
		t2.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-B', '1.2.3.4', '%s')`,
		t2.Format("2006-01-02 15:04:05"))))

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/timeline?start=%s&end=%s&category=state_change",
		t1.Add(-time.Minute).Format(time.RFC3339), t2.Add(time.Minute).Format(time.RFC3339)), nil)
	rr := httptest.NewRecorder()
	handlers.GetTimeline(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TimelineResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	var attrEvents []handlers.TimelineEvent
	for _, e := range resp.Events {
		if e.EventType == "dz_connected" {
			attrEvents = append(attrEvents, e)
		}
	}
	require.Len(t, attrEvents, 1, "expected 1 dz_connected event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dz_connected", details["action"])
	assert.Equal(t, "vote-B", details["vote_pubkey"])
}

func TestDZStakeAttribution_StakeChange(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupTimelineSchema(t)
	ctx := t.Context()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_users_current (dz_ip, status) VALUES ('1.2.3.4', 'activated')`))
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO solana_vote_accounts_current (vote_pubkey, node_pubkey, activated_stake_lamports) VALUES ('vote-C', 'node-C', 80000000000000)`))

	// T1: validator C on DZ, 100k SOL
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-C', 'node-C', 100000000000000, '%s')`,
		t1.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-C', '1.2.3.4', '%s')`,
		t1.Format("2006-01-02 15:04:05"))))

	// T2: validator C on DZ, 80k SOL (decreased)
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-C', 'node-C', 80000000000000, '%s')`,
		t2.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-C', '1.2.3.4', '%s')`,
		t2.Format("2006-01-02 15:04:05"))))

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
}

func TestDZStakeAttribution_ValidatorLeft(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupTimelineSchema(t)
	ctx := t.Context()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_users_current (dz_ip, status) VALUES ('1.2.3.4', 'activated')`))
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO solana_vote_accounts_current (vote_pubkey, node_pubkey, activated_stake_lamports) VALUES ('vote-D', 'node-D', 50000000000000)`))

	// T1: validator D on DZ, 50k SOL
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-D', 'node-D', 50000000000000, '%s')`,
		t1.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-D', '1.2.3.4', '%s')`,
		t1.Format("2006-01-02 15:04:05"))))

	// T2: validator D not in vote accounts (left Solana) - no rows inserted for T2
	// But we need at least one row at T2 so the snapshot exists for the DZ total calculation
	// Add a different validator at T2 so the snapshot exists
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-other', 'node-other', 1000000000, '%s')`,
		t2.Format("2006-01-02 15:04:05"))))
	require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
		`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-other', '9.9.9.9', '%s')`,
		t2.Format("2006-01-02 15:04:05"))))

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
	require.Len(t, attrEvents, 1, "expected 1 validator_left_dz event")

	details, ok := attrEvents[0].Details.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "validator_left_dz", details["action"])
}

func TestDZStakeAttribution_NoChange(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupTimelineSchema(t)
	ctx := t.Context()

	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_users_current (dz_ip, status) VALUES ('1.2.3.4', 'activated')`))
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO solana_vote_accounts_current (vote_pubkey, node_pubkey, activated_stake_lamports) VALUES ('vote-E', 'node-E', 100000000000000)`))

	// T1 and T2: same validator, same stake, same DZ status - no change
	for _, ts := range []time.Time{t1, t2} {
		require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
			`INSERT INTO dim_solana_vote_accounts_history (vote_pubkey, node_pubkey, activated_stake_lamports, snapshot_ts) VALUES ('vote-E', 'node-E', 100000000000000, '%s')`,
			ts.Format("2006-01-02 15:04:05"))))
		require.NoError(t, config.DB.Exec(ctx, fmt.Sprintf(
			`INSERT INTO dim_solana_gossip_nodes_history (pubkey, gossip_ip, snapshot_ts) VALUES ('node-E', '1.2.3.4', '%s')`,
			ts.Format("2006-01-02 15:04:05"))))
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
		if e.EventType == "dz_disconnected" || e.EventType == "dz_connected" || e.EventType == "stake_changed" || e.EventType == "validator_left_dz" {
			t.Errorf("unexpected DZ stake attribution event: %s", e.EventType)
		}
	}
}
