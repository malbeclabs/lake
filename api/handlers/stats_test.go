package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetStats_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()

	handlers.GetStats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.StatsResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// With empty tables, most stats should be zero
	assert.Equal(t, uint64(0), response.ValidatorsOnDZ)
	assert.Equal(t, float64(0), response.TotalStakeSol)
	assert.Equal(t, float64(0), response.StakeSharePct)
	assert.Equal(t, uint64(0), response.Users)
	assert.Equal(t, uint64(0), response.Devices)
	assert.Equal(t, uint64(0), response.Links)
	assert.Equal(t, uint64(0), response.Contributors)
	assert.Equal(t, uint64(0), response.Metros)
	assert.NotEmpty(t, response.FetchedAt)
}

func TestGetStats_WithData(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	ctx := t.Context()

	// Insert test data for users
	err := config.DB.Exec(ctx, `INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id) VALUES
		('u1', now(), now(), generateUUIDv4(), 0, 1, 'u1', 'activated', '', '', '', '1.2.3.4', '1.2.3.4', 0),
		('u2', now(), now(), generateUUIDv4(), 0, 2, 'u2', 'activated', '', '', '', '5.6.7.8', '5.6.7.8', 0)`)
	require.NoError(t, err)

	// Insert test data for devices
	err = config.DB.Exec(ctx, `INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users) VALUES
		('device1', now(), now(), generateUUIDv4(), 0, 1, 'device1', 'up', '', '', '', '', '', 0),
		('device2', now(), now(), generateUUIDv4(), 0, 2, 'device2', 'up', '', '', '', '', '', 0),
		('device3', now(), now(), generateUUIDv4(), 0, 3, 'device3', 'up', '', '', '', '', '', 0)`)
	require.NoError(t, err)

	// Insert test data for links (WAN and PNI types)
	err = config.DB.Exec(ctx, `INSERT INTO dim_dz_links_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, code, link_type, bandwidth_bps, tunnel_net, contributor_pk, side_a_pk, side_z_pk, side_a_iface_name, side_z_iface_name, committed_rtt_ns, committed_jitter_ns, isis_delay_override_ns) VALUES
		('link1', now(), now(), generateUUIDv4(), 0, 1, 'link1', 'activated', '', 'WAN', 1000000000, '', '', '', '', '', '', 0, 0, 0),
		('link2', now(), now(), generateUUIDv4(), 0, 2, 'link2', 'activated', '', 'WAN', 2000000000, '', '', '', '', '', '', 0, 0, 0),
		('link3', now(), now(), generateUUIDv4(), 0, 3, 'link3', 'inactive', '', 'WAN', 500000000, '', '', '', '', '', '', 0, 0, 0),
		('link4', now(), now(), generateUUIDv4(), 0, 4, 'link4', 'activated', '', 'PNI', 10000000000, '', '', '', '', '', '', 0, 0, 0)`)
	require.NoError(t, err)

	// Insert test data for contributors
	err = config.DB.Exec(ctx, `INSERT INTO dim_dz_contributors_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name) VALUES
		('contrib1', now(), now(), generateUUIDv4(), 0, 1, 'contrib1', '', ''),
		('contrib2', now(), now(), generateUUIDv4(), 0, 2, 'contrib2', '', '')`)
	require.NoError(t, err)

	// Insert test data for metros
	err = config.DB.Exec(ctx, `INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, latitude, longitude) VALUES
		('NYC', now(), now(), generateUUIDv4(), 0, 1, 'NYC', '', '', 0, 0),
		('LAX', now(), now(), generateUUIDv4(), 0, 2, 'LAX', '', '', 0, 0),
		('SFO', now(), now(), generateUUIDv4(), 0, 3, 'SFO', '', '', 0, 0)`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()

	handlers.GetStats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.StatsResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), response.Users)
	assert.Equal(t, uint64(3), response.Devices)
	assert.Equal(t, uint64(4), response.Links) // All links, not just activated
	assert.Equal(t, uint64(2), response.Contributors)
	assert.Equal(t, uint64(3), response.Metros)
	assert.Equal(t, int64(13500000000), response.BandwidthBps) // All links (WAN + PNI)
	assert.NotEmpty(t, response.FetchedAt)
}

func TestGetStats_ResponseHeaders(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()

	handlers.GetStats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	// Cache header should be set (either HIT or MISS)
	cacheHeader := rr.Header().Get("X-Cache")
	assert.True(t, cacheHeader == "HIT" || cacheHeader == "MISS", "X-Cache header should be HIT or MISS")
}

func TestGetStats_ValidatorsWithStake(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	apitesting.SetSequentialFallback(t)

	ctx := t.Context()

	// Set up the chain: dz_user -> gossip_node -> vote_account
	// User with gossip IP
	err := config.DB.Exec(ctx, `INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id) VALUES
		('u1', now(), now(), generateUUIDv4(), 0, 1, 'u1', 'activated', '', '', '', '10.0.0.1', '10.0.0.1', 0)`)
	require.NoError(t, err)

	// Gossip node matching the user's IP
	err = config.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version) VALUES
		('node_pubkey_1', now(), now(), generateUUIDv4(), 0, 1, 'node_pubkey_1', 0, '10.0.0.1', 0, '', 0, '')`)
	require.NoError(t, err)

	// Vote account for the gossip node with stake
	err = config.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage) VALUES
		('vote_1', now(), now(), generateUUIDv4(), 0, 1, 'vote_1', 0, 'node_pubkey_1', 10000000000000, 'true', 0)`) // 10000 SOL in lamports
	require.NoError(t, err)

	// Also add a vote account without matching user for total stake calculation
	err = config.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version) VALUES
		('node_pubkey_2', now(), now(), generateUUIDv4(), 0, 2, 'node_pubkey_2', 0, '20.0.0.1', 0, '', 0, '')`)
	require.NoError(t, err)
	err = config.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage) VALUES
		('vote_2', now(), now(), generateUUIDv4(), 0, 2, 'vote_2', 0, 'node_pubkey_2', 10000000000000, 'true', 0)`) // Another 10000 SOL
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()

	handlers.GetStats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.StatsResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), response.ValidatorsOnDZ)
	assert.Equal(t, float64(10000), response.TotalStakeSol) // 10000 SOL
	assert.Equal(t, float64(50), response.StakeSharePct)    // 50% of total stake
}
