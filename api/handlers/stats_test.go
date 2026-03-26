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

// setupStatsSchema creates the schema needed for stats queries via migrations
func setupStatsSchema(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
}

func TestGetStats_Empty(t *testing.T) {
	setupStatsSchema(t)

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
	setupStatsSchema(t)
	ctx := t.Context()

	// Insert users via history table
	for _, ip := range []string{"1.2.3.4", "5.6.7.8"} {
		require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_users_history (
			entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, client_ip, dz_ip, status, kind, owner_pubkey, device_pk
		) VALUES ($1, now(), now(), $2, 0, $3, $4, $5, 'activated', '', '', '')`,
			ip, "00000000-0000-0000-0000-000000000001", ip, ip, ip))
	}

	// Insert devices
	for _, pk := range []string{"device1", "device2", "device3"} {
		seedDeviceMetadata(t, pk, pk, "router", "", "", 0, "activated")
	}

	// Insert links
	seedLinkMetadata(t, "link1", "link1", "WAN", "", "", "", 1000000000, 0, "activated")
	seedLinkMetadata(t, "link2", "link2", "WAN", "", "", "", 2000000000, 0, "activated")
	seedLinkMetadata(t, "link3", "link3", "WAN", "", "", "", 500000000, 0, "inactive")
	seedLinkMetadata(t, "link4", "link4", "PNI", "", "", "", 10000000000, 0, "activated")

	// Insert contributors
	seedContributor(t, "contrib1", "contrib1")
	seedContributor(t, "contrib2", "contrib2")

	// Insert metros
	seedMetro(t, "NYC", "NYC")
	seedMetro(t, "LAX", "LAX")
	seedMetro(t, "SFO", "SFO")

	// Optimize all history tables
	for _, table := range []string{"dim_dz_users_history", "dim_dz_devices_history", "dim_dz_links_history", "dim_dz_contributors_history", "dim_dz_metros_history"} {
		require.NoError(t, config.DB.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()

	handlers.GetStats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.StatsResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
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
	setupStatsSchema(t)

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
	setupStatsSchema(t)
	ctx := t.Context()

	// User with gossip IP
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_users_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, client_ip, dz_ip, status, kind, owner_pubkey, device_pk
	) VALUES ('u1', now(), now(), '00000000-0000-0000-0000-000000000001', 0, 'u1', '10.0.0.1', '10.0.0.1', 'activated', '', '', '')`))
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_dz_users_history FINAL`))

	// Gossip nodes
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version
	) VALUES ('g1', now(), now(), '00000000-0000-0000-0000-000000000001', 0, 'node_pubkey_1', 0, '10.0.0.1', 0, '', 0, '')`))
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version
	) VALUES ('g2', now(), now(), '00000000-0000-0000-0000-000000000001', 0, 'node_pubkey_2', 0, '20.0.0.1', 0, '', 0, '')`))
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_solana_gossip_nodes_history FINAL`))

	// Vote accounts
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage
	) VALUES ('v1', now(), now(), '00000000-0000-0000-0000-000000000001', 0, 'vote_1', 0, 'node_pubkey_1', 10000000000000, 'true', 0)`))
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history (
		entity_id, snapshot_ts, ingested_at, op_id, is_deleted, vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage
	) VALUES ('v2', now(), now(), '00000000-0000-0000-0000-000000000001', 0, 'vote_2', 0, 'node_pubkey_2', 10000000000000, 'true', 0)`))
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_solana_vote_accounts_history FINAL`))

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rr := httptest.NewRecorder()

	handlers.GetStats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.StatsResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), response.ValidatorsOnDZ)
	assert.Equal(t, float64(10000), response.TotalStakeSol) // 10000 SOL
	assert.Equal(t, float64(50), response.StakeSharePct)    // 50% of total stake
}
