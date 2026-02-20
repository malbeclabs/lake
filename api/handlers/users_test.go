package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertUserTestData(t *testing.T) {
	ctx := t.Context()

	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
			('metro-ams', now(), now(), generateUUIDv4(), 0, 1, 'metro-ams', 'ams', 'Amsterdam')
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
			('contrib-1', now(), now(), generateUUIDv4(), 0, 1, 'contrib-1', 'contrib-a', '')
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users)
		VALUES
			('dev-ams1', now(), now(), generateUUIDv4(), 0, 1, 'dev-ams1', 'up', 'edge', 'ams001-dz001', '', 'contrib-1', 'metro-ams', 0)
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id)
		VALUES
			('user-1', now(), now(), generateUUIDv4(), 0, 1, 'user-1', 'owner-pubkey-1', 'activated', 'validator', '10.0.0.1', '10.0.0.1', 'dev-ams1', 501)
	`)
	require.NoError(t, err)
}

func TestGetUser_ReturnsDetail(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertUserTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/user-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "user-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUser(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var user handlers.UserDetail
	err := json.NewDecoder(rr.Body).Decode(&user)
	require.NoError(t, err)

	assert.Equal(t, "user-1", user.PK)
	assert.Equal(t, "owner-pubkey-1", user.OwnerPubkey)
	assert.Equal(t, "activated", user.Status)
	assert.Equal(t, "validator", user.Kind)
	assert.Equal(t, "10.0.0.1", user.DzIP)
	assert.Equal(t, int32(501), user.TunnelID)
	assert.Equal(t, "dev-ams1", user.DevicePK)
	assert.Equal(t, "ams001-dz001", user.DeviceCode)
	assert.Equal(t, "metro-ams", user.MetroPK)
	assert.Equal(t, "ams", user.MetroCode)
	assert.Equal(t, "Amsterdam", user.MetroName)
	assert.Equal(t, "contrib-1", user.ContributorPK)
	assert.Equal(t, "contrib-a", user.ContributorCode)
}

func TestGetUser_IsValidatorFalseWhenNoGossipMatch(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertUserTestData(t)

	// No gossip or vote account data inserted — is_validator should be false

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/user-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "user-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUser(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var user handlers.UserDetail
	err := json.NewDecoder(rr.Body).Decode(&user)
	require.NoError(t, err)

	assert.False(t, user.IsValidator, "is_validator should be false when no gossip/vote match exists")
	assert.Empty(t, user.VotePubkey)
	assert.Equal(t, float64(0), user.StakeSol)
}

func TestGetUser_IsValidatorTrueWhenMatched(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertUserTestData(t)

	ctx := t.Context()

	// Insert gossip node matching user's client_ip
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
			('node-pub-1', now(), now(), generateUUIDv4(), 0, 1, 'node-pub-1', 0, '10.0.0.1', 0, '', 0, '')
	`)
	require.NoError(t, err)

	// Insert vote account matching the gossip node
	err = config.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
			('vote-pub-1', now(), now(), generateUUIDv4(), 0, 1,
			 'vote-pub-1', 0, 'node-pub-1', 5000000000000, 'true', 0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/user-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "user-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUser(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var user handlers.UserDetail
	err = json.NewDecoder(rr.Body).Decode(&user)
	require.NoError(t, err)

	assert.True(t, user.IsValidator, "is_validator should be true when gossip/vote match exists")
	assert.Equal(t, "vote-pub-1", user.VotePubkey)
	assert.Equal(t, float64(5000), user.StakeSol)
}

func TestGetUser_NotFound(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUser(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetUserTraffic_ReturnsData(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertUserTestData(t)

	ctx := t.Context()

	// Insert traffic counters for tunnel 501
	err := config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, user_tunnel_id, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta, delta_duration)
		VALUES
			(now(), 'dev-ams1', 501, 1000000, 500000, 1000, 500, 4.0),
			(now(), 'dev-ams1', 501, 1000000, 500000, 1000, 500, 4.0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/user-1/traffic?time_range=1h", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "user-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUserTraffic(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var points []handlers.UserTrafficPoint
	err = json.NewDecoder(rr.Body).Decode(&points)
	require.NoError(t, err)
	require.NotEmpty(t, points, "should return at least one traffic point")

	// Verify tunnel_id and traffic values
	assert.Equal(t, int64(501), points[0].TunnelID, "tunnel_id should match")
	assert.Greater(t, points[0].InBps, float64(0), "in_bps should be > 0")
	assert.Greater(t, points[0].OutBps, float64(0), "out_bps should be > 0")
	assert.Greater(t, points[0].InPps, float64(0), "in_pps should be > 0")
	assert.Greater(t, points[0].OutPps, float64(0), "out_pps should be > 0")
}

func TestGetUserTraffic_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertUserTestData(t)

	// No traffic counters inserted

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/user-1/traffic", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "user-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUserTraffic(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var points []handlers.UserTrafficPoint
	err := json.NewDecoder(rr.Body).Decode(&points)
	require.NoError(t, err)
	assert.Empty(t, points)
}

func TestGetUserTraffic_MissingPK(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users//traffic", nil)
	rctx := chi.NewRouteContext()
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetUserTraffic(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}
