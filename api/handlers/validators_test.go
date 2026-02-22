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

// seedValidatorData inserts minimal dimension and fact data for validator queries.
// Uses _history tables (SCD2 pattern) since the schema comes from migrations.
func seedValidatorData(t *testing.T) {
	ctx := t.Context()

	// Vote account
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		('vote1', now(), now(), generateUUIDv4(), 0, 1,
		 'vote1', 100, 'node1', 1000000000000, 'true', 5)`))

	// Gossip node
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
		('node1', now(), now(), generateUUIDv4(), 0, 1,
		 'node1', 100, '1.2.3.4', 8001, '', 0, '2.0.0')`))

	// Block production fact with recent data
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_solana_block_production
		(epoch, event_ts, ingested_at, leader_identity_pubkey, leader_slots_assigned_cum, blocks_produced_cum)
		VALUES
		(100, now() - INTERVAL 30 MINUTE, now(), 'node1', 100, 95)`))

	// GeoIP record
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_geoip_records_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 ip, asn, asn_org, city, region, country, latitude, longitude)
		VALUES
		('1.2.3.4', now(), now(), generateUUIDv4(), 0, 1,
		 '1.2.3.4', 12345, 'TestASN', 'Berlin', 'BE', 'DE', 52.52, 13.405)`))
}

func TestGetValidators(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedValidatorData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators", nil)
	rr := httptest.NewRecorder()

	handlers.GetValidators(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.ValidatorListResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 1, resp.Total)
	require.Len(t, resp.Items, 1)

	v := resp.Items[0]
	assert.Equal(t, "vote1", v.VotePubkey)
	assert.Equal(t, "node1", v.NodePubkey)
	assert.Equal(t, "2.0.0", v.Version)
	assert.Equal(t, "Berlin", v.City)
	assert.Equal(t, "DE", v.Country)
	assert.Equal(t, 5.0, v.SkipRate, "skip rate should be 5%% (5 skipped out of 100)")
}

func TestGetValidators_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators", nil)
	rr := httptest.NewRecorder()

	handlers.GetValidators(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.ValidatorListResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 0, resp.Total)
	assert.Empty(t, resp.Items)
}

func TestGetValidator(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedValidatorData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators/vote1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("vote_pubkey", "vote1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	handlers.GetValidator(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.ValidatorDetail
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "vote1", resp.VotePubkey)
	assert.Equal(t, "node1", resp.NodePubkey)
	assert.Equal(t, "2.0.0", resp.Version)
	assert.Equal(t, "Berlin", resp.City)
	assert.Equal(t, "DE", resp.Country)
	assert.Equal(t, 5.0, resp.SkipRate, "skip rate should be 5%% (5 skipped out of 100)")
}

func TestGetValidator_NotFound(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("vote_pubkey", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	handlers.GetValidator(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}
