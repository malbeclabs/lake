package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedValidatorData inserts minimal dimension and fact data for validator queries.
// Uses _history tables (SCD2 pattern) since the schema comes from migrations.
func seedValidatorData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	// Vote account
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		('vote1', now(), now(), generateUUIDv4(), 0, 1,
		 'vote1', 100, 'node1', 1000000000000, 'true', 5)`))

	// Gossip node
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
		('node1', now(), now(), generateUUIDv4(), 0, 1,
		 'node1', 100, '1.2.3.4', 8001, '', 0, '2.0.0')`))

	// Block production fact with recent data
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_solana_block_production
		(epoch, event_ts, ingested_at, leader_identity_pubkey, leader_slots_assigned_cum, blocks_produced_cum)
		VALUES
		(100, now() - INTERVAL 30 MINUTE, now(), 'node1', 100, 95)`))

	// GeoIP record
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dim_geoip_records_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 ip, asn, asn_org, city, region, country, latitude, longitude)
		VALUES
		('1.2.3.4', now(), now(), generateUUIDv4(), 0, 1,
		 '1.2.3.4', 12345, 'TestASN', 'Berlin', 'BE', 'DE', 52.52, 13.405)`))
}

func TestGetValidators(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedValidatorData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators", nil)
	rr := httptest.NewRecorder()

	api.GetValidators(rr, req)

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
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators", nil)
	rr := httptest.NewRecorder()

	api.GetValidators(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.ValidatorListResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 0, resp.Total)
	assert.Empty(t, resp.Items)
}

// seedValidator inserts one additional vote account + gossip node pair, so a test
// can page through a set larger than seedValidatorData's single validator.
func seedValidator(t *testing.T, api *handlers.API, votePubkey, nodePubkey, gossipIP string, stakeLamports int64) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		($1, now(), now(), generateUUIDv4(), 0, 1, $2, 100, $3, $4, 'true', 5)`,
		votePubkey, votePubkey, nodePubkey, stakeLamports))

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dim_solana_gossip_nodes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
		($1, now(), now(), generateUUIDv4(), 0, 1, $2, 100, $3, 8001, '', 0, '2.0.0')`,
		nodePubkey, nodePubkey, gossipIP))
}

// cleanupValidatorsPageCache deletes the shared validators page-cache row after the
// test. SetupPostgresForTest gives no per-test isolation, so tests that write this
// production key must clean up or the entry leaks into later tests. context.Background
// (not t.Context) because the test context is already canceled during t.Cleanup.
func cleanupValidatorsPageCache(t *testing.T, api *handlers.API) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = api.PgPool.Exec(context.Background(),
			`DELETE FROM page_cache WHERE key = $1`, handlers.ValidatorsPageCacheKey)
	})
}

// TestGetValidators_CachedPageMatchesLive pins that a page sliced out of the cache
// is the same response the live query returns. Without this nothing exercised the
// HIT path at all: the other validators tests use NewTestAPI, which leaves PgPool
// nil, so readPageCacheWithAge short-circuits and only the live path ever ran.
//
// It also pins the one known divergence (offset >= Total, where the live path
// reports total: 0 because its window aggregates are only read off returned rows)
// so that behavior can't change silently in either direction.
//
// Must NOT run in parallel: it and the other pg-backed cache tests share one
// Postgres and write the same production key (see cleanupValidatorsPageCache).
func TestGetValidators_CachedPageMatchesLive(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	cleanupValidatorsPageCache(t, api)
	seedValidatorData(t, api)
	seedValidator(t, api, "vote2", "node2", "5.6.7.8", 3000000000000)
	seedValidator(t, api, "vote3", "node3", "9.10.11.12", 2000000000000)

	get := func(t *testing.T, query string) (*httptest.ResponseRecorder, handlers.ValidatorListResponse) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/solana/validators?"+query, nil)
		rr := httptest.NewRecorder()
		api.GetValidators(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
		var resp handlers.ValidatorListResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		return rr, resp
	}

	// Live baselines first, with no cache entry present.
	live := map[string]handlers.ValidatorListResponse{}
	shapes := []string{"limit=900&offset=0", "limit=100&offset=1", "limit=100&offset=3"}
	for _, shape := range shapes {
		rr, resp := get(t, shape)
		require.Equal(t, "MISS", rr.Header().Get("X-Cache"), shape)
		// Vary is emitted on the MISS path too, so a shared cache can tell envs apart
		// regardless of server cache state.
		assert.Equal(t, "X-DZ-Env", rr.Header().Get("Vary"), shape)
		assert.Empty(t, rr.Header().Get("Cache-Control"), "MISS must not advertise freshness: %s", shape)
		live[shape] = resp
	}
	require.Equal(t, 3, live["limit=900&offset=0"].Total, "seed should produce 3 validators")

	// Populate the cache exactly as the worker does.
	data, err := api.FetchValidatorsData(t.Context())
	require.NoError(t, err)
	require.NoError(t, api.WritePageCache(t.Context(), handlers.ValidatorsPageCacheKey, data))

	for _, shape := range shapes {
		t.Run(shape, func(t *testing.T) {
			rr, resp := get(t, shape)
			require.Equal(t, "HIT", rr.Header().Get("X-Cache"))
			assert.Equal(t, "public, max-age=60", rr.Header().Get("Cache-Control"))
			assert.Equal(t, "X-DZ-Env", rr.Header().Get("Vary"))

			want := live[shape]
			assert.Equal(t, want.Items, resp.Items, "cached items must equal the live page")
			assert.Equal(t, want.Limit, resp.Limit)
			assert.Equal(t, want.Offset, resp.Offset)

			if resp.Offset >= 3 {
				// The known divergence: live reports 0 for a page past the end, the
				// cached path reports the real whole-set counts.
				assert.Empty(t, resp.Items)
				assert.Equal(t, 0, want.Total, "live path reports total 0 past the end")
				assert.Equal(t, 3, resp.Total, "cached path reports the real total")
				return
			}
			assert.Equal(t, want.Total, resp.Total)
			assert.Equal(t, want.OnDZCount, resp.OnDZCount)
		})
	}
}

// TestGetValidators_StaleCacheFallsThroughToLive pins that an entry older than
// validatorsCacheStaleAfter is not served — a worker that stopped running must not
// pin an answer indefinitely, especially with Cache-Control: max-age=60 on top.
//
// Must NOT run in parallel, for the same shared-key reason as the test above.
func TestGetValidators_StaleCacheFallsThroughToLive(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	cleanupValidatorsPageCache(t, api)
	seedValidatorData(t, api)

	data, err := api.FetchValidatorsData(t.Context())
	require.NoError(t, err)
	require.NoError(t, api.WritePageCache(t.Context(), handlers.ValidatorsPageCacheKey, data))

	// Backdate well past the staleness bound.
	_, err = api.PgPool.Exec(t.Context(),
		`UPDATE page_cache SET updated_at = NOW() - INTERVAL '1 hour' WHERE key = $1`,
		handlers.ValidatorsPageCacheKey)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/solana/validators", nil)
	rr := httptest.NewRecorder()
	api.GetValidators(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, "MISS", rr.Header().Get("X-Cache"), "a stale entry must run live")
}

func TestGetValidator(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedValidatorData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators/vote1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("vote_pubkey", "vote1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	api.GetValidator(rr, req)

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
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/validators/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("vote_pubkey", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	api.GetValidator(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}
