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

// insertContributorMulticastData inserts a contributor with one device whose
// on-chain multicast counts are left at 0, plus activated multicast users that
// subscribe to / publish to groups. The contributor detail must report the live
// per-device counts, not the stale on-chain zeros (#650).
func insertContributorMulticastData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name) VALUES
		('c-mc', now(), now(), generateUUIDv4(), 0, 1, 'c-mc', 'CMC', 'Multicast Contributor')
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, contributor_pk, metro_pk, public_ip, max_users) VALUES
		('cdev-1', now(), now(), generateUUIDv4(), 0, 1, 'cdev-1', 'C-DEV-01', 'up', 'switch', 'c-mc', '', '10.8.0.1', 100)
	`)
	require.NoError(t, err)

	// 3 subscriber-only, 2 publisher-only, 1 both-roles, 1 pending (excluded).
	// Live totals: subscribers 4, publishers 3.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id, publishers, subscribers) VALUES
		('c-s1',   now(), now(), generateUUIDv4(), 0, 1, 'c-s1',   'activated', 'cdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g1"]'),
		('c-s2',   now(), now(), generateUUIDv4(), 0, 2, 'c-s2',   'activated', 'cdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g1"]'),
		('c-s3',   now(), now(), generateUUIDv4(), 0, 3, 'c-s3',   'activated', 'cdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g2"]'),
		('c-p1',   now(), now(), generateUUIDv4(), 0, 4, 'c-p1',   'activated', 'cdev-1', 'multicast', 'p', '', '', 0, '["g1"]', '[]'),
		('c-p2',   now(), now(), generateUUIDv4(), 0, 5, 'c-p2',   'activated', 'cdev-1', 'multicast', 'p', '', '', 0, '["g1"]', '[]'),
		('c-b1',   now(), now(), generateUUIDv4(), 0, 6, 'c-b1',   'activated', 'cdev-1', 'multicast', 'p', '', '', 0, '["g2"]', '["g2"]'),
		('c-pend', now(), now(), generateUUIDv4(), 0, 7, 'c-pend', 'pending',   'cdev-1', 'multicast', 'p', '', '', 0, '[]',     '["g1"]')
	`)
	require.NoError(t, err)
}

func TestGetContributor_MulticastCountsFromLiveUsers(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertContributorMulticastData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/contributors/c-mc", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "c-mc")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetContributor(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var contributor handlers.ContributorDetail
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&contributor))
	assert.Equal(t, uint64(4), contributor.MulticastSubscribersCount)
	assert.Equal(t, uint64(3), contributor.MulticastPublishersCount)
}
