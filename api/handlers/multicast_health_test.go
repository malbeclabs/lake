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

// Reuses insertMulticastTestData from multicast_test.go: ams-publisher
// (Tunnel501) + nyc-subscriber (Tunnel502) in group "test-group" (mcast IP
// 233.0.0.1).

func insertMulticastHealthFixtures(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	// FHR (ams) — publisher endpoint observable
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-fhr', now(), now(), generateUUIDv4(), 0, 1,
			'dev-ams1', 'default', 'sparse', '233.0.0.1', '10.0.0.1',
			'SBNP', 0, 'Tunnel501', '', '', 0, 0, '', 0, 0, '', 0, now())`))

	// LHR (nyc) — subscriber endpoint observable
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list, rpf_interface, rpf_rib, rpf_prefix,
			 rpf_preference, rpf_metric, rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		VALUES ('mr-lhr', now(), now(), generateUUIDv4(), 0, 1,
			'dev-nyc1', 'default', 'sparse', '233.0.0.1', '10.0.0.1',
			'SMP', 0, 'Port-Channel1', '', '', 0, 0, '', 0, 0, '["Tunnel502"]', 1, now())`))

	// Rate fixtures — publisher RX = 10 Mbps; subscriber TX = 10 Mbps → reconciled.
	// Without these the rate dimension would collapse health_status to 'unknown'
	// via 'no_data' for both endpoints.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, user_pk, max_in_bps, max_out_bps, ingested_at)
		VALUES
			(now() - INTERVAL 1 MINUTE, 'dev-ams1', 'Tunnel501', 501, 'user-pub', 10000000, 0, now()),
			(now() - INTERVAL 1 MINUTE, 'dev-nyc1', 'Tunnel502', 502, 'user-sub', 0, 10000000, now())`))

	require.NoError(t, api.DB.Exec(ctx, `SYSTEM REFRESH VIEW dz_device_interface_ips`))
	require.NoError(t, api.DB.Exec(ctx, `SYSTEM WAIT VIEW dz_device_interface_ips`))
}

func makeGroupHealthRequest(api *handlers.API, group string, path string) (*httptest.ResponseRecorder, []byte) {
	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/"+group+path, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", group)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	return executeRequest(api, req, path)
}

func executeRequest(api *handlers.API, req *http.Request, path string) (*httptest.ResponseRecorder, []byte) {
	rr := httptest.NewRecorder()
	switch req.URL.Path {
	case "/api/dz/multicast-groups/" + chi.URLParam(req, "pk") + "/health":
		api.GetMulticastGroupHealth(rr, req)
	case "/api/dz/multicast-groups/" + chi.URLParam(req, "pk") + "/health/users":
		api.GetMulticastGroupHealthUsers(rr, req)
	case "/api/dz/multicast-groups/" + chi.URLParam(req, "pk") + "/health/paths":
		api.GetMulticastGroupHealthPaths(rr, req)
	case "/api/dz/multicast-groups/" + chi.URLParam(req, "pk") + "/health/path-root-causes":
		api.GetMulticastGroupHealthPathRootCauses(rr, req)
	case "/user-health":
		api.GetUserHealth(rr, req)
	}
	return rr, rr.Body.Bytes()
}

func TestGetMulticastGroupHealth_Summary(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	rr, body := makeGroupHealthRequest(api, "test-group", "/health")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", string(body))
	assert.Equal(t, "MISS", rr.Header().Get("X-Cache"))

	var resp handlers.MulticastHealthGroupSummaryResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "test-group", resp.Group.Code)
	assert.True(t, resp.SourceAvailable)

	// 1 publisher + 1 subscriber → 2 user rows, both reconciled.
	assert.EqualValues(t, 2, resp.Counts.Users.Total)
	assert.EqualValues(t, 2, resp.Counts.Users.Healthy)
	assert.EqualValues(t, 0, resp.Counts.Users.Unhealthy)

	// 1 (publisher, subscriber) pair → 1 path row, healthy.
	assert.EqualValues(t, 1, resp.Counts.Paths.Total)
	assert.EqualValues(t, 1, resp.Counts.Paths.Healthy)

	// 2 mroute rows (FHR + LHR), both healthy.
	assert.EqualValues(t, 2, resp.Counts.Mroutes.Total)
	assert.EqualValues(t, 2, resp.Counts.Mroutes.Healthy)
}

func TestFetchMulticastHealthSummariesData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	cache, err := api.FetchMulticastHealthSummariesData(t.Context(), "test-group")
	require.NoError(t, err)

	var summary *handlers.MulticastHealthGroupSummaryResponse
	for i := range cache.Summaries {
		if cache.Summaries[i].Group.Code == "test-group" {
			summary = &cache.Summaries[i]
			break
		}
	}
	require.NotNil(t, summary)
	assert.True(t, summary.SourceAvailable)
	assert.EqualValues(t, 2, summary.Counts.Users.Total)
	assert.EqualValues(t, 1, summary.Counts.Paths.Total)
	assert.EqualValues(t, 2, summary.Counts.Mroutes.Total)
}

func TestGetMulticastGroupHealth_UsesPageCache(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)
	cached := handlers.MulticastHealthGroupSummaryResponse{
		Group: handlers.MulticastDeliveryGroup{
			PK:              "cached-group-pk",
			Code:            "cached-group-code",
			MulticastIP:     "233.0.0.42",
			PublisherCount:  4,
			SubscriberCount: 5,
		},
		SourceAvailable: true,
		GeneratedAt:     "2026-06-05T00:00:00Z",
		Counts: handlers.MulticastHealthCounts{
			Paths: handlers.MulticastHealthStatusCounts{Healthy: 40, Unhealthy: 2, Total: 42},
		},
	}
	require.NoError(t, api.WritePageCache(t.Context(), handlers.MulticastHealthSummariesCacheKey, handlers.MulticastHealthSummariesCache{
		GeneratedAt: cached.GeneratedAt,
		Summaries:   []handlers.MulticastHealthGroupSummaryResponse{cached},
	}))

	rr, body := makeGroupHealthRequest(api, "cached-group-code", "/health")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", string(body))
	assert.Equal(t, "HIT", rr.Header().Get("X-Cache"))

	var resp handlers.MulticastHealthGroupSummaryResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "cached-group-pk", resp.Group.PK)
	assert.EqualValues(t, 42, resp.Counts.Paths.Total)
}

func TestGetMulticastGroupHealth_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	rr, _ := makeGroupHealthRequest(api, "no-such-group", "/health")
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetMulticastGroupHealth_Users(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	rr, body := makeGroupHealthRequest(api, "test-group", "/health/users")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", string(body))

	var resp handlers.MulticastHealthGroupUsersResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "test-group", resp.Group.Code)
	require.Len(t, resp.Items, 2)
	assert.EqualValues(t, 2, resp.Total)

	// Rate is presence-only now: both endpoints transmit non-zero, so both are
	// 'active' and healthy. expected_bps_5m is always nil (reconciliation removed).
	var pub, sub *handlers.MulticastHealthUserItem
	for i := range resp.Items {
		it := &resp.Items[i]
		assert.True(t, it.Reconciled, "user %s expected reconciled", it.UserPK)
		assert.Equal(t, "healthy", it.ControlPlaneStatus, "user %s expected CP healthy", it.UserPK)
		assert.Equal(t, "active", it.RateStatus, "user %s expected rate active", it.UserPK)
		assert.Equal(t, "healthy", it.HealthStatus, "user %s expected combined healthy", it.UserPK)
		assert.NotNil(t, it.ObservedBps5m, "user %s missing observed rate", it.UserPK)
		assert.NotNil(t, it.RateBucketTS, "user %s missing rate bucket ts", it.UserPK)
		assert.Nil(t, it.ExpectedBps5m, "user %s expected_bps should be nil (reconciliation removed)", it.UserPK)
		switch it.Mode {
		case "P":
			pub = it
		case "S":
			sub = it
		}
	}
	// Both endpoints are transmitting → both read 'active'.
	require.NotNil(t, pub)
	require.NotNil(t, sub)
	assert.Equal(t, "active", pub.RateStatusReason)
	assert.Equal(t, "active", sub.RateStatusReason)
	assert.InDelta(t, 10_000_000, *sub.ObservedBps5m, 1)
}

func TestGetMulticastGroupHealth_UsersPagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	rr, body := makeGroupHealthRequest(api, "test-group", "/health/users?limit=1&offset=1")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", string(body))

	var resp handlers.MulticastHealthGroupUsersResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.EqualValues(t, 2, resp.Total)
	assert.EqualValues(t, 1, resp.Limit)
	assert.EqualValues(t, 1, resp.Offset)
	require.Len(t, resp.Items, 1)
}

func TestGetMulticastGroupHealth_Paths(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	rr, body := makeGroupHealthRequest(api, "test-group", "/health/paths?limit=1&offset=0")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", string(body))

	var resp handlers.MulticastHealthGroupPathsResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "test-group", resp.Group.Code)
	assert.EqualValues(t, 1, resp.Total)
	assert.EqualValues(t, 1, resp.Limit)
	assert.EqualValues(t, 0, resp.Offset)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "user-pub", resp.Items[0].PublisherUserPK)
	assert.Equal(t, "user-sub", resp.Items[0].SubscriberUserPK)
	assert.True(t, resp.Items[0].EndpointsReconciled)
	assert.Equal(t, "healthy", resp.Items[0].HealthStatus)
	assert.Equal(t, "endpoints_only", resp.Items[0].VerificationMethod)
}

// TestGetMulticastGroupHealth_PathRootCauses verifies the fan-out rollup:
// two extra subscribers of group-1 whose OIF is never observed each make their
// (user-pub → sub) path non-healthy, and the endpoint should surface each one
// as a root cause with affected_pairs=1 — user-sub2 as 'unhealthy' (BGP up,
// just not forwarding) and user-sub3 as 'disconnected' (BGP down). The healthy
// user-pub → user-sub path contributes no root cause.
func TestGetMulticastGroupHealth_PathRootCauses(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	// Two more subscribers of group-1 on nyc, neither observed in any OIF.
	// user-sub3's BGP is down → 'disconnected'; user-sub2's is up → 'unhealthy'.
	require.NoError(t, api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers, bgp_status)
		VALUES
			('user-sub2', now(), now(), generateUUIDv4(), 0, 1, 'user-sub2', 'pubkey-sub2', 'activated', 'multicast', '10.0.0.3', '10.0.0.3', 'dev-nyc1', 503, '[]', '["group-1"]', 'up'),
			('user-sub3', now(), now(), generateUUIDv4(), 0, 1, 'user-sub3', 'pubkey-sub3', 'activated', 'multicast', '10.0.0.4', '10.0.0.4', 'dev-nyc1', 504, '[]', '["group-1"]', 'down')`))

	rr, body := makeGroupHealthRequest(api, "test-group", "/health/path-root-causes")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", string(body))

	var resp handlers.MulticastHealthPathRootCausesResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, "test-group", resp.Group.Code)
	require.Len(t, resp.Items, 2, "body: %s", string(body))
	assert.EqualValues(t, 2, resp.Total)

	byUser := map[string]handlers.MulticastHealthPathRootCause{}
	for _, it := range resp.Items {
		byUser[it.UserPK] = it
	}

	sub2, ok := byUser["user-sub2"]
	require.True(t, ok, "expected user-sub2 root cause, body: %s", string(body))
	assert.Equal(t, "subscriber", sub2.FaultingRole)
	assert.Equal(t, "unhealthy", sub2.EndpointStatus)
	assert.EqualValues(t, 1, sub2.AffectedPairs)
	assert.Equal(t, "nyc001-dz001", sub2.DeviceCode)
	assert.EqualValues(t, 503, sub2.TunnelID)

	sub3, ok := byUser["user-sub3"]
	require.True(t, ok, "expected user-sub3 root cause, body: %s", string(body))
	assert.Equal(t, "subscriber", sub3.FaultingRole)
	assert.Equal(t, "disconnected", sub3.EndpointStatus)
	assert.EqualValues(t, 1, sub3.AffectedPairs)

	// The healthy user-pub → user-sub path must not surface a root cause, and
	// the observed publisher (user-pub) is never at fault.
	_, hasPub := byUser["user-pub"]
	assert.False(t, hasPub, "observed publisher should not be a root cause")
}

func TestGetUserHealth_PerGroupRows(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/user-pub/health", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "user-pub")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()
	api.GetUserHealth(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	var resp handlers.MulticastHealthUserResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "user-pub", resp.UserPK)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "test-group", resp.Items[0].MulticastGroupCode)
	assert.Equal(t, "P", resp.Items[0].Mode)
	assert.True(t, resp.Items[0].Reconciled)
	assert.Equal(t, "healthy", resp.Items[0].HealthStatus)
}

func TestGetUserHealth_NoMemberships(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	// No fixtures — user doesn't exist as a multicast user, but endpoint
	// should return 200 with empty items rather than 404.

	req := httptest.NewRequest(http.MethodGet, "/api/dz/users/unknown-user/health", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "unknown-user")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()
	api.GetUserHealth(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	var resp handlers.MulticastHealthUserResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "unknown-user", resp.UserPK)
	assert.Empty(t, resp.Items)
}
