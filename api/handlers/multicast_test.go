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

func insertMulticastTestData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	// Insert metros
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
			('metro-ams', now(), now(), generateUUIDv4(), 0, 1, 'metro-ams', 'ams', 'Amsterdam'),
			('metro-nyc', now(), now(), generateUUIDv4(), 0, 2, 'metro-nyc', 'nyc', 'New York')
	`)
	require.NoError(t, err)

	// Insert devices
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users)
		VALUES
			('dev-ams1', now(), now(), generateUUIDv4(), 0, 1, 'dev-ams1', 'up', 'edge', 'ams001-dz001', '', '', 'metro-ams', 0),
			('dev-nyc1', now(), now(), generateUUIDv4(), 0, 2, 'dev-nyc1', 'up', 'edge', 'nyc001-dz001', '', '', 'metro-nyc', 0)
	`)
	require.NoError(t, err)

	// Insert multicast group
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('group-1', now(), now(), generateUUIDv4(), 0, 1, 'group-1', '', 'test-group', '233.0.0.1', 100000000, 'activated', 0, 0)
	`)
	require.NoError(t, err)

	// Insert multicast users: one publisher, one subscriber
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers)
		VALUES
			('user-pub', now(), now(), generateUUIDv4(), 0, 1, 'user-pub', 'pubkey-pub', 'activated', 'multicast', '10.0.0.1', '10.0.0.1', 'dev-ams1', 501, '["group-1"]', '[]'),
			('user-sub', now(), now(), generateUUIDv4(), 0, 2, 'user-sub', 'pubkey-sub', 'activated', 'multicast', '10.0.0.2', '10.0.0.2', 'dev-nyc1', 502, '[]', '["group-1"]')
	`)
	require.NoError(t, err)
}

func TestGetMulticastGroups_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups", nil)
	rr := httptest.NewRecorder()
	api.GetMulticastGroups(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var groups []handlers.MulticastGroupListItem
	err := json.NewDecoder(rr.Body).Decode(&groups)
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestGetMulticastGroups_ReturnsRealCounts(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups", nil)
	rr := httptest.NewRecorder()
	api.GetMulticastGroups(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var groups []handlers.MulticastGroupListItem
	err := json.NewDecoder(rr.Body).Decode(&groups)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	// The table has publisher_count=0 / subscriber_count=0, but the enrichment
	// query should compute the real counts from dz_users_current.
	assert.Equal(t, "test-group", groups[0].Code)
	assert.Equal(t, uint32(1), groups[0].PublisherCount, "should compute real publisher count from users")
	assert.Equal(t, uint32(1), groups[0].SubscriberCount, "should compute real subscriber count from users")
}

func TestGetMulticastGroup_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetMulticastGroup_ReturnsMetadataOnly(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var detail handlers.MulticastGroupDetail
	err := json.NewDecoder(rr.Body).Decode(&detail)
	require.NoError(t, err)

	assert.Equal(t, "test-group", detail.Code)
	assert.Equal(t, "233.0.0.1", detail.MulticastIP)
}

func TestGetMulticastGroupMembers_ReturnsMembers(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	// Fetch publishers tab
	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=publishers", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MulticastMembersResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, 1, resp.PublisherCount)
	assert.Equal(t, 1, resp.SubscriberCount)
	require.Len(t, resp.Items, 1)

	pub := resp.Items[0]
	assert.Equal(t, "user-pub", pub.UserPK)
	assert.Equal(t, "P", pub.Mode)
	assert.Equal(t, "ams001-dz001", pub.DeviceCode)
	assert.Equal(t, "ams", pub.MetroCode)
	assert.Equal(t, int32(501), pub.TunnelID)

	// Fetch subscribers tab
	req = httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=subscribers", nil)
	rctx = chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr = httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	sub := resp.Items[0]
	assert.Equal(t, "user-sub", sub.UserPK)
	assert.Equal(t, "S", sub.Mode)
	assert.Equal(t, "nyc001-dz001", sub.DeviceCode)
	assert.Equal(t, "nyc", sub.MetroCode)
	assert.Equal(t, int32(502), sub.TunnelID)
}

func TestGetMulticastGroupMembers_TrafficBps(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	ctx := t.Context()

	// Insert rollup data for both tunnels (recent, within 15 min)
	err := api.DB.Exec(ctx, `
		INSERT INTO device_interface_rollup_5m
			(bucket_ts, device_pk, intf, user_tunnel_id, ingested_at, avg_in_bps, avg_out_bps, avg_in_pps, avg_out_pps)
		VALUES
			(now() - INTERVAL 5 MINUTE, 'dev-ams1', 'tun501', 501, now(), 2000.0, 100000000.0, 10.0, 50.0),
			(now() - INTERVAL 5 MINUTE, 'dev-nyc1', 'tun502', 502, now(), 100000000.0, 2000.0, 50.0, 10.0)
	`)
	require.NoError(t, err)

	// Check publisher traffic
	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=publishers", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MulticastMembersResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Greater(t, resp.Items[0].TrafficBps, float64(0), "publisher should have traffic rate")

	// Check subscriber traffic
	req = httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=subscribers", nil)
	rctx = chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr = httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Greater(t, resp.Items[0].TrafficBps, float64(0), "subscriber should have traffic rate")
}

func TestGetMulticastGroupMembers_TrafficBps_NoCounters(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	// Don't insert any traffic counters — traffic_bps should be 0

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=publishers", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MulticastMembersResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, float64(0), resp.Items[0].TrafficBps, "traffic_bps should be 0 when no counters exist")
}

func TestGetMulticastGroup_MissingCode(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/", nil)
	rctx := chi.NewRouteContext()
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetMulticastGroupMembers_LeaderEnrichment(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	ctx := t.Context()

	// The publisher user has client_ip='10.0.0.1'
	// Insert gossip node mapping: node pubkey -> gossip_ip = client_ip
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
			('node-pubkey-pub', now(), now(), generateUUIDv4(), 0, 1, 'node-pubkey-pub', 0, '10.0.0.1', 0, '', 0, '')
	`)
	require.NoError(t, err)

	// Insert leader schedule: slots include 100 (current), 90 (past), 110 (future)
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_leader_schedule_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, node_pubkey, epoch, slots, slot_count)
		VALUES
			('node-pubkey-pub', now(), now(), generateUUIDv4(), 0, 1, 'node-pubkey-pub', 0, '[90,100,110]', 3)
	`)
	require.NoError(t, err)

	// Insert vote activity with cluster_slot=100 (matches a leader slot)
	err = api.DB.Exec(ctx, `
		INSERT INTO fact_solana_vote_account_activity (event_ts, cluster_slot) VALUES
		(now(), 100)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=publishers", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MulticastMembersResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	pub := resp.Items[0]
	assert.True(t, pub.IsLeader, "publisher should be the current leader")
	assert.Equal(t, "node-pubkey-pub", pub.NodePubkey)
	assert.Equal(t, int64(100), pub.CurrentSlot)
	require.NotNil(t, pub.LastLeaderSlot)
	assert.Equal(t, int64(100), *pub.LastLeaderSlot)
	require.NotNil(t, pub.NextLeaderSlot)
	assert.Equal(t, int64(110), *pub.NextLeaderSlot)

	// Check subscriber has no leader data
	req = httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=subscribers", nil)
	rctx = chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr = httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	sub := resp.Items[0]
	assert.False(t, sub.IsLeader)
	assert.Empty(t, sub.NodePubkey)
}

func TestGetMulticastGroupTraffic_ReturnsTimeSeries(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	ctx := t.Context()

	// Insert traffic counter data for both tunnels (recent, within 1 hour)
	err := api.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, user_tunnel_id, in_octets_delta, out_octets_delta, delta_duration)
		VALUES
			(now() - INTERVAL 30 MINUTE, 'dev-ams1', 501, 1000, 500, 4.0),
			(now() - INTERVAL 15 MINUTE, 'dev-ams1', 501, 2000, 600, 4.0),
			(now() - INTERVAL 15 MINUTE, 'dev-nyc1', 502, 800, 3000, 4.0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/traffic?time_range=1h", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupTraffic(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var points []handlers.MulticastTrafficPoint
	err = json.NewDecoder(rr.Body).Decode(&points)
	require.NoError(t, err)
	assert.NotEmpty(t, points, "should return traffic time series data")

	// Verify we got data for both devices
	devices := map[string]bool{}
	for _, p := range points {
		devices[p.DevicePK] = true
		assert.NotEmpty(t, p.Time, "time should be set")
		assert.NotEmpty(t, p.Mode, "mode should be set")
	}
	assert.True(t, devices["dev-ams1"], "should have publisher device data")
	assert.True(t, devices["dev-nyc1"], "should have subscriber device data")
}

func TestGetMulticastGroupTraffic_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/nonexistent/traffic", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupTraffic(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetMulticastGroupTraffic_NoCounters(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/traffic?time_range=1h", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupTraffic(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var points []handlers.MulticastTrafficPoint
	err := json.NewDecoder(rr.Body).Decode(&points)
	require.NoError(t, err)
	assert.Empty(t, points, "should return empty array when no counters exist")
}

func TestGetMulticastGroupMembers_NoLeader(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	ctx := t.Context()

	// Insert gossip node mapping
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
			('node-pubkey-pub', now(), now(), generateUUIDv4(), 0, 1, 'node-pubkey-pub', 0, '10.0.0.1', 0, '', 0, '')
	`)
	require.NoError(t, err)

	// Insert leader schedule: slots 80, 90, 110 — current slot 100 is NOT in the list
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_leader_schedule_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, node_pubkey, epoch, slots, slot_count)
		VALUES
			('node-pubkey-pub', now(), now(), generateUUIDv4(), 0, 1, 'node-pubkey-pub', 0, '[80,90,110]', 3)
	`)
	require.NoError(t, err)

	// Insert vote activity with cluster_slot=100
	err = api.DB.Exec(ctx, `
		INSERT INTO fact_solana_vote_account_activity (event_ts, cluster_slot) VALUES
		(now(), 100)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=publishers", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MulticastMembersResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	pub := resp.Items[0]
	assert.False(t, pub.IsLeader, "publisher should not be the leader")
	assert.Equal(t, "node-pubkey-pub", pub.NodePubkey)
	assert.Equal(t, int64(100), pub.CurrentSlot)
	require.NotNil(t, pub.LastLeaderSlot)
	assert.Equal(t, int64(90), *pub.LastLeaderSlot)
	require.NotNil(t, pub.NextLeaderSlot)
	assert.Equal(t, int64(110), *pub.NextLeaderSlot)
}

func TestGetMulticastGroupMembers_ValidatorEnrichment(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	ctx := t.Context()

	// Insert gossip node for publisher
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_solana_gossip_nodes_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pubkey, epoch, gossip_ip, gossip_port, tpuquic_ip, tpuquic_port, version)
		VALUES
			('node-pub-1', now(), now(), generateUUIDv4(), 0, 1, 'node-pub-1', 0, '10.0.0.1', 0, '', 0, '')
	`)
	require.NoError(t, err)

	// Insert vote account for the gossip node
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
			('vote-pub-1', now(), now(), generateUUIDv4(), 0, 1,
			 'vote-pub-1', 0, 'node-pub-1', 5000000000000, 'true', 0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group/members?tab=publishers", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMulticastGroupMembers(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MulticastMembersResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	pub := resp.Items[0]
	assert.Equal(t, "node-pub-1", pub.NodePubkey, "should resolve node_pubkey from gossip")
	assert.Equal(t, "vote-pub-1", pub.VotePubkey, "should resolve vote_pubkey from vote accounts")
	assert.Equal(t, float64(5000), pub.StakeSol, "should resolve stake from vote accounts")
}
