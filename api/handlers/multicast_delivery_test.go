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

type multicastDeliveryHandler func(http.ResponseWriter, *http.Request)

func multicastDeliveryRequest[T any](api *handlers.API, group, path, query string, handler multicastDeliveryHandler) (*httptest.ResponseRecorder, T) {
	url := "/api/dz/multicast-groups/" + group + path
	if query != "" {
		url += "?" + query
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", group)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handler(rr, req)

	var resp T
	if rr.Code == http.StatusOK {
		_ = json.NewDecoder(rr.Body).Decode(&resp)
	}
	return rr, resp
}

func mroutesRequest(api *handlers.API, group, query string) (*httptest.ResponseRecorder, handlers.MulticastDeliveryMroutesResponse) {
	return multicastDeliveryRequest[handlers.MulticastDeliveryMroutesResponse](api, group, "/mroutes", query, api.GetMulticastGroupMroutes)
}

func oifsRequest(api *handlers.API, group, query string) (*httptest.ResponseRecorder, handlers.MulticastDeliveryOIFsResponse) {
	return multicastDeliveryRequest[handlers.MulticastDeliveryOIFsResponse](api, group, "/oifs", query, api.GetMulticastGroupOIFs)
}

func msdpRequest(api *handlers.API, group, query string) (*httptest.ResponseRecorder, handlers.MulticastDeliveryMSDPResponse) {
	return multicastDeliveryRequest[handlers.MulticastDeliveryMSDPResponse](api, group, "/msdp", query, api.GetMulticastGroupMSDP)
}

func deliveryTreeRequest(api *handlers.API, group, query string) (*httptest.ResponseRecorder, handlers.MulticastDeliveryTreeResponse) {
	return multicastDeliveryRequest[handlers.MulticastDeliveryTreeResponse](api, group, "/delivery-tree", query, api.GetMulticastGroupDeliveryTree)
}

func insertMulticastDeliveryLink(t *testing.T, api *handlers.API) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_links_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, status, code, tunnel_net, contributor_pk,
			 side_a_pk, side_z_pk, side_a_iface_name, side_z_iface_name,
			 side_a_ip, side_z_ip, link_type, committed_rtt_ns, committed_jitter_ns,
			 bandwidth_bps, isis_delay_override_ns, link_topologies, unicast_drained)
		VALUES
			('link-ams-nyc', now(), now(), generateUUIDv4(), 0, 1,
			 'link-ams-nyc', 'activated', 'ams-nyc', '', '',
			 'dev-ams1', 'dev-nyc1', 'Ethernet1', 'Ethernet2',
			 '', '', 'fiber', 1000000, 0,
			 1000000000, 0, '[]', 0)
	`)
	require.NoError(t, err)
}

func insertMulticastDeliveryMroute(t *testing.T, api *handlers.API, entityID, devicePK, sourceAddress, oifList, snapshotExpr string) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_ip_mroute_entries_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, vrf, mode, group_address, source_address,
			 route_flags, register_in_oif_list,
			 rpf_interface, rpf_rib, rpf_prefix, rpf_preference, rpf_metric,
			 rpf_neighbor, rpf_attached, rpf_has_block,
			 oif_list, oif_count, creation_time)
		SELECT
			?, `+snapshotExpr+`, now(), generateUUIDv4(), 0, 1,
			?, 'default', 'sparse', '233.0.0.1', ?,
			'S', 0,
			'Ethernet0', 'default', '10.0.0.0/24', 110, 10,
			'10.0.0.254', 0, 0,
			?, JSONLength(?), now()
	`, entityID, devicePK, sourceAddress, oifList, oifList)
	require.NoError(t, err)
}

func insertMulticastDeliveryMSDP(t *testing.T, api *handlers.API) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_ip_msdp_peers_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, peer_address, state, session_start_time, sa_count, reset_count)
		VALUES
			('msdp-peer-1', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-ams1', '10.0.0.254', 'Established', now() - INTERVAL 1 HOUR, 1, 0)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_ip_msdp_pim_sa_cache_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, group_address, source_address, rp_address)
		VALUES
			('pim-sa-1', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-ams1', '233.0.0.1', '10.0.0.1', '10.0.0.254')
	`)
	require.NoError(t, err)

	err = api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_ip_msdp_sa_cache_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 device_pubkey, group_address, source_address, remote_address, status, rp_address)
		VALUES
			('sa-1', now(), now(), generateUUIDv4(), 0, 1,
			 'dev-ams1', '233.0.0.1', '10.0.0.1', '10.0.0.2', 'accepted', '10.0.0.254')
	`)
	require.NoError(t, err)
}

func TestGetMulticastGroupMroutes_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	rr, _ := mroutesRequest(api, "missing-group", "")

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestMulticastDeliverySplitEndpoints_NoMrouteState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	rr, mroutes := mroutesRequest(api, "test-group", "")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, mroutes.SourceAvailable)
	assert.Equal(t, "missing", mroutes.Freshness.Mroute.Status)
	assert.Empty(t, mroutes.Items)
	assert.Zero(t, mroutes.Total)

	rr, tree := deliveryTreeRequest(api, "test-group", "")
	require.Equal(t, http.StatusOK, rr.Code)
	require.NotEmpty(t, tree.Anomalies)
	assert.Equal(t, "no_mroute_state", tree.Anomalies[0].Kind)
}

func TestMulticastDeliverySplitEndpoints_ResolveObservedState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastDeliveryLink(t, api)
	insertMulticastDeliveryMroute(t, api, "mroute-link", "dev-ams1", "10.0.0.1", `["Ethernet1","Weird0"]`, "now()")
	insertMulticastDeliveryMroute(t, api, "mroute-tunnel", "dev-nyc1", "10.0.0.1", `["Tunnel502"]`, "now()")
	insertMulticastDeliveryMSDP(t, api)

	rr, mroutes := mroutesRequest(api, "test-group", "")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, mroutes.SourceAvailable)
	assert.Equal(t, "fresh", mroutes.Freshness.Mroute.Status)
	require.Len(t, mroutes.Items, 2)
	assert.Equal(t, "publisher_matched", mroutes.Items[0].SourceMatchStatus)
	assert.NotEmpty(t, mroutes.Items[0].MrouteID)

	rr, pagedMroutes := mroutesRequest(api, "test-group", "limit=1")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, 2, pagedMroutes.Total)
	assert.Len(t, pagedMroutes.Items, 1)

	rr, oifs := oifsRequest(api, "test-group", "")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Len(t, oifs.Items, 3)

	kinds := map[string]handlers.MulticastDeliveryOIF{}
	for _, oif := range oifs.Items {
		kinds[oif.OIFName] = oif
	}
	require.Contains(t, kinds, "Ethernet1")
	assert.Equal(t, "underlay_link", kinds["Ethernet1"].OIFKind)
	assert.Equal(t, "link-ams-nyc", kinds["Ethernet1"].LinkPK)
	assert.Equal(t, "dev-nyc1", kinds["Ethernet1"].PeerDevicePK)

	require.Contains(t, kinds, "Tunnel502")
	assert.Equal(t, "subscriber_tunnel", kinds["Tunnel502"].OIFKind)
	assert.Equal(t, "user-sub", kinds["Tunnel502"].SubscriberUserPK)

	require.Contains(t, kinds, "Weird0")
	assert.Equal(t, "unknown", kinds["Weird0"].OIFKind)

	rr, msdp := msdpRequest(api, "test-group", "kind=all")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "all", msdp.Kind)
	assert.Equal(t, 3, msdp.Total)
	assert.Len(t, msdp.Items, 3)
	seenMSDPKinds := map[string]bool{}
	for _, item := range msdp.Items {
		seenMSDPKinds[item.Kind] = true
	}
	assert.True(t, seenMSDPKinds["peers"])
	assert.True(t, seenMSDPKinds["pim_sa_cache"])
	assert.True(t, seenMSDPKinds["sa_cache"])

	rr, tree := deliveryTreeRequest(api, "test-group", "")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.NotEmpty(t, tree.ObservedSegments)
	assert.NotEmpty(t, tree.ExpectedSegments)
	assert.False(t, tree.Freshness.PIMNeighbors.Available)

	var unknownOIF bool
	for _, anomaly := range tree.Anomalies {
		if anomaly.Kind == "unknown_oif" {
			unknownOIF = true
			assert.Contains(t, anomaly.ObjectIDs, "mroute_id")
		}
	}
	assert.True(t, unknownOIF)
}

func TestGetMulticastGroupMroutes_StaleState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastDeliveryMroute(t, api, "mroute-stale", "dev-ams1", "10.0.0.1", `["Register0"]`, "now() - INTERVAL 10 MINUTE")

	rr, resp := mroutesRequest(api, "test-group", "")

	require.Equal(t, http.StatusOK, rr.Code)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "stale", resp.Items[0].FreshnessStatus)
	assert.Equal(t, "stale", resp.Freshness.Mroute.Status)
}

func TestGetMulticastGroupMroutes_UsesSourceIngestionFreshness(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastDeliveryMroute(t, api, "mroute-unchanged", "dev-ams1", "10.0.0.1", `["Register0"]`, "now() - INTERVAL 2 HOUR")

	err := api.DB.Exec(t.Context(), `
		INSERT INTO log_ingestion_runs
			(run_id, workflow, activity, network, status, started_at, finished_at, duration_ms, rows_affected, error_message)
		VALUES
			(generateUUIDv4(), 'dzingest', 'SyncIPMroute', 'mainnet-beta', 'success', now(), now(), 1000, NULL, NULL)
	`)
	require.NoError(t, err)

	rr, resp := mroutesRequest(api, "test-group", "")

	require.Equal(t, http.StatusOK, rr.Code)
	require.Len(t, resp.Items, 1)
	assert.Equal(t, "fresh", resp.Freshness.Mroute.Status)
	assert.Equal(t, "fresh", resp.Items[0].FreshnessStatus)
	assert.NotNil(t, resp.Freshness.Mroute.LatestSnapshotTS)
	assert.NotNil(t, resp.Freshness.Mroute.LatestIngestedAt)
}
