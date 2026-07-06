package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// insertServiceFilterTopology seeds a 4-device topology exercising both unicast
// filter conditions:
//
//	A --[tagged, not drained]-- B --[untagged]-- C
//	A --[tagged, drained]------ D
//
// Under multicast (algo 0) all three links are usable. Under unicast (flex-algo
// 128) only A-B survives: B-C is untagged and A-D is drained.
func insertServiceFilterTopology(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, metro_pk, public_ip, contributor_pk, max_users) VALUES
		('dev-a', now(), now(), generateUUIDv4(), 0, 1, 'dev-a', 'DEV-A', 'up', 'router', '', '10.0.0.1', '', 0),
		('dev-b', now(), now(), generateUUIDv4(), 0, 2, 'dev-b', 'DEV-B', 'up', 'router', '', '10.0.0.2', '', 0),
		('dev-c', now(), now(), generateUUIDv4(), 0, 3, 'dev-c', 'DEV-C', 'up', 'router', '', '10.0.0.3', '', 0),
		('dev-d', now(), now(), generateUUIDv4(), 0, 4, 'dev-d', 'DEV-D', 'up', 'router', '', '10.0.0.4', '', 0)
	`)
	require.NoError(t, err)

	// committed_rtt_ns is 3ms (must not equal the 1s sentinel loadTopologyGraph excludes).
	// link_topologies / unicast_drained are the RFC-18 columns the unicast filter keys on.
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_links_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, link_type, bandwidth_bps, side_a_pk, side_z_pk, contributor_pk, committed_rtt_ns, tunnel_net, side_a_iface_name, side_z_iface_name, committed_jitter_ns, isis_delay_override_ns, link_topologies, unicast_drained) VALUES
		('link-ab', now(), now(), generateUUIDv4(), 0, 1, 'link-ab', 'A-B', 'activated', 'backbone', 10000000000, 'dev-a', 'dev-b', '', 3000000, '', '', '', 0, 0, '["UNICAST-DEFAULT"]', 0),
		('link-bc', now(), now(), generateUUIDv4(), 0, 2, 'link-bc', 'B-C', 'activated', 'backbone', 10000000000, 'dev-b', 'dev-c', '', 3000000, '', '', '', 0, 0, '[]', 0),
		('link-ad', now(), now(), generateUUIDv4(), 0, 3, 'link-ad', 'A-D', 'activated', 'backbone', 10000000000, 'dev-a', 'dev-d', '', 3000000, '', '', '', 0, 0, '["UNICAST-DEFAULT"]', 1)
	`)
	require.NoError(t, err)
}

func getISISPaths(t *testing.T, api *handlers.API, from, to, service string) handlers.MultiPathResponse {
	t.Helper()
	url := fmt.Sprintf("/api/topology/paths?from=%s&to=%s", from, to)
	if service != "" {
		url += "&service=" + service
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	api.GetISISPaths(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MultiPathResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return resp
}

func TestGetISISPaths_ServiceTopologyFiltering(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	insertServiceFilterTopology(t, api)

	tests := []struct {
		name       string
		from, to   string
		service    string
		expectPath bool
	}{
		// A-B is tagged and not drained: reachable under both services.
		{"unicast direct tagged link", "dev-a", "dev-b", "unicast", true},
		{"multicast direct tagged link", "dev-a", "dev-b", "multicast", true},

		// A->C traverses the untagged B-C link: multicast only.
		{"multicast via untagged link", "dev-a", "dev-c", "multicast", true},
		{"unicast excludes untagged link", "dev-a", "dev-c", "unicast", false},

		// A-D is tagged but drained from unicast topologies: multicast only.
		{"multicast via drained link", "dev-a", "dev-d", "multicast", true},
		{"unicast excludes drained link", "dev-a", "dev-d", "unicast", false},

		// No service param defaults to all-links behavior (algo 0).
		{"unset service includes untagged link", "dev-a", "dev-c", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := getISISPaths(t, api, tc.from, tc.to, tc.service)
			if tc.expectPath {
				assert.Empty(t, resp.Error)
				assert.NotEmpty(t, resp.Paths, "expected a path for %s->%s (%s)", tc.from, tc.to, tc.service)
			} else {
				assert.Empty(t, resp.Paths, "expected no path for %s->%s (%s)", tc.from, tc.to, tc.service)
			}
		})
	}
}

func TestGetISISPaths_InvalidServiceRejected(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	resp := getISISPaths(t, api, "dev-a", "dev-b", "bogus")
	assert.Contains(t, resp.Error, "service must be")
	assert.Empty(t, resp.Paths)
}

// GetMetroDevicePaths validates service the same way GetISISPaths does; a bad
// value is rejected before any topology work.
func TestGetMetroDevicePaths_InvalidServiceRejected(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/topology/metro-device-paths?from=metro-a&to=metro-b&service=bogus", nil)
	rr := httptest.NewRecorder()
	api.GetMetroDevicePaths(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.MetroDevicePathsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Contains(t, resp.Error, "service must be")
}
