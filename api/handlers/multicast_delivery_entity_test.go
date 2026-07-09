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

func deviceMulticastDeliveryRequest(api *handlers.API, pk, query string) (*httptest.ResponseRecorder, handlers.MulticastDeliveryDeviceResponse) {
	url := "/api/dz/devices/" + pk + "/multicast-delivery"
	if query != "" {
		url += "?" + query
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", pk)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetDeviceMulticastDelivery(rr, req)

	var resp handlers.MulticastDeliveryDeviceResponse
	if rr.Code == http.StatusOK {
		_ = json.NewDecoder(rr.Body).Decode(&resp)
	}
	return rr, resp
}

func linkMulticastDeliveryRequest(api *handlers.API, pk, query string) (*httptest.ResponseRecorder, handlers.MulticastDeliveryLinkResponse) {
	url := "/api/dz/links/" + pk + "/multicast-delivery"
	if query != "" {
		url += "?" + query
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", pk)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetLinkMulticastDelivery(rr, req)

	var resp handlers.MulticastDeliveryLinkResponse
	if rr.Code == http.StatusOK {
		_ = json.NewDecoder(rr.Body).Decode(&resp)
	}
	return rr, resp
}

func TestDeviceMulticastDelivery_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	rr, _ := deviceMulticastDeliveryRequest(api, "missing-device", "")

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestLinkMulticastDelivery_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	rr, _ := linkMulticastDeliveryRequest(api, "missing-link", "")

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestDeviceMulticastDelivery_EmptyState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)

	rr, resp := deviceMulticastDeliveryRequest(api, "dev-ams1", "")

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, resp.SourceAvailable)
	assert.Equal(t, "dev-ams1", resp.Device.PK)
	assert.Zero(t, resp.Summary.GroupCount)
	assert.Zero(t, resp.RouteTotal)
	assert.Zero(t, resp.OIFTotal)
	assert.Empty(t, resp.Routes)
	assert.Empty(t, resp.OIFs)
	assert.Contains(t, resp.CoverageNote, "absence is not proof")
}

func TestDeviceMulticastDelivery_ObservedStateAndPagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	refreshDeviceInterfaceIPSMV(t, api)
	insertMulticastDeliveryLink(t, api)
	insertMulticastDeliveryMroute(t, api, "mroute-link", "dev-ams1", "10.0.0.1", `["Ethernet1","Weird0"]`, "now()")
	insertMulticastDeliveryMroute(t, api, "mroute-tunnel", "dev-nyc1", "10.0.0.1", `["Tunnel502"]`, "now()")
	insertMulticastDeliveryMSDP(t, api)

	rr, resp := deviceMulticastDeliveryRequest(api, "dev-ams1", "limit=1")

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, resp.SourceAvailable)
	assert.Equal(t, "fresh", resp.Freshness.Mroute.Status)
	assert.Equal(t, 1, resp.Summary.GroupCount)
	assert.Equal(t, 2, resp.RouteTotal)
	assert.Equal(t, 3, resp.OIFTotal)
	assert.Len(t, resp.Routes, 1)
	assert.Len(t, resp.OIFs, 1)
	assert.Equal(t, "test-group", resp.Groups[0].GroupCode)
	assert.Equal(t, 1, resp.Summary.MSDPPeerCount)
	assert.Equal(t, 2, resp.Summary.MSDPSACount)
	require.Len(t, resp.MSDPPeers, 1)
	assert.Equal(t, "Established", resp.MSDPPeers[0].State)
	require.Len(t, resp.MSDPSAs, 2)

	roles := map[string]handlers.MulticastDeliveryDeviceRole{}
	for _, role := range resp.Roles {
		roles[role.Role] = role
	}
	assert.Contains(t, roles, "publisher_host")
	assert.Contains(t, roles, "transit")
	assert.Contains(t, roles, "control_plane")

	var unknownOIF bool
	for _, anomaly := range resp.Anomalies {
		if anomaly.Kind == "unknown_oif" {
			unknownOIF = true
		}
	}
	assert.True(t, unknownOIF)
}

func TestDeviceMulticastDelivery_HealthFiltersUseHealthSemantics(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	insertMulticastHealthFixtures(t, api)

	rr, resp := deviceMulticastDeliveryRequest(api, "dev-nyc1", "source=10.0.0.1&health=healthy&limit=10&endpoint_limit=10")

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, 1, resp.RouteTotal)
	require.Len(t, resp.Routes, 1)
	assert.Equal(t, "10.0.0.1", resp.Routes[0].SourceAddress)
	assert.Equal(t, 1, resp.HealthUserTotal, "source filters forwarding state; health rows remain device/group health context")
	require.Len(t, resp.HealthUsers, 1)
	assert.Equal(t, "user-sub", resp.HealthUsers[0].UserPK)
	assert.EqualValues(t, 1, resp.Summary.UserHealthCounts.Healthy)
	assert.Equal(t, 1, resp.EndpointHealthTotal)
	assert.EqualValues(t, 1, resp.Summary.EndpointHealthCounts.Healthy)

	rr, endpointFiltered := deviceMulticastDeliveryRequest(api, "dev-ams1", "endpoint_ip=10.0.0.1&health=healthy&limit=10&endpoint_limit=10")
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Equal(t, 1, endpointFiltered.HealthUserTotal)
	require.Len(t, endpointFiltered.HealthUsers, 1)
	assert.Equal(t, "user-pub", endpointFiltered.HealthUsers[0].UserPK)
	assert.Equal(t, 1, endpointFiltered.EndpointHealthTotal)

	rr, _ = deviceMulticastDeliveryRequest(api, "dev-nyc1", "health=broken")
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// disconnected is a valid health filter value (regression: allowlist must
	// include it, matching the disconnected count bucket in the response).
	rr, _ = deviceMulticastDeliveryRequest(api, "dev-nyc1", "health=disconnected")
	assert.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
}

func TestLinkMulticastDelivery_ObservedStateAndDirectionFilter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertMulticastTestData(t, api)
	refreshDeviceInterfaceIPSMV(t, api)
	insertMulticastDeliveryLink(t, api)
	insertMulticastDeliveryMroute(t, api, "mroute-link", "dev-ams1", "10.0.0.1", `["Ethernet1","Weird0"]`, "now()")
	insertMulticastHealthFixtures(t, api)

	rr, resp := linkMulticastDeliveryRequest(api, "link-ams-nyc", "")

	require.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, resp.SourceAvailable)
	assert.Equal(t, "link-ams-nyc", resp.Link.PK)
	assert.Equal(t, 1, resp.BranchTotal)
	assert.Equal(t, 1, resp.Summary.BranchCount)
	assert.Equal(t, 1, resp.Summary.AToZCount)
	assert.Zero(t, resp.Summary.ZToACount)
	require.Len(t, resp.Branches, 1)
	assert.Equal(t, "a_to_z", resp.Branches[0].Direction)
	assert.Equal(t, "underlay_link", resp.Branches[0].OIFKind)
	assert.Equal(t, "test-group", resp.Groups[0].GroupCode)
	assert.Greater(t, resp.Groups[0].HealthCounts.Total, uint64(0))
	assert.Greater(t, resp.Summary.RelatedGroupHealthCounts.Total, uint64(0))

	rr, filtered := linkMulticastDeliveryRequest(api, "link-ams-nyc", "direction=z_to_a")
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Zero(t, filtered.BranchTotal)
	assert.Empty(t, filtered.Branches)
}
