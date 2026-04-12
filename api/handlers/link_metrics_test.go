package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedBulkLinkTestData sets up common dimension data for bulk link metrics tests.
// Returns the truncated "now" time used for rollup bucket alignment.
func seedBulkLinkTestData(t *testing.T, api *handlers.API) time.Time {
	t.Helper()
	seedMetro(t, api, "metro-a", "NYC")
	seedMetro(t, api, "metro-z", "LAX")
	seedContributor(t, api, "contrib-1", "acme")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-a", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-z", 10, "activated")
	return time.Now().UTC().Truncate(5 * time.Minute)
}

// TestQueryLinkRollupSummary verifies the lightweight first-pass query
// executes valid SQL and correctly scans ClickHouse types (Bool, UInt64, Float64).
func TestQueryLinkRollupSummary(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	now := seedBulkLinkTestData(t, api)

	seedLinkMetadata(t, api, "link-healthy", "NYC-LAX-H", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")
	seedLinkMetadata(t, api, "link-loss", "NYC-LAX-L", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")
	seedLinkMetadata(t, api, "link-isis", "NYC-LAX-I", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")

	for i := 0; i < 6; i++ {
		ts := now.Add(-time.Duration(6-i) * 5 * time.Minute)
		seedLinkRollup(t, api, ts, "link-healthy", 100, 100, 0, 0, 90, 90, "activated", false, false)
		seedLinkRollup(t, api, ts, "link-loss", 100, 100, 5.0, 3.0, 90, 90, "activated", false, false)
		seedLinkRollup(t, api, ts, "link-isis", 100, 100, 0, 0, 90, 90, "activated", false, true)
	}

	params := handlers.ExportParseBucketParams("1h", 3)
	result, err := handlers.ExportQueryLinkRollupSummary(t.Context(), api.DB, params)
	require.NoError(t, err)

	assert.Len(t, result, 3)

	// Healthy link: no issue indicators
	h := result["link-healthy"]
	require.NotNil(t, h)
	assert.False(t, h.AnyISISDown)
	assert.False(t, h.AnyDrained)
	assert.Equal(t, float64(0), h.MaxALossPct)
	assert.Equal(t, float64(0), h.MaxZLossPct)
	assert.Equal(t, uint64(6), h.BucketCount)

	// Loss link: has loss indicators
	l := result["link-loss"]
	require.NotNil(t, l)
	assert.False(t, l.AnyISISDown)
	assert.InDelta(t, 5.0, l.MaxALossPct, 0.1)
	assert.InDelta(t, 3.0, l.MaxZLossPct, 0.1)

	// ISIS link: isis_down set
	isis := result["link-isis"]
	require.NotNil(t, isis)
	assert.True(t, isis.AnyISISDown)
}

// TestQueryLinkRollupSummary_Empty verifies the query works with no data.
func TestQueryLinkRollupSummary_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	params := handlers.ExportParseBucketParams("1h", 3)
	result, err := handlers.ExportQueryLinkRollupSummary(t.Context(), api.DB, params)
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestQueryInterfaceIssueLinkPKs verifies the first-pass interface error query
// correctly identifies links with errors and excludes clean links.
func TestQueryInterfaceIssueLinkPKs(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	now := time.Now().UTC().Truncate(5 * time.Minute)

	// link-errors has interface errors, link-clean does not
	seedInterfaceRollup(t, api, now.Add(-10*time.Minute), "dev-a", "Ethernet1/1", "link-errors", "A", 50, 10, 1_000_000, "activated")
	seedInterfaceRollup(t, api, now.Add(-10*time.Minute), "dev-z", "Ethernet1/1", "link-clean", "Z", 0, 0, 500_000, "activated")

	params := handlers.ExportParseBucketParams("1h", 3)
	result, err := handlers.ExportQueryInterfaceIssueLinkPKs(t.Context(), api.DB, params)
	require.NoError(t, err)

	assert.True(t, result["link-errors"], "link with errors should be identified")
	assert.False(t, result["link-clean"], "link without errors should not be identified")
}

// TestQueryInterfaceIssueLinkPKs_Empty verifies the query works with no data.
func TestQueryInterfaceIssueLinkPKs_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	params := handlers.ExportParseBucketParams("1h", 3)
	result, err := handlers.ExportQueryInterfaceIssueLinkPKs(t.Context(), api.DB, params)
	require.NoError(t, err)
	assert.Empty(t, result)
}

// TestGetBulkLinkMetrics_IssuesOnly verifies the has_issues=true endpoint
// returns only links with issues and applies the two-pass filter correctly.
func TestGetBulkLinkMetrics_IssuesOnly(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	now := seedBulkLinkTestData(t, api)

	// Healthy link: no issues
	seedLinkMetadata(t, api, "link-healthy", "NYC-LAX-H", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")
	// Link with packet loss
	seedLinkMetadata(t, api, "link-loss", "NYC-LAX-L", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")

	for i := 0; i < 12; i++ {
		ts := now.Add(-time.Duration(12-i) * 5 * time.Minute)
		seedLinkRollup(t, api, ts, "link-healthy", 100, 100, 0, 0, 90, 90, "activated", false, false)
		seedLinkRollup(t, api, ts, "link-loss", 100, 100, 5.0, 0, 90, 90, "activated", false, false)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/link-metrics?range=24h&include=status&has_issues=true", nil)
	rr := httptest.NewRecorder()
	api.GetBulkLinkMetrics(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.BulkLinkMetricsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Only the link with loss should be returned
	assert.Contains(t, resp.Links, "link-loss", "link with loss should be included")
	assert.NotContains(t, resp.Links, "link-healthy", "healthy link should be excluded")
}

// TestGetBulkLinkMetrics_AllLinks verifies the endpoint without has_issues
// returns all links including healthy ones.
func TestGetBulkLinkMetrics_AllLinks(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	now := seedBulkLinkTestData(t, api)

	seedLinkMetadata(t, api, "link-healthy", "NYC-LAX-H", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")
	seedLinkMetadata(t, api, "link-loss", "NYC-LAX-L", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")

	for i := 0; i < 12; i++ {
		ts := now.Add(-time.Duration(12-i) * 5 * time.Minute)
		seedLinkRollup(t, api, ts, "link-healthy", 100, 100, 0, 0, 90, 90, "activated", false, false)
		seedLinkRollup(t, api, ts, "link-loss", 100, 100, 5.0, 0, 90, 90, "activated", false, false)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/link-metrics?range=24h&include=status", nil)
	rr := httptest.NewRecorder()
	api.GetBulkLinkMetrics(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.BulkLinkMetricsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Both links should be returned
	assert.Contains(t, resp.Links, "link-healthy")
	assert.Contains(t, resp.Links, "link-loss")
}

// TestGetBulkLinkMetrics_IssuesOnly_Empty verifies the endpoint returns an
// empty response when no links have issues.
func TestGetBulkLinkMetrics_IssuesOnly_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	now := seedBulkLinkTestData(t, api)

	seedLinkMetadata(t, api, "link-healthy", "NYC-LAX-H", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated")

	for i := 0; i < 12; i++ {
		ts := now.Add(-time.Duration(12-i) * 5 * time.Minute)
		seedLinkRollup(t, api, ts, "link-healthy", 100, 100, 0, 0, 90, 90, "activated", false, false)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/link-metrics?range=24h&include=status&has_issues=true", nil)
	rr := httptest.NewRecorder()
	api.GetBulkLinkMetrics(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.BulkLinkMetricsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Empty(t, resp.Links)
}

// TestGetBulkLinkMetrics_DrainedLink verifies that drained links are included
// in the issues-only response.
func TestGetBulkLinkMetrics_DrainedLink(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	now := seedBulkLinkTestData(t, api)

	seedLinkMetadata(t, api, "link-drained", "NYC-LAX-D", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "soft-drained")

	for i := 0; i < 12; i++ {
		ts := now.Add(-time.Duration(12-i) * 5 * time.Minute)
		seedLinkRollup(t, api, ts, "link-drained", 100, 100, 0, 0, 90, 90, "soft-drained", false, false)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/link-metrics?range=24h&include=status&has_issues=true", nil)
	rr := httptest.NewRecorder()
	api.GetBulkLinkMetrics(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.BulkLinkMetricsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Contains(t, resp.Links, "link-drained", "drained link should be included in issues")
}
