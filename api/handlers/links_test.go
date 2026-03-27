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

func setupLinksTables(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
}

func insertLinksTestDimensions(t *testing.T) {
	ctx := t.Context()

	seedMetro(t, "metro-nyc", "NYC")
	seedMetro(t, "metro-lax", "LAX")
	seedContributor(t, "contrib-1", "CONTRIB1")
	seedDeviceMetadata(t, "dev-nyc-1", "NYC-CORE-01", "router", "contrib-1", "metro-nyc", 0, "activated")
	seedDeviceMetadata(t, "dev-lax-1", "LAX-CORE-01", "router", "contrib-1", "metro-lax", 0, "activated")
	seedDeviceMetadata(t, "dev-nyc-2", "NYC-EDGE-01", "router", "contrib-1", "metro-nyc", 0, "activated")

	seedLinkMetadata(t, "link-1", "NYC-LAX-001", "backbone", "contrib-1", "dev-nyc-1", "dev-lax-1", 10000000000, 3000000, "up")
	seedLinkMetadata(t, "link-2", "NYC-EDGE-001", "access", "", "dev-nyc-1", "dev-nyc-2", 1000000000, 1000000, "up")
	seedLinkMetadata(t, "link-3", "LAX-INTERNAL", "internal", "", "dev-lax-1", "", 100000000, 0, "down")

	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_dz_links_history FINAL`))
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_dz_devices_history FINAL`))
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_dz_metros_history FINAL`))
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE dim_dz_contributors_history FINAL`))
}

func TestGetLinks_Empty(t *testing.T) {
	setupLinksTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinks(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.LinkListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Items)
	assert.Equal(t, 0, response.Total)
}

func TestGetLinks_ReturnsAllLinks(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinks(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.LinkListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 3)
}

func TestGetLinks_IncludesDeviceInfo(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinks(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.LinkListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find the backbone link
	var backboneLink *handlers.LinkListItem
	for i := range response.Items {
		if response.Items[i].Code == "NYC-LAX-001" {
			backboneLink = &response.Items[i]
			break
		}
	}
	require.NotNil(t, backboneLink)
	assert.Equal(t, "NYC-CORE-01", backboneLink.SideACode)
	assert.Equal(t, "NYC", backboneLink.SideAMetro)
	assert.Equal(t, "LAX-CORE-01", backboneLink.SideZCode)
	assert.Equal(t, "LAX", backboneLink.SideZMetro)
	assert.Equal(t, "CONTRIB1", backboneLink.ContributorCode)
}

func TestGetLinks_Pagination(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	// First page
	req := httptest.NewRequest(http.MethodGet, "/api/dz/links?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinks(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.LinkListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 2)
	assert.Equal(t, 2, response.Limit)
	assert.Equal(t, 0, response.Offset)

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/api/dz/links?limit=2&offset=2", nil)
	rr = httptest.NewRecorder()
	handlers.GetLinks(rr, req)

	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, 2, response.Offset)
}

func TestGetLinks_OrderedByCode(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinks(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.LinkListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Verify sorted by code
	assert.Equal(t, "LAX-INTERNAL", response.Items[0].Code)
	assert.Equal(t, "NYC-EDGE-001", response.Items[1].Code)
	assert.Equal(t, "NYC-LAX-001", response.Items[2].Code)
}

func TestGetLink_NotFound(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetLink(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetLink_MissingPK(t *testing.T) {
	setupLinksTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/", nil)
	rctx := chi.NewRouteContext()
	// Don't add pk param
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetLink(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetLink_ReturnsDetails(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/link-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "link-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetLink(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var link handlers.LinkDetail
	err := json.NewDecoder(rr.Body).Decode(&link)
	require.NoError(t, err)

	assert.Equal(t, "link-1", link.PK)
	assert.Equal(t, "NYC-LAX-001", link.Code)
	assert.Equal(t, "up", link.Status)
	assert.Equal(t, "backbone", link.LinkType)
	assert.Equal(t, int64(10000000000), link.BandwidthBps)
	assert.Equal(t, "NYC-CORE-01", link.SideACode)
	assert.Equal(t, "LAX-CORE-01", link.SideZCode)
}

func setupLinkHealthData(t *testing.T) {
	ctx := t.Context()

	// Insert link rollup data (recent bucket so links are not "dark")
	err := config.DB.Exec(ctx, `
		INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_avg_rtt_us, a_p95_rtt_us, a_loss_pct, a_samples, z_avg_rtt_us, z_p95_rtt_us, z_loss_pct, z_samples) VALUES
		(now() - INTERVAL 5 MINUTE, 'link-1', now(), 1500.0, 2000.0, 0.0, 100, 1500.0, 2000.0, 0.0, 100),
		(now() - INTERVAL 5 MINUTE, 'link-2', now(), 500.0, 800.0, 0.05, 100, 500.0, 800.0, 0.05, 100)
	`)
	require.NoError(t, err)
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE link_rollup_5m FINAL`))
}

func TestGetLinkHealth_Empty(t *testing.T) {
	setupLinksTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/health", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkHealth(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.TopologyLinkHealthResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Links)
	assert.Equal(t, 0, response.TotalLinks)
}

func TestGetLinkHealth_ReturnsHealth(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)
	setupLinkHealthData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/health", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkHealth(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.TopologyLinkHealthResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Only links with both side_a_pk and side_z_pk are returned
	// link-1 and link-2 have both sides, link-3 only has side_a
	assert.Equal(t, 2, response.TotalLinks)
	assert.Len(t, response.Links, 2)
}

func TestGetLinkHealth_CalculatesSlaStatus(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)
	setupLinkHealthData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/health", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkHealth(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.TopologyLinkHealthResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find link-1 (healthy: avg_rtt_us=1500, committed=3000000ns=3000us, ratio=0.5)
	var link1 *handlers.TopologyLinkHealth
	for i := range response.Links {
		if response.Links[i].LinkPK == "link-1" {
			link1 = &response.Links[i]
			break
		}
	}
	require.NotNil(t, link1)
	assert.Equal(t, "healthy", link1.SlaStatus)
	assert.InDelta(t, 0.5, link1.SlaRatio, 0.01) // 1500 / 3000 = 0.5
}

func TestGetLinkHealth_CountsByStatus(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)
	setupLinkHealthData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/health", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkHealth(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.TopologyLinkHealthResponse
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Both links should be healthy based on our test data
	assert.Equal(t, 2, response.HealthyCount)
	assert.Equal(t, 0, response.WarningCount)
	assert.Equal(t, 0, response.CriticalCount)
	assert.Equal(t, 0, response.UnknownCount)
}

func TestGetLinkHealth_IsDownForcesCritical(t *testing.T) {
	setupLinksTables(t)
	insertLinksTestDimensions(t)

	ctx := t.Context()

	// Insert rollup data: link-1 healthy, link-2 is_down (100% loss)
	err := config.DB.Exec(ctx, `
		INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_avg_rtt_us, a_p95_rtt_us, a_loss_pct, a_samples, z_avg_rtt_us, z_p95_rtt_us, z_loss_pct, z_samples) VALUES
		(now() - INTERVAL 5 MINUTE, 'link-1', now(), 1500.0, 2000.0, 0.0, 100, 1500.0, 2000.0, 0.0, 100),
		(now() - INTERVAL 5 MINUTE, 'link-2', now(), 500.0, 800.0, 100.0, 100, 500.0, 800.0, 100.0, 100)
	`)
	require.NoError(t, err)
	require.NoError(t, config.DB.Exec(ctx, `OPTIMIZE TABLE link_rollup_5m FINAL`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/links/health", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkHealth(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.TopologyLinkHealthResponse
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find link-2 (is_down should force critical)
	var link2 *handlers.TopologyLinkHealth
	for i := range response.Links {
		if response.Links[i].LinkPK == "link-2" {
			link2 = &response.Links[i]
			break
		}
	}
	require.NotNil(t, link2)
	assert.True(t, link2.IsDown)
	assert.Equal(t, "critical", link2.SlaStatus)

	// Verify counts
	assert.Equal(t, 1, response.HealthyCount)
	assert.Equal(t, 1, response.CriticalCount)
}
