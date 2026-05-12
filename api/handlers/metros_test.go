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

func insertMetrosTestData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	// Insert metros
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, latitude, longitude) VALUES
		('metro-nyc', now(), now(), generateUUIDv4(), 0, 1, 'metro-nyc', 'NYC', 'New York', 40.7128, -74.0060),
		('metro-lax', now(), now(), generateUUIDv4(), 0, 2, 'metro-lax', 'LAX', 'Los Angeles', 34.0522, -118.2437),
		('metro-chi', now(), now(), generateUUIDv4(), 0, 3, 'metro-chi', 'CHI', 'Chicago', 41.8781, -87.6298)
	`)
	require.NoError(t, err)

	// Insert devices
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, metro_pk, public_ip, contributor_pk, max_users) VALUES
		('dev-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-1', 'NYC-CORE-01', 'up', 'router', 'metro-nyc', '10.0.0.1', '', 0),
		('dev-2', now(), now(), generateUUIDv4(), 0, 2, 'dev-2', 'NYC-EDGE-01', 'up', 'switch', 'metro-nyc', '10.0.0.2', '', 0),
		('dev-3', now(), now(), generateUUIDv4(), 0, 3, 'dev-3', 'LAX-CORE-01', 'up', 'router', 'metro-lax', '10.0.1.1', '', 0),
		('dev-4', now(), now(), generateUUIDv4(), 0, 4, 'dev-4', 'CHI-CORE-01', 'up', 'router', 'metro-chi', '10.0.2.1', '', 0)
	`)
	require.NoError(t, err)

	// Insert users
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id) VALUES
		('user-1', now(), now(), generateUUIDv4(), 0, 1, 'user-1', 'activated', 'dev-1', 'validator', 'pubkey1', '192.168.1.1', '192.168.1.1', 0),
		('user-2', now(), now(), generateUUIDv4(), 0, 2, 'user-2', 'activated', 'dev-1', 'validator', 'pubkey2', '192.168.1.2', '192.168.1.2', 0),
		('user-3', now(), now(), generateUUIDv4(), 0, 3, 'user-3', 'activated', 'dev-3', 'validator', 'pubkey3', '192.168.2.1', '192.168.2.1', 0),
		('user-4', now(), now(), generateUUIDv4(), 0, 4, 'user-4', 'pending', 'dev-2', 'validator', 'pubkey4', '192.168.1.3', '192.168.1.3', 0)
	`)
	require.NoError(t, err)
}

func TestGetMetros_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	api.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Items)
	assert.Equal(t, 0, response.Total)
}

func TestGetMetros_ReturnsAllMetros(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros?sort_by=code&sort_dir=asc", nil)
	rr := httptest.NewRecorder()
	api.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 3)

	// Verify order (ascending by code)
	assert.Equal(t, "CHI", response.Items[0].Code)
	assert.Equal(t, "LAX", response.Items[1].Code)
	assert.Equal(t, "NYC", response.Items[2].Code)
}

func TestGetMetros_IncludesDeviceCounts(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	api.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find NYC metro (has 2 devices)
	var nycMetro *handlers.MetroListItem
	for i := range response.Items {
		if response.Items[i].Code == "NYC" {
			nycMetro = &response.Items[i]
			break
		}
	}
	require.NotNil(t, nycMetro)
	assert.Equal(t, uint64(2), nycMetro.DeviceCount)
}

func TestGetMetros_IncludesUserCounts(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	api.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find NYC metro (has 2 activated users on dev-1, 1 pending on dev-2)
	var nycMetro *handlers.MetroListItem
	for i := range response.Items {
		if response.Items[i].Code == "NYC" {
			nycMetro = &response.Items[i]
			break
		}
	}
	require.NotNil(t, nycMetro)
	assert.Equal(t, uint64(2), nycMetro.UserCount) // Only activated users
}

func TestGetMetros_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	// First page
	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	api.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 2)
	assert.Equal(t, 2, response.Limit)
	assert.Equal(t, 0, response.Offset)

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/api/dz/metros?limit=2&offset=2", nil)
	rr = httptest.NewRecorder()
	api.GetMetros(rr, req)

	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, 2, response.Offset)
}

func TestGetMetros_IncludesCoordinates(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	api.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find NYC and check coordinates
	var nycMetro *handlers.MetroListItem
	for i := range response.Items {
		if response.Items[i].Code == "NYC" {
			nycMetro = &response.Items[i]
			break
		}
	}
	require.NotNil(t, nycMetro)
	assert.InDelta(t, 40.7128, nycMetro.Latitude, 0.001)
	assert.InDelta(t, -74.0060, nycMetro.Longitude, 0.001)
}

func TestGetMetro_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMetro(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetMetro_MissingPK(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros/", nil)
	rctx := chi.NewRouteContext()
	// Don't add pk param
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMetro(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetMetro_ReturnsDetails(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertMetrosTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros/metro-nyc", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "metro-nyc")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetMetro(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var metro handlers.MetroDetail
	err := json.NewDecoder(rr.Body).Decode(&metro)
	require.NoError(t, err)

	assert.Equal(t, "metro-nyc", metro.PK)
	assert.Equal(t, "NYC", metro.Code)
	assert.Equal(t, "New York", metro.Name)
	assert.Equal(t, uint64(2), metro.DeviceCount)
	assert.Equal(t, uint64(2), metro.UserCount)
}
