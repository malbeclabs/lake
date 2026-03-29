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

func insertDevicesTestData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	// Insert contributors
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_contributors_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name) VALUES
		('contrib-1', now(), now(), generateUUIDv4(), 0, 1, 'contrib-1', 'CONTRIB1', 'Contributor One')
	`)
	require.NoError(t, err)

	// Insert metros
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name, latitude, longitude) VALUES
		('metro-nyc', now(), now(), generateUUIDv4(), 0, 1, 'metro-nyc', 'NYC', 'New York', 40.7128, -74.0060),
		('metro-lax', now(), now(), generateUUIDv4(), 0, 2, 'metro-lax', 'LAX', 'Los Angeles', 34.0522, -118.2437)
	`)
	require.NoError(t, err)

	// Insert devices
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, contributor_pk, metro_pk, public_ip, max_users) VALUES
		('dev-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-1', 'NYC-CORE-01', 'up', 'router', 'contrib-1', 'metro-nyc', '10.0.0.1', 100),
		('dev-2', now(), now(), generateUUIDv4(), 0, 2, 'dev-2', 'NYC-EDGE-01', 'up', 'switch', '', 'metro-nyc', '10.0.0.2', 50),
		('dev-3', now(), now(), generateUUIDv4(), 0, 3, 'dev-3', 'LAX-CORE-01', 'down', 'router', 'contrib-1', 'metro-lax', '10.0.1.1', 100)
	`)
	require.NoError(t, err)

	// Insert users
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id) VALUES
		('user-1', now(), now(), generateUUIDv4(), 0, 1, 'user-1', 'activated', 'dev-1', 'validator', 'pubkey1', '192.168.1.1', '192.168.1.1', 0),
		('user-2', now(), now(), generateUUIDv4(), 0, 2, 'user-2', 'activated', 'dev-1', 'validator', 'pubkey2', '192.168.1.2', '192.168.1.2', 0),
		('user-3', now(), now(), generateUUIDv4(), 0, 3, 'user-3', 'pending', 'dev-1', 'validator', 'pubkey3', '192.168.1.3', '192.168.1.3', 0),
		('user-4', now(), now(), generateUUIDv4(), 0, 4, 'user-4', 'activated', 'dev-3', 'validator', 'pubkey4', '192.168.2.1', '192.168.2.1', 0)
	`)
	require.NoError(t, err)
}

func TestGetDevices_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices", nil)
	rr := httptest.NewRecorder()
	api.GetDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.DeviceListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Items)
	assert.Equal(t, 0, response.Total)
}

func TestGetDevices_ReturnsAllDevices(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices", nil)
	rr := httptest.NewRecorder()
	api.GetDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.DeviceListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 3)
}

func TestGetDevices_IncludesMetroInfo(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices", nil)
	rr := httptest.NewRecorder()
	api.GetDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.DeviceListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find NYC-CORE-01
	var nycDevice *handlers.DeviceListItem
	for i := range response.Items {
		if response.Items[i].Code == "NYC-CORE-01" {
			nycDevice = &response.Items[i]
			break
		}
	}
	require.NotNil(t, nycDevice)
	assert.Equal(t, "metro-nyc", nycDevice.MetroPK)
	assert.Equal(t, "NYC", nycDevice.MetroCode)
	assert.Equal(t, "CONTRIB1", nycDevice.ContributorCode)
}

func TestGetDevices_IncludesUserCounts(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices", nil)
	rr := httptest.NewRecorder()
	api.GetDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.DeviceListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Find NYC-CORE-01 (has 2 activated users, 1 pending)
	var nycDevice *handlers.DeviceListItem
	for i := range response.Items {
		if response.Items[i].Code == "NYC-CORE-01" {
			nycDevice = &response.Items[i]
			break
		}
	}
	require.NotNil(t, nycDevice)
	assert.Equal(t, uint64(2), nycDevice.CurrentUsers) // Only activated users
}

func TestGetDevices_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	// First page
	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	api.GetDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.DeviceListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 2)
	assert.Equal(t, 2, response.Limit)
	assert.Equal(t, 0, response.Offset)

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/api/dz/devices?limit=2&offset=2", nil)
	rr = httptest.NewRecorder()
	api.GetDevices(rr, req)

	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, 2, response.Offset)
}

func TestGetDevices_OrderedByCode(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices", nil)
	rr := httptest.NewRecorder()
	api.GetDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.DeviceListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	// Verify sorted by code
	assert.Equal(t, "LAX-CORE-01", response.Items[0].Code)
	assert.Equal(t, "NYC-CORE-01", response.Items[1].Code)
	assert.Equal(t, "NYC-EDGE-01", response.Items[2].Code)
}

func TestGetDevice_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetDevice(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetDevice_MissingPK(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices/", nil)
	rctx := chi.NewRouteContext()
	// Don't add pk param
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetDevice(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetDevice_ReturnsDetails(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices/dev-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "dev-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetDevice(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var device handlers.DeviceDetail
	err := json.NewDecoder(rr.Body).Decode(&device)
	require.NoError(t, err)

	assert.Equal(t, "dev-1", device.PK)
	assert.Equal(t, "NYC-CORE-01", device.Code)
	assert.Equal(t, "up", device.Status)
	assert.Equal(t, "router", device.DeviceType)
	assert.Equal(t, "metro-nyc", device.MetroPK)
	assert.Equal(t, "NYC", device.MetroCode)
	assert.Equal(t, "New York", device.MetroName)
	assert.Equal(t, "CONTRIB1", device.ContributorCode)
	assert.Equal(t, int32(100), device.MaxUsers)
	assert.Equal(t, uint64(2), device.CurrentUsers) // Only activated users
}

func TestGetDevice_IncludesContributorInfo(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices/dev-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "dev-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetDevice(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var device handlers.DeviceDetail
	err := json.NewDecoder(rr.Body).Decode(&device)
	require.NoError(t, err)

	assert.Equal(t, "contrib-1", device.ContributorPK)
	assert.Equal(t, "CONTRIB1", device.ContributorCode)
}

func TestGetDevice_HandlesNullContributor(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	insertDevicesTestData(t, api)

	// dev-2 has no contributor
	req := httptest.NewRequest(http.MethodGet, "/api/dz/devices/dev-2", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "dev-2")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	api.GetDevice(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var device handlers.DeviceDetail
	err := json.NewDecoder(rr.Body).Decode(&device)
	require.NoError(t, err)

	assert.Equal(t, "", device.ContributorPK)
	assert.Equal(t, "", device.ContributorCode)
}
