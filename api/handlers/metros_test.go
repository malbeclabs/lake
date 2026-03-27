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

func setupMetrosTables(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
}

func insertMetrosTestData(t *testing.T) {
	ctx := t.Context()

	// Insert metros with names and coordinates
	for _, m := range []struct {
		pk, code, name string
		lat, lon       float64
	}{
		{"metro-nyc", "NYC", "New York", 40.7128, -74.0060},
		{"metro-lax", "LAX", "Los Angeles", 34.0522, -118.2437},
		{"metro-chi", "CHI", "Chicago", 41.8781, -87.6298},
	} {
		require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_metros_history (
			entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, code, name, latitude, longitude
		) VALUES ($1, now(), now(), $2, 0, $3, $4, $5, $6, $7)`,
			m.pk, "00000000-0000-0000-0000-000000000001", m.pk, m.code, m.name, m.lat, m.lon))
	}

	// Insert devices
	seedDeviceMetadata(t, "dev-1", "NYC-CORE-01", "router", "", "metro-nyc", 0, "activated")
	seedDeviceMetadata(t, "dev-2", "NYC-EDGE-01", "switch", "", "metro-nyc", 0, "activated")
	seedDeviceMetadata(t, "dev-3", "LAX-CORE-01", "router", "", "metro-lax", 0, "activated")
	seedDeviceMetadata(t, "dev-4", "CHI-CORE-01", "router", "", "metro-chi", 0, "activated")

	// Insert users
	for _, u := range []struct{ pk, status, devicePK, clientIP string }{
		{"user-1", "activated", "dev-1", "192.168.1.1"},
		{"user-2", "activated", "dev-1", "192.168.1.2"},
		{"user-3", "activated", "dev-3", "192.168.2.1"},
		{"user-4", "pending", "dev-2", "192.168.1.3"},
	} {
		require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_users_history (
			entity_id, snapshot_ts, ingested_at, op_id, is_deleted,
			pk, status, device_pk, client_ip, dz_ip, kind, owner_pubkey
		) VALUES ($1, now(), now(), $2, 0, $3, $4, $5, $6, $7, 'validator', 'pubkey')`,
			u.pk, "00000000-0000-0000-0000-000000000001", u.pk, u.status, u.devicePK, u.clientIP, u.clientIP))
	}

	for _, table := range []string{"dim_dz_metros_history", "dim_dz_devices_history", "dim_dz_users_history"} {
		require.NoError(t, config.DB.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"))
	}
}

func TestGetMetros_Empty(t *testing.T) {
	setupMetrosTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Items)
	assert.Equal(t, 0, response.Total)
}

func TestGetMetros_ReturnsAllMetros(t *testing.T) {
	setupMetrosTables(t)
	insertMetrosTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetros(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.MetroListItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 3)

	// Verify order (should be by code)
	assert.Equal(t, "CHI", response.Items[0].Code)
	assert.Equal(t, "LAX", response.Items[1].Code)
	assert.Equal(t, "NYC", response.Items[2].Code)
}

func TestGetMetros_IncludesDeviceCounts(t *testing.T) {
	setupMetrosTables(t)
	insertMetrosTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetros(rr, req)

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
	setupMetrosTables(t)
	insertMetrosTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetros(rr, req)

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
	setupMetrosTables(t)
	insertMetrosTestData(t)

	// First page
	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetros(rr, req)

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
	handlers.GetMetros(rr, req)

	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, 2, response.Offset)
}

func TestGetMetros_IncludesCoordinates(t *testing.T) {
	setupMetrosTables(t)
	insertMetrosTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros", nil)
	rr := httptest.NewRecorder()
	handlers.GetMetros(rr, req)

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
	setupMetrosTables(t)
	insertMetrosTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMetro(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetMetro_MissingPK(t *testing.T) {
	setupMetrosTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros/", nil)
	rctx := chi.NewRouteContext()
	// Don't add pk param
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMetro(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetMetro_ReturnsDetails(t *testing.T) {
	setupMetrosTables(t)
	insertMetrosTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/metros/metro-nyc", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", "metro-nyc")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMetro(rr, req)

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
