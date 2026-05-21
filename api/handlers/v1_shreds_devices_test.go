package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apitesting "github.com/malbeclabs/lake/api/testing"
	v1 "github.com/malbeclabs/lake/api/v1"
)

// v1EdgeShredsDevicesContractFields is the authoritative JSON key set for the
// /api/v1/edge/shreds/devices response. A mismatch means the public contract
// has changed — bump the API version.
var v1EdgeShredsDevicesContractFields = struct {
	top    []string
	device []string
}{
	top: []string{"items", "total", "limit", "offset", "$schema"},
	device: []string{
		"device_key",
		"device_code",
		"metro_exchange_key",
		"metro_code",
		"is_enabled",
		"base_price_dollars",
		"premium_dollars",
		"total_price_dollars",
		"granted_seats",
		"capacity",
		"available_seats",
	},
}

func TestV1EdgeShredsDevices_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/devices", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var resp v1.EdgeShredsDevicesResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Items)
	assert.Equal(t, 0, resp.Total)
}

func TestV1EdgeShredsDevices_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/devices", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assertJSONKeys(t, raw, v1EdgeShredsDevicesContractFields.top, "response")

	items, ok := raw["items"].([]any)
	require.True(t, ok, "items must be a JSON array")
	require.NotEmpty(t, items, "test data should produce devices")
	for i, it := range items {
		obj, ok := it.(map[string]any)
		require.True(t, ok, "items[%d] must be a JSON object", i)
		assertJSONKeys(t, obj, v1EdgeShredsDevicesContractFields.device, "items[i]")
	}
}

func TestV1EdgeShredsDevices_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/devices", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsDevicesResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 1, resp.Total)
	require.Len(t, resp.Items, 1)

	d := resp.Items[0]
	assert.Equal(t, "dev-1", d.DeviceKey)
	assert.Equal(t, "NYC-CORE-01", d.DeviceCode)
	assert.Equal(t, "metro-nyc", d.MetroExchangeKey)
	assert.Equal(t, "NYC", d.MetroCode)
	assert.Equal(t, uint8(1), d.IsEnabled)
	assert.Equal(t, uint16(10), d.BasePriceDollars)
	assert.Equal(t, int16(-2), d.PremiumDollars)
	// base (10) + premium (-2) = 8
	assert.Equal(t, int64(8), d.TotalPriceDollars)
	assert.Equal(t, uint16(2), d.GrantedSeats)
	assert.Equal(t, uint16(10), d.Capacity)
	// capacity (10) - granted (2) = 8
	assert.Equal(t, int64(8), d.AvailableSeats)
}

func TestV1EdgeShredsDevices_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/devices?limit=1&offset=0", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsDevicesResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 1, resp.Limit)
	assert.Equal(t, 0, resp.Offset)
	require.Len(t, resp.Items, 1)

	// Offset past end returns empty page.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/devices?limit=1&offset=5", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 1, resp.Total)
	assert.Equal(t, 5, resp.Offset)
	assert.Empty(t, resp.Items)
}
