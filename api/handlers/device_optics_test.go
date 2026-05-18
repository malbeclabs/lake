package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTelemetryOpticsTables creates the telemetry_mainnet_beta database +
// the two tables the optics handlers read, matching production schema closely
// enough for the handler queries to resolve.
func createTelemetryOpticsTables(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	const db = "telemetry_mainnet_beta"

	err := api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db))
	require.NoError(t, err)

	err = api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.transceiver_state (
			timestamp        DateTime64(9),
			device_pubkey    LowCardinality(String),
			interface_name   String,
			channel_index    UInt16,
			input_power      Float64,
			output_power     Float64,
			laser_bias_current Float64
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, channel_index, timestamp)
	`, db))
	require.NoError(t, err)

	// _latest is a regular table in tests; we insert the same rows we want
	// classified as "current".
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.transceiver_state_latest (
			timestamp        DateTime64(9),
			device_pubkey    LowCardinality(String),
			interface_name   String,
			channel_index    UInt16,
			input_power      Float64,
			output_power     Float64,
			laser_bias_current Float64
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, channel_index)
	`, db))
	require.NoError(t, err)

	err = api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.transceiver_thresholds_latest (
			timestamp                  DateTime64(9),
			device_pubkey              LowCardinality(String),
			interface_name             String,
			severity                   LowCardinality(String),
			input_power_lower          Float64,
			input_power_upper          Float64,
			output_power_lower         Float64,
			output_power_upper         Float64,
			laser_bias_current_lower   Float64,
			laser_bias_current_upper   Float64,
			module_temperature_lower   Float64,
			module_temperature_upper   Float64,
			supply_voltage_lower       Float64,
			supply_voltage_upper       Float64
		) ENGINE = MergeTree
		ORDER BY (device_pubkey, interface_name, severity)
	`, db))
	require.NoError(t, err)
}

func insertOpticsTestDevice(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, contributor_pk, metro_pk, public_ip, max_users)
		VALUES
		('dev-optics', now(), now(), generateUUIDv4(), 0, 1, 'dev-optics', 'NYC-OPTICS-01', 'activated', 'router', '', '', '10.0.0.1', 100)
	`)
	require.NoError(t, err)
}

// withDevicePK returns a request with the pk URL param set on the chi route
// context, matching how the router would invoke the handler.
func withDevicePK(req *http.Request, pk string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("pk", pk)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func TestGetDeviceOptics_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	req := withDevicePK(httptest.NewRequest(http.MethodGet, "/api/dz/devices/missing/optics", nil), "missing")
	rr := httptest.NewRecorder()
	api.GetDeviceOptics(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetDeviceOptics_ClassifiesLanes(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	insertOpticsTestDevice(t, api)
	createTelemetryOpticsTables(t, api)

	ctx := t.Context()

	// State: 3 lanes
	//  - Eth1/ch0 healthy (in=-5, out=-2, bias=30)
	//  - Eth2/ch0 input below CRITICAL (in=-25, out=-2, bias=30)
	//  - Eth3/ch0 has no thresholds (severity = unknown)
	err := api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.transceiver_state_latest
		(timestamp, device_pubkey, interface_name, channel_index, input_power, output_power, laser_bias_current)
		VALUES
		(now(), 'dev-optics', 'Ethernet1', 0, -5,  -2, 30),
		(now(), 'dev-optics', 'Ethernet2', 0, -25, -2, 30),
		(now(), 'dev-optics', 'Ethernet3', 0, -5,  -2, 30)
	`, "`telemetry_mainnet_beta`"))
	require.NoError(t, err)

	// Thresholds for Eth1 and Eth2 only.
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.transceiver_thresholds_latest
		(timestamp, device_pubkey, interface_name, severity,
		 input_power_lower, input_power_upper,
		 output_power_lower, output_power_upper,
		 laser_bias_current_lower, laser_bias_current_upper,
		 module_temperature_lower, module_temperature_upper,
		 supply_voltage_lower, supply_voltage_upper)
		VALUES
		(now(), 'dev-optics', 'Ethernet1', 'CRITICAL', -20, 3, -12, 3, 1, 100, 0, 0, 0, 0),
		(now(), 'dev-optics', 'Ethernet1', 'WARNING',  -14, 0, -8,  0, 2, 90,  0, 0, 0, 0),
		(now(), 'dev-optics', 'Ethernet2', 'CRITICAL', -20, 3, -12, 3, 1, 100, 0, 0, 0, 0),
		(now(), 'dev-optics', 'Ethernet2', 'WARNING',  -14, 0, -8,  0, 2, 90,  0, 0, 0, 0)
	`, "`telemetry_mainnet_beta`"))
	require.NoError(t, err)

	req := withDevicePK(httptest.NewRequest(http.MethodGet, "/api/dz/devices/dev-optics/optics", nil), "dev-optics")
	rr := httptest.NewRecorder()
	api.GetDeviceOptics(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceOpticsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, "dev-optics", resp.DevicePK)
	assert.Equal(t, "NYC-OPTICS-01", resp.DeviceCode)
	require.Len(t, resp.Lanes, 3)

	// Lanes come back ordered by interface name.
	assert.Equal(t, "Ethernet1", resp.Lanes[0].InterfaceName)
	assert.Equal(t, handlers.OpticsSeverityOK, resp.Lanes[0].OverallSeverity)

	assert.Equal(t, "Ethernet2", resp.Lanes[1].InterfaceName)
	assert.Equal(t, handlers.OpticsSeverityCritical, resp.Lanes[1].InputSeverity)
	assert.Equal(t, handlers.OpticsSeverityCritical, resp.Lanes[1].OverallSeverity)

	assert.Equal(t, "Ethernet3", resp.Lanes[2].InterfaceName)
	assert.Equal(t, handlers.OpticsSeverityUnknown, resp.Lanes[2].OverallSeverity)
	assert.Nil(t, resp.Lanes[2].Thresholds)

	assert.Equal(t, 1, resp.Summary.OK)
	assert.Equal(t, 1, resp.Summary.Critical)
	assert.Equal(t, 1, resp.Summary.Unknown)
	assert.Equal(t, 0, resp.Summary.Warning)
	assert.Equal(t, 3, resp.Summary.Total)
}

func TestGetDeviceOpticsHistory_BucketsSamples(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	insertOpticsTestDevice(t, api)
	createTelemetryOpticsTables(t, api)

	ctx := t.Context()
	// 5 samples in the last hour for the same lane.
	err := api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.transceiver_state
		(timestamp, device_pubkey, interface_name, channel_index, input_power, output_power, laser_bias_current)
		VALUES
		(now() - INTERVAL 50 MINUTE, 'dev-optics', 'Ethernet1', 0, -5.0, -2.0, 30),
		(now() - INTERVAL 40 MINUTE, 'dev-optics', 'Ethernet1', 0, -5.5, -2.1, 30),
		(now() - INTERVAL 30 MINUTE, 'dev-optics', 'Ethernet1', 0, -6.0, -2.2, 30),
		(now() - INTERVAL 20 MINUTE, 'dev-optics', 'Ethernet1', 0, -5.8, -2.1, 30),
		(now() - INTERVAL 10 MINUTE, 'dev-optics', 'Ethernet1', 0, -5.2, -2.0, 30),
		(now() - INTERVAL 5  MINUTE, 'dev-optics', 'Ethernet2', 0, -8.0, -3.0, 25)
	`, "`telemetry_mainnet_beta`"))
	require.NoError(t, err)

	req := withDevicePK(httptest.NewRequest(http.MethodGet, "/api/dz/devices/dev-optics/optics/history?interface=Ethernet1&hours=6", nil), "dev-optics")
	rr := httptest.NewRecorder()
	api.GetDeviceOpticsHistory(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceOpticsHistoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, "Ethernet1", resp.InterfaceName)
	assert.Equal(t, 60, resp.BucketSeconds)
	assert.NotEmpty(t, resp.Buckets)
	for _, b := range resp.Buckets {
		assert.LessOrEqual(t, b.MinInputPower, b.AvgInputPower)
		assert.LessOrEqual(t, b.AvgInputPower, b.MaxInputPower)
	}
}

func TestGetDeviceOpticsHistory_RequiresInterface(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	req := withDevicePK(httptest.NewRequest(http.MethodGet, "/api/dz/devices/dev-optics/optics/history", nil), "dev-optics")
	rr := httptest.NewRecorder()
	api.GetDeviceOpticsHistory(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}
