package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertControllerCallTestDevice(t *testing.T, api *handlers.API, pk, code string) {
	t.Helper()
	err := api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, status, device_type, contributor_pk, metro_pk, public_ip, max_users)
		VALUES
		('%[1]s', now(), now(), generateUUIDv4(), 0, 1, '%[1]s', '%[2]s', 'activated', 'router', '', '', '10.0.0.1', 100)
	`, pk, code))
	require.NoError(t, err)
}

func createControllerCallsTable(t *testing.T, api *handlers.API, db string) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db)))
	quotedDB := "`" + db + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.controller_grpc_getconfig_success (
			timestamp DateTime64(3),
			device_pubkey LowCardinality(String)
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (timestamp, device_pubkey)
	`, quotedDB)))
}

func dropControllerCallsTable(t *testing.T, api *handlers.API, db string) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.controller_grpc_getconfig_success", db)))
	t.Cleanup(func() {
		_ = api.DB.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`.controller_grpc_getconfig_success", db))
	})
}

func resetControllerCallsTable(t *testing.T, api *handlers.API, db string) {
	t.Helper()
	dropControllerCallsTable(t, api, db)
	createControllerCallsTable(t, api, db)
}

func insertControllerCallEvents(t *testing.T, api *handlers.API, db, pk string, times ...time.Time) {
	t.Helper()
	query := fmt.Sprintf("INSERT INTO `%s`.controller_grpc_getconfig_success (timestamp, device_pubkey) VALUES (?, ?)", db)
	for _, ts := range times {
		require.NoError(t, api.DB.Exec(t.Context(), query, ts.UTC(), pk))
	}
}

func TestGetDeviceControllerCalls_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB

	req := withDevicePK(httptest.NewRequest(http.MethodGet, "/api/dz/devices/missing/controller-calls", nil), "missing")
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetDeviceControllerCalls_SourceUnavailableReturnsNoData(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["testnet"] = api.Database
	api.EnvDBs["testnet"] = api.DB
	dropControllerCallsTable(t, api, "testnet")
	insertControllerCallTestDevice(t, api, "dev-controller-nodata", "TEST-CONTROLLER-NODATA")

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	url := fmt.Sprintf("/api/dz/devices/dev-controller-nodata/controller-calls?start_time=%d&end_time=%d&bucket=30m", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-nodata")
	req = req.WithContext(handlers.ContextWithEnv(req.Context(), handlers.EnvTestnet))
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceControllerCallsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.False(t, resp.SourceAvailable)
	assert.Equal(t, handlers.ControllerCallStatusNoData, resp.LastStatus)
	assert.Equal(t, 1800, resp.BucketSeconds)
	require.Len(t, resp.Buckets, 4)
	for _, bucket := range resp.Buckets {
		assert.Equal(t, handlers.ControllerCallStatusNoData, bucket.Status)
		assert.Zero(t, bucket.Calls)
	}
}

func TestGetDeviceControllerCalls_ReadsControllerEnvSourceDB(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["testnet"] = api.Database
	api.EnvDBs["testnet"] = api.DB
	insertControllerCallTestDevice(t, api, "dev-controller-env-source", "DEV-CONTROLLER-ENV")
	resetControllerCallsTable(t, api, "telemetry_testnet")
	resetControllerCallsTable(t, api, "testnet")

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	insertControllerCallEvents(t, api, "telemetry_testnet", "dev-controller-env-source", start.Add(10*time.Minute), start.Add(20*time.Minute))
	insertControllerCallEvents(t, api, "testnet", "dev-controller-env-source", start.Add(50*time.Minute))

	url := fmt.Sprintf("/api/dz/devices/dev-controller-env-source/controller-calls?start_time=%d&end_time=%d&bucket=30m", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-env-source")
	req = req.WithContext(handlers.ContextWithEnv(req.Context(), handlers.EnvTestnet))
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceControllerCallsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.True(t, resp.SourceAvailable)
	assert.Equal(t, uint64(1), resp.TotalCalls)
	require.NotNil(t, resp.LastCallAt)
	assert.Equal(t, start.Add(50*time.Minute).Format(time.RFC3339), *resp.LastCallAt)
}

func TestGetDeviceControllerCalls_DoesNotReadTelemetrySourceDB(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["testnet"] = api.Database
	api.EnvDBs["testnet"] = api.DB
	insertControllerCallTestDevice(t, api, "dev-controller-telemetry-source", "TEST-CONTROLLER-TELEMETRY")
	dropControllerCallsTable(t, api, "testnet")
	resetControllerCallsTable(t, api, "telemetry_testnet")

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	insertControllerCallEvents(t, api, "telemetry_testnet", "dev-controller-telemetry-source", start.Add(15*time.Minute))

	url := fmt.Sprintf("/api/dz/devices/dev-controller-telemetry-source/controller-calls?start_time=%d&end_time=%d&bucket=30m", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-telemetry-source")
	req = req.WithContext(handlers.ContextWithEnv(req.Context(), handlers.EnvTestnet))
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceControllerCallsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.False(t, resp.SourceAvailable)
	assert.Equal(t, uint64(0), resp.TotalCalls)
	assert.Nil(t, resp.LastCallAt)
	require.Len(t, resp.Buckets, 2)
	for _, bucket := range resp.Buckets {
		assert.Equal(t, handlers.ControllerCallStatusNoData, bucket.Status)
		assert.Zero(t, bucket.Calls)
	}
}

func TestGetDeviceControllerCalls_StoppedUnlatchesAfterHistoryWindow(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["testnet"] = api.Database
	api.EnvDBs["testnet"] = api.DB
	insertControllerCallTestDevice(t, api, "dev-controller-unlatch", "DEV-CONTROLLER-UNLATCH")
	resetControllerCallsTable(t, api, "testnet")

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(74 * time.Hour)
	historyTS := start.Add(-time.Hour).Format("2006-01-02 15:04:05")
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %[1]s.controller_grpc_getconfig_success
		SELECT toDateTime64('%[2]s', 3), 'dev-controller-unlatch'
		FROM numbers(4001)
	`, "`testnet`", historyTS)))

	url := fmt.Sprintf("/api/dz/devices/dev-controller-unlatch/controller-calls?start_time=%d&end_time=%d&bucket=30m", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-unlatch")
	req = req.WithContext(handlers.ContextWithEnv(req.Context(), handlers.EnvTestnet))
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceControllerCallsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.Buckets, 148)
	assert.Equal(t, handlers.ControllerCallStatusStopped, resp.Buckets[0].Status)
	assert.Equal(t, handlers.ControllerCallStatusStopped, resp.Buckets[141].Status)
	assert.Equal(t, handlers.ControllerCallStatusNoData, resp.Buckets[142].Status)
	assert.Equal(t, handlers.ControllerCallStatusNoData, resp.LastStatus)
}

func TestGetDeviceControllerCalls_GapUsesLastCallBeforeBucketEnd(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["testnet"] = api.Database
	api.EnvDBs["testnet"] = api.DB
	insertControllerCallTestDevice(t, api, "dev-controller-gap", "DEV-CONTROLLER-GAP")
	resetControllerCallsTable(t, api, "testnet")

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	insertControllerCallEvents(t, api, "testnet", "dev-controller-gap", start.Add(-10*time.Second), start.Add(45*time.Second))

	url := fmt.Sprintf("/api/dz/devices/dev-controller-gap/controller-calls?start_time=%d&end_time=%d&bucket=30s", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-gap")
	req = req.WithContext(handlers.ContextWithEnv(req.Context(), handlers.EnvTestnet))
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceControllerCallsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.Buckets, 2)
	require.NotNil(t, resp.Buckets[0].GapSeconds)
	assert.Equal(t, 40, *resp.Buckets[0].GapSeconds)
	require.NotNil(t, resp.Buckets[1].GapSeconds)
	assert.Equal(t, 15, *resp.Buckets[1].GapSeconds)
}

func TestGetDeviceControllerCalls_ClassifiesStoppedAndRecovered(t *testing.T) {
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB
	insertControllerCallTestDevice(t, api, "dev-controller-recovered", "NYC-CONTROLLER-01")
	resetControllerCallsTable(t, api, "mainnet-beta")

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	historyTS := start.Add(-60 * time.Minute).Format("2006-01-02 15:04:05")
	recoveredTS := start.Add(70 * time.Minute).Format("2006-01-02 15:04:05")
	callingTS := start.Add(100 * time.Minute).Format("2006-01-02 15:04:05")

	quotedDB := "`mainnet-beta`"
	require.NoError(t, api.DB.Exec(t.Context(), fmt.Sprintf(`
		INSERT INTO %[1]s.controller_grpc_getconfig_success
		SELECT toDateTime64('%[2]s', 3), 'dev-controller-recovered'
		FROM numbers(4001)
		UNION ALL SELECT toDateTime64('%[3]s', 3), 'dev-controller-recovered'
		UNION ALL SELECT toDateTime64('%[4]s', 3), 'dev-controller-recovered'
	`, quotedDB, historyTS, recoveredTS, callingTS)))

	url := fmt.Sprintf("/api/dz/devices/dev-controller-recovered/controller-calls?start_time=%d&end_time=%d&bucket=30m", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-recovered")
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp handlers.DeviceControllerCallsResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.True(t, resp.SourceAvailable)
	assert.Equal(t, "dev-controller-recovered", resp.DevicePK)
	assert.Equal(t, "NYC-CONTROLLER-01", resp.DeviceCode)
	assert.Equal(t, uint64(2), resp.TotalCalls)
	assert.Equal(t, 30, resp.AlertThresholdMinutes)
	assert.Equal(t, 72, resp.HistoryWindowHours)
	require.Len(t, resp.Buckets, 4)

	assert.Equal(t, handlers.ControllerCallStatusStopped, resp.Buckets[0].Status)
	assert.Equal(t, handlers.ControllerCallStatusStopped, resp.Buckets[1].Status)
	assert.Equal(t, handlers.ControllerCallStatusRecovered, resp.Buckets[2].Status)
	assert.Equal(t, uint64(1), resp.Buckets[2].Calls)
	assert.Equal(t, handlers.ControllerCallStatusCalling, resp.Buckets[3].Status)
	assert.Equal(t, uint64(1), resp.Buckets[3].Calls)
	assert.Equal(t, handlers.ControllerCallStatusCalling, resp.LastStatus)
	require.NotNil(t, resp.LastCallAt)
	assert.Equal(t, start.Add(100*time.Minute).Format(time.RFC3339), *resp.LastCallAt)
}

func TestGetDeviceControllerCalls_InvalidRange(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	api.EnvDatabases["mainnet-beta"] = api.Database
	api.EnvDBs["mainnet-beta"] = api.DB
	insertControllerCallTestDevice(t, api, "dev-controller-invalid", "NYC-CONTROLLER-BAD")

	start := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	end := start.Add(-time.Hour)
	url := fmt.Sprintf("/api/dz/devices/dev-controller-invalid/controller-calls?start_time=%d&end_time=%d", start.Unix(), end.Unix())
	req := withDevicePK(httptest.NewRequest(http.MethodGet, url, nil), "dev-controller-invalid")
	rr := httptest.NewRecorder()
	api.GetDeviceControllerCalls(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "end_time must be after start_time")
}
