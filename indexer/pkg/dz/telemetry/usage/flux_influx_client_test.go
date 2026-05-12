package dztelemusage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLake_TelemetryUsage_FluxInfluxDBClient_NewFluxInfluxDBClient(t *testing.T) {
	t.Parallel()

	t.Run("returns error when url is empty", func(t *testing.T) {
		t.Parallel()
		_, err := NewFluxInfluxDBClient("", "token", "org", "bucket")
		require.Error(t, err)
		require.Contains(t, err.Error(), "url is required")
	})

	t.Run("returns error when token is empty", func(t *testing.T) {
		t.Parallel()
		_, err := NewFluxInfluxDBClient("http://localhost:8086", "", "org", "bucket")
		require.Error(t, err)
		require.Contains(t, err.Error(), "token is required")
	})

	t.Run("returns error when bucket is empty", func(t *testing.T) {
		t.Parallel()
		_, err := NewFluxInfluxDBClient("http://localhost:8086", "token", "org", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "bucket is required")
	})

	t.Run("creates client successfully with empty org", func(t *testing.T) {
		t.Parallel()
		client, err := NewFluxInfluxDBClient("http://localhost:8086", "token", "", "bucket")
		require.NoError(t, err)
		require.NotNil(t, client)
		_ = client.Close()
	})

	t.Run("creates client successfully", func(t *testing.T) {
		t.Parallel()
		client, err := NewFluxInfluxDBClient("http://localhost:8086", "token", "myorg", "bucket")
		require.NoError(t, err)
		require.NotNil(t, client)
		_ = client.Close()
	})
}

func TestLake_TelemetryUsage_FluxInfluxDBClient_normalizeIntfCounterRow(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2024, 6, 15, 10, 30, 0, 123456789, time.UTC)

	t.Run("renames _time to time as RFC3339Nano string", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"_time":      fixedTime,
			"dzd_pubkey": "abc123",
			"intf":       "eth0",
			"in-octets":  int64(1000),
		}
		row := normalizeIntfCounterRow(values, fixedTime)

		require.NotNil(t, row["time"])
		timeStr, ok := row["time"].(string)
		require.True(t, ok, "time must be a string")

		parsed, err := time.Parse(time.RFC3339Nano, timeStr)
		require.NoError(t, err)
		require.True(t, parsed.Equal(fixedTime), "parsed time must match original")

		// _time should be stripped
		_, hasUnderscoreTime := row["_time"]
		require.False(t, hasUnderscoreTime, "_time should be removed")
	})

	t.Run("strips flux metadata columns", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"_time":        fixedTime,
			"_measurement": "intfCounters",
			"_start":       fixedTime.Add(-time.Hour),
			"_stop":        fixedTime,
			"result":       "_result",
			"table":        int64(0),
			"_field":       "in-octets",
			"dzd_pubkey":   "abc123",
			"in-octets":    int64(9999),
		}
		row := normalizeIntfCounterRow(values, fixedTime)

		require.NotContains(t, row, "_measurement")
		require.NotContains(t, row, "_start")
		require.NotContains(t, row, "_stop")
		require.NotContains(t, row, "result")
		require.NotContains(t, row, "table")
		require.NotContains(t, row, "_field")
		require.NotContains(t, row, "_time")
	})

	t.Run("preserves tag and field columns", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"_time":               fixedTime,
			"dzd_pubkey":          "pubkey1",
			"host":                "router1",
			"intf":                "eth0",
			"model_name":          "Model X",
			"serial_number":       "SN12345",
			"in-octets":           int64(100000),
			"out-octets":          int64(50000),
			"in-pkts":             int64(200),
			"out-pkts":            int64(150),
			"in-errors":           nil,
			"carrier-transitions": int64(0),
		}
		row := normalizeIntfCounterRow(values, fixedTime)

		require.Equal(t, "pubkey1", row["dzd_pubkey"])
		require.Equal(t, "router1", row["host"])
		require.Equal(t, "eth0", row["intf"])
		require.Equal(t, "Model X", row["model_name"])
		require.Equal(t, "SN12345", row["serial_number"])
		require.Equal(t, int64(100000), row["in-octets"])
		require.Equal(t, int64(50000), row["out-octets"])
		require.Equal(t, int64(200), row["in-pkts"])
		require.Equal(t, int64(150), row["out-pkts"])
		require.Nil(t, row["in-errors"])
		require.Equal(t, int64(0), row["carrier-transitions"])
	})

	t.Run("output is parseable by extractStringFromRow and extractInt64FromRow", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"_time":      fixedTime,
			"dzd_pubkey": "pubkey2",
			"intf":       "eth1",
			"in-octets":  int64(42000),
			"in-errors":  nil,
		}
		row := normalizeIntfCounterRow(values, fixedTime)

		timeVal := extractStringFromRow(row, "time")
		require.NotNil(t, timeVal)
		require.NotEmpty(t, *timeVal)

		devicePK := extractStringFromRow(row, "dzd_pubkey")
		require.NotNil(t, devicePK)
		require.Equal(t, "pubkey2", *devicePK)

		inOctets := extractInt64FromRow(row, "in-octets")
		require.NotNil(t, inOctets)
		require.Equal(t, int64(42000), *inOctets)

		inErrors := extractInt64FromRow(row, "in-errors")
		require.Nil(t, inErrors)
	})

	t.Run("time is parseable by all formats used in convertRowsToUsage", func(t *testing.T) {
		t.Parallel()
		// Test with nanosecond precision
		tNano := time.Date(2024, 1, 15, 12, 0, 0, 987654321, time.UTC)
		row := normalizeIntfCounterRow(map[string]any{"_time": tNano}, tNano)

		timeStr, ok := row["time"].(string)
		require.True(t, ok)

		// Must be parseable by RFC3339Nano (first format tried in convertRowsToUsage)
		parsed, err := time.Parse(time.RFC3339Nano, timeStr)
		require.NoError(t, err)
		require.True(t, parsed.Equal(tNano))
	})
}

func TestLake_TelemetryUsage_FluxInfluxDBClient_normalizeBaselineRow(t *testing.T) {
	t.Parallel()

	t.Run("renames _value to value", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"dzd_pubkey": "abc",
			"intf":       "eth0",
			"_value":     int64(42),
		}
		row := normalizeBaselineRow(values)

		require.Equal(t, int64(42), row["value"])
		require.NotContains(t, row, "_value")
	})

	t.Run("strips flux metadata columns", func(t *testing.T) {
		t.Parallel()
		fixedTime := time.Now().UTC()
		values := map[string]any{
			"_time":        fixedTime,
			"_measurement": "intfCounters",
			"_start":       fixedTime.Add(-time.Hour),
			"_stop":        fixedTime,
			"result":       "_result",
			"table":        int64(0),
			"dzd_pubkey":   "abc",
			"intf":         "eth0",
			"_value":       int64(99),
		}
		row := normalizeBaselineRow(values)

		require.NotContains(t, row, "_time")
		require.NotContains(t, row, "_measurement")
		require.NotContains(t, row, "_start")
		require.NotContains(t, row, "_stop")
		require.NotContains(t, row, "result")
		require.NotContains(t, row, "table")
	})

	t.Run("preserves dzd_pubkey and intf", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"dzd_pubkey": "pubkey123",
			"intf":       "Tunnel501",
			"_value":     int64(7),
		}
		row := normalizeBaselineRow(values)

		require.Equal(t, "pubkey123", row["dzd_pubkey"])
		require.Equal(t, "Tunnel501", row["intf"])
		require.Equal(t, int64(7), row["value"])
	})

	t.Run("output is parseable by extractStringFromRow and extractInt64FromRow", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"dzd_pubkey": "pk1",
			"intf":       "eth0",
			"_value":     int64(55),
		}
		row := normalizeBaselineRow(values)

		devicePK := extractStringFromRow(row, "dzd_pubkey")
		require.NotNil(t, devicePK)
		require.Equal(t, "pk1", *devicePK)

		intf := extractStringFromRow(row, "intf")
		require.NotNil(t, intf)
		require.Equal(t, "eth0", *intf)

		val := extractInt64FromRow(row, "value")
		require.NotNil(t, val)
		require.Equal(t, int64(55), *val)
	})

	t.Run("handles nil _value", func(t *testing.T) {
		t.Parallel()
		values := map[string]any{
			"dzd_pubkey": "pk2",
			"intf":       "eth1",
			"_value":     nil,
		}
		row := normalizeBaselineRow(values)

		val := extractInt64FromRow(row, "value")
		require.Nil(t, val)
	})
}
