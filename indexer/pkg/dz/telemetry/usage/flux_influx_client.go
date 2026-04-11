package dztelemusage

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// FluxInfluxDBClient implements InfluxDBClient using the InfluxDB v2 HTTP API with Flux queries.
// It uses the /api/v2/query endpoint which has a separate rate-limit budget from the
// Flight SQL endpoint used by SDKInfluxDBClient.
type FluxInfluxDBClient struct {
	client   influxdb2.Client
	queryAPI api.QueryAPI
	bucket   string
}

// NewFluxInfluxDBClient creates a new InfluxDB client that uses Flux queries.
// url is the InfluxDB server URL (e.g. "https://us-east-1-1.aws.cloud2.influxdata.com").
// token is the InfluxDB API token (passed as "Authorization: Token <token>").
// org is the InfluxDB organization name or ID; pass empty string to use the token's default org.
// bucket is the InfluxDB bucket name.
func NewFluxInfluxDBClient(url, token, org, bucket string) (*FluxInfluxDBClient, error) {
	if url == "" {
		return nil, fmt.Errorf("influxdb url is required")
	}
	if token == "" {
		return nil, fmt.Errorf("influxdb token is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("influxdb bucket is required")
	}
	client := influxdb2.NewClient(url, token)
	queryAPI := client.QueryAPI(org)
	return &FluxInfluxDBClient{
		client:   client,
		queryAPI: queryAPI,
		bucket:   bucket,
	}, nil
}

// QueryIntfCounters fetches all interface counter rows for [start, end) using a Flux pivot query.
// The pivot turns per-field rows into one row per (time, device, interface) with all counter
// values as columns — matching the shape returned by the Flight SQL client.
func (c *FluxInfluxDBClient) QueryIntfCounters(ctx context.Context, start, end time.Time) ([]map[string]any, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "intfCounters")
  |> pivot(rowKey: ["_time", "dzd_pubkey", "host", "intf", "model_name", "serial_number"], columnKey: ["_field"], valueColumn: "_value")
`, c.bucket, start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))

	result, err := c.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute flux intf counters query: %w", err)
	}

	var rows []map[string]any
	for result.Next() {
		row := normalizeIntfCounterRow(result.Record().Values(), result.Record().Time())
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating flux intf counters results: %w", err)
	}

	return rows, nil
}

// QueryBaselineCounter fetches the last non-null value of field for each (dzd_pubkey, intf)
// pair in [lookbackStart, windowStart) using a Flux last() query.
// Returns rows with keys: dzd_pubkey, intf, value.
func (c *FluxInfluxDBClient) QueryBaselineCounter(ctx context.Context, field string, lookbackStart, windowStart time.Time) ([]map[string]any, error) {
	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "intfCounters" and r._field == "%s")
  |> filter(fn: (r) => exists r._value)
  |> group(columns: ["dzd_pubkey", "intf"])
  |> last()
  |> keep(columns: ["dzd_pubkey", "intf", "_value"])
`, c.bucket, lookbackStart.UTC().Format(time.RFC3339Nano), windowStart.UTC().Format(time.RFC3339Nano), field)

	result, err := c.queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute flux baseline counter query: %w", err)
	}

	var rows []map[string]any
	for result.Next() {
		row := normalizeBaselineRow(result.Record().Values())
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error iterating flux baseline counter results: %w", err)
	}

	return rows, nil
}

// Close closes the underlying InfluxDB client.
func (c *FluxInfluxDBClient) Close() error {
	c.client.Close()
	return nil
}

// fluxMetaColumns are Flux-internal columns added by the query engine that are not
// part of the InfluxDB data. They are stripped from output rows.
var fluxMetaColumns = map[string]bool{
	"_time":        true,
	"_measurement": true,
	"_start":       true,
	"_stop":        true,
	"result":       true,
	"table":        true,
	"_field":       true,
}

// normalizeIntfCounterRow converts a Flux pivot record to the row format expected by
// convertRowsToUsage. It renames _time → time (as RFC3339Nano string) and strips
// Flux metadata columns.
func normalizeIntfCounterRow(values map[string]interface{}, t time.Time) map[string]any {
	row := make(map[string]any, len(values))
	for k, v := range values {
		if fluxMetaColumns[k] {
			continue
		}
		row[k] = v
	}
	// Set time as RFC3339Nano string so the existing time-parsing code in convertRowsToUsage works.
	row["time"] = t.UTC().Format(time.RFC3339Nano)
	return row
}

// normalizeBaselineRow converts a Flux last() record to the row format expected by
// queryBaselineCounters. It renames _value → value and strips Flux metadata columns.
func normalizeBaselineRow(values map[string]interface{}) map[string]any {
	row := make(map[string]any, len(values))
	for k, v := range values {
		if fluxMetaColumns[k] {
			continue
		}
		if k == "_value" {
			row["value"] = v
		} else {
			row[k] = v
		}
	}
	return row
}
