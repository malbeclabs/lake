package metrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	BuildInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "doublezero_data_indexer_build_info",
			Help: "Build information of the DoubleZero Data Indexer",
		},
		[]string{"version", "commit", "date"},
	)

	ViewRefreshTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_view_refresh_total",
			Help: "Total number of view refreshes (status: success, partial, error, panic; partial = stopped at the refresh budget with backlog pending)",
		},
		[]string{"view_type", "status"},
	)

	// ShredLeafFetchTotal counts validator-rewards leaf-export fetches from the
	// foundation S3 bucket by HTTP outcome. 403 ("forbidden") is treated as
	// "not exported yet" (the public bucket returns 403 for missing keys), so it
	// is no longer logged as an error — this metric is what keeps a real access
	// loss visible: only mainnet ever produces "ok", so a sustained collapse of
	// "ok" to zero, or a 403 where a 200 is expected, indicates the export
	// access was lost rather than the epoch simply being unpublished.
	ShredLeafFetchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_shred_leaf_fetch_total",
			Help: "Total validator-rewards leaf-export fetches from S3 by HTTP outcome (ok, not_found, forbidden, error)",
		},
		[]string{"status"},
	)

	// PermissionEventsSkippedTx counts transactions the permission-events indexer skipped
	// because the RPC would not serve them (getTransaction not-found for a finalized,
	// listed signature — pruned or inconsistent upstream history). Each skip is a
	// potentially missing audit row that no automatic path recovers (the backfill skips
	// them identically); a sustained non-zero rate means upstream retention is dropping
	// events and a manual re-backfill against an archival node is warranted.
	PermissionEventsSkippedTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_permission_events_skipped_tx_total",
			Help: "Permission-events transactions skipped because the RPC could not serve them",
		},
	)

	ViewRefreshDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "doublezero_data_indexer_view_refresh_duration_seconds",
			Help:    "Duration of view refreshes",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~410s (~6.8 minutes)
		},
		[]string{"view_type"},
	)

	// ClickHouse connection pool metrics
	ClickHousePoolOpenConns = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "doublezero_data_indexer_clickhouse_pool_open_connections",
			Help: "Number of open ClickHouse connections",
		},
	)

	ClickHousePoolIdleConns = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "doublezero_data_indexer_clickhouse_pool_idle_connections",
			Help: "Number of idle ClickHouse connections",
		},
	)

	ClickHousePoolMaxOpenConns = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "doublezero_data_indexer_clickhouse_pool_max_open_connections",
			Help: "Maximum number of open ClickHouse connections",
		},
	)

	DatabaseQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_database_queries_total",
			Help: "Total number of database queries",
		},
		[]string{"status"},
	)

	DatabaseQueryDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "doublezero_data_indexer_database_query_duration_seconds",
			Help:    "Duration of database queries",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 0.001s to ~4.1s
		},
	)

	MaintenanceOperationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_maintenance_operation_total",
			Help: "Total number of maintenance operations",
		},
		[]string{"operation_type", "status"},
	)

	MaintenanceOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "doublezero_data_indexer_maintenance_operation_duration_seconds",
			Help:    "Duration of maintenance operations",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s to ~2048s (~34 minutes)
		},
		[]string{"operation_type"},
	)

	MaintenanceTablesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_maintenance_tables_processed_total",
			Help: "Total number of tables processed during maintenance operations",
		},
		[]string{"operation_type", "status"},
	)

	InfluxQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_influx_queries_total",
			Help: "Total number of InfluxDB queries",
		},
		[]string{"dz_env", "query_type", "status"},
	)

	InfluxQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "doublezero_data_indexer_influx_query_duration_seconds",
			Help:    "Duration of InfluxDB queries",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~410s
		},
		[]string{"dz_env", "query_type"},
	)

	InfluxQueryRowsReturned = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "doublezero_data_indexer_influx_query_rows_returned",
			Help:    "Number of rows returned by InfluxDB queries",
			Buckets: prometheus.ExponentialBuckets(1, 4, 12), // 1 to ~4M rows
		},
		[]string{"dz_env", "query_type"},
	)

	// InfluxBaselineFallbackTotal counts how many times the baseline query fell back to InfluxDB
	// because ClickHouse returned 0 rows. High values indicate ClickHouse baseline data is stale
	// or missing, which triggers expensive 10-year InfluxDB scans.
	InfluxBaselineFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_influx_baseline_fallback_total",
			Help: "Total number of times baseline query fell back from ClickHouse to InfluxDB (0 rows from ClickHouse)",
		},
		[]string{"dz_env"},
	)

	// ClickHouseBaselineQueryTotal counts sparse-counter baseline cache misses:
	// each increment is a refresh/backfill that bypassed the in-memory watermark
	// cache and attempted the ClickHouse scan. In steady state this should fire
	// only on indexer restart per env; a high rate means the watermark cache is
	// not hitting.
	ClickHouseBaselineQueryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_clickhouse_baseline_query_total",
			Help: "Total sparse-counter baseline cache misses (watermark cache bypassed, ClickHouse scan attempted)",
		},
		[]string{"dz_env"},
	)

	// TelemetryUsageWatermarkLagSeconds is now − the telemetry-usage ingest
	// watermark (ClickHouse max event_ts), published at the top of every refresh
	// including ones that later fail. A frozen watermark climbs monotonically
	// here from the first failing cycle, which is what makes #740's 22.6h of
	// zero ingest detectable without waiting for the 24h horizon ERROR. One
	// series per dz_env; a single indexer pod publishes all envs it runs.
	//
	// An env with no rows yet has no watermark, so it publishes no series until
	// its first insert.
	TelemetryUsageWatermarkLagSeconds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "doublezero_data_indexer_telemetry_usage_watermark_lag_seconds",
			Help: "Age of the telemetry-usage ingest watermark (now - max event_ts) in seconds",
		},
		[]string{"dz_env"},
	)

	// ClickHousePrevRTTQueryTotal counts previous-RTT cache misses that hit
	// ClickHouse: kind=bounded is the prevRTTLookback-bounded seed scan,
	// kind=fallback is the unbounded per-circuit scan for circuits the bounded
	// pass missed. In steady state both should be ~0 except around indexer
	// restart; a sustained rate means the carry-forward cache is not hitting.
	ClickHousePrevRTTQueryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_clickhouse_prev_rtt_query_total",
			Help: "Previous-RTT cache misses that hit ClickHouse (kind=bounded|fallback)",
		},
		[]string{"dz_env", "table", "kind"}, // table: device_link|internet_metro
	)

	// ClickHousePrevRTTFallbackCircuitsTotal counts the circuits resolved via
	// the unbounded fallback pass. Queries alone can't distinguish one quiet
	// circuit from pathological new-circuit churn; a high circuits-to-queries
	// ratio flags the latter.
	ClickHousePrevRTTFallbackCircuitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "doublezero_data_indexer_clickhouse_prev_rtt_fallback_circuits_total",
			Help: "Circuits resolved via the unbounded previous-RTT fallback query",
		},
		[]string{"dz_env", "table"},
	)
)

// RecordInfluxQuery records metrics for an InfluxDB query.
// dzEnv is the DZ network environment (e.g. "mainnet-beta", "testnet", "devnet").
// queryType describes the kind of query (e.g. "interface_usage", "baseline_in_errors", "backfill").
func RecordInfluxQuery(dzEnv, queryType string, duration time.Duration, rows int, err error) {
	status := "success"
	if err != nil {
		switch {
		case context.DeadlineExceeded == err || isDeadlineExceeded(err):
			status = "timeout"
		case context.Canceled == err || isCanceled(err):
			status = "cancelled"
		default:
			status = "error"
		}
	}
	InfluxQueriesTotal.WithLabelValues(dzEnv, queryType, status).Inc()
	InfluxQueryDuration.WithLabelValues(dzEnv, queryType).Observe(duration.Seconds())
	if err == nil {
		InfluxQueryRowsReturned.WithLabelValues(dzEnv, queryType).Observe(float64(rows))
	}
}

// RecordDatabaseQuery records metrics for a ClickHouse query.
func RecordDatabaseQuery(duration time.Duration, err error) {
	status := "success"
	if err != nil {
		switch {
		case context.DeadlineExceeded == err || isDeadlineExceeded(err):
			status = "timeout"
		case context.Canceled == err || isCanceled(err):
			status = "cancelled"
		default:
			status = "error"
		}
	}
	DatabaseQueriesTotal.WithLabelValues(status).Inc()
	DatabaseQueryDuration.Observe(duration.Seconds())
}

func isDeadlineExceeded(err error) bool {
	for e := err; e != nil; e = unwrapErr(e) {
		if e == context.DeadlineExceeded {
			return true
		}
	}
	return false
}

func isCanceled(err error) bool {
	for e := err; e != nil; e = unwrapErr(e) {
		if e == context.Canceled {
			return true
		}
	}
	return false
}

func unwrapErr(err error) error {
	u, ok := err.(interface{ Unwrap() error })
	if !ok {
		return nil
	}
	return u.Unwrap()
}

// CollectClickHousePoolStats updates connection pool gauges.
// Call this periodically from a background goroutine.
func CollectClickHousePoolStats(stats driver.Stats) {
	ClickHousePoolOpenConns.Set(float64(stats.Open))
	ClickHousePoolIdleConns.Set(float64(stats.Idle))
	ClickHousePoolMaxOpenConns.Set(float64(stats.MaxOpenConns))
}
