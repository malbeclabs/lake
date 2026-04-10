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
			Help: "Total number of view refreshes",
		},
		[]string{"view_type", "status"},
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
