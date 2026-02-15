package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// --- Shared helpers ---

// dashboardTimeRange converts a time range string (e.g., "12h") to a ClickHouse interval.
func dashboardTimeRange(tr string) string {
	switch tr {
	case "1h":
		return "1 HOUR"
	case "3h":
		return "3 HOUR"
	case "6h":
		return "6 HOUR"
	case "12h":
		return "12 HOUR"
	case "24h":
		return "24 HOUR"
	case "3d":
		return "3 DAY"
	case "7d":
		return "7 DAY"
	case "14d":
		return "14 DAY"
	case "30d":
		return "30 DAY"
	default:
		return "12 HOUR"
	}
}

// parseBucket converts a user-facing bucket string to a ClickHouse interval.
func parseBucket(b string) string {
	switch b {
	case "10s":
		return "10 SECOND"
	case "30s":
		return "30 SECOND"
	case "1m":
		return "1 MINUTE"
	case "5m":
		return "5 MINUTE"
	case "10m":
		return "10 MINUTE"
	case "15m":
		return "15 MINUTE"
	case "30m":
		return "30 MINUTE"
	case "1h":
		return "1 HOUR"
	default:
		return ""
	}
}

// effectiveBucket returns a sensible bucket for the given time range if none specified.
func effectiveBucket(timeRange, bucket string) string {
	if bucket != "" {
		return bucket
	}
	switch timeRange {
	case "1h":
		return "10 SECOND"
	case "3h":
		return "30 SECOND"
	case "6h", "12h":
		return "1 MINUTE"
	case "24h":
		return "5 MINUTE"
	case "3d":
		return "10 MINUTE"
	case "7d":
		return "30 MINUTE"
	case "14d", "30d":
		return "1 HOUR"
	default:
		return "1 MINUTE"
	}
}

// bucketForDuration returns a sensible bucket interval for the given duration.
func bucketForDuration(d time.Duration) string {
	switch {
	case d < 3*time.Hour:
		return "10 SECOND"
	case d < 12*time.Hour:
		return "1 MINUTE"
	case d < 3*24*time.Hour:
		return "5 MINUTE"
	case d < 7*24*time.Hour:
		return "30 MINUTE"
	default:
		return "1 HOUR"
	}
}

// dashboardTimeFilter extracts time filter and bucket interval from the request.
// When start_time and end_time query params are present (unix seconds), it returns
// an absolute time filter. Otherwise it falls back to the preset time_range param.
func dashboardTimeFilter(r *http.Request) (timeFilter string, bucketInterval string) {
	startStr := r.URL.Query().Get("start_time")
	endStr := r.URL.Query().Get("end_time")

	if startStr != "" && endStr != "" {
		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 == nil && err2 == nil && end > start {
			timeFilter = fmt.Sprintf("event_ts BETWEEN toDateTime(%d) AND toDateTime(%d)", start, end)
			duration := time.Duration(end-start) * time.Second
			bucketInterval = bucketForDuration(duration)
			if bp := parseBucket(r.URL.Query().Get("bucket")); bp != "" {
				bucketInterval = bp
			}
			return
		}
	}

	timeRange := r.URL.Query().Get("time_range")
	if timeRange == "" {
		timeRange = "12h"
	}
	rangeInterval := dashboardTimeRange(timeRange)
	timeFilter = fmt.Sprintf("event_ts >= now() - INTERVAL %s", rangeInterval)

	bucketParam := parseBucket(r.URL.Query().Get("bucket"))
	bucketInterval = effectiveBucket(timeRange, bucketParam)
	return
}

// buildDimensionFilters builds SQL WHERE clauses for dimension filters.
// It returns:
//   - filterSQL: clauses for dimension tables (m, d, l, co) with leading AND
//   - intfFilterSQL: clause for f.intf with leading AND (must go in the CTE where f is in scope)
//   - intfTypeSQL: clause for interface type filtering with leading AND (must go in CTE)
//   - join flags indicating which dimension joins are needed
func buildDimensionFilters(r *http.Request) (filterSQL, intfFilterSQL, intfTypeSQL string, needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin bool) {
	var clauses []string

	if metros := r.URL.Query().Get("metro"); metros != "" {
		needsDeviceJoin = true
		needsMetroJoin = true
		vals := strings.Split(metros, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		clauses = append(clauses, fmt.Sprintf("m.code IN (%s)", strings.Join(quoted, ",")))
	}

	if devices := r.URL.Query().Get("device"); devices != "" {
		needsDeviceJoin = true
		vals := strings.Split(devices, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		clauses = append(clauses, fmt.Sprintf("d.code IN (%s)", strings.Join(quoted, ",")))
	}

	if linkTypes := r.URL.Query().Get("link_type"); linkTypes != "" {
		needsLinkJoin = true
		vals := strings.Split(linkTypes, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		clauses = append(clauses, fmt.Sprintf("l.link_type IN (%s)", strings.Join(quoted, ",")))
	}

	if contributors := r.URL.Query().Get("contributor"); contributors != "" {
		needsDeviceJoin = true
		needsContributorJoin = true
		vals := strings.Split(contributors, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		clauses = append(clauses, fmt.Sprintf("co.code IN (%s)", strings.Join(quoted, ",")))
	}

	if intfs := r.URL.Query().Get("intf"); intfs != "" {
		vals := strings.Split(intfs, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		intfFilterSQL = fmt.Sprintf(" AND f.intf IN (%s)", strings.Join(quoted, ","))
	}

	intfTypeSQL = buildIntfTypeFilter(r.URL.Query().Get("intf_type"))

	if len(clauses) > 0 {
		filterSQL = " AND " + strings.Join(clauses, " AND ")
	}
	return
}

// buildIntfTypeFilter returns a SQL clause for filtering by interface type.
// Values: "all" (no filter), "link" (link interfaces), "tunnel" (tunnel interfaces),
// "other" (neither link nor tunnel). Default is "all".
func buildIntfTypeFilter(intfType string) string {
	switch intfType {
	case "link":
		return " AND f.link_pk != ''"
	case "tunnel":
		return " AND f.user_tunnel_id IS NOT NULL"
	case "other":
		return " AND f.link_pk = '' AND f.user_tunnel_id IS NULL"
	default:
		// "all" or empty: no filter
		return ""
	}
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// --- Query builders (exported for testing) ---

// BuildStressQuery builds the ClickHouse query for the stress endpoint.
func BuildStressQuery(timeFilter, bucketInterval, metric, groupBy, filterSQL, intfFilterSQL, intfTypeSQL string, threshold float64,
	needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin bool) (query string, grouped bool) {

	// Determine group_by column and required joins
	var groupBySelect string
	switch groupBy {
	case "metro":
		needsDeviceJoin = true
		needsMetroJoin = true
		grouped = true
		groupBySelect = ", m.code AS group_key, m.name AS group_label"
	case "device":
		needsDeviceJoin = true
		grouped = true
		groupBySelect = ", d.code AS group_key, d.code AS group_label"
	case "link_type":
		grouped = true
		groupBySelect = ", l.link_type AS group_key, l.link_type AS group_label"
	case "contributor":
		needsDeviceJoin = true
		needsContributorJoin = true
		grouped = true
		groupBySelect = ", co.code AS group_key, co.name AS group_label"
	}

	// Build dimension join clauses
	var dimJoins string
	if needsDeviceJoin {
		dimJoins += " INNER JOIN dz_devices_current d ON ir.device_pk = d.pk"
	}
	// Always need link join for utilization metric
	dimJoins += " LEFT JOIN dz_links_current l ON ir.link_pk = l.pk"
	if needsMetroJoin {
		dimJoins += " LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk"
	}
	if needsContributorJoin {
		dimJoins += " LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk"
	}

	// Build the metric expressions (separate in/out) and filter
	var metricExprIn, metricExprOut, metricFilter string
	switch metric {
	case "throughput":
		metricExprIn = "ir.in_bps"
		metricExprOut = "ir.out_bps"
	case "packets":
		metricExprIn = "ir.in_pps"
		metricExprOut = "ir.out_pps"
	default: // utilization
		metricExprIn = `CASE WHEN l.bandwidth_bps > 0
			THEN ir.in_bps / l.bandwidth_bps
			ELSE NULL END`
		metricExprOut = `CASE WHEN l.bandwidth_bps > 0
			THEN ir.out_bps / l.bandwidth_bps
			ELSE NULL END`
		metricFilter = " AND metric_val_in IS NOT NULL"
	}

	// Build percentile select
	var selectCols, groupByCols string
	if grouped {
		selectCols = fmt.Sprintf(`
			formatDateTime(bucket_ts, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS ts,
			group_key, group_label,
			quantile(0.5)(metric_val_in) AS p50_in,
			quantile(0.95)(metric_val_in) AS p95_in,
			max(metric_val_in) AS max_in,
			quantile(0.5)(metric_val_out) AS p50_out,
			quantile(0.95)(metric_val_out) AS p95_out,
			max(metric_val_out) AS max_out,
			countIf(greatest(metric_val_in, metric_val_out) >= %f) AS stressed_count,
			count() AS total_count`, threshold)
		groupByCols = "bucket_ts, group_key, group_label"
	} else {
		selectCols = fmt.Sprintf(`
			formatDateTime(bucket_ts, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS ts,
			quantile(0.5)(metric_val_in) AS p50_in,
			quantile(0.95)(metric_val_in) AS p95_in,
			max(metric_val_in) AS max_in,
			quantile(0.5)(metric_val_out) AS p50_out,
			quantile(0.95)(metric_val_out) AS p95_out,
			max(metric_val_out) AS max_out,
			countIf(greatest(metric_val_in, metric_val_out) >= %f) AS stressed_count,
			count() AS total_count`, threshold)
		groupByCols = "bucket_ts"
	}

	query = fmt.Sprintf(`
		WITH interface_rates AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL %s) AS bucket_ts,
				f.device_pk, f.intf, f.link_pk,
				max(f.in_octets_delta * 8 / f.delta_duration) AS in_bps,
				max(f.out_octets_delta * 8 / f.delta_duration) AS out_bps,
				max(COALESCE(f.in_pkts_delta, 0) / f.delta_duration) AS in_pps,
				max(COALESCE(f.out_pkts_delta, 0) / f.delta_duration) AS out_pps
			FROM fact_dz_device_interface_counters f
			WHERE %s
				AND delta_duration > 0
				AND in_octets_delta >= 0
				AND out_octets_delta >= 0
				%s
				%s
			GROUP BY bucket_ts, f.device_pk, f.intf, f.link_pk
		),
		with_metric AS (
			SELECT
				ir.bucket_ts, ir.device_pk, ir.intf, ir.link_pk, ir.in_bps, ir.out_bps,
				%s AS metric_val_in,
				%s AS metric_val_out
				%s
			FROM interface_rates ir
			%s
			WHERE 1=1 %s
		)
		SELECT %s
		FROM with_metric
		WHERE 1=1 %s
		GROUP BY %s
		ORDER BY bucket_ts`,
		bucketInterval, timeFilter,
		intfTypeSQL, intfFilterSQL,
		metricExprIn, metricExprOut, groupBySelect,
		dimJoins, filterSQL,
		selectCols, metricFilter, groupByCols)

	return
}

// BuildTopQuery builds the ClickHouse query for the top endpoint.
func BuildTopQuery(timeFilter, entity, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL string, limit int) string {
	// Validate sort direction
	dir := "DESC"
	if sortDir == "ASC" {
		dir = "ASC"
	}

	// Validate sort metric
	orderCol := "max_util"
	switch sortMetric {
	case "max_util":
		orderCol = "max_util"
	case "p95_util":
		orderCol = "p95_util"
	case "avg_util":
		orderCol = "avg_util"
	case "max_throughput":
		orderCol = "max_in_bps + max_out_bps"
	case "max_in_bps":
		orderCol = "max_in_bps"
	case "max_out_bps":
		orderCol = "max_out_bps"
	case "bandwidth_bps":
		orderCol = "bandwidth_bps"
	case "headroom":
		orderCol = "COALESCE(toFloat64(l.bandwidth_bps), 0) - greatest(ir.p95_in_bps, ir.p95_out_bps)"
	}

	if entity == "device" {
		// Device-level: aggregate across all interfaces per device.
		// No link_pk in GROUP BY (a device has many links) → no utilization.
		switch orderCol {
		case "max_util", "p95_util", "avg_util":
			orderCol = "max_in_bps + max_out_bps"
		case "bandwidth_bps":
			orderCol = "max_in_bps + max_out_bps"
		case "COALESCE(toFloat64(l.bandwidth_bps), 0) - greatest(ir.p95_in_bps, ir.p95_out_bps)":
			orderCol = "max_in_bps + max_out_bps"
		}
		return fmt.Sprintf(`
			WITH device_rates AS (
				SELECT
					f.device_pk,
					max(f.in_octets_delta * 8 / f.delta_duration) AS max_in_bps,
					max(f.out_octets_delta * 8 / f.delta_duration) AS max_out_bps,
					avg(f.in_octets_delta * 8 / f.delta_duration) AS avg_in_bps,
					avg(f.out_octets_delta * 8 / f.delta_duration) AS avg_out_bps,
					quantile(0.95)(f.in_octets_delta * 8 / f.delta_duration) AS p95_in_bps,
					quantile(0.95)(f.out_octets_delta * 8 / f.delta_duration) AS p95_out_bps
				FROM fact_dz_device_interface_counters f
				WHERE %s
					AND f.delta_duration > 0
					AND f.in_octets_delta >= 0
					AND f.out_octets_delta >= 0
					%s
					%s
				GROUP BY f.device_pk
			)
			SELECT
				dr.device_pk,
				d.code AS device_code,
				'' AS intf,
				COALESCE(m.code, '') AS metro_code,
				'' AS link_type,
				COALESCE(co.code, '') AS contributor_code,
				toFloat64(0) AS bandwidth_bps,
				toFloat64(0) AS max_util,
				toFloat64(0) AS avg_util,
				toFloat64(0) AS p95_util,
				dr.max_in_bps,
				dr.max_out_bps
			FROM device_rates dr
			INNER JOIN dz_devices_current d ON dr.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
			WHERE 1=1 %s
			ORDER BY %s %s
			LIMIT %d`,
			timeFilter, intfTypeSQL, intfFilterSQL, filterSQL, orderCol, dir, limit)
	}

	// Interface-level: GROUP BY includes intf and link_pk for utilization.
	return fmt.Sprintf(`
		WITH interface_rates AS (
			SELECT
				f.device_pk,
				f.intf,
				f.link_pk,
				max(f.in_octets_delta * 8 / f.delta_duration) AS max_in_bps,
				max(f.out_octets_delta * 8 / f.delta_duration) AS max_out_bps,
				avg(f.in_octets_delta * 8 / f.delta_duration) AS avg_in_bps,
				avg(f.out_octets_delta * 8 / f.delta_duration) AS avg_out_bps,
				quantile(0.95)(f.in_octets_delta * 8 / f.delta_duration) AS p95_in_bps,
				quantile(0.95)(f.out_octets_delta * 8 / f.delta_duration) AS p95_out_bps
			FROM fact_dz_device_interface_counters f
			WHERE %s
				AND f.delta_duration > 0
				AND f.in_octets_delta >= 0
				AND f.out_octets_delta >= 0
				%s
				%s
			GROUP BY f.device_pk, f.intf, f.link_pk
		)
		SELECT
			ir.device_pk,
			d.code AS device_code,
			ir.intf,
			COALESCE(m.code, '') AS metro_code,
			COALESCE(l.link_type, '') AS link_type,
			COALESCE(co.code, '') AS contributor_code,
			COALESCE(toFloat64(l.bandwidth_bps), 0) AS bandwidth_bps,
			CASE WHEN COALESCE(l.bandwidth_bps, 0) > 0
				THEN greatest(ir.max_in_bps, ir.max_out_bps) / l.bandwidth_bps
				ELSE 0 END AS max_util,
			CASE WHEN COALESCE(l.bandwidth_bps, 0) > 0
				THEN greatest(ir.avg_in_bps, ir.avg_out_bps) / l.bandwidth_bps
				ELSE 0 END AS avg_util,
			CASE WHEN COALESCE(l.bandwidth_bps, 0) > 0
				THEN greatest(ir.p95_in_bps, ir.p95_out_bps) / l.bandwidth_bps
				ELSE 0 END AS p95_util,
			ir.max_in_bps,
			ir.max_out_bps
		FROM interface_rates ir
		INNER JOIN dz_devices_current d ON ir.device_pk = d.pk
		LEFT JOIN dz_links_current l ON ir.link_pk = l.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
		WHERE 1=1 %s
		ORDER BY %s %s
		LIMIT %d`,
		timeFilter, intfTypeSQL, intfFilterSQL, filterSQL, orderCol, dir, limit)
}

// BuildDrilldownQuery builds the main ClickHouse query for the drilldown endpoint.
func BuildDrilldownQuery(timeFilter, bucketInterval, devicePk, intfFilter string) string {
	return fmt.Sprintf(`
		SELECT
			formatDateTime(toStartOfInterval(f.event_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			f.intf,
			max(f.in_octets_delta * 8 / f.delta_duration) AS in_bps,
			max(f.out_octets_delta * 8 / f.delta_duration) AS out_bps,
			sum(COALESCE(f.in_discards_delta, 0)) AS in_discards,
			sum(COALESCE(f.out_discards_delta, 0)) AS out_discards,
			max(COALESCE(f.in_pkts_delta, 0) / f.delta_duration) AS in_pps,
			max(COALESCE(f.out_pkts_delta, 0) / f.delta_duration) AS out_pps
		FROM fact_dz_device_interface_counters f
		WHERE %s
			AND f.device_pk = '%s'
			%s
			AND f.delta_duration > 0
			AND f.in_octets_delta >= 0
			AND f.out_octets_delta >= 0
		GROUP BY time, f.intf
		ORDER BY time, f.intf`,
		bucketInterval, timeFilter, escapeSingleQuote(devicePk), intfFilter)
}

// BuildBurstinessQuery builds the ClickHouse query for the burstiness endpoint.
// It pre-aggregates raw samples into time buckets (using max per bucket per interface)
// before computing P50/P99, so the results align with what time-series charts display.
func BuildBurstinessQuery(timeFilter, bucketInterval, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL string, threshold float64, minBps float64, limit int) string {
	// Validate sort direction
	dir := "DESC"
	if sortDir == "ASC" {
		dir = "ASC"
	}

	// Validate sort metric
	orderCol := "burstiness"
	switch sortMetric {
	case "burstiness":
		orderCol = "burstiness"
	case "p50_util":
		orderCol = "p50_util"
	case "p99_util":
		orderCol = "p99_util"
	case "pct_time_stressed":
		orderCol = "pct_time_stressed"
	case "p50_bps":
		orderCol = "p50_bps"
	case "p99_bps":
		orderCol = "p99_bps"
	}

	return fmt.Sprintf(`
		WITH bucketed AS (
			SELECT
				toStartOfInterval(f.event_ts, INTERVAL %s) AS bucket_ts,
				f.device_pk,
				f.intf,
				f.link_pk,
				max(greatest(
					f.in_octets_delta * 8 / f.delta_duration,
					f.out_octets_delta * 8 / f.delta_duration
				)) AS throughput_bps,
				CASE WHEN max(l.bandwidth_bps) > 0
					THEN max(greatest(
						f.in_octets_delta * 8 / f.delta_duration,
						f.out_octets_delta * 8 / f.delta_duration
					)) / max(l.bandwidth_bps)
					ELSE NULL END AS utilization,
				argMax(
					CASE WHEN f.in_octets_delta >= f.out_octets_delta THEN 1 ELSE 0 END,
					greatest(f.in_octets_delta * 8 / f.delta_duration, f.out_octets_delta * 8 / f.delta_duration)
				) AS is_rx
			FROM fact_dz_device_interface_counters f
			LEFT JOIN dz_links_current l ON f.link_pk = l.pk
			INNER JOIN dz_devices_current d ON f.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
			WHERE %s
				AND f.delta_duration > 0
				AND f.in_octets_delta >= 0
				AND f.out_octets_delta >= 0
				%s
				%s
				%s
			GROUP BY bucket_ts, f.device_pk, f.intf, f.link_pk
		)
		SELECT
			b.device_pk,
			d.code AS device_code,
			b.intf,
			COALESCE(m.code, '') AS metro_code,
			COALESCE(toFloat64(l.bandwidth_bps), 0) AS bandwidth_bps,
			quantile(0.5)(b.utilization) AS p50_util,
			quantile(0.99)(b.utilization) AS p99_util,
			CASE
				WHEN COALESCE(toFloat64(l.bandwidth_bps), 0) > 0
				THEN quantile(0.99)(b.utilization) - quantile(0.5)(b.utilization)
				ELSE CASE
					WHEN quantile(0.5)(b.throughput_bps) > 0
					THEN (quantile(0.99)(b.throughput_bps) / quantile(0.5)(b.throughput_bps)) - 1
					ELSE 0
				END
			END AS burstiness,
			CASE
				WHEN COALESCE(toFloat64(l.bandwidth_bps), 0) > 0
				THEN countIf(b.utilization >= %f) / count()
				ELSE 0
			END AS pct_time_stressed,
			quantile(0.5)(b.throughput_bps) AS p50_bps,
			quantile(0.99)(b.throughput_bps) AS p99_bps,
			CASE WHEN argMax(b.is_rx, b.throughput_bps) = 1 THEN 'rx' ELSE 'tx' END AS peak_direction,
			COALESCE(co.code, '') AS contributor_code
		FROM bucketed b
		INNER JOIN dz_devices_current d ON b.device_pk = d.pk
		LEFT JOIN dz_links_current l ON b.link_pk = l.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
		GROUP BY b.device_pk, b.intf, b.link_pk, d.code, m.code, l.bandwidth_bps, co.code
		HAVING burstiness > 0
			AND (COALESCE(toFloat64(l.bandwidth_bps), 0) > 0 OR p50_bps >= %f)
		ORDER BY %s %s
		LIMIT %d`,
		bucketInterval, timeFilter, intfTypeSQL, filterSQL, intfFilterSQL, threshold, minBps, orderCol, dir, limit)
}

// BuildHealthQuery builds the ClickHouse query for the interface health endpoint.
func BuildHealthQuery(timeFilter, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL string, limit int) string {
	// Validate sort direction
	dir := "DESC"
	if sortDir == "ASC" {
		dir = "ASC"
	}

	// Validate sort metric
	orderCol := "total_events"
	switch sortMetric {
	case "total_events":
		orderCol = "total_events"
	case "total_errors":
		orderCol = "total_errors"
	case "total_discards":
		orderCol = "total_discards"
	case "total_fcs_errors":
		orderCol = "total_fcs_errors"
	case "total_carrier_transitions":
		orderCol = "total_carrier_transitions"
	}

	return fmt.Sprintf(`
		WITH health AS (
			SELECT
				f.device_pk,
				f.intf,
				f.link_pk,
				sum(COALESCE(f.in_errors_delta, 0)) + sum(COALESCE(f.out_errors_delta, 0)) AS total_errors,
				sum(COALESCE(f.in_discards_delta, 0)) + sum(COALESCE(f.out_discards_delta, 0)) AS total_discards,
				sum(COALESCE(f.in_fcs_errors_delta, 0)) AS total_fcs_errors,
				sum(COALESCE(f.carrier_transitions_delta, 0)) AS total_carrier_transitions,
				count() AS sample_count
			FROM fact_dz_device_interface_counters f
			WHERE %s
				%s
				%s
			GROUP BY f.device_pk, f.intf, f.link_pk
		)
		SELECT
			h.device_pk,
			d.code AS device_code,
			h.intf,
			COALESCE(m.code, '') AS metro_code,
			h.total_errors,
			h.total_discards,
			h.total_fcs_errors,
			h.total_carrier_transitions,
			h.total_errors + h.total_discards + h.total_fcs_errors + h.total_carrier_transitions AS total_events,
			COALESCE(co.code, '') AS contributor_code
		FROM health h
		INNER JOIN dz_devices_current d ON h.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
		LEFT JOIN dz_links_current l ON h.link_pk = l.pk
		WHERE 1=1 %s
		HAVING total_events > 0
		ORDER BY %s %s
		LIMIT %d`,
		timeFilter, intfTypeSQL, intfFilterSQL,
		filterSQL,
		orderCol, dir, limit)
}

// --- Health endpoint ---

type HealthEntity struct {
	DevicePk                string `json:"device_pk"`
	DeviceCode              string `json:"device_code"`
	Intf                    string `json:"intf"`
	MetroCode               string `json:"metro_code"`
	ContributorCode         string `json:"contributor_code"`
	TotalErrors             int64  `json:"total_errors"`
	TotalDiscards           int64  `json:"total_discards"`
	TotalFcsErrors          int64  `json:"total_fcs_errors"`
	TotalCarrierTransitions int64  `json:"total_carrier_transitions"`
	TotalEvents             int64  `json:"total_events"`
}

type HealthResponse struct {
	Entities []HealthEntity `json:"entities"`
}

func GetTrafficDashboardHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, _ := dashboardTimeFilter(r)

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}

	sortMetric := r.URL.Query().Get("sort")
	if sortMetric == "" {
		sortMetric = "total_events"
	}
	sortDir := strings.ToUpper(r.URL.Query().Get("dir"))

	filterSQL, intfFilterSQL, intfTypeSQL, _, _, _, _ := buildDimensionFilters(r)

	query := BuildHealthQuery(timeFilter, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, limit)

	start := time.Now()
	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic dashboard health query error: %v\nQuery: %s", err, query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	entities := []HealthEntity{}
	for rows.Next() {
		var e HealthEntity
		if err := rows.Scan(&e.DevicePk, &e.DeviceCode, &e.Intf, &e.MetroCode,
			&e.TotalErrors, &e.TotalDiscards, &e.TotalFcsErrors,
			&e.TotalCarrierTransitions, &e.TotalEvents, &e.ContributorCode); err != nil {
			log.Printf("Traffic dashboard health row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		entities = append(entities, e)
	}

	resp := HealthResponse{Entities: entities}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// --- Stress endpoint ---

type StressResponse struct {
	Timestamps    []string      `json:"timestamps"`
	P50In         []float64     `json:"p50_in"`
	P95In         []float64     `json:"p95_in"`
	MaxIn         []float64     `json:"max_in"`
	P50Out        []float64     `json:"p50_out"`
	P95Out        []float64     `json:"p95_out"`
	MaxOut        []float64     `json:"max_out"`
	StressedCount []int64       `json:"stressed_count"`
	TotalCount    []int64       `json:"total_count"`
	EffBucket     string        `json:"effective_bucket"`
	Groups        []StressGroup `json:"groups,omitempty"`
}

type StressGroup struct {
	Key           string    `json:"key"`
	Label         string    `json:"label"`
	P50In         []float64 `json:"p50_in"`
	P95In         []float64 `json:"p95_in"`
	MaxIn         []float64 `json:"max_in"`
	P50Out        []float64 `json:"p50_out"`
	P95Out        []float64 `json:"p95_out"`
	MaxOut        []float64 `json:"max_out"`
	StressedCount []int64   `json:"stressed_count"`
}

func GetTrafficDashboardStress(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, bucketInterval := dashboardTimeFilter(r)

	threshold := 0.8
	if t := r.URL.Query().Get("threshold"); t != "" {
		if v, err := strconv.ParseFloat(t, 64); err == nil && v > 0 && v <= 1 {
			threshold = v
		}
	}

	groupBy := r.URL.Query().Get("group_by")
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		metric = "utilization"
	}

	filterSQL, intfFilterSQL, intfTypeSQL, needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin := buildDimensionFilters(r)

	query, grouped := BuildStressQuery(timeFilter, bucketInterval, metric, groupBy, filterSQL, intfFilterSQL, intfTypeSQL, threshold,
		needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin)

	start := time.Now()
	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic dashboard stress query error: %v\nQuery: %s", err, query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	if grouped {
		type rowData struct {
			p50In       float64
			p95In       float64
			maxIn       float64
			p50Out      float64
			p95Out      float64
			maxOut      float64
			stressedCnt int64
		}

		groupOrder := []string{}
		groupLabels := map[string]string{}
		tsOrder := []string{}
		tsSet := map[string]bool{}
		dataByGroup := map[string]map[string]*rowData{}

		for rows.Next() {
			var ts, gk, gl string
			var p50In, p95In, maxIn, p50Out, p95Out, maxOut float64
			var sc, tc uint64
			if err := rows.Scan(&ts, &gk, &gl, &p50In, &p95In, &maxIn, &p50Out, &p95Out, &maxOut, &sc, &tc); err != nil {
				log.Printf("Traffic dashboard stress row scan error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !tsSet[ts] {
				tsSet[ts] = true
				tsOrder = append(tsOrder, ts)
			}
			if _, ok := dataByGroup[gk]; !ok {
				groupOrder = append(groupOrder, gk)
				groupLabels[gk] = gl
				dataByGroup[gk] = map[string]*rowData{}
			}
			dataByGroup[gk][ts] = &rowData{
				p50In: p50In, p95In: p95In, maxIn: maxIn,
				p50Out: p50Out, p95Out: p95Out, maxOut: maxOut,
				stressedCnt: int64(sc),
			}
		}

		groups := make([]StressGroup, 0, len(groupOrder))
		for _, gk := range groupOrder {
			g := StressGroup{
				Key:           gk,
				Label:         groupLabels[gk],
				P50In:         make([]float64, len(tsOrder)),
				P95In:         make([]float64, len(tsOrder)),
				MaxIn:         make([]float64, len(tsOrder)),
				P50Out:        make([]float64, len(tsOrder)),
				P95Out:        make([]float64, len(tsOrder)),
				MaxOut:        make([]float64, len(tsOrder)),
				StressedCount: make([]int64, len(tsOrder)),
			}
			for i, ts := range tsOrder {
				if d, ok := dataByGroup[gk][ts]; ok {
					g.P50In[i] = d.p50In
					g.P95In[i] = d.p95In
					g.MaxIn[i] = d.maxIn
					g.P50Out[i] = d.p50Out
					g.P95Out[i] = d.p95Out
					g.MaxOut[i] = d.maxOut
					g.StressedCount[i] = d.stressedCnt
				}
			}
			groups = append(groups, g)
		}

		resp := StressResponse{
			Timestamps: tsOrder,
			Groups:     groups,
			EffBucket:  bucketInterval,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	} else {
		var timestamps []string
		var p50Ins, p95Ins, maxIns []float64
		var p50Outs, p95Outs, maxOuts []float64
		var stressedCounts, totalCounts []int64

		for rows.Next() {
			var ts string
			var p50In, p95In, maxIn, p50Out, p95Out, maxOut float64
			var sc, tc uint64
			if err := rows.Scan(&ts, &p50In, &p95In, &maxIn, &p50Out, &p95Out, &maxOut, &sc, &tc); err != nil {
				log.Printf("Traffic dashboard stress row scan error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			timestamps = append(timestamps, ts)
			p50Ins = append(p50Ins, p50In)
			p95Ins = append(p95Ins, p95In)
			maxIns = append(maxIns, maxIn)
			p50Outs = append(p50Outs, p50Out)
			p95Outs = append(p95Outs, p95Out)
			maxOuts = append(maxOuts, maxOut)
			stressedCounts = append(stressedCounts, int64(sc))
			totalCounts = append(totalCounts, int64(tc))
		}

		if timestamps == nil {
			timestamps = []string{}
			p50Ins = []float64{}
			p95Ins = []float64{}
			maxIns = []float64{}
			p50Outs = []float64{}
			p95Outs = []float64{}
			maxOuts = []float64{}
			stressedCounts = []int64{}
			totalCounts = []int64{}
		}

		resp := StressResponse{
			Timestamps:    timestamps,
			P50In:         p50Ins,
			P95In:         p95Ins,
			MaxIn:         maxIns,
			P50Out:        p50Outs,
			P95Out:        p95Outs,
			MaxOut:        maxOuts,
			StressedCount: stressedCounts,
			TotalCount:    totalCounts,
			EffBucket:     bucketInterval,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// --- Top endpoint ---

type TopEntity struct {
	DevicePk        string  `json:"device_pk"`
	DeviceCode      string  `json:"device_code"`
	Intf            string  `json:"intf,omitempty"`
	MetroCode       string  `json:"metro_code"`
	LinkType        string  `json:"link_type"`
	ContributorCode string  `json:"contributor_code"`
	BandwidthBps    float64 `json:"bandwidth_bps"`
	MaxUtil         float64 `json:"max_util"`
	AvgUtil         float64 `json:"avg_util"`
	P95Util         float64 `json:"p95_util"`
	MaxInBps        float64 `json:"max_in_bps"`
	MaxOutBps       float64 `json:"max_out_bps"`
}

type TopResponse struct {
	Entities []TopEntity `json:"entities"`
}

func GetTrafficDashboardTop(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, _ := dashboardTimeFilter(r)

	entity := r.URL.Query().Get("entity")
	if entity == "" {
		entity = "interface"
	}

	sortMetric := r.URL.Query().Get("metric")
	if sortMetric == "" {
		sortMetric = "max_util"
	}

	sortDir := strings.ToUpper(r.URL.Query().Get("dir"))

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}

	filterSQL, intfFilterSQL, intfTypeSQL, _, _, _, _ := buildDimensionFilters(r)

	query := BuildTopQuery(timeFilter, entity, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, limit)

	start := time.Now()
	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic dashboard top query error: %v\nQuery: %s", err, query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	entities := []TopEntity{}
	for rows.Next() {
		var e TopEntity
		if err := rows.Scan(&e.DevicePk, &e.DeviceCode, &e.Intf, &e.MetroCode,
			&e.LinkType, &e.ContributorCode, &e.BandwidthBps,
			&e.MaxUtil, &e.AvgUtil, &e.P95Util,
			&e.MaxInBps, &e.MaxOutBps); err != nil {
			log.Printf("Traffic dashboard top row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		entities = append(entities, e)
	}

	resp := TopResponse{Entities: entities}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// --- Drilldown endpoint ---

type DrilldownPoint struct {
	Time        string  `json:"time"`
	Intf        string  `json:"intf"`
	InBps       float64 `json:"in_bps"`
	OutBps      float64 `json:"out_bps"`
	InDiscards  int64   `json:"in_discards"`
	OutDiscards int64   `json:"out_discards"`
	InPps       float64 `json:"in_pps"`
	OutPps      float64 `json:"out_pps"`
}

type DrilldownSeries struct {
	Intf         string  `json:"intf"`
	BandwidthBps float64 `json:"bandwidth_bps"`
	LinkType     string  `json:"link_type"`
}

type DrilldownResponse struct {
	Points    []DrilldownPoint  `json:"points"`
	Series    []DrilldownSeries `json:"series"`
	EffBucket string            `json:"effective_bucket"`
}

func GetTrafficDashboardDrilldown(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, bucketInterval := dashboardTimeFilter(r)

	devicePk := r.URL.Query().Get("device_pk")
	if devicePk == "" {
		http.Error(w, "device_pk is required", http.StatusBadRequest)
		return
	}

	intf := r.URL.Query().Get("intf")
	intfType := r.URL.Query().Get("intf_type")

	var intfFilter string
	if intf != "" {
		intfFilter = fmt.Sprintf("AND f.intf = '%s'", escapeSingleQuote(intf))
	} else {
		intfFilter = buildIntfTypeFilter(intfType)
	}

	query := BuildDrilldownQuery(timeFilter, bucketInterval, devicePk, intfFilter)

	start := time.Now()
	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic dashboard drilldown query error: %v\nQuery: %s", err, query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	points := []DrilldownPoint{}
	intfSet := map[string]bool{}
	for rows.Next() {
		var p DrilldownPoint
		var inDisc, outDisc int64
		if err := rows.Scan(&p.Time, &p.Intf, &p.InBps, &p.OutBps, &inDisc, &outDisc, &p.InPps, &p.OutPps); err != nil {
			log.Printf("Traffic dashboard drilldown row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		p.InDiscards = inDisc
		p.OutDiscards = outDisc
		points = append(points, p)
		intfSet[p.Intf] = true
	}

	// Fetch series metadata (bandwidth, link_type) for each interface
	intfNames := make([]string, 0, len(intfSet))
	for name := range intfSet {
		intfNames = append(intfNames, name)
	}

	series := []DrilldownSeries{}
	if len(intfNames) > 0 {
		quoted := make([]string, len(intfNames))
		for i, v := range intfNames {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}

		metaQuery := fmt.Sprintf(`
			SELECT
				f.intf,
				COALESCE(toFloat64(l.bandwidth_bps), 0) AS bandwidth_bps,
				COALESCE(l.link_type, '') AS link_type
			FROM (
				SELECT DISTINCT intf, link_pk
				FROM fact_dz_device_interface_counters
				WHERE device_pk = '%s'
					AND intf IN (%s)
					AND %s
			) f
			LEFT JOIN dz_links_current l ON f.link_pk = l.pk`,
			escapeSingleQuote(devicePk),
			strings.Join(quoted, ","),
			timeFilter)

		metaRows, err := envDB(ctx).Query(ctx, metaQuery)
		if err == nil {
			defer metaRows.Close()
			for metaRows.Next() {
				var s DrilldownSeries
				if err := metaRows.Scan(&s.Intf, &s.BandwidthBps, &s.LinkType); err == nil {
					series = append(series, s)
				}
			}
		}
	}

	resp := DrilldownResponse{
		Points:    points,
		Series:    series,
		EffBucket: bucketInterval,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// --- Burstiness endpoint ---

type BurstinessEntity struct {
	DevicePk        string  `json:"device_pk"`
	DeviceCode      string  `json:"device_code"`
	Intf            string  `json:"intf"`
	MetroCode       string  `json:"metro_code"`
	BandwidthBps    float64 `json:"bandwidth_bps"`
	P50Util         float64 `json:"p50_util"`
	P99Util         float64 `json:"p99_util"`
	Burstiness      float64 `json:"burstiness"`
	PctTimeStressed float64 `json:"pct_time_stressed"`
	P50Bps          float64 `json:"p50_bps"`
	P99Bps          float64 `json:"p99_bps"`
	PeakDirection   string  `json:"peak_direction"`
	ContributorCode string  `json:"contributor_code"`
}

type BurstinessResponse struct {
	Entities []BurstinessEntity `json:"entities"`
}

func GetTrafficDashboardBurstiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, bucketInterval := dashboardTimeFilter(r)

	threshold := 0.8
	if t := r.URL.Query().Get("threshold"); t != "" {
		if v, err := strconv.ParseFloat(t, 64); err == nil && v > 0 && v <= 1 {
			threshold = v
		}
	}

	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
			limit = v
		}
	}

	minBps := 1000000.0 // 1 Mbps default
	if m := r.URL.Query().Get("min_bps"); m != "" {
		if v, err := strconv.ParseFloat(m, 64); err == nil && v >= 0 {
			minBps = v
		}
	}

	sortMetric := r.URL.Query().Get("sort")
	if sortMetric == "" {
		sortMetric = "burstiness"
	}
	sortDir := strings.ToUpper(r.URL.Query().Get("dir"))

	filterSQL, intfFilterSQL, intfTypeSQL, _, _, _, _ := buildDimensionFilters(r)

	query := BuildBurstinessQuery(timeFilter, bucketInterval, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, threshold, minBps, limit)

	start := time.Now()
	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic dashboard burstiness query error: %v\nQuery: %s", err, query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	entities := []BurstinessEntity{}
	for rows.Next() {
		var e BurstinessEntity
		if err := rows.Scan(&e.DevicePk, &e.DeviceCode, &e.Intf, &e.MetroCode,
			&e.BandwidthBps, &e.P50Util, &e.P99Util, &e.Burstiness,
			&e.PctTimeStressed, &e.P50Bps, &e.P99Bps, &e.PeakDirection, &e.ContributorCode); err != nil {
			log.Printf("Traffic dashboard burstiness row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		entities = append(entities, e)
	}

	resp := BurstinessResponse{Entities: entities}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
