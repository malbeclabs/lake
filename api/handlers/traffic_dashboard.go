package handlers

import (
	"context"
	"encoding/json"
	"fmt"
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
		return "6 HOUR"
	}
}

// parseBucket converts a user-facing bucket string to a ClickHouse interval.
// Accepts both short forms ("5m") and full interval strings ("5 MINUTE").
func parseBucket(b string) string {
	switch b {
	case "10s", "10 SECOND":
		return "10 SECOND"
	case "30s", "30 SECOND":
		return "30 SECOND"
	case "1m", "1 MINUTE":
		return "1 MINUTE"
	case "5m", "5 MINUTE":
		return "5 MINUTE"
	case "10m", "10 MINUTE":
		return "10 MINUTE"
	case "15m", "15 MINUTE":
		return "15 MINUTE"
	case "30m", "30 MINUTE":
		return "30 MINUTE"
	case "1h", "1 HOUR":
		return "1 HOUR"
	case "4h", "4 HOUR":
		return "4 HOUR"
	case "12h", "12 HOUR":
		return "12 HOUR"
	case "1d", "1 DAY":
		return "1 DAY"
	default:
		return ""
	}
}

// effectiveBucket returns a sensible bucket for the given time range if none specified.
// Sub-5m buckets use raw fact table data; >= 5m uses rollup.
func effectiveBucket(timeRange, bucket string) string {
	if bucket != "" {
		return bucket
	}
	switch timeRange {
	case "1h":
		return "10 SECOND"
	case "3h":
		return "30 SECOND"
	case "6h":
		return "1 MINUTE"
	case "12h":
		return "5 MINUTE"
	case "24h":
		return "10 MINUTE"
	case "3d":
		return "30 MINUTE"
	case "7d":
		return "4 HOUR"
	case "14d":
		return "12 HOUR"
	case "30d":
		return "1 DAY"
	default:
		return "1 MINUTE"
	}
}

// bucketForDuration returns a sensible bucket interval for the given duration.
func bucketForDuration(d time.Duration) string {
	switch {
	case d <= time.Hour:
		return "10 SECOND"
	case d <= 3*time.Hour:
		return "30 SECOND"
	case d <= 6*time.Hour:
		return "1 MINUTE"
	case d <= 12*time.Hour:
		return "5 MINUTE"
	case d <= 24*time.Hour:
		return "10 MINUTE"
	case d <= 3*24*time.Hour:
		return "30 MINUTE"
	case d <= 7*24*time.Hour:
		return "4 HOUR"
	case d <= 14*24*time.Hour:
		return "12 HOUR"
	default:
		return "1 DAY"
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

// rollupEffectiveBucket returns a sensible bucket for the given time range,
// enforcing a minimum of 5 MINUTE since rollup tables have 5m granularity.
func rollupEffectiveBucket(timeRange, bucket string) string {
	if bucket != "" {
		return bucket
	}
	switch timeRange {
	case "1h", "3h", "6h":
		return "5 MINUTE"
	case "12h":
		return "10 MINUTE"
	case "24h":
		return "15 MINUTE"
	case "3d":
		return "30 MINUTE"
	case "7d":
		return "4 HOUR"
	case "14d":
		return "12 HOUR"
	case "30d":
		return "1 DAY"
	default:
		return "5 MINUTE"
	}
}

// rollupBucketForDuration returns a sensible bucket interval for the given duration,
// enforcing a minimum of 5 MINUTE since rollup tables have 5m granularity.
func rollupBucketForDuration(d time.Duration) string {
	switch {
	case d < 3*24*time.Hour:
		return "5 MINUTE"
	case d < 7*24*time.Hour:
		return "30 MINUTE"
	default:
		return "1 HOUR"
	}
}

// rollupTimeFilter extracts time filter and bucket interval from the request
// for use with rollup tables. Uses bucket_ts instead of event_ts and enforces
// a minimum 5 MINUTE bucket interval.
func rollupTimeFilter(r *http.Request) (timeFilter string, bucketInterval string) {
	startStr := r.URL.Query().Get("start_time")
	endStr := r.URL.Query().Get("end_time")

	if startStr != "" && endStr != "" {
		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 == nil && err2 == nil && end > start {
			timeFilter = fmt.Sprintf("bucket_ts BETWEEN toDateTime(%d) AND toDateTime(%d)", start, end)
			duration := time.Duration(end-start) * time.Second
			bucketInterval = rollupBucketForDuration(duration)
			if bp := parseBucket(r.URL.Query().Get("bucket")); bp != "" {
				bucketInterval = clampBucket(bp)
			}
			return
		}
	}

	timeRange := r.URL.Query().Get("time_range")
	if timeRange == "" {
		timeRange = "12h"
	}
	rangeInterval := dashboardTimeRange(timeRange)
	timeFilter = fmt.Sprintf("bucket_ts >= now() - INTERVAL %s", rangeInterval)

	bucketParam := parseBucket(r.URL.Query().Get("bucket"))
	bucketInterval = rollupEffectiveBucket(timeRange, clampBucket(bucketParam))
	return
}

// isRawBucket returns true if the bucket interval requires raw fact table queries
// (i.e. sub-5-minute granularity that the rollup table cannot provide).
func isRawBucket(b string) bool {
	switch b {
	case "10 SECOND", "30 SECOND", "1 MINUTE":
		return true
	default:
		return false
	}
}

// trafficTimeFilter resolves the time filter, bucket interval, and data source
// for traffic queries. Uses raw fact table for sub-5m buckets, rollup for >= 5m.
func trafficTimeFilter(r *http.Request) (timeFilter string, bucketInterval string, useRaw bool) {
	// First resolve using the raw/fact-table logic to get the natural bucket
	timeFilter, bucketInterval = dashboardTimeFilter(r)
	if isRawBucket(bucketInterval) {
		return timeFilter, bucketInterval, true
	}
	// Bucket is >= 5m, use rollup
	timeFilter, bucketInterval = rollupTimeFilter(r)
	return timeFilter, bucketInterval, false
}

// clampBucket enforces a minimum of 5 MINUTE for rollup queries.
func clampBucket(b string) string {
	switch b {
	case "10 SECOND", "30 SECOND", "1 MINUTE":
		return "5 MINUTE"
	case "":
		return ""
	default:
		return b
	}
}

// bandwidthExpr returns the SQL expression for bandwidth, falling back to
// the device interface dimension table when the link bandwidth is 0.
func bandwidthExpr(needsInterfaceJoin bool) string {
	if needsInterfaceJoin {
		return "COALESCE(NULLIF(toFloat64(l.bandwidth_bps), 0), NULLIF(toFloat64(di.bandwidth), 0), 0)"
	}
	return "l.bandwidth_bps"
}

// buildDimensionFilters builds SQL WHERE clauses for dimension filters.
// It returns:
//   - filterSQL: clauses for dimension tables (m, d, l, co) with leading AND
//   - intfFilterSQL: clause for f.intf with leading AND (must go in the CTE where f is in scope)
//   - intfTypeSQL: clause for interface type filtering with leading AND (must go in CTE)
//   - userKindSQL: clause for user kind filtering with leading AND (requires user join)
//   - join flags indicating which dimension joins are needed
func buildDimensionFilters(r *http.Request) (filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL string, needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin bool) {
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

	if userKinds := r.URL.Query().Get("user_kind"); userKinds != "" {
		needsUserJoin = true
		vals := strings.Split(userKinds, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		userKindSQL = fmt.Sprintf(" AND u.kind IN (%s)", strings.Join(quoted, ","))
	}

	if cyoaTypes := r.URL.Query().Get("cyoa_type"); cyoaTypes != "" {
		needsInterfaceJoin = true
		vals := strings.Split(cyoaTypes, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		clauses = append(clauses, fmt.Sprintf("di.cyoa_type IN (%s)", strings.Join(quoted, ",")))
	}

	if interfaceTypes := r.URL.Query().Get("interface_type"); interfaceTypes != "" {
		needsInterfaceJoin = true
		vals := strings.Split(interfaceTypes, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		clauses = append(clauses, fmt.Sprintf("di.interface_type IN (%s)", strings.Join(quoted, ",")))
	}

	if intfs := r.URL.Query().Get("intf"); intfs != "" {
		vals := strings.Split(intfs, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		intfFilterSQL = fmt.Sprintf(" AND f.intf IN (%s)", strings.Join(quoted, ","))
	}

	var intfTypeNeedsJoin bool
	intfTypeSQL, intfTypeNeedsJoin = buildIntfTypeFilter(r.URL.Query().Get("intf_type"))
	if intfTypeNeedsJoin {
		needsInterfaceJoin = true
	}

	if len(clauses) > 0 {
		filterSQL = " AND " + strings.Join(clauses, " AND ")
	}
	return
}

// buildIntfTypeFilter returns a SQL clause for filtering by interface type and
// whether the device interfaces dimension join is needed.
// Values: "all" (no filter), "link", "tunnel", "cyoa", "dia", "other".
func buildIntfTypeFilter(intfType string) (string, bool) {
	switch intfType {
	case "link":
		return " AND f.link_pk != ''", false
	case "tunnel":
		return " AND f.user_tunnel_id IS NOT NULL", false
	case "cyoa":
		return " AND di.cyoa_type NOT IN ('none', '')", true
	case "other":
		return " AND f.link_pk = '' AND f.user_tunnel_id IS NULL AND (di.cyoa_type IN ('none', '') OR di.cyoa_type IS NULL)", true
	default:
		// "all" or empty: no filter
		return "", false
	}
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// --- Query builders (exported for testing) ---

// BuildStressQuery builds the ClickHouse query for the stress endpoint.
// Reads from device_interface_rollup_5m and re-aggregates into the requested bucket.
func BuildStressQuery(timeFilter, bucketInterval, metric, groupBy, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL string, threshold float64,
	needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin bool) (query string, grouped bool) {

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
	case "user_kind":
		needsUserJoin = true
		grouped = true
		groupBySelect = ", u.kind AS group_key, u.kind AS group_label"
	case "cyoa_type":
		needsInterfaceJoin = true
		grouped = true
		groupBySelect = ", di.cyoa_type AS group_key, di.cyoa_type AS group_label"
	}

	// Build dimension join clauses on the rollup alias "f"
	var dimJoins string
	if needsDeviceJoin {
		dimJoins += " INNER JOIN dz_devices_current d ON f.device_pk = d.pk"
	}
	// Always need link join for utilization metric
	dimJoins += " LEFT JOIN dz_links_current l ON f.link_pk = l.pk"
	if needsMetroJoin {
		dimJoins += " LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk"
	}
	if needsContributorJoin {
		dimJoins += " LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk"
	}
	if needsUserJoin {
		dimJoins += " LEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
	}
	if needsInterfaceJoin {
		dimJoins += " LEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}

	bwExpr := bandwidthExpr(needsInterfaceJoin)

	// Build the metric expressions (separate in/out) and filter.
	// Rollup has max_in_bps/max_out_bps per 5m bucket which represents the
	// peak rate per interface per bucket — equivalent to what the old query
	// computed via max(in_octets_delta * 8 / delta_duration).
	var metricExprIn, metricExprOut, metricFilter string
	switch metric {
	case "throughput":
		metricExprIn = "f.max_in_bps"
		metricExprOut = "f.max_out_bps"
	case "packets":
		metricExprIn = "f.max_in_pps"
		metricExprOut = "f.max_out_pps"
	default: // utilization
		metricExprIn = fmt.Sprintf(`CASE WHEN %s > 0
			THEN f.max_in_bps / %s
			ELSE NULL END`, bwExpr, bwExpr)
		metricExprOut = fmt.Sprintf(`CASE WHEN %s > 0
			THEN f.max_out_bps / %s
			ELSE NULL END`, bwExpr, bwExpr)
		metricFilter = " AND metric_val_in IS NOT NULL"
	}

	// Build percentile select
	var selectCols, groupByCols string
	if grouped {
		selectCols = fmt.Sprintf(`
			formatDateTime(ts, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS ts,
			group_key, group_label,
			quantile(0.5)(metric_val_in) AS p50_in,
			quantile(0.95)(metric_val_in) AS p95_in,
			max(metric_val_in) AS max_in,
			quantile(0.5)(metric_val_out) AS p50_out,
			quantile(0.95)(metric_val_out) AS p95_out,
			max(metric_val_out) AS max_out,
			countIf(greatest(metric_val_in, metric_val_out) >= %f) AS stressed_count,
			count() AS total_count`, threshold)
		groupByCols = "ts, group_key, group_label"
	} else {
		selectCols = fmt.Sprintf(`
			formatDateTime(ts, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS ts,
			quantile(0.5)(metric_val_in) AS p50_in,
			quantile(0.95)(metric_val_in) AS p95_in,
			max(metric_val_in) AS max_in,
			quantile(0.5)(metric_val_out) AS p50_out,
			quantile(0.95)(metric_val_out) AS p95_out,
			max(metric_val_out) AS max_out,
			countIf(greatest(metric_val_in, metric_val_out) >= %f) AS stressed_count,
			count() AS total_count`, threshold)
		groupByCols = "ts"
	}

	query = fmt.Sprintf(`
		WITH with_metric AS (
			SELECT
				toStartOfInterval(f.bucket_ts, INTERVAL %s) AS ts,
				%s AS metric_val_in,
				%s AS metric_val_out
				%s
			FROM device_interface_rollup_5m f
			%s
			WHERE f.%s
				%s
				%s
				%s
				%s
		)
		SELECT %s
		FROM with_metric
		WHERE 1=1 %s
		GROUP BY %s
		ORDER BY ts`,
		bucketInterval,
		metricExprIn, metricExprOut, groupBySelect,
		dimJoins,
		timeFilter,
		intfTypeSQL, intfFilterSQL,
		filterSQL, userKindSQL,
		selectCols, metricFilter, groupByCols)

	return
}

// BuildStressQueryRaw builds the stress query using raw fact_dz_device_interface_counters
// for sub-5m bucket granularity. Same interface as BuildStressQuery.
func BuildStressQueryRaw(timeFilter, bucketInterval, metric, groupBy, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL string, threshold float64,
	needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin bool) (query string, grouped bool) {

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
	case "user_kind":
		needsUserJoin = true
		grouped = true
		groupBySelect = ", u.kind AS group_key, u.kind AS group_label"
	case "cyoa_type":
		needsInterfaceJoin = true
		grouped = true
		groupBySelect = ", di.cyoa_type AS group_key, di.cyoa_type AS group_label"
	}

	var userTunnelSelect, userTunnelGroupBy, userTunnelPassthrough string
	if needsUserJoin {
		userTunnelSelect = ", f.user_tunnel_id"
		userTunnelGroupBy = ", f.user_tunnel_id"
		userTunnelPassthrough = ", ir.user_tunnel_id"
	}

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
	if needsUserJoin {
		dimJoins += " LEFT JOIN dz_users_current u ON ir.user_tunnel_id = u.tunnel_id"
	}
	if needsInterfaceJoin {
		dimJoins += " LEFT JOIN dz_device_interfaces_current di ON ir.device_pk = di.device_pk AND ir.intf = di.intf"
	}

	bwExpr := bandwidthExpr(needsInterfaceJoin)

	var metricExprIn, metricExprOut, metricFilter string
	switch metric {
	case "throughput":
		metricExprIn = "ir.in_bps"
		metricExprOut = "ir.out_bps"
	case "packets":
		metricExprIn = "ir.in_pps"
		metricExprOut = "ir.out_pps"
	default:
		metricExprIn = fmt.Sprintf(`CASE WHEN %s > 0
			THEN ir.in_bps / %s
			ELSE NULL END`, bwExpr, bwExpr)
		metricExprOut = fmt.Sprintf(`CASE WHEN %s > 0
			THEN ir.out_bps / %s
			ELSE NULL END`, bwExpr, bwExpr)
		metricFilter = " AND metric_val_in IS NOT NULL"
	}

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

	// When intfTypeSQL references di, join it in the first CTE too
	var rawIntfJoin string
	if needsInterfaceJoin {
		rawIntfJoin = "\n\t\t\tLEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}

	query = fmt.Sprintf(`
		WITH interface_rates AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL %s) AS bucket_ts,
				f.device_pk, f.intf, f.link_pk%s,
				max(f.in_octets_delta * 8 / f.delta_duration) AS in_bps,
				max(f.out_octets_delta * 8 / f.delta_duration) AS out_bps,
				max(COALESCE(f.in_pkts_delta, 0) / f.delta_duration) AS in_pps,
				max(COALESCE(f.out_pkts_delta, 0) / f.delta_duration) AS out_pps
			FROM fact_dz_device_interface_counters f%s
			WHERE %s
				AND delta_duration > 0
				AND in_octets_delta >= 0
				AND out_octets_delta >= 0
				%s
				%s
			GROUP BY bucket_ts, f.device_pk, f.intf, f.link_pk%s
		),
		with_metric AS (
			SELECT
				ir.bucket_ts, ir.device_pk, ir.intf, ir.link_pk, ir.in_bps, ir.out_bps,
				%s AS metric_val_in,
				%s AS metric_val_out
				%s
				%s
			FROM interface_rates ir
			%s
			WHERE 1=1 %s%s
		)
		SELECT %s
		FROM with_metric
		WHERE 1=1 %s
		GROUP BY %s
		ORDER BY bucket_ts`,
		bucketInterval, userTunnelSelect, rawIntfJoin, timeFilter,
		intfTypeSQL, intfFilterSQL,
		userTunnelGroupBy,
		metricExprIn, metricExprOut, groupBySelect,
		userTunnelPassthrough,
		dimJoins, filterSQL, userKindSQL,
		selectCols, metricFilter, groupByCols)

	return
}

// BuildDrilldownQueryRaw builds the drilldown query using raw fact_dz_device_interface_counters
// for sub-5m bucket granularity. Same interface as BuildDrilldownQuery.
func BuildDrilldownQueryRaw(timeFilter, bucketInterval, devicePk, intfFilter string, needsInterfaceJoin bool) string {
	var diJoin string
	if needsInterfaceJoin {
		diJoin = "\n\t\tLEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}
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
		FROM fact_dz_device_interface_counters f%s
		WHERE %s
			AND f.device_pk = '%s'
			%s
			AND f.delta_duration > 0
			AND f.in_octets_delta >= 0
			AND f.out_octets_delta >= 0
		GROUP BY time, f.intf
		ORDER BY time, f.intf`,
		bucketInterval, diJoin, timeFilter, escapeSingleQuote(devicePk), intfFilter)
}

// BuildTopQuery builds the ClickHouse query for the top endpoint.
// Reads from device_interface_rollup_5m and aggregates across all buckets.
func BuildTopQuery(timeFilter, entity, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL string, needsUserJoin, needsInterfaceJoin bool, limit int) string {
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
		orderCol = "COALESCE(toFloat64(l.bandwidth_bps), 0) - greatest(p95_in_bps, p95_out_bps)"
	}

	// When user join is needed, join users for filtering
	var userJoinSQL, userKindFilter string
	if needsUserJoin {
		userJoinSQL = " LEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
		userKindFilter = userKindSQL
	}

	var intfJoinSQL string
	if needsInterfaceJoin {
		intfJoinSQL = " LEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}

	if entity == "device" {
		// Device-level: aggregate across all interfaces per device.
		// No link_pk in GROUP BY (a device has many links) → no utilization.
		switch orderCol {
		case "max_util", "p95_util", "avg_util":
			orderCol = "max_in_bps + max_out_bps"
		case "bandwidth_bps":
			orderCol = "max_in_bps + max_out_bps"
		case "COALESCE(toFloat64(l.bandwidth_bps), 0) - greatest(p95_in_bps, p95_out_bps)":
			orderCol = "max_in_bps + max_out_bps"
		}
		return fmt.Sprintf(`
			WITH device_rates AS (
				SELECT
					f.device_pk AS device_pk,
					max(f.max_in_bps) AS max_in_bps,
					max(f.max_out_bps) AS max_out_bps,
					avg(f.avg_in_bps) AS avg_in_bps,
					avg(f.avg_out_bps) AS avg_out_bps,
					quantile(0.95)(f.max_in_bps) AS p95_in_bps,
					quantile(0.95)(f.max_out_bps) AS p95_out_bps
				FROM device_interface_rollup_5m f%s%s
				WHERE f.%s
					%s
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
			userJoinSQL, intfJoinSQL, timeFilter, intfTypeSQL, intfFilterSQL, userKindFilter, filterSQL, orderCol, dir, limit)
	}

	bwExpr := bandwidthExpr(needsInterfaceJoin)

	var intfJoinIRSQL string
	if needsInterfaceJoin {
		intfJoinIRSQL = " LEFT JOIN dz_device_interfaces_current di ON ir.device_pk = di.device_pk AND ir.intf = di.intf"
	}

	// Interface-level: GROUP BY includes intf and link_pk for utilization.
	return fmt.Sprintf(`
		WITH interface_rates AS (
			SELECT
				f.device_pk,
				f.intf,
				f.link_pk,
				max(f.max_in_bps) AS max_in_bps,
				max(f.max_out_bps) AS max_out_bps,
				avg(f.avg_in_bps) AS avg_in_bps,
				avg(f.avg_out_bps) AS avg_out_bps,
				quantile(0.95)(f.max_in_bps) AS p95_in_bps,
				quantile(0.95)(f.max_out_bps) AS p95_out_bps
			FROM device_interface_rollup_5m f%s%s
			WHERE f.%s
				%s
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
			COALESCE(toFloat64(%s), 0) AS bandwidth_bps,
			CASE WHEN %s > 0
				THEN greatest(ir.max_in_bps, ir.max_out_bps) / %s
				ELSE 0 END AS max_util,
			CASE WHEN %s > 0
				THEN greatest(ir.avg_in_bps, ir.avg_out_bps) / %s
				ELSE 0 END AS avg_util,
			CASE WHEN %s > 0
				THEN greatest(ir.p95_in_bps, ir.p95_out_bps) / %s
				ELSE 0 END AS p95_util,
			ir.max_in_bps,
			ir.max_out_bps
		FROM interface_rates ir
		INNER JOIN dz_devices_current d ON ir.device_pk = d.pk
		LEFT JOIN dz_links_current l ON ir.link_pk = l.pk%s
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
		WHERE 1=1 %s
		ORDER BY %s %s
		LIMIT %d`,
		userJoinSQL, intfJoinSQL, timeFilter, intfTypeSQL, intfFilterSQL, userKindFilter,
		bwExpr, bwExpr, bwExpr, bwExpr, bwExpr, bwExpr, bwExpr,
		intfJoinIRSQL,
		filterSQL, orderCol, dir, limit)
}

// BuildDrilldownQuery builds the main ClickHouse query for the drilldown endpoint.
// Reads from device_interface_rollup_5m and re-aggregates into the requested bucket.
func BuildDrilldownQuery(timeFilter, bucketInterval, devicePk, intfFilter string, needsInterfaceJoin bool) string {
	var diJoin string
	if needsInterfaceJoin {
		diJoin = "\n\t\tLEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}
	return fmt.Sprintf(`
		SELECT
			formatDateTime(toStartOfInterval(f.bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			f.intf,
			max(f.max_in_bps) AS in_bps,
			max(f.max_out_bps) AS out_bps,
			toInt64(sum(f.in_discards)) AS in_discards,
			toInt64(sum(f.out_discards)) AS out_discards,
			max(f.max_in_pps) AS in_pps,
			max(f.max_out_pps) AS out_pps
		FROM device_interface_rollup_5m f%s
		WHERE f.%s
			AND f.device_pk = '%s'
			%s
		GROUP BY time, f.intf
		ORDER BY time, f.intf`,
		bucketInterval, diJoin, timeFilter, escapeSingleQuote(devicePk), intfFilter)
}

// BuildBurstinessQuery builds the ClickHouse query for the burstiness endpoint.
// Reads from device_interface_rollup_5m. Each rollup row already represents one
// 5-minute bucket with max throughput, so we compute P50/P99 across buckets directly.
func BuildBurstinessQuery(timeFilter, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL string, needsUserJoin, needsInterfaceJoin bool, threshold float64, minBps, minPeakBps float64, limit, offset int) string {
	// Validate sort direction
	dir := "DESC"
	if sortDir == "ASC" {
		dir = "ASC"
	}

	// Validate sort metric
	orderCol := "spike_count"
	switch sortMetric {
	case "spike_count":
		orderCol = "spike_count"
	case "max_spike_ratio":
		orderCol = "max_spike_ratio"
	case "p50_bps":
		orderCol = "p50_bps"
	case "max_spike_bps":
		orderCol = "max_spike_bps"
	}

	var userJoinSQL, userKindFilter string
	if needsUserJoin {
		userJoinSQL = "\n\t\t\tLEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
		userKindFilter = userKindSQL
	}

	var intfJoinSQL string
	if needsInterfaceJoin {
		intfJoinSQL = "\n\t\t\tLEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}

	// Two-pass query: first compute per-interface baselines, then count spikes.
	// A spike is a 5-min bucket where max throughput exceeds 2x the median (P50).
	// The spike threshold is the greater of 2*P50 and the absolute min_bps floor.
	// Wraps the result in a subquery with count() OVER() for total count before LIMIT.
	return fmt.Sprintf(`
		WITH baselines AS (
			SELECT
				f.device_pk AS b_device_pk,
				f.intf AS b_intf,
				f.link_pk AS b_link_pk,
				quantile(0.5)(greatest(f.max_in_bps, f.max_out_bps)) AS p50_bps,
				quantile(0.95)(greatest(f.max_in_bps, f.max_out_bps)) AS p95_bps
			FROM device_interface_rollup_5m f
			INNER JOIN dz_devices_current d ON f.device_pk = d.pk
			LEFT JOIN dz_links_current l ON f.link_pk = l.pk%s%s
			WHERE f.%s
				%s
				%s
				%s
				%s
			GROUP BY f.device_pk, f.intf, f.link_pk
			HAVING p50_bps >= %f
		),
		spike_results AS (
			SELECT
				f.device_pk,
				d.code AS device_code,
				f.intf,
				COALESCE(m.code, '') AS metro_code,
				COALESCE(co.code, '') AS contributor_code,
				COALESCE(toFloat64(l.bandwidth_bps), 0) AS bandwidth_bps,
				b.p50_bps,
				b.p95_bps,
				countIf(greatest(f.max_in_bps, f.max_out_bps) > greatest(2 * b.p50_bps, b.p95_bps)) AS spike_count,
				count() AS total_buckets,
				maxIf(greatest(f.max_in_bps, f.max_out_bps), greatest(f.max_in_bps, f.max_out_bps) > greatest(2 * b.p50_bps, b.p95_bps)) AS max_spike_bps,
				if(b.p50_bps > 0,
					maxIf(greatest(f.max_in_bps, f.max_out_bps), greatest(f.max_in_bps, f.max_out_bps) > greatest(2 * b.p50_bps, b.p95_bps)) / b.p50_bps,
					0) AS max_spike_ratio,
				argMaxIf(f.bucket_ts, greatest(f.max_in_bps, f.max_out_bps), greatest(f.max_in_bps, f.max_out_bps) > greatest(2 * b.p50_bps, b.p95_bps)) AS last_spike_time,
				CASE WHEN argMax(
					CASE WHEN f.max_in_bps >= f.max_out_bps THEN 1 ELSE 0 END,
					greatest(f.max_in_bps, f.max_out_bps)
				) = 1 THEN 'rx' ELSE 'tx' END AS peak_direction
			FROM device_interface_rollup_5m f
			INNER JOIN baselines b ON f.device_pk = b.b_device_pk AND f.intf = b.b_intf AND f.link_pk = b.b_link_pk
			INNER JOIN dz_devices_current d ON f.device_pk = d.pk
			LEFT JOIN dz_links_current l ON f.link_pk = l.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk
			WHERE f.%s
			GROUP BY f.device_pk, f.intf, f.link_pk, d.code, m.code, l.bandwidth_bps, co.code, b.p50_bps, b.p95_bps
			HAVING spike_count > 0 AND max_spike_bps >= %f
		)
		SELECT *, count() OVER () AS total_matching
		FROM spike_results
		ORDER BY %s %s
		LIMIT %d OFFSET %d`,
		userJoinSQL, intfJoinSQL, timeFilter, intfTypeSQL, filterSQL, intfFilterSQL, userKindFilter,
		minBps,
		timeFilter,
		minPeakBps, orderCol, dir, limit, offset)
}

// BuildHealthQuery builds the ClickHouse query for the interface health endpoint.
func BuildHealthQuery(timeFilter, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL string, needsUserJoin, needsInterfaceJoin bool, limit int) string {
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

	var userJoinSQL, userKindFilter string
	if needsUserJoin {
		userJoinSQL = "\n\t\tLEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
		userKindFilter = userKindSQL
	}

	var intfJoinSQL string
	if needsInterfaceJoin {
		intfJoinSQL = "\n\t\tLEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf"
	}

	return fmt.Sprintf(`
		WITH health AS (
			SELECT
				f.device_pk AS device_pk,
				f.intf AS intf,
				f.link_pk AS link_pk,
				toInt64(sum(f.in_errors) + sum(f.out_errors)) AS total_errors,
				toInt64(sum(f.in_discards) + sum(f.out_discards)) AS total_discards,
				toInt64(sum(f.in_fcs_errors)) AS total_fcs_errors,
				toInt64(sum(f.carrier_transitions)) AS total_carrier_transitions
			FROM device_interface_rollup_5m f%s%s
			WHERE f.%s
				%s
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
		userJoinSQL, intfJoinSQL, timeFilter, intfTypeSQL, intfFilterSQL, userKindFilter,
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

func (a *API) GetTrafficDashboardHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, _ := rollupTimeFilter(r)

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

	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, _, _, _, needsUserJoin, needsInterfaceJoin := buildDimensionFilters(r)

	query := BuildHealthQuery(timeFilter, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, needsUserJoin, needsInterfaceJoin, limit)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic dashboard health query error", "error", err, "query", query)
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
			logError("traffic dashboard health row scan error", "error", err)
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

func (a *API) GetTrafficDashboardStress(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

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

	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin := buildDimensionFilters(r)

	var query string
	var grouped bool
	if useRaw {
		query, grouped = BuildStressQueryRaw(timeFilter, bucketInterval, metric, groupBy, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, threshold,
			needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin)
	} else {
		query, grouped = BuildStressQuery(timeFilter, bucketInterval, metric, groupBy, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, threshold,
			needsDeviceJoin, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin)
	}

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic dashboard stress query error", "error", err, "query", query)
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
				logError("traffic dashboard stress row scan error", "error", err)
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
				logError("traffic dashboard stress row scan error", "error", err)
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

func (a *API) GetTrafficDashboardTop(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, _ := rollupTimeFilter(r)

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

	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, _, _, _, needsUserJoin, needsInterfaceJoin := buildDimensionFilters(r)

	query := BuildTopQuery(timeFilter, entity, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, needsUserJoin, needsInterfaceJoin, limit)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic dashboard top query error", "error", err, "query", query)
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
			logError("traffic dashboard top row scan error", "error", err)
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

func (a *API) GetTrafficDashboardDrilldown(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	devicePk := r.URL.Query().Get("device_pk")
	if devicePk == "" {
		http.Error(w, "device_pk is required", http.StatusBadRequest)
		return
	}

	intf := r.URL.Query().Get("intf")
	intfType := r.URL.Query().Get("intf_type")

	var intfFilter string
	var needsDiJoin bool
	if intf != "" {
		intfFilter = fmt.Sprintf("AND f.intf = '%s'", escapeSingleQuote(intf))
	} else {
		intfFilter, needsDiJoin = buildIntfTypeFilter(intfType)
	}

	var query string
	if useRaw {
		query = BuildDrilldownQueryRaw(timeFilter, bucketInterval, devicePk, intfFilter, needsDiJoin)
	} else {
		query = BuildDrilldownQuery(timeFilter, bucketInterval, devicePk, intfFilter, needsDiJoin)
	}

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic dashboard drilldown query error", "error", err, "query", query)
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
			logError("traffic dashboard drilldown row scan error", "error", err)
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

		metaTable := "device_interface_rollup_5m"
		if useRaw {
			metaTable = "fact_dz_device_interface_counters"
		}
		metaQuery := fmt.Sprintf(`
			SELECT
				f.intf,
				COALESCE(toFloat64(l.bandwidth_bps), 0) AS bandwidth_bps,
				COALESCE(l.link_type, '') AS link_type
			FROM (
				SELECT DISTINCT intf, link_pk
				FROM %s
				WHERE device_pk = '%s'
					AND intf IN (%s)
					AND %s
			) f
			LEFT JOIN dz_links_current l ON f.link_pk = l.pk`,
			metaTable,
			escapeSingleQuote(devicePk),
			strings.Join(quoted, ","),
			timeFilter)

		metaRows, err := a.envDB(ctx).Query(ctx, metaQuery)
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
	ContributorCode string  `json:"contributor_code"`
	BandwidthBps    float64 `json:"bandwidth_bps"`
	P50Bps          float64 `json:"p50_bps"`
	P95Bps          float64 `json:"p95_bps"`
	SpikeCount      uint64  `json:"spike_count"`
	TotalBuckets    uint64  `json:"total_buckets"`
	MaxSpikeBps     float64 `json:"max_spike_bps"`
	MaxSpikeRatio   float64 `json:"max_spike_ratio"`
	LastSpikeTime   string  `json:"last_spike_time"`
	PeakDirection   string  `json:"peak_direction"`
}

type BurstinessResponse struct {
	Entities []BurstinessEntity `json:"entities"`
	Total    uint64             `json:"total"`
}

func (a *API) GetTrafficDashboardBurstiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	timeFilter, _ := rollupTimeFilter(r)

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

	minPeakBps := 0.0
	if m := r.URL.Query().Get("min_peak_bps"); m != "" {
		if v, err := strconv.ParseFloat(m, 64); err == nil && v >= 0 {
			minPeakBps = v
		}
	}

	offset := 0
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}

	sortMetric := r.URL.Query().Get("sort")
	if sortMetric == "" {
		sortMetric = "spike_count"
	}
	sortDir := strings.ToUpper(r.URL.Query().Get("dir"))

	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, _, _, _, needsUserJoin, needsInterfaceJoin := buildDimensionFilters(r)

	query := BuildBurstinessQuery(timeFilter, sortMetric, sortDir, filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, needsUserJoin, needsInterfaceJoin, threshold, minBps, minPeakBps, limit, offset)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic dashboard burstiness query error", "error", err, "query", query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	entities := []BurstinessEntity{}
	var total uint64
	for rows.Next() {
		var e BurstinessEntity
		var lastSpikeTime time.Time
		if err := rows.Scan(&e.DevicePk, &e.DeviceCode, &e.Intf, &e.MetroCode,
			&e.ContributorCode, &e.BandwidthBps, &e.P50Bps, &e.P95Bps,
			&e.SpikeCount, &e.TotalBuckets, &e.MaxSpikeBps, &e.MaxSpikeRatio,
			&lastSpikeTime, &e.PeakDirection, &total); err != nil {
			logError("traffic dashboard burstiness row scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !lastSpikeTime.IsZero() {
			e.LastSpikeTime = lastSpikeTime.UTC().Format(time.RFC3339)
		}
		entities = append(entities, e)
	}

	resp := BurstinessResponse{Entities: entities, Total: total}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
