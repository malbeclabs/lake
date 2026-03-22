package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// --- Raw source toggle ---

type rawSourceKey struct{}

// withRawSource returns a context that signals queries to use raw fact tables.
func withRawSource(ctx context.Context) context.Context {
	return context.WithValue(ctx, rawSourceKey{}, true)
}

// isRawSource checks if the context requests raw fact table queries.
func isRawSource(ctx context.Context) bool {
	v, _ := ctx.Value(rawSourceKey{}).(bool)
	return v
}

// --- Search filters ---

// statusFilter represents a single search filter (e.g. "device:NYC-A").
type statusFilter struct {
	Type  string // "device", "link", "metro", "contributor"
	Value string // search value (case-insensitive substring match, except metro=exact)
}

// parseStatusFilterParam parses a comma-separated filter param into typed filters.
// Format: "type:value,type:value" e.g. "device:NYC-A,metro:NYC"
func parseStatusFilterParam(raw string) []statusFilter {
	if raw == "" {
		return nil
	}
	var filters []statusFilter
	for _, part := range strings.Split(raw, ",") {
		idx := strings.Index(part, ":")
		if idx < 0 {
			continue
		}
		ft := strings.TrimSpace(part[:idx])
		fv := strings.TrimSpace(part[idx+1:])
		if ft != "" && fv != "" {
			filters = append(filters, statusFilter{Type: ft, Value: strings.ToLower(fv)})
		}
	}
	return filters
}

// linkMatchesFilters checks if a link matches the given filters.
// Logic: AND across filter types, OR within same type.
func linkMatchesFilters(meta *statusLinkMeta, filters []statusFilter) bool {
	if len(filters) == 0 {
		return true
	}
	// Group by type
	byType := make(map[string][]string)
	for _, f := range filters {
		byType[f.Type] = append(byType[f.Type], f.Value)
	}
	for ft, values := range byType {
		matched := false
		for _, v := range values {
			switch ft {
			case "link":
				if strings.Contains(strings.ToLower(meta.Code), v) {
					matched = true
				}
			case "device":
				if strings.Contains(strings.ToLower(meta.SideADevice), v) || strings.Contains(strings.ToLower(meta.SideZDevice), v) {
					matched = true
				}
			case "metro":
				if strings.EqualFold(meta.SideAMetro, v) || strings.EqualFold(meta.SideZMetro, v) {
					matched = true
				}
			case "contributor":
				if strings.Contains(strings.ToLower(meta.Contributor), v) {
					matched = true
				}
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// deviceMatchesFilters checks if a device matches the given filters.
func deviceMatchesFilters(meta *statusDeviceMeta, filters []statusFilter) bool {
	if len(filters) == 0 {
		return true
	}
	byType := make(map[string][]string)
	for _, f := range filters {
		byType[f.Type] = append(byType[f.Type], f.Value)
	}
	for ft, values := range byType {
		matched := false
		for _, v := range values {
			switch ft {
			case "device":
				if strings.Contains(strings.ToLower(meta.Code), v) {
					matched = true
				}
			case "metro":
				if strings.EqualFold(meta.Metro, v) {
					matched = true
				}
			case "contributor":
				if strings.Contains(strings.ToLower(meta.Contributor), v) {
					matched = true
				}
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// --- Bucket parameter helpers ---

// bucketParams holds resolved time-window and bucket-size parameters.
type bucketParams struct {
	TotalMinutes  int
	TotalHours    int
	BucketMinutes int
	BucketCount   int
	TimeRange     string // normalized: "1h", "3h", "6h", "12h", "24h", "3d", "7d"
	UseRaw        bool   // query raw fact tables instead of rollup tables
}

// parseBucketParams resolves a time range string and requested bucket count into
// concrete parameters for rollup queries.
func parseBucketParams(timeRange string, requestedBuckets int) bucketParams {
	var totalMinutes int
	switch timeRange {
	case "1h":
		totalMinutes = 60
	case "3h":
		totalMinutes = 3 * 60
	case "6h":
		totalMinutes = 6 * 60
	case "12h":
		totalMinutes = 12 * 60
	case "3d":
		totalMinutes = 3 * 24 * 60
	case "7d":
		totalMinutes = 7 * 24 * 60
	default:
		timeRange = "24h"
		totalMinutes = 24 * 60
	}

	bucketMinutes := snapBucketMinutes(totalMinutes / requestedBuckets)
	return bucketParams{
		TotalMinutes:  totalMinutes,
		TotalHours:    totalMinutes / 60,
		BucketMinutes: bucketMinutes,
		BucketCount:   totalMinutes / bucketMinutes,
		TimeRange:     timeRange,
	}
}

// --- Link rollup query ---

// linkRollupRow represents a single row from a re-bucketed link rollup query.
type linkRollupRow struct {
	BucketTS time.Time
	LinkPK   string

	// Per-direction latency/loss (sample-weighted avg for re-bucketed data)
	AAvgRttUs float64
	AMinRttUs float64
	AP50RttUs float64
	AP90RttUs float64
	AP95RttUs float64
	AP99RttUs float64
	AMaxRttUs float64
	ALossPct  float64
	ASamples  uint64

	ZAvgRttUs float64
	ZMinRttUs float64
	ZP50RttUs float64
	ZP90RttUs float64
	ZP95RttUs float64
	ZP99RttUs float64
	ZMaxRttUs float64
	ZLossPct  float64
	ZSamples  uint64

	// Entity state (baked in at rollup write time)
	Status       string // latest status in display bucket
	Provisioning bool   // latest provisioning flag
	ISISDown     bool   // true if ISIS was down in any sub-bucket
	WasDrained   bool   // true if link was drained in any sub-bucket
}

// linkBucketKey uniquely identifies a link+bucket combination.
type linkBucketKey struct {
	LinkPK   string
	BucketTS time.Time
}

// queryLinkRollup reads link_rollup_5m FINAL with re-bucketing to the display
// bucket size. Sample-weighted avg for latency, max for loss, sum for samples.
// Returns a map keyed by (link_pk, bucket_ts).
// If linkPKs is non-empty, only those links are returned.
func queryLinkRollup(ctx context.Context, db driver.Conn, params bucketParams, linkPKs ...string) (map[linkBucketKey]*linkRollupRow, error) {
	bucketExpr := bucketIntervalExpr("bucket_ts", params.BucketMinutes)

	var filterClause string
	var args []any
	args = append(args, time.Now().UTC().Add(-time.Duration(params.TotalMinutes)*time.Minute))

	if len(linkPKs) > 0 {
		filterClause = " AND link_pk IN ($2)"
		args = append(args, linkPKs)
	}

	// Build the query. In raw mode, we prepend CTEs that compute from fact tables
	// and reference them as the source. In rollup mode, we read directly from the table.
	var query string
	if params.UseRaw {
		query = fmt.Sprintf(`
		WITH
		latency_buckets AS (
			SELECT
				toStartOfFiveMinutes(f.event_ts) AS bucket_ts,
				f.link_pk AS link_pk,
				avgIf(f.rtt_us, f.origin_device_pk = l.side_a_pk) AS a_avg_rtt_us,
				toFloat64(minIf(f.rtt_us, f.origin_device_pk = l.side_a_pk)) AS a_min_rtt_us,
				quantileIf(0.50)(f.rtt_us, f.origin_device_pk = l.side_a_pk) AS a_p50_rtt_us,
				quantileIf(0.90)(f.rtt_us, f.origin_device_pk = l.side_a_pk) AS a_p90_rtt_us,
				quantileIf(0.95)(f.rtt_us, f.origin_device_pk = l.side_a_pk) AS a_p95_rtt_us,
				quantileIf(0.99)(f.rtt_us, f.origin_device_pk = l.side_a_pk) AS a_p99_rtt_us,
				toFloat64(maxIf(f.rtt_us, f.origin_device_pk = l.side_a_pk)) AS a_max_rtt_us,
				countIf(f.origin_device_pk = l.side_a_pk AND (f.loss = true OR f.rtt_us = 0)) * 100.0
					/ greatest(countIf(f.origin_device_pk = l.side_a_pk), 1) AS a_loss_pct,
				toUInt32(countIf(f.origin_device_pk = l.side_a_pk)) AS a_samples,
				avgIf(f.rtt_us, f.origin_device_pk != l.side_a_pk) AS z_avg_rtt_us,
				toFloat64(minIf(f.rtt_us, f.origin_device_pk != l.side_a_pk)) AS z_min_rtt_us,
				quantileIf(0.50)(f.rtt_us, f.origin_device_pk != l.side_a_pk) AS z_p50_rtt_us,
				quantileIf(0.90)(f.rtt_us, f.origin_device_pk != l.side_a_pk) AS z_p90_rtt_us,
				quantileIf(0.95)(f.rtt_us, f.origin_device_pk != l.side_a_pk) AS z_p95_rtt_us,
				quantileIf(0.99)(f.rtt_us, f.origin_device_pk != l.side_a_pk) AS z_p99_rtt_us,
				toFloat64(maxIf(f.rtt_us, f.origin_device_pk != l.side_a_pk)) AS z_max_rtt_us,
				countIf(f.origin_device_pk != l.side_a_pk AND (f.loss = true OR f.rtt_us = 0)) * 100.0
					/ greatest(countIf(f.origin_device_pk != l.side_a_pk), 1) AS z_loss_pct,
				toUInt32(countIf(f.origin_device_pk != l.side_a_pk)) AS z_samples
			FROM fact_dz_device_link_latency f
			JOIN dz_links_current l ON f.link_pk = l.pk
			WHERE f.event_ts >= $1
			GROUP BY bucket_ts, f.link_pk
		),
		-- Link state as of each bucket's end time (bucket_ts + 5min).
		-- Uses argMax over all snapshots up to that point per link.
		link_state_asof AS (
			SELECT
				lb.bucket_ts AS ls_bucket, lb.link_pk AS ls_pk,
				argMax(lh.status, lh.snapshot_ts) AS status,
				argMax(lh.committed_rtt_ns, lh.snapshot_ts) AS committed_rtt_ns
			FROM latency_buckets lb
			JOIN dim_dz_links_history lh ON lb.link_pk = lh.pk AND lh.snapshot_ts <= lb.bucket_ts + INTERVAL 5 MINUTE
			GROUP BY ls_bucket, ls_pk
		),
		-- ISIS adjacency state as of each bucket's end time
		isis_state_asof AS (
			SELECT
				lb.bucket_ts AS is_bucket, lb.link_pk AS is_pk,
				argMax(ih.is_deleted, ih.snapshot_ts) AS is_deleted
			FROM latency_buckets lb
			JOIN dim_isis_adjacencies_history ih ON lb.link_pk = ih.link_pk AND ih.snapshot_ts <= lb.bucket_ts + INTERVAL 5 MINUTE
			WHERE ih.link_pk != ''
			GROUP BY is_bucket, is_pk
		),
		raw_source AS (
			SELECT
				lb.bucket_ts AS bucket_ts, lb.link_pk AS link_pk,
				lb.a_avg_rtt_us, lb.a_min_rtt_us, lb.a_p50_rtt_us, lb.a_p90_rtt_us,
				lb.a_p95_rtt_us, lb.a_p99_rtt_us, lb.a_max_rtt_us, lb.a_loss_pct, lb.a_samples,
				lb.z_avg_rtt_us, lb.z_min_rtt_us, lb.z_p50_rtt_us, lb.z_p90_rtt_us,
				lb.z_p95_rtt_us, lb.z_p99_rtt_us, lb.z_max_rtt_us, lb.z_loss_pct, lb.z_samples,
				COALESCE(ls.status, '') AS status,
				COALESCE(ls.committed_rtt_ns, 0) = 500000 AS provisioning,
				CASE
					WHEN ia.is_pk IS NULL THEN false
					ELSE ia.is_deleted = 1
				END AS isis_down
			FROM latency_buckets lb
			LEFT JOIN link_state_asof ls ON lb.link_pk = ls.ls_pk AND lb.bucket_ts = ls.ls_bucket
			LEFT JOIN isis_state_asof ia ON lb.link_pk = ia.is_pk AND lb.bucket_ts = ia.is_bucket
		),
		agg AS (
			SELECT
				%s as display_bucket,
				link_pk,
				sumIf(a_avg_rtt_us * a_samples, a_samples > 0) as a_w_avg,
				sumIf(a_p50_rtt_us * a_samples, a_samples > 0) as a_w_p50,
				sumIf(a_p90_rtt_us * a_samples, a_samples > 0) as a_w_p90,
				sumIf(a_p95_rtt_us * a_samples, a_samples > 0) as a_w_p95,
				sumIf(a_p99_rtt_us * a_samples, a_samples > 0) as a_w_p99,
				minIf(a_min_rtt_us, a_samples > 0) as a_min,
				maxIf(a_max_rtt_us, a_samples > 0) as a_max,
				max(a_loss_pct) as a_loss,
				toUInt64(sum(a_samples)) as a_n,
				sumIf(z_avg_rtt_us * z_samples, z_samples > 0) as z_w_avg,
				sumIf(z_p50_rtt_us * z_samples, z_samples > 0) as z_w_p50,
				sumIf(z_p90_rtt_us * z_samples, z_samples > 0) as z_w_p90,
				sumIf(z_p95_rtt_us * z_samples, z_samples > 0) as z_w_p95,
				sumIf(z_p99_rtt_us * z_samples, z_samples > 0) as z_w_p99,
				minIf(z_min_rtt_us, z_samples > 0) as z_min,
				maxIf(z_max_rtt_us, z_samples > 0) as z_max,
				max(z_loss_pct) as z_loss,
				toUInt64(sum(z_samples)) as z_n,
				argMax(status, bucket_ts) as agg_status,
				argMax(provisioning, bucket_ts) as agg_provisioning,
				max(isis_down) as agg_isis_down,
				max(status IN ('soft-drained', 'hard-drained')) as agg_was_drained
			FROM raw_source
			WHERE bucket_ts >= $1%s
			GROUP BY display_bucket, link_pk
		)
		SELECT
			display_bucket, link_pk,
			if(a_n > 0, a_w_avg / a_n, 0) as a_avg_rtt_us,
			a_min as a_min_rtt_us,
			if(a_n > 0, a_w_p50 / a_n, 0) as a_p50_rtt_us,
			if(a_n > 0, a_w_p90 / a_n, 0) as a_p90_rtt_us,
			if(a_n > 0, a_w_p95 / a_n, 0) as a_p95_rtt_us,
			if(a_n > 0, a_w_p99 / a_n, 0) as a_p99_rtt_us,
			a_max as a_max_rtt_us, a_loss as a_loss_pct, a_n as a_samples,
			if(z_n > 0, z_w_avg / z_n, 0) as z_avg_rtt_us,
			z_min as z_min_rtt_us,
			if(z_n > 0, z_w_p50 / z_n, 0) as z_p50_rtt_us,
			if(z_n > 0, z_w_p90 / z_n, 0) as z_p90_rtt_us,
			if(z_n > 0, z_w_p95 / z_n, 0) as z_p95_rtt_us,
			if(z_n > 0, z_w_p99 / z_n, 0) as z_p99_rtt_us,
			z_max as z_max_rtt_us, z_loss as z_loss_pct, z_n as z_samples,
			agg_status as status, agg_provisioning as provisioning, agg_isis_down as isis_down,
			agg_was_drained as was_drained
		FROM agg
		ORDER BY link_pk, display_bucket
	`, bucketExpr, filterClause)
	} else {
		query = fmt.Sprintf(`
		WITH agg AS (
			SELECT
				%s as display_bucket,
				link_pk,
				sumIf(a_avg_rtt_us * a_samples, a_samples > 0) as a_w_avg,
				sumIf(a_p50_rtt_us * a_samples, a_samples > 0) as a_w_p50,
				sumIf(a_p90_rtt_us * a_samples, a_samples > 0) as a_w_p90,
				sumIf(a_p95_rtt_us * a_samples, a_samples > 0) as a_w_p95,
				sumIf(a_p99_rtt_us * a_samples, a_samples > 0) as a_w_p99,
				minIf(a_min_rtt_us, a_samples > 0) as a_min,
				maxIf(a_max_rtt_us, a_samples > 0) as a_max,
				max(a_loss_pct) as a_loss,
				toUInt64(sum(a_samples)) as a_n,
				sumIf(z_avg_rtt_us * z_samples, z_samples > 0) as z_w_avg,
				sumIf(z_p50_rtt_us * z_samples, z_samples > 0) as z_w_p50,
				sumIf(z_p90_rtt_us * z_samples, z_samples > 0) as z_w_p90,
				sumIf(z_p95_rtt_us * z_samples, z_samples > 0) as z_w_p95,
				sumIf(z_p99_rtt_us * z_samples, z_samples > 0) as z_w_p99,
				minIf(z_min_rtt_us, z_samples > 0) as z_min,
				maxIf(z_max_rtt_us, z_samples > 0) as z_max,
				max(z_loss_pct) as z_loss,
				toUInt64(sum(z_samples)) as z_n,
				argMax(status, bucket_ts) as agg_status,
				argMax(provisioning, bucket_ts) as agg_provisioning,
				max(isis_down) as agg_isis_down,
				max(status IN ('soft-drained', 'hard-drained')) as agg_was_drained
			FROM link_rollup_5m FINAL
			WHERE bucket_ts >= $1%s
			GROUP BY display_bucket, link_pk
		)
		SELECT
			display_bucket, link_pk,
			if(a_n > 0, a_w_avg / a_n, 0) as a_avg_rtt_us,
			a_min as a_min_rtt_us,
			if(a_n > 0, a_w_p50 / a_n, 0) as a_p50_rtt_us,
			if(a_n > 0, a_w_p90 / a_n, 0) as a_p90_rtt_us,
			if(a_n > 0, a_w_p95 / a_n, 0) as a_p95_rtt_us,
			if(a_n > 0, a_w_p99 / a_n, 0) as a_p99_rtt_us,
			a_max as a_max_rtt_us, a_loss as a_loss_pct, a_n as a_samples,
			if(z_n > 0, z_w_avg / z_n, 0) as z_avg_rtt_us,
			z_min as z_min_rtt_us,
			if(z_n > 0, z_w_p50 / z_n, 0) as z_p50_rtt_us,
			if(z_n > 0, z_w_p90 / z_n, 0) as z_p90_rtt_us,
			if(z_n > 0, z_w_p95 / z_n, 0) as z_p95_rtt_us,
			if(z_n > 0, z_w_p99 / z_n, 0) as z_p99_rtt_us,
			z_max as z_max_rtt_us, z_loss as z_loss_pct, z_n as z_samples,
			agg_status as status, agg_provisioning as provisioning, agg_isis_down as isis_down,
			agg_was_drained as was_drained
		FROM agg
		ORDER BY link_pk, display_bucket
	`, bucketExpr, filterClause)
	}

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("link rollup query: %w", err)
	}
	defer rows.Close()

	result := make(map[linkBucketKey]*linkRollupRow)
	for rows.Next() {
		var r linkRollupRow
		if err := rows.Scan(
			&r.BucketTS, &r.LinkPK,
			&r.AAvgRttUs, &r.AMinRttUs, &r.AP50RttUs, &r.AP90RttUs, &r.AP95RttUs, &r.AP99RttUs, &r.AMaxRttUs, &r.ALossPct, &r.ASamples,
			&r.ZAvgRttUs, &r.ZMinRttUs, &r.ZP50RttUs, &r.ZP90RttUs, &r.ZP95RttUs, &r.ZP99RttUs, &r.ZMaxRttUs, &r.ZLossPct, &r.ZSamples,
			&r.Status, &r.Provisioning, &r.ISISDown, &r.WasDrained,
		); err != nil {
			return nil, fmt.Errorf("link rollup scan: %w", err)
		}
		r.BucketTS = r.BucketTS.UTC()
		key := linkBucketKey{LinkPK: r.LinkPK, BucketTS: r.BucketTS}
		result[key] = &r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("link rollup rows: %w", err)
	}

	return result, nil
}

// --- Interface rollup query ---

// interfaceGroupBy controls how interface rollup data is aggregated.
type interfaceGroupBy int

const (
	// groupByLinkSide groups by (link_pk, link_side) for link timeline views.
	groupByLinkSide interfaceGroupBy = iota
	// groupByDevice groups by device_pk for device timeline views.
	groupByDevice
	// groupByDeviceIntf groups by (device_pk, intf) for per-interface detail views.
	groupByDeviceIntf
)

// interfaceRollupOpts configures the interface rollup query.
type interfaceRollupOpts struct {
	GroupBy    interfaceGroupBy
	LinkPKs    []string // filter by link_pk (for link views)
	DevicePKs  []string // filter by device_pk (for device views)
	UserOnly   bool     // filter to user_pk != '' (for user traffic queries)
	ErrorsOnly bool     // filter to rows with any errors > 0
}

// interfaceRollupRow represents a single row from a re-bucketed interface rollup query.
type interfaceRollupRow struct {
	BucketTS time.Time

	// Grouping keys (which are populated depends on GroupBy)
	LinkPK   string
	LinkSide string
	DevicePK string
	Intf     string

	// Error/discard counters (summed across sub-buckets)
	InErrors           uint64
	OutErrors          uint64
	InFcsErrors        uint64
	InDiscards         uint64
	OutDiscards        uint64
	CarrierTransitions uint64

	// Traffic rates (sample-weighted avg across sub-buckets)
	AvgInBps  float64
	MaxInBps  float64
	AvgOutBps float64
	MaxOutBps float64
	AvgInPps  float64
	MaxInPps  float64
	AvgOutPps float64
	MaxOutPps float64

	// Entity state
	Status          string // latest in display bucket
	ISISOverload    bool   // true if overload in any sub-bucket
	ISISUnreachable bool   // true if unreachable in any sub-bucket
	WasDrained      bool   // true if drained in any sub-bucket

	// User context (for user traffic queries)
	UserPK string
}

// queryInterfaceRollup reads device_interface_rollup_5m FINAL with re-bucketing.
// Aggregation: sum for counters, avg/max for rates.
func queryInterfaceRollup(ctx context.Context, db driver.Conn, params bucketParams, opts interfaceRollupOpts) ([]interfaceRollupRow, error) {
	bucketExpr := bucketIntervalExpr("bucket_ts", params.BucketMinutes)

	// Build GROUP BY and SELECT keys based on grouping mode
	var groupKeys, selectKeys string
	switch opts.GroupBy {
	case groupByLinkSide:
		groupKeys = "link_pk, link_side"
		selectKeys = "link_pk, link_side, '' as device_pk, '' as intf"
	case groupByDevice:
		groupKeys = "device_pk"
		selectKeys = "'' as link_pk, '' as link_side, device_pk, '' as intf"
	case groupByDeviceIntf:
		groupKeys = "device_pk, intf"
		selectKeys = "link_pk, link_side, device_pk, intf"
	}

	// For groupByDeviceIntf, link_pk/link_side need special handling since they're not in GROUP BY
	var linkPKExpr, linkSideExpr string
	if opts.GroupBy == groupByDeviceIntf {
		linkPKExpr = "any(link_pk)"
		linkSideExpr = "any(link_side)"
		selectKeys = fmt.Sprintf("%s as link_pk, %s as link_side, device_pk, intf", linkPKExpr, linkSideExpr)
	}

	// Build WHERE filters
	var filters []string
	var args []any
	args = append(args, time.Now().UTC().Add(-time.Duration(params.TotalMinutes)*time.Minute))
	filters = append(filters, "bucket_ts >= $1")

	argIdx := 2
	if len(opts.LinkPKs) > 0 {
		filters = append(filters, fmt.Sprintf("link_pk IN ($%d)", argIdx))
		args = append(args, opts.LinkPKs)
		argIdx++
	}
	if len(opts.DevicePKs) > 0 {
		filters = append(filters, fmt.Sprintf("device_pk IN ($%d)", argIdx))
		args = append(args, opts.DevicePKs)
		argIdx++ //nolint:ineffassign // argIdx tracks position for future filter additions
	}
	if opts.UserOnly {
		filters = append(filters, "user_pk != ''")
	}
	whereClause := strings.Join(filters, " AND ")

	// ErrorsOnly uses HAVING (post-aggregation) since the column names collide
	// with aggregate aliases — ClickHouse resolves aliases in WHERE, causing
	// "aggregate inside aggregate" errors.
	// ErrorsOnly uses HAVING (post-aggregation). Aliases must differ from column
	// names to avoid ClickHouse resolving them inside the aggregate functions.
	havingClause := ""
	if opts.ErrorsOnly {
		havingClause = "HAVING total_in_errors > 0 OR total_out_errors > 0 OR total_in_fcs_errors > 0 OR total_in_discards > 0 OR total_out_discards > 0 OR total_carrier_transitions > 0"
	}

	query := fmt.Sprintf(`
		SELECT
			%s as display_bucket,
			%s,
			-- Counters: sum (aliases prefixed to avoid collision with column names)
			sum(in_errors) as total_in_errors,
			sum(out_errors) as total_out_errors,
			sum(in_fcs_errors) as total_in_fcs_errors,
			sum(in_discards) as total_in_discards,
			sum(out_discards) as total_out_discards,
			sum(carrier_transitions) as total_carrier_transitions,
			-- Traffic rates: avg and max
			avg(avg_in_bps) as avg_in_bps,
			max(max_in_bps) as max_in_bps,
			avg(avg_out_bps) as avg_out_bps,
			max(max_out_bps) as max_out_bps,
			avg(avg_in_pps) as avg_in_pps,
			max(max_in_pps) as max_in_pps,
			avg(avg_out_pps) as avg_out_pps,
			max(max_out_pps) as max_out_pps,
			-- Entity state
			argMax(status, bucket_ts) as agg_status,
			max(isis_overload) as isis_overload,
			max(isis_unreachable) as isis_unreachable,
			max(status IN ('soft-drained', 'hard-drained')) as was_drained,
			-- User context
			anyIf(user_pk, user_pk != '') as user_pk
		FROM %s
		WHERE %s
		GROUP BY display_bucket, %s
		%s
		ORDER BY %s, display_bucket
	`, bucketExpr, selectKeys, func() string {
		if params.UseRaw {
			return rawInterfaceSource()
		}
		return "device_interface_rollup_5m FINAL"
	}(), whereClause, groupKeys, havingClause, groupKeys)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("interface rollup query: %w", err)
	}
	defer rows.Close()

	var result []interfaceRollupRow
	for rows.Next() {
		var r interfaceRollupRow
		if err := rows.Scan(
			&r.BucketTS,
			&r.LinkPK, &r.LinkSide, &r.DevicePK, &r.Intf,
			&r.InErrors, &r.OutErrors, &r.InFcsErrors, &r.InDiscards, &r.OutDiscards, &r.CarrierTransitions,
			&r.AvgInBps, &r.MaxInBps, &r.AvgOutBps, &r.MaxOutBps,
			&r.AvgInPps, &r.MaxInPps, &r.AvgOutPps, &r.MaxOutPps,
			&r.Status, &r.ISISOverload, &r.ISISUnreachable, &r.WasDrained,
			&r.UserPK,
		); err != nil {
			return nil, fmt.Errorf("interface rollup scan: %w", err)
		}
		r.BucketTS = r.BucketTS.UTC()
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("interface rollup rows: %w", err)
	}

	return result, nil
}

// --- Helpers ---

// bucketIntervalExpr returns a ClickHouse expression that truncates a timestamp
// column to the given bucket size in minutes.
func bucketIntervalExpr(column string, bucketMinutes int) string {
	if bucketMinutes >= 60 && bucketMinutes%60 == 0 {
		return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d HOUR, 'UTC')", column, bucketMinutes/60)
	}
	return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d MINUTE, 'UTC')", column, bucketMinutes)
}

// rawInterfaceSource returns a flat subquery (no CTEs) that computes
// device_interface_rollup_5m-equivalent columns from raw fact tables.
// State uses current tables since ClickHouse doesn't support CTEs in subqueries.
func rawInterfaceSource() string {
	return `(
		SELECT
			toStartOfFiveMinutes(ic.event_ts) AS bucket_ts,
			ic.device_pk AS device_pk,
			ic.intf AS intf,
			anyIf(ic.link_pk, ic.link_pk != '') AS link_pk,
			anyIf(ic.link_side, ic.link_side != '') AS link_side,
			toUInt64(SUM(greatest(0, ic.in_errors_delta))) AS in_errors,
			toUInt64(SUM(greatest(0, ic.out_errors_delta))) AS out_errors,
			toUInt64(SUM(greatest(0, ic.in_fcs_errors_delta))) AS in_fcs_errors,
			toUInt64(SUM(greatest(0, ic.in_discards_delta))) AS in_discards,
			toUInt64(SUM(greatest(0, ic.out_discards_delta))) AS out_discards,
			toUInt64(SUM(greatest(0, ic.carrier_transitions_delta))) AS carrier_transitions,
			avgIf(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS avg_in_bps,
			minIf(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS min_in_bps,
			quantileIf(0.50)(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS p50_in_bps,
			quantileIf(0.90)(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS p90_in_bps,
			quantileIf(0.95)(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS p95_in_bps,
			quantileIf(0.99)(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS p99_in_bps,
			maxIf(ic.in_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.in_octets_delta >= 0) AS max_in_bps,
			avgIf(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS avg_out_bps,
			minIf(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS min_out_bps,
			quantileIf(0.50)(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS p50_out_bps,
			quantileIf(0.90)(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS p90_out_bps,
			quantileIf(0.95)(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS p95_out_bps,
			quantileIf(0.99)(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS p99_out_bps,
			maxIf(ic.out_octets_delta * 8 / ic.delta_duration, ic.delta_duration > 0 AND ic.out_octets_delta >= 0) AS max_out_bps,
			avgIf(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS avg_in_pps,
			minIf(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS min_in_pps,
			quantileIf(0.50)(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS p50_in_pps,
			quantileIf(0.90)(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS p90_in_pps,
			quantileIf(0.95)(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS p95_in_pps,
			quantileIf(0.99)(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS p99_in_pps,
			maxIf(ic.in_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.in_pkts_delta >= 0) AS max_in_pps,
			avgIf(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS avg_out_pps,
			minIf(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS min_out_pps,
			quantileIf(0.50)(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS p50_out_pps,
			quantileIf(0.90)(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS p90_out_pps,
			quantileIf(0.95)(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS p95_out_pps,
			quantileIf(0.99)(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS p99_out_pps,
			maxIf(ic.out_pkts_delta / ic.delta_duration, ic.delta_duration > 0 AND ic.out_pkts_delta >= 0) AS max_out_pps,
			COALESCE(dc.status, '') AS status,
			false AS isis_overload,
			false AS isis_unreachable,
			anyIf(ic.user_tunnel_id, ic.user_tunnel_id IS NOT NULL) AS user_tunnel_id,
			'' AS user_pk
		FROM fact_dz_device_interface_counters ic
		LEFT JOIN dz_devices_current dc ON ic.device_pk = dc.pk
		GROUP BY bucket_ts, ic.device_pk, ic.intf, status
	)`
}

// statusLinkMeta holds static link metadata from dimension tables for status pages.
type statusLinkMeta struct {
	PK             string
	Code           string
	LinkType       string
	Contributor    string
	SideAMetro     string
	SideZMetro     string
	SideADevice    string
	SideZDevice    string
	SideADevicePK  string
	SideZDevicePK  string
	BandwidthBps   int64
	CommittedRttUs float64
	CommittedRttNs int64
	Status         string
}

// queryStatusLinkMeta fetches metadata for active links (activated, soft-drained, hard-drained).
// If linkPKs is provided, only those links are returned.
func queryStatusLinkMeta(ctx context.Context, db driver.Conn, linkPKs ...string) (map[string]*statusLinkMeta, error) {
	var filterClause string
	var args []any
	if len(linkPKs) > 0 {
		filterClause = " AND l.pk IN ($1)"
		args = append(args, linkPKs)
	}

	query := fmt.Sprintf(`
		SELECT
			l.pk,
			l.code,
			l.link_type,
			COALESCE(c.code, '') as contributor,
			ma.code as side_a_metro,
			mz.code as side_z_metro,
			da.code as side_a_device,
			dz.code as side_z_device,
			l.side_a_pk,
			l.side_z_pk,
			l.bandwidth_bps,
			l.committed_rtt_ns / 1000.0 as committed_rtt_us,
			l.committed_rtt_ns,
			l.status
		FROM dz_links_current l
		JOIN dz_devices_current da ON l.side_a_pk = da.pk
		JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
		WHERE l.status IN ('activated', 'soft-drained', 'hard-drained')%s
	`, filterClause)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("link metadata query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*statusLinkMeta)
	for rows.Next() {
		var m statusLinkMeta
		if err := rows.Scan(
			&m.PK, &m.Code, &m.LinkType, &m.Contributor,
			&m.SideAMetro, &m.SideZMetro, &m.SideADevice, &m.SideZDevice,
			&m.SideADevicePK, &m.SideZDevicePK,
			&m.BandwidthBps, &m.CommittedRttUs, &m.CommittedRttNs, &m.Status,
		); err != nil {
			return nil, fmt.Errorf("link metadata scan: %w", err)
		}
		result[m.PK] = &m
	}
	return result, rows.Err()
}

// statusDeviceMeta holds static device metadata from dimension tables for status pages.
type statusDeviceMeta struct {
	PK          string
	Code        string
	DeviceType  string
	Contributor string
	Metro       string
	MaxUsers    int32
	Status      string
}

// queryStatusDeviceMeta fetches metadata for active devices.
// If devicePKs is provided, only those devices are returned.
func queryStatusDeviceMeta(ctx context.Context, db driver.Conn, devicePKs ...string) (map[string]*statusDeviceMeta, error) {
	var filterClause string
	var args []any
	if len(devicePKs) > 0 {
		filterClause = " AND d.pk IN ($1)"
		args = append(args, devicePKs)
	}

	query := fmt.Sprintf(`
		SELECT
			d.pk,
			d.code,
			d.device_type,
			COALESCE(c.code, '') as contributor,
			COALESCE(m.code, '') as metro,
			d.max_users,
			d.status
		FROM dz_devices_current d
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE d.status IN ('activated', 'soft-drained', 'hard-drained', 'suspended')%s
	`, filterClause)

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("device metadata query: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*statusDeviceMeta)
	for rows.Next() {
		var m statusDeviceMeta
		if err := rows.Scan(&m.PK, &m.Code, &m.DeviceType, &m.Contributor, &m.Metro, &m.MaxUsers, &m.Status); err != nil {
			return nil, fmt.Errorf("device metadata scan: %w", err)
		}
		result[m.PK] = &m
	}
	return result, rows.Err()
}
