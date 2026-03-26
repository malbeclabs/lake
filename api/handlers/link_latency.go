package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// LinkLatencySummary is the per-link aggregated row for the summary table.
type LinkLatencySummary struct {
	LinkPk            string  `json:"link_pk"`
	LinkCode          string  `json:"link_code"`
	LinkType          string  `json:"link_type"`
	LinkStatus        string  `json:"link_status"`
	Provisioning      bool    `json:"provisioning"`
	ISISDown          bool    `json:"isis_down"`
	ContributorCode   string  `json:"contributor_code"`
	SideACode         string  `json:"side_a_code"`
	SideZCode         string  `json:"side_z_code"`
	CommittedRttMs    float64 `json:"committed_rtt_ms"`
	CommittedJitterMs float64 `json:"committed_jitter_ms"`
	RttAtoZMs         float64 `json:"rtt_a_to_z_ms"`
	RttZtoAMs         float64 `json:"rtt_z_to_a_ms"`
	JitterAtoZMs      float64 `json:"jitter_a_to_z_ms"`
	JitterZtoAMs      float64 `json:"jitter_z_to_a_ms"`
	LossAPct          float64 `json:"loss_a_pct"`
	LossZPct          float64 `json:"loss_z_pct"`
	Samples           uint64  `json:"samples"`
}

// LinkLatencySummaryResponse is the JSON response for the link latency summary endpoint.
type LinkLatencySummaryResponse struct {
	Links []LinkLatencySummary `json:"links"`
}

// MultiLinkLatencyPoint is a time-series point for one link (or aggregate) in a multi-link query.
type MultiLinkLatencyPoint struct {
	Time         string  `json:"time"`
	LinkPk       string  `json:"link_pk"`
	LinkCode     string  `json:"link_code"`
	RttAtoZMs    float64 `json:"rtt_a_to_z_ms"`
	RttZtoAMs    float64 `json:"rtt_z_to_a_ms"`
	JitterAtoZMs float64 `json:"jitter_a_to_z_ms"`
	JitterZtoAMs float64 `json:"jitter_z_to_a_ms"`
	LossPct      float64 `json:"loss_pct"`
}

// MultiLinkLatencyResponse is the JSON response for the multi-link latency history endpoint.
type MultiLinkLatencyResponse struct {
	Points []MultiLinkLatencyPoint `json:"points"`
}

// linkLatencyAgg parses the aggregation mode from the request and returns
// the rollup column prefix and re-aggregation SQL function.
func linkLatencyAgg(r *http.Request) (aggPrefix, rollupAggFunc string) {
	agg := r.URL.Query().Get("agg")
	if agg == "" {
		agg = "avg"
	}

	switch agg {
	case "min":
		aggPrefix = "min"
	case "p50":
		aggPrefix = "p50"
	case "p90":
		aggPrefix = "p90"
	case "p95":
		aggPrefix = "p95"
	case "p99":
		aggPrefix = "p99"
	case "max":
		aggPrefix = "max"
	default:
		aggPrefix = "avg"
	}

	rollupAggFunc = "AVG"
	switch agg {
	case "max":
		rollupAggFunc = "MAX"
	case "min":
		rollupAggFunc = "MIN"
	}
	return
}

// linkLatencyFilterSQL builds WHERE clauses for metro, device, contributor, and link_type filters.
// Returns the SQL fragment (including leading " AND ") and join flags.
func linkLatencyFilterSQL(r *http.Request) (filterSQL string, needsContributorJoin, needsMetroJoin bool) {
	var filterClauses []string

	if metros := r.URL.Query().Get("metro"); metros != "" {
		vals := strings.Split(metros, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		inList := strings.Join(quoted, ",")
		filterClauses = append(filterClauses, fmt.Sprintf("(ma.code IN (%s) OR mz.code IN (%s))", inList, inList))
		needsMetroJoin = true
	}

	if devices := r.URL.Query().Get("device"); devices != "" {
		vals := strings.Split(devices, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		inList := strings.Join(quoted, ",")
		filterClauses = append(filterClauses, fmt.Sprintf("(da.code IN (%s) OR dz.code IN (%s))", inList, inList))
	}

	if deviceA := r.URL.Query().Get("device_a"); deviceA != "" {
		vals := strings.Split(deviceA, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("da.code IN (%s)", strings.Join(quoted, ",")))
	}

	if deviceZ := r.URL.Query().Get("device_z"); deviceZ != "" {
		vals := strings.Split(deviceZ, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("dz.code IN (%s)", strings.Join(quoted, ",")))
	}

	if contributors := r.URL.Query().Get("contributor"); contributors != "" {
		vals := strings.Split(contributors, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("co.code IN (%s)", strings.Join(quoted, ",")))
		needsContributorJoin = true
	}

	if linkTypes := r.URL.Query().Get("link_type"); linkTypes != "" {
		vals := strings.Split(linkTypes, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("l.link_type IN (%s)", strings.Join(quoted, ",")))
	}

	if codes := r.URL.Query().Get("code"); codes != "" {
		vals := strings.Split(codes, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("l.code IN (%s)", strings.Join(quoted, ",")))
	}

	if statuses := r.URL.Query().Get("status"); statuses != "" {
		vals := strings.Split(statuses, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("l.status IN (%s)", strings.Join(quoted, ",")))
	}

	if search := r.URL.Query().Get("search"); search != "" {
		needle := escapeSingleQuote(search)
		filterClauses = append(filterClauses, fmt.Sprintf(
			"(l.code ILIKE '%%%s%%' OR l.link_type ILIKE '%%%s%%' OR da.code ILIKE '%%%s%%' OR dz.code ILIKE '%%%s%%')",
			needle, needle, needle, needle))
	}

	if pksParam := r.URL.Query().Get("pks"); pksParam != "" {
		pks := strings.Split(pksParam, ",")
		quoted := make([]string, len(pks))
		for i, pk := range pks {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(strings.TrimSpace(pk)))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("r.link_pk IN (%s)", strings.Join(quoted, ",")))
	}

	if len(filterClauses) > 0 {
		filterSQL = " AND " + strings.Join(filterClauses, " AND ")
	}
	return
}

// GetLinkLatencyData returns per-link aggregated latency summary data.
// Supports filtering by device, device_a, device_z, contributor, link_type.
func GetLinkLatencyData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	aggPrefix, rollupAggFunc := linkLatencyAgg(r)
	timeFilter, _ := rollupTimeFilter(r)
	filterSQL, _, needsMetroJoin := linkLatencyFilterSQL(r)

	metroJoin := ""
	if needsMetroJoin {
		metroJoin = `LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk`
	}

	// By default exclude drained/provisioning links unless show_excluded=true
	statusFilter := ""
	if r.URL.Query().Get("show_excluded") != "true" {
		statusFilter = " AND l.status NOT IN ('soft-drained', 'hard-drained', 'suspended') AND COALESCE(l.committed_rtt_ns, 0) != 1000000000 AND (ia.link_pk IS NULL OR ia.is_deleted = 0)"
	}

	start := time.Now()

	query := fmt.Sprintf(`
		SELECT
			r.link_pk,
			l.code AS link_code,
			l.link_type,
			l.status AS link_status,
			COALESCE(l.committed_rtt_ns, 0) = 1000000000 AS provisioning,
			CASE WHEN ia.link_pk IS NULL THEN false ELSE ia.is_deleted = 1 END AS isis_down,
			COALESCE(co.code, '') AS contributor_code,
			da.code AS side_a_code,
			dz.code AS side_z_code,
			COALESCE(l.committed_rtt_ns, 0) / 1000000.0 AS committed_rtt_ms,
			COALESCE(l.committed_jitter_ns, 0) / 1000000.0 AS committed_jitter_ms,
			%s(r.a_%s_rtt_us) / 1000.0 AS rtt_a_to_z_ms,
			%s(r.z_%s_rtt_us) / 1000.0 AS rtt_z_to_a_ms,
			%s(r.a_%s_jitter_us) / 1000.0 AS jitter_a_to_z_ms,
			%s(r.z_%s_jitter_us) / 1000.0 AS jitter_z_to_a_ms,
			MAX(r.a_loss_pct) AS loss_a_pct,
			MAX(r.z_loss_pct) AS loss_z_pct,
			SUM(r.a_samples + r.z_samples) AS samples
		FROM link_rollup_5m r
		JOIN dz_links_current l ON r.link_pk = l.pk
		JOIN dz_devices_current da ON l.side_a_pk = da.pk
		JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT JOIN dz_contributors_current co ON l.contributor_pk = co.pk
		LEFT JOIN (
			SELECT link_pk, argMax(is_deleted, ingested_at) AS is_deleted
			FROM dim_isis_adjacencies_history
			GROUP BY link_pk
		) ia ON l.pk = ia.link_pk
		%s
		WHERE r.%s
			AND (r.a_samples > 0 OR r.z_samples > 0)
			%s
			%s
		GROUP BY r.link_pk, l.code, l.link_type, l.status, l.committed_rtt_ns, co.code, da.code, dz.code, l.committed_jitter_ns, isis_down
		ORDER BY l.code`,
		rollupAggFunc, aggPrefix,
		rollupAggFunc, aggPrefix,
		rollupAggFunc, aggPrefix,
		rollupAggFunc, aggPrefix,
		metroJoin,
		timeFilter,
		filterSQL,
		statusFilter)

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("link latency query error", "error", err, "duration", duration)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var links []LinkLatencySummary

	for rows.Next() {
		var s LinkLatencySummary
		var rttA, rttZ, jitterA, jitterZ, lossA, lossZ *float64
		if err := rows.Scan(&s.LinkPk, &s.LinkCode, &s.LinkType, &s.LinkStatus, &s.Provisioning, &s.ISISDown,
			&s.ContributorCode, &s.SideACode, &s.SideZCode, &s.CommittedRttMs, &s.CommittedJitterMs,
			&rttA, &rttZ, &jitterA, &jitterZ, &lossA, &lossZ,
			&s.Samples); err != nil {
			slog.Error("link latency scan error", "error", err)
			break
		}
		if rttA != nil && !math.IsNaN(*rttA) {
			s.RttAtoZMs = *rttA
		}
		if rttZ != nil && !math.IsNaN(*rttZ) {
			s.RttZtoAMs = *rttZ
		}
		if jitterA != nil && !math.IsNaN(*jitterA) {
			s.JitterAtoZMs = *jitterA
		}
		if jitterZ != nil && !math.IsNaN(*jitterZ) {
			s.JitterZtoAMs = *jitterZ
		}
		if lossA != nil && !math.IsNaN(*lossA) {
			s.LossAPct = *lossA
		}
		if lossZ != nil && !math.IsNaN(*lossZ) {
			s.LossZPct = *lossZ
		}
		links = append(links, s)
	}

	if links == nil {
		links = []LinkLatencySummary{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(LinkLatencySummaryResponse{
		Links: links,
	}); err != nil {
		slog.Error("failed to encode link latency response", "error", err)
	}
}

// GetMultiLinkLatencyHistory returns time-series latency data for multiple links.
//
// Modes (via "mode" query param):
//   - "per_link" (default): returns per-link time series, grouped by link_pk.
//     Requires "pks" param (comma-separated link PKs, max 20).
//   - "aggregate": returns a single aggregated time series across all matching links.
//     Uses the same filter params as GetLinkLatencyData (device, contributor, link_type).
func GetMultiLinkLatencyHistory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "per_link"
	}

	// Normalize time params
	q := r.URL.Query()
	if q.Get("range") != "" && q.Get("time_range") == "" {
		q.Set("time_range", q.Get("range"))
	}
	r.URL.RawQuery = q.Encode()

	timeFilter, bucketInterval := rollupTimeFilter(r)
	aggPrefix, rollupAggFunc := linkLatencyAgg(r)

	start := time.Now()
	var query string
	var scanPerLink bool

	if mode == "aggregate" {
		// Aggregate mode: multiple percentile series (avg, p95, p99, max) across all matching links
		filterSQL, needsContributorJoin, needsMetroJoin := linkLatencyFilterSQL(r)

		extraJoins := ""
		if needsContributorJoin {
			extraJoins += " LEFT JOIN dz_contributors_current co ON l.contributor_pk = co.pk"
		}
		if needsMetroJoin {
			extraJoins += " LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk"
		}

		// Each stat line produces avg of both directions combined
		query = fmt.Sprintf(`
			SELECT
				formatDateTime(toStartOfInterval(r.bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%SZ') AS time_bucket,
				AVG(r.a_avg_rtt_us + r.z_avg_rtt_us) / 2.0 / 1000.0 AS avg_rtt_ms,
				AVG(r.a_p95_rtt_us + r.z_p95_rtt_us) / 2.0 / 1000.0 AS p95_rtt_ms,
				AVG(r.a_p99_rtt_us + r.z_p99_rtt_us) / 2.0 / 1000.0 AS p99_rtt_ms,
				MAX(greatest(r.a_max_rtt_us, r.z_max_rtt_us)) / 1000.0 AS max_rtt_ms,
				AVG(r.a_avg_jitter_us + r.z_avg_jitter_us) / 2.0 / 1000.0 AS avg_jitter_ms,
				AVG(r.a_p95_jitter_us + r.z_p95_jitter_us) / 2.0 / 1000.0 AS p95_jitter_ms,
				AVG(r.a_p99_jitter_us + r.z_p99_jitter_us) / 2.0 / 1000.0 AS p99_jitter_ms,
				MAX(greatest(r.a_max_jitter_us, r.z_max_jitter_us)) / 1000.0 AS max_jitter_ms,
				AVG(greatest(r.a_loss_pct, r.z_loss_pct)) AS avg_loss_pct,
				MAX(greatest(r.a_loss_pct, r.z_loss_pct)) AS max_loss_pct
			FROM link_rollup_5m r
			JOIN dz_links_current l ON r.link_pk = l.pk
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			%s
			WHERE r.%s
				AND (r.a_samples > 0 OR r.z_samples > 0)
				%s
			GROUP BY time_bucket
			ORDER BY time_bucket`,
			bucketInterval,
			extraJoins,
			timeFilter,
			filterSQL)

		rows, err := envDB(ctx).Query(ctx, query)
		duration := time.Since(start)
		metrics.RecordClickHouseQuery(duration, err)

		if err != nil {
			slog.Error("aggregate latency query error", "error", err, "duration", duration)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Fan out each row into 4 series points (avg, p95, p99, max)
		type aggRow struct {
			time                                       string
			avgRtt, p95Rtt, p99Rtt, maxRtt             *float64
			avgJitter, p95Jitter, p99Jitter, maxJitter *float64
			avgLoss, maxLoss                           *float64
		}
		var points []MultiLinkLatencyPoint
		for rows.Next() {
			var row aggRow
			if err := rows.Scan(&row.time,
				&row.avgRtt, &row.p95Rtt, &row.p99Rtt, &row.maxRtt,
				&row.avgJitter, &row.p95Jitter, &row.p99Jitter, &row.maxJitter,
				&row.avgLoss, &row.maxLoss); err != nil {
				slog.Error("aggregate latency scan error", "error", err)
				break
			}

			type statLine struct {
				pk, code          string
				rtt, jitter, loss *float64
			}
			lines := []statLine{
				{"_avg", "Avg", row.avgRtt, row.avgJitter, row.avgLoss},
				{"_p95", "P95", row.p95Rtt, row.p95Jitter, nil},
				{"_p99", "P99", row.p99Rtt, row.p99Jitter, nil},
				{"_max", "Max", row.maxRtt, row.maxJitter, row.maxLoss},
			}
			for _, l := range lines {
				p := MultiLinkLatencyPoint{Time: row.time, LinkPk: l.pk, LinkCode: l.code}
				if l.rtt != nil && !math.IsNaN(*l.rtt) {
					// Use same value for both directions (already averaged)
					p.RttAtoZMs = *l.rtt
					p.RttZtoAMs = *l.rtt
				}
				if l.jitter != nil && !math.IsNaN(*l.jitter) {
					p.JitterAtoZMs = *l.jitter
					p.JitterZtoAMs = *l.jitter
				}
				if l.loss != nil && !math.IsNaN(*l.loss) {
					p.LossPct = *l.loss
				}
				points = append(points, p)
			}
		}

		if points == nil {
			points = []MultiLinkLatencyPoint{}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(MultiLinkLatencyResponse{Points: points})
		return
	} else {
		// Per-link mode: one series per link
		// Supports both explicit PKs (for pinned selections) and filter params — all via linkLatencyFilterSQL
		filterSQL, needsContributorJoin, _ := linkLatencyFilterSQL(r)
		extraJoins := ""
		if needsContributorJoin {
			extraJoins += " LEFT JOIN dz_contributors_current co ON l.contributor_pk = co.pk"
		}

		query = fmt.Sprintf(`
			SELECT
				formatDateTime(toStartOfInterval(r.bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%SZ') AS time_bucket,
				r.link_pk,
				l.code AS link_code,
				%s(r.a_%s_rtt_us) / 1000.0 AS rtt_a_to_z_ms,
				%s(r.z_%s_rtt_us) / 1000.0 AS rtt_z_to_a_ms,
				%s(r.a_%s_jitter_us) / 1000.0 AS jitter_a_to_z_ms,
				%s(r.z_%s_jitter_us) / 1000.0 AS jitter_z_to_a_ms,
				MAX(greatest(r.a_loss_pct, r.z_loss_pct)) AS loss_pct
			FROM link_rollup_5m r
			JOIN dz_links_current l ON r.link_pk = l.pk
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			%s
			WHERE r.%s
				AND (r.a_samples > 0 OR r.z_samples > 0)
				%s
			GROUP BY time_bucket, r.link_pk, l.code
			ORDER BY time_bucket, l.code`,
			bucketInterval,
			rollupAggFunc, aggPrefix,
			rollupAggFunc, aggPrefix,
			rollupAggFunc, aggPrefix,
			rollupAggFunc, aggPrefix,
			extraJoins,
			timeFilter,
			filterSQL)
		scanPerLink = true
	}

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("multi-link latency query error", "error", err, "duration", duration)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []MultiLinkLatencyPoint
	for rows.Next() {
		var p MultiLinkLatencyPoint
		var rttA, rttZ, jitterA, jitterZ, loss *float64

		var scanErr error
		if scanPerLink {
			scanErr = rows.Scan(&p.Time, &p.LinkPk, &p.LinkCode,
				&rttA, &rttZ, &jitterA, &jitterZ, &loss)
		} else {
			scanErr = rows.Scan(&p.Time,
				&rttA, &rttZ, &jitterA, &jitterZ, &loss)
			p.LinkPk = "_aggregate"
			p.LinkCode = "All Links"
		}
		if scanErr != nil {
			slog.Error("multi-link latency scan error", "error", scanErr)
			break
		}

		if rttA != nil && !math.IsNaN(*rttA) {
			p.RttAtoZMs = *rttA
		}
		if rttZ != nil && !math.IsNaN(*rttZ) {
			p.RttZtoAMs = *rttZ
		}
		if jitterA != nil && !math.IsNaN(*jitterA) {
			p.JitterAtoZMs = *jitterA
		}
		if jitterZ != nil && !math.IsNaN(*jitterZ) {
			p.JitterZtoAMs = *jitterZ
		}
		if loss != nil && !math.IsNaN(*loss) {
			p.LossPct = *loss
		}
		points = append(points, p)
	}

	if points == nil {
		points = []MultiLinkLatencyPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(MultiLinkLatencyResponse{
		Points: points,
	}); err != nil {
		slog.Error("failed to encode multi-link latency response", "error", err)
	}
}
