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

// LinkLatencyPoint is a single time-series point for one link.
type LinkLatencyPoint struct {
	Time         string  `json:"time"`
	LinkPk       string  `json:"link_pk"`
	LinkCode     string  `json:"link_code"`
	SideACode    string  `json:"side_a_code"`
	SideZCode    string  `json:"side_z_code"`
	RttAtoZMs    float64 `json:"rtt_a_to_z_ms"`
	RttZtoAMs    float64 `json:"rtt_z_to_a_ms"`
	JitterAtoZMs float64 `json:"jitter_a_to_z_ms"`
	JitterZtoAMs float64 `json:"jitter_z_to_a_ms"`
	LossAPct     float64 `json:"loss_a_pct"`
	LossZPct     float64 `json:"loss_z_pct"`
}

// LinkLatencySeriesInfo holds metadata for sorting links in the legend.
type LinkLatencySeriesInfo struct {
	LinkPk    string  `json:"link_pk"`
	LinkCode  string  `json:"link_code"`
	SideACode string  `json:"side_a_code"`
	SideZCode string  `json:"side_z_code"`
	MeanRttMs float64 `json:"mean_rtt_ms"`
}

// LinkLatencyDataResponse is the JSON response for the link latency data endpoint.
type LinkLatencyDataResponse struct {
	Points    []LinkLatencyPoint      `json:"points"`
	Series    []LinkLatencySeriesInfo `json:"series"`
	EffBucket string                  `json:"effective_bucket"`
}

// GetLinkLatencyData returns time-series latency data for all links,
// with A→Z and Z→A directions. Supports filtering by device, contributor, link_type.
func GetLinkLatencyData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Parse aggregation mode
	agg := r.URL.Query().Get("agg")
	if agg == "" {
		agg = "avg"
	}

	// Map agg to rollup column prefix and re-aggregation function
	var aggPrefix string
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

	rollupAggFunc := "AVG"
	switch agg {
	case "max":
		rollupAggFunc = "MAX"
	case "min":
		rollupAggFunc = "MIN"
	}

	// Resolve time filter (always use rollup path)
	timeFilter, bucketInterval := rollupTimeFilter(r)

	// Build link-level dimension filters
	var filterClauses []string

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
	}

	if linkTypes := r.URL.Query().Get("link_type"); linkTypes != "" {
		vals := strings.Split(linkTypes, ",")
		quoted := make([]string, len(vals))
		for i, v := range vals {
			quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
		}
		filterClauses = append(filterClauses, fmt.Sprintf("l.link_type IN (%s)", strings.Join(quoted, ",")))
	}

	var filterSQL string
	if len(filterClauses) > 0 {
		filterSQL = " AND " + strings.Join(filterClauses, " AND ")
	}

	// Determine which joins are needed
	needsContributorJoin := strings.Contains(filterSQL, "co.")
	contributorJoin := ""
	if needsContributorJoin {
		contributorJoin = "LEFT JOIN dz_contributors_current co ON l.contributor_pk = co.pk"
	}

	start := time.Now()

	query := fmt.Sprintf(`
		SELECT
			formatDateTime(toStartOfInterval(r.bucket_ts, INTERVAL %s), '%%Y-%%m-%%dT%%H:%%i:%%SZ') as time_bucket,
			r.link_pk,
			l.code as link_code,
			da.code as side_a_code,
			dz.code as side_z_code,
			%s(r.a_%s_rtt_us) / 1000.0 as rtt_a_to_z_ms,
			%s(r.z_%s_rtt_us) / 1000.0 as rtt_z_to_a_ms,
			%s(r.a_%s_jitter_us) / 1000.0 as jitter_a_to_z_ms,
			%s(r.z_%s_jitter_us) / 1000.0 as jitter_z_to_a_ms,
			MAX(r.a_loss_pct) as loss_a_pct,
			MAX(r.z_loss_pct) as loss_z_pct
		FROM link_rollup_5m r
		JOIN dz_links_current l ON r.link_pk = l.pk
		JOIN dz_devices_current da ON l.side_a_pk = da.pk
		JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		%s
		WHERE r.%s
			AND (r.a_samples > 0 OR r.z_samples > 0)
			%s
		GROUP BY time_bucket, r.link_pk, l.code, da.code, dz.code
		ORDER BY time_bucket, l.code`,
		bucketInterval,
		rollupAggFunc, aggPrefix,
		rollupAggFunc, aggPrefix,
		rollupAggFunc, aggPrefix,
		rollupAggFunc, aggPrefix,
		contributorJoin,
		timeFilter,
		filterSQL)

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		slog.Error("link latency query error", "error", err, "duration", duration)
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var points []LinkLatencyPoint
	// Track per-link RTT sums for series mean computation
	type linkAcc struct {
		code      string
		sideACode string
		sideZCode string
		rttSum    float64
		count     int
	}
	linkAccMap := make(map[string]*linkAcc)

	for rows.Next() {
		var p LinkLatencyPoint
		var rttA, rttZ, jitterA, jitterZ, lossA, lossZ *float64
		if err := rows.Scan(&p.Time, &p.LinkPk, &p.LinkCode, &p.SideACode, &p.SideZCode,
			&rttA, &rttZ, &jitterA, &jitterZ, &lossA, &lossZ); err != nil {
			slog.Error("link latency scan error", "error", err)
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
		if lossA != nil && !math.IsNaN(*lossA) {
			p.LossAPct = *lossA
		}
		if lossZ != nil && !math.IsNaN(*lossZ) {
			p.LossZPct = *lossZ
		}
		points = append(points, p)

		acc, ok := linkAccMap[p.LinkPk]
		if !ok {
			acc = &linkAcc{code: p.LinkCode, sideACode: p.SideACode, sideZCode: p.SideZCode}
			linkAccMap[p.LinkPk] = acc
		}
		avgRtt := (p.RttAtoZMs + p.RttZtoAMs) / 2
		if avgRtt > 0 {
			acc.rttSum += avgRtt
			acc.count++
		}
	}

	// Build series sorted by mean RTT (highest first)
	series := make([]LinkLatencySeriesInfo, 0, len(linkAccMap))
	for pk, acc := range linkAccMap {
		meanRtt := 0.0
		if acc.count > 0 {
			meanRtt = acc.rttSum / float64(acc.count)
		}
		series = append(series, LinkLatencySeriesInfo{
			LinkPk:    pk,
			LinkCode:  acc.code,
			SideACode: acc.sideACode,
			SideZCode: acc.sideZCode,
			MeanRttMs: meanRtt,
		})
	}

	if points == nil {
		points = []LinkLatencyPoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(LinkLatencyDataResponse{
		Points:    points,
		Series:    series,
		EffBucket: bucketInterval,
	}); err != nil {
		slog.Error("failed to encode link latency response", "error", err)
	}
}
