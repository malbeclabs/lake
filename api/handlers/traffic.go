package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

type TrafficPoint struct {
	Time               string  `json:"time"`
	DevicePk           string  `json:"device_pk"`
	Device             string  `json:"device"`
	Intf               string  `json:"intf"`
	InBps              float64 `json:"in_bps"`
	OutBps             float64 `json:"out_bps"`
	InDiscards         int64   `json:"in_discards"`
	OutDiscards        int64   `json:"out_discards"`
	InErrors           int64   `json:"in_errors"`
	OutErrors          int64   `json:"out_errors"`
	InFcsErrors        int64   `json:"in_fcs_errors"`
	CarrierTransitions int64   `json:"carrier_transitions"`
}

type SeriesInfo struct {
	Key       string  `json:"key"`
	Device    string  `json:"device"`
	Intf      string  `json:"intf"`
	Direction string  `json:"direction"`
	Mean      float64 `json:"mean"`
	LinkPK    string  `json:"link_pk,omitempty"`
	CYOAType  string  `json:"cyoa_type,omitempty"`
}

// TrafficDataResponse is the JSON response for the traffic data endpoint.
type TrafficDataResponse struct {
	Points         []TrafficPoint      `json:"points"`
	Series         []SeriesInfo        `json:"series"`
	DiscardsSeries []DiscardSeriesInfo `json:"discards_series"`
	EffBucket      string              `json:"effective_bucket"`
	Truncated      bool                `json:"truncated"`
}

// maxTrafficRows is a safety limit on the number of rows returned.
// Set high since rollup data is lightweight (~8 cols per row).
const maxTrafficRows = 2_000_000

// trafficDimensionJoins builds the SQL JOIN clauses needed for dimension filtering
// in the traffic/discards endpoints. The source table must be aliased as "f" and
// the devices CTE (with pk, code, metro_pk, contributor_pk) as "d".
func trafficDimensionJoins(needsLinkJoin, needsMetroJoin, needsContributorJoin, needsInterfaceJoin bool) string {
	var joins []string
	if needsLinkJoin {
		joins = append(joins, "LEFT JOIN dz_links_current l ON f.link_pk = l.pk")
	}
	if needsMetroJoin {
		joins = append(joins, "LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk")
	}
	if needsContributorJoin {
		joins = append(joins, "LEFT JOIN dz_contributors_current co ON d.contributor_pk = co.pk")
	}
	if needsInterfaceJoin {
		joins = append(joins, "LEFT JOIN dz_device_interfaces_current di ON f.device_pk = di.device_pk AND f.intf = di.intf")
	}
	if len(joins) == 0 {
		return ""
	}
	return "\n\t\t\t" + strings.Join(joins, "\n\t\t\t")
}

// trafficIntfTypeFilter resolves the interface type filter for traffic endpoints.
// It uses intfTypeSQL from buildDimensionFilters when available, and falls back
// to the legacy tunnel_only parameter for backward compatibility.
func trafficIntfTypeFilter(r *http.Request, intfTypeSQL string) string {
	if intfTypeSQL != "" {
		return intfTypeSQL
	}
	// Backward compat: map tunnel_only to an interface filter
	switch r.URL.Query().Get("tunnel_only") {
	case "true":
		return " AND f.intf LIKE 'Tunnel%%'"
	case "false":
		return " AND f.intf NOT LIKE 'Tunnel%%'"
	default:
		return ""
	}
}

func (a *API) GetTrafficData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	// Parse query parameters
	agg := r.URL.Query().Get("agg")
	if agg == "" {
		agg = "max"
	}
	// For raw fact table queries, map agg to a SQL aggregate function.
	// Percentiles use quantile(); min/max/avg use standard aggregates.
	var aggFunc string
	// For rollup re-aggregation across 5m buckets: MAX preserves peaks for
	// percentile columns, AVG for avg, MIN for min.
	var rollupAggFunc string
	switch agg {
	case "avg":
		aggFunc = "AVG"
		rollupAggFunc = "AVG"
	case "min":
		aggFunc = "MIN"
		rollupAggFunc = "MIN"
	case "p50":
		aggFunc = "quantile(0.5)"
		rollupAggFunc = "MAX"
	case "p90":
		aggFunc = "quantile(0.9)"
		rollupAggFunc = "MAX"
	case "p95":
		aggFunc = "quantile(0.95)"
		rollupAggFunc = "MAX"
	case "p99":
		aggFunc = "quantile(0.99)"
		rollupAggFunc = "MAX"
	default:
		aggFunc = "MAX"
		rollupAggFunc = "MAX"
	}

	metric := r.URL.Query().Get("metric")

	// Resolve time filter and data source (raw fact table for sub-5m, rollup for >= 5m)
	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	// Build dimension filters.
	// Always join device interfaces so series metadata includes cyoa_type.
	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, _ := buildDimensionFilters(r)
	needsInterfaceJoin := true
	intfTypeFilter := trafficIntfTypeFilter(r, intfTypeSQL)
	dimJoins := trafficDimensionJoins(needsLinkJoin, needsMetroJoin, needsContributorJoin, needsInterfaceJoin)

	// Add user join when user_kind filter is present
	var userJoinSQL, userKindFilter string
	if needsUserJoin {
		userJoinSQL = "\n\t\t\tLEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
		userKindFilter = userKindSQL
	}

	start := time.Now()

	var query, meanQuery string

	if useRaw {
		// Sub-5m bucket: query raw fact table
		var inExpr, outExpr, fInExpr, fOutExpr, srcColumns, srcFilters string
		switch metric {
		case "packets":
			srcColumns = "f.device_pk AS device_pk, f.intf AS intf, f.event_ts, f.in_pkts_delta, f.out_pkts_delta, f.delta_duration, f.in_discards_delta, f.out_discards_delta, f.in_errors_delta, f.out_errors_delta, f.in_fcs_errors_delta, f.carrier_transitions_delta"
			inExpr = "in_pkts_delta / delta_duration"
			outExpr = "out_pkts_delta / delta_duration"
			fInExpr = "f.in_pkts_delta / f.delta_duration"
			fOutExpr = "f.out_pkts_delta / f.delta_duration"
			srcFilters = `AND f.delta_duration > 0
				AND f.in_pkts_delta >= 0
				AND f.out_pkts_delta >= 0`
		default: // throughput
			srcColumns = "f.device_pk AS device_pk, f.intf AS intf, f.event_ts, f.in_octets_delta, f.out_octets_delta, f.delta_duration, f.in_discards_delta, f.out_discards_delta, f.in_errors_delta, f.out_errors_delta, f.in_fcs_errors_delta, f.carrier_transitions_delta"
			inExpr = "in_octets_delta * 8 / delta_duration"
			outExpr = "out_octets_delta * 8 / delta_duration"
			fInExpr = "f.in_octets_delta * 8 / f.delta_duration"
			fOutExpr = "f.out_octets_delta * 8 / f.delta_duration"
			srcFilters = `AND f.delta_duration > 0
				AND f.in_octets_delta >= 0
				AND f.out_octets_delta >= 0`
		}

		query = fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		),
		src AS (
			SELECT %s
			FROM fact_dz_device_interface_counters f
			INNER JOIN devices d ON d.pk = f.device_pk%s%s
			WHERE f.%s
				%s%s
				%s
				%s
				%s
		),
		rates AS (
			SELECT
				device_pk,
				intf,
				toStartOfInterval(event_ts, INTERVAL %s) AS time_bucket,
				%s(%s) AS in_bps,
				%s(%s) AS out_bps,
				SUM(COALESCE(in_discards_delta, 0)) AS in_discards,
				SUM(COALESCE(out_discards_delta, 0)) AS out_discards,
				SUM(COALESCE(in_errors_delta, 0)) AS in_errors,
				SUM(COALESCE(out_errors_delta, 0)) AS out_errors,
				SUM(COALESCE(in_fcs_errors_delta, 0)) AS in_fcs_errors,
				SUM(COALESCE(carrier_transitions_delta, 0)) AS carrier_transitions
			FROM src
			GROUP BY device_pk, intf, time_bucket
		)
		SELECT
			formatDateTime(r.time_bucket, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			r.device_pk,
			d.code AS device,
			r.intf,
			r.in_bps,
			r.out_bps,
			r.in_discards,
			r.out_discards,
			r.in_errors,
			r.out_errors,
			r.in_fcs_errors,
			r.carrier_transitions
		FROM rates r
		INNER JOIN devices d ON d.pk = r.device_pk
		WHERE r.time_bucket IS NOT NULL
		ORDER BY r.time_bucket, d.code, r.intf
		LIMIT %d
	`, srcColumns, dimJoins, userJoinSQL, timeFilter, intfFilterSQL, intfTypeFilter, srcFilters, filterSQL, userKindFilter, bucketInterval, aggFunc, inExpr, aggFunc, outExpr, maxTrafficRows)

		meanQuery = fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		)
		SELECT
			d.code AS device,
			f.intf,
			AVG(%s) AS mean_in_bps,
			AVG(%s) AS mean_out_bps,
			toInt64(SUM(COALESCE(f.in_discards_delta, 0))) AS total_in_discards,
			toInt64(SUM(COALESCE(f.out_discards_delta, 0))) AS total_out_discards,
			anyLast(f.link_pk) AS link_pk,
			COALESCE(anyLast(di.cyoa_type), '') AS cyoa_type
		FROM fact_dz_device_interface_counters f
		INNER JOIN devices d ON d.pk = f.device_pk%s%s
		WHERE f.%s
			%s%s
			%s
			%s
			%s
		GROUP BY d.code, f.intf
		ORDER BY d.code, f.intf
	`, fInExpr, fOutExpr, dimJoins, userJoinSQL, timeFilter, intfFilterSQL, intfTypeFilter, srcFilters, filterSQL, userKindFilter)
	} else {
		// >= 5m bucket: query rollup table
		// Map agg to rollup column prefix
		aggPrefix := "max"
		switch agg {
		case "avg":
			aggPrefix = "avg"
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
		}

		var inCol, outCol, meanInCol, meanOutCol string
		switch metric {
		case "packets":
			inCol = fmt.Sprintf("f.%s_in_pps", aggPrefix)
			outCol = fmt.Sprintf("f.%s_out_pps", aggPrefix)
			meanInCol = "f.avg_in_pps"
			meanOutCol = "f.avg_out_pps"
		default: // throughput
			inCol = fmt.Sprintf("f.%s_in_bps", aggPrefix)
			outCol = fmt.Sprintf("f.%s_out_bps", aggPrefix)
			meanInCol = "f.avg_in_bps"
			meanOutCol = "f.avg_out_bps"
		}

		query = fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		),
		rates AS (
			SELECT
				f.device_pk AS device_pk,
				f.intf AS intf,
				toStartOfInterval(f.bucket_ts, INTERVAL %s) AS time_bucket,
				%s(%s) AS in_bps,
				%s(%s) AS out_bps,
				toInt64(SUM(f.in_discards)) AS in_discards,
				toInt64(SUM(f.out_discards)) AS out_discards,
				toInt64(SUM(f.in_errors)) AS in_errors,
				toInt64(SUM(f.out_errors)) AS out_errors,
				toInt64(SUM(f.in_fcs_errors)) AS in_fcs_errors,
				toInt64(SUM(f.carrier_transitions)) AS carrier_transitions
			FROM device_interface_rollup_5m f
			INNER JOIN devices d ON d.pk = f.device_pk%s%s
			WHERE f.%s
				%s%s
				%s
				%s
			GROUP BY f.device_pk, f.intf, time_bucket
		)
		SELECT
			formatDateTime(r.time_bucket, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			r.device_pk,
			d.code AS device,
			r.intf,
			r.in_bps,
			r.out_bps,
			r.in_discards,
			r.out_discards,
			r.in_errors,
			r.out_errors,
			r.in_fcs_errors,
			r.carrier_transitions
		FROM rates r
		INNER JOIN devices d ON d.pk = r.device_pk
		WHERE r.time_bucket IS NOT NULL
		ORDER BY r.time_bucket, d.code, r.intf
		LIMIT %d
	`, bucketInterval, rollupAggFunc, inCol, rollupAggFunc, outCol,
			dimJoins, userJoinSQL, timeFilter, intfFilterSQL, intfTypeFilter,
			filterSQL, userKindFilter, maxTrafficRows)

		meanQuery = fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		)
		SELECT
			d.code AS device,
			f.intf,
			AVG(%s) AS mean_in_bps,
			AVG(%s) AS mean_out_bps,
			toInt64(SUM(f.in_discards)) AS total_in_discards,
			toInt64(SUM(f.out_discards)) AS total_out_discards,
			anyLast(f.link_pk) AS link_pk,
			COALESCE(anyLast(di.cyoa_type), '') AS cyoa_type
		FROM device_interface_rollup_5m f
		INNER JOIN devices d ON d.pk = f.device_pk%s%s
		WHERE f.%s
			%s%s
			%s
			%s
		GROUP BY d.code, f.intf
		ORDER BY d.code, f.intf
	`, meanInCol, meanOutCol, dimJoins, userJoinSQL, timeFilter,
			intfFilterSQL, intfTypeFilter, filterSQL, userKindFilter)
	}

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	meanRows, err := a.envDB(ctx).Query(ctx, meanQuery)
	meanDuration := time.Since(start) - duration
	metrics.RecordClickHouseQuery(meanDuration, err)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("traffic mean query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer meanRows.Close()

	// Build series info from mean query (small result set — one row per device/intf).
	// Discard totals are int64 from raw queries and uint64 from rollup queries,
	// but both are scanned identically since the rollup mean query uses SUM on UInt64.
	series := []SeriesInfo{}
	discardsSeries := []DiscardSeriesInfo{}
	for meanRows.Next() {
		var device, intf string
		var meanIn, meanOut float64
		var totalInDiscards, totalOutDiscards int64
		var linkPK, cyoaType string
		if err := meanRows.Scan(&device, &intf, &meanIn, &meanOut, &totalInDiscards, &totalOutDiscards, &linkPK, &cyoaType); err != nil {
			logError("traffic mean row scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		key := fmt.Sprintf("%s-%s", device, intf)
		series = append(series, SeriesInfo{
			Key:       fmt.Sprintf("%s (in)", key),
			Device:    device,
			Intf:      intf,
			Direction: "in",
			Mean:      meanIn,
			LinkPK:    linkPK,
			CYOAType:  cyoaType,
		})
		series = append(series, SeriesInfo{
			Key:       fmt.Sprintf("%s (out)", key),
			Device:    device,
			Intf:      intf,
			Direction: "out",
			Mean:      meanOut,
			LinkPK:    linkPK,
			CYOAType:  cyoaType,
		})
		if totalInDiscards > 0 {
			discardsSeries = append(discardsSeries, DiscardSeriesInfo{
				Key:    fmt.Sprintf("%s (In)", key),
				Device: device,
				Intf:   intf,
				Total:  totalInDiscards,
			})
		}
		if totalOutDiscards > 0 {
			discardsSeries = append(discardsSeries, DiscardSeriesInfo{
				Key:    fmt.Sprintf("%s (Out)", key),
				Device: device,
				Intf:   intf,
				Total:  totalOutDiscards,
			})
		}
	}

	// Stream JSON response
	w.Header().Set("Content-Type", "application/json")
	bw := bufio.NewWriterSize(w, 32*1024)

	_, _ = bw.WriteString(`{"points":[`)

	pointCount := 0
	var scanErr error
	for rows.Next() {
		var point TrafficPoint
		if err := rows.Scan(&point.Time, &point.DevicePk, &point.Device, &point.Intf, &point.InBps, &point.OutBps, &point.InDiscards, &point.OutDiscards, &point.InErrors, &point.OutErrors, &point.InFcsErrors, &point.CarrierTransitions); err != nil {
			logError("traffic row scan error", "error", err)
			scanErr = err
			break
		}
		if pointCount > 0 {
			_ = bw.WriteByte(',')
		}
		pointJSON, err := json.Marshal(point)
		if err != nil {
			logError("failed to encode traffic point", "error", err)
			scanErr = err
			break
		}
		_, _ = bw.Write(pointJSON)
		pointCount++
	}

	if scanErr == nil {
		if err := rows.Err(); err != nil {
			logError("rows iteration error", "error", err)
		}
	}

	_, _ = bw.WriteString(`],"series":`)
	seriesJSON, _ := json.Marshal(series)
	_, _ = bw.Write(seriesJSON)
	_, _ = bw.WriteString(`,"discards_series":`)
	discardsSeriesJSON, _ := json.Marshal(discardsSeries)
	_, _ = bw.Write(discardsSeriesJSON)
	_, _ = fmt.Fprintf(bw, `,"effective_bucket":%q,"truncated":%t}`, bucketInterval, pointCount >= maxTrafficRows)
	_, _ = bw.WriteString("\n")
	_ = bw.Flush()
}

// DiscardsDataResponse is the response for the discards endpoint
type DiscardsDataResponse struct {
	Points []DiscardsPoint     `json:"points"`
	Series []DiscardSeriesInfo `json:"series"`
}

// DiscardsPoint represents a single data point for discards
type DiscardsPoint struct {
	Time        string `json:"time"`
	DevicePk    string `json:"device_pk"`
	Device      string `json:"device"`
	Intf        string `json:"intf"`
	InDiscards  int64  `json:"in_discards"`
	OutDiscards int64  `json:"out_discards"`
}

// DiscardSeriesInfo describes a discard series for filtering
type DiscardSeriesInfo struct {
	Key    string `json:"key"`
	Device string `json:"device"`
	Intf   string `json:"intf"`
	Total  int64  `json:"total"`
}

// GetDiscardsData returns discard data for all device-interfaces
func (a *API) GetDiscardsData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	timeFilter, bucketInterval, useRaw := trafficTimeFilter(r)

	// Build dimension filters
	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin, needsInterfaceJoin := buildDimensionFilters(r)
	intfTypeFilter := trafficIntfTypeFilter(r, intfTypeSQL)
	dimJoins := trafficDimensionJoins(needsLinkJoin, needsMetroJoin, needsContributorJoin, needsInterfaceJoin)

	// Add user join when user_kind filter is present
	var userJoinSQL, userKindFilter string
	if needsUserJoin {
		userJoinSQL = "\n\t\t\tLEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
		userKindFilter = userKindSQL
	}

	// Default to non-tunnel if no interface type filter specified
	if intfTypeFilter == "" {
		intfTypeFilter = " AND f.intf NOT LIKE 'Tunnel%%'"
	}

	start := time.Now()

	var query string
	if useRaw {
		query = fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		),
		agg AS (
			SELECT
				f.device_pk,
				f.intf,
				toStartOfInterval(f.event_ts, INTERVAL %s) AS time_bucket,
				SUM(COALESCE(f.in_discards_delta, 0)) AS in_discards,
				SUM(COALESCE(f.out_discards_delta, 0)) AS out_discards
			FROM fact_dz_device_interface_counters f
			INNER JOIN devices d ON d.pk = f.device_pk%s%s
			WHERE f.%s
				%s%s
				AND (COALESCE(f.in_discards_delta, 0) > 0 OR COALESCE(f.out_discards_delta, 0) > 0)
				%s
				%s
			GROUP BY f.device_pk, f.intf, time_bucket
		)
		SELECT
			formatDateTime(a.time_bucket, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			a.device_pk,
			d.code AS device,
			a.intf,
			a.in_discards,
			a.out_discards
		FROM agg a
		INNER JOIN devices d ON d.pk = a.device_pk
		WHERE a.time_bucket IS NOT NULL
		ORDER BY a.time_bucket, d.code, a.intf
	`, bucketInterval, dimJoins, userJoinSQL, timeFilter, intfFilterSQL, intfTypeFilter, filterSQL, userKindFilter)
	} else {
		query = fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		),
		agg AS (
			SELECT
				f.device_pk,
				f.intf,
				toStartOfInterval(f.bucket_ts, INTERVAL %s) AS time_bucket,
				toInt64(SUM(f.in_discards)) AS in_discards,
				toInt64(SUM(f.out_discards)) AS out_discards
			FROM device_interface_rollup_5m f
			INNER JOIN devices d ON d.pk = f.device_pk%s%s
			WHERE f.%s
				%s%s
				AND (f.in_discards > 0 OR f.out_discards > 0)
				%s
				%s
			GROUP BY f.device_pk, f.intf, time_bucket
		)
		SELECT
			formatDateTime(a.time_bucket, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			a.device_pk,
			d.code AS device,
			a.intf,
			a.in_discards,
			a.out_discards
		FROM agg a
		INNER JOIN devices d ON d.pk = a.device_pk
		WHERE a.time_bucket IS NOT NULL
		ORDER BY a.time_bucket, d.code, a.intf
	`, bucketInterval, dimJoins, userJoinSQL, timeFilter, intfFilterSQL, intfTypeFilter, filterSQL, userKindFilter)
	}

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logError("discards query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Collect points and calculate totals per series
	points := []DiscardsPoint{}
	seriesMap := make(map[string]*DiscardSeriesMean)

	for rows.Next() {
		var point DiscardsPoint
		if err := rows.Scan(&point.Time, &point.DevicePk, &point.Device, &point.Intf, &point.InDiscards, &point.OutDiscards); err != nil {
			logError("discards row scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		points = append(points, point)

		baseKey := fmt.Sprintf("%s-%s", point.Device, point.Intf)

		inKey := fmt.Sprintf("%s (In)", baseKey)
		if _, exists := seriesMap[inKey]; !exists {
			seriesMap[inKey] = &DiscardSeriesMean{
				Device: point.Device,
				Intf:   point.Intf,
			}
		}
		seriesMap[inKey].Total += point.InDiscards

		outKey := fmt.Sprintf("%s (Out)", baseKey)
		if _, exists := seriesMap[outKey]; !exists {
			seriesMap[outKey] = &DiscardSeriesMean{
				Device: point.Device,
				Intf:   point.Intf,
			}
		}
		seriesMap[outKey].Total += point.OutDiscards
	}

	if err := rows.Err(); err != nil {
		logError("rows error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	series := []DiscardSeriesInfo{}
	for key, mean := range seriesMap {
		series = append(series, DiscardSeriesInfo{
			Key:    key,
			Device: mean.Device,
			Intf:   mean.Intf,
			Total:  mean.Total,
		})
	}

	response := DiscardsDataResponse{
		Points: points,
		Series: series,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// DiscardSeriesMean is used to accumulate discard totals
type DiscardSeriesMean struct {
	Device string
	Intf   string
	Total  int64
}
