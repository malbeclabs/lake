package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

type TrafficPoint struct {
	Time     string  `json:"time"`
	DevicePk string  `json:"device_pk"`
	Device   string  `json:"device"`
	Intf     string  `json:"intf"`
	InBps    float64 `json:"in_bps"`
	OutBps   float64 `json:"out_bps"`
}

type SeriesInfo struct {
	Key       string  `json:"key"`
	Device    string  `json:"device"`
	Intf      string  `json:"intf"`
	Direction string  `json:"direction"`
	Mean      float64 `json:"mean"`
}

// minBucketForRange returns the minimum allowed bucket interval for a given
// time range to prevent unbounded queries from returning millions of rows.
// maxTrafficRows is a safety limit on the number of rows returned.
const maxTrafficRows = 500_000

// trafficDimensionJoins builds the SQL JOIN clauses needed for dimension filtering
// in the traffic/discards endpoints. The fact table must be aliased as "f" and
// the devices CTE (with pk, code, metro_pk, contributor_pk) as "d".
func trafficDimensionJoins(needsLinkJoin, needsMetroJoin, needsContributorJoin bool) string {
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

func GetTrafficData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Parse query parameters
	agg := r.URL.Query().Get("agg")
	if agg == "" {
		agg = "max"
	}
	aggFunc := "MAX"
	if agg == "avg" {
		aggFunc = "AVG"
	}

	metric := r.URL.Query().Get("metric")
	// Determine SQL expressions based on metric.
	// inExpr/outExpr are used in the rates CTE (no table prefix).
	// fInExpr/fOutExpr are used in the mean query (with f. prefix).
	var inExpr, outExpr, fInExpr, fOutExpr, srcColumns, srcFilters string
	switch metric {
	case "packets":
		srcColumns = "f.device_pk, f.intf, f.event_ts, f.in_pkts_delta, f.out_pkts_delta, f.delta_duration"
		inExpr = "in_pkts_delta / delta_duration"
		outExpr = "out_pkts_delta / delta_duration"
		fInExpr = "f.in_pkts_delta / f.delta_duration"
		fOutExpr = "f.out_pkts_delta / f.delta_duration"
		srcFilters = `AND f.delta_duration > 0
				AND f.in_pkts_delta >= 0
				AND f.out_pkts_delta >= 0`
	default: // throughput
		srcColumns = "f.device_pk, f.intf, f.event_ts, f.in_octets_delta, f.out_octets_delta, f.delta_duration"
		inExpr = "in_octets_delta * 8 / delta_duration"
		outExpr = "out_octets_delta * 8 / delta_duration"
		fInExpr = "f.in_octets_delta * 8 / f.delta_duration"
		fOutExpr = "f.out_octets_delta * 8 / f.delta_duration"
		srcFilters = `AND f.delta_duration > 0
				AND f.in_octets_delta >= 0
				AND f.out_octets_delta >= 0`
	}

	// Use shared time filter (supports both preset time_range and custom start_time/end_time)
	timeFilter, bucketInterval := dashboardTimeFilter(r)

	// Build dimension filters
	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin := buildDimensionFilters(r)
	intfTypeFilter := trafficIntfTypeFilter(r, intfTypeSQL)
	dimJoins := trafficDimensionJoins(needsLinkJoin, needsMetroJoin, needsContributorJoin)

	// Add user join when user_kind filter is present
	var userJoinSQL, userKindFilter string
	if needsUserJoin {
		userJoinSQL = "\n\t\t\tLEFT JOIN dz_users_current u ON f.user_tunnel_id = u.tunnel_id"
		userKindFilter = userKindSQL
	}

	start := time.Now()

	// All queries use bucketing now (minimum bucket enforced above).
	// Series means are computed in ClickHouse to avoid accumulating rows in Go.
	query := fmt.Sprintf(`
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
				%s(%s) AS out_bps
			FROM src
			GROUP BY device_pk, intf, time_bucket
		)
		SELECT
			formatDateTime(r.time_bucket, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
			r.device_pk,
			d.code AS device,
			r.intf,
			r.in_bps,
			r.out_bps
		FROM rates r
		INNER JOIN devices d ON d.pk = r.device_pk
		WHERE r.time_bucket IS NOT NULL
		ORDER BY r.time_bucket, d.code, r.intf
		LIMIT %d
	`, srcColumns, dimJoins, userJoinSQL, timeFilter, intfFilterSQL, intfTypeFilter, srcFilters, filterSQL, userKindFilter, bucketInterval, aggFunc, inExpr, aggFunc, outExpr, maxTrafficRows)

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Compute series means via a second lightweight query in ClickHouse.
	// This avoids accumulating all points in Go just for mean calculation.
	meanQuery := fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code, metro_pk, contributor_pk
			FROM dz_devices_current
		)
		SELECT
			d.code AS device,
			f.intf,
			AVG(%s) AS mean_in_bps,
			AVG(%s) AS mean_out_bps
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

	meanRows, err := envDB(ctx).Query(ctx, meanQuery)
	meanDuration := time.Since(start) - duration
	metrics.RecordClickHouseQuery(meanDuration, err)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Traffic mean query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer meanRows.Close()

	// Build series info from mean query (small result set — one row per device/intf)
	series := []SeriesInfo{}
	for meanRows.Next() {
		var device, intf string
		var meanIn, meanOut float64
		if err := meanRows.Scan(&device, &intf, &meanIn, &meanOut); err != nil {
			log.Printf("Traffic mean row scan error: %v", err)
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
		})
		series = append(series, SeriesInfo{
			Key:       fmt.Sprintf("%s (out)", key),
			Device:    device,
			Intf:      intf,
			Direction: "out",
			Mean:      meanOut,
		})
	}

	// Stream JSON response directly from ClickHouse rows to avoid holding
	// all points in memory. The response is written as:
	//   {"points":[...rows streamed...],"series":[...],"effective_bucket":"...","truncated":bool}
	w.Header().Set("Content-Type", "application/json")
	bw := bufio.NewWriterSize(w, 32*1024)

	_, _ = bw.WriteString(`{"points":[`)

	pointCount := 0
	var scanErr error
	for rows.Next() {
		var point TrafficPoint
		if err := rows.Scan(&point.Time, &point.DevicePk, &point.Device, &point.Intf, &point.InBps, &point.OutBps); err != nil {
			log.Printf("Traffic row scan error: %v", err)
			// Already started writing — can't send HTTP error. Log and break.
			scanErr = err
			break
		}
		if pointCount > 0 {
			_ = bw.WriteByte(',')
		}
		pointJSON, err := json.Marshal(point)
		if err != nil {
			log.Printf("Traffic point encode error: %v", err)
			scanErr = err
			break
		}
		_, _ = bw.Write(pointJSON)
		pointCount++
	}

	if scanErr == nil {
		if err := rows.Err(); err != nil {
			log.Printf("Rows iteration error: %v", err)
		}
	}

	// Write series, metadata, and close
	_, _ = bw.WriteString(`],"series":`)
	seriesJSON, _ := json.Marshal(series)
	_, _ = bw.Write(seriesJSON)
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
func GetDiscardsData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Use shared time filter (supports both preset time_range and custom start_time/end_time)
	timeFilter, bucketInterval := dashboardTimeFilter(r)

	// Build dimension filters
	filterSQL, intfFilterSQL, intfTypeSQL, userKindSQL, _, needsLinkJoin, needsMetroJoin, needsContributorJoin, needsUserJoin := buildDimensionFilters(r)
	intfTypeFilter := trafficIntfTypeFilter(r, intfTypeSQL)
	dimJoins := trafficDimensionJoins(needsLinkJoin, needsMetroJoin, needsContributorJoin)

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

	// Build ClickHouse query - aggregate discards per time bucket
	query := fmt.Sprintf(`
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

	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Discards query error: %v", err)
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
			log.Printf("Discards row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		points = append(points, point)

		// Track totals for each device+interface+direction (separate in/out)
		baseKey := fmt.Sprintf("%s-%s", point.Device, point.Intf)

		// In discards series
		inKey := fmt.Sprintf("%s (In)", baseKey)
		if _, exists := seriesMap[inKey]; !exists {
			seriesMap[inKey] = &DiscardSeriesMean{
				Device: point.Device,
				Intf:   point.Intf,
			}
		}
		seriesMap[inKey].Total += point.InDiscards

		// Out discards series
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
		log.Printf("Rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build series info
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
		log.Printf("JSON encoding error: %v", err)
	}
}

// DiscardSeriesMean is used to accumulate discard totals
type DiscardSeriesMean struct {
	Device string
	Intf   string
	Total  int64
}
