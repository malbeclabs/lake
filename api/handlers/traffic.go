package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/metrics"
)

type TrafficDataResponse struct {
	Points []TrafficPoint `json:"points"`
	Series []SeriesInfo   `json:"series"`
}

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

func GetTrafficData(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Parse query parameters
	timeRange := r.URL.Query().Get("time_range")
	if timeRange == "" {
		timeRange = "12h"
	}

	tunnelOnly := r.URL.Query().Get("tunnel_only")
	isTunnel := tunnelOnly == "true"

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		bucket = "30 SECOND"
	}

	agg := r.URL.Query().Get("agg")
	if agg == "" {
		agg = "max"
	}
	aggFunc := "MAX"
	if agg == "avg" {
		aggFunc = "AVG"
	}

	// Convert time range to interval
	var rangeInterval string
	switch timeRange {
	case "1h":
		rangeInterval = "1 HOUR"
	case "3h":
		rangeInterval = "3 HOUR"
	case "6h":
		rangeInterval = "6 HOUR"
	case "12h":
		rangeInterval = "12 HOUR"
	case "24h":
		rangeInterval = "24 HOUR"
	case "3d":
		rangeInterval = "3 DAY"
	case "7d":
		rangeInterval = "7 DAY"
	default:
		rangeInterval = "6 HOUR"
	}

	// Use the bucket interval directly (except for "none" which is handled separately)
	bucketInterval := bucket

	// Build interface filter
	var intfFilter string
	if isTunnel {
		intfFilter = "AND intf LIKE 'Tunnel%'"
	} else {
		intfFilter = "AND intf NOT LIKE 'Tunnel%'"
	}

	start := time.Now()

	// Build ClickHouse query with adaptive bucketing and pre-filtering
	// Join devices early to filter out any orphaned data
	var query string
	if bucket == "none" {
		// No bucketing - return raw data points
		query = fmt.Sprintf(`
			WITH devices AS (
				SELECT pk, code
				FROM dz_devices_current
			)
			SELECT
				formatDateTime(c.event_ts, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
				c.device_pk,
				d.code AS device,
				c.intf,
				c.in_octets_delta * 8 / c.delta_duration AS in_bps,
				c.out_octets_delta * 8 / c.delta_duration AS out_bps
			FROM fact_dz_device_interface_counters c
			INNER JOIN devices d ON d.pk = c.device_pk
			WHERE c.event_ts >= now() - INTERVAL %s
				%s
				AND c.delta_duration > 0
				AND c.in_octets_delta >= 0
				AND c.out_octets_delta >= 0
			ORDER BY c.event_ts, d.code, c.intf
		`, rangeInterval, intfFilter)
	} else {
		// With bucketing
		query = fmt.Sprintf(`
			WITH devices AS (
				SELECT pk, code
				FROM dz_devices_current
			),
			src AS (
				SELECT c.device_pk, c.intf, c.event_ts, c.in_octets_delta, c.out_octets_delta, c.delta_duration
				FROM fact_dz_device_interface_counters c
				INNER JOIN devices d ON d.pk = c.device_pk
				WHERE c.event_ts >= now() - INTERVAL %s
					%s
					AND c.delta_duration > 0
					AND c.in_octets_delta >= 0
					AND c.out_octets_delta >= 0
			),
			rates AS (
				SELECT
					device_pk,
					intf,
					toStartOfInterval(event_ts, INTERVAL %s) AS time_bucket,
					%s(in_octets_delta * 8 / delta_duration) AS in_bps,
					%s(out_octets_delta * 8 / delta_duration) AS out_bps
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
		`, rangeInterval, intfFilter, bucketInterval, aggFunc, aggFunc)
	}

	rows, err := config.DB.Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("Traffic query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Collect points and calculate means
	points := []TrafficPoint{}
	seriesMap := make(map[string]*SeriesMean)

	for rows.Next() {
		var point TrafficPoint
		if err := rows.Scan(&point.Time, &point.DevicePk, &point.Device, &point.Intf, &point.InBps, &point.OutBps); err != nil {
			log.Printf("Traffic row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		points = append(points, point)

		// Track means for each device+interface
		key := fmt.Sprintf("%s-%s", point.Device, point.Intf)
		if _, exists := seriesMap[key]; !exists {
			seriesMap[key] = &SeriesMean{
				Device: point.Device,
				Intf:   point.Intf,
			}
		}
		seriesMap[key].InSum += point.InBps
		seriesMap[key].OutSum += point.OutBps
		seriesMap[key].Count++
	}

	if err := rows.Err(); err != nil {
		log.Printf("Rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build series info with means
	series := []SeriesInfo{}
	for key, mean := range seriesMap {
		if mean.Count > 0 {
			// Add "in" series
			series = append(series, SeriesInfo{
				Key:       fmt.Sprintf("%s (in)", key),
				Device:    mean.Device,
				Intf:      mean.Intf,
				Direction: "in",
				Mean:      mean.InSum / float64(mean.Count),
			})
			// Add "out" series
			series = append(series, SeriesInfo{
				Key:       fmt.Sprintf("%s (out)", key),
				Device:    mean.Device,
				Intf:      mean.Intf,
				Direction: "out",
				Mean:      mean.OutSum / float64(mean.Count),
			})
		}
	}

	response := TrafficDataResponse{
		Points: points,
		Series: series,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

// SeriesMean is used to accumulate values for mean calculation
type SeriesMean struct {
	Device string
	Intf   string
	InSum  float64
	OutSum float64
	Count  int
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

	// Parse query parameters
	timeRange := r.URL.Query().Get("time_range")
	if timeRange == "" {
		timeRange = "12h"
	}

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		bucket = "30 SECOND"
	}

	// Convert time range to interval
	var rangeInterval string
	switch timeRange {
	case "1h":
		rangeInterval = "1 HOUR"
	case "3h":
		rangeInterval = "3 HOUR"
	case "6h":
		rangeInterval = "6 HOUR"
	case "12h":
		rangeInterval = "12 HOUR"
	case "24h":
		rangeInterval = "24 HOUR"
	case "3d":
		rangeInterval = "3 DAY"
	case "7d":
		rangeInterval = "7 DAY"
	default:
		rangeInterval = "6 HOUR"
	}

	start := time.Now()

	// Build ClickHouse query - aggregate discards per time bucket
	var query string
	if bucket == "none" {
		// No bucketing - return raw data points
		query = fmt.Sprintf(`
			WITH devices AS (
				SELECT pk, code
				FROM dz_devices_current
			)
			SELECT
				formatDateTime(c.event_ts, '%%Y-%%m-%%dT%%H:%%i:%%sZ') AS time,
				c.device_pk,
				d.code AS device,
				c.intf,
				COALESCE(c.in_discards_delta, 0) AS in_discards,
				COALESCE(c.out_discards_delta, 0) AS out_discards
			FROM fact_dz_device_interface_counters c
			INNER JOIN devices d ON d.pk = c.device_pk
			WHERE c.event_ts >= now() - INTERVAL %s
				AND c.intf NOT LIKE 'Tunnel%%'
				AND (COALESCE(c.in_discards_delta, 0) > 0 OR COALESCE(c.out_discards_delta, 0) > 0)
			ORDER BY c.event_ts, d.code, c.intf
		`, rangeInterval)
	} else {
		// With bucketing - sum discards per bucket
		// Filter for non-zero discards early to reduce data processed
		query = fmt.Sprintf(`
			WITH devices AS (
				SELECT pk, code
				FROM dz_devices_current
			),
			agg AS (
				SELECT
					c.device_pk,
					c.intf,
					toStartOfInterval(c.event_ts, INTERVAL %s) AS time_bucket,
					SUM(COALESCE(c.in_discards_delta, 0)) AS in_discards,
					SUM(COALESCE(c.out_discards_delta, 0)) AS out_discards
				FROM fact_dz_device_interface_counters c
				WHERE c.event_ts >= now() - INTERVAL %s
					AND c.intf NOT LIKE 'Tunnel%%'
					AND (COALESCE(c.in_discards_delta, 0) > 0 OR COALESCE(c.out_discards_delta, 0) > 0)
				GROUP BY c.device_pk, c.intf, time_bucket
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
		`, bucket, rangeInterval)
	}

	rows, err := config.DB.Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
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

		// Track totals for each device+interface
		key := fmt.Sprintf("%s-%s", point.Device, point.Intf)
		if _, exists := seriesMap[key]; !exists {
			seriesMap[key] = &DiscardSeriesMean{
				Device: point.Device,
				Intf:   point.Intf,
			}
		}
		seriesMap[key].Total += point.InDiscards + point.OutDiscards
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
