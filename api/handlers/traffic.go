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
	Points          []TrafficPoint `json:"points"`
	Series          []SeriesInfo   `json:"series"`
	EffectiveBucket string         `json:"effective_bucket"`
	Truncated       bool           `json:"truncated"`
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

// minBucketForRange returns the minimum allowed bucket interval for a given
// time range to prevent unbounded queries from returning millions of rows.
func minBucketForRange(timeRange string) string {
	switch timeRange {
	case "1h":
		return "2 SECOND"
	case "3h":
		return "10 SECOND"
	case "6h", "12h":
		return "30 SECOND"
	case "24h":
		return "1 MINUTE"
	case "3d":
		return "5 MINUTE"
	case "7d":
		return "10 MINUTE"
	default:
		return "30 SECOND"
	}
}

// bucketSeconds parses a ClickHouse interval string into seconds for comparison.
func bucketSeconds(bucket string) int {
	switch bucket {
	case "2 SECOND":
		return 2
	case "10 SECOND":
		return 10
	case "30 SECOND":
		return 30
	case "1 MINUTE":
		return 60
	case "5 MINUTE":
		return 300
	case "10 MINUTE":
		return 600
	case "15 MINUTE":
		return 900
	case "30 MINUTE":
		return 1800
	case "1 HOUR":
		return 3600
	default:
		return 0
	}
}

// maxTrafficRows is a safety limit on the number of rows returned.
const maxTrafficRows = 500_000

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

	// Enforce minimum bucket size based on time range to prevent OOM
	if bucket == "none" {
		bucket = minBucketForRange(timeRange)
	} else {
		minBucket := minBucketForRange(timeRange)
		if bucketSeconds(bucket) < bucketSeconds(minBucket) {
			bucket = minBucket
		}
	}
	bucketInterval := bucket

	// Build interface filter
	var intfFilter string
	if isTunnel {
		intfFilter = "AND intf LIKE 'Tunnel%'"
	} else {
		intfFilter = "AND intf NOT LIKE 'Tunnel%'"
	}

	start := time.Now()

	// All queries use bucketing now (minimum bucket enforced above).
	// Series means are computed in ClickHouse to avoid accumulating rows in Go.
	query := fmt.Sprintf(`
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
		LIMIT %d
	`, rangeInterval, intfFilter, bucketInterval, aggFunc, aggFunc, maxTrafficRows)

	rows, err := config.DB.Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("Traffic query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Compute series means via a second lightweight query in ClickHouse.
	// This avoids accumulating all points in Go just for mean calculation.
	meanQuery := fmt.Sprintf(`
		WITH devices AS (
			SELECT pk, code
			FROM dz_devices_current
		)
		SELECT
			d.code AS device,
			c.intf,
			AVG(c.in_octets_delta * 8 / c.delta_duration) AS mean_in_bps,
			AVG(c.out_octets_delta * 8 / c.delta_duration) AS mean_out_bps
		FROM fact_dz_device_interface_counters c
		INNER JOIN devices d ON d.pk = c.device_pk
		WHERE c.event_ts >= now() - INTERVAL %s
			%s
			AND c.delta_duration > 0
			AND c.in_octets_delta >= 0
			AND c.out_octets_delta >= 0
		GROUP BY d.code, c.intf
		ORDER BY d.code, c.intf
	`, rangeInterval, intfFilter)

	meanRows, err := config.DB.Query(ctx, meanQuery)
	meanDuration := time.Since(start) - duration
	metrics.RecordClickHouseQuery(meanDuration, err)
	if err != nil {
		log.Printf("Traffic mean query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer meanRows.Close()

	// Build series info from mean query
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

	// Collect points (bounded by LIMIT in query)
	points := []TrafficPoint{}
	for rows.Next() {
		var point TrafficPoint
		if err := rows.Scan(&point.Time, &point.DevicePk, &point.Device, &point.Intf, &point.InBps, &point.OutBps); err != nil {
			log.Printf("Traffic row scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		points = append(points, point)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := TrafficDataResponse{
		Points:          points,
		Series:          series,
		EffectiveBucket: bucket,
		Truncated:       len(points) >= maxTrafficRows,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}

