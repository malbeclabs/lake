package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

const (
	ControllerCallStatusCalling     = "calling"
	ControllerCallStatusStopped     = "stopped"
	ControllerCallStatusRecovered   = "recovered"
	ControllerCallStatusNoData      = "no_data"
	ControllerCallStatusNotExpected = "not_expected"

	controllerCallsTable              = "controller_grpc_getconfig_success"
	controllerCallAlertThreshold      = 30 * time.Minute
	controllerCallHistoryWindow       = 72 * time.Hour
	controllerCallPriorHistoryMinimum = uint64(4000)
)

// DeviceControllerCallsResponse is the response for
// GET /api/dz/devices/{pk}/controller-calls.
type DeviceControllerCallsResponse struct {
	DevicePK              string                        `json:"device_pk"`
	DeviceCode            string                        `json:"device_code"`
	DeviceStatus          string                        `json:"device_status"`
	TimeRange             string                        `json:"time_range"`
	BucketSeconds         int                           `json:"bucket_seconds"`
	BucketCount           int                           `json:"bucket_count"`
	From                  string                        `json:"from"`
	To                    string                        `json:"to"`
	SourceAvailable       bool                          `json:"source_available"`
	LastCallAt            *string                       `json:"last_call_at,omitempty"`
	CurrentGapSeconds     *int                          `json:"current_gap_seconds,omitempty"`
	LastStatus            string                        `json:"last_status"`
	TotalCalls            uint64                        `json:"total_calls"`
	MinutesWithCalls      uint64                        `json:"minutes_with_calls"`
	AlertThresholdMinutes int                           `json:"alert_threshold_minutes"`
	HistoryWindowHours    int                           `json:"history_window_hours"`
	PriorHistoryMinimum   uint64                        `json:"prior_history_minimum"`
	Buckets               []DeviceControllerCallsBucket `json:"buckets"`
}

// DeviceControllerCallsBucket describes controller GetConfig calls for one bucket.
type DeviceControllerCallsBucket struct {
	TS               string `json:"ts"`
	Calls            uint64 `json:"calls"`
	MinutesWithCalls uint64 `json:"minutes_with_calls"`
	Status           string `json:"status"`
	GapSeconds       *int   `json:"gap_seconds,omitempty"`
}

type deviceControllerCallMeta struct {
	Code   string
	Status string
}

type controllerCallBucketAgg struct {
	Calls            uint64
	MinutesWithCalls uint64
}

type controllerCallMinuteAgg struct {
	TS     time.Time
	LastTS time.Time
	Calls  uint64
}

// GetDeviceControllerCalls returns controller.GetConfig call history for one device.
func (a *API) GetDeviceControllerCalls(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}

	params, err := resolveControllerCallParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn := a.envDB(ctx)
	meta, err := queryDeviceControllerCallMeta(ctx, conn, pk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "device not found", http.StatusNotFound)
			return
		}
		logError("device controller calls: device lookup failed", "error", err, "pk", pk)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	sourceDB := ControllerCallsDatabaseForEnv(EnvFromContext(ctx))
	sourceAvailable, err := controllerCallsSourceAvailable(ctx, conn, sourceDB)
	if err != nil {
		logError("device controller calls: source check failed", "error", err, "pk", pk, "database", sourceDB)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	bucketSecs := params.BucketSeconds
	if bucketSecs <= 0 {
		bucketSecs = 300
	}

	if !sourceAvailable {
		resp := emptyDeviceControllerCallsResponse(pk, meta, params, bucketSecs, false)
		writeJSON(w, resp)
		return
	}

	bucketAggs, err := queryControllerCallBucketAggs(ctx, conn, sourceDB, pk, params, bucketSecs)
	if err != nil {
		if controllerCallSourceErr(err) {
			resp := emptyDeviceControllerCallsResponse(pk, meta, params, bucketSecs, false)
			writeJSON(w, resp)
			return
		}
		logError("device controller calls: bucket query failed", "error", err, "pk", pk, "database", sourceDB)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	minuteAggs, err := queryControllerCallMinuteAggs(ctx, conn, sourceDB, pk, params)
	if err != nil {
		if controllerCallSourceErr(err) {
			resp := emptyDeviceControllerCallsResponse(pk, meta, params, bucketSecs, false)
			writeJSON(w, resp)
			return
		}
		logError("device controller calls: minute query failed", "error", err, "pk", pk, "database", sourceDB)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	resp := buildDeviceControllerCallsResponse(pk, meta, params, bucketSecs, true, bucketAggs, minuteAggs)
	writeJSON(w, resp)
}

func resolveControllerCallParams(r *http.Request) (bucketParams, error) {
	q := r.URL.Query()
	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	startTimeStr := q.Get("start_time")
	endTimeStr := q.Get("end_time")
	bucketStr := q.Get("bucket")

	var params bucketParams
	if startTimeStr != "" && endTimeStr != "" {
		startUnix, err1 := strconv.ParseInt(startTimeStr, 10, 64)
		endUnix, err2 := strconv.ParseInt(endTimeStr, 10, 64)
		if err1 != nil || err2 != nil {
			return params, fmt.Errorf("invalid start_time or end_time")
		}
		startTime := time.Unix(startUnix, 0).UTC()
		endTime := time.Unix(endUnix, 0).UTC()
		if !endTime.After(startTime) {
			return params, fmt.Errorf("end_time must be after start_time")
		}
		params = parseBucketParamsCustom(startTime, endTime, 24)
	} else {
		now := time.Now().UTC()
		duration := presetToDuration(timeRange)
		startTime := now.Add(-duration)
		params = parseBucketParamsCustom(startTime, now, 24)
		params.TimeRange = timeRange
	}

	if bucketStr != "" && bucketStr != "auto" {
		interval, ok := parseBucketString(bucketStr)
		if !ok {
			return params, fmt.Errorf("invalid bucket value")
		}
		secs := intervalToSeconds(interval)
		var totalSecs int
		if params.StartTime != nil && params.EndTime != nil {
			totalSecs = int(params.EndTime.Sub(*params.StartTime).Seconds())
		} else {
			totalSecs = params.TotalMinutes * 60
		}
		count := (totalSecs + secs - 1) / secs
		if count < 1 {
			count = 1
		}
		params.BucketSeconds = secs
		params.BucketMinutes = secs / 60
		params.BucketInterval = interval
		params.BucketCount = count
		params.UseRaw = true
	}

	return params, nil
}

func queryDeviceControllerCallMeta(ctx context.Context, conn driver.Conn, pk string) (deviceControllerCallMeta, error) {
	var meta deviceControllerCallMeta
	query := `SELECT code, status FROM dz_devices_current WHERE pk = ?`
	start := time.Now()
	err := conn.QueryRow(ctx, query, pk).Scan(&meta.Code, &meta.Status)
	metrics.RecordClickHouseQuery("device_controller_calls_device", time.Since(start), err)
	return meta, err
}

func controllerCallsSourceAvailable(ctx context.Context, conn driver.Conn, database string) (bool, error) {
	var count uint64
	query := `
		SELECT count()
		FROM system.tables
		WHERE database = ?
		  AND name = ?
	`
	start := time.Now()
	err := conn.QueryRow(ctx, query, database, controllerCallsTable).Scan(&count)
	metrics.RecordClickHouseQuery("device_controller_calls_source", time.Since(start), err)
	return count > 0, err
}

func queryControllerCallBucketAggs(ctx context.Context, conn driver.Conn, sourceDB, pk string, params bucketParams, bucketSecs int) (map[time.Time]controllerCallBucketAgg, error) {
	query := fmt.Sprintf(`
		SELECT
			toDateTime(toStartOfInterval(timestamp, INTERVAL %[2]d SECOND, 'UTC')) AS bucket_ts,
			count() AS calls,
			uniqExact(toStartOfMinute(timestamp)) AS minutes_with_calls
		FROM %[1]s.%[3]s
		WHERE timestamp >= ?
		  AND timestamp < ?
		  AND device_pubkey = ?
		GROUP BY bucket_ts
		ORDER BY bucket_ts
		LIMIT 100000
		SETTINGS max_execution_time = 30,
		         max_rows_to_read = 1000000000,
		         timeout_before_checking_execution_speed = 0
	`, quoteClickHouseIdent(sourceDB), bucketSecs, quoteClickHouseIdent(controllerCallsTable))

	start := time.Now()
	rows, err := conn.Query(ctx, query, *params.StartTime, *params.EndTime, pk)
	metrics.RecordClickHouseQuery("device_controller_calls_buckets", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[time.Time]controllerCallBucketAgg)
	for rows.Next() {
		var (
			ts               time.Time
			calls            uint64
			minutesWithCalls uint64
		)
		if err := rows.Scan(&ts, &calls, &minutesWithCalls); err != nil {
			return nil, err
		}
		out[ts.UTC()] = controllerCallBucketAgg{Calls: calls, MinutesWithCalls: minutesWithCalls}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func queryControllerCallMinuteAggs(ctx context.Context, conn driver.Conn, sourceDB, pk string, params bucketParams) ([]controllerCallMinuteAgg, error) {
	historyStart := params.StartTime.Add(-controllerCallHistoryWindow)
	query := fmt.Sprintf(`
		SELECT
			toDateTime(toStartOfMinute(timestamp)) AS minute_ts,
			max(timestamp) AS last_ts,
			count() AS calls
		FROM %[1]s.%[2]s
		WHERE timestamp >= ?
		  AND timestamp < ?
		  AND device_pubkey = ?
		GROUP BY minute_ts
		ORDER BY minute_ts
		LIMIT 100000
		SETTINGS max_execution_time = 30,
		         max_rows_to_read = 1000000000,
		         timeout_before_checking_execution_speed = 0
	`, quoteClickHouseIdent(sourceDB), quoteClickHouseIdent(controllerCallsTable))

	start := time.Now()
	rows, err := conn.Query(ctx, query, historyStart, *params.EndTime, pk)
	metrics.RecordClickHouseQuery("device_controller_calls_minutes", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []controllerCallMinuteAgg{}
	for rows.Next() {
		var (
			ts     time.Time
			lastTS time.Time
			calls  uint64
		)
		if err := rows.Scan(&ts, &lastTS, &calls); err != nil {
			return nil, err
		}
		out = append(out, controllerCallMinuteAgg{TS: ts.UTC(), LastTS: lastTS.UTC(), Calls: calls})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func buildDeviceControllerCallsResponse(
	pk string,
	meta deviceControllerCallMeta,
	params bucketParams,
	bucketSecs int,
	sourceAvailable bool,
	bucketAggs map[time.Time]controllerCallBucketAgg,
	minuteAggs []controllerCallMinuteAgg,
) DeviceControllerCallsResponse {
	buckets := make([]DeviceControllerCallsBucket, 0, params.BucketCount)
	bucketDuration := time.Duration(bucketSecs) * time.Second

	prefixTimes := make([]time.Time, 0, len(minuteAggs))
	latestTimes := make([]time.Time, 0, len(minuteAggs))
	prefixCalls := make([]uint64, len(minuteAggs)+1)
	var lastCallAt *time.Time
	for i, minute := range minuteAggs {
		prefixTimes = append(prefixTimes, minute.TS)
		latestTimes = append(latestTimes, minute.LastTS)
		prefixCalls[i+1] = prefixCalls[i] + minute.Calls
		if minute.Calls > 0 {
			lastTS := minute.LastTS
			lastCallAt = &lastTS
		}
	}

	var (
		totalCalls       uint64
		minutesWithCalls uint64
		stoppedOpen      bool
		lastStatus       = ControllerCallStatusNoData
	)

	for i := 0; i < params.BucketCount; i++ {
		bucketStart := params.StartTime.Add(time.Duration(i) * bucketDuration).UTC()
		bucketEnd := bucketStart.Add(bucketDuration)
		agg := bucketAggs[bucketStart]
		totalCalls += agg.Calls
		minutesWithCalls += agg.MinutesWithCalls

		status := ControllerCallStatusNoData
		if !sourceAvailable {
			status = ControllerCallStatusNoData
		} else if agg.Calls > 0 {
			if stoppedOpen {
				status = ControllerCallStatusRecovered
				stoppedOpen = false
			} else {
				status = ControllerCallStatusCalling
			}
		} else {
			recentCalls := sumControllerCalls(prefixTimes, prefixCalls, bucketEnd.Add(-controllerCallAlertThreshold), bucketEnd)
			if recentCalls > 0 {
				status = ControllerCallStatusCalling
			} else if sumControllerCalls(prefixTimes, prefixCalls, bucketEnd.Add(-controllerCallHistoryWindow), bucketEnd) > controllerCallPriorHistoryMinimum {
				status = ControllerCallStatusStopped
				stoppedOpen = true
			} else {
				stoppedOpen = false
			}
		}

		var gapSeconds *int
		if last := latestControllerCallAt(prefixTimes, latestTimes, bucketEnd); last != nil {
			gap := int(bucketEnd.Sub(*last).Seconds())
			if gap >= 0 {
				gapSeconds = &gap
			}
		}

		lastStatus = status
		buckets = append(buckets, DeviceControllerCallsBucket{
			TS:               bucketStart.Format(time.RFC3339),
			Calls:            agg.Calls,
			MinutesWithCalls: agg.MinutesWithCalls,
			Status:           status,
			GapSeconds:       gapSeconds,
		})
	}

	var lastCallAtStr *string
	var currentGapSeconds *int
	if lastCallAt != nil {
		formatted := lastCallAt.UTC().Format(time.RFC3339)
		lastCallAtStr = &formatted
		gap := int(params.EndTime.Sub(*lastCallAt).Seconds())
		if gap >= 0 {
			currentGapSeconds = &gap
		}
	}

	return DeviceControllerCallsResponse{
		DevicePK:              pk,
		DeviceCode:            meta.Code,
		DeviceStatus:          meta.Status,
		TimeRange:             params.TimeRange,
		BucketSeconds:         bucketSecs,
		BucketCount:           params.BucketCount,
		From:                  params.StartTime.UTC().Format(time.RFC3339),
		To:                    params.EndTime.UTC().Format(time.RFC3339),
		SourceAvailable:       sourceAvailable,
		LastCallAt:            lastCallAtStr,
		CurrentGapSeconds:     currentGapSeconds,
		LastStatus:            lastStatus,
		TotalCalls:            totalCalls,
		MinutesWithCalls:      minutesWithCalls,
		AlertThresholdMinutes: int(controllerCallAlertThreshold / time.Minute),
		HistoryWindowHours:    int(controllerCallHistoryWindow / time.Hour),
		PriorHistoryMinimum:   controllerCallPriorHistoryMinimum,
		Buckets:               buckets,
	}
}

func emptyDeviceControllerCallsResponse(pk string, meta deviceControllerCallMeta, params bucketParams, bucketSecs int, sourceAvailable bool) DeviceControllerCallsResponse {
	return buildDeviceControllerCallsResponse(pk, meta, params, bucketSecs, sourceAvailable, map[time.Time]controllerCallBucketAgg{}, nil)
}

func sumControllerCalls(times []time.Time, prefix []uint64, start, end time.Time) uint64 {
	if len(times) == 0 || !end.After(start) {
		return 0
	}
	lo := sortSearchTimes(times, start)
	hi := sortSearchTimes(times, end)
	return prefix[hi] - prefix[lo]
}

func latestControllerCallAt(times, latestTimes []time.Time, before time.Time) *time.Time {
	idx := sortSearchTimes(times, before) - 1
	for idx >= 0 && idx < len(times) {
		ts := latestTimes[idx]
		if ts.Before(before) {
			return &ts
		}
		idx--
	}
	return nil
}

func sortSearchTimes(times []time.Time, target time.Time) int {
	lo, hi := 0, len(times)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if times[mid].Before(target) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func controllerCallSourceErr(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "unknown table") ||
		strings.Contains(errStr, "unknown database") ||
		strings.Contains(errStr, "table not found") ||
		strings.Contains(errStr, "access denied") ||
		strings.Contains(errStr, "permission denied")
}
