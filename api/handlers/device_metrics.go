package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/health"
	"golang.org/x/sync/errgroup"
)

// --- Response types ---

// DeviceMetricsResponse is the top-level response for GET /api/device-metrics/{pk}.
type DeviceMetricsResponse struct {
	DevicePK        string                `json:"device_pk"`
	DeviceCode      string                `json:"device_code"`
	DeviceType      string                `json:"device_type"`
	ContributorCode string                `json:"contributor_code"`
	Metro           string                `json:"metro"`
	MaxUsers        int32                 `json:"max_users"`
	TimeRange       string                `json:"time_range"`
	BucketSeconds   int                   `json:"bucket_seconds"`
	BucketCount     int                   `json:"bucket_count"`
	Buckets         []DeviceMetricsBucket `json:"buckets"`
	StatusChanges   []EntityStatusChange  `json:"status_changes,omitempty"`
}

// DeviceMetricsBucket holds all metric categories for a single time bucket.
type DeviceMetricsBucket struct {
	TS      string                `json:"ts"`
	Status  *DeviceMetricsStatus  `json:"status,omitempty"`
	Traffic *DeviceMetricsTraffic `json:"traffic,omitempty"`
}

// DeviceMetricsStatus represents health/drain/ISIS state for a bucket.
type DeviceMetricsStatus struct {
	Health          string `json:"health"`
	DrainStatus     string `json:"drain_status"`
	Collecting      bool   `json:"collecting"`
	ISISOverload    bool   `json:"isis_overload"`
	ISISUnreachable bool   `json:"isis_unreachable"`
	NoProbes        bool   `json:"no_probes"`
}

// DeviceMetricsTraffic holds aggregated throughput and interface counters.
type DeviceMetricsTraffic struct {
	InBps              float64 `json:"in_bps"`
	OutBps             float64 `json:"out_bps"`
	MaxInBps           float64 `json:"max_in_bps"`
	MaxOutBps          float64 `json:"max_out_bps"`
	InPps              float64 `json:"in_pps"`
	OutPps             float64 `json:"out_pps"`
	MaxInPps           float64 `json:"max_in_pps"`
	MaxOutPps          float64 `json:"max_out_pps"`
	InErrors           uint64  `json:"in_errors"`
	OutErrors          uint64  `json:"out_errors"`
	InFcsErrors        uint64  `json:"in_fcs_errors"`
	InDiscards         uint64  `json:"in_discards"`
	OutDiscards        uint64  `json:"out_discards"`
	CarrierTransitions uint64  `json:"carrier_transitions"`
}

// --- Include flags ---

type deviceMetricsInclude struct {
	Status        bool
	Traffic       bool
	StatusChanges bool
}

func parseDeviceMetricsInclude(raw string) deviceMetricsInclude {
	if raw == "" || raw == "all" {
		return deviceMetricsInclude{Status: true, Traffic: true, StatusChanges: true}
	}
	var inc deviceMetricsInclude
	for _, part := range strings.Split(raw, ",") {
		switch strings.TrimSpace(part) {
		case "status":
			inc.Status = true
		case "traffic":
			inc.Traffic = true
		case "status_changes":
			inc.StatusChanges = true
		}
	}
	return inc
}

// --- Handler ---

// GetDeviceMetrics handles GET /api/device-metrics/{pk}.
// It returns all metrics for a single device in a unified bucket structure.
func (a *API) GetDeviceMetrics(w http.ResponseWriter, r *http.Request) {
	devicePK := chi.URLParam(r, "pk")
	if devicePK == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	include := parseDeviceMetricsInclude(q.Get("include"))

	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	startTimeStr := q.Get("start_time")
	endTimeStr := q.Get("end_time")
	bucketStr := q.Get("bucket")

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	// Compute bucket params
	var params bucketParams
	if startTimeStr != "" && endTimeStr != "" {
		startUnix, err1 := strconv.ParseInt(startTimeStr, 10, 64)
		endUnix, err2 := strconv.ParseInt(endTimeStr, 10, 64)
		if err1 != nil || err2 != nil {
			http.Error(w, "invalid start_time or end_time", http.StatusBadRequest)
			return
		}
		startTime := time.Unix(startUnix, 0).UTC()
		endTime := time.Unix(endUnix, 0).UTC()
		params = parseBucketParamsCustom(startTime, endTime, 24)
	} else {
		now := time.Now().UTC()
		duration := presetToDuration(timeRange)
		startTime := now.Add(-duration)
		params = parseBucketParamsCustom(startTime, now, 24)
		params.TimeRange = timeRange
		params.UseRaw = isRawSource(ctx)
	}

	// Override bucket size if explicitly requested
	if bucketStr != "" && bucketStr != "auto" {
		interval, ok := parseBucketString(bucketStr)
		if !ok {
			http.Error(w, "invalid bucket value", http.StatusBadRequest)
			return
		}
		secs := intervalToSeconds(interval)
		var totalSecs int
		if params.StartTime != nil && params.EndTime != nil {
			totalSecs = int(params.EndTime.Sub(*params.StartTime).Seconds())
		} else {
			totalSecs = params.TotalMinutes * 60
		}
		count := totalSecs / secs
		if count < 1 {
			count = 1
		}
		params.BucketSeconds = secs
		params.BucketMinutes = secs / 60
		params.BucketInterval = interval
		params.BucketCount = count
		params.UseRaw = isRawBucket(interval)
	}

	resp, err := a.fetchDeviceMetrics(ctx, devicePK, params, include)
	if err != nil {
		slog.Error("error fetching device metrics", "error", err, "device_pk", devicePK)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if resp == nil {
		http.Error(w, "Device not found", http.StatusNotFound)
		return
	}

	writeJSON(w, resp)
}

// fetchDeviceMetrics runs parallel queries and assembles the unified response.
func (a *API) fetchDeviceMetrics(ctx context.Context, devicePK string, params bucketParams, include deviceMetricsInclude) (*DeviceMetricsResponse, error) {
	db := a.envDB(ctx)

	var bucketDuration time.Duration
	if params.BucketSeconds > 0 {
		bucketDuration = time.Duration(params.BucketSeconds) * time.Second
	} else {
		bucketDuration = time.Duration(params.BucketMinutes) * time.Minute
	}
	now := time.Now().UTC()
	if params.EndTime != nil {
		now = *params.EndTime
	}

	var (
		meta          *statusDeviceMeta
		intfRows      []interfaceRollupRow
		statusChanges []EntityStatusChange
		hasProbes     bool // whether any link connected to this device has probe data
	)

	g, gctx := errgroup.WithContext(ctx)

	// Always fetch metadata
	g.Go(func() error {
		metas, err := queryStatusDeviceMeta(gctx, db, devicePK)
		if err != nil {
			return fmt.Errorf("device metadata: %w", err)
		}
		meta = metas[devicePK]
		return nil
	})

	// Traffic and status both come from interface rollup
	if include.Traffic || include.Status {
		g.Go(func() error {
			var err error
			intfRows, err = queryInterfaceRollup(gctx, db, params, interfaceRollupOpts{
				GroupBy:   groupByDevice,
				DevicePKs: []string{devicePK},
			})
			if err != nil {
				return fmt.Errorf("device interface rollup: %w", err)
			}
			return nil
		})
	}

	// Check if this device has links sending probes in the time window
	if include.Status {
		g.Go(func() error {
			// Check if this device is originating latency probes on any of
			// its links. Side A of a link emits a_samples, side Z emits z_samples.
			query := `
				SELECT count(*) > 0
				FROM link_rollup_5m r FINAL
				JOIN dz_links_current l ON r.link_pk = l.pk
				WHERE r.bucket_ts >= $2
				  AND (
				    (l.side_a_pk = $1 AND r.a_samples > 0)
				    OR (l.side_z_pk = $1 AND r.z_samples > 0)
				  )
			`
			var startTime time.Time
			if params.StartTime != nil {
				startTime = *params.StartTime
			} else {
				startTime = time.Now().UTC().Add(-time.Duration(params.TotalMinutes) * time.Minute)
			}
			row := db.QueryRow(gctx, query, devicePK, startTime)
			if err := row.Scan(&hasProbes); err != nil {
				return fmt.Errorf("device probe check: %w", err)
			}
			return nil
		})
	}

	// Status changes
	if include.StatusChanges {
		g.Go(func() error {
			var startTS, endTS string
			if params.StartTime != nil {
				startTS = params.StartTime.Format(time.RFC3339)
			} else {
				startTS = time.Now().UTC().Add(-time.Duration(params.TotalMinutes) * time.Minute).Format(time.RFC3339)
			}
			if params.EndTime != nil {
				e := params.EndTime.Format(time.RFC3339)
				endTS = e
				statusChanges = fetchDeviceStatusChanges(gctx, db, devicePK, startTS, &endTS)
			} else {
				statusChanges = fetchDeviceStatusChanges(gctx, db, devicePK, startTS, nil)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if meta == nil {
		return nil, nil
	}

	// Index interface rows by bucket timestamp
	intfIndex := make(map[time.Time]*interfaceRollupRow)
	for i := range intfRows {
		intfIndex[intfRows[i].BucketTS] = &intfRows[i]
	}

	isDrained := health.IsDrainedStatus(meta.Status)

	// Build buckets
	buckets := make([]DeviceMetricsBucket, 0, params.BucketCount)
	for i := params.BucketCount - 1; i >= 0; i-- {
		var bucketStart time.Time
		if params.StartTime != nil {
			bucketStart = params.StartTime.Truncate(bucketDuration).Add(time.Duration(params.BucketCount-1-i) * bucketDuration)
		} else {
			bucketStart = now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
		}
		isCollecting := i == 0

		row := intfIndex[bucketStart]

		bucket := DeviceMetricsBucket{
			TS: bucketStart.Format(time.RFC3339),
		}

		// --- Status ---
		if include.Status {
			st := buildDeviceMetricsStatus(row, meta, isDrained, isCollecting, hasProbes)
			bucket.Status = &st
		}

		// --- Traffic ---
		if include.Traffic && row != nil {
			bucket.Traffic = &DeviceMetricsTraffic{
				InBps:              row.AvgInBps,
				OutBps:             row.AvgOutBps,
				MaxInBps:           row.MaxInBps,
				MaxOutBps:          row.MaxOutBps,
				InPps:              row.AvgInPps,
				OutPps:             row.AvgOutPps,
				MaxInPps:           row.MaxInPps,
				MaxOutPps:          row.MaxOutPps,
				InErrors:           row.InErrors,
				OutErrors:          row.OutErrors,
				InFcsErrors:        row.InFcsErrors,
				InDiscards:         row.InDiscards,
				OutDiscards:        row.OutDiscards,
				CarrierTransitions: row.CarrierTransitions,
			}
		}

		buckets = append(buckets, bucket)
	}

	bucketSecs := params.BucketSeconds
	if bucketSecs == 0 {
		bucketSecs = params.BucketMinutes * 60
	}

	return &DeviceMetricsResponse{
		DevicePK:        meta.PK,
		DeviceCode:      meta.Code,
		DeviceType:      meta.DeviceType,
		ContributorCode: meta.Contributor,
		Metro:           meta.Metro,
		MaxUsers:        meta.MaxUsers,
		TimeRange:       params.TimeRange,
		BucketSeconds:   bucketSecs,
		BucketCount:     params.BucketCount,
		Buckets:         buckets,
		StatusChanges:   statusChanges,
	}, nil
}

// buildDeviceMetricsStatus computes health status for a single device bucket.
// Mirrors the logic in fetchDeviceHistoryFromRollup.
func buildDeviceMetricsStatus(
	row *interfaceRollupRow,
	meta *statusDeviceMeta,
	isDrained bool,
	isCollecting bool,
	hasProbes bool,
) DeviceMetricsStatus {
	if row == nil {
		drainStatus := ""
		if isDrained {
			drainStatus = meta.Status
		}
		statusStr := "no_data"
		if drainStatus != "" {
			statusStr = "disabled"
		}
		return DeviceMetricsStatus{
			Health:      statusStr,
			DrainStatus: drainStatus,
			Collecting:  isCollecting,
		}
	}

	totalErrors := row.InErrors + row.OutErrors + row.InFcsErrors
	totalDiscards := row.InDiscards + row.OutDiscards
	statusStr := health.ClassifyDeviceStatus(totalErrors, totalDiscards, row.CarrierTransitions)

	// Drain status from rollup
	drainStatus := ""
	if health.IsDrainedStatus(row.Status) || row.WasDrained {
		if health.IsDrainedStatus(row.Status) {
			drainStatus = row.Status
		} else {
			drainStatus = "soft-drained"
		}
	}
	if drainStatus != "" {
		statusStr = "disabled"
	}

	// If the device isn't sending latency probes and it's not drained/collecting,
	// upgrade to unhealthy — probes are the heartbeat from the device.
	noProbes := !hasProbes && drainStatus == "" && !isCollecting
	if noProbes {
		if statusStr == "healthy" || statusStr == "degraded" {
			statusStr = "unhealthy"
		}
	}

	return DeviceMetricsStatus{
		Health:          statusStr,
		DrainStatus:     drainStatus,
		NoProbes:        noProbes,
		Collecting:      isCollecting,
		ISISOverload:    row.ISISOverload,
		ISISUnreachable: row.ISISUnreachable,
	}
}
