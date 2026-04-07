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
	TS         string                   `json:"ts"`
	Status     *DeviceMetricsStatus     `json:"status,omitempty"`
	Traffic    *DeviceMetricsTraffic    `json:"traffic,omitempty"`
	Interfaces []DeviceInterfaceTraffic `json:"interfaces,omitempty"`
}

// DeviceInterfaceTraffic holds per-interface traffic for a single bucket.
type DeviceInterfaceTraffic struct {
	Intf               string  `json:"intf"`
	LinkPK             string  `json:"link_pk,omitempty"`
	LinkCode           string  `json:"link_code,omitempty"`
	LinkSide           string  `json:"link_side,omitempty"`
	UserPK             string  `json:"user_pk,omitempty"`
	CYOAType           string  `json:"cyoa_type,omitempty"`
	InBps              float64 `json:"in_bps"`
	OutBps             float64 `json:"out_bps"`
	MaxInBps           float64 `json:"max_in_bps"`
	MaxOutBps          float64 `json:"max_out_bps"`
	InErrors           uint64  `json:"in_errors"`
	OutErrors          uint64  `json:"out_errors"`
	InFcsErrors        uint64  `json:"in_fcs_errors"`
	InDiscards         uint64  `json:"in_discards"`
	OutDiscards        uint64  `json:"out_discards"`
	CarrierTransitions uint64  `json:"carrier_transitions"`
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
		perIntfRows   []interfaceRollupRow
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

	// Per-interface traffic breakdown
	if include.Traffic {
		g.Go(func() error {
			var err error
			perIntfRows, err = queryInterfaceRollup(gctx, db, params, interfaceRollupOpts{
				GroupBy:   groupByDeviceIntf,
				DevicePKs: []string{devicePK},
			})
			if err != nil {
				return fmt.Errorf("device per-interface rollup: %w", err)
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

	// Index per-interface rows by bucket timestamp
	perIntfIndex := make(map[time.Time][]interfaceRollupRow)
	for _, r := range perIntfRows {
		perIntfIndex[r.BucketTS] = append(perIntfIndex[r.BucketTS], r)
	}

	// Resolve link PKs to codes
	linkCodes := make(map[string]string)
	if len(perIntfRows) > 0 {
		linkPKSet := make(map[string]struct{})
		for _, r := range perIntfRows {
			if r.LinkPK != "" {
				linkPKSet[r.LinkPK] = struct{}{}
			}
		}
		if len(linkPKSet) > 0 {
			pks := make([]string, 0, len(linkPKSet))
			for pk := range linkPKSet {
				pks = append(pks, pk)
			}
			rows, err := db.Query(ctx, "SELECT pk, code FROM dz_links_current WHERE pk IN ($1)", pks)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var pk, code string
					if err := rows.Scan(&pk, &code); err == nil {
						linkCodes[pk] = code
					}
				}
			}
		}
	}

	// Resolve interface CYOA types from the device interfaces dimension table
	cyoaTypes := make(map[string]string)
	{
		rows, err := db.Query(ctx, "SELECT intf, cyoa_type FROM dz_device_interfaces_current WHERE device_pk = $1 AND cyoa_type != 'none'", devicePK)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var intf, cyoaType string
				if err := rows.Scan(&intf, &cyoaType); err == nil {
					cyoaTypes[intf] = cyoaType
				}
			}
		}
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

		// --- Per-interface traffic ---
		if include.Traffic {
			if intfRowsForBucket := perIntfIndex[bucketStart]; len(intfRowsForBucket) > 0 {
				intfs := make([]DeviceInterfaceTraffic, 0, len(intfRowsForBucket))
				for _, ir := range intfRowsForBucket {
					intfs = append(intfs, DeviceInterfaceTraffic{
						Intf:               ir.Intf,
						LinkPK:             ir.LinkPK,
						LinkCode:           linkCodes[ir.LinkPK],
						LinkSide:           ir.LinkSide,
						UserPK:             ir.UserPK,
						CYOAType:           cyoaTypes[ir.Intf],
						InBps:              ir.AvgInBps,
						OutBps:             ir.AvgOutBps,
						MaxInBps:           ir.MaxInBps,
						MaxOutBps:          ir.MaxOutBps,
						InErrors:           ir.InErrors,
						OutErrors:          ir.OutErrors,
						InFcsErrors:        ir.InFcsErrors,
						InDiscards:         ir.InDiscards,
						OutDiscards:        ir.OutDiscards,
						CarrierTransitions: ir.CarrierTransitions,
					})
				}
				bucket.Interfaces = intfs
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

// --- Bulk handler ---

// BulkDeviceMetricsResponse wraps metrics for multiple devices.
type BulkDeviceMetricsResponse struct {
	TimeRange     string                            `json:"time_range"`
	BucketSeconds int                               `json:"bucket_seconds"`
	BucketCount   int                               `json:"bucket_count"`
	Devices       map[string]*DeviceMetricsResponse `json:"devices"`
}

// isDefaultBulkDeviceMetricsRequest returns true when the request uses default parameters,
// suitable for serving from the page cache.
func bulkDeviceMetricsCacheKey(r *http.Request) string {
	q := r.URL.Query()
	inc := q.Get("include")
	rng := q.Get("range")
	incOK := inc == "" || inc == "all" || inc == "status,traffic" || inc == "status"
	if !incOK || (rng != "" && rng != "24h") || q.Get("start_time") != "" || q.Get("end_time") != "" || q.Get("bucket") != "" {
		return ""
	}
	if q.Get("has_issues") == "true" {
		return "bulk_device_metrics_issues"
	}
	return "bulk_device_metrics"
}

// GetBulkDeviceMetrics handles GET /api/device-metrics.
// It returns metrics for all devices in a single response.
func (a *API) GetBulkDeviceMetrics(w http.ResponseWriter, r *http.Request) {
	// Try page cache for default requests
	if cacheKey := bulkDeviceMetricsCacheKey(r); cacheKey != "" && isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), cacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	w.Header().Set("X-Cache", "MISS")

	q := r.URL.Query()
	include := parseDeviceMetricsInclude(q.Get("include"))

	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	ctx, cancel := statusContext(r, 30*time.Second)
	defer cancel()

	now := time.Now().UTC()
	duration := presetToDuration(timeRange)
	startTime := now.Add(-duration)
	params := parseBucketParamsCustom(startTime, now, 24)
	params.TimeRange = timeRange

	resp, err := a.fetchBulkDeviceMetrics(ctx, params, include)
	if err != nil {
		slog.Error("error fetching bulk device metrics", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if q.Get("has_issues") == "true" {
		filterBulkDeviceMetricsIssuesOnly(resp)
	}

	writeJSON(w, resp)
}

// filterBulkDeviceMetricsIssuesOnly removes devices that have no issues from the response.
func filterBulkDeviceMetricsIssuesOnly(resp *BulkDeviceMetricsResponse) {
	for pk, device := range resp.Devices {
		keep := false
		for _, b := range device.Buckets {
			if b.Status == nil {
				continue
			}
			if b.Status.DrainStatus != "" {
				keep = true
				break
			}
			if !b.Status.Collecting && b.Status.Health != "healthy" && b.Status.Health != "" {
				keep = true
				break
			}
		}
		if !keep {
			delete(resp.Devices, pk)
		}
	}
}

// FetchBulkDeviceMetricsData is the exported entry point for the page cache worker.
func (a *API) FetchBulkDeviceMetricsData(ctx context.Context) (*BulkDeviceMetricsResponse, error) {
	now := time.Now().UTC()
	duration := presetToDuration("24h")
	startTime := now.Add(-duration)
	params := parseBucketParamsCustom(startTime, now, 24)
	params.TimeRange = "24h"
	include := parseDeviceMetricsInclude("status,traffic")
	return a.fetchBulkDeviceMetrics(ctx, params, include)
}

// FetchBulkDeviceMetricsIssuesData is the page cache variant that only includes devices with issues.
func (a *API) FetchBulkDeviceMetricsIssuesData(ctx context.Context) (*BulkDeviceMetricsResponse, error) {
	resp, err := a.FetchBulkDeviceMetricsData(ctx)
	if err != nil {
		return nil, err
	}
	filterBulkDeviceMetricsIssuesOnly(resp)
	return resp, nil
}

// fetchBulkDeviceMetrics runs parallel queries for ALL devices and assembles the bulk response.
func (a *API) fetchBulkDeviceMetrics(ctx context.Context, params bucketParams, include deviceMetricsInclude) (*BulkDeviceMetricsResponse, error) {
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
		metaMap     map[string]*statusDeviceMeta
		intfRows    []interfaceRollupRow
		probeDevSet map[string]bool // devices that have probes
	)

	g, gctx := errgroup.WithContext(ctx)

	// Fetch all device metadata (no PK filter)
	g.Go(func() error {
		var err error
		metaMap, err = queryStatusDeviceMeta(gctx, db)
		if err != nil {
			return fmt.Errorf("bulk device metadata: %w", err)
		}
		return nil
	})

	// Traffic and status both come from interface rollup
	if include.Traffic || include.Status {
		g.Go(func() error {
			var err error
			intfRows, err = queryInterfaceRollup(gctx, db, params, interfaceRollupOpts{
				GroupBy: groupByDevice,
			})
			if err != nil {
				return fmt.Errorf("bulk device interface rollup: %w", err)
			}
			return nil
		})
	}

	// Bulk probe check: find all devices that originate latency probes
	if include.Status {
		g.Go(func() error {
			var startTime time.Time
			if params.StartTime != nil {
				startTime = *params.StartTime
			} else {
				startTime = time.Now().UTC().Add(-time.Duration(params.TotalMinutes) * time.Minute)
			}
			query := `
				SELECT DISTINCT device_pk
				FROM (
					SELECT l.side_a_pk AS device_pk
					FROM link_rollup_5m r FINAL
					JOIN dz_links_current l ON r.link_pk = l.pk
					WHERE r.bucket_ts >= $1 AND r.a_samples > 0
					UNION ALL
					SELECT l.side_z_pk AS device_pk
					FROM link_rollup_5m r FINAL
					JOIN dz_links_current l ON r.link_pk = l.pk
					WHERE r.bucket_ts >= $1 AND r.z_samples > 0
				)
			`
			rows, err := db.Query(gctx, query, startTime)
			if err != nil {
				return fmt.Errorf("bulk device probe check: %w", err)
			}
			defer rows.Close()
			probeDevSet = make(map[string]bool)
			for rows.Next() {
				var pk string
				if err := rows.Scan(&pk); err != nil {
					return fmt.Errorf("bulk device probe scan: %w", err)
				}
				probeDevSet[pk] = true
			}
			return rows.Err()
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Index interface rows by (device_pk, bucket_ts)
	type devBucketKey struct {
		devicePK string
		bucketTS time.Time
	}
	intfIndex := make(map[devBucketKey]*interfaceRollupRow)
	for i := range intfRows {
		bk := devBucketKey{
			devicePK: intfRows[i].DevicePK,
			bucketTS: intfRows[i].BucketTS,
		}
		intfIndex[bk] = &intfRows[i]
	}

	bucketSecs := params.BucketSeconds
	if bucketSecs == 0 {
		bucketSecs = params.BucketMinutes * 60
	}

	// Build per-device responses
	devices := make(map[string]*DeviceMetricsResponse, len(metaMap))
	for devicePK, meta := range metaMap {
		isDrained := health.IsDrainedStatus(meta.Status)
		hasProbes := probeDevSet[devicePK]

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

			row := intfIndex[devBucketKey{devicePK: devicePK, bucketTS: bucketStart}]

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

		devices[devicePK] = &DeviceMetricsResponse{
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
		}
	}

	return &BulkDeviceMetricsResponse{
		TimeRange:     params.TimeRange,
		BucketSeconds: bucketSecs,
		BucketCount:   params.BucketCount,
		Devices:       devices,
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
