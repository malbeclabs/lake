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

// LinkMetricsResponse is the top-level response for GET /api/link-metrics/{pk}.
type LinkMetricsResponse struct {
	LinkPK            string               `json:"link_pk"`
	LinkCode          string               `json:"link_code"`
	LinkType          string               `json:"link_type"`
	ContributorCode   string               `json:"contributor_code"`
	SideAMetro        string               `json:"side_a_metro"`
	SideZMetro        string               `json:"side_z_metro"`
	CommittedRttUs    float64              `json:"committed_rtt_us"`
	CommittedJitterUs float64              `json:"committed_jitter_us"`
	BandwidthBps      int64                `json:"bandwidth_bps"`
	TimeRange         string               `json:"time_range"`
	BucketSeconds     int                  `json:"bucket_seconds"`
	BucketCount       int                  `json:"bucket_count"`
	Buckets           []LinkMetricsBucket  `json:"buckets"`
	StatusChanges     []EntityStatusChange `json:"status_changes,omitempty"`
}

// LinkMetricsBucket holds all metric categories for a single time bucket.
type LinkMetricsBucket struct {
	TS      string              `json:"ts"`
	Status  *LinkMetricsStatus  `json:"status,omitempty"`
	Latency *LinkMetricsLatency `json:"latency,omitempty"`
	Traffic *LinkMetricsTraffic `json:"traffic,omitempty"`
}

// LinkMetricsStatus represents health/drain/provisioning state for a bucket.
type LinkMetricsStatus struct {
	Health       string   `json:"health"`
	DrainStatus  string   `json:"drain_status"`
	Provisioning bool     `json:"provisioning"`
	ISISDown     bool     `json:"isis_down"`
	Collecting   bool     `json:"collecting"`
	Reasons      []string `json:"reasons,omitempty"`
}

// LinkMetricsLatency holds per-direction latency and jitter percentiles.
type LinkMetricsLatency struct {
	AAvgRttUs float64 `json:"a_avg_rtt_us"`
	AMinRttUs float64 `json:"a_min_rtt_us"`
	AP50RttUs float64 `json:"a_p50_rtt_us"`
	AP90RttUs float64 `json:"a_p90_rtt_us"`
	AP95RttUs float64 `json:"a_p95_rtt_us"`
	AP99RttUs float64 `json:"a_p99_rtt_us"`
	AMaxRttUs float64 `json:"a_max_rtt_us"`
	ALossPct  float64 `json:"a_loss_pct"`
	ASamples  uint64  `json:"a_samples"`
	ZAvgRttUs float64 `json:"z_avg_rtt_us"`
	ZMinRttUs float64 `json:"z_min_rtt_us"`
	ZP50RttUs float64 `json:"z_p50_rtt_us"`
	ZP90RttUs float64 `json:"z_p90_rtt_us"`
	ZP95RttUs float64 `json:"z_p95_rtt_us"`
	ZP99RttUs float64 `json:"z_p99_rtt_us"`
	ZMaxRttUs float64 `json:"z_max_rtt_us"`
	ZLossPct  float64 `json:"z_loss_pct"`
	ZSamples  uint64  `json:"z_samples"`

	AAvgJitterUs float64 `json:"a_avg_jitter_us"`
	AMinJitterUs float64 `json:"a_min_jitter_us"`
	AP50JitterUs float64 `json:"a_p50_jitter_us"`
	AP90JitterUs float64 `json:"a_p90_jitter_us"`
	AP95JitterUs float64 `json:"a_p95_jitter_us"`
	AP99JitterUs float64 `json:"a_p99_jitter_us"`
	AMaxJitterUs float64 `json:"a_max_jitter_us"`
	ZAvgJitterUs float64 `json:"z_avg_jitter_us"`
	ZMinJitterUs float64 `json:"z_min_jitter_us"`
	ZP50JitterUs float64 `json:"z_p50_jitter_us"`
	ZP90JitterUs float64 `json:"z_p90_jitter_us"`
	ZP95JitterUs float64 `json:"z_p95_jitter_us"`
	ZP99JitterUs float64 `json:"z_p99_jitter_us"`
	ZMaxJitterUs float64 `json:"z_max_jitter_us"`
}

// LinkMetricsTraffic holds per-side throughput and interface counters plus utilization.
type LinkMetricsTraffic struct {
	SideAInBps              float64 `json:"side_a_in_bps"`
	SideAOutBps             float64 `json:"side_a_out_bps"`
	SideZInBps              float64 `json:"side_z_in_bps"`
	SideZOutBps             float64 `json:"side_z_out_bps"`
	SideAMaxInBps           float64 `json:"side_a_max_in_bps"`
	SideAMaxOutBps          float64 `json:"side_a_max_out_bps"`
	SideZMaxInBps           float64 `json:"side_z_max_in_bps"`
	SideZMaxOutBps          float64 `json:"side_z_max_out_bps"`
	SideAInPps              float64 `json:"side_a_in_pps"`
	SideAOutPps             float64 `json:"side_a_out_pps"`
	SideZInPps              float64 `json:"side_z_in_pps"`
	SideZOutPps             float64 `json:"side_z_out_pps"`
	SideAMaxInPps           float64 `json:"side_a_max_in_pps"`
	SideAMaxOutPps          float64 `json:"side_a_max_out_pps"`
	SideZMaxInPps           float64 `json:"side_z_max_in_pps"`
	SideZMaxOutPps          float64 `json:"side_z_max_out_pps"`
	SideAInErrors           uint64  `json:"side_a_in_errors"`
	SideAOutErrors          uint64  `json:"side_a_out_errors"`
	SideAInFcsErrors        uint64  `json:"side_a_in_fcs_errors"`
	SideAInDiscards         uint64  `json:"side_a_in_discards"`
	SideAOutDiscards        uint64  `json:"side_a_out_discards"`
	SideACarrierTransitions uint64  `json:"side_a_carrier_transitions"`
	SideZInErrors           uint64  `json:"side_z_in_errors"`
	SideZOutErrors          uint64  `json:"side_z_out_errors"`
	SideZInFcsErrors        uint64  `json:"side_z_in_fcs_errors"`
	SideZInDiscards         uint64  `json:"side_z_in_discards"`
	SideZOutDiscards        uint64  `json:"side_z_out_discards"`
	SideZCarrierTransitions uint64  `json:"side_z_carrier_transitions"`
	UtilizationInPct        float64 `json:"utilization_in_pct"`
	UtilizationOutPct       float64 `json:"utilization_out_pct"`
}

// BulkLinkMetricsResponse wraps metrics for multiple links.
type BulkLinkMetricsResponse struct {
	TimeRange     string                          `json:"time_range"`
	BucketSeconds int                             `json:"bucket_seconds"`
	BucketCount   int                             `json:"bucket_count"`
	Links         map[string]*LinkMetricsResponse `json:"links"`
}

// bulkIntfKey indexes interface rollup rows by (link_pk, bucket, side) for bulk queries.
type bulkIntfKey struct {
	linkPK   string
	bucketTS time.Time
	side     string
}

// --- Include flags ---

type linkMetricsInclude struct {
	Status        bool
	Latency       bool
	Traffic       bool
	StatusChanges bool
}

func parseLinkMetricsInclude(raw string) linkMetricsInclude {
	if raw == "" || raw == "all" {
		return linkMetricsInclude{Status: true, Latency: true, Traffic: true, StatusChanges: true}
	}
	var inc linkMetricsInclude
	for _, part := range strings.Split(raw, ",") {
		switch strings.TrimSpace(part) {
		case "status":
			inc.Status = true
		case "latency":
			inc.Latency = true
		case "traffic":
			inc.Traffic = true
		case "status_changes":
			inc.StatusChanges = true
		}
	}
	return inc
}

// linkMetricsSideKey is used to index interface rollup data by (bucket, side).
type linkMetricsSideKey struct {
	bucketTS time.Time
	side     string
}

// --- Bucket param parsing from explicit bucket string ---

// parseBucketString converts a user-facing bucket size string (e.g. "10s", "5m", "1h")
// into a SQL interval string understood by bucketForDuration / intervalToSeconds.
func parseBucketString(s string) (string, bool) {
	m := map[string]string{
		"10s": "10 SECOND",
		"30s": "30 SECOND",
		"1m":  "1 MINUTE",
		"5m":  "5 MINUTE",
		"10m": "10 MINUTE",
		"15m": "15 MINUTE",
		"30m": "30 MINUTE",
		"1h":  "1 HOUR",
		"4h":  "4 HOUR",
		"12h": "12 HOUR",
		"1d":  "1 DAY",
	}
	v, ok := m[s]
	return v, ok
}

// --- Handler ---

// GetLinkMetrics handles GET /api/link-metrics/{pk}.
// It returns all metrics for a single link in a unified bucket structure.
func (a *API) GetLinkMetrics(w http.ResponseWriter, r *http.Request) {
	linkPK := chi.URLParam(r, "pk")
	if linkPK == "" {
		http.Error(w, "missing link pk", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	include := parseLinkMetricsInclude(q.Get("include"))

	// Parse time range / custom window
	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	startTimeStr := q.Get("start_time")
	endTimeStr := q.Get("end_time")
	bucketStr := q.Get("bucket")

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	// Compute bucket params using bucketForDuration for optimal granularity.
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

	resp, err := a.fetchLinkMetrics(ctx, linkPK, params, include)
	if err != nil {
		slog.Error("error fetching link metrics", "error", err, "link_pk", linkPK)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if resp == nil {
		http.Error(w, "Link not found", http.StatusNotFound)
		return
	}

	writeJSON(w, resp)
}

// fetchLinkMetrics runs parallel queries and assembles the unified response.
func (a *API) fetchLinkMetrics(ctx context.Context, linkPK string, params bucketParams, include linkMetricsInclude) (*LinkMetricsResponse, error) {
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
		meta            *statusLinkMeta
		linkRollupMap   map[linkBucketKey]*linkRollupRow
		intfRows        []interfaceRollupRow
		statusChanges   []EntityStatusChange
		currentISISDown map[string]bool
	)

	g, gctx := errgroup.WithContext(ctx)

	// Always fetch metadata
	g.Go(func() error {
		metas, err := queryStatusLinkMeta(gctx, db, linkPK)
		if err != nil {
			return fmt.Errorf("link metadata: %w", err)
		}
		meta = metas[linkPK]
		return nil
	})

	// Latency/status rollup
	if include.Latency || include.Status {
		g.Go(func() error {
			var err error
			linkRollupMap, err = queryLinkRollup(gctx, db, params, linkPK)
			if err != nil {
				return fmt.Errorf("link rollup: %w", err)
			}
			return nil
		})
	}

	// Real-time ISIS adjacency state for collecting bucket
	if include.Status {
		g.Go(func() error {
			var err error
			currentISISDown, err = queryCurrentISISDown(gctx, db, linkPK)
			if err != nil {
				slog.Warn("failed to query current ISIS state", "error", err)
				currentISISDown = nil
			}
			return nil
		})
	}

	// Traffic (interface rollup)
	if include.Traffic {
		g.Go(func() error {
			var err error
			intfRows, err = queryInterfaceRollup(gctx, db, params, interfaceRollupOpts{
				GroupBy: groupByLinkSide,
				LinkPKs: []string{linkPK},
			})
			if err != nil {
				return fmt.Errorf("interface rollup: %w", err)
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
				statusChanges = fetchLinkStatusChanges(gctx, db, linkPK, startTS, &endTS)
			} else {
				statusChanges = fetchLinkStatusChanges(gctx, db, linkPK, startTS, nil)
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

	// Index interface rows by (bucket, side)
	intfIndex := make(map[linkMetricsSideKey]*interfaceRollupRow)
	for i := range intfRows {
		sk := linkMetricsSideKey{bucketTS: intfRows[i].BucketTS, side: intfRows[i].LinkSide}
		intfIndex[sk] = &intfRows[i]
	}

	committedRtt := meta.CommittedRttUs

	// For health classification, only consider latency on inter-metro WAN links
	healthCommittedRtt := committedRtt
	if meta.LinkType != "WAN" || meta.SideAMetro == meta.SideZMetro {
		healthCommittedRtt = 0
	}

	// Build buckets
	buckets := make([]LinkMetricsBucket, 0, params.BucketCount)
	for i := params.BucketCount - 1; i >= 0; i-- {
		var bucketStart time.Time
		if params.StartTime != nil {
			bucketStart = params.StartTime.Truncate(bucketDuration).Add(time.Duration(params.BucketCount-1-i) * bucketDuration)
		} else {
			bucketStart = now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
		}
		isCollecting := i == 0

		bk := linkBucketKey{LinkPK: linkPK, BucketTS: bucketStart}
		rollup := linkRollupMap[bk]

		bucket := LinkMetricsBucket{
			TS: bucketStart.Format(time.RFC3339),
		}

		// --- Status ---
		if include.Status {
			st := buildLinkMetricsStatus(rollup, meta, healthCommittedRtt, isCollecting, bucketStart, intfIndex, currentISISDown[linkPK])
			bucket.Status = &st
		}

		// --- Latency ---
		if include.Latency && rollup != nil && (rollup.ASamples > 0 || rollup.ZSamples > 0) {
			bucket.Latency = &LinkMetricsLatency{
				AAvgRttUs: rollup.AAvgRttUs,
				AMinRttUs: rollup.AMinRttUs,
				AP50RttUs: rollup.AP50RttUs,
				AP90RttUs: rollup.AP90RttUs,
				AP95RttUs: rollup.AP95RttUs,
				AP99RttUs: rollup.AP99RttUs,
				AMaxRttUs: rollup.AMaxRttUs,
				ALossPct:  rollup.ALossPct,
				ASamples:  rollup.ASamples,
				ZAvgRttUs: rollup.ZAvgRttUs,
				ZMinRttUs: rollup.ZMinRttUs,
				ZP50RttUs: rollup.ZP50RttUs,
				ZP90RttUs: rollup.ZP90RttUs,
				ZP95RttUs: rollup.ZP95RttUs,
				ZP99RttUs: rollup.ZP99RttUs,
				ZMaxRttUs: rollup.ZMaxRttUs,
				ZLossPct:  rollup.ZLossPct,
				ZSamples:  rollup.ZSamples,

				AAvgJitterUs: rollup.AAvgJitterUs,
				AMinJitterUs: rollup.AMinJitterUs,
				AP50JitterUs: rollup.AP50JitterUs,
				AP90JitterUs: rollup.AP90JitterUs,
				AP95JitterUs: rollup.AP95JitterUs,
				AP99JitterUs: rollup.AP99JitterUs,
				AMaxJitterUs: rollup.AMaxJitterUs,
				ZAvgJitterUs: rollup.ZAvgJitterUs,
				ZMinJitterUs: rollup.ZMinJitterUs,
				ZP50JitterUs: rollup.ZP50JitterUs,
				ZP90JitterUs: rollup.ZP90JitterUs,
				ZP95JitterUs: rollup.ZP95JitterUs,
				ZP99JitterUs: rollup.ZP99JitterUs,
				ZMaxJitterUs: rollup.ZMaxJitterUs,
			}
		}

		// --- Traffic ---
		if include.Traffic {
			traffic := buildLinkMetricsTraffic(bucketStart, intfIndex, meta.BandwidthBps)
			if traffic != nil {
				bucket.Traffic = traffic
			}
		}

		buckets = append(buckets, bucket)
	}

	bucketSecs := params.BucketSeconds
	if bucketSecs == 0 {
		bucketSecs = params.BucketMinutes * 60
	}

	return &LinkMetricsResponse{
		LinkPK:            meta.PK,
		LinkCode:          meta.Code,
		LinkType:          meta.LinkType,
		ContributorCode:   meta.Contributor,
		SideAMetro:        meta.SideAMetro,
		SideZMetro:        meta.SideZMetro,
		CommittedRttUs:    committedRtt,
		CommittedJitterUs: meta.CommittedJitterUs,
		BandwidthBps:      meta.BandwidthBps,
		TimeRange:         params.TimeRange,
		BucketSeconds:     bucketSecs,
		BucketCount:       params.BucketCount,
		Buckets:           buckets,
		StatusChanges:     statusChanges,
	}, nil
}

// buildLinkMetricsStatus computes health status for a single bucket.
// Mirrors the logic in fetchSingleLinkHistoryWithParams.
func buildLinkMetricsStatus(
	rollup *linkRollupRow,
	meta *statusLinkMeta,
	committedRtt float64,
	isCollecting bool,
	bucketStart time.Time,
	intfIndex map[linkMetricsSideKey]*interfaceRollupRow,
	currentISISDown bool,
) LinkMetricsStatus {
	isisDown := false
	if rollup != nil {
		isisDown = rollup.ISISDown
	}
	// For the collecting bucket, overlay real-time ISIS state so the timeline
	// reflects adjacency loss immediately rather than waiting for the next
	// rollup materialization.
	if isCollecting && currentISISDown {
		isisDown = true
	}

	provisioning := meta.CommittedRttNs == committedRttProvisioningNs

	// Drain status
	drainStatus := ""
	if rollup != nil && (health.IsDrainedStatus(rollup.Status) || rollup.WasDrained) {
		if health.IsDrainedStatus(rollup.Status) {
			drainStatus = rollup.Status
		} else {
			drainStatus = "soft-drained"
		}
	}

	if rollup == nil || (rollup.ASamples == 0 && rollup.ZSamples == 0) {
		return LinkMetricsStatus{
			Health:       "no_data",
			DrainStatus:  drainStatus,
			Provisioning: provisioning,
			ISISDown:     isisDown,
			Collecting:   isCollecting,
		}
	}

	// Compute combined latency and loss
	totalSamples := rollup.ASamples + rollup.ZSamples
	avgLatency := float64(0)
	if totalSamples > 0 {
		avgLatency = (rollup.AAvgRttUs*float64(rollup.ASamples) + rollup.ZAvgRttUs*float64(rollup.ZSamples)) / float64(totalSamples)
	}
	lossPct := rollup.ALossPct
	if rollup.ZLossPct > lossPct {
		lossPct = rollup.ZLossPct
	}

	statusStr := health.ClassifyLinkStatus(avgLatency, lossPct, committedRtt)

	// One-sided reporting — one side sends probes, the other doesn't.
	// Don't upgrade to unhealthy; transient one-sided is common at bucket
	// boundaries. Keep whatever status the latency/loss classification gave.
	if drainStatus != "hard-drained" && (rollup.ASamples == 0) != (rollup.ZSamples == 0) {
		if isCollecting {
			statusStr = "no_data"
		}
	}

	// Upgrade status based on interface issues (same thresholds as status_rollup_handlers)
	const interfaceUnhealthyThreshold = uint64(100)
	var totalErrors, totalDiscards, totalCarrier uint64
	hasIssues := false

	if a, ok := intfIndex[linkMetricsSideKey{bucketTS: bucketStart, side: "A"}]; ok {
		totalErrors += a.InErrors + a.OutErrors
		totalDiscards += a.InDiscards + a.OutDiscards
		totalCarrier += a.CarrierTransitions
		if a.InErrors > 0 || a.OutErrors > 0 || a.InFcsErrors > 0 || a.InDiscards > 0 || a.OutDiscards > 0 || a.CarrierTransitions > 0 {
			hasIssues = true
		}
		totalErrors += a.InFcsErrors
	}
	if z, ok := intfIndex[linkMetricsSideKey{bucketTS: bucketStart, side: "Z"}]; ok {
		totalErrors += z.InErrors + z.OutErrors
		totalDiscards += z.InDiscards + z.OutDiscards
		totalCarrier += z.CarrierTransitions
		if z.InErrors > 0 || z.OutErrors > 0 || z.InFcsErrors > 0 || z.InDiscards > 0 || z.OutDiscards > 0 || z.CarrierTransitions > 0 {
			hasIssues = true
		}
		totalErrors += z.InFcsErrors
	}

	if totalErrors >= interfaceUnhealthyThreshold || totalDiscards >= interfaceUnhealthyThreshold || totalCarrier >= interfaceUnhealthyThreshold {
		if statusStr == "healthy" || statusStr == "degraded" {
			statusStr = "unhealthy"
		}
	} else if hasIssues && statusStr == "healthy" {
		statusStr = "degraded"
	}

	// Build human-readable reasons
	var reasons []string
	if isisDown {
		reasons = append(reasons, "ISIS down")
	}
	if lossPct >= 25 {
		reasons = append(reasons, fmt.Sprintf("Severe packet loss (%.1f%%)", lossPct))
	} else if lossPct >= 1 {
		reasons = append(reasons, fmt.Sprintf("Moderate packet loss (%.1f%%)", lossPct))
	} else if lossPct > 0 {
		reasons = append(reasons, fmt.Sprintf("Minor packet loss (%.2f%%)", lossPct))
	}
	if committedRtt > 0 && avgLatency > 0 {
		overPct := ((avgLatency - committedRtt) / committedRtt) * 100
		if overPct >= 20 {
			reasons = append(reasons, fmt.Sprintf("High latency (%d%% over SLO)", int(overPct)))
		}
	}
	if hasIssues {
		var parts []string
		if totalErrors > 0 {
			parts = append(parts, fmt.Sprintf("%d interface errors", totalErrors))
		}
		if totalDiscards > 0 {
			parts = append(parts, fmt.Sprintf("%d discards", totalDiscards))
		}
		if totalCarrier > 0 {
			parts = append(parts, fmt.Sprintf("%d carrier transitions", totalCarrier))
		}
		if len(parts) > 0 {
			reasons = append(reasons, strings.Join(parts, ", "))
		}
	}
	if drainStatus != "" && (rollup.ASamples == 0) != (rollup.ZSamples == 0) {
		// One-sided, already handled above
	} else if (rollup.ASamples == 0) != (rollup.ZSamples == 0) {
		reasons = append(reasons, "One-sided reporting")
	}

	return LinkMetricsStatus{
		Health:       statusStr,
		DrainStatus:  drainStatus,
		Provisioning: provisioning,
		ISISDown:     isisDown,
		Collecting:   isCollecting,
		Reasons:      reasons,
	}
}

// buildLinkMetricsTraffic constructs traffic data for a single bucket from the interface index.
func buildLinkMetricsTraffic(
	bucketStart time.Time,
	intfIndex map[linkMetricsSideKey]*interfaceRollupRow,
	bandwidthBps int64,
) *LinkMetricsTraffic {
	a, hasA := intfIndex[linkMetricsSideKey{bucketTS: bucketStart, side: "A"}]
	z, hasZ := intfIndex[linkMetricsSideKey{bucketTS: bucketStart, side: "Z"}]
	if !hasA && !hasZ {
		return nil
	}

	t := &LinkMetricsTraffic{}
	if hasA {
		t.SideAInBps = a.AvgInBps
		t.SideAOutBps = a.AvgOutBps
		t.SideAMaxInBps = a.MaxInBps
		t.SideAMaxOutBps = a.MaxOutBps
		t.SideAInPps = a.AvgInPps
		t.SideAOutPps = a.AvgOutPps
		t.SideAMaxInPps = a.MaxInPps
		t.SideAMaxOutPps = a.MaxOutPps
		t.SideAInErrors = a.InErrors
		t.SideAOutErrors = a.OutErrors
		t.SideAInFcsErrors = a.InFcsErrors
		t.SideAInDiscards = a.InDiscards
		t.SideAOutDiscards = a.OutDiscards
		t.SideACarrierTransitions = a.CarrierTransitions
	}
	if hasZ {
		t.SideZInBps = z.AvgInBps
		t.SideZOutBps = z.AvgOutBps
		t.SideZMaxInBps = z.MaxInBps
		t.SideZMaxOutBps = z.MaxOutBps
		t.SideZInPps = z.AvgInPps
		t.SideZOutPps = z.AvgOutPps
		t.SideZMaxInPps = z.MaxInPps
		t.SideZMaxOutPps = z.MaxOutPps
		t.SideZInErrors = z.InErrors
		t.SideZOutErrors = z.OutErrors
		t.SideZInFcsErrors = z.InFcsErrors
		t.SideZInDiscards = z.InDiscards
		t.SideZOutDiscards = z.OutDiscards
		t.SideZCarrierTransitions = z.CarrierTransitions
	}

	if bandwidthBps > 0 {
		var totalInBps, totalOutBps float64
		if hasA {
			totalInBps += a.AvgInBps
			totalOutBps += a.AvgOutBps
		}
		if hasZ {
			totalInBps += z.AvgInBps
			totalOutBps += z.AvgOutBps
		}
		t.UtilizationInPct = (totalInBps / float64(bandwidthBps)) * 100
		t.UtilizationOutPct = (totalOutBps / float64(bandwidthBps)) * 100
	}

	return t
}

// --- Bulk handler ---

// isDefaultBulkLinkMetricsRequest returns true when the request uses default parameters,
// suitable for serving from the page cache.
// bulkLinkMetricsCacheKey returns the cache key if this request is cacheable, or "" if not.
func bulkLinkMetricsCacheKey(r *http.Request) string {
	q := r.URL.Query()
	inc := q.Get("include")
	rng := q.Get("range")
	incOK := inc == "" || inc == "all" || inc == "status,traffic" || inc == "status"
	if !incOK || (rng != "" && rng != "24h") || q.Get("start_time") != "" || q.Get("end_time") != "" || q.Get("bucket") != "" {
		return ""
	}
	if q.Get("has_issues") == "true" {
		return "bulk_link_metrics_issues"
	}
	return "bulk_link_metrics"
}

// GetBulkLinkMetrics handles GET /api/link-metrics.
// It returns metrics for all links in a single response.
func (a *API) GetBulkLinkMetrics(w http.ResponseWriter, r *http.Request) {
	// Try page cache for default requests
	if cacheKey := bulkLinkMetricsCacheKey(r); cacheKey != "" && isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), cacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	w.Header().Set("X-Cache", "MISS")

	q := r.URL.Query()
	include := parseLinkMetricsInclude(q.Get("include"))

	// Parse time range / custom window
	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}
	ctx, cancel := statusContext(r, 30*time.Second)
	defer cancel()

	var params bucketParams
	now := time.Now().UTC()
	duration := presetToDuration(timeRange)
	startTime := now.Add(-duration)
	params = parseBucketParamsCustom(startTime, now, 24)
	params.TimeRange = timeRange

	resp, err := a.fetchBulkLinkMetrics(ctx, params, include)
	if err != nil {
		slog.Error("error fetching bulk link metrics", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if q.Get("has_issues") == "true" {
		filterBulkLinkMetricsIssuesOnly(resp)
	}

	writeJSON(w, resp)
}

// filterBulkLinkMetricsIssuesOnly removes links that have no issues from the response.
// Keeps links with non-healthy buckets, or that are currently drained/provisioning.
func filterBulkLinkMetricsIssuesOnly(resp *BulkLinkMetricsResponse) {
	for pk, link := range resp.Links {
		keep := false
		for _, b := range link.Buckets {
			if b.Status == nil {
				continue
			}
			if b.Status.DrainStatus != "" || b.Status.Provisioning {
				keep = true
				break
			}
			if b.Status.ISISDown {
				keep = true
				break
			}
			if !b.Status.Collecting && b.Status.Health != "healthy" && b.Status.Health != "" {
				keep = true
				break
			}
		}
		if !keep {
			delete(resp.Links, pk)
		}
	}
}

// FetchBulkLinkMetricsData is the exported entry point for the page cache worker.
func (a *API) FetchBulkLinkMetricsData(ctx context.Context) (*BulkLinkMetricsResponse, error) {
	now := time.Now().UTC()
	duration := presetToDuration("24h")
	startTime := now.Add(-duration)
	params := parseBucketParamsCustom(startTime, now, 24)
	params.TimeRange = "24h"
	include := parseLinkMetricsInclude("status,traffic")
	return a.fetchBulkLinkMetrics(ctx, params, include)
}

// FetchBulkLinkMetricsIssuesData is the page cache variant that only includes links with issues.
func (a *API) FetchBulkLinkMetricsIssuesData(ctx context.Context) (*BulkLinkMetricsResponse, error) {
	resp, err := a.FetchBulkLinkMetricsData(ctx)
	if err != nil {
		return nil, err
	}
	filterBulkLinkMetricsIssuesOnly(resp)
	return resp, nil
}

// fetchBulkLinkMetrics runs parallel queries for ALL links and assembles the bulk response.
func (a *API) fetchBulkLinkMetrics(ctx context.Context, params bucketParams, include linkMetricsInclude) (*BulkLinkMetricsResponse, error) {
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
		metaMap         map[string]*statusLinkMeta
		linkRollupMap   map[linkBucketKey]*linkRollupRow
		intfRows        []interfaceRollupRow
		currentISISDown map[string]bool
	)

	g, gctx := errgroup.WithContext(ctx)

	// Fetch all link metadata (no PK filter)
	g.Go(func() error {
		var err error
		metaMap, err = queryStatusLinkMeta(gctx, db)
		if err != nil {
			return fmt.Errorf("bulk link metadata: %w", err)
		}
		return nil
	})

	// Latency/status rollup for all links (no PK filter)
	if include.Latency || include.Status {
		g.Go(func() error {
			var err error
			linkRollupMap, err = queryLinkRollup(gctx, db, params)
			if err != nil {
				return fmt.Errorf("bulk link rollup: %w", err)
			}
			return nil
		})
	}

	// Real-time ISIS adjacency state for collecting bucket
	if include.Status {
		g.Go(func() error {
			var err error
			currentISISDown, err = queryCurrentISISDown(gctx, db)
			if err != nil {
				slog.Warn("failed to query current ISIS state for bulk", "error", err)
				currentISISDown = nil
			}
			return nil
		})
	}

	// Traffic (interface rollup) for all links (no PK filter)
	if include.Traffic {
		g.Go(func() error {
			var err error
			intfRows, err = queryInterfaceRollup(gctx, db, params, interfaceRollupOpts{
				GroupBy: groupByLinkSide,
			})
			if err != nil {
				return fmt.Errorf("bulk interface rollup: %w", err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Index interface rows by (link_pk, bucket, side)
	bulkIntfIndex := make(map[bulkIntfKey]*interfaceRollupRow)
	for i := range intfRows {
		bk := bulkIntfKey{
			linkPK:   intfRows[i].LinkPK,
			bucketTS: intfRows[i].BucketTS,
			side:     intfRows[i].LinkSide,
		}
		bulkIntfIndex[bk] = &intfRows[i]
	}

	bucketSecs := params.BucketSeconds
	if bucketSecs == 0 {
		bucketSecs = params.BucketMinutes * 60
	}

	// Build per-link responses
	links := make(map[string]*LinkMetricsResponse, len(metaMap))
	for linkPK, meta := range metaMap {
		committedRtt := meta.CommittedRttUs

		// For health classification, only consider latency on inter-metro WAN links
		healthCommittedRtt := committedRtt
		if meta.LinkType != "WAN" || meta.SideAMetro == meta.SideZMetro {
			healthCommittedRtt = 0
		}

		// Extract per-link interface index subset for buildLinkMetricsStatus/Traffic
		perLinkIntfIndex := make(map[linkMetricsSideKey]*interfaceRollupRow)
		for bk, row := range bulkIntfIndex {
			if bk.linkPK == linkPK {
				perLinkIntfIndex[linkMetricsSideKey{bucketTS: bk.bucketTS, side: bk.side}] = row
			}
		}

		// Build buckets
		buckets := make([]LinkMetricsBucket, 0, params.BucketCount)
		for i := params.BucketCount - 1; i >= 0; i-- {
			var bucketStart time.Time
			if params.StartTime != nil {
				bucketStart = params.StartTime.Truncate(bucketDuration).Add(time.Duration(params.BucketCount-1-i) * bucketDuration)
			} else {
				bucketStart = now.Truncate(bucketDuration).Add(-time.Duration(i) * bucketDuration)
			}
			isCollecting := i == 0

			bk := linkBucketKey{LinkPK: linkPK, BucketTS: bucketStart}
			rollup := linkRollupMap[bk]

			bucket := LinkMetricsBucket{
				TS: bucketStart.Format(time.RFC3339),
			}

			if include.Status {
				st := buildLinkMetricsStatus(rollup, meta, healthCommittedRtt, isCollecting, bucketStart, perLinkIntfIndex, currentISISDown[linkPK])
				bucket.Status = &st
			}

			if include.Latency && rollup != nil && (rollup.ASamples > 0 || rollup.ZSamples > 0) {
				bucket.Latency = &LinkMetricsLatency{
					AAvgRttUs: rollup.AAvgRttUs,
					AMinRttUs: rollup.AMinRttUs,
					AP50RttUs: rollup.AP50RttUs,
					AP90RttUs: rollup.AP90RttUs,
					AP95RttUs: rollup.AP95RttUs,
					AP99RttUs: rollup.AP99RttUs,
					AMaxRttUs: rollup.AMaxRttUs,
					ALossPct:  rollup.ALossPct,
					ASamples:  rollup.ASamples,
					ZAvgRttUs: rollup.ZAvgRttUs,
					ZMinRttUs: rollup.ZMinRttUs,
					ZP50RttUs: rollup.ZP50RttUs,
					ZP90RttUs: rollup.ZP90RttUs,
					ZP95RttUs: rollup.ZP95RttUs,
					ZP99RttUs: rollup.ZP99RttUs,
					ZMaxRttUs: rollup.ZMaxRttUs,
					ZLossPct:  rollup.ZLossPct,
					ZSamples:  rollup.ZSamples,

					AAvgJitterUs: rollup.AAvgJitterUs,
					AMinJitterUs: rollup.AMinJitterUs,
					AP50JitterUs: rollup.AP50JitterUs,
					AP90JitterUs: rollup.AP90JitterUs,
					AP95JitterUs: rollup.AP95JitterUs,
					AP99JitterUs: rollup.AP99JitterUs,
					AMaxJitterUs: rollup.AMaxJitterUs,
					ZAvgJitterUs: rollup.ZAvgJitterUs,
					ZMinJitterUs: rollup.ZMinJitterUs,
					ZP50JitterUs: rollup.ZP50JitterUs,
					ZP90JitterUs: rollup.ZP90JitterUs,
					ZP95JitterUs: rollup.ZP95JitterUs,
					ZP99JitterUs: rollup.ZP99JitterUs,
					ZMaxJitterUs: rollup.ZMaxJitterUs,
				}
			}

			if include.Traffic {
				traffic := buildLinkMetricsTraffic(bucketStart, perLinkIntfIndex, meta.BandwidthBps)
				if traffic != nil {
					bucket.Traffic = traffic
				}
			}

			buckets = append(buckets, bucket)
		}

		links[linkPK] = &LinkMetricsResponse{
			LinkPK:            meta.PK,
			LinkCode:          meta.Code,
			LinkType:          meta.LinkType,
			ContributorCode:   meta.Contributor,
			SideAMetro:        meta.SideAMetro,
			SideZMetro:        meta.SideZMetro,
			CommittedRttUs:    committedRtt,
			CommittedJitterUs: meta.CommittedJitterUs,
			BandwidthBps:      meta.BandwidthBps,
			TimeRange:         params.TimeRange,
			BucketSeconds:     bucketSecs,
			BucketCount:       params.BucketCount,
			Buckets:           buckets,
		}
	}

	return &BulkLinkMetricsResponse{
		TimeRange:     params.TimeRange,
		BucketSeconds: bucketSecs,
		BucketCount:   params.BucketCount,
		Links:         links,
	}, nil
}
