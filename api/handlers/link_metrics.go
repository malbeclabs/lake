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
	LinkPK             string               `json:"link_pk"`
	LinkCode           string               `json:"link_code"`
	LinkType           string               `json:"link_type"`
	ContributorCode    string               `json:"contributor_code"`
	SideAMetro         string               `json:"side_a_metro"`
	SideZMetro         string               `json:"side_z_metro"`
	SideADevice        string               `json:"side_a_device"`
	SideZDevice        string               `json:"side_z_device"`
	SideAIfaceName     string               `json:"side_a_iface_name"`
	SideZIfaceName     string               `json:"side_z_iface_name"`
	CommittedRttUs     float64              `json:"committed_rtt_us"`
	CommittedJitterUs  float64              `json:"committed_jitter_us"`
	BandwidthBps       int64                `json:"bandwidth_bps"`
	CurrentDrainStatus string               `json:"current_drain_status"`
	TimeRange          string               `json:"time_range"`
	BucketSeconds      int                  `json:"bucket_seconds"`
	BucketCount        int                  `json:"bucket_count"`
	Buckets            []LinkMetricsBucket  `json:"buckets"`
	StatusChanges      []EntityStatusChange `json:"status_changes,omitempty"`
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
	SideAP50InBps           float64 `json:"side_a_p50_in_bps"`
	SideAP90InBps           float64 `json:"side_a_p90_in_bps"`
	SideAP95InBps           float64 `json:"side_a_p95_in_bps"`
	SideAP99InBps           float64 `json:"side_a_p99_in_bps"`
	SideAMaxInBps           float64 `json:"side_a_max_in_bps"`
	SideAOutBps             float64 `json:"side_a_out_bps"`
	SideAP50OutBps          float64 `json:"side_a_p50_out_bps"`
	SideAP90OutBps          float64 `json:"side_a_p90_out_bps"`
	SideAP95OutBps          float64 `json:"side_a_p95_out_bps"`
	SideAP99OutBps          float64 `json:"side_a_p99_out_bps"`
	SideAMaxOutBps          float64 `json:"side_a_max_out_bps"`
	SideZInBps              float64 `json:"side_z_in_bps"`
	SideZP50InBps           float64 `json:"side_z_p50_in_bps"`
	SideZP90InBps           float64 `json:"side_z_p90_in_bps"`
	SideZP95InBps           float64 `json:"side_z_p95_in_bps"`
	SideZP99InBps           float64 `json:"side_z_p99_in_bps"`
	SideZMaxInBps           float64 `json:"side_z_max_in_bps"`
	SideZOutBps             float64 `json:"side_z_out_bps"`
	SideZP50OutBps          float64 `json:"side_z_p50_out_bps"`
	SideZP90OutBps          float64 `json:"side_z_p90_out_bps"`
	SideZP95OutBps          float64 `json:"side_z_p95_out_bps"`
	SideZP99OutBps          float64 `json:"side_z_p99_out_bps"`
	SideZMaxOutBps          float64 `json:"side_z_max_out_bps"`
	SideAInPps              float64 `json:"side_a_in_pps"`
	SideAP50InPps           float64 `json:"side_a_p50_in_pps"`
	SideAP90InPps           float64 `json:"side_a_p90_in_pps"`
	SideAP95InPps           float64 `json:"side_a_p95_in_pps"`
	SideAP99InPps           float64 `json:"side_a_p99_in_pps"`
	SideAMaxInPps           float64 `json:"side_a_max_in_pps"`
	SideAOutPps             float64 `json:"side_a_out_pps"`
	SideAP50OutPps          float64 `json:"side_a_p50_out_pps"`
	SideAP90OutPps          float64 `json:"side_a_p90_out_pps"`
	SideAP95OutPps          float64 `json:"side_a_p95_out_pps"`
	SideAP99OutPps          float64 `json:"side_a_p99_out_pps"`
	SideAMaxOutPps          float64 `json:"side_a_max_out_pps"`
	SideZInPps              float64 `json:"side_z_in_pps"`
	SideZP50InPps           float64 `json:"side_z_p50_in_pps"`
	SideZP90InPps           float64 `json:"side_z_p90_in_pps"`
	SideZP95InPps           float64 `json:"side_z_p95_in_pps"`
	SideZP99InPps           float64 `json:"side_z_p99_in_pps"`
	SideZMaxInPps           float64 `json:"side_z_max_in_pps"`
	SideZOutPps             float64 `json:"side_z_out_pps"`
	SideZP50OutPps          float64 `json:"side_z_p50_out_pps"`
	SideZP90OutPps          float64 `json:"side_z_p90_out_pps"`
	SideZP95OutPps          float64 `json:"side_z_p95_out_pps"`
	SideZP99OutPps          float64 `json:"side_z_p99_out_pps"`
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
		logError("error fetching link metrics", "error", err, "link_pk", linkPK)
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

	currentDrainStatus := ""
	if health.IsDrainedStatus(meta.Status) {
		currentDrainStatus = meta.Status
	}

	return &LinkMetricsResponse{
		LinkPK:             meta.PK,
		LinkCode:           meta.Code,
		LinkType:           meta.LinkType,
		ContributorCode:    meta.Contributor,
		SideAMetro:         meta.SideAMetro,
		SideZMetro:         meta.SideZMetro,
		SideADevice:        meta.SideADevice,
		SideZDevice:        meta.SideZDevice,
		SideAIfaceName:     meta.SideAIfaceName,
		SideZIfaceName:     meta.SideZIfaceName,
		CommittedRttUs:     committedRtt,
		CommittedJitterUs:  meta.CommittedJitterUs,
		BandwidthBps:       meta.BandwidthBps,
		CurrentDrainStatus: currentDrainStatus,
		TimeRange:          params.TimeRange,
		BucketSeconds:      bucketSecs,
		BucketCount:        params.BucketCount,
		Buckets:            buckets,
		StatusChanges:      statusChanges,
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
		t.SideAP50InBps = a.P50InBps
		t.SideAP90InBps = a.P90InBps
		t.SideAP95InBps = a.P95InBps
		t.SideAP99InBps = a.P99InBps
		t.SideAMaxInBps = a.MaxInBps
		t.SideAOutBps = a.AvgOutBps
		t.SideAP50OutBps = a.P50OutBps
		t.SideAP90OutBps = a.P90OutBps
		t.SideAP95OutBps = a.P95OutBps
		t.SideAP99OutBps = a.P99OutBps
		t.SideAMaxOutBps = a.MaxOutBps
		t.SideAInPps = a.AvgInPps
		t.SideAP50InPps = a.P50InPps
		t.SideAP90InPps = a.P90InPps
		t.SideAP95InPps = a.P95InPps
		t.SideAP99InPps = a.P99InPps
		t.SideAMaxInPps = a.MaxInPps
		t.SideAOutPps = a.AvgOutPps
		t.SideAP50OutPps = a.P50OutPps
		t.SideAP90OutPps = a.P90OutPps
		t.SideAP95OutPps = a.P95OutPps
		t.SideAP99OutPps = a.P99OutPps
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
		t.SideZP50InBps = z.P50InBps
		t.SideZP90InBps = z.P90InBps
		t.SideZP95InBps = z.P95InBps
		t.SideZP99InBps = z.P99InBps
		t.SideZMaxInBps = z.MaxInBps
		t.SideZOutBps = z.AvgOutBps
		t.SideZP50OutBps = z.P50OutBps
		t.SideZP90OutBps = z.P90OutBps
		t.SideZP95OutBps = z.P95OutBps
		t.SideZP99OutBps = z.P99OutBps
		t.SideZMaxOutBps = z.MaxOutBps
		t.SideZInPps = z.AvgInPps
		t.SideZP50InPps = z.P50InPps
		t.SideZP90InPps = z.P90InPps
		t.SideZP95InPps = z.P95InPps
		t.SideZP99InPps = z.P99InPps
		t.SideZMaxInPps = z.MaxInPps
		t.SideZOutPps = z.AvgOutPps
		t.SideZP50OutPps = z.P50OutPps
		t.SideZP90OutPps = z.P90OutPps
		t.SideZP95OutPps = z.P95OutPps
		t.SideZP99OutPps = z.P99OutPps
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

	issuesOnly := q.Get("has_issues") == "true"
	resp, err := a.fetchBulkLinkMetrics(ctx, params, include, issuesOnly)
	if err != nil {
		slog.Error("error fetching bulk link metrics", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Final safety filter: the first-pass is intentionally over-inclusive,
	// so apply the exact Go-level filter to remove any false positives.
	if issuesOnly {
		filterBulkLinkMetricsIssuesOnly(resp)
	}

	writeJSON(w, resp)
}

// filterBulkLinkMetricsIssuesOnly removes links that have no issues from the response.
// Keeps links with non-healthy buckets, or that are currently drained/provisioning.
// Links whose only issue is no_data (e.g., a transient rollup gap) are excluded
// unless they also have degraded/unhealthy buckets — matching the frontend's default filter.
func filterBulkLinkMetricsIssuesOnly(resp *BulkLinkMetricsResponse) {
	for pk, link := range resp.Links {
		hasRealIssue := false
		hasNoData := false
		for _, b := range link.Buckets {
			if b.Status == nil {
				continue
			}
			if b.Status.DrainStatus != "" || b.Status.Provisioning {
				hasRealIssue = true
				break
			}
			if b.Status.ISISDown {
				hasRealIssue = true
				break
			}
			if !b.Status.Collecting && b.Status.Health != "healthy" && b.Status.Health != "" {
				if b.Status.Health == "no_data" {
					hasNoData = true
				} else {
					hasRealIssue = true
					break
				}
			}
		}
		if !hasRealIssue && !hasNoData {
			delete(resp.Links, pk)
			continue
		}
		// Links with only no_data: keep only if they also have degraded/unhealthy buckets
		if !hasRealIssue && hasNoData {
			hasSevere := false
			for _, b := range link.Buckets {
				if b.Status != nil && !b.Status.Collecting &&
					(b.Status.Health == "unhealthy" || b.Status.Health == "degraded") {
					hasSevere = true
					break
				}
			}
			if !hasSevere {
				delete(resp.Links, pk)
			}
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
	return a.fetchBulkLinkMetrics(ctx, params, include, false)
}

// FetchBulkLinkMetricsIssuesData is the page cache variant that only includes links with issues.
// It reuses FetchBulkLinkMetricsData (which fetches all links in a single pass) and post-filters,
// since the page cache worker amortizes the cost and both cache entries share the same query work.
func (a *API) FetchBulkLinkMetricsIssuesData(ctx context.Context) (*BulkLinkMetricsResponse, error) {
	resp, err := a.FetchBulkLinkMetricsData(ctx)
	if err != nil {
		return nil, err
	}
	filterBulkLinkMetricsIssuesOnly(resp)
	return resp, nil
}

// determineIssueLinkPKs identifies which links likely have issues based on
// lightweight first-pass data. Intentionally over-inclusive (false positives OK)
// to avoid missing any true issue links. The caller applies the exact Go-level
// filter afterward.
func determineIssueLinkPKs(
	metaMap map[string]*statusLinkMeta,
	rollupSummary map[string]*linkRollupSummary,
	intfIssuePKs map[string]bool,
	currentISISDown map[string]bool,
	params bucketParams,
) map[string]bool {
	result := make(map[string]bool)

	for pk, meta := range metaMap {
		// Drained or provisioning links always count as having issues.
		if health.IsDrainedStatus(meta.Status) || meta.CommittedRttNs == committedRttProvisioningNs {
			result[pk] = true
			continue
		}

		// Current ISIS down.
		if currentISISDown[pk] {
			result[pk] = true
			continue
		}

		summary, inRollup := rollupSummary[pk]

		// Any issue indicator from the rollup.
		if inRollup && (summary.AnyISISDown || summary.AnyDrained) {
			result[pk] = true
			continue
		}
		if inRollup && (summary.MaxALossPct > 0 || summary.MaxZLossPct > 0) {
			result[pk] = true
			continue
		}

		// Latency overage check (over-inclusive: skips inter-metro WAN filter).
		if inRollup {
			committedRttUs := meta.CommittedRttUs
			if committedRttUs > 0 {
				if summary.MaxAAvgRttUs > committedRttUs*1.2 || summary.MaxZAvgRttUs > committedRttUs*1.2 {
					result[pk] = true
					continue
				}
			}
		}
	}

	// Interface errors/discards/carrier transitions.
	for pk := range intfIssuePKs {
		result[pk] = true
	}

	return result
}

// fetchBulkLinkMetrics runs parallel queries and assembles the bulk response.
// When issuesOnly is true, it runs a lightweight first pass to identify links
// with issues, then fetches detailed data only for those links.
func (a *API) fetchBulkLinkMetrics(ctx context.Context, params bucketParams, include linkMetricsInclude, issuesOnly bool) (*BulkLinkMetricsResponse, error) {
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

	// When issuesOnly, run lightweight first-pass queries to identify which
	// links have issues, then run the expensive rollup queries only for those.
	var issuePKSet map[string]bool
	if issuesOnly {
		var (
			rollupSummary  map[string]*linkRollupSummary
			intfIssuePKSet map[string]bool
		)

		g, gctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			var err error
			metaMap, err = queryStatusLinkMeta(gctx, db)
			if err != nil {
				return fmt.Errorf("bulk link metadata: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			var err error
			rollupSummary, err = queryLinkRollupSummary(gctx, db, params)
			if err != nil {
				return fmt.Errorf("link rollup summary: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			var err error
			intfIssuePKSet, err = queryInterfaceIssueLinkPKs(gctx, db, params)
			if err != nil {
				return fmt.Errorf("interface issue link PKs: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			var err error
			currentISISDown, err = queryCurrentISISDown(gctx, db)
			if err != nil {
				slog.Warn("failed to query current ISIS state for bulk", "error", err)
				currentISISDown = nil
			}
			return nil
		})

		if err := g.Wait(); err != nil {
			return nil, err
		}

		// Determine which links have issues.
		issuePKSet = determineIssueLinkPKs(metaMap, rollupSummary, intfIssuePKSet, currentISISDown, params)

		if len(issuePKSet) == 0 {
			bucketSecs := params.BucketSeconds
			if bucketSecs == 0 {
				bucketSecs = params.BucketMinutes * 60
			}
			return &BulkLinkMetricsResponse{
				TimeRange:     params.TimeRange,
				BucketSeconds: bucketSecs,
				BucketCount:   params.BucketCount,
				Links:         make(map[string]*LinkMetricsResponse),
			}, nil
		}

		issuePKSlice := make([]string, 0, len(issuePKSet))
		for pk := range issuePKSet {
			issuePKSlice = append(issuePKSlice, pk)
		}

		// Phase 2: run full rollup queries filtered to issue links only.
		g2, gctx2 := errgroup.WithContext(ctx)

		if include.Latency || include.Status {
			g2.Go(func() error {
				var err error
				linkRollupMap, err = queryLinkRollup(gctx2, db, params, issuePKSlice...)
				if err != nil {
					return fmt.Errorf("bulk link rollup: %w", err)
				}
				return nil
			})
		}

		if include.Traffic {
			g2.Go(func() error {
				var err error
				intfRows, err = queryInterfaceRollup(gctx2, db, params, interfaceRollupOpts{
					GroupBy: groupByLinkSide,
					LinkPKs: issuePKSlice,
				})
				if err != nil {
					return fmt.Errorf("bulk interface rollup: %w", err)
				}
				return nil
			})
		}

		if err := g2.Wait(); err != nil {
			return nil, err
		}
	} else {
		// Original path: fetch everything for all links.
		g, gctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			var err error
			metaMap, err = queryStatusLinkMeta(gctx, db)
			if err != nil {
				return fmt.Errorf("bulk link metadata: %w", err)
			}
			return nil
		})

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

	// Build per-link responses. When issuesOnly, only assemble links in the
	// issue set; other links were not fetched in the rollup queries.
	linkCount := len(metaMap)
	if len(issuePKSet) > 0 {
		linkCount = len(issuePKSet)
	}
	links := make(map[string]*LinkMetricsResponse, linkCount)
	for linkPK, meta := range metaMap {
		if len(issuePKSet) > 0 && !issuePKSet[linkPK] {
			continue
		}
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

		currentDrainStatus := ""
		if health.IsDrainedStatus(meta.Status) {
			currentDrainStatus = meta.Status
		}

		links[linkPK] = &LinkMetricsResponse{
			LinkPK:             meta.PK,
			LinkCode:           meta.Code,
			LinkType:           meta.LinkType,
			ContributorCode:    meta.Contributor,
			SideAMetro:         meta.SideAMetro,
			SideZMetro:         meta.SideZMetro,
			SideADevice:        meta.SideADevice,
			SideZDevice:        meta.SideZDevice,
			SideAIfaceName:     meta.SideAIfaceName,
			SideZIfaceName:     meta.SideZIfaceName,
			CommittedRttUs:     committedRtt,
			CommittedJitterUs:  meta.CommittedJitterUs,
			BandwidthBps:       meta.BandwidthBps,
			CurrentDrainStatus: currentDrainStatus,
			TimeRange:          params.TimeRange,
			BucketSeconds:      bucketSecs,
			BucketCount:        params.BucketCount,
			Buckets:            buckets,
		}
	}

	return &BulkLinkMetricsResponse{
		TimeRange:     params.TimeRange,
		BucketSeconds: bucketSecs,
		BucketCount:   params.BucketCount,
		Links:         links,
	}, nil
}
