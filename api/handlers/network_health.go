package handlers

// Network Health Reporting: a public, windowed report of how the DoubleZero
// network performed over a time window (default: last 30 days), network-wide.
//
// Design notes (see plans/network-health-dashboard/SPEC.md):
//   - Facts only. This endpoint returns raw measurements; it does not score,
//     rank, or judge contributors. The frontend must present them neutrally.
//     One deliberate exception: NHTickets.SelfReportedPct (the share of a
//     contributor's own incidents that the contributor filed themselves) may be
//     graded good/warn/bad on that contributor's OWN view. It is the
//     contributor's own self-fact about their own incidents, shown only on their
//     own page (never a cross-contributor leaderboard), so it does not judge one
//     contributor against another.
//   - Safety: every query carries an inline SETTINGS cap and the whole fetch
//     runs under a context deadline. Time bounds are passed as bound parameters
//     (time.Time), never string-interpolated. Rollup/percentile values are
//     already clean rates, so no counter-wrap guard is needed there; the raw
//     fact table (traffic bytes) keeps the out_octets_delta > 0 guard.
//   - The default (30d) view is precomputed by the worker into page_cache and
//     served on HIT; custom windows compute live under the deadline.

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	// The Network Health page is split into independent data-source groups, each
	// with its OWN page_cache key so it loads and caches on its own. Each suffix is
	// bumped independently when that one group's payload shape changes, so a stale
	// blob from an older shape is never served (it would crash the newer frontend).
	// The bare route /api/network-health serves the Overview group (see
	// GetNetworkHealth), so its key stands in for the old monolith key.
	NetworkHealthOverviewCacheKey     = "network_health_overview_30d_v2"
	NetworkHealthAvailabilityCacheKey = "network_health_availability_30d_v1"
	NetworkHealthLatencyCacheKey      = "network_health_latency_30d_v1"
	NetworkHealthCapacityCacheKey     = "network_health_capacity_30d_v1"
	NetworkHealthOutagesCacheKey      = "network_health_outages_30d_v1"
	NetworkHealthDrainCacheKey        = "network_health_drain_30d_v2"
	NetworkHealthTicketsCacheKey      = "network_health_tickets_30d_v2"
	NetworkHealthImpactfulCacheKey    = "network_health_impactful_30d_v1"
	// NetworkHealthDeferredCacheKey is the page_cache key for the default 30d
	// deferred payload (the slow undrain/recovery-health figures, split out of the
	// main page so the main page loads without waiting on the heavy scan).
	NetworkHealthDeferredCacheKey = "network_health_deferred_30d_v2"
	// NetworkHealthDefaultDays is the default (and cached) window length.
	NetworkHealthDefaultDays = 30

	networkHealthMaxDays = 92
	// nhOpsTicketsMaxLookback bounds how far back the ops-management pagination
	// walks. The ops API is newest-first with no date filter, so a window
	// positioned in the past is paged through everything newer than it. The widest
	// window the page can request ending now is networkHealthMaxDays plus its
	// equal-length prior window (184 days), so this floor leaves every reachable
	// window untouched and refuses only windows that would page the whole stream.
	nhOpsTicketsMaxLookback = 200 * 24 * time.Hour

	// nhOpsUsersCacheTTL bounds staleness of the memoized ops /users registry (up
	// to 20 paged requests per call). It maps user pubkey -> contributor, changes
	// on human timescales, and is re-fetched by the cache worker every refresh
	// cycle (30s), so a few minutes of staleness is invisible and removes ~90% of
	// the calls.
	nhOpsUsersCacheTTL = 5 * time.Minute

	// nhOpsUsersFetchTimeout bounds the detached miss-path registry fetch so a
	// collapsed run cannot hang after the winning caller disconnects (see
	// cachedOpsUsers).
	nhOpsUsersFetchTimeout = 20 * time.Second

	// max_memory_usage caps each query at ~2 GiB so a single heavy query errors
	// (and degrades to an empty panel via nhGo) instead of breaching the server
	// total memory limit and taking down sibling queries. Public-page safety.
	// max_bytes_before_external_group_by / max_bytes_before_external_sort let a
	// big GROUP BY or ORDER BY spill to disk well before that cap, so a heavy
	// 30d scan over device_interface_rollup_5m/link_rollup_5m (sorted by
	// bucket_ts first, so a per-entity filter still scans the whole range)
	// finishes slower instead of erroring with code 241 ("memory limit
	// exceeded ... while reading column device_pk/link_pk").
	// max_execution_time = 55 is chosen against two different callers of these
	// queries: the page-cache worker's refresh() wraps each cache-refresh call
	// in its own 60s context (api/worker/workflow.go), so 55s leaves headroom
	// for the worker to finish every panel on one refresh instead of leaving it
	// empty until a later tick; live requests (contributor scope, see
	// GetNetworkHealth) are bounded by their own ~35s request context, which
	// cancels the ClickHouse query client-side well before 55s, so raising this
	// shared setting does not relax anything on that path.
	networkHealthQuerySettings = " SETTINGS max_execution_time = 55, max_memory_usage = 2000000000, max_bytes_before_external_group_by = 1000000000, max_bytes_before_external_sort = 1000000000, timeout_before_checking_execution_speed = 0"

	// networkHealthDeferredQuerySettings is networkHealthQuerySettings with a much
	// higher max_execution_time (170s) for the deferred recovery-health scan. That
	// scan (fetchRecoveryHealth over link_rollup_5m) is the one query that could
	// exceed the main page's 35s/55s budget, so it is served from its own deferred
	// endpoint (GetNetworkHealthDeferred) under a 170s request deadline. The memory
	// cap and spill settings are unchanged (still 2 GiB), so this only buys time,
	// never more memory.
	networkHealthDeferredQuerySettings = " SETTINGS max_execution_time = 170, max_memory_usage = 2000000000, max_bytes_before_external_group_by = 1000000000, max_bytes_before_external_sort = 1000000000, timeout_before_checking_execution_speed = 0"

	// throughputMaxThreads caps parallelism on the one query that dedups the
	// whole user-traffic slice of device_interface_rollup_5m (~14.4M rows over
	// 30d, one dedup group per row). At default parallelism each thread buffers
	// its own native-stream read of that slice and the query breaches the 2 GB
	// cap at the source read (code 241, SourceFromNativeStream); 4 threads keeps
	// the concurrent read buffers small enough to finish under the cap (measured
	// ~40 ms vs OOM). Appended after networkHealthQuerySettings so the later
	// max_threads wins. This is the scan the network peak is derived from and the
	// one that renders the throughput time series, so it fixes both.
	throughputMaxThreads = ", max_threads = 4"
)

// nhPanels records which panel queries failed inside one group's errgroup so the
// group reports a partial payload instead of publishing a failed query's zero.
// A panel named in Degraded renders as unavailable on the page; a critical panel
// additionally sets the group's Error, which tells the cache worker to keep the
// last good blob.
type nhPanels struct {
	mu       sync.Mutex
	failed   []string
	critical bool
}

// fail records one failed panel. critical marks a panel whose absence makes the
// whole group's figures misleading rather than merely incomplete.
func (p *nhPanels) fail(panel string, critical bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed = append(p.failed, panel)
	if critical {
		p.critical = true
	}
}

// list returns the failed panel names sorted, or nil when none failed.
func (p *nhPanels) list() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.failed) == 0 {
		return nil
	}
	out := append([]string(nil), p.failed...)
	sort.Strings(out)
	return out
}

// criticalFailed reports whether any critical panel failed.
func (p *nhPanels) criticalFailed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.critical
}

// nhGo runs one panel query concurrently as best-effort. If it fails or times out
// (e.g. a heavy scan hitting max_execution_time on the remote proxy) the panel is
// recorded on p and the goroutine returns nil, so a single slow or broken panel
// degrades to a panel marked unavailable instead of cancelling every sibling query
// and blanking the whole report. The line logs at WARN: the alert for a sustained
// failure is owned by the cache worker's escalator (api/worker/workflow.go), which
// is the layer that knows the refresh cadence.
func nhGo(g *errgroup.Group, p *nhPanels, panel string, fn func() error) {
	nhRun(g, p, panel, false, fn)
}

// nhGoCritical is nhGo for a panel whose failure also sets the group's Error, so
// the payload reads as unavailable rather than as a real zero and the cache
// worker keeps the last good blob.
func nhGoCritical(g *errgroup.Group, p *nhPanels, panel string, fn func() error) {
	nhRun(g, p, panel, true, fn)
}

func nhRun(g *errgroup.Group, p *nhPanels, panel string, critical bool, fn func() error) {
	g.Go(func() error {
		if err := fn(); err != nil {
			p.fail(panel, critical)
			logWarn("network health panel failed", "panel", panel, "critical", critical, "error", err)
		}
		return nil
	})
}

// The Network Health page is served as independent data-source groups, each its
// own endpoint + cache blob (see the *CacheKey constants and the Get* handlers).
// Every group is a subset of the old monolith payload; nothing is dropped and no
// field is owned by more than one group. Each group carries the resolved window
// (so the frontend can label it), a generated_at stamp, the names of any panels
// that could not be computed (Degraded), and an optional error.
//
// Degraded names a panel whose query failed, so the frontend renders that panel
// as unavailable instead of drawing a failed query's zero as a real measurement.
// Error is set when a CRITICAL panel of the group failed (see nhGoCritical), on
// the scoped path as well as the network-wide one; the cache worker turns it into
// a refreshError so the last good blob is kept instead of caching zeros — see the
// worker entries in api/worker/workflow.go.

// NHOverview is the Overview group: the headline signals plus trend context. It
// deliberately does NOT own the outage/impactful headline tiles (those come from
// the Outages and Impactful groups so their heavy scans never block Overview);
// the frontend headline row cross-reads all three. Contributors is the list that
// backs the filter-bar dropdown (code + footprint only, no outage breakdown).
type NHOverview struct {
	Window       NHWindow        `json:"window"`
	Headline     NHHeadline      `json:"headline"`
	Isis         NHIsis          `json:"isis"`
	Freshness    NHFreshness     `json:"freshness"`
	Throughput   []NHTsPoint     `json:"throughput_ts"`
	Contributors []NHContributor `json:"contributors"`
	GeneratedAt  string          `json:"generated_at"`
	Degraded     []string        `json:"degraded,omitempty"`
	Error        string          `json:"error,omitempty"`
}

// NHAvailabilityGroup is the Availability group (no deltas).
type NHAvailabilityGroup struct {
	Window             NHWindow         `json:"window"`
	LinkAvailability   []NHAvailability `json:"link_availability"`
	DeviceAvailability []NHAvailability `json:"device_availability"`
	GeneratedAt        string           `json:"generated_at"`
	Degraded           []string         `json:"degraded,omitempty"`
	Error              string           `json:"error,omitempty"`
}

// NHLatencyGroup is the Latency group: committed-vs-measured link latency plus
// the committed-RTT (SLA) adherence tile (no deltas).
type NHLatencyGroup struct {
	Window       NHWindow     `json:"window"`
	LatencyLinks []NHPerfLink `json:"latency_links"`
	Sla          NHSla        `json:"sla"`
	GeneratedAt  string       `json:"generated_at"`
	Degraded     []string     `json:"degraded,omitempty"`
	Error        string       `json:"error,omitempty"`
}

// NHCapacityGroup is the Capacity group. Capacity is the network SEAT-utilization
// summary; CapacityLinks is the fullest-links (bandwidth) capacity-planning
// panel — two different concepts (no deltas).
type NHCapacityGroup struct {
	Window        NHWindow         `json:"window"`
	Capacity      NHCapacity       `json:"capacity"`
	DeviceSlots   []NHDeviceSlots  `json:"device_slots"`
	DiaInterfaces []NHDiaInterface `json:"dia_interfaces"`
	TopLinks      []NHTrafficLink  `json:"top_links"`
	CapacityLinks []NHCapacityLink `json:"capacity_links"`
	GeneratedAt   string           `json:"generated_at"`
	Degraded      []string         `json:"degraded,omitempty"`
	Error         string           `json:"error,omitempty"`
}

// NHOutagesGroup is the Outages group: the reliability episode scan (which also
// feeds the headline OutageCount tile + its delta), outage summary, downtime
// rankings, outages-over-time, and interface-error hotspots. Prev carries the
// prior-window values for the reliability + outage-summary deltas. The
// per-contributor footprint lives in the Overview group (the scope dropdown's
// source), not here.
type NHOutagesGroup struct {
	Window           NHWindow         `json:"window"`
	Reliability      NHReliability    `json:"reliability"`
	OutageCount      uint64           `json:"outage_count"`
	OutageCountDelta *float64         `json:"outage_count_delta"`
	OutageSummary    *NHOutageSummary `json:"outage_summary"`
	DowntimeLinks    []NHDowntimeRow  `json:"downtime_links"`
	DowntimeDevices  []NHDowntimeRow  `json:"downtime_devices"`
	OutagesOverTime  []NHCountPoint   `json:"outages_ts"`
	ErrorHotspots    []NHErrorHotspot `json:"error_hotspots"`
	Prev             *NHOutagesPrev   `json:"prev"`
	GeneratedAt      string           `json:"generated_at"`
	Degraded         []string         `json:"degraded,omitempty"`
	Error            string           `json:"error,omitempty"`
}

// NHOutagesPrev is the prior-window reliability + outage-summary values for the
// Outages group's deltas.
type NHOutagesPrev struct {
	Reliability   NHReliabilityPrev `json:"reliability"`
	OutageSummary *NHOutageSummary  `json:"outage_summary"`
}

// NHDrainGroup is the Drain group: the pure-Go drain/undrain timing figures. The
// undrain (recovery-health) figures are NOT here — that heavy scan stays on the
// /deferred endpoint. Prev carries the prior-window figures for the deltas.
type NHDrainGroup struct {
	Window      NHWindow       `json:"window"`
	DrainTiming NHDrainTiming  `json:"drain_timing"`
	Prev        *NHDrainTiming `json:"prev"`
	GeneratedAt string         `json:"generated_at"`
	Degraded    []string       `json:"degraded,omitempty"`
	Error       string         `json:"error,omitempty"`
}

// NHTicketsGroup is the Tickets group: the public ops-management aggregate for
// the current and prior windows (split from one union fetch over the ops API).
type NHTicketsGroup struct {
	Window      NHWindow   `json:"window"`
	OpsTickets  *NHTickets `json:"ops_tickets"`
	Prev        *NHTickets `json:"prev"`
	GeneratedAt string     `json:"generated_at"`
	Degraded    []string   `json:"degraded,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// NHImpactful is the Impactful group (its own heavy endpoint, 170s live deadline,
// like /deferred): the impact-weighted downtime headline tile + its delta. The
// frontend derives availability_pct from Overview.ActiveLinks + this figure.
type NHImpactful struct {
	Window                 NHWindow         `json:"window"`
	ImpactfulDowntimeHours float64          `json:"impactful_downtime_hours"`
	ImpactfulDowntimeDelta *float64         `json:"impactful_downtime_delta"`
	Prev                   *NHImpactfulPrev `json:"prev"`
	Unavailable            bool             `json:"unavailable"`
	GeneratedAt            string           `json:"generated_at"`
	Degraded               []string         `json:"degraded,omitempty"`
	Error                  string           `json:"error,omitempty"`
}

// NHImpactfulPrev is the prior-window impactful-downtime value for the delta.
type NHImpactfulPrev struct {
	ImpactfulDowntimeHours float64 `json:"impactful_downtime_hours"`
}

// NHReliabilityPrev is the prior-window reliability figures (a subset of
// NHReliability) used to compute deltas on the reliability section.
type NHReliabilityPrev struct {
	OutageCount         uint64  `json:"outage_count"`
	CappedDowntimeHours float64 `json:"capped_downtime_hours"`
}

// NHTickets is the PUBLIC ops-management aggregate. Text-free by construction:
// only numbers and the fixed enums (severity, type). No ticket titles,
// descriptions, reporter identities, contributor pubkeys, or root-cause text
// ever appear here. It is produced by the Tickets group
// (FetchNetworkHealthTicketsData): on its own endpoint the external ops-API
// pagination is capped to 20s and never blocks another panel, so unlike the old
// monolith the network-wide live path no longer skips it. A contributor-scoped
// view filters the aggregate to that contributor's tickets.
//
// Timing (response/resolution) is computed over INCIDENTS ONLY. Maintenance is
// scheduled ahead of time, so a created-before-start offset would drag those
// medians negative; maintenance instead gets its own lead/duration figures.
type NHTickets struct {
	Total       int `json:"total"`
	Incidents   int `json:"incidents"`
	Maintenance int `json:"maintenance"`
	Sev1        int `json:"sev1"`
	Sev2        int `json:"sev2"`
	Sev3        int `json:"sev3"`

	ResponseP50Min   *int `json:"response_p50_min"`   // incident start -> ticket created
	ResolutionP50Min *int `json:"resolution_p50_min"` // incident start -> ticket end (closed only)
	ClosedIncidents  int  `json:"closed_incidents"`

	// Self-reported classification (incidents only). An incident is
	// self-reported when its creator (user_pubkey) is a contributor user, and
	// DoubleZero-filed when the creator is DoubleZero staff or the creator is
	// unknown. SelfReportedPct is the share of incidents the contributor filed
	// themselves; it is nil when the /users registry could not be fetched (shown
	// as "unavailable" rather than mislabeling every incident as DoubleZero).
	SelfReportedCount    int      `json:"self_reported_count"`
	DoubleZeroFiledCount int      `json:"doublezero_filed_count"`
	SelfReportedPct      *float64 `json:"self_reported_pct"`

	// Maintenance-only timing. Lead = scheduled start - created (how far ahead it
	// was announced); Duration = end - start (how long the window ran, closed
	// set includes "completed"). No time-to-file for maintenance.
	MaintenanceLeadP50Min     *int `json:"maintenance_lead_p50_min"`
	MaintenanceDurationP50Min *int `json:"maintenance_duration_p50_min"`
	ClosedMaintenance         int  `json:"closed_maintenance"`

	OutageCount       int      `json:"outage_count"`
	OutagesWithTicket int      `json:"outages_with_ticket"`
	OutagesNoTicket   int      `json:"outages_no_ticket"`
	NoTicketSharePct  *float64 `json:"no_ticket_share_pct"`

	// NoTicketOutages is the actionable list behind OutagesNoTicket: the
	// telemetry-derived outages with no matching ops ticket, sorted by duration
	// descending and capped to the longest few. Facts only (link code + start +
	// duration hours); no ticket free text, since by definition these have no
	// ticket. Lets an operator see WHICH outages went unfiled, not just how many.
	NoTicketOutages []NHNoTicketOutage `json:"no_ticket_outages"`

	// RootCauses is the incident root-cause breakdown over the window: one row per
	// fixed root-cause enum token plus "other" for anything outside that set,
	// sorted by count descending. nhRootCauseTokens is the token list; do not
	// restate it here, so the two cannot drift. Enum tokens only, so the text-free
	// public contract holds.
	RootCauses []NHRootCauseCount `json:"root_causes"`
}

// NHRootCauseCount is one incident root-cause enum's share of incidents that
// have a recorded cause. Cause is a fixed enum token (never free text).
type NHRootCauseCount struct {
	Cause string   `json:"cause"`
	Count int      `json:"count"`
	Pct   *float64 `json:"pct"`
}

// nhRootCauseTokens is the fixed root-cause enum published by
// /network-health/tickets: the eleven snake_case tokens the ops-management API
// emits. The root_cause field is operator-entered upstream, so anything outside
// this set is counted under nhRootCauseOther rather than published verbatim on a
// public page.
var nhRootCauseTokens = map[string]bool{
	"self_resolved": true, "network_external": true, "fiber_cut": true,
	"configuration": true, "hardware": true, "carrier": true, "false_positive": true,
	"duplicate": true, "software": true, "dz_managed": true, "human_error": true,
}

// nhRootCauseOther is the catch-all bucket for an unrecognised upstream cause.
const nhRootCauseOther = "other"

// nhRootCause maps an upstream root_cause to a published enum token.
// Unrecognised values collapse to nhRootCauseOther so the incident still counts
// toward the breakdown denominator without its text reaching the public payload.
func nhRootCause(raw string) string {
	t := strings.ToLower(strings.TrimSpace(raw))
	if nhRootCauseTokens[t] {
		return t
	}
	return nhRootCauseOther
}

// NHNoTicketOutage is one telemetry-derived outage that has no matching ops
// ticket. Facts only: the affected link's code, the outage start, and its
// duration in hours. No ticket free text (there is no ticket).
type NHNoTicketOutage struct {
	LinkPK   string  `json:"link_pk"`
	LinkCode string  `json:"link_code"`
	StartTs  string  `json:"start_ts"`
	Hours    float64 `json:"hours"`
}

// NHSla is committed-RTT adherence: links (with a committed RTT) whose observed
// average RTT stayed within their onchain committed value over the window.
type NHSla struct {
	Within    uint64   `json:"within"`
	Total     uint64   `json:"total"`
	WithinPct *float64 `json:"within_pct"`
}

// NHCapacity is network seat utilization (users vs configured capacity).
type NHCapacity struct {
	UnicastUsers uint64   `json:"unicast_users"`
	MaxUsers     uint64   `json:"max_users"`
	UtilPct      *float64 `json:"util_pct"`
}

// NHDeviceSlots is one device's seat usage (unicast + multicast sub/pub) vs its
// total configured max_users. Per-role maxes (max_unicast_users,
// max_multicast_*) are 0/unset on most devices, so max_users (the device's
// single overall cap) is the denominator; UsedPct is nil when max_users is 0.
type NHDeviceSlots struct {
	PK       string   `json:"pk"`
	Code     string   `json:"code"`
	Unicast  uint64   `json:"unicast"`
	McastSub uint64   `json:"mcast_sub"`
	McastPub uint64   `json:"mcast_pub"`
	MaxUsers uint64   `json:"max_users"`
	UsedPct  *float64 `json:"used_pct"`
}

// NHDiaInterface is one Direct Internet Access interface's provisioned
// capacity (port speed, committed rate) vs observed traffic (p50/p99 out).
// UtilPct is measured against interface bandwidth (physical port speed), so
// Denom is always "port"; CirGbps is kept as an informational figure only.
type NHDiaInterface struct {
	DevicePK string   `json:"device_pk"`
	Device   string   `json:"device"`
	Intf     string   `json:"intf"`
	PortGbps float64  `json:"port_gbps"`
	CirGbps  float64  `json:"cir_gbps"`
	P50Gbps  float64  `json:"p50_gbps"`
	P99Gbps  float64  `json:"p99_gbps"`
	UtilPct  *float64 `json:"util_pct"`
	Denom    string   `json:"denom"` // "cir" or "port"
}

// NHIsis is a control-plane health tile.
type NHIsis struct {
	Overloaded  uint64 `json:"overloaded"`
	Unreachable uint64 `json:"unreachable"`
	Devices     uint64 `json:"devices"`
	Adjacencies uint64 `json:"adjacencies"`
}

// NHFreshness lets a viewer date the data (a trust signal). Coverage is measured
// relative to the feed's own latest timestamp, not wall-clock.
//
// LagSeconds is computed when the panel runs and served verbatim from the cached
// blob, so it understates real lag by however old that blob is — up to the
// overview entry's refresh cadence (the worker's networkHealthOverviewInterval).
// Fine for a coverage signal measured in hours; not a source for anything that
// needs the lag to the second.
type NHFreshness struct {
	FeedMax       string `json:"feed_max"`
	LagSeconds    int64  `json:"lag_seconds"`
	DevicesFresh  uint64 `json:"devices_fresh"`
	DevicesActive uint64 `json:"devices_active"`
}

// NHTsPoint is one throughput time-series point (bits/sec).
type NHTsPoint struct {
	T      string  `json:"t"`
	AvgBps float64 `json:"avg_bps"`
	MaxBps float64 `json:"max_bps"`
}

// NHCountPoint is one count time-series point (e.g. outages started per bucket).
type NHCountPoint struct {
	T     string `json:"t"`
	Count uint64 `json:"count"`
}

// NHTrafficLink is a link ranked by traffic carried. Traffic is a neutral fact.
type NHTrafficLink struct {
	LinkPK     string  `json:"link_pk"`
	LinkCode   string  `json:"link_code"`
	SideAMetro string  `json:"side_a_metro"`
	SideZMetro string  `json:"side_z_metro"`
	Status     string  `json:"status"`
	AvgGbps    float64 `json:"avg_gbps"`
	MaxGbps    float64 `json:"max_gbps"`
}

// NHCapacityLink is a link's peak utilization against its provisioned bandwidth
// (a capacity-planning signal: which links are closest to full).
type NHCapacityLink struct {
	LinkPK        string  `json:"link_pk"`
	LinkCode      string  `json:"link_code"`
	SideAMetro    string  `json:"side_a_metro"`
	SideZMetro    string  `json:"side_z_metro"`
	BandwidthGbps float64 `json:"bandwidth_gbps"`
	PeakGbps      float64 `json:"peak_gbps"`
	UtilPct       float64 `json:"util_pct"`
	P50Util       float64 `json:"p50_util"`
	P99Util       float64 `json:"p99_util"`
}

// NHPerfLink compares a link's onchain committed RTT to its measured RTT over
// the window (the network's own latency promise). Facts, not pass/fail.
type NHPerfLink struct {
	LinkPK           string  `json:"link_pk"`
	LinkCode         string  `json:"link_code"`
	SideAMetro       string  `json:"side_a_metro"`
	SideZMetro       string  `json:"side_z_metro"`
	CommittedMs      float64 `json:"committed_ms"`
	MeasuredAvgMs    float64 `json:"measured_avg_ms"`
	MeasuredMaxMs    float64 `json:"measured_max_ms"`
	OverCommittedPct float64 `json:"over_committed_pct"`
	DriftMs          float64 `json:"drift_ms"`
	DriftPct         float64 `json:"drift_pct"`
	// RawCommittedMs is the onchain committed RTT before any delay override.
	// Overridden is true when an IS-IS delay override is set, in which case
	// CommittedMs (and the drift/over figures) reflect the override (the value
	// the network actually routes on), not the raw onchain commitment.
	RawCommittedMs float64 `json:"raw_committed_ms"`
	Overridden     bool    `json:"overridden"`
}

// NHAvailability is one entity's time-based 3-way state split over the window,
// summing to 100% of the (non-provisioning) window: Available (activated and
// up), Drained (soft/hard drained: intentional, not a fault), and Outage
// (activated but isis_down or high loss: the fault downtime that reconciles
// with the downtime/incidents metric). Drain counts as unavailable but is
// shown as its own segment rather than folded into "outage", so a heavily
// soft-drained link doesn't read as if it had a fault. A fact, not a score.
type NHAvailability struct {
	PK           string  `json:"pk"`
	Code         string  `json:"code"`
	Metros       string  `json:"metros,omitempty"` // links only: "ams ↔ lon"
	AvailPct     float64 `json:"avail_pct"`        // 100*avail/(avail+drained+outage)
	DrainedPct   float64 `json:"drained_pct"`
	OutagePct    float64 `json:"outage_pct"`
	AvailHours   float64 `json:"avail_hours"`   // avail_buckets * 5 / 60
	OutageHours  float64 `json:"outage_hours"`  // outage_buckets * 5 / 60
	DrainedHours float64 `json:"drained_hours"` // drained_buckets * 5 / 60
}

// NHErrorHotspot is a device with interface errors/discards or carrier flaps
// (an early warning of degrading optics/fiber before an outage).
type NHErrorHotspot struct {
	DevicePK     string `json:"device_pk"`
	DeviceCode   string `json:"device_code"`
	Errors       uint64 `json:"errors"`
	CarrierFlaps uint64 `json:"carrier_flaps"`
}

// NHDrainTiming reports how link drains/undrains played out over the window.
// Plain measurements (minutes); no targets, no pass/fail (see spec 1.1).
type NHDrainTiming struct {
	OutageCount         int      `json:"outage_count"`
	EventsWithDrain     int      `json:"events_with_drain"`
	Drains              int      `json:"drains"`
	Undrains            int      `json:"undrains"`
	TimeToDrainP50Min   *float64 `json:"time_to_drain_p50_min"`
	TimeToDrainMaxMin   *float64 `json:"time_to_drain_max_min"`
	TimeDrainedP50Min   *float64 `json:"time_drained_p50_min"`
	TimeDrainedMaxMin   *float64 `json:"time_drained_max_min"`
	TimeToUndrainP50Min *float64 `json:"time_to_undrain_p50_min"`
	TimeToUndrainMaxMin *float64 `json:"time_to_undrain_max_min"`
	DrainWithin30mPct   *float64 `json:"drain_within_30m_pct"`
	// MatchedUndrains is the number of drain->undrain pairs found. When 0 the
	// undrain-timing figures are absent because nothing was paired (not a
	// failure). UndrainUnavailable is set when the recovery-health query failed,
	// so the frontend shows "unavailable" rather than a bare dash.
	MatchedUndrains    int  `json:"matched_undrains"`
	UndrainUnavailable bool `json:"undrain_unavailable"`
}

// NHDeferred is the deferred (slow) portion of the drain-timing panel: the
// recovery-health-derived "time to undrain a healthy link" figures. These come
// from fetchRecoveryHealth, the one drain-timing query heavy enough to exceed
// the main page's deadline, so they are served from GetNetworkHealthDeferred
// (a separate endpoint under a longer 170s deadline) instead of blocking the
// main payload. Prev holds the prior-window figures for the section delta.
type NHDeferred struct {
	TimeToUndrainP50Min *float64    `json:"time_to_undrain_p50_min"`
	TimeToUndrainMaxMin *float64    `json:"time_to_undrain_max_min"`
	UndrainUnavailable  bool        `json:"undrain_unavailable"`
	MatchedUndrains     int         `json:"matched_undrains"`
	Prev                *NHDeferred `json:"prev,omitempty"`
	Degraded            []string    `json:"degraded,omitempty"`
	Error               string      `json:"error,omitempty"`
}

// nhDrainMatch is a matched drain->undrain interval for one link, used to find
// when the link recovered (came back healthy) before it was undrained.
type nhDrainMatch struct {
	linkPK    string
	drainTS   time.Time
	undrainTS time.Time
}

// nhEvent is one reconstructed link-down incident (isis_down run on an activated link).
type nhEvent struct {
	linkPK string
	start  time.Time
	end    time.Time
}

// nhChange is one link status transition.
type nhChange struct {
	linkPK string
	prev   string
	next   string
	ts     time.Time
}

type NHWindow struct {
	Start string `json:"start"`
	End   string `json:"end"`
	Days  int    `json:"days"`
	Label string `json:"label"`
}

// NHHeadline carries only the figures the Overview group owns. The outage and
// impactful-downtime headline tiles are owned by the Outages and Impactful
// groups (see the NHOverview doc comment), so they are not mirrored here.
type NHHeadline struct {
	PeakBps          float64  `json:"peak_bps"`
	JitterImprovePct float64  `json:"jitter_improve_pct"`
	DzLossPct        float64  `json:"dz_loss_pct"`
	ActiveLinks      uint64   `json:"active_links"`
	ActiveDevices    uint64   `json:"active_devices"`
	ActiveMetros     uint64   `json:"active_metros"`
	Deltas           NHDeltas `json:"deltas"`
}

// NHDeltas are percentage changes versus the previous equal-length window.
// nil means the prior value was zero/unavailable (no delta shown).
type NHDeltas struct {
	PeakBps     *float64 `json:"peak_bps"`
	ActiveLinks *float64 `json:"active_links"`
}

type NHReliability struct {
	OutageCount         uint64         `json:"outage_count"`
	DistinctLinks       uint64         `json:"distinct_links"`
	CappedDowntimeHours float64        `json:"capped_downtime_hours"`
	DegradedLinks       uint64         `json:"degraded_links"`
	DurationHistogram   NHDurationHist `json:"duration_histogram"`
}

// NHDurationHist buckets outages by duration. Descriptive only.
type NHDurationHist struct {
	FlapLE5m        uint64 `json:"flap_le5m"`
	Short5to15m     uint64 `json:"short_5_15m"`
	Medium15to60m   uint64 `json:"medium_15_60m"`
	Sustained1to24h uint64 `json:"sustained_1_24h"`
	ChronicGt24h    uint64 `json:"chronic_gt24h"`
}

// NHContributor is a plain per-contributor fact row. No score, no rank.
type NHContributor struct {
	Code                string  `json:"code"`
	Outages             uint64  `json:"outages"`
	CappedDowntimeHours float64 `json:"capped_downtime_hours"`
	DistinctLinks       uint64  `json:"distinct_links"`
	Links               uint64  `json:"links"`
	Devices             uint64  `json:"devices"`
}

// NHOutageSummary is the network-wide (or contributor-scoped) outage total for
// the window, split by entity kind. OutageHours is the link outage-hours total
// (uncapped; a fact, not the headline's capped figure).
type NHOutageSummary struct {
	LinkOutages     uint64  `json:"link_outages"`
	OutageHours     float64 `json:"outage_hours"`
	LinksAffected   uint64  `json:"links_affected"`
	DeviceOutages   uint64  `json:"device_outages"`
	DevicesAffected uint64  `json:"devices_affected"`
}

// NHDowntimeRow is one entity's outage-hours total over the window, ranked by
// Hours descending. PK lets the frontend link through to the entity's
// drill-down page. Metros holds the metro pair for links ("ams ↔ lon") or the
// single metro for devices.
type NHDowntimeRow struct {
	PK      string  `json:"pk"`
	Code    string  `json:"code"`
	Metros  string  `json:"metros,omitempty"`
	Outages uint64  `json:"outages"`
	Hours   float64 `json:"hours"`
}

// nhGroupDeadline is the live-compute deadline for the fast groups (cache MISS
// or a non-default window). Matches the old monolith's 35s request budget; each
// group is a strict subset so it finishes well inside it. The two heavy groups
// use nhHeavyGroupDeadline instead.
const nhGroupDeadline = 35 * time.Second

// nhHeavyGroupDeadline is the live-compute deadline for the two heavy groups
// (impactful, deferred). Their queries run under max_execution_time = 170 (see
// networkHealthDeferredQuerySettings), so the request budget matches.
const nhHeavyGroupDeadline = 170 * time.Second

// nhLiveComputeSem bounds concurrent LIVE (cache-miss or non-default-window)
// Network Health computes across all group handlers to 4, matching the worker's
// pageCacheRefreshConcurrency, so a burst of uncached public requests cannot
// oversubscribe ClickHouse. It is acquired ONLY on the live-compute paths of the
// HTTP handlers; cache HITs never touch it, and the worker calls Fetch*Data
// directly (bypassing these handlers) so it is excluded too.
var nhLiveComputeSem = make(chan struct{}, 4)

// nhAcquireLive reserves a live-compute slot, waiting for one to free rather
// than shedding immediately. A scoped page load fires ~9 group requests at once
// and every one computes live (scoped views are never precomputed); with only 4
// slots, an immediate-shed policy 503'd the requests that lost the race, so the
// UI showed "Couldn't load" on ~5 of 9 panels. Waiting for a slot instead lets
// them queue and fill in progressively as the running queries finish. The wait
// is bounded by ctx (the caller passes the group deadline), so a request still
// sheds if its deadline expires while queued (genuine sustained overload) or if
// the client disconnected. On success it returns a release func and true; on
// failure it returns false and the caller must return 503 without launching a
// query. Release must be deferred.
func nhAcquireLive(ctx context.Context) (func(), bool) {
	select {
	case nhLiveComputeSem <- struct{}{}:
		return func() { <-nhLiveComputeSem }, true
	case <-ctx.Done():
		return nil, false
	}
}

// nhWriteLiveUnavailable writes the fixed 503 used when the live-compute
// concurrency limit is saturated (no query is launched).
func nhWriteLiveUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Retry-After", "5")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":"network health data temporarily unavailable"}`))
}

// serveNHGroup collapses the shared handler body for every Network Health group
// endpoint: resolve the window, then serve the network-wide precomputed blob on
// a cache HIT (default window, mainnet), else compute live under the deadline.
// Contributor-scoped views are never precomputed and always compute live. fetch
// computes this group's payload for the resolved window + scope.
func (a *API) serveNHGroup(
	w http.ResponseWriter, r *http.Request,
	defaultKey string,
	fetch func(ctx context.Context, start, end time.Time, contrib string) any,
	deadline time.Duration,
) {
	start, end, cacheable := networkHealthWindow(
		r.URL.Query().Get("start"),
		r.URL.Query().Get("end"),
		r.URL.Query().Get("days"),
	)

	writeLive := func(contrib string) {
		// One deadline covers the whole live operation: waiting for a slot plus
		// the query. Concurrent ClickHouse queries are still bounded to 4, but
		// waiters queue for a free slot instead of shedding on contention, so a
		// single page load's ~9 concurrent group requests fill in progressively
		// rather than 503-ing each other. Only a request whose deadline expires
		// while queued (sustained overload) or whose client disconnected is shed.
		ctx, cancel := context.WithTimeout(r.Context(), deadline)
		defer cancel()
		release, ok := nhAcquireLive(ctx)
		if !ok {
			nhWriteLiveUnavailable(w)
			return
		}
		defer release()
		w.Header().Set("X-Cache", "MISS")
		writeJSON(w, fetch(ctx, start, end, contrib))
	}

	// Contributor scope re-scopes the whole one-pager and is never precomputed
	// (see api/worker/workflow.go), so it always computes live. The code is a
	// bound parameter (no interpolation).
	if code := r.URL.Query().Get("contributor"); code != "" {
		writeLive(code)
		return
	}

	// Default view is precomputed by the worker; serve from cache (mainnet only).
	if cacheable && isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), defaultKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	writeLive("")
}

// GetNetworkHealth serves the Overview group. The bare /api/network-health route
// is repurposed to Overview (backward-compatible); the other groups have their
// own /api/network-health/<group> routes. Public endpoint.
func (a *API) GetNetworkHealth(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthOverviewCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthOverviewData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthAvailability serves the Availability group. Public endpoint.
func (a *API) GetNetworkHealthAvailability(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthAvailabilityCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthAvailabilityData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthLatency serves the Latency group. Public endpoint.
func (a *API) GetNetworkHealthLatency(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthLatencyCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthLatencyData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthCapacity serves the Capacity group. Public endpoint.
func (a *API) GetNetworkHealthCapacity(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthCapacityCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthCapacityData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthOutages serves the Outages group. Public endpoint.
func (a *API) GetNetworkHealthOutages(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthOutagesCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthOutagesData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthDrain serves the Drain group. Public endpoint.
func (a *API) GetNetworkHealthDrain(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthDrainCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthDrainData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthTickets serves the Tickets group. Unlike the old monolith's
// network-wide live path (which skipped the ops-API fetch to stay under its
// deadline), this dedicated endpoint always fetches: the external pagination is
// capped to 20s internally and no longer blocks any other panel. Public endpoint.
func (a *API) GetNetworkHealthTickets(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthTicketsCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthTicketsData(ctx, start, end, contrib)
		}, nhGroupDeadline)
}

// GetNetworkHealthImpactful serves the Impactful group. Like /deferred it runs
// its heavy scan live under a longer 170s deadline so it never blocks the fast
// groups. Public endpoint.
func (a *API) GetNetworkHealthImpactful(w http.ResponseWriter, r *http.Request) {
	a.serveNHGroup(w, r, NetworkHealthImpactfulCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthImpactfulData(ctx, start, end, contrib)
		}, nhHeavyGroupDeadline)
}

// GetNetworkHealthDeferred serves the deferred (slow undrain) portion of the
// Network Health drain-timing panel. It is split from GetNetworkHealth so the
// main page loads without waiting on the heavy recovery-health scan; the
// frontend fetches this endpoint separately and fills in the undrain figures
// when they arrive. Public endpoint.
func (a *API) GetNetworkHealthDeferred(w http.ResponseWriter, r *http.Request) {
	a.serveNHDeferred(w, r, nhHeavyGroupDeadline)
}

// serveNHDeferred is GetNetworkHealthDeferred with the live-compute deadline as
// a parameter, so tests can exercise the queue wait without waiting out the real
// budget. The body is serveNHGroup's, under the longer heavy-group deadline.
func (a *API) serveNHDeferred(w http.ResponseWriter, r *http.Request, deadline time.Duration) {
	a.serveNHGroup(w, r, NetworkHealthDeferredCacheKey,
		func(ctx context.Context, start, end time.Time, contrib string) any {
			return a.FetchNetworkHealthDeferredData(ctx, start, end, contrib)
		}, deadline)
}

// networkHealthWindow resolves the reporting window. If start+end are both given
// (date 'YYYY-MM-DD' or RFC3339) it uses that custom range (clamped to
// networkHealthMaxDays); otherwise it uses `days` (default 30) ending at the most
// recent whole UTC day. cacheable is true only for the exact default view, so
// custom ranges never read or write the shared cache.
func networkHealthWindow(startStr, endStr, daysStr string) (time.Time, time.Time, bool) {
	if startStr != "" && endStr != "" {
		s, sok := parseNHTime(startStr)
		e, eok := parseNHTime(endStr)
		if sok && eok && e.After(s) {
			maxSpan := time.Duration(networkHealthMaxDays) * 24 * time.Hour
			if e.Sub(s) > maxSpan {
				s = e.Add(-maxSpan)
			}
			return s, e, false
		}
	}
	days := NetworkHealthDefaultDays
	if v, err := strconv.Atoi(daysStr); err == nil {
		days = v
	}
	if days < 1 {
		days = 1
	}
	if days > networkHealthMaxDays {
		days = networkHealthMaxDays
	}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -days)
	return start, end, days == NetworkHealthDefaultDays
}

// DefaultNetworkHealthWindow is the window the cache worker precomputes.
//
// Both boundaries are day-aligned, so the window moves once per day and excludes
// everything since midnight. Within a day, the only thing that can change a
// purely-historical group's answer is a late-arriving rollup row or advancing
// FINAL dedup state — which is why those groups refresh on a cadence rather than
// every cycle (see the worker's networkHealthHistoryInterval).
func DefaultNetworkHealthWindow() (time.Time, time.Time) {
	end := time.Now().UTC().Truncate(24 * time.Hour)
	return end.AddDate(0, 0, -NetworkHealthDefaultDays), end
}

func parseNHTime(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func nhWindowLabel(start, end time.Time, days int) string {
	if days == 7 || days == NetworkHealthDefaultDays {
		return "Last " + strconv.Itoa(days) + " days"
	}
	return start.Format("2006-01-02") + " to " + end.Format("2006-01-02")
}

// nhWindow builds the resolved reporting window struct for a group payload.
func nhWindow(start, end time.Time) NHWindow {
	const layout = "2006-01-02 15:04:05"
	days := int(math.Round(end.Sub(start).Hours() / 24))
	return NHWindow{
		Start: start.Format(layout),
		End:   end.Format(layout),
		Days:  days,
		Label: nhWindowLabel(start, end, days),
	}
}

// nhPriorWindow returns the previous equal-length window (priorStart, priorEnd)
// ending exactly where the current window begins, for the "vs prior" deltas.
func nhPriorWindow(start, end time.Time) (time.Time, time.Time) {
	return start.Add(-end.Sub(start)), start
}

// fetchContribLinkPKs resolves the link pks owned by contrib for scoping the
// drain-timing scans to one contributor's links. The bool reports whether the
// scans should run at all, and the error separates a failed lookup from a
// contributor that genuinely owns no links:
//   - contrib == ""             -> (nil, true, nil):   network-wide, no link filter.
//   - contrib set, links found  -> (pks, true, nil):   filter to those link pks.
//   - contrib set, zero links   -> (nil, false, nil):  scoped to nothing; caller must
//     return an empty result, NOT fall back to a network-wide scan.
//   - contrib set, query error  -> (nil, false, err):  cannot scope safely; caller
//     must report the failure instead of publishing that empty result as a fact.
//
// The false case is what keeps a scoped request from silently leaking network-wide
// data when the contributor owns no links or the pk lookup fails.
func (a *API) fetchContribLinkPKs(ctx context.Context, contrib string) ([]string, bool, error) {
	if contrib == "" {
		return nil, true, nil
	}
	rows, err := a.envDB(ctx).Query(ctx,
		`SELECT pk FROM dz_links_current WHERE contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)`+networkHealthQuerySettings, contrib)
	if err != nil {
		logWarn("network health: resolve contributor link pks failed", "error", err, "contributor", contrib)
		return nil, false, err
	}
	var pks []string
	for rows.Next() {
		var pk string
		if rows.Scan(&pk) == nil {
			pks = append(pks, pk)
		}
	}
	rows.Close()
	if len(pks) == 0 {
		return nil, false, nil
	}
	return pks, true, nil
}

// fetchImpactfulDowntime computes impact-weighted downtime (hours) over [s, e):
// the downtime on links that were carrying traffic when they went down. This is
// the heaviest query in the report, so it lives on its own /impactful endpoint
// (like /deferred) under a 170s deadline and uses networkHealthDeferredQuerySettings
// (max_execution_time = 170). contrib scopes it to one contributor's links.
//
// Impactful = the link was carrying traffic when it went down: either an
// attributable link (has interface-counter rows) with pre-outage bps >= 1Mbps,
// or an unattributable link (no rows at all, e.g. a port-channel subinterface
// whose counters land on a different link_pk) that is the primary (lowest
// committed-RTT) path for its metro-pair, i.e. the one actually carrying traffic.
func (a *API) fetchImpactfulDowntime(ctx context.Context, s, e time.Time, contrib string) (float64, error) {
	scoped := contrib != ""
	// Bounded pre-outage scan (preserves the exact "traffic in the 60 min before
	// the outage" semantics without an unbounded 30-day LEFT JOIN): the outage
	// episodes come from link_rollup_5m (~a few hundred links), and both
	// device_interface_rollup_5m scans are pruned to those outage links (olinks)
	// and to the data-derived pre-outage span, so the correlated 60-min-pre-window
	// join runs over a tiny slice. The episodes come from the shared outage
	// definition (nhOutEpisodeCTE), so this headline figure counts the same
	// failures as the outage and drain panels.
	q := nhOutEpisodeCTE(scoped, false) + `,
	outages AS (
		SELECT link_pk, started_at, least(dur_s, 86400) AS capped
		FROM epi WHERE dur_s >= ` + nhSustainedOutageSQL + `),
	olinks AS (SELECT DISTINCT link_pk FROM outages),
	span AS (SELECT min(started_at) - INTERVAL 60 MINUTE AS pre_lo, max(started_at) AS pre_hi FROM outages),
	ri AS (
		SELECT link_pk, bucket_ts, avg_in_bps + avg_out_bps AS bps
		FROM device_interface_rollup_5m
		WHERE link_pk IN (SELECT link_pk FROM olinks)
		  AND bucket_ts >= (SELECT pre_lo FROM span) AND bucket_ts < (SELECT pre_hi FROM span)),
	attributable AS (
		-- Links with at least one interface-counter row in the window, pruned to
		-- the outage links. "Unattributed" is decided by this set rather than by
		-- pre_bps IS NULL, because the LEFT JOIN below fills unmatched rows with
		-- ClickHouse zero-values, not NULL.
		SELECT DISTINCT link_pk FROM device_interface_rollup_5m
		WHERE link_pk IN (SELECT link_pk FROM olinks)
		  AND bucket_ts >= ? AND bucket_ts < ?),
	bps AS (
		SELECT o.link_pk AS link_pk, o.started_at AS started_at, any(o.capped) AS capped,
		       if(o.link_pk IN (SELECT link_pk FROM attributable), max(r.bps), NULL) AS pre_bps
		FROM outages o
		LEFT JOIN ri r ON r.link_pk = o.link_pk
			AND r.bucket_ts >= o.started_at - INTERVAL 60 MINUTE AND r.bucket_ts < o.started_at
		GROUP BY o.link_pk, o.started_at),
	link_metrics AS (
		-- Inter-metro activated links, weighted like the isis_ksp topology graph:
		-- isis_delay_override_ns when set, else committed_rtt_ns (1000000000 is the
		-- "unmeasured" sentinel the ksp graph also excludes). Same-metro/DZX links
		-- are left out here and fall back to the attributable rule above.
		SELECT
			l.pk AS link_pk,
			if(da.metro_pk < dz.metro_pk, da.metro_pk, dz.metro_pk) AS metro_lo,
			if(da.metro_pk < dz.metro_pk, dz.metro_pk, da.metro_pk) AS metro_hi,
			if(l.isis_delay_override_ns > 0, l.isis_delay_override_ns, l.committed_rtt_ns) AS metric_ns
		FROM dz_links_current l
		JOIN dz_devices_current da ON l.side_a_pk = da.pk
		JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		WHERE l.status = 'activated'
		  AND l.committed_rtt_ns != 1000000000
		  AND da.metro_pk != '' AND dz.metro_pk != ''
		  AND da.metro_pk != dz.metro_pk
	),
	primary_links AS (
		-- Lowest-metric link(s) per metro-pair: the active path carrying traffic.
		SELECT link_pk FROM (
			SELECT link_pk, metric_ns,
			       min(metric_ns) OVER (PARTITION BY metro_lo, metro_hi) AS min_metric_ns
			FROM link_metrics
		) WHERE metric_ns = min_metric_ns
	)
	SELECT
		round(sumIf(capped, pre_bps >= 1000000
			OR (pre_bps IS NULL AND link_pk IN (SELECT link_pk FROM primary_links))) / 3600, 1) AS impactful_h
	FROM bps` + networkHealthDeferredQuerySettings
	impArgs := []any{s, e}
	if scoped {
		impArgs = append(impArgs, contrib)
	}
	impArgs = append(impArgs, s, e)
	var impactful float64
	if err := a.envDB(ctx).QueryRow(ctx, q, impArgs...).Scan(&impactful); err != nil {
		return 0, err
	}
	return nzFloat(impactful), nil
}

// FetchNetworkHealthOverviewData computes the Overview group: headline signals
// (traffic, peak, latency-vs-internet, active counts + their deltas), IS-IS,
// telemetry freshness, the throughput time series, and the contributor list that
// backs the filter dropdown. Reuses the existing per-metric fetchers unchanged.
// Called by the handler (cache miss) and the cache-refresh worker.
//
// A panel added here is not free: latency_vs_internet and the throughput series
// are ~95% of this group's ClickHouse cost, and this group alone is the most
// expensive thing the page cache refreshes. The two point-in-time panels
// (freshness, isis) are what keeps the group on a short cadence at all; the rest
// reads the day-aligned window (see DefaultNetworkHealthWindow) and does not
// change between refreshes.
func (a *API) FetchNetworkHealthOverviewData(ctx context.Context, start, end time.Time, contrib string) *NHOverview {
	priorStart, priorEnd := nhPriorWindow(start, end)
	scoped := contrib != ""
	intervalSec := nhIntervalSeconds(start, end)

	resp := &NHOverview{
		Window:       nhWindow(start, end),
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Throughput:   []NHTsPoint{},
		Contributors: []NHContributor{},
	}

	cHist := ""
	if scoped {
		cHist = " AND cpk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	withC := func(args ...any) []any {
		if scoped {
			return append(args, contrib)
		}
		return args
	}

	var priorPeak float64
	var priorLinks uint64
	linksByCode := map[string]uint64{}
	devicesByCode := map[string]uint64{}
	// throughput_ts is the only headline-critical panel in this group (peak +
	// trend); everything else degrades to an unavailable panel.
	panels := &nhPanels{}

	g := new(errgroup.Group)
	g.SetLimit(10)

	// --- Headline: active counts as-of window end ---
	nhGo(g, panels, "active_devices", func() error {
		q := `SELECT count() FROM (
			SELECT pk, argMax(status, snapshot_ts) AS st, argMax(is_deleted, snapshot_ts) AS del, argMax(contributor_pk, snapshot_ts) AS cpk
			FROM dim_dz_devices_history WHERE snapshot_ts <= ? GROUP BY pk
		) WHERE st = 'activated' AND del = 0` + cHist + networkHealthQuerySettings
		return a.scanUint(ctx, &resp.Headline.ActiveDevices, q, withC(end)...)
	})
	nhGo(g, panels, "active_links", func() error {
		q := `SELECT count() FROM (
			SELECT pk, argMax(status, snapshot_ts) AS st, argMax(is_deleted, snapshot_ts) AS del, argMax(contributor_pk, snapshot_ts) AS cpk
			FROM dim_dz_links_history WHERE snapshot_ts <= ? GROUP BY pk
		) WHERE st = 'activated' AND del = 0` + cHist + networkHealthQuerySettings
		return a.scanUint(ctx, &resp.Headline.ActiveLinks, q, withC(end)...)
	})
	if !scoped {
		nhGo(g, panels, "metros", func() error {
			q := `SELECT count() FROM (
				SELECT pk, argMax(is_deleted, snapshot_ts) AS del
				FROM dim_dz_metros_history WHERE snapshot_ts <= ? GROUP BY pk
			) WHERE del = 0` + networkHealthQuerySettings
			return a.scanUint(ctx, &resp.Headline.ActiveMetros, q, end)
		})
	}
	nhGo(g, panels, "active_links_prior", func() error {
		q := `SELECT count() FROM (
			SELECT pk, argMax(status, snapshot_ts) AS st, argMax(is_deleted, snapshot_ts) AS del, argMax(contributor_pk, snapshot_ts) AS cpk
			FROM dim_dz_links_history WHERE snapshot_ts <= ? GROUP BY pk
		) WHERE st = 'activated' AND del = 0` + cHist + networkHealthQuerySettings
		return a.scanUint(ctx, &priorLinks, q, withC(priorEnd)...)
	})

	// --- Headline: peak throughput (current from the trend scan; prior for delta) ---
	nhGoCritical(g, panels, "throughput_ts", func() error {
		ts, peak, err := a.fetchThroughputTS(ctx, start, end, intervalSec, contrib)
		if err != nil {
			return err
		}
		resp.Throughput = ts
		resp.Headline.PeakBps = peak
		return nil
	})
	nhGo(g, panels, "peak_prior", func() error {
		_, peak, err := a.fetchThroughputTS(ctx, priorStart, priorEnd, nhIntervalSeconds(priorStart, priorEnd), contrib)
		if err != nil {
			return err
		}
		priorPeak = peak
		return nil
	})

	// --- Headline: latency improvement vs internet (point-in-time, network-only) ---
	if !scoped {
		nhGo(g, panels, "latency_vs_internet", func() error {
			q := `SELECT
				round(avgIf(jitter_improvement_pct, origin_metro != '' AND target_metro != ''), 2),
				round(avgIf(dz_loss_pct, origin_metro != '' AND target_metro != ''), 2)
				FROM dz_vs_internet_latency_comparison` + networkHealthQuerySettings
			var jitter, loss float64
			if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&jitter, &loss); err != nil {
				return err
			}
			resp.Headline.JitterImprovePct = nzFloat(jitter)
			resp.Headline.DzLossPct = nzFloat(loss)
			return nil
		})

		// --- IS-IS + telemetry freshness (point-in-time, network-only) ---
		nhGo(g, panels, "isis", func() error {
			q := `SELECT countIf(overload = 1), countIf(node_unreachable = 1), count()
			FROM isis_devices_current` + networkHealthQuerySettings
			if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&resp.Isis.Overloaded, &resp.Isis.Unreachable, &resp.Isis.Devices); err != nil {
				return err
			}
			return a.envDB(ctx).QueryRow(ctx, `SELECT count() FROM isis_adjacencies_current`+networkHealthQuerySettings).Scan(&resp.Isis.Adjacencies)
		})
		nhGo(g, panels, "freshness", func() error {
			var feedMax time.Time
			if err := a.envDB(ctx).QueryRow(ctx,
				`SELECT max(event_ts), toInt64(dateDiff('second', max(event_ts), now()))
			 FROM fact_dz_device_interface_counters`+networkHealthQuerySettings,
			).Scan(&feedMax, &resp.Freshness.LagSeconds); err != nil {
				return err
			}
			resp.Freshness.FeedMax = feedMax.UTC().Format(time.RFC3339)
			q := `SELECT countIf(last >= ? - INTERVAL 1 HOUR), count() FROM (
			SELECT device_pk, max(event_ts) AS last FROM fact_dz_device_interface_counters
			WHERE event_ts >= ? - INTERVAL 2 HOUR GROUP BY device_pk
		)` + networkHealthQuerySettings
			return a.envDB(ctx).QueryRow(ctx, q, feedMax, feedMax).Scan(
				&resp.Freshness.DevicesFresh, &resp.Freshness.DevicesActive)
		})

		// --- Contributor list (footprint only) for the filter dropdown. The
		// per-contributor OUTAGE breakdown lives in the Outages group. ---
		nhGo(g, panels, "contributors", func() error {
			q := `SELECT c.code, count()
			FROM dz_links_current AS l
			LEFT JOIN dz_contributors_current AS c ON c.pk = l.contributor_pk
			WHERE l.status = 'activated'
			GROUP BY c.code` + networkHealthQuerySettings
			return a.scanCodeCount(ctx, linksByCode, q)
		})
		nhGo(g, panels, "contributor_devices", func() error {
			q := `SELECT c.code, count()
			FROM dz_devices_current AS d
			LEFT JOIN dz_contributors_current AS c ON c.pk = d.contributor_pk
			WHERE d.status = 'activated'
			GROUP BY c.code` + networkHealthQuerySettings
			return a.scanCodeCount(ctx, devicesByCode, q)
		})
	}

	_ = g.Wait()

	// Deltas vs prior window (peak/active-links; outage/impactful deltas are owned
	// by the Outages/Impactful groups).
	resp.Headline.Deltas = NHDeltas{
		PeakBps:     pctDelta(resp.Headline.PeakBps, priorPeak),
		ActiveLinks: pctDelta(float64(resp.Headline.ActiveLinks), float64(priorLinks)),
	}

	// Contributor list = union of codes seen in the footprint queries.
	if !scoped {
		seen := map[string]bool{}
		for code := range linksByCode {
			seen[code] = true
		}
		for code := range devicesByCode {
			seen[code] = true
		}
		for code := range seen {
			if code == "" {
				continue
			}
			resp.Contributors = append(resp.Contributors, NHContributor{
				Code:    code,
				Links:   linksByCode[code],
				Devices: devicesByCode[code],
			})
		}
	}

	resp.Degraded = panels.list()
	if panels.criticalFailed() {
		// Generic client-facing message; the raw DB error is logged, not exposed.
		resp.Error = "network health overview data temporarily unavailable"
	}
	return resp
}

// FetchNetworkHealthAvailabilityData computes the Availability group: the least-
// available link and device rankings (no deltas).
func (a *API) FetchNetworkHealthAvailabilityData(ctx context.Context, start, end time.Time, contrib string) *NHAvailabilityGroup {
	resp := &NHAvailabilityGroup{
		Window:             nhWindow(start, end),
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		LinkAvailability:   []NHAvailability{},
		DeviceAvailability: []NHAvailability{},
	}

	panels := &nhPanels{}
	g := new(errgroup.Group)
	g.SetLimit(10)
	nhGo(g, panels, "link_availability", func() error {
		rows, err := a.fetchLinkAvailability(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.LinkAvailability = rows
		return nil
	})
	nhGo(g, panels, "device_availability", func() error {
		rows, err := a.fetchDeviceAvailability(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.DeviceAvailability = rows
		return nil
	})
	_ = g.Wait()
	resp.Degraded = panels.list()
	return resp
}

// FetchNetworkHealthLatencyData computes the Latency group: committed-vs-measured
// per-link latency and the committed-RTT (SLA) adherence tile (no deltas).
func (a *API) FetchNetworkHealthLatencyData(ctx context.Context, start, end time.Time, contrib string) *NHLatencyGroup {
	scoped := contrib != ""
	resp := &NHLatencyGroup{
		Window:       nhWindow(start, end),
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		LatencyLinks: []NHPerfLink{},
	}

	cLinkCur := ""
	if scoped {
		cLinkCur = " AND l.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	withC := func(args ...any) []any {
		if scoped {
			return append(args, contrib)
		}
		return args
	}

	panels := &nhPanels{}
	g := new(errgroup.Group)
	g.SetLimit(10)
	nhGo(g, panels, "latency_links", func() error {
		rows, err := a.fetchLatencyLinks(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.LatencyLinks = rows
		return nil
	})
	nhGo(g, panels, "sla", func() error {
		q := `SELECT countIf(observed_us <= committed_us), count() FROM (
			SELECT l.pk AS pk, l.committed_rtt_ns / 1000.0 AS committed_us,
				avg((r.a_avg_rtt_us + r.z_avg_rtt_us) / 2) AS observed_us
			FROM dz_links_current l
			INNER JOIN link_rollup_5m r ON r.link_pk = l.pk
			WHERE l.committed_rtt_ns > 0 AND l.status = 'activated'
			  AND r.bucket_ts >= ? AND r.bucket_ts < ?
			  AND r.status = 'activated' AND r.isis_down = 0` + cLinkCur + `
			GROUP BY l.pk, committed_us
		)` + networkHealthQuerySettings
		if err := a.envDB(ctx).QueryRow(ctx, q, withC(start, end)...).Scan(&resp.Sla.Within, &resp.Sla.Total); err != nil {
			return err
		}
		resp.Sla.WithinPct = pctPtr(int(resp.Sla.Within), int(resp.Sla.Total))
		return nil
	})
	_ = g.Wait()
	resp.Degraded = panels.list()
	return resp
}

// FetchNetworkHealthCapacityData computes the Capacity group: the network seat-
// utilization summary, fullest devices by seat, DIA interfaces, top links by
// traffic, and fullest links by bandwidth (no deltas).
func (a *API) FetchNetworkHealthCapacityData(ctx context.Context, start, end time.Time, contrib string) *NHCapacityGroup {
	scoped := contrib != ""
	resp := &NHCapacityGroup{
		Window:        nhWindow(start, end),
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DeviceSlots:   []NHDeviceSlots{},
		DiaInterfaces: []NHDiaInterface{},
		TopLinks:      []NHTrafficLink{},
		CapacityLinks: []NHCapacityLink{},
	}

	cDevBare := ""
	if scoped {
		cDevBare = " AND contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	withC := func(args ...any) []any {
		if scoped {
			return append(args, contrib)
		}
		return args
	}

	panels := &nhPanels{}
	g := new(errgroup.Group)
	g.SetLimit(10)
	// --- Seat capacity utilization (point-in-time) ---
	nhGo(g, panels, "seat_capacity", func() error {
		q := `SELECT sum(unicast_users_count), sum(max_users)
			FROM dz_devices_current WHERE status = 'activated'` + cDevBare + networkHealthQuerySettings
		var users uint64
		var maxUsers int64
		if err := a.envDB(ctx).QueryRow(ctx, q, withC()...).Scan(&users, &maxUsers); err != nil {
			return err
		}
		resp.Capacity.UnicastUsers = users
		if maxUsers > 0 {
			resp.Capacity.MaxUsers = uint64(maxUsers)
			p := math.Round(float64(users)/float64(maxUsers)*1000) / 10
			resp.Capacity.UtilPct = &p
		}
		return nil
	})
	nhGo(g, panels, "device_slots", func() error {
		rows, err := a.fetchDeviceSlots(ctx, contrib)
		if err != nil {
			return err
		}
		resp.DeviceSlots = rows
		return nil
	})
	nhGo(g, panels, "dia_interfaces", func() error {
		rows, err := a.fetchDiaInterfaces(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.DiaInterfaces = rows
		return nil
	})
	nhGo(g, panels, "top_links", func() error {
		rows, err := a.fetchTopLinks(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.TopLinks = rows
		return nil
	})
	nhGo(g, panels, "fullest_links", func() error {
		rows, err := a.fetchFullestLinks(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.CapacityLinks = rows
		return nil
	})
	_ = g.Wait()
	resp.Degraded = panels.list()
	return resp
}

// FetchNetworkHealthOutagesData computes the Outages group: the reliability
// episode scan (which also feeds the headline OutageCount tile + delta),
// degraded links, outage summary, downtime rankings, outages-over-time,
// interface-error hotspots, and the per-contributor outage breakdown.
// Prev holds the prior-window reliability + outage-summary for the deltas.
func (a *API) FetchNetworkHealthOutagesData(ctx context.Context, start, end time.Time, contrib string) *NHOutagesGroup {
	priorStart, priorEnd := nhPriorWindow(start, end)
	scoped := contrib != ""
	intervalSec := nhIntervalSeconds(start, end)

	resp := &NHOutagesGroup{
		Window:          nhWindow(start, end),
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		DowntimeLinks:   []NHDowntimeRow{},
		DowntimeDevices: []NHDowntimeRow{},
		OutagesOverTime: []NHCountPoint{},
		ErrorHotspots:   []NHErrorHotspot{},
		Prev:            &NHOutagesPrev{},
	}

	cRollupLink := ""
	if scoped {
		cRollupLink = " AND link_pk IN (SELECT pk FROM dz_links_current WHERE contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?))"
	}
	withC := func(args ...any) []any {
		if scoped {
			return append(args, contrib)
		}
		return args
	}

	var priorOutages uint64
	// reliability is the only headline-critical panel in this group: it feeds the
	// outage headline tile and every sentence in the reliability panel, so a zero
	// from it reads as "nothing failed". Every other panel here degrades on its
	// own so one failed scan cannot blank the group.
	panels := &nhPanels{}

	// The shared outage-episode definition (see nhOutEpisodeCTE), so the count
	// published here and the one the Drain group publishes come from one rule.
	outEpisodeCTE := nhOutEpisodeCTE(scoped, false)

	g := new(errgroup.Group)
	g.SetLimit(10)

	// --- Reliability (headline-critical): outages, distinct links, capped downtime, histogram ---
	nhGoCritical(g, panels, "reliability", func() error {
		// The epi CTE carries ALL episodes (no >= 2-bucket floor). The sustained
		// headline metrics gate on dur_s >= nhSustainedOutageSeconds (>= 10 min) so
		// they stay de-saturated, while the duration histogram counts ALL episodes so
		// the <= 5-minute flap bucket populates.
		q := outEpisodeCTE + `
			SELECT
			countIf(dur_s >= ` + nhSustainedOutageSQL + `),
			uniqExactIf(link_pk, dur_s >= ` + nhSustainedOutageSQL + `),
			round(sumIf(least(dur_s, 86400), dur_s >= ` + nhSustainedOutageSQL + `) / 3600, 1),
			countIf(dur_s <= 300),
			countIf(dur_s > 300 AND dur_s <= 900),
			countIf(dur_s > 900 AND dur_s <= 3600),
			countIf(dur_s > 3600 AND dur_s <= 86400),
			countIf(dur_s > 86400)
			FROM epi` + networkHealthQuerySettings
		row := a.envDB(ctx).QueryRow(ctx, q, withC(start, end)...)
		var h NHDurationHist
		var count, distinct uint64
		var downtime float64
		if err := row.Scan(&count, &distinct, &downtime,
			&h.FlapLE5m, &h.Short5to15m, &h.Medium15to60m, &h.Sustained1to24h, &h.ChronicGt24h); err != nil {
			return err
		}
		resp.Reliability.OutageCount = count
		resp.Reliability.DistinctLinks = distinct
		resp.Reliability.CappedDowntimeHours = nzFloat(downtime)
		resp.Reliability.DurationHistogram = h
		resp.OutageCount = count
		return nil
	})
	// Prior outage count + capped downtime (for the delta). Same shared CTE, gated
	// on the sustained floor to match the OutageCount above.
	nhGo(g, panels, "reliability_prior", func() error {
		q := outEpisodeCTE + `
			SELECT countIf(dur_s >= ` + nhSustainedOutageSQL + `),
			round(sumIf(least(dur_s, 86400), dur_s >= ` + nhSustainedOutageSQL + `) / 3600, 1)
			FROM epi` + networkHealthQuerySettings
		var cnt uint64
		var downtime float64
		if err := a.envDB(ctx).QueryRow(ctx, q, withC(priorStart, priorEnd)...).Scan(&cnt, &downtime); err != nil {
			return err
		}
		priorOutages = cnt
		resp.Prev.Reliability.OutageCount = cnt
		resp.Prev.Reliability.CappedDowntimeHours = nzFloat(downtime)
		return nil
	})

	// --- Reliability: degraded links (loss but not down) ---
	// One sentence in the reliability panel, which already names degraded_links
	// among its sources, so a failure here renders that panel unavailable without
	// touching the rest of the group.
	nhGo(g, panels, "degraded_links", func() error {
		q := `SELECT uniqExact(link_pk) FROM link_rollup_5m
			WHERE bucket_ts >= ? AND bucket_ts < ?
			  AND status = 'activated' AND isis_down = 0
			  AND (a_loss_pct > 1.0 OR z_loss_pct > 1.0)` + cRollupLink + networkHealthQuerySettings
		return a.scanUint(ctx, &resp.Reliability.DegradedLinks, q, withC(start, end)...)
	})

	nhGo(g, panels, "outage_summary", func() error {
		s, err := a.fetchOutageSummary(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.OutageSummary = s
		return nil
	})
	nhGo(g, panels, "outage_summary_prev", func() error {
		s, err := a.fetchOutageSummary(ctx, priorStart, priorEnd, contrib)
		if err != nil {
			return err
		}
		resp.Prev.OutageSummary = s
		return nil
	})
	nhGo(g, panels, "downtime_links", func() error {
		rows, err := a.fetchDowntimeLinks(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.DowntimeLinks = rows
		return nil
	})
	nhGo(g, panels, "downtime_devices", func() error {
		rows, err := a.fetchDowntimeDevices(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.DowntimeDevices = rows
		return nil
	})
	nhGo(g, panels, "outages_ts", func() error {
		ts, err := a.fetchOutagesTS(ctx, start, end, intervalSec, contrib)
		if err != nil {
			return err
		}
		resp.OutagesOverTime = ts
		return nil
	})
	nhGo(g, panels, "error_hotspots", func() error {
		rows, err := a.fetchErrorHotspots(ctx, start, end, contrib)
		if err != nil {
			return err
		}
		resp.ErrorHotspots = rows
		return nil
	})

	_ = g.Wait()

	resp.OutageCountDelta = pctDelta(float64(resp.OutageCount), float64(priorOutages))

	resp.Degraded = panels.list()
	if panels.criticalFailed() {
		// Generic client-facing message; the raw DB error is logged, not exposed.
		resp.Error = "network health outages data temporarily unavailable"
	}
	return resp
}

// FetchNetworkHealthDrainData computes the Drain group: the pure-Go drain/undrain
// timing figures (the heavy undrain recovery-health figures stay on /deferred).
// Prev holds the prior-window figures for the deltas.
func (a *API) FetchNetworkHealthDrainData(ctx context.Context, start, end time.Time, contrib string) *NHDrainGroup {
	priorStart, priorEnd := nhPriorWindow(start, end)
	resp := &NHDrainGroup{
		Window:      nhWindow(start, end),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Every current-window figure is derived in Go from the union of the two
	// current scans, so a missing scan silently changes every number rather than
	// emptying a list: those two are critical. The two prior-window scans feed only
	// the vs-prior deltas, so a failure there degrades the delta and leaves the
	// current-window figures published.
	panels := &nhPanels{}

	contribLinkPKs, ok, err := a.fetchContribLinkPKs(ctx, contrib)
	if err != nil {
		// The pk lookup failed, so the scope cannot be resolved: report the failure
		// instead of publishing "0 drains, 0 undrains" for this contributor.
		panels.fail("contrib_link_pks", true)
		resp.Prev = &NHDrainTiming{}
		resp.Degraded = panels.list()
		resp.Error = "network health drain data temporarily unavailable"
		return resp
	}
	if !ok {
		// Scoped request that resolves to no links: return an empty result rather
		// than falling back to a network-wide scan. Not an error.
		resp.Prev = &NHDrainTiming{}
		return resp
	}

	var events, priorEvents []nhEvent
	var changes, priorChanges []nhChange

	g := new(errgroup.Group)
	g.SetLimit(10)
	nhGoCritical(g, panels, "link_down_events", func() error {
		evs, err := a.fetchLinkDownEvents(ctx, start, end, contribLinkPKs)
		if err != nil {
			return err
		}
		events = evs
		return nil
	})
	nhGoCritical(g, panels, "status_changes", func() error {
		chs, err := a.fetchStatusChanges(ctx, start, end, contribLinkPKs)
		if err != nil {
			return err
		}
		changes = chs
		return nil
	})
	nhGo(g, panels, "link_down_events_prev", func() error {
		evs, err := a.fetchLinkDownEvents(ctx, priorStart, priorEnd, contribLinkPKs)
		if err != nil {
			return err
		}
		priorEvents = evs
		return nil
	})
	nhGo(g, panels, "status_changes_prev", func() error {
		chs, err := a.fetchStatusChanges(ctx, priorStart, priorEnd, contribLinkPKs)
		if err != nil {
			return err
		}
		priorChanges = chs
		return nil
	})
	_ = g.Wait()

	// The undrain (recovery-health) figures are NOT computed here; they are served
	// from the deferred endpoint. TimeToUndrain* stay nil (MatchedUndrains still
	// reflects the Go-matched pair count).
	dt, _ := computeDrainTiming(events, changes)
	resp.DrainTiming = dt
	dtPrev, _ := computeDrainTiming(priorEvents, priorChanges)
	resp.Prev = &dtPrev

	resp.Degraded = panels.list()
	if panels.criticalFailed() {
		// Generic client-facing message; the raw DB error is logged, not exposed.
		resp.Error = "network health drain data temporarily unavailable"
	}
	return resp
}

// FetchNetworkHealthTicketsData computes the Tickets group: the public ops-
// management aggregate for the current and prior windows, split from one union
// fetch over the ops API. The fetch's lower bound (priorStart) is enforced by the
// paging loop; its upper bound (end) is enforced here by splitTicketsByWindow, so
// the ticket counts and the outage figures beside them cover the same window.
// resolveContributorScope + filterTicketsByContributor scope it when contrib is
// set, and the scope is resolved BEFORE the fetch so an unknown contributor code
// costs no upstream calls. Windows reaching further back than
// nhOpsTicketsMaxLookback are refused rather than paged. Text-free by
// construction.
func (a *API) FetchNetworkHealthTicketsData(ctx context.Context, start, end time.Time, contrib string) *NHTicketsGroup {
	priorStart, _ := nhPriorWindow(start, end)
	scoped := contrib != ""
	resp := &NHTicketsGroup{
		Window:      nhWindow(start, end),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// The ops API is newest-first with no date filter, so the paging loop walks
	// back from now to priorStart. Refuse a window that would walk further than
	// the widest window the page can ask for, rather than truncating the fetch and
	// publishing an undercount with no signal.
	if time.Since(priorStart) > nhOpsTicketsMaxLookback {
		logWarn("network health: ops tickets window predates the lookback floor",
			"prior_start", priorStart.UTC().Format(time.RFC3339), "max_lookback", nhOpsTicketsMaxLookback)
		resp.Degraded = []string{"ops_tickets"}
		resp.Error = "network health: ops ticket history only reaches back " +
			strconv.Itoa(int(nhOpsTicketsMaxLookback/(24*time.Hour))) + " days"
		return resp
	}

	// Resolve the contributor scope first: an unknown code fails here, before the
	// pagination spends up to maxPages credentialed round trips on tickets that
	// would then be filtered away.
	outageContrib := ""
	scopePubkey := ""
	var cscope *nhContributorScope
	if scoped {
		s, err := a.resolveContributorScope(ctx, contrib)
		if err != nil {
			logWarn("network health: resolve contributor scope for tickets failed", "error", err, "contributor", contrib)
			resp.Degraded = []string{"contributor_scope"}
			resp.Error = "network health: ops tickets unavailable"
			return resp
		}
		cscope = s
		outageContrib = contrib
		scopePubkey = cscope.pubkey
	}

	// Cap the external pagination loop independently of the caller's deadline.
	tctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	// One fetch over the union window [priorStart, end): newest-first, stops at
	// priorStart. The current and prior sets are split in Go (no second round-trip).
	tickets, err := a.fetchOpsTicketsSince(tctx, priorStart)
	if err != nil {
		logWarn("network health: ops tickets fetch failed", "error", err)
		// Surface the failure so GroupBoundary shows the unavailable state and the
		// worker keeps the last-good blob, rather than the panels silently vanishing.
		resp.Degraded = []string{"ops_tickets"}
		resp.Error = "network health: ops tickets unavailable"
		return resp
	}
	if tickets == nil {
		return resp // no API key configured (silent empty, not an error)
	}
	if scoped {
		tickets = filterTicketsByContributor(tickets, cscope)
	}

	var degraded []string

	// A dropped ticket is a missing count, so say so instead of publishing a quiet
	// undercount: name the panel once the dropped set is material, and report the
	// group unavailable when the whole fetch is unusable (an upstream timestamp
	// format change) so the worker keeps the last good blob. Both gates run after
	// the contributor filter, so the share alone would fire on a single malformed
	// row in a small scope; the absolute floor keeps that from hiding a handful of
	// correct tickets.
	split := splitTicketsByWindow(tickets, priorStart, start, end)
	if split.undated > 0 {
		logWarn("network health: ops tickets dropped with unparseable created_at",
			"count", split.undated, "total", len(tickets))
		material := split.undated >= nhTicketUndatedDegradeMin &&
			float64(split.undated) >= nhTicketUndatedDegradeShare*float64(len(tickets))
		if material {
			degraded = append(degraded, "ops_tickets")
		}
		if material && split.undated == len(tickets) {
			resp.Degraded = []string{"ops_tickets"}
			resp.Error = "network health: ops tickets unavailable"
			return resp
		}
	}

	// One wider outage-list scan over [priorStart, end), partitioned in Go. It is
	// the denominator of the coverage figures, so a failure leaves those figures
	// zeroed: name the panel so the coverage figures read as unavailable, without
	// blanking the ticket counts and maintenance figures that computed fine.
	outs, err := a.fetchOutageListForTickets(ctx, priorStart, end, outageContrib)
	if err != nil {
		logWarn("network health panel failed", "panel", "outage_list", "critical", false, "error", err)
		outs = nil
		degraded = append(degraded, "outage_list")
	}
	var curOuts, priorOuts []nhOutage
	for _, o := range outs {
		if !o.start.Before(start) {
			curOuts = append(curOuts, o)
		} else {
			priorOuts = append(priorOuts, o)
		}
	}

	userContrib, err := a.cachedOpsUsers(ctx)
	if err != nil {
		logWarn("network health panel failed", "panel", "ops_users", "critical", false, "error", err)
		degraded = append(degraded, "ops_users")
	}
	curAgg := computeTicketAggregates(split.cur, split.cover, curOuts, userContrib, scopePubkey)
	resp.OpsTickets = &curAgg
	priorAgg := computeTicketAggregates(split.prior, split.cover, priorOuts, userContrib, scopePubkey)
	resp.Prev = &priorAgg
	sort.Strings(degraded)
	resp.Degraded = degraded
	return resp
}

// nhTicketCoverTolerance is how far a ticket's incident interval may sit outside
// an outage's interval and still count as covering it (see the overlap check in
// computeTicketAggregates). It bounds interval distance, not filing time.
const nhTicketCoverTolerance = 30 * time.Minute

// nhTicketUndatedDegradeShare is the dropped share of a fetch (tickets whose
// created_at does not parse) at which the ticket figures are reported as
// incomplete rather than published as an undercount.
const nhTicketUndatedDegradeShare = 0.05

// nhTicketUndatedDegradeMin is the absolute number of dropped tickets that must
// accompany the share before the ticket figures are reported as incomplete. A
// contributor-scoped fetch holds only that contributor's tickets, so the share on
// its own would blank a dozen correct tickets over one malformed row.
const nhTicketUndatedDegradeMin = 3

// nhTicketSplit is one ops-API fetch partitioned into the reporting windows.
// cur/prior are the counted sets, each bounded by its own window. cover is every
// dated ticket in the fetch and is used ONLY for outage coverage, never for a
// published count.
type nhTicketSplit struct {
	cur, prior []nhRawTicket
	cover      []nhRawTicket
	undated    int
}

// splitTicketsByWindow partitions tickets by created_at (filing time) into the
// current window [start, end) and the prior window [priorStart, start). The ops
// API has no date filter, so the fetch returns everything newer than priorStart
// and the upper bound is applied here. A ticket whose created_at does not parse
// cannot be placed in either window, so it is dropped and counted in undated
// rather than counted as current.
//
// cover collects every dated ticket regardless of window, including tickets filed
// after end. computeTicketAggregates matches coverage on the ticket's incident
// interval (start_at/end_at), not on created_at, so a ticket filed hours or days
// after an outage still covers it; bounding cover by filing time would report
// those outages as unfiled.
func splitTicketsByWindow(tickets []nhRawTicket, priorStart, start, end time.Time) nhTicketSplit {
	var sp nhTicketSplit
	for _, t := range tickets {
		ct, ok := nhParseTicketTime(&t.CreatedAt)
		if !ok {
			sp.undated++
			continue
		}
		sp.cover = append(sp.cover, t)
		switch {
		case ct.Before(priorStart):
			// Older than the union window the fetch was asked for.
		case ct.Before(start):
			sp.prior = append(sp.prior, t)
		case ct.Before(end):
			sp.cur = append(sp.cur, t)
		}
	}
	return sp
}

// FetchNetworkHealthImpactfulData computes the Impactful group: impact-weighted
// downtime for the current window and (network-wide only) the prior window for
// the delta. Its heavy scan runs on this dedicated endpoint (170s) so it never
// blocks the fast groups. Best-effort: on a current-window failure Unavailable is
// set and Error is populated so the cache worker keeps the last good blob; a
// prior-window failure only nils the delta.
func (a *API) FetchNetworkHealthImpactfulData(ctx context.Context, start, end time.Time, contrib string) *NHImpactful {
	priorStart, priorEnd := nhPriorWindow(start, end)
	scoped := contrib != ""
	resp := &NHImpactful{
		Window:      nhWindow(start, end),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Prev:        &NHImpactfulPrev{},
	}

	cur, err := a.fetchImpactfulDowntime(ctx, start, end, contrib)
	if err != nil {
		logWarn("network health panel failed", "panel", "impactful", "critical", true, "error", err)
		resp.Unavailable = true
		resp.Degraded = []string{"impactful"}
		// Generic client-facing message; the raw DB error is logged above, not exposed.
		resp.Error = "network health impactful data temporarily unavailable"
		return resp
	}
	resp.ImpactfulDowntimeHours = cur

	// Prior-window impactful downtime (for the delta), network-wide only so the
	// live scoped request never pays a second heavy run. Strictly best-effort.
	if !scoped {
		if prior, err := a.fetchImpactfulDowntime(ctx, priorStart, priorEnd, contrib); err != nil {
			logWarn("network health panel failed", "panel", "impactful_prior", "critical", false, "error", err)
			resp.Degraded = []string{"impactful_prior"}
		} else {
			resp.Prev.ImpactfulDowntimeHours = prior
			resp.ImpactfulDowntimeDelta = pctDelta(cur, prior)
		}
	}
	return resp
}

// scanUint runs a single-value uint64 query.
func (a *API) scanUint(ctx context.Context, dst *uint64, query string, args ...any) error {
	return a.envDB(ctx).QueryRow(ctx, query, args...).Scan(dst)
}

// scanCodeCount fills a map[code]count from a two-column (String, UInt64) query.
func (a *API) scanCodeCount(ctx context.Context, dst map[string]uint64, query string, args ...any) error {
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var code string
		var n uint64
		if err := rows.Scan(&code, &n); err != nil {
			return err
		}
		dst[code] = n
	}
	return rows.Err()
}

// nzFloat maps NaN/Inf to 0 so JSON encoding never fails.
func nzFloat(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

// pctDelta returns the percentage change from prior to cur, or nil if prior is 0.
func pctDelta(cur, prior float64) *float64 {
	if prior == 0 {
		return nil
	}
	d := (cur - prior) / prior * 100
	if math.IsNaN(d) || math.IsInf(d, 0) {
		return nil
	}
	d = math.Round(d*100) / 100
	return &d
}

// fetchLinkDownEvents reconstructs the window's link failures as intervals, from
// the same episode definition the Outages group publishes as outage_count (see
// nhOutEpisodeCTE): sustained episodes only, loss-only failures included,
// provisioning excluded, FINAL-deduped. linkPKs optionally restricts to a set of
// links.
func (a *API) fetchLinkDownEvents(ctx context.Context, start, end time.Time, linkPKs []string) ([]nhEvent, error) {
	args := []any{start, end}
	if linkPKs != nil {
		args = append(args, linkPKs)
	}
	q := nhOutEpisodeCTE(false, linkPKs != nil) + `
		SELECT link_pk, started_at, ended_at FROM epi
		WHERE dur_s >= ` + nhSustainedOutageSQL + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []nhEvent
	for rows.Next() {
		var e nhEvent
		if err := rows.Scan(&e.linkPK, &e.start, &e.end); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// fetchStatusChanges returns link status transitions in the window. linkPKs
// optionally restricts to a set of links.
func (a *API) fetchStatusChanges(ctx context.Context, start, end time.Time, linkPKs []string) ([]nhChange, error) {
	filter := ""
	args := []any{start, end}
	if linkPKs != nil {
		filter = " AND link_pk IN ?"
		args = append(args, linkPKs)
	}
	q := `SELECT link_pk, previous_status, new_status, changed_ts
		FROM dz_link_status_changes
		WHERE changed_ts >= ? AND changed_ts < ?` + filter + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []nhChange
	for rows.Next() {
		var c nhChange
		if err := rows.Scan(&c.linkPK, &c.prev, &c.next, &c.ts); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// nhDrainedStatus reports whether a link status means the link is intentionally
// out of service. Both drain levels count, matching nhAvailStateCountIfs.
func nhDrainedStatus(s string) bool {
	return s == "soft-drained" || s == "hard-drained"
}

// computeDrainTiming pairs each link-down event with the drain that followed and
// the undrain after that, and summarizes the durations. Mirrors the ops-report
// reconcile.match_drains / drain_timing logic (without the recovery-point refinement).
func computeDrainTiming(events []nhEvent, changes []nhChange) (NHDrainTiming, []nhDrainMatch) {
	// Build a per-link timeline of drain/undrain transitions.
	type tev struct {
		ts    time.Time
		drain bool // true = entered a drained state, false = left one
	}
	timeline := map[string][]tev{}
	drainsByLink := map[string][]time.Time{}
	var drains, undrains int
	for _, c := range changes {
		// Entering drained from any non-drained status is one drain; leaving it for
		// any non-drained status is one undrain. A change of drain LEVEL inside a
		// drained run (soft <-> hard) is neither, so it neither strands the open
		// drain nor counts a second one.
		was, now := nhDrainedStatus(c.prev), nhDrainedStatus(c.next)
		switch {
		case !was && now:
			timeline[c.linkPK] = append(timeline[c.linkPK], tev{ts: c.ts, drain: true})
			drainsByLink[c.linkPK] = append(drainsByLink[c.linkPK], c.ts)
			drains++
		case was && !now:
			timeline[c.linkPK] = append(timeline[c.linkPK], tev{ts: c.ts, drain: false})
			undrains++
		}
	}
	for k := range drainsByLink {
		sort.Slice(drainsByLink[k], func(i, j int) bool { return drainsByLink[k][i].Before(drainsByLink[k][j]) })
	}

	// Pair each drain with the undrain that ends it, via a per-link state machine.
	// This covers ALL drains (planned maintenance included), not only drains that
	// followed an outage, so "time drained" and "time to undrain" reflect how long
	// healthy capacity actually sat out of service.
	var drained []float64
	matches := []nhDrainMatch{}
	for lp, evs := range timeline {
		sort.Slice(evs, func(i, j int) bool { return evs[i].ts.Before(evs[j].ts) })
		var drainStart *time.Time
		for _, e := range evs {
			if e.drain {
				if drainStart == nil {
					d := e.ts
					drainStart = &d
				}
			} else if drainStart != nil {
				drained = append(drained, e.ts.Sub(*drainStart).Minutes())
				matches = append(matches, nhDrainMatch{linkPK: lp, drainTS: *drainStart, undrainTS: e.ts})
				drainStart = nil
			}
		}
	}

	// Time-to-drain and drain-within-30m are anchored to the outage event as t0,
	// so they still require a link-down event to pair with a drain.
	const matchWindow = 48 * time.Hour
	var ttd []float64
	eventsWithDrain := 0
	drainWithin30m := 0
	for _, e := range events {
		// Is there a drain within 30 min of the failure (start .. end+30m)?
		for _, d := range drainsByLink[e.linkPK] {
			if !d.Before(e.start) && !d.After(e.end.Add(30*time.Minute)) {
				drainWithin30m++
				break
			}
		}
		// First drain within the match window after the incident start.
		for _, d := range drainsByLink[e.linkPK] {
			if !d.Before(e.start) && !d.After(e.start.Add(matchWindow)) {
				ttd = append(ttd, d.Sub(e.start).Minutes())
				eventsWithDrain++
				break
			}
		}
	}

	return NHDrainTiming{
		OutageCount:       len(events),
		EventsWithDrain:   eventsWithDrain,
		Drains:            drains,
		Undrains:          undrains,
		TimeToDrainP50Min: medianMinutes(ttd),
		TimeToDrainMaxMin: maxMinutes(ttd),
		TimeDrainedP50Min: medianMinutes(drained),
		TimeDrainedMaxMin: maxMinutes(drained),
		DrainWithin30mPct: pctPtr(drainWithin30m, len(events)),
		MatchedUndrains:   len(matches),
	}, matches
}

// fetchRecoveryHealth returns, for each matched drain->undrain interval, the
// minutes from when the link recovered (start of the unbroken healthy run ending
// just before the undrain) to the undrain. This is "time to undrain a healthy
// link". Links left drained while still unhealthy contribute 0 (recovered at the
// undrain). Uses the same health expression as soak/recovery in the ops report.
// This is the heavy scan split out of the main page: it runs only on the
// deferred endpoint (GetNetworkHealthDeferred) and uses
// networkHealthDeferredQuerySettings (max_execution_time = 170).
func (a *API) fetchRecoveryHealth(ctx context.Context, matches []nhDrainMatch) ([]float64, error) {
	if len(matches) == 0 {
		return nil, nil
	}
	type bucket struct {
		ts      time.Time
		healthy bool
	}
	byLink := map[string][]bucket{}
	// Chunk is kept small and every chunk carries global time bounds + a link_pk IN
	// list so the (bucket_ts, link_pk) primary key prunes the scan. Without these,
	// the OR-only WHERE scanned link_rollup_5m unbounded and OOMed on heavy
	// contributors (the memory limit was hit at multi-GiB).
	const chunk = 20
	for i := 0; i < len(matches); i += chunk {
		end := i + chunk
		if end > len(matches) {
			end = len(matches)
		}
		clauses := make([]string, 0, end-i)
		linkSet := map[string]struct{}{}
		var gMin, gMax time.Time
		for _, m := range matches[i:end] {
			clauses = append(clauses, "(link_pk = ? AND bucket_ts >= ? AND bucket_ts <= ?)")
			linkSet[m.linkPK] = struct{}{}
			if gMin.IsZero() || m.drainTS.Before(gMin) {
				gMin = m.drainTS
			}
			if m.undrainTS.After(gMax) {
				gMax = m.undrainTS
			}
		}
		linkPKs := make([]string, 0, len(linkSet))
		for k := range linkSet {
			linkPKs = append(linkPKs, k)
		}
		// Arg order matches placeholder order: global bounds, link IN list, then
		// per-interval (link_pk, drainTS, undrainTS) triples.
		args := make([]any, 0, 3+(end-i)*3)
		args = append(args, gMin, gMax, linkPKs)
		for _, m := range matches[i:end] {
			args = append(args, m.linkPK, m.drainTS, m.undrainTS)
		}
		q := `SELECT link_pk, bucket_ts,
				(isis_down = 0 AND provisioning = 0 AND a_loss_pct <= 0.5 AND z_loss_pct <= 0.5) AS healthy
			FROM link_rollup_5m
			WHERE bucket_ts >= ? AND bucket_ts <= ? AND link_pk IN ?
			  AND (` + strings.Join(clauses, " OR ") + `)` + networkHealthDeferredQuerySettings
		rows, err := a.envDB(ctx).Query(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var lp string
			var ts time.Time
			var h uint8
			if err := rows.Scan(&lp, &ts, &h); err != nil {
				rows.Close()
				return nil, err
			}
			byLink[lp] = append(byLink[lp], bucket{ts: ts, healthy: h == 1})
		}
		rows.Close()
	}

	var ttu []float64
	for _, m := range matches {
		bs := make([]bucket, 0)
		for _, b := range byLink[m.linkPK] {
			if !b.ts.Before(m.drainTS) && !b.ts.After(m.undrainTS) {
				bs = append(bs, b)
			}
		}
		sort.Slice(bs, func(i, j int) bool { return bs[i].ts.After(bs[j].ts) }) // desc
		var recovery *time.Time
		for _, b := range bs {
			if !b.ts.Before(m.undrainTS) {
				continue
			}
			if b.healthy {
				t := b.ts
				recovery = &t
			} else {
				break
			}
		}
		if recovery != nil {
			ttu = append(ttu, m.undrainTS.Sub(*recovery).Minutes())
		} else {
			ttu = append(ttu, 0)
		}
	}
	return ttu, nil
}

// FetchNetworkHealthDeferredData computes the deferred (slow) undrain figures for
// the drain-timing panel: the recovery-health-derived "time to undrain a healthy
// link" p50/max for the current window and the prior window (for the section
// delta). This is split from the Drain group (FetchNetworkHealthDrainData) so the
// page does not wait on the heavy link_rollup_5m recovery scan. contrib scopes
// the computation to one contributor's links (same scoping the Drain group uses).
//
// Best-effort throughout: a failed recovery-health scan marks the undrain figures
// unavailable rather than failing the payload. out.Error is set only when the
// CURRENT-window figures genuinely could not be computed (drain-timing inputs
// failed, or the recovery-health scan errored while there were pairs to match),
// so the cache worker keeps the last good blob instead of caching an "unavailable"
// over a good value. A prior-window failure only nils the delta.
func (a *API) FetchNetworkHealthDeferredData(ctx context.Context, start, end time.Time, contrib string) *NHDeferred {
	priorStart, priorEnd := nhPriorWindow(start, end)

	out := &NHDeferred{}

	contribLinkPKs, ok, err := a.fetchContribLinkPKs(ctx, contrib)
	if err != nil {
		// The pk lookup failed, so the scope cannot be resolved: report the failure
		// instead of publishing empty undrain figures for this contributor.
		out.Prev = &NHDeferred{}
		out.UndrainUnavailable = true
		out.Degraded = []string{"contrib_link_pks"}
		out.Error = "network health deferred data temporarily unavailable"
		return out
	}
	if !ok {
		// Scoped request that resolves to no links: return an empty result rather
		// than falling back to a network-wide scan. Not an error.
		out.Prev = &NHDeferred{}
		return out
	}

	// Current window: reconstruct drain->undrain pairs, then measure recovery. The
	// pairs come from the status changes alone (computeDrainTiming derives matches
	// from transitions, not from link-down events), so no event scan runs here.
	changes, chErr := a.fetchStatusChanges(ctx, start, end, contribLinkPKs)
	_, matches := computeDrainTiming(nil, changes)
	out.MatchedUndrains = len(matches)
	var degraded []string
	if chErr != nil {
		logWarn("network health panel failed", "panel", "status_changes", "critical", true, "error", chErr)
		out.UndrainUnavailable = true
		degraded = append(degraded, "status_changes")
		out.Error = "network health deferred: drain-timing inputs failed"
	} else if ttu, err := a.fetchRecoveryHealth(ctx, matches); err != nil {
		logWarn("network health panel failed", "panel", "recovery_health", "critical", len(matches) > 0, "error", err)
		out.UndrainUnavailable = true
		degraded = append(degraded, "recovery_health")
		if len(matches) > 0 {
			// Generic client-facing message; the raw DB error is logged above, not exposed.
			out.Error = "network health deferred data temporarily unavailable"
		}
	} else {
		out.TimeToUndrainP50Min = medianMinutes(ttu)
		out.TimeToUndrainMaxMin = maxMinutes(ttu)
	}

	// Prior window (for the section delta). Strictly best-effort: a prior failure
	// only marks the prior figures unavailable and never sets out.Error.
	prev := &NHDeferred{}
	priorChanges, pchErr := a.fetchStatusChanges(ctx, priorStart, priorEnd, contribLinkPKs)
	_, priorMatches := computeDrainTiming(nil, priorChanges)
	prev.MatchedUndrains = len(priorMatches)
	if pchErr != nil {
		logWarn("network health panel failed", "panel", "status_changes_prev", "critical", false, "error", pchErr)
		prev.UndrainUnavailable = true
		degraded = append(degraded, "status_changes_prev")
	} else if ttuPrev, err := a.fetchRecoveryHealth(ctx, priorMatches); err != nil {
		logWarn("network health panel failed", "panel", "recovery_health_prev", "critical", false, "error", err)
		prev.UndrainUnavailable = true
		degraded = append(degraded, "recovery_health_prev")
	} else {
		prev.TimeToUndrainP50Min = medianMinutes(ttuPrev)
		prev.TimeToUndrainMaxMin = maxMinutes(ttuPrev)
	}
	out.Prev = prev
	sort.Strings(degraded)
	out.Degraded = degraded

	return out
}

func medianMinutes(xs []float64) *float64 {
	if len(xs) == 0 {
		return nil
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	var m float64
	n := len(s)
	if n%2 == 1 {
		m = s[n/2]
	} else {
		m = (s[n/2-1] + s[n/2]) / 2
	}
	m = math.Round(m)
	return &m
}

func maxMinutes(xs []float64) *float64 {
	if len(xs) == 0 {
		return nil
	}
	m := xs[0]
	for _, x := range xs[1:] {
		if x > m {
			m = x
		}
	}
	m = math.Round(m)
	return &m
}

func pctPtr(n, total int) *float64 {
	if total == 0 {
		return nil
	}
	p := math.Round(float64(n)/float64(total)*1000) / 10
	return &p
}

// nhRawTicket decodes only the raw ops-API fields the aggregate needs. Kept
// server-side and never marshaled to the response. The lake OpsTicket struct
// omits root_cause, so we decode the raw JSON here. ContributorName/
// ContributorPubkey and AffectedDevices are used only to match a ticket to a
// scoped contributor (see filterTicketsByContributor) — never surfaced in the
// NHTickets response.
type nhRawTicket struct {
	ID                string            `json:"id"`
	Type              string            `json:"type"`
	Severity          *string           `json:"severity"`
	Status            string            `json:"status"`
	StartAt           *string           `json:"start_at"`
	EndAt             *string           `json:"end_at"`
	CreatedAt         string            `json:"created_at"`
	RootCause         *string           `json:"root_cause"`
	UserPubkey        string            `json:"user_pubkey"` // ticket creator (for self-reported classification)
	ReporterName      string            `json:"reporter_name"`
	ContributorName   *string           `json:"contributor_name"`
	ContributorPubkey *string           `json:"contributor_pubkey"`
	AffectedLinks     []OpsTicketEntity `json:"affected_links"`
	AffectedDevices   []OpsTicketEntity `json:"affected_devices"`
}

type nhOutage struct {
	linkPK   string
	linkCode string
	start    time.Time
	end      time.Time
}

// fetchOpsTicketsSince pages the ops-management API for tickets created at or
// after `since`. The API has no date filter and is sorted newest-first, so we
// stop once a page's oldest ticket predates the cutoff. Returns nil (no error)
// when no API key is configured, so the panel is simply omitted.
//
// A mid-pagination failure (timeout, transient 5xx, rate limit) degrades to
// the tickets already collected instead of discarding the whole window: the
// aggregate undercounts the oldest pages rather than the panel going blank
// until the next successful refresh. Only a failure on the very first page
// (no partial data to salvage) is treated as a hard error.
func (a *API) fetchOpsTicketsSince(ctx context.Context, since time.Time) ([]nhRawTicket, error) {
	client := newOpsClient()
	if client.apiKey == "" {
		return nil, nil
	}
	sinceStr := since.UTC().Format(time.RFC3339)
	const pageSize = 100
	// maxPages*pageSize is a hard cap on the newest-first stream. The prior-window
	// delta doubles the span this fetch covers (a 30d view now pages back 60d, a
	// 92d custom window back 184d), so the cap is raised well above observed
	// volume (~1,900 tickets / 60d) to keep the OLDEST prior-window tickets from
	// truncating and undercounting the prior aggregate.
	const maxPages = 100
	out := []nhRawTicket{}
	for page := 0; page < maxPages; page++ {
		path := "/tickets?limit=" + strconv.Itoa(pageSize) + "&offset=" + strconv.Itoa(page*pageSize)
		body, err := client.get(ctx, path)
		if err != nil {
			if page == 0 {
				return nil, err
			}
			logWarn("network health: ops tickets pagination stopped early", "error", err, "page", page, "collected", len(out))
			break
		}
		var env struct {
			Data struct {
				Tickets []nhRawTicket `json:"tickets"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			if page == 0 {
				return nil, err
			}
			logWarn("network health: ops tickets pagination stopped early (bad json)", "error", err, "page", page, "collected", len(out))
			break
		}
		tickets := env.Data.Tickets
		if len(tickets) == 0 {
			break
		}
		for _, t := range tickets {
			if t.ID != "" && t.CreatedAt >= sinceStr {
				out = append(out, t)
			}
		}
		oldest := tickets[len(tickets)-1].CreatedAt
		if oldest < sinceStr || len(tickets) < pageSize {
			break
		}
	}
	return out, nil
}

// opsUsersCache memoizes the ops-management /users registry for
// nhOpsUsersCacheTTL. The registry is up to 20 paged credentialed requests and
// was re-fetched on every tickets computation (every worker refresh cycle plus
// every live request). Concurrent misses collapse into one run via singleflight.
// The zero value is ready to use, so a directly-constructed API needs no
// initialization and each test's API starts cold.
type opsUsersCache struct {
	mu      sync.Mutex
	users   map[string]string
	expires time.Time
	sf      singleflight.Group
}

// cachedOpsUsers returns the ops /users registry, re-fetching it only when the
// cached copy has expired. An EMPTY registry is never cached: empty means the
// registry was unavailable (no API key, or a first-page failure) and pinning it
// would publish "unavailable" for SelfReportedPct for the whole TTL.
func (a *API) cachedOpsUsers(ctx context.Context) (map[string]string, error) {
	a.opsUsersCache.mu.Lock()
	if len(a.opsUsersCache.users) > 0 && time.Now().Before(a.opsUsersCache.expires) {
		users := a.opsUsersCache.users
		a.opsUsersCache.mu.Unlock()
		return users, nil
	}
	a.opsUsersCache.mu.Unlock()

	// The collapsed miss-path fetch must not be tied to the winning caller's
	// context: with a plain Do the shared fetch inherits the winner's ctx, so one
	// caller's disconnect would fail every collapsed waiter. Detach with
	// WithoutCancel under its own deadline and select on the caller's ctx via
	// DoChan (same pattern as cachedScalar).
	ch := a.opsUsersCache.sf.DoChan("ops_users", func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nhOpsUsersFetchTimeout)
		defer cancel()
		users, err := a.fetchOpsUsers(fctx)
		if err != nil {
			return map[string]string{}, err
		}
		if len(users) > 0 {
			a.opsUsersCache.mu.Lock()
			a.opsUsersCache.users = users
			a.opsUsersCache.expires = time.Now().Add(nhOpsUsersCacheTTL)
			a.opsUsersCache.mu.Unlock()
		}
		return users, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return map[string]string{}, res.Err
		}
		return res.Val.(map[string]string), nil
	case <-ctx.Done():
		return map[string]string{}, ctx.Err()
	}
}

// fetchOpsUsers pages the ops-management /users registry and returns a map of
// user pubkey -> the contributor_pubkey that user belongs to (empty string for
// a DoubleZero user, whose contributor_pubkey is null). Used to classify who
// filed each incident (see computeTicketAggregates: an incident is self-reported
// only when its creator belongs to the SAME contributor the incident is about,
// i.e. userContrib[user_pubkey] == ticket.contributor_pubkey). Returns an empty
// map (no error) when no API key is configured. On a fetch/decode failure the
// map is empty and the caller leaves SelfReportedPct nil ("unavailable") rather
// than mislabeling every incident.
func (a *API) fetchOpsUsers(ctx context.Context) (map[string]string, error) {
	client := newOpsClient()
	users := map[string]string{}
	if client.apiKey == "" {
		return users, nil
	}
	const pageSize = 500
	const maxPages = 20
	for page := 0; page < maxPages; page++ {
		path := "/users?limit=" + strconv.Itoa(pageSize) + "&offset=" + strconv.Itoa(page*pageSize)
		body, err := client.get(ctx, path)
		if err != nil {
			if page == 0 {
				return map[string]string{}, err
			}
			logWarn("network health: ops users pagination stopped early", "error", err, "page", page)
			break
		}
		var env struct {
			Data []struct {
				Pubkey            string  `json:"pubkey"`
				ContributorPubkey *string `json:"contributor_pubkey"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			if page == 0 {
				return map[string]string{}, err
			}
			logWarn("network health: ops users pagination stopped early (bad json)", "error", err, "page", page)
			break
		}
		if len(env.Data) == 0 {
			break
		}
		for _, u := range env.Data {
			if u.Pubkey == "" {
				continue
			}
			if u.ContributorPubkey == nil {
				users[u.Pubkey] = "" // DoubleZero (no contributor affiliation)
			} else {
				users[u.Pubkey] = *u.ContributorPubkey
			}
		}
		if len(env.Data) < pageSize {
			break
		}
	}
	return users, nil
}

// nhContributorScope identifies a contributor for matching ops tickets against:
// its onchain pubkey/name (to match a ticket's own contributor_name/
// contributor_pubkey fields directly) plus the link/device codes it owns (to
// match tickets filed against its infrastructure even when the ticket's own
// contributor field points elsewhere or is unset).
type nhContributorScope struct {
	pubkey      string
	name        string
	linkCodes   map[string]struct{}
	deviceCodes map[string]struct{}
}

// resolveContributorScope resolves the onchain identity (pubkey/name) and the
// set of link/device codes owned by the contributor with the given `code`, for
// matching ops tickets in filterTicketsByContributor. One-time lookup per
// scoped request (not per ticket).
func (a *API) resolveContributorScope(ctx context.Context, code string) (*nhContributorScope, error) {
	scope := &nhContributorScope{linkCodes: map[string]struct{}{}, deviceCodes: map[string]struct{}{}}
	var pk string
	q := `SELECT pk, name FROM dz_contributors_current WHERE code = ?` + networkHealthQuerySettings
	if err := a.envDB(ctx).QueryRow(ctx, q, code).Scan(&pk, &scope.name); err != nil {
		return nil, err
	}
	scope.pubkey = pk

	if rows, err := a.envDB(ctx).Query(ctx,
		`SELECT code FROM dz_links_current WHERE contributor_pk = ?`+networkHealthQuerySettings, pk); err == nil {
		for rows.Next() {
			var c string
			if rows.Scan(&c) == nil {
				scope.linkCodes[c] = struct{}{}
			}
		}
		rows.Close()
	} else {
		logWarn("network health: resolve contributor link codes failed", "error", err, "contributor", code)
	}
	if rows, err := a.envDB(ctx).Query(ctx,
		`SELECT code FROM dz_devices_current WHERE contributor_pk = ?`+networkHealthQuerySettings, pk); err == nil {
		for rows.Next() {
			var c string
			if rows.Scan(&c) == nil {
				scope.deviceCodes[c] = struct{}{}
			}
		}
		rows.Close()
	} else {
		logWarn("network health: resolve contributor device codes failed", "error", err, "contributor", code)
	}
	return scope, nil
}

// nhTicketMatchesContributor reports whether a ticket belongs to the scoped
// contributor: either the ticket's own contributor identity (name or pubkey)
// matches, or one of its affected links/devices is owned by that contributor.
func nhTicketMatchesContributor(t nhRawTicket, scope *nhContributorScope) bool {
	if t.ContributorPubkey != nil && *t.ContributorPubkey == scope.pubkey {
		return true
	}
	if t.ContributorName != nil && *t.ContributorName == scope.name {
		return true
	}
	for _, l := range t.AffectedLinks {
		if _, ok := scope.linkCodes[l.Code]; ok {
			return true
		}
	}
	for _, d := range t.AffectedDevices {
		if _, ok := scope.deviceCodes[d.Code]; ok {
			return true
		}
	}
	return false
}

// filterTicketsByContributor returns only the tickets that belong to scope.
func filterTicketsByContributor(tickets []nhRawTicket, scope *nhContributorScope) []nhRawTicket {
	out := make([]nhRawTicket, 0, len(tickets))
	for _, t := range tickets {
		if nhTicketMatchesContributor(t, scope) {
			out = append(out, t)
		}
	}
	return out
}

// fetchOutageListForTickets returns the window's outages (link code + interval)
// for matching against tickets (the full list, not the top-N flappers).
// contrib, when non-empty, restricts the outages to links owned by that
// contributor, so a scoped view's outage-coverage numbers reconcile with its
// own (filtered) tickets. Episodes are derived from link_rollup_5m (full
// window) rather than the ~8-day-capped incident view.
func (a *API) fetchOutageListForTickets(ctx context.Context, start, end time.Time, contrib string) ([]nhOutage, error) {
	args := []any{start, end}
	if contrib != "" {
		args = append(args, contrib)
	}
	q := nhOutEpisodeCTE(contrib != "", false) + `
		SELECT epi.link_pk, coalesce(lk.code, ''), epi.started_at, epi.ended_at
		FROM epi JOIN dz_links_current lk ON lk.pk = epi.link_pk
		WHERE epi.dur_s >= ` + nhSustainedOutageSQL + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []nhOutage{}
	for rows.Next() {
		var o nhOutage
		if err := rows.Scan(&o.linkPK, &o.linkCode, &o.start, &o.end); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func nhParseTicketTime(s *string) (time.Time, bool) {
	if s == nil || *s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, *s); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(strings.Replace(*s, "T", " ", 1), "Z")); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

// computeTicketAggregates ports the ops-report ticket_stats / ticket_timing /
// outage-coverage logic. Aggregates whatever ticket set it's given: the full
// network-wide set, or a contributor-filtered subset for a scoped view.
// Text-free.
//
// dzUsers is the set of DoubleZero-staff pubkeys (see fetchOpsUsers). Incidents
// are classified self-reported vs DoubleZero-filed by their creator's pubkey.
// When dzUsers is empty (registry fetch failed) SelfReportedPct is left nil so
// the frontend shows "unavailable" instead of mislabeling every incident.
// userContrib maps a user pubkey -> the contributor it belongs to ("" for a
// DoubleZero user); see fetchOpsUsers. scopePubkey, when non-empty, restricts
// the incident metrics to incidents ABOUT that contributor (its own
// contributor_pubkey) so that another contributor's self-filed incident on a
// shared link does not leak into this contributor's numbers.
//
// tickets is the counted set (the window's tickets). coverTickets is every
// fetched ticket and is used ONLY to index outage coverage: the match runs on the
// ticket's incident interval, so a ticket filed after the window end still covers
// an outage inside it without entering any published count.
func computeTicketAggregates(tickets, coverTickets []nhRawTicket, outages []nhOutage, userContrib map[string]string, scopePubkey string) NHTickets {
	agg := NHTickets{Total: len(tickets)}

	// Timing accumulators. resp/reso are INCIDENTS ONLY (maintenance is scheduled
	// ahead, so its created-before-start offset would drag the medians negative);
	// mLead/mDur are the maintenance-only lead/duration figures.
	var resp, reso []float64
	var mLead, mDur []float64
	closedIncidents, closedMaintenance := 0, 0
	selfReported, doubleZeroFiled := 0, 0
	// Incident root-cause tally (enum tokens only). incidentsWithCause is the
	// denominator for each cause's share.
	rootCauseCounts := map[string]int{}
	incidentsWithCause := 0
	for _, t := range tickets {
		// Scoped contributor view: count only tickets ABOUT this contributor (its
		// own contributor_pubkey), not tickets that merely affect its links, which
		// are filed by and belong to other contributors. Applies to every ticket
		// type (incidents and maintenance alike) so a shared-link maintenance filed
		// by another contributor does not leak into this contributor's numbers.
		if scopePubkey != "" &&
			(t.ContributorPubkey == nil || *t.ContributorPubkey != scopePubkey) {
			continue
		}
		if t.Severity != nil {
			switch *t.Severity {
			case "sev1":
				agg.Sev1++
			case "sev2":
				agg.Sev2++
			case "sev3":
				agg.Sev3++
			}
		}
		startT, hasStart := nhParseTicketTime(t.StartAt)
		createdT, hasCreated := nhParseTicketTime(&t.CreatedAt)

		switch t.Type {
		case "incident":
			agg.Incidents++
			if t.RootCause != nil && *t.RootCause != "" {
				rootCauseCounts[nhRootCause(*t.RootCause)]++
				incidentsWithCause++
			}
			if hasStart && hasCreated {
				resp = append(resp, createdT.Sub(startT).Minutes())
			}
			// Self-reported = the incident's creator belongs to the SAME
			// contributor the incident is about (the contributor filed it
			// themselves). A DoubleZero creator (userContrib value ""), a creator
			// not in the registry, or a creator from a different contributor is
			// DoubleZero/other-filed.
			ticketContrib := ""
			if t.ContributorPubkey != nil {
				ticketContrib = *t.ContributorPubkey
			}
			if ticketContrib != "" && t.UserPubkey != "" && userContrib[t.UserPubkey] == ticketContrib {
				selfReported++
			} else {
				doubleZeroFiled++
			}
			if t.Status == "closed" || t.Status == "resolved" {
				closedIncidents++
				if endT, hasEnd := nhParseTicketTime(t.EndAt); hasEnd && hasStart {
					reso = append(reso, endT.Sub(startT).Minutes())
				}
			}
		case "maintenance":
			agg.Maintenance++
			if hasStart && hasCreated {
				mLead = append(mLead, startT.Sub(createdT).Minutes())
			}
			if t.Status == "closed" || t.Status == "resolved" || t.Status == "completed" {
				closedMaintenance++
				if endT, hasEnd := nhParseTicketTime(t.EndAt); hasEnd && hasStart {
					mDur = append(mDur, endT.Sub(startT).Minutes())
				}
			}
		}
	}
	agg.ClosedIncidents = closedIncidents
	agg.ResponseP50Min = medianInt(resp)
	agg.ResolutionP50Min = medianInt(reso)
	agg.SelfReportedCount = selfReported
	agg.DoubleZeroFiledCount = doubleZeroFiled
	if len(userContrib) > 0 {
		agg.SelfReportedPct = pctPtr(selfReported, agg.Incidents)
	}
	agg.MaintenanceLeadP50Min = medianInt(mLead)
	agg.MaintenanceDurationP50Min = medianInt(mDur)
	agg.ClosedMaintenance = closedMaintenance

	// Root-cause breakdown: one row per cause, sorted by count descending
	// (cause name as a stable tiebreak). Pct is the share of incidents that have
	// a recorded cause (self_resolved included).
	agg.RootCauses = make([]NHRootCauseCount, 0, len(rootCauseCounts))
	for cause, n := range rootCauseCounts {
		agg.RootCauses = append(agg.RootCauses, NHRootCauseCount{
			Cause: cause, Count: n, Pct: pctPtr(n, incidentsWithCause),
		})
	}
	sort.Slice(agg.RootCauses, func(i, j int) bool {
		if agg.RootCauses[i].Count != agg.RootCauses[j].Count {
			return agg.RootCauses[i].Count > agg.RootCauses[j].Count
		}
		return agg.RootCauses[i].Cause < agg.RootCauses[j].Cause
	})

	// Coverage: outages matched to any ticket by affected-link code + time overlap.
	const tol = nhTicketCoverTolerance
	byCode := map[string][]nhRawTicket{}
	for _, t := range coverTickets {
		for _, l := range t.AffectedLinks {
			if l.Code != "" {
				byCode[l.Code] = append(byCode[l.Code], t)
			}
		}
	}
	covered := 0
	noTicket := []NHNoTicketOutage{}
	for _, o := range outages {
		isCovered := false
		for _, t := range byCode[o.linkCode] {
			tStart, ok := nhParseTicketTime(t.StartAt)
			if !ok {
				// No parseable start_at: anchor the interval to the outage so a
				// ticket naming this link still covers it, bounded by the ticket's
				// own filing time so it cannot cover an outage that began after it
				// was filed. Every cover ticket has a parseable created_at (see
				// splitTicketsByWindow).
				createdT, okCreated := nhParseTicketTime(&t.CreatedAt)
				if !okCreated || o.start.After(createdT.Add(tol)) {
					continue
				}
				tStart = o.start
			}
			tEnd, ok := nhParseTicketTime(t.EndAt)
			if !ok {
				tEnd = tStart
			}
			// windows overlap within tolerance
			if !tStart.Add(-tol).After(o.end) && !tEnd.Add(tol).Before(o.start) {
				isCovered = true
				break
			}
		}
		if isCovered {
			covered++
			continue
		}
		noTicket = append(noTicket, NHNoTicketOutage{
			LinkPK:   o.linkPK,
			LinkCode: o.linkCode,
			StartTs:  o.start.UTC().Format(time.RFC3339),
			Hours:    math.Round(o.end.Sub(o.start).Hours()*100) / 100,
		})
	}
	// Actionable list: longest unfiled outages first (link code as a stable
	// tiebreak), capped so the public payload stays small.
	sort.Slice(noTicket, func(i, j int) bool {
		if noTicket[i].Hours != noTicket[j].Hours {
			return noTicket[i].Hours > noTicket[j].Hours
		}
		return noTicket[i].LinkCode < noTicket[j].LinkCode
	})
	const noTicketOutageCap = 15
	if len(noTicket) > noTicketOutageCap {
		noTicket = noTicket[:noTicketOutageCap]
	}
	agg.NoTicketOutages = noTicket
	agg.OutageCount = len(outages)
	agg.OutagesWithTicket = covered
	agg.OutagesNoTicket = len(outages) - covered
	agg.NoTicketSharePct = pctPtr(len(outages)-covered, len(outages))
	return agg
}

func medianInt(xs []float64) *int {
	m := medianMinutes(xs)
	if m == nil {
		return nil
	}
	v := int(*m)
	return &v
}

// nhIntervalSeconds picks a time-series bucket size that keeps the point count
// readable for the window (about 100-200 points).
func nhIntervalSeconds(start, end time.Time) int {
	d := end.Sub(start)
	switch {
	case d <= 48*time.Hour:
		return 300 // 5 min
	case d <= 14*24*time.Hour:
		return 3600 // 1 hour
	default:
		return 21600 // 6 hours
	}
}

// fetchThroughputTS returns the per-interval network throughput time series and
// the global peak (bits/sec) over the window. The peak is the max of the
// per-5-min-bucket network bps, so it is the global max of the returned points'
// MaxBps and needs no separate scan (see the peak-throughput headline wiring).
func (a *API) fetchThroughputTS(ctx context.Context, start, end time.Time, sec int, contrib string) ([]NHTsPoint, float64, error) {
	cf, args := "", []any{start, end}
	if contrib != "" {
		cf = " AND device_pk IN (SELECT pk FROM dz_devices_current WHERE contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?))"
		args = append(args, contrib)
	}
	q := `SELECT toStartOfInterval(bucket_ts, INTERVAL ` + strconv.Itoa(sec) + ` SECOND) AS t,
			round(avg(bps), 0), round(max(bps), 0)
		FROM (
			SELECT bucket_ts, sum(sout) AS bps FROM (
				SELECT bucket_ts, device_pk, intf, argMax(avg_out_bps, ingested_at) AS sout
				FROM device_interface_rollup_5m
				WHERE bucket_ts >= ? AND bucket_ts < ? AND user_tunnel_id > 0` + cf + `
				GROUP BY bucket_ts, device_pk, intf
			) GROUP BY bucket_ts
		) GROUP BY t ORDER BY t` + networkHealthQuerySettings + throughputMaxThreads
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []NHTsPoint{}
	var peak float64
	for rows.Next() {
		var t time.Time
		var avg, mx float64
		if err := rows.Scan(&t, &avg, &mx); err != nil {
			return nil, 0, err
		}
		mx = nzFloat(mx)
		if mx > peak {
			peak = mx
		}
		out = append(out, NHTsPoint{T: t.UTC().Format(time.RFC3339), AvgBps: nzFloat(avg), MaxBps: mx})
	}
	return out, peak, rows.Err()
}

func (a *API) fetchOutagesTS(ctx context.Context, start, end time.Time, sec int, contrib string) ([]NHCountPoint, error) {
	args := []any{start, end}
	if contrib != "" {
		args = append(args, contrib)
	}
	q := nhOutEpisodeCTE(contrib != "", false) + `
		SELECT toStartOfInterval(started_at, INTERVAL ` + strconv.Itoa(sec) + ` SECOND) AS t, count()
		FROM epi WHERE dur_s >= ` + nhSustainedOutageSQL + `
		GROUP BY t ORDER BY t` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHCountPoint{}
	for rows.Next() {
		var t time.Time
		var n uint64
		if err := rows.Scan(&t, &n); err != nil {
			return nil, err
		}
		out = append(out, NHCountPoint{T: t.UTC().Format(time.RFC3339), Count: n})
	}
	return out, rows.Err()
}

func (a *API) fetchTopLinks(ctx context.Context, start, end time.Time, contrib string) ([]NHTrafficLink, error) {
	cf, args := "", []any{start, end}
	if contrib != "" {
		cf = " AND link_pk IN (SELECT pk FROM dz_links_current WHERE contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?))"
		args = append(args, contrib)
	}
	q := `SELECT t.link_pk, coalesce(l.code, ''), coalesce(ma.code, ''), coalesce(mz.code, ''),
			coalesce(l.status, ''), round(t.avg_bps / 1e9, 2), round(t.max_bps / 1e9, 2)
		FROM (
			SELECT link_pk, avg(bps) AS avg_bps, max(bps) AS max_bps FROM (
				SELECT link_pk, bucket_ts, sum(sbps) AS bps FROM (
					SELECT link_pk, bucket_ts, link_side, argMax(avg_out_bps, ingested_at) AS sbps
					FROM device_interface_rollup_5m
					WHERE link_pk != '' AND bucket_ts >= ? AND bucket_ts < ?` + cf + `
					GROUP BY link_pk, bucket_ts, link_side
				) GROUP BY link_pk, bucket_ts
			) GROUP BY link_pk ORDER BY avg_bps DESC LIMIT 10
		) AS t
		LEFT JOIN dz_links_current AS l ON l.pk = t.link_pk
		LEFT JOIN dz_devices_current AS da ON da.pk = l.side_a_pk
		LEFT JOIN dz_devices_current AS dzz ON dzz.pk = l.side_z_pk
		LEFT JOIN dz_metros_current AS ma ON ma.pk = da.metro_pk
		LEFT JOIN dz_metros_current AS mz ON mz.pk = dzz.metro_pk
		ORDER BY t.avg_bps DESC` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHTrafficLink{}
	for rows.Next() {
		var r NHTrafficLink
		if err := rows.Scan(&r.LinkPK, &r.LinkCode, &r.SideAMetro, &r.SideZMetro, &r.Status, &r.AvgGbps, &r.MaxGbps); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fetchLatencyLinks compares each committed-RTT link's onchain committed value to
// its measured RTT over the window. avg/ratio are unaffected by rollup duplicate
// rows, so no FINAL/dedup is needed.
func (a *API) fetchLatencyLinks(ctx context.Context, start, end time.Time, contrib string) ([]NHPerfLink, error) {
	cf, args := "", []any{start, end}
	if contrib != "" {
		cf = " AND l.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
		args = append(args, contrib)
	}
	// committed_ms, over_pct, drift_ms and drift_pct are all computed off the
	// EFFECTIVE committed RTT: the IS-IS delay override when set (the value the
	// network actually routes on), else the raw onchain committed_rtt_ns. Same
	// pattern the impactful query uses. raw_committed_ms exposes the pre-override
	// value and `overridden` flags the row. The WHERE guarantees effective > 0,
	// so drift_pct's denominator is always > 0 (the old committed>0 guard is
	// dropped). GROUP BY carries both raw cols so the two extra scalars are valid.
	q := `SELECT r.link_pk, coalesce(l.code, ''), coalesce(ma.code, ''), coalesce(mz.code, ''),
			round(if(l.isis_delay_override_ns > 0, l.isis_delay_override_ns, l.committed_rtt_ns) / 1e6, 3) AS committed_ms,
			round(avg((r.a_avg_rtt_us + r.z_avg_rtt_us) / 2) / 1000, 3),
			round(max(greatest(r.a_avg_rtt_us, r.z_avg_rtt_us)) / 1000, 3),
			round(100 * countIf((r.a_avg_rtt_us + r.z_avg_rtt_us) / 2 > if(l.isis_delay_override_ns > 0, l.isis_delay_override_ns, l.committed_rtt_ns) / 1000) / count(), 1) AS over_pct,
			round(avg((r.a_avg_rtt_us + r.z_avg_rtt_us) / 2) / 1000 - if(l.isis_delay_override_ns > 0, l.isis_delay_override_ns, l.committed_rtt_ns) / 1e6, 2) AS drift_ms,
			round(100 * ((avg((r.a_avg_rtt_us + r.z_avg_rtt_us) / 2) / 1000) / (if(l.isis_delay_override_ns > 0, l.isis_delay_override_ns, l.committed_rtt_ns) / 1e6) - 1), 1) AS drift_pct,
			round(l.committed_rtt_ns / 1e6, 3) AS raw_committed_ms,
			toBool(l.isis_delay_override_ns > 0) AS overridden
		FROM link_rollup_5m r
		INNER JOIN dz_links_current l ON l.pk = r.link_pk
		LEFT JOIN dz_devices_current da ON da.pk = l.side_a_pk
		LEFT JOIN dz_devices_current dzz ON dzz.pk = l.side_z_pk
		LEFT JOIN dz_metros_current ma ON ma.pk = da.metro_pk
		LEFT JOIN dz_metros_current mz ON mz.pk = dzz.metro_pk
		WHERE r.bucket_ts >= ? AND r.bucket_ts < ?
		  AND r.status = 'activated' AND r.isis_down = 0
		  AND if(l.isis_delay_override_ns > 0, l.isis_delay_override_ns, l.committed_rtt_ns) > 0` + cf + `
		GROUP BY r.link_pk, l.code, ma.code, mz.code, l.committed_rtt_ns, l.isis_delay_override_ns
		ORDER BY over_pct DESC, l.code` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHPerfLink{}
	for rows.Next() {
		var r NHPerfLink
		if err := rows.Scan(&r.LinkPK, &r.LinkCode, &r.SideAMetro, &r.SideZMetro,
			&r.CommittedMs, &r.MeasuredAvgMs, &r.MeasuredMaxMs, &r.OverCommittedPct,
			&r.DriftMs, &r.DriftPct, &r.RawCommittedMs, &r.Overridden); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fetchFullestLinks ranks links by how full they are against their
// provisioned bandwidth (l.bandwidth_bps > 0 only). A single dedup pass over device_interface_rollup_5m
// (argMax by ingested_at, per link_pk+bucket_ts+link_side) feeds two views of
// the same data: `peak` is the legacy combined-directions peak (summed across
// sides per bucket, then maxed over the window) driving PeakGbps/UtilPct;
// `util` is the P50/P99-vs-bandwidth pair from the redesign spec (avg/quantile
// per side over the window, then maxed across sides — the busier direction).
func (a *API) fetchFullestLinks(ctx context.Context, start, end time.Time, contrib string) ([]NHCapacityLink, error) {
	cf, args := "", []any{start, end}
	if contrib != "" {
		cf = " AND link_pk IN (SELECT pk FROM dz_links_current WHERE contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?))"
		args = append(args, contrib)
	}
	q := `WITH dedup AS (
			SELECT link_pk, bucket_ts, link_side,
			       argMax(p50_out_bps, ingested_at) AS p50bps,
			       argMax(p99_out_bps, ingested_at) AS p99bps
			FROM device_interface_rollup_5m
			WHERE link_pk != '' AND bucket_ts >= ? AND bucket_ts < ?` + cf + `
			GROUP BY link_pk, bucket_ts, link_side
		),
		peak AS (
			SELECT link_pk, max(bps) AS peak_bps FROM (
				SELECT link_pk, bucket_ts, sum(p99bps) AS bps FROM dedup GROUP BY link_pk, bucket_ts
			) GROUP BY link_pk
		),
		util AS (
			SELECT link_pk, max(p50bps) AS p50bps, max(p99bps) AS p99bps FROM (
				SELECT link_pk, link_side, avg(p50bps) AS p50bps, quantile(0.99)(p99bps) AS p99bps
				FROM dedup GROUP BY link_pk, link_side
			) GROUP BY link_pk
		)
		SELECT peak.link_pk, coalesce(l.code, ''), coalesce(ma.code, ''), coalesce(mz.code, ''),
			round(l.bandwidth_bps / 1e9, 1), round(peak.peak_bps / 1e9, 2),
			round(100 * peak.peak_bps / l.bandwidth_bps, 1) AS util_pct,
			round(100 * util.p50bps / l.bandwidth_bps, 1) AS p50_util,
			round(100 * util.p99bps / l.bandwidth_bps, 1) AS p99_util
		FROM peak
		INNER JOIN util ON util.link_pk = peak.link_pk
		INNER JOIN dz_links_current AS l ON l.pk = peak.link_pk
		LEFT JOIN dz_devices_current AS da ON da.pk = l.side_a_pk
		LEFT JOIN dz_devices_current AS dzz ON dzz.pk = l.side_z_pk
		LEFT JOIN dz_metros_current AS ma ON ma.pk = da.metro_pk
		LEFT JOIN dz_metros_current AS mz ON mz.pk = dzz.metro_pk
		WHERE l.bandwidth_bps > 0
		ORDER BY p99_util DESC LIMIT 10` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHCapacityLink{}
	for rows.Next() {
		var r NHCapacityLink
		if err := rows.Scan(&r.LinkPK, &r.LinkCode, &r.SideAMetro, &r.SideZMetro, &r.BandwidthGbps, &r.PeakGbps, &r.UtilPct, &r.P50Util, &r.P99Util); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// nhSustainedOutageSeconds is the sustained-failure floor: an outage episode
// must span at least two 5-minute buckets (>= 10 minutes) to count as a link
// failure. nhSustainedOutageSQL is the same value for inlining into a query.
const nhSustainedOutageSeconds = 600

var nhSustainedOutageSQL = strconv.Itoa(nhSustainedOutageSeconds)

// nhOutageBucketPredicate is the shared definition of an outage bucket: an
// activated link_rollup_5m bucket that is IS-IS down or above the loss
// threshold. alias qualifies the column references ("" for an unaliased FROM).
func nhOutageBucketPredicate(alias string) string {
	c := ""
	if alias != "" {
		c = alias + "."
	}
	return c + "status = 'activated' AND (" + c + "isis_down = 1 OR greatest(" + c + "a_loss_pct, " + c + "z_loss_pct) >= 10)"
}

// nhOutEpisodeCTE builds the shared outage-episode CTE over [start, end): outage
// buckets (nhOutageBucketPredicate, non-provisioning, FINAL-deduped) collapsed
// into contiguous episodes by the row_number island trick. epi carries one row
// per episode: link_pk, started_at, ended_at and dur_s = buckets*300. scoped adds
// a contributor-code filter INSIDE the CTE; byLinkPKs adds an explicit pk-list
// filter. Placeholders bind in order: start, end, then the contributor code when
// scoped, then the pk list when byLinkPKs. Callers gate on
// dur_s >= nhSustainedOutageSeconds for the sustained set; the ungated CTE also
// carries the sub-floor flaps the duration histogram counts.
func nhOutEpisodeCTE(scoped, byLinkPKs bool) string {
	join, filter := "", ""
	if scoped {
		join = " JOIN dz_links_current sl ON sl.pk = r.link_pk"
		filter = " AND sl.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	if byLinkPKs {
		filter += " AND r.link_pk IN ?"
	}
	return `WITH outb AS (
			SELECT r.link_pk AS link_pk, r.bucket_ts AS bucket_ts
			FROM link_rollup_5m r FINAL` + join + `
			WHERE r.bucket_ts >= ? AND r.bucket_ts < ? AND r.provisioning = false
			  AND ` + nhOutageBucketPredicate("r") + filter + `),
		isl AS (
			SELECT link_pk, bucket_ts,
			  bucket_ts - toIntervalSecond(row_number() OVER (PARTITION BY link_pk ORDER BY bucket_ts) * 300) AS grp
			FROM outb),
		epi AS (
			SELECT link_pk, min(bucket_ts) AS started_at,
			  max(bucket_ts) + toIntervalSecond(300) AS ended_at,
			  count() * 300 AS dur_s
			FROM isl GROUP BY link_pk, grp)`
}

// nhAvailStateCountIfs is the shared bucket-classification snippet for the
// availability 3-way split, used by fetchLinkAvailability, fetchDeviceAvailability,
// and the single-link view. A bucket is 5 minutes of link_rollup_5m. See the
// NHAvailability doc comment for the definition of each state; provisioning
// buckets are filtered out by the caller's WHERE clause before this runs.
const nhAvailStateCountIfs = `countIf(status = 'activated' AND isis_down = 0 AND greatest(a_loss_pct, z_loss_pct) < 10) AS avail_buckets,
	       countIf(status IN ('soft-drained', 'hard-drained')) AS drained_buckets,
	       countIf(status = 'activated' AND (isis_down = 1 OR greatest(a_loss_pct, z_loss_pct) >= 10)) AS outage_buckets`

// fetchLinkAvailability ranks activated links by the 3-way availability split
// (least available first) over the window: Available / Drained / Outage bucket
// shares of link_rollup_5m, excluding provisioning buckets entirely. Links with
// no classified buckets in the window (a full gap) are excluded rather than
// counted as 0% available.
func (a *API) fetchLinkAvailability(ctx context.Context, start, end time.Time, contrib string) ([]NHAvailability, error) {
	scope := ""
	args := []any{start, end}
	if contrib != "" {
		scope = " AND l.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	q := `WITH agg AS (
		SELECT link_pk, ` + nhAvailStateCountIfs + `
		FROM link_rollup_5m FINAL
		WHERE bucket_ts >= ? AND bucket_ts < ? AND provisioning = false
		GROUP BY link_pk HAVING (avail_buckets + drained_buckets + outage_buckets) > 0)
	  SELECT l.pk, l.code,
	         concat(ma.code, ' ↔ ', mz.code) AS metros,
	         round(100*avail_buckets/(avail_buckets+drained_buckets+outage_buckets), 2) AS avail_pct,
	         round(100*drained_buckets/(avail_buckets+drained_buckets+outage_buckets), 2) AS drained_pct,
	         round(100*outage_buckets/(avail_buckets+drained_buckets+outage_buckets), 2) AS outage_pct,
	         round(avail_buckets*5/60, 2) AS avail_hours,
	         round(outage_buckets*5/60, 2) AS outage_hours,
	         round(drained_buckets*5/60, 2) AS drained_hours
	  FROM agg
	  JOIN dz_links_current l ON l.pk = agg.link_pk
	  JOIN dz_devices_current da ON da.pk = l.side_a_pk
	  JOIN dz_devices_current dz ON dz.pk = l.side_z_pk
	  JOIN dz_metros_current ma ON ma.pk = da.metro_pk
	  JOIN dz_metros_current mz ON mz.pk = dz.metro_pk
	  WHERE l.status IN ('activated', 'soft-drained', 'hard-drained')` + scope + `
	  ORDER BY avail_pct ASC LIMIT 10` + networkHealthQuerySettings
	if contrib != "" {
		args = append(args, contrib)
	}
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHAvailability{}
	for rows.Next() {
		var r NHAvailability
		if err := rows.Scan(&r.PK, &r.Code, &r.Metros, &r.AvailPct, &r.DrainedPct, &r.OutagePct, &r.AvailHours, &r.OutageHours, &r.DrainedHours); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fetchDeviceAvailability ranks activated devices by DEVICE REACHABILITY (least
// reachable / most fault time first). Unlike the link panel, a device is
// classified once per 5-min bucket from the states of ALL its links: AVAILABLE
// whenever at least one link is working, OUTAGE only when it has a fault-down
// link and no working link (the network genuinely cannot reach it), DRAINED
// only when it is out purely for maintenance (drained link(s), none working, no
// fault). So a single link down while others are up does not lower the device,
// and a maintenance-only device is not flagged as least available. Every
// device-bucket falls into exactly one state, so the three percentages sum to
// ~100. Ranked by outage (fault) buckets descending.
func (a *API) fetchDeviceAvailability(ctx context.Context, start, end time.Time, contrib string) ([]NHAvailability, error) {
	devScope := ""
	args := []any{start, end}
	if contrib != "" {
		devScope = " AND d.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	q := `WITH lb AS (
		SELECT link_pk, bucket_ts,
		  (status = 'activated' AND isis_down = 0 AND greatest(a_loss_pct, z_loss_pct) < 10) AS working,
		  (status IN ('soft-drained','hard-drained')) AS drained,
		  (status = 'activated' AND (isis_down = 1 OR greatest(a_loss_pct, z_loss_pct) >= 10)) AS fault
		FROM link_rollup_5m FINAL
		WHERE bucket_ts >= ? AND bucket_ts < ? AND provisioning = false),
	  dev_bucket AS (
		SELECT dev, bucket_ts,
		  max(working) AS has_working, max(fault) AS has_fault, max(drained) AS has_drained
		FROM (
		  SELECT l.side_a_pk AS dev, lb.bucket_ts AS bucket_ts, lb.working AS working, lb.fault AS fault, lb.drained AS drained
		  FROM lb JOIN dz_links_current l ON l.pk = lb.link_pk WHERE l.status = 'activated'
		  UNION ALL
		  SELECT l.side_z_pk AS dev, lb.bucket_ts AS bucket_ts, lb.working AS working, lb.fault AS fault, lb.drained AS drained
		  FROM lb JOIN dz_links_current l ON l.pk = lb.link_pk WHERE l.status = 'activated')
		GROUP BY dev, bucket_ts),
	  dev_class AS (
		SELECT dev,
		  has_working AS avail,
		  (NOT has_working AND has_fault) AS outage,
		  (NOT has_working AND NOT has_fault AND has_drained) AS drained
		FROM dev_bucket)
	  SELECT d.pk, d.code,
	         round(100*countIf(avail)/(countIf(avail)+countIf(outage)+countIf(drained)), 2) AS avail_pct,
	         round(100*countIf(drained)/(countIf(avail)+countIf(outage)+countIf(drained)), 2) AS drained_pct,
	         round(100*countIf(outage)/(countIf(avail)+countIf(outage)+countIf(drained)), 2) AS outage_pct,
	         round(countIf(avail)*5/60, 2) AS avail_hours,
	         round(countIf(outage)*5/60, 2) AS outage_hours,
	         round(countIf(drained)*5/60, 2) AS drained_hours
	  FROM dev_class JOIN dz_devices_current d ON d.pk = dev_class.dev
	  WHERE 1=1` + devScope + `
	  GROUP BY d.pk, d.code HAVING (countIf(avail)+countIf(outage)+countIf(drained)) > 0
	  ORDER BY countIf(outage) DESC LIMIT 10` + networkHealthQuerySettings
	if contrib != "" {
		args = append(args, contrib)
	}
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHAvailability{}
	for rows.Next() {
		var r NHAvailability
		if err := rows.Scan(&r.PK, &r.Code, &r.AvailPct, &r.DrainedPct, &r.OutagePct, &r.AvailHours, &r.OutageHours, &r.DrainedHours); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fetchOutageSummary totals outage counts, combined outage-hours, and distinct
// affected entities over the window, split by link vs device. Episodes are
// derived from link_rollup_5m over the full window (the incident views cap to
// ~8 days), using the unified outage definition shared across the page.
func (a *API) fetchOutageSummary(ctx context.Context, start, end time.Time, contrib string) (*NHOutageSummary, error) {
	devScopeFilter := ""
	linkArgs := []any{start, end}
	devArgs := []any{start, end}
	if contrib != "" {
		devScopeFilter = " AND d.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
		linkArgs = append(linkArgs, contrib)
		devArgs = append(devArgs, contrib)
	}
	var s NHOutageSummary
	var linkHours float64
	// Link outages: distinct episodes from link_rollup_5m over the full window.
	linkQ := nhOutEpisodeCTE(contrib != "", false) + `
		SELECT count(), round(sum(least(dur_s, 86400)) / 3600, 0), uniqExact(link_pk)
		FROM epi WHERE dur_s >= ` + nhSustainedOutageSQL + networkHealthQuerySettings
	if err := a.envDB(ctx).QueryRow(ctx, linkQ, linkArgs...).
		Scan(&s.LinkOutages, &linkHours, &s.LinksAffected); err != nil {
		return nil, err
	}
	// Device outages: distinct episodes per activated device, aggregating each
	// device's link outage buckets across both endpoints (mirrors
	// fetchDowntimeDevices).
	devQ := `WITH outb AS (
			SELECT link_pk, bucket_ts
			FROM link_rollup_5m FINAL
			WHERE bucket_ts >= ? AND bucket_ts < ? AND provisioning = false
			  AND ` + nhOutageBucketPredicate("") + `),
		dev_buckets AS (
			SELECT l.side_a_pk AS dev, o.bucket_ts AS bucket_ts
			FROM outb o JOIN dz_links_current l ON l.pk = o.link_pk WHERE l.status = 'activated'
			UNION DISTINCT
			SELECT l.side_z_pk AS dev, o.bucket_ts AS bucket_ts
			FROM outb o JOIN dz_links_current l ON l.pk = o.link_pk WHERE l.status = 'activated'),
		isl AS (
			SELECT dev, bucket_ts,
			  bucket_ts - toIntervalSecond(row_number() OVER (PARTITION BY dev ORDER BY bucket_ts) * 300) AS grp
			FROM dev_buckets),
		epi AS (SELECT dev, grp FROM isl GROUP BY dev, grp HAVING count() >= 2)
		SELECT count(), uniqExact(epi.dev)
		FROM epi
		JOIN dz_devices_current d ON d.pk = epi.dev
		WHERE d.status = 'activated'` + devScopeFilter + networkHealthQuerySettings
	if err := a.envDB(ctx).QueryRow(ctx, devQ, devArgs...).
		Scan(&s.DeviceOutages, &s.DevicesAffected); err != nil {
		return nil, err
	}
	s.OutageHours = nzFloat(linkHours)
	return &s, nil
}

// fetchDowntimeLinks ranks links by total outage-hours over the window (top 10),
// for the "most downtime" view. Outage-hours are counted directly from
// link_rollup_5m (full retention, no double-counting across incident types) using
// the same outage definition as the availability panels: an activated link that
// is isis_down or has >=10% loss in a 5-min bucket. Outages = the number of
// distinct contiguous outage episodes (gaps between outage buckets split them),
// found with the standard row_number island trick.
func (a *API) fetchDowntimeLinks(ctx context.Context, start, end time.Time, contrib string) ([]NHDowntimeRow, error) {
	args := []any{start, end}
	if contrib != "" {
		args = append(args, contrib)
	}
	q := nhOutEpisodeCTE(contrib != "", false) + `,
	  tot AS (SELECT link_pk, sum(dur_s) / 300 AS buckets, count() AS episodes
	          FROM epi WHERE dur_s >= ` + nhSustainedOutageSQL + ` GROUP BY link_pk)
	  SELECT l.pk, l.code, concat(ma.code, ' ↔ ', mz.code) AS metros,
	         tot.episodes AS outages, round(tot.buckets * 5 / 60, 1) AS hrs
	  FROM tot
	  JOIN dz_links_current l ON l.pk = tot.link_pk
	  JOIN dz_devices_current da ON da.pk = l.side_a_pk
	  JOIN dz_devices_current dz ON dz.pk = l.side_z_pk
	  JOIN dz_metros_current ma ON ma.pk = da.metro_pk
	  JOIN dz_metros_current mz ON mz.pk = dz.metro_pk
	  WHERE l.status = 'activated'
	  ORDER BY hrs DESC LIMIT 10` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHDowntimeRow{}
	for rows.Next() {
		var r NHDowntimeRow
		if err := rows.Scan(&r.PK, &r.Code, &r.Metros, &r.Outages, &r.Hours); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fetchDowntimeDevices ranks devices by total outage-hours over the window (top
// 10). Device version of fetchDowntimeLinks: it aggregates each device's link
// outage buckets across both endpoints (UNION DISTINCT so a bucket where two of a
// device's links are down counts once), then counts hours and episodes the same
// way. Sourced from link_rollup_5m rather than device_incidents_v, which only
// retains ~2 days and so left this panel near-empty over a 30-day window.
func (a *API) fetchDowntimeDevices(ctx context.Context, start, end time.Time, contrib string) ([]NHDowntimeRow, error) {
	devScope, args := "", []any{start, end}
	if contrib != "" {
		devScope = " AND d.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	q := `WITH outb AS (
		SELECT link_pk, bucket_ts
		FROM link_rollup_5m FINAL
		WHERE bucket_ts >= ? AND bucket_ts < ? AND provisioning = false
		  AND ` + nhOutageBucketPredicate("") + `),
	  dev_buckets AS (
		SELECT l.side_a_pk AS dev, o.bucket_ts AS bucket_ts
		FROM outb o JOIN dz_links_current l ON l.pk = o.link_pk WHERE l.status = 'activated'
		UNION DISTINCT
		SELECT l.side_z_pk AS dev, o.bucket_ts AS bucket_ts
		FROM outb o JOIN dz_links_current l ON l.pk = o.link_pk WHERE l.status = 'activated'),
	  isl AS (
		SELECT dev, bucket_ts,
		  bucket_ts - toIntervalSecond(row_number() OVER (PARTITION BY dev ORDER BY bucket_ts) * 300) AS grp
		FROM dev_buckets),
	  ep AS (SELECT dev, grp, count() AS b FROM isl GROUP BY dev, grp HAVING b >= 2),
	  epi AS (SELECT dev, sum(b) AS buckets, count() AS episodes FROM ep GROUP BY dev)
	  SELECT d.pk, d.code, m.code AS metro, epi.episodes AS outages, round(epi.buckets * 5 / 60, 1) AS hrs
	  FROM epi
	  JOIN dz_devices_current d ON d.pk = epi.dev
	  JOIN dz_metros_current m ON m.pk = d.metro_pk
	  WHERE d.status = 'activated'` + devScope + `
	  ORDER BY hrs DESC LIMIT 10` + networkHealthQuerySettings
	if contrib != "" {
		args = append(args, contrib)
	}
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHDowntimeRow{}
	for rows.Next() {
		var r NHDowntimeRow
		if err := rows.Scan(&r.PK, &r.Code, &r.Metros, &r.Outages, &r.Hours); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (a *API) fetchErrorHotspots(ctx context.Context, start, end time.Time, contrib string) ([]NHErrorHotspot, error) {
	cf, args := "", []any{start, end}
	if contrib != "" {
		cf = " AND d.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
		args = append(args, contrib)
	}
	q := `SELECT r.device_pk, any(coalesce(d.code, '')),
			sum(r.in_errors + r.out_errors + r.in_fcs_errors + r.in_discards + r.out_discards) AS errs,
			sum(r.carrier_transitions) AS flaps
		FROM device_interface_rollup_5m AS r
		LEFT JOIN dz_devices_current AS d ON d.pk = r.device_pk
		WHERE r.bucket_ts >= ? AND r.bucket_ts < ?` + cf + `
		GROUP BY r.device_pk HAVING errs > 0 OR flaps > 0
		ORDER BY errs DESC LIMIT 10` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHErrorHotspot{}
	for rows.Next() {
		var r NHErrorHotspot
		if err := rows.Scan(&r.DevicePK, &r.DeviceCode, &r.Errors, &r.CarrierFlaps); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fetchDeviceSlots returns the fullest activated devices by seat usage: unicast
// users plus multicast subscribers/publishers against the device's total
// max_users cap. Per-role maxes (max_unicast_users, max_multicast_*) are
// 0/unset on most devices, so max_users is used as the single denominator;
// UsedPct is nil (via SQL NULL) when max_users is 0.
func (a *API) fetchDeviceSlots(ctx context.Context, contrib string) ([]NHDeviceSlots, error) {
	cf, args := "", []any{}
	if contrib != "" {
		cf = " AND contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
		args = append(args, contrib)
	}
	q := `SELECT pk, code, unicast_users_count, multicast_subscribers_count, multicast_publishers_count,
			max_users,
			if(max_users > 0, round(100*(unicast_users_count+multicast_subscribers_count+multicast_publishers_count)/max_users,0), NULL) AS used_pct
		FROM dz_devices_current
		WHERE status='activated'` + cf + `
		ORDER BY used_pct DESC NULLS LAST LIMIT 12` + networkHealthQuerySettings
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHDeviceSlots{}
	for rows.Next() {
		var s NHDeviceSlots
		var unicast, mcastSub, mcastPub uint16
		var maxUsers int32
		if err := rows.Scan(&s.PK, &s.Code, &unicast, &mcastSub, &mcastPub, &maxUsers, &s.UsedPct); err != nil {
			return nil, err
		}
		s.Unicast, s.McastSub, s.McastPub = uint64(unicast), uint64(mcastSub), uint64(mcastPub)
		if maxUsers > 0 {
			s.MaxUsers = uint64(maxUsers)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// fetchDiaInterfaces returns the busiest Direct Internet Access interfaces by
// p99 traffic, with utilization measured against interface bandwidth (physical
// port speed); CIR is returned as an informational figure only. DIA
// interfaces are almost all status='unlinked' (pre-activation onchain state),
// so unlike other device-role fetchers this does NOT filter status='activated'.
//
// The dedup CTE restricts device_interface_rollup_5m to the (device_pk, intf)
// pairs that are actually DIA (dia_intfs) before the argMax/GROUP BY dedup.
// DIA interfaces are a small slice of all interfaces (~100 of ~550 network-wide),
// so without this the dedup aggregates every interface on every device over the
// whole window before the outer query's dia_type='dia' filter ever applies,
// which was measured OOMing (code 241, AggregatingTransform) at the 2 GiB cap
// on the live 30d default window. Restricting the join key first is a no-op on
// the result (the outer WHERE i.dia_type='dia' already discards everything
// else) and confirmed row-for-row identical against the unfiltered query on
// shorter windows.
func (a *API) fetchDiaInterfaces(ctx context.Context, start, end time.Time, contrib string) ([]NHDiaInterface, error) {
	cf, args := "", []any{start, end}
	if contrib != "" {
		cf = " AND dev.contributor_pk IN (SELECT pk FROM dz_contributors_current WHERE code = ?)"
	}
	q := `WITH dia_intfs AS (
			SELECT device_pk, intf FROM dz_device_interfaces_current WHERE dia_type = 'dia'
		),
		dedup AS (
			SELECT device_pk, intf, bucket_ts,
			       argMax(p50_out_bps, ingested_at) AS p50,
			       argMax(p99_out_bps, ingested_at) AS p99
			FROM device_interface_rollup_5m
			WHERE bucket_ts >= ? AND bucket_ts < ?
			  AND (device_pk, intf) IN (SELECT device_pk, intf FROM dia_intfs)
			GROUP BY device_pk, intf, bucket_ts
		),
		cnt AS (
			SELECT device_pk, intf, avg(p50) AS p50, quantile(0.99)(p99) AS p99
			FROM dedup GROUP BY device_pk, intf
		)
		SELECT dev.pk, dev.code, i.intf,
			round(i.bandwidth/1e9,1) AS port_g, round(i.cir/1e9,1) AS cir_g,
			round(c.p50/1e9,2) AS p50_g, round(c.p99/1e9,2) AS p99_g,
			if(i.bandwidth>0, round(100*c.p99/i.bandwidth,0), NULL) AS util,
			'port' AS denom
		FROM dz_device_interfaces_current i
		INNER JOIN cnt c ON c.device_pk=i.device_pk AND c.intf=i.intf
		JOIN dz_devices_current dev ON dev.pk=i.device_pk
		WHERE i.dia_type='dia'` + cf + `
		ORDER BY c.p99 DESC LIMIT 12` + networkHealthQuerySettings
	if contrib != "" {
		args = append(args, contrib)
	}
	rows, err := a.envDB(ctx).Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []NHDiaInterface{}
	for rows.Next() {
		var d NHDiaInterface
		if err := rows.Scan(&d.DevicePK, &d.Device, &d.Intf, &d.PortGbps, &d.CirGbps, &d.P50Gbps, &d.P99Gbps, &d.UtilPct, &d.Denom); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
