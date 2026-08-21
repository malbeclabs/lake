package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/utils/pkg/logger"
)

const (
	TaskQueue  = "api-page-cache"
	WorkflowID = "api-page-cache"

	fastRefreshInterval = 3 * time.Second

	// Escalation thresholds: WARN below, ERROR at/above. Transient causes
	// (connection blips, timeouts, rate limits) self-heal, so they get the
	// higher threshold — only a sustained run is worth paging on. These match
	// the logger.Escalator defaults; named here because tests and the
	// deadline-sentinel comments reference them.
	errorAfterFailures          = logger.DefaultErrorAfter
	transientErrorAfterFailures = logger.DefaultTransientErrorAfter

	// slowRefreshThreshold surfaces per-entry duration at INFO when a single
	// cache refresh (query + write) takes at least this long. Normal entries
	// finish in well under a second; anything above this is worth flagging
	// when the activity is running close to its StartToCloseTimeout budget.
	slowRefreshThreshold = 10 * time.Second

	// retryAttemptMargin pads a failed attempt's observed cost when checking
	// whether the parent activity still has room for a second attempt (see
	// retryBudget). It absorbs the variance between two runs of the same query;
	// a normal entry refreshes well inside it (see slowRefreshThreshold).
	retryAttemptMargin = 10 * time.Second

	// Defaults for the per-environment load-shaping knobs (see loadRefreshConfig).
	// Prod runs these against ClickHouse Cloud; staging overrides them lower/longer
	// to spread load across its smaller self-hosted ClickHouse.
	defaultRefreshInterval    = 30 * time.Second
	defaultRefreshConcurrency = 8
	defaultActivityTimeout    = 3 * time.Minute
	// defaultRefreshTimeout is the per-entry refresh context deadline, applied
	// when a cacheEntry does not set its own timeout. It is also the ceiling for
	// every entry in the slow batch (see TestEntryTimeoutsFitTheirActivityBudget):
	// an entry that needs longer belongs in heavyEntries.
	defaultRefreshTimeout = 60 * time.Second

	// nhHeavyRefreshTimeout is the per-entry budget for the two heavy Network
	// Health groups. Their queries run under max_execution_time = 170 (see
	// handlers' networkHealthDeferredQuerySettings) plus decode and write.
	nhHeavyRefreshTimeout = 180 * time.Second
	// heavyActivityHeadroom is what RefreshHeavyCaches keeps above its slowest
	// entry, so an entry that exhausts its own budget records its own failure
	// instead of being cancelled by the activity deadline first.
	heavyActivityHeadroom = 60 * time.Second
	// heavyActivityTimeout is RefreshHeavyCaches's StartToCloseTimeout.
	heavyActivityTimeout = nhHeavyRefreshTimeout + heavyActivityHeadroom

	// continueAsNewTargetWindow bounds workflow history: PageCacheWorkflow
	// continues-as-new after roughly this much wall-clock regardless of the
	// refresh interval, so a longer interval can't inflate per-run history toward
	// Temporal's ~10k-event soft / ~51k hard limits.
	continueAsNewTargetWindow = 30 * time.Minute

	// Sane bounds for the knobs; out-of-range env values are clamped (with a warn).
	minRefreshInterval    = 5 * time.Second
	maxRefreshInterval    = 10 * time.Minute
	minRefreshConcurrency = 1
	maxRefreshConcurrency = 32
	minActivityTimeout    = 30 * time.Second
	maxActivityTimeout    = 10 * time.Minute
)

// PageCacheParams carries the load-shaping config into PageCacheWorkflow as an
// argument, so the values are recorded in workflow history and replay is
// deterministic by construction. This matters because the workflow's fast-refresh
// loop emits a command count that depends on RefreshInterval: reading it from a
// mutable package var would let a rolling deploy that changed the value replay an
// in-flight run with a different value → history divergence → the workflow task
// fails forever and the page cache silently freezes. As an argument, a config
// change instead starts a fresh run (Start terminates + restarts) with the new
// value, and any in-flight replay uses the value from its own history.
type PageCacheParams struct {
	RefreshInterval        time.Duration
	ActivityTimeout        time.Duration
	ContinueAsNewThreshold int
}

// withDefaults normalizes zero/nonsensical values to defaults. Applied at the top
// of the workflow so it stays deterministic even if started without params.
func (p PageCacheParams) withDefaults() PageCacheParams {
	if p.RefreshInterval <= 0 {
		p.RefreshInterval = defaultRefreshInterval
	}
	if p.ActivityTimeout <= 0 {
		p.ActivityTimeout = defaultActivityTimeout
	}
	if p.ContinueAsNewThreshold <= 0 {
		// Floor of 2: the loop skips the heavy refresh on its final iteration, so a
		// single-iteration window would never run one at all.
		p.ContinueAsNewThreshold = max(int(continueAsNewTargetWindow/p.RefreshInterval), 2)
	}
	return p
}

// Sentinels recorded when a refresh is cut short by its activity's deadline (not
// a worker shutdown). They differ by cadence so escalation matches the cause:
var (
	// errBatchDeadline: the full RefreshCaches batch ran out of its
	// StartToCloseTimeout before this entry ran — deterministic tail starvation.
	// Phrased to avoid dberror's transient keywords so it escalates at
	// errorAfterFailures, making starvation visible.
	errBatchDeadline = errors.New("page-cache batch budget exhausted before refresh completed")

	// errFastRefreshDeadline: a fast-cadence (RefreshLatestCaches) entry hit the
	// fast activity's StartToCloseTimeout — ordinary, self-healing ClickHouse
	// contention. Phrased as a timeout so dberror classifies it transient and it
	// escalates only at transientErrorAfterFailures (a blip shouldn't page).
	errFastRefreshDeadline = errors.New("page-cache fast refresh deadline exceeded")

	// errHeavyRefreshDeadline: a heavy Network Health entry (RefreshHeavyCaches)
	// hit the heavy activity's StartToCloseTimeout, i.e. it overran its own
	// budget plus heavyActivityHeadroom. Phrased as a timeout so it escalates at
	// transientErrorAfterFailures, matching errFastRefreshDeadline: the entry is
	// slow, not starved, and the last good blob is still served.
	errHeavyRefreshDeadline = errors.New("page-cache heavy refresh deadline exceeded")
)

// cacheEntry defines a single cache key to refresh. every sets a refresh
// cadence: the minimum wall-clock spacing between two refreshes of this key
// (zero, the default, means every cycle). Keeping the cadence on the entry
// itself — rather than in a side map keyed by string — removes the drift hazard
// where a renamed key silently reverts to every-cycle refresh.
//
// A duration rather than a cycle count because the cycle period is not a
// constant: it is configured per environment (prod ~68s, staging ~4 min) and
// shortens as entries leave the every-cycle path, so one count would mean a
// different staleness in each. It is a floor, not a period — the gate is checked
// once per cycle, so actual spacing lands in [every, every + one cycle period).
type cacheEntry struct {
	name  string
	key   string
	every time.Duration
	// dayAligned entries read handlers.DefaultNetworkHealthWindow and are also due
	// once their blob predates the current window's end, whatever every says.
	// Otherwise groups on different cadences describe different windows after
	// midnight UTC, and the frontend blanks its traffic-weighted availability stat
	// when two payloads disagree (deriveAvailability, network-health-reporting-page.tsx).
	dayAligned bool
	fn         func(ctx context.Context) (any, error)
	// timeout overrides the per-refresh context deadline. Zero means the default
	// (see refresh). Only heavyEntries set it: they run in their own activity,
	// which is the one with budget for a timeout above defaultRefreshTimeout.
	timeout time.Duration
}

// Per-entry refresh cadences (see cacheEntry.every). Each answers how stale the
// view may be, not how expensive it is.
const (
	// publisher_check reads shredder.publisher_shred_stats, the heaviest recurring
	// query on the shared ClickHouse, but its data moves on epoch timescales (~2 days).
	publisherCheckInterval = 2 * time.Minute

	// Matches the UI's ~60s poll and absorbs the external ~10s poller that
	// previously ran the query ~6,500×/day.
	validatorsListingInterval = 60 * time.Second

	// Two full all-pairs path computations over two graphs, keyed off link topology
	// tags — which change when someone changes them and not otherwise.
	algoDivergenceInterval = 5 * time.Minute

	// The only Network Health group with point-in-time tiles (telemetry freshness,
	// ISIS state). Those are a few CPU-seconds of the group's ~200, so the cadence
	// is short enough for them and still sheds its two 30-day scans.
	networkHealthOverviewInterval = 5 * time.Minute

	// The purely-historical Network Health groups: DefaultNetworkHealthWindow is
	// day-aligned at both ends, so within a day only late-arriving rollup rows and
	// advancing FINAL dedup state change their answer.
	networkHealthHistoryInterval = 30 * time.Minute

	// A long-window comparison table, not a live tail — and the most expensive view
	// the page cache reads, scanned twice per cycle (also by overview's
	// latency_vs_internet panel).
	latencyComparisonInterval = 30 * time.Minute

	// The two 24h aggregates, which have the worst read-per-query of anything the
	// API runs. The live tail is unaffected — separate edge_scoreboard:latest*
	// entries on the fast cadence serve it.
	edgeScoreboardInterval = 5 * time.Minute
)

// cacheAgesEscalationKey keys the gate's own age read, so a gate outage is one
// alert rather than one per entry.
const cacheAgesEscalationKey = "page_cache:ages"

// cacheAgeReadTimeout bounds the gate's page_cache read, which runs at the head of
// every batch on the pgx pool the request path shares. Unbounded, a saturated pool
// would consume the whole activity budget before any refresh started, recording
// every entry as batch starvation — which pages. Slower than this is fail-open.
const cacheAgeReadTimeout = 5 * time.Second

// dueForRefresh reports whether an entry is due. A zero updatedAt means the key
// has never been written, which is always due.
func dueForRefresh(e cacheEntry, updatedAt, now, windowEnd time.Time) bool {
	if e.every <= 0 || updatedAt.IsZero() {
		return true
	}
	// A blob written before the current window's end describes yesterday's
	// window, whatever the cadence says (see cacheEntry.dayAligned).
	if e.dayAligned && updatedAt.Before(windowEnd) {
		return true
	}
	return now.Sub(updatedAt) >= e.every
}

// dueEntries splits a batch by cadence, from one batched read of
// page_cache.updated_at. A batch whose entries set no cadence reads nothing.
//
// A failed refresh does not write, so updated_at does not advance and the entry is
// due again next cycle: the refresh's own escalation counters keep running at the
// cycle rate however large every is.
//
// Fail-open on a read error or a slow read, so a Postgres problem cannot freeze
// every cadenced entry. A sustained one still pages: it silently reverts every
// cadence to every-cycle refresh, and nothing else reports that.
func (a *Activities) dueEntries(ctx context.Context, entries []cacheEntry) (due []cacheEntry, skipped int) {
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.every > 0 {
			keys = append(keys, e.key)
		}
	}
	if len(keys) == 0 {
		return entries, 0
	}

	readCtx, cancel := context.WithTimeout(ctx, cacheAgeReadTimeout)
	ages, err := a.API.PageCacheAges(readCtx, keys)
	cancel()
	if err != nil {
		a.esc.Fail(a.Log, cacheAgesEscalationKey, "cache cadence age read failed; refreshing every entry", "error", err)
		return entries, 0
	}
	a.esc.Reset(cacheAgesEscalationKey)

	now := time.Now()
	_, windowEnd := handlers.DefaultNetworkHealthWindow()
	due = make([]cacheEntry, 0, len(entries))
	for _, e := range entries {
		if dueForRefresh(e, ages[e.key], now, windowEnd) {
			due = append(due, e)
			continue
		}
		skipped++
	}
	return due, skipped
}

// Activities holds the logger and API deps for the refresh activity.
type Activities struct {
	Log *slog.Logger
	API *handlers.API
	// RefreshConcurrency bounds how many cache entries refresh at once. Set from
	// config in Start; 0 falls back to defaultRefreshConcurrency. Activity-side
	// only (no replay concern), so it stays a plain field rather than a workflow
	// argument.
	RefreshConcurrency int
	// esc escalates consecutive refresh failures per cache key from WARN to
	// ERROR (zero value: errorAfterFailures / transientErrorAfterFailures).
	esc logger.Escalator
	// degradedEsc escalates a Network Health refresh that succeeded but could not
	// compute every panel. Set in the Activities literal (see pagecache.go): a
	// degraded panel is a served-degraded condition, not a terminal one, so it
	// escalates far later than a failed refresh.
	degradedEsc logger.Escalator
	writeMu     sync.Mutex // serializes WritePageCache calls to avoid Postgres OOM from concurrent large JSONB upserts
}

// nhDegradedErrorAfter keeps a degraded Network Health panel off the Escalator's
// default count of 3: the blob is still written, so a brief hole must not page.
// It only binds an every-cycle group — at any cadence the window below wins.
const nhDegradedErrorAfter = 20

// nhDegradedErrorWindow is how long a run of degraded refreshes must last before
// escalating, whichever threshold is crossed first. A count no longer describes a
// duration here: a degraded refresh writes its blob (so the healthy panels stay
// fresh), which advances updated_at and sleeps the entry for a full cadence — 20
// counts would be 10 hours at networkHealthHistoryInterval. It is the only counter
// with that problem; a failed refresh writes nothing, so it is due again next cycle.
//
// Escalation still cannot beat the sampling rate: a run's elapsed time is only
// measured when the entry next refreshes, so a dark panel pages on the first
// degraded refresh at or after this long from the run's start — ~10 minutes for
// the overview group, ~30 for the 30-minute ones. Setting it below one cadence
// buys nothing.
const nhDegradedErrorWindow = 10 * time.Minute

// nhDegraded escalates a partially-failed Network Health refresh under its own
// counter, keyed separately from the entry's query leg. The blob is still
// written, so the healthy panels stay fresh; a panel that keeps failing pages
// once the streak crosses the threshold.
func (a *Activities) nhDegraded(name, key string, panels []string) {
	if len(panels) == 0 {
		a.degradedEsc.Reset(key + ":degraded")
		return
	}
	a.degradedEsc.Fail(a.Log, key+":degraded", "cache refresh degraded", "cache", name, "panels", panels)
}

// nhOutcome turns one Network Health group's payload into a refresh outcome.
// errMsg is the group's Error, meaning a critical panel failed: it becomes a
// refreshError so the last good blob is kept instead of caching zeros, and the
// entry's own escalator owns that alert. Otherwise the blob is written and any
// degraded panels escalate under their own, much later counter, so one failure
// still produces at most one alert-bearing line.
func (a *Activities) nhOutcome(name, key, errMsg string, degraded []string) error {
	if errMsg != "" {
		return &refreshError{errMsg}
	}
	a.nhDegraded(name, key, degraded)
	return nil
}

func (a *Activities) entries() []cacheEntry {
	api := a.API
	return []cacheEntry{
		{name: "topology", key: "topology", fn: func(ctx context.Context) (any, error) {
			resp, err := api.FetchTopologyData(ctx)
			if err != nil {
				return nil, err
			}
			if resp.Error != "" {
				return nil, &refreshError{resp.Error}
			}
			return resp, nil
		}},
		{name: "status", key: "status", fn: func(ctx context.Context) (any, error) {
			resp := api.FetchStatusData(ctx)
			if resp.Error != "" {
				return nil, &refreshError{resp.Error}
			}
			return resp, nil
		}},
		{name: "incidents", key: "incidents", fn: func(ctx context.Context) (any, error) {
			resp := api.FetchDefaultIncidentsData(ctx)
			if resp == nil {
				return nil, &refreshError{"nil response"}
			}
			return resp, nil
		}},
		{name: "device incidents", key: "device_incidents", fn: func(ctx context.Context) (any, error) {
			resp := api.FetchDefaultDeviceIncidentsData(ctx)
			if resp == nil {
				return nil, &refreshError{"nil response"}
			}
			return resp, nil
		}},
		{name: "flex-algo divergence", key: "algo_divergence", every: algoDivergenceInterval, fn: func(ctx context.Context) (any, error) {
			resp, err := api.FetchAlgoDivergenceData(ctx)
			if err != nil {
				return nil, err
			}
			// The only caller that publishes. This refresh runs over mainnet
			// alone, and the gauges carry no environment label — see
			// handlers.PublishAlgoDivergenceMetrics.
			handlers.PublishAlgoDivergenceMetrics(resp)
			return resp, nil
		}},
		{name: "link history", key: "link_history:24h:72", fn: func(ctx context.Context) (any, error) {
			return api.FetchLinkHistoryData(ctx, "24h", 72)
		}},
		{name: "device history", key: "device_history:24h:72", fn: func(ctx context.Context) (any, error) {
			return api.FetchDeviceHistoryData(ctx, "24h", 72)
		}},
		{name: "latency comparison", key: "latency_comparison", every: latencyComparisonInterval, fn: func(ctx context.Context) (any, error) {
			return api.FetchLatencyComparisonData(ctx)
		}},
		{name: "dz ledger", key: "dz_ledger", fn: func(ctx context.Context) (any, error) {
			return handlers.FetchDZLedgerData(ctx)
		}},
		{name: "solana ledger", key: "solana_ledger", fn: func(ctx context.Context) (any, error) {
			return handlers.FetchSolanaLedgerData(ctx)
		}},
		{name: "validator perf", key: "validator_perf", fn: func(ctx context.Context) (any, error) {
			return api.FetchValidatorPerfData(ctx)
		}},
		{name: "stake overview", key: "stake_overview", fn: func(ctx context.Context) (any, error) {
			return api.FetchStakeOverviewData(ctx)
		}},
		{name: "publisher check", key: "publisher_check", every: publisherCheckInterval, fn: func(ctx context.Context) (any, error) {
			return api.FetchPublisherCheckData(ctx, "", handlers.DefaultPublisherCheckEpochs, 0)
		}},
		{name: "shreds rewards", key: "shreds_rewards", fn: func(ctx context.Context) (any, error) {
			return api.FetchShredsRewardsData(ctx)
		}},
		{name: "edge scoreboard", key: "edge_scoreboard", every: edgeScoreboardInterval, fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardData(ctx, "24h", false, 0, 0, 1000)
		}},
		{name: "edge scoreboard (leaders)", key: "edge_scoreboard:leaders", every: edgeScoreboardInterval, fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardData(ctx, "24h", true, 0, 0, 1000)
		}},
		{name: "hyperliquid scoreboard", key: "hyperliquid_scoreboard", fn: func(ctx context.Context) (any, error) {
			return api.FetchHyperliquidScoreboardData(ctx, "1h", "")
		}},
		{name: "kalshi scoreboard", key: "kalshi_scoreboard", fn: func(ctx context.Context) (any, error) {
			return api.FetchKalshiScoreboardData(ctx, "1h", "")
		}},
		{name: "bulk link metrics", key: "bulk_link_metrics", fn: func(ctx context.Context) (any, error) {
			return api.FetchBulkLinkMetricsData(ctx)
		}},
		{name: "bulk link metrics (issues)", key: "bulk_link_metrics_issues", fn: func(ctx context.Context) (any, error) {
			return api.FetchBulkLinkMetricsIssuesData(ctx)
		}},
		{name: "bulk device metrics", key: "bulk_device_metrics", fn: func(ctx context.Context) (any, error) {
			return api.FetchBulkDeviceMetricsData(ctx)
		}},
		{name: "bulk device metrics (issues)", key: "bulk_device_metrics_issues", fn: func(ctx context.Context) (any, error) {
			return api.FetchBulkDeviceMetricsIssuesData(ctx)
		}},
		{name: "geo concentration", key: "geo_concentration", fn: func(ctx context.Context) (any, error) {
			return api.FetchGeoConcentrationData(ctx)
		}},
		{name: "geo validators", key: "geo_validators", fn: func(ctx context.Context) (any, error) {
			return api.FetchGeoValidatorsData(ctx, "", "")
		}},
		// The unfiltered stake-desc validators listing is polled continuously by the
		// UI (limit=100) and an external consumer (limit=900). Cache the complete
		// set so the handler can slice any requested page out of it — its stake/geo
		// data moves on slow timescales.
		{name: "validators", key: handlers.ValidatorsPageCacheKey, every: validatorsListingInterval, fn: func(ctx context.Context) (any, error) {
			return api.FetchValidatorsData(ctx)
		}},
		{name: "edge multicast", key: handlers.EdgeMulticastCacheKey, fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeMulticastData(ctx)
		}},
		{name: "multicast health summaries", key: handlers.MulticastHealthSummariesCacheKey, fn: func(ctx context.Context) (any, error) {
			return api.FetchMulticastHealthSummariesData(ctx, handlers.ShredGroupPK)
		}},
		// Pre-fetch the hot first page of /health/users and /health/paths for
		// ShredGroupPK. The UI's default request (offset=0, limit=
		// MulticastHealthCachedPageSize) hits these caches and returns in ~1ms
		// instead of running the view live (multi-second on edge-solana-shreds).
		{name: "multicast health users (shreds)", key: handlers.MulticastHealthUsersCacheKey(handlers.ShredGroupPK), fn: func(ctx context.Context) (any, error) {
			return api.FetchMulticastHealthUsersPageData(ctx, handlers.ShredGroupPK)
		}},
		{name: "multicast health paths (shreds)", key: handlers.MulticastHealthPathsCacheKey(handlers.ShredGroupPK), fn: func(ctx context.Context) (any, error) {
			return api.FetchMulticastHealthPathsPageData(ctx, handlers.ShredGroupPK)
		}},
		// Network Health is split into independent data-source-group caches so the
		// page loads progressively and no slow group blocks another. Each group is a
		// strict subset of the old monolith, so each refreshes faster than the single
		// entry did. Every group reports two things: resp.Error, set when a CRITICAL
		// panel failed, which becomes a refreshError so the last-good blob is kept
		// instead of caching zeros; and resp.Degraded, the panels that failed without
		// invalidating the rest, which still writes but escalates under its own
		// counter (see nhDegraded). The two heavy groups (impactful, deferred)
		// refresh in their own activity, see heavyEntries.
		{name: "network health overview", key: handlers.NetworkHealthOverviewCacheKey, dayAligned: true, every: networkHealthOverviewInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthOverviewData(ctx, start, end, "")
			if err := a.nhOutcome("network health overview", handlers.NetworkHealthOverviewCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health availability", key: handlers.NetworkHealthAvailabilityCacheKey, dayAligned: true, every: networkHealthHistoryInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthAvailabilityData(ctx, start, end, "")
			if err := a.nhOutcome("network health availability", handlers.NetworkHealthAvailabilityCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health latency", key: handlers.NetworkHealthLatencyCacheKey, dayAligned: true, every: networkHealthHistoryInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthLatencyData(ctx, start, end, "")
			if err := a.nhOutcome("network health latency", handlers.NetworkHealthLatencyCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health capacity", key: handlers.NetworkHealthCapacityCacheKey, dayAligned: true, every: networkHealthHistoryInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthCapacityData(ctx, start, end, "")
			if err := a.nhOutcome("network health capacity", handlers.NetworkHealthCapacityCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health outages", key: handlers.NetworkHealthOutagesCacheKey, dayAligned: true, every: networkHealthHistoryInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthOutagesData(ctx, start, end, "")
			if err := a.nhOutcome("network health outages", handlers.NetworkHealthOutagesCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health drain", key: handlers.NetworkHealthDrainCacheKey, dayAligned: true, every: networkHealthHistoryInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthDrainData(ctx, start, end, "")
			if err := a.nhOutcome("network health drain", handlers.NetworkHealthDrainCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health tickets", key: handlers.NetworkHealthTicketsCacheKey, dayAligned: true, every: networkHealthHistoryInterval, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthTicketsData(ctx, start, end, "")
			// A transient ops-API outage sets resp.Error; keep the last-good blob
			// instead of caching an empty aggregate (like overview/outages/impactful).
			if err := a.nhOutcome("network health tickets", handlers.NetworkHealthTicketsCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
	}
}

// metroPathLatencyStrategies are refreshed as separate keys under one logical entry.
var metroPathLatencyStrategies = []string{"latency", "hops", "bandwidth"}

type refreshError struct{ msg string }

func (e *refreshError) Error() string { return e.msg }

// batchConcurrency is the in-flight limit for the slow batch: the configured
// concurrency minus the heavy entries' share. RefreshHeavyCaches runs alongside
// this batch, so reserving their slots keeps the aggregate in-flight entry count
// against ClickHouse at the configured value rather than above it. Floored at 1
// so the minimum concurrency setting still refreshes.
//
// The reservation is unconditional rather than tracking live heavy occupancy.
// The workflow no longer awaits the heavy run inside its loop, so a slow scan
// spans several cycles and overlaps several batches; the batch's width is what
// keeps that from oversubscribing ClickHouse. Reclaiming the two slots while the
// scans are idle would shorten a batch that is already far shorter than the
// refresh interval, and only at the price of making the heavy entries queue
// behind batch entries for a shared limiter, which would eat the
// heavyActivityHeadroom that lets them record their own failures.
func (a *Activities) batchConcurrency() int {
	limit := a.RefreshConcurrency
	if limit <= 0 {
		limit = defaultRefreshConcurrency
	}
	if n := limit - len(a.heavyEntries()); n >= 1 {
		limit = n
	}
	return limit
}

// RefreshCaches refreshes the slow-batch page cache entries, writing results to
// Postgres. The heavy Network Health scans are not in this batch (see
// heavyEntries), so every entry here fits defaultRefreshTimeout and the batch
// still refreshes within the cycle.
//
// Concurrency history: 2-wide was too slow (each entry refreshed only every few
// minutes); fully unbounded (~28 entries) oversubscribed ClickHouse (~55
// concurrent queries pegged the node, timeouts + per-entry retries amplified the
// storm). A bounded limit keeps in-flight queries near what ClickHouse can run
// while still refreshing the batch within the cycle.
func (a *Activities) RefreshCaches(ctx context.Context) error {
	start := time.Now()
	limit := a.batchConcurrency()
	// Distinguish a real worker shutdown (deploy) from an activity-deadline
	// cancellation so tail-entry starvation under the StartToCloseTimeout is
	// counted rather than silently swallowed as "shutdown".
	shuttingDown := workerStopping(ctx)

	due, skipped := a.dueEntries(ctx, a.entries())

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)

	for _, entry := range due {
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn, entry.timeout, shuttingDown, errBatchDeadline)
			return nil
		})
	}

	// Metro path latency: one fetch per strategy, each written to its own key
	for _, strategy := range metroPathLatencyStrategies {
		g.Go(func() error {
			a.refresh(gctx, "metro path latency:"+strategy, "metro_path_latency:"+strategy, func(ctx context.Context) (any, error) {
				return a.API.FetchMetroPathLatencyData(ctx, strategy, "", 0)
			}, 0, shuttingDown, errBatchDeadline)
			return nil
		})
	}

	// Network health, per contributor: NOT precomputed. This used to iterate
	// every active contributor here and run the full heavy network-health
	// pipeline (topology graph + 30d path-impact + DIA scan + rollup scans) per
	// code, which meant ~15 full heavy pipelines running alongside the other
	// cache entries every refresh cycle. That oversubscribed ClickHouse badly
	// enough to saturate the cluster and starve the network-default view's own
	// queries (see .superpowers/sdd/stabilize-report.md). Scoped requests now
	// always fall back to a live compute: each group handler computes the scoped
	// view live under its deadline — slower per-contributor page loads, but it
	// does not compete with the default view for cluster capacity.
	_ = g.Wait()
	// Successful refreshes log at DEBUG (suppressed in prod), so these counts are
	// the only evidence of what a cycle did. They count entries run, not written —
	// a failed refresh reports itself (see recordFailure).
	a.Log.Info("page cache refresh complete",
		"duration", time.Since(start).Round(time.Millisecond),
		"attempted", len(due)+len(metroPathLatencyStrategies),
		"skipped", skipped)
	return nil
}

// workerStopping returns a predicate reporting whether the Temporal worker is
// shutting down (deploy/stop). Used to tell a benign shutdown cancellation from
// an activity-deadline cancellation. Must be called with an activity context.
func workerStopping(ctx context.Context) func() bool {
	stop := activity.GetWorkerStopChannel(ctx)
	return func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
}

// latestEntries are refreshed on the fast cadence (see fastRefreshInterval). They back
// the edge scoreboard live-tail so client polls read cached latest slots instead of
// hitting ClickHouse every few seconds.
func (a *Activities) latestEntries() []cacheEntry {
	api := a.API
	return []cacheEntry{
		{name: "edge scoreboard (latest)", key: "edge_scoreboard:latest", fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardLatest(ctx, false, 1000)
		}},
		{name: "edge scoreboard (latest, leaders)", key: "edge_scoreboard:latest:leaders", fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardLatest(ctx, true, 1000)
		}},
	}
}

// RefreshLatestCaches refreshes just the fast-cadence entries (latest slots slice).
func (a *Activities) RefreshLatestCaches(ctx context.Context) error {
	shuttingDown := workerStopping(ctx)
	g, gctx := errgroup.WithContext(ctx)
	for _, entry := range a.latestEntries() {
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn, entry.timeout, shuttingDown, errFastRefreshDeadline)
			return nil
		})
	}
	_ = g.Wait()
	return nil
}

// heavyEntries are the two Network Health groups whose ClickHouse scans need
// longer than the slow batch's per-entry ceiling (defaultRefreshTimeout). They
// refresh in their own activity so a 180s scan cannot consume the batch's
// StartToCloseTimeout: inside the batch they left it zero headroom, so a slow
// scan was cut by the activity deadline (recorded as batch starvation, both
// against the scan itself and against whichever entries had not run, never as
// its own failure) and stretched every other page's refresh cadence by up to
// 180s.
func (a *Activities) heavyEntries() []cacheEntry {
	api := a.API
	return []cacheEntry{
		{name: "network health impactful", key: handlers.NetworkHealthImpactfulCacheKey, dayAligned: true, every: networkHealthHistoryInterval, timeout: nhHeavyRefreshTimeout, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthImpactfulData(ctx, start, end, "")
			if err := a.nhOutcome("network health impactful", handlers.NetworkHealthImpactfulCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
		{name: "network health deferred", key: handlers.NetworkHealthDeferredCacheKey, dayAligned: true, every: networkHealthHistoryInterval, timeout: nhHeavyRefreshTimeout, fn: func(ctx context.Context) (any, error) {
			start, end := handlers.DefaultNetworkHealthWindow()
			resp := api.FetchNetworkHealthDeferredData(ctx, start, end, "")
			if err := a.nhOutcome("network health deferred", handlers.NetworkHealthDeferredCacheKey, resp.Error, resp.Degraded); err != nil {
				return nil, err
			}
			return resp, nil
		}},
	}
}

// RefreshHeavyCaches refreshes the heavy Network Health entries under their own
// StartToCloseTimeout (heavyActivityTimeout), which sits heavyActivityHeadroom
// above their per-entry budget. PageCacheWorkflow starts it alongside the batch
// and never awaits it inside the loop, so it runs on its own cadence instead of
// dictating the cycle; a new run starts only once the previous one has finished,
// so two heavy runs never overlap each other. Both entries carry a cadence, so a
// run started before either is due costs one age read and returns — cheaper than
// gating the scheduling, which is bound up with the continue-as-new drain.
func (a *Activities) RefreshHeavyCaches(ctx context.Context) error {
	shuttingDown := workerStopping(ctx)
	due, _ := a.dueEntries(ctx, a.heavyEntries())
	g, gctx := errgroup.WithContext(ctx)
	for _, entry := range due {
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn, entry.timeout, shuttingDown, errHeavyRefreshDeadline)
			return nil
		})
	}
	_ = g.Wait()
	return nil
}

// refresh runs one cache entry's fetch under a per-attempt context deadline and
// writes the result. timeout overrides that deadline when > 0; otherwise
// defaultRefreshTimeout applies. deadlineErr is the sentinel recorded when the
// parent (activity) context is cancelled by its own deadline rather than a
// worker shutdown; it selects the escalation cadence (errBatchDeadline for the
// slow batch, errFastRefreshDeadline for the fast loop, errHeavyRefreshDeadline
// for the heavy one).
func (a *Activities) refresh(parentCtx context.Context, name, key string, fn func(context.Context) (any, error), timeout time.Duration, shuttingDown func() bool, deadlineErr error) {
	start := time.Now()
	var queryDuration, writeDuration time.Duration

	if timeout <= 0 {
		timeout = defaultRefreshTimeout
	}

	const maxAttempts = 2
	for attempt := range maxAttempts {
		if parentCtx.Err() != nil {
			// Batch context already done before this entry ran.
			a.interrupted(name, key, nil, shuttingDown, deadlineErr)
			return
		}

		ctx, cancel := context.WithTimeout(parentCtx, timeout)
		queryStart := time.Now()
		result, err := fn(ctx)
		queryDuration = time.Since(queryStart)
		cancel()

		if err != nil {
			if parentCtx.Err() != nil {
				a.interrupted(name, key, err, shuttingDown, deadlineErr)
				return
			}
			// Query error or timeout. Retry once before counting as a failure,
			// but only when the parent still has room for a second attempt:
			// otherwise the retry is cut by the activity deadline and recorded as
			// deadline starvation instead of this entry's own failure.
			if attempt < maxAttempts-1 && hasBudgetFor(parentCtx, retryBudget(queryDuration, timeout)) {
				a.Log.Warn("cache refresh failed, retrying", "cache", name, "attempt", attempt+1, "error", err)
				continue
			}
			a.recordFailure(name, key, err)
			return
		}

		a.esc.Reset(key)

		writeStart := time.Now()
		a.writeMu.Lock()
		err = a.API.WritePageCache(parentCtx, key, result)
		a.writeMu.Unlock()
		writeDuration = time.Since(writeStart)
		if err != nil {
			if parentCtx.Err() != nil {
				return
			}
			a.recordWriteFailure(name, key, err)
			return
		}
		a.esc.Reset(key + ":write")

		// Surface slow entries at INFO so we can spot the outlier that's
		// eating the activity's StartToCloseTimeout budget. Normal entries
		// log at DEBUG (suppressed in prod).
		total := time.Since(start)
		args := []any{
			"cache", name,
			"duration", total.Round(time.Millisecond),
			"query_duration", queryDuration.Round(time.Millisecond),
			"write_duration", writeDuration.Round(time.Millisecond),
		}
		if total >= slowRefreshThreshold {
			a.Log.Info("slow cache refresh", args...)
		} else {
			a.Log.Debug("cache refreshed", args...)
		}
		return
	}
}

// hasBudgetFor reports whether ctx has at least d left. A context without a
// deadline is unlimited.
func hasBudgetFor(ctx context.Context, d time.Duration) bool {
	dl, ok := ctx.Deadline()
	return !ok || time.Until(dl) >= d
}

// retryBudget is how much of the parent activity's budget a second attempt
// needs: what the failed attempt actually cost plus retryAttemptMargin, capped
// at the entry's own timeout since no attempt can run longer than that. Gating
// on the full timeout instead denies the retry to an entry that failed in
// milliseconds with most of the budget still on the clock, and a.esc.Reset only
// runs on success, so that skipped retry is recorded as a failure.
func retryBudget(attemptCost, timeout time.Duration) time.Duration {
	return min(attemptCost+retryAttemptMargin, timeout)
}

// interrupted handles a refresh cut short because its parent context was
// cancelled. A genuine worker shutdown (deploy) is benign and not counted;
// otherwise the activity ran out of its StartToCloseTimeout and this entry is
// being starved — count it (as deadlineErr) so it surfaces/escalates rather than
// hiding as "shutdown". deadlineErr selects the escalation cadence per cadence
// (errBatchDeadline: strict, for the slow batch; errFastRefreshDeadline and
// errHeavyRefreshDeadline: transient, for the self-healing fast and heavy
// activities). cause is the underlying fn error, if any, and
// is attached as a log attribute only (not wrapped into the classification error).
func (a *Activities) interrupted(name, key string, cause error, shuttingDown func() bool, deadlineErr error) {
	if shuttingDown == nil || shuttingDown() {
		a.Log.Warn("cache refresh interrupted (shutdown)", "cache", name, "error", cause)
		return
	}
	if cause != nil {
		a.recordFailure(name, key, deadlineErr, "cause", cause)
	} else {
		a.recordFailure(name, key, deadlineErr)
	}
}

// recordWriteFailure escalates cache-write failures under a separate key from
// the query leg: a Postgres blip warns, but a sustained outage (whose errors
// classify transient indefinitely, e.g. connection refused) still reaches
// ERROR and pages. Nothing else alerts on this — handlers fall back silently
// and PageCacheWorkflow never fails since activities return nil.
func (a *Activities) recordWriteFailure(name, key string, err error) {
	a.esc.Fail(a.Log, key+":write", "cache write failed", "cache", name, "error", err)
}

// recordFailure increments the consecutive-failure counter for key and logs at
// WARN below the escalation threshold, ERROR at/above it (see logger.Escalator:
// transient causes get the higher threshold since they self-heal). extra
// key/value pairs are appended to the log line (e.g. the underlying cause).
func (a *Activities) recordFailure(name, key string, err error, extra ...any) {
	args := append([]any{"cache", name, "error", err}, extra...)
	a.esc.Fail(a.Log, key, "cache refresh failed", args...)
}

// PageCacheWorkflow is a long-running workflow that refreshes all page caches on
// p.RefreshInterval. It continues-as-new after p.ContinueAsNewThreshold iterations
// (derived to bound wall-clock ≈ continueAsNewTargetWindow) to keep workflow
// history bounded. p is passed as an argument (not read from package state) so
// replay is deterministic across a config change — see PageCacheParams.
func PageCacheWorkflow(ctx temporalworkflow.Context, iteration int, p PageCacheParams) error {
	p = p.withDefaults()

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: p.ActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	// RefreshLatestCaches runs every 3s and only hits edge_scoreboard:latest
	// queries, but those occasionally take >30s under ClickHouse contention
	// from the heavier RefreshCaches queries running in parallel. A 60s
	// budget absorbs that variance without being wide enough to mask a real
	// regression.
	fastActOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	fastCtx := temporalworkflow.WithActivityOptions(ctx, fastActOpts)

	// RefreshHeavyCaches carries the two 180s Network Health scans. Its budget is
	// their per-entry timeout plus heavyActivityHeadroom, so an entry that exhausts
	// its own budget records its own failure instead of being cancelled by the
	// activity deadline at the same instant. The headroom is also the window in
	// which an entry that fails early can still take its second attempt (see
	// refresh's retryBudget gate).
	heavyActOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: heavyActivityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	heavyCtx := temporalworkflow.WithActivityOptions(ctx, heavyActOpts)

	// heavy is the outstanding RefreshHeavyCaches run. It is started at the top of
	// a cycle and never awaited inside the loop, so the cycle period stays
	// batch + sleep window: a heavy scan that runs to its 240s budget refreshes
	// its own two blobs later without stretching every other page cache's refresh
	// interval with it. Its slots against ClickHouse stay reserved for as long as
	// it runs, across cycle boundaries (see batchConcurrency).
	var heavy temporalworkflow.Future

	for iteration < p.ContinueAsNewThreshold {
		// Only one heavy run at a time: a scan still going from an earlier cycle
		// keeps this cycle from starting a second copy. We also stop starting runs
		// within heavyStartLeadCycles of the continue-as-new boundary, so the run
		// the drain below waits on has already had its full budget to finish. The
		// drain then returns at once instead of stalling every page cache (the 3s
		// latest-slots one included) for the run's remaining budget — a run started
		// on the second-to-last cycle would otherwise leave the drain blocking for
		// nearly the whole heavy budget, every continue-as-new window. Skipping the
		// heavy refresh for those last few cycles costs far less than that stall.
		// Scheduling this activity needs no version guard: Start terminates and
		// restarts the workflow on deploy.
		if heavyStartDue(iteration, p.ContinueAsNewThreshold, p.RefreshInterval) && heavyRefreshDue(heavy) {
			heavy = temporalworkflow.ExecuteActivity(heavyCtx, (*Activities).RefreshHeavyCaches)
		}

		// No cycle counter: cadence is checked against page_cache.updated_at inside
		// the activity (see cacheEntry.every), which keeps this loop out of the
		// freshness contract and survives the continue-as-new reset. Dropping the
		// argument needs no version guard — Start terminates and restarts the workflow
		// (see pagecache.go), the Go SDK doesn't compare activity inputs on replay, and
		// a mixed-version worker during a rolling deploy either zero-fills the missing
		// arg (old code) or drops the extra one (new code).
		_ = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshCaches).Get(ctx, nil)

		iteration++
		if iteration < p.ContinueAsNewThreshold {
			// Tick the fast-cadence refresh repeatedly during the outer sleep window
			// so latest-slots caches stay fresh for live-tail clients.
			deadline := temporalworkflow.Now(ctx).Add(p.RefreshInterval)
			for temporalworkflow.Now(ctx).Before(deadline) {
				_ = temporalworkflow.ExecuteActivity(fastCtx, (*Activities).RefreshLatestCaches).Get(fastCtx, nil)
				remaining := deadline.Sub(temporalworkflow.Now(ctx))
				if remaining <= 0 {
					break
				}
				sleep := fastRefreshInterval
				if remaining < sleep {
					sleep = remaining
				}
				if err := temporalworkflow.Sleep(ctx, sleep); err != nil {
					return err
				}
			}
		}
	}

	// Drain the outstanding heavy run before continuing as new. A pending activity
	// is cancelled at the continue-as-new boundary, which would cut a scan
	// mid-flight, record it as a failure, and leave both heavy blobs unwritten for
	// that cycle. The start guard above stops launching runs within
	// heavyStartLeadCycles of the boundary, so the run this waits on has already
	// had its full budget to finish: heavy.Get returns without blocking rather than
	// stalling the latest-slots refresh for the run's remaining budget.
	if heavy != nil {
		_ = heavy.Get(ctx, nil)
	}

	return temporalworkflow.NewContinueAsNewError(ctx, PageCacheWorkflow, 0, p)
}

// heavyRefreshDue reports whether a new RefreshHeavyCaches run may start: only
// when none is outstanding. heavy is nil before the first run. Future.IsReady
// resolves from workflow history, so this decision replays deterministically.
func heavyRefreshDue(heavy temporalworkflow.Future) bool {
	return heavy == nil || heavy.IsReady()
}

// heavyStartLeadCycles is how many refresh cycles before the continue-as-new
// boundary PageCacheWorkflow stops starting heavy runs. A RefreshHeavyCaches run
// takes up to heavyActivityTimeout to resolve; over the cycle period (rounded up)
// that is how many cycles a run needs to be guaranteed done. Starting no run
// within this many cycles of the boundary means the run the end-of-loop drain
// waits on has already resolved, so the drain never blocks the workflow coroutine
// (and the 3s latest-slots refresh with it). Floored at 1 so the final iteration
// is always skipped.
func heavyStartLeadCycles(refreshInterval time.Duration) int {
	if refreshInterval <= 0 {
		return 1
	}
	lead := int((heavyActivityTimeout + refreshInterval - 1) / refreshInterval)
	if lead < 1 {
		lead = 1
	}
	return lead
}

// heavyStartDue reports whether a new heavy run may start at this iteration: only
// when at least heavyStartLeadCycles cycles remain before the threshold, so the
// run finishes within its budget before the drain. The lead is capped at
// threshold-1 so a window too short to fit a full run before the boundary still
// starts one at iteration 0 rather than never refreshing the heavy blobs.
func heavyStartDue(iteration, threshold int, refreshInterval time.Duration) bool {
	lead := heavyStartLeadCycles(refreshInterval)
	if lead > threshold-1 {
		lead = threshold - 1
	}
	return iteration+lead < threshold
}
