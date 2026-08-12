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

	// Defaults for the per-environment load-shaping knobs (see loadRefreshConfig).
	// Prod runs these against ClickHouse Cloud; staging overrides them lower/longer
	// to spread load across its smaller self-hosted ClickHouse.
	defaultRefreshInterval    = 30 * time.Second
	defaultRefreshConcurrency = 8
	defaultActivityTimeout    = 3 * time.Minute

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
		p.ContinueAsNewThreshold = max(int(continueAsNewTargetWindow/p.RefreshInterval), 1)
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
)

// cacheEntry defines a single cache key to refresh. everyN sets a slow-refresh
// cadence: the entry refreshes only on every Nth slow cycle (everyN <= 1, the
// zero value, means every cycle). Keeping the cadence on the entry itself — rather
// than in a side map keyed by string — removes the drift hazard where a renamed
// key silently reverts to every-cycle refresh.
type cacheEntry struct {
	name   string
	key    string
	everyN int
	fn     func(ctx context.Context) (any, error)
}

// publisherCheckEveryN slows the publisher_check refresh. It reads
// shredder.publisher_shred_stats, the heaviest recurring query on the shared
// ClickHouse, yet its data only changes on epoch timescales (~2 days) — refreshing
// every 4th cycle (~2 min at the default 30s interval) removes it from the 30s
// treadmill with no user-visible staleness. edge_scoreboard also reads that table
// but backs a live-tail view, so it stays at every cycle.
//
// Tradeoff: the failure counters advance only once per due cycle, so a persistently
// broken refresh takes ~4× longer to cross the escalation thresholds (~6 min for a
// strict cause, ~20 min for a transient one) than an every-cycle entry. Acceptable
// for data this slow-moving; the cached-or-live readers cap staleness independently.
const publisherCheckEveryN = 4

// validatorsListingEveryN refreshes the validators listing every other
// slow cycle (~60s at the default 30s interval), matching the UI's ~60s poll and
// absorbing the external ~10s poller that previously ran the query ~6,500×/day.
const validatorsListingEveryN = 2

// dueThisCycle reports whether an entry with the given cadence refreshes on the
// given zero-based cycle. everyN <= 1 means every cycle.
func dueThisCycle(everyN, cycle int) bool {
	if everyN <= 1 {
		return true
	}
	return cycle%everyN == 0
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
	esc     logger.Escalator
	writeMu sync.Mutex // serializes WritePageCache calls to avoid Postgres OOM from concurrent large JSONB upserts
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
		{name: "link history", key: "link_history:24h:72", fn: func(ctx context.Context) (any, error) {
			return api.FetchLinkHistoryData(ctx, "24h", 72)
		}},
		{name: "device history", key: "device_history:24h:72", fn: func(ctx context.Context) (any, error) {
			return api.FetchDeviceHistoryData(ctx, "24h", 72)
		}},
		{name: "latency comparison", key: "latency_comparison", fn: func(ctx context.Context) (any, error) {
			return api.FetchLatencyComparisonData(ctx)
		}},
		{name: "dz ledger", key: "dz_ledger", fn: func(ctx context.Context) (any, error) {
			return handlers.FetchLedgerData(ctx, handlers.GetDZLedgerRPCURL())
		}},
		{name: "solana ledger", key: "solana_ledger", fn: func(ctx context.Context) (any, error) {
			return handlers.FetchLedgerData(ctx, handlers.GetSolanaRPCURL())
		}},
		{name: "validator perf", key: "validator_perf", fn: func(ctx context.Context) (any, error) {
			return api.FetchValidatorPerfData(ctx)
		}},
		{name: "stake overview", key: "stake_overview", fn: func(ctx context.Context) (any, error) {
			return api.FetchStakeOverviewData(ctx)
		}},
		{name: "publisher check", key: "publisher_check", everyN: publisherCheckEveryN, fn: func(ctx context.Context) (any, error) {
			return api.FetchPublisherCheckData(ctx, "", handlers.DefaultPublisherCheckEpochs, 0)
		}},
		{name: "shreds rewards", key: "shreds_rewards", fn: func(ctx context.Context) (any, error) {
			return api.FetchShredsRewardsData(ctx)
		}},
		{name: "edge scoreboard", key: "edge_scoreboard", fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardData(ctx, "24h", false, 0, 0, 1000)
		}},
		{name: "edge scoreboard (leaders)", key: "edge_scoreboard:leaders", fn: func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardData(ctx, "24h", true, 0, 0, 1000)
		}},
		{name: "hyperliquid scoreboard", key: "hyperliquid_scoreboard", fn: func(ctx context.Context) (any, error) {
			return api.FetchHyperliquidScoreboardData(ctx, "1h", "")
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
		// set every other slow cycle (~60s) so the handler can slice any requested
		// page out of it — its stake/geo data moves on slow timescales.
		{name: "validators", key: handlers.ValidatorsPageCacheKey, everyN: validatorsListingEveryN, fn: func(ctx context.Context) (any, error) {
			return api.FetchValidatorsData(ctx)
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
	}
}

// metroPathLatencyStrategies are refreshed as separate keys under one logical entry.
var metroPathLatencyStrategies = []string{"latency", "hops", "bandwidth"}

type refreshError struct{ msg string }

func (e *refreshError) Error() string { return e.msg }

// RefreshCaches refreshes all page cache entries, writing results to Postgres.
//
// Concurrency history: 2-wide was too slow (each entry refreshed only every few
// minutes); fully unbounded (~28 entries) oversubscribed ClickHouse (~55
// concurrent queries pegged the node, timeouts + per-entry retries amplified the
// storm). A bounded limit keeps in-flight queries near what ClickHouse can run
// while still refreshing the batch within the cycle.
func (a *Activities) RefreshCaches(ctx context.Context, cycle int) error {
	start := time.Now()
	limit := a.RefreshConcurrency
	if limit <= 0 {
		limit = defaultRefreshConcurrency
	}
	// Distinguish a real worker shutdown (deploy) from an activity-deadline
	// cancellation so tail-entry starvation under the StartToCloseTimeout is
	// counted rather than silently swallowed as "shutdown".
	shuttingDown := workerStopping(ctx)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(limit)

	for _, entry := range a.entries() {
		if !dueThisCycle(entry.everyN, cycle) {
			continue
		}
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn, shuttingDown, errBatchDeadline)
			return nil
		})
	}

	// Metro path latency: one fetch per strategy, each written to its own key
	for _, strategy := range metroPathLatencyStrategies {
		g.Go(func() error {
			a.refresh(gctx, "metro path latency:"+strategy, "metro_path_latency:"+strategy, func(ctx context.Context) (any, error) {
				return a.API.FetchMetroPathLatencyData(ctx, strategy, 0)
			}, shuttingDown, errBatchDeadline)
			return nil
		})
	}

	_ = g.Wait()
	a.Log.Info("page cache refresh complete", "duration", time.Since(start).Round(time.Millisecond))
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
			a.refresh(gctx, entry.name, entry.key, entry.fn, shuttingDown, errFastRefreshDeadline)
			return nil
		})
	}
	_ = g.Wait()
	return nil
}

// deadlineErr is the sentinel recorded when the parent (activity) context is
// cancelled by its own deadline rather than a worker shutdown — it selects the
// escalation cadence (errBatchDeadline for the slow batch, errFastRefreshDeadline
// for the fast loop).
func (a *Activities) refresh(parentCtx context.Context, name, key string, fn func(context.Context) (any, error), shuttingDown func() bool, deadlineErr error) {
	start := time.Now()
	var queryDuration, writeDuration time.Duration

	const maxAttempts = 2
	for attempt := range maxAttempts {
		if parentCtx.Err() != nil {
			// Batch context already done before this entry ran.
			a.interrupted(name, key, nil, shuttingDown, deadlineErr)
			return
		}

		ctx, cancel := context.WithTimeout(parentCtx, 60*time.Second)
		queryStart := time.Now()
		result, err := fn(ctx)
		queryDuration = time.Since(queryStart)
		cancel()

		if err != nil {
			if parentCtx.Err() != nil {
				a.interrupted(name, key, err, shuttingDown, deadlineErr)
				return
			}
			// Query error or timeout. Retry once before counting as a failure.
			if attempt < maxAttempts-1 {
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

// interrupted handles a refresh cut short because its parent context was
// cancelled. A genuine worker shutdown (deploy) is benign and not counted;
// otherwise the activity ran out of its StartToCloseTimeout and this entry is
// being starved — count it (as deadlineErr) so it surfaces/escalates rather than
// hiding as "shutdown". deadlineErr selects the escalation cadence per cadence
// (errBatchDeadline: strict, for the slow batch; errFastRefreshDeadline: transient,
// for the self-healing fast loop). cause is the underlying fn error, if any, and
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

	for iteration < p.ContinueAsNewThreshold {
		// iteration doubles as the slow-refresh cycle counter (see publisherCheckEveryN).
		// It resets to 0 at each continue-as-new boundary; carrying it across would
		// require threading it through NewContinueAsNewError for no real benefit, so
		// we accept the worst case of one early publisher_check refresh per
		// continue-as-new (~every 30 min).
		//
		// The cycle argument was added to RefreshCaches without a version guard, which
		// is safe under our deploy model: Start terminates and restarts the workflow
		// (see pagecache.go), the Go SDK doesn't compare activity inputs on replay, and
		// during a rolling deploy a mixed-version worker either zero-fills the missing
		// arg (old code) or drops the extra one (new code) — no history divergence.
		_ = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshCaches, iteration).Get(ctx, nil)

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

	return temporalworkflow.NewContinueAsNewError(ctx, PageCacheWorkflow, 0, p)
}
