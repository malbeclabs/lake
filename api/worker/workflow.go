package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
	"golang.org/x/sync/errgroup"

	"github.com/malbeclabs/lake/api/handlers"
)

const (
	TaskQueue  = "api-page-cache"
	WorkflowID = "api-page-cache"

	refreshInterval        = 30 * time.Second
	fastRefreshInterval    = 3 * time.Second
	continueAsNewThreshold = 60 // ~30 min at 30s intervals
	errorAfterFailures     = 3  // log WARN for transient failures, ERROR after this many consecutive failures
)

// cacheEntry defines a single cache key to refresh.
type cacheEntry struct {
	name string
	key  string
	fn   func(ctx context.Context) (any, error)
}

// Activities holds the logger and API deps for the refresh activity.
type Activities struct {
	Log      *slog.Logger
	API      *handlers.API
	failures sync.Map   // map[string]int: consecutive failure count per cache key
	writeMu  sync.Mutex // serializes WritePageCache calls to avoid Postgres OOM from concurrent large JSONB upserts
}

func (a *Activities) entries() []cacheEntry {
	api := a.API
	return []cacheEntry{
		{"topology", "topology", func(ctx context.Context) (any, error) {
			resp, err := api.FetchTopologyData(ctx)
			if err != nil {
				return nil, err
			}
			if resp.Error != "" {
				return nil, &refreshError{resp.Error}
			}
			return resp, nil
		}},
		{"status", "status", func(ctx context.Context) (any, error) {
			resp := api.FetchStatusData(ctx)
			if resp.Error != "" {
				return nil, &refreshError{resp.Error}
			}
			return resp, nil
		}},
		{"incidents", "incidents", func(ctx context.Context) (any, error) {
			resp := api.FetchDefaultIncidentsData(ctx)
			if resp == nil {
				return nil, &refreshError{"nil response"}
			}
			return resp, nil
		}},
		{"device incidents", "device_incidents", func(ctx context.Context) (any, error) {
			resp := api.FetchDefaultDeviceIncidentsData(ctx)
			if resp == nil {
				return nil, &refreshError{"nil response"}
			}
			return resp, nil
		}},
		{"link history", "link_history:24h:72", func(ctx context.Context) (any, error) {
			return api.FetchLinkHistoryData(ctx, "24h", 72)
		}},
		{"device history", "device_history:24h:72", func(ctx context.Context) (any, error) {
			return api.FetchDeviceHistoryData(ctx, "24h", 72)
		}},
		{"latency comparison", "latency_comparison", func(ctx context.Context) (any, error) {
			return api.FetchLatencyComparisonData(ctx)
		}},
		{"dz ledger", "dz_ledger", func(ctx context.Context) (any, error) {
			return handlers.FetchLedgerData(ctx, handlers.GetDZLedgerRPCURL())
		}},
		{"solana ledger", "solana_ledger", func(ctx context.Context) (any, error) {
			return handlers.FetchLedgerData(ctx, handlers.GetSolanaRPCURL())
		}},
		{"validator perf", "validator_perf", func(ctx context.Context) (any, error) {
			return api.FetchValidatorPerfData(ctx)
		}},
		{"stake overview", "stake_overview", func(ctx context.Context) (any, error) {
			return api.FetchStakeOverviewData(ctx)
		}},
		{"publisher check", "publisher_check", func(ctx context.Context) (any, error) {
			return api.FetchPublisherCheckData(ctx, "", 2, 0)
		}},
		{"edge scoreboard", "edge_scoreboard", func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardData(ctx, "24h", false, 0, 0, 1000)
		}},
		{"edge scoreboard (leaders)", "edge_scoreboard:leaders", func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardData(ctx, "24h", true, 0, 0, 1000)
		}},
		{"bulk link metrics", "bulk_link_metrics", func(ctx context.Context) (any, error) {
			return api.FetchBulkLinkMetricsData(ctx)
		}},
		{"bulk link metrics (issues)", "bulk_link_metrics_issues", func(ctx context.Context) (any, error) {
			return api.FetchBulkLinkMetricsIssuesData(ctx)
		}},
		{"bulk device metrics", "bulk_device_metrics", func(ctx context.Context) (any, error) {
			return api.FetchBulkDeviceMetricsData(ctx)
		}},
		{"bulk device metrics (issues)", "bulk_device_metrics_issues", func(ctx context.Context) (any, error) {
			return api.FetchBulkDeviceMetricsIssuesData(ctx)
		}},
		{"geo concentration", "geo_concentration", func(ctx context.Context) (any, error) {
			return api.FetchGeoConcentrationData(ctx)
		}},
		{"geo validators", "geo_validators", func(ctx context.Context) (any, error) {
			return api.FetchGeoValidatorsData(ctx, "", "")
		}},
	}
}

// metroPathLatencyStrategies are refreshed as separate keys under one logical entry.
var metroPathLatencyStrategies = []string{"latency", "hops", "bandwidth"}

type refreshError struct{ msg string }

func (e *refreshError) Error() string { return e.msg }

// RefreshCaches refreshes all page cache entries, writing results to Postgres.
func (a *Activities) RefreshCaches(ctx context.Context) error {
	start := time.Now()
	// Run all entries fully in parallel. With ~21 entries at 2-wide concurrency,
	// the batch took longer than the 30s refresh interval, causing each entry to
	// effectively refresh only once every few minutes. Fully parallel execution
	// means the activity completes in max(entry_times) rather than sum/2, so the
	// 30s sleep actually achieves a ~30s refresh cycle. Each entry has its own
	// 45s timeout, so failures remain bounded.
	g, gctx := errgroup.WithContext(ctx)

	for _, entry := range a.entries() {
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn)
			return nil
		})
	}

	// Metro path latency: one fetch per strategy, each written to its own key
	for _, strategy := range metroPathLatencyStrategies {
		g.Go(func() error {
			a.refresh(gctx, "metro path latency:"+strategy, "metro_path_latency:"+strategy, func(ctx context.Context) (any, error) {
				return a.API.FetchMetroPathLatencyData(ctx, strategy)
			})
			return nil
		})
	}

	_ = g.Wait()
	a.Log.Info("page cache refresh complete", "duration", time.Since(start).Round(time.Millisecond))
	return nil
}

// latestEntries are refreshed on the fast cadence (see fastRefreshInterval). They back
// the edge scoreboard live-tail so client polls read cached latest slots instead of
// hitting ClickHouse every few seconds.
func (a *Activities) latestEntries() []cacheEntry {
	api := a.API
	return []cacheEntry{
		{"edge scoreboard (latest)", "edge_scoreboard:latest", func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardLatest(ctx, false, 1000)
		}},
		{"edge scoreboard (latest, leaders)", "edge_scoreboard:latest:leaders", func(ctx context.Context) (any, error) {
			return api.FetchEdgeScoreboardLatest(ctx, true, 1000)
		}},
	}
}

// RefreshLatestCaches refreshes just the fast-cadence entries (latest slots slice).
func (a *Activities) RefreshLatestCaches(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, entry := range a.latestEntries() {
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn)
			return nil
		})
	}
	_ = g.Wait()
	return nil
}

func (a *Activities) refresh(parentCtx context.Context, name, key string, fn func(context.Context) (any, error)) {
	const maxAttempts = 2
	for attempt := range maxAttempts {
		if parentCtx.Err() != nil {
			a.Log.Warn("cache refresh interrupted (shutdown)", "cache", name)
			return
		}

		ctx, cancel := context.WithTimeout(parentCtx, 45*time.Second)
		result, err := fn(ctx)
		cancel()

		if err != nil {
			if parentCtx.Err() != nil {
				// Temporal is shutting down — not a query failure, don't count it.
				a.Log.Warn("cache refresh interrupted (shutdown)", "cache", name, "error", err)
				return
			}
			// Query error or timeout. Retry once before counting as a failure.
			if attempt < maxAttempts-1 {
				a.Log.Warn("cache refresh failed, retrying", "cache", name, "attempt", attempt+1, "error", err)
				continue
			}
			n := a.incFailures(key)
			if n >= errorAfterFailures {
				a.Log.Error("cache refresh failed", "cache", name, "consecutive_failures", n, "error", err)
			} else {
				a.Log.Warn("cache refresh failed", "cache", name, "consecutive_failures", n, "error", err)
			}
			return
		}

		a.failures.Delete(key)

		a.writeMu.Lock()
		err = a.API.WritePageCache(parentCtx, key, result)
		a.writeMu.Unlock()
		if err != nil {
			if parentCtx.Err() != nil {
				return
			}
			a.Log.Error("cache write failed", "cache", name, "error", err)
			return
		}

		a.Log.Debug("cache refreshed", "cache", name)
		return
	}
}

func (a *Activities) incFailures(key string) int {
	for {
		v, _ := a.failures.LoadOrStore(key, 1)
		n := v.(int)
		if a.failures.CompareAndSwap(key, n, n+1) {
			return n + 1
		}
	}
}

// PageCacheWorkflow is a long-running workflow that refreshes all page caches
// every 30s. It uses continue-as-new after 60 iterations (~30 min) to keep
// workflow history bounded.
func PageCacheWorkflow(ctx temporalworkflow.Context, iteration int) error {
	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	fastActOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	fastCtx := temporalworkflow.WithActivityOptions(ctx, fastActOpts)

	for iteration < continueAsNewThreshold {
		_ = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshCaches).Get(ctx, nil)

		iteration++
		if iteration < continueAsNewThreshold {
			// Tick the fast-cadence refresh repeatedly during the outer sleep window
			// so latest-slots caches stay fresh for live-tail clients.
			deadline := temporalworkflow.Now(ctx).Add(refreshInterval)
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

	return temporalworkflow.NewContinueAsNewError(ctx, PageCacheWorkflow, 0)
}
