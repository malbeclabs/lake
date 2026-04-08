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
	continueAsNewThreshold = 60 // ~30 min at 30s intervals
	maxConcurrentRefreshes = 2
	errorAfterFailures     = 3 // log WARN for transient failures, ERROR after this many consecutive failures
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
	failures sync.Map // map[string]int: consecutive failure count per cache key
}

func (a *Activities) entries() []cacheEntry {
	api := a.API
	return []cacheEntry{
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
			return api.FetchEdgeScoreboardData(ctx, "24h")
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
	}
}

// metroPathLatencyStrategies are refreshed as separate keys under one logical entry.
var metroPathLatencyStrategies = []string{"latency", "hops", "bandwidth"}

type refreshError struct{ msg string }

func (e *refreshError) Error() string { return e.msg }

// RefreshCaches refreshes all page cache entries, writing results to Postgres.
func (a *Activities) RefreshCaches(ctx context.Context) error {
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentRefreshes)

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

func (a *Activities) refresh(ctx context.Context, name, key string, fn func(context.Context) (any, error)) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	result, err := fn(ctx)
	if err != nil {
		if ctx.Err() != nil {
			a.Log.Warn("cache refresh interrupted (shutdown)", "cache", name, "error", err)
			return
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

	if err := a.API.WritePageCache(ctx, key, result); err != nil {
		if ctx.Err() != nil {
			return
		}
		a.Log.Error("cache write failed", "cache", name, "error", err)
		return
	}

	a.Log.Debug("cache refreshed", "cache", name)
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

	for iteration < continueAsNewThreshold {
		_ = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshCaches).Get(ctx, nil)

		iteration++
		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, refreshInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, PageCacheWorkflow, 0)
}
