package worker

import (
	"context"
	"log/slog"
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
)

// cacheEntry defines a single cache key to refresh.
type cacheEntry struct {
	name string
	key  string
	fn   func(ctx context.Context) (any, error)
}

// Activities holds the logger for the refresh activity.
type Activities struct {
	log *slog.Logger
}

var entries = []cacheEntry{
	{"status", "status", func(ctx context.Context) (any, error) {
		resp := handlers.FetchStatusData(ctx)
		if resp.Error != "" {
			return nil, &refreshError{resp.Error}
		}
		return resp, nil
	}},
	{"timeline", "timeline", func(ctx context.Context) (any, error) {
		return handlers.FetchDefaultTimelineData(ctx), nil
	}},
	{"incidents", "incidents", func(ctx context.Context) (any, error) {
		resp := handlers.FetchDefaultIncidentsData(ctx)
		if resp == nil {
			return nil, &refreshError{"nil response"}
		}
		return resp, nil
	}},
	{"device incidents", "device_incidents", func(ctx context.Context) (any, error) {
		resp := handlers.FetchDefaultDeviceIncidentsData(ctx)
		if resp == nil {
			return nil, &refreshError{"nil response"}
		}
		return resp, nil
	}},
	{"link history", "link_history:24h:72", func(ctx context.Context) (any, error) {
		return handlers.FetchLinkHistoryData(ctx, "24h", 72)
	}},
	{"device history", "device_history:24h:72", func(ctx context.Context) (any, error) {
		return handlers.FetchDeviceHistoryData(ctx, "24h", 72)
	}},
	{"latency comparison", "latency_comparison", func(ctx context.Context) (any, error) {
		return handlers.FetchLatencyComparisonData(ctx)
	}},
	{"dz ledger", "dz_ledger", func(ctx context.Context) (any, error) {
		return handlers.FetchLedgerData(ctx, handlers.GetDZLedgerRPCURL())
	}},
	{"solana ledger", "solana_ledger", func(ctx context.Context) (any, error) {
		return handlers.FetchLedgerData(ctx, handlers.GetSolanaRPCURL())
	}},
	{"validator perf", "validator_perf", func(ctx context.Context) (any, error) {
		return handlers.FetchValidatorPerfData(ctx)
	}},
	{"stake overview", "stake_overview", func(ctx context.Context) (any, error) {
		return handlers.FetchStakeOverviewData(ctx)
	}},
	{"publisher check", "publisher_check", func(ctx context.Context) (any, error) {
		return handlers.FetchPublisherCheckData(ctx, "", 2, 0)
	}},
	{"edge scoreboard", "edge_scoreboard", func(ctx context.Context) (any, error) {
		return handlers.FetchEdgeScoreboardData(ctx, "24h")
	}},
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

	for _, entry := range entries {
		entry := entry
		g.Go(func() error {
			a.refresh(gctx, entry.name, entry.key, entry.fn)
			return nil
		})
	}

	// Metro path latency: one fetch per strategy, each written to its own key
	for _, strategy := range metroPathLatencyStrategies {
		strategy := strategy
		g.Go(func() error {
			a.refresh(gctx, "metro path latency:"+strategy, "metro_path_latency:"+strategy, func(ctx context.Context) (any, error) {
				return handlers.FetchMetroPathLatencyData(ctx, strategy)
			})
			return nil
		})
	}

	_ = g.Wait()
	a.log.Info("page cache refresh complete", "duration", time.Since(start).Round(time.Millisecond))
	return nil
}

func (a *Activities) refresh(ctx context.Context, name, key string, fn func(context.Context) (any, error)) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	result, err := fn(ctx)
	if err != nil {
		a.log.Error("cache refresh failed", "cache", name, "error", err)
		return
	}

	if err := handlers.WritePageCache(ctx, key, result); err != nil {
		a.log.Error("cache write failed", "cache", name, "error", err)
		return
	}

	a.log.Debug("cache refreshed", "cache", name)
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
