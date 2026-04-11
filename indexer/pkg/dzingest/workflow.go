package dzingest

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const (
	// refreshInterval is how often the DZ ingest workflow runs a refresh cycle.
	refreshInterval = 60 * time.Second

	// continueAsNewThreshold is the number of iterations before the workflow
	// uses continue-as-new to reset history.
	continueAsNewThreshold = 60

	// telemUsageEveryN controls how often telemetry usage refreshes run.
	// At 60s base interval, 1 iteration = ~1 minute. Source data is reported
	// at ~2s resolution so indexing every minute keeps data reasonably fresh.
	telemUsageEveryN = 1
)

// RegisterWorkflows registers all DZ ingest workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(DZIngestWorkflow)
	w.RegisterWorkflow(BackfillEscrowEventsWorkflow)
}

// DZIngestWorkflow is a long-running workflow that refreshes DZ mainnet data
// every 60 seconds. It uses continue-as-new after 60 iterations (~1 hour) to
// keep workflow history bounded.
//
// Activity failures are logged and the workflow continues to the next iteration.
func DZIngestWorkflow(ctx temporalworkflow.Context, iteration int) error {
	logger := temporalworkflow.GetLogger(ctx)

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		// Serviceability must run first — other activities depend on its
		// ClickHouse state (device/link/user dimension tables).
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshServiceability).Get(ctx, nil); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("serviceability refresh failed", "error", err)
		}

		// Run telemetry latency, shreds, escrow events, ISIS sync, and graph sync in parallel.
		telemLatencyFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshTelemetryLatency)
		shredsFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshShreds)
		escrowEventsFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshShredEscrowEvents)
		isisSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncISIS)
		graphSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncGraph)

		// Telemetry usage runs less frequently (~5 minutes).
		var telemUsageFuture temporalworkflow.Future
		if iteration%telemUsageEveryN == 0 {
			telemUsageFuture = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshTelemetryUsage)
		}

		if err := telemLatencyFuture.Get(ctx, nil); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("telemetry latency refresh failed", "error", err)
		}
		if err := shredsFuture.Get(ctx, nil); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("shreds refresh failed", "error", err)
		}
		if err := escrowEventsFuture.Get(ctx, nil); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("escrow events refresh failed", "error", err)
		}
		if err := isisSyncFuture.Get(ctx, nil); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("isis sync failed", "error", err)
		}
		if err := graphSyncFuture.Get(ctx, nil); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error("graph sync failed", "error", err)
		}
		if telemUsageFuture != nil {
			if err := telemUsageFuture.Get(ctx, nil); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logger.Error("telemetry usage refresh failed", "error", err)
			}
		}

		iteration++
		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, refreshInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, DZIngestWorkflow, 0)
}
