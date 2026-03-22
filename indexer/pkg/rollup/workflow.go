package rollup

import (
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const (
	// rollupInterval is how often the rollup workflow computes new buckets.
	rollupInterval = 30 * time.Second

	// continueAsNewThreshold is the number of iterations before the workflow
	// uses continue-as-new to reset history and avoid unbounded growth.
	continueAsNewThreshold = 60
)

// RegisterWorkflows registers all rollup workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(ComputeRollupWorkflow)
	w.RegisterWorkflow(BackfillRollupWorkflow)
}

// ComputeRollupWorkflow is a long-running workflow that computes rollup buckets
// every 60 seconds. It uses continue-as-new after 60 iterations (~1 hour) to
// keep workflow history bounded.
//
// Activity failures are logged and the workflow continues to the next iteration
// rather than failing, so the rollup loop runs indefinitely.
func ComputeRollupWorkflow(ctx temporalworkflow.Context, iteration int) error {
	logger := temporalworkflow.GetLogger(ctx)

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		now := temporalworkflow.Now(ctx)
		window := BackfillChunkInput{
			WindowStart: now.Add(-10 * time.Minute).Truncate(5 * time.Minute),
			WindowEnd:   now.Truncate(5 * time.Minute),
		}

		runIteration(ctx, logger, window)

		iteration++

		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, rollupInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, ComputeRollupWorkflow, 0)
}

// runIteration executes one rollup cycle. Errors are logged, not returned,
// so the workflow loop continues on failure.
func runIteration(ctx temporalworkflow.Context, logger log.Logger, window BackfillChunkInput) {
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).RollupLinks, window).Get(ctx, nil); err != nil {
		logger.Error("link rollup failed", "error", err, "window_start", window.WindowStart, "window_end", window.WindowEnd)
	}
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).RollupDeviceInterfaces, window).Get(ctx, nil); err != nil {
		logger.Error("device interface rollup failed", "error", err, "window_start", window.WindowStart, "window_end", window.WindowEnd)
	}
}

// BackfillRollupWorkflow processes historical data in time chunks.
func BackfillRollupWorkflow(ctx temporalworkflow.Context, input BackfillInput) error {
	if input.ChunkSize == 0 {
		input.ChunkSize = 1 * time.Hour
	}

	logger := temporalworkflow.GetLogger(ctx)

	chunkOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, chunkOpts)

	chunkStart := input.StartTime
	for chunkStart.Before(input.EndTime) {
		chunkEnd := chunkStart.Add(input.ChunkSize)
		if chunkEnd.After(input.EndTime) {
			chunkEnd = input.EndTime
		}

		chunk := BackfillChunkInput{
			WindowStart:    chunkStart,
			WindowEnd:      chunkEnd,
			SourceDatabase: input.SourceDatabase,
		}

		runIteration(ctx, logger, chunk)

		chunkStart = chunkEnd
	}

	return nil
}
