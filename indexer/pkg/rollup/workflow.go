package rollup

import (
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"

	lakelogger "github.com/malbeclabs/lake/utils/pkg/logger"
)

const (
	// rollupInterval is how often the rollup workflow computes new buckets.
	rollupInterval = 30 * time.Second

	// rollupWindow is how far back the live rollup looks. Each bucket stays
	// in the window for this duration, giving it multiple chances to be
	// processed. Larger values are more resilient to worker stalls but do
	// more redundant work.
	rollupWindow = 30 * time.Minute

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
	log := temporalworkflow.GetLogger(ctx)

	// Activity errors reaching the workflow are Temporal-level (StartToClose
	// timeouts, scheduling failures) — the activity-side failure counter never
	// sees them, and timeouts classify as transient, so escalate them at the
	// strict threshold to keep sustained timeouts visible.
	esc := &lakelogger.Escalator{TransientErrorAfter: lakelogger.DefaultErrorAfter}

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
			WindowStart: now.Add(-rollupWindow).Truncate(5 * time.Minute),
			WindowEnd:   now,
		}

		runIteration(ctx, log, esc, window)

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
func runIteration(ctx temporalworkflow.Context, log log.Logger, esc *lakelogger.Escalator, window BackfillChunkInput) {
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).RollupLinks, window).Get(ctx, nil); err != nil {
		if ctx.Err() == nil {
			esc.Fail(log, "link_rollup", "link rollup failed", "error", err, "window_start", window.WindowStart, "window_end", window.WindowEnd)
		}
	} else {
		esc.Reset("link_rollup")
	}
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).RollupDeviceInterfaces, window).Get(ctx, nil); err != nil {
		if ctx.Err() == nil {
			esc.Fail(log, "device_interface_rollup", "device interface rollup failed", "error", err, "window_start", window.WindowStart, "window_end", window.WindowEnd)
		}
	} else {
		esc.Reset("device_interface_rollup")
	}
}

// BackfillRollupWorkflow processes historical data in time chunks.
func BackfillRollupWorkflow(ctx temporalworkflow.Context, input BackfillInput) error {
	if input.ChunkSize == 0 {
		input.ChunkSize = 1 * time.Hour
	}

	log := temporalworkflow.GetLogger(ctx)

	// Activity errors reaching the workflow are Temporal-level (StartToClose
	// timeouts, scheduling failures) — the activity-side failure counter never
	// sees them, and timeouts classify as transient, so escalate them at the
	// strict threshold to keep sustained timeouts visible.
	esc := &lakelogger.Escalator{TransientErrorAfter: lakelogger.DefaultErrorAfter}

	chunkOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, chunkOpts)

	// Cap end time to avoid overlapping with the live rollup window.
	// The live rollup covers the last rollupWindow, so backfill should
	// stop before that to avoid resource contention and dropped buckets.
	endTime := input.EndTime
	liveStart := temporalworkflow.Now(ctx).Add(-rollupWindow).Truncate(5 * time.Minute)
	if endTime.After(liveStart) {
		endTime = liveStart
		log.Info("capped backfill end time to avoid live rollup overlap", "end_time", endTime)
	}

	chunkStart := input.StartTime
	for chunkStart.Before(endTime) {
		chunkEnd := chunkStart.Add(input.ChunkSize)
		if chunkEnd.After(endTime) {
			chunkEnd = endTime
		}

		chunk := BackfillChunkInput{
			WindowStart:    chunkStart,
			WindowEnd:      chunkEnd,
			SourceDatabase: input.SourceDatabase,
		}

		runIteration(ctx, log, esc, chunk)

		chunkStart = chunkEnd
	}

	return nil
}
