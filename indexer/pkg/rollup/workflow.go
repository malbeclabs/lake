package rollup

import (
	"errors"
	"fmt"
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
	// Counts reset at continue-as-new (~hourly), deferring escalation by up
	// to ErrorAfter-1 iterations across the boundary — fine at threshold 3;
	// revisit before raising the threshold.
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

		// The live loop must keep running: a bucket stays in the overlap window
		// (rollupWindow) for several iterations, so a failed cycle gets another
		// chance on its own. The escalator already logged it.
		_ = runIteration(ctx, log, esc, window)

		iteration++

		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, rollupInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, ComputeRollupWorkflow, 0)
}

// runIteration executes one rollup cycle. Errors are logged via the escalator,
// which owns the paging decision, and also returned so a caller that must not
// silently skip a window can act on them. The live loop discards the return and
// continues; the backfill records it (see BackfillRollupWorkflow).
//
// A cancellation is reported as ctx.Err() rather than an activity failure: the
// window did not fail, the workflow is shutting down.
func runIteration(ctx temporalworkflow.Context, log log.Logger, esc *lakelogger.Escalator, window BackfillChunkInput) error {
	linkErr := temporalworkflow.ExecuteActivity(ctx, (*Activities).RollupLinks, window).Get(ctx, nil)
	if linkErr != nil && ctx.Err() != nil {
		return ctx.Err() // workflow context done; the error is just the cancellation
	}
	esc.Observe(log, "link_rollup", "link rollup failed", linkErr, "window_start", window.WindowStart, "window_end", window.WindowEnd)

	intfErr := temporalworkflow.ExecuteActivity(ctx, (*Activities).RollupDeviceInterfaces, window).Get(ctx, nil)
	if intfErr != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	esc.Observe(log, "device_interface_rollup", "device interface rollup failed", intfErr, "window_start", window.WindowStart, "window_end", window.WindowEnd)

	return errors.Join(linkErr, intfErr)
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
	// Counts reset at continue-as-new (~hourly), deferring escalation by up
	// to ErrorAfter-1 iterations across the boundary — fine at threshold 3;
	// revisit before raising the threshold.
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

	// A backfill is a one-shot pass: unlike the live rollup there is no overlap
	// window and no later cycle to recompute a window that failed. Advancing past
	// a failed chunk therefore leaves a permanent hole in the rollups. Collect the
	// failures instead — keep going so one bad window doesn't abandon the rest of
	// the range, then fail with the windows to re-run. Re-running is safe: both
	// rollup tables are ReplacingMergeTree keyed on the bucket, so recomputing a
	// window is idempotent.
	var failed []BackfillChunkInput
	totalChunks := 0

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

		totalChunks++
		if err := runIteration(ctx, log, esc, chunk); err != nil {
			if ctx.Err() != nil {
				return err // shutting down, not a chunk failure
			}
			failed = append(failed, chunk)
		}

		chunkStart = chunkEnd
	}

	if len(failed) > 0 {
		// Each failure was already logged with its window by the escalator, so this
		// carries the summary and the range to re-run rather than repeating them.
		return fmt.Errorf("backfill incomplete: %d of %d chunks failed, leaving gaps between %s and %s; re-run the backfill over the failed windows (each is logged with window_start/window_end)",
			len(failed), totalChunks,
			failed[0].WindowStart.Format(time.RFC3339),
			failed[len(failed)-1].WindowEnd.Format(time.RFC3339))
	}

	return nil
}
