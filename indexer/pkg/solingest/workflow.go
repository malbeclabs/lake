package solingest

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/malbeclabs/lake/utils/pkg/logger"
)

const (
	// refreshInterval is how often the Solana ingest workflow runs a refresh cycle.
	refreshInterval = 60 * time.Second

	// continueAsNewThreshold is the number of iterations before the workflow
	// uses continue-as-new to reset history.
	continueAsNewThreshold = 60

	// blockProductionEveryN controls how often block production refreshes run.
	// At 60s base interval, 60 iterations = ~1 hour.
	blockProductionEveryN = 60

	// validatorsAppEveryN controls how often validators.app refreshes run.
	// At 60s base interval, 5 iterations = ~5 minutes.
	validatorsAppEveryN = 5
)

// RegisterWorkflows registers all Solana ingest workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(SolIngestWorkflow)
}

// SolIngestWorkflow is a long-running workflow that refreshes Solana-related
// data every 60 seconds. It uses continue-as-new after 60 iterations (~1 hour)
// to keep workflow history bounded.
//
// Activity failures are logged and the workflow continues to the next iteration.
func SolIngestWorkflow(ctx temporalworkflow.Context, iteration int) error {
	log := temporalworkflow.GetLogger(ctx)

	// Activity errors reaching the workflow are Temporal-level (StartToClose
	// timeouts, scheduling failures) — the activity-side failure counter never
	// sees them, and timeouts classify as transient, so escalate them at the
	// strict threshold to keep sustained timeouts visible.
	// Counts reset at continue-as-new (~hourly), deferring escalation by up
	// to ErrorAfter-1 iterations across the boundary — fine at threshold 3;
	// revisit before raising the threshold.
	esc := &logger.Escalator{TransientErrorAfter: logger.DefaultErrorAfter}
	var err error

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		// Solana validator state must run first — GeoIP depends on gossip IPs.
		err = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshSolana).Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "solana", "solana refresh failed", err)

		// Run GeoIP in parallel with optional activities.
		geoipFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshGeoIP)

		// Block production runs hourly.
		var blockProdFuture temporalworkflow.Future
		if iteration%blockProductionEveryN == 0 {
			blockProdFuture = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshBlockProduction)
		}

		// validators.app runs every ~5 minutes.
		var validatorsAppFuture temporalworkflow.Future
		if iteration%validatorsAppEveryN == 0 {
			validatorsAppFuture = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshValidatorsApp)
		}

		err = geoipFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "geoip", "geoip refresh failed", err)
		if blockProdFuture != nil {
			err = blockProdFuture.Get(ctx, nil)
			if err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			esc.Observe(log, "block_production", "block production refresh failed", err)
		}
		if validatorsAppFuture != nil {
			err = validatorsAppFuture.Get(ctx, nil)
			if err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			esc.Observe(log, "validatorsapp", "validatorsapp refresh failed", err)
		}

		iteration++
		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, refreshInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, SolIngestWorkflow, 0)
}
