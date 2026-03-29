package solingest

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
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
	logger := temporalworkflow.GetLogger(ctx)

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		// Solana validator state must run first — GeoIP depends on gossip IPs.
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshSolana).Get(ctx, nil); err != nil {
			logger.Error("solana refresh failed", "error", err)
		}

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

		if err := geoipFuture.Get(ctx, nil); err != nil {
			logger.Error("geoip refresh failed", "error", err)
		}
		if blockProdFuture != nil {
			if err := blockProdFuture.Get(ctx, nil); err != nil {
				logger.Error("block production refresh failed", "error", err)
			}
		}
		if validatorsAppFuture != nil {
			if err := validatorsAppFuture.Get(ctx, nil); err != nil {
				logger.Error("validatorsapp refresh failed", "error", err)
			}
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
