package dzingest

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// BackfillEscrowEventsInput configures the escrow events backfill.
type BackfillEscrowEventsInput struct {
	Truncate bool // If true, truncate the fact table before backfilling.
}

// BackfillEscrowEventsWorkflow re-fetches all escrow transaction history
// from on-chain, ignoring existing high-water marks. Events are upserted
// via ReplacingMergeTree so re-ingesting is safe.
func BackfillEscrowEventsWorkflow(ctx temporalworkflow.Context, input BackfillEscrowEventsInput) error {
	logger := temporalworkflow.GetLogger(ctx)

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	if input.Truncate {
		logger.Info("truncating escrow events table")
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).TruncateEscrowEvents).Get(ctx, nil); err != nil {
			return fmt.Errorf("truncate escrow events: %w", err)
		}
	}

	logger.Info("backfilling escrow events")
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).BackfillEscrowEvents).Get(ctx, nil); err != nil {
		return fmt.Errorf("backfill escrow events: %w", err)
	}

	logger.Info("escrow events backfill complete")
	return nil
}

// TruncateEscrowEvents truncates the escrow events fact table.
func (a *Activities) TruncateEscrowEvents(ctx context.Context) error {
	if a.EscrowEvents == nil {
		return fmt.Errorf("escrow events view not configured")
	}
	conn, err := a.EscrowEvents.ClickHouse().Conn(ctx)
	if err != nil {
		return fmt.Errorf("get ClickHouse connection: %w", err)
	}
	return conn.Exec(ctx, "TRUNCATE TABLE fact_dz_shred_escrow_events")
}

// BackfillEscrowEvents runs a full escrow events refresh, ignoring high-water marks.
func (a *Activities) BackfillEscrowEvents(ctx context.Context) error {
	if a.EscrowEvents == nil {
		return fmt.Errorf("escrow events view not configured")
	}

	// First ensure the shreds view has escrow data cached.
	if a.Shreds != nil {
		if _, err := a.Shreds.Refresh(ctx); err != nil {
			return fmt.Errorf("shreds refresh (for escrow list): %w", err)
		}
	}

	result, err := a.EscrowEvents.BackfillRefresh(ctx)
	if err != nil {
		return fmt.Errorf("escrow events backfill: %w", err)
	}

	a.Log.Info("escrow events backfill complete", "events_inserted", result.RowsAffected)
	return nil
}
