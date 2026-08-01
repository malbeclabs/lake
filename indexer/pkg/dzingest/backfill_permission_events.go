package dzingest

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// BackfillPermissionEventsInput configures the permission events backfill.
type BackfillPermissionEventsInput struct {
	Truncate bool // If true, truncate the fact table + scan/account cursors before backfilling.
}

// BackfillPermissionEventsWorkflow re-scans all serviceability transaction history
// for Permission-management instructions, ignoring the stored scan cursor. Events are
// upserted via ReplacingMergeTree so re-ingesting is safe.
func BackfillPermissionEventsWorkflow(ctx temporalworkflow.Context, input BackfillPermissionEventsInput) error {
	logger := temporalworkflow.GetLogger(ctx)

	actOpts := temporalworkflow.ActivityOptions{
		// Backfill sweeps the program's full signature history, which can be large.
		StartToCloseTimeout: 60 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	if input.Truncate {
		logger.Info("truncating permission events table")
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).TruncatePermissionEvents).Get(ctx, nil); err != nil {
			return fmt.Errorf("truncate permission events: %w", err)
		}
	}

	logger.Info("backfilling permission events")
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).BackfillPermissionEvents).Get(ctx, nil); err != nil {
		return fmt.Errorf("backfill permission events: %w", err)
	}

	logger.Info("permission events backfill complete")
	return nil
}

// TruncatePermissionEvents truncates the permission events fact table and both cursors.
// The per-account cursor must reset with the fact table: left behind, it would sit
// stale-newer than the re-derived high-water marks and make the steady-state watch skip
// everything below it — a silent gap if the subsequent backfill is interrupted.
func (a *Activities) TruncatePermissionEvents(ctx context.Context) error {
	if a.PermissionEvents == nil {
		return fmt.Errorf("permission events view not configured")
	}
	conn, err := a.PermissionEvents.ClickHouse().Conn(ctx)
	if err != nil {
		return fmt.Errorf("get ClickHouse connection: %w", err)
	}
	for _, table := range []string{
		"fact_dz_permission_events",
		"dz_permission_events_scan_cursor",
		"dz_permission_events_account_cursor",
	} {
		if err := conn.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncate %s: %w", table, err)
		}
	}
	return nil
}

// BackfillPermissionEvents runs a full permission events re-scan, ignoring the cursor.
func (a *Activities) BackfillPermissionEvents(ctx context.Context) error {
	if a.PermissionEvents == nil {
		return fmt.Errorf("permission events view not configured")
	}
	result, err := a.PermissionEvents.BackfillRefresh(ctx)
	if err != nil {
		return fmt.Errorf("permission events backfill: %w", err)
	}
	a.Log.Info("permission events backfill complete", "events_inserted", result.RowsAffected)
	return nil
}
