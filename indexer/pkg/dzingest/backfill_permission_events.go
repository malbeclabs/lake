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
	Truncate bool // If true, truncate the fact table + scan cursor before backfilling.
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

// TruncatePermissionEvents truncates the permission events fact table and its scan cursor.
func (a *Activities) TruncatePermissionEvents(ctx context.Context) error {
	if a.PermissionEvents == nil {
		return fmt.Errorf("permission events view not configured")
	}
	conn, err := a.PermissionEvents.ClickHouse().Conn(ctx)
	if err != nil {
		return fmt.Errorf("get ClickHouse connection: %w", err)
	}
	if err := conn.Exec(ctx, "TRUNCATE TABLE fact_dz_permission_events"); err != nil {
		return fmt.Errorf("truncate fact_dz_permission_events: %w", err)
	}
	if err := conn.Exec(ctx, "TRUNCATE TABLE dz_permission_events_scan_cursor"); err != nil {
		return fmt.Errorf("truncate dz_permission_events_scan_cursor: %w", err)
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
