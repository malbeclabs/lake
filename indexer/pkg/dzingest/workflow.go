package dzingest

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/malbeclabs/lake/utils/pkg/logger"
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

	// permissionEventsEveryN controls how often the permission audit refresh runs.
	// Serviceability permission changes are sporadic, so at the 60s base interval this
	// runs every ~5 minutes — enough freshness for an audit page while avoiding a
	// per-minute getProgramAccounts poll.
	permissionEventsEveryN = 5
)

// RegisterWorkflows registers all DZ ingest workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(DZIngestWorkflow)
	w.RegisterWorkflow(BackfillEscrowEventsWorkflow)
	w.RegisterWorkflow(BackfillPermissionEventsWorkflow)
}

// DZIngestWorkflow is a long-running workflow that refreshes DZ mainnet data
// every 60 seconds. It uses continue-as-new after 60 iterations (~1 hour) to
// keep workflow history bounded.
//
// Activity failures are logged and the workflow continues to the next iteration.
func DZIngestWorkflow(ctx temporalworkflow.Context, iteration int) error {
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
		// Serviceability must run first — other activities depend on its
		// ClickHouse state (device/link/user dimension tables).
		err = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshServiceability).Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "serviceability", "serviceability refresh failed", err)

		// Run telemetry latency, geolocation, shreds, escrow events, ISIS sync, and graph sync in parallel.
		telemLatencyFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshTelemetryLatency)
		geolocationFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshGeolocation)
		shredsFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshShreds)
		escrowEventsFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshShredEscrowEvents)
		isisSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncISIS)
		mrouteSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncIPMroute)
		msdpSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncMSDP)
		graphSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncGraph)

		// Telemetry usage runs less frequently (~5 minutes).
		var telemUsageFuture temporalworkflow.Future
		if iteration%telemUsageEveryN == 0 {
			telemUsageFuture = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshTelemetryUsage)
		}

		// Permission audit events run less frequently (~5 minutes); changes are sporadic.
		var permissionEventsFuture temporalworkflow.Future
		if iteration%permissionEventsEveryN == 0 {
			permissionEventsFuture = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshPermissionEvents)
		}

		err = telemLatencyFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "telemetry_latency", "telemetry latency refresh failed", err)
		err = geolocationFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "geolocation", "geolocation refresh failed", err)
		err = shredsFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "shreds", "shreds refresh failed", err)
		err = escrowEventsFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "escrow_events", "escrow events refresh failed", err)
		err = isisSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "isis", "isis sync failed", err)
		err = mrouteSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "mroute", "mroute sync failed", err)
		err = msdpSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "msdp", "msdp sync failed", err)
		err = graphSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		esc.Observe(log, "graph", "graph sync failed", err)
		if telemUsageFuture != nil {
			err = telemUsageFuture.Get(ctx, nil)
			if err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			esc.Observe(log, "telemetry_usage", "telemetry usage refresh failed", err)
		}
		if permissionEventsFuture != nil {
			err = permissionEventsFuture.Get(ctx, nil)
			if err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			esc.Observe(log, "permission_events", "permission events refresh failed", err)
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
