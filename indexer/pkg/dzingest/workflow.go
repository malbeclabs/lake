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

	// telemUsageStartToCloseTimeout is the dedicated activity deadline for
	// RefreshTelemetryUsage, larger than the shared 5m. A capped catch-up refresh
	// runs 3 Flux queries (overlap re-read + two new chunks), each bounded by
	// defaultFluxHTTPTimeout (4m), so the worst case is 12m of InfluxDB plus the
	// ClickHouse dedup/baseline/insert work; 15m bounds that. This deadline does
	// cancel the Flux queries, which is the point: set below the InfluxDB worst
	// case it cancels mid-query, so the insert never runs and maxTime is pinned in
	// a retry loop (the #665/#671 failure mode). (The rare InfluxDB baseline
	// fallback adds a further 120s before the chunked read, leaving ~1m for
	// ClickHouse — tighter than the 60s max_execution_time the baseline scan alone
	// can use, but it needs a cache miss plus 0 ClickHouse baselines and the next
	// cycle recovers.)
	//
	// #711 and #714 set this value and the span it must cover independently, and
	// the pair drifted into incoherence — 22.6h of frozen staging ingest (#740).
	// dztelemusage.WorstCaseRefreshFluxBudget now names the InfluxDB worst case and
	// workflow_test asserts this deadline exceeds it; the view additionally shrinks
	// its catch-up span on a failed capped cycle, so an environment that still
	// overruns cannot repeat identical work forever.
	telemUsageStartToCloseTimeout = 15 * time.Minute

	// permissionEventsEveryN controls how often the permission audit refresh runs.
	// Serviceability permission changes are sporadic, so at the 60s base interval this
	// runs every ~5 minutes — enough freshness for an audit page while avoiding a
	// per-minute getProgramAccounts poll.
	permissionEventsEveryN = 5

	// pcapWarehouseStartToClose bounds one SyncPCAPWarehouse pass, whose own budget
	// (pcapwarehouse.DefaultBudget) is set inside it.
	pcapWarehouseStartToClose = 5 * time.Minute

	// pcapWarehouseStartLead is how many iterations before continue-as-new the loop
	// stops starting warehouse passes: a pass resolves within
	// pcapWarehouseStartToClose, so over the 60s interval (plus one for the cycle's own
	// length) one started this far out is done by the time the loop drains it, and the
	// drain never holds up the boundary.
	pcapWarehouseStartLead = int(pcapWarehouseStartToClose/refreshInterval) + 1
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

	// Every dzingest activity goes through activities.refresh, which swallows
	// the error (returns nil to Temporal) and owns the paging decision via its
	// own escalation-gated counter. Errors still reaching the workflow future
	// are Temporal-level: StartToClose timeouts (the activity function also
	// returns on the expired ctx and logs activity-side), scheduling failures,
	// and activity panics (paged by the SDK's "Activity error." line, which
	// temporalLogger demotes only for transient causes). A second alert-bearing
	// line here doubled every page (#730, a #697 regression via #711's
	// MaximumAttempts: 1), so log these at WARN: the Temporal-level cause
	// (timeout vs scheduling) stays visible for debugging without paging. Per
	// the #696/#697 principle, one failure yields at most one alert-bearing
	// line, owned by the layer with escalation context.
	//
	// Consequence: time-to-page for a sustained timeout now follows the
	// activity-side thresholds (3 consecutive, or 10 for a transient-classified
	// cause that self-heals) instead of the former workflow-escalator's flat 3.
	// Intended — the activity layer's error classification owns the decision,
	// matching how every other activity already escalates.
	warnOnErr := func(msg string, err error) {
		if err != nil {
			log.Warn(msg, "error", err)
		}
	}
	var err error

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	// The pcap warehouse pass is not waited on by the iteration that starts it. A
	// backfill fills its whole budget for hours, and every other dataset here waits on
	// the slowest activity of the cycle, so holding the loop for it would slow them all
	// for as long as the backfill runs. A pass may span iterations instead, and the
	// next starts only once it has resolved. IsReady resolves from workflow history,
	// so the decision replays deterministically.
	pcapWarehouseCtx := temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: pcapWarehouseStartToClose,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	var pcapWarehouseFuture temporalworkflow.Future

	for iteration < continueAsNewThreshold {
		// Serviceability must run first — other activities depend on its
		// ClickHouse state (device/link/user dimension tables).
		err = temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshServiceability).Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("serviceability refresh failed", err)

		// Run telemetry latency, geolocation, shreds, escrow events, ISIS sync, and graph sync in parallel.
		telemLatencyFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshTelemetryLatency)
		geolocationFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshGeolocation)
		shredsFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshShreds)
		escrowEventsFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshShredEscrowEvents)
		isisSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncISIS)
		mrouteSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncIPMroute)
		msdpSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncMSDP)
		graphSyncFuture := temporalworkflow.ExecuteActivity(ctx, (*Activities).SyncGraph)

		if pcapWarehouseFuture != nil && pcapWarehouseFuture.IsReady() {
			warnOnErr("pcap warehouse sync failed", pcapWarehouseFuture.Get(ctx, nil))
			pcapWarehouseFuture = nil
		}
		if pcapWarehouseFuture == nil && iteration < continueAsNewThreshold-pcapWarehouseStartLead {
			pcapWarehouseFuture = temporalworkflow.ExecuteActivity(pcapWarehouseCtx, (*Activities).SyncPCAPWarehouse)
		}

		// Telemetry usage runs every iteration (~1 minute) but under a dedicated
		// longer deadline (telemUsageStartToCloseTimeout) because a refresh
		// runs up to 3 Flux queries. Use MaximumAttempts: 1, not the shared
		// retry policy: the workflow reschedules this activity every iteration,
		// so a Temporal retry adds no recovery a re-run wouldn't — while a
		// 15m×3 retry chain would block the whole ingest loop for up to ~45m
		// and stack attempts on refreshMu behind a first attempt whose Flux
		// query ignores the expired ctx. One attempt keeps the worst-case loop
		// stall at 15m.
		var telemUsageFuture temporalworkflow.Future
		if iteration%telemUsageEveryN == 0 {
			telemUsageCtx := temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
				StartToCloseTimeout: telemUsageStartToCloseTimeout,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
			})
			telemUsageFuture = temporalworkflow.ExecuteActivity(telemUsageCtx, (*Activities).RefreshTelemetryUsage)
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
		warnOnErr("telemetry latency refresh failed", err)
		err = geolocationFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("geolocation refresh failed", err)
		err = shredsFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("shreds refresh failed", err)
		err = escrowEventsFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("escrow events refresh failed", err)
		err = isisSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("isis sync failed", err)
		err = mrouteSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("mroute sync failed", err)
		err = msdpSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("msdp sync failed", err)
		err = graphSyncFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("graph sync failed", err)
		if telemUsageFuture != nil {
			err = telemUsageFuture.Get(ctx, nil)
			if err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			warnOnErr("telemetry usage refresh failed", err)
		}
		if permissionEventsFuture != nil {
			err = permissionEventsFuture.Get(ctx, nil)
			if err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			warnOnErr("permission events refresh failed", err)
		}

		iteration++
		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, refreshInterval); err != nil {
				return err
			}
		}
	}

	// Drain the outstanding warehouse pass: a pending activity is cancelled at the
	// continue-as-new boundary. None starts within pcapWarehouseStartLead iterations of
	// it, so this has already resolved and returns without blocking.
	if pcapWarehouseFuture != nil {
		err = pcapWarehouseFuture.Get(ctx, nil)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		warnOnErr("pcap warehouse sync failed", err)
	}

	return temporalworkflow.NewContinueAsNewError(ctx, DZIngestWorkflow, 0)
}
