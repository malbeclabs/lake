// Package dzingest runs a Temporal workflow that periodically refreshes
// DoubleZero data: serviceability state, telemetry latency, telemetry
// usage, Neo4j graph sync, and IS-IS topology sync. One workflow instance
// runs per network (mainnet-beta, testnet, devnet).
package dzingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	dzgeoloc "github.com/malbeclabs/lake/indexer/pkg/dz/geolocation"
	dzgraph "github.com/malbeclabs/lake/indexer/pkg/dz/graph"
	"github.com/malbeclabs/lake/indexer/pkg/dz/isis"
	"github.com/malbeclabs/lake/indexer/pkg/dz/mroute"
	"github.com/malbeclabs/lake/indexer/pkg/dz/msdp"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	"github.com/malbeclabs/lake/indexer/pkg/dz/serviceability/permissionevents"
	dzshreds "github.com/malbeclabs/lake/indexer/pkg/dz/shreds"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/escrowevents"
	"github.com/malbeclabs/lake/indexer/pkg/dz/shreds/feedsubscription"
	dztelemlatency "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/latency"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// Config configures the DZ ingest worker.
type Config struct {
	Log          *slog.Logger
	IngestionLog *ingestionlog.Writer // optional

	// Network identifies the DZ environment (e.g. "mainnet-beta", "testnet", "devnet").
	// Used to namespace the Temporal task queue and workflow ID.
	Network string

	// Views and stores for activity execution.
	Serviceability   *dzsvc.View
	Geolocation      *dzgeoloc.View         // optional
	Shreds           *dzshreds.View         // optional
	EscrowEvents     *escrowevents.View     // optional
	FeedSubscription *feedsubscription.View // optional
	PermissionEvents *permissionevents.View // optional
	TelemLatency     *dztelemlatency.View
	TelemUsage       *dztelemusage.View // optional
	GraphStore       *dzgraph.Store     // optional
	ISISSource       isis.Source        // optional
	ISISStore        *isis.Store        // optional
	MrouteSource     mroute.Source      // optional
	MrouteStore      *mroute.Store      // optional
	MSDPSource       msdp.Source        // optional
	MSDPStore        *msdp.Store        // optional
}

// TaskQueue returns the Temporal task queue name for the given network.
func TaskQueue(network string) string { return "indexer-dz-ingest-" + network }

func taskQueue(network string) string  { return TaskQueue(network) }
func workflowID(network string) string { return TaskQueue(network) }

// Start connects to Temporal and begins processing DZ ingest workflows.
// It blocks until ctx is cancelled or an error occurs.
func Start(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	temporalHost := envOrDefault("TEMPORAL_HOST_PORT", "localhost:7233")
	temporalNS := envOrDefault("TEMPORAL_NAMESPACE", "default")
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  temporalHost,
		Namespace: temporalNS,
		Logger:    newTemporalLogger(log),
	})
	if err != nil {
		return fmt.Errorf("dzingest: temporal dial: %w", err)
	}
	defer tc.Close()
	log.Info("dzingest: temporal connected", "host", temporalHost, "namespace", temporalNS)

	tq := taskQueue(cfg.Network)
	wfID := workflowID(cfg.Network)

	activities := &Activities{
		Log:              log.With("component", "dz-ingest"),
		IngestionLog:     cfg.IngestionLog,
		Network:          cfg.Network,
		Serviceability:   cfg.Serviceability,
		Geolocation:      cfg.Geolocation,
		Shreds:           cfg.Shreds,
		EscrowEvents:     cfg.EscrowEvents,
		FeedSubscription: cfg.FeedSubscription,
		PermissionEvents: cfg.PermissionEvents,
		TelemLatency:     cfg.TelemLatency,
		TelemUsage:       cfg.TelemUsage,
		GraphStore:       cfg.GraphStore,
		ISISSource:       cfg.ISISSource,
		ISISStore:        cfg.ISISStore,
		MrouteSource:     cfg.MrouteSource,
		MrouteStore:      cfg.MrouteStore,
		MSDPSource:       cfg.MSDPSource,
		MSDPStore:        cfg.MSDPStore,
	}

	w := worker.New(tc, tq, worker.Options{})
	RegisterWorkflows(w)
	w.RegisterActivity(activities)

	run, err := startDZIngestWorkflow(ctx, tc, log, cfg.Network)
	if err != nil {
		return err
	}

	go watchWorkflow(ctx, tc, log, wfID, run)

	log.Info("dzingest: starting worker", "task_queue", tq)

	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(worker.InterruptCh()) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// deployStartOptions returns the start options for the DZ ingest workflow. Both
// fields are load-bearing and neither may be relaxed — see "Temporal Workflow
// Restarts on Deploy" in CLAUDE.md for what each one does and what silently
// breaks without it.
func deployStartOptions(network string) temporalclient.StartWorkflowOptions {
	return temporalclient.StartWorkflowOptions{
		ID:                                       workflowID(network),
		TaskQueue:                                taskQueue(network),
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
}

// watchWorkflow surfaces workflow failures in logs, reattaching to the workflow
// ID's current run when the one it was watching ends. It returns on a terminated
// run rather than reattaching: a terminate that no start followed leaves the same
// closed run as current, whose Get errors immediately, so reattaching to it spins
// at RPC speed. The deploy case needs no reattach either — this process's own
// start terminated the previous run, and a new process watches the new one.
func watchWorkflow(ctx context.Context, tc temporalclient.Client, log *slog.Logger, wfID string, run temporalclient.WorkflowRun) {
	current := run
	for {
		err := current.Get(ctx, nil)
		if err == nil || ctx.Err() != nil || isWorkflowTerminated(err) {
			return
		}
		log.Warn("dzingest: workflow interrupted, reattaching", "id", wfID, "error", err)
		current = tc.GetWorkflow(ctx, wfID, "")
	}
}

// isWorkflowTerminated reports whether err is a run that was terminated rather
// than one that failed.
func isWorkflowTerminated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "terminated")
}

// startDZIngestWorkflow starts a fresh DZ ingest run, terminating any run left
// over from a previous deploy. The run ID is logged because it is the only way to
// tell a fresh run from an adopted one.
func startDZIngestWorkflow(ctx context.Context, tc temporalclient.Client, log *slog.Logger, network string) (temporalclient.WorkflowRun, error) {
	opts := deployStartOptions(network)
	run, err := tc.ExecuteWorkflow(ctx, opts, DZIngestWorkflow, 0)
	if err != nil {
		return nil, fmt.Errorf("dzingest: failed to start workflow: %w", err)
	}
	log.Info("dzingest: workflow started", "id", opts.ID, "run_id", run.GetRunID())
	return run, nil
}

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {} // suppress noisy debug logs
func (l *temporalLogger) Info(msg string, keyvals ...any) {
	// Temporal logs "Task processing failed with error" at INFO when an
	// activity reports back to a workflow that's already completed or
	// continued-as-new (typical during deploys). The Error= keyval trips
	// cloud-log heuristics that promote the line to ERROR severity, so
	// demote to Debug to keep prod logs quiet.
	if msg == "Task processing failed with error" && hasBenignTaskProcessingError(keyvals) {
		l.log.Debug(msg, keyvals...)
		return
	}
	l.log.Info(msg, keyvals...)
}
func (l *temporalLogger) Warn(msg string, keyvals ...any) { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) {
	if isContextCancellation(keyvals) {
		l.log.Warn(msg, keyvals...)
		return
	}
	// The periodic DZ ingest activities return nil to Temporal (see
	// activities.refresh), so they never reach this "Activity error." log. The
	// manual backfill activities do return their error, and BackfillRefresh
	// shares the permission-events near-tip not-found gate — a transient-by-
	// design condition (dberror.ErrTransient) that would otherwise page an
	// attended backfill. Demote transient causes to WARN; non-transient
	// failures still log ERROR. Mirrors the rollup worker's temporalLogger.
	if msg == "Activity error." && isTransientActivityError(keyvals) {
		l.log.Warn(msg, keyvals...)
		return
	}
	l.log.Error(msg, keyvals...)
}

// isTransientActivityError reports whether Temporal's activity-error keyvals
// carry an Error that dberror classifies as transient (a self-healing upstream
// blip, or one explicitly marked with dberror.ErrTransient). At the "Activity
// error." log site the Error keyval is the raw error the activity returned.
func isTransientActivityError(keyvals []any) bool {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if keyvals[i] != "Error" {
			continue
		}
		if err, ok := keyvals[i+1].(error); ok {
			return dberror.IsTransient(err)
		}
	}
	return false
}

// hasBenignTaskProcessingError reports whether Temporal's "Task processing
// failed with error" keyvals carry an error that's expected during deploys
// (activity reports back after the workflow has already finished or been
// continue-as-newed; in-flight RPC observes the gRPC client connection
// closing as the pod shuts down).
func hasBenignTaskProcessingError(keyvals []any) bool {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if keyvals[i] != "Error" {
			continue
		}
		if err, ok := keyvals[i+1].(error); ok {
			msg := err.Error()
			if strings.Contains(msg, "workflow execution already completed") ||
				strings.Contains(msg, "grpc: the client connection is closing") {
				return true
			}
		}
	}
	return false
}

// isContextCancellation checks Temporal's key-value log pairs for errors
// caused by context cancellation (e.g. worker shutdown).
func isContextCancellation(keyvals []any) bool {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if keyvals[i] == "Error" {
			if err, ok := keyvals[i+1].(error); ok {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return true
				}
			}
		}
	}
	return false
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
