// Package rollup runs Temporal workflows that roll up raw telemetry into
// pre-aggregated bucket tables in ClickHouse. It is designed to be embedded
// in the indexer process (started by default) and can host multiple workflow
// types as they are added over time.
package rollup

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Config configures the rollup worker.
type Config struct {
	Log *slog.Logger

	// Network identifies the DZ environment (e.g. "mainnet-beta", "testnet", "devnet").
	// Used to namespace the Temporal task queue and workflow ID.
	// Empty string preserves the legacy "indexer-rollup" naming.
	Network string

	// ClickHouse connection parameters.
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
	ClickHouseSecure   bool
}

// Start connects to ClickHouse and Temporal, then begins processing rollup
// workflows. It blocks until ctx is cancelled or an error occurs.
func Start(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	// Open a dedicated ClickHouse connection for the rollup worker.
	chOpts := &clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUsername,
			Password: cfg.ClickHousePassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 120,
		},
		DialTimeout: 5 * time.Second,
	}
	if cfg.ClickHouseSecure {
		chOpts.TLS = &tls.Config{}
	}

	chConn, err := clickhouse.Open(chOpts)
	if err != nil {
		return fmt.Errorf("rollup: clickhouse open: %w", err)
	}
	if err := chConn.Ping(ctx); err != nil {
		chConn.Close()
		return fmt.Errorf("rollup: clickhouse ping: %w", err)
	}
	defer chConn.Close()
	log.Info("rollup: clickhouse connected", "addr", cfg.ClickHouseAddr, "database", cfg.ClickHouseDatabase)

	// Connect to Temporal
	temporalHost := envOrDefault("TEMPORAL_HOST_PORT", "localhost:7233")
	temporalNS := envOrDefault("TEMPORAL_NAMESPACE", "default")
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  temporalHost,
		Namespace: temporalNS,
		Logger:    newTemporalLogger(log),
	})
	if err != nil {
		return fmt.Errorf("rollup: temporal dial: %w", err)
	}
	defer tc.Close()
	log.Info("rollup: temporal connected", "host", temporalHost, "namespace", temporalNS)

	tq := taskQueue(cfg.Network)
	wfID := workflowID(cfg.Network)

	// Register rollup workflows
	ingestionLogWriter := ingestionlog.NewWriter(chConn, log)
	activities := &Activities{
		ClickHouse:        chConn,
		Log:               log.With("component", "rollup"),
		IngestionLog:      ingestionLogWriter,
		Network:           cfg.Network,
		TelemetryDatabase: telemetryDatabaseForNetwork(cfg.Network),
	}

	w := worker.New(tc, tq, worker.Options{})
	RegisterWorkflows(w)
	w.RegisterActivity(activities)

	run, err := startComputeRollupWorkflow(ctx, tc, log, cfg.Network)
	if err != nil {
		return err
	}

	// Watch the workflow in the background so failures surface in logs.
	go watchWorkflow(ctx, tc, log, wfID, run)

	log.Info("rollup: starting worker", "task_queue", tq)

	// Run blocks until ctx is cancelled or worker error
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(worker.InterruptCh()) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// computeRollupStartOptions returns the start options for the compute-rollup
// workflow. Both fields are load-bearing and neither may be relaxed — see
// "Temporal Workflow Restarts on Deploy" in CLAUDE.md for what each one does and
// what silently breaks without it.
func computeRollupStartOptions(network string) temporalclient.StartWorkflowOptions {
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
		log.Warn("rollup: workflow interrupted, reattaching", "id", wfID, "error", err)
		current = tc.GetWorkflow(ctx, wfID, "")
	}
}

// isWorkflowTerminated reports whether err is a run that was terminated rather
// than one that failed.
func isWorkflowTerminated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "terminated")
}

// startComputeRollupWorkflow starts a fresh compute-rollup run, terminating any
// run left over from a previous deploy. The run ID is logged because it is the
// only way to tell a fresh run from an adopted one.
func startComputeRollupWorkflow(ctx context.Context, tc temporalclient.Client, log *slog.Logger, network string) (temporalclient.WorkflowRun, error) {
	opts := computeRollupStartOptions(network)
	run, err := tc.ExecuteWorkflow(ctx, opts, ComputeRollupWorkflow, 0)
	if err != nil {
		return nil, fmt.Errorf("rollup: failed to start workflow: %w", err)
	}
	log.Info("rollup: workflow started", "id", opts.ID, "run_id", run.GetRunID())
	return run, nil
}

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {} // no-op to avoid blocking workflow goroutine
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
	// Temporal logs "Activity error." at ERROR on every failed attempt,
	// including non-final ones the activity's retry policy will recover. A
	// transient cause (ClickHouse connection blip, timeout) self-heals on
	// retry, so demote to WARN; a sustained failure still pages via the
	// workflow-side Escalator once retries are exhausted across iterations.
	if msg == "Activity error." && isTransientActivityError(keyvals) {
		l.log.Warn(msg, keyvals...)
		return
	}
	l.log.Error(msg, keyvals...)
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

// isTransientActivityError reports whether Temporal's activity-error keyvals
// carry an Error that dberror classifies as transient (a self-healing upstream
// blip the activity's retry policy will recover). At the "Activity error." log
// site the Error keyval is the raw error the activity returned (conversion to
// the SDK's ApplicationError happens afterward). dberror.Classify matches on
// the message string, so classification would also hold if a converted error
// ever appeared here.
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

// telemetryDatabaseForNetwork returns the gNMI telemetry database for a DZ network
// (e.g. "mainnet-beta" -> "telemetry_mainnet_beta"). It is a sibling database on the
// same ClickHouse cluster as the lake DB. Empty network -> "" (gNMI off, fact-only).
func telemetryDatabaseForNetwork(network string) string {
	if network == "" {
		return ""
	}
	return "telemetry_" + strings.ReplaceAll(network, "-", "_")
}
