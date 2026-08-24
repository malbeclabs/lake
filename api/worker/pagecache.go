package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/utils/pkg/logger"
)

// Config configures the page cache worker.
type Config struct {
	Log *slog.Logger
	API *handlers.API
}

// Start connects to Temporal, registers workflows and activities, then runs
// the page cache worker. It blocks until ctx is cancelled or an error occurs.
func Start(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	// Load-shaping knobs (see PageCacheParams / loadRefreshConfig), overridable
	// per-environment. Staging runs gentler values (lower concurrency, longer
	// interval) to spread page-cache load across its smaller self-hosted
	// ClickHouse rather than scaling it up. Read once here; the interval/timeout
	// go into the workflow as arguments (replay-deterministic) and concurrency
	// onto the activity struct.
	params, concurrency := loadRefreshConfig(log)
	params = params.withDefaults() // derive ContinueAsNewThreshold (single source) for the log + workflow arg
	log.Info("page-cache: refresh config",
		"interval", params.RefreshInterval, "concurrency", concurrency,
		"activity_timeout", params.ActivityTimeout, "continue_as_new_after", params.ContinueAsNewThreshold)

	// Connect to Temporal
	temporalHost := envOrDefault("TEMPORAL_HOST_PORT", "localhost:7233")
	temporalNS := envOrDefault("TEMPORAL_NAMESPACE", "default")
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  temporalHost,
		Namespace: temporalNS,
		Logger:    newTemporalLogger(log),
	})
	if err != nil {
		return fmt.Errorf("page-cache: temporal dial: %w", err)
	}
	defer tc.Close()
	log.Info("page-cache: temporal connected", "host", temporalHost, "namespace", temporalNS)

	// Register workflows and activities
	activities := newActivities(log, cfg.API, concurrency)

	w := worker.New(tc, TaskQueue, worker.Options{})
	w.RegisterWorkflow(PageCacheWorkflow)
	w.RegisterActivity(activities)

	run, err := startPageCacheWorkflow(ctx, tc, log, params)
	if err != nil {
		return err
	}

	// Watch the workflow in the background so failures surface in logs.
	// Suppress "terminated" errors — a new deploy terminates the previous
	// workflow before the old process's context is cancelled.
	go func() {
		if err := run.Get(ctx, nil); err != nil && ctx.Err() == nil && !isWorkflowTerminated(err) {
			log.Error("page-cache: workflow failed", "id", WorkflowID, "error", err)
		}
	}()

	log.Info("page-cache: starting worker", "task_queue", TaskQueue)

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

// pageCacheStartOptions returns the start options for the page-cache workflow.
// Both fields are load-bearing and neither may be relaxed — see "Temporal
// Workflow Restarts on Deploy" in CLAUDE.md for what each one does and what
// silently breaks without it.
func pageCacheStartOptions() temporalclient.StartWorkflowOptions {
	return temporalclient.StartWorkflowOptions{
		ID:                                       WorkflowID,
		TaskQueue:                                TaskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}
}

// startPageCacheWorkflow starts a fresh page-cache run, terminating any run left
// over from a previous deploy. The run ID is logged because it is the only way to
// tell a fresh run from an adopted one.
func startPageCacheWorkflow(ctx context.Context, tc temporalclient.Client, log *slog.Logger, params PageCacheParams) (temporalclient.WorkflowRun, error) {
	run, err := tc.ExecuteWorkflow(ctx, pageCacheStartOptions(), PageCacheWorkflow, 0, params)
	if err != nil {
		return nil, fmt.Errorf("page-cache: failed to start workflow: %w", err)
	}
	log.Info("page-cache: workflow started", "id", WorkflowID, "run_id", run.GetRunID())
	return run, nil
}

// newActivities builds the activity struct with the escalation policy the page
// cache depends on. Extracted from Start so tests exercise the same construction
// the worker uses rather than a hand-rolled literal that can drift from it.
//
// degradedEsc's thresholds are set here, before the first Fail, because
// logger.Escalator reads them without its mutex.
func newActivities(log *slog.Logger, api *handlers.API, concurrency int) *Activities {
	return &Activities{
		Log:                log.With("component", "page-cache"),
		API:                api,
		RefreshConcurrency: concurrency,
		degradedEsc: logger.Escalator{
			ErrorAfter:          nhDegradedErrorAfter,
			TransientErrorAfter: nhDegradedErrorAfter,
			ErrorAfterDuration:  nhDegradedErrorWindow,
		},
	}
}

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {} // suppress to avoid noisy workflow logs
func (l *temporalLogger) Info(msg string, keyvals ...any) {
	// Temporal logs these at INFO during normal lifecycle events
	// (StartToCloseTimeout expiry; activity reporting back to a workflow
	// that already completed/continued-as-new during a deploy). The Error=
	// keyval trips cloud-log heuristics that promote the line to ERROR
	// severity. Demote to Debug so it stays suppressed in prod but is
	// recoverable by flipping verbose logging on during an incident.
	if msg == "Task processing failed with client side error" {
		l.log.Debug(msg, keyvals...)
		return
	}
	if msg == "Task processing failed with error" && hasBenignTaskProcessingError(keyvals) {
		l.log.Debug(msg, keyvals...)
		return
	}
	l.log.Info(msg, keyvals...)
}
func (l *temporalLogger) Warn(msg string, keyvals ...any)  { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) { l.log.Error(msg, keyvals...) }

// hasBenignTaskProcessingError reports whether Temporal's "Task processing
// failed with error" keyvals carry an error that's expected during deploys
// (activity completes after the workflow has already finished or been
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

func isWorkflowTerminated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "terminated")
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// loadRefreshConfig reads the page-cache load-shaping knobs from the environment,
// clamping to sane bounds and warning on anything set-but-invalid/out-of-range so
// a typo (e.g. PAGE_CACHE_REFRESH_INTERVAL=90 with no unit) is visible rather than
// silently falling back to prod defaults. Returns the workflow params (interval,
// activity timeout) and the activity-side concurrency. ContinueAsNewThreshold is
// left zero and derived by PageCacheParams.withDefaults (the single source, so
// the derivation isn't duplicated). Pure and side-effect-free (aside from logging)
// so it's unit-testable without a Temporal connection.
func loadRefreshConfig(log *slog.Logger) (PageCacheParams, int) {
	interval := durationEnv(log, "PAGE_CACHE_REFRESH_INTERVAL", defaultRefreshInterval, minRefreshInterval, maxRefreshInterval)
	concurrency := intEnv(log, "PAGE_CACHE_REFRESH_CONCURRENCY", defaultRefreshConcurrency, minRefreshConcurrency, maxRefreshConcurrency)
	timeout := durationEnv(log, "PAGE_CACHE_REFRESH_TIMEOUT", defaultActivityTimeout, minActivityTimeout, maxActivityTimeout)
	return PageCacheParams{
		RefreshInterval: interval,
		ActivityTimeout: timeout,
	}, concurrency
}

// durationEnv parses key as a Go duration (e.g. "90s"), clamped to [min,max].
// Unset → def; set-but-unparseable → def with a warn; out-of-range → clamped with
// a warn (so an operator sees their value was adjusted).
func durationEnv(log *slog.Logger, key string, def, min, max time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Warn("ignoring invalid duration env var; using default", "var", key, "value", raw, "default", def)
		return def
	}
	if d < min {
		log.Warn("duration env var below minimum; clamping", "var", key, "value", d.String(), "min", min.String())
		return min
	}
	if d > max {
		log.Warn("duration env var above maximum; clamping", "var", key, "value", d.String(), "max", max.String())
		return max
	}
	return d
}

// intEnv parses key as an int, clamped to [min,max]. Unset → def;
// set-but-unparseable → def with a warn; out-of-range → clamped with a warn.
func intEnv(log *slog.Logger, key string, def, min, max int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		log.Warn("ignoring invalid int env var; using default", "var", key, "value", raw, "default", def)
		return def
	}
	if n < min {
		log.Warn("int env var below minimum; clamping", "var", key, "value", n, "min", min)
		return min
	}
	if n > max {
		log.Warn("int env var above maximum; clamping", "var", key, "value", n, "max", max)
		return max
	}
	return n
}
