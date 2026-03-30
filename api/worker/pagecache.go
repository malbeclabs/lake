package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/malbeclabs/lake/api/handlers"
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
	activities := &Activities{
		Log: log.With("component", "page-cache"),
		API: cfg.API,
	}

	w := worker.New(tc, TaskQueue, worker.Options{})
	w.RegisterWorkflow(PageCacheWorkflow)
	w.RegisterActivity(activities)

	// Terminate any existing workflow from a previous deploy, then start fresh.
	_ = tc.TerminateWorkflow(ctx, WorkflowID, "", "restarting on deploy")
	run, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        WorkflowID,
		TaskQueue: TaskQueue,
	}, PageCacheWorkflow, 0)
	if err != nil {
		return fmt.Errorf("page-cache: failed to start workflow: %w", err)
	}
	log.Info("page-cache: workflow started", "id", WorkflowID)

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

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {} // suppress to avoid noisy workflow logs
func (l *temporalLogger) Info(msg string, keyvals ...any)  { l.log.Info(msg, keyvals...) }
func (l *temporalLogger) Warn(msg string, keyvals ...any)  { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) { l.log.Error(msg, keyvals...) }

func isWorkflowTerminated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "terminated")
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
