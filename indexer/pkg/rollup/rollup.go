// Package rollup runs Temporal workflows that roll up raw telemetry into
// pre-aggregated bucket tables in ClickHouse. It is designed to be embedded
// in the indexer process (started by default) and can host multiple workflow
// types as they are added over time.
package rollup

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Config configures the rollup worker.
type Config struct {
	Log *slog.Logger

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

	// Register rollup workflows
	activities := &Activities{
		ClickHouse: chConn,
		Log:        log.With("component", "rollup"),
	}

	w := worker.New(tc, TaskQueue, worker.Options{})
	RegisterWorkflows(w)
	w.RegisterActivity(activities)

	// Terminate any existing rollup workflow from a previous deploy, then start
	// fresh. This ensures the new worker code is used immediately rather than
	// replaying old history which could cause non-determinism errors.
	_ = tc.TerminateWorkflow(ctx, WorkflowID, "", "restarting on deploy")
	run, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        WorkflowID,
		TaskQueue: TaskQueue,
	}, ComputeRollupWorkflow, 0)
	if err != nil {
		return fmt.Errorf("rollup: failed to start workflow: %w", err)
	}
	log.Info("rollup: workflow started", "id", WorkflowID)

	// Watch the workflow in the background so failures surface in logs.
	go func() {
		if err := run.Get(ctx, nil); err != nil && ctx.Err() == nil {
			log.Error("rollup: workflow failed", "id", WorkflowID, "error", err)
		}
	}()

	log.Info("rollup: starting worker", "task_queue", TaskQueue)

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

func (l *temporalLogger) Debug(msg string, keyvals ...any) {} // no-op to avoid blocking workflow goroutine
func (l *temporalLogger) Info(msg string, keyvals ...any)  { l.log.Info(msg, keyvals...) }
func (l *temporalLogger) Warn(msg string, keyvals ...any)  { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) { l.log.Error(msg, keyvals...) }

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
