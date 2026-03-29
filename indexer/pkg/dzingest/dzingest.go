// Package dzingest runs a Temporal workflow that periodically refreshes
// DoubleZero data: serviceability state, telemetry latency, telemetry
// usage, Neo4j graph sync, and IS-IS topology sync. One workflow instance
// runs per network (mainnet-beta, testnet, devnet).
package dzingest

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	dzgraph "github.com/malbeclabs/lake/indexer/pkg/dz/graph"
	"github.com/malbeclabs/lake/indexer/pkg/dz/isis"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	dztelemlatency "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/latency"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
)

// Config configures the DZ ingest worker.
type Config struct {
	Log *slog.Logger

	// Network identifies the DZ environment (e.g. "mainnet-beta", "testnet", "devnet").
	// Used to namespace the Temporal task queue and workflow ID.
	Network string

	// Views and stores for activity execution.
	Serviceability *dzsvc.View
	TelemLatency   *dztelemlatency.View
	TelemUsage     *dztelemusage.View // optional
	GraphStore     *dzgraph.Store     // optional
	ISISSource     isis.Source        // optional
	ISISStore      *isis.Store        // optional
}

func taskQueue(network string) string  { return "indexer-dz-ingest-" + network }
func workflowID(network string) string { return "indexer-dz-ingest-" + network }

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
		Log:            log.With("component", "dz-ingest"),
		Serviceability: cfg.Serviceability,
		TelemLatency:   cfg.TelemLatency,
		TelemUsage:     cfg.TelemUsage,
		GraphStore:     cfg.GraphStore,
		ISISSource:     cfg.ISISSource,
		ISISStore:      cfg.ISISStore,
	}

	w := worker.New(tc, tq, worker.Options{})
	RegisterWorkflows(w)
	w.RegisterActivity(activities)

	// Terminate any existing workflow from a previous deploy, then start fresh.
	_ = tc.TerminateWorkflow(ctx, wfID, "", "restarting on deploy")
	run, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: tq,
	}, DZIngestWorkflow, 0)
	if err != nil {
		return fmt.Errorf("dzingest: failed to start workflow: %w", err)
	}
	log.Info("dzingest: workflow started", "id", wfID)

	go func() {
		if err := run.Get(ctx, nil); err != nil && ctx.Err() == nil {
			log.Error("dzingest: workflow failed", "id", wfID, "error", err)
		}
	}()

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

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {} // suppress noisy debug logs
func (l *temporalLogger) Info(msg string, keyvals ...any)  { l.log.Info(msg, keyvals...) }
func (l *temporalLogger) Warn(msg string, keyvals ...any)  { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) { l.log.Error(msg, keyvals...) }

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
