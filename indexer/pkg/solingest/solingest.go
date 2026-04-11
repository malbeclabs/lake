// Package solingest runs a Temporal workflow that periodically refreshes
// Solana-related data: validator state, block production, GeoIP enrichment,
// and validators.app metadata. One workflow instance runs per network
// (mainnet-beta, testnet, devnet).
package solingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	mcpgeoip "github.com/malbeclabs/lake/indexer/pkg/geoip"
	"github.com/malbeclabs/lake/indexer/pkg/ingestionlog"
	"github.com/malbeclabs/lake/indexer/pkg/sol"
	"github.com/malbeclabs/lake/indexer/pkg/validatorsapp"
)

// Config configures the Solana ingest worker.
type Config struct {
	Log          *slog.Logger
	IngestionLog *ingestionlog.Writer // optional

	// Network identifies the DZ environment (e.g. "mainnet-beta", "testnet", "devnet").
	// Used to namespace the Temporal task queue and workflow ID.
	Network string

	// Views for activity execution.
	Solana        *sol.View
	GeoIP         *mcpgeoip.View      // optional
	ValidatorsApp *validatorsapp.View // optional
}

func taskQueue(network string) string  { return "indexer-sol-ingest-" + network }
func workflowID(network string) string { return "indexer-sol-ingest-" + network }

// Start connects to Temporal and begins processing Solana ingest workflows.
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
		return fmt.Errorf("solingest: temporal dial: %w", err)
	}
	defer tc.Close()
	log.Info("solingest: temporal connected", "host", temporalHost, "namespace", temporalNS)

	tq := taskQueue(cfg.Network)
	wfID := workflowID(cfg.Network)

	activities := &Activities{
		Log:           log.With("component", "sol-ingest"),
		IngestionLog:  cfg.IngestionLog,
		Network:       cfg.Network,
		Solana:        cfg.Solana,
		GeoIP:         cfg.GeoIP,
		ValidatorsApp: cfg.ValidatorsApp,
	}

	w := worker.New(tc, tq, worker.Options{})
	RegisterWorkflows(w)
	w.RegisterActivity(activities)

	// Terminate any existing workflow from a previous deploy, then start fresh.
	_ = tc.TerminateWorkflow(ctx, wfID, "", "restarting on deploy")
	run, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: tq,
	}, SolIngestWorkflow, 0)
	if err != nil {
		return fmt.Errorf("solingest: failed to start workflow: %w", err)
	}
	log.Info("solingest: workflow started", "id", wfID)

	go func() {
		current := run
		for {
			if err := current.Get(ctx, nil); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("solingest: workflow interrupted, reattaching", "id", wfID, "error", err)
				current = tc.GetWorkflow(ctx, wfID, "")
			} else {
				return
			}
		}
	}()

	log.Info("solingest: starting worker", "task_queue", tq)

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
func (l *temporalLogger) Error(msg string, keyvals ...any) {
	if isContextCancellation(keyvals) {
		l.log.Warn(msg, keyvals...)
		return
	}
	l.log.Error(msg, keyvals...)
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
