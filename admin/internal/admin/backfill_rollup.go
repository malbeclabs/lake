package admin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/rollup"
	"go.temporal.io/sdk/client"
)

// BackfillRollupConfig holds configuration for the backfill-rollup command.
type BackfillRollupConfig struct {
	StartTime      time.Time
	EndTime        time.Time
	ChunkInterval  time.Duration
	SourceDatabase string // if set, read source data from this database (e.g. remote proxy tables)
}

// BackfillRollup starts the BackfillRollupWorkflow on Temporal.
func BackfillRollup(log *slog.Logger, cfg BackfillRollupConfig) error {
	if cfg.ChunkInterval == 0 {
		cfg.ChunkInterval = 1 * time.Hour
	}

	temporalAddr := os.Getenv("TEMPORAL_HOST_PORT")
	if temporalAddr == "" {
		temporalAddr = "localhost:7233"
	}
	temporalNamespace := os.Getenv("TEMPORAL_NAMESPACE")
	if temporalNamespace == "" {
		temporalNamespace = "default"
	}

	c, err := client.Dial(client.Options{
		HostPort:  temporalAddr,
		Namespace: temporalNamespace,
		Logger:    log,
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	input := rollup.BackfillInput{
		StartTime:      cfg.StartTime,
		EndTime:        cfg.EndTime,
		ChunkSize:      cfg.ChunkInterval,
		SourceDatabase: cfg.SourceDatabase,
	}

	workflowID := fmt.Sprintf("rollup-backfill-%d", time.Now().Unix())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: rollup.TaskQueue,
	}, rollup.BackfillRollupWorkflow, input)
	if err != nil {
		return fmt.Errorf("start backfill workflow: %w", err)
	}

	logAttrs := []any{
		"workflow_id", run.GetID(),
		"run_id", run.GetRunID(),
		"start", cfg.StartTime.Format(time.RFC3339),
		"end", cfg.EndTime.Format(time.RFC3339),
		"chunk", cfg.ChunkInterval,
	}
	if cfg.SourceDatabase != "" {
		logAttrs = append(logAttrs, "source_database", cfg.SourceDatabase)
	}
	log.Info("started rollup backfill workflow", logAttrs...)

	log.Info("waiting for completion...")
	if err := run.Get(context.Background(), nil); err != nil {
		return fmt.Errorf("backfill workflow failed: %w", err)
	}

	log.Info("rollup backfill complete")
	return nil
}
