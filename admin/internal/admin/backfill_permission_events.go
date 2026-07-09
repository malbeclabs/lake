package admin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/dzingest"
	"go.temporal.io/sdk/client"
)

// BackfillPermissionEventsConfig holds configuration for the backfill-permission-events command.
type BackfillPermissionEventsConfig struct {
	Network  string // DZ environment (e.g. "mainnet-beta")
	Truncate bool   // If true, truncate the fact table + scan cursor before backfilling.
}

// BackfillPermissionEvents starts the BackfillPermissionEventsWorkflow on Temporal.
func BackfillPermissionEvents(log *slog.Logger, cfg BackfillPermissionEventsConfig) error {
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

	input := dzingest.BackfillPermissionEventsInput{
		Truncate: cfg.Truncate,
	}

	workflowID := fmt.Sprintf("backfill-permission-events-%d", time.Now().Unix())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: dzingest.TaskQueue(cfg.Network),
	}, dzingest.BackfillPermissionEventsWorkflow, input)
	if err != nil {
		return fmt.Errorf("start backfill workflow: %w", err)
	}

	log.Info("started permission events backfill workflow",
		"workflow_id", run.GetID(),
		"run_id", run.GetRunID(),
		"network", cfg.Network,
		"truncate", cfg.Truncate,
	)

	log.Info("waiting for completion...")
	if err := run.Get(context.Background(), nil); err != nil {
		return fmt.Errorf("backfill workflow failed: %w", err)
	}

	log.Info("permission events backfill complete")
	return nil
}
