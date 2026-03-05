package admin

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
)

// BackfillDeviceInterfaceCountersFromCSV imports a CSV file exported by --export-influx-csv into ClickHouse.
func BackfillDeviceInterfaceCountersFromCSV(
	log *slog.Logger,
	clickhouseAddr, clickhouseDatabase, clickhouseUsername, clickhousePassword string,
	clickhouseSecure bool,
	csvPath string,
) error {
	ctx := context.Background()

	chDB, err := clickhouse.NewClient(ctx, log, clickhouseAddr, clickhouseDatabase, clickhouseUsername, clickhousePassword, clickhouseSecure)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer chDB.Close()

	store, err := dztelemusage.NewStore(dztelemusage.StoreConfig{
		Logger:     log,
		ClickHouse: chDB,
	})
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %w", csvPath, err)
	}
	defer f.Close()

	result, err := store.BackfillFromReader(ctx, f)
	if err != nil {
		return fmt.Errorf("backfill failed: %w", err)
	}

	log.Info("backfill complete",
		"queried", result.RowsQueried,
		"inserted", result.RowsInserted,
		"start", result.StartTime,
		"end", result.EndTime,
	)
	return nil
}
