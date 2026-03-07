package admin

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
)

// BackfillSparseCountersConfig holds configuration for the backfill-sparse-counters command.
type BackfillSparseCountersConfig struct {
	StartTime     time.Time
	EndTime       time.Time
	ChunkInterval time.Duration
	DryRun        bool
	Yes           bool // Skip confirmation prompt
}

const defaultSparseBackfillChunkInterval = 24 * time.Hour

// BackfillSparseCounters forward-fills NULL sparse counter values (errors/discards)
// from the last known non-null value, and recomputes their deltas. This fixes data
// where the ingestion bug caused sparse counter baselines to be lost when
// already-written rows were skipped during the overlap window.
//
// Only sparse counter columns and their deltas are modified; all other columns
// (non-sparse counters, non-sparse deltas, delta_duration) are preserved as-is.
// Corrected rows are re-inserted with a newer ingested_at so ReplacingMergeTree
// supersedes the old rows.
func BackfillSparseCounters(
	log *slog.Logger,
	clickhouseAddr, clickhouseDatabase, clickhouseUsername, clickhousePassword string,
	clickhouseSecure bool,
	cfg BackfillSparseCountersConfig,
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

	bounds, err := store.GetDataBoundaries(ctx)
	if err != nil {
		return fmt.Errorf("failed to query data boundaries: %w", err)
	}

	startTime := cfg.StartTime
	endTime := cfg.EndTime

	chunkInterval := cfg.ChunkInterval
	if chunkInterval <= 0 {
		chunkInterval = defaultSparseBackfillChunkInterval
	}

	fmt.Printf("Backfill Sparse Counters (forward-fill NULLs)\n")
	fmt.Printf("  Time range:      %s - %s\n", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	fmt.Printf("  Duration:        %s\n", endTime.Sub(startTime))
	fmt.Printf("  Chunk interval:  %s\n", chunkInterval)
	fmt.Printf("  Dry run:         %v\n", cfg.DryRun)
	fmt.Println()

	fmt.Printf("Existing Data in ClickHouse:\n")
	if bounds != nil && bounds.RowCount > 0 {
		fmt.Printf("  Row count:       %d\n", bounds.RowCount)
		if bounds.MinTime != nil {
			fmt.Printf("  Time range:      %s - %s\n", bounds.MinTime.Format(time.RFC3339), bounds.MaxTime.Format(time.RFC3339))
		}
	} else {
		fmt.Printf("  (no existing data)\n")
	}
	fmt.Println()

	if cfg.DryRun {
		fmt.Println("[DRY RUN] Scanning to show what would be corrected...")
		fmt.Println()
	} else if !cfg.Yes {
		fmt.Printf("This will re-insert corrected rows into ClickHouse.\n")
		fmt.Printf("Type 'yes' to confirm: ")

		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read confirmation: %w", err)
		}
		if strings.TrimSpace(strings.ToLower(response)) != "yes" {
			fmt.Println("Operation cancelled.")
			return nil
		}
		fmt.Println()
	}

	// We need a baseline chunk before startTime to establish last-known values
	// for sparse counters that may be NULL in the first real chunk.
	baselineStart := startTime.Add(-chunkInterval)

	type intfKey struct {
		devicePK string
		intf     string
	}
	// Track last known absolute value per (device_pk, intf) per sparse field
	prevAbsolute := make(map[intfKey]map[string]int64)

	var totalRowsScanned, totalRowsCorrected int64

	chunkStart := baselineStart
	isBaselineChunk := true
	for chunkStart.Before(endTime) {
		chunkEnd := chunkStart.Add(chunkInterval)
		if chunkEnd.After(endTime) && !isBaselineChunk {
			chunkEnd = endTime
		}

		fmt.Printf("Processing %s - %s", chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))
		if isBaselineChunk {
			fmt.Printf(" (baseline)")
		}
		fmt.Println("...")

		rows, err := queryChunk(ctx, chDB, chunkStart, chunkEnd)
		if err != nil {
			return fmt.Errorf("failed to query chunk %s - %s: %w", chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), err)
		}

		var corrections []dztelemusage.InterfaceUsage
		for _, row := range rows {
			key := intfKey{devicePK: row.devicePK, intf: row.intf}

			prev, hasPrev := prevAbsolute[key]
			if !hasPrev {
				prev = make(map[string]int64)
				prevAbsolute[key] = prev
			}

			needsCorrection := false
			correctedAbsolute := make(map[string]*int64, len(sparseFields))
			correctedDeltas := make(map[string]*int64, len(sparseFields))

			for _, sf := range sparseFields {
				absVal := row.absolute[sf.absCol]
				existingDelta := row.deltas[sf.deltaCol]

				if absVal != nil {
					// Non-null value from ClickHouse — keep it, compute delta
					correctedAbsolute[sf.absCol] = absVal
					if prevVal, ok := prev[sf.absCol]; ok {
						delta := max(*absVal-prevVal, 0)
						correctedDeltas[sf.deltaCol] = &delta
						if existingDelta == nil || *existingDelta != delta {
							needsCorrection = true
						}
					} else {
						correctedDeltas[sf.deltaCol] = nil
						if existingDelta != nil {
							needsCorrection = true
						}
					}
					prev[sf.absCol] = *absVal
				} else if prevVal, ok := prev[sf.absCol]; ok {
					// NULL in ClickHouse but we have a previous value — forward-fill
					filled := prevVal
					correctedAbsolute[sf.absCol] = &filled
					zero := int64(0)
					correctedDeltas[sf.deltaCol] = &zero
					// absVal was nil, so the existing row has NULL — always needs correction
					needsCorrection = true
				} else {
					// NULL and no previous value — leave as NULL
					correctedAbsolute[sf.absCol] = nil
					correctedDeltas[sf.deltaCol] = nil
					if existingDelta != nil {
						needsCorrection = true
					}
				}
			}

			if !isBaselineChunk && needsCorrection {
				u := row.toInterfaceUsage()
				// Apply corrected sparse absolute values
				u.InErrors = correctedAbsolute["in_errors"]
				u.OutErrors = correctedAbsolute["out_errors"]
				u.InDiscards = correctedAbsolute["in_discards"]
				u.OutDiscards = correctedAbsolute["out_discards"]
				u.InFCSErrors = correctedAbsolute["in_fcs_errors"]
				// Apply corrected sparse deltas
				u.InErrorsDelta = correctedDeltas["in_errors_delta"]
				u.OutErrorsDelta = correctedDeltas["out_errors_delta"]
				u.InDiscardsDelta = correctedDeltas["in_discards_delta"]
				u.OutDiscardsDelta = correctedDeltas["out_discards_delta"]
				u.InFCSErrorsDelta = correctedDeltas["in_fcs_errors_delta"]
				corrections = append(corrections, u)
			}
		}

		if !isBaselineChunk {
			totalRowsScanned += int64(len(rows))
			totalRowsCorrected += int64(len(corrections))

			if len(corrections) > 0 {
				fmt.Printf("  Scanned %d rows, %d need correction\n", len(rows), len(corrections))
				if !cfg.DryRun {
					if err := store.InsertInterfaceUsage(ctx, corrections); err != nil {
						return fmt.Errorf("failed to insert corrections for chunk %s - %s: %w", chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), err)
					}
				}
			} else {
				fmt.Printf("  Scanned %d rows, all sparse counters correct\n", len(rows))
			}
		} else {
			fmt.Printf("  Read %d baseline rows\n", len(rows))
		}

		if isBaselineChunk {
			isBaselineChunk = false
			chunkStart = startTime
		} else {
			chunkStart = chunkEnd
		}
	}

	fmt.Printf("\nBackfill completed: scanned %d rows, corrected %d rows\n", totalRowsScanned, totalRowsCorrected)
	if cfg.DryRun {
		fmt.Println("[DRY RUN] No rows were actually written")
	}
	return nil
}
