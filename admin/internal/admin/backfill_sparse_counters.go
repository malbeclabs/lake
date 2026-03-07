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

const (
	defaultSparseBackfillChunkInterval = 1 * time.Hour
	// Flush corrections to ClickHouse in batches to bound memory usage.
	sparseBackfillFlushSize = 10_000
)

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

	prevAbsolute := make(map[intfKey]map[string]int64)

	var totalRowsScanned, totalRowsCorrected int64

	// Phase 1: Load baselines from before startTime.
	// We stream the baseline chunk to populate prevAbsolute without keeping rows.
	baselineStart := startTime.Add(-chunkInterval)
	fmt.Printf("Processing %s - %s (baseline)...\n", baselineStart.Format(time.RFC3339), startTime.Format(time.RFC3339))

	baselineCount, err := streamSparseChunk(ctx, chDB, baselineStart, startTime, prevAbsolute, nil, nil, true)
	if err != nil {
		return fmt.Errorf("failed to stream baseline chunk: %w", err)
	}
	fmt.Printf("  Streamed %d baseline rows\n", baselineCount)

	// Phase 2: Stream each chunk, accumulate and flush corrections.
	chunkStart := startTime
	for chunkStart.Before(endTime) {
		chunkEnd := chunkStart.Add(chunkInterval)
		if chunkEnd.After(endTime) {
			chunkEnd = endTime
		}

		fmt.Printf("Processing %s - %s...\n", chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))

		var chunkScanned, chunkCorrected int64
		flushFn := func(corrections []dztelemusage.InterfaceUsage) error {
			if !cfg.DryRun {
				if err := store.InsertInterfaceUsage(ctx, corrections); err != nil {
					return fmt.Errorf("failed to insert corrections: %w", err)
				}
			}
			return nil
		}

		chunkScanned, err = streamSparseChunk(ctx, chDB, chunkStart, chunkEnd, prevAbsolute, &chunkCorrected, flushFn, false)
		if err != nil {
			return fmt.Errorf("failed to process chunk %s - %s: %w", chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), err)
		}

		totalRowsScanned += chunkScanned
		totalRowsCorrected += chunkCorrected

		if chunkCorrected > 0 {
			fmt.Printf("  Streamed %d rows, %d corrected\n", chunkScanned, chunkCorrected)
		} else {
			fmt.Printf("  Streamed %d rows, all sparse counters correct\n", chunkScanned)
		}

		chunkStart = chunkEnd
	}

	fmt.Printf("\nBackfill completed: scanned %d rows, corrected %d rows\n", totalRowsScanned, totalRowsCorrected)
	if cfg.DryRun {
		fmt.Println("[DRY RUN] No rows were actually written")
	}
	return nil
}

type intfKey struct {
	devicePK string
	intf     string
}

// streamSparseChunk streams rows from ClickHouse for a time range, tracking previous
// sparse counter values in prevAbsolute. When baselineOnly is false, it accumulates
// corrections and flushes them via flushFn every sparseBackfillFlushSize rows.
// Returns the number of rows streamed.
func streamSparseChunk(
	ctx context.Context,
	chDB clickhouse.Client,
	start, end time.Time,
	prevAbsolute map[intfKey]map[string]int64,
	correctedCount *int64,
	flushFn func([]dztelemusage.InterfaceUsage) error,
	baselineOnly bool,
) (int64, error) {
	conn, err := chDB.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection: %w", err)
	}

	query := `
		SELECT
			event_ts, device_pk, intf, host, user_tunnel_id, link_pk, link_side,
			model_name, serial_number, carrier_transitions, in_broadcast_pkts,
			in_discards, in_errors, in_fcs_errors, in_multicast_pkts, in_octets,
			in_pkts, in_unicast_pkts, out_broadcast_pkts, out_discards, out_errors,
			out_multicast_pkts, out_octets, out_pkts, out_unicast_pkts,
			carrier_transitions_delta, in_broadcast_pkts_delta, in_discards_delta,
			in_errors_delta, in_fcs_errors_delta, in_multicast_pkts_delta,
			in_octets_delta, in_pkts_delta, in_unicast_pkts_delta,
			out_broadcast_pkts_delta, out_discards_delta, out_errors_delta,
			out_multicast_pkts_delta, out_octets_delta, out_pkts_delta,
			out_unicast_pkts_delta, delta_duration
		FROM fact_dz_device_interface_counters FINAL
		WHERE event_ts >= ? AND event_ts < ?
		ORDER BY device_pk, intf, event_ts
	`

	rows, err := conn.Query(ctx, query, start, end)
	if err != nil {
		return 0, fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	var corrections []dztelemusage.InterfaceUsage
	var rowCount int64

	for rows.Next() {
		var r counterRow
		r.absolute = make(map[string]*int64)
		r.deltas = make(map[string]*int64)

		var (
			inDiscards, inErrors, inFCSErrors  *int64
			outDiscards, outErrors             *int64
			inDiscardsDelta, inErrorsDelta     *int64
			inFCSErrorsDelta, outDiscardsDelta *int64
			outErrorsDelta                     *int64
		)

		err := rows.Scan(
			&r.eventTS, &r.devicePK, &r.intf, &r.host, &r.userTunnelID,
			&r.linkPK, &r.linkSide, &r.modelName, &r.serialNumber,
			&r.carrierTransitions, &r.inBroadcastPkts,
			&inDiscards, &inErrors, &inFCSErrors,
			&r.inMulticastPkts, &r.inOctets, &r.inPkts, &r.inUnicastPkts,
			&r.outBroadcastPkts, &outDiscards, &outErrors,
			&r.outMulticastPkts, &r.outOctets, &r.outPkts, &r.outUnicastPkts,
			&r.carrierTransitionsDelta, &r.inBroadcastPktsDelta,
			&inDiscardsDelta, &inErrorsDelta, &inFCSErrorsDelta,
			&r.inMulticastPktsDelta, &r.inOctetsDelta, &r.inPktsDelta,
			&r.inUnicastPktsDelta, &r.outBroadcastPktsDelta,
			&outDiscardsDelta, &outErrorsDelta,
			&r.outMulticastPktsDelta, &r.outOctetsDelta, &r.outPktsDelta,
			&r.outUnicastPktsDelta, &r.deltaDuration,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to scan row: %w", err)
		}
		rowCount++

		r.absolute["in_discards"] = inDiscards
		r.absolute["in_errors"] = inErrors
		r.absolute["in_fcs_errors"] = inFCSErrors
		r.absolute["out_discards"] = outDiscards
		r.absolute["out_errors"] = outErrors
		r.deltas["in_discards_delta"] = inDiscardsDelta
		r.deltas["in_errors_delta"] = inErrorsDelta
		r.deltas["in_fcs_errors_delta"] = inFCSErrorsDelta
		r.deltas["out_discards_delta"] = outDiscardsDelta
		r.deltas["out_errors_delta"] = outErrorsDelta

		key := intfKey{devicePK: r.devicePK, intf: r.intf}
		prev, hasPrev := prevAbsolute[key]
		if !hasPrev {
			prev = make(map[string]int64)
			prevAbsolute[key] = prev
		}

		if baselineOnly {
			// Only update prevAbsolute, don't compute corrections
			for _, sf := range sparseFields {
				if v := r.absolute[sf.absCol]; v != nil {
					prev[sf.absCol] = *v
				}
			}
			continue
		}

		// Compute corrections
		needsCorrection := false
		correctedAbsolute := make(map[string]*int64, len(sparseFields))
		correctedDeltas := make(map[string]*int64, len(sparseFields))

		for _, sf := range sparseFields {
			absVal := r.absolute[sf.absCol]
			existingDelta := r.deltas[sf.deltaCol]

			if absVal != nil {
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
				filled := prevVal
				correctedAbsolute[sf.absCol] = &filled
				zero := int64(0)
				correctedDeltas[sf.deltaCol] = &zero
				needsCorrection = true
			} else {
				correctedAbsolute[sf.absCol] = nil
				correctedDeltas[sf.deltaCol] = nil
				if existingDelta != nil {
					needsCorrection = true
				}
			}
		}

		if needsCorrection {
			u := r.toInterfaceUsage()
			u.InErrors = correctedAbsolute["in_errors"]
			u.OutErrors = correctedAbsolute["out_errors"]
			u.InDiscards = correctedAbsolute["in_discards"]
			u.OutDiscards = correctedAbsolute["out_discards"]
			u.InFCSErrors = correctedAbsolute["in_fcs_errors"]
			u.InErrorsDelta = correctedDeltas["in_errors_delta"]
			u.OutErrorsDelta = correctedDeltas["out_errors_delta"]
			u.InDiscardsDelta = correctedDeltas["in_discards_delta"]
			u.OutDiscardsDelta = correctedDeltas["out_discards_delta"]
			u.InFCSErrorsDelta = correctedDeltas["in_fcs_errors_delta"]
			corrections = append(corrections, u)
			*correctedCount++

			// Flush batch to bound memory
			if len(corrections) >= sparseBackfillFlushSize && flushFn != nil {
				if err := flushFn(corrections); err != nil {
					return 0, err
				}
				corrections = corrections[:0]
			}
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	// Flush remaining corrections
	if len(corrections) > 0 && flushFn != nil {
		if err := flushFn(corrections); err != nil {
			return 0, err
		}
	}

	return rowCount, nil
}
