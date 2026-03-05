package admin

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	dztelemusage "github.com/malbeclabs/lake/indexer/pkg/dz/telemetry/usage"
)

// RecomputeSparseCounterDeltasConfig holds the configuration for the recompute command
type RecomputeSparseCounterDeltasConfig struct {
	StartTime     time.Time
	EndTime       time.Time
	ChunkInterval time.Duration
	DryRun        bool
}

const defaultRecomputeChunkInterval = 24 * time.Hour

// sparseField maps an absolute counter column to its delta column.
type sparseField struct {
	absCol   string
	deltaCol string
}

var sparseFields = []sparseField{
	{"in_errors", "in_errors_delta"},
	{"out_errors", "out_errors_delta"},
	{"in_discards", "in_discards_delta"},
	{"out_discards", "out_discards_delta"},
	{"in_fcs_errors", "in_fcs_errors_delta"},
}

// RecomputeSparseCounterDeltas reads existing rows from ClickHouse, recomputes sparse counter
// deltas from absolute values, and re-inserts corrected rows with a newer ingested_at so
// ReplacingMergeTree supersedes the old rows.
func RecomputeSparseCounterDeltas(
	log *slog.Logger,
	clickhouseAddr, clickhouseDatabase, clickhouseUsername, clickhousePassword string,
	clickhouseSecure bool,
	cfg RecomputeSparseCounterDeltasConfig,
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
		chunkInterval = defaultRecomputeChunkInterval
	}

	fmt.Printf("Recompute Sparse Counter Deltas\n")
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

	// We need to query one chunk before the start time to establish baselines for
	// the first real chunk. This ensures the first row in startTime's chunk has a
	// "previous" value to compute its delta from.
	baselineStart := startTime.Add(-chunkInterval)

	var totalRowsScanned, totalRowsCorrected int64

	// prevAbsolute tracks the last absolute value per (device_pk, intf) per sparse field
	// across chunks so deltas are correct at chunk boundaries.
	type intfKey struct {
		devicePK string
		intf     string
	}
	prevAbsolute := make(map[intfKey]map[string]int64)
	prevTime := make(map[intfKey]time.Time)

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

			// Compute corrected deltas and delta_duration
			needsCorrection := false
			correctedDeltas := make(map[string]*int64, len(sparseFields))
			for _, sf := range sparseFields {
				absVal := row.absolute[sf.absCol]
				existingDelta := row.deltas[sf.deltaCol]

				if absVal == nil {
					// No absolute value, delta should be nil
					correctedDeltas[sf.deltaCol] = nil
					if existingDelta != nil {
						needsCorrection = true
					}
					continue
				}

				if !hasPrev {
					// First row for this interface, no previous to compare
					correctedDeltas[sf.deltaCol] = nil
					if existingDelta != nil {
						needsCorrection = true
					}
				} else if prevVal, ok := prev[sf.absCol]; ok {
					delta := max(*absVal-prevVal, 0) // clamp to 0 on counter reset
					correctedDeltas[sf.deltaCol] = &delta
					if existingDelta == nil || *existingDelta != delta {
						needsCorrection = true
					}
				} else {
					// Had prev for other fields but not this one
					correctedDeltas[sf.deltaCol] = nil
					if existingDelta != nil {
						needsCorrection = true
					}
				}

				// Update previous
				prev[sf.absCol] = *absVal
			}

			// Compute corrected delta_duration
			var correctedDeltaDuration *float64
			if pt, ok := prevTime[key]; ok {
				dur := row.eventTS.Sub(pt).Seconds()
				correctedDeltaDuration = &dur
				if row.deltaDuration == nil || *row.deltaDuration != dur {
					needsCorrection = true
				}
			} else {
				if row.deltaDuration != nil {
					needsCorrection = true
				}
			}
			prevTime[key] = row.eventTS

			if !isBaselineChunk && needsCorrection {
				u := row.toInterfaceUsage()
				// Apply corrected sparse deltas
				u.InErrorsDelta = correctedDeltas["in_errors_delta"]
				u.OutErrorsDelta = correctedDeltas["out_errors_delta"]
				u.InDiscardsDelta = correctedDeltas["in_discards_delta"]
				u.OutDiscardsDelta = correctedDeltas["out_discards_delta"]
				u.InFCSErrorsDelta = correctedDeltas["in_fcs_errors_delta"]
				u.DeltaDuration = correctedDeltaDuration
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
				fmt.Printf("  Scanned %d rows, all deltas correct\n", len(rows))
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

	fmt.Printf("\nRecompute completed: scanned %d rows, corrected %d rows\n", totalRowsScanned, totalRowsCorrected)
	if cfg.DryRun {
		fmt.Println("[DRY RUN] No rows were actually written")
	}
	return nil
}

// counterRow holds a parsed row from ClickHouse with all fields needed for recomputation.
type counterRow struct {
	eventTS  time.Time
	devicePK string
	intf     string

	// All fields needed to reconstruct InterfaceUsage
	host               *string
	userTunnelID       *int64
	linkPK             *string
	linkSide           *string
	modelName          *string
	serialNumber       *string
	carrierTransitions *int64
	inBroadcastPkts    *int64
	inMulticastPkts    *int64
	inOctets           *int64
	inPkts             *int64
	inUnicastPkts      *int64
	outBroadcastPkts   *int64
	outMulticastPkts   *int64
	outOctets          *int64
	outPkts            *int64
	outUnicastPkts     *int64

	// Non-sparse deltas (preserved as-is)
	carrierTransitionsDelta *int64
	inBroadcastPktsDelta    *int64
	inMulticastPktsDelta    *int64
	inOctetsDelta           *int64
	inPktsDelta             *int64
	inUnicastPktsDelta      *int64
	outBroadcastPktsDelta   *int64
	outMulticastPktsDelta   *int64
	outOctetsDelta          *int64
	outPktsDelta            *int64
	outUnicastPktsDelta     *int64

	// Sparse counter absolute values
	absolute map[string]*int64 // absCol -> value
	// Sparse counter deltas (existing, possibly incorrect)
	deltas map[string]*int64 // deltaCol -> value

	deltaDuration *float64
}

func (r *counterRow) toInterfaceUsage() dztelemusage.InterfaceUsage {
	dpk := r.devicePK
	intf := r.intf
	return dztelemusage.InterfaceUsage{
		Time:               r.eventTS,
		DevicePK:           &dpk,
		Host:               r.host,
		Intf:               &intf,
		UserTunnelID:       r.userTunnelID,
		LinkPK:             r.linkPK,
		LinkSide:           r.linkSide,
		ModelName:          r.modelName,
		SerialNumber:       r.serialNumber,
		CarrierTransitions: r.carrierTransitions,
		InBroadcastPkts:    r.inBroadcastPkts,
		InDiscards:         r.absolute["in_discards"],
		InErrors:           r.absolute["in_errors"],
		InFCSErrors:        r.absolute["in_fcs_errors"],
		InMulticastPkts:    r.inMulticastPkts,
		InOctets:           r.inOctets,
		InPkts:             r.inPkts,
		InUnicastPkts:      r.inUnicastPkts,
		OutBroadcastPkts:   r.outBroadcastPkts,
		OutDiscards:        r.absolute["out_discards"],
		OutErrors:          r.absolute["out_errors"],
		OutMulticastPkts:   r.outMulticastPkts,
		OutOctets:          r.outOctets,
		OutPkts:            r.outPkts,
		OutUnicastPkts:     r.outUnicastPkts,
		// Non-sparse deltas preserved
		CarrierTransitionsDelta: r.carrierTransitionsDelta,
		InBroadcastPktsDelta:    r.inBroadcastPktsDelta,
		InMulticastPktsDelta:    r.inMulticastPktsDelta,
		InOctetsDelta:           r.inOctetsDelta,
		InPktsDelta:             r.inPktsDelta,
		InUnicastPktsDelta:      r.inUnicastPktsDelta,
		OutBroadcastPktsDelta:   r.outBroadcastPktsDelta,
		OutMulticastPktsDelta:   r.outMulticastPktsDelta,
		OutOctetsDelta:          r.outOctetsDelta,
		OutPktsDelta:            r.outPktsDelta,
		OutUnicastPktsDelta:     r.outUnicastPktsDelta,
		// Sparse deltas and delta_duration will be set by caller
	}
}

func queryChunk(ctx context.Context, chDB clickhouse.Client, start, end time.Time) ([]counterRow, error) {
	conn, err := chDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	query := `
		SELECT
			event_ts,
			device_pk,
			intf,
			host,
			user_tunnel_id,
			link_pk,
			link_side,
			model_name,
			serial_number,
			carrier_transitions,
			in_broadcast_pkts,
			in_discards,
			in_errors,
			in_fcs_errors,
			in_multicast_pkts,
			in_octets,
			in_pkts,
			in_unicast_pkts,
			out_broadcast_pkts,
			out_discards,
			out_errors,
			out_multicast_pkts,
			out_octets,
			out_pkts,
			out_unicast_pkts,
			carrier_transitions_delta,
			in_broadcast_pkts_delta,
			in_discards_delta,
			in_errors_delta,
			in_fcs_errors_delta,
			in_multicast_pkts_delta,
			in_octets_delta,
			in_pkts_delta,
			in_unicast_pkts_delta,
			out_broadcast_pkts_delta,
			out_discards_delta,
			out_errors_delta,
			out_multicast_pkts_delta,
			out_octets_delta,
			out_pkts_delta,
			out_unicast_pkts_delta,
			delta_duration
		FROM fact_dz_device_interface_counters FINAL
		WHERE event_ts >= ? AND event_ts < ?
		ORDER BY device_pk, intf, event_ts
	`

	rows, err := conn.Query(ctx, query, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	var result []counterRow
	for rows.Next() {
		var r counterRow
		r.absolute = make(map[string]*int64)
		r.deltas = make(map[string]*int64)

		var (
			inDiscards       *int64
			inErrors         *int64
			inFCSErrors      *int64
			outDiscards      *int64
			outErrors        *int64
			inDiscardsDelta  *int64
			inErrorsDelta    *int64
			inFCSErrorsDelta *int64
			outDiscardsDelta *int64
			outErrorsDelta   *int64
		)

		err := rows.Scan(
			&r.eventTS,
			&r.devicePK,
			&r.intf,
			&r.host,
			&r.userTunnelID,
			&r.linkPK,
			&r.linkSide,
			&r.modelName,
			&r.serialNumber,
			&r.carrierTransitions,
			&r.inBroadcastPkts,
			&inDiscards,
			&inErrors,
			&inFCSErrors,
			&r.inMulticastPkts,
			&r.inOctets,
			&r.inPkts,
			&r.inUnicastPkts,
			&r.outBroadcastPkts,
			&outDiscards,
			&outErrors,
			&r.outMulticastPkts,
			&r.outOctets,
			&r.outPkts,
			&r.outUnicastPkts,
			&r.carrierTransitionsDelta,
			&r.inBroadcastPktsDelta,
			&inDiscardsDelta,
			&inErrorsDelta,
			&inFCSErrorsDelta,
			&r.inMulticastPktsDelta,
			&r.inOctetsDelta,
			&r.inPktsDelta,
			&r.inUnicastPktsDelta,
			&r.outBroadcastPktsDelta,
			&outDiscardsDelta,
			&outErrorsDelta,
			&r.outMulticastPktsDelta,
			&r.outOctetsDelta,
			&r.outPktsDelta,
			&r.outUnicastPktsDelta,
			&r.deltaDuration,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

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

		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return result, nil
}
