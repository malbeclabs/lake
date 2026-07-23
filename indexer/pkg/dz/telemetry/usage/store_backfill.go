package dztelemusage

import (
	"context"
	"fmt"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/metrics"
)

// BackfillResult contains the results of a backfill operation
type BackfillResult struct {
	StartTime    time.Time
	EndTime      time.Time
	RowsQueried  int
	RowsInserted int
}

// BackfillForTimeRange fetches interface usage data from InfluxDB for a time range and inserts into ClickHouse.
// It relies on ReplacingMergeTree for deduplication, making it safe to re-run.
func (v *View) BackfillForTimeRange(ctx context.Context, startTime, endTime time.Time) (*BackfillResult, error) {
	if startTime.After(endTime) {
		return nil, fmt.Errorf("start time (%s) must be before end time (%s)", startTime, endTime)
	}

	// Serialize with the refresh loop: both read and write the shared baseline
	// cache/watermark. In production backfill runs in a standalone admin process
	// that never starts the refresh loop, but taking the lock keeps the cache
	// invariant enforced in code rather than by that call-site convention. Note
	// the lock spans the InfluxDB query and ClickHouse insert: if this is ever
	// called from a long-lived service sharing a View with the refresh loop, a
	// multi-day backfill would block every live refresh for its duration.
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()

	// Query baseline counters from ClickHouse (or InfluxDB if not available).
	// chMaxTime is nil: the guard's forward gate compares ClickHouse's global max
	// event_ts against the watermark, but during a historical backfill that max
	// is the live indexer's realtime data, far past every chunk — gating on it
	// would defeat the chunk-to-chunk carry entirely. Skipping the gate is safe
	// here because each backfill process starts with a nil cache and processes
	// one contiguous ascending range, so a cache hit only ever lands exactly at
	// the previous chunk's end.
	baselines, err := v.queryBaselineCountersFromClickHouse(ctx, startTime, nil)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to query baseline counters from clickhouse for backfill", "error", err)
		// Fall back to empty baselines - sparse counters may have incorrect deltas for first measurement
		baselines = &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}
	}

	// Build link lookup for enrichment
	linkLookup, err := v.buildLinkLookup(ctx)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to build link lookup for backfill, proceeding without", "error", err)
		linkLookup = make(map[string]LinkInfo)
	}

	// Query InfluxDB for the time range
	startTimeUTC := startTime.UTC()
	endTimeUTC := endTime.UTC()

	v.log.Info("telemetry/usage: querying influxdb for backfill", "from", startTimeUTC, "to", endTimeUTC)

	queryStart := time.Now()
	rows, err := queryIntfCountersChunked(ctx, v.cfg.InfluxDB, startTimeUTC, endTimeUTC, v.cfg.QueryChunk)
	queryDuration := time.Since(queryStart)
	metrics.RecordInfluxQuery(v.cfg.DZEnv, "backfill", queryDuration, len(rows), err)
	if err != nil {
		return nil, fmt.Errorf("failed to query influxdb for backfill: %w", err)
	}

	v.log.Info("telemetry/usage: backfill queried influxdb", "rows", len(rows), "duration", queryDuration.String())

	if len(rows) == 0 {
		// A chunk with zero InfluxDB rows (old/gappy historical data) still must
		// advance the watermark, or it goes stale mid-backfill and stops tracking
		// the data end the cached baselines represent. Nothing changed through
		// endTime, so the start-of-window baselines are also the end-of-window
		// state: merge them (a no-op when they came from the cache) and advance.
		v.updateBaselineCache(baselines, endTime)
		return &BackfillResult{
			StartTime:    startTime,
			EndTime:      endTime,
			RowsQueried:  0,
			RowsInserted: 0,
		}, nil
	}

	// Convert rows to InterfaceUsage
	// Pass nil for alreadyWritten - backfill relies on ReplacingMergeTree for deduplication
	usage, endBaselines, err := v.convertRowsToUsage(rows, baselines, linkLookup, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows for backfill: %w", err)
	}

	// carryBaselines carries this chunk's end-of-window baselines forward so the
	// next contiguous backfill chunk reuses them instead of re-running the
	// expensive historical ClickHouse scan (which timed out repeatedly during the
	// June backfill). The admin backfill loop processes ascending contiguous
	// chunks in one process, so the next chunk's startTime == this chunk's endTime
	// hits the watermark cache in queryBaselineCountersFromClickHouse. A
	// non-contiguous backward call simply misses the cache and re-queries — a safe
	// fallback (see the guard's doc for the forward-jump caveat). Done only after
	// the rows are durably in ClickHouse (mirrors Refresh), so a failed insert
	// can't leave the cache holding unpersisted values.
	carryBaselines := func() { v.updateBaselineCache(endBaselines, endTime) }

	if len(usage) == 0 {
		carryBaselines()
		return &BackfillResult{
			StartTime:    startTime,
			EndTime:      endTime,
			RowsQueried:  len(rows),
			RowsInserted: 0,
		}, nil
	}

	// Insert into ClickHouse
	if err := v.store.InsertInterfaceUsage(ctx, usage); err != nil {
		return nil, fmt.Errorf("failed to insert backfill data: %w", err)
	}
	carryBaselines()

	return &BackfillResult{
		StartTime:    startTime,
		EndTime:      endTime,
		RowsQueried:  len(rows),
		RowsInserted: len(usage),
	}, nil
}
