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

	// Query baseline counters from ClickHouse (or InfluxDB if not available)
	baselines, err := v.queryBaselineCountersFromClickHouse(ctx, startTime)
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
	rows, err := v.cfg.InfluxDB.QueryIntfCounters(ctx, startTimeUTC, endTimeUTC)
	queryDuration := time.Since(queryStart)
	metrics.RecordInfluxQuery(v.cfg.DZEnv, "backfill", queryDuration, len(rows), err)
	if err != nil {
		return nil, fmt.Errorf("failed to query influxdb for backfill: %w", err)
	}

	v.log.Info("telemetry/usage: backfill queried influxdb", "rows", len(rows), "duration", queryDuration.String())

	if len(rows) == 0 {
		return &BackfillResult{
			StartTime:    startTime,
			EndTime:      endTime,
			RowsQueried:  0,
			RowsInserted: 0,
		}, nil
	}

	// Convert rows to InterfaceUsage
	// Pass nil for alreadyWritten - backfill relies on ReplacingMergeTree for deduplication
	usage, err := v.convertRowsToUsage(rows, baselines, linkLookup, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to convert rows for backfill: %w", err)
	}

	if len(usage) == 0 {
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

	return &BackfillResult{
		StartTime:    startTime,
		EndTime:      endTime,
		RowsQueried:  len(rows),
		RowsInserted: len(usage),
	}, nil
}
