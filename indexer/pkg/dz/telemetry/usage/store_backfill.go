package dztelemusage

import (
	"context"
	"fmt"
	"io"
	"time"
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

	sqlQuery := fmt.Sprintf(`
		SELECT
			time,
			dzd_pubkey,
			host,
			intf,
			model_name,
			serial_number,
			"carrier-transitions",
			"in-broadcast-pkts",
			"in-discards",
			"in-errors",
			"in-fcs-errors",
			"in-multicast-pkts",
			"in-octets",
			"in-pkts",
			"in-unicast-pkts",
			"out-broadcast-pkts",
			"out-discards",
			"out-errors",
			"out-multicast-pkts",
			"out-octets",
			"out-pkts",
			"out-unicast-pkts"
		FROM "intfCounters"
		WHERE time >= '%s' AND time < '%s'
	`, startTimeUTC.Format(time.RFC3339Nano), endTimeUTC.Format(time.RFC3339Nano))

	rows, err := v.cfg.InfluxDB.QuerySQL(ctx, sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query influxdb for backfill: %w", err)
	}

	v.log.Info("telemetry/usage: backfill queried influxdb", "rows", len(rows))

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

// BackfillFromReader reads intfCounters CSV data from r and imports it into ClickHouse.
// The CSV format must match the output of ExportInterfaceCountersCSV.
// This does not require a live InfluxDB connection.
// It relies on ReplacingMergeTree for deduplication, making it safe to re-run.
func (v *View) BackfillFromReader(ctx context.Context, r io.Reader) (*BackfillResult, error) {
	rows, err := ParseInfluxCSV(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	v.log.Info("telemetry/usage: backfill from CSV parsed rows", "rows", len(rows))

	if len(rows) == 0 {
		return &BackfillResult{}, nil
	}

	// Derive time range from the data for the result summary
	var startTime, endTime time.Time
	for _, row := range rows {
		timeStr := extractStringFromRow(row, "time")
		if timeStr == nil {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, *timeStr)
		if err != nil {
			t, err = time.Parse(time.RFC3339, *timeStr)
			if err != nil {
				continue
			}
		}
		if startTime.IsZero() || t.Before(startTime) {
			startTime = t
		}
		if t.After(endTime) {
			endTime = t
		}
	}

	// Query baseline counters from ClickHouse to compute accurate deltas
	baselines, err := v.queryBaselineCountersFromClickHouse(ctx, startTime)
	if err != nil {
		v.log.Warn("telemetry/usage: failed to query baseline counters for CSV backfill, proceeding without", "error", err)
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
		v.log.Warn("telemetry/usage: failed to build link lookup for CSV backfill, proceeding without", "error", err)
		linkLookup = make(map[string]LinkInfo)
	}

	usage, err := v.convertRowsToUsage(rows, baselines, linkLookup, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to convert CSV rows: %w", err)
	}

	if len(usage) == 0 {
		return &BackfillResult{
			StartTime:    startTime,
			EndTime:      endTime,
			RowsQueried:  len(rows),
			RowsInserted: 0,
		}, nil
	}

	if err := v.store.InsertInterfaceUsage(ctx, usage); err != nil {
		return nil, fmt.Errorf("failed to insert CSV backfill data: %w", err)
	}

	return &BackfillResult{
		StartTime:    startTime,
		EndTime:      endTime,
		RowsQueried:  len(rows),
		RowsInserted: len(usage),
	}, nil
}
