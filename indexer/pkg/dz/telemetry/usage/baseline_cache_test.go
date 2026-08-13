package dztelemusage

import (
	"context"
	"testing"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/metrics"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newBaselineTestView builds a view backed by the shared ClickHouse instance
// with a mock InfluxDB client, for exercising the baseline cache directly.
func newBaselineTestView(t *testing.T, influx InfluxDBClient, dzEnv string) *View {
	t.Helper()
	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clockwork.NewFakeClock(),
		ClickHouse:      testClient(t),
		InfluxDB:        influx,
		Bucket:          "test-bucket",
		RefreshInterval: time.Second,
		QueryWindow:     time.Hour,
		QueryChunk:      5 * time.Minute,
		DZEnv:           dzEnv,
	})
	require.NoError(t, err)
	return view
}

// insertSparseRow writes a single interface-counters row carrying an in_errors
// value at event_ts, so queryBaselineCountersFromClickHouse can read it back.
func insertSparseRow(t *testing.T, view *View, devicePK, intf string, eventTS time.Time, inErrors int64) {
	t.Helper()
	err := view.store.InsertInterfaceUsage(context.Background(), []InterfaceUsage{{
		Time:     eventTS,
		DevicePK: stringPtr(devicePK),
		Intf:     stringPtr(intf),
		InErrors: int64Ptr(inErrors),
	}})
	require.NoError(t, err)
}

// TestLake_TelemetryUsage_BaselineCache_HitAtWatermark proves a windowStart at or
// near the cached watermark reuses the in-memory baselines and does NOT re-query
// ClickHouse — including a windowStart above the watermark (the "data too old"
// branch shape).
func TestLake_TelemetryUsage_BaselineCache_HitAtWatermark(t *testing.T) {
	t.Parallel()

	view := newBaselineTestView(t, &mockInfluxDBClient{}, "test-cache-hit")
	key := "devA:eth0"

	// ClickHouse holds a different value than the cache. A cache hit must return
	// the cached value, proving the scan was skipped.
	watermark := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	insertSparseRow(t, view, "devA", "eth0", watermark.Add(-1*time.Minute), 999)

	view.baselineCache = &CounterBaselines{
		InDiscards:  map[string]*int64{},
		InErrors:    map[string]*int64{key: int64Ptr(100)},
		InFCSErrors: map[string]*int64{},
		OutDiscards: map[string]*int64{},
		OutErrors:   map[string]*int64{},
	}
	view.baselineCacheWatermark = watermark

	counter := metrics.ClickHouseBaselineQueryTotal.WithLabelValues("test-cache-hit")
	before := testutil.ToFloat64(counter)

	// Single-writer maxTime: ClickHouse's max event_ts sits at/below the watermark.
	chMaxTime := watermark.Add(-1 * time.Minute)

	// windowStart slightly below the watermark (normal 5m overlap).
	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), watermark.Add(-5*time.Minute), &chMaxTime)
	require.NoError(t, err)
	require.NotNil(t, got.InErrors[key])
	require.Equal(t, int64(100), *got.InErrors[key], "should return cached value, not ClickHouse value")

	// windowStart above the watermark ("data too old" branch): still a hit while
	// max event_ts hasn't moved past the watermark (single-writer recovery).
	got, err = view.queryBaselineCountersFromClickHouse(context.Background(), watermark.Add(5*time.Minute), &chMaxTime)
	require.NoError(t, err)
	require.Equal(t, int64(100), *got.InErrors[key])

	// The forward side is deliberately unbounded for single-writer stall
	// recovery: even a large jump (source down for hours, windowStart resumed at
	// now-QueryWindow) must hit while max event_ts stays at the watermark —
	// nobody wrote the gap, so the cached state still describes it.
	got, err = view.queryBaselineCountersFromClickHouse(context.Background(), watermark.Add(6*time.Hour), &chMaxTime)
	require.NoError(t, err)
	require.Equal(t, int64(100), *got.InErrors[key],
		"large forward jump with unmoved max event_ts must still cache-hit")

	require.Equal(t, before, testutil.ToFloat64(counter), "baseline query must not run on cache hit")
}

// TestLake_TelemetryUsage_BaselineCache_BypassForHistorical proves a historical
// windowStart far below the watermark bypasses the cache and reads ClickHouse.
func TestLake_TelemetryUsage_BaselineCache_BypassForHistorical(t *testing.T) {
	t.Parallel()

	view := newBaselineTestView(t, &mockInfluxDBClient{}, "test-cache-bypass")
	key := "devB:eth0"

	watermark := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	view.baselineCache = &CounterBaselines{
		InDiscards:  map[string]*int64{},
		InErrors:    map[string]*int64{key: int64Ptr(100)},
		InFCSErrors: map[string]*int64{},
		OutDiscards: map[string]*int64{},
		OutErrors:   map[string]*int64{},
	}
	view.baselineCacheWatermark = watermark

	// A historical window a month back. The ClickHouse row sits within its
	// 2-day lookback; the cache does not apply.
	historicalWindow := watermark.Add(-30 * 24 * time.Hour)
	insertSparseRow(t, view, "devB", "eth0", historicalWindow.Add(-1*time.Hour), 777)

	counter := metrics.ClickHouseBaselineQueryTotal.WithLabelValues("test-cache-bypass")
	before := testutil.ToFloat64(counter)

	// nil chMaxTime mimics the backfill call site, which skips the forward gate.
	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), historicalWindow, nil)
	require.NoError(t, err)
	require.NotNil(t, got.InErrors[key])
	require.Equal(t, int64(777), *got.InErrors[key], "historical window must read fresh ClickHouse value")
	require.Equal(t, before+1, testutil.ToFloat64(counter), "baseline query must run on cache bypass")
}

// TestLake_TelemetryUsage_BaselineCache_LookbackBound proves the 2-day lookback:
// an interface whose last report is older than baselineLookback gets no baseline.
func TestLake_TelemetryUsage_BaselineCache_LookbackBound(t *testing.T) {
	t.Parallel()

	view := newBaselineTestView(t, &mockInfluxDBClient{}, "test-lookback")

	windowStart := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	// devOld reported 3 days before windowStart — outside the 2-day lookback.
	insertSparseRow(t, view, "devOld", "eth0", windowStart.Add(-3*24*time.Hour), 11)
	// devNew reported 1 day before windowStart — inside the lookback.
	insertSparseRow(t, view, "devNew", "eth0", windowStart.Add(-1*24*time.Hour), 22)

	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), windowStart, nil)
	require.NoError(t, err)

	require.Nil(t, got.InErrors["devOld:eth0"], "interface silent > baselineLookback must have no baseline")
	require.NotNil(t, got.InErrors["devNew:eth0"])
	require.Equal(t, int64(22), *got.InErrors["devNew:eth0"])
}

// TestLake_TelemetryUsage_BaselineCache_MergePreservesSilentKeys proves the cache
// is merged, not replaced: a key absent from a window's end-of-window baselines
// (an interface that reported no rows that window) keeps its carried value, and a
// window with no new data does not wipe the cache — only advances the watermark.
func TestLake_TelemetryUsage_BaselineCache_MergePreservesSilentKeys(t *testing.T) {
	t.Parallel()

	w1 := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	w2 := w1.Add(10 * time.Minute)

	view := &View{
		baselineCache: &CounterBaselines{
			InDiscards:  map[string]*int64{},
			InErrors:    map[string]*int64{"devA:eth0": int64Ptr(100)},
			InFCSErrors: map[string]*int64{},
			OutDiscards: map[string]*int64{},
			OutErrors:   map[string]*int64{},
		},
		baselineCacheWatermark: w1,
	}

	// Next window: only devB reported; devA was silent.
	view.updateBaselineCache(&CounterBaselines{
		InDiscards:  map[string]*int64{},
		InErrors:    map[string]*int64{"devB:eth0": int64Ptr(200)},
		InFCSErrors: map[string]*int64{},
		OutDiscards: map[string]*int64{},
		OutErrors:   map[string]*int64{},
	}, w2)

	require.NotNil(t, view.baselineCache.InErrors["devA:eth0"], "silent key must be preserved by merge")
	require.Equal(t, int64(100), *view.baselineCache.InErrors["devA:eth0"])
	require.Equal(t, int64(200), *view.baselineCache.InErrors["devB:eth0"])
	require.Equal(t, w2, view.baselineCacheWatermark)

	// An empty window (0 rows → empty end-of-window baselines) must not wipe the
	// cache, only advance the watermark — otherwise the next refresh cache-hits on
	// 0 keys and falls into the expensive InfluxDB fallback.
	w3 := w2.Add(10 * time.Minute)
	view.updateBaselineCache(&CounterBaselines{
		InDiscards:  map[string]*int64{},
		InErrors:    map[string]*int64{},
		InFCSErrors: map[string]*int64{},
		OutDiscards: map[string]*int64{},
		OutErrors:   map[string]*int64{},
	}, w3)

	require.Equal(t, int64(100), *view.baselineCache.InErrors["devA:eth0"])
	require.Equal(t, int64(200), *view.baselineCache.InErrors["devB:eth0"])
	require.Equal(t, w3, view.baselineCacheWatermark, "empty window must still advance the watermark")

	// A nil end-of-window struct is a no-op (watermark unchanged).
	view.updateBaselineCache(nil, w3.Add(time.Hour))
	require.Equal(t, w3, view.baselineCacheWatermark)
}

// TestLake_TelemetryUsage_BaselineCache_BackfillCarry proves two sequential
// contiguous backfill chunks scan ClickHouse for baselines only once: the second
// chunk reuses the first chunk's carried end-of-window baselines.
func TestLake_TelemetryUsage_BaselineCache_BackfillCarry(t *testing.T) {
	t.Parallel()

	// Mock InfluxDB returns one sparse row per chunk so convertRowsToUsage
	// produces non-empty end-of-window baselines to carry forward.
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, _ time.Time) ([]map[string]any, error) {
			return []map[string]any{{
				"time":       s.Format(time.RFC3339Nano),
				"dzd_pubkey": "devC",
				"intf":       "eth0",
				"in-errors":  int64(5),
				"in-octets":  int64(1000),
			}}, nil
		},
	}
	view := newBaselineTestView(t, influx, "test-backfill-carry")

	counter := metrics.ClickHouseBaselineQueryTotal.WithLabelValues("test-backfill-carry")
	before := testutil.ToFloat64(counter)

	// Two contiguous ascending chunks: chunk2 starts where chunk1 ended.
	c1Start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	c1End := c1Start.Add(5 * time.Minute)
	c2End := c1End.Add(5 * time.Minute)

	_, err := view.BackfillForTimeRange(context.Background(), c1Start, c1End)
	require.NoError(t, err)
	require.Equal(t, c1End, view.baselineCacheWatermark, "chunk 1 must set watermark to its end")

	_, err = view.BackfillForTimeRange(context.Background(), c1End, c2End)
	require.NoError(t, err)
	require.Equal(t, c2End, view.baselineCacheWatermark, "chunk 2 must advance the watermark")

	require.Equal(t, before+1, testutil.ToFloat64(counter),
		"baseline scan must run once across two contiguous backfill chunks")
}

// TestLake_TelemetryUsage_BaselineCache_ForeignWriteForcesRescan proves the
// forward gate: when ClickHouse's max event_ts has advanced past this process's
// watermark (a second writer — e.g. the admin backfill's continue mode — filled
// the gap while the watermark was frozen), the cache must MISS and re-scan, or
// post-gap sparse deltas are computed against pre-gap baselines.
func TestLake_TelemetryUsage_BaselineCache_ForeignWriteForcesRescan(t *testing.T) {
	t.Parallel()

	view := newBaselineTestView(t, &mockInfluxDBClient{}, "test-foreign-write")
	key := "devD:eth0"

	// This process's watermark froze at T; the cache holds the pre-gap value 100.
	watermark := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	view.baselineCache = &CounterBaselines{
		InDiscards:  map[string]*int64{},
		InErrors:    map[string]*int64{key: int64Ptr(100)},
		InFCSErrors: map[string]*int64{},
		OutDiscards: map[string]*int64{},
		OutErrors:   map[string]*int64{},
	}
	view.baselineCacheWatermark = watermark

	// A foreign writer backfilled the gap: rows exist past the watermark, and
	// ClickHouse's max event_ts now sits well above it.
	insertSparseRow(t, view, "devD", "eth0", watermark.Add(20*time.Minute), 777)
	chMaxTime := watermark.Add(30 * time.Minute)

	counter := metrics.ClickHouseBaselineQueryTotal.WithLabelValues("test-foreign-write")
	before := testutil.ToFloat64(counter)

	// Recovery shape: windowStart = maxTime - overlap, above the stale watermark.
	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), watermark.Add(25*time.Minute), &chMaxTime)
	require.NoError(t, err)
	require.NotNil(t, got.InErrors[key])
	require.Equal(t, int64(777), *got.InErrors[key],
		"foreign write past the watermark must force a re-scan, not serve the stale cache")
	require.Equal(t, before+1, testutil.ToFloat64(counter), "baseline scan must run")

	// The whole cache must be discarded, not just bypassed: the post-refresh
	// merge would otherwise carry pre-gap values for silent keys back in.
	require.Nil(t, view.baselineCache, "foreign writes must invalidate the cached baselines")
	require.True(t, view.baselineCacheWatermark.IsZero())
}

// TestLake_TelemetryUsage_BaselineCache_BackfillZeroRowChunkAdvancesWatermark
// proves a backfill chunk with zero InfluxDB rows (old/gappy historical data)
// still advances the watermark, so the following chunk cache-hits and the
// watermark keeps tracking the data end the cached baselines represent.
func TestLake_TelemetryUsage_BaselineCache_BackfillZeroRowChunkAdvancesWatermark(t *testing.T) {
	t.Parallel()

	c1Start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	c1End := c1Start.Add(5 * time.Minute)
	c2End := c1End.Add(5 * time.Minute)
	c3End := c2End.Add(5 * time.Minute)

	// Chunks 1 and 3 carry one sparse row each; chunk 2 is empty.
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, _ time.Time) ([]map[string]any, error) {
			if s.Equal(c1End) {
				return []map[string]any{}, nil
			}
			return []map[string]any{{
				"time":       s.Format(time.RFC3339Nano),
				"dzd_pubkey": "devE",
				"intf":       "eth0",
				"in-errors":  int64(5),
				"in-octets":  int64(1000),
			}}, nil
		},
	}
	view := newBaselineTestView(t, influx, "test-backfill-zero-row")

	counter := metrics.ClickHouseBaselineQueryTotal.WithLabelValues("test-backfill-zero-row")
	before := testutil.ToFloat64(counter)

	res, err := view.BackfillForTimeRange(context.Background(), c1Start, c1End)
	require.NoError(t, err)
	require.Equal(t, 1, res.RowsQueried)
	require.Equal(t, c1End, view.baselineCacheWatermark)

	res, err = view.BackfillForTimeRange(context.Background(), c1End, c2End)
	require.NoError(t, err)
	require.Equal(t, 0, res.RowsQueried)
	require.Equal(t, c2End, view.baselineCacheWatermark, "zero-row chunk must still advance the watermark")
	require.NotNil(t, view.baselineCache.InErrors["devE:eth0"], "zero-row chunk must not drop carried baselines")

	_, err = view.BackfillForTimeRange(context.Background(), c2End, c3End)
	require.NoError(t, err)
	require.Equal(t, c3End, view.baselineCacheWatermark)

	require.Equal(t, before+1, testutil.ToFloat64(counter),
		"baseline scan must run once across all three chunks, including past the empty one")
}

// TestLake_TelemetryUsage_BaselineCache_FailedInsertLeavesCacheUnchanged proves
// the commit-after-insert invariant: when InsertInterfaceUsage fails, the
// baseline cache and watermark must be untouched, so the next refresh cannot
// delta re-read overlap rows against unpersisted end-of-window values.
func TestLake_TelemetryUsage_BaselineCache_FailedInsertLeavesCacheUnchanged(t *testing.T) {
	t.Parallel()

	// The mock returns two rows (the first per key is consumed as the non-sparse
	// baseline, the second becomes an insertable usage row), then cancels the
	// refresh context so the ClickHouse insert fails after conversion succeeded.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, _ time.Time) ([]map[string]any, error) {
			rows := []map[string]any{
				{
					"time":       s.Format(time.RFC3339Nano),
					"dzd_pubkey": "devF",
					"intf":       "eth0",
					"in-errors":  int64(5),
					"in-octets":  int64(1000),
				},
				{
					"time":       s.Add(time.Minute).Format(time.RFC3339Nano),
					"dzd_pubkey": "devF",
					"intf":       "eth0",
					"in-errors":  int64(6),
					"in-octets":  int64(2000),
				},
			}
			cancel()
			return rows, nil
		},
	}
	view := newBaselineTestView(t, influx, "test-failed-insert")

	_, err := view.Refresh(ctx)
	require.Error(t, err, "refresh must fail when the insert fails")

	require.Nil(t, view.baselineCache, "failed insert must not populate the baseline cache")
	require.True(t, view.baselineCacheWatermark.IsZero(), "failed insert must not advance the watermark")
}
