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

	// windowStart slightly below the watermark (normal 5m overlap).
	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), watermark.Add(-5*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, got.InErrors[key])
	require.Equal(t, int64(100), *got.InErrors[key], "should return cached value, not ClickHouse value")

	// windowStart above the watermark ("data too old" branch): still a hit.
	got, err = view.queryBaselineCountersFromClickHouse(context.Background(), watermark.Add(5*time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(100), *got.InErrors[key])

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

	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), historicalWindow)
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

	got, err := view.queryBaselineCountersFromClickHouse(context.Background(), windowStart)
	require.NoError(t, err)

	require.Nil(t, got.InErrors["devOld:eth0"], "interface silent > baselineLookback must have no baseline")
	require.NotNil(t, got.InErrors["devNew:eth0"])
	require.Equal(t, int64(22), *got.InErrors["devNew:eth0"])
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
