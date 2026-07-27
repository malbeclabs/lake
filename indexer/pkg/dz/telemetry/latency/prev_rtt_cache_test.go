package dztelemlatency

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newCacheTestStore builds a store over a fresh test database with a distinct
// DZEnv so each test reads its own Prometheus counter children.
func newCacheTestStore(t *testing.T, db clickhouse.Client, dzEnv string) *Store {
	t.Helper()
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: db,
		DZEnv:      dzEnv,
	})
	require.NoError(t, err)
	return store
}

// prevRTTQueryCount sums the bounded and fallback previous-RTT query counters
// for one env+table, so tests can assert "no ClickHouse read happened" without
// caring which pass would have run.
func prevRTTQueryCount(dzEnv, table string) float64 {
	return testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, table, "bounded")) +
		testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, table, "fallback"))
}

// queryDeviceLinkIPDV reads back the ipdv_us written for one sample.
func queryDeviceLinkIPDV(t *testing.T, db clickhouse.Client, originPK, targetPK, linkPK string, epoch uint64, sampleIndex int) *int64 {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.Query(context.Background(),
		"SELECT ipdv_us FROM fact_dz_device_link_latency WHERE origin_device_pk = ? AND target_device_pk = ? AND link_pk = ? AND epoch = ? AND sample_index = ?",
		originPK, targetPK, linkPK, epoch, sampleIndex)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "sample %d must exist", sampleIndex)
	var ipdv *int64
	require.NoError(t, rows.Scan(&ipdv))
	return ipdv
}

// queryInternetMetroIPDV reads back the ipdv_us written for one sample.
func queryInternetMetroIPDV(t *testing.T, db clickhouse.Client, originPK, targetPK, provider string, epoch uint64, sampleIndex int) *int64 {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.Query(context.Background(),
		"SELECT ipdv_us FROM fact_dz_internet_metro_latency WHERE origin_metro_pk = ? AND target_metro_pk = ? AND data_provider = ? AND epoch = ? AND sample_index = ?",
		originPK, targetPK, provider, epoch, sampleIndex)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next(), "sample %d must exist", sampleIndex)
	var ipdv *int64
	require.NoError(t, rows.Scan(&ipdv))
	return ipdv
}

// TestLake_TelemetryLatency_PrevRTTCache_HitOnSameStore proves a second append
// on the same store computes IPDV from the in-memory carry-forward cache with
// no ClickHouse previous-RTT read at all.
func TestLake_TelemetryLatency_PrevRTTCache_HitOnSameStore(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-dl-cache-hit"
	db := testClient(t)
	store := newCacheTestStore(t, db, dzEnv)

	originPK, targetPK, linkPK := testPK(1), testPK(2), testPK(3)
	now := time.Now()

	err := store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 0, Time: now.Add(-2 * time.Minute), RTTMicroseconds: 5000},
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 1, Time: now.Add(-1 * time.Minute), RTTMicroseconds: 6000},
	})
	require.NoError(t, err)

	before := prevRTTQueryCount(dzEnv, "device_link")

	err = store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 2, Time: now, RTTMicroseconds: 7000},
	})
	require.NoError(t, err)

	require.Equal(t, before, prevRTTQueryCount(dzEnv, "device_link"),
		"second append on the same store must not query ClickHouse for previous RTTs")

	ipdv := queryDeviceLinkIPDV(t, db, originPK, targetPK, linkPK, 100, 2)
	require.NotNil(t, ipdv)
	require.Equal(t, int64(1000), *ipdv, "IPDV must be computed against the cached previous RTT (7000-6000)")
}

// TestLake_TelemetryLatency_PrevRTTCache_BoundedSeedOnRestart proves a fresh
// store (restart path) seeds a recently-active circuit from the bounded scan
// alone, without the unbounded fallback.
func TestLake_TelemetryLatency_PrevRTTCache_BoundedSeedOnRestart(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-dl-bounded"
	db := testClient(t)
	now := time.Now()
	originPK, targetPK, linkPK := testPK(1), testPK(2), testPK(3)

	// First store writes recent history (well inside prevRTTLookback).
	seedStore := newCacheTestStore(t, db, dzEnv+"-seed")
	err := seedStore.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 0, Time: now.Add(-1 * time.Hour), RTTMicroseconds: 5000},
	})
	require.NoError(t, err)

	// A new store simulates an indexer restart: empty cache, same data.
	store := newCacheTestStore(t, db, dzEnv)
	boundedBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "device_link", "bounded"))
	fallbackBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "device_link", "fallback"))

	err = store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 1, Time: now, RTTMicroseconds: 6500},
	})
	require.NoError(t, err)

	require.Equal(t, boundedBefore+1, testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "device_link", "bounded")),
		"restart must seed via one bounded query")
	require.Equal(t, fallbackBefore, testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "device_link", "fallback")),
		"a circuit active within the lookback must not need the fallback")

	ipdv := queryDeviceLinkIPDV(t, db, originPK, targetPK, linkPK, 100, 1)
	require.NotNil(t, ipdv)
	require.Equal(t, int64(1500), *ipdv, "IPDV must be computed against the bounded-seeded previous RTT (6500-5000)")
}

// TestLake_TelemetryLatency_PrevRTTCache_FallbackForQuietCircuit proves a
// circuit whose last sample is older than prevRTTLookback still gets its IPDV
// baseline via the unbounded per-circuit fallback, and that both the fallback
// query and fallback circuit counters record it.
func TestLake_TelemetryLatency_PrevRTTCache_FallbackForQuietCircuit(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-dl-fallback"
	db := testClient(t)
	now := time.Now()
	originPK, targetPK, linkPK := testPK(1), testPK(2), testPK(3)

	// History older than the 2-day lookback: the bounded pass must miss it.
	seedStore := newCacheTestStore(t, db, dzEnv+"-seed")
	err := seedStore.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 90, SampleIndex: 0, Time: now.Add(-3 * 24 * time.Hour), RTTMicroseconds: 5000},
	})
	require.NoError(t, err)

	store := newCacheTestStore(t, db, dzEnv)
	fallbackBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "device_link", "fallback"))
	circuitsBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(dzEnv, "device_link"))

	err = store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 0, Time: now, RTTMicroseconds: 6200},
	})
	require.NoError(t, err)

	require.Equal(t, fallbackBefore+1, testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "device_link", "fallback")),
		"quiet circuit must be resolved by one fallback query")
	require.Equal(t, circuitsBefore+1, testutil.ToFloat64(metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(dzEnv, "device_link")),
		"fallback circuits counter must count the quiet circuit")

	ipdv := queryDeviceLinkIPDV(t, db, originPK, targetPK, linkPK, 100, 0)
	require.NotNil(t, ipdv)
	require.Equal(t, int64(1200), *ipdv, "quiet circuit keeps its carried-forward IPDV baseline (6200-5000)")
}

// TestLake_TelemetryLatency_PrevRTTCache_NewCircuit proves a circuit with no
// history gets a NULL IPDV on its first sample and is counted by the fallback
// circuits counter (both passes ran and found nothing).
func TestLake_TelemetryLatency_PrevRTTCache_NewCircuit(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-dl-new"
	db := testClient(t)
	store := newCacheTestStore(t, db, dzEnv)
	originPK, targetPK, linkPK := testPK(1), testPK(2), testPK(3)

	circuitsBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(dzEnv, "device_link"))

	err := store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 0, Time: time.Now(), RTTMicroseconds: 5000},
	})
	require.NoError(t, err)

	require.Equal(t, circuitsBefore+1, testutil.ToFloat64(metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(dzEnv, "device_link")))
	require.Nil(t, queryDeviceLinkIPDV(t, db, originPK, targetPK, linkPK, 100, 0),
		"first sample of a brand-new circuit must have NULL IPDV")
}

// TestLake_TelemetryLatency_PrevRTTCache_AllLossCircuitQueriedOnce proves the
// negative-cache sentinel: a circuit that has never produced a non-zero RTT is
// looked up in ClickHouse once, not on every append — without the sentinel the
// unbounded fallback (the exact query shape #720 removes) would rerun per
// refresh for as long as the circuit stays all-loss.
func TestLake_TelemetryLatency_PrevRTTCache_AllLossCircuitQueriedOnce(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-dl-all-loss"
	db := testClient(t)
	store := newCacheTestStore(t, db, dzEnv)
	originPK, targetPK, linkPK := testPK(1), testPK(2), testPK(3)
	now := time.Now()

	// All-loss batch: both passes run and find nothing, caching the sentinel.
	err := store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 0, Time: now.Add(-2 * time.Minute), RTTMicroseconds: 0},
	})
	require.NoError(t, err)

	before := prevRTTQueryCount(dzEnv, "device_link")

	// Second all-loss batch must be served from the sentinel, no queries.
	err = store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 1, Time: now.Add(-1 * time.Minute), RTTMicroseconds: 0},
	})
	require.NoError(t, err)
	require.Equal(t, before, prevRTTQueryCount(dzEnv, "device_link"),
		"an all-loss circuit must not requery ClickHouse on subsequent appends")

	// First non-zero sample: still no query (sentinel hit), NULL IPDV (no
	// baseline), and the real RTT replaces the sentinel.
	err = store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: originPK, TargetDevicePK: targetPK, LinkPK: linkPK, Epoch: 100, SampleIndex: 2, Time: now, RTTMicroseconds: 5000},
	})
	require.NoError(t, err)
	require.Equal(t, before, prevRTTQueryCount(dzEnv, "device_link"))
	require.Nil(t, queryDeviceLinkIPDV(t, db, originPK, targetPK, linkPK, 100, 2),
		"first non-zero sample after all-loss history must have NULL IPDV")
}

// TestLake_TelemetryLatency_PrevRTTCache_InternetMetro_HitOnSameStore mirrors
// the device-link cache-hit test for the internet-metro append path.
func TestLake_TelemetryLatency_PrevRTTCache_InternetMetro_HitOnSameStore(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-im-cache-hit"
	db := testClient(t)
	store := newCacheTestStore(t, db, dzEnv)

	originPK, targetPK, provider := testPK(1), testPK(2), "RIPE Atlas"
	now := time.Now()

	err := store.AppendInternetMetroLatencySamples(context.Background(), []InternetMetroLatencySample{
		{OriginMetroPK: originPK, TargetMetroPK: targetPK, DataProvider: provider, Epoch: 100, SampleIndex: 0, Time: now.Add(-2 * time.Minute), RTTMicroseconds: 5000},
		{OriginMetroPK: originPK, TargetMetroPK: targetPK, DataProvider: provider, Epoch: 100, SampleIndex: 1, Time: now.Add(-1 * time.Minute), RTTMicroseconds: 6000},
	})
	require.NoError(t, err)

	before := prevRTTQueryCount(dzEnv, "internet_metro")

	err = store.AppendInternetMetroLatencySamples(context.Background(), []InternetMetroLatencySample{
		{OriginMetroPK: originPK, TargetMetroPK: targetPK, DataProvider: provider, Epoch: 100, SampleIndex: 2, Time: now, RTTMicroseconds: 7000},
	})
	require.NoError(t, err)

	require.Equal(t, before, prevRTTQueryCount(dzEnv, "internet_metro"),
		"second append on the same store must not query ClickHouse for previous RTTs")

	ipdv := queryInternetMetroIPDV(t, db, originPK, targetPK, provider, 100, 2)
	require.NotNil(t, ipdv)
	require.Equal(t, int64(1000), *ipdv)
}

// TestLake_TelemetryLatency_PrevRTTCache_InternetMetro_FallbackForQuietCircuit
// mirrors the device-link quiet-circuit fallback test for internet-metro.
func TestLake_TelemetryLatency_PrevRTTCache_InternetMetro_FallbackForQuietCircuit(t *testing.T) {
	t.Parallel()

	const dzEnv = "test-prevrtt-im-fallback"
	db := testClient(t)
	now := time.Now()
	originPK, targetPK, provider := testPK(1), testPK(2), "RIPE Atlas"

	seedStore := newCacheTestStore(t, db, dzEnv+"-seed")
	err := seedStore.AppendInternetMetroLatencySamples(context.Background(), []InternetMetroLatencySample{
		{OriginMetroPK: originPK, TargetMetroPK: targetPK, DataProvider: provider, Epoch: 90, SampleIndex: 0, Time: now.Add(-3 * 24 * time.Hour), RTTMicroseconds: 5000},
	})
	require.NoError(t, err)

	store := newCacheTestStore(t, db, dzEnv)
	fallbackBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "internet_metro", "fallback"))
	circuitsBefore := testutil.ToFloat64(metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(dzEnv, "internet_metro"))

	err = store.AppendInternetMetroLatencySamples(context.Background(), []InternetMetroLatencySample{
		{OriginMetroPK: originPK, TargetMetroPK: targetPK, DataProvider: provider, Epoch: 100, SampleIndex: 0, Time: now, RTTMicroseconds: 6200},
	})
	require.NoError(t, err)

	require.Equal(t, fallbackBefore+1, testutil.ToFloat64(metrics.ClickHousePrevRTTQueryTotal.WithLabelValues(dzEnv, "internet_metro", "fallback")))
	require.Equal(t, circuitsBefore+1, testutil.ToFloat64(metrics.ClickHousePrevRTTFallbackCircuitsTotal.WithLabelValues(dzEnv, "internet_metro")))

	ipdv := queryInternetMetroIPDV(t, db, originPK, targetPK, provider, 100, 0)
	require.NotNil(t, ipdv)
	require.Equal(t, int64(1200), *ipdv, "quiet circuit keeps its carried-forward IPDV baseline (6200-5000)")
}

// TestLake_TelemetryLatency_Store_MaxSampleIndices_CarryForward proves a
// circuit-epoch whose samples are all older than maxSampleIndexLookback keeps
// its max index on the store that wrote it (no recurring full-tail refetch),
// is dropped exactly once on a fresh store (the per-process-start refetch),
// and is carried forward again after that refetch's append.
func TestLake_TelemetryLatency_Store_MaxSampleIndices_CarryForward(t *testing.T) {
	t.Parallel()

	db := testClient(t)
	now := time.Now()
	staleKey := fmt.Sprintf("%s:%s:%s:99", testPK(1), testPK(2), testPK(3))
	staleSamples := []DeviceLinkLatencySample{
		// Previous epoch, agent quiet for 5 days: all rows outside the bound.
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 99, SampleIndex: 0, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5000},
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 99, SampleIndex: 1, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5100},
	}

	writer := newCacheTestStore(t, db, "test-maxidx-dl-carry-writer")
	err := writer.AppendDeviceLinkLatencySamples(context.Background(), append(staleSamples,
		// Current epoch on another circuit, recent.
		DeviceLinkLatencySample{OriginDevicePK: testPK(4), TargetDevicePK: testPK(5), LinkPK: testPK(6), Epoch: 100, SampleIndex: 0, Time: now.Add(-1 * time.Hour), RTTMicroseconds: 6000},
	))
	require.NoError(t, err)

	indices, err := writer.GetExistingMaxSampleIndices()
	require.NoError(t, err)
	require.Contains(t, indices, staleKey,
		"the writing process must carry a stale circuit-epoch's max index forward past the scan bound")
	require.Equal(t, 1, indices[staleKey])
	require.Contains(t, indices, fmt.Sprintf("%s:%s:%s:100", testPK(4), testPK(5), testPK(6)))

	// A fresh store (restart) sees only the bounded scan: the stale key is
	// dropped and the caller refetches that epoch's tail once.
	restarted := newCacheTestStore(t, db, "test-maxidx-dl-carry-restart")
	indices, err = restarted.GetExistingMaxSampleIndices()
	require.NoError(t, err)
	require.NotContains(t, indices, staleKey,
		"a fresh store must drop keys whose rows are all older than the scan bound")

	// The refetch's (deduped) re-append repopulates the carry-forward, so the
	// refetch happens once per process start, not once per refresh.
	err = restarted.AppendDeviceLinkLatencySamples(context.Background(), staleSamples)
	require.NoError(t, err)
	indices, err = restarted.GetExistingMaxSampleIndices()
	require.NoError(t, err)
	require.Contains(t, indices, staleKey,
		"the re-append must restore the carry-forward on the new store")
	require.Equal(t, 1, indices[staleKey])
}

// TestLake_TelemetryLatency_Store_MaxSampleIndices_CarryForwardPruning proves
// cached epochs at least two behind the newest written epoch are pruned:
// callers only fetch the current and previous epoch, so those entries would
// only accumulate for the process lifetime.
func TestLake_TelemetryLatency_Store_MaxSampleIndices_CarryForwardPruning(t *testing.T) {
	t.Parallel()

	db := testClient(t)
	store := newCacheTestStore(t, db, "test-maxidx-dl-prune")
	now := time.Now()

	err := store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 90, SampleIndex: 0, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5000},
	})
	require.NoError(t, err)

	err = store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 100, SampleIndex: 0, Time: now.Add(-1 * time.Hour), RTTMicroseconds: 6000},
	})
	require.NoError(t, err)

	indices, err := store.GetExistingMaxSampleIndices()
	require.NoError(t, err)
	require.NotContains(t, indices, fmt.Sprintf("%s:%s:%s:90", testPK(1), testPK(2), testPK(3)),
		"an epoch two or more behind the newest written epoch must be pruned from the carry-forward")
	require.Contains(t, indices, fmt.Sprintf("%s:%s:%s:100", testPK(1), testPK(2), testPK(3)))
}

// TestLake_TelemetryLatency_Store_InternetMaxSampleIndices_CarryForward mirrors
// the device-link carry-forward test for the internet-metro table.
func TestLake_TelemetryLatency_Store_InternetMaxSampleIndices_CarryForward(t *testing.T) {
	t.Parallel()

	db := testClient(t)
	now := time.Now()
	staleKey := fmt.Sprintf("%s:%s:%s:99", testPK(1), testPK(2), "RIPE Atlas")
	staleSamples := []InternetMetroLatencySample{
		{OriginMetroPK: testPK(1), TargetMetroPK: testPK(2), DataProvider: "RIPE Atlas", Epoch: 99, SampleIndex: 0, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5000},
	}

	writer := newCacheTestStore(t, db, "test-maxidx-im-carry-writer")
	err := writer.AppendInternetMetroLatencySamples(context.Background(), append(staleSamples,
		InternetMetroLatencySample{OriginMetroPK: testPK(1), TargetMetroPK: testPK(2), DataProvider: "RIPE Atlas", Epoch: 100, SampleIndex: 0, Time: now.Add(-1 * time.Hour), RTTMicroseconds: 6000},
	))
	require.NoError(t, err)

	indices, err := writer.GetExistingInternetMaxSampleIndices()
	require.NoError(t, err)
	require.Contains(t, indices, staleKey,
		"the writing process must carry a stale circuit-epoch's max index forward past the scan bound")
	require.Contains(t, indices, fmt.Sprintf("%s:%s:%s:100", testPK(1), testPK(2), "RIPE Atlas"))

	restarted := newCacheTestStore(t, db, "test-maxidx-im-carry-restart")
	indices, err = restarted.GetExistingInternetMaxSampleIndices()
	require.NoError(t, err)
	require.NotContains(t, indices, staleKey,
		"a fresh store must drop keys whose rows are all older than the scan bound")
}
