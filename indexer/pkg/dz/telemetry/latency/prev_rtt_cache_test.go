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

// TestLake_TelemetryLatency_Store_GetExistingMaxSampleIndices_TimeBound proves
// the maxSampleIndexLookback filter: keys whose samples are all older than 4
// days are dropped (the caller then refetches the epoch tail; dedup makes that
// safe), while recent keys are returned.
func TestLake_TelemetryLatency_Store_GetExistingMaxSampleIndices_TimeBound(t *testing.T) {
	t.Parallel()

	db := testClient(t)
	store := newCacheTestStore(t, db, "test-maxidx-dl-bound")
	now := time.Now()

	err := store.AppendDeviceLinkLatencySamples(context.Background(), []DeviceLinkLatencySample{
		// Old epoch: all samples beyond the 4-day lookback.
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 90, SampleIndex: 0, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5000},
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 90, SampleIndex: 1, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5100},
		// Recent epoch on the same circuit.
		{OriginDevicePK: testPK(1), TargetDevicePK: testPK(2), LinkPK: testPK(3), Epoch: 100, SampleIndex: 0, Time: now.Add(-1 * time.Hour), RTTMicroseconds: 6000},
	})
	require.NoError(t, err)

	indices, err := store.GetExistingMaxSampleIndices()
	require.NoError(t, err)
	require.Len(t, indices, 1, "epoch older than the lookback must be dropped")
	require.Equal(t, 0, indices[fmt.Sprintf("%s:%s:%s:100", testPK(1), testPK(2), testPK(3))])
}

// TestLake_TelemetryLatency_Store_GetExistingInternetMaxSampleIndices_TimeBound
// mirrors the device-link time-bound test for the internet-metro table.
func TestLake_TelemetryLatency_Store_GetExistingInternetMaxSampleIndices_TimeBound(t *testing.T) {
	t.Parallel()

	db := testClient(t)
	store := newCacheTestStore(t, db, "test-maxidx-im-bound")
	now := time.Now()

	err := store.AppendInternetMetroLatencySamples(context.Background(), []InternetMetroLatencySample{
		{OriginMetroPK: testPK(1), TargetMetroPK: testPK(2), DataProvider: "RIPE Atlas", Epoch: 90, SampleIndex: 0, Time: now.Add(-5 * 24 * time.Hour), RTTMicroseconds: 5000},
		{OriginMetroPK: testPK(1), TargetMetroPK: testPK(2), DataProvider: "RIPE Atlas", Epoch: 100, SampleIndex: 0, Time: now.Add(-1 * time.Hour), RTTMicroseconds: 6000},
	})
	require.NoError(t, err)

	indices, err := store.GetExistingInternetMaxSampleIndices()
	require.NoError(t, err)
	require.Len(t, indices, 1, "epoch older than the lookback must be dropped")
	require.Equal(t, 0, indices[fmt.Sprintf("%s:%s:%s:100", testPK(1), testPK(2), "RIPE Atlas")])
}
