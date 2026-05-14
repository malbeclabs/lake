package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	v1 "github.com/malbeclabs/lake/api/v1"
)

// v1DZMetroPairLatencyContractFields locks down the public JSON shape.
// Renaming or removing a field is a breaking change — bump the API version.
var v1DZMetroPairLatencyContractFields = struct {
	top    []string
	pair   []string
	bucket []string
}{
	top: []string{
		"$schema",
		"time_range",
		"bucket_seconds",
		"bucket_count",
		"total_pairs",
		"pair_limit",
		"pair_offset",
		"pairs",
	},
	pair: []string{
		"metro_a_code",
		"metro_a_name",
		"metro_b_code",
		"metro_b_name",
		"buckets",
	},
	bucket: []string{
		"ts",
		"dz_samples", "dz_loss_pct",
		"dz_avg_rtt_us", "dz_min_rtt_us", "dz_p50_rtt_us", "dz_p90_rtt_us", "dz_p95_rtt_us", "dz_p99_rtt_us", "dz_max_rtt_us",
		"dz_avg_jitter_us", "dz_min_jitter_us", "dz_p50_jitter_us", "dz_p90_jitter_us", "dz_p95_jitter_us", "dz_p99_jitter_us", "dz_max_jitter_us",
		"internet_samples",
		"internet_avg_rtt_us", "internet_min_rtt_us", "internet_p50_rtt_us", "internet_p90_rtt_us", "internet_p95_rtt_us", "internet_p99_rtt_us", "internet_max_rtt_us",
		"internet_avg_jitter_us", "internet_min_jitter_us", "internet_p50_jitter_us", "internet_p90_jitter_us", "internet_p95_jitter_us", "internet_p99_jitter_us", "internet_max_jitter_us",
		"avg_rtt_improvement_pct", "avg_jitter_improvement_pct",
	},
}

// seedInternetMetroLatency inserts a single sample into fact_dz_internet_metro_latency.
func seedInternetMetroLatency(t *testing.T, api *handlers.API, eventTS time.Time, originMetroPK, targetMetroPK, dataProvider string, rttUs, ipdvUs int64, sampleIndex int32) {
	t.Helper()
	err := api.DB.Exec(t.Context(), `INSERT INTO fact_dz_internet_metro_latency (
		event_ts, ingested_at, epoch, sample_index,
		origin_metro_pk, target_metro_pk, data_provider, rtt_us, ipdv_us
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		eventTS, time.Now(), int64(1), sampleIndex,
		originMetroPK, targetMetroPK, dataProvider, rttUs, ipdvUs,
	)
	require.NoError(t, err)
}

// seedDzVsInternetFixture sets up two metro pairs (NYC↔LAX with DZ+internet,
// NYC↔FRA with DZ-only). Returns the truncated "now" used for bucket alignment.
func seedDzVsInternetFixture(t *testing.T, api *handlers.API) time.Time {
	t.Helper()
	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-lax", "LAX")
	seedMetro(t, api, "metro-fra", "FRA")
	seedContributor(t, api, "contrib-acme", "acme")
	seedDeviceMetadata(t, api, "dev-nyc", "DEV-NYC", "router", "contrib-acme", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-lax", "DEV-LAX", "router", "contrib-acme", "metro-lax", 10, "activated")
	seedDeviceMetadata(t, api, "dev-fra", "DEV-FRA", "router", "contrib-acme", "metro-fra", 10, "activated")
	seedLinkMetadata(t, api, "link-nyc-lax", "NYC-LAX-1", "WAN", "contrib-acme", "dev-nyc", "dev-lax", 10_000_000_000, 500_000, "activated")
	seedLinkMetadata(t, api, "link-nyc-fra", "NYC-FRA-1", "WAN", "contrib-acme", "dev-nyc", "dev-fra", 10_000_000_000, 700_000, "activated")

	now := time.Now().UTC().Truncate(time.Hour)

	// DZ samples for both pairs in the most recent rollup bucket.
	seedLinkRollup(t, api, now.Add(-time.Hour), "link-nyc-lax", 50_000, 51_000, 0.5, 0.25, 3600, 3600, "activated", false, false)
	seedLinkRollup(t, api, now.Add(-time.Hour), "link-nyc-fra", 70_000, 71_000, 0, 0, 3600, 3600, "activated", false, false)

	// Internet samples for NYC↔LAX. Seed at the same wall-clock time as the
	// DZ rollup so both align into a single 10m bucket. Multiple providers so
	// we can test data_provider filtering.
	for i := int32(0); i < 4; i++ {
		seedInternetMetroLatency(t, api, now.Add(-time.Hour), "metro-nyc", "metro-lax", "ripe-atlas", 80_000, 1_500, i)
	}
	for i := int32(0); i < 4; i++ {
		seedInternetMetroLatency(t, api, now.Add(-time.Hour), "metro-nyc", "metro-lax", "wheresitup", 90_000, 2_000, i+10)
	}
	// Internet-only samples for LAX↔FRA — there is no DZ link between these
	// metros, so this pair must NOT appear in the response (DZ-coverage filter).
	for i := int32(0); i < 4; i++ {
		seedInternetMetroLatency(t, api, now.Add(-time.Hour), "metro-lax", "metro-fra", "ripe-atlas", 120_000, 3_000, i+20)
	}
	return now
}

func TestV1DZMetroPairLatency_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assertJSONKeys(t, raw, v1DZMetroPairLatencyContractFields.top, "response")

	pairs, ok := raw["pairs"].([]any)
	require.True(t, ok, "pairs must be a JSON array")
	require.NotEmpty(t, pairs, "response should include pairs")
	for i, p := range pairs {
		obj, ok := p.(map[string]any)
		require.True(t, ok, "pairs[%d] must be a JSON object", i)
		assertJSONKeys(t, obj, v1DZMetroPairLatencyContractFields.pair, "pairs[i]")
		buckets, ok := obj["buckets"].([]any)
		require.True(t, ok, "pairs[%d].buckets must be a JSON array", i)
		require.NotEmpty(t, buckets, "pairs[%d] should have at least one bucket", i)
		for j, b := range buckets {
			bobj, ok := b.(map[string]any)
			require.True(t, ok, "pairs[%d].buckets[%d] must be a JSON object", i, j)
			assertJSONKeys(t, bobj, v1DZMetroPairLatencyContractFields.bucket, "pairs[i].buckets[j]")
		}
	}
}

func TestV1DZMetroPairLatency_AllPairs(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZMetroPairLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Two pairs in fixture: FRA-NYC (DZ-only), LAX-NYC (DZ+internet).
	// Sorted by (metro_a_code, metro_b_code) — normalized so FRA<NYC, LAX<NYC.
	assert.Equal(t, 2, resp.TotalPairs)
	require.Len(t, resp.Pairs, 2)
	assert.Equal(t, "FRA", resp.Pairs[0].MetroACode)
	assert.Equal(t, "NYC", resp.Pairs[0].MetroBCode)
	assert.Equal(t, "LAX", resp.Pairs[1].MetroACode)
	assert.Equal(t, "NYC", resp.Pairs[1].MetroBCode)

	// LAX-NYC should have both DZ and internet samples in some bucket.
	laxNyc := resp.Pairs[1]
	var withBoth *v1.DZMetroPairLatencyBucket
	for i := range laxNyc.Buckets {
		if laxNyc.Buckets[i].DZSamples > 0 && laxNyc.Buckets[i].InternetSamples > 0 {
			withBoth = &laxNyc.Buckets[i]
			break
		}
	}
	require.NotNil(t, withBoth, "expected a bucket with both DZ and internet samples")
	// 3600 a_samples + 3600 z_samples on link-nyc-lax.
	assert.Equal(t, uint64(7200), withBoth.DZSamples)
	// 4 ripe-atlas + 4 wheresitup.
	assert.Equal(t, uint64(8), withBoth.InternetSamples)
	// Sample-weighted average across both directions: (50000*3600 + 51000*3600)/7200 = 50500.
	assert.InDelta(t, 50_500.0, withBoth.DZAvgRttUs, 1.0)
	// Internet RTT is a mix of 80_000 and 90_000 with equal weight → 85_000 avg.
	assert.InDelta(t, 85_000.0, withBoth.InternetAvgRttUs, 1.0)
	// Improvement: (85_000 - 50_500) / 85_000 * 100 ≈ 40.59%.
	assert.InDelta(t, 40.588, withBoth.AvgRttImprovementPct, 0.05)

	// FRA-NYC is DZ-only: at least one bucket has DZ samples but no internet.
	fraNyc := resp.Pairs[0]
	var dzOnly *v1.DZMetroPairLatencyBucket
	for i := range fraNyc.Buckets {
		if fraNyc.Buckets[i].DZSamples > 0 {
			dzOnly = &fraNyc.Buckets[i]
			break
		}
	}
	require.NotNil(t, dzOnly, "expected a DZ-populated bucket on FRA-NYC")
	assert.Equal(t, uint64(0), dzOnly.InternetSamples)
	// No internet samples in this bucket → improvement is 0 (indeterminate).
	assert.Equal(t, 0.0, dzOnly.AvgRttImprovementPct)

	// LAX-FRA has internet samples but no DZ link — must be excluded.
	for _, p := range resp.Pairs {
		assert.False(t, p.MetroACode == "FRA" && p.MetroBCode == "LAX",
			"internet-only pair FRA-LAX must not appear in the response")
	}
}

func TestV1DZMetroPairLatency_FilterByMetro(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)
	// FRA should only match the NYC↔FRA pair.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?metro_code=FRA", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZMetroPairLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Pairs, 1)
	assert.Equal(t, "FRA", resp.Pairs[0].MetroACode)
	assert.Equal(t, "NYC", resp.Pairs[0].MetroBCode)
}

func TestV1DZMetroPairLatency_FilterMultipleMetros(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?metro_code=LAX&metro_code=FRA", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZMetroPairLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Len(t, resp.Pairs, 2)
}

func TestV1DZMetroPairLatency_FilterByDataProvider(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)
	// data_provider only narrows the internet side. DZ-only pairs (NYC↔FRA)
	// should still appear because the DZ side is unaffected.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?data_provider=ripe-atlas", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZMetroPairLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Equal(t, 2, resp.TotalPairs)

	// LAX-NYC should now only have 4 ripe-atlas internet samples (not the
	// 4 wheresitup ones). Internet avg = 80_000 us.
	var laxNyc *v1.DZMetroPairLatency
	for i := range resp.Pairs {
		if resp.Pairs[i].MetroACode == "LAX" && resp.Pairs[i].MetroBCode == "NYC" {
			laxNyc = &resp.Pairs[i]
			break
		}
	}
	require.NotNil(t, laxNyc)
	var inetBucket *v1.DZMetroPairLatencyBucket
	for i := range laxNyc.Buckets {
		if laxNyc.Buckets[i].InternetSamples > 0 {
			inetBucket = &laxNyc.Buckets[i]
			break
		}
	}
	require.NotNil(t, inetBucket)
	assert.Equal(t, uint64(4), inetBucket.InternetSamples)
	assert.InDelta(t, 80_000.0, inetBucket.InternetAvgRttUs, 1.0)
}

func TestV1DZMetroPairLatency_FilterNoMatches(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?metro_code=does-not-exist", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZMetroPairLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 0, resp.TotalPairs)
	assert.Empty(t, resp.Pairs)
}

func TestV1DZMetroPairLatency_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedDzVsInternetFixture(t, api)

	r := newV1Router(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?pair_limit=1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZMetroPairLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.TotalPairs)
	require.Len(t, resp.Pairs, 1)
	first := resp.Pairs[0].MetroACode + "-" + resp.Pairs[0].MetroBCode

	req = httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?pair_limit=1&pair_offset=1", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Pairs, 1)
	second := resp.Pairs[0].MetroACode + "-" + resp.Pairs[0].MetroBCode
	assert.NotEqual(t, first, second)

	// Offset past total: empty page, total unchanged.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?pair_offset=10", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.TotalPairs)
	assert.Empty(t, resp.Pairs)
}

func TestV1DZMetroPairLatency_InvalidParams(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)

	for _, url := range []string{
		"/api/v1/dz/metro-pairs/latency?range=99h",
		"/api/v1/dz/metro-pairs/latency?bucket=2s",
		"/api/v1/dz/metro-pairs/latency?start_time=-1",
		"/api/v1/dz/metro-pairs/latency?pair_limit=0",
		"/api/v1/dz/metro-pairs/latency?pair_limit=9999",
		"/api/v1/dz/metro-pairs/latency?pair_offset=-1",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}
}

func TestV1DZMetroPairLatency_RawBucketWindowCap(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)

	for _, url := range []string{
		"/api/v1/dz/metro-pairs/latency?bucket=10s&range=24h",
		"/api/v1/dz/metro-pairs/latency?bucket=30s&range=24h",
		"/api/v1/dz/metro-pairs/latency?bucket=1m&range=24h",
		"/api/v1/dz/metro-pairs/latency?bucket=10s&start_time=1000000000&end_time=1000021600",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}

	// Within-cap raw windows are accepted (empty result is fine).
	for _, url := range []string{
		"/api/v1/dz/metro-pairs/latency?bucket=10s&range=1h",
		"/api/v1/dz/metro-pairs/latency?bucket=30s&range=3h",
		"/api/v1/dz/metro-pairs/latency?bucket=1m&range=6h",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}
}

func TestV1DZMetroPairLatency_InvertedCustomWindow(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/metro-pairs/latency?start_time=2000&end_time=1000", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "body: %s", rr.Body.String())
}
