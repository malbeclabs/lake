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

// v1DZLinkLatencyContractFields locks down the public JSON shape. Renaming or
// removing a field is a breaking change — bump the API version instead.
var v1DZLinkLatencyContractFields = struct {
	top    []string
	link   []string
	bucket []string
}{
	top: []string{
		"$schema",
		"time_range",
		"bucket_seconds",
		"bucket_count",
		"total_links",
		"link_limit",
		"link_offset",
		"links",
	},
	link: []string{
		"link_pk",
		"link_code",
		"link_type",
		"contributor_code",
		"side_z_contributor_code",
		"side_a_device",
		"side_z_device",
		"side_a_metro",
		"side_z_metro",
		"committed_rtt_us",
		"committed_jitter_us",
		"buckets",
	},
	bucket: []string{
		"ts",
		"a_samples", "a_loss_pct",
		"a_avg_rtt_us", "a_min_rtt_us", "a_p50_rtt_us", "a_p90_rtt_us", "a_p95_rtt_us", "a_p99_rtt_us", "a_max_rtt_us",
		"a_avg_jitter_us", "a_min_jitter_us", "a_p50_jitter_us", "a_p90_jitter_us", "a_p95_jitter_us", "a_p99_jitter_us", "a_max_jitter_us",
		"z_samples", "z_loss_pct",
		"z_avg_rtt_us", "z_min_rtt_us", "z_p50_rtt_us", "z_p90_rtt_us", "z_p95_rtt_us", "z_p99_rtt_us", "z_max_rtt_us",
		"z_avg_jitter_us", "z_min_jitter_us", "z_p50_jitter_us", "z_p90_jitter_us", "z_p95_jitter_us", "z_p99_jitter_us", "z_max_jitter_us",
	},
}

// seedTwoLinksWithLatency seeds two contributor/metro pairs with one link each
// and a single recent rollup bucket per link, returning the truncated "now"
// used for bucket alignment.
func seedTwoLinksWithLatency(t *testing.T, api *handlers.API) time.Time {
	t.Helper()
	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-lax", "LAX")
	seedMetro(t, api, "metro-fra", "FRA")
	seedContributor(t, api, "contrib-acme", "acme")
	seedContributor(t, api, "contrib-zenith", "zenith")
	seedDeviceMetadata(t, api, "dev-nyc-acme", "DEV-NYC-ACME", "router", "contrib-acme", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-lax-acme", "DEV-LAX-ACME", "router", "contrib-acme", "metro-lax", 10, "activated")
	seedDeviceMetadata(t, api, "dev-fra-zen", "DEV-FRA-ZEN", "router", "contrib-zenith", "metro-fra", 10, "activated")

	seedLinkMetadata(t, api, "link-1", "NYC-LAX-1", "WAN", "contrib-acme", "dev-nyc-acme", "dev-lax-acme", 10_000_000_000, 500_000, "activated")
	seedLinkMetadata(t, api, "link-2", "NYC-FRA-1", "WAN", "contrib-zenith", "dev-nyc-acme", "dev-fra-zen", 10_000_000_000, 700_000, "activated")

	now := time.Now().UTC().Truncate(time.Hour)
	seedLinkRollup(t, api, now.Add(-time.Hour), "link-1", 50_000, 51_000, 0.5, 0.25, 3600, 3600, "activated", false, false)
	seedLinkRollup(t, api, now.Add(-time.Hour), "link-2", 70_000, 71_000, 0, 0, 3600, 3600, "activated", false, false)
	return now
}

func TestV1DZLinkLatency_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assertJSONKeys(t, raw, v1DZLinkLatencyContractFields.top, "response")

	links, ok := raw["links"].([]any)
	require.True(t, ok, "links must be a JSON array")
	require.NotEmpty(t, links, "response should include links")
	for i, l := range links {
		obj, ok := l.(map[string]any)
		require.True(t, ok, "links[%d] must be a JSON object", i)
		assertJSONKeys(t, obj, v1DZLinkLatencyContractFields.link, "links[i]")
		buckets, ok := obj["buckets"].([]any)
		require.True(t, ok, "links[%d].buckets must be a JSON array", i)
		require.NotEmpty(t, buckets, "links[%d] should have at least one bucket", i)
		for j, b := range buckets {
			bobj, ok := b.(map[string]any)
			require.True(t, ok, "links[%d].buckets[%d] must be a JSON object", i, j)
			assertJSONKeys(t, bobj, v1DZLinkLatencyContractFields.bucket, "links[i].buckets[j]")
		}
	}
}

func TestV1DZLinkLatency_AllLinks(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	assert.Equal(t, 2, resp.TotalLinks)
	assert.Equal(t, 100, resp.LinkLimit)
	assert.Equal(t, 0, resp.LinkOffset)
	require.Len(t, resp.Links, 2)
	// Sorted by link_code → NYC-FRA-1 before NYC-LAX-1.
	assert.Equal(t, "NYC-FRA-1", resp.Links[0].LinkCode)
	assert.Equal(t, "NYC-LAX-1", resp.Links[1].LinkCode)

	laxLink := resp.Links[1]
	assert.Equal(t, "link-1", laxLink.LinkPK)
	assert.Equal(t, "acme", laxLink.ContributorCode)
	assert.Equal(t, "WAN", laxLink.LinkType)
	assert.Equal(t, "DEV-NYC-ACME", laxLink.SideADevice)
	assert.Equal(t, "DEV-LAX-ACME", laxLink.SideZDevice)
	assert.Equal(t, "NYC", laxLink.SideAMetro)
	assert.Equal(t, "LAX", laxLink.SideZMetro)
	assert.InDelta(t, 500.0, laxLink.CommittedRttUs, 0.01)
	require.NotEmpty(t, laxLink.Buckets)

	// Find populated bucket and verify the seeded values.
	var populated *v1.DZLinkLatencyBucket
	for i := range laxLink.Buckets {
		if laxLink.Buckets[i].ASamples > 0 {
			populated = &laxLink.Buckets[i]
			break
		}
	}
	require.NotNil(t, populated)
	assert.InDelta(t, 50_000.0, populated.AAvgRttUs, 0.01)
	assert.InDelta(t, 51_000.0, populated.ZAvgRttUs, 0.01)
	assert.InDelta(t, 0.5, populated.ALossPct, 0.01)
}

func TestV1DZLinkLatency_FilterByLinkPK(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?link_pk=link-2", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Links, 1)
	assert.Equal(t, "link-2", resp.Links[0].LinkPK)
}

func TestV1DZLinkLatency_FilterByLinkCode(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	// Case-insensitive exact match.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?link_code=nyc-lax-1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Links, 1)
	assert.Equal(t, "NYC-LAX-1", resp.Links[0].LinkCode)
}

func TestV1DZLinkLatency_FilterByContributor(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?contributor_code=zenith", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Links, 1)
	assert.Equal(t, "link-2", resp.Links[0].LinkPK)
}

func TestV1DZLinkLatency_FilterByMetro(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	// FRA only appears on link-2 (NYC↔FRA); should not match link-1 (NYC↔LAX).
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?metro_code=FRA", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Links, 1)
	assert.Equal(t, "link-2", resp.Links[0].LinkPK)
}

func TestV1DZLinkLatency_FilterMultipleValues(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	// Repeated query params OR within a filter.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?metro_code=LAX&metro_code=FRA", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Len(t, resp.Links, 2)
}

func TestV1DZLinkLatency_FilterNoMatches(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?metro_code=does-not-exist", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 0, resp.TotalLinks)
	assert.Empty(t, resp.Links)
}

func TestV1DZLinkLatency_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoLinksWithLatency(t, api)

	r := newV1Router(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?link_limit=1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.DZLinkLatencyResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.TotalLinks)
	assert.Equal(t, 1, resp.LinkLimit)
	require.Len(t, resp.Links, 1)
	first := resp.Links[0].LinkCode

	req = httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?link_limit=1&link_offset=1", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Links, 1)
	assert.NotEqual(t, first, resp.Links[0].LinkCode)

	// Offset past total returns empty page; total still reflects full match count.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?link_offset=10", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.TotalLinks)
	assert.Empty(t, resp.Links)
}

func TestV1DZLinkLatency_InvalidParams(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)

	for _, url := range []string{
		"/api/v1/dz/links/latency?range=99h",
		"/api/v1/dz/links/latency?bucket=2s",
		"/api/v1/dz/links/latency?start_time=-1",
		"/api/v1/dz/links/latency?link_limit=0",
		"/api/v1/dz/links/latency?link_limit=9999",
		"/api/v1/dz/links/latency?link_offset=-1",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}
}

func TestV1DZLinkLatency_RawBucketWindowCap(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)

	// Reject: raw-mode bucket combined with too-wide a window.
	for _, url := range []string{
		"/api/v1/dz/links/latency?bucket=10s&range=24h",
		"/api/v1/dz/links/latency?bucket=30s&range=24h",
		"/api/v1/dz/links/latency?bucket=1m&range=24h",
		// Custom window: 10s bucket over 6h is too wide.
		"/api/v1/dz/links/latency?bucket=10s&start_time=1000000000&end_time=1000021600",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}

	// Accept: raw-mode bucket inside its allowed window. No data is fine —
	// the response is just an empty link list.
	for _, url := range []string{
		"/api/v1/dz/links/latency?bucket=10s&range=1h",
		"/api/v1/dz/links/latency?bucket=30s&range=3h",
		"/api/v1/dz/links/latency?bucket=1m&range=6h",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}
}

func TestV1DZLinkLatency_InvertedCustomWindow(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dz/links/latency?start_time=2000&end_time=1000", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "body: %s", rr.Body.String())
}
