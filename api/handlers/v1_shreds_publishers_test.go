package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	v1 "github.com/malbeclabs/lake/api/v1"
)

func newV1Router(t *testing.T, api *handlers.API) *chi.Mux {
	t.Helper()
	r := chi.NewRouter()
	v1.Mount(r, api)
	return r
}

// v1EdgeShredsPublishersContractFields is the authoritative list of JSON keys
// in the public /api/v1/edge/shreds/publishers/leaders response. A mismatch means the
// public contract has changed — bump the API version instead of renaming.
var v1EdgeShredsPublishersContractFields = struct {
	top       []string
	publisher []string
}{
	top: []string{
		"$schema", // huma adds this for spec-linkage; part of the v1 contract.
		"epoch",
		"max_slot",
		"total_network_stake",
		"total_publishers",
		"total_publisher_stake",
		"publishers",
		"total",
		"limit",
		"offset",
	},
	publisher: []string{
		"publisher_ip",
		"client_ip",
		"node_pubkey",
		"vote_pubkey",
		"dz_user_pubkey",
		"dz_device_code",
		"dz_metro_code",
		"activated_stake",
		"multicast_connected",
		"publishing_leader_shreds",
		"publishing_retransmitted",
		"leader_slots",
		"total_slots",
		"total_unique_shreds",
		"slots_needing_repair",
		"validator_client",
		"validator_version",
		"validator_name",
		"validator_version_ok",
		"is_backup",
	},
}

func TestV1EdgeShredsPublishers_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	createPublisherShredStatsTable(t, api)

	ctx := t.Context()
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_multicast_groups_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, owner_pubkey, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count)
		VALUES
			('bebop-group', now(), now(), generateUUIDv4(), 0, 1,
			 '31fdXyG3x8k5Ache7jKNQsuwaMf44oqYQndoBsT1JfVj', '', 'bebop', '233.84.178.1', 100000000, 'activated', 0, 0)
	`)
	require.NoError(t, err)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Publishers)
}

// TestV1EdgeShredsPublishers_Contract locks down the public JSON shape of the
// shreds publishers endpoint. If a field is renamed or removed, downstream
// consumers break — bump the API version instead of changing a field.
func TestV1EdgeShredsPublishers_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertPublisherCheckTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assertJSONKeys(t, raw, v1EdgeShredsPublishersContractFields.top, "response")

	publishers, ok := raw["publishers"].([]any)
	require.True(t, ok, "publishers must be a JSON array")
	require.NotEmpty(t, publishers, "test data should produce publishers")
	for i, p := range publishers {
		obj, ok := p.(map[string]any)
		require.True(t, ok, "publishers[%d] must be a JSON object", i)
		assertJSONKeys(t, obj, v1EdgeShredsPublishersContractFields.publisher, "publishers[i]")
	}
}

func TestV1EdgeShredsPublishers_AllPublishers(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertPublisherCheckTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, uint64(800), resp.Epoch)
	require.Len(t, resp.Publishers, 3)

	pub1 := resp.Publishers[0]
	assert.Equal(t, "10.0.0.1", pub1.PublisherIP)
	assert.Equal(t, "dzuser1", pub1.DZUserPubkey)
	assert.True(t, pub1.MulticastConnected)
	assert.True(t, pub1.PublishingLeaderShreds)
	assert.Equal(t, uint64(1), pub1.LeaderSlots)
	assert.Equal(t, uint64(2), pub1.TotalSlots)
	assert.Equal(t, "Validator 1", pub1.ValidatorName)
	assert.Equal(t, "Jito", pub1.ValidatorClient)
	assert.Equal(t, "2.2.3", pub1.ValidatorVersion)
	assert.True(t, pub1.ValidatorVersionOk)
}

func TestV1EdgeShredsPublishers_FilterByDZID(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertPublisherCheckTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders?q=dzuser1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Publishers, 1)
	assert.Equal(t, "dzuser1", resp.Publishers[0].DZUserPubkey)
}

func TestV1EdgeShredsPublishers_FilterByIP(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertPublisherCheckTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders?q=10.0.0.1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Publishers, 1)
	assert.Equal(t, "10.0.0.1", resp.Publishers[0].PublisherIP)
}

func TestV1EdgeShredsPublishers_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertPublisherCheckTestData(t, api)

	r := newV1Router(t, api)

	// Default: returns all 3 publishers, total reflects matched set.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 100, resp.Limit)
	assert.Equal(t, 0, resp.Offset)
	require.Len(t, resp.Publishers, 3)

	// limit=1 returns the first publisher only; total still reflects the full match count.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders?limit=1", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 1, resp.Limit)
	assert.Equal(t, 0, resp.Offset)
	require.Len(t, resp.Publishers, 1)
	first := resp.Publishers[0]

	// offset=1&limit=1 returns the second publisher.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders?limit=1&offset=1", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 1, resp.Offset)
	require.Len(t, resp.Publishers, 1)
	assert.NotEqual(t, first.DZUserPubkey, resp.Publishers[0].DZUserPubkey)

	// offset past the end returns an empty page with total still set.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders?offset=100", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Empty(t, resp.Publishers)

	// Invalid bounds are rejected by huma.
	for _, url := range []string{
		"/api/v1/edge/shreds/publishers/leaders?limit=0",
		"/api/v1/edge/shreds/publishers/leaders?limit=9999",
		"/api/v1/edge/shreds/publishers/leaders?offset=-1",
	} {
		req = httptest.NewRequest(http.MethodGet, url, nil)
		rr = httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		require.Equal(t, http.StatusUnprocessableEntity, rr.Code, "url=%s body=%s", url, rr.Body.String())
	}
}

func TestV1EdgeShredsPublishers_EpochsAndSlotsParams(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertPublisherCheckTestData(t, api)

	cases := []struct {
		name       string
		url        string
		wantStatus int
	}{
		{"default", "/api/v1/edge/shreds/publishers/leaders", http.StatusOK},
		{"epochs=1", "/api/v1/edge/shreds/publishers/leaders?epochs=1", http.StatusOK},
		{"slots=500", "/api/v1/edge/shreds/publishers/leaders?slots=500", http.StatusOK},
		{"slots with epochs (slots takes precedence)", "/api/v1/edge/shreds/publishers/leaders?slots=500&epochs=5", http.StatusOK},
		// huma validates input — invalid values return 422 (part of the v1 error contract).
		{"non-numeric epochs is rejected", "/api/v1/edge/shreds/publishers/leaders?epochs=abc", http.StatusUnprocessableEntity},
		{"out-of-range epochs is rejected", "/api/v1/edge/shreds/publishers/leaders?epochs=99", http.StatusUnprocessableEntity},
		{"out-of-range slots is rejected", "/api/v1/edge/shreds/publishers/leaders?slots=9999", http.StatusUnprocessableEntity},
	}

	r := newV1Router(t, api)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
			require.Equal(t, tc.wantStatus, rr.Code, "body: %s", rr.Body.String())
		})
	}
}

// TestV1EdgeShredsPublishers_ServedFromCache asserts a default-shape request is
// served from the page cache when a cached payload exists. The API has no
// ClickHouse connection, so a cache miss would fail — returning the distinctive
// cached values proves the cache was used (and the heavy live query was skipped).
func TestV1EdgeShredsPublishers_ServedFromCache(t *testing.T) {
	api := apitesting.NewTestAPIPg(t, testPgDB)

	cached := handlers.PublisherCheckResponse{
		Epoch:               4242,
		MaxSlot:             999999,
		TotalNetworkStake:   123456789,
		TotalPublishers:     7,
		TotalPublisherStake: 42,
		Publishers: []handlers.PublisherCheckItem{
			{PublisherIP: "10.9.9.9", DZUserPubkey: "cached-user"},
		},
	}
	require.NoError(t, api.WritePageCache(t.Context(), "publisher_check", cached))

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, uint64(4242), resp.Epoch)
	assert.Equal(t, uint64(7), resp.TotalPublishers)
	assert.EqualValues(t, 42, resp.TotalPublisherStake)
	require.Len(t, resp.Publishers, 1)
	assert.Equal(t, "cached-user", resp.Publishers[0].DZUserPubkey)
}

// TestV1EdgeShredsPublishers_NonDefaultShapeBypassesCache asserts that a
// non-default-shape request (a q filter) runs live and is NOT served the
// default-shape cached payload. Guards against a regression in
// isDefaultPublisherCheckShape serving unfiltered cached data for a filtered
// request on the public endpoint.
func TestV1EdgeShredsPublishers_NonDefaultShapeBypassesCache(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	insertPublisherCheckTestData(t, api)

	// Populate the default-shape cache with a distinctive sentinel epoch.
	require.NoError(t, api.WritePageCache(t.Context(), "publisher_check", handlers.PublisherCheckResponse{
		Epoch:      4242,
		Publishers: []handlers.PublisherCheckItem{{DZUserPubkey: "cached-user"}},
	}))

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/publishers/leaders?q=dzuser1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsPublishersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	// Live query result, not the cached sentinel.
	assert.Equal(t, uint64(800), resp.Epoch)
	require.Len(t, resp.Publishers, 1)
	assert.Equal(t, "dzuser1", resp.Publishers[0].DZUserPubkey)
}
