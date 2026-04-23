package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apitesting "github.com/malbeclabs/lake/api/testing"
	v1 "github.com/malbeclabs/lake/api/v1"
)

// v1ShredsSubscribersContractFields is the authoritative JSON keys for the
// /api/v1/edge/shreds/subscribers response. A mismatch means the public contract
// has changed — bump the API version.
var v1ShredsSubscribersContractFields = struct {
	top        []string
	subscriber []string
}{
	top: []string{"items", "total", "limit", "offset", "$schema"},
	subscriber: []string{
		"seat_pk",
		"device_key",
		"device_code",
		"metro_pk",
		"metro_code",
		"tenure_epochs",
		"active_epoch",
		"total_usdc_balance",
		"price_per_epoch_dollars",
		"funding_authority_key",
		"user_pk",
		"user_owner_pubkey",
		"user_status",
		"last_activity",
	},
}

func TestV1ShredsSubscribers_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var resp v1.ShredsSubscribersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Items)
	assert.Equal(t, 0, resp.Total)
}

func TestV1ShredsSubscribers_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	// $schema is added by huma for spec-linkage; acceptable in the public contract.
	assertJSONKeys(t, raw, v1ShredsSubscribersContractFields.top, "response")

	items, ok := raw["items"].([]any)
	require.True(t, ok, "items must be a JSON array")
	require.NotEmpty(t, items, "test data should produce subscribers")
	for i, it := range items {
		obj, ok := it.(map[string]any)
		require.True(t, ok, "items[%d] must be a JSON object", i)
		assertJSONKeys(t, obj, v1ShredsSubscribersContractFields.subscriber, "items[i]")

		// client_ip must NOT be exposed in the v1 contract.
		_, hasIP := obj["client_ip"]
		assert.False(t, hasIP, "items[%d]: client_ip must not be exposed in v1", i)
	}
}

func TestV1ShredsSubscribers_AllSubscribers(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.ShredsSubscribersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	require.Len(t, resp.Items, 3)

	// Ordered by active_epoch DESC, pk ASC.
	assert.Equal(t, "seat-1", resp.Items[0].SeatPK)
	assert.Equal(t, uint64(950), resp.Items[0].ActiveEpoch)
	assert.Equal(t, "funder-1", resp.Items[0].FundingAuthorityKey)
	assert.Equal(t, "dev-1", resp.Items[0].DeviceKey)
	assert.Equal(t, "NYC-CORE-01", resp.Items[0].DeviceCode)
	assert.Equal(t, "metro-nyc", resp.Items[0].MetroPK)
	assert.Equal(t, "NYC", resp.Items[0].MetroCode)
	assert.Equal(t, "50.000000", resp.Items[0].TotalUSDCBalance)

	assert.Equal(t, "seat-2", resp.Items[1].SeatPK)
	assert.Equal(t, "seat-3", resp.Items[2].SeatPK)
}

func TestV1ShredsSubscribers_FilterByFunder(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers?funder=funder-1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.ShredsSubscribersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.Total, "funder-1 owns 2 seats")
	require.Len(t, resp.Items, 2)
	for _, s := range resp.Items {
		assert.Equal(t, "funder-1", s.FundingAuthorityKey)
	}
	assert.ElementsMatch(t, []string{"seat-1", "seat-3"}, []string{resp.Items[0].SeatPK, resp.Items[1].SeatPK})
}

func TestV1ShredsSubscribers_FilterByFunder_ExactMatchNotSubstring(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	// "funder" is a substring of "funder-1"/"funder-2" but must NOT match
	// because the v1 filter is exact match. Guards against regressions
	// where someone swaps to ILIKE/startsWith.
	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers?funder=funder", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.ShredsSubscribersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 0, resp.Total)
	assert.Empty(t, resp.Items)
}

func TestV1ShredsSubscribers_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.ShredsSubscribersResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Limit)
	assert.Equal(t, 0, resp.Offset)
	require.Len(t, resp.Items, 2)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/subscribers?limit=2&offset=2", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Offset)
	require.Len(t, resp.Items, 1)
}
