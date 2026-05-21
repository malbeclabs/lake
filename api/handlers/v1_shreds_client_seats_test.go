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

// v1EdgeShredsClientSeatsContractFields is the authoritative JSON key set for
// the /api/v1/edge/shreds/client-seats response. A mismatch means the public
// contract has changed — bump the API version.
var v1EdgeShredsClientSeatsContractFields = struct {
	top  []string
	seat []string
}{
	top: []string{"items", "total", "limit", "offset", "$schema"},
	seat: []string{
		"seat_pk",
		"device_key",
		"device_code",
		"metro_pk",
		"metro_code",
		"tenure_epochs",
		"funded_epoch",
		"active_epoch",
		"has_price_override",
		"override_usdc_price_dollars",
		"escrow_count",
		"total_usdc_balance",
		"price_per_epoch_dollars",
		"funding_authority_key",
		"user_pk",
		"user_owner_pubkey",
		"user_status",
		"last_activity",
	},
}

func TestV1EdgeShredsClientSeats_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var resp v1.EdgeShredsClientSeatsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Items)
	assert.Equal(t, 0, resp.Total)
}

func TestV1EdgeShredsClientSeats_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assertJSONKeys(t, raw, v1EdgeShredsClientSeatsContractFields.top, "response")

	items, ok := raw["items"].([]any)
	require.True(t, ok, "items must be a JSON array")
	require.NotEmpty(t, items, "test data should produce client seats")
	for i, it := range items {
		obj, ok := it.(map[string]any)
		require.True(t, ok, "items[%d] must be a JSON object", i)
		assertJSONKeys(t, obj, v1EdgeShredsClientSeatsContractFields.seat, "items[i]")

		// client_ip must NOT be exposed in the v1 contract — v1 is unauthed.
		_, hasIP := obj["client_ip"]
		assert.False(t, hasIP, "items[%d]: client_ip must not be exposed in v1", i)
	}
}

func TestV1EdgeShredsClientSeats_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsClientSeatsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	require.Len(t, resp.Items, 3)

	// Inherits sort order from FetchShredSubscribers: active_epoch DESC, pk ASC.
	// seat-1 (epoch 950), seat-2 (epoch 950), seat-3 (epoch 945).
	assert.Equal(t, "seat-1", resp.Items[0].SeatPK)
	assert.Equal(t, uint64(950), resp.Items[0].ActiveEpoch)
	assert.Equal(t, uint64(948), resp.Items[0].FundedEpoch)
	assert.Equal(t, uint32(1), resp.Items[0].EscrowCount)
	assert.Equal(t, uint8(0), resp.Items[0].HasPriceOverride)
	assert.Equal(t, "50.000000", resp.Items[0].TotalUSDCBalance)

	// seat-2 has the price override.
	seat2 := resp.Items[1]
	assert.Equal(t, "seat-2", seat2.SeatPK)
	assert.Equal(t, uint8(1), seat2.HasPriceOverride)
	assert.Equal(t, uint16(25), seat2.OverrideUSDCPriceDollars)
	assert.Equal(t, int64(25), seat2.PricePerEpochDollars)
	assert.Equal(t, uint32(0), seat2.EscrowCount)
}

func TestV1EdgeShredsClientSeats_FilterByFunder(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats?funder=funder-1", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsClientSeatsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.Total, "funder-1 owns 2 seats")
	require.Len(t, resp.Items, 2)
	for _, s := range resp.Items {
		assert.Equal(t, "funder-1", s.FundingAuthorityKey)
	}
	assert.ElementsMatch(t, []string{"seat-1", "seat-3"}, []string{resp.Items[0].SeatPK, resp.Items[1].SeatPK})
}

func TestV1EdgeShredsClientSeats_FilterByFunder_ExactMatchNotSubstring(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	// "funder" is a substring of "funder-1"/"funder-2" but must NOT match —
	// the v1 funder filter is exact match. Guards against regressions where
	// someone swaps to ILIKE / startsWith.
	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats?funder=funder", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsClientSeatsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 0, resp.Total)
	assert.Empty(t, resp.Items)
}

func TestV1EdgeShredsClientSeats_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsClientSeatsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Limit)
	assert.Equal(t, 0, resp.Offset)
	require.Len(t, resp.Items, 2)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/client-seats?limit=2&offset=2", nil)
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 3, resp.Total)
	assert.Equal(t, 2, resp.Offset)
	require.Len(t, resp.Items, 1)
}
