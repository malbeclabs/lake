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

// v1EdgeShredsOverviewContractFields is the authoritative JSON key set for the
// /api/v1/edge/shreds/overview response. A mismatch means the public contract
// has changed — bump the API version.
var v1EdgeShredsOverviewContractFields = []string{
	"$schema",
	"phase",
	"current_subscription_epoch",
	"current_solana_epoch",
	"total_metros",
	"total_enabled_devices",
	"total_client_seats",
	"settled_devices_count",
	"settled_client_seats_count",
	"next_seat_funding_index",
	"client_seat_count",
	"payment_escrow_count",
	"metro_history_count",
	"device_history_count",
	"validator_client_reward_count",
}

func TestV1EdgeShredsOverview_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/overview", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var resp v1.EdgeShredsOverview
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "", resp.Phase)
	assert.Equal(t, uint64(0), resp.CurrentSubscriptionEpoch)
	assert.Equal(t, uint64(0), resp.CurrentSolanaEpoch)
	assert.Equal(t, uint64(0), resp.ClientSeatCount)
}

func TestV1EdgeShredsOverview_Contract(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/overview", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	assertJSONKeys(t, raw, v1EdgeShredsOverviewContractFields, "response")
}

func TestV1EdgeShredsOverview_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	r := newV1Router(t, api)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/edge/shreds/overview", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp v1.EdgeShredsOverview
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Mirror values from insertShredsTestData.
	assert.Equal(t, "open for requests", resp.Phase)
	assert.Equal(t, uint64(950), resp.CurrentSubscriptionEpoch)
	assert.Equal(t, uint64(950), resp.CurrentSolanaEpoch)
	assert.Equal(t, uint64(3), resp.ClientSeatCount)
	assert.Equal(t, uint64(1), resp.PaymentEscrowCount)
	assert.Equal(t, uint64(1), resp.MetroHistoryCount)
	assert.Equal(t, uint64(1), resp.DeviceHistoryCount)
}
