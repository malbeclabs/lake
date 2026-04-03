package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertShredsTestData(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	// Insert execution controller
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_execution_controller_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, phase, current_subscription_epoch, total_metros, total_enabled_devices,
		 total_client_seats, updated_device_prices_count, settled_devices_count,
		 settled_client_seats_count, last_settled_slot, last_updating_prices_slot,
		 last_open_for_requests_slot, last_closed_for_requests_slot, next_seat_funding_index)
		VALUES
		('ec-1', now(), now(), generateUUIDv4(), 0, 1,
		 'singleton', 'open for requests', 950, 2, 3,
		 5, 0, 0, 0, 100, 200, 300, 400, 10)
	`)
	require.NoError(t, err)

	// Insert serviceability devices and metros for joins
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, status, device_type, contributor_pk, metro_pk, public_ip, max_users)
		VALUES
		('dev-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-1', 'NYC-CORE-01', 'up', 'router', '', 'metro-nyc', '10.0.0.1', 100)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, name, latitude, longitude)
		VALUES
		('metro-nyc', now(), now(), generateUUIDv4(), 0, 1, 'metro-nyc', 'NYC', 'New York', 40.7, -74.0)
	`)
	require.NoError(t, err)

	// Insert users for join
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, device_pk, kind, owner_pubkey, client_ip, dz_ip, tunnel_id)
		VALUES
		('user-1', now(), now(), generateUUIDv4(), 0, 1,
		 'user-1', 'activated', 'dev-1', 'validator', 'owner-pubkey-1', '192.168.1.1', '10.0.0.1', 0)
	`)
	require.NoError(t, err)

	// Insert solana vote accounts for epoch
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		('vote-1', now(), now(), generateUUIDv4(), 0, 1,
		 'vote-1', 950, 'node-1', 1000000000, 'true', 0)
	`)
	require.NoError(t, err)

	// Insert shred client seats
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key)
		VALUES
		('seat-1', now(), now(), generateUUIDv4(), 0, 1,
		 'seat-1', 'dev-1', '192.168.1.1', 3, 948, 950,
		 0, 0, 1, 'funder-1'),
		('seat-2', now(), now(), generateUUIDv4(), 0, 2,
		 'seat-2', 'dev-1', '192.168.1.2', 1, 950, 950,
		 1, 25, 0, 'funder-2')
	`)
	require.NoError(t, err)

	// Insert payment escrows
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_payment_escrows_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, client_seat_key, withdraw_authority_key, usdc_balance)
		VALUES
		('escrow-1', now(), now(), generateUUIDv4(), 0, 1,
		 'escrow-1', 'seat-1', 'withdraw-1', 50000000)
	`)
	require.NoError(t, err)

	// Insert shred metro histories
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_metro_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, exchange_key, is_current_price_finalized, total_initialized_devices,
		 current_epoch, current_usdc_price_dollars)
		VALUES
		('mh-1', now(), now(), generateUUIDv4(), 0, 1,
		 'mh-1', 'metro-nyc', 1, 3, 950, 10)
	`)
	require.NoError(t, err)

	// Insert shred device histories
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_device_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, is_enabled, has_settled_seats, metro_exchange_key,
		 active_granted_seats, active_total_available_seats,
		 current_epoch, current_requested_seat_count, current_granted_seat_count,
		 current_total_available_seats, current_usdc_metro_premium_dollars)
		VALUES
		('dh-1', now(), now(), generateUUIDv4(), 0, 1,
		 'dh-1', 'dev-1', 1, 1, 'metro-nyc',
		 2, 10, 950, 3, 2, 10, -2)
	`)
	require.NoError(t, err)
}

func TestGetShredsOverview_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/overview", nil)
	rr := httptest.NewRecorder()
	api.GetShredsOverview(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var overview handlers.ShredsOverview
	err := json.NewDecoder(rr.Body).Decode(&overview)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), overview.CurrentSubscriptionEpoch)
}

func TestGetShredsOverview_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/overview", nil)
	rr := httptest.NewRecorder()
	api.GetShredsOverview(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var overview handlers.ShredsOverview
	err := json.NewDecoder(rr.Body).Decode(&overview)
	require.NoError(t, err)
	assert.Equal(t, "open for requests", overview.Phase)
	assert.Equal(t, uint64(950), overview.CurrentSubscriptionEpoch)
	assert.Equal(t, uint64(950), overview.CurrentSolanaEpoch)
	assert.Equal(t, uint64(2), overview.ClientSeatCount)
	assert.Equal(t, uint64(1), overview.PaymentEscrowCount)
	assert.Equal(t, uint64(1), overview.MetroHistoryCount)
	assert.Equal(t, uint64(1), overview.DeviceHistoryCount)
}

func TestGetShredClientSeats_Empty(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Empty(t, response.Items)
}

func TestGetShredClientSeats_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 2, response.Total)
	assert.Len(t, response.Items, 2)

	// Verify joins resolved
	seat1 := findSeat(response.Items, "seat-1")
	require.NotNil(t, seat1)
	assert.Equal(t, "NYC-CORE-01", seat1.DeviceCode)
	assert.Equal(t, "metro-nyc", seat1.MetroPK)
	assert.Equal(t, "NYC", seat1.MetroCode)
	assert.Equal(t, "192.168.1.1", seat1.ClientIP)
	assert.Equal(t, uint64(50000000), seat1.TotalUSDCBalance)
	assert.Equal(t, "user-1", seat1.UserPK)
	assert.Equal(t, "owner-pubkey-1", seat1.UserOwnerPubkey)
	// Price per epoch = metro price (10) + device premium (-2) = 8
	assert.Equal(t, int64(8), seat1.PricePerEpochDollars)

	// Verify price override seat
	seat2 := findSeat(response.Items, "seat-2")
	require.NotNil(t, seat2)
	assert.Equal(t, int64(25), seat2.PricePerEpochDollars)
	assert.Equal(t, uint8(1), seat2.HasPriceOverride)
}

func TestGetShredDeviceHistories_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/device-histories", nil)
	rr := httptest.NewRecorder()
	api.GetShredDeviceHistories(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredDeviceHistoryItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 1, response.Total)
	assert.Equal(t, "NYC-CORE-01", response.Items[0].DeviceCode)
	assert.Equal(t, "NYC", response.Items[0].MetroCode)
	assert.Equal(t, uint16(2), response.Items[0].ActiveGrantedSeats)
}

func TestGetShredMetroHistories_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/metro-histories", nil)
	rr := httptest.NewRecorder()
	api.GetShredMetroHistories(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredMetroHistoryItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 1, response.Total)
	assert.Equal(t, "NYC", response.Items[0].MetroCode)
	assert.Equal(t, uint16(10), response.Items[0].CurrentUSDCPriceDollars)
}

func TestGetShredFunders_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/funders", nil)
	rr := httptest.NewRecorder()
	api.GetShredFunders(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var funders []handlers.ShredFunderItem
	err := json.NewDecoder(rr.Body).Decode(&funders)
	require.NoError(t, err)
	assert.Len(t, funders, 2)
}

func findSeat(items []handlers.ShredClientSeatItem, pk string) *handlers.ShredClientSeatItem {
	for i := range items {
		if items[i].PK == pk {
			return &items[i]
		}
	}
	return nil
}
