package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		 1, 25, 0, 'funder-2'),
		('seat-3', now(), now(), generateUUIDv4(), 0, 3,
		 'seat-3', 'dev-1', '192.168.1.3', 5, 940, 945,
		 0, 0, 1, 'funder-1')
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
		 current_epoch, current_usdc_price_dollars, retransmit_only_enabled)
		VALUES
		('mh-1', now(), now(), generateUUIDv4(), 0, 1,
		 'mh-1', 'metro-nyc', 1, 3, 950, 10, 1)
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
	assert.Equal(t, uint64(3), overview.ClientSeatCount)
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
	// Internal user can see client_ip.
	api.GetShredClientSeats(rr, withAccount(req, &handlers.Account{IsInternalUser: true}))

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 3)

	// Verify joins resolved
	seat1 := findSeat(response.Items, "seat-1")
	require.NotNil(t, seat1)
	assert.Equal(t, "NYC-CORE-01", seat1.DeviceCode)
	assert.Equal(t, "metro-nyc", seat1.MetroPK)
	assert.Equal(t, "NYC", seat1.MetroCode)
	assert.Equal(t, "192.168.1.1", seat1.ClientIP)
	// Single escrow: spendable == all-escrows == the one balance.
	assert.Equal(t, uint64(50000000), seat1.SpendableUSDCBalance)
	assert.Equal(t, uint64(50000000), seat1.AllEscrowsUSDCBalance)
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

func TestGetShredClientSeats_ClientIPRedacted(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	cases := []struct {
		name string
		req  func(r *http.Request) *http.Request
	}{
		{"anonymous", func(r *http.Request) *http.Request { return r }},
		{"wallet auth", func(r *http.Request) *http.Request {
			return withAccount(r, &handlers.Account{AccountType: "wallet", IsInternalUser: false})
		}},
		{"non-internal domain", func(r *http.Request) *http.Request {
			return withAccount(r, &handlers.Account{AccountType: "domain", IsInternalUser: false})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// IP filter and ip-sorted requests must be ignored (not leak via sort/filter either).
			req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?filters=ip:192.168&sort_by=ip", nil)
			rr := httptest.NewRecorder()
			api.GetShredClientSeats(rr, tc.req(req))

			assert.Equal(t, http.StatusOK, rr.Code)

			var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
			err := json.NewDecoder(rr.Body).Decode(&response)
			require.NoError(t, err)
			// Filter is dropped, so all seats returned.
			assert.Equal(t, 3, response.Total)
			for _, item := range response.Items {
				assert.Empty(t, item.ClientIP, "client_ip should be redacted for %s", tc.name)
			}
		})
	}
}

func TestGetShredClientSeats_StatusFilter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	// Test data: seat-1 (epoch=950, escrow=1, active), seat-2 (epoch=950, escrow=0, closed),
	// seat-3 (epoch=945, escrow=1, inactive). Current epoch=950.

	tests := []struct {
		name   string
		status string
		want   int
	}{
		{"active only", "active", 1},     // seat-1 (epoch=950, escrow>0)
		{"inactive only", "inactive", 1}, // seat-3 (epoch=945, escrow>0)
		{"closed only", "closed", 1},     // seat-2 (escrow=0)
		{"active+inactive", "active,inactive", 2},
		{"all statuses", "active,inactive,closed", 3},
		{"no filter", "", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "/api/dz/shreds/client-seats"
			if tt.status != "" {
				url += "?status=" + tt.status
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			rr := httptest.NewRecorder()
			api.GetShredClientSeats(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)

			var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
			err := json.NewDecoder(rr.Body).Decode(&response)
			require.NoError(t, err)
			assert.Equal(t, tt.want, response.Total, "status=%q", tt.status)
		})
	}
}

// TestGetShredClientSeats_MultiEscrowUsesMaxNotSum is the regression test for the
// multi-escrow production case: a seat with two escrows ($5.83 + $25.65) at a
// $30/epoch price. The oracle evaluates activation per-escrow, so no single escrow
// covers the price and the seat never activates. The dashboard must report the
// greatest single escrow (25.65) as spendable — not the sum (31.48) — so status
// derives to inactive/expired, not pending.
func TestGetShredClientSeats_MultiEscrowUsesMaxNotSum(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Current Solana epoch = 950.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		('vote-1', now(), now(), generateUUIDv4(), 0, 1,
		 'vote-1', 950, 'node-1', 1000000000, 'true', 0)
	`))

	// Seat active_epoch (945) < current epoch (950), escrow_count = 2, $30/epoch override.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key)
		VALUES
		('seat-me', now(), now(), generateUUIDv4(), 0, 1,
		 'seat-me', 'dev-1', '203.0.113.7', 0, 945, 945,
		 1, 30, 2, 'funder-me')
	`))

	// Two escrows: $5.83 and $25.65. Neither alone covers the $30 price.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_payment_escrows_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, client_seat_key, withdraw_authority_key, usdc_balance)
		VALUES
		('escrow-me-1', now(), now(), generateUUIDv4(), 0, 1, 'escrow-me-1', 'seat-me', 'withdraw-me', 5830000),
		('escrow-me-2', now(), now(), generateUUIDv4(), 0, 1, 'escrow-me-2', 'seat-me', 'withdraw-me', 25650000)
	`))

	// Balance fields: spendable = max (25.65), all-escrows = sum (31.48).
	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	require.Len(t, response.Items, 1)
	seat := response.Items[0]
	assert.Equal(t, uint32(2), seat.EscrowCount)
	assert.Equal(t, uint64(25650000), seat.SpendableUSDCBalance, "spendable must be the greatest single escrow, not the sum")
	assert.Equal(t, uint64(31480000), seat.AllEscrowsUSDCBalance, "all-escrows total exposes stranded funds")

	// Status must derive to inactive/expired (prepaid = intDiv(25.65, 30) = 0),
	// NOT pending (which the sum-based bug produced: intDiv(31.48, 30) = 1).
	statusChecks := []struct {
		status string
		want   int
	}{
		{"inactive", 1},
		{"pending", 0},
		{"active", 0},
	}
	for _, sc := range statusChecks {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?status="+sc.status, nil)
		rr := httptest.NewRecorder()
		api.GetShredClientSeats(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		var resp handlers.PaginatedResponse[handlers.ShredClientSeatItem]
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		assert.Equal(t, sc.want, resp.Total, "status=%q", sc.status)
	}
}

// TestGetShredClientSeats_PrepaidSortAndFilter pins the prepaid semantics change:
// the "prepaid" sort orders by prepaid-epochs (spendable / price), not the stale
// price_per_epoch_dollars proxy, and the "prepaid" filter is honored (it was silently
// dropped before the shared expr existed). The two seats are constructed so price order
// and prepaid-epoch order disagree: the cheaper seat has more prepaid runway.
func TestGetShredClientSeats_PrepaidSortAndFilter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// seat-hi: $8/epoch, one $48 escrow → prepaid = 6.
	// seat-lo: $30/epoch, one $25.65 escrow → prepaid = 0.
	// Price order (asc) is seat-hi < seat-lo; prepaid order (desc) is seat-hi > seat-lo.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key)
		VALUES
		('seat-hi', now(), now(), generateUUIDv4(), 0, 1, 'seat-hi', 'dev-1', '203.0.113.8', 0, 900, 900, 1, 8, 1, 'funder-ps'),
		('seat-lo', now(), now(), generateUUIDv4(), 0, 1, 'seat-lo', 'dev-1', '203.0.113.9', 0, 900, 900, 1, 30, 1, 'funder-ps')
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_payment_escrows_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, client_seat_key, withdraw_authority_key, usdc_balance)
		VALUES
		('escrow-hi', now(), now(), generateUUIDv4(), 0, 1, 'escrow-hi', 'seat-hi', 'withdraw-ps', 48000000),
		('escrow-lo', now(), now(), generateUUIDv4(), 0, 1, 'escrow-lo', 'seat-lo', 'withdraw-ps', 25650000)
	`))

	// Sort by prepaid descending: seat-hi (6 epochs) before seat-lo (0 epochs).
	// The old price-based sort would put seat-lo ($30) first.
	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?filters=funder:funder-ps&sort_by=prepaid&sort_dir=desc", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var sorted handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&sorted))
	require.Len(t, sorted.Items, 2)
	assert.Equal(t, "seat-hi", sorted.Items[0].PK, "prepaid sort must order by epochs, not price")
	assert.Equal(t, "seat-lo", sorted.Items[1].PK)

	// Filter by prepaid >= 1: only seat-hi qualifies.
	req = httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?filters=funder:funder-ps&filters=prepaid:%3E=1", nil)
	rr = httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var filtered handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&filtered))
	require.Len(t, filtered.Items, 1, "prepaid filter must be honored")
	assert.Equal(t, "seat-hi", filtered.Items[0].PK)
	assert.Equal(t, 1, filtered.Total)
}

func TestGetShredClientSeats_Sort(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?sort_by=tenure&sort_dir=asc", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	require.Len(t, response.Items, 3)
	// Ascending by tenure: seat-2 (1), seat-1 (3), seat-3 (5)
	assert.Equal(t, uint16(1), response.Items[0].TenureEpochs)
	assert.Equal(t, uint16(3), response.Items[1].TenureEpochs)
	assert.Equal(t, uint16(5), response.Items[2].TenureEpochs)
}

func TestGetShredClientSeats_Filter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	// Filter by seat PK
	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?filters=seat:seat-1", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 1, response.Total)
	assert.Equal(t, "seat-1", response.Items[0].PK)
}

func TestGetShredClientSeats_Pagination(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/client-seats?limit=1&offset=0", nil)
	rr := httptest.NewRecorder()
	api.GetShredClientSeats(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredClientSeatItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 3, response.Total)
	assert.Len(t, response.Items, 1)
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
	assert.Equal(t, uint8(1), response.Items[0].RetransmitOnlyEnabled)
}

// The devices listing inherits retransmit_only_enabled from the device's metro,
// and must fall back to 0 for a device whose metro has no history row.
func TestGetShredDevices_RetransmitOnlyFromMetro(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api)

	// Second device in a metro with no dim_dz_shred_metro_histories row.
	err := api.DB.Exec(t.Context(), `
		INSERT INTO dim_dz_shred_device_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, is_enabled, has_settled_seats, metro_exchange_key,
		 active_granted_seats, active_total_available_seats,
		 current_epoch, current_requested_seat_count, current_granted_seat_count,
		 current_total_available_seats, current_usdc_metro_premium_dollars)
		VALUES
		('dh-2', now(), now(), generateUUIDv4(), 0, 2,
		 'dh-2', 'dev-2', 1, 1, 'metro-unknown',
		 0, 10, 950, 0, 0, 10, 0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/devices", nil)
	rr := httptest.NewRecorder()
	api.GetShredDevices(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredDeviceItem]
	err = json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)

	flags := map[string]uint8{}
	for _, d := range response.Items {
		flags[d.DeviceKey] = d.RetransmitOnlyEnabled
	}
	// Presence first: a device whose metro has no history row must still be
	// listed, otherwise the zero-value lookups below would pass vacuously.
	require.Contains(t, flags, "dev-1")
	require.Contains(t, flags, "dev-2")
	assert.Equal(t, uint8(1), flags["dev-1"], "dev-1's metro has the flag set")
	assert.Equal(t, uint8(0), flags["dev-2"], "dev-2's metro has no history row")
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

// insertEscrowEventsTestData seeds 4 events anchored to the returned base time:
// base+0h (tx-1 fund), base+2h (tx-2 allocate_seat), base+24h (tx-3 fund),
// base+48h (tx-4 close). Base is 4 days ago so events stay inside the default 30d range.
func insertEscrowEventsTestData(t *testing.T, api *handlers.API) time.Time {
	ctx := t.Context()

	base := time.Now().UTC().Add(-4 * 24 * time.Hour).Truncate(time.Hour)
	ts := func(d time.Duration) string {
		return base.Add(d).Format("2006-01-02 15:04:05")
	}

	err := api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO fact_dz_shred_escrow_events
		(event_ts, ingested_at, escrow_pk, client_seat_pk, tx_signature, slot,
		 event_type, amount_usdc, balance_after_usdc, epoch, status, signer)
		VALUES
		('%s', now(), 'escrow-1', 'seat-1', 'tx-1', 100, 'fund', 50000000, 50000000, 950, 'ok', 'signer-1'),
		('%s', now(), 'escrow-1', 'seat-1', 'tx-2', 200, 'allocate_seat', NULL, 40000000, 950, 'ok', 'signer-1'),
		('%s', now(), 'escrow-2', 'seat-2', 'tx-3', 300, 'fund', 100000000, 100000000, 950, 'ok', 'DZfHfcCXTLwgZeCRKQ1FL1UuwAwFAZM93g86NMYpfYan'),
		('%s', now(), 'escrow-1', 'seat-1', 'tx-4', 400, 'close', 40000000, 0, 950, 'ok', 'signer-1')
	`, ts(0), ts(2*time.Hour), ts(24*time.Hour), ts(48*time.Hour)))
	require.NoError(t, err)
	return base
}

func TestGetShredEscrowEvents_WithData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertEscrowEventsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d", nil)
	rr := httptest.NewRecorder()
	api.GetShredEscrowEvents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	// 3 events (internal-signer excluded by default)
	assert.Equal(t, 3, response.Total)
	// Should be sorted by time DESC by default
	assert.Equal(t, "tx-4", response.Items[0].TxSignature)
	assert.Equal(t, "close", response.Items[0].EventType)
	assert.NotEmpty(t, response.Items[0].SolscanURL)
	assert.NotEmpty(t, response.Items[0].Signer)
}

func TestGetShredEscrowEvents_ClientIPRedacted(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertShredsTestData(t, api) // gives seat-1 -> client_ip 192.168.1.1
	insertEscrowEventsTestData(t, api)

	t.Run("anonymous redacts", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d", nil)
		rr := httptest.NewRecorder()
		api.GetShredEscrowEvents(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
		err := json.NewDecoder(rr.Body).Decode(&response)
		require.NoError(t, err)
		require.NotEmpty(t, response.Items)
		for _, item := range response.Items {
			assert.Empty(t, item.ClientIP, "client_ip should be redacted for anonymous users")
		}
	})

	t.Run("wallet auth redacts", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d", nil)
		rr := httptest.NewRecorder()
		api.GetShredEscrowEvents(rr, withAccount(req, &handlers.Account{AccountType: "wallet", IsInternalUser: false}))

		assert.Equal(t, http.StatusOK, rr.Code)
		var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
		err := json.NewDecoder(rr.Body).Decode(&response)
		require.NoError(t, err)
		for _, item := range response.Items {
			assert.Empty(t, item.ClientIP, "client_ip should be redacted for wallet-auth users")
		}
	})

	t.Run("internal user sees ip", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d&filters=seat:seat-1", nil)
		rr := httptest.NewRecorder()
		api.GetShredEscrowEvents(rr, withAccount(req, &handlers.Account{AccountType: "domain", IsInternalUser: true}))

		assert.Equal(t, http.StatusOK, rr.Code)
		var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
		err := json.NewDecoder(rr.Body).Decode(&response)
		require.NoError(t, err)
		require.NotEmpty(t, response.Items)
		for _, item := range response.Items {
			assert.Equal(t, "192.168.1.1", item.ClientIP)
		}
	})
}

func TestGetShredEscrowEvents_IncludeInternal(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertEscrowEventsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d&include_internal=true", nil)
	rr := httptest.NewRecorder()
	api.GetShredEscrowEvents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 4, response.Total) // All events including internal
}

func TestGetShredEscrowEvents_TimeRange(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	base := insertEscrowEventsTestData(t, api)

	// Window covers only base+0h (tx-1) and base+2h (tx-2), excluding tx-3 (+24h) and tx-4 (+48h).
	start := base.Add(-1 * time.Hour).Unix()
	end := base.Add(12 * time.Hour).Unix()
	url := fmt.Sprintf("/api/dz/shreds/escrow-events?start_time=%d&end_time=%d&include_internal=true", start, end)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	api.GetShredEscrowEvents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 2, response.Total) // tx-1 and tx-2
}

func TestGetShredEscrowEvents_Filter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertEscrowEventsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d&filters=type:fund&include_internal=true", nil)
	rr := httptest.NewRecorder()
	api.GetShredEscrowEvents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, 2, response.Total) // Two fund events
	for _, item := range response.Items {
		assert.Equal(t, "fund", item.EventType)
	}
}

func TestGetShredEscrowEvents_Sort(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	insertEscrowEventsTestData(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/escrow-events?range=30d&sort_by=time&sort_dir=asc&include_internal=true", nil)
	rr := httptest.NewRecorder()
	api.GetShredEscrowEvents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var response handlers.PaginatedResponse[handlers.ShredEscrowEventItem]
	err := json.NewDecoder(rr.Body).Decode(&response)
	require.NoError(t, err)
	require.Len(t, response.Items, 4)
	assert.Equal(t, "tx-1", response.Items[0].TxSignature) // Earliest first
	assert.Equal(t, "tx-4", response.Items[3].TxSignature) // Latest last
}

func findSeat(items []handlers.ShredClientSeatItem, pk string) *handlers.ShredClientSeatItem {
	for i := range items {
		if items[i].PK == pk {
			return &items[i]
		}
	}
	return nil
}

func TestGetShredEpochRevenue_Prorated(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Devices and metros for the legacy-fallback path.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_metro_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, exchange_key, is_current_price_finalized, total_initialized_devices,
		 current_epoch, current_usdc_price_dollars)
		VALUES
		('mh-100', now(), now(), generateUUIDv4(), 0, 1,
		 'mh-100', 'metro-x', 1, 1, 100, 10)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_device_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, is_enabled, has_settled_seats, metro_exchange_key,
		 active_granted_seats, active_total_available_seats,
		 current_epoch, current_requested_seat_count, current_granted_seat_count,
		 current_total_available_seats, current_usdc_metro_premium_dollars)
		VALUES
		('dh-100', now(), now(), generateUUIDv4(), 0, 1,
		 'dh-100', 'dev-x', 1, 1, 'metro-x',
		 4, 10, 100, 4, 4, 10, -2)
	`))

	// Four seats in active_epoch 100 with slots_per_epoch = 432000 (epoch start
	// at 43_200_000, epoch end at 43_632_000):
	//   batch:    start = epoch_start, last_price=15  → charged = 15.0  (full epoch)
	//   instant:  start = epoch_start+216000, last_price=20 → charged = 10.0 (half epoch)
	//   legacy:   start=0, last_price=0, no override     → charged = 10 + (-2) = 8.0
	//   override: start=0, last_price=0, override=25      → charged = 25.0
	// Expected total: 58.0, payment_count = 4.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
		 subscription_start_slot, last_usdc_price_dollars)
		VALUES
		('seat-batch', now(), now(), generateUUIDv4(), 0, 1,
		 'seat-batch', 'dev-x', '10.0.0.1', 1, 99, 100,
		 0, 0, 1, 'funder-1',
		 43200000, 15),
		('seat-instant', now(), now(), generateUUIDv4(), 0, 2,
		 'seat-instant', 'dev-x', '10.0.0.2', 1, 100, 100,
		 0, 0, 1, 'funder-2',
		 43416000, 20),
		('seat-legacy', now(), now(), generateUUIDv4(), 0, 3,
		 'seat-legacy', 'dev-x', '10.0.0.3', 1, 100, 100,
		 0, 0, 1, 'funder-3',
		 0, 0),
		('seat-override', now(), now(), generateUUIDv4(), 0, 4,
		 'seat-override', 'dev-x', '10.0.0.4', 1, 100, 100,
		 1, 25, 1, 'funder-4',
		 0, 0)
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/epoch-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredEpochRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var items []handlers.ShredEpochRevenueItem
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	require.Len(t, items, 1)
	assert.Equal(t, uint64(100), items[0].Epoch)
	assert.InDelta(t, 58.0, items[0].TotalDollars, 1e-6)
	assert.InDelta(t, 58.0, items[0].TotalUSDC, 1e-6)
	assert.Equal(t, uint64(4), items[0].PaymentCount)
}

// A seat that withdrew mid-epoch via the prorated instruction emits
// "Refunded N USDC", which the parser stores in
// fact_dz_shred_escrow_events.amount_usdc on the withdraw_seat row. The
// revenue query subtracts those refunds from the gross charge per
// (seat, active_epoch). Non-prorated withdrawals leave amount_usdc null and
// are ignored.
func TestGetShredEpochRevenue_NetsOutProratedRefund(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// active_epoch=300: epoch_start=129_600_000, epoch_end=130_032_000.
	//   seat-refund: batch-allocated at epoch_start, last_price=40 → gross=40.
	//                Prorated withdrawal at slot 129_816_000 (halfway through),
	//                refund=20 USDC = 20_000_000 micro. Net=20.
	//   seat-old-withdraw: identical allocation, but withdrawn via the old
	//                non-prorated instruction (amount_usdc NULL) → no refund
	//                applied, full 40 charged.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
		 subscription_start_slot, last_usdc_price_dollars)
		VALUES
		('seat-refund', now(), now(), generateUUIDv4(), 0, 1,
		 'seat-refund', 'dev-x', '10.0.0.6', 1, 299, 300,
		 0, 0, 1, 'funder-6',
		 129600000, 40),
		('seat-old-withdraw', now(), now(), generateUUIDv4(), 0, 2,
		 'seat-old-withdraw', 'dev-x', '10.0.0.7', 1, 299, 300,
		 0, 0, 1, 'funder-7',
		 129600000, 40)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO fact_dz_shred_escrow_events
		(event_ts, ingested_at, escrow_pk, client_seat_pk, tx_signature, slot,
		 event_type, amount_usdc, balance_after_usdc, epoch, status, signer)
		VALUES
		(now(), now(), 'esc-refund', 'seat-refund', 'tx-refund', 129816000,
		 'withdraw_seat', 20000000, NULL, NULL, 'ok', 'signer-1'),
		(now(), now(), 'esc-old',    'seat-old-withdraw', 'tx-old', 129816000,
		 'withdraw_seat', NULL, NULL, NULL, 'ok', 'signer-1')
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/epoch-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredEpochRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var items []handlers.ShredEpochRevenueItem
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	require.Len(t, items, 1)
	assert.Equal(t, uint64(300), items[0].Epoch)
	// 40 (seat-refund gross) - 20 (refund) + 40 (seat-old-withdraw, no refund) = 60.
	assert.InDelta(t, 60.0, items[0].TotalDollars, 1e-6)
	assert.Equal(t, uint64(2), items[0].PaymentCount)
}

// Sanity-check that a seat which deactivated mid-epoch (last_price/start_slot
// zeroed in its latest snapshot) still contributes the original prorated
// charge — max() over snapshots picks the allocation-time values, not the
// post-deactivation zeros.
func TestGetShredEpochRevenue_DeactivatedSeatChargeRetained(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Seat allocated at epoch start (full price), then deactivated. Two
	// snapshots: allocation-time with values, then a later snapshot with
	// zeros. Both rows have active_epoch=200 and is_deleted=0.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
		 subscription_start_slot, last_usdc_price_dollars)
		VALUES
		('seat-cycled', toDateTime('2026-01-01 00:00:00'), now(), generateUUIDv4(), 0, 1,
		 'seat-cycled', 'dev-x', '10.0.0.5', 1, 199, 200,
		 0, 0, 1, 'funder-5',
		 86400000, 30),
		('seat-cycled', toDateTime('2026-01-02 00:00:00'), now(), generateUUIDv4(), 0, 2,
		 'seat-cycled', 'dev-x', '10.0.0.5', 1, 199, 200,
		 0, 0, 1, 'funder-5',
		 0, 0)
	`))

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/epoch-revenue", nil)
	rr := httptest.NewRecorder()
	api.GetShredEpochRevenue(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var items []handlers.ShredEpochRevenueItem
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	require.Len(t, items, 1)
	assert.Equal(t, uint64(200), items[0].Epoch)
	// Allocated at epoch_start=200*432000=86400000, full epoch → 30.0.
	assert.InDelta(t, 30.0, items[0].TotalDollars, 1e-6)
}
