package handlers_test

import (
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunSeatAlertSweep_SendsOncePerEpoch(t *testing.T) {
	t.Parallel()
	// ClickHouse (for seats/overview) + Postgres (for alerts) both needed.
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	ctx := t.Context()
	account := createTestAccount(t, ctx, api)
	sender := &fakeSender{}
	api.TelegramSender = sender

	// Seed one active, low-runway seat in ClickHouse and matching overview epoch.
	seedLowRunwaySeat(t, api, "seat-eval-1") // helper below sets active_epoch>=solana epoch, prepaid<1

	// Create + activate an alert for that seat, epochs_left <= 1.
	a, err := api.CreateSeatAlert(ctx, account.ID, "seat-eval-1", "epochs_left", 1, true)
	require.NoError(t, err)
	_, err = api.ActivateSeatAlertByToken(ctx, a.ActivationToken, 7001, "tester")
	require.NoError(t, err)

	require.NoError(t, api.RunSeatAlertSweep(ctx))
	assert.Equal(t, 1, sender.calls, "should send once")

	// Second sweep in the same epoch must not resend (dedup).
	require.NoError(t, api.RunSeatAlertSweep(ctx))
	assert.Equal(t, 1, sender.calls, "should not resend same epoch")
}

// seedLowRunwaySeat inserts a single active shred client seat into ClickHouse,
// along with its escrow and the supporting device/metro/solana-epoch rows the
// FetchShredsOverview and FetchShredSubscribers queries join against, so that
// RunSeatAlertSweep sees it as active with less than one epoch of prepaid
// balance. It mirrors insertShredsTestData in shreds_test.go: same tables,
// same MergeTree "_history" write path (the "_current" tables the handlers
// query are views over these, ranked by snapshot_ts/ingested_at/op_id per
// entity_id), same column sets.
func seedLowRunwaySeat(t *testing.T, api *handlers.API, seatPK string) {
	t.Helper()
	ctx := t.Context()

	const (
		// Solana epoch shared by the vote account and the seat's active_epoch,
		// so FetchShredsOverview.CurrentSolanaEpoch (max(epoch) from
		// solana_vote_accounts_current) is non-zero and the seat reads as active
		// (active_epoch >= current_solana_epoch).
		solanaEpoch = 1000

		// Metro price + device premium, so price_per_epoch_dollars > 0
		// (price_per_epoch_dollars = metroPriceDollars + devicePremium = 10).
		metroPriceDollars = 10
		devicePremium     = 0

		// Escrow balance below one epoch's price (10 USDC), so
		// PrepaidEpochs(balance, price) floors to 0.
		usdcBalanceMicro = 3_000_000
	)

	// Execution controller singleton. FetchShredsOverview's phase/epoch fields
	// come from this row; CurrentSolanaEpoch itself comes from
	// solana_vote_accounts_current below, not from here.
	err := api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_execution_controller_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, phase, current_subscription_epoch, total_metros, total_enabled_devices,
		 total_client_seats, updated_device_prices_count, settled_devices_count,
		 settled_client_seats_count, last_settled_slot, last_updating_prices_slot,
		 last_open_for_requests_slot, last_closed_for_requests_slot, next_seat_funding_index)
		VALUES
		('ec-eval-1', now(), now(), generateUUIDv4(), 0, 1,
		 'singleton', 'open for requests', 1000, 1, 1,
		 1, 0, 0, 0, 100, 200, 300, 400, 10)
	`)
	require.NoError(t, err)

	// Device and metro, so the seat's device/metro joins resolve (device code,
	// metro code, and the metro-price join via d.metro_pk = mh.exchange_key).
	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, status, device_type, contributor_pk, metro_pk, public_ip, max_users)
		VALUES
		('dev-eval-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-eval-1', 'EVAL-CORE-01', 'up', 'router', '', 'metro-eval-1', '10.1.0.1', 100)
	`)
	require.NoError(t, err)

	err = api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, name, latitude, longitude)
		VALUES
		('metro-eval-1', now(), now(), generateUUIDv4(), 0, 1, 'metro-eval-1', 'EVL', 'Evalville', 0, 0)
	`)
	require.NoError(t, err)

	// Solana vote account, for the current epoch.
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dim_solana_vote_accounts_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 vote_pubkey, epoch, node_pubkey, activated_stake_lamports, epoch_vote_account, commission_percentage)
		VALUES
		('vote-eval-1', now(), now(), generateUUIDv4(), 0, 1,
		 'vote-eval-1', %d, 'node-eval-1', 1000000000, 'true', 0)
	`, solanaEpoch))
	require.NoError(t, err)

	// Client seat: escrow_count > 0 and active_epoch >= current solana epoch,
	// so it reads as active (seatIsActive in seat_alerts_eval.go).
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key)
		VALUES
		('%s', now(), now(), generateUUIDv4(), 0, 1,
		 '%s', 'dev-eval-1', '10.1.0.2', 1, %d, %d,
		 0, 0, 1, 'funder-eval-1')
	`, seatPK, seatPK, solanaEpoch-1, solanaEpoch))
	require.NoError(t, err)

	// Escrow balance below one epoch's price, so PrepaidEpochs() floors to 0.
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dim_dz_shred_payment_escrows_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, client_seat_key, withdraw_authority_key, usdc_balance)
		VALUES
		('escrow-eval-1', now(), now(), generateUUIDv4(), 0, 1,
		 'escrow-eval-1', '%s', 'withdraw-eval-1', %d)
	`, seatPK, usdcBalanceMicro))
	require.NoError(t, err)

	// Metro price, joined via device.metro_pk = mh.exchange_key.
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dim_dz_shred_metro_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, exchange_key, is_current_price_finalized, total_initialized_devices,
		 current_epoch, current_usdc_price_dollars)
		VALUES
		('mh-eval-1', now(), now(), generateUUIDv4(), 0, 1,
		 'mh-eval-1', 'metro-eval-1', 1, 1, %d, %d)
	`, solanaEpoch, metroPriceDollars))
	require.NoError(t, err)

	// Device premium, joined directly via device_key = seat.device_key.
	err = api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dim_dz_shred_device_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, is_enabled, has_settled_seats, metro_exchange_key,
		 active_granted_seats, active_total_available_seats,
		 current_epoch, current_requested_seat_count, current_granted_seat_count,
		 current_total_available_seats, current_usdc_metro_premium_dollars)
		VALUES
		('dh-eval-1', now(), now(), generateUUIDv4(), 0, 1,
		 'dh-eval-1', 'dev-eval-1', 1, 1, 'metro-eval-1',
		 1, 1, %d, 1, 1, 1, %d)
	`, solanaEpoch, devicePremium))
	require.NoError(t, err)
}
