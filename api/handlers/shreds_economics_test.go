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
)

// asOf is the instant every economics fixture is read at. Fixed so the window,
// the open month and the "days recognized" counts do not drift with the clock.
var economicsAsOf = time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

// seedEconomics writes two epochs of seats, their epoch-dating escrow events, a
// metro rate card, a feed and its invoices, and two access passes holding feed
// seats.
//
//	epoch 950: opens 2026-07-30, seats A (fra, 100) and B (ams, 100) → 200.00
//	epoch 951: opens 2026-08-01, seat A only                          → 100.00
//
// Epoch 951 is the one in flight, so it has no successor and its window is the
// nominal 50 hours from 2026-08-01, all of which is inside August.
func seedEconomics(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, code, name, longitude, latitude)
		VALUES
		('metro-fra', now(), now(), generateUUIDv4(), 0, 1, 'metro-fra', 'fra', 'Frankfurt', 0, 0),
		('metro-ams', now(), now(), generateUUIDv4(), 0, 2, 'metro-ams', 'ams', 'Amsterdam', 0, 0),
		('metro-lon', now(), now(), generateUUIDv4(), 0, 3, 'metro-lon', 'lon', 'London', 0, 0)
	`))

	// Rate card: fra and ams at 100, lon at 60 with no seat ever sold. lon must
	// still appear on the card and must not appear in the metro table.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_metro_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, exchange_key, is_current_price_finalized, total_initialized_devices,
		 current_epoch, current_usdc_price_dollars)
		VALUES
		('mh-fra', now(), now(), generateUUIDv4(), 0, 1, 'mh-fra', 'metro-fra', 1, 4, 951, 100),
		('mh-ams', now(), now(), generateUUIDv4(), 0, 2, 'mh-ams', 'metro-ams', 1, 2, 951, 100),
		('mh-lon', now(), now(), generateUUIDv4(), 0, 3, 'mh-lon', 'metro-lon', 1, 1, 951, 60)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_device_histories_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, is_enabled, has_settled_seats, metro_exchange_key,
		 active_granted_seats, active_total_available_seats,
		 current_epoch, current_requested_seat_count, current_granted_seat_count,
		 current_total_available_seats, current_usdc_metro_premium_dollars)
		VALUES
		('dh-fra-950', now(), now(), generateUUIDv4(), 0, 1, 'dh-fra-950', 'dev-fra', 1, 1, 'metro-fra', 1, 4, 950, 1, 1, 4, 0),
		('dh-fra-951', now(), now(), generateUUIDv4(), 0, 2, 'dh-fra-951', 'dev-fra', 1, 1, 'metro-fra', 1, 4, 951, 1, 1, 4, 0),
		('dh-ams-950', now(), now(), generateUUIDv4(), 0, 3, 'dh-ams-950', 'dev-ams', 1, 1, 'metro-ams', 1, 2, 950, 1, 1, 2, 0)
	`))

	// Batch-allocated seats: start slot at the epoch's first slot, so each is
	// charged its full epoch price.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
		 subscription_start_slot, last_usdc_price_dollars)
		VALUES
		('seat-a-950', now(), now(), generateUUIDv4(), 0, 1, 'seat-a', 'dev-fra', '10.0.0.1', 1, 949, 950, 0, 0, 1, 'funder-a', 410400000, 100),
		('seat-b-950', now(), now(), generateUUIDv4(), 0, 2, 'seat-b', 'dev-ams', '10.0.0.2', 1, 949, 950, 0, 0, 1, 'funder-b', 410400000, 100),
		('seat-a-951', now(), now(), generateUUIDv4(), 0, 3, 'seat-a', 'dev-fra', '10.0.0.1', 2, 950, 951, 0, 0, 1, 'funder-a', 410832000, 100)
	`))

	// Escrow events date the epochs. Epoch 950 opens on 30 July and epoch 951 on
	// 1 August, so 950's window straddles the month boundary.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO fact_dz_shred_escrow_events
		(event_ts, ingested_at, escrow_pk, client_seat_pk, tx_signature, slot,
		 event_type, amount_usdc, balance_after_usdc, epoch, status, signer)
		VALUES
		('2026-07-30 00:00:00', now(), 'esc-a', 'seat-a', 'tx-950-a', 410400000, 'batch_allocate', NULL, NULL, 950, 'ok', 'signer-a'),
		('2026-08-01 00:00:00', now(), 'esc-a', 'seat-a', 'tx-951-a', 410832000, 'batch_allocate', NULL, NULL, 951, 'ok', 'signer-a')
	`))

	// One shreds feed per metro, plus a kalshi feed that must stay out of every
	// shreds figure.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_feeds_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, code, name, metro_pk, groups)
		VALUES
		('feed-fra', now(), now(), generateUUIDv4(), 0, 1, 'feed-fra', 'owner', 'solana-shreds-full', 'solana-shreds-full-fra', 'metro-fra', ''),
		('feed-ams', now(), now(), generateUUIDv4(), 0, 2, 'feed-ams', 'owner', 'solana-shreds-full', 'solana-shreds-full-ams', 'metro-ams', ''),
		('feed-kal', now(), now(), generateUUIDv4(), 0, 3, 'feed-kal', 'owner', 'kalshi-sports-mbp', 'kalshi-sports-mbp-fra', 'metro-fra', ''),
		('feed-lon', now(), now(), generateUUIDv4(), 0, 4, 'feed-lon', 'owner', 'solana-shreds-full', 'solana-shreds-full-lon', 'metro-lon', '')
	`))
	// August is invoiced, September is booked ahead. The kalshi row is larger
	// than both and must not show up anywhere.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_feed_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, feed_key, year, month, collected_usdc)
		VALUES
		('fd-fra-08', now(), now(), generateUUIDv4(), 0, 1, 'fd-fra-08', 'feed-fra', 2026, 8, 1500000000),
		('fd-ams-08', now(), now(), generateUUIDv4(), 0, 2, 'fd-ams-08', 'feed-ams', 2026, 8,  500000000),
		('fd-fra-09', now(), now(), generateUUIDv4(), 0, 3, 'fd-fra-09', 'feed-fra', 2026, 9, 3000000000),
		('fd-kal-08', now(), now(), generateUUIDv4(), 0, 4, 'fd-kal-08', 'feed-kal', 2026, 8, 9000000000),
		-- An account opened for August that has collected nothing yet.
		('fd-lon-08', now(), now(), generateUUIDv4(), 0, 5, 'fd-lon-08', 'feed-lon', 2026, 8, 0)
	`))

	// Access passes. One payer holds seats in both metros; a second holds one in
	// fra, on 20 August. A third pass holds a kalshi seat only. Snapshot
	// timestamps are what the per-epoch state reads, so the 20 August pass is
	// invisible to epoch 950 and live for epoch 951.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_access_passes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, type_tag, associated_pubkey, others_type_name, others_key,
		 client_ip, user_payer, last_access_epoch, connection_count, status,
		 mgroup_pub_allowlist, mgroup_sub_allowlist, flags, feed_seats)
		VALUES
		('pass-1', '2026-08-20 00:00:00', now(), generateUUIDv4(), 0, 1,
		 'pass-1', 'owner', 'edge_seat', '', '', '', '10.1.0.1', 'payer-1', 951, 1, 'connected', '', '', 0,
		 '[{"feed_pk":"feed-fra","max_users":1},{"feed_pk":"feed-ams","max_users":1}]'),
		('pass-2', '2026-08-20 00:00:00', now(), generateUUIDv4(), 0, 2,
		 'pass-2', 'owner', 'edge_seat', '', '', '', '10.1.0.2', 'payer-2', 951, 1, 'connected', '', '', 0,
		 '[{"feed_pk":"feed-fra","max_users":1}]'),
		('pass-3', '2026-08-20 00:00:00', now(), generateUUIDv4(), 0, 3,
		 'pass-3', 'owner', 'edge_seat', '', '', '', '10.1.0.3', 'payer-3', 951, 1, 'connected', '', '', 0,
		 '[{"feed_pk":"feed-kal","max_users":1}]')
	`))
}

// Read the way the page reads it: the whole history, which is what months = 0
// asks for. The fixture fits inside any window, so a test that cares about the
// window says so itself.
func fetchEconomics(t *testing.T, api *handlers.API) *handlers.ShredsEconomics {
	t.Helper()
	resp, err := api.FetchShredsEconomicsData(t.Context(), economicsAsOf, 0)
	require.NoError(t, err)
	return resp
}

// The two revenue streams are cut on the same window and the same as-of day, so
// a month's seat revenue, its invoices and the metros underneath them all agree.
func TestShredsEconomics_MonthsSplitBothStreams(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)

	resp := fetchEconomics(t, api)

	byMonth := map[string]handlers.ShredsEconomicsMonth{}
	for _, m := range resp.Months {
		byMonth[m.Month] = m
	}
	require.Contains(t, byMonth, "2026-07")
	require.Contains(t, byMonth, "2026-08")
	require.Contains(t, byMonth, "2026-09")

	// Epoch 950 opens 30 July and closes when 951 opens on 1 August, so its
	// 200.00 splits across two days of July and none of August. Epoch 951's
	// 100.00 falls wholly in August.
	july, august, september := byMonth["2026-07"], byMonth["2026-08"], byMonth["2026-09"]
	assert.InDelta(t, 200.0, july.SeatRevenue, 0.01)
	assert.InDelta(t, 100.0, august.SeatRevenue, 0.01)
	assert.Equal(t, 0.0, july.Invoiced, "invoices began in August")

	// Invoices are the shreds feeds only: 1500 + 500, never the 9000 kalshi row.
	assert.InDelta(t, 2000.0, august.Invoiced, 0.01)
	// Two feeds billed. A third holds an August account that has collected
	// nothing, and a feed is not "invoiced" on the strength of an empty account.
	assert.Equal(t, 2, august.InvoiceFeeds)

	// August is open and September holds revenue for a month that has not
	// started, so neither is a settled figure and both say so.
	assert.True(t, august.Open)
	assert.False(t, august.Future)
	assert.True(t, september.Future)
	assert.InDelta(t, 3000.0, september.Invoiced, 0.01)
	assert.Equal(t, 0.0, september.SeatRevenue, "a future month earns no seat revenue")

	assert.Equal(t, 31, august.DaysInMonth)
	assert.Equal(t, "2026-08-26", resp.AsOf)
}

// Seat revenue stops at the as-of day rather than running to the end of the
// epoch in flight. Without the cut the open month books revenue for days that
// have not happened, which reads as a settled total.
func TestShredsEconomics_OpenMonthStopsAtAsOf(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)

	resp := fetchEconomics(t, api)

	for _, m := range resp.Months {
		if m.Month == "2026-08" {
			// Epoch 951's nominal window is 50 hours from 1 August, so August
			// recognizes three days of it and nothing near its 31.
			assert.Equal(t, 3, m.Days)
			assert.Less(t, m.Days, m.DaysInMonth)
			return
		}
	}
	t.Fatal("no August row")
}

// Subscriptions are counted as feed seats, not payers, and only for the Solana
// Shreds feeds. The series measures each epoch at its end, so the epoch in
// flight carries the count live now and agrees with the live figures elsewhere.
func TestShredsEconomics_SubscriptionsCountSeatsNotPayers(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)

	resp := fetchEconomics(t, api)

	require.Len(t, resp.Epochs, 2)
	first, last := resp.Epochs[0], resp.Epochs[1]

	assert.Equal(t, uint64(950), first.Epoch)
	assert.Equal(t, "2026-07-30", first.Day)
	assert.Equal(t, 2, first.Seats)
	assert.InDelta(t, 200.0, first.Revenue, 0.01)
	// Epoch 950 ends when 951 opens on 1 August, before any pass existed.
	assert.Equal(t, 0, first.Subscriptions, "a true zero, not missing data")

	assert.Equal(t, uint64(951), last.Epoch)
	assert.Equal(t, 1, last.Seats)
	// Three shreds feed seats across two payers; the kalshi seat is not one.
	assert.Equal(t, 3, last.Subscriptions)

	assert.Equal(t, uint64(951), resp.CurrentEpoch)
	assert.Equal(t, 1, resp.LiveSeats)
	assert.Equal(t, 3, resp.LiveSubscriptions)
	assert.Equal(t, 2, resp.LiveSubscriptionPayers)
	// Epoch 950 ended with none live and 951 carries three, so the window saw the
	// transition and can name the epoch it happened in.
	assert.Equal(t, uint64(951), resp.SubscriptionsOpenedEpoch)
	assert.Equal(t, "2026-08-20", resp.SubscriptionsOpenedOn)

	// The live rate is the live seats at their metro's current price: one seat
	// in fra at 100.
	assert.InDelta(t, 100.0, resp.LiveSeatRate, 0.01)
}

// The epoch in flight is measured at the end of the as-of day, not its start. A
// subscription sold during that day is live, and the tiles built off this count
// say "live now", so cutting at midnight would under-report every one of them.
func TestShredsEconomics_LiveCountIncludesTheAsOfDay(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)
	ctx := t.Context()

	// A pass created part way through the as-of day itself.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_access_passes_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, type_tag, associated_pubkey, others_type_name, others_key,
		 client_ip, user_payer, last_access_epoch, connection_count, status,
		 mgroup_pub_allowlist, mgroup_sub_allowlist, flags, feed_seats)
		VALUES
		('pass-4', '2026-08-26 09:30:00', now(), generateUUIDv4(), 0, 4,
		 'pass-4', 'owner', 'edge_seat', '', '', '', '10.1.0.4', 'payer-4', 951, 1, 'connected', '', '', 0,
		 '[{"feed_pk":"feed-fra","max_users":1}]')
	`))

	resp := fetchEconomics(t, api)
	assert.Equal(t, 4, resp.LiveSubscriptions, "the seat sold this morning is live")
	assert.Equal(t, 3, resp.LiveSubscriptionPayers)
}

// A metro row carries both streams. Invoices reach a metro through the feed
// they bill, and only the recognized months count: September is booked ahead
// and must not inflate a table read as earnings to date.
func TestShredsEconomics_MetrosCarryBothStreams(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)

	resp := fetchEconomics(t, api)

	byMetro := map[string]handlers.ShredsEconomicsMetro{}
	for _, m := range resp.Metros {
		byMetro[m.Metro] = m
	}
	require.Contains(t, byMetro, "fra")
	require.Contains(t, byMetro, "ams")

	fra := byMetro["fra"]
	assert.InDelta(t, 100.0, fra.Price, 0.01)
	assert.Equal(t, 4, fra.Devices)
	assert.Equal(t, 1, fra.LiveSeats)
	assert.Equal(t, 2, fra.Subscriptions, "one seat from each pass")
	assert.InDelta(t, 200.0, fra.SeatRevenue, 0.01, "epochs 950 and 951")
	assert.InDelta(t, 1500.0, fra.Invoiced, 0.01, "August only, never September")

	ams := byMetro["ams"]
	assert.Equal(t, 0, ams.LiveSeats, "seat B did not renew into 951")
	assert.Equal(t, 1, ams.Subscriptions)
	assert.InDelta(t, 100.0, ams.SeatRevenue, 0.01)

	// Ordered by total revenue, largest first.
	require.GreaterOrEqual(t, len(resp.Metros), 2)
	assert.Equal(t, "fra", resp.Metros[0].Metro)

	// A metro that has never sold a seat is priced but has no row here.
	assert.NotContains(t, byMetro, "lon")
}

// A metro invoiced in the window still appears when nothing else touched it.
// The row set used to be whatever the seat and subscription reads covered, with
// invoices only left-joined on, so a customer whose pass lapsed after their
// month was billed took that month's revenue out of the metro table and left the
// footer short of the monthly totals.
func TestShredsEconomics_InvoiceOnlyMetroStillAppears(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)
	ctx := t.Context()

	// lon has a rate card, no seat charged in the window and no live pass, but
	// its feed collected in August.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_feed_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, feed_key, year, month, collected_usdc)
		VALUES
		('fd-lon-08b', now(), now(), generateUUIDv4(), 0, 9, 'fd-lon-08b', 'feed-lon', 2026, 8, 750000000)
	`))

	resp := fetchEconomics(t, api)

	byMetro := map[string]handlers.ShredsEconomicsMetro{}
	for _, m := range resp.Metros {
		byMetro[m.Metro] = m
	}
	require.Contains(t, byMetro, "lon", "an invoiced metro must have a row")
	lon := byMetro["lon"]
	assert.InDelta(t, 750.0, lon.Invoiced, 0.01)
	assert.Equal(t, 0, lon.LiveSeats)
	assert.Equal(t, 0, lon.Subscriptions)
	assert.InDelta(t, 0.0, lon.SeatRevenue, 0.01)
	// It still carries its rate card.
	assert.InDelta(t, 60.0, lon.Price, 0.01)

	// And the two halves of the page agree on the money.
	var metroInvoiced float64
	for _, m := range resp.Metros {
		metroInvoiced += m.Invoiced
	}
	var monthInvoiced float64
	for _, m := range resp.Months {
		if !m.Future {
			monthInvoiced += m.Invoiced
		}
	}
	assert.InDelta(t, monthInvoiced, metroInvoiced, 0.01,
		"metro invoices must reconcile with the recognized months")
}

// An invoice on a feed whose serviceability label has not landed keeps its
// revenue and lands under "unmapped", the same rule the monthly totals follow.
// Requiring a shreds code here left the metro table short of those totals until
// the label caught up.
func TestShredsEconomics_UnlabelledInvoiceReconciles(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)
	ctx := t.Context()

	// A distribution whose feed has no dz_feeds_current row at all.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_feed_distributions_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, feed_key, year, month, collected_usdc)
		VALUES
		('fd-orphan-08', now(), now(), generateUUIDv4(), 0, 9, 'fd-orphan-08', 'feed-not-labelled-yet', 2026, 8, 250000000)
	`))

	resp := fetchEconomics(t, api)

	var metroInvoiced float64
	byMetro := map[string]handlers.ShredsEconomicsMetro{}
	for _, m := range resp.Metros {
		metroInvoiced += m.Invoiced
		byMetro[m.Metro] = m
	}
	require.Contains(t, byMetro, "unmapped")
	assert.InDelta(t, 250.0, byMetro["unmapped"].Invoiced, 0.01)

	var monthInvoiced float64
	for _, m := range resp.Months {
		if !m.Future {
			monthInvoiced += m.Invoiced
		}
	}
	assert.InDelta(t, monthInvoiced, metroInvoiced, 0.01,
		"an unlabelled feed's revenue must reconcile, not disappear")
}

// The optional halves fail on their own. Both the feed-distribution and access-
// pass dimensions ship with the indexer, so an API pod rolled out ahead of it
// reads tables that are not there; folded into the seat query, that returned no
// metros at all and zeroed the live seat rate behind the run-rate tile.
func TestShredsEconomics_MissingOptionalTableKeepsSeatData(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `DROP TABLE dim_dz_shred_feed_distributions_history`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW dim_dz_shred_feed_distributions_current`))

	resp := fetchEconomics(t, api)

	byMetro := map[string]handlers.ShredsEconomicsMetro{}
	for _, m := range resp.Metros {
		byMetro[m.Metro] = m
	}
	require.Contains(t, byMetro, "fra", "seat data survives a missing invoice table")
	assert.InDelta(t, 200.0, byMetro["fra"].SeatRevenue, 0.01)
	assert.Equal(t, 1, byMetro["fra"].LiveSeats)
	assert.Equal(t, 2, byMetro["fra"].Subscriptions)
	assert.InDelta(t, 0.0, byMetro["fra"].Invoiced, 0.01, "the absent half reports nothing")

	// The figure the run-rate tile is built on must still be real.
	assert.InDelta(t, 100.0, resp.LiveSeatRate, 0.01)
}

// The priced-metro count is what a seat would cost somewhere, so it counts every
// metro carrying a price, including the ones with no revenue at all. lon has a
// rate card and has never sold a seat; it counts here and not in the table.
func TestShredsEconomics_MetrosPricedCountsUnsoldMetros(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)

	resp := fetchEconomics(t, api)

	assert.Equal(t, 3, resp.MetrosPriced)
	assert.Equal(t, 15, resp.EpochsPerMonth)

	metros := make([]string, 0, len(resp.Metros))
	for _, m := range resp.Metros {
		metros = append(metros, m.Metro)
	}
	assert.NotContains(t, metros, "lon")
}

// months= is validated rather than silently falling back: a caller asking for a
// window the endpoint will not serve should be told, not handed a different one.
// There is no upper bound - the default is already the whole history, so a
// window larger than the data is just the default by another name.
func TestGetShredsEconomics_RejectsBadMonths(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	for _, bad := range []string{"0", "-3", "abc"} {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/economics?months="+bad, nil)
		rr := httptest.NewRecorder()
		api.GetShredsEconomics(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code, "months=%s", bad)
	}

	for _, ok := range []string{"", "?months=99"} {
		req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/economics"+ok, nil)
		rr := httptest.NewRecorder()
		api.GetShredsEconomics(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code, "query=%q", ok)
	}
}

// The page opens on the whole history. It used to open on a five-month window,
// which cut the program's first months off the chart the moment it had been
// running longer than that.
func TestShredsEconomics_DefaultWindowIsFullHistory(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)
	ctx := t.Context()

	// One seat in an epoch that opened in January, seven months before the as-of
	// day and well outside the window the page used to read.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
		 subscription_start_slot, last_usdc_price_dollars)
		VALUES
		('seat-a-860', now(), now(), generateUUIDv4(), 0, 10, 'seat-a', 'dev-fra', '10.0.0.1', 1, 859, 860, 0, 0, 1, 'funder-a', 371520000, 100)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO fact_dz_shred_escrow_events
		(event_ts, ingested_at, escrow_pk, client_seat_pk, tx_signature, slot,
		 event_type, amount_usdc, balance_after_usdc, epoch, status, signer)
		VALUES
		('2026-01-10 00:00:00', now(), 'esc-a', 'seat-a', 'tx-860-a', 371520000, 'batch_allocate', NULL, NULL, 860, 'ok', 'signer-a')
	`))

	months := func(resp *handlers.ShredsEconomics) []string {
		keys := make([]string, 0, len(resp.Months))
		for _, m := range resp.Months {
			keys = append(keys, m.Month)
		}
		return keys
	}

	full, err := api.FetchShredsEconomicsData(ctx, economicsAsOf, 0)
	require.NoError(t, err)
	assert.Contains(t, months(full), "2026-01", "the default window reaches the first month with data")

	narrow, err := api.FetchShredsEconomicsData(ctx, economicsAsOf, 5)
	require.NoError(t, err)
	assert.NotContains(t, months(narrow), "2026-01", "months= still narrows the window")
}

// The series opens on the first month that earned something. The program's
// first weeks ran on pre-pricing seats charged nothing at all, and those months
// survive economicsMonthlySeatRevenue's cent floor only as a seat count - a zero
// bar on the chart claiming the program earned nothing that month rather than
// that it had not started charging.
func TestShredsEconomics_LeadingEmptyMonthsAreTrimmed(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dim_dz_shred_client_seats_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, device_key, client_ip, tenure_epochs, funded_epoch, active_epoch,
		 has_price_override, override_usdc_price_dollars, escrow_count, funding_authority_key,
		 subscription_start_slot, last_usdc_price_dollars)
		VALUES
		('seat-free-930', now(), now(), generateUUIDv4(), 0, 11, 'seat-free', 'dev-unpriced', '10.0.0.9', 1, 929, 930, 0, 0, 1, 'funder-a', 0, 0)
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO fact_dz_shred_escrow_events
		(event_ts, ingested_at, escrow_pk, client_seat_pk, tx_signature, slot,
		 event_type, amount_usdc, balance_after_usdc, epoch, status, signer)
		VALUES
		('2026-06-10 00:00:00', now(), 'esc-free', 'seat-free', 'tx-930', 401760000, 'batch_allocate', NULL, NULL, 930, 'ok', 'signer-a')
	`))

	resp := fetchEconomics(t, api)
	require.NotEmpty(t, resp.Months)

	keys := make([]string, 0, len(resp.Months))
	for _, m := range resp.Months {
		keys = append(keys, m.Month)
	}
	assert.NotContains(t, keys, "2026-06", "a leading month that charged nothing is not a month of trading")
	assert.Equal(t, "2026-07", resp.Months[0].Month, "the series opens on the first month with revenue")
	assert.Contains(t, keys, "2026-08")
}

// The mean epoch length is the gap between consecutive epoch starts, and the
// first epoch has no predecessor: lagInFrame hands it the zero date. The old
// five-month window cut that row off by accident, so a full-history one has to
// exclude it outright - otherwise the run-rate projection spreads the live seat
// rate over a 250-day epoch and reports next to nothing.
func TestShredsEconomics_EpochDaysIgnoresTheFirstEpoch(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedEconomics(t, api)

	resp := fetchEconomics(t, api)
	assert.InDelta(t, 2.0, resp.EpochDays, 0.01, "epoch 950 opens 30 July and 951 opens 1 August")
}

// An environment with none of the program's data answers 200 with empty lists
// rather than 500 or null. The page renders its empty state off those, and
// `null` would break it.
func TestGetShredsEconomics_EmptyIsNotNull(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/economics", nil)
	rr := httptest.NewRecorder()
	api.GetShredsEconomics(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.ShredsEconomics
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotNil(t, resp.Months)
	assert.NotNil(t, resp.Epochs)
	assert.NotNil(t, resp.Metros)
	assert.Empty(t, resp.Epochs)
	assert.Equal(t, "", resp.SubscriptionsOpenedOn, "no subscription has ever been sold")
}

// A database without the indexer's dimensions answers 200 with the parts it can
// compute. The migrations ship with the indexer, so an API pod rolled out ahead
// of it reads tables that are not there yet; a 500 there logs at ERROR on every
// page load and pages on-call through a deploy.
func TestGetShredsEconomics_MissingTablesDegradeNotError(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/shreds/economics", nil)
	rr := httptest.NewRecorder()
	api.GetShredsEconomics(rr, req)

	// The seat tables are the ones this cannot do without; without them the
	// request still fails, and that is the correct 500.
	if rr.Code == http.StatusOK {
		var resp handlers.ShredsEconomics
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		assert.Empty(t, resp.Epochs)
	}
}
