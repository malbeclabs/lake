package handlers

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// The shreds economics page reads one payload. Every figure on it is either a
// seat charge or a feed invoice, and the two are summed, so they have to be cut
// on the same window and the same as-of instant. Splitting them across
// endpoints let one half answer for a different day than the other, which is
// how a month total stopped matching the metros underneath it.

// shredSlotsPerEpoch is Solana's slots per epoch. The on-chain program prorates
// an instant-allocated seat's charge by the slots it was actually active for,
// so this is both the divisor and the cap.
const shredSlotsPerEpoch = 432000

// shredEpochsPerMonth is the rate-card convention: prices are quoted per epoch
// and a month is sold as 15 of them. It is not the measured epoch cadence
// (ShredsEconomics.EpochDays is), and the two are deliberately not reconciled -
// one prices, the other projects.
const shredEpochsPerMonth = 15

// defaultEconomicsMonths is how many calendar months of revenue the page shows,
// counting the open one. Months ahead of it are always included on top: the
// subscription program bills forward, so a month that has not started can
// already hold invoiced revenue, and hiding it would read as revenue lost.
const defaultEconomicsMonths = 5

// maxEconomicsMonths bounds the window a caller can ask for. The accrual query
// spreads every epoch's charge across the days it covers, so the window is the
// only thing standing between this endpoint and a full-history scan.
const maxEconomicsMonths = 24

// activeSlotsExpr is the slots a seat was active for in its epoch: the epoch's
// last slot less the seat's start slot, capped at a whole epoch and floored at
// zero. Requires seat_per_epoch joined as s.
var activeSlotsExpr = fmt.Sprintf(
	`greatest(least(toInt64((s.active_epoch + 1) * %[1]d) - toInt64(s.start_slot), toInt64(%[1]d)), toInt64(0))`,
	shredSlotsPerEpoch)

// seatChargesCTE reconstructs what every client seat was charged in every epoch.
// It is the shared body behind epoch revenue, monthly accrual and the metro
// table, so those three can never disagree about what a seat cost.
//
// The charge is the on-chain price scaled by the slots the seat was active for,
// net of prorated withdraw_seat refunds. Pre-upgrade seats carry neither a last
// price nor a start slot and fall back to the override-or-metro+premium formula
// they were actually billed under. See GetShredEpochRevenue for the derivation.
//
// Exposes epoch, pk, metro_key, charged_dollars and active_slots. Callers add
// their own WHERE against seat_charges; the CTE itself is unfiltered beyond
// dropping deleted rows and epoch 0.
var seatChargesCTE = fmt.Sprintf(`
	seat_per_epoch AS (
		SELECT
			pk,
			active_epoch,
			argMax(device_key, snapshot_ts) AS device_key,
			argMax(has_price_override, snapshot_ts) AS has_override,
			argMax(override_usdc_price_dollars, snapshot_ts) AS override_price,
			max(subscription_start_slot) AS start_slot,
			max(last_usdc_price_dollars) AS last_price
		FROM dim_dz_shred_client_seats_history
		WHERE is_deleted = 0 AND active_epoch > 0
		GROUP BY pk, active_epoch
	),
	device_per_epoch AS (
		SELECT
			device_key,
			current_epoch AS epoch,
			argMax(metro_exchange_key, snapshot_ts) AS metro_key,
			argMax(current_usdc_metro_premium_dollars, snapshot_ts) AS premium
		FROM dim_dz_shred_device_histories_history
		WHERE is_deleted = 0
		GROUP BY device_key, current_epoch
	),
	metro_per_epoch AS (
		SELECT
			exchange_key,
			current_epoch AS epoch,
			argMax(current_usdc_price_dollars, snapshot_ts) AS price
		FROM dim_dz_shred_metro_histories_history
		WHERE is_deleted = 0
		GROUP BY exchange_key, current_epoch
	),
	seat_refunds AS (
		SELECT
			client_seat_pk AS pk,
			intDiv(slot, %[1]d) AS active_epoch,
			sum(coalesce(amount_usdc, 0)) / %[2]d AS refund_dollars
		FROM fact_dz_shred_escrow_events
		WHERE event_type = 'withdraw_seat' AND amount_usdc IS NOT NULL AND status = 'ok'
		GROUP BY pk, active_epoch
	),
	seat_charges AS (
		SELECT
			s.active_epoch AS epoch,
			s.pk AS pk,
			d.metro_key AS metro_key,
			CASE
				WHEN s.last_price > 0 AND s.start_slot > 0 THEN toFloat64(s.last_price) * %[3]s / %[1]d
				WHEN s.has_override = 1 THEN toFloat64(s.override_price)
				ELSE toFloat64(coalesce(m.price, 0)) + toFloat64(coalesce(d.premium, 0))
			END - coalesce(r.refund_dollars, 0) AS charged_dollars,
			CASE
				WHEN s.last_price > 0 AND s.start_slot > 0 THEN %[3]s
				ELSE toInt64(%[1]d)
			END AS active_slots
		FROM seat_per_epoch AS s
		LEFT JOIN device_per_epoch AS d ON s.device_key = d.device_key AND d.epoch = s.active_epoch
		LEFT JOIN metro_per_epoch AS m ON d.metro_key = m.exchange_key AND m.epoch = s.active_epoch
		LEFT JOIN seat_refunds AS r ON r.pk = s.pk AND r.active_epoch = s.active_epoch
	)`, shredSlotsPerEpoch, int(usdcBaseUnitsPerDollar), activeSlotsExpr)

// epochBoundsCTE dates each epoch by the first escrow event recorded in it.
// The chain does not publish epoch boundaries to this warehouse, so the first
// charge of an epoch is the closest observable stand-in for when it opened.
const epochBoundsCTE = `
	epoch_bounds AS (
		SELECT epoch, min(event_ts) AS epoch_start
		FROM fact_dz_shred_escrow_events
		WHERE status = 'ok' AND epoch IS NOT NULL
		GROUP BY epoch
	)`

// epochWindowCTE closes each epoch at the next one's start. The epoch in flight
// has no next, so it gets a nominal 50-hour window; every caller then cuts the
// result at the as-of date, which is what keeps the open epoch from accruing
// into days that have not happened. Requires epoch_bounds.
const epochWindowCTE = `
	epoch_win AS (
		SELECT
			epoch,
			epoch_start,
			if(nxt > epoch_start, nxt, epoch_start + INTERVAL 50 HOUR) AS epoch_end
		FROM (
			SELECT
				epoch,
				epoch_start,
				leadInFrame(epoch_start) OVER (ORDER BY epoch ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS nxt
			FROM epoch_bounds
		)
	)`

// ShredsEconomicsMonth is one calendar month of revenue.
//
// SeatRevenue is accrued, not cash: each epoch's charge is spread across the
// calendar days its window covers and cut at the as-of date, so the open month
// holds only the days that have elapsed. Invoiced is cash the subscription
// program has already allocated to the month, which is why a future month can
// carry one and not the other.
type ShredsEconomicsMonth struct {
	Month         string  `json:"month"` // YYYY-MM
	SeatRevenue   float64 `json:"seat_revenue"`
	Invoiced      float64 `json:"invoiced"`
	InvoiceFeeds  int     `json:"invoice_feeds"`
	Days          int     `json:"days"` // days of seat revenue recognized
	DaysInMonth   int     `json:"days_in_month"`
	Seats         int     `json:"seats"`         // distinct client seats charged
	Subscriptions int     `json:"subscriptions"` // subscription seats live at month end
	Open          bool    `json:"open"`          // the month in progress
	Future        bool    `json:"future"`        // billed ahead, nothing earned yet
}

// ShredsEconomicsEpoch is one epoch of the seat and subscription series.
//
// Subscriptions is the count live at the epoch's end, so the series is read
// left to right as "how many were live by then". The epoch in flight has no end
// yet and carries the count live right now, which is what makes the last point
// agree with the live figures elsewhere on the page.
type ShredsEconomicsEpoch struct {
	Epoch         uint64  `json:"epoch"`
	Day           string  `json:"day"` // YYYY-MM-DD, epoch start
	Seats         int     `json:"seats"`
	Subscriptions int     `json:"subscriptions"`
	Revenue       float64 `json:"revenue"`
}

// ShredsEconomicsMetro is one metro's share of both revenue streams.
//
// Subscriptions do not sum to the live total the way payers would: one payer
// holding seats in three metros counts in all three.
type ShredsEconomicsMetro struct {
	Metro         string  `json:"metro"`
	Price         float64 `json:"price"` // USDC per epoch, the rate card
	Devices       int     `json:"devices"`
	LiveSeats     int     `json:"live_seats"`
	Subscriptions int     `json:"subscriptions"`
	SeatRevenue   float64 `json:"seat_revenue"`
	Invoiced      float64 `json:"invoiced"`
}

// ShredsEconomics is the whole shreds economics page.
type ShredsEconomics struct {
	// AsOf is the UTC day revenue is recognized through. Everything on the page
	// is cut here.
	AsOf         string  `json:"as_of"` // YYYY-MM-DD
	CurrentEpoch uint64  `json:"current_epoch"`
	EpochDays    float64 `json:"epoch_days"` // measured mean epoch length in the window
	// EpochsPerMonth is the rate card's month, not the measured cadence.
	EpochsPerMonth int `json:"epochs_per_month"`

	LiveSeats              int     `json:"live_seats"`     // client seats charged in the current epoch
	LiveSeatRate           float64 `json:"live_seat_rate"` // USDC per epoch across those seats
	LiveSubscriptions      int     `json:"live_subscriptions"`
	LiveSubscriptionPayers int     `json:"live_subscription_payers"`
	MetrosPriced           int     `json:"metros_priced"`

	// SubscriptionsOpenedOn is the day the first Solana Shreds feed seat
	// appeared, empty before any has. Every month before it is a true zero, not
	// missing data, and the page says so rather than leaving a reader to guess.
	SubscriptionsOpenedOn    string `json:"subscriptions_opened_on"` // YYYY-MM-DD
	SubscriptionsOpenedEpoch uint64 `json:"subscriptions_opened_epoch"`

	Months []ShredsEconomicsMonth `json:"months"`
	Epochs []ShredsEconomicsEpoch `json:"epochs"`
	Metros []ShredsEconomicsMetro `json:"metros"`
}

// GetShredsEconomics returns the shreds economics page in one payload.
//
// months bounds the window to that many calendar months ending with the open
// one (default defaultEconomicsMonths, capped at maxEconomicsMonths). Months
// ahead of the window are returned on top of it whenever they carry invoiced
// revenue.
func (a *API) GetShredsEconomics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	months := defaultEconomicsMonths
	if raw := r.URL.Query().Get("months"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxEconomicsMonths {
			http.Error(w, fmt.Sprintf("months must be a whole number between 1 and %d", maxEconomicsMonths), http.StatusBadRequest)
			return
		}
		months = n
	}

	resp, err := a.FetchShredsEconomicsData(ctx, time.Now().UTC(), months)
	if err != nil {
		logError("shreds economics query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

// FetchShredsEconomicsData computes the page. asOf is the instant revenue is
// recognized through; it is a parameter rather than now() in SQL so every query
// in the set cuts at the same moment and tests are deterministic.
//
// A missing dimension resolves to the empty slice for the part that reads it
// rather than failing the request: the feed-subscription and access-pass tables
// ship with the indexer, so an API pod rolled out ahead of it would otherwise
// 500 the whole page over one absent table.
func (a *API) FetchShredsEconomicsData(ctx context.Context, asOf time.Time, months int) (*ShredsEconomics, error) {
	asOf = asOf.UTC()
	asOfDay := asOf.Format(time.DateOnly)
	// The window opens on the first of the month months-1 back, so the count
	// includes the open month.
	windowStart := time.Date(asOf.Year(), asOf.Month(), 1, 0, 0, 0, 0, time.UTC).
		AddDate(0, -(months - 1), 0).Format(time.DateOnly)

	resp := &ShredsEconomics{
		AsOf:           asOfDay,
		EpochsPerMonth: shredEpochsPerMonth,
		Months:         []ShredsEconomicsMonth{},
		Epochs:         []ShredsEconomicsEpoch{},
		Metros:         []ShredsEconomicsMetro{},
	}

	epochs, err := a.economicsEpochSeries(ctx, windowStart, asOfDay)
	if err != nil {
		return nil, err
	}
	subs, err := a.economicsSubscriptionSeries(ctx, windowStart, asOfDay)
	if err != nil {
		return nil, err
	}
	for i := range epochs {
		if s, ok := subs[epochs[i].Epoch]; ok {
			epochs[i].Subscriptions = s.seats
		}
	}
	resp.Epochs = epochs
	if n := len(epochs); n > 0 {
		last := epochs[n-1]
		resp.CurrentEpoch = last.Epoch
		resp.LiveSeats = last.Seats
		resp.LiveSubscriptions = last.Subscriptions
		resp.LiveSubscriptionPayers = subs[last.Epoch].payers
	}
	// The first epoch that ended with a subscription live, reported only when the
	// window actually saw the transition. A window opening after subscriptions
	// began would otherwise name its own first epoch, which is a window boundary
	// and not an opening: SubscriptionsOpenedOn is all-time, and the two must not
	// describe different events.
	for _, e := range epochs {
		if e.Subscriptions == 0 {
			continue
		}
		if e.Epoch != epochs[0].Epoch {
			resp.SubscriptionsOpenedEpoch = e.Epoch
		}
		break
	}

	if resp.Months, err = a.economicsMonths(ctx, windowStart, asOf, epochs); err != nil {
		return nil, err
	}
	if resp.Metros, err = a.economicsMetros(ctx, windowStart, asOf); err != nil {
		return nil, err
	}
	if resp.MetrosPriced, err = a.economicsMetrosPriced(ctx); err != nil {
		return nil, err
	}
	// The live rate is what the seats charged in the current epoch cost at their
	// metro's current price, so it is summed from the metro table rather than
	// re-derived: the two must agree, they sit next to each other on the page.
	for _, m := range resp.Metros {
		resp.LiveSeatRate += m.Price * float64(m.LiveSeats)
	}

	if resp.EpochDays, err = a.economicsEpochDays(ctx, windowStart); err != nil {
		return nil, err
	}
	if resp.SubscriptionsOpenedOn, err = a.economicsFirstSubscriptionDay(ctx); err != nil {
		return nil, err
	}
	return resp, nil
}

// economicsEpochSeries returns each epoch in the window with the seats charged
// in it and what they were charged, oldest first.
func (a *API) economicsEpochSeries(ctx context.Context, windowStart, asOfDay string) ([]ShredsEconomicsEpoch, error) {
	query := `
		WITH ` + seatChargesCTE + `,` + epochBoundsCTE + `
		SELECT
			c.epoch AS epoch,
			toDate(any(b.epoch_start)) AS day,
			toUInt32(uniqExact(c.pk)) AS seats,
			sum(c.charged_dollars) AS revenue
		FROM seat_charges AS c
		INNER JOIN epoch_bounds AS b ON b.epoch = c.epoch
		WHERE b.epoch_start >= toDate(?) AND b.epoch_start < toDate(?) + 1
		GROUP BY epoch
		ORDER BY epoch
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, windowStart, asOfDay)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []ShredsEconomicsEpoch{}
	for rows.Next() {
		var (
			item ShredsEconomicsEpoch
			day  time.Time
			n    uint32
		)
		if err := rows.Scan(&item.Epoch, &day, &n, &item.Revenue); err != nil {
			return nil, err
		}
		item.Day = day.Format(time.DateOnly)
		item.Seats = int(n)
		items = append(items, item)
	}
	return items, rows.Err()
}

// economicsSubscription is the subscription state at one epoch's end.
type economicsSubscription struct {
	seats  int
	payers int
}

// economicsSubscriptionSeries counts the Solana Shreds feed seats live at the
// end of each epoch in the window.
//
// A subscription is one feed_seats entry on an edge_seat access pass, counted
// as a seat and not as a payer: one payer commonly holds seats in several
// metros. The pass dimension is a snapshot history, so the state at a moment is
// the latest snapshot at or before it, per pass.
//
// The epoch in flight is measured at now() rather than at an end it does not
// have, which is what makes the last point of the series the live count.
func (a *API) economicsSubscriptionSeries(ctx context.Context, windowStart, asOfDay string) (map[uint64]economicsSubscription, error) {
	query := `
		WITH ` + epochBoundsCTE + `,
		marks AS (
			SELECT
				epoch,
				epoch_start,
				-- Each epoch is measured at its end, which is the next one's start.
				-- The epoch in flight has no next, so it is measured at the end of
				-- the as-of day: toDate(?) + 1 is the day after, and every epoch's
				-- mark is capped there so none reads past what was recognized.
				least(if(nxt > epoch_start, nxt, toDateTime(toDate(?) + 1)), toDateTime(toDate(?) + 1)) AS as_of
			FROM (
				SELECT
					epoch,
					epoch_start,
					leadInFrame(epoch_start) OVER (ORDER BY epoch ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS nxt
				FROM epoch_bounds
			)
			WHERE epoch_start >= toDate(?) AND epoch_start < toDate(?) + 1
		),
		state AS (
			SELECT
				mk.epoch AS epoch,
				ap.entity_id AS eid,
				argMax(ap.user_payer, ap.snapshot_ts) AS payer,
				argMax(ap.feed_seats, ap.snapshot_ts) AS fs,
				argMax(ap.is_deleted, ap.snapshot_ts) AS del
			FROM marks AS mk
			CROSS JOIN dim_dz_access_passes_history AS ap
			WHERE ap.type_tag = 'edge_seat' AND ap.snapshot_ts <= mk.as_of
			GROUP BY epoch, eid
		),
		entries AS (
			SELECT
				s.epoch AS epoch,
				s.payer AS payer,
				JSONExtractString(e, 'feed_pk') AS feed_pk
			FROM state AS s
			ARRAY JOIN JSONExtractArrayRaw(s.fs) AS e
			WHERE s.del = 0 AND s.fs NOT IN ('', '[]')
		)
		SELECT
			en.epoch AS epoch,
			toUInt32(countIf(startsWith(ifNull(f.code, ''), ?))) AS seats,
			toUInt32(uniqExactIf(en.payer, startsWith(ifNull(f.code, ''), ?))) AS payers
		FROM entries AS en
		LEFT JOIN dz_feeds_current AS f ON f.pk = en.feed_pk
		GROUP BY epoch
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query,
		asOfDay, asOfDay, // the in-flight epoch's as-of, then the cap on every epoch's
		windowStart, asOfDay,
		ShredsFeedCodePrefix, ShredsFeedCodePrefix,
	)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		if isMissingTable(err) {
			logWarn("shreds economics: access pass dimension not available", "error", err)
			return map[uint64]economicsSubscription{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	out := map[uint64]economicsSubscription{}
	for rows.Next() {
		var (
			epoch         uint64
			seats, payers uint32
		)
		if err := rows.Scan(&epoch, &seats, &payers); err != nil {
			return nil, err
		}
		out[epoch] = economicsSubscription{seats: int(seats), payers: int(payers)}
	}
	return out, rows.Err()
}

// economicsMonths merges the three monthly reads into one row per month.
func (a *API) economicsMonths(ctx context.Context, windowStart string, asOf time.Time, epochs []ShredsEconomicsEpoch) ([]ShredsEconomicsMonth, error) {
	asOfDay := asOf.Format(time.DateOnly)
	byMonth := map[string]*ShredsEconomicsMonth{}
	month := func(key string) *ShredsEconomicsMonth {
		m, ok := byMonth[key]
		if !ok {
			m = &ShredsEconomicsMonth{Month: key}
			byMonth[key] = m
		}
		return m
	}

	accrued, err := a.economicsMonthlySeatRevenue(ctx, windowStart, asOfDay)
	if err != nil {
		return nil, err
	}
	for _, row := range accrued {
		m := month(row.Month)
		m.SeatRevenue = row.SeatRevenue
		m.Days = row.Days
	}

	seats, err := a.economicsMonthlySeats(ctx, windowStart, asOfDay)
	if err != nil {
		return nil, err
	}
	for key, n := range seats {
		month(key).Seats = n
	}

	invoiced, err := a.economicsMonthlyInvoiced(ctx, windowStart)
	if err != nil {
		return nil, err
	}
	for _, row := range invoiced {
		m := month(row.Month)
		m.Invoiced = row.Invoiced
		m.InvoiceFeeds = row.InvoiceFeeds
	}

	// Subscription seats live at the month's end: the last epoch that started
	// inside it. The open month's last epoch is the one in flight, which already
	// carries the live count.
	subsAtMonthEnd := map[string]int{}
	for _, e := range epochs {
		if len(e.Day) >= 7 {
			subsAtMonthEnd[e.Day[:7]] = e.Subscriptions
		}
	}

	openKey := asOf.Format("2006-01")
	out := make([]ShredsEconomicsMonth, 0, len(byMonth))
	for _, m := range byMonth {
		m.Open = m.Month == openKey
		m.Future = m.Month > openKey
		m.DaysInMonth = daysInMonth(m.Month)
		if !m.Future {
			m.Subscriptions = subsAtMonthEnd[m.Month]
		}
		out = append(out, *m)
	}
	// Keys are zero-padded YYYY-MM, so string order is chronological.
	slices.SortFunc(out, func(a, b ShredsEconomicsMonth) int {
		return strings.Compare(a.Month, b.Month)
	})
	return out, nil
}

// economicsMonthlySeatRevenue spreads each epoch's charge across the calendar
// days its window covers and sums by month.
//
// This is accrual, not cash. An epoch's charge lands when it is levied but
// covers the days the epoch runs, so an epoch straddling a month boundary is
// split between the two, and the open month holds only its elapsed days. Days
// after the as-of date are cut, which is also what stops the epoch in flight
// from booking revenue for time that has not passed.
func (a *API) economicsMonthlySeatRevenue(ctx context.Context, windowStart, asOfDay string) ([]ShredsEconomicsMonth, error) {
	query := `
		WITH ` + seatChargesCTE + `,` + epochBoundsCTE + `,` + epochWindowCTE + `,
		accrual AS (
			-- Seats sharing an epoch and an active_slots count share a window, so
			-- the spread runs once per distinct window rather than once per seat.
			SELECT
				c.epoch AS epoch,
				c.active_slots AS active_slots,
				sum(c.charged_dollars) AS usdc,
				any(e.epoch_end) AS win_end,
				any(e.epoch_end) - toIntervalSecond(
					toUInt32(dateDiff('second', any(e.epoch_start), any(e.epoch_end)) * c.active_slots / ?)
				) AS win_start
			FROM seat_charges AS c
			INNER JOIN epoch_win AS e ON e.epoch = c.epoch
			WHERE c.active_slots > 0
			GROUP BY c.epoch, c.active_slots
		),
		spread AS (
			SELECT
				a.usdc AS usdc,
				day,
				dateDiff('second',
					greatest(a.win_start, toDateTime(day)),
					least(a.win_end, toDateTime(day) + INTERVAL 1 DAY)
				) AS overlap,
				dateDiff('second', a.win_start, a.win_end) AS total
			FROM accrual AS a
			ARRAY JOIN arrayMap(
				i -> toDate(a.win_start) + i,
				range(toUInt32(dateDiff('day', toDate(a.win_start), toDate(a.win_end)) + 1))
			) AS day
		)
		SELECT
			formatDateTime(toStartOfMonth(day), '%Y-%m') AS month,
			sum(usdc * overlap / total) AS usdc,
			toUInt32(uniqExact(day)) AS days
		FROM spread
		WHERE overlap > 0 AND total > 0
		  AND day >= toDate(?) AND day <= toDate(?)
		GROUP BY month
		-- A month whose charges net out below a cent is the tail of an epoch that
		-- mostly belongs to the month before it, not a month of trading.
		HAVING abs(usdc) >= 0.005
		ORDER BY month
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, shredSlotsPerEpoch, windowStart, asOfDay)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []ShredsEconomicsMonth{}
	for rows.Next() {
		var (
			item ShredsEconomicsMonth
			days uint32
		)
		if err := rows.Scan(&item.Month, &item.SeatRevenue, &days); err != nil {
			return nil, err
		}
		item.Days = int(days)
		items = append(items, item)
	}
	return items, rows.Err()
}

// economicsMonthlySeats counts the distinct client seats charged in each month,
// keyed by the month the epoch opened in. A seat charged in two months counts
// in both: the figure answers "how many seats did we bill this month", not how
// many distinct seats exist.
func (a *API) economicsMonthlySeats(ctx context.Context, windowStart, asOfDay string) (map[string]int, error) {
	query := `
		WITH ` + epochBoundsCTE + `,
		seats AS (
			SELECT pk, active_epoch
			FROM dim_dz_shred_client_seats_history
			WHERE is_deleted = 0 AND active_epoch > 0
			GROUP BY pk, active_epoch
		)
		SELECT
			formatDateTime(toStartOfMonth(b.epoch_start), '%Y-%m') AS month,
			toUInt32(uniqExact(s.pk)) AS seats
		FROM seats AS s
		INNER JOIN epoch_bounds AS b ON b.epoch = s.active_epoch
		WHERE b.epoch_start >= toDate(?) AND b.epoch_start < toDate(?) + 1
		GROUP BY month
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, windowStart, asOfDay)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var (
			month string
			seats uint32
		)
		if err := rows.Scan(&month, &seats); err != nil {
			return nil, err
		}
		out[month] = int(seats)
	}
	return out, rows.Err()
}

// economicsMonthlyInvoiced sums the feed-subscription revenue allocated to each
// month, scoped to the Solana Shreds feeds.
//
// There is no upper bound on the window: the program splits one payment across
// the calendar months the subscription covers, so months ahead of today already
// carry revenue and the page shows them as booked rather than earned.
func (a *API) economicsMonthlyInvoiced(ctx context.Context, windowStart string) ([]ShredsEconomicsMonth, error) {
	query := `
		SELECT
			formatDateTime(makeDate(fd.year, fd.month, 1), '%Y-%m') AS month,
			sum(fd.collected_usdc) / ? AS invoiced,
			-- Feeds that actually billed, not feeds holding an account. A
			-- distribution account is created before it collects anything, so
			-- counting rows would report a feed as invoiced on the strength of an
			-- empty account and the figure would climb without revenue moving.
			toUInt32(uniqExactIf(fd.feed_key, fd.collected_usdc > 0)) AS feeds
		FROM dim_dz_shred_feed_distributions_current AS fd
		LEFT JOIN dz_feeds_current AS f ON f.pk = fd.feed_key
		-- An unlabelled feed is kept, exactly as GetShredFeedRevenue keeps it:
		-- revenue must not vanish because a serviceability label is late.
		WHERE (ifNull(f.code, '') = '' OR startsWith(ifNull(f.code, ''), ?))
		  AND makeDate(fd.year, fd.month, 1) >= toDate(?)
		GROUP BY month
		HAVING invoiced > 0
		ORDER BY month
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, int(usdcBaseUnitsPerDollar), ShredsFeedCodePrefix, windowStart)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		if isMissingTable(err) {
			logWarn("shreds economics: feed distributions not available", "error", err)
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := []ShredsEconomicsMonth{}
	for rows.Next() {
		var (
			item  ShredsEconomicsMonth
			feeds uint32
		)
		if err := rows.Scan(&item.Month, &item.Invoiced, &feeds); err != nil {
			return nil, err
		}
		item.InvoiceFeeds = int(feeds)
		items = append(items, item)
	}
	return items, rows.Err()
}

// economicsMetros returns both revenue streams per metro, largest first.
//
// Seat revenue is the charge levied in the window's epochs; invoices are the
// revenue allocated to the window's recognized months, so months booked ahead
// stay out of a table read as earnings to date. Invoices reach a metro through
// the feed they bill, which carries the metro on dz_feeds_current.
//
// The join is a full outer one because the two streams do not cover the same
// metros: a metro can hold subscriptions with no seat ever charged, and the
// reverse.
func (a *API) economicsMetros(ctx context.Context, windowStart string, asOf time.Time) ([]ShredsEconomicsMetro, error) {
	invoiceEnd := time.Date(asOf.Year(), asOf.Month(), 1, 0, 0, 0, 0, time.UTC).Format(time.DateOnly)
	asOfDay := asOf.Format(time.DateOnly)

	query := `
		WITH ` + seatChargesCTE + `,` + epochBoundsCTE + `,
		window_epochs AS (
			SELECT epoch FROM epoch_bounds
			WHERE epoch_start >= toDate(?) AND epoch_start < toDate(?) + 1
		),
		current_epoch AS (SELECT max(epoch) AS epoch FROM window_epochs),
		metro_rate AS (
			SELECT
				exchange_key AS ek,
				argMax(current_usdc_price_dollars, snapshot_ts) AS price,
				argMax(total_initialized_devices, snapshot_ts) AS devices
			FROM dim_dz_shred_metro_histories_current
			GROUP BY exchange_key
		),
		metro_code AS (SELECT pk AS mpk, any(code) AS code FROM dz_metros_current GROUP BY pk),
		feed_map AS (SELECT pk AS fpk, any(code) AS code, any(metro_pk) AS mpk FROM dz_feeds_current GROUP BY pk),
		seat_agg AS (
			SELECT
				c.metro_key AS mk,
				sum(c.charged_dollars) AS seat_usdc,
				toUInt32(uniqExactIf(c.pk, c.epoch = (SELECT epoch FROM current_epoch))) AS live_seats
			FROM seat_charges AS c
			WHERE c.epoch IN (SELECT epoch FROM window_epochs)
			GROUP BY mk
		),
		inv_agg AS (
			SELECT fm.mpk AS mk, sum(fd.collected_usdc) / ? AS invoiced
			FROM dim_dz_shred_feed_distributions_current AS fd
			INNER JOIN feed_map AS fm ON fm.fpk = fd.feed_key
			WHERE startsWith(fm.code, ?)
			  AND makeDate(fd.year, fd.month, 1) >= toDate(?)
			  AND makeDate(fd.year, fd.month, 1) <= toDate(?)
			GROUP BY mk
		),
		pass_state AS (
			SELECT
				entity_id,
				argMax(feed_seats, snapshot_ts) AS fs,
				argMax(is_deleted, snapshot_ts) AS del
			FROM dim_dz_access_passes_history
			WHERE type_tag = 'edge_seat'
			GROUP BY entity_id
		),
		sub_agg AS (
			SELECT fm.mpk AS mk, toUInt32(count()) AS sub_seats
			FROM pass_state AS p
			ARRAY JOIN JSONExtractArrayRaw(p.fs) AS e
			INNER JOIN feed_map AS fm ON fm.fpk = JSONExtractString(e, 'feed_pk')
			WHERE p.del = 0 AND p.fs NOT IN ('', '[]') AND startsWith(fm.code, ?)
			GROUP BY mk
		)
		SELECT
			ifNull(mc.code, '') AS metro,
			toFloat64(ifNull(mr.price, 0)) AS price,
			toUInt32(ifNull(mr.devices, 0)) AS devices,
			ifNull(sa.live_seats, 0) AS live_seats,
			ifNull(sb.sub_seats, 0) AS subscriptions,
			ifNull(sa.seat_usdc, 0) AS seat_revenue,
			ifNull(ia.invoiced, 0) AS invoiced
		FROM seat_agg AS sa
		FULL OUTER JOIN sub_agg AS sb ON sb.mk = sa.mk
		LEFT JOIN metro_code AS mc ON mc.mpk = coalesce(nullIf(sa.mk, ''), sb.mk)
		LEFT JOIN metro_rate AS mr ON mr.ek = coalesce(nullIf(sa.mk, ''), sb.mk)
		LEFT JOIN inv_agg AS ia ON ia.mk = coalesce(nullIf(sa.mk, ''), sb.mk)
		-- A seat whose device carries no metro produces an unnamed row with
		-- nothing on it. Drop it, but keep any row that has something to report.
		WHERE metro != '' OR live_seats > 0 OR subscriptions > 0 OR seat_revenue != 0 OR invoiced != 0
		ORDER BY seat_revenue + invoiced DESC, metro
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query,
		windowStart, asOfDay, // window_epochs
		int(usdcBaseUnitsPerDollar), ShredsFeedCodePrefix, windowStart, invoiceEnd, // inv_agg
		ShredsFeedCodePrefix, // sub_agg
	)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		if isMissingTable(err) {
			logWarn("shreds economics: metro breakdown not available", "error", err)
			return []ShredsEconomicsMetro{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := []ShredsEconomicsMetro{}
	for rows.Next() {
		var (
			item             ShredsEconomicsMetro
			devices          uint32
			liveSeats, subs  uint32
			seatRev, invoice float64
		)
		if err := rows.Scan(&item.Metro, &item.Price, &devices, &liveSeats, &subs, &seatRev, &invoice); err != nil {
			return nil, err
		}
		if item.Metro == "" {
			item.Metro = "unmapped"
		}
		item.Devices = int(devices)
		item.LiveSeats = int(liveSeats)
		item.Subscriptions = int(subs)
		item.SeatRevenue = seatRev
		item.Invoiced = invoice
		items = append(items, item)
	}
	return items, rows.Err()
}

// economicsMetrosPriced counts the metros carrying a rate-card price, including
// the ones that have never sold a seat: the card is what a seat would cost, not
// what one did. It is the denominator behind the metro table's "of N priced".
func (a *API) economicsMetrosPriced(ctx context.Context) (int, error) {
	query := `
		WITH metro_rate AS (
			SELECT exchange_key AS ek, argMax(current_usdc_price_dollars, snapshot_ts) AS price
			FROM dim_dz_shred_metro_histories_current
			GROUP BY exchange_key
		),
		metro_code AS (SELECT pk AS mpk, any(code) AS code FROM dz_metros_current GROUP BY pk)
		SELECT toUInt32(count())
		FROM metro_rate AS mr
		INNER JOIN metro_code AS mc ON mc.mpk = mr.ek
		WHERE mr.price > 0 AND mc.code != ''
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var n uint32
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return int(n), rows.Err()
}

// economicsEpochDays is the mean gap between epoch starts in the window. The
// page projects the open month to its end at the rate the live seats imply, and
// that rate is per epoch, so it needs to know how long an epoch actually runs -
// which is not the 15-epoch month the rate card quotes.
func (a *API) economicsEpochDays(ctx context.Context, windowStart string) (float64, error) {
	query := `
		SELECT avg(gap_hours) / 24 AS epoch_days
		FROM (
			SELECT
				epoch_start,
				dateDiff('hour', lagInFrame(epoch_start) OVER (ORDER BY epoch), epoch_start) AS gap_hours
			FROM (
				SELECT epoch, min(event_ts) AS epoch_start
				FROM fact_dz_shred_escrow_events
				WHERE status = 'ok' AND epoch IS NOT NULL
				GROUP BY epoch
			)
		)
		WHERE gap_hours > 0 AND epoch_start >= toDate(?)
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, windowStart)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var days float64
	if rows.Next() {
		if err := rows.Scan(&days); err != nil {
			return 0, err
		}
	}
	// avg() over no rows is NaN, which json.Encode refuses outright - one epoch
	// in the window, or none, would otherwise fail the whole response.
	if math.IsNaN(days) || math.IsInf(days, 0) {
		days = 0
	}
	return days, rows.Err()
}

// economicsFirstSubscriptionDay is the day the first Solana Shreds feed seat
// appeared on an access pass, or "" if none has.
func (a *API) economicsFirstSubscriptionDay(ctx context.Context) (string, error) {
	query := `
		WITH entries AS (
			SELECT ap.snapshot_ts AS ts, JSONExtractString(e, 'feed_pk') AS feed_pk
			FROM dim_dz_access_passes_history AS ap
			ARRAY JOIN JSONExtractArrayRaw(ap.feed_seats) AS e
			WHERE ap.type_tag = 'edge_seat' AND ap.is_deleted = 0 AND ap.feed_seats NOT IN ('', '[]')
		)
		SELECT toDate(min(en.ts)) AS first_day, count() AS n
		FROM entries AS en
		LEFT JOIN dz_feeds_current AS f ON f.pk = en.feed_pk
		WHERE startsWith(ifNull(f.code, ''), ?)
	`

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, ShredsFeedCodePrefix)
	metrics.RecordClickHouseQuery("shreds", time.Since(start), err)
	if err != nil {
		if isMissingTable(err) {
			logWarn("shreds economics: access pass dimension not available", "error", err)
			return "", nil
		}
		return "", err
	}
	defer rows.Close()

	var (
		day time.Time
		n   uint64
	)
	if rows.Next() {
		if err := rows.Scan(&day, &n); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	// min() over no rows returns the zero date, which would read as a real day.
	if n == 0 {
		return "", nil
	}
	return day.Format(time.DateOnly), nil
}

// daysInMonth returns the length of a YYYY-MM month, 0 if it does not parse.
func daysInMonth(month string) int {
	t, err := time.Parse("2006-01", month)
	if err != nil {
		return 0
	}
	return t.AddDate(0, 1, -1).Day()
}
