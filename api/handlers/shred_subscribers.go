package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// fetchShredSeatByClientIP returns the Shreds client seat for a given client IP,
// or nil if none exists. Used to link access passes managed by the Shreds product to
// their corresponding subscription seat.
func (a *API) fetchShredSeatByClientIP(ctx context.Context, clientIP string) (*ShredSubscriberRow, error) {
	if clientIP == "" {
		return nil, nil
	}

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, `
		WITH `+escrowBalancesCTE+`
		SELECT
			s.pk, s.device_key, COALESCE(d.code, '') as device_code,
			COALESCE(d.metro_pk, '') as metro_pk, COALESCE(m.code, '') as metro_code,
			s.client_ip, s.tenure_epochs, s.funded_epoch, s.active_epoch,
			s.has_price_override, s.override_usdc_price_dollars, s.escrow_count,
			COALESCE(eb.spendable_usdc_balance, 0) as spendable_usdc_balance,
			COALESCE(eb.all_escrows_usdc_balance, 0) as all_escrows_usdc_balance,
			CASE
				WHEN s.has_price_override = 1 THEN toInt64(s.override_usdc_price_dollars)
				ELSE toInt64(COALESCE(mh.current_usdc_price_dollars, 0)) + toInt64(COALESCE(dh.current_usdc_metro_premium_dollars, 0))
			END as price_per_epoch_dollars,
			s.funding_authority_key,
			COALESCE(u.pk, '') as user_pk,
			COALESCE(u.owner_pubkey, '') as user_owner_pubkey,
			COALESCE(u.status, '') as user_status,
			NULL as last_activity
		FROM dim_dz_shred_client_seats_current s
		LEFT JOIN dz_devices_current d ON s.device_key = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dim_dz_shred_metro_histories_current mh ON mh.exchange_key = d.metro_pk
		LEFT JOIN dim_dz_shred_device_histories_current dh ON dh.device_key = s.device_key
		ANY LEFT JOIN dz_users_current u ON u.device_pk = s.device_key AND u.client_ip = s.client_ip
		LEFT JOIN escrow_balances eb ON eb.client_seat_key = s.pk
		WHERE s.client_ip = ?
		LIMIT 1
	`, clientIP)
	metrics.RecordClickHouseQuery("shred_subscribers", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}

	var s ShredSubscriberRow
	if err := rows.Scan(
		&s.PK, &s.DeviceKey, &s.DeviceCode, &s.MetroPK, &s.MetroCode,
		&s.ClientIP, &s.TenureEpochs, &s.FundedEpoch, &s.ActiveEpoch,
		&s.HasPriceOverride, &s.OverrideUSDCPriceDollars, &s.EscrowCount,
		&s.SpendableUSDCBalance, &s.AllEscrowsUSDCBalance,
		&s.PricePerEpochDollars, &s.FundingAuthorityKey,
		&s.UserPK, &s.UserOwnerPubkey, &s.UserStatus, &s.LastActivity,
	); err != nil {
		return nil, err
	}
	return &s, rows.Err()
}

// ShredSubscriberRow is the raw, internal shape of a shred subscriber (client seat)
// returned by FetchShredSubscribers. Consumers (including the v1 API) map this
// to their own public shapes.
type ShredSubscriberRow struct {
	PK                       string
	DeviceKey                string
	DeviceCode               string
	MetroPK                  string
	MetroCode                string
	ClientIP                 string
	TenureEpochs             uint16
	FundedEpoch              uint64
	ActiveEpoch              uint64
	HasPriceOverride         uint8
	OverrideUSDCPriceDollars uint16
	EscrowCount              uint32
	// SpendableUSDCBalance is the largest single escrow balance (max, not sum):
	// the oracle evaluates activation/renewal per-escrow, so a charge must be
	// covered by one escrow. AllEscrowsUSDCBalance is the across-escrow sum,
	// retained for operator visibility into stranded funds.
	SpendableUSDCBalance  uint64
	AllEscrowsUSDCBalance uint64
	PricePerEpochDollars  int64
	FundingAuthorityKey   string
	UserPK                string
	UserOwnerPubkey       string
	UserStatus            string
	LastActivity          *time.Time
}

// FetchShredSubscribers returns a page of shred subscribers (client seats),
// optionally filtered by funder pubkey (funding_authority_key, exact match).
// Ordered by active_epoch DESC, pk ASC.
func (a *API) FetchShredSubscribers(ctx context.Context, funder string, limit, offset int) ([]ShredSubscriberRow, uint64, error) {
	var whereSQL string
	var whereArgs []any
	if funder != "" {
		whereSQL = " WHERE s.funding_authority_key = ?"
		whereArgs = append(whereArgs, funder)
	}

	start := time.Now()

	countQuery := `
		SELECT count(*)
		FROM dim_dz_shred_client_seats_current s
	` + whereSQL

	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		metrics.RecordClickHouseQuery("shred_subscribers", time.Since(start), err)
		return nil, 0, err
	}

	query := `
		WITH ` + escrowBalancesCTE + `,
		last_events AS (
			SELECT client_seat_pk, max(event_ts) as last_activity
			FROM fact_dz_shred_escrow_events FINAL
			GROUP BY client_seat_pk
		)
		SELECT
			s.pk, s.device_key, COALESCE(d.code, '') as device_code,
			COALESCE(d.metro_pk, '') as metro_pk, COALESCE(m.code, '') as metro_code,
			s.client_ip, s.tenure_epochs, s.funded_epoch, s.active_epoch,
			s.has_price_override, s.override_usdc_price_dollars, s.escrow_count,
			COALESCE(eb.spendable_usdc_balance, 0) as spendable_usdc_balance,
			COALESCE(eb.all_escrows_usdc_balance, 0) as all_escrows_usdc_balance,
			CASE
				WHEN s.has_price_override = 1 THEN toInt64(s.override_usdc_price_dollars)
				ELSE toInt64(COALESCE(mh.current_usdc_price_dollars, 0)) + toInt64(COALESCE(dh.current_usdc_metro_premium_dollars, 0))
			END as price_per_epoch_dollars,
			s.funding_authority_key,
			COALESCE(u.pk, '') as user_pk,
			COALESCE(u.owner_pubkey, '') as user_owner_pubkey,
			COALESCE(u.status, '') as user_status,
			le.last_activity as last_activity
		FROM dim_dz_shred_client_seats_current s
		LEFT JOIN dz_devices_current d ON s.device_key = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dim_dz_shred_metro_histories_current mh ON mh.exchange_key = d.metro_pk
		LEFT JOIN dim_dz_shred_device_histories_current dh ON dh.device_key = s.device_key
		ANY LEFT JOIN dz_users_current u ON u.device_pk = s.device_key AND u.client_ip = s.client_ip
		LEFT JOIN escrow_balances eb ON eb.client_seat_key = s.pk
		LEFT JOIN last_events le ON le.client_seat_pk = s.pk
	` + whereSQL + ` ORDER BY s.active_epoch DESC, s.pk ASC LIMIT ? OFFSET ?`

	queryArgs := append(whereArgs, limit, offset)

	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	metrics.RecordClickHouseQuery("shred_subscribers", time.Since(start), err)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var items []ShredSubscriberRow
	for rows.Next() {
		var s ShredSubscriberRow
		if err := rows.Scan(
			&s.PK, &s.DeviceKey, &s.DeviceCode, &s.MetroPK, &s.MetroCode,
			&s.ClientIP, &s.TenureEpochs, &s.FundedEpoch, &s.ActiveEpoch,
			&s.HasPriceOverride, &s.OverrideUSDCPriceDollars, &s.EscrowCount,
			&s.SpendableUSDCBalance, &s.AllEscrowsUSDCBalance,
			&s.PricePerEpochDollars, &s.FundingAuthorityKey,
			&s.UserPK, &s.UserOwnerPubkey, &s.UserStatus, &s.LastActivity,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, s)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}
