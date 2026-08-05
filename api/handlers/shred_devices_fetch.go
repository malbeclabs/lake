package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// FetchEdgeShredsDevices returns a paginated page of shred devices for the env
// in ctx, sorted by device_code ASC so the v1 response is deterministic. This
// is the narrowed v1 surface — no filter / sort knobs, just pagination —
// intentionally decoupled from the broader internal GetShredDevices handler
// which still exposes filter/sort for the lake UI.
//
// Returns the same ShredDeviceItem rows the legacy handler returns; the v1 op
// is responsible for shaping them into the public response.
func (a *API) FetchEdgeShredsDevices(ctx context.Context, limit, offset int) ([]ShredDeviceItem, uint64, error) {
	start := time.Now()

	baseQuery := `
		SELECT
			dh.device_key,
			COALESCE(d.code, '') as device_code,
			dh.metro_exchange_key,
			COALESCE(m.code, '') as metro_code,
			dh.is_enabled,
			COALESCE(mh.current_usdc_price_dollars, 0) as base_price_dollars,
			dh.current_usdc_metro_premium_dollars as premium_dollars,
			toInt64(COALESCE(mh.current_usdc_price_dollars, 0)) + toInt64(dh.current_usdc_metro_premium_dollars) as total_price_dollars,
			dh.active_granted_seats as granted_seats,
			dh.active_total_available_seats as capacity,
			toInt32(dh.active_total_available_seats) - toInt32(dh.active_granted_seats) as available_seats,
			COALESCE(mh.retransmit_only_enabled, 0) as retransmit_only_enabled
		FROM dim_dz_shred_device_histories_current dh
		LEFT JOIN dz_devices_current d ON dh.device_key = d.pk
		LEFT JOIN dz_metros_current m ON dh.metro_exchange_key = m.pk
		LEFT JOIN dim_dz_shred_metro_histories_current mh ON mh.exchange_key = dh.metro_exchange_key
	`

	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, `SELECT count(*) FROM dim_dz_shred_device_histories_current`).Scan(&total); err != nil {
		metrics.RecordClickHouseQuery("edge_shreds_devices", time.Since(start), err)
		return nil, 0, err
	}

	query := baseQuery + ` ORDER BY device_code ASC, dh.device_key ASC LIMIT ? OFFSET ?`
	rows, err := a.envDB(ctx).Query(ctx, query, limit, offset)
	metrics.RecordClickHouseQuery("edge_shreds_devices", time.Since(start), err)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var items []ShredDeviceItem
	for rows.Next() {
		var d ShredDeviceItem
		if err := rows.Scan(
			&d.DeviceKey, &d.DeviceCode,
			&d.MetroExchangeKey, &d.MetroCode,
			&d.IsEnabled,
			&d.BasePriceDollars, &d.PremiumDollars, &d.TotalPriceDollars,
			&d.GrantedSeats, &d.Capacity, &d.AvailableSeats,
			&d.RetransmitOnlyEnabled,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}
