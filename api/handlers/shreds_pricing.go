package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// ShredPricingItem represents per-device pricing for the subscribe flow.
type ShredPricingItem struct {
	DeviceKey         string `json:"device_key"`
	DeviceCode        string `json:"device_code"`
	MetroExchangeKey  string `json:"metro_exchange_key"`
	MetroCode         string `json:"metro_code"`
	IsEnabled         uint8  `json:"is_enabled"`
	BasePriceDollars  uint16 `json:"base_price_dollars"`
	PremiumDollars    int16  `json:"premium_dollars"`
	TotalPriceDollars int64  `json:"total_price_dollars"`
	GrantedSeats      uint16 `json:"granted_seats"`
	AvailableSeats    uint16 `json:"available_seats"`
	IsPriceFinalized  uint8  `json:"is_price_finalized"`
	CurrentEpoch      uint64 `json:"current_epoch"`
}

func (a *API) GetShredPricing(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()

	query := `
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
			dh.active_total_available_seats as available_seats,
			COALESCE(mh.is_current_price_finalized, 0) as is_price_finalized,
			COALESCE(mh.current_epoch, 0) as current_epoch
		FROM dim_dz_shred_device_histories_current dh
		LEFT JOIN dz_devices_current d ON dh.device_key = d.pk
		LEFT JOIN dz_metros_current m ON dh.metro_exchange_key = m.pk
		LEFT JOIN dim_dz_shred_metro_histories_current mh ON mh.exchange_key = dh.metro_exchange_key
		WHERE dh.is_enabled = 1
		ORDER BY metro_code ASC, device_code ASC
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred pricing query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ShredPricingItem
	for rows.Next() {
		var p ShredPricingItem
		if err := rows.Scan(
			&p.DeviceKey, &p.DeviceCode,
			&p.MetroExchangeKey, &p.MetroCode,
			&p.IsEnabled,
			&p.BasePriceDollars, &p.PremiumDollars, &p.TotalPriceDollars,
			&p.GrantedSeats, &p.AvailableSeats,
			&p.IsPriceFinalized, &p.CurrentEpoch,
		); err != nil {
			logError("shred pricing row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		items = append(items, p)
	}
	if items == nil {
		items = []ShredPricingItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(items); err != nil {
		logError("failed to encode shred pricing response", "error", err)
	}
}
