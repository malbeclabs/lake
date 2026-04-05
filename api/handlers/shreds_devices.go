package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// ShredDeviceItem represents a device with pricing and seat info for the devices page.
type ShredDeviceItem struct {
	DeviceKey         string `json:"device_key"`
	DeviceCode        string `json:"device_code"`
	MetroExchangeKey  string `json:"metro_exchange_key"`
	MetroCode         string `json:"metro_code"`
	IsEnabled         uint8  `json:"is_enabled"`
	BasePriceDollars  uint16 `json:"base_price_dollars"`
	PremiumDollars    int16  `json:"premium_dollars"`
	TotalPriceDollars int64  `json:"total_price_dollars"`
	GrantedSeats      uint16 `json:"granted_seats"`
	Capacity          uint16 `json:"capacity"`
	AvailableSeats    int64  `json:"available_seats"`
}

var deviceSortFields = map[string]string{
	"device":    "device_code",
	"metro":     "metro_code",
	"price":     "total_price_dollars",
	"granted":   "granted_seats",
	"capacity":  "capacity",
	"available": "available_seats",
}

var deviceFilterFields = map[string]FilterFieldConfig{
	"device":    {Column: "device_code", Type: FieldTypeText},
	"metro":     {Column: "metro_code", Type: FieldTypeText},
	"price":     {Column: "total_price_dollars", Type: FieldTypeNumeric},
	"granted":   {Column: "granted_seats", Type: FieldTypeNumeric},
	"capacity":  {Column: "capacity", Type: FieldTypeNumeric},
	"available": {Column: "available_seats", Type: FieldTypeNumeric},
}

func (a *API) GetShredDevices(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "granted", deviceSortFields)
	filters := ParseFilters(r)

	start := time.Now()

	// Build WHERE clause.
	filterClause, filterArgs := filters.BuildFilterClause(deviceFilterFields)
	whereSQL := ""
	if filterClause != "" {
		whereSQL = " WHERE " + filterClause
	}

	// Wrap the base query so filters and sorting work on computed columns.
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
			toInt32(dh.active_total_available_seats) - toInt32(dh.active_granted_seats) as available_seats
		FROM dim_dz_shred_device_histories_current dh
		LEFT JOIN dz_devices_current d ON dh.device_key = d.pk
		LEFT JOIN dz_metros_current m ON dh.metro_exchange_key = m.pk
		LEFT JOIN dim_dz_shred_metro_histories_current mh ON mh.exchange_key = dh.metro_exchange_key
	`

	// Count query.
	countQuery := `SELECT count(*) FROM (` + baseQuery + `) sub` + whereSQL
	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, countQuery, filterArgs...).Scan(&total); err != nil {
		logError("shred devices count failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Data query.
	orderBy := sort.OrderByClause(deviceSortFields)
	query := `SELECT * FROM (` + baseQuery + `) sub` + whereSQL + ` ` + orderBy + ` LIMIT ? OFFSET ?`
	queryArgs := append(filterArgs, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred devices query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
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
		); err != nil {
			logError("shred devices row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		items = append(items, d)
	}
	if items == nil {
		items = []ShredDeviceItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(PaginatedResponse[ShredDeviceItem]{
		Items: items, Total: int(total), Limit: pagination.Limit, Offset: pagination.Offset,
	}); err != nil {
		logError("failed to encode shred devices response", "error", err)
	}
}
