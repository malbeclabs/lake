package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// ShredsOverview is a summary of the shred subscription program state.
type ShredsOverview struct {
	// Execution controller state.
	Phase                    string `json:"phase"`
	CurrentSubscriptionEpoch uint64 `json:"current_subscription_epoch"`
	TotalMetros              uint16 `json:"total_metros"`
	TotalEnabledDevices      uint16 `json:"total_enabled_devices"`
	TotalClientSeats         uint32 `json:"total_client_seats"`
	SettledDevicesCount      uint16 `json:"settled_devices_count"`
	SettledClientSeatsCount  uint16 `json:"settled_client_seats_count"`
	NextSeatFundingIndex     uint64 `json:"next_seat_funding_index"`

	// Current Solana epoch (for determining active/inactive seats).
	CurrentSolanaEpoch uint64 `json:"current_solana_epoch"`

	// Aggregate counts.
	ClientSeatCount            uint64 `json:"client_seat_count"`
	PaymentEscrowCount         uint64 `json:"payment_escrow_count"`
	MetroHistoryCount          uint64 `json:"metro_history_count"`
	DeviceHistoryCount         uint64 `json:"device_history_count"`
	ValidatorClientRewardCount uint64 `json:"validator_client_reward_count"`
}

func (a *API) GetShredsOverview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()

	// Fetch execution controller singleton.
	ecQuery := `
		SELECT
			COALESCE(phase, '') as phase,
			current_subscription_epoch,
			total_metros,
			total_enabled_devices,
			total_client_seats,
			settled_devices_count,
			settled_client_seats_count,
			next_seat_funding_index
		FROM dim_dz_shred_execution_controller_current
		LIMIT 1
	`

	var overview ShredsOverview
	err := a.envDB(ctx).QueryRow(ctx, ecQuery).Scan(
		&overview.Phase,
		&overview.CurrentSubscriptionEpoch,
		&overview.TotalMetros,
		&overview.TotalEnabledDevices,
		&overview.TotalClientSeats,
		&overview.SettledDevicesCount,
		&overview.SettledClientSeatsCount,
		&overview.NextSeatFundingIndex,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		// If no execution controller exists yet, return empty overview.
		overview = ShredsOverview{}
	}

	// Fetch aggregate counts in parallel-ish (sequential but fast).
	// Fetch current Solana epoch.
	var solanaEpoch int64
	if err := a.envDB(ctx).QueryRow(ctx, `SELECT max(epoch) FROM solana_vote_accounts_current`).Scan(&solanaEpoch); err != nil {
		logError("failed to fetch current solana epoch", "error", err)
	}
	overview.CurrentSolanaEpoch = uint64(solanaEpoch)

	countQueries := []struct {
		query string
		dest  *uint64
	}{
		{"SELECT count(*) FROM dim_dz_shred_client_seats_current", &overview.ClientSeatCount},
		{"SELECT count(*) FROM dim_dz_shred_payment_escrows_current", &overview.PaymentEscrowCount},
		{"SELECT count(*) FROM dim_dz_shred_metro_histories_current", &overview.MetroHistoryCount},
		{"SELECT count(*) FROM dim_dz_shred_device_histories_current", &overview.DeviceHistoryCount},
		{"SELECT count(*) FROM dim_dz_shred_validator_client_rewards_current", &overview.ValidatorClientRewardCount},
	}

	for _, cq := range countQueries {
		if err := a.envDB(ctx).QueryRow(ctx, cq.query).Scan(cq.dest); err != nil {
			// Tables may not exist yet; treat as zero.
			*cq.dest = 0
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(overview); err != nil {
		logError("failed to encode shreds overview", "error", err)
	}
}

// ShredClientSeatItem represents a client seat in list responses.
type ShredClientSeatItem struct {
	PK                       string `json:"pk"`
	DeviceKey                string `json:"device_key"`
	DeviceCode               string `json:"device_code"`
	MetroPK                  string `json:"metro_pk"`
	MetroCode                string `json:"metro_code"`
	ClientIP                 string `json:"client_ip"`
	TenureEpochs             uint16 `json:"tenure_epochs"`
	FundedEpoch              uint64 `json:"funded_epoch"`
	ActiveEpoch              uint64 `json:"active_epoch"`
	HasPriceOverride         uint8  `json:"has_price_override"`
	OverrideUSDCPriceDollars uint16 `json:"override_usdc_price_dollars"`
	EscrowCount              uint32 `json:"escrow_count"`
	TotalUSDCBalance         uint64 `json:"total_usdc_balance"`
	PricePerEpochDollars     int64  `json:"price_per_epoch_dollars"`
	FundingAuthorityKey      string `json:"funding_authority_key"`
	UserPK                   string `json:"user_pk"`
	UserOwnerPubkey          string `json:"user_owner_pubkey"`
	UserStatus               string `json:"user_status"`
	LastActivity             string `json:"last_activity"`
}

var seatSortFields = map[string]string{
	"seat":          "s.pk",
	"device":        "device_code",
	"metro":         "metro_code",
	"ip":            "s.client_ip",
	"tenure":        "s.tenure_epochs",
	"active_epoch":  "s.active_epoch",
	"funder":        "s.funding_authority_key",
	"balance":       "total_usdc_balance",
	"prepaid":       "price_per_epoch_dollars",
	"last_activity": "last_activity",
}

var seatFilterFields = map[string]FilterFieldConfig{
	"seat":    {Column: "s.pk", Type: FieldTypeText},
	"device":  {Column: "COALESCE(d.code, s.device_key)", Type: FieldTypeText},
	"metro":   {Column: "COALESCE(m.code, '')", Type: FieldTypeText},
	"ip":      {Column: "s.client_ip", Type: FieldTypeText},
	"funder":  {Column: "s.funding_authority_key", Type: FieldTypeText},
	"tenure":  {Column: "s.tenure_epochs", Type: FieldTypeNumeric},
	"epoch":   {Column: "s.active_epoch", Type: FieldTypeNumeric},
	"balance": {Column: "COALESCE(eb.total_usdc_balance, 0)", Type: FieldTypeNumeric},
}

func (a *API) GetShredClientSeats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "active_epoch", seatSortFields)
	filters := ParseFilters(r)

	// Status filter: active, inactive, closed (comma-separated).
	statusParam := r.URL.Query().Get("status")

	start := time.Now()

	// Build WHERE clause.
	var whereClauses []string
	var whereArgs []any

	filterClause, filterArgs := filters.BuildFilterClause(seatFilterFields)
	if filterClause != "" {
		whereClauses = append(whereClauses, filterClause)
		whereArgs = append(whereArgs, filterArgs...)
	}

	// Status filtering requires current Solana epoch.
	if statusParam != "" {
		var solanaEpoch int64
		_ = a.envDB(ctx).QueryRow(ctx, `SELECT max(epoch) FROM solana_vote_accounts_current`).Scan(&solanaEpoch)

		statuses := make(map[string]bool)
		for _, s := range splitCSV(statusParam) {
			statuses[s] = true
		}

		// Price expression used for prepaid epoch calculation.
		priceExpr := `CASE
			WHEN s.has_price_override = 1 THEN toInt64(s.override_usdc_price_dollars)
			ELSE toInt64(COALESCE(mh.current_usdc_price_dollars, 0)) + toInt64(COALESCE(dh.current_usdc_metro_premium_dollars, 0))
		END`
		prepaidExpr := `CASE WHEN ` + priceExpr + ` > 0 THEN intDiv(COALESCE(eb.total_usdc_balance, 0) / 1000000, ` + priceExpr + `) ELSE 0 END`

		var statusOr []string
		if statuses["active"] {
			// Active but NOT expiring (prepaid >= 2).
			statusOr = append(statusOr, "(s.active_epoch >= ? AND s.escrow_count > 0 AND "+prepaidExpr+" >= 2)")
			whereArgs = append(whereArgs, solanaEpoch)
		}
		if statuses["expiring"] {
			// Active but expiring soon (prepaid < 2).
			statusOr = append(statusOr, "(s.active_epoch >= ? AND s.escrow_count > 0 AND "+prepaidExpr+" < 2)")
			whereArgs = append(whereArgs, solanaEpoch)
		}
		if statuses["pending"] {
			// Funded but not yet active (balance covers at least 1 epoch).
			statusOr = append(statusOr, "(s.active_epoch < ? AND s.escrow_count > 0 AND "+prepaidExpr+" >= 1)")
			whereArgs = append(whereArgs, solanaEpoch)
		}
		if statuses["inactive"] {
			// Expired: not active, insufficient balance for next epoch.
			statusOr = append(statusOr, "(s.active_epoch < ? AND s.escrow_count > 0 AND "+prepaidExpr+" < 1)")
			whereArgs = append(whereArgs, solanaEpoch)
		}
		if statuses["closed"] {
			statusOr = append(statusOr, "(s.escrow_count = 0)")
		}
		if len(statusOr) > 0 {
			whereClauses = append(whereClauses, "("+strings.Join(statusOr, " OR ")+")")
		}
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = " WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Count query.
	countQuery := `
		WITH escrow_balances AS (
			SELECT client_seat_key, sum(usdc_balance) as total_usdc_balance
			FROM dim_dz_shred_payment_escrows_current
			GROUP BY client_seat_key
		)
		SELECT count(*)
		FROM dim_dz_shred_client_seats_current s
		LEFT JOIN dz_devices_current d ON s.device_key = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dim_dz_shred_metro_histories_current mh ON mh.exchange_key = d.metro_pk
		LEFT JOIN dim_dz_shred_device_histories_current dh ON dh.device_key = s.device_key
		LEFT JOIN escrow_balances eb ON eb.client_seat_key = s.pk
	` + whereSQL

	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		logError("shred client seats count failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Data query.
	orderBy := sort.OrderByClause(seatSortFields)
	query := `
		WITH escrow_balances AS (
			SELECT client_seat_key, sum(usdc_balance) as total_usdc_balance
			FROM dim_dz_shred_payment_escrows_current
			GROUP BY client_seat_key
		),
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
			COALESCE(eb.total_usdc_balance, 0) as total_usdc_balance,
			CASE
				WHEN s.has_price_override = 1 THEN toInt32(s.override_usdc_price_dollars)
				ELSE toInt32(COALESCE(mh.current_usdc_price_dollars, 0)) + toInt32(COALESCE(dh.current_usdc_metro_premium_dollars, 0))
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
	` + whereSQL + ` ` + orderBy + ` LIMIT ? OFFSET ?`
	queryArgs := append(whereArgs, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred client seats query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ShredClientSeatItem
	for rows.Next() {
		var s ShredClientSeatItem
		var lastActivity *time.Time
		if err := rows.Scan(
			&s.PK, &s.DeviceKey, &s.DeviceCode, &s.MetroPK, &s.MetroCode,
			&s.ClientIP, &s.TenureEpochs, &s.FundedEpoch, &s.ActiveEpoch,
			&s.HasPriceOverride, &s.OverrideUSDCPriceDollars, &s.EscrowCount, &s.TotalUSDCBalance,
			&s.PricePerEpochDollars, &s.FundingAuthorityKey,
			&s.UserPK, &s.UserOwnerPubkey, &s.UserStatus, &lastActivity,
		); err != nil {
			logError("shred client seats row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		if lastActivity != nil && !lastActivity.IsZero() {
			s.LastActivity = lastActivity.UTC().Format(time.RFC3339)
		}
		items = append(items, s)
	}
	if items == nil {
		items = []ShredClientSeatItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(PaginatedResponse[ShredClientSeatItem]{
		Items: items, Total: int(total), Limit: pagination.Limit, Offset: pagination.Offset,
	}); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// ShredDeviceHistoryItem represents a device subscription history in list responses.
type ShredDeviceHistoryItem struct {
	PK                             string `json:"pk"`
	DeviceKey                      string `json:"device_key"`
	DeviceCode                     string `json:"device_code"`
	IsEnabled                      uint8  `json:"is_enabled"`
	HasSettledSeats                uint8  `json:"has_settled_seats"`
	MetroExchangeKey               string `json:"metro_exchange_key"`
	MetroCode                      string `json:"metro_code"`
	ActiveGrantedSeats             uint16 `json:"active_granted_seats"`
	ActiveTotalAvailableSeats      uint16 `json:"active_total_available_seats"`
	CurrentEpoch                   uint64 `json:"current_epoch"`
	CurrentRequestedSeatCount      uint16 `json:"current_requested_seat_count"`
	CurrentGrantedSeatCount        uint16 `json:"current_granted_seat_count"`
	CurrentTotalAvailableSeats     uint16 `json:"current_total_available_seats"`
	CurrentUSDCMetroPremiumDollars int16  `json:"current_usdc_metro_premium_dollars"`
}

func (a *API) GetShredDeviceHistories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	start := time.Now()

	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, `SELECT count(*) FROM dim_dz_shred_device_histories_current`).Scan(&total); err != nil {
		logError("shred device histories count failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	query := `
		SELECT
			sh.pk, sh.device_key, COALESCE(d.code, '') as device_code,
			sh.is_enabled, sh.has_settled_seats,
			sh.metro_exchange_key, COALESCE(m.code, '') as metro_code,
			sh.active_granted_seats, sh.active_total_available_seats,
			sh.current_epoch, sh.current_requested_seat_count, sh.current_granted_seat_count,
			sh.current_total_available_seats, sh.current_usdc_metro_premium_dollars
		FROM dim_dz_shred_device_histories_current sh
		LEFT JOIN dz_devices_current d ON sh.device_key = d.pk
		LEFT JOIN dz_metros_current m ON sh.metro_exchange_key = m.pk
		ORDER BY sh.active_granted_seats DESC
		LIMIT ? OFFSET ?
	`

	rows, err := a.envDB(ctx).Query(ctx, query, pagination.Limit, pagination.Offset)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred device histories query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ShredDeviceHistoryItem
	for rows.Next() {
		var d ShredDeviceHistoryItem
		if err := rows.Scan(
			&d.PK, &d.DeviceKey, &d.DeviceCode,
			&d.IsEnabled, &d.HasSettledSeats,
			&d.MetroExchangeKey, &d.MetroCode,
			&d.ActiveGrantedSeats, &d.ActiveTotalAvailableSeats,
			&d.CurrentEpoch, &d.CurrentRequestedSeatCount, &d.CurrentGrantedSeatCount,
			&d.CurrentTotalAvailableSeats, &d.CurrentUSDCMetroPremiumDollars,
		); err != nil {
			logError("shred device histories row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		items = append(items, d)
	}
	if items == nil {
		items = []ShredDeviceHistoryItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(PaginatedResponse[ShredDeviceHistoryItem]{
		Items: items, Total: int(total), Limit: pagination.Limit, Offset: pagination.Offset,
	}); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// ShredMetroHistoryItem represents a metro pricing history in list responses.
type ShredMetroHistoryItem struct {
	PK                      string `json:"pk"`
	ExchangeKey             string `json:"exchange_key"`
	MetroCode               string `json:"metro_code"`
	IsCurrentPriceFinalized uint8  `json:"is_current_price_finalized"`
	TotalInitializedDevices uint16 `json:"total_initialized_devices"`
	CurrentEpoch            uint64 `json:"current_epoch"`
	CurrentUSDCPriceDollars uint16 `json:"current_usdc_price_dollars"`
}

func (a *API) GetShredMetroHistories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	start := time.Now()

	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, `SELECT count(*) FROM dim_dz_shred_metro_histories_current`).Scan(&total); err != nil {
		logError("shred metro histories count failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	query := `
		SELECT
			sh.pk, sh.exchange_key, COALESCE(m.code, '') as metro_code,
			sh.is_current_price_finalized, sh.total_initialized_devices,
			sh.current_epoch, sh.current_usdc_price_dollars
		FROM dim_dz_shred_metro_histories_current sh
		LEFT JOIN dz_metros_current m ON sh.exchange_key = m.pk
		ORDER BY sh.total_initialized_devices DESC
		LIMIT ? OFFSET ?
	`

	rows, err := a.envDB(ctx).Query(ctx, query, pagination.Limit, pagination.Offset)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred metro histories query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ShredMetroHistoryItem
	for rows.Next() {
		var m ShredMetroHistoryItem
		if err := rows.Scan(
			&m.PK, &m.ExchangeKey, &m.MetroCode,
			&m.IsCurrentPriceFinalized, &m.TotalInitializedDevices,
			&m.CurrentEpoch, &m.CurrentUSDCPriceDollars,
		); err != nil {
			logError("shred metro histories row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		items = append(items, m)
	}
	if items == nil {
		items = []ShredMetroHistoryItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(PaginatedResponse[ShredMetroHistoryItem]{
		Items: items, Total: int(total), Limit: pagination.Limit, Offset: pagination.Offset,
	}); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// ShredFunderItem represents a funding authority with aggregated seat stats.
type ShredFunderItem struct {
	FundingAuthorityKey string `json:"funding_authority_key"`
	TotalSeats          uint64 `json:"total_seats"`
	ActiveSeats         uint64 `json:"active_seats"`
	InactiveSeats       uint64 `json:"inactive_seats"`
	ClosedSeats         uint64 `json:"closed_seats"`
	TotalEscrows        uint64 `json:"total_escrows"`
	UniqueDevices       uint64 `json:"unique_devices"`
}

func (a *API) GetShredFunders(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()

	query := `
		WITH current_epoch AS (
			SELECT max(epoch) as epoch FROM solana_vote_accounts_current
		)
		SELECT
			s.funding_authority_key,
			count(*) as total_seats,
			countIf(s.active_epoch >= ce.epoch AND s.escrow_count > 0) as active_seats,
			countIf(s.active_epoch < ce.epoch AND s.escrow_count > 0) as inactive_seats,
			countIf(s.escrow_count = 0) as closed_seats,
			sum(s.escrow_count) as total_escrows,
			uniq(s.device_key) as unique_devices
		FROM dim_dz_shred_client_seats_current s
		CROSS JOIN current_epoch ce
		GROUP BY s.funding_authority_key
		ORDER BY active_seats DESC, total_seats DESC
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred funders query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ShredFunderItem
	for rows.Next() {
		var f ShredFunderItem
		if err := rows.Scan(
			&f.FundingAuthorityKey, &f.TotalSeats, &f.ActiveSeats, &f.InactiveSeats,
			&f.ClosedSeats, &f.TotalEscrows, &f.UniqueDevices,
		); err != nil {
			logError("shred funders row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		items = append(items, f)
	}
	if items == nil {
		items = []ShredFunderItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(items); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// ShredEscrowEventItem represents a payment escrow event.
type ShredEscrowEventItem struct {
	EventTS          string  `json:"event_ts"`
	EscrowPK         string  `json:"escrow_pk"`
	ClientSeatPK     string  `json:"client_seat_pk"`
	TxSignature      string  `json:"tx_signature"`
	Slot             uint64  `json:"slot"`
	EventType        string  `json:"event_type"`
	AmountUSDC       *int64  `json:"amount_usdc"`
	BalanceAfterUSDC *int64  `json:"balance_after_usdc"`
	Epoch            *uint64 `json:"epoch"`
	Status           string  `json:"status"`
	Signer           string  `json:"signer"`
	ClientIP         string  `json:"client_ip"`
	SolscanURL       string  `json:"solscan_url"`
}

// escrowEventExcludedSigners are internal/test accounts excluded by default.
var escrowEventExcludedSigners = []string{
	"DZfHfcCXTLwgZeCRKQ1FL1UuwAwFAZM93g86NMYpfYan",
}

var escrowEventSortFields = map[string]string{
	"time":    "e.event_ts",
	"type":    "e.event_type",
	"amount":  "e.amount_usdc",
	"balance": "e.balance_after_usdc",
	"epoch":   "e.epoch",
	"slot":    "e.slot",
}

var escrowEventFilterFields = map[string]FilterFieldConfig{
	"type":   {Column: "e.event_type", Type: FieldTypeText},
	"escrow": {Column: "e.escrow_pk", Type: FieldTypeText},
	"seat":   {Column: "e.client_seat_pk", Type: FieldTypeText},
	"status": {Column: "e.status", Type: FieldTypeText},
	"epoch":  {Column: "e.epoch", Type: FieldTypeNumeric},
	"signer": {Column: "e.signer", Type: FieldTypeText},
}

// splitCSV splits a comma-separated string into trimmed non-empty values.
func splitCSV(s string) []string {
	var result []string
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			result = append(result, v)
		}
	}
	return result
}

// parseTimeRangeDuration converts a preset time range string to a duration.
func parseTimeRangeDuration(rangeStr string) time.Duration {
	switch rangeStr {
	case "1h":
		return 1 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "24h":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "7d":
		return 7 * 24 * time.Hour
	case "14d":
		return 14 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	default:
		return 7 * 24 * time.Hour // default to 7d
	}
}

func (a *API) GetShredEscrowEvents(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "time", escrowEventSortFields)
	filters := ParseFilters(r)

	// Time range: preset (range=7d) or custom (start_time=X&end_time=Y as unix seconds).
	var startTime, endTime time.Time
	now := time.Now().UTC()

	rangeParam := r.URL.Query().Get("range")
	startTimeParam := r.URL.Query().Get("start_time")
	endTimeParam := r.URL.Query().Get("end_time")

	if startTimeParam != "" && endTimeParam != "" {
		if st, err := strconv.ParseInt(startTimeParam, 10, 64); err == nil {
			startTime = time.Unix(st, 0).UTC()
		}
		if et, err := strconv.ParseInt(endTimeParam, 10, 64); err == nil {
			endTime = time.Unix(et, 0).UTC()
		}
	}
	if startTime.IsZero() || endTime.IsZero() {
		if rangeParam == "" {
			rangeParam = "7d"
		}
		endTime = now
		startTime = now.Add(-parseTimeRangeDuration(rangeParam))
	}

	start := time.Now()

	// Build WHERE clause.
	whereClause := ` WHERE e.event_ts >= ? AND e.event_ts <= ?`
	whereArgs := []any{startTime, endTime}

	filterClause, filterArgs := filters.BuildFilterClause(escrowEventFilterFields)
	if filterClause != "" {
		whereClause += ` AND ` + filterClause
		whereArgs = append(whereArgs, filterArgs...)
	}

	// Exclude internal/test signers unless include_internal=true.
	if r.URL.Query().Get("include_internal") != "true" && len(escrowEventExcludedSigners) > 0 {
		for _, signer := range escrowEventExcludedSigners {
			whereClause += ` AND e.signer != ?`
			whereArgs = append(whereArgs, signer)
		}
	}

	// Count query.
	countQuery := `SELECT count(*) FROM fact_dz_shred_escrow_events AS e FINAL` + whereClause
	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		logError("shred escrow events count failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Data query.
	orderBy := sort.OrderByClause(escrowEventSortFields)
	query := `
		SELECT
			e.event_ts, e.escrow_pk, e.client_seat_pk, e.tx_signature, e.slot,
			e.event_type, e.amount_usdc, e.balance_after_usdc, e.epoch, e.status, e.signer,
			COALESCE(s.client_ip, '') as client_ip
		FROM fact_dz_shred_escrow_events AS e FINAL
		LEFT JOIN dim_dz_shred_client_seats_current s ON e.client_seat_pk = s.pk
	` + whereClause + ` ` + orderBy + ` LIMIT ? OFFSET ?`
	queryArgs := append(whereArgs, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("shred escrow events query failed", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var items []ShredEscrowEventItem
	for rows.Next() {
		var e ShredEscrowEventItem
		var eventTS time.Time
		if err := rows.Scan(
			&eventTS, &e.EscrowPK, &e.ClientSeatPK, &e.TxSignature, &e.Slot,
			&e.EventType, &e.AmountUSDC, &e.BalanceAfterUSDC, &e.Epoch, &e.Status, &e.Signer,
			&e.ClientIP,
		); err != nil {
			logError("shred escrow events row scan failed", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		e.EventTS = eventTS.UTC().Format(time.RFC3339)
		e.SolscanURL = "https://solscan.io/tx/" + e.TxSignature
		items = append(items, e)
	}
	if items == nil {
		items = []ShredEscrowEventItem{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(PaginatedResponse[ShredEscrowEventItem]{
		Items: items, Total: int(total), Limit: pagination.Limit, Offset: pagination.Offset,
	}); err != nil {
		logError("failed to encode response", "error", err)
	}
}
