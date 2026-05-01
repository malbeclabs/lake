package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/metrics"
)

type AccessPassListItem struct {
	PK               string `json:"pk"`
	OwnerPubkey      string `json:"owner_pubkey"`
	TypeTag          string `json:"type_tag"`
	Status           string `json:"status"`
	ClientIP         string `json:"client_ip"`
	ConnectionCount  uint16 `json:"connection_count"`
	AssociatedPubkey string `json:"associated_pubkey"`
	FirstPubCode     string `json:"first_pub_code"`
	FirstSubCode     string `json:"first_sub_code"`
}

type MulticastGroupRef struct {
	PK          string `json:"pk"`
	Code        string `json:"code"`
	MulticastIP string `json:"multicast_ip"`
	Status      string `json:"status"`
}

// AccessPassShredsSeat is embedded in AccessPassDetail when the access pass
// belongs to the Shreds product (user_payer == ShredsInternalUserPayer).
type AccessPassShredsSeat struct {
	PK                   string `json:"pk"`
	DeviceKey            string `json:"device_key"`
	DeviceCode           string `json:"device_code"`
	MetroPK              string `json:"metro_pk"`
	MetroCode            string `json:"metro_code"`
	TenureEpochs         uint16 `json:"tenure_epochs"`
	FundedEpoch          uint64 `json:"funded_epoch"`
	ActiveEpoch          uint64 `json:"active_epoch"`
	EscrowCount          uint32 `json:"escrow_count"`
	TotalUSDCBalance     uint64 `json:"total_usdc_balance"`
	PricePerEpochDollars int64  `json:"price_per_epoch_dollars"`
	FundingAuthorityKey  string `json:"funding_authority_key"`
}

type AccessPassDetail struct {
	PK                  string                `json:"pk"`
	OwnerPubkey         string                `json:"owner_pubkey"`
	TypeTag             string                `json:"type_tag"`
	Status              string                `json:"status"`
	ClientIP            string                `json:"client_ip"`
	UserPayer           string                `json:"user_payer"`
	AssociatedPubkey    string                `json:"associated_pubkey"`
	OthersTypeName      string                `json:"others_type_name"`
	OthersKey           string                `json:"others_key"`
	LastAccessEpoch     uint64                `json:"last_access_epoch"`
	ConnectionCount     uint16                `json:"connection_count"`
	Flags               uint8                 `json:"flags"`
	MGroupPubAllowlist  []MulticastGroupRef   `json:"mgroup_pub_allowlist"`
	MGroupSubAllowlist  []MulticastGroupRef   `json:"mgroup_sub_allowlist"`
	ValidatorVotePubkey string                `json:"validator_vote_pubkey,omitempty"`
	ValidatorNodePubkey string                `json:"validator_node_pubkey,omitempty"`
	ShredsSeat          *AccessPassShredsSeat `json:"shreds_seat,omitempty"`
}

var accessPassSortFields = map[string]string{
	"type":        "type_tag",
	"status":      "status",
	"connections": "connection_count",
	"client_ip":   "client_ip",
}

var accessPassFilterFields = map[string]FilterFieldConfig{
	"type":       {Column: "type_tag", Type: FieldTypeText},
	"status":     {Column: "status", Type: FieldTypeText},
	"owner":      {Column: "owner_pubkey", Type: FieldTypeText},
	"client_ip":  {Column: "client_ip", Type: FieldTypeText},
	"user_payer": {Column: "user_payer", Type: FieldTypeText},
}

func (a *API) GetAccessPasses(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "type", accessPassSortFields)
	allFilters := ParseFilters(r)

	// Separate pub_group / sub_group filters — they need JSON-array subqueries
	// and cannot be handled by BuildFilterClause.
	var pubGroupVals, subGroupVals []string
	var stdFilters []FilterParams
	for _, f := range allFilters.Filters {
		switch f.Field {
		case "pub_group":
			pubGroupVals = append(pubGroupVals, f.Value)
		case "sub_group":
			subGroupVals = append(subGroupVals, f.Value)
		default:
			stdFilters = append(stdFilters, f)
		}
	}
	allFilters.Filters = stdFilters

	start := time.Now()

	filterClause, filterArgs := allFilters.BuildFilterClause(accessPassFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}

	// Build subquery conditions for pub/sub group filters. Each value is matched
	// against both group code (case-insensitive substring) and exact group PK.
	var args []any
	args = append(args, filterArgs...)
	for _, v := range pubGroupVals {
		whereFilter += ` AND ap.pk IN (
			SELECT DISTINCT a.pk FROM dz_access_passes_current a
			ANY INNER JOIN dz_multicast_groups_current g
				ON positionCaseInsensitive(a.mgroup_pub_allowlist, g.pk) > 0
			WHERE positionCaseInsensitive(g.code, ?) > 0 OR g.pk = ?
		)`
		args = append(args, v, v)
	}
	for _, v := range subGroupVals {
		whereFilter += ` AND ap.pk IN (
			SELECT DISTINCT a.pk FROM dz_access_passes_current a
			ANY INNER JOIN dz_multicast_groups_current g
				ON positionCaseInsensitive(a.mgroup_sub_allowlist, g.pk) > 0
			WHERE positionCaseInsensitive(g.code, ?) > 0 OR g.pk = ?
		)`
		args = append(args, v, v)
	}

	orderBy := sort.OrderByClause(accessPassSortFields)

	query := `
		WITH pub_codes AS (
			SELECT ap.pk, COALESCE(g.code, '') as code
			FROM dz_access_passes_current ap
			LEFT JOIN dz_multicast_groups_current g
				ON g.pk = extract(ap.mgroup_pub_allowlist, '"([^"]+)"')
			WHERE ap.mgroup_pub_allowlist NOT IN ('', '[]', 'null')
		),
		sub_codes AS (
			SELECT ap.pk, COALESCE(g.code, '') as code
			FROM dz_access_passes_current ap
			LEFT JOIN dz_multicast_groups_current g
				ON g.pk = extract(ap.mgroup_sub_allowlist, '"([^"]+)"')
			WHERE ap.mgroup_sub_allowlist NOT IN ('', '[]', 'null')
		)
		SELECT ap.pk, ap.owner_pubkey, ap.type_tag, ap.status, ap.client_ip,
		       ap.connection_count, ap.associated_pubkey,
		       COALESCE(pc.code, '') as first_pub_code,
		       COALESCE(sc.code, '') as first_sub_code,
		       count() OVER () as _total
		FROM dz_access_passes_current ap
		LEFT JOIN pub_codes pc ON ap.pk = pc.pk
		LEFT JOIN sub_codes sc ON ap.pk = sc.pk
		WHERE 1=1` + whereFilter + " " + orderBy + `
		LIMIT ? OFFSET ?
	`

	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("access passes query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var passes []AccessPassListItem
	var total uint64
	for rows.Next() {
		var ap AccessPassListItem
		if err := rows.Scan(
			&ap.PK,
			&ap.OwnerPubkey,
			&ap.TypeTag,
			&ap.Status,
			&ap.ClientIP,
			&ap.ConnectionCount,
			&ap.AssociatedPubkey,
			&ap.FirstPubCode,
			&ap.FirstSubCode,
			&total,
		); err != nil {
			logError("access passes scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		passes = append(passes, ap)
	}

	if err := rows.Err(); err != nil {
		logError("access passes rows error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if passes == nil {
		passes = []AccessPassListItem{}
	}

	response := PaginatedResponse[AccessPassListItem]{
		Items:  passes,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) GetAccessPass(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing access pass pk", http.StatusBadRequest)
		return
	}

	start := time.Now()

	var ap AccessPassDetail
	var pubAllowlistJSON, subAllowlistJSON string
	err := a.envDB(ctx).QueryRow(ctx, `
		SELECT pk, owner_pubkey, type_tag, status, client_ip, user_payer,
		       associated_pubkey, others_type_name, others_key,
		       last_access_epoch, connection_count, flags,
		       mgroup_pub_allowlist, mgroup_sub_allowlist
		FROM dz_access_passes_current
		WHERE pk = ?
	`, pk).Scan(
		&ap.PK,
		&ap.OwnerPubkey,
		&ap.TypeTag,
		&ap.Status,
		&ap.ClientIP,
		&ap.UserPayer,
		&ap.AssociatedPubkey,
		&ap.OthersTypeName,
		&ap.OthersKey,
		&ap.LastAccessEpoch,
		&ap.ConnectionCount,
		&ap.Flags,
		&pubAllowlistJSON,
		&subAllowlistJSON,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "access pass not found", http.StatusNotFound)
			return
		}
		logError("access pass query error", "error", err)
		http.Error(w, "access pass not found", http.StatusNotFound)
		return
	}
	// Resolve multicast group allowlists
	ap.MGroupPubAllowlist = resolveMulticastGroupRefs(ctx, a, pubAllowlistJSON)
	ap.MGroupSubAllowlist = resolveMulticastGroupRefs(ctx, a, subAllowlistJSON)

	// Resolve validator if applicable
	if ap.TypeTag == "solana_validator" && ap.AssociatedPubkey != "" {
		ap.ValidatorVotePubkey, ap.ValidatorNodePubkey = resolveAccessPassValidator(ctx, a, ap.AssociatedPubkey)
	}

	// If this access pass is managed by the Shreds product, attach the associated
	// client seat (linked by client_ip) so the UI can display subscription details.
	if isShredsInternalPayer(ap.UserPayer) && ap.ClientIP != "" {
		seat, seatErr := a.fetchShredSeatByClientIP(ctx, ap.ClientIP)
		if seatErr != nil {
			logError("failed to fetch shreds seat for access pass", "pk", pk, "error", seatErr)
		} else if seat != nil {
			ap.ShredsSeat = &AccessPassShredsSeat{
				PK:                   seat.PK,
				DeviceKey:            seat.DeviceKey,
				DeviceCode:           seat.DeviceCode,
				MetroPK:              seat.MetroPK,
				MetroCode:            seat.MetroCode,
				TenureEpochs:         seat.TenureEpochs,
				FundedEpoch:          seat.FundedEpoch,
				ActiveEpoch:          seat.ActiveEpoch,
				EscrowCount:          seat.EscrowCount,
				TotalUSDCBalance:     seat.TotalUSDCBalance,
				PricePerEpochDollars: seat.PricePerEpochDollars,
				FundingAuthorityKey:  seat.FundingAuthorityKey,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ap); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func resolveMulticastGroupRefs(ctx context.Context, a *API, allowlistJSON string) []MulticastGroupRef {
	if allowlistJSON == "" || allowlistJSON == "[]" || allowlistJSON == "null" {
		return []MulticastGroupRef{}
	}

	var pks []string
	if err := json.Unmarshal([]byte(allowlistJSON), &pks); err != nil || len(pks) == 0 {
		return []MulticastGroupRef{}
	}

	rows, err := a.envDB(ctx).Query(ctx, `
		SELECT pk, COALESCE(code, ''), COALESCE(multicast_ip, ''), COALESCE(status, '')
		FROM dz_multicast_groups_current
		WHERE pk IN (?)
	`, pks)
	if err != nil {
		return []MulticastGroupRef{}
	}
	defer rows.Close()

	var refs []MulticastGroupRef
	for rows.Next() {
		var ref MulticastGroupRef
		if err := rows.Scan(&ref.PK, &ref.Code, &ref.MulticastIP, &ref.Status); err != nil {
			continue
		}
		refs = append(refs, ref)
	}

	if refs == nil {
		return []MulticastGroupRef{}
	}
	return refs
}

type AccessPassConnection struct {
	PK          string `json:"pk"`
	OwnerPubkey string `json:"owner_pubkey"`
	Status      string `json:"status"`
	Kind        string `json:"kind"`
	DzIP        string `json:"dz_ip"`
	ClientIP    string `json:"client_ip"`
	DeviceCode  string `json:"device_code"`
	MetroCode   string `json:"metro_code"`
	TenantCode  string `json:"tenant_code"`
}

func (a *API) GetAccessPassConnections(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing access pass pk", http.StatusBadRequest)
		return
	}

	// Fetch the access pass to get owner_pubkey, user_payer, client_ip
	var ownerPubkey, userPayer, clientIP string
	err := a.envDB(ctx).QueryRow(ctx, `
		SELECT owner_pubkey, user_payer, client_ip
		FROM dz_access_passes_current
		WHERE pk = ?
	`, pk).Scan(&ownerPubkey, &userPayer, &clientIP)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "access pass not found", http.StatusNotFound)
			return
		}
		logError("access pass connections lookup error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	start := time.Now()

	// For Shreds product passes, owner_pubkey and user_payer both belong to the
	// internal Shreds account — matching against them would return connections for
	// the entire product. Only match by client_ip in that case.
	connQuery := `
		SELECT
			u.pk,
			COALESCE(u.owner_pubkey, '') as owner_pubkey,
			u.status,
			COALESCE(u.kind, '') as kind,
			COALESCE(u.dz_ip, '') as dz_ip,
			COALESCE(u.client_ip, '') as client_ip,
			COALESCE(d.code, '') as device_code,
			COALESCE(m.code, '') as metro_code,
			COALESCE(t.code, '') as tenant_code
		FROM dz_users_current u
		LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_tenants_current t ON u.tenant_pk = t.pk
		WHERE u.owner_pubkey IN (?, ?) OR (? != '' AND u.client_ip = ?)
		ORDER BY u.status ASC, u.kind ASC
		LIMIT 200
	`
	connArgs := []any{ownerPubkey, userPayer, clientIP, clientIP}
	if isShredsInternalPayer(userPayer) {
		connQuery = `
			SELECT
				u.pk,
				COALESCE(u.owner_pubkey, '') as owner_pubkey,
				u.status,
				COALESCE(u.kind, '') as kind,
				COALESCE(u.dz_ip, '') as dz_ip,
				COALESCE(u.client_ip, '') as client_ip,
				COALESCE(d.code, '') as device_code,
				COALESCE(m.code, '') as metro_code,
				COALESCE(t.code, '') as tenant_code
			FROM dz_users_current u
			LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_tenants_current t ON u.tenant_pk = t.pk
			WHERE ? != '' AND u.client_ip = ?
			ORDER BY u.status ASC, u.kind ASC
			LIMIT 200
		`
		connArgs = []any{clientIP, clientIP}
	}

	rows, err := a.envDB(ctx).Query(ctx, connQuery, connArgs...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("access pass connections query error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var conns []AccessPassConnection
	for rows.Next() {
		var c AccessPassConnection
		if err := rows.Scan(
			&c.PK, &c.OwnerPubkey, &c.Status, &c.Kind,
			&c.DzIP, &c.ClientIP, &c.DeviceCode, &c.MetroCode, &c.TenantCode,
		); err != nil {
			logError("access pass connections scan error", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		conns = append(conns, c)
	}
	if err := rows.Err(); err != nil {
		logError("access pass connections rows error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if conns == nil {
		conns = []AccessPassConnection{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(conns); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func resolveAccessPassValidator(ctx context.Context, a *API, associatedPubkey string) (votePubkey, nodePubkey string) {
	// Try as vote_pubkey first
	err := a.envDB(ctx).QueryRow(ctx, `
		SELECT vote_pubkey, node_pubkey
		FROM solana_vote_accounts_current
		WHERE vote_pubkey = ?
		LIMIT 1
	`, associatedPubkey).Scan(&votePubkey, &nodePubkey)
	if err == nil {
		return
	}

	// Try as node_pubkey (identity key)
	err = a.envDB(ctx).QueryRow(ctx, `
		SELECT vote_pubkey, node_pubkey
		FROM solana_vote_accounts_current
		WHERE node_pubkey = ?
		LIMIT 1
	`, associatedPubkey).Scan(&votePubkey, &nodePubkey)
	if err == nil {
		return
	}

	return "", ""
}
