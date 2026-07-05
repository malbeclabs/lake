package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

const (
	permissionAuditDefaultLimit = 200
	permissionAuditMaxLimit     = 1000
)

// PermissionAuditRow is a single serviceability Permission-management event.
type PermissionAuditRow struct {
	EventTS            time.Time `json:"eventTs"`
	TxSignature        string    `json:"txSignature"`
	Slot               uint64    `json:"slot"`
	Signer             string    `json:"signer"`       // acting admin (transaction fee-payer)
	PermissionPK       string    `json:"permissionPk"` // the Permission PDA
	TargetPubkey       string    `json:"targetPubkey"` // grantee
	EventType          string    `json:"eventType"`
	PermissionsAdded   string    `json:"permissionsAdded"`
	PermissionsRemoved string    `json:"permissionsRemoved"`
	Success            bool      `json:"success"`
}

// PermissionAuditResponse is the payload for the permission audit page.
type PermissionAuditResponse struct {
	Events []PermissionAuditRow `json:"events"`
}

// GetPermissionAudit serves the serviceability permission audit trail. Gated to
// internal-domain users (see RequireInternalDomain in api/main.go).
func (a *API) GetPermissionAudit(w http.ResponseWriter, r *http.Request) {
	limit := permissionAuditDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > permissionAuditMaxLimit {
		limit = permissionAuditMaxLimit
	}

	resp, err := a.FetchPermissionAuditData(r.Context(), limit)
	if err != nil {
		logError("failed to fetch permission audit", "error", err)
		http.Error(w, "failed to fetch permission audit", http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

// FetchPermissionAuditData returns the most recent permission events, newest first.
// The grantee (target_pubkey) is only carried by CreatePermission instructions, so it
// is backfilled for the other event types by joining each permission PDA to its create row.
func (a *API) FetchPermissionAuditData(ctx context.Context, limit int) (PermissionAuditResponse, error) {
	const query = `
		SELECT
			e.event_ts,
			e.tx_signature,
			e.slot,
			e.signer,
			e.permission_pk,
			if(e.target_pubkey != '', e.target_pubkey, tgt.target) AS target_pubkey,
			e.event_type,
			e.permissions_added,
			e.permissions_removed,
			e.success
		FROM fact_dz_permission_events FINAL AS e
		LEFT JOIN (
			SELECT permission_pk, max(target_pubkey) AS target
			FROM fact_dz_permission_events
			WHERE target_pubkey != ''
			GROUP BY permission_pk
		) AS tgt USING (permission_pk)
		ORDER BY e.event_ts DESC, e.slot DESC, e.tx_signature DESC
		LIMIT ?
	`

	rows, err := a.safeQueryRows(ctx, query, limit)
	if err != nil {
		return PermissionAuditResponse{}, err
	}
	defer rows.Close()

	resp := PermissionAuditResponse{Events: []PermissionAuditRow{}}
	for rows.Next() {
		var row PermissionAuditRow
		var success uint8
		if err := rows.Scan(
			&row.EventTS,
			&row.TxSignature,
			&row.Slot,
			&row.Signer,
			&row.PermissionPK,
			&row.TargetPubkey,
			&row.EventType,
			&row.PermissionsAdded,
			&row.PermissionsRemoved,
			&success,
		); err != nil {
			return PermissionAuditResponse{}, err
		}
		row.Success = success == 1
		resp.Events = append(resp.Events, row)
	}
	if err := rows.Err(); err != nil {
		return PermissionAuditResponse{}, err
	}
	return resp, nil
}
