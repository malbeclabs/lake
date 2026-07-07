package handlers

import (
	"context"
	"net/http"
	"time"
)

const permissionAuditDefaultLimit = 200

// PermissionAuditRow is a single serviceability Permission-management event.
type PermissionAuditRow struct {
	EventTS            time.Time `json:"eventTs"`
	TxSignature        string    `json:"txSignature"`
	Slot               uint64    `json:"slot"`
	InstructionIndex   uint16    `json:"instructionIndex"` // index within the tx; distinguishes multiple events in one tx
	Signer             string    `json:"signer"`           // acting admin (transaction fee-payer)
	PermissionPK       string    `json:"permissionPk"`     // the Permission PDA
	TargetPubkey       string    `json:"targetPubkey"`     // grantee
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
	limit := ParsePagination(r, permissionAuditDefaultLimit).Limit

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
			e.instruction_index,
			e.signer,
			e.permission_pk,
			if(e.target_pubkey != '', e.target_pubkey, tgt.target) AS target_pubkey,
			e.event_type,
			e.permissions_added,
			e.permissions_removed,
			e.success
		FROM fact_dz_permission_events AS e FINAL
		LEFT JOIN (
			SELECT permission_pk, max(target_pubkey) AS target
			FROM fact_dz_permission_events
			WHERE target_pubkey != ''
			GROUP BY permission_pk
		) AS tgt USING (permission_pk)
		ORDER BY e.slot DESC, e.event_ts DESC, e.tx_signature DESC, e.instruction_index DESC
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
			&row.InstructionIndex,
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
