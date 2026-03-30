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

type TenantListItem struct {
	PK            string `json:"pk"`
	OwnerPubkey   string `json:"owner_pubkey"`
	Code          string `json:"code"`
	PaymentStatus string `json:"payment_status"`
	VrfID         uint16 `json:"vrf_id"`
	MetroRouting  bool   `json:"metro_routing"`
	RouteLiveness bool   `json:"route_liveness"`
	BillingRate   uint64 `json:"billing_rate"`
}

func (a *API) GetTenants(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	start := time.Now()

	countQuery := `SELECT count(*) FROM dz_tenants_current`
	var total uint64
	if err := a.envDB(ctx).QueryRow(ctx, countQuery).Scan(&total); err != nil {
		logError("tenants count query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	query := `
		SELECT
			pk,
			COALESCE(owner_pubkey, '') as owner_pubkey,
			COALESCE(code, '') as code,
			COALESCE(payment_status, '') as payment_status,
			vrf_id,
			metro_routing,
			route_liveness,
			billing_rate
		FROM dz_tenants_current
		ORDER BY code
		LIMIT ? OFFSET ?
	`

	rows, err := a.envDB(ctx).Query(ctx, query, pagination.Limit, pagination.Offset)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		logError("tenants query failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var tenants []TenantListItem
	for rows.Next() {
		var t TenantListItem
		if err := rows.Scan(
			&t.PK,
			&t.OwnerPubkey,
			&t.Code,
			&t.PaymentStatus,
			&t.VrfID,
			&t.MetroRouting,
			&t.RouteLiveness,
			&t.BillingRate,
		); err != nil {
			logError("tenants scan failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tenants = append(tenants, t)
	}

	if err := rows.Err(); err != nil {
		logError("tenants rows iteration failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if tenants == nil {
		tenants = []TenantListItem{}
	}

	response := PaginatedResponse[TenantListItem]{
		Items:  tenants,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}

func (a *API) GetTenant(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	pk := chi.URLParam(r, "pk")
	if pk == "" {
		http.Error(w, "missing tenant pk", http.StatusBadRequest)
		return
	}

	start := time.Now()
	query := `
		SELECT
			pk,
			COALESCE(owner_pubkey, '') as owner_pubkey,
			COALESCE(code, '') as code,
			COALESCE(payment_status, '') as payment_status,
			vrf_id,
			metro_routing,
			route_liveness,
			billing_rate
		FROM dz_tenants_current
		WHERE pk = ?
	`

	var t TenantListItem
	err := a.envDB(ctx).QueryRow(ctx, query, pk).Scan(
		&t.PK,
		&t.OwnerPubkey,
		&t.Code,
		&t.PaymentStatus,
		&t.VrfID,
		&t.MetroRouting,
		&t.RouteLiveness,
		&t.BillingRate,
	)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "tenant not found", http.StatusNotFound)
			return
		}
		logError("tenant query failed", "error", err)
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(t); err != nil {
		logError("failed to encode response", "error", err)
	}
}
