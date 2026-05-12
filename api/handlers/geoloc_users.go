package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

type GeolocUser struct {
	PK                   string `json:"pk"`
	Owner                string `json:"owner"`
	Code                 string `json:"code"`
	TokenAccount         string `json:"token_account"`
	PaymentStatus        string `json:"payment_status"`
	Status               string `json:"status"`
	TargetCount          uint32 `json:"target_count"`
	BillingRate          uint64 `json:"billing_rate"`
	LastDeductionDZEpoch uint64 `json:"last_deduction_dz_epoch"`
}

var geolocUserSortFields = map[string]string{
	"code":    "code",
	"owner":   "owner",
	"status":  "status",
	"payment": "payment_status",
	"targets": "target_count",
	"rate":    "billing_rate",
}

var geolocUserFilterFields = map[string]FilterFieldConfig{
	"code":    {Column: "code", Type: FieldTypeText},
	"owner":   {Column: "owner", Type: FieldTypeText},
	"status":  {Column: "status", Type: FieldTypeText},
	"payment": {Column: "payment_status", Type: FieldTypeText},
	"targets": {Column: "target_count", Type: FieldTypeNumeric},
	"rate":    {Column: "billing_rate", Type: FieldTypeNumeric},
}

func (a *API) GetGeolocUsers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	pagination := ParsePagination(r, 100)
	sort := ParseSort(r, "code", geolocUserSortFields)
	filters := ParseFilters(r)
	start := time.Now()

	filterClause, filterArgs := filters.BuildFilterClause(geolocUserFilterFields)
	whereFilter := ""
	if filterClause != "" {
		whereFilter = " AND " + filterClause
	}
	orderBy := sort.OrderByClause(geolocUserSortFields)

	query := `
		SELECT pk, owner, code, token_account, payment_status, status, target_count, billing_rate, last_deduction_dz_epoch, count() OVER () as _total
		FROM geoloc_users_current
		WHERE 1=1` + whereFilter + " " + orderBy + `
		LIMIT ? OFFSET ?
	`

	var args []any
	args = append(args, filterArgs...)
	args = append(args, pagination.Limit, pagination.Offset)

	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("geoloc_users", duration, err)

	if err != nil {
		logError("geoloc users query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []GeolocUser
	var total uint64
	for rows.Next() {
		var g GeolocUser
		if err := rows.Scan(
			&g.PK,
			&g.Owner,
			&g.Code,
			&g.TokenAccount,
			&g.PaymentStatus,
			&g.Status,
			&g.TargetCount,
			&g.BillingRate,
			&g.LastDeductionDZEpoch,
			&total,
		); err != nil {
			logError("geoloc users scan error", "error", err)
			http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
			return
		}
		users = append(users, g)
	}

	if err := rows.Err(); err != nil {
		logError("geoloc users rows error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null
	if users == nil {
		users = []GeolocUser{}
	}

	response := PaginatedResponse[GeolocUser]{
		Items:  users,
		Total:  int(total),
		Limit:  pagination.Limit,
		Offset: pagination.Offset,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logError("failed to encode response", "error", err)
	}
}
