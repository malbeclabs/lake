package handlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// GetMulticastGroupHealthUsers returns per-user reconciliation rows from
// health_multicast_user filtered to one group.
func (a *API) GetMulticastGroupHealthUsers(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing multicast group pk", http.StatusBadRequest)
		return
	}
	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "multicast group not found", http.StatusNotFound)
			return
		}
		logError("multicast group health/users group query error", "error", err, "pk", pkOrCode)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	limit, offset := parseLimitOffset(r)
	items, total, err := a.queryMulticastHealthUsers(ctx, "multicast_group_pk = ?", []any{group.PK}, limit, offset)
	if err != nil {
		logError("multicast group health/users query error", "error", err, "pk", group.PK)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, MulticastHealthGroupUsersResponse{
		Group:       group,
		GeneratedAt: formatMulticastTime(time.Now().UTC()),
		Items:       items,
		Total:       total,
		Limit:       limit,
		Offset:      offset,
	})
}

// GetUserHealth returns per-group reconciliation rows for one multicast user.
func (a *API) GetUserHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	userPK := chi.URLParam(r, "pk")
	if userPK == "" {
		http.Error(w, "missing user pk", http.StatusBadRequest)
		return
	}

	items, _, err := a.queryMulticastHealthUsers(ctx, "user_pk = ?", []any{userPK}, 0, 0)
	if err != nil {
		logError("user health query error", "error", err, "user_pk", userPK)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	if len(items) == 0 {
		// The user may exist but have no multicast memberships, or may not
		// exist as a multicast user at all. We return an empty Items list
		// rather than 404 because "no health rows" is a valid steady state.
		writeJSON(w, MulticastHealthUserResponse{
			UserPK:      userPK,
			GeneratedAt: formatMulticastTime(time.Now().UTC()),
			Items:       []MulticastHealthUserItem{},
		})
		return
	}

	first := items[0]
	writeJSON(w, MulticastHealthUserResponse{
		UserPK:          first.UserPK,
		UserOwnerPubkey: first.UserOwnerPubkey,
		UserDZIP:        first.UserDZIP,
		UserTunnelID:    first.UserTunnelID,
		UserDevicePK:    first.UserDevicePK,
		UserDeviceCode:  first.UserDeviceCode,
		GeneratedAt:     formatMulticastTime(time.Now().UTC()),
		Items:           items,
	})
}

// queryMulticastHealthUsers reads from health_multicast_user_rate with a
// single WHERE clause filter (either "multicast_group_pk = ?" or
// "user_pk = ?"). The view exposes both the CP-only verdict
// (control_plane_status) and the combined verdict (health_status); we
// surface both so consumers can drill in.
func (a *API) queryMulticastHealthUsers(ctx context.Context, whereClause string, args []any, limit, offset int) ([]MulticastHealthUserItem, int, error) {
	total := 0
	if limit > 0 {
		var err error
		total, err = a.queryMulticastHealthTotal(ctx, "health_multicast_user_rate", whereClause, args, "multicast_health_users_count")
		if err != nil {
			return nil, 0, err
		}
	}

	limitClause := ""
	queryArgs := append([]any{}, args...)
	if limit > 0 {
		limitClause = "\n\t\tLIMIT ? OFFSET ?"
		queryArgs = append(queryArgs, limit, offset)
	}

	query := `
		SELECT
			user_pk,
			user_owner_pubkey,
			user_dz_ip,
			user_tunnel_id,
			user_device_pk,
			user_device_code,
			multicast_group_pk,
			multicast_group_code,
			group_address,
			mode,
			expected_tunnel_position,
			publisher_iif_observed,
			subscriber_oif_observed,
			reconciled,
			control_plane_status,
			mismatch_reason,
			rate_bucket_ts,
			observed_bps_5m,
			expected_bps_5m,
			rate_status,
			rate_status_reason,
			health_status
		FROM health_multicast_user_rate
		WHERE ` + whereClause + `
		-- Sort actionable rows first (unhealthy → degraded → unknown → healthy)
		-- so paginated consumers land on the rows operators most need to see.
		ORDER BY
			multiIf(health_status = 'unhealthy', 0,
			        health_status = 'degraded',  1,
			        health_status = 'unknown',   2,
			                                     3),
			multicast_group_code, user_pk` + limitClause + `
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	metrics.RecordClickHouseQuery("multicast_health_users", time.Since(start), err)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := []MulticastHealthUserItem{}
	for rows.Next() {
		var it MulticastHealthUserItem
		if err := rows.Scan(
			&it.UserPK,
			&it.UserOwnerPubkey,
			&it.UserDZIP,
			&it.UserTunnelID,
			&it.UserDevicePK,
			&it.UserDeviceCode,
			&it.MulticastGroupPK,
			&it.MulticastGroupCode,
			&it.GroupAddress,
			&it.Mode,
			&it.ExpectedTunnelPos,
			&it.PublisherIIFObserved,
			&it.SubscriberOIFObserved,
			&it.Reconciled,
			&it.ControlPlaneStatus,
			&it.MismatchReason,
			&it.RateBucketTS,
			&it.ObservedBps5m,
			&it.ExpectedBps5m,
			&it.RateStatus,
			&it.RateStatusReason,
			&it.HealthStatus,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if limit == 0 {
		total = len(items)
	}
	return items, total, nil
}
