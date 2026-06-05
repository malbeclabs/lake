package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
)

// multicastHealthUserSearchFields maps the prefixes accepted in a
// `field:value` search token to the underlying columns + match mode.
// Enum-like fields (status / mode / tunnel) need exact match because
// their values share substrings — e.g. `status:healthy` would otherwise
// match `unhealthy`. Free-form columns stay substring.
var multicastHealthUserSearchFields = map[string]healthSearchFieldSpec{
	"user":    {cols: []string{"user_pk", "user_owner_pubkey"}},
	"account": {cols: []string{"user_pk"}},
	"owner":   {cols: []string{"user_owner_pubkey"}},
	"pubkey":  {cols: []string{"user_pk", "user_owner_pubkey"}},
	"ip":      {cols: []string{"user_dz_ip"}},
	"dz_ip":   {cols: []string{"user_dz_ip"}},
	"device":  {cols: []string{"user_device_code", "user_device_pk"}},
	"tunnel":  {cols: []string{"user_tunnel_id"}, exact: true},
	"mode":    {cols: []string{"mode"}, exact: true},
	"status":  {cols: []string{"health_status"}, exact: true},
	"health":  {cols: []string{"health_status"}, exact: true},
}

// multicastHealthUserSearchFallback is OR-matched when the token has no
// field prefix.
var multicastHealthUserSearchFallback = []string{
	"user_pk",
	"user_owner_pubkey",
	"user_dz_ip",
	"user_device_code",
	"user_device_pk",
	"user_tunnel_id",
	"mode",
	"health_status",
}

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
	search := parseHealthSearch(r)

	// Cache read-through: only the hot first page of the hot group with no
	// search filter is pre-fetched by the worker. Searched / paged / non-hot
	// requests fall through to a live query.
	if search == "" && isMainnet(r.Context()) && offset == 0 && limit == MulticastHealthCachedPageSize && group.PK == ShredGroupPK {
		if cached, ok := a.readMulticastHealthUsersCache(ctx, group.PK); ok {
			w.Header().Set("X-Cache", "HIT")
			writeJSON(w, cached)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	where := "multicast_group_pk = ?"
	args := []any{group.PK}
	if clause, extra := buildHealthSearchClause(search, multicastHealthUserSearchFields, multicastHealthUserSearchFallback); clause != "" {
		where += clause
		args = append(args, extra...)
	}
	items, total, err := a.queryMulticastHealthUsers(ctx, where, args, limit, offset)
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

// readMulticastHealthUsersCache returns the cached first-page users response
// for the given group pk if the worker has written it. Missing entry or
// decode failure → cache miss, caller falls through to live query.
func (a *API) readMulticastHealthUsersCache(ctx context.Context, groupPK string) (*MulticastHealthGroupUsersResponse, bool) {
	data, err := a.readPageCache(ctx, MulticastHealthUsersCacheKey(groupPK))
	if err != nil {
		return nil, false
	}
	var cached MulticastHealthGroupUsersResponse
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil, false
	}
	return &cached, true
}

// FetchMulticastHealthUsersPageData fetches the hot first page of
// /health/users (offset=0, limit=MulticastHealthCachedPageSize) for the
// given pkOrCode. The worker calls this on every refresh cycle and writes
// the result under MulticastHealthUsersCacheKey(pk).
func (a *API) FetchMulticastHealthUsersPageData(ctx context.Context, pkOrCode string) (*MulticastHealthGroupUsersResponse, error) {
	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		return nil, err
	}
	items, total, err := a.queryMulticastHealthUsers(
		ctx, "multicast_group_pk = ?", []any{group.PK}, MulticastHealthCachedPageSize, 0,
	)
	if err != nil {
		return nil, err
	}
	return &MulticastHealthGroupUsersResponse{
		Group:       group,
		GeneratedAt: formatMulticastTime(time.Now().UTC()),
		Items:       items,
		Total:       total,
		Limit:       MulticastHealthCachedPageSize,
		Offset:      0,
	}, nil
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
