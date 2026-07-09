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

// multicastHealthPathSearchFields maps `field:value` prefixes accepted in
// the path-table search to underlying columns + match mode. status is
// enum-like and uses exact match; everything else is substring.
var multicastHealthPathSearchFields = map[string]healthSearchFieldSpec{
	"publisher":  {cols: []string{"publisher_user_pk", "publisher_owner_pubkey", "publisher_dz_ip", "publisher_device_code"}},
	"subscriber": {cols: []string{"subscriber_user_pk", "subscriber_owner_pubkey", "subscriber_dz_ip", "subscriber_device_code"}},
	"user":       {cols: []string{"publisher_user_pk", "publisher_owner_pubkey", "subscriber_user_pk", "subscriber_owner_pubkey"}},
	"owner":      {cols: []string{"publisher_owner_pubkey", "subscriber_owner_pubkey"}},
	"pubkey":     {cols: []string{"publisher_user_pk", "publisher_owner_pubkey", "subscriber_user_pk", "subscriber_owner_pubkey"}},
	"ip":         {cols: []string{"publisher_dz_ip", "subscriber_dz_ip"}},
	"dz_ip":      {cols: []string{"publisher_dz_ip", "subscriber_dz_ip"}},
	"device":     {cols: []string{"publisher_device_code", "subscriber_device_code", "publisher_device_pk", "subscriber_device_pk"}},
	"status":     {cols: []string{"health_status"}, exact: true},
	"health":     {cols: []string{"health_status"}, exact: true},
}

// multicastHealthPathSearchFallback is OR-matched when the token has no
// field prefix.
var multicastHealthPathSearchFallback = []string{
	"publisher_owner_pubkey",
	"publisher_user_pk",
	"publisher_dz_ip",
	"publisher_device_code",
	"publisher_device_pk",
	"subscriber_owner_pubkey",
	"subscriber_user_pk",
	"subscriber_dz_ip",
	"subscriber_device_code",
	"subscriber_device_pk",
	"health_status",
}

// GetMulticastGroupHealthPaths returns per-(publisher, subscriber) path
// reconciliation rows from health_publisher_subscriber_path for one group.
func (a *API) GetMulticastGroupHealthPaths(w http.ResponseWriter, r *http.Request) {
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
		logError("multicast group health/paths group query error", "error", err, "pk", pkOrCode)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	limit, offset := parseLimitOffset(r)
	search := parseHealthSearch(r)

	// Cache read-through: only the hot first page with no search filter
	// is pre-fetched by the worker. Searched / paged / non-hot requests
	// fall through to a live query.
	if search == "" && isMainnet(r.Context()) && offset == 0 && limit == MulticastHealthCachedPageSize && group.PK == ShredGroupPK {
		if cached, ok := a.readMulticastHealthPathsCache(ctx, group.PK); ok {
			w.Header().Set("X-Cache", "HIT")
			writeJSON(w, cached)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	items, total, err := a.queryMulticastHealthPaths(ctx, group.PK, search, limit, offset)
	if err != nil {
		logError("multicast group health/paths query error", "error", err, "pk", group.PK)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, MulticastHealthGroupPathsResponse{
		Group:       group,
		GeneratedAt: formatMulticastTime(time.Now().UTC()),
		Items:       items,
		Total:       total,
		Limit:       limit,
		Offset:      offset,
	})
}

// GetMulticastGroupHealthPathRootCauses returns the unhealthy per-path fan-out
// collapsed to the faulting endpoint (publisher or subscriber), with a count of
// how many (publisher, subscriber) pairs each one drags down. This is the
// actionable summary behind the raw per-path table.
func (a *API) GetMulticastGroupHealthPathRootCauses(w http.ResponseWriter, r *http.Request) {
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
		logError("multicast group health/path-root-causes group query error", "error", err, "pk", pkOrCode)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	items, err := a.queryMulticastHealthPathRootCauses(ctx, group.PK)
	if err != nil {
		logError("multicast group health/path-root-causes query error", "error", err, "pk", group.PK)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, MulticastHealthPathRootCausesResponse{
		Group:       group,
		GeneratedAt: formatMulticastTime(time.Now().UTC()),
		Items:       items,
		Total:       len(items),
	})
}

// queryMulticastHealthPathRootCauses attributes every non-healthy path to the
// endpoint(s) that are not observed. A BGP-down endpoint has no session, so no
// (S,G)/OIF, so its *_endpoint_observed is false — meaning this single filter
// catches both 'unhealthy' (endpoint present but not forwarding) and
// 'disconnected' (BGP down) attributions. endpoint_status reports the
// endpoint's own condition (disconnected if its BGP is down, else unhealthy).
func (a *API) queryMulticastHealthPathRootCauses(ctx context.Context, groupPK string) ([]MulticastHealthPathRootCause, error) {
	query := `
		WITH faults AS (
			SELECT
				'publisher' AS faulting_role,
				publisher_user_pk AS endpoint_user_pk,
				publisher_owner_pubkey AS endpoint_owner_pubkey,
				publisher_dz_ip AS endpoint_dz_ip,
				publisher_tunnel_id AS endpoint_tunnel_id,
				publisher_device_pk AS endpoint_device_pk,
				publisher_device_code AS endpoint_device_code
			FROM health_publisher_subscriber_path
			WHERE multicast_group_pk = ?
			  AND health_status != 'healthy'
			  AND publisher_endpoint_observed = 0
			UNION ALL
			SELECT
				'subscriber' AS faulting_role,
				subscriber_user_pk AS endpoint_user_pk,
				subscriber_owner_pubkey AS endpoint_owner_pubkey,
				subscriber_dz_ip AS endpoint_dz_ip,
				subscriber_tunnel_id AS endpoint_tunnel_id,
				subscriber_device_pk AS endpoint_device_pk,
				subscriber_device_code AS endpoint_device_code
			FROM health_publisher_subscriber_path
			WHERE multicast_group_pk = ?
			  AND health_status != 'healthy'
			  AND subscriber_endpoint_observed = 0
		)
		SELECT
			f.faulting_role,
			f.endpoint_user_pk,
			any(f.endpoint_owner_pubkey),
			any(f.endpoint_dz_ip),
			any(f.endpoint_tunnel_id),
			any(f.endpoint_device_pk),
			any(f.endpoint_device_code),
			if(any(u.bgp_status) = 'down', 'disconnected', 'unhealthy') AS endpoint_status,
			toInt32(count()) AS affected_pairs
		FROM faults f
		LEFT ANY JOIN dz_users_current u ON f.endpoint_user_pk = u.pk
		GROUP BY f.faulting_role, f.endpoint_user_pk
		ORDER BY affected_pairs DESC, endpoint_status, f.endpoint_user_pk
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, groupPK, groupPK)
	metrics.RecordClickHouseQuery("multicast_health_path_root_causes", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []MulticastHealthPathRootCause{}
	for rows.Next() {
		var it MulticastHealthPathRootCause
		if err := rows.Scan(
			&it.FaultingRole,
			&it.UserPK,
			&it.OwnerPubkey,
			&it.DZIP,
			&it.TunnelID,
			&it.DevicePK,
			&it.DeviceCode,
			&it.EndpointStatus,
			&it.AffectedPairs,
		); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// readMulticastHealthPathsCache returns the cached first-page paths response
// for the given group pk if the worker has written it. Missing entry or
// decode failure → cache miss, caller falls through to live query.
func (a *API) readMulticastHealthPathsCache(ctx context.Context, groupPK string) (*MulticastHealthGroupPathsResponse, bool) {
	data, err := a.readPageCache(ctx, MulticastHealthPathsCacheKey(groupPK))
	if err != nil {
		return nil, false
	}
	var cached MulticastHealthGroupPathsResponse
	if err := json.Unmarshal(data, &cached); err != nil {
		return nil, false
	}
	return &cached, true
}

// FetchMulticastHealthPathsPageData fetches the hot first page of
// /health/paths (offset=0, limit=MulticastHealthCachedPageSize) for the
// given pkOrCode. The worker calls this on every refresh cycle.
func (a *API) FetchMulticastHealthPathsPageData(ctx context.Context, pkOrCode string) (*MulticastHealthGroupPathsResponse, error) {
	group, err := a.queryMulticastDeliveryGroup(ctx, pkOrCode)
	if err != nil {
		return nil, err
	}
	items, total, err := a.queryMulticastHealthPaths(ctx, group.PK, "", MulticastHealthCachedPageSize, 0)
	if err != nil {
		return nil, err
	}
	return &MulticastHealthGroupPathsResponse{
		Group:       group,
		GeneratedAt: formatMulticastTime(time.Now().UTC()),
		Items:       items,
		Total:       total,
		Limit:       MulticastHealthCachedPageSize,
		Offset:      0,
	}, nil
}

func (a *API) queryMulticastHealthPaths(ctx context.Context, groupPK, search string, limit, offset int) ([]MulticastHealthPathItem, int, error) {
	whereClause := "multicast_group_pk = ?"
	args := []any{groupPK}
	if clause, extra := buildHealthSearchClause(search, multicastHealthPathSearchFields, multicastHealthPathSearchFallback); clause != "" {
		whereClause += clause
		args = append(args, extra...)
	}
	total := 0
	if limit > 0 {
		var err error
		total, err = a.queryMulticastHealthTotal(ctx, "health_publisher_subscriber_path", whereClause, args, "multicast_health_paths_count")
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
			multicast_group_pk,
			multicast_group_code,
			group_address,
			publisher_user_pk,
			publisher_owner_pubkey,
			publisher_dz_ip,
			publisher_tunnel_id,
			publisher_device_pk,
			publisher_device_code,
			subscriber_user_pk,
			subscriber_owner_pubkey,
			subscriber_dz_ip,
			subscriber_tunnel_id,
			subscriber_device_pk,
			subscriber_device_code,
			publisher_endpoint_observed,
			subscriber_endpoint_observed,
			endpoints_reconciled,
			health_status,
			verification_method,
			missing_endpoint_reasons
		FROM health_publisher_subscriber_path
		WHERE ` + whereClause + `
		-- Sort actionable rows first (see healthStatusSeverityOrderSQL).
		ORDER BY ` + healthStatusSeverityOrderSQL + `,
			publisher_dz_ip, subscriber_device_code, subscriber_user_pk` + limitClause + `
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, queryArgs...)
	metrics.RecordClickHouseQuery("multicast_health_paths", time.Since(start), err)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := []MulticastHealthPathItem{}
	for rows.Next() {
		var it MulticastHealthPathItem
		if err := rows.Scan(
			&it.MulticastGroupPK,
			&it.MulticastGroupCode,
			&it.GroupAddress,
			&it.PublisherUserPK,
			&it.PublisherOwnerPubkey,
			&it.PublisherDZIP,
			&it.PublisherTunnelID,
			&it.PublisherDevicePK,
			&it.PublisherDeviceCode,
			&it.SubscriberUserPK,
			&it.SubscriberOwnerPubkey,
			&it.SubscriberDZIP,
			&it.SubscriberTunnelID,
			&it.SubscriberDevicePK,
			&it.SubscriberDeviceCode,
			&it.PublisherEndpointObserved,
			&it.SubscriberEndpointObserved,
			&it.EndpointsReconciled,
			&it.HealthStatus,
			&it.VerificationMethod,
			&it.MissingEndpointReasons,
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
