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

// multicastHealthPathSearchCols are the columns the `?search=` filter
// substring-matches across (case-insensitive).
var multicastHealthPathSearchCols = []string{
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
	if clause, extra := buildHealthSearchClause(search, multicastHealthPathSearchCols); clause != "" {
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
		-- Sort actionable rows first so paginated consumers land on the
		-- unhealthy/degraded pairs first.
		ORDER BY
			multiIf(health_status = 'unhealthy', 0,
			        health_status = 'degraded',  1,
			        health_status = 'unknown',   2,
			                                     3),
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
