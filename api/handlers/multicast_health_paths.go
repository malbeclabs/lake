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
	"github.com/malbeclabs/lake/utils/pkg/dberror"
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

// multicastHealthRootCauseLimit bounds the returned root causes; after a
// whole-group event every endpoint can be a fault, so the panel shows the
// top-N by blast radius while Total still reports the true distinct count.
const multicastHealthRootCauseLimit = 100

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

	items, total, err := a.queryMulticastHealthPathRootCauses(ctx, group.PK)
	if err != nil {
		logError("multicast group health/path-root-causes query error", "error", err, "pk", group.PK)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, MulticastHealthPathRootCausesResponse{
		Group:       group,
		GeneratedAt: formatMulticastTime(time.Now().UTC()),
		Items:       items,
		Total:       total,
	})
}

// queryMulticastHealthPathRootCauses attributes every non-healthy path to a
// single primary faulting endpoint and rolls the fan-out up per endpoint.
//
// A single scan of the (expensive, non-materialized) path view feeds an
// ARRAY JOIN over two candidate endpoint tuples per pair, each gated by the
// observed flags — replacing a UNION ALL that scanned the view twice.
// Candidates:
//   - publisher, when publisher_endpoint_observed = 0
//   - subscriber, when subscriber_endpoint_observed = 0 AND publisher is
//     observed
//
// The publisher gate on the subscriber candidate makes each pair attribute to
// a single primary endpoint: when a publisher is itself unobserved (e.g. BGP
// down) there is no (S,G) for that source, so every one of its subscribers is
// trivially unobserved too — the fault is upstream and the pair belongs to the
// publisher alone. Without this a down publisher with N subscribers would
// re-create the exact fan-out this rollup exists to collapse.
//
// A BGP-down endpoint has no session, so no (S,G)/OIF, so its *_endpoint_observed
// is false — the not-observed gate therefore catches both 'unhealthy' (present
// but not forwarding) and 'disconnected' (BGP down). endpoint_status reports the
// endpoint's own condition via dz_users_current (the path view only exposes the
// pair-level verdict, not which side is down). Grouping is by endpoint alone so
// a P+S user broken on both sides is one row with a combined role label.
//
// Returns the top-N rows by blast radius plus the true distinct-endpoint total.
func (a *API) queryMulticastHealthPathRootCauses(ctx context.Context, groupPK string) ([]MulticastHealthPathRootCause, int, error) {
	query := `
		WITH faults AS (
			SELECT
				c.1 AS faulting_role,
				c.2 AS endpoint_user_pk,
				c.3 AS endpoint_owner_pubkey,
				c.4 AS endpoint_dz_ip,
				c.5 AS endpoint_tunnel_id,
				c.6 AS endpoint_device_pk,
				c.7 AS endpoint_device_code
			FROM health_publisher_subscriber_path
			ARRAY JOIN [
				if(publisher_endpoint_observed = 0,
					('publisher', publisher_user_pk, publisher_owner_pubkey, publisher_dz_ip,
					 CAST(publisher_tunnel_id AS Int32), publisher_device_pk, publisher_device_code),
					('', '', '', '', CAST(0 AS Int32), '', '')),
				if(subscriber_endpoint_observed = 0 AND publisher_endpoint_observed = 1,
					('subscriber', subscriber_user_pk, subscriber_owner_pubkey, subscriber_dz_ip,
					 CAST(subscriber_tunnel_id AS Int32), subscriber_device_pk, subscriber_device_code),
					('', '', '', '', CAST(0 AS Int32), '', ''))
			] AS c
			WHERE multicast_group_pk = ?
			  AND health_status != 'healthy'
		),
		grouped AS (
			SELECT
				arrayStringConcat(arraySort(groupUniqArray(f.faulting_role)), '+') AS faulting_role,
				f.endpoint_user_pk AS endpoint_user_pk,
				any(f.endpoint_owner_pubkey) AS endpoint_owner_pubkey,
				any(f.endpoint_dz_ip) AS endpoint_dz_ip,
				any(f.endpoint_tunnel_id) AS endpoint_tunnel_id,
				any(f.endpoint_device_pk) AS endpoint_device_pk,
				any(f.endpoint_device_code) AS endpoint_device_code,
				if(any(u.bgp_status) = 'down', 'disconnected', 'unhealthy') AS endpoint_status,
				toInt32(count()) AS affected_pairs
			FROM faults f
			LEFT ANY JOIN dz_users_current u ON f.endpoint_user_pk = u.pk
			WHERE f.faulting_role != ''
			GROUP BY f.endpoint_user_pk
		)
		SELECT
			faulting_role,
			endpoint_user_pk,
			endpoint_owner_pubkey,
			endpoint_dz_ip,
			endpoint_tunnel_id,
			endpoint_device_pk,
			endpoint_device_code,
			endpoint_status,
			affected_pairs,
			toInt32(count() OVER ()) AS total_endpoints
		FROM grouped
		ORDER BY affected_pairs DESC, endpoint_status, endpoint_user_pk
		LIMIT ?
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, groupPK, multicastHealthRootCauseLimit)
	metrics.RecordClickHouseQuery("multicast_health_path_root_causes", time.Since(start), err)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := []MulticastHealthPathRootCause{}
	total := 0
	for rows.Next() {
		var it MulticastHealthPathRootCause
		var rowTotal int32
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
			&rowTotal,
		); err != nil {
			return nil, 0, err
		}
		total = int(rowTotal)
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
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
