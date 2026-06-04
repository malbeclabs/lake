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
	items, total, err := a.queryMulticastHealthPaths(ctx, group.PK, limit, offset)
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

func (a *API) queryMulticastHealthPaths(ctx context.Context, groupPK string, limit, offset int) ([]MulticastHealthPathItem, int, error) {
	whereClause := "multicast_group_pk = ?"
	args := []any{groupPK}
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
