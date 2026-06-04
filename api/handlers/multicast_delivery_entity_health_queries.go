package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryDeviceMulticastHealthUsers(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastHealthUserItem, int, MulticastEntityHealthStatusCounts, error) {
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	sourceFilter, sourceArgs := sqlInFilter("user_dz_ip", params.Sources)
	healthFilter, healthArgs := sqlInFilter("health_status", params.Health)
	counts, err := a.queryEntityHealthCounts(ctx, "health_multicast_user_rate", "user_device_pk = ?"+groupFilter+sourceFilter+healthFilter, append(append(append([]any{device.PK}, groupArgs...), sourceArgs...), healthArgs...), "multicast_device_health_user_counts")
	if err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
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
		WHERE user_device_pk = ?` + groupFilter + sourceFilter + healthFilter + `
		ORDER BY multiIf(health_status = 'unhealthy', 0, health_status = 'degraded', 1, health_status = 'unknown', 2, 3), multicast_group_code, user_pk
		LIMIT ? OFFSET ?
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	args := []any{device.PK}
	args = append(args, groupArgs...)
	args = append(args, sourceArgs...)
	args = append(args, healthArgs...)
	args = append(args, params.Limit, params.Offset)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_health_users", time.Since(start), err)
	if err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
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
			return nil, 0, MulticastEntityHealthStatusCounts{}, err
		}
		items = append(items, it)
	}
	return items, int(counts.Total), counts, rows.Err()
}

func (a *API) queryDeviceMulticastEndpointHealth(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastHealthPathItem, int, MulticastEntityHealthStatusCounts, error) {
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	sourceFilter, sourceArgs := sqlInFilter("publisher_dz_ip", params.Sources)
	healthFilter, healthArgs := sqlInFilter("health_status", params.Health)
	where := "(publisher_device_pk = ? OR subscriber_device_pk = ?)" + groupFilter + sourceFilter + healthFilter
	baseArgs := []any{device.PK, device.PK}
	baseArgs = append(baseArgs, groupArgs...)
	baseArgs = append(baseArgs, sourceArgs...)
	baseArgs = append(baseArgs, healthArgs...)
	counts, err := a.queryEntityHealthCounts(ctx, "health_publisher_subscriber_path", where, baseArgs, "multicast_device_endpoint_health_counts")
	if err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
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
		WHERE ` + where + `
		ORDER BY multiIf(health_status = 'unhealthy', 0, health_status = 'degraded', 1, health_status = 'unknown', 2, 3), multicast_group_code, publisher_dz_ip, subscriber_device_code, subscriber_user_pk
		LIMIT ? OFFSET ?
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	args := append([]any{}, baseArgs...)
	args = append(args, params.EndpointLimit, params.EndpointOffset)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_endpoint_health", time.Since(start), err)
	if err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
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
			return nil, 0, MulticastEntityHealthStatusCounts{}, err
		}
		items = append(items, it)
	}
	return items, int(counts.Total), counts, rows.Err()
}

func (a *API) queryLinkRelatedGroupHealth(ctx context.Context, groups []MulticastDeliveryEntityGroup) ([]MulticastDeliveryEntityGroup, MulticastEntityHealthStatusCounts, error) {
	groupPKs := make([]string, 0, len(groups))
	for _, group := range groups {
		if group.GroupPK != "" {
			groupPKs = append(groupPKs, group.GroupPK)
		}
	}
	if len(groupPKs) == 0 {
		return groups, MulticastEntityHealthStatusCounts{}, nil
	}
	query := `
		SELECT multicast_group_pk, multicast_group_code, any(group_address), health_status, count() AS n FROM (
			SELECT multicast_group_pk, multicast_group_code, group_address, health_status FROM health_mroute WHERE multicast_group_pk IN (?)
			UNION ALL
			SELECT multicast_group_pk, multicast_group_code, group_address, health_status FROM health_multicast_user_rate WHERE multicast_group_pk IN (?)
			UNION ALL
			SELECT multicast_group_pk, multicast_group_code, group_address, health_status FROM health_publisher_subscriber_path WHERE multicast_group_pk IN (?)
		)
		GROUP BY multicast_group_pk, multicast_group_code, health_status
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, groupPKs, groupPKs, groupPKs)
	metrics.RecordClickHouseQuery("multicast_link_related_group_health", time.Since(start), err)
	if err != nil {
		return groups, MulticastEntityHealthStatusCounts{}, err
	}
	defer rows.Close()
	byGroup := map[string]MulticastEntityHealthStatusCounts{}
	total := MulticastEntityHealthStatusCounts{}
	for rows.Next() {
		var groupPK, groupCode, groupAddress, status string
		var n uint64
		if err := rows.Scan(&groupPK, &groupCode, &groupAddress, &status, &n); err != nil {
			return nil, MulticastEntityHealthStatusCounts{}, err
		}
		counts := byGroup[groupPK]
		addEntityStatusCount(&counts, status, n)
		byGroup[groupPK] = counts
		addEntityStatusCount(&total, status, n)
		_ = groupCode
		_ = groupAddress
	}
	if err := rows.Err(); err != nil {
		return nil, MulticastEntityHealthStatusCounts{}, err
	}
	for i := range groups {
		if groups[i].GroupPK != "" {
			groups[i].HealthCounts = byGroup[groups[i].GroupPK]
		}
	}
	return groups, total, nil
}

func (a *API) queryEntityHealthCounts(ctx context.Context, table, whereClause string, args []any, metricName string) (MulticastEntityHealthStatusCounts, error) {
	query := `
		SELECT health_status, count() AS n
		FROM ` + table + `
		WHERE ` + whereClause + `
		GROUP BY health_status
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery(metricName, time.Since(start), err)
	if err != nil {
		return MulticastEntityHealthStatusCounts{}, err
	}
	defer rows.Close()
	counts := MulticastEntityHealthStatusCounts{}
	for rows.Next() {
		var status string
		var n uint64
		if err := rows.Scan(&status, &n); err != nil {
			return MulticastEntityHealthStatusCounts{}, err
		}
		addEntityStatusCount(&counts, status, n)
	}
	return counts, rows.Err()
}

func addEntityStatusCount(bucket *MulticastEntityHealthStatusCounts, status string, n uint64) {
	if bucket == nil {
		return
	}
	switch status {
	case "healthy":
		bucket.Healthy += n
	case "degraded":
		bucket.Degraded += n
	case "unhealthy":
		bucket.Unhealthy += n
	case "unknown", "":
		bucket.Unknown += n
	default:
		bucket.Unknown += n
	}
	bucket.Total += n
}
