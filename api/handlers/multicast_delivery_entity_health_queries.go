package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// multicastHealthWindowCountsSQL folds the health-status buckets a paged health
// query needs into the page query itself, so the totals cost no extra scan of
// the table.
const multicastHealthWindowCountsSQL = `
			count() OVER () AS health_total,
			countIf(health_status = 'healthy') OVER () AS health_healthy,
			countIf(health_status = 'degraded') OVER () AS health_degraded,
			countIf(health_status = 'unhealthy') OVER () AS health_unhealthy,
			countIf(health_status = 'disconnected') OVER () AS health_disconnected`

// multicastHealthWindowScan holds the multicastHealthWindowCountsSQL columns of
// one page row.
type multicastHealthWindowScan struct {
	total        uint64
	healthy      uint64
	degraded     uint64
	unhealthy    uint64
	disconnected uint64
}

// counts derives Unknown as the remainder rather than counting it, so an
// unrecognized health_status lands in Unknown exactly as addEntityStatusCount
// puts it there.
func (w multicastHealthWindowScan) counts() MulticastEntityHealthStatusCounts {
	counts := MulticastEntityHealthStatusCounts{
		Healthy:      w.healthy,
		Degraded:     w.degraded,
		Unhealthy:    w.unhealthy,
		Disconnected: w.disconnected,
		Total:        w.total,
	}
	if named := w.healthy + w.degraded + w.unhealthy + w.disconnected; w.total > named {
		counts.Unknown = w.total - named
	}
	return counts
}

func (a *API) queryDeviceMulticastHealthUsers(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastHealthUserItem, int, MulticastEntityHealthStatusCounts, error) {
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	endpointFilter, endpointArgs := sqlInFilter("user_dz_ip", params.EndpointIPs)
	healthFilter, healthArgs := sqlInFilter("health_status", params.Health)
	where := "user_device_pk = ?" + groupFilter + endpointFilter + healthFilter
	baseArgs := []any{device.PK}
	baseArgs = append(baseArgs, groupArgs...)
	baseArgs = append(baseArgs, endpointArgs...)
	baseArgs = append(baseArgs, healthArgs...)
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
			health_status,` + multicastHealthWindowCountsSQL + `
		FROM health_multicast_user_rate
		WHERE ` + where + `
		ORDER BY ` + healthStatusSeverityOrderSQL + `,multicast_group_code, user_pk
		LIMIT ? OFFSET ?
		` + multicastDeliveryQuerySettings + `
	`
	args := append([]any{}, baseArgs...)
	args = append(args, params.Limit, params.Offset)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_health_users", time.Since(start), err)
	if err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
	}
	defer rows.Close()
	items := []MulticastHealthUserItem{}
	counts := MulticastEntityHealthStatusCounts{}
	for rows.Next() {
		var it MulticastHealthUserItem
		var w multicastHealthWindowScan
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
			&w.total,
			&w.healthy,
			&w.degraded,
			&w.unhealthy,
			&w.disconnected,
		); err != nil {
			return nil, 0, MulticastEntityHealthStatusCounts{}, err
		}
		counts = w.counts()
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
	}
	// Offset past the end: no page row carries the window counts (see
	// queryMulticastDeviceDeliveryMroutes).
	if len(items) == 0 && params.Offset > 0 {
		counts, err = a.queryEntityHealthCounts(ctx, "health_multicast_user_rate", where, baseArgs, "multicast_device_health_user_counts")
		if err != nil {
			return nil, 0, MulticastEntityHealthStatusCounts{}, err
		}
	}
	return items, int(counts.Total), counts, nil
}

func (a *API) queryDeviceMulticastEndpointHealth(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastHealthPathItem, int, MulticastEntityHealthStatusCounts, error) {
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	endpointFilter, endpointArgs := sqlEndpointIPFilter(params.EndpointIPs)
	healthFilter, healthArgs := sqlInFilter("health_status", params.Health)
	where := "(publisher_device_pk = ? OR subscriber_device_pk = ?)" + groupFilter + endpointFilter + healthFilter
	baseArgs := []any{device.PK, device.PK}
	baseArgs = append(baseArgs, groupArgs...)
	baseArgs = append(baseArgs, endpointArgs...)
	baseArgs = append(baseArgs, healthArgs...)
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
			missing_endpoint_reasons,` + multicastHealthWindowCountsSQL + `
		FROM health_publisher_subscriber_path
		WHERE ` + where + `
		ORDER BY ` + healthStatusSeverityOrderSQL + `,multicast_group_code, publisher_dz_ip, subscriber_device_code, subscriber_user_pk
		LIMIT ? OFFSET ?
		` + multicastDeliveryQuerySettings + `
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
	counts := MulticastEntityHealthStatusCounts{}
	for rows.Next() {
		var it MulticastHealthPathItem
		var w multicastHealthWindowScan
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
			&w.total,
			&w.healthy,
			&w.degraded,
			&w.unhealthy,
			&w.disconnected,
		); err != nil {
			return nil, 0, MulticastEntityHealthStatusCounts{}, err
		}
		counts = w.counts()
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, MulticastEntityHealthStatusCounts{}, err
	}
	// Offset past the end: no page row carries the window counts (see
	// queryMulticastDeviceDeliveryMroutes).
	if len(items) == 0 && params.EndpointOffset > 0 {
		counts, err = a.queryEntityHealthCounts(ctx, "health_publisher_subscriber_path", where, baseArgs, "multicast_device_endpoint_health_counts")
		if err != nil {
			return nil, 0, MulticastEntityHealthStatusCounts{}, err
		}
	}
	return items, int(counts.Total), counts, nil
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
		SELECT multicast_group_pk, health_status, count() AS n FROM (
			SELECT multicast_group_pk, health_status FROM health_mroute WHERE multicast_group_pk IN (?)
			UNION ALL
			SELECT multicast_group_pk, health_status FROM health_multicast_user_rate WHERE multicast_group_pk IN (?)
			UNION ALL
			SELECT multicast_group_pk, health_status FROM health_publisher_subscriber_path WHERE multicast_group_pk IN (?)
		)
		GROUP BY multicast_group_pk, health_status
		` + multicastDeliveryQuerySettings + `
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
		var groupPK, status string
		var n uint64
		if err := rows.Scan(&groupPK, &status, &n); err != nil {
			return nil, MulticastEntityHealthStatusCounts{}, err
		}
		counts := byGroup[groupPK]
		addEntityStatusCount(&counts, status, n)
		byGroup[groupPK] = counts
		addEntityStatusCount(&total, status, n)
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

func sqlEndpointIPFilter(values []string) (string, []any) {
	if len(values) == 0 {
		return "", nil
	}
	return " AND (publisher_dz_ip IN (?) OR subscriber_dz_ip IN (?))", []any{values, values}
}

func (a *API) queryEntityHealthCounts(ctx context.Context, table, whereClause string, args []any, metricName string) (MulticastEntityHealthStatusCounts, error) {
	query := `
		SELECT health_status, count() AS n
		FROM ` + table + `
		WHERE ` + whereClause + `
		GROUP BY health_status
		` + multicastDeliveryFallbackQuerySettings(ctx) + `
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
	case "disconnected":
		bucket.Disconnected += n
	case "unknown", "":
		bucket.Unknown += n
	default:
		bucket.Unknown += n
	}
	bucket.Total += n
}
