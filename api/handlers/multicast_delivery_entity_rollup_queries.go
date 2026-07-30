package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryMulticastDeviceDeliveryGroups(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastDeliveryEntityGroup, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	oifKindFilter, oifKindArgs := sqlInFilter("oif_kind", params.OIFKinds)
	query := `
		SELECT
			multicast_group_pk,
			any(multicast_group_code) AS multicast_group_code,
			any(group_address) AS group_address,
			uniqExact(source_address) AS source_count,
			sum(mroute_count) AS mroute_count,
			sum(oif_count) AS oif_count
		FROM (
			SELECT multicast_group_pk, multicast_group_code, group_address, source_address, 1 AS mroute_count, 0 AS oif_count
			FROM enriched_ip_mroute
			WHERE (device_pk = ? OR publisher_device_pk = ?)` + sourceFilter + groupFilter + `
			UNION ALL
			SELECT multicast_group_pk, multicast_group_code, group_address, source_address, 0 AS mroute_count, 1 AS oif_count
			FROM enriched_ip_mroute_oifs
			WHERE (device_pk = ? OR publisher_device_pk = ? OR subscriber_device_pk = ? OR peer_device_pk = ?)` + sourceFilter + groupFilter + oifKindFilter + `
		)
		GROUP BY multicast_group_pk
		ORDER BY multicast_group_code, group_address, multicast_group_pk
		LIMIT 500
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{device.PK, device.PK}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, device.PK, device.PK, device.PK, device.PK)
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, oifKindArgs...)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_delivery_groups", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := []MulticastDeliveryEntityGroup{}
	for rows.Next() {
		var group MulticastDeliveryEntityGroup
		var sourceCount, mrouteCount, oifCount uint64
		if err := rows.Scan(&group.GroupPK, &group.GroupCode, &group.GroupAddress, &sourceCount, &mrouteCount, &oifCount); err != nil {
			return nil, err
		}
		group.SourceCount = int(sourceCount)
		group.MrouteCount = int(mrouteCount)
		group.OIFCount = int(oifCount)
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

func (a *API) countMulticastDeviceDeliveryMroutes(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) (int, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	query := `
		SELECT count()
		FROM enriched_ip_mroute
		WHERE (device_pk = ? OR publisher_device_pk = ?)` + sourceFilter + groupFilter + `
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{device.PK, device.PK}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	var total uint64
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, args...).Scan(&total)
	metrics.RecordClickHouseQuery("multicast_device_delivery_mroute_count", time.Since(start), err)
	return int(total), err
}

func (a *API) countMulticastDeviceDeliveryOIFs(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) (int, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	oifKindFilter, oifKindArgs := sqlInFilter("oif_kind", params.OIFKinds)
	query := `
		SELECT count()
		FROM enriched_ip_mroute_oifs
		WHERE (device_pk = ? OR publisher_device_pk = ? OR subscriber_device_pk = ? OR peer_device_pk = ?)` + sourceFilter + groupFilter + oifKindFilter + `
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{device.PK, device.PK, device.PK, device.PK}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, oifKindArgs...)
	var total uint64
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, args...).Scan(&total)
	metrics.RecordClickHouseQuery("multicast_device_delivery_oif_count", time.Since(start), err)
	return int(total), err
}

func (a *API) queryMulticastLinkDeliveryGroups(ctx context.Context, link MulticastDeliveryLink, params multicastDeliveryEntityParams) ([]MulticastDeliveryEntityGroup, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	oifKindFilter, oifKindArgs := sqlInFilter("oif_kind", params.OIFKinds)
	query := `
		SELECT
			multicast_group_pk,
			any(multicast_group_code) AS multicast_group_code,
			any(group_address) AS group_address,
			uniqExact(source_address) AS source_count,
			toUInt64(0) AS mroute_count,
			count() AS oif_count
		FROM (
			SELECT
				multicast_group_pk,
				multicast_group_code,
				group_address,
				source_address,
				multiIf(
					link_side = 'a', 'a_to_z',
					link_side = 'z', 'z_to_a',
					device_pk = ? AND peer_device_pk = ?, 'a_to_z',
					device_pk = ? AND peer_device_pk = ?, 'z_to_a',
					'unknown'
				) AS direction
			FROM enriched_ip_mroute_oifs
			WHERE (link_pk = ? OR link_code = ?)` + sourceFilter + groupFilter + oifKindFilter + `
		)
		WHERE (? = '' OR direction = ?)
		GROUP BY multicast_group_pk
		ORDER BY multicast_group_code, group_address, multicast_group_pk
		LIMIT 500
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{link.SideAPK, link.SideZPK, link.SideZPK, link.SideAPK, link.PK, link.Code}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, oifKindArgs...)
	args = append(args, params.Direction, params.Direction)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_link_delivery_groups", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := []MulticastDeliveryEntityGroup{}
	for rows.Next() {
		var group MulticastDeliveryEntityGroup
		var sourceCount, mrouteCount, oifCount uint64
		if err := rows.Scan(&group.GroupPK, &group.GroupCode, &group.GroupAddress, &sourceCount, &mrouteCount, &oifCount); err != nil {
			return nil, err
		}
		group.SourceCount = int(sourceCount)
		group.MrouteCount = int(mrouteCount)
		group.OIFCount = int(oifCount)
		groups = append(groups, group)
	}
	return groups, rows.Err()
}
