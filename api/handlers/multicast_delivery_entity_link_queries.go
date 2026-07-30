package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryMulticastLinkDeliveryBranches(ctx context.Context, link MulticastDeliveryLink, params multicastDeliveryEntityParams) ([]MulticastDeliveryLinkBranch, []time.Time, int, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	oifKindFilter, oifKindArgs := sqlInFilter("oif_kind", params.OIFKinds)
	query := `
		SELECT
			mroute_entity_id,
			snapshot_ts,
			device_pk,
			device_code,
			group_address,
			multicast_group_pk,
			multicast_group_code,
			source_address,
			vrf,
			mode,
			publisher_user_pk,
			publisher_device_pk,
			publisher_device_code,
			oif_name,
			link_pk,
			link_code,
			link_side,
			peer_device_pk,
			peer_device_code,
			peer_interface_name,
			link_type,
			bandwidth_bps,
			link_topologies,
			unicast_drained,
			interface_type,
			routing_mode,
			interface_bandwidth,
			interface_mtu,
			user_tunnel_endpoint,
			subscriber_user_pk,
			subscriber_device_pk,
			subscriber_device_code,
			subscriber_tunnel_id,
			subscriber_owner_pubkey,
			subscriber_dz_ip,
			subscriber_client_ip,
			oif_kind,
			observed_delivery_role,
			mroute_id,
			direction,
			count() OVER () AS total_count
		FROM (
			SELECT
				mroute_entity_id,
				"o.snapshot_ts" AS snapshot_ts,
				device_pk,
				device_code,
				group_address,
				multicast_group_pk,
				multicast_group_code,
				source_address,
				vrf,
				mode,
				publisher_user_pk,
				publisher_device_pk,
				publisher_device_code,
				oif_name,
				link_pk,
				link_code,
				link_side,
				peer_device_pk,
				peer_device_code,
				peer_interface_name,
				link_type,
				bandwidth_bps,
				link_topologies,
				unicast_drained,
				interface_type,
				routing_mode,
				interface_bandwidth,
				interface_mtu,
				user_tunnel_endpoint,
				subscriber_user_pk,
				subscriber_device_pk,
				subscriber_device_code,
				subscriber_tunnel_id,
				subscriber_owner_pubkey,
				subscriber_dz_ip,
				subscriber_client_ip,
				oif_kind,
				observed_delivery_role,
				mroute_id,
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
		ORDER BY multicast_group_code, group_address, source_address, device_code, oif_name
		LIMIT ? OFFSET ?
		` + multicastDeliveryQuerySettings + `
	`
	args := []any{link.SideAPK, link.SideZPK, link.SideZPK, link.SideAPK, link.PK, link.Code}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, oifKindArgs...)
	args = append(args, params.Direction, params.Direction, params.Limit, params.Offset)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_link_delivery_branches", time.Since(start), err)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	branches := []MulticastDeliveryLinkBranch{}
	times := []time.Time{}
	total := 0
	for rows.Next() {
		var s multicastOIFScan
		var vrf, routeMode string
		var direction string
		var rowTotal uint64
		if err := rows.Scan(
			&s.OIF.EntityID,
			&s.snapshotTS,
			&s.OIF.DevicePK,
			&s.OIF.DeviceCode,
			&s.OIF.GroupAddress,
			&s.OIF.MulticastGroupPK,
			&s.OIF.MulticastGroupCode,
			&s.OIF.SourceAddress,
			&vrf,
			&routeMode,
			&s.OIF.PublisherUserPK,
			&s.OIF.PublisherDevicePK,
			&s.OIF.PublisherDeviceCode,
			&s.OIF.OIFName,
			&s.OIF.LinkPK,
			&s.OIF.LinkCode,
			&s.OIF.LinkSide,
			&s.OIF.PeerDevicePK,
			&s.OIF.PeerDeviceCode,
			&s.OIF.PeerInterfaceName,
			&s.OIF.LinkType,
			&s.OIF.BandwidthBPS,
			&s.OIF.LinkTopologies,
			&s.unicastDrained,
			&s.OIF.InterfaceType,
			&s.OIF.RoutingMode,
			&s.OIF.InterfaceBandwidth,
			&s.OIF.InterfaceMTU,
			&s.userTunnelEndpoint,
			&s.OIF.SubscriberUserPK,
			&s.OIF.SubscriberDevicePK,
			&s.OIF.SubscriberDeviceCode,
			&s.OIF.SubscriberTunnelID,
			&s.OIF.SubscriberOwnerPubkey,
			&s.OIF.SubscriberDZIP,
			&s.OIF.SubscriberClientIP,
			&s.OIF.OIFKind,
			&s.OIF.ObservedDeliveryRole,
			&s.OIF.MrouteID,
			&direction,
			&rowTotal,
		); err != nil {
			return nil, nil, 0, err
		}
		total = int(rowTotal)
		s.OIF.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.OIF.AgeSeconds = ageSeconds(s.snapshotTS)
		s.OIF.FreshnessStatus = multicastFreshnessStatus(s.OIF.AgeSeconds)
		s.OIF.UnicastDrained = s.unicastDrained != 0
		s.OIF.UserTunnelEndpoint = s.userTunnelEndpoint != 0
		branches = append(branches, MulticastDeliveryLinkBranch{
			MulticastDeliveryOIF: s.OIF,
			Direction:            direction,
		})
		times = append(times, s.snapshotTS)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	// Offset past the end: no page row carries the window total (see
	// queryMulticastDeviceDeliveryMroutes).
	if len(branches) == 0 && params.Offset > 0 {
		total, err = a.countMulticastLinkDeliveryBranches(ctx, link, params)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	return branches, times, total, nil
}

func (a *API) countMulticastLinkDeliveryBranches(ctx context.Context, link MulticastDeliveryLink, params multicastDeliveryEntityParams) (int, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	oifKindFilter, oifKindArgs := sqlInFilter("oif_kind", params.OIFKinds)
	query := `
		SELECT count()
		FROM (
			SELECT
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
		` + multicastDeliveryFallbackQuerySettings(ctx) + `
	`
	args := []any{link.SideAPK, link.SideZPK, link.SideZPK, link.SideAPK, link.PK, link.Code}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, oifKindArgs...)
	args = append(args, params.Direction, params.Direction)
	var total uint64
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, args...).Scan(&total)
	metrics.RecordClickHouseQuery("multicast_link_delivery_branch_count", time.Since(start), err)
	return int(total), err
}
