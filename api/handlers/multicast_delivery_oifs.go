package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryMulticastDeliveryOIFs(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryOIF, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("r.source_address", params.Sources)
	query := `
		WITH routes_filtered AS (
			SELECT *
			FROM dz_ip_mroute_entries_current r
			WHERE r.group_address = ?` + sourceFilter + `
		),
		routes_enriched AS (
			SELECT
				r.entity_id AS entity_id,
				r.snapshot_ts AS snapshot_ts,
				r.device_pubkey AS device_pubkey,
				r.group_address AS group_address,
				r.source_address AS source_address,
				r.vrf AS vrf,
				r.mode AS mode,
				r.oif_list AS oif_list,
				d.code AS device_code,
				pub.pk AS publisher_user_pk,
				pub.device_pk AS publisher_device_pk
			FROM routes_filtered r
			LEFT ANY JOIN dz_devices_current d ON r.device_pubkey = d.pk
			LEFT ANY JOIN (
				SELECT pk, dz_ip, device_pk
				FROM dz_users_current
				WHERE status = 'activated'
					AND kind = 'multicast'
					AND has(JSONExtract(publishers, 'Array(String)'), ?)
			) pub ON r.source_address = pub.dz_ip
		),
		oif_rows AS (
			SELECT
				re.entity_id AS entity_id,
				re.snapshot_ts AS snapshot_ts,
				re.device_pubkey AS device_pubkey,
				re.device_code AS device_code,
				re.group_address AS group_address,
				re.source_address AS source_address,
				re.vrf AS vrf,
				re.mode AS mode,
				re.publisher_user_pk AS publisher_user_pk,
				re.publisher_device_pk AS publisher_device_pk,
				oif_name
			FROM routes_enriched re
			ARRAY JOIN JSONExtract(re.oif_list, 'Array(String)') AS oif_name
			WHERE re.oif_list != ''
		)
		SELECT
			o.entity_id,
			o.snapshot_ts,
			o.device_pubkey,
			o.device_code,
			o.group_address,
			o.source_address,
			o.vrf,
			o.mode,
			o.publisher_user_pk,
			o.publisher_device_pk,
			o.oif_name,
			if(la.pk != '', la.pk, lz.pk) AS link_pk,
			if(la.pk != '', la.code, lz.code) AS link_code,
			if(la.pk != '', 'a', if(lz.pk != '', 'z', '')) AS link_side,
			if(la.pk != '', la.side_z_pk, if(lz.pk != '', lz.side_a_pk, '')) AS peer_device_pk,
			if(la.pk != '', peer_a.code, if(lz.pk != '', peer_z.code, '')) AS peer_device_code,
			if(la.pk != '', la.side_z_iface_name, if(lz.pk != '', lz.side_a_iface_name, '')) AS peer_interface_name,
			if(la.pk != '', la.link_type, lz.link_type) AS link_type,
			if(la.pk != '', la.bandwidth_bps, lz.bandwidth_bps) AS bandwidth_bps,
			if(la.pk != '', la.link_topologies, lz.link_topologies) AS link_topologies,
			if(la.pk != '', la.unicast_drained, lz.unicast_drained) AS unicast_drained,
			iface.interface_type,
			iface.routing_mode,
			iface.bandwidth AS interface_bandwidth,
			iface.mtu AS interface_mtu,
			iface.user_tunnel_endpoint,
			sub.pk AS subscriber_user_pk,
			sub.device_pk AS subscriber_device_pk,
			sub_dev.code AS subscriber_device_code,
			sub.tunnel_id AS subscriber_tunnel_id,
			sub.owner_pubkey AS subscriber_owner_pubkey,
			sub.dz_ip AS subscriber_dz_ip,
			sub.client_ip AS subscriber_client_ip
		FROM oif_rows o
		LEFT ANY JOIN dz_links_current la ON o.device_pubkey = la.side_a_pk AND o.oif_name = la.side_a_iface_name
		LEFT ANY JOIN dz_links_current lz ON o.device_pubkey = lz.side_z_pk AND o.oif_name = lz.side_z_iface_name
		LEFT ANY JOIN dz_devices_current peer_a ON la.side_z_pk = peer_a.pk
		LEFT ANY JOIN dz_devices_current peer_z ON lz.side_a_pk = peer_z.pk
		LEFT ANY JOIN dz_device_interfaces_current iface ON o.device_pubkey = iface.device_pk AND o.oif_name = iface.intf
		LEFT ANY JOIN (
			SELECT pk, owner_pubkey, client_ip, dz_ip, device_pk, tunnel_id
			FROM dz_users_current
			WHERE status = 'activated'
				AND kind = 'multicast'
				AND has(JSONExtract(subscribers, 'Array(String)'), ?)
		) sub ON sub.device_pk = o.device_pubkey
			AND sub.tunnel_id = toInt32OrZero(extract(o.oif_name, '^Tunnel(\\d+)$'))
		LEFT ANY JOIN dz_devices_current sub_dev ON sub.device_pk = sub_dev.pk
		ORDER BY o.source_address, o.device_pubkey, o.oif_name
		LIMIT ?
		SETTINGS max_execution_time = 15,
			max_result_rows = 20000,
			result_overflow_mode = 'break',
			timeout_before_checking_execution_speed = 0
	`
	args := []any{group.MulticastIP}
	args = append(args, sourceArgs...)
	args = append(args, group.PK, group.PK, multicastDeliveryMaxOIFRows)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_delivery_oifs", time.Since(start), err)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	oifs := []MulticastDeliveryOIF{}
	times := []time.Time{}
	for rows.Next() {
		var s multicastOIFScan
		var vrf, routeMode string
		if err := rows.Scan(
			&s.OIF.EntityID,
			&s.snapshotTS,
			&s.OIF.DevicePK,
			&s.OIF.DeviceCode,
			&s.OIF.GroupAddress,
			&s.OIF.SourceAddress,
			&vrf,
			&routeMode,
			&s.OIF.PublisherUserPK,
			&s.OIF.PublisherDevicePK,
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
		); err != nil {
			return nil, nil, err
		}
		s.OIF.RouteID = multicastRouteID(s.OIF.DevicePK, vrf, routeMode, s.OIF.GroupAddress, s.OIF.SourceAddress)
		s.OIF.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.OIF.AgeSeconds = ageSeconds(s.snapshotTS)
		s.OIF.FreshnessStatus = multicastFreshnessStatus(s.OIF.AgeSeconds)
		s.OIF.UnicastDrained = s.unicastDrained != 0
		s.OIF.UserTunnelEndpoint = s.userTunnelEndpoint != 0
		s.OIF.OIFKind, s.OIF.ObservedDeliveryRole = classifyMulticastOIF(s.OIF)
		if multicastDeliveryOIFMatches(s.OIF, params) {
			oifs = append(oifs, s.OIF)
			times = append(times, s.snapshotTS)
		}
	}
	return oifs, times, rows.Err()
}
