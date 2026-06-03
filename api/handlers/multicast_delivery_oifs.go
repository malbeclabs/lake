package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// queryMulticastDeliveryOIFs reads pre-enriched per-OIF rows from the
// enriched_ip_mroute_oifs view. The view owns OIF expansion, classification,
// and underlay-link/subscriber-tunnel/local-interface enrichment.
func (a *API) queryMulticastDeliveryOIFs(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryOIF, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	query := `
		SELECT
			mroute_entity_id,
			"o.snapshot_ts" AS snapshot_ts,
			device_pk,
			device_code,
			group_address,
			source_address,
			vrf,
			mode,
			publisher_user_pk,
			publisher_device_pk,
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
			mroute_id
		FROM enriched_ip_mroute_oifs
		WHERE multicast_group_pk = ?` + sourceFilter + `
		ORDER BY source_address, device_pk, oif_name
		SETTINGS max_execution_time = 30,
			timeout_before_checking_execution_speed = 0
	`
	args := []any{group.PK}
	args = append(args, sourceArgs...)

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
			&s.OIF.OIFKind,
			&s.OIF.ObservedDeliveryRole,
			&s.OIF.MrouteID,
		); err != nil {
			return nil, nil, err
		}
		s.OIF.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.OIF.AgeSeconds = ageSeconds(s.snapshotTS)
		s.OIF.FreshnessStatus = multicastFreshnessStatus(s.OIF.AgeSeconds)
		s.OIF.UnicastDrained = s.unicastDrained != 0
		s.OIF.UserTunnelEndpoint = s.userTunnelEndpoint != 0
		if multicastDeliveryOIFMatches(s.OIF, params) {
			oifs = append(oifs, s.OIF)
			times = append(times, s.snapshotTS)
		}
	}
	return oifs, times, rows.Err()
}
