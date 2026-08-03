package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryMulticastDeliveryDevice(ctx context.Context, pkOrCode string) (MulticastDeliveryDevice, error) {
	query := `
		SELECT
			d.pk,
			d.code,
			d.status,
			d.device_type,
			COALESCE(d.contributor_pk, '') AS contributor_pk,
			COALESCE(c.code, '') AS contributor_code,
			COALESCE(d.metro_pk, '') AS metro_pk,
			COALESCE(m.code, '') AS metro_code
		FROM dz_devices_current d
		LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
		WHERE d.pk = ? OR d.code = ?
		LIMIT 1
		SETTINGS max_execution_time = 10, timeout_before_checking_execution_speed = 0
	`
	var device MulticastDeliveryDevice
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, pkOrCode, pkOrCode).Scan(
		&device.PK,
		&device.Code,
		&device.Status,
		&device.DeviceType,
		&device.ContributorPK,
		&device.ContributorCode,
		&device.MetroPK,
		&device.MetroCode,
	)
	metrics.RecordClickHouseQuery("multicast_delivery_device", time.Since(start), err)
	return device, err
}

func (a *API) queryMulticastDeliveryLink(ctx context.Context, pkOrCode string) (MulticastDeliveryLink, error) {
	query := `
		SELECT
			l.pk,
			l.code,
			l.status,
			l.link_type,
			COALESCE(l.side_a_pk, '') AS side_a_pk,
			COALESCE(da.code, '') AS side_a_code,
			COALESCE(l.side_a_iface_name, '') AS side_a_iface_name,
			COALESCE(l.side_z_pk, '') AS side_z_pk,
			COALESCE(dz.code, '') AS side_z_code,
			COALESCE(l.side_z_iface_name, '') AS side_z_iface_name,
			COALESCE(l.contributor_pk, '') AS contributor_pk,
			COALESCE(c.code, '') AS contributor_code
		FROM dz_links_current l
		LEFT ANY JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT ANY JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT ANY JOIN dz_contributors_current c ON l.contributor_pk = c.pk
		WHERE l.pk = ? OR l.code = ?
		LIMIT 1
		SETTINGS max_execution_time = 10, timeout_before_checking_execution_speed = 0
	`
	var link MulticastDeliveryLink
	start := time.Now()
	err := a.envDB(ctx).QueryRow(ctx, query, pkOrCode, pkOrCode).Scan(
		&link.PK,
		&link.Code,
		&link.Status,
		&link.LinkType,
		&link.SideAPK,
		&link.SideACode,
		&link.SideAIfaceName,
		&link.SideZPK,
		&link.SideZCode,
		&link.SideZIfaceName,
		&link.ContributorPK,
		&link.ContributorCode,
	)
	metrics.RecordClickHouseQuery("multicast_delivery_link", time.Since(start), err)
	return link, err
}

func (a *API) queryMulticastDeviceDeliveryMroutes(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastDeliveryMroute, []time.Time, int, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	query := `
		SELECT
			mroute_entity_id,
			"r.snapshot_ts" AS snapshot_ts,
			"r.ingested_at" AS ingested_at,
			device_pk,
			device_code,
			device_status,
			"d.device_type" AS device_type,
			metro_code,
			contributor_code,
			vrf,
			mode,
			group_address,
			multicast_group_pk,
			multicast_group_code,
			source_address,
			route_flags,
			register_in_oif_list,
			rpf_interface,
			rpf_rib,
			rpf_prefix,
			rpf_preference,
			rpf_metric,
			rpf_neighbor,
			rpf_attached,
			rpf_has_block,
			oif_list,
			oif_count,
			creation_time,
			publisher_user_pk,
			publisher_device_pk,
			publisher_device_code,
			publisher_metro_code,
			publisher_contributor_code,
			publisher_tunnel_id,
			publisher_owner_pubkey,
			publisher_dz_ip,
			source_match_status,
			mroute_id,
			count() OVER () AS total_count
		FROM enriched_ip_mroute
		WHERE (device_pk = ? OR publisher_device_pk = ?)` + sourceFilter + groupFilter + `
		ORDER BY multicast_group_code, group_address, source_address, device_code, vrf, mode
		LIMIT ? OFFSET ?
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{device.PK, device.PK}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, params.Limit, params.Offset)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_delivery_mroutes", time.Since(start), err)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	mroutes := []MulticastDeliveryMroute{}
	times := []time.Time{}
	total := 0
	for rows.Next() {
		var s multicastMrouteScan
		var rowTotal uint64
		if err := rows.Scan(
			&s.Mroute.EntityID,
			&s.snapshotTS,
			&s.ingestedAt,
			&s.Mroute.DevicePK,
			&s.Mroute.DeviceCode,
			&s.Mroute.DeviceStatus,
			&s.Mroute.DeviceType,
			&s.Mroute.MetroCode,
			&s.Mroute.ContributorCode,
			&s.Mroute.VRF,
			&s.Mroute.Mode,
			&s.Mroute.GroupAddress,
			&s.Mroute.MulticastGroupPK,
			&s.Mroute.MulticastGroupCode,
			&s.Mroute.SourceAddress,
			&s.Mroute.RouteFlags,
			&s.registerInOIFList,
			&s.Mroute.RPFInterface,
			&s.Mroute.RPFRIB,
			&s.Mroute.RPFPrefix,
			&s.Mroute.RPFPreference,
			&s.Mroute.RPFMetric,
			&s.Mroute.RPFNeighbor,
			&s.rpfAttached,
			&s.rpfHasBlock,
			&s.Mroute.OIFList,
			&s.Mroute.OIFCount,
			&s.creationTime,
			&s.Mroute.PublisherUserPK,
			&s.Mroute.PublisherDevicePK,
			&s.Mroute.PublisherDeviceCode,
			&s.Mroute.PublisherMetroCode,
			&s.Mroute.PublisherContributorCode,
			&s.Mroute.PublisherTunnelID,
			&s.Mroute.PublisherOwnerPubkey,
			&s.Mroute.PublisherDZIP,
			&s.Mroute.SourceMatchStatus,
			&s.Mroute.MrouteID,
			&rowTotal,
		); err != nil {
			return nil, nil, 0, err
		}
		total = int(rowTotal)
		s.Mroute.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.Mroute.IngestedAt = formatMulticastTime(s.ingestedAt)
		s.Mroute.CreationTime = formatMulticastTime(s.creationTime)
		s.Mroute.AgeSeconds = ageSeconds(s.snapshotTS)
		s.Mroute.FreshnessStatus = multicastFreshnessStatus(s.Mroute.AgeSeconds)
		s.Mroute.RegisterInOIFList = s.registerInOIFList != 0
		s.Mroute.RPFAttached = s.rpfAttached != 0
		s.Mroute.RPFHasBlock = s.rpfHasBlock != 0
		mroutes = append(mroutes, s.Mroute)
		times = append(times, s.snapshotTS)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	// The total rides on the page rows as a window aggregate, which saves a
	// second full scan of the view but leaves nothing to read it from when an
	// offset lands past the end. Only then does the count query still run; at
	// offset 0 an empty page means the total really is zero.
	if len(mroutes) == 0 && params.Offset > 0 {
		total, err = a.countMulticastDeviceDeliveryMroutes(ctx, device, params)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	return mroutes, times, total, nil
}

func (a *API) queryMulticastDeviceDeliveryOIFs(ctx context.Context, device MulticastDeliveryDevice, params multicastDeliveryEntityParams) ([]MulticastDeliveryOIF, []time.Time, int, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	oifKindFilter, oifKindArgs := sqlInFilter("oif_kind", params.OIFKinds)
	query := `
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
			count() OVER () AS total_count
		FROM enriched_ip_mroute_oifs
		WHERE (device_pk = ? OR publisher_device_pk = ? OR subscriber_device_pk = ? OR peer_device_pk = ?)` + sourceFilter + groupFilter + oifKindFilter + `
		ORDER BY multiIf(oif_kind = 'unknown', 0, oif_kind = 'subscriber_tunnel', 1, 2), multicast_group_code, group_address, source_address, device_code, oif_name
		LIMIT ? OFFSET ?
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{device.PK, device.PK, device.PK, device.PK}
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	args = append(args, oifKindArgs...)
	args = append(args, params.Limit, params.Offset)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_delivery_oifs", time.Since(start), err)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	oifs := []MulticastDeliveryOIF{}
	times := []time.Time{}
	total := 0
	for rows.Next() {
		var s multicastOIFScan
		var vrf, routeMode string
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
		oifs = append(oifs, s.OIF)
		times = append(times, s.snapshotTS)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0, err
	}
	// Offset past the end: no page row carries the window total (see
	// queryMulticastDeviceDeliveryMroutes).
	if len(oifs) == 0 && params.Offset > 0 {
		total, err = a.countMulticastDeviceDeliveryOIFs(ctx, device, params)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	return oifs, times, total, nil
}

func (a *API) queryMulticastDeviceDeliveryMSDPPeers(ctx context.Context, device MulticastDeliveryDevice) ([]MulticastDeliveryMSDPPeer, []time.Time, error) {
	query := `
		SELECT
			msdp_peer_entity_id,
			"p.snapshot_ts" AS snapshot_ts,
			device_pk,
			device_code,
			peer_address,
			peer_device_pk,
			peer_device_code,
			peer_interface_name,
			state,
			session_start_time,
			sa_count,
			reset_count
		FROM enriched_ip_msdp_peers
		WHERE device_pk = ? OR peer_device_pk = ?
		ORDER BY device_code, peer_address
		LIMIT 2000
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, device.PK, device.PK)
	metrics.RecordClickHouseQuery("multicast_device_delivery_msdp_peers", time.Since(start), err)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	peers := []MulticastDeliveryMSDPPeer{}
	times := []time.Time{}
	for rows.Next() {
		var s multicastMSDPPeerScan
		if err := rows.Scan(
			&s.Peer.EntityID,
			&s.snapshotTS,
			&s.Peer.DevicePK,
			&s.Peer.DeviceCode,
			&s.Peer.PeerAddress,
			&s.Peer.PeerDevicePK,
			&s.Peer.PeerDeviceCode,
			&s.Peer.PeerInterfaceName,
			&s.Peer.State,
			&s.sessionStartTime,
			&s.Peer.SACount,
			&s.Peer.ResetCount,
		); err != nil {
			return nil, nil, err
		}
		s.Peer.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.Peer.SessionStartTime = formatMulticastTime(s.sessionStartTime)
		s.Peer.AgeSeconds = ageSeconds(s.snapshotTS)
		s.Peer.FreshnessStatus = multicastFreshnessStatus(s.Peer.AgeSeconds)
		peers = append(peers, s.Peer)
		times = append(times, s.snapshotTS)
	}
	return peers, times, rows.Err()
}

func (a *API) queryMulticastDeviceDeliveryMSDPSAs(ctx context.Context, viewName, entityCol string, device MulticastDeliveryDevice, params multicastDeliveryEntityParams, includeRemote bool) ([]MulticastDeliveryMSDPSA, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	groupFilter, groupArgs := sqlMulticastGroupFilter(params.Groups)
	remoteCols := "'' AS remote_address, '' AS remote_device_pk, '' AS remote_device_code, '' AS remote_interface_name, '' AS accept_status,"
	remoteFilter := ""
	remoteArg := []any{}
	if includeRemote {
		remoteCols = "remote_address, remote_device_pk, remote_device_code, remote_interface_name, accept_status,"
		remoteFilter = " OR remote_device_pk = ?"
		remoteArg = append(remoteArg, device.PK)
	}
	query := `
		SELECT
			` + entityCol + ` AS entity_id,
			"sa.snapshot_ts" AS snapshot_ts,
			device_pk,
			device_code,
			group_address,
			source_address,
			publisher_user_pk,
			publisher_device_pk,
			` + remoteCols + `
			rp_address,
			source_match_status
		FROM ` + viewName + `
		WHERE (device_pk = ? OR publisher_device_pk = ?` + remoteFilter + `)` + sourceFilter + groupFilter + `
		ORDER BY group_address, source_address, device_code
		LIMIT 5000
		` + multicastDeliveryQuerySettings(ctx) + `
	`
	args := []any{device.PK, device.PK}
	args = append(args, remoteArg...)
	args = append(args, sourceArgs...)
	args = append(args, groupArgs...)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_device_delivery_msdp_sa", time.Since(start), err)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	sas := []MulticastDeliveryMSDPSA{}
	times := []time.Time{}
	for rows.Next() {
		var s multicastMSDPSAScan
		if err := rows.Scan(
			&s.SA.EntityID,
			&s.snapshotTS,
			&s.SA.DevicePK,
			&s.SA.DeviceCode,
			&s.SA.GroupAddress,
			&s.SA.SourceAddress,
			&s.SA.PublisherUserPK,
			&s.SA.PublisherDevicePK,
			&s.SA.RemoteAddress,
			&s.SA.RemoteDevicePK,
			&s.SA.RemoteDeviceCode,
			&s.SA.RemoteInterfaceName,
			&s.SA.AcceptStatus,
			&s.SA.RPAddress,
			&s.SA.SourceMatchStatus,
		); err != nil {
			return nil, nil, err
		}
		s.SA.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.SA.AgeSeconds = ageSeconds(s.snapshotTS)
		s.SA.FreshnessStatus = multicastFreshnessStatus(s.SA.AgeSeconds)
		sas = append(sas, s.SA)
		times = append(times, s.snapshotTS)
	}
	return sas, times, rows.Err()
}

func sqlMulticastGroupFilter(values []string) (string, []any) {
	if len(values) == 0 {
		return "", nil
	}
	return " AND (multicast_group_pk IN (?) OR multicast_group_code IN (?) OR group_address IN (?))", []any{values, values, values}
}
