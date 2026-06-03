package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// queryMulticastDeliveryMSDPPeers reads pre-enriched MSDP peer sessions from
// enriched_ip_msdp_peers. The view resolves peer_address back to a peer device
// through dz_device_interface_ips when the peer address is present in device
// interface metadata.
func (a *API) queryMulticastDeliveryMSDPPeers(ctx context.Context, params MulticastDeliveryParams, mroutes []MulticastDeliveryMroute) ([]MulticastDeliveryMSDPPeer, []time.Time, error) {
	devicePKs := uniqueMrouteDevices(mroutes)
	if len(devicePKs) == 0 {
		return []MulticastDeliveryMSDPPeer{}, nil, nil
	}
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
		WHERE device_pk IN (?)
		ORDER BY device_pk, peer_address
		SETTINGS max_execution_time = 30,
			timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, devicePKs)
	metrics.RecordClickHouseQuery("multicast_delivery_msdp_peers", time.Since(start), err)
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
		if csvContainsOrEmpty(params.Devices, s.Peer.DevicePK) || csvContainsOrEmpty(params.Devices, s.Peer.DeviceCode) {
			peers = append(peers, s.Peer)
			times = append(times, s.snapshotTS)
		}
	}
	return peers, times, rows.Err()
}

func (a *API) queryMulticastDeliveryPimSACache(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryMSDPSA, []time.Time, error) {
	return a.queryEnrichedMSDPSA(ctx, "enriched_ip_msdp_pim_sa_cache", "msdp_pim_sa_entity_id", group, params, false)
}

func (a *API) queryMulticastDeliverySACache(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryMSDPSA, []time.Time, error) {
	return a.queryEnrichedMSDPSA(ctx, "enriched_ip_msdp_sa_cache", "msdp_sa_entity_id", group, params, true)
}

// queryEnrichedMSDPSA reads from one of the two MSDP SA-cache enriched views.
// The PIM SA-cache view does not carry remote_address or accept_status; the
// includeRemote flag swaps those SELECT columns out for empty literals to keep
// the scan shape shared.
func (a *API) queryEnrichedMSDPSA(ctx context.Context, viewName, entityCol string, group MulticastDeliveryGroup, params MulticastDeliveryParams, includeRemote bool) ([]MulticastDeliveryMSDPSA, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
	remoteCols := "'' AS remote_address, '' AS remote_device_pk, '' AS remote_device_code, '' AS remote_interface_name, '' AS accept_status,"
	if includeRemote {
		remoteCols = "remote_address, remote_device_pk, remote_device_code, remote_interface_name, accept_status,"
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
		WHERE multicast_group_pk = ?` + sourceFilter + `
		ORDER BY source_address, device_pk
		SETTINGS max_execution_time = 30,
			timeout_before_checking_execution_speed = 0
	`
	args := []any{group.PK}
	args = append(args, sourceArgs...)
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_delivery_msdp_sa", time.Since(start), err)
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
		if multicastDeliverySAMatches(s.SA, params) {
			sas = append(sas, s.SA)
			times = append(times, s.snapshotTS)
		}
	}
	return sas, times, rows.Err()
}
