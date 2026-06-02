package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryMulticastDeliveryMSDPPeers(ctx context.Context, params MulticastDeliveryParams, mroutes []MulticastDeliveryMroute) ([]MulticastDeliveryMSDPPeer, []time.Time, error) {
	devicePKs := uniqueMrouteDevices(mroutes)
	if len(devicePKs) == 0 {
		return []MulticastDeliveryMSDPPeer{}, nil, nil
	}
	query := `
		SELECT
			p.entity_id,
			p.snapshot_ts,
			p.device_pubkey,
			d.code AS device_code,
			p.peer_address,
			p.state,
			p.session_start_time,
			p.sa_count,
			p.reset_count
		FROM dz_ip_msdp_peers_current p
		LEFT ANY JOIN dz_devices_current d ON p.device_pubkey = d.pk
		WHERE p.device_pubkey IN (?)
		ORDER BY p.device_pubkey, p.peer_address
		LIMIT 1000
		SETTINGS max_execution_time = 10,
			max_result_rows = 1000,
			result_overflow_mode = 'break',
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
	return a.queryMulticastDeliverySA(ctx, "dz_ip_msdp_pim_sa_cache_current", group, params, false)
}

func (a *API) queryMulticastDeliverySACache(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryMSDPSA, []time.Time, error) {
	return a.queryMulticastDeliverySA(ctx, "dz_ip_msdp_sa_cache_current", group, params, true)
}

func (a *API) queryMulticastDeliverySA(ctx context.Context, table string, group MulticastDeliveryGroup, params MulticastDeliveryParams, includeStatus bool) ([]MulticastDeliveryMSDPSA, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("sa.source_address", params.Sources)
	selectExtra := "'' AS remote_address, '' AS status,"
	if includeStatus {
		selectExtra = "sa.remote_address AS remote_address, sa.status AS status,"
	}
	query := `
		WITH sa_filtered AS (
			SELECT *
			FROM ` + table + ` sa
			WHERE sa.group_address = ?` + sourceFilter + `
		)
		SELECT
			sa.entity_id,
			sa.snapshot_ts,
			sa.device_pubkey,
			d.code AS device_code,
			sa.group_address,
			sa.source_address,
			pub.pk AS publisher_user_pk,
			pub.device_pk AS publisher_device_pk,
			` + selectExtra + `
			sa.rp_address,
			CASE
				WHEN pub.pk != '' THEN 'publisher_matched'
				WHEN sa.source_address = '' OR sa.source_address = '*' THEN 'group_only'
				ELSE 'unknown_source'
			END AS source_match_status
		FROM sa_filtered sa
		LEFT ANY JOIN dz_devices_current d ON sa.device_pubkey = d.pk
		LEFT ANY JOIN (
			SELECT pk, dz_ip, device_pk
			FROM dz_users_current
			WHERE status = 'activated'
				AND kind = 'multicast'
				AND has(JSONExtract(publishers, 'Array(String)'), ?)
		) pub ON sa.source_address = pub.dz_ip
		ORDER BY sa.source_address, sa.device_pubkey
		LIMIT 5000
		SETTINGS max_execution_time = 10,
			max_result_rows = 5000,
			result_overflow_mode = 'break',
			timeout_before_checking_execution_speed = 0
	`
	args := []any{group.MulticastIP}
	args = append(args, sourceArgs...)
	args = append(args, group.PK)
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
			&s.SA.Status,
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
