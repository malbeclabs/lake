package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

func (a *API) queryMulticastDeliveryRoutes(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryRoute, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("r.source_address", params.Sources)
	query := `
		WITH routes_filtered AS (
			SELECT *
			FROM dz_ip_mroute_entries_current r
			WHERE r.group_address = ?` + sourceFilter + `
		)
		SELECT
			r.entity_id,
			r.snapshot_ts,
			r.ingested_at,
			r.device_pubkey,
			d.code AS device_code,
			d.status AS device_status,
			d.device_type AS device_type,
			m.code AS metro_code,
			c.code AS contributor_code,
			r.vrf,
			r.mode,
			r.group_address,
			r.source_address,
			r.route_flags,
			r.register_in_oif_list,
			r.rpf_interface,
			r.rpf_rib,
			r.rpf_prefix,
			r.rpf_preference,
			r.rpf_metric,
			r.rpf_neighbor,
			r.rpf_attached,
			r.rpf_has_block,
			r.oif_list,
			r.oif_count,
			r.creation_time,
			pub.pk AS publisher_user_pk,
			pub.device_pk AS publisher_device_pk,
			pub_dev.code AS publisher_device_code,
			pub_m.code AS publisher_metro_code,
			pub_c.code AS publisher_contributor_code,
			pub.tunnel_id AS publisher_tunnel_id,
			pub.owner_pubkey AS publisher_owner_pubkey,
			pub.dz_ip AS publisher_dz_ip,
			CASE
				WHEN pub.pk != '' THEN 'publisher_matched'
				WHEN r.source_address = '' OR r.source_address = '*' THEN 'group_only'
				ELSE 'unknown_source'
			END AS source_match_status
		FROM routes_filtered r
		LEFT ANY JOIN dz_devices_current d ON r.device_pubkey = d.pk
		LEFT ANY JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT ANY JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		LEFT ANY JOIN (
			SELECT pk, owner_pubkey, dz_ip, device_pk, tunnel_id
			FROM dz_users_current
			WHERE status = 'activated'
				AND kind = 'multicast'
				AND has(JSONExtract(publishers, 'Array(String)'), ?)
		) pub ON r.source_address = pub.dz_ip
		LEFT ANY JOIN dz_devices_current pub_dev ON pub.device_pk = pub_dev.pk
		LEFT ANY JOIN dz_metros_current pub_m ON pub_dev.metro_pk = pub_m.pk
		LEFT ANY JOIN dz_contributors_current pub_c ON pub_dev.contributor_pk = pub_c.pk
		ORDER BY r.source_address, r.device_pubkey, r.vrf, r.mode
		LIMIT ?
		SETTINGS max_execution_time = 15,
			max_result_rows = 10000,
			result_overflow_mode = 'break',
			timeout_before_checking_execution_speed = 0
	`
	args := []any{group.MulticastIP}
	args = append(args, sourceArgs...)
	args = append(args, group.PK, multicastDeliveryMaxRouteRows)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_delivery_routes", time.Since(start), err)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	routes := []MulticastDeliveryRoute{}
	times := []time.Time{}
	for rows.Next() {
		var s multicastRouteScan
		if err := rows.Scan(
			&s.Route.EntityID,
			&s.snapshotTS,
			&s.ingestedAt,
			&s.Route.DevicePK,
			&s.Route.DeviceCode,
			&s.Route.DeviceStatus,
			&s.Route.DeviceType,
			&s.Route.MetroCode,
			&s.Route.ContributorCode,
			&s.Route.VRF,
			&s.Route.Mode,
			&s.Route.GroupAddress,
			&s.Route.SourceAddress,
			&s.Route.RouteFlags,
			&s.registerInOIFList,
			&s.Route.RPFInterface,
			&s.Route.RPFRIB,
			&s.Route.RPFPrefix,
			&s.Route.RPFPreference,
			&s.Route.RPFMetric,
			&s.Route.RPFNeighbor,
			&s.rpfAttached,
			&s.rpfHasBlock,
			&s.Route.OIFList,
			&s.Route.OIFCount,
			&s.creationTime,
			&s.Route.PublisherUserPK,
			&s.Route.PublisherDevicePK,
			&s.Route.PublisherDeviceCode,
			&s.Route.PublisherMetroCode,
			&s.Route.PublisherContributorCode,
			&s.Route.PublisherTunnelID,
			&s.Route.PublisherOwnerPubkey,
			&s.Route.PublisherDZIP,
			&s.Route.SourceMatchStatus,
		); err != nil {
			return nil, nil, err
		}
		s.Route.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.Route.IngestedAt = formatMulticastTime(s.ingestedAt)
		s.Route.CreationTime = formatMulticastTime(s.creationTime)
		s.Route.AgeSeconds = ageSeconds(s.snapshotTS)
		s.Route.FreshnessStatus = multicastFreshnessStatus(s.Route.AgeSeconds)
		s.Route.RegisterInOIFList = s.registerInOIFList != 0
		s.Route.RPFAttached = s.rpfAttached != 0
		s.Route.RPFHasBlock = s.rpfHasBlock != 0
		s.Route.RouteID = multicastRouteID(s.Route.DevicePK, s.Route.VRF, s.Route.Mode, s.Route.GroupAddress, s.Route.SourceAddress)
		if multicastDeliveryRouteMatches(s.Route, params) {
			routes = append(routes, s.Route)
			times = append(times, s.snapshotTS)
		}
	}
	return routes, times, rows.Err()
}
