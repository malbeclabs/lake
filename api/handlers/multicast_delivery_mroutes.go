package handlers

import (
	"context"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// queryMulticastDeliveryMroutes reads pre-enriched mroutes from the
// enriched_ip_mroute view. All device/metro/contributor/group/publisher joins
// live in the view definition.
func (a *API) queryMulticastDeliveryMroutes(ctx context.Context, group MulticastDeliveryGroup, params MulticastDeliveryParams) ([]MulticastDeliveryMroute, []time.Time, error) {
	sourceFilter, sourceArgs := sqlInFilter("source_address", params.Sources)
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
			mroute_id
		FROM enriched_ip_mroute
		WHERE multicast_group_pk = ?` + sourceFilter + `
		ORDER BY source_address, device_pk, vrf, mode
		SETTINGS max_execution_time = 30,
			timeout_before_checking_execution_speed = 0
	`
	args := []any{group.PK}
	args = append(args, sourceArgs...)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, args...)
	metrics.RecordClickHouseQuery("multicast_delivery_mroutes", time.Since(start), err)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	mroutes := []MulticastDeliveryMroute{}
	times := []time.Time{}
	for rows.Next() {
		var s multicastMrouteScan
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
		); err != nil {
			return nil, nil, err
		}
		s.Mroute.SnapshotTS = formatMulticastTime(s.snapshotTS)
		s.Mroute.IngestedAt = formatMulticastTime(s.ingestedAt)
		s.Mroute.CreationTime = formatMulticastTime(s.creationTime)
		s.Mroute.AgeSeconds = ageSeconds(s.snapshotTS)
		s.Mroute.FreshnessStatus = multicastFreshnessStatus(s.Mroute.AgeSeconds)
		s.Mroute.RegisterInOIFList = s.registerInOIFList != 0
		s.Mroute.RPFAttached = s.rpfAttached != 0
		s.Mroute.RPFHasBlock = s.rpfHasBlock != 0
		if multicastDeliveryMrouteMatches(s.Mroute, params) {
			mroutes = append(mroutes, s.Mroute)
			times = append(times, s.snapshotTS)
		}
	}
	return mroutes, times, rows.Err()
}
