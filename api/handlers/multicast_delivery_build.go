package handlers

import (
	"context"
	"strings"
	"time"
)

func (a *API) buildExpectedMulticastDeliverySegments(ctx context.Context, groupPK string, params MulticastDeliveryParams) ([]MulticastDeliverySegment, error) {
	members, err := a.queryMulticastDeliveryMembers(ctx, groupPK)
	if err != nil {
		return nil, err
	}
	publishers, subscribers := splitMulticastDeliveryMembers(members, params)
	if len(publishers) == 0 || len(subscribers) == 0 {
		return []MulticastDeliverySegment{}, nil
	}
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		return []MulticastDeliverySegment{}, err
	}
	segmentMap := map[string]*MulticastDeliverySegment{}
	for _, pub := range publishers {
		if pub.DevicePK == "" {
			continue
		}
		for _, sub := range subscribers {
			if sub.DevicePK == "" || pub.DevicePK == sub.DevicePK {
				continue
			}
			path := dijkstra(g, pub.DevicePK, sub.DevicePK, nil, nil)
			if path == nil {
				continue
			}
			for i := 0; i < len(path.Nodes)-1; i++ {
				fromPK := path.Nodes[i]
				toPK := path.Nodes[i+1]
				key := fromPK + "|" + toPK
				seg := segmentMap[key]
				if seg == nil {
					seg = &MulticastDeliverySegment{
						Source:              "expected",
						Status:              "expected",
						FromDevicePK:        fromPK,
						FromDeviceCode:      g.Nodes[fromPK].Code,
						ToDevicePK:          toPK,
						ToDeviceCode:        g.Nodes[toPK].Code,
						PublisherUserPKs:    []string{},
						PublisherDevicePKs:  []string{},
						SubscriberUserPKs:   []string{},
						SubscriberDevicePKs: []string{},
					}
					segmentMap[key] = seg
				}
				appendUniqueString(&seg.PublisherUserPKs, pub.UserPK)
				appendUniqueString(&seg.PublisherDevicePKs, pub.DevicePK)
				appendUniqueString(&seg.SubscriberUserPKs, sub.UserPK)
				appendUniqueString(&seg.SubscriberDevicePKs, sub.DevicePK)
			}
		}
	}
	segments := make([]MulticastDeliverySegment, 0, len(segmentMap))
	for _, seg := range segmentMap {
		segments = append(segments, *seg)
	}
	return segments, nil
}

func splitMulticastDeliveryMembers(members []MulticastDeliveryMember, params MulticastDeliveryParams) ([]MulticastDeliveryMember, []MulticastDeliveryMember) {
	publishers := []MulticastDeliveryMember{}
	subscribers := []MulticastDeliveryMember{}
	for _, m := range members {
		if (m.Mode == "P" || m.Mode == "P+S") && multicastDeliveryMemberMatchesPublisher(m, params) {
			publishers = append(publishers, m)
		}
		if (m.Mode == "S" || m.Mode == "P+S") && multicastDeliveryMemberMatchesDevice(m.DevicePK, m.DeviceCode, params) {
			subscribers = append(subscribers, m)
		}
	}
	return publishers, subscribers
}

func buildMulticastFreshness(now time.Time, available map[string]bool, sourceTimes map[string]time.Time, mrouteTimes, msdpPeerTimes, pimSATimes, saTimes []time.Time) MulticastDeliveryFreshness {
	return MulticastDeliveryFreshness{
		Mroute:         freshnessForTimes(now, available["dz_ip_mroute_entries_current"], sourceTimes["dz_ip_mroute_entries_current"], mrouteTimes, ""),
		MSDPPeers:      freshnessForTimes(now, available["dz_ip_msdp_peers_current"], sourceTimes["dz_ip_msdp_peers_current"], msdpPeerTimes, ""),
		MSDPPimSACache: freshnessForTimes(now, available["dz_ip_msdp_pim_sa_cache_current"], sourceTimes["dz_ip_msdp_pim_sa_cache_current"], pimSATimes, ""),
		MSDPSACache:    freshnessForTimes(now, available["dz_ip_msdp_sa_cache_current"], sourceTimes["dz_ip_msdp_sa_cache_current"], saTimes, ""),
		PIMNeighbors:   freshnessForTimes(now, false, time.Time{}, nil, "PIM neighbor adjacency source is not implemented; MSDP PIM SA cache is exposed separately"),
	}
}

func freshnessForTimes(now time.Time, available bool, sourceTime time.Time, times []time.Time, note string) MulticastDeliverySourceFreshness {
	f := MulticastDeliverySourceFreshness{
		Available: available,
		Status:    "missing",
		RowCount:  len(times),
		Note:      note,
	}
	if !available {
		return f
	}
	if len(times) > 0 {
		latest := times[0]
		for _, ts := range times[1:] {
			if ts.After(latest) {
				latest = ts
			}
		}
		latestStr := formatMulticastTime(latest)
		f.LatestSnapshotTS = &latestStr
	}

	freshnessTime := sourceTime
	if !freshnessTime.IsZero() {
		latestIngested := formatMulticastTime(freshnessTime)
		f.LatestIngestedAt = &latestIngested
	} else if len(times) > 0 {
		freshnessTime = times[0]
		for _, ts := range times[1:] {
			if ts.After(freshnessTime) {
				freshnessTime = ts
			}
		}
	} else {
		return f
	}

	age := int(now.Sub(freshnessTime).Seconds())
	if age < 0 {
		age = 0
	}
	f.AgeSeconds = &age
	f.Status = multicastFreshnessStatus(age)
	return f
}

func applyMulticastFreshnessToRows(resp *MulticastDeliveryStateResponse) {
	applyRouteFreshness(resp.Freshness.Mroute, resp.Routes)
	applyOIFFreshness(resp.Freshness.Mroute, resp.OIFs)
	applyMSDPPeerFreshness(resp.Freshness.MSDPPeers, resp.MSDP.Peers)
	applyMSDPSAFreshness(resp.Freshness.MSDPPimSACache, resp.MSDP.PimSACache)
	applyMSDPSAFreshness(resp.Freshness.MSDPSACache, resp.MSDP.SACache)
}

func applyRouteFreshness(f MulticastDeliverySourceFreshness, rows []MulticastDeliveryRoute) {
	if f.AgeSeconds == nil {
		return
	}
	for i := range rows {
		rows[i].AgeSeconds = *f.AgeSeconds
		rows[i].FreshnessStatus = f.Status
	}
}

func applyOIFFreshness(f MulticastDeliverySourceFreshness, rows []MulticastDeliveryOIF) {
	if f.AgeSeconds == nil {
		return
	}
	for i := range rows {
		rows[i].AgeSeconds = *f.AgeSeconds
		rows[i].FreshnessStatus = f.Status
	}
}

func applyMSDPPeerFreshness(f MulticastDeliverySourceFreshness, rows []MulticastDeliveryMSDPPeer) {
	if f.AgeSeconds == nil {
		return
	}
	for i := range rows {
		rows[i].AgeSeconds = *f.AgeSeconds
		rows[i].FreshnessStatus = f.Status
	}
}

func applyMSDPSAFreshness(f MulticastDeliverySourceFreshness, rows []MulticastDeliveryMSDPSA) {
	if f.AgeSeconds == nil {
		return
	}
	for i := range rows {
		rows[i].AgeSeconds = *f.AgeSeconds
		rows[i].FreshnessStatus = f.Status
	}
}

func buildObservedMulticastSegments(oifs []MulticastDeliveryOIF) []MulticastDeliverySegment {
	segmentMap := map[string]*MulticastDeliverySegment{}
	for _, oif := range oifs {
		toPK := oif.PeerDevicePK
		toCode := oif.PeerDeviceCode
		if oif.OIFKind == "subscriber_tunnel" {
			toPK = oif.SubscriberDevicePK
			toCode = oif.SubscriberDeviceCode
		}
		if oif.DevicePK == "" || toPK == "" {
			continue
		}
		key := oif.DevicePK + "|" + toPK
		seg := segmentMap[key]
		if seg == nil {
			seg = &MulticastDeliverySegment{
				Source:              "observed",
				Status:              "observed",
				FromDevicePK:        oif.DevicePK,
				FromDeviceCode:      oif.DeviceCode,
				ToDevicePK:          toPK,
				ToDeviceCode:        toCode,
				LinkPK:              oif.LinkPK,
				LinkCode:            oif.LinkCode,
				PublisherUserPKs:    []string{},
				PublisherDevicePKs:  []string{},
				SubscriberUserPKs:   []string{},
				SubscriberDevicePKs: []string{},
				OIFNames:            []string{},
			}
			segmentMap[key] = seg
		}
		appendUniqueString(&seg.PublisherUserPKs, oif.PublisherUserPK)
		appendUniqueString(&seg.PublisherDevicePKs, oif.PublisherDevicePK)
		appendUniqueString(&seg.OIFNames, oif.OIFName)
		if oif.SubscriberUserPK != "" {
			appendUniqueString(&seg.SubscriberUserPKs, oif.SubscriberUserPK)
			appendUniqueString(&seg.SubscriberDevicePKs, oif.SubscriberDevicePK)
		}
	}
	segments := make([]MulticastDeliverySegment, 0, len(segmentMap))
	for _, seg := range segmentMap {
		segments = append(segments, *seg)
	}
	return segments
}

func buildMulticastSubscriberOutcomes(subscribers []MulticastDeliveryMember, observed, expected []MulticastDeliverySegment) []MulticastDeliverySubscriberState {
	observedDevices := map[string]bool{}
	for _, seg := range observed {
		for _, pk := range seg.SubscriberDevicePKs {
			observedDevices[pk] = true
		}
		if len(seg.SubscriberDevicePKs) == 0 && seg.ToDevicePK != "" {
			observedDevices[seg.ToDevicePK] = true
		}
	}
	expectedDevices := map[string]bool{}
	for _, seg := range expected {
		for _, pk := range seg.SubscriberDevicePKs {
			expectedDevices[pk] = true
		}
	}
	outcomes := []MulticastDeliverySubscriberState{}
	for _, sub := range subscribers {
		state := MulticastDeliverySubscriberState{
			SubscriberUserPK:     sub.UserPK,
			SubscriberDevicePK:   sub.DevicePK,
			SubscriberDeviceCode: sub.DeviceCode,
			Status:               "not_enough_data",
			Reason:               "no observed or expected branch was available for this subscriber",
		}
		if observedDevices[sub.DevicePK] {
			state.Status = "observed"
			state.Reason = "subscriber tunnel or downstream branch was observed"
		} else if expectedDevices[sub.DevicePK] {
			state.Status = "expected_but_missing"
			state.Reason = "expected branch exists but no observed branch reached the subscriber device"
		}
		outcomes = append(outcomes, state)
	}
	return outcomes
}

func buildMulticastDeliveryAnomalies(group MulticastDeliveryGroup, routes []MulticastDeliveryRoute, oifs []MulticastDeliveryOIF, freshness MulticastDeliveryFreshness, outcomes []MulticastDeliverySubscriberState) []MulticastDeliveryAnomaly {
	anomalies := []MulticastDeliveryAnomaly{}
	if len(routes) == 0 {
		anomalies = append(anomalies, multicastAnomaly(
			"no-mroute-state", "info", "no_mroute_state", "group",
			map[string]string{"group_pk": group.PK},
			"no current mroute rows were observed for this multicast group",
		))
	}
	if freshness.Mroute.Status == "stale" {
		anomalies = append(anomalies, multicastAnomaly(
			"stale-mroute-state", "warning", "stale_state", "group",
			map[string]string{"group_pk": group.PK},
			"latest mroute snapshot is stale",
		))
	}
	for _, route := range routes {
		if route.SourceMatchStatus == "unknown_source" {
			anomalies = append(anomalies, multicastAnomaly(
				"unknown-source-"+route.RouteID, "warning", "unknown_source", "route",
				map[string]string{"group_pk": group.PK, "route_id": route.RouteID, "source_address": route.SourceAddress},
				"mroute source did not match an activated multicast publisher for this group",
			))
		}
	}
	for _, oif := range oifs {
		if oif.OIFKind == "unknown" {
			anomalies = append(anomalies, multicastAnomaly(
				"unknown-oif-"+oif.RouteID+"-"+oif.OIFName, "warning", "unknown_oif", "route",
				map[string]string{"group_pk": group.PK, "route_id": oif.RouteID, "oif_name": oif.OIFName},
				"mroute OIF could not be resolved to a link, interface, subscriber tunnel, or special interface",
			))
		}
	}
	for _, outcome := range outcomes {
		if outcome.Status == "expected_but_missing" {
			anomalies = append(anomalies, multicastAnomaly(
				"expected-subscriber-missing-"+outcome.SubscriberUserPK, "warning", "expected_subscriber_missing", "subscriber",
				map[string]string{"group_pk": group.PK, "subscriber_user_pk": outcome.SubscriberUserPK, "subscriber_device_pk": outcome.SubscriberDevicePK},
				"expected topology has a branch for this subscriber, but observed forwarding did not reach it",
			))
		}
	}
	return anomalies
}

func classifyMulticastOIF(oif MulticastDeliveryOIF) (string, string) {
	name := strings.ToLower(oif.OIFName)
	if oif.LinkPK != "" {
		return "underlay_link", "toward_network"
	}
	if oif.SubscriberUserPK != "" {
		return "subscriber_tunnel", "toward_subscriber"
	}
	if oif.InterfaceType != "" || oif.RoutingMode != "" {
		return "local_interface", "unclassified"
	}
	if strings.HasPrefix(name, "register") {
		return "register", "local_register"
	}
	if strings.HasPrefix(name, "null") {
		return "null", "unclassified"
	}
	return "unknown", "unclassified"
}

func multicastFreshnessStatus(age int) string {
	if age <= multicastDeliveryFreshSeconds {
		return "fresh"
	}
	return "stale"
}

func ageSeconds(ts time.Time) int {
	age := int(time.Since(ts).Seconds())
	if age < 0 {
		return 0
	}
	return age
}

func formatMulticastTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

func multicastRouteID(devicePK, vrf, mode, groupAddress, sourceAddress string) string {
	return strings.Join([]string{devicePK, vrf, mode, groupAddress, sourceAddress}, "|")
}

func multicastAnomaly(id, severity, kind, scope string, objectIDs map[string]string, message string) MulticastDeliveryAnomaly {
	return MulticastDeliveryAnomaly{
		ID:        id,
		Severity:  severity,
		Kind:      kind,
		Scope:     scope,
		ObjectIDs: objectIDs,
		Message:   message,
	}
}

func multicastDeliveryRouteMatches(route MulticastDeliveryRoute, params MulticastDeliveryParams) bool {
	if !csvContainsOrEmpty(params.Publishers, route.PublisherUserPK) && !csvContainsOrEmpty(params.Publishers, route.PublisherDevicePK) {
		return false
	}
	return multicastDeliveryMemberMatchesDevice(route.DevicePK, route.DeviceCode, params)
}

func multicastDeliveryOIFMatches(oif MulticastDeliveryOIF, params MulticastDeliveryParams) bool {
	if !csvContainsOrEmpty(params.Publishers, oif.PublisherUserPK) && !csvContainsOrEmpty(params.Publishers, oif.PublisherDevicePK) {
		return false
	}
	if !multicastDeliveryMemberMatchesDevice(oif.DevicePK, oif.DeviceCode, params) {
		return false
	}
	if !csvContainsOrEmpty(params.Links, oif.LinkPK) && !csvContainsOrEmpty(params.Links, oif.LinkCode) {
		return false
	}
	return true
}

func multicastDeliverySAMatches(sa MulticastDeliveryMSDPSA, params MulticastDeliveryParams) bool {
	if !csvContainsOrEmpty(params.Publishers, sa.PublisherUserPK) && !csvContainsOrEmpty(params.Publishers, sa.PublisherDevicePK) {
		return false
	}
	return csvContainsOrEmpty(params.Devices, sa.DevicePK) || csvContainsOrEmpty(params.Devices, sa.DeviceCode)
}

func multicastDeliveryMemberMatchesPublisher(member MulticastDeliveryMember, params MulticastDeliveryParams) bool {
	if !csvContainsOrEmpty(params.Publishers, member.UserPK) && !csvContainsOrEmpty(params.Publishers, member.DevicePK) {
		return false
	}
	if !csvContainsOrEmpty(params.Sources, member.DZIP) {
		return false
	}
	return multicastDeliveryMemberMatchesDevice(member.DevicePK, member.DeviceCode, params)
}

func multicastDeliveryMemberMatchesDevice(devicePK, deviceCode string, params MulticastDeliveryParams) bool {
	return csvContainsOrEmpty(params.Devices, devicePK) || csvContainsOrEmpty(params.Devices, deviceCode)
}

func uniqueRouteDevices(routes []MulticastDeliveryRoute) []string {
	seen := map[string]bool{}
	devices := []string{}
	for _, route := range routes {
		if route.DevicePK == "" || seen[route.DevicePK] {
			continue
		}
		seen[route.DevicePK] = true
		devices = append(devices, route.DevicePK)
	}
	return devices
}

func sqlInFilter(column string, values []string) (string, []any) {
	if len(values) == 0 {
		return "", nil
	}
	return " AND " + column + " IN (?)", []any{values}
}

func splitCSVParam(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || seen[part] {
			continue
		}
		seen[part] = true
		out = append(out, part)
	}
	return out
}

func csvSet(value string) map[string]bool {
	set := map[string]bool{}
	for _, part := range splitCSVParam(value) {
		set[part] = true
	}
	return set
}

func csvContainsOrEmpty(values []string, candidate string) bool {
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func appendUniqueString(values *[]string, value string) {
	if value == "" {
		return
	}
	for _, existing := range *values {
		if existing == value {
			return
		}
	}
	*values = append(*values, value)
}

func multicastDeliverySourceErr(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "unknown table") ||
		strings.Contains(errStr, "table not found") ||
		strings.Contains(errStr, "doesn't exist") ||
		strings.Contains(errStr, "does not exist")
}
