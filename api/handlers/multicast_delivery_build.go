package handlers

import (
	"context"
	"strconv"
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
	g, err := a.loadTopologyGraph(ctx, "")
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
		if (m.Mode == "S" || m.Mode == "P+S") && multicastDeliveryMemberMatchesSubscriber(m, params) {
			subscribers = append(subscribers, m)
		}
	}
	return publishers, subscribers
}

func buildMulticastFreshness(now time.Time, available map[string]bool, sourceTimes map[string]time.Time, mrouteTimes, msdpPeerTimes, pimSATimes, saTimes []time.Time) MulticastDeliveryFreshness {
	return MulticastDeliveryFreshness{
		Mroute:         freshnessForTimes(now, available[multicastDeliveryMrouteView], sourceTimes[multicastDeliveryMrouteSource], mrouteTimes, ""),
		MSDPPeers:      freshnessForTimes(now, available[multicastDeliveryMSDPPeersView], sourceTimes[multicastDeliveryMSDPPeersSource], msdpPeerTimes, ""),
		MSDPPimSACache: freshnessForTimes(now, available[multicastDeliveryMSDPPimSAView], sourceTimes[multicastDeliveryMSDPPimSASource], pimSATimes, ""),
		MSDPSACache:    freshnessForTimes(now, available[multicastDeliveryMSDPSAView], sourceTimes[multicastDeliveryMSDPSASource], saTimes, ""),
		PIMNeighbors:   freshnessForTimes(now, false, time.Time{}, nil, "PIM neighbor adjacency source is not implemented; MSDP PIM SA cache is exposed separately"),
	}
}

func copyMulticastDeliveryAvailability(available map[string]bool) map[string]bool {
	out := make(map[string]bool, len(available))
	for key, value := range available {
		out[key] = value
	}
	return out
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

func applyMrouteFreshness(f MulticastDeliverySourceFreshness, rows []MulticastDeliveryMroute) {
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

func buildMulticastDeliveryAnomalies(group MulticastDeliveryGroup, mroutes []MulticastDeliveryMroute, oifs []MulticastDeliveryOIF, freshness MulticastDeliveryFreshness, outcomes []MulticastDeliverySubscriberState) []MulticastDeliveryAnomaly {
	anomalies := []MulticastDeliveryAnomaly{}
	if len(mroutes) == 0 {
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
	for _, mroute := range mroutes {
		if mroute.SourceMatchStatus == "unknown_source" {
			anomalies = append(anomalies, multicastAnomaly(
				"unknown-source-"+mroute.MrouteID, "warning", "unknown_source", "mroute",
				map[string]string{"group_pk": group.PK, "mroute_id": mroute.MrouteID, "source_address": mroute.SourceAddress},
				"mroute source did not match an activated multicast publisher for this group",
			))
		}
	}
	for _, oif := range oifs {
		if oif.OIFKind == "unknown" {
			anomalies = append(anomalies, multicastAnomaly(
				"unknown-oif-"+oif.MrouteID+"-"+oif.OIFName, "warning", "unknown_oif", "mroute",
				map[string]string{"group_pk": group.PK, "mroute_id": oif.MrouteID, "oif_name": oif.OIFName},
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

func multicastDeliveryMrouteMatches(mroute MulticastDeliveryMroute, params MulticastDeliveryParams) bool {
	if !csvContainsOrEmpty(params.Publishers, mroute.PublisherUserPK) && !csvContainsOrEmpty(params.Publishers, mroute.PublisherDevicePK) {
		return false
	}
	return multicastDeliveryMemberMatchesDevice(mroute.DevicePK, mroute.DeviceCode, params)
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
	if !csvContainsOrEmpty(params.OIFKinds, oif.OIFKind) {
		return false
	}
	if !multicastDeliveryOIFMatchesSubscriber(oif, params) {
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

func multicastDeliveryMemberMatchesSubscriber(member MulticastDeliveryMember, params MulticastDeliveryParams) bool {
	if !csvContainsOrEmpty(params.Subscribers, member.UserPK) &&
		!csvContainsOrEmpty(params.Subscribers, member.DevicePK) &&
		!csvContainsOrEmpty(params.Subscribers, member.DeviceCode) &&
		!csvContainsOrEmpty(params.Subscribers, strconv.Itoa(int(member.TunnelID))) {
		return false
	}
	return multicastDeliveryMemberMatchesDevice(member.DevicePK, member.DeviceCode, params)
}

func multicastDeliveryMemberMatchesDevice(devicePK, deviceCode string, params MulticastDeliveryParams) bool {
	return csvContainsOrEmpty(params.Devices, devicePK) || csvContainsOrEmpty(params.Devices, deviceCode)
}

func multicastDeliveryOIFMatchesSubscriber(oif MulticastDeliveryOIF, params MulticastDeliveryParams) bool {
	if len(params.Subscribers) == 0 {
		return true
	}
	return csvContainsOrEmpty(params.Subscribers, oif.SubscriberUserPK) ||
		csvContainsOrEmpty(params.Subscribers, oif.SubscriberDevicePK) ||
		csvContainsOrEmpty(params.Subscribers, oif.SubscriberDeviceCode) ||
		csvContainsOrEmpty(params.Subscribers, strconv.Itoa(int(oif.SubscriberTunnelID)))
}

func uniqueMrouteDevices(mroutes []MulticastDeliveryMroute) []string {
	seen := map[string]bool{}
	devices := []string{}
	for _, mroute := range mroutes {
		if mroute.DevicePK == "" || seen[mroute.DevicePK] {
			continue
		}
		seen[mroute.DevicePK] = true
		devices = append(devices, mroute.DevicePK)
	}
	return devices
}

func shouldQueryMSDPPeers(kind string) bool {
	return kind == "all" || kind == "peers"
}

func shouldQueryMSDPPimSACache(kind string) bool {
	return kind == "all" || kind == "pim_sa_cache"
}

func shouldQueryMSDPSACache(kind string) bool {
	return kind == "all" || kind == "sa_cache"
}

func buildMulticastMSDPItems(kind string, peers []MulticastDeliveryMSDPPeer, pimSAs, saCache []MulticastDeliveryMSDPSA) []MulticastDeliveryMSDPItem {
	items := []MulticastDeliveryMSDPItem{}
	if shouldQueryMSDPPeers(kind) {
		for i := range peers {
			peer := peers[i]
			items = append(items, MulticastDeliveryMSDPItem{Kind: "peers", Peer: &peer})
		}
	}
	if shouldQueryMSDPPimSACache(kind) {
		for i := range pimSAs {
			sa := pimSAs[i]
			items = append(items, MulticastDeliveryMSDPItem{Kind: "pim_sa_cache", SA: &sa})
		}
	}
	if shouldQueryMSDPSACache(kind) {
		for i := range saCache {
			sa := saCache[i]
			items = append(items, MulticastDeliveryMSDPItem{Kind: "sa_cache", SA: &sa})
		}
	}
	return items
}

func paginateMulticastDeliveryItems[T any](items []T, params MulticastDeliveryParams) ([]T, int) {
	total := len(items)
	if params.Offset >= total {
		return []T{}, total
	}
	end := params.Offset + params.Limit
	if end > total {
		end = total
	}
	return items[params.Offset:end], total
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
