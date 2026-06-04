package handlers

import "sort"

func buildMulticastEntityGroups(mroutes []MulticastDeliveryMroute, oifs []MulticastDeliveryOIF) []MulticastDeliveryEntityGroup {
	type groupAccumulator struct {
		group   MulticastDeliveryEntityGroup
		sources map[string]bool
	}
	groups := map[string]*groupAccumulator{}
	groupKey := func(pk, code, address string) string {
		for _, value := range []string{pk, code, address} {
			if value != "" {
				return value
			}
		}
		return "unknown"
	}
	ensure := func(pk, code, address string) *groupAccumulator {
		key := groupKey(pk, code, address)
		acc := groups[key]
		if acc == nil {
			acc = &groupAccumulator{
				group: MulticastDeliveryEntityGroup{
					GroupPK:      pk,
					GroupCode:    code,
					GroupAddress: address,
				},
				sources: map[string]bool{},
			}
			groups[key] = acc
		}
		if acc.group.GroupPK == "" {
			acc.group.GroupPK = pk
		}
		if acc.group.GroupCode == "" {
			acc.group.GroupCode = code
		}
		if acc.group.GroupAddress == "" {
			acc.group.GroupAddress = address
		}
		return acc
	}
	for _, mroute := range mroutes {
		acc := ensure(mroute.MulticastGroupPK, mroute.MulticastGroupCode, mroute.GroupAddress)
		acc.group.MrouteCount++
		acc.sources[mroute.SourceAddress] = true
	}
	for _, oif := range oifs {
		acc := ensure(oif.MulticastGroupPK, oif.MulticastGroupCode, oif.GroupAddress)
		acc.group.OIFCount++
		acc.sources[oif.SourceAddress] = true
	}
	out := make([]MulticastDeliveryEntityGroup, 0, len(groups))
	for _, acc := range groups {
		acc.group.SourceCount = len(acc.sources)
		out = append(out, acc.group)
	}
	sort.Slice(out, func(i, j int) bool {
		left := firstNonEmpty(out[i].GroupCode, out[i].GroupAddress, out[i].GroupPK)
		right := firstNonEmpty(out[j].GroupCode, out[j].GroupAddress, out[j].GroupPK)
		return left < right
	})
	return out
}

func buildMulticastDeviceSummary(mroutes []MulticastDeliveryMroute, oifs []MulticastDeliveryOIF, peers []MulticastDeliveryMSDPPeer, sas []MulticastDeliveryMSDPSA, groups []MulticastDeliveryEntityGroup, anomalyCount int) MulticastDeliveryDeviceSummary {
	sources := map[string]bool{}
	summary := MulticastDeliveryDeviceSummary{
		GroupCount:    len(groups),
		MrouteCount:   len(mroutes),
		OIFCount:      len(oifs),
		MSDPPeerCount: len(peers),
		MSDPSACount:   len(sas),
		AnomalyCount:  anomalyCount,
	}
	for _, mroute := range mroutes {
		sources[mroute.SourceAddress] = true
		if mroute.OIFCount > 0 {
			summary.RoutesWithOIFs++
		}
	}
	for _, oif := range oifs {
		sources[oif.SourceAddress] = true
		switch oif.OIFKind {
		case "underlay_link":
			summary.UnderlayOIFCount++
		case "subscriber_tunnel":
			summary.SubscriberTunnelOIFCount++
		}
	}
	summary.SourceCount = len(sources)
	return summary
}

func buildMulticastLinkSummary(branches []MulticastDeliveryLinkBranch, groups []MulticastDeliveryEntityGroup, anomalyCount int) MulticastDeliveryLinkSummary {
	sources := map[string]bool{}
	reportingDevices := map[string]bool{}
	summary := MulticastDeliveryLinkSummary{
		GroupCount:   len(groups),
		BranchCount:  len(branches),
		AnomalyCount: anomalyCount,
	}
	for _, branch := range branches {
		sources[branch.SourceAddress] = true
		reportingDevices[branch.DevicePK] = true
		switch branch.Direction {
		case "a_to_z":
			summary.AToZCount++
		case "z_to_a":
			summary.ZToACount++
		default:
			summary.UnknownDirectionCount++
		}
	}
	summary.SourceCount = len(sources)
	summary.ReportingDeviceCount = len(reportingDevices)
	return summary
}

func buildMulticastDeviceRoles(device MulticastDeliveryDevice, mroutes []MulticastDeliveryMroute, oifs []MulticastDeliveryOIF, peers []MulticastDeliveryMSDPPeer, sas []MulticastDeliveryMSDPSA) []MulticastDeliveryDeviceRole {
	type roleAccumulator struct {
		role    MulticastDeliveryDeviceRole
		groups  map[string]bool
		sources map[string]bool
	}
	defs := map[string]MulticastDeliveryDeviceRole{
		"publisher_host":  {Role: "publisher_host", Label: "Publisher host", Description: "Source address is owned by a multicast publisher on this device."},
		"transit":         {Role: "transit", Label: "Transit or branch", Description: "This device reports multicast route state with outgoing interfaces."},
		"subscriber_edge": {Role: "subscriber_edge", Label: "Subscriber edge", Description: "Observed subscriber tunnel state points at this device."},
		"control_plane":   {Role: "control_plane", Label: "Control plane", Description: "MSDP peer or SA-cache state involves this device."},
		"reporting":       {Role: "reporting", Label: "Reporting state", Description: "This device reports multicast route state for the group."},
	}
	roles := map[string]*roleAccumulator{}
	ensure := func(role string) *roleAccumulator {
		acc := roles[role]
		if acc == nil {
			def := defs[role]
			acc = &roleAccumulator{role: def, groups: map[string]bool{}, sources: map[string]bool{}}
			roles[role] = acc
		}
		return acc
	}
	addRoute := func(role string, mroute MulticastDeliveryMroute) {
		acc := ensure(role)
		acc.role.MrouteCount++
		acc.groups[firstNonEmpty(mroute.MulticastGroupPK, mroute.MulticastGroupCode, mroute.GroupAddress)] = true
		acc.sources[mroute.SourceAddress] = true
	}
	addOIF := func(role string, oif MulticastDeliveryOIF) {
		acc := ensure(role)
		acc.role.OIFCount++
		acc.groups[firstNonEmpty(oif.MulticastGroupPK, oif.MulticastGroupCode, oif.GroupAddress)] = true
		acc.sources[oif.SourceAddress] = true
	}
	for _, mroute := range mroutes {
		if mroute.PublisherDevicePK == device.PK {
			addRoute("publisher_host", mroute)
		}
		if mroute.DevicePK == device.PK {
			addRoute("reporting", mroute)
			if mroute.OIFCount > 0 {
				addRoute("transit", mroute)
			}
		}
	}
	for _, oif := range oifs {
		if oif.DevicePK == device.PK && oif.OIFKind == "underlay_link" {
			addOIF("transit", oif)
		}
		if oif.SubscriberDevicePK == device.PK || (oif.OIFKind == "subscriber_tunnel" && oif.DevicePK == device.PK) {
			addOIF("subscriber_edge", oif)
		}
	}
	if len(peers) > 0 || len(sas) > 0 {
		acc := ensure("control_plane")
		for _, peer := range peers {
			acc.sources[peer.PeerAddress] = true
		}
		for _, sa := range sas {
			acc.groups[firstNonEmpty(sa.GroupAddress)] = true
			acc.sources[sa.SourceAddress] = true
		}
	}
	out := make([]MulticastDeliveryDeviceRole, 0, len(roles))
	for _, acc := range roles {
		acc.role.GroupCount = len(acc.groups)
		acc.role.SourceCount = len(acc.sources)
		out = append(out, acc.role)
	}
	order := map[string]int{"publisher_host": 0, "transit": 1, "subscriber_edge": 2, "control_plane": 3, "reporting": 4}
	sort.Slice(out, func(i, j int) bool {
		return order[out[i].Role] < order[out[j].Role]
	})
	return out
}

func filterMulticastDeviceRoles(roles []MulticastDeliveryDeviceRole, allowed []string) []MulticastDeliveryDeviceRole {
	out := []MulticastDeliveryDeviceRole{}
	for _, role := range roles {
		if csvContainsOrEmpty(allowed, role.Role) {
			out = append(out, role)
		}
	}
	return out
}

func buildMulticastLinkDirections(branches []MulticastDeliveryLinkBranch) []MulticastDeliveryDirection {
	type directionAccumulator struct {
		direction MulticastDeliveryDirection
		groups    map[string]bool
		sources   map[string]bool
	}
	defs := map[string]MulticastDeliveryDirection{
		"a_to_z":  {Direction: "a_to_z", Label: "A to Z"},
		"z_to_a":  {Direction: "z_to_a", Label: "Z to A"},
		"unknown": {Direction: "unknown", Label: "Unknown"},
	}
	directions := map[string]*directionAccumulator{}
	ensure := func(direction string) *directionAccumulator {
		acc := directions[direction]
		if acc == nil {
			def := defs[direction]
			if def.Direction == "" {
				def = defs["unknown"]
			}
			acc = &directionAccumulator{direction: def, groups: map[string]bool{}, sources: map[string]bool{}}
			directions[direction] = acc
		}
		return acc
	}
	for _, branch := range branches {
		acc := ensure(branch.Direction)
		acc.direction.BranchCount++
		acc.groups[firstNonEmpty(branch.MulticastGroupPK, branch.MulticastGroupCode, branch.GroupAddress)] = true
		acc.sources[branch.SourceAddress] = true
	}
	out := make([]MulticastDeliveryDirection, 0, len(directions))
	for _, acc := range directions {
		acc.direction.GroupCount = len(acc.groups)
		acc.direction.SourceCount = len(acc.sources)
		out = append(out, acc.direction)
	}
	order := map[string]int{"a_to_z": 0, "z_to_a": 1, "unknown": 2}
	sort.Slice(out, func(i, j int) bool {
		return order[out[i].Direction] < order[out[j].Direction]
	})
	return out
}

func buildMulticastEntityAnomalies(entityKind, entityPK string, freshness MulticastDeliveryFreshness, available map[string]bool, mroutes []MulticastDeliveryMroute, oifs []MulticastDeliveryOIF) []MulticastDeliveryAnomaly {
	anomalies := []MulticastDeliveryAnomaly{}
	if !available[multicastDeliveryMrouteView] && !available[multicastDeliveryOIFView] {
		anomalies = append(anomalies, multicastAnomaly(
			"mroute-source-unavailable", "warning", "source_unavailable", entityKind,
			map[string]string{entityKind + "_pk": entityPK},
			"multicast route forwarding state source is unavailable for this environment",
		))
	}
	if freshness.Mroute.Status == "stale" {
		anomalies = append(anomalies, multicastAnomaly(
			"stale-mroute-state", "warning", "stale_state", entityKind,
			map[string]string{entityKind + "_pk": entityPK},
			"latest multicast route snapshot is stale",
		))
	}
	for _, mroute := range mroutes {
		if len(anomalies) >= 50 {
			break
		}
		if mroute.SourceMatchStatus == "unknown_source" && mroute.SourceAddress != "0.0.0.0" {
			anomalies = append(anomalies, multicastAnomaly(
				"unknown-source-"+mroute.MrouteID, "warning", "unknown_source", "mroute",
				map[string]string{entityKind + "_pk": entityPK, "mroute_id": mroute.MrouteID, "source_address": mroute.SourceAddress},
				"mroute source did not match an activated multicast publisher for this group",
			))
		}
	}
	for _, oif := range oifs {
		if len(anomalies) >= 50 {
			break
		}
		if oif.OIFKind == "unknown" {
			anomalies = append(anomalies, multicastAnomaly(
				"unknown-oif-"+oif.MrouteID+"-"+oif.OIFName, "warning", "unknown_oif", "mroute",
				map[string]string{entityKind + "_pk": entityPK, "mroute_id": oif.MrouteID, "oif_name": oif.OIFName},
				"mroute OIF could not be resolved to a link, interface, subscriber tunnel, or special interface",
			))
		}
	}
	return anomalies
}
