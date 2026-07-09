package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers/dberror"
)

const multicastDeliveryEntityCoverageNote = "Current observed route state is limited to devices reporting multicast forwarding telemetry; absence is not proof of packet loss."

type multicastDeliveryEntityParams struct {
	Groups         []string
	Sources        []string
	EndpointIPs    []string
	OIFKinds       []string
	Roles          []string
	Health         []string
	Direction      string
	Limit          int
	Offset         int
	EndpointLimit  int
	EndpointOffset int
}

// GetDeviceMulticastDelivery returns observed multicast route/OIF state related
// to one device across all multicast groups.
func (a *API) GetDeviceMulticastDelivery(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing device pk", http.StatusBadRequest)
		return
	}

	params, err := parseMulticastDeliveryEntityParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	device, err := a.queryMulticastDeliveryDevice(ctx, pkOrCode)
	if err != nil {
		writeMulticastDeliveryEntityError(w, err, "multicast device delivery query error", "device", pkOrCode)
		return
	}

	available, err := a.queryMulticastDeliverySources(ctx)
	if err != nil {
		writeMulticastDeliveryEntityError(w, err, "multicast device delivery sources query error", "device", pkOrCode)
		return
	}
	sourceTimes, err := a.queryMulticastDeliverySourceIngestTimes(ctx)
	if err != nil {
		writeMulticastDeliveryEntityError(w, err, "multicast device delivery freshness query error", "device", pkOrCode)
		return
	}

	var mroutes []MulticastDeliveryMroute
	var oifs []MulticastDeliveryOIF
	var msdpPeers []MulticastDeliveryMSDPPeer
	var msdpSAs []MulticastDeliveryMSDPSA
	var routeTotal, oifTotal int
	var mrouteTimes, oifTimes, peerTimes, pimSATimes, saTimes []time.Time

	if available[multicastDeliveryMrouteView] {
		mroutes, mrouteTimes, routeTotal, err = a.queryMulticastDeviceDeliveryMroutes(ctx, device, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryEntityError(w, err, "multicast device mroutes query error", "device", pkOrCode)
				return
			}
			available[multicastDeliveryMrouteView] = false
			mroutes = []MulticastDeliveryMroute{}
			mrouteTimes = nil
		}
	}

	if available[multicastDeliveryOIFView] {
		oifs, oifTimes, oifTotal, err = a.queryMulticastDeviceDeliveryOIFs(ctx, device, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryEntityError(w, err, "multicast device oifs query error", "device", pkOrCode)
				return
			}
			available[multicastDeliveryOIFView] = false
			oifs = []MulticastDeliveryOIF{}
			oifTimes = nil
		}
	}

	if available[multicastDeliveryMSDPPeersView] {
		msdpPeers, peerTimes, err = a.queryMulticastDeviceDeliveryMSDPPeers(ctx, device)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryEntityError(w, err, "multicast device msdp peers query error", "device", pkOrCode)
				return
			}
			available[multicastDeliveryMSDPPeersView] = false
			msdpPeers = []MulticastDeliveryMSDPPeer{}
			peerTimes = nil
		}
	}

	if available[multicastDeliveryMSDPPimSAView] {
		var pimSAs []MulticastDeliveryMSDPSA
		pimSAs, pimSATimes, err = a.queryMulticastDeviceDeliveryMSDPSAs(ctx, multicastDeliveryMSDPPimSAView, "msdp_pim_sa_entity_id", device, params, false)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryEntityError(w, err, "multicast device pim sa query error", "device", pkOrCode)
				return
			}
			available[multicastDeliveryMSDPPimSAView] = false
			pimSATimes = nil
		} else {
			msdpSAs = append(msdpSAs, pimSAs...)
		}
	}

	if available[multicastDeliveryMSDPSAView] {
		var saCache []MulticastDeliveryMSDPSA
		saCache, saTimes, err = a.queryMulticastDeviceDeliveryMSDPSAs(ctx, multicastDeliveryMSDPSAView, "msdp_sa_entity_id", device, params, true)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryEntityError(w, err, "multicast device sa cache query error", "device", pkOrCode)
				return
			}
			available[multicastDeliveryMSDPSAView] = false
			saTimes = nil
		} else {
			msdpSAs = append(msdpSAs, saCache...)
		}
	}

	freshnessAvailable := copyMulticastDeliveryAvailability(available)
	freshnessAvailable[multicastDeliveryMrouteView] = available[multicastDeliveryMrouteView] || available[multicastDeliveryOIFView]
	stateTimes := append([]time.Time{}, mrouteTimes...)
	stateTimes = append(stateTimes, oifTimes...)
	freshness := buildMulticastFreshness(now, freshnessAvailable, sourceTimes, stateTimes, peerTimes, pimSATimes, saTimes)
	applyMrouteFreshness(freshness.Mroute, mroutes)
	applyOIFFreshness(freshness.Mroute, oifs)
	applyMSDPPeerFreshness(freshness.MSDPPeers, msdpPeers)

	roles := buildMulticastDeviceRoles(device, mroutes, oifs, msdpPeers, msdpSAs)
	if len(params.Roles) > 0 {
		roles = filterMulticastDeviceRoles(roles, params.Roles)
	}
	groups, groupErr := a.queryMulticastDeviceDeliveryGroups(ctx, device, params)
	if groupErr != nil {
		if !multicastDeliverySourceErr(groupErr) {
			writeMulticastDeliveryEntityError(w, groupErr, "multicast device groups query error", "device", pkOrCode)
			return
		}
		groups = buildMulticastEntityGroups(mroutes, oifs)
	}
	healthUsers, healthUserTotal, userHealthCounts, healthErr := a.queryDeviceMulticastHealthUsers(ctx, device, params)
	if healthErr != nil {
		if !multicastDeliverySourceErr(healthErr) {
			writeMulticastDeliveryEntityError(w, healthErr, "multicast device health users query error", "device", pkOrCode)
			return
		}
		healthUsers = []MulticastHealthUserItem{}
	}
	endpointHealthItems, endpointHealthTotal, endpointHealthCounts, endpointErr := a.queryDeviceMulticastEndpointHealth(ctx, device, params)
	if endpointErr != nil {
		if !multicastDeliverySourceErr(endpointErr) {
			writeMulticastDeliveryEntityError(w, endpointErr, "multicast device endpoint health query error", "device", pkOrCode)
			return
		}
		endpointHealthItems = []MulticastHealthPathItem{}
	}
	anomalies := buildMulticastEntityAnomalies("device", device.PK, freshness, available, mroutes, oifs)
	summary := buildMulticastDeviceSummary(mroutes, oifs, msdpPeers, msdpSAs, groups, len(anomalies))
	summary.MrouteCount = routeTotal
	summary.OIFCount = oifTotal
	summary.UserHealthCounts = userHealthCounts
	summary.EndpointHealthCounts = endpointHealthCounts

	writeJSON(w, MulticastDeliveryDeviceResponse{
		Device:              device,
		SourceAvailable:     available[multicastDeliveryMrouteView] || available[multicastDeliveryOIFView] || available[multicastDeliveryMSDPPeersView] || available[multicastDeliveryMSDPPimSAView] || available[multicastDeliveryMSDPSAView],
		GeneratedAt:         formatMulticastTime(now),
		Freshness:           freshness,
		CoverageNote:        multicastDeliveryEntityCoverageNote,
		HealthContextNote:   "Device health context combines observed forwarding state with multicast user rate and endpoint checks. Endpoint rows verify publisher and subscriber endpoints only, not every transit hop.",
		Summary:             summary,
		Groups:              groups,
		Roles:               roles,
		HealthUsers:         healthUsers,
		HealthUserTotal:     healthUserTotal,
		EndpointHealthItems: endpointHealthItems,
		EndpointHealthTotal: endpointHealthTotal,
		EndpointLimit:       params.EndpointLimit,
		EndpointOffset:      params.EndpointOffset,
		Routes:              mroutes,
		OIFs:                oifs,
		MSDPPeers:           msdpPeers,
		MSDPSAs:             msdpSAs,
		RouteTotal:          routeTotal,
		OIFTotal:            oifTotal,
		Limit:               params.Limit,
		Offset:              params.Offset,
		Anomalies:           anomalies,
	})
}

// GetLinkMulticastDelivery returns observed multicast OIF state for one link
// across all multicast groups.
func (a *API) GetLinkMulticastDelivery(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	pkOrCode := chi.URLParam(r, "pk")
	if pkOrCode == "" {
		http.Error(w, "missing link pk", http.StatusBadRequest)
		return
	}

	params, err := parseMulticastDeliveryEntityParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	link, err := a.queryMulticastDeliveryLink(ctx, pkOrCode)
	if err != nil {
		writeMulticastDeliveryEntityError(w, err, "multicast link delivery query error", "link", pkOrCode)
		return
	}

	available, err := a.queryMulticastDeliverySources(ctx)
	if err != nil {
		writeMulticastDeliveryEntityError(w, err, "multicast link delivery sources query error", "link", pkOrCode)
		return
	}
	sourceTimes, err := a.queryMulticastDeliverySourceIngestTimes(ctx)
	if err != nil {
		writeMulticastDeliveryEntityError(w, err, "multicast link delivery freshness query error", "link", pkOrCode)
		return
	}

	branches := []MulticastDeliveryLinkBranch{}
	var oifTimes []time.Time
	var branchTotal int
	if available[multicastDeliveryOIFView] {
		branches, oifTimes, branchTotal, err = a.queryMulticastLinkDeliveryBranches(ctx, link, params)
		if err != nil {
			if !multicastDeliverySourceErr(err) {
				writeMulticastDeliveryEntityError(w, err, "multicast link branches query error", "link", pkOrCode)
				return
			}
			available[multicastDeliveryOIFView] = false
			branches = []MulticastDeliveryLinkBranch{}
			oifTimes = nil
		}
	}

	freshnessAvailable := copyMulticastDeliveryAvailability(available)
	freshnessAvailable[multicastDeliveryMrouteView] = available[multicastDeliveryOIFView]
	freshness := buildMulticastFreshness(now, freshnessAvailable, sourceTimes, oifTimes, nil, nil, nil)
	for i := range branches {
		if freshness.Mroute.AgeSeconds != nil {
			branches[i].AgeSeconds = *freshness.Mroute.AgeSeconds
			branches[i].FreshnessStatus = freshness.Mroute.Status
		}
	}

	oifs := make([]MulticastDeliveryOIF, 0, len(branches))
	for _, branch := range branches {
		oifs = append(oifs, branch.MulticastDeliveryOIF)
	}
	groups, groupErr := a.queryMulticastLinkDeliveryGroups(ctx, link, params)
	if groupErr != nil {
		if !multicastDeliverySourceErr(groupErr) {
			writeMulticastDeliveryEntityError(w, groupErr, "multicast link groups query error", "link", pkOrCode)
			return
		}
		groups = buildMulticastEntityGroups(nil, oifs)
	}
	groupsWithHealth, relatedHealthCounts, healthErr := a.queryLinkRelatedGroupHealth(ctx, groups)
	if healthErr != nil {
		if !multicastDeliverySourceErr(healthErr) {
			writeMulticastDeliveryEntityError(w, healthErr, "multicast link related health query error", "link", pkOrCode)
			return
		}
	} else {
		groups = groupsWithHealth
	}
	anomalies := buildMulticastEntityAnomalies("link", link.PK, freshness, available, nil, oifs)
	directions := buildMulticastLinkDirections(branches)
	summary := buildMulticastLinkSummary(branches, groups, len(anomalies))
	summary.BranchCount = branchTotal
	summary.RelatedGroupHealthCounts = relatedHealthCounts

	writeJSON(w, MulticastDeliveryLinkResponse{
		Link:              link,
		SourceAvailable:   available[multicastDeliveryOIFView],
		GeneratedAt:       formatMulticastTime(now),
		Freshness:         freshness,
		CoverageNote:      "Observed branches are current multicast route OIF state, not measured traffic volume.",
		HealthContextNote: "Related group health is context. Endpoint health does not prove this link caused degradation.",
		Summary:           summary,
		Groups:            groups,
		Branches:          branches,
		Directions:        directions,
		BranchTotal:       branchTotal,
		Limit:             params.Limit,
		Offset:            params.Offset,
		Anomalies:         anomalies,
	})
}

func parseMulticastDeliveryEntityParams(r *http.Request) (multicastDeliveryEntityParams, error) {
	q := r.URL.Query()
	direction := strings.TrimSpace(q.Get("direction"))
	if direction != "" && direction != "a_to_z" && direction != "z_to_a" && direction != "unknown" {
		return multicastDeliveryEntityParams{}, fmt.Errorf("invalid direction")
	}
	health := splitCSVParam(q.Get("health"))
	for _, status := range health {
		if status != "healthy" && status != "degraded" && status != "unhealthy" && status != "disconnected" && status != "unknown" {
			return multicastDeliveryEntityParams{}, fmt.Errorf("invalid health")
		}
	}
	pagination := ParsePagination(r, 50)
	endpointLimit, endpointOffset := parseEndpointPagination(r)
	return multicastDeliveryEntityParams{
		Groups:         splitCSVParam(firstNonEmpty(q.Get("group"), q.Get("groups"))),
		Sources:        splitCSVParam(q.Get("source")),
		EndpointIPs:    splitCSVParam(q.Get("endpoint_ip")),
		OIFKinds:       splitCSVParam(q.Get("oif_kind")),
		Roles:          splitCSVParam(q.Get("role")),
		Health:         health,
		Direction:      direction,
		Limit:          pagination.Limit,
		Offset:         pagination.Offset,
		EndpointLimit:  endpointLimit,
		EndpointOffset: endpointOffset,
	}, nil
}

func parseEndpointPagination(r *http.Request) (limit, offset int) {
	limit = 25
	q := r.URL.Query()
	if raw := q.Get("endpoint_limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
			if limit > MaxLimit {
				limit = MaxLimit
			}
		}
	}
	if raw := q.Get("endpoint_offset"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			offset = n
		}
	}
	return limit, offset
}

func writeMulticastDeliveryEntityError(w http.ResponseWriter, err error, msg, entityKind, pkOrCode string) {
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, entityKind+" not found", http.StatusNotFound)
		return
	}
	logError(msg, "error", err, "pk", pkOrCode)
	http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
}
