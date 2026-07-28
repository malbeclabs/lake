package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
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

	// The handler runs up to 11 ClickHouse queries under a single 30s request
	// deadline; sequentially their wall times accumulate to the margin on loaded
	// runners. Independent queries therefore run concurrently, in two stages:
	//
	//   stage 1: device row, source availability, source ingest times
	//            (independent of each other; everything else needs their results)
	//   stage 2: per-view state, rollup, and health queries
	//            (each depends only on stage 1's device + available map)
	//
	// Goroutines write only their own result/error variables; errors are
	// processed after each Wait in the original sequential order so error
	// precedence (device 404 first) and the source-unavailable downgrades are
	// unchanged. No cross-cancellation on error: sibling errors would surface as
	// context.Canceled and scramble that precedence, and each query already
	// bounds itself with max_execution_time.
	now := time.Now().UTC()

	var (
		device                                  MulticastDeliveryDevice
		available                               map[string]bool
		sourceTimes                             map[string]time.Time
		deviceErr, availableErr, sourceTimesErr error
	)
	var wg sync.WaitGroup
	runQuery := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	runQuery(func() { device, deviceErr = a.queryMulticastDeliveryDevice(ctx, pkOrCode) })
	runQuery(func() { available, availableErr = a.queryMulticastDeliverySources(ctx) })
	runQuery(func() { sourceTimes, sourceTimesErr = a.queryMulticastDeliverySourceIngestTimes(ctx) })
	wg.Wait()

	if deviceErr != nil {
		writeMulticastDeliveryEntityError(w, deviceErr, "multicast device delivery query error", "device", pkOrCode)
		return
	}
	if availableErr != nil {
		writeMulticastDeliveryEntityError(w, availableErr, "multicast device delivery sources query error", "device", pkOrCode)
		return
	}
	if sourceTimesErr != nil {
		writeMulticastDeliveryEntityError(w, sourceTimesErr, "multicast device delivery freshness query error", "device", pkOrCode)
		return
	}

	var mroutes []MulticastDeliveryMroute
	var oifs []MulticastDeliveryOIF
	var msdpPeers []MulticastDeliveryMSDPPeer
	var msdpSAs, pimSAs, saCache []MulticastDeliveryMSDPSA
	var routeTotal, oifTotal int
	var mrouteTimes, oifTimes, peerTimes, pimSATimes, saTimes []time.Time
	var mrouteErr, oifErr, peersErr, pimSAErr, saErr error

	var groups []MulticastDeliveryEntityGroup
	var groupErr error
	var healthUsers []MulticastHealthUserItem
	var healthUserTotal int
	var userHealthCounts MulticastEntityHealthStatusCounts
	var healthErr error
	var endpointHealthItems []MulticastHealthPathItem
	var endpointHealthTotal int
	var endpointHealthCounts MulticastEntityHealthStatusCounts
	var endpointErr error

	if available[multicastDeliveryMrouteView] {
		runQuery(func() {
			mroutes, mrouteTimes, routeTotal, mrouteErr = a.queryMulticastDeviceDeliveryMroutes(ctx, device, params)
		})
	}
	if available[multicastDeliveryOIFView] {
		runQuery(func() { oifs, oifTimes, oifTotal, oifErr = a.queryMulticastDeviceDeliveryOIFs(ctx, device, params) })
	}
	if available[multicastDeliveryMSDPPeersView] {
		runQuery(func() { msdpPeers, peerTimes, peersErr = a.queryMulticastDeviceDeliveryMSDPPeers(ctx, device) })
	}
	if available[multicastDeliveryMSDPPimSAView] {
		runQuery(func() {
			pimSAs, pimSATimes, pimSAErr = a.queryMulticastDeviceDeliveryMSDPSAs(ctx, multicastDeliveryMSDPPimSAView, "msdp_pim_sa_entity_id", device, params, false)
		})
	}
	if available[multicastDeliveryMSDPSAView] {
		runQuery(func() {
			saCache, saTimes, saErr = a.queryMulticastDeviceDeliveryMSDPSAs(ctx, multicastDeliveryMSDPSAView, "msdp_sa_entity_id", device, params, true)
		})
	}
	runQuery(func() { groups, groupErr = a.queryMulticastDeviceDeliveryGroups(ctx, device, params) })
	runQuery(func() {
		healthUsers, healthUserTotal, userHealthCounts, healthErr = a.queryDeviceMulticastHealthUsers(ctx, device, params)
	})
	runQuery(func() {
		endpointHealthItems, endpointHealthTotal, endpointHealthCounts, endpointErr = a.queryDeviceMulticastEndpointHealth(ctx, device, params)
	})
	wg.Wait()

	if mrouteErr != nil {
		if !multicastDeliverySourceErr(mrouteErr) {
			writeMulticastDeliveryEntityError(w, mrouteErr, "multicast device mroutes query error", "device", pkOrCode)
			return
		}
		available[multicastDeliveryMrouteView] = false
		mroutes = []MulticastDeliveryMroute{}
		mrouteTimes = nil
	}

	if oifErr != nil {
		if !multicastDeliverySourceErr(oifErr) {
			writeMulticastDeliveryEntityError(w, oifErr, "multicast device oifs query error", "device", pkOrCode)
			return
		}
		available[multicastDeliveryOIFView] = false
		oifs = []MulticastDeliveryOIF{}
		oifTimes = nil
	}

	if peersErr != nil {
		if !multicastDeliverySourceErr(peersErr) {
			writeMulticastDeliveryEntityError(w, peersErr, "multicast device msdp peers query error", "device", pkOrCode)
			return
		}
		available[multicastDeliveryMSDPPeersView] = false
		msdpPeers = []MulticastDeliveryMSDPPeer{}
		peerTimes = nil
	}

	if available[multicastDeliveryMSDPPimSAView] {
		if pimSAErr != nil {
			if !multicastDeliverySourceErr(pimSAErr) {
				writeMulticastDeliveryEntityError(w, pimSAErr, "multicast device pim sa query error", "device", pkOrCode)
				return
			}
			available[multicastDeliveryMSDPPimSAView] = false
			pimSATimes = nil
		} else {
			msdpSAs = append(msdpSAs, pimSAs...)
		}
	}

	if available[multicastDeliveryMSDPSAView] {
		if saErr != nil {
			if !multicastDeliverySourceErr(saErr) {
				writeMulticastDeliveryEntityError(w, saErr, "multicast device sa cache query error", "device", pkOrCode)
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
	if groupErr != nil {
		if !multicastDeliverySourceErr(groupErr) {
			writeMulticastDeliveryEntityError(w, groupErr, "multicast device groups query error", "device", pkOrCode)
			return
		}
		groups = buildMulticastEntityGroups(mroutes, oifs)
	}
	if healthErr != nil {
		if !multicastDeliverySourceErr(healthErr) {
			writeMulticastDeliveryEntityError(w, healthErr, "multicast device health users query error", "device", pkOrCode)
			return
		}
		healthUsers = []MulticastHealthUserItem{}
	}
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
