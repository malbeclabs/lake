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

// The multicast-delivery fan-outs bound their aggregate in-flight
// ClickHouse queries per env (across all concurrent requests). clickhouse-go
// does not queue pool acquisitions: past DialTimeout (5s) an acquisition
// fails with ErrAcquireConnTimeout, which is not a source-unavailable error
// and so surfaces as a 500 — to this endpoint and to every other handler
// sharing the env pool. The bound is 60% of the env pool's MaxOpenConns
// (6 of the 10-conn testnet pool, 60 of mainnet's 100), leaving the
// rest for other endpoints on the same pool (same rationale and shape as
// maxConcurrentPublisherCheckLive).
//
// maxConcurrentMulticastDeliveryQueries is the fallback bound when the pool
// size is unavailable, matching the smallest env pool's derived value.
const maxConcurrentMulticastDeliveryQueries = 6

const (
	// multicastDeliveryRequestTimeout bounds a whole entity request.
	multicastDeliveryRequestTimeout = 30 * time.Second

	// multicastDeliveryQueryTimeoutSeconds must stay strictly below
	// multicastDeliveryRequestTimeout so an overrun surfaces as a ClickHouse
	// TIMEOUT_EXCEEDED naming the query (logged at WARN, transient) rather than
	// a bare context.DeadlineExceeded from the handler's own deadline winning
	// the race. See TestMulticastDeliveryQueryTimeoutLosesToRequestDeadline.
	multicastDeliveryQueryTimeoutSeconds = 20
)

// multicastDeliveryQuerySettings is the SETTINGS clause every multicast
// delivery entity query ends with. The cap is the per-query budget or what is
// left of the request deadline, whichever is smaller: ClickHouse counts
// max_execution_time from the moment the query starts executing, so a query
// that waited on the fan-out semaphore, or that runs second within its branch
// (a count fallback), would otherwise outlast the handler's own deadline and
// lose the race the cap exists to lose.
func multicastDeliveryQuerySettings(ctx context.Context) string {
	seconds := multicastDeliveryQueryTimeoutSeconds
	if deadline, ok := ctx.Deadline(); ok {
		// One second of margin, so ClickHouse still gets to report its own
		// TIMEOUT_EXCEEDED before the request deadline fires.
		remaining := int(time.Until(deadline).Round(time.Second)/time.Second) - 1
		if remaining < seconds {
			seconds = max(remaining, 1)
		}
	}
	return fmt.Sprintf(
		"SETTINGS max_execution_time = %d, timeout_before_checking_execution_speed = 0",
		seconds,
	)
}

func multicastDeliveryQuerySemSize(maxOpenConns int) int {
	size := maxOpenConns * 6 / 10
	if size < 1 {
		return 1
	}
	return size
}

// multicastDeliveryFanout runs a multicast-delivery handler's independent
// ClickHouse queries concurrently, each holding a slot in the request env's
// semaphore.
type multicastDeliveryFanout struct {
	ctx context.Context
	sem chan struct{}
	wg  sync.WaitGroup
}

func (a *API) newMulticastDeliveryFanout(ctx context.Context) *multicastDeliveryFanout {
	return &multicastDeliveryFanout{ctx: ctx, sem: a.multicastDeliveryQuerySem(ctx)}
}

func (f *multicastDeliveryFanout) spawn(fn func()) {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		select {
		case f.sem <- struct{}{}:
			defer func() { <-f.sem }()
		case <-f.ctx.Done():
			// No slot before the request deadline (or the handler already
			// returned). Run fn anyway: with ctx done it fails fast with
			// the context error, keeping error handling uniform instead of
			// leaving its result vars zeroed as if the query succeeded.
		}
		fn()
	}()
}

// wait blocks until every query spawned since the last wait has finished, so a
// handler can Wait per stage.
func (f *multicastDeliveryFanout) wait() { f.wg.Wait() }

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
	ctx, cancel := context.WithTimeout(r.Context(), multicastDeliveryRequestTimeout)
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

	// The handler runs up to 11 ClickHouse queries — one per branch, since each
	// page query carries its own total as a window aggregate — under a single 30s
	// request deadline; sequentially their wall times accumulate to the margin on
	// loaded runners. Independent queries therefore run concurrently, in two stages:
	//
	//   stage 1: device row, source availability, source ingest times
	//            (independent of each other; everything else needs their results)
	//   stage 2: per-view state, rollup, and health queries
	//            (each depends only on stage 1's device + available map)
	//
	// Goroutines write only their own result/error variables; errors are
	// processed after each Wait in the original sequential order so error
	// precedence (device 404 first) and the source-unavailable downgrades are
	// unchanged. The device lookup gates everything else, so it gets its own
	// done signal: on device error (typically a 404) the handler returns as
	// soon as that result lands, without waiting for the sibling stage-1
	// queries — the deferred cancel aborts them and their writes go to
	// variables the handler no longer reads. No cross-cancellation on other
	// errors: sibling errors would surface as context.Canceled and scramble
	// the precedence, and each query already bounds itself with
	// max_execution_time.
	//
	// Spawned queries acquire the per-env semaphore so the fan-out's
	// aggregate ClickHouse footprint stays below that env's pool (see
	// multicastDeliveryQuerySemSize). The device lookup skips the
	// semaphore: it is a single point lookup that gates everything else, and
	// its common failure mode (404) must stay fast even when the semaphore is
	// saturated — one ungated conn per request matches the old sequential
	// footprint.
	now := time.Now().UTC()

	var (
		device                                  MulticastDeliveryDevice
		available                               map[string]bool
		sourceTimes                             map[string]time.Time
		deviceErr, availableErr, sourceTimesErr error
	)
	fanout := a.newMulticastDeliveryFanout(ctx)
	spawn := fanout.spawn
	deviceDone := make(chan struct{})
	go func() {
		defer close(deviceDone)
		device, deviceErr = a.queryMulticastDeliveryDevice(ctx, pkOrCode)
	}()
	spawn(func() { available, availableErr = a.queryMulticastDeliverySources(ctx) })
	spawn(func() { sourceTimes, sourceTimesErr = a.queryMulticastDeliverySourceIngestTimes(ctx) })

	<-deviceDone
	if deviceErr != nil {
		writeMulticastDeliveryEntityError(w, deviceErr, "multicast device delivery query error", "device", pkOrCode)
		return
	}
	fanout.wait()

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
		spawn(func() {
			mroutes, mrouteTimes, routeTotal, mrouteErr = a.queryMulticastDeviceDeliveryMroutes(ctx, device, params)
		})
	}
	if available[multicastDeliveryOIFView] {
		spawn(func() { oifs, oifTimes, oifTotal, oifErr = a.queryMulticastDeviceDeliveryOIFs(ctx, device, params) })
	}
	if available[multicastDeliveryMSDPPeersView] {
		spawn(func() { msdpPeers, peerTimes, peersErr = a.queryMulticastDeviceDeliveryMSDPPeers(ctx, device) })
	}
	if available[multicastDeliveryMSDPPimSAView] {
		spawn(func() {
			pimSAs, pimSATimes, pimSAErr = a.queryMulticastDeviceDeliveryMSDPSAs(ctx, multicastDeliveryMSDPPimSAView, "msdp_pim_sa_entity_id", device, params, false)
		})
	}
	if available[multicastDeliveryMSDPSAView] {
		spawn(func() {
			saCache, saTimes, saErr = a.queryMulticastDeviceDeliveryMSDPSAs(ctx, multicastDeliveryMSDPSAView, "msdp_sa_entity_id", device, params, true)
		})
	}
	spawn(func() { groups, groupErr = a.queryMulticastDeviceDeliveryGroups(ctx, device, params) })
	spawn(func() {
		healthUsers, healthUserTotal, userHealthCounts, healthErr = a.queryDeviceMulticastHealthUsers(ctx, device, params)
	})
	spawn(func() {
		endpointHealthItems, endpointHealthTotal, endpointHealthCounts, endpointErr = a.queryDeviceMulticastEndpointHealth(ctx, device, params)
	})
	fanout.wait()

	if available[multicastDeliveryMrouteView] && mrouteErr != nil {
		if !multicastDeliverySourceErr(mrouteErr) {
			writeMulticastDeliveryEntityError(w, mrouteErr, "multicast device mroutes query error", "device", pkOrCode)
			return
		}
		available[multicastDeliveryMrouteView] = false
		mroutes = []MulticastDeliveryMroute{}
		mrouteTimes = nil
	}

	if available[multicastDeliveryOIFView] && oifErr != nil {
		if !multicastDeliverySourceErr(oifErr) {
			writeMulticastDeliveryEntityError(w, oifErr, "multicast device oifs query error", "device", pkOrCode)
			return
		}
		available[multicastDeliveryOIFView] = false
		oifs = []MulticastDeliveryOIF{}
		oifTimes = nil
	}

	if available[multicastDeliveryMSDPPeersView] && peersErr != nil {
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
	ctx, cancel := context.WithTimeout(r.Context(), multicastDeliveryRequestTimeout)
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

	// Same two-stage fan-out, semaphore, and error-ordering rationale as
	// GetDeviceMulticastDelivery above: the link lookup gates the rest and
	// stays ungated so a 404 stays fast, then the queries that depend only on
	// it run concurrently. Sequentially, these queries each scan the same
	// view chains and their wall times accumulated to the 30s request deadline
	// on loaded runners. Related-group health needs the group rows, so it
	// chains behind the groups query inside one slot rather than forming a
	// third stage; its one caller-visible fallback (groups unavailable) is
	// handled below, where the OIF rows it needs exist.
	now := time.Now().UTC()

	var (
		link                                  MulticastDeliveryLink
		available                             map[string]bool
		sourceTimes                           map[string]time.Time
		linkErr, availableErr, sourceTimesErr error
	)
	fanout := a.newMulticastDeliveryFanout(ctx)
	linkDone := make(chan struct{})
	go func() {
		defer close(linkDone)
		link, linkErr = a.queryMulticastDeliveryLink(ctx, pkOrCode)
	}()
	fanout.spawn(func() { available, availableErr = a.queryMulticastDeliverySources(ctx) })
	fanout.spawn(func() { sourceTimes, sourceTimesErr = a.queryMulticastDeliverySourceIngestTimes(ctx) })

	<-linkDone
	if linkErr != nil {
		writeMulticastDeliveryEntityError(w, linkErr, "multicast link delivery query error", "link", pkOrCode)
		return
	}
	fanout.wait()

	if availableErr != nil {
		writeMulticastDeliveryEntityError(w, availableErr, "multicast link delivery sources query error", "link", pkOrCode)
		return
	}
	if sourceTimesErr != nil {
		writeMulticastDeliveryEntityError(w, sourceTimesErr, "multicast link delivery freshness query error", "link", pkOrCode)
		return
	}

	branches := []MulticastDeliveryLinkBranch{}
	var oifTimes []time.Time
	var branchTotal int
	var branchErr error
	var groups, groupsWithHealth []MulticastDeliveryEntityGroup
	var relatedHealthCounts MulticastEntityHealthStatusCounts
	var groupErr, healthErr error

	if available[multicastDeliveryOIFView] {
		fanout.spawn(func() {
			branches, oifTimes, branchTotal, branchErr = a.queryMulticastLinkDeliveryBranches(ctx, link, params)
		})
	}
	fanout.spawn(func() {
		groups, groupErr = a.queryMulticastLinkDeliveryGroups(ctx, link, params)
		if groupErr == nil {
			groupsWithHealth, relatedHealthCounts, healthErr = a.queryLinkRelatedGroupHealth(ctx, groups)
		}
	})
	fanout.wait()

	if branchErr != nil {
		if !multicastDeliverySourceErr(branchErr) {
			writeMulticastDeliveryEntityError(w, branchErr, "multicast link branches query error", "link", pkOrCode)
			return
		}
		available[multicastDeliveryOIFView] = false
		branches = []MulticastDeliveryLinkBranch{}
		oifTimes = nil
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
	if groupErr != nil {
		if !multicastDeliverySourceErr(groupErr) {
			writeMulticastDeliveryEntityError(w, groupErr, "multicast link groups query error", "link", pkOrCode)
			return
		}
		groups = buildMulticastEntityGroups(nil, oifs)
		groupsWithHealth, relatedHealthCounts, healthErr = a.queryLinkRelatedGroupHealth(ctx, groups)
	}
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
