package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/dberror"
)

// Edge multicast overview: every multicast group that carries an edge service, grouped by the
// service that sells it, with who publishes into it, who receives it, and whether anything is
// actually flowing right now.
//
// The three existing multicast views answer this one group at a time (/dz/multicast-groups/{pk}).
// The question this page exists for is the fleet one — "is any feed silent?" — which nobody can
// answer by opening nine tabs.
//
// # What "traffic" means here, and what it does not
//
// Rates come from device_interface_rollup_5m via health_multicast_user_rate: the same numbers the
// per-group Health tab shows, so the two pages never disagree. That carries two limits, both
// surfaced in the payload rather than hidden:
//
//  1. GRAIN AND AGE. The rollup buckets are five minutes wide and the counter pipeline lands them
//     several minutes late (measured on mainnet: newest bucket 5-10 min behind wall clock). So a
//     publisher that died twenty seconds ago still reads active here. ObservedAt/ObservedAgeSeconds
//     carry the age of the freshest bucket so the UI can say how stale the verdict is instead of
//     showing a confident green dot over ten-minute-old data. The sub-minute answer comes from a
//     different plane entirely — see edge_multicast_lastheard.go, which reads what the recording
//     nodes actually received. (Not from the Kalshi L2 coverage view: that one is refreshed every
//     ten minutes over a fifteen-minute window, so its last-heard is staler than these counters,
//     not fresher.)
//
//  2. PER-TUNNEL, NOT PER-GROUP. Counters are per interface. A user that publishes into several
//     groups from one tunnel — the normal case here: the Kalshi publisher feeds four groups from
//     tunnel 503, and 539 of the 768 edge-solana-shreds publishers also publish into
//     edge-solana-root — contributes one rate that cannot be split between them. PublishersMultiGroup
//     counts those users per group and TrafficAmbiguous flags it, which is the same guard
//     health_multicast_user_rate applies as 'multi_group_ambiguity'. The bps figure is then an upper
//     bound for the group, and the UI must not present it as exact.
//
// Neither limit blocks the question the page is for. "No publisher has moved a byte in the last
// bucket" is a true and useful statement even at five-minute grain; Silent reports exactly that.

// EdgeMulticastCacheKey is the page-cache key written by the page-cache worker. The fleet
// aggregate runs four grouped queries over health_multicast_user_rate and dz_users_current
// (~350ms together on mainnet, the per-publisher read included), which is cheap enough to serve
// live but wasteful to repeat per viewer on a page that refreshes on a timer.
const EdgeMulticastCacheKey = "edge_multicast"

// edgeMulticastGroupCodePrefix is what makes a multicast group part of the Edge product: its
// ledger code. Membership in the product is a naming convention (`edge-<venue>-<product>-<plane>`
// for the market-data feeds, `edge-solana-*` for shreds), not something the feed catalog decides.
//
// Scoping by prefix rather than by "some feed row lists it" gets two cases right that the catalog
// gets wrong. A group provisioned onchain before its feed exists — edge-solana-shreds1 on mainnet
// today, no feed, no members — is part of the product and belongs on this page from the moment it
// is created, which is exactly when someone wants to watch it come up. And a group a NON-product
// feed happens to carry — mg02 and mg03, claimed by the qa-payments feed — is not Edge and does
// not belong here at all.
//
// The lab and partner groups (mbone, rebop, tiredsolid, sentrynet, jito-shredstream, …) are
// excluded by the same rule. They are visible on /dz/multicast-groups, which is the every-group
// view; this page is the product view.
// The hyphen is part of the prefix: without it startsWith also matches a code like
// 'edgecase-lab', which would land an unrelated group in the unclaimed section flagged silent,
// with nothing on the page to say why it is there.
const edgeMulticastGroupCodePrefix = "edge-"

// edgeMulticastPlanes maps the plane suffix a group's ledger code carries to its display label.
// A plane is one feed spec of one product line: market-by-price, market-by-order, or top-of-book.
// The
// suffix is the only place that distinction is recorded, on both the group code
// (edge-kalshi-perps-mbp) and the feed code (kalshi-perps-mbp).
//
// mbo has no group on mainnet yet and is listed anyway: the suffix set is what the feed catalog
// and the capture source ids are built from, so a plane missing here would silently collapse into
// its product line's section with a blank column instead of showing up as a plane of its own.
var edgeMulticastPlanes = map[string]string{
	"mbp": "MBP",
	"mbo": "MBO",
	"tob": "TOP",
}

// edgeMulticastPlaneFor returns the display label for a code's plane, or "" when the code carries
// none — the Solana groups, which are not split by plane.
func edgeMulticastPlaneFor(code string) string {
	if i := strings.LastIndexByte(code, '-'); i >= 0 {
		return edgeMulticastPlanes[code[i+1:]]
	}
	return ""
}

// edgeMulticastPlaneSuffixSQL renders the plane suffixes as a regex alternation for the feed
// query, which strips them to group the planes of one product into one section. Generated from
// edgeMulticastPlanes rather than spelled out in the SQL string: a plane added to the map but not
// to the query would keep its own section and read as a separate product, which is exactly the
// split this grouping exists to undo.
func edgeMulticastPlaneSuffixSQL() string {
	suffixes := make([]string, 0, len(edgeMulticastPlanes))
	for suffix := range edgeMulticastPlanes {
		suffixes = append(suffixes, suffix)
	}
	sort.Strings(suffixes) // stable query text across calls
	return "-(" + strings.Join(suffixes, "|") + ")$"
}

// edgeMulticastUnclaimedService is the bucket for an Edge group no feed row claims yet. Listed
// last rather than dropped: a product group with no feed behind it is either new or misconfigured,
// and both are things to notice.
const edgeMulticastUnclaimedService = "edge-unclaimed"

// EdgeMulticastRoleCounts breaks one side of a group (publishers or subscribers) into what the
// rate data says about it. Total comes from the ledger; the rest from the rate view.
//
// Active + Idle + Unknown always equals Total: Unknown absorbs both the rows the view marked
// 'no_data' and any ledger member the view has no row for at all, so the three never fail to
// account for a member.
type EdgeMulticastRoleCounts struct {
	Total  int `json:"total"`
	Active int `json:"active"`
	Idle   int `json:"idle"`

	// Unknown is "we cannot say": no counter row inside the freshness window, or no row in
	// the health view. It is NOT "down" — a device that stopped reporting telemetry lands
	// here alongside a member that genuinely went away.
	Unknown int `json:"unknown"`

	// Recorders, InternalProbes and Customers split the same Total by whose box it is — see
	// edge_multicast_class.go for how a member is classified and why owner identity cannot do
	// it. The three always sum to Total; Customers absorbs every member nothing speaks for.
	Recorders      int `json:"recorders"`
	InternalProbes int `json:"internal_probes"`
	Customers      int `json:"customers"`

	// ClassAsserted and ClassDerived are how many of those classifications are actually known:
	// asserted by an operator row, or derived from the capture-host list. Total minus the two
	// is the remainder that merely defaulted to customer, which is a much weaker claim and has
	// to stay visibly weaker on screen.
	ClassAsserted int `json:"class_asserted"`
	ClassDerived  int `json:"class_derived"`
}

// EdgeMulticastGroup is one multicast group as this page sees it.
type EdgeMulticastGroup struct {
	PK           string `json:"pk"`
	Code         string `json:"code"`
	MulticastIP  string `json:"multicast_ip"`
	Status       string `json:"status"`
	MaxBandwidth uint64 `json:"max_bandwidth"`

	// Plane is which view of the product this group carries — MBP, MBO or TOP — read off the
	// code's suffix. Empty for a group that is not plane-split, i.e. every Solana group.
	Plane string `json:"plane,omitempty"`

	Publishers  EdgeMulticastRoleCounts `json:"publishers"`
	Subscribers EdgeMulticastRoleCounts `json:"subscribers"`

	// PublisherLines is this group's publishers one line each — the grain the verdict is
	// actually taken at, since a feed with one live publisher and one dead one rolls up
	// identically to a healthy one. Sorted worst-first and capped at
	// edgeMulticastPublisherLineCap; PublisherLinesTotal is the count before the cap, so a
	// truncated list says so instead of reading as the whole published set.
	PublisherLines      []EdgeMulticastPublisher `json:"publisher_lines"`
	PublisherLinesTotal int                      `json:"publisher_lines_total"`

	// PublishersBelowFloor is how many publishers are not clearing
	// edgeMulticastPublisherFloorBps — idle ones included, since zero is below any floor.
	// Counted over every publisher, never over the capped list.
	PublishersBelowFloor int `json:"publishers_below_floor"`

	// PublishersPublishing is the complement: publishers measured at or above the floor. Carried
	// explicitly rather than left for the UI to derive from Total minus the remainders, which is
	// arithmetic that goes wrong the first time the ledger and the rate view disagree about who
	// exists.
	PublishersPublishing int `json:"publishers_publishing"`

	// CaptureNodes is the application plane's per-node view of this group: what each recording
	// node wrote down in the window, and how that compares with its peers. Empty for a group no
	// capture covers, which is most of them.
	CaptureNodes []EdgeMulticastCaptureNode `json:"capture_nodes,omitempty"`

	// CaptureNodesLagging is how many of those nodes are below the parity floor — recorders on
	// one group receive the same feed, so a node far under the median is not hearing it.
	CaptureNodesLagging int `json:"capture_nodes_lagging,omitempty"`

	// Sequence is the roll-up of the group's recorded sequence series — nil for a group with no
	// recorder running the Edge wire protocol behind it, which is every group except the
	// market-by-price ones today.
	//
	// A roll-up, and not the verdict: a series belongs to one publisher, so the verdict lives on
	// EdgeMulticastPublisher.Sequence, one per line. This exists because the lines are collapsed
	// by default and because an instance whose source address matches no publisher would
	// otherwise have nowhere to be reported. See edge_multicast_sequence.go.
	Sequence *EdgeMulticastSequenceHealth `json:"sequence,omitempty"`

	// IngressBps is the sum of the publishers' measured receive rate at their tunnels — what
	// the network is taking in for this group. EgressBps is the sum over subscribers of what
	// their tunnels send out. Both are upper bounds when TrafficAmbiguous is set.
	//
	// The fleet page renders ingress only. EgressBps scales with fan-out and, because the
	// counters are per tunnel, a subscriber on several groups adds the same figure to each of
	// them: on mainnet the perps groups read ~878 Mbps of egress against 3.6 Mbps of ingress,
	// which is the sports traffic on the same tunnels rather than anything about this group. It
	// stays in the payload as a raw measurement for API callers and is deliberately not on
	// screen, where a 240x number invites exactly the wrong reading.
	IngressBps float64 `json:"ingress_bps"`
	EgressBps  float64 `json:"egress_bps"`

	// PublishersMultiGroup is how many of this group's publishers also publish elsewhere from
	// the same account, which is what makes the rate un-attributable. See the file header.
	PublishersMultiGroup int  `json:"publishers_multi_group"`
	TrafficAmbiguous     bool `json:"traffic_ambiguous"`

	// ObservedAt is the newest rate bucket backing any number above; nil when no member has a
	// bucket in the window.
	//
	// The age is NOT precomputed here, and that matters on a cache-first endpoint: an age taken
	// at fetch time is served unchanged for the rest of the refresh interval — 30s by default,
	// up to ten minutes by env — so it silently understates itself by up to one interval. The
	// page subtracts from GeneratedAt (for judgements about the data) or from wall clock (for
	// "is it alive now"), the way kalshi_l2_coverage.go leaves last_seen to the page.
	ObservedAt *time.Time `json:"observed_at,omitempty"`

	// LastHeard is the newest application-plane observation attributed to this group: a message
	// a recording node actually received, not a device counter. Where it exists it can answer
	// "did anything arrive in the last few seconds", which the five-minute rollups above
	// structurally cannot. Nil when no capture covers the group, which is most of them.
	LastHeard *time.Time `json:"last_heard,omitempty"`

	// LastHeardTable names the proxied table behind the timestamp, so a reader can tell a Kalshi
	// observation from a shred race. Empty when LastHeard is nil.
	LastHeardTable string `json:"last_heard_table,omitempty"`

	// LastHeardCaptureSources is how many capture sources were folded into that one timestamp.
	// Above 1 it is a max over them and a single dead one does NOT move it — the Kalshi sports
	// groups carry dozens of league capture sources each. The per-capture-source view is
	// /dz/kalshi/l2.
	LastHeardCaptureSources int `json:"last_heard_capture_sources,omitempty"`

	// Silent is the page's headline signal: the group has publishers and not one of them moved
	// a byte in the freshest bucket. False when publishers are active OR when there is no rate
	// data at all — "we cannot see" is not "it is dead", and conflating them would fire on
	// every telemetry gap.
	Silent bool `json:"silent"`

	// Health is the feed's verdict, and it is a traffic verdict over two per-member checks:
	// every publisher clears edgeMulticastPublisherFloorBps, and every recording node on the
	// group hears a share of the feed comparable with its peers. 'healthy' / 'thin' / 'skewed' /
	// 'silent' / 'unknown' / "" (no publishers) — see edgeMulticastVerdict.
	//
	// It is deliberately NOT the control-plane reconciliation verdict. That one is a worst-of
	// over every member, and on this page it is red permanently for two independent reasons:
	// a single publisher whose (S,G) is missing from one device's mroute snapshot flips to
	// 'unhealthy' for a cycle (6 of 767 on edge-solana-shreds at any given moment, always a
	// different six), and every group carries customers with BGP down, which ranks 'disconnected'
	// over 'healthy'. Measured on mainnet: zero of the nine edge groups could ever read healthy.
	// A badge that is always red says nothing about the feed it is supposed to describe.
	Health string `json:"health"`

	// HealthCounts is the control-plane reconciliation breakdown for the group's members. It no
	// longer sets Health — it is the drill-down the badge's tooltip shows and the Health tab
	// paginates, kept so a reader who wants per-member state can see it is there.
	HealthCounts MulticastHealthStatusCounts `json:"health_counts"`
}

// EdgeMulticastService is one product — a feed family — and the groups behind it, one row per
// plane.
type EdgeMulticastService struct {
	Code string `json:"code"`

	// Managed is false for the unclaimed bucket, whose groups no feed row claims.
	Managed bool `json:"managed"`

	// MetroCount is how many distinct metros the family is sold in, counted across every plane
	// and every catalog row.
	MetroCount int                  `json:"metro_count"`
	Groups     []EdgeMulticastGroup `json:"groups"`
}

// EdgeMulticastResponse is the API response.
type EdgeMulticastResponse struct {
	GeneratedAt time.Time `json:"generated_at"`

	// RateGrainMinutes is the width of the buckets the rates come from. On screen next to the
	// observed age it is what stops a five-minute average being read as an instantaneous rate.
	RateGrainMinutes int `json:"rate_grain_minutes"`

	// PublisherFloorBps is the per-publisher floor the verdict applies, carried in the payload
	// so the UI states the threshold it is judging against instead of hardcoding a second copy
	// of it that can drift.
	PublisherFloorBps int `json:"publisher_floor_bps"`

	// LastHeardAvailable is false when no application-plane table was queryable at all — the
	// normal state in local dev, where the proxied feeds and shredder tables do not exist. The
	// UI drops the column rather than rendering a screenful of blanks.
	LastHeardAvailable bool `json:"last_heard_available"`

	// SequenceAsOf is when the sequence-counter numbers were computed, which is NOT GeneratedAt:
	// they are folded from the L2 coverage refresher's cache and are up to one refresher
	// interval (ten minutes) older than the rest of the payload. Nil when no group has any.
	// Carried so the column can age itself instead of borrowing this payload's freshness.
	SequenceAsOf *time.Time `json:"sequence_as_of,omitempty"`

	Services []EdgeMulticastService `json:"services"`
}

// edgeMulticastRateGrainMinutes mirrors the bucket width of device_interface_rollup_5m. It is
// descriptive: changing it here changes what the page claims, not what the data is.
const edgeMulticastRateGrainMinutes = 5

// GetEdgeMulticast serves the edge multicast overview.
func (a *API) GetEdgeMulticast(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), EdgeMulticastCacheKey); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	resp, err := a.FetchEdgeMulticastData(ctx)
	if err != nil {
		logError("edge multicast query error", "error", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}

// edgeMulticastFeedGroups maps group pk → feed families claiming it, plus each family's metro
// count. Families, not codes: see edgeMulticastFeedFamily.
type edgeMulticastFeedGroups struct {
	byGroup     map[string][]string
	metroCounts map[string]int
}

// edgeMulticastMemberSplit is the ledger-side count for one side of a group, already split by
// classification.
type edgeMulticastMemberSplit struct {
	total     int
	recorders int
	probes    int
	asserted  int
	derived   int
}

// edgeMulticastMembership is the ledger-side count for one group.
type edgeMulticastMembership struct {
	publishers           edgeMulticastMemberSplit
	subscribers          edgeMulticastMemberSplit
	publishersMultiGroup int
}

// edgeMulticastRates is the rate-view side for one group.
type edgeMulticastRates struct {
	pubActive  int
	pubIdle    int
	subActive  int
	subIdle    int
	ingressBps float64
	egressBps  float64
	newest     *time.Time
	counts     MulticastHealthStatusCounts
}

// FetchEdgeMulticastData builds the fleet overview. Four queries, none of them per group: the
// catalog, the ledger membership, the rate/health rollup, and the per-publisher read. Adding a
// group or a feed changes no code and adds no round-trip.
func (a *API) FetchEdgeMulticastData(ctx context.Context) (*EdgeMulticastResponse, error) {
	// Loaded before the membership query and allowed to fail it: serving the page with the
	// override table unread would silently recount every asserted recorder as a customer,
	// which is worse than serving the last good cache.
	classes, err := a.loadMulticastMemberClasses(ctx)
	if err != nil {
		return nil, err
	}
	feeds, err := a.queryEdgeMulticastFeeds(ctx)
	if err != nil {
		return nil, err
	}
	groups, err := a.queryEdgeMulticastGroups(ctx)
	if err != nil {
		return nil, err
	}
	membership, err := a.queryEdgeMulticastMembership(ctx, classes)
	if err != nil {
		return nil, err
	}
	rates, err := a.queryEdgeMulticastRates(ctx)
	if err != nil {
		return nil, err
	}
	groupPKs := make([]string, 0, len(groups))
	for _, g := range groups {
		groupPKs = append(groupPKs, g.PK)
	}
	publisherLines, err := a.queryEdgeMulticastPublisherLines(ctx, groupPKs, classes)
	if err != nil {
		return nil, err
	}

	// Optional, and deliberately non-fatal. Unlike the three queries above, this one reads
	// proxied tables that are legitimately absent in local dev and may blip independently of
	// the lake database. The rest of the payload is complete and correct without it, so a
	// failure costs one column instead of the page — and the worker must still be able to
	// cache the good part. WARN, never ERROR: nothing here is worth paging on.
	captureSources := newEdgeMulticastCaptureSourceMap(groups)
	lastHeard, lastHeardAvailable, err := a.queryEdgeMulticastLastHeard(ctx, captureSources)
	if err != nil {
		slog.Warn("edge multicast app-plane last-heard unavailable", "error", err)
		lastHeard, lastHeardAvailable = nil, false
	}

	// Same contract, and this one reads no ClickHouse at all: a miss costs the column.
	sequence, sequenceAsOf, err := a.edgeMulticastSequenceHealth(ctx, captureSources)
	if err != nil {
		slog.Warn("edge multicast sequence health unavailable", "error", err)
		sequence = nil
	}

	now := time.Now().UTC()
	resp := &EdgeMulticastResponse{
		GeneratedAt:        now,
		RateGrainMinutes:   edgeMulticastRateGrainMinutes,
		PublisherFloorBps:  edgeMulticastPublisherFloorBps,
		LastHeardAvailable: lastHeardAvailable,
		Services:           []EdgeMulticastService{},
	}
	if !sequenceAsOf.IsZero() {
		at := sequenceAsOf
		resp.SequenceAsOf = &at
	}

	byService := map[string][]EdgeMulticastGroup{}
	for _, g := range groups {
		row := buildEdgeMulticastGroup(g, membership[g.PK], rates[g.PK], lastHeard[g.PK], publisherLines[g.PK], sequence[g.PK])
		codes := feeds.byGroup[g.PK]
		if len(codes) == 0 {
			codes = []string{edgeMulticastUnclaimedService}
		}
		// A group shared by several feeds (the tob/mbp pair of one exchange does not share,
		// but nothing stops it) is listed under each of them. Duplicating the row is right:
		// each service's section has to be complete on its own to be read on its own.
		for _, code := range codes {
			byService[code] = append(byService[code], row)
		}
	}

	for code, rows := range byService {
		sort.Slice(rows, func(i, j int) bool { return rows[i].Code < rows[j].Code })
		resp.Services = append(resp.Services, EdgeMulticastService{
			Code:       code,
			Managed:    code != edgeMulticastUnclaimedService,
			MetroCount: feeds.metroCounts[code],
			Groups:     rows,
		})
	}

	// Feed-backed services first, alphabetically; the unclaimed bucket last regardless of name.
	sort.Slice(resp.Services, func(i, j int) bool {
		if resp.Services[i].Managed != resp.Services[j].Managed {
			return resp.Services[i].Managed
		}
		return resp.Services[i].Code < resp.Services[j].Code
	})
	return resp, nil
}

// buildEdgeMulticastGroup merges the four inputs for one group.
//
// Totals come from the ledger and the breakdown from the rate view, so Unknown is computed as
// the remainder rather than read: a member the view dropped (no health row at all) is exactly
// as unknown as one it marked 'no_data', and folding both into the remainder keeps the parts
// summing to Total whatever the view does.
func buildEdgeMulticastGroup(g MulticastDeliveryGroup, m edgeMulticastMembership, r edgeMulticastRates, lh edgeMulticastLastHeard, lines []EdgeMulticastPublisher, sequence *EdgeMulticastSequenceHealth) EdgeMulticastGroup {
	out := EdgeMulticastGroup{
		PK:                   g.PK,
		Code:                 g.Code,
		MulticastIP:          g.MulticastIP,
		Status:               g.Status,
		MaxBandwidth:         g.MaxBandwidth,
		Plane:                edgeMulticastPlaneFor(g.Code),
		Publishers:           edgeMulticastRoleCounts(m.publishers, r.pubActive, r.pubIdle),
		Subscribers:          edgeMulticastRoleCounts(m.subscribers, r.subActive, r.subIdle),
		IngressBps:           r.ingressBps,
		EgressBps:            r.egressBps,
		PublishersMultiGroup: m.publishersMultiGroup,
		TrafficAmbiguous:     m.publishersMultiGroup > 0,
		ObservedAt:           r.newest,
		HealthCounts:         r.counts,
	}
	out.Publishers.Unknown = max(0, out.Publishers.Total-out.Publishers.Active-out.Publishers.Idle)
	out.Subscribers.Unknown = max(0, out.Subscribers.Total-out.Subscribers.Active-out.Subscribers.Idle)

	if !lh.at.IsZero() {
		at := lh.at
		out.LastHeard = &at
		out.LastHeardTable = lh.table
		out.LastHeardCaptureSources = lh.captureSources
	}

	// The floor check is tallied over every publisher and the list is truncated afterwards, so
	// the verdict is the same whatever the cap is set to.
	stats := edgeMulticastPublisherStatsOf(lines)
	out.PublisherLinesTotal = len(lines)
	out.PublishersBelowFloor = stats.belowFloor()
	out.PublishersPublishing = stats.publishing
	// Also before the truncation: each recorded series is reported on the line that emitted it,
	// and the roll-up counts publishers rather than series.
	out.Sequence = sequence
	attachEdgeMulticastSequenceHealth(lines, out.Sequence)
	out.PublisherLines = lines
	if len(out.PublisherLines) > edgeMulticastPublisherLineCap {
		out.PublisherLines = out.PublisherLines[:edgeMulticastPublisherLineCap]
	}

	out.CaptureNodes = edgeMulticastCaptureNodes(lh.nodeObs())
	out.CaptureNodesLagging = edgeMulticastLaggingNodes(out.CaptureNodes)

	// Silent stays sourced from the counters and is NOT crossed with LastHeard. The app-plane
	// signal is receive-side: a recorder outage and a publisher outage produce the same absence,
	// so letting it set Silent would report a dead recorder as a dead feed.
	//
	// Silent needs evidence, not absence of it: publishers exist, at least one of them has a
	// rate row in the window, and none of those rows carried traffic.
	out.Silent = out.Publishers.Total > 0 && out.Publishers.Active == 0 && out.Publishers.Idle > 0

	// After the Unknown remainders above: the verdict partitions the ledger's publishers, not
	// just the ones the rate view had a row for.
	out.Health = edgeMulticastVerdict(out.Publishers, stats, out.CaptureNodesLagging)
	return out
}

// edgeMulticastRoleCounts assembles one side of a group from its ledger split and its measured
// activity. Customers is the remainder rather than a counted value, so the three classes cannot
// drift out of sync with Total whatever the classification tiers do.
func edgeMulticastRoleCounts(split edgeMulticastMemberSplit, active, idle int) EdgeMulticastRoleCounts {
	return EdgeMulticastRoleCounts{
		Total:          split.total,
		Active:         active,
		Idle:           idle,
		Recorders:      split.recorders,
		InternalProbes: split.probes,
		Customers:      max(0, split.total-split.recorders-split.probes),
		ClassAsserted:  split.asserted,
		ClassDerived:   split.derived,
	}
}

// edgeMulticastVerdict is the group's verdict, taken over two per-member checks and nothing else:
// every publisher is above the floor, and every recording node hears its share of the feed.
//
//	silent  — publishers were measured and not one of them moved a byte (the actionable state)
//	thin    — at least one publisher is below edgeMulticastPublisherFloorBps, idle ones included
//	skewed  — publishers are all above the floor, but a recording node is far under its peers
//	unknown — publishers exist and nothing measured any of them: a monitoring gap
//	healthy — every measured publisher clears the floor and no node is lagging
//	""      — no publishers at all, e.g. a group provisioned before anyone joined it
//
// Ordered worst-first on purpose, and the two halves are not symmetric. A failed floor check is
// a statement about the feed's publishers and outranks a parity gap, which is a statement about
// one receiver; reporting 'skewed' while a publisher sits at zero would name the smaller problem.
//
// Two things it deliberately does not do. It does not consult SUBSCRIBERS: one that receives
// nothing is a customer who stopped listening or a customer with BGP down, neither of which is a
// fault in the feed, and letting either paint the row is how the always-red badge this replaced
// came about. And it does not let an UNMEASURED publisher force a verdict — with some publishers
// measured and above the floor, a peer with no counter row leaves the feed healthy and shows up
// as 'unknown' on its own line. A telemetry gap on one device is not a fault in the feed, and
// treating it as one puts every feed in amber whenever the rollup pipeline hiccups.
func edgeMulticastVerdict(pubs EdgeMulticastRoleCounts, stats edgeMulticastPublisherStats, laggingNodes int) string {
	switch {
	case pubs.Total == 0:
		return ""
	case stats.measured() == 0:
		return "unknown"
	case stats.publishing == 0 && stats.thin == 0:
		return "silent"
	case stats.belowFloor() > 0:
		return "thin"
	case laggingNodes > 0:
		return "skewed"
	}
	return "healthy"
}

// queryEdgeMulticastFeeds reads the feed catalog: which groups each feed carries and in how
// many metros it is sold. dz_feeds_current holds one row per (feed, metro).
//
// Both aggregates are taken per feed FAMILY — the feed code with its plane suffix stripped, so
// the two planes of one product form one section — across every one of its metro rows, and that
// matters in both directions. The group set is a UNION because a feed's rows do not all list the
// same groups — solana-shreds-full names three groups in some metros and five across the catalog,
// so reading any one row would under-report it. The metro count is a DISTINCT over metro_pk for
// the same reason in reverse: a feed sold in 30 metros with five groups must read 30, not 150,
// and two planes sold in overlapping metros are counted once.
//
// Neither aggregate goes through arrayJoin, and that is deliberate: arrayJoin over an empty array
// drops the row, so a catalog row whose `groups` is '[]' — a feed sold in a metro where the group
// is not provisioned yet — would vanish before being counted and the section header would claim
// fewer metros than the catalog sells. That is the same "the feed comes before the group" case the
// prefix scoping exists for, in the other direction. Flattening the arrays inside the aggregate
// keeps every row in the metro count while still unioning the groups.
func (a *API) queryEdgeMulticastFeeds(ctx context.Context) (edgeMulticastFeedGroups, error) {
	out := edgeMulticastFeedGroups{
		byGroup:     map[string][]string{},
		metroCounts: map[string]int{},
	}
	query := fmt.Sprintf(`
		SELECT
			feed_family,
			uniqExact(metro_pk) AS metro_count,
			arrayDistinct(arrayFilter(x -> x != '', arrayFlatten(groupArray(group_pks)))) AS group_pks
		FROM (
			SELECT
				-- The plane suffix is stripped here rather than in Go so both aggregates below
				-- are already family-scoped. The pattern comes from edgeMulticastPlanes.
				replaceRegexpOne(code, '%s', '') AS feed_family,
				metro_pk,
				JSONExtract(groups, 'Array(String)') AS group_pks
			FROM dz_feeds_current
		)
		GROUP BY feed_family
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`, edgeMulticastPlaneSuffixSQL())
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("edge_multicast_feeds", time.Since(start), err)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var feedFamily string
		var metroCount uint64
		var groupPKs []string
		if err := rows.Scan(&feedFamily, &metroCount, &groupPKs); err != nil {
			return out, err
		}
		out.metroCounts[feedFamily] = int(metroCount)
		for _, groupPK := range groupPKs {
			out.byGroup[groupPK] = append(out.byGroup[groupPK], feedFamily)
		}
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	for _, codes := range out.byGroup {
		sort.Strings(codes)
	}
	return out, nil
}

// queryEdgeMulticastGroups reads the Edge product's multicast groups: every group whose ledger
// code carries the product prefix, whether or not a feed claims it. See
// edgeMulticastGroupCodePrefix for why the prefix and not the feed catalog decides.
func (a *API) queryEdgeMulticastGroups(ctx context.Context) ([]MulticastDeliveryGroup, error) {
	query := `
		SELECT
			pk,
			COALESCE(code, '') AS code,
			COALESCE(multicast_ip, '') AS multicast_ip,
			COALESCE(max_bandwidth, 0) AS max_bandwidth,
			COALESCE(status, '') AS status
		FROM dz_multicast_groups_current
		WHERE startsWith(COALESCE(code, ''), ?)
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query, edgeMulticastGroupCodePrefix)
	metrics.RecordClickHouseQuery("edge_multicast_groups", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MulticastDeliveryGroup
	for rows.Next() {
		var g MulticastDeliveryGroup
		if err := rows.Scan(&g.PK, &g.Code, &g.MulticastIP, &g.MaxBandwidth, &g.Status); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// queryEdgeMulticastMembership counts each group's ledger-side membership and the two facts the
// rate view cannot supply: how each member is classified, and how many publishers spread one
// tunnel across several groups.
//
// The classification sets arrive already resolved (see edge_multicast_class.go) and are inlined
// as IP lists. Inlining rather than parameterising keeps this one round-trip for the whole
// fleet; every address in the lists is either a repo literal or has passed net.ParseIP, and
// multicastMemberIPPredicate re-checks before rendering.
func (a *API) queryEdgeMulticastMembership(ctx context.Context, classes multicastMemberClasses) (map[string]edgeMulticastMembership, error) {
	isRecorder := multicastMemberIPPredicate("client_ip", classes.recorderIPs)
	isProbe := multicastMemberIPPredicate("client_ip", classes.probeIPs)
	isAsserted := multicastMemberIPPredicate("client_ip", classes.assertedIPs)
	isDerived := multicastMemberIPPredicate("client_ip", classes.derivedIPs)

	query := fmt.Sprintf(`
		WITH membership AS (
			SELECT
				arrayJoin(JSONExtract(publishers, 'Array(String)')) AS group_pk,
				'P' AS role,
				client_ip,
				length(arrayDistinct(arrayConcat(
					JSONExtract(publishers, 'Array(String)'),
					JSONExtract(subscribers, 'Array(String)')
				))) AS group_span
			FROM dz_users_current
			WHERE status = 'activated' AND kind = 'multicast'
			UNION ALL
			SELECT
				arrayJoin(JSONExtract(subscribers, 'Array(String)')) AS group_pk,
				'S' AS role,
				client_ip,
				length(arrayDistinct(arrayConcat(
					JSONExtract(publishers, 'Array(String)'),
					JSONExtract(subscribers, 'Array(String)')
				))) AS group_span
			FROM dz_users_current
			WHERE status = 'activated' AND kind = 'multicast'
		)
		SELECT
			group_pk,
			countIf(role = 'P') AS pub_total,
			countIf(role = 'P' AND %[1]s) AS pub_recorders,
			countIf(role = 'P' AND %[2]s) AS pub_probes,
			countIf(role = 'P' AND %[3]s) AS pub_asserted,
			countIf(role = 'P' AND %[4]s) AS pub_derived,
			countIf(role = 'S') AS sub_total,
			countIf(role = 'S' AND %[1]s) AS sub_recorders,
			countIf(role = 'S' AND %[2]s) AS sub_probes,
			countIf(role = 'S' AND %[3]s) AS sub_asserted,
			countIf(role = 'S' AND %[4]s) AS sub_derived,
			countIf(role = 'P' AND group_span > 1) AS publishers_multi_group
		FROM membership
		GROUP BY group_pk
		SETTINGS max_execution_time = 30, timeout_before_checking_execution_speed = 0
	`, isRecorder, isProbe, isAsserted, isDerived)

	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("edge_multicast_membership", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]edgeMulticastMembership{}
	for rows.Next() {
		var groupPK string
		var pub, sub [5]uint64
		var multiGroup uint64
		if err := rows.Scan(&groupPK,
			&pub[0], &pub[1], &pub[2], &pub[3], &pub[4],
			&sub[0], &sub[1], &sub[2], &sub[3], &sub[4],
			&multiGroup); err != nil {
			return nil, err
		}
		out[groupPK] = edgeMulticastMembership{
			publishers:           edgeMulticastSplitFrom(pub),
			subscribers:          edgeMulticastSplitFrom(sub),
			publishersMultiGroup: int(multiGroup),
		}
	}
	return out, rows.Err()
}

// edgeMulticastSplitFrom maps the (total, recorders, probes, asserted, derived) tuple the query
// returns in that fixed order.
func edgeMulticastSplitFrom(v [5]uint64) edgeMulticastMemberSplit {
	return edgeMulticastMemberSplit{
		total:     int(v[0]),
		recorders: int(v[1]),
		probes:    int(v[2]),
		asserted:  int(v[3]),
		derived:   int(v[4]),
	}
}

// queryEdgeMulticastRates rolls the per-user rate view up per group.
//
// One pass over health_multicast_user_rate for the whole fleet (~280ms on mainnet), grouped by
// mode so publisher and subscriber rates never mix.
//
// A P+S member counts on the SUBSCRIBER side only, and that is a correctness fix rather than a
// simplification. The view hands out one rate per row and picks the direction by mode: 'P' reads
// ur_max_in_bps (what the member sends), 'S' and 'P+S' read ur_max_out_bps (what its tunnel
// sends out). So a P+S row's rate says nothing about whether that member is publishing. Counting
// it on the publisher side inverted the page's headline in both directions: RPF means a member
// does not receive its own group back, so a group whose only publisher also subscribes reads
// max_out = 0 — the view marks it 'group_idle' — and the row rendered Active=0, Idle=1, hence
// Silent and a red feed while the publisher was sending; a P+S member with egress but no publish
// traffic rendered healthy. Excluded from the tally, those members fall into Publishers.Unknown,
// which is the true statement: nothing here measured their send side.
//
// IngressBps stays sourced from mode 'P' alone for the same reason. A group published only by
// P+S members therefore reports 0 ingress and Unknown publishers — visibly unmeasured, rather
// than confidently wrong. Carrying the publisher rate alongside observed_bps_5m in the view is
// what would fix it properly, and that is an indexer change.
func (a *API) queryEdgeMulticastRates(ctx context.Context) (map[string]edgeMulticastRates, error) {
	query := `
		SELECT
			multicast_group_pk,
			mode,
			health_status,
			count() AS users,
			countIf(observed_bps_5m > 0) AS active,
			countIf(observed_bps_5m = 0) AS idle,
			sum(ifNull(observed_bps_5m, 0)) AS bps,
			max(rate_bucket_ts) AS newest_bucket
		FROM health_multicast_user_rate
		GROUP BY multicast_group_pk, mode, health_status
		SETTINGS max_execution_time = 45, timeout_before_checking_execution_speed = 0
	`
	start := time.Now()
	rows, err := a.envDB(ctx).Query(ctx, query)
	metrics.RecordClickHouseQuery("edge_multicast_rates", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]edgeMulticastRates{}
	for rows.Next() {
		var groupPK, mode, health string
		var users, active, idle uint64
		var bps float64
		var newest *time.Time
		if err := rows.Scan(&groupPK, &mode, &health, &users, &active, &idle, &bps, &newest); err != nil {
			return nil, err
		}

		agg := out[groupPK]
		if mode == "P" {
			agg.pubActive += int(active)
			agg.pubIdle += int(idle)
		}
		if mode == "S" || mode == "P+S" {
			agg.subActive += int(active)
			agg.subIdle += int(idle)
		}
		switch mode {
		case "P":
			agg.ingressBps += bps
		case "S", "P+S":
			agg.egressBps += bps
		}
		addStatusCount(&agg.counts, health, users)
		if newest != nil && (agg.newest == nil || newest.After(*agg.newest)) {
			t := newest.UTC()
			agg.newest = &t
		}
		out[groupPK] = agg
	}
	return out, rows.Err()
}
