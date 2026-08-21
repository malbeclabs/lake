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
// The question this page exists for is the fleet one — "is any lane silent?" — which nobody can
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
// aggregate runs three grouped queries over health_multicast_user_rate and dz_users_current
// (~350ms together on mainnet), which is cheap enough to serve live but wasteful to repeat per
// viewer on a page that refreshes on a timer.
const EdgeMulticastCacheKey = "edge_multicast"

// edgeMulticastGroupCodePrefix is what makes a multicast group part of the Edge product: its
// ledger code. Membership in the product is a naming convention (`edge-<venue>-<product>-<plane>`
// for the market-data lanes, `edge-solana-*` for shreds), not something the feed catalog decides.
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
const edgeMulticastGroupCodePrefix = "edge"

// edgeMulticastPlanes maps the plane suffix a lane's ledger code carries to its display label.
// A lane is one plane of one product: market-by-price, market-by-order, or top-of-book. The
// suffix is the only place that distinction is recorded, on both the group code
// (edge-kalshi-perps-mbp) and the feed code (kalshi-perps-mbp).
//
// mbo has no group on mainnet yet and is listed anyway: the suffix set is what the feed catalog
// and the capture source ids are built from, so a plane missing here would silently collapse into
// its product's section with a blank column instead of showing up as a lane of its own.
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

	// IngressBps is the sum of the publishers' measured receive rate at their tunnels — what
	// the network is taking in for this group. EgressBps is the sum over subscribers of what
	// their tunnels send out, so it scales with fan-out and is many times ingress on a healthy
	// group. Both are upper bounds when TrafficAmbiguous is set.
	IngressBps float64 `json:"ingress_bps"`
	EgressBps  float64 `json:"egress_bps"`

	// PublishersMultiGroup is how many of this group's publishers also publish elsewhere from
	// the same account, which is what makes the rate un-attributable. See the file header.
	PublishersMultiGroup int  `json:"publishers_multi_group"`
	TrafficAmbiguous     bool `json:"traffic_ambiguous"`

	// ObservedAt is the newest rate bucket backing any number above; nil when no member has a
	// bucket in the window. ObservedAgeSeconds is its age at response time — the honest answer
	// to "how live is this?", which the UI shows next to every rate.
	ObservedAt         *time.Time `json:"observed_at,omitempty"`
	ObservedAgeSeconds *float64   `json:"observed_age_seconds,omitempty"`

	// LastHeard is the newest application-plane observation attributed to this group: a message
	// a recording node actually received, not a device counter. Where it exists it can answer
	// "did anything arrive in the last few seconds", which the five-minute rollups above
	// structurally cannot. Nil when no capture covers the group, which is most of them.
	LastHeard        *time.Time `json:"last_heard,omitempty"`
	LastHeardAgeSecs *float64   `json:"last_heard_age_seconds,omitempty"`

	// LastHeardSource names the table behind the timestamp, so a reader can tell a Kalshi
	// observation from a shred race. Empty when LastHeard is nil.
	LastHeardSource string `json:"last_heard_source,omitempty"`

	// LastHeardLanes is how many capture sources were folded into that one timestamp. Above 1
	// it is a max over lanes and a single dead lane does NOT move it — the Kalshi sports groups
	// carry dozens of league lanes each. The per-lane view is /dz/kalshi/l2.
	LastHeardLanes int `json:"last_heard_lanes,omitempty"`

	// Silent is the page's headline signal: the group has publishers and not one of them moved
	// a byte in the freshest bucket. False when publishers are active OR when there is no rate
	// data at all — "we cannot see" is not "it is dead", and conflating them would fire on
	// every telemetry gap.
	Silent bool `json:"silent"`

	// Health is the worst per-user verdict present, ranked by the same severity order the
	// per-group Health tab sorts by (unhealthy → degraded → unknown → disconnected → healthy),
	// with HealthCounts carrying the breakdown. Deliberately the existing scale rather than a
	// new one, so a group cannot read healthy here and unhealthy one click deeper.
	Health       string                      `json:"health"`
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

	// LastHeardAvailable is false when no application-plane table was queryable at all — the
	// normal state in local dev, where the proxied feeds and shredder tables do not exist. The
	// UI drops the column rather than rendering a screenful of blanks.
	LastHeardAvailable bool `json:"last_heard_available"`

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

// FetchEdgeMulticastData builds the fleet overview. Three queries, none of them per group: the
// catalog, the ledger membership, and the rate/health rollup. Adding a group or a feed changes
// no code and adds no round-trip.
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

	// Optional, and deliberately non-fatal. Unlike the three queries above, this one reads
	// proxied tables that are legitimately absent in local dev and may blip independently of
	// the lake database. The rest of the payload is complete and correct without it, so a
	// failure costs one column instead of the page — and the worker must still be able to
	// cache the good part. WARN, never ERROR: nothing here is worth paging on.
	lastHeard, lastHeardAvailable, err := a.queryEdgeMulticastLastHeard(ctx, groups)
	if err != nil {
		slog.Warn("edge multicast app-plane last-heard unavailable", "error", err)
		lastHeard, lastHeardAvailable = nil, false
	}

	now := time.Now().UTC()
	resp := &EdgeMulticastResponse{
		GeneratedAt:        now,
		RateGrainMinutes:   edgeMulticastRateGrainMinutes,
		LastHeardAvailable: lastHeardAvailable,
		Services:           []EdgeMulticastService{},
	}

	byService := map[string][]EdgeMulticastGroup{}
	for _, g := range groups {
		row := buildEdgeMulticastGroup(g, membership[g.PK], rates[g.PK], lastHeard[g.PK], now)
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

// buildEdgeMulticastGroup merges the three sources for one group.
//
// Totals come from the ledger and the breakdown from the rate view, so Unknown is computed as
// the remainder rather than read: a member the view dropped (no health row at all) is exactly
// as unknown as one it marked 'no_data', and folding both into the remainder keeps the parts
// summing to Total whatever the view does.
func buildEdgeMulticastGroup(g MulticastDeliveryGroup, m edgeMulticastMembership, r edgeMulticastRates, lh edgeMulticastLastHeard, now time.Time) EdgeMulticastGroup {
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
		Health:               worstEdgeMulticastHealth(r.counts),
	}
	out.Publishers.Unknown = max(0, out.Publishers.Total-out.Publishers.Active-out.Publishers.Idle)
	out.Subscribers.Unknown = max(0, out.Subscribers.Total-out.Subscribers.Active-out.Subscribers.Idle)

	if r.newest != nil {
		age := now.Sub(*r.newest).Seconds()
		out.ObservedAgeSeconds = &age
	}
	if !lh.at.IsZero() {
		at := lh.at
		age := now.Sub(at).Seconds()
		out.LastHeard = &at
		out.LastHeardAgeSecs = &age
		out.LastHeardSource = lh.source
		out.LastHeardLanes = lh.lanes
	}

	// Silent stays sourced from the counters and is NOT crossed with LastHeard. The app-plane
	// signal is receive-side: a recorder outage and a publisher outage produce the same absence,
	// so letting it set Silent would report a dead recorder as a dead lane.
	//
	// Silent needs evidence, not absence of it: publishers exist, at least one of them has a
	// rate row in the window, and none of those rows carried traffic.
	out.Silent = out.Publishers.Total > 0 && out.Publishers.Active == 0 && out.Publishers.Idle > 0
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

// worstEdgeMulticastHealth collapses the per-user verdicts to the most actionable one present,
// in the order healthStatusSeverityOrderSQL ranks them. Empty when the group has no rows.
func worstEdgeMulticastHealth(c MulticastHealthStatusCounts) string {
	switch {
	case c.Unhealthy > 0:
		return "unhealthy"
	case c.Degraded > 0:
		return "degraded"
	case c.Unknown > 0:
		return "unknown"
	case c.Disconnected > 0:
		return "disconnected"
	case c.Healthy > 0:
		return "healthy"
	}
	return ""
}

// queryEdgeMulticastFeeds reads the feed catalog: which groups each feed carries and in how
// many metros it is sold. dz_feeds_current holds one row per (feed, metro).
//
// Both aggregates are taken per feed FAMILY — the feed code with its plane suffix stripped, so
// the two planes of one product form one section — across every one of its metro rows, and that
// matters in both directions. The group set is a UNION because a feed's rows do not all list the same
// groups — solana-shreds-full names three groups in some metros and five across the catalog, so
// reading any one row would under-report it. The metro count is a DISTINCT over metro_pk for the
// same reason in reverse: arrayJoin multiplies each metro row by its group count, so counting
// anything but distinct metros would multiply a 30-metro feed by its five groups and claim 90.
// Taking it over the family also means two planes sold in overlapping metros are counted once.
func (a *API) queryEdgeMulticastFeeds(ctx context.Context) (edgeMulticastFeedGroups, error) {
	out := edgeMulticastFeedGroups{
		byGroup:     map[string][]string{},
		metroCounts: map[string]int{},
	}
	query := fmt.Sprintf(`
		SELECT
			feed_family,
			uniqExact(metro_pk) AS metro_count,
			groupUniqArray(group_pk) AS group_pks
		FROM (
			SELECT
				-- The plane suffix is stripped here rather than in Go so both aggregates below
				-- are already family-scoped. The pattern comes from edgeMulticastPlanes.
				replaceRegexpOne(code, '%s', '') AS feed_family,
				metro_pk,
				arrayJoin(JSONExtract(groups, 'Array(String)')) AS group_pk
			FROM dz_feeds_current
		)
		WHERE group_pk != ''
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
// mode so publisher and subscriber rates never mix. A P+S user counts on both sides; its rate is
// the view's reconciled side (egress), so it adds to EgressBps only — attributing it to ingress
// as well would double-count the one measurement it has.
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
		if mode == "P" || mode == "P+S" {
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
