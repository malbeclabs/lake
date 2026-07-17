package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

// ChangeRef points a finding back at the plan change that caused it. Seq is
// stable within a plan; Label is the human identity from ref_snapshot.
type ChangeRef struct {
	Seq    int        `json:"seq"`
	OpType PlanOpType `json:"op_type"`
	Label  string     `json:"label"`
}

// Severity ranks a finding for the impact panel (SC-4: "high" | "medium" | "low").
type Severity string

const (
	SeverityHigh   Severity = "high"
	SeverityMedium Severity = "medium"
	SeverityLow    Severity = "low"
)

// PartitionIssue reports a connectivity regression: an isolated device, an
// isolated metro, or a metro that dropped to a single exit path. The SC-4 wire
// fields (severity, entity_type, entity_pk, entity_code, description, caused_by)
// are what Phase 5 renders; Type/MetroCode are extra internal detail.
type PartitionIssue struct {
	Severity    Severity    `json:"severity"`
	EntityType  string      `json:"entity_type"` // device | metro
	EntityPK    string      `json:"entity_pk"`
	EntityCode  string      `json:"entity_code"`
	Description string      `json:"description"`
	CausedBy    []ChangeRef `json:"caused_by"`
	Type        string      `json:"type"` // device_isolated | metro_isolated | single_exit_metro
	MetroCode   string      `json:"metro_code,omitempty"`
}

// changeFootprint is the set of entities a single change touches, used for
// attribution and cross-plan overlap.
type changeFootprint struct {
	Seq        int
	OpType     PlanOpType
	Label      string
	LinkPKs    map[string]bool
	DevicePKs  map[string]bool
	MetroCodes map[string]bool
}

// changeFootprints computes what each change touches, resolving existing
// entities against the baseline graph.
func changeFootprints(baseline *kspGraph, changes []PlanChange) map[int]changeFootprint {
	out := make(map[int]changeFootprint, len(changes))
	for _, c := range changes {
		p, _ := decodePlanChangePayload(c)
		fp := changeFootprint{
			Seq: c.Seq, OpType: c.OpType, Label: refSnapshotLabel(c),
			LinkPKs: map[string]bool{}, DevicePKs: map[string]bool{}, MetroCodes: map[string]bool{},
		}
		addDevMetro := func(key string) {
			if key == "" {
				return
			}
			fp.DevicePKs[key] = true
			if n, ok := baseline.Nodes[key]; ok && n.MetroCode != "" {
				fp.MetroCodes[n.MetroCode] = true
			}
		}
		switch c.OpType {
		case OpAddDevice:
			if c.LocalRef != "" {
				fp.DevicePKs[c.LocalRef] = true
			}
			// Resolve the metro code the same way the apply path does, so a
			// new-metro add_device attributes to its own metro (its inline code)
			// and not just an existing metro_pk.
			if _, mc := resolveAddDeviceMetro(baseline, p); mc != "" {
				fp.MetroCodes[mc] = true
			}
		case OpRemoveDevice:
			addDevMetro(c.RefDevicePK)
			for _, e := range baseline.Adj[c.RefDevicePK] {
				if e.LinkPK != "" {
					fp.LinkPKs[e.LinkPK] = true
				}
			}
		case OpAddLink:
			if c.LocalRef != "" {
				fp.LinkPKs[c.LocalRef] = true
			}
			addDevMetro(p.SideADevicePK)
			addDevMetro(p.SideZDevicePK)
		case OpRemoveLink:
			fp.LinkPKs[c.RefLinkPK] = true
			if ep, ok := baseline.LinkIndex[c.RefLinkPK]; ok {
				addDevMetro(ep[0])
				addDevMetro(ep[1])
			}
		case OpMoveLinkEnd:
			fp.LinkPKs[c.RefLinkPK] = true
			if ep, ok := baseline.LinkIndex[c.RefLinkPK]; ok {
				addDevMetro(ep[0])
				addDevMetro(ep[1])
			}
			addDevMetro(c.NewDevicePK) // SC-1: move target is the column, not payload
		}
		out[c.Seq] = fp
	}
	return out
}

// sortedFootprints returns footprints ordered by seq for deterministic output.
func sortedFootprints(fps map[int]changeFootprint) []changeFootprint {
	out := make([]changeFootprint, 0, len(fps))
	for _, fp := range fps {
		out = append(out, fp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// connectedComponents labels each node with a component id via BFS.
func connectedComponents(g *kspGraph) map[string]int {
	comp := make(map[string]int, len(g.Nodes))
	id := 0
	// Deterministic start order.
	starts := make([]string, 0, len(g.Nodes))
	for n := range g.Nodes {
		starts = append(starts, n)
	}
	sort.Strings(starts)
	for _, start := range starts {
		if _, seen := comp[start]; seen {
			continue
		}
		queue := []string{start}
		comp[start] = id
		for len(queue) > 0 {
			n := queue[0]
			queue = queue[1:]
			for _, e := range g.Adj[n] {
				if _, seen := comp[e.To]; !seen {
					comp[e.To] = id
					queue = append(queue, e.To)
				}
			}
		}
		id++
	}
	return comp
}

// mainComponent returns the member set of the largest connected component.
// Ties broken by the lexicographically smallest member.
func mainComponent(g *kspGraph) map[string]bool {
	comp := connectedComponents(g)
	members := map[int][]string{}
	for node, id := range comp {
		members[id] = append(members[id], node)
	}
	bestID, bestSize, bestMin := -1, -1, ""
	for id, ns := range members {
		mn := ns[0]
		for _, n := range ns {
			if n < mn {
				mn = n
			}
		}
		if len(ns) > bestSize || (len(ns) == bestSize && mn < bestMin) {
			bestID, bestSize, bestMin = id, len(ns), mn
		}
	}
	set := map[string]bool{}
	for _, n := range members[bestID] {
		set[n] = true
	}
	return set
}

// metroExitCounts counts, per metro, how many of its devices have at least one
// edge to a device in a different metro (an "exit device").
func metroExitCounts(g *kspGraph) map[string]int {
	out := map[string]int{}
	for pk, n := range g.Nodes {
		if n.MetroCode == "" {
			continue
		}
		hasExit := false
		for _, e := range g.Adj[pk] {
			if tn, ok := g.Nodes[e.To]; ok && tn.MetroCode != "" && tn.MetroCode != n.MetroCode {
				hasExit = true
				break
			}
		}
		if hasExit {
			out[n.MetroCode]++
		}
	}
	return out
}

// changeRefsForMetros returns changes touching any of the given metros.
func changeRefsForMetros(fps map[int]changeFootprint, metroCodes ...string) []ChangeRef {
	var refs []ChangeRef
	for _, fp := range sortedFootprints(fps) {
		for _, mc := range metroCodes {
			if fp.MetroCodes[mc] {
				refs = append(refs, ChangeRef{Seq: fp.Seq, OpType: fp.OpType, Label: fp.Label})
				break
			}
		}
	}
	return refs
}

// computePartitionImpact reports connectivity regressions comparing baseline to
// draft. Only new problems (not pre-existing ones) are returned.
func computePartitionImpact(baseline, draft *kspGraph, fps map[int]changeFootprint) []PartitionIssue {
	var issues []PartitionIssue

	baseMain := mainComponent(baseline)
	draftMain := mainComponent(draft)

	// Isolated devices: nodes in draft, not in draft main component, that were
	// either in the baseline main component or are brand-new.
	nodes := make([]string, 0, len(draft.Nodes))
	for n := range draft.Nodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if draftMain[node] {
			continue
		}
		_, inBaseline := baseline.Nodes[node]
		wasConnected := baseMain[node]
		if inBaseline && !wasConnected {
			continue // already isolated before the plan
		}
		info := draft.Nodes[node]
		var caused []ChangeRef
		for _, fp := range sortedFootprints(fps) {
			if fp.DevicePKs[node] || (info.MetroCode != "" && fp.MetroCodes[info.MetroCode]) {
				caused = append(caused, ChangeRef{Seq: fp.Seq, OpType: fp.OpType, Label: fp.Label})
			}
		}
		issues = append(issues, PartitionIssue{
			Severity: SeverityHigh,
			Type:     "device_isolated", EntityPK: node, EntityCode: info.Code, EntityType: "device",
			MetroCode: info.MetroCode, Description: "Device loses connectivity to the core network",
			CausedBy: caused,
		})
	}

	// Metro exit regressions.
	baseExit := metroExitCounts(baseline)
	draftExit := metroExitCounts(draft)
	metros := make([]string, 0, len(baseExit))
	for m := range baseExit {
		metros = append(metros, m)
	}
	sort.Strings(metros)
	for _, m := range metros {
		before := baseExit[m]
		after := draftExit[m]
		switch {
		case before > 0 && after == 0:
			issues = append(issues, PartitionIssue{
				Severity: SeverityHigh,
				Type:     "metro_isolated", EntityCode: m, EntityType: "metro", MetroCode: m,
				Description: "Metro loses all external connectivity",
				CausedBy:    changeRefsForMetros(fps, m),
			})
		case before > 1 && after == 1:
			issues = append(issues, PartitionIssue{
				Severity: SeverityMedium,
				Type:     "single_exit_metro", EntityCode: m, EntityType: "metro", MetroCode: m,
				Description: "Metro drops to a single exit device (no redundancy)",
				CausedBy:    changeRefsForMetros(fps, m),
			})
		}
	}

	return issues
}

// MetroLatencyDelta reports a best-path latency regression between two metros.
// SC-4 wire shape: BeforeUS/AfterUS are -1 when a pair is unreachable.
type MetroLatencyDelta struct {
	Severity Severity    `json:"severity"`
	MetroA   string      `json:"metro_a"`
	MetroZ   string      `json:"metro_z"`
	BeforeUS float64     `json:"before_us"` // -1 if unreachable
	AfterUS  float64     `json:"after_us"`  // -1 if unreachable
	DeltaUS  float64     `json:"delta_us"`  // -1 when the pair becomes unreachable
	CausedBy []ChangeRef `json:"caused_by"`
}

// latencySeverity ranks a metro-pair regression: unreachable is high; otherwise
// scale by the microsecond increase.
func latencySeverity(deltaUS float64, unreachable bool) Severity {
	switch {
	case unreachable, deltaUS >= 5000:
		return SeverityHigh
	case deltaUS >= 1000:
		return SeverityMedium
	default:
		return SeverityLow
	}
}

// edgeLinkPK returns the link pk of the (lowest-metric, sorted-first) edge
// between two adjacent nodes.
func edgeLinkPK(g *kspGraph, from, to string) string {
	for _, e := range g.Adj[from] {
		if e.To == to {
			return e.LinkPK
		}
	}
	return ""
}

func metroPairKey(a, b string) string {
	if b < a {
		a, b = b, a
	}
	return a + "|" + b
}

func indexMetroPaths(paths []metroPairPath) map[string]metroPairPath {
	out := make(map[string]metroPairPath, len(paths))
	for _, p := range paths {
		out[metroPairKey(p.FromMetroCode, p.ToMetroCode)] = p
	}
	return out
}

// attributeLatency finds the changes responsible for a metro-pair regression:
// changes whose footprint links lie on the baseline best path, or that touch
// either endpoint metro.
func attributeLatency(baseline *kspGraph, bp metroPairPath, fromCode, toCode string, fps map[int]changeFootprint) []ChangeRef {
	pathLinks := map[string]bool{}
	if bp.Path != nil {
		for i := 0; i < len(bp.Path.Nodes)-1; i++ {
			if lp := edgeLinkPK(baseline, bp.Path.Nodes[i], bp.Path.Nodes[i+1]); lp != "" {
				pathLinks[lp] = true
			}
		}
	}
	var refs []ChangeRef
	for _, fp := range sortedFootprints(fps) {
		hit := false
		for lp := range fp.LinkPKs {
			if pathLinks[lp] {
				hit = true
				break
			}
		}
		if !hit && (fp.MetroCodes[fromCode] || fp.MetroCodes[toCode]) {
			hit = true
		}
		if hit {
			refs = append(refs, ChangeRef{Seq: fp.Seq, OpType: fp.OpType, Label: fp.Label})
		}
	}
	return refs
}

// computeLatencyImpact returns best-path metro-pair latency regressions,
// worst-first (disconnected pairs first, then largest increase).
func computeLatencyImpact(baseline, draft *kspGraph, fps map[int]changeFootprint) []MetroLatencyDelta {
	basePaths := indexMetroPaths(computeMetroPairPaths(baseline))
	draftPaths := indexMetroPaths(computeMetroPairPaths(draft))

	var out []MetroLatencyDelta
	keys := make([]string, 0, len(basePaths))
	for k := range basePaths {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		bp := basePaths[key]
		codes := strings.SplitN(key, "|", 2)
		before := float64(bp.Path.TotalMetric)
		d := MetroLatencyDelta{MetroA: codes[0], MetroZ: codes[1], BeforeUS: before}
		if dp, ok := draftPaths[key]; ok {
			after := float64(dp.Path.TotalMetric)
			if after <= before {
				continue // no regression
			}
			d.AfterUS = after
			d.DeltaUS = after - before
			d.Severity = latencySeverity(d.DeltaUS, false)
		} else {
			// Pair became unreachable in the draft: SC-4 sentinel -1.
			d.AfterUS = -1
			d.DeltaUS = -1
			d.Severity = latencySeverity(0, true)
		}
		d.CausedBy = attributeLatency(baseline, bp, codes[0], codes[1], fps)
		out = append(out, d)
	}

	// Worst-first: unreachable pairs (after_us == -1) first, then largest increase.
	sort.SliceStable(out, func(i, j int) bool {
		di, dj := out[i].AfterUS < 0, out[j].AfterUS < 0
		if di != dj {
			return di
		}
		if out[i].DeltaUS != out[j].DeltaUS {
			return out[i].DeltaUS > out[j].DeltaUS
		}
		if out[i].MetroA != out[j].MetroA {
			return out[i].MetroA < out[j].MetroA
		}
		return out[i].MetroZ < out[j].MetroZ
	})
	if len(out) > 100 {
		out = out[:100]
	}
	return out
}

// RedundancyChange reports a drop in the number of independent (node-disjoint)
// paths between two metros.
type RedundancyChange struct {
	Severity    Severity    `json:"severity"`
	MetroA      string      `json:"metro_a"`
	MetroZ      string      `json:"metro_z"`
	BeforePaths int         `json:"before_paths"`
	AfterPaths  int         `json:"after_paths"`
	CausedBy    []ChangeRef `json:"caused_by"`
}

// redundancySeverity ranks a redundancy drop by how much diversity is left.
func redundancySeverity(afterPaths int) Severity {
	switch {
	case afterPaths == 0:
		return SeverityHigh
	case afterPaths == 1:
		return SeverityMedium
	default:
		return SeverityLow
	}
}

// countNodeDisjointPaths greedily counts node-disjoint shortest paths from src
// to dst (up to cap) by removing each found path's intermediate nodes.
func countNodeDisjointPaths(g *kspGraph, src, dst string, cap int) int {
	if src == dst {
		return 0
	}
	excl := map[string]bool{}
	count := 0
	for count < cap {
		p := dijkstra(g, src, dst, excl, nil)
		if p == nil {
			break
		}
		count++
		for i := 1; i < len(p.Nodes)-1; i++ {
			excl[p.Nodes[i]] = true
		}
	}
	return count
}

// countMetroDisjointPaths counts node-disjoint paths between two metros by
// attaching a super-source to the from-metro devices and a super-sink to the
// to-metro devices on a clone, then counting disjoint paths between them.
func countMetroDisjointPaths(g *kspGraph, fromCode, toCode string, cap int) int {
	const src, dst = "__src__", "__dst__"
	c := cloneGraph(g)
	c.Nodes[src] = kspNodeInfo{PK: src}
	c.Nodes[dst] = kspNodeInfo{PK: dst}
	attached := 0
	for pk, n := range g.Nodes {
		if n.MetroCode == fromCode {
			c.Adj[src] = append(c.Adj[src], kspEdge{To: pk, Metric: 1})
			c.Adj[pk] = append(c.Adj[pk], kspEdge{To: src, Metric: 1})
			attached++
		} else if n.MetroCode == toCode {
			c.Adj[dst] = append(c.Adj[dst], kspEdge{To: pk, Metric: 1})
			c.Adj[pk] = append(c.Adj[pk], kspEdge{To: dst, Metric: 1})
			attached++
		}
	}
	if attached == 0 {
		return 0
	}
	return countNodeDisjointPaths(c, src, dst, cap)
}

// candidateMetroPairs returns unordered metro pairs where at least one endpoint
// metro is touched by a change: affected metros × all metros.
func candidateMetroPairs(baseline, draft *kspGraph, fps map[int]changeFootprint) [][2]string {
	affected := map[string]bool{}
	for _, fp := range fps {
		for m := range fp.MetroCodes {
			if m != "" {
				affected[m] = true
			}
		}
	}
	all := map[string]bool{}
	for _, n := range baseline.Nodes {
		if n.MetroCode != "" {
			all[n.MetroCode] = true
		}
	}
	for _, n := range draft.Nodes {
		if n.MetroCode != "" {
			all[n.MetroCode] = true
		}
	}
	aff := sortedSet(affected)
	allS := sortedSet(all)
	seen := map[string]bool{}
	var pairs [][2]string
	for _, a := range aff {
		for _, b := range allS {
			if a == b {
				continue
			}
			x, y := a, b
			if y < x {
				x, y = y, x
			}
			key := x + "|" + y
			if seen[key] {
				continue
			}
			seen[key] = true
			pairs = append(pairs, [2]string{x, y})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	return pairs
}

func sortedSet(s map[string]bool) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// computeRedundancyImpact reports metro pairs whose node-disjoint path count
// dropped between baseline and draft.
func computeRedundancyImpact(baseline, draft *kspGraph, candidates [][2]string, fps map[int]changeFootprint) []RedundancyChange {
	const cap = 4
	var out []RedundancyChange
	for _, pair := range candidates {
		before := countMetroDisjointPaths(baseline, pair[0], pair[1], cap)
		after := countMetroDisjointPaths(draft, pair[0], pair[1], cap)
		if after >= before {
			continue
		}
		out = append(out, RedundancyChange{
			Severity: redundancySeverity(after),
			MetroA:   pair[0], MetroZ: pair[1],
			BeforePaths: before, AfterPaths: after,
			CausedBy: changeRefsForMetros(fps, pair[0], pair[1]),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		di := out[i].BeforePaths - out[i].AfterPaths
		dj := out[j].BeforePaths - out[j].AfterPaths
		if di != dj {
			return di > dj
		}
		if out[i].MetroA != out[j].MetroA {
			return out[i].MetroA < out[j].MetroA
		}
		return out[i].MetroZ < out[j].MetroZ
	})
	return out
}

// capacityUtilThreshold is the projected-utilization level above which a reroute
// link is flagged as at capacity risk.
const capacityUtilThreshold = 0.90

// CapacityRisk flags a link that would run hot after traffic reroutes onto it.
// It is an estimate: current measured traffic is displaced onto the reroute
// path; there is no demand matrix. The SC-4 wire fields (severity, link_pk,
// description, estimated, caused_by) are what Phase 5 renders; the rest is
// extra internal detail backing the honest-approximation disclosure.
type CapacityRisk struct {
	Severity          Severity    `json:"severity"`
	LinkPK            string      `json:"link_pk"`
	Description       string      `json:"description"`
	Estimated         bool        `json:"estimated"` // always true in v1 (approximation)
	RerouteFromLinkPK string      `json:"reroute_from_link_pk"`
	CurrentBps        float64     `json:"current_bps"`
	DisplacedBps      float64     `json:"displaced_bps"`
	ProjectedBps      float64     `json:"projected_bps"`
	BandwidthBps      uint64      `json:"bandwidth_bps"`
	UtilizationPct    float64     `json:"utilization_pct"`
	CausedBy          []ChangeRef `json:"caused_by"`
	Note              string      `json:"note"`
}

// computeCapacityImpact estimates capacity fallback risk. For each removed link
// (from remove_link, or a link attached to a removed device), its measured
// traffic is displaced onto the draft reroute between its endpoints; reroute
// links projected over threshold are flagged.
func computeCapacityImpact(baseline, draft *kspGraph, linkTraffic map[string]float64, fps map[int]changeFootprint) []CapacityRisk {
	const note = "estimate: measured traffic displaced onto reroute path; no demand matrix"
	var out []CapacityRisk
	seen := map[string]bool{} // dedup by removedLink|rerouteLink

	for _, fp := range sortedFootprints(fps) {
		if fp.OpType != OpRemoveLink && fp.OpType != OpRemoveDevice {
			continue
		}
		ref := ChangeRef{Seq: fp.Seq, OpType: fp.OpType, Label: fp.Label}
		for removedLink := range fp.LinkPKs {
			ep, ok := baseline.LinkIndex[removedLink]
			if !ok {
				continue
			}
			displaced := linkTraffic[removedLink]
			if displaced <= 0 {
				continue
			}
			reroute := dijkstra(draft, ep[0], ep[1], nil, nil)
			if reroute == nil {
				continue // partition, handled elsewhere
			}
			for i := 0; i < len(reroute.Nodes)-1; i++ {
				lp := edgeLinkPK(draft, reroute.Nodes[i], reroute.Nodes[i+1])
				bw := edgeBandwidth(draft, reroute.Nodes[i], reroute.Nodes[i+1])
				if lp == "" || bw == 0 {
					continue
				}
				existing := linkTraffic[lp]
				projected := existing + displaced
				util := projected / float64(bw)
				if util < capacityUtilThreshold {
					continue
				}
				key := removedLink + "|" + lp
				if seen[key] {
					continue
				}
				seen[key] = true
				sev := SeverityMedium
				if util >= 1.0 {
					sev = SeverityHigh
				}
				out = append(out, CapacityRisk{
					Severity:          sev,
					LinkPK:            lp,
					Description:       "Link projected over the capacity threshold after reroute",
					Estimated:         true,
					RerouteFromLinkPK: removedLink,
					CurrentBps:        existing, DisplacedBps: displaced, ProjectedBps: projected,
					BandwidthBps: bw, UtilizationPct: util * 100,
					CausedBy: []ChangeRef{ref}, Note: note,
				})
			}
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].UtilizationPct != out[j].UtilizationPct {
			return out[i].UtilizationPct > out[j].UtilizationPct
		}
		return out[i].LinkPK < out[j].LinkPK
	})
	return out
}

// PlanOverlapWarning flags another active plan that touches the same entity.
type PlanOverlapWarning struct {
	Severity        Severity `json:"severity"`
	OtherPlanID     string   `json:"other_plan_id"`
	OtherPlanName   string   `json:"other_plan_name"`
	OtherPlanStatus string   `json:"other_plan_status"`
	EntityType      string   `json:"entity_type"` // device | link
	EntityPK        string   `json:"entity_pk"`
	EntityCode      string   `json:"entity_code"`
	Description     string   `json:"description"`
}

// DataIssue reports a change that could not be analyzed (e.g. sentinel latency,
// zero bandwidth, unresolved ref, remove_device with attached links).
type DataIssue struct {
	Message string `json:"message"`
}

// PlanImpactReport is the result of impact analysis on a draft topology (SC-4).
type PlanImpactReport struct {
	PartitionIssues   []PartitionIssue     `json:"partition_issues"`
	LatencyDeltas     []MetroLatencyDelta  `json:"latency_deltas"`
	RedundancyChanges []RedundancyChange   `json:"redundancy_changes"`
	CapacityRisks     []CapacityRisk       `json:"capacity_risks"`
	OverlapWarnings   []PlanOverlapWarning `json:"overlap_warnings"`
	DataIssues        []DataIssue          `json:"data_issues"`
	Estimated         bool                 `json:"estimated"`
	GeneratedAt       time.Time            `json:"generated_at"`
}

// newImpactReport returns a report with all slices non-nil.
func newImpactReport() PlanImpactReport {
	return PlanImpactReport{
		PartitionIssues:   []PartitionIssue{},
		LatencyDeltas:     []MetroLatencyDelta{},
		RedundancyChanges: []RedundancyChange{},
		CapacityRisks:     []CapacityRisk{},
		OverlapWarnings:   []PlanOverlapWarning{},
		DataIssues:        []DataIssue{},
		GeneratedAt:       time.Now().UTC(),
	}
}

// anyEstimated reports whether any create/move op used a great-circle latency
// estimate, so the report can flag estimate-based results.
func anyEstimated(changes []PlanChange) bool {
	for _, c := range changes {
		if c.OpType != OpAddLink && c.OpType != OpMoveLinkEnd {
			continue
		}
		p, _ := decodePlanChangePayload(c)
		if p.EstimateSource == "great_circle" {
			return true
		}
	}
	return false
}

// computePlanImpact runs all four impact checks on a baseline/draft pair.
func computePlanImpact(baseline, draft *kspGraph, changes []PlanChange, linkTraffic map[string]float64, overlaps []PlanOverlapWarning) PlanImpactReport {
	fps := changeFootprints(baseline, changes)
	rep := newImpactReport()

	if p := computePartitionImpact(baseline, draft, fps); len(p) > 0 {
		rep.PartitionIssues = p
	}
	if l := computeLatencyImpact(baseline, draft, fps); len(l) > 0 {
		rep.LatencyDeltas = l
	}
	cands := candidateMetroPairs(baseline, draft, fps)
	if r := computeRedundancyImpact(baseline, draft, cands, fps); len(r) > 0 {
		rep.RedundancyChanges = r
	}
	if c := computeCapacityImpact(baseline, draft, linkTraffic, fps); len(c) > 0 {
		rep.CapacityRisks = c
	}
	if len(overlaps) > 0 {
		rep.OverlapWarnings = overlaps
	}
	rep.Estimated = anyEstimated(changes)
	return rep
}

// pendingOnly returns only the changes whose state is 'pending' (SC-8): the
// draft graph used for impact analysis is baseline + pending changes only. A
// 'done' change is already reflected in the live baseline (re-applying it
// would double-count the edge/device); 'skipped'/'superseded' changes are
// excluded outright. This is the single filtering point for the impact
// endpoint's draft graph; it does not touch applyChanges (state-agnostic by
// design) or deriveActionList (shows ALL changes with their state).
func pendingOnly(changes []PlanChange) []PlanChange {
	out := make([]PlanChange, 0, len(changes))
	for _, c := range changes {
		if c.State == StatePending {
			out = append(out, c)
		}
	}
	return out
}

// PlanImpactRequest optionally overrides the stored plan for a live preview of
// an unsaved draft.
type PlanImpactRequest struct {
	Environment string       `json:"environment,omitempty"`
	Changes     []PlanChange `json:"changes,omitempty"`
}

// PostTopologyPlanImpact computes impact analysis for a plan's draft topology.
func (a *API) PostTopologyPlanImpact(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	planID := chi.URLParam(r, "id")
	if planID == "" {
		http.Error(w, "plan id is required", http.StatusBadRequest)
		return
	}

	var req PlanImpactRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // optional body
	}

	env, err := a.loadPlanEnv(ctx, planID)
	if err != nil {
		http.Error(w, "plan not found", http.StatusNotFound)
		return
	}
	if req.Environment != "" {
		env = req.Environment
	}
	if env == "" {
		env = string(EnvFromContext(ctx))
	}

	// Changes: inline body overrides stored pending changes.
	changes := req.Changes
	if len(changes) == 0 {
		changes, err = a.loadPendingPlanChanges(ctx, planID)
		if err != nil {
			http.Error(w, "failed to load plan changes", http.StatusInternalServerError)
			return
		}
	}
	// SC-8: the draft graph (used for both applyChanges below and every impact
	// check that follows) is baseline + only pending changes. done changes are
	// already reflected in the live baseline (re-applying would double-count
	// the edge/device); skipped/superseded are excluded outright. This is a
	// no-op for the loadPendingPlanChanges path (its SQL already filters
	// state='pending'); it is what actually matters for the inline req.Changes
	// override, whose caller (an unsaved-draft preview) may pass changes of
	// any state. applyChanges itself stays state-agnostic on purpose — the
	// action list (deriveActionList) is a separate artifact that shows ALL
	// changes with their state, so filtering happens here, not there.
	changes = pendingOnly(changes)

	baseline, err := a.buildPlannerGraph(ctx, env)
	if err != nil {
		http.Error(w, "failed to load baseline topology", http.StatusInternalServerError)
		return
	}

	draft := cloneGraph(baseline)
	if err := applyChanges(draft, changes); err != nil {
		rep := newImpactReport()
		rep.DataIssues = append(rep.DataIssues, DataIssue{Message: err.Error()})
		writeJSON(w, rep)
		return
	}

	linkTraffic, err := a.loadLinkTrafficForImpact(ctx, env)
	if err != nil {
		linkTraffic = map[string]float64{} // capacity check degrades gracefully
	}

	// Cross-plan overlap: gather this plan's touched device/link pks.
	fps := changeFootprints(baseline, changes)
	devSet, linkSet := map[string]bool{}, map[string]bool{}
	for _, fp := range fps {
		for d := range fp.DevicePKs {
			devSet[d] = true
		}
		for l := range fp.LinkPKs {
			linkSet[l] = true
		}
	}
	overlaps, err := a.findOverlappingPlans(ctx, planID, env, sortedSet(devSet), sortedSet(linkSet))
	if err != nil {
		overlaps = nil
	}
	// Enrich device overlap warnings with codes from the baseline graph (link
	// codes are not carried in the graph, so those EntityCode values stay empty).
	for i := range overlaps {
		if overlaps[i].EntityType == "device" {
			if n, ok := baseline.Nodes[overlaps[i].EntityPK]; ok {
				overlaps[i].EntityCode = n.Code
			}
		}
	}

	rep := computePlanImpact(baseline, draft, changes, linkTraffic, overlaps)
	writeJSON(w, rep)
}

// loadPlanEnv returns the plan's environment.
func (a *API) loadPlanEnv(ctx context.Context, planID string) (string, error) {
	var env string
	err := a.PgPool.QueryRow(ctx,
		`SELECT environment FROM topology_plans WHERE id = $1 AND deleted_at IS NULL`, planID,
	).Scan(&env)
	return env, err
}

// loadPendingPlanChanges loads a plan's pending changes ordered by seq.
func (a *API) loadPendingPlanChanges(ctx context.Context, planID string) ([]PlanChange, error) {
	rows, err := a.PgPool.Query(ctx, `
		SELECT id, seq, op_type::text,
		       COALESCE(ref_device_pk, ''), COALESCE(ref_link_pk, ''), COALESCE(new_device_pk, ''),
		       COALESCE(local_ref, ''), payload, ref_snapshot, state::text
		FROM topology_plan_changes
		WHERE plan_id = $1 AND state = 'pending'
		ORDER BY seq`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PlanChange
	for rows.Next() {
		var c PlanChange // c.ID is uuid.UUID (SC-1); pgx scans the uuid column directly
		var op, state string
		var payload, snapshot []byte
		if err := rows.Scan(&c.ID, &c.Seq, &op,
			&c.RefDevicePK, &c.RefLinkPK, &c.NewDevicePK, &c.LocalRef,
			&payload, &snapshot, &state); err != nil {
			return nil, err
		}
		c.OpType = PlanOpType(op)
		c.State = PlanChangeState(state)
		c.Payload = json.RawMessage(payload)
		c.RefSnapshot = json.RawMessage(snapshot)
		out = append(out, c)
	}
	return out, rows.Err()
}

// loadLinkTrafficForImpact returns recent per-link traffic (bps), max of in/out.
// It reads from the ClickHouse DB of the plan's resolved environment (the same
// env used for buildPlannerGraph), not the ambient request env, so capacity
// numbers and the topology graph always come from the same environment.
func (a *API) loadLinkTrafficForImpact(ctx context.Context, env string) (map[string]float64, error) {
	if env != "" {
		ctx = ContextWithEnv(ctx, DZEnv(env))
	}
	rows, err := a.envDB(ctx).Query(ctx, `
		SELECT link_pk, avg(avg_in_bps) AS inb, avg(avg_out_bps) AS outb
		FROM device_interface_rollup_5m
		WHERE bucket_ts >= now() - INTERVAL 15 MINUTE
		  AND link_pk != ''
		GROUP BY link_pk`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]float64{}
	for rows.Next() {
		var pk string
		var inb, outb float64
		if err := rows.Scan(&pk, &inb, &outb); err != nil {
			return nil, err
		}
		if outb > inb {
			out[pk] = outb
		} else {
			out[pk] = inb
		}
	}
	return out, rows.Err()
}

// findOverlappingPlans returns warnings for other active (draft/approved) plans
// in the same env whose pending changes touch any of the given device/link pks.
func (a *API) findOverlappingPlans(ctx context.Context, planID, env string, devicePKs, linkPKs []string) ([]PlanOverlapWarning, error) {
	rows, err := a.PgPool.Query(ctx, `
		SELECT p.id::text, p.name, p.status::text,
		       COALESCE(c.ref_device_pk, ''), COALESCE(c.new_device_pk, ''), COALESCE(c.ref_link_pk, '')
		FROM topology_plan_changes c
		JOIN topology_plans p ON c.plan_id = p.id
		WHERE p.id <> $1
		  AND p.environment = $2
		  AND p.deleted_at IS NULL
		  AND p.status IN ('draft', 'approved')
		  AND c.state = 'pending'
		  AND (
		        (COALESCE(c.ref_device_pk, '') <> '' AND c.ref_device_pk = ANY($3::text[]))
		     OR (COALESCE(c.new_device_pk, '') <> '' AND c.new_device_pk = ANY($3::text[]))
		     OR (COALESCE(c.ref_link_pk, '')   <> '' AND c.ref_link_pk   = ANY($4::text[]))
		  )`, planID, env, devicePKs, linkPKs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	devSet := map[string]bool{}
	for _, d := range devicePKs {
		devSet[d] = true
	}
	linkSet := map[string]bool{}
	for _, l := range linkPKs {
		linkSet[l] = true
	}

	seen := map[string]bool{}
	var out []PlanOverlapWarning
	for rows.Next() {
		var otherID, otherName, otherStatus, refDev, newDev, refLink string
		if err := rows.Scan(&otherID, &otherName, &otherStatus, &refDev, &newDev, &refLink); err != nil {
			return nil, err
		}
		add := func(entityPK, entityType string) {
			if entityPK == "" {
				return
			}
			key := otherID + "|" + entityType + "|" + entityPK
			if seen[key] {
				return
			}
			seen[key] = true
			out = append(out, PlanOverlapWarning{
				Severity:        SeverityMedium,
				OtherPlanID:     otherID,
				OtherPlanName:   otherName,
				OtherPlanStatus: otherStatus,
				EntityType:      entityType,
				EntityPK:        entityPK,
				Description:     "Another active plan (" + otherName + ") also changes this " + entityType,
			})
		}
		if devSet[refDev] {
			add(refDev, "device")
		}
		if devSet[newDev] {
			add(newDev, "device")
		}
		if linkSet[refLink] {
			add(refLink, "link")
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].EntityPK != out[j].EntityPK {
			return out[i].EntityPK < out[j].EntityPK
		}
		return out[i].OtherPlanID < out[j].OtherPlanID
	})
	return out, rows.Err()
}
