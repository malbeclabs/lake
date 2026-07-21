package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputePartitionImpact_DeviceIsolated(t *testing.T) {
	// a -- b -- c (linear). Remove l2 (b-c): c becomes isolated.
	base := mkPlannerBase()
	// mkPlannerBase has a parallel a-b link; that does not affect c's isolation.
	draft := cloneGraph(base)
	changes := []PlanChange{{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "l2",
		RefSnapshot: json.RawMessage(`{"link_code":"B-C"}`)}}
	require.NoError(t, applyChanges(draft, changes))

	fps := changeFootprints(base, changes)
	issues := computePartitionImpact(base, draft, fps)

	require.NotEmpty(t, issues)
	var isolated *PartitionIssue
	for i := range issues {
		if issues[i].Type == "device_isolated" && issues[i].EntityPK == "c" {
			isolated = &issues[i]
		}
	}
	require.NotNil(t, isolated, "device c should be reported isolated")
	require.NotEmpty(t, isolated.CausedBy)
	assert.Equal(t, 10, isolated.CausedBy[0].Seq)
}

// TestChangeFootprints_AddDeviceMetro asserts an add_device footprint carries
// the resolved metro code -- both for an existing metro_pk (resolved against
// the baseline nodes) and for a brand-new inline metro (its own code), so the
// footprint stays consistent with the apply path's resolveAddDeviceMetro.
func TestChangeFootprints_AddDeviceMetro(t *testing.T) {
	g := mkPlannerBase() // nodes a/b/c in metros m1/m2/m3 (codes M1/M2/M3)

	changes := []PlanChange{
		{
			Seq: 10, OpType: OpAddDevice, LocalRef: "tmp_existing",
			Payload: json.RawMessage(`{"contributor_pk":"c9","metro_pk":"m1","code":"new-existing"}`),
		},
		{
			Seq: 20, OpType: OpAddDevice, LocalRef: "tmp_new",
			Payload: json.RawMessage(`{"contributor_code":"newco","code":"new-metro-dev","new_metro":{"code":"ZZZ","latitude":10,"longitude":20}}`),
		},
	}

	fps := changeFootprints(g, changes)

	require.True(t, fps[10].MetroCodes["M1"], "existing-metro add_device footprint includes the resolved metro code")
	require.True(t, fps[20].MetroCodes["ZZZ"], "new-metro add_device footprint includes its own inline metro code")
}

func TestComputePartitionImpact_NoRegression(t *testing.T) {
	base := mkPlannerBase()
	draft := cloneGraph(base)
	// Remove only the parallel link l3: nothing gets isolated.
	changes := []PlanChange{{Seq: 1, OpType: OpRemoveLink, RefLinkPK: "l3"}}
	require.NoError(t, applyChanges(draft, changes))
	issues := computePartitionImpact(base, draft, changeFootprints(base, changes))
	assert.Empty(t, issues)
}

func TestComputePartitionImpact_SingleExitMetro(t *testing.T) {
	// M1 has two devices a1,a2, each with an inter-metro link to b (M2).
	// Removing a2's inter-metro link makes M1 single-exit.
	g := mkGraph()
	g.addNode("a1", "A1", "m1", "M1", "c1")
	g.addNode("a2", "A2", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addLink("la1b", "a1", "b", 10, 100)
	g.addLink("la2b", "a2", "b", 10, 100)
	g.addLink("a1a2", "a1", "a2", 1, 100) // intra-metro
	draft := cloneGraph(g)
	changes := []PlanChange{{Seq: 5, OpType: OpRemoveLink, RefLinkPK: "la2b"}}
	require.NoError(t, applyChanges(draft, changes))

	issues := computePartitionImpact(g, draft, changeFootprints(g, changes))
	var se *PartitionIssue
	for i := range issues {
		if issues[i].Type == "single_exit_metro" && issues[i].EntityCode == "M1" {
			se = &issues[i]
		}
	}
	require.NotNil(t, se, "M1 should be reported single-exit")
}

func TestComputeLatencyImpact_RerouteAndDisconnect(t *testing.T) {
	// M1(a) -- M2(b) -- M3(c) with a long direct a-c backup(100), plus a leaf
	// M4(d) hanging off c via c-d.
	//   a-b: 10, b-c: 10  => M1-M3 best = 20 via b; a-c: 100 (backup)
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("c", "C", "m3", "M3", "c3")
	g.addNode("d", "D", "m4", "M4", "c4")
	g.addLink("ab", "a", "b", 10, 100)
	g.addLink("bc", "b", "c", 10, 100)
	g.addLink("ac", "a", "c", 100, 100)
	g.addLink("cd", "c", "d", 10, 100)

	// Remove b-c: M1-M3 reroutes 20 -> 100 via the a-c backup (+80).
	// Remove c-d: M4 loses its only link and becomes unreachable.
	// (A single removal is either a bridge (disconnect) or not (reroute); it takes
	// two changes to exercise both an unreachable pair and a reroute.)
	draft := cloneGraph(g)
	changes := []PlanChange{
		{Seq: 7, OpType: OpRemoveLink, RefLinkPK: "bc", RefSnapshot: json.RawMessage(`{"link_code":"B-C"}`)},
		{Seq: 8, OpType: OpRemoveLink, RefLinkPK: "cd", RefSnapshot: json.RawMessage(`{"link_code":"C-D"}`)},
	}
	require.NoError(t, applyChanges(draft, changes))

	deltas := computeLatencyImpact(g, draft, changeFootprints(g, changes))
	require.NotEmpty(t, deltas)

	// An unreachable pair (after_us == -1) sorts first.
	assert.Equal(t, float64(-1), deltas[0].AfterUS)
	// Find the M1-M3 reroute.
	var m13 *MetroLatencyDelta
	for i := range deltas {
		if deltas[i].MetroA == "M1" && deltas[i].MetroZ == "M3" {
			m13 = &deltas[i]
		}
	}
	require.NotNil(t, m13)
	assert.Equal(t, float64(20), m13.BeforeUS)
	assert.Equal(t, float64(100), m13.AfterUS)
	assert.Equal(t, float64(80), m13.DeltaUS)
	require.NotEmpty(t, m13.CausedBy)
	assert.Equal(t, 7, m13.CausedBy[0].Seq)
}

func TestComputeLatencyImpact_NoRegressionOmitted(t *testing.T) {
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addLink("ab", "a", "b", 10, 100)
	g.addLink("ab2", "a", "b", 20, 100) // redundant, slower
	draft := cloneGraph(g)
	changes := []PlanChange{{Seq: 1, OpType: OpRemoveLink, RefLinkPK: "ab2"}}
	require.NoError(t, applyChanges(draft, changes))
	// Best M1-M2 path unchanged (still 10) -> no latency delta.
	assert.Empty(t, computeLatencyImpact(g, draft, changeFootprints(g, changes)))
}

func TestComputeRedundancyImpact(t *testing.T) {
	// Two node-disjoint M1->M2 paths: a-b and a2-b2. Remove one -> redundancy 2->1.
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("a2", "A2", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("b2", "B2", "m2", "M2", "c2")
	g.addLink("ab", "a", "b", 10, 100)
	g.addLink("a2b2", "a2", "b2", 10, 100)

	assert.Equal(t, 2, countMetroDisjointPaths(g, "M1", "M2", 4))

	draft := cloneGraph(g)
	changes := []PlanChange{{Seq: 3, OpType: OpRemoveLink, RefLinkPK: "a2b2",
		RefSnapshot: json.RawMessage(`{"link_code":"A2-B2"}`)}}
	require.NoError(t, applyChanges(draft, changes))

	cands := candidateMetroPairs(g, draft, changeFootprints(g, changes))
	rc := computeRedundancyImpact(g, draft, cands, changeFootprints(g, changes))
	require.Len(t, rc, 1)
	assert.Equal(t, "M1", rc[0].MetroA)
	assert.Equal(t, "M2", rc[0].MetroZ)
	assert.Equal(t, 2, rc[0].BeforePaths)
	assert.Equal(t, 1, rc[0].AfterPaths)
	require.NotEmpty(t, rc[0].CausedBy)
	assert.Equal(t, 3, rc[0].CausedBy[0].Seq)
}

func TestComputeCapacityImpact(t *testing.T) {
	// a==b two links: L1 (fast, 100bps) and L2 (backup, 100bps).
	// L1 carries 60bps; removing L1 displaces 60bps onto L2 which already
	// carries 40bps -> 100/100 = 100% > 90% threshold -> flagged.
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addLink("L1", "a", "b", 10, 100)
	g.addLink("L2", "a", "b", 20, 100)

	draft := cloneGraph(g)
	changes := []PlanChange{{Seq: 4, OpType: OpRemoveLink, RefLinkPK: "L1",
		RefSnapshot: json.RawMessage(`{"link_code":"A-B-1"}`)}}
	require.NoError(t, applyChanges(draft, changes))

	traffic := map[string]float64{"L1": 60, "L2": 40}
	risks := computeCapacityImpact(g, draft, traffic, changeFootprints(g, changes))
	require.Len(t, risks, 1)
	assert.Equal(t, "L2", risks[0].LinkPK)
	assert.InDelta(t, 100.0, risks[0].ProjectedBps, 0.01)
	assert.InDelta(t, 100.0, risks[0].UtilizationPct, 0.01)
	assert.Equal(t, 4, risks[0].CausedBy[0].Seq)
	assert.NotEmpty(t, risks[0].Note)
}

func TestComputeCapacityImpact_BelowThresholdOmitted(t *testing.T) {
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addLink("L1", "a", "b", 10, 100)
	g.addLink("L2", "a", "b", 20, 100)
	draft := cloneGraph(g)
	changes := []PlanChange{{Seq: 1, OpType: OpRemoveLink, RefLinkPK: "L1"}}
	require.NoError(t, applyChanges(draft, changes))
	traffic := map[string]float64{"L1": 10, "L2": 10} // 20/100 = 20%
	assert.Empty(t, computeCapacityImpact(g, draft, traffic, changeFootprints(g, changes)))
}

func TestComputePlanImpact_Composes(t *testing.T) {
	// Diamond across two metros:
	//   M1{a,a2} -- M2{b,b2}, two inter-metro links a-b(10, fast) and a2-b2(50,
	//   slow), plus intra-metro links a-a2(1) and b-b2(1).
	// Removing the fast a-b link must simultaneously trigger THREE checks with
	// real content, so this test proves the orchestrator wired each check to the
	// right graph rather than just returning pre-initialized empty slices:
	//   - latency:     M1-M2 best path 10 -> 50 (delta 40)
	//   - redundancy:  M1-M2 node-disjoint paths 2 -> 1
	//   - partition:   M1 (and M2) drop from 2 exit devices to 1 (single_exit_metro)
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("a2", "A2", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("b2", "B2", "m2", "M2", "c2")
	g.addLink("ab", "a", "b", 10, 100)     // fast inter-metro (removed below)
	g.addLink("a2b2", "a2", "b2", 50, 100) // slow inter-metro backup
	g.addLink("aa2", "a", "a2", 1, 100)    // intra M1
	g.addLink("bb2", "b", "b2", 1, 100)    // intra M2

	changes := []PlanChange{{Seq: 1, OpType: OpRemoveLink, RefLinkPK: "ab",
		RefSnapshot: json.RawMessage(`{"link_code":"A-B"}`)}}
	draft := cloneGraph(g)
	require.NoError(t, applyChanges(draft, changes))

	overlaps := []PlanOverlapWarning{{OtherPlanID: "p2", EntityPK: "ab", EntityType: "link"}}
	rep := computePlanImpact(g, draft, changes, map[string]float64{}, overlaps)

	// Latency check: the M1-M2 pair reroutes from the fast link onto the slow
	// backup, a real +40 regression attributed to seq 1.
	var lat *MetroLatencyDelta
	for i := range rep.LatencyDeltas {
		if rep.LatencyDeltas[i].MetroA == "M1" && rep.LatencyDeltas[i].MetroZ == "M2" {
			lat = &rep.LatencyDeltas[i]
		}
	}
	require.NotNil(t, lat, "expected an M1-M2 latency regression")
	assert.Equal(t, float64(10), lat.BeforeUS)
	assert.Equal(t, float64(50), lat.AfterUS)
	assert.Equal(t, float64(40), lat.DeltaUS)
	require.NotEmpty(t, lat.CausedBy)
	assert.Equal(t, 1, lat.CausedBy[0].Seq)

	// Redundancy check: M1-M2 drops from 2 node-disjoint paths to 1.
	var red *RedundancyChange
	for i := range rep.RedundancyChanges {
		if rep.RedundancyChanges[i].MetroA == "M1" && rep.RedundancyChanges[i].MetroZ == "M2" {
			red = &rep.RedundancyChanges[i]
		}
	}
	require.NotNil(t, red, "expected an M1-M2 redundancy drop")
	assert.Equal(t, 2, red.BeforePaths)
	assert.Equal(t, 1, red.AfterPaths)
	assert.Greater(t, red.BeforePaths, red.AfterPaths)

	// Partition check: M1 loses one of its two exit devices -> single_exit_metro.
	var part *PartitionIssue
	for i := range rep.PartitionIssues {
		if rep.PartitionIssues[i].Type == "single_exit_metro" && rep.PartitionIssues[i].EntityCode == "M1" {
			part = &rep.PartitionIssues[i]
		}
	}
	require.NotNil(t, part, "expected M1 reported single-exit")
	assert.Equal(t, "metro", part.EntityType)

	// Capacity: no traffic supplied, so no risk is produced.
	assert.Empty(t, rep.CapacityRisks)

	// Overlaps pass through unchanged; estimated is false (no great_circle
	// create/move op); generated_at is stamped.
	assert.Equal(t, overlaps, rep.OverlapWarnings)
	assert.False(t, rep.Estimated)
	assert.False(t, rep.GeneratedAt.IsZero())
}

// TestComputePlanImpact_HubDeviceRemoval_Reroute reproduces the QA report:
// "removing a device says I can't reach these metros anymore, but traffic
// would just take a longer path." It removes a hub device H the same way the
// real UI does (PlannerMap.tsx's handleDeviceClick stages remove_link for
// every link still attached to H, THEN remove_device H, in that seq order) on
// a topology with a genuine reroute:
//
//	a1(M1) --5-- h(MH) --5-- c1(M3)      (fast path via hub H, removed)
//	a1(M1) --20-- b1(M2) --20-- c1(M3)   (slower alternate NOT using H)
//	h(MH) --5-- d1(M4)                   (d1's ONLY link: genuinely isolated)
//
// M1-M3 must reroute (before=10 via H, after=40 via b1) with a real
// added-latency delta, NOT the after_us=-1 unreachable sentinel. d1, whose
// sole link was to H, has no alternate path and must be genuinely reported
// isolated -- this is the contrast case proving the test would catch a
// reroute engine that over-reports "no path".
func TestComputePlanImpact_HubDeviceRemoval_Reroute(t *testing.T) {
	base := mkGraph()
	base.addNode("a1", "A1", "m1", "M1", "c1")
	base.addNode("b1", "B1", "m2", "M2", "c2")
	base.addNode("c1", "C1", "m3", "M3", "c3")
	base.addNode("d1", "D1", "m4", "M4", "c4")
	base.addNode("h", "H", "mh", "MH", "chub")
	base.addLink("ah", "a1", "h", 5, 1000)
	base.addLink("hc", "h", "c1", 5, 1000)
	base.addLink("hd", "h", "d1", 5, 1000)
	base.addLink("ab", "a1", "b1", 20, 1000)
	base.addLink("bc", "b1", "c1", 20, 1000)

	// Mirror the real UI staging order exactly: attachedLinks(h) staged as
	// remove_link first (ah, hc, hd), remove_device(h) last.
	changes := []PlanChange{
		{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "ah", RefSnapshot: json.RawMessage(`{"link_code":"A1-H"}`)},
		{Seq: 20, OpType: OpRemoveLink, RefLinkPK: "hc", RefSnapshot: json.RawMessage(`{"link_code":"H-C1"}`)},
		{Seq: 30, OpType: OpRemoveLink, RefLinkPK: "hd", RefSnapshot: json.RawMessage(`{"link_code":"H-D1"}`)},
		{Seq: 40, OpType: OpRemoveDevice, RefDevicePK: "h", RefSnapshot: json.RawMessage(`{"device_code":"H"}`)},
	}
	draft := cloneGraph(base)
	require.NoError(t, applyChanges(draft, changes))

	fps := changeFootprints(base, changes)

	// --- Partition check ---
	issues := computePartitionImpact(base, draft, fps)
	isolatedDevices := map[string]bool{}
	for _, iss := range issues {
		if iss.Type == "device_isolated" {
			isolatedDevices[iss.EntityPK] = true
		}
	}
	assert.False(t, isolatedDevices["a1"], "a1 stays connected via the a1-b1-c1 reroute")
	assert.False(t, isolatedDevices["b1"], "b1 was never affected")
	assert.False(t, isolatedDevices["c1"], "c1 stays connected via the a1-b1-c1 reroute")
	assert.True(t, isolatedDevices["d1"], "d1's only link was to h; it is genuinely isolated (contrast case)")

	// --- Latency check ---
	deltas := computeLatencyImpact(base, draft, fps)
	byPair := map[string]MetroLatencyDelta{}
	for _, d := range deltas {
		byPair[metroPairKey(d.MetroA, d.MetroZ)] = d
	}

	m13, ok := byPair[metroPairKey("M1", "M3")]
	require.True(t, ok, "M1-M3 must appear as a reroute regression")
	assert.Equal(t, float64(10), m13.BeforeUS, "baseline M1-M3 best path is via h (5+5)")
	assert.Equal(t, float64(40), m13.AfterUS, "draft M1-M3 best path reroutes via b1 (20+20), not unreachable")
	assert.Equal(t, float64(30), m13.DeltaUS)
	assert.NotEqual(t, float64(-1), m13.AfterUS, "a reroute exists; must not report the unreachable sentinel")

	// M1-M2 and M2-M3 are untouched by the removal (still direct), so no delta.
	_, hasM12 := byPair[metroPairKey("M1", "M2")]
	assert.False(t, hasM12, "M1-M2 unaffected by h's removal")
	_, hasM23 := byPair[metroPairKey("M2", "M3")]
	assert.False(t, hasM23, "M2-M3 unaffected by h's removal")

	// d1 (M4) has no alternate path at all: every M4 pair genuinely disconnects.
	for _, pair := range [][2]string{{"M1", "M4"}, {"M2", "M4"}, {"M3", "M4"}} {
		d, ok := byPair[metroPairKey(pair[0], pair[1])]
		require.True(t, ok, "%v must appear (d1 had no alternate path)", pair)
		assert.Equal(t, float64(-1), d.AfterUS, "%v has no reroute; -1 is correct here", pair)
	}
}

func TestAnyEstimated(t *testing.T) {
	assert.True(t, anyEstimated([]PlanChange{{OpType: OpAddLink,
		Payload: json.RawMessage(`{"estimate_source":"great_circle"}`)}}))
	assert.False(t, anyEstimated([]PlanChange{{OpType: OpAddLink,
		Payload: json.RawMessage(`{"estimate_source":"manual"}`)}}))
}

// TestMaxNodeDisjointPaths_AddingRouteNeverDecreases is the regression test for
// the "4 -> 3" bug: the max node-disjoint path count between two metros must
// only ever go up (or stay the same) when a new disjoint route is added, never
// down. M1{a1,a2} and M2{b1,b2} start with two node-disjoint routes (a1-b1,
// a2-b2); adding a third device pair on both sides plus a third disjoint link
// (a3-b3) must raise the count from 2 to 3.
func TestMaxNodeDisjointPaths_AddingRouteNeverDecreases(t *testing.T) {
	g := mkGraph()
	g.addNode("a1", "A1", "m1", "M1", "c1")
	g.addNode("a2", "A2", "m1", "M1", "c1")
	g.addNode("a3", "A3", "m1", "M1", "c1")
	g.addNode("b1", "B1", "m2", "M2", "c2")
	g.addNode("b2", "B2", "m2", "M2", "c2")
	g.addNode("b3", "B3", "m2", "M2", "c2")
	g.addLink("l1", "a1", "b1", 10, 100)
	g.addLink("l2", "a2", "b2", 10, 100)

	assert.Equal(t, 2, countMetroDisjointPaths(g, "M1", "M2", 8))

	draft := cloneGraph(g)
	draft.addLink("l3", "a3", "b3", 10, 100)

	assert.Equal(t, 3, countMetroDisjointPaths(draft, "M1", "M2", 8),
		"adding a third node-disjoint route must raise the count, never lower it")
}

// TestMaxNodeDisjointPaths_ShortcutEdgeDoesNotDropCount reproduces the exact
// failure mode of the old greedy countNodeDisjointPaths: it ran dijkstra
// repeatedly and deleted each found path's intermediate nodes. s-a-t (cost 6)
// and s-b-c-t (cost 7) are two node-disjoint paths (true max = 2). Adding the
// a-c shortcut makes s-a-c-t (cost 3) the new shortest path; the OLD greedy
// algorithm would pick it first and delete BOTH a and c, stranding b (which
// only reaches t via c) -- dropping the count to 1 even though the true max
// node-disjoint path count is unchanged: {s-a-t, s-b-c-t} still both exist.
// maxNodeDisjointPaths (max-flow with node splitting) must not drop below 2.
func TestMaxNodeDisjointPaths_ShortcutEdgeDoesNotDropCount(t *testing.T) {
	g := mkGraph()
	g.addNode("s", "S", "ms", "MS", "cs")
	g.addNode("t", "T", "mt", "MT", "ct")
	g.addNode("a", "A", "ma", "MA", "ca")
	g.addNode("b", "B", "mb", "MB", "cb")
	g.addNode("c", "C", "mc", "MC", "cc")
	g.addLink("sa", "s", "a", 1, 100)
	g.addLink("at", "a", "t", 5, 100)
	g.addLink("sb", "s", "b", 1, 100)
	g.addLink("bc", "b", "c", 5, 100)
	g.addLink("ct", "c", "t", 1, 100)

	assert.Equal(t, 2, maxNodeDisjointPaths(g, "s", "t", 8))

	draft := cloneGraph(g)
	draft.addLink("ac", "a", "c", 1, 100) // shortcut: makes s-a-c-t the new shortest path

	assert.Equal(t, 2, maxNodeDisjointPaths(draft, "s", "t", 8),
		"adding the a-c shortcut must not drop the max disjoint path count below the pre-shortcut value")
}

// TestComputeLatencyImprovements_FasterAndNewlyReachable exercises both
// improvement shapes on one draft: M1(a) --100-- M2(b) --100-- M3(c), plus a
// short M1-M4(d) leg (a-d, 5). The draft adds a fast direct a-c shortcut (10)
// and removes b-c, and separately connects a previously-isolated M5(e).
//   - M1-M3 speeds up via the new a-c shortcut (200 -> 10): an improvement.
//   - M3-M4 ALSO reroutes through the same new a-c link (205 -> 15): a second
//     pair benefiting from the same change, as the brief requires.
//   - M1-M5 was unreachable (e had no edges) and becomes reachable via the new
//     e-a link: a newly-reachable row (before_us = -1).
//   - M2-M3 regresses (b-c removed, reroutes via a-c: 100 -> 110) and must NOT
//     appear in the improvements list; it must appear in computeLatencyImpact.
func TestComputeLatencyImprovements_FasterAndNewlyReachable(t *testing.T) {
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("c", "C", "m3", "M3", "c3")
	g.addNode("d", "D", "m4", "M4", "c4")
	g.addNode("e", "E", "m5", "M5", "c5") // isolated in the baseline
	g.addLink("ab", "a", "b", 100, 100)
	g.addLink("bc", "b", "c", 100, 100)
	g.addLink("ad", "a", "d", 5, 100)

	draft := cloneGraph(g)
	changes := []PlanChange{
		{Seq: 1, OpType: OpAddLink, LocalRef: "ac",
			Payload: json.RawMessage(`{"side_a_device_pk":"a","side_z_device_pk":"c","latency_ns":10000,"bandwidth_bps":100,"link_type":"WAN"}`)},
		{Seq: 2, OpType: OpRemoveLink, RefLinkPK: "bc", RefSnapshot: json.RawMessage(`{"link_code":"B-C"}`)},
		{Seq: 3, OpType: OpAddLink, LocalRef: "ea",
			Payload: json.RawMessage(`{"side_a_device_pk":"e","side_z_device_pk":"a","latency_ns":20000,"bandwidth_bps":100,"link_type":"WAN"}`)},
	}
	require.NoError(t, applyChanges(draft, changes))

	fps := changeFootprints(g, changes)
	improvements := computeLatencyImprovements(g, draft, fps)
	regressions := computeLatencyImpact(g, draft, fps)

	byPair := map[string]MetroLatencyDelta{}
	for _, d := range improvements {
		byPair[metroPairKey(d.MetroA, d.MetroZ)] = d
		assert.Equal(t, SeverityLow, d.Severity, "improvements are always low severity")
	}

	m13, ok := byPair[metroPairKey("M1", "M3")]
	require.True(t, ok, "M1-M3 should show an improvement")
	assert.Equal(t, float64(200), m13.BeforeUS)
	assert.Equal(t, float64(10), m13.AfterUS)
	assert.Equal(t, float64(-190), m13.DeltaUS)
	require.NotEmpty(t, m13.CausedBy)

	m34, ok := byPair[metroPairKey("M3", "M4")]
	require.True(t, ok, "M3-M4 should also improve via the new shortcut")
	assert.Equal(t, float64(205), m34.BeforeUS)
	assert.Equal(t, float64(15), m34.AfterUS)

	m15, ok := byPair[metroPairKey("M1", "M5")]
	require.True(t, ok, "M1-M5 should be newly reachable")
	assert.Equal(t, float64(-1), m15.BeforeUS)
	assert.Equal(t, float64(20), m15.AfterUS)
	assert.Equal(t, float64(0), m15.DeltaUS)

	_, isImprovement := byPair[metroPairKey("M2", "M3")]
	assert.False(t, isImprovement, "a regression must not appear in the improvements list")

	var m23 *MetroLatencyDelta
	for i := range regressions {
		if regressions[i].MetroA == "M2" && regressions[i].MetroZ == "M3" {
			m23 = &regressions[i]
		}
	}
	require.NotNil(t, m23, "M2-M3 should appear as a regression instead")
	assert.Equal(t, float64(100), m23.BeforeUS)
	assert.Equal(t, float64(110), m23.AfterUS)
}

// TestComputeRedundancyImprovements mirrors TestComputeRedundancyImpact in
// reverse: adding a parallel node-disjoint link between two metros must raise
// the path count (1 -> 2), not lower it.
func TestComputeRedundancyImprovements(t *testing.T) {
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("a2", "A2", "m1", "M1", "c1")
	g.addNode("b2", "B2", "m2", "M2", "c2")
	g.addLink("ab", "a", "b", 10, 100)

	require.Equal(t, 1, countMetroDisjointPaths(g, "M1", "M2", 8))

	draft := cloneGraph(g)
	changes := []PlanChange{{
		Seq: 9, OpType: OpAddLink, LocalRef: "a2b2",
		Payload: json.RawMessage(`{"side_a_device_pk":"a2","side_z_device_pk":"b2","latency_ns":10000,"bandwidth_bps":100,"link_type":"WAN"}`),
	}}
	require.NoError(t, applyChanges(draft, changes))

	fps := changeFootprints(g, changes)
	cands := candidateMetroPairs(g, draft, fps)
	ri := computeRedundancyImprovements(g, draft, cands, fps)
	require.Len(t, ri, 1)
	assert.Equal(t, "M1", ri[0].MetroA)
	assert.Equal(t, "M2", ri[0].MetroZ)
	assert.Equal(t, 1, ri[0].BeforePaths)
	assert.Equal(t, 2, ri[0].AfterPaths)
	assert.Equal(t, SeverityLow, ri[0].Severity)
	require.NotEmpty(t, ri[0].CausedBy)
}

// TestComputePlanImpact_PopulatesImprovements confirms the orchestrator wires
// both new improvement slices end to end: an add-link draft that both speeds
// up a metro pair AND gives it a second independent path populates both
// LatencyImprovements and RedundancyImprovements; a pure removal draft that
// changes nothing for the better leaves both empty but non-nil.
func TestComputePlanImpact_PopulatesImprovements(t *testing.T) {
	// M1{a,a2} -- M2{b,b2}; baseline has only the slow a2-b2 link (50). Adding
	// the fast direct a-b link (10) both improves latency (50 -> 10) and adds a
	// second node-disjoint path (1 -> 2).
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("a2", "A2", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("b2", "B2", "m2", "M2", "c2")
	g.addLink("a2b2", "a2", "b2", 50, 100)

	addChanges := []PlanChange{{
		Seq: 1, OpType: OpAddLink, LocalRef: "ab",
		Payload: json.RawMessage(`{"side_a_device_pk":"a","side_z_device_pk":"b","latency_ns":10000,"bandwidth_bps":100,"link_type":"WAN"}`),
	}}
	draftAdd := cloneGraph(g)
	require.NoError(t, applyChanges(draftAdd, addChanges))
	repAdd := computePlanImpact(g, draftAdd, addChanges, map[string]float64{}, nil)
	assert.NotEmpty(t, repAdd.LatencyImprovements, "add-link draft should show a latency improvement")
	assert.NotEmpty(t, repAdd.RedundancyImprovements, "add-link draft should show a redundancy improvement")

	// Pure removal draft: removing an unused slower parallel link changes nothing
	// for the better (or worse); both new slices stay empty but non-nil.
	g2 := mkGraph()
	g2.addNode("x", "X", "m1", "M1", "c1")
	g2.addNode("y", "Y", "m2", "M2", "c2")
	g2.addLink("xy", "x", "y", 10, 100)
	g2.addLink("xy2", "x", "y", 20, 100) // slower parallel, never on the best path

	removeChanges := []PlanChange{{Seq: 2, OpType: OpRemoveLink, RefLinkPK: "xy2"}}
	draftRemove := cloneGraph(g2)
	require.NoError(t, applyChanges(draftRemove, removeChanges))
	repRemove := computePlanImpact(g2, draftRemove, removeChanges, map[string]float64{}, nil)
	assert.NotNil(t, repRemove.LatencyImprovements)
	assert.Empty(t, repRemove.LatencyImprovements)
	assert.NotNil(t, repRemove.RedundancyImprovements)
	assert.Empty(t, repRemove.RedundancyImprovements)
}
