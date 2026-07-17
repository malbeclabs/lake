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

func TestAnyEstimated(t *testing.T) {
	assert.True(t, anyEstimated([]PlanChange{{OpType: OpAddLink,
		Payload: json.RawMessage(`{"estimate_source":"great_circle"}`)}}))
	assert.False(t, anyEstimated([]PlanChange{{OpType: OpAddLink,
		Payload: json.RawMessage(`{"estimate_source":"manual"}`)}}))
}
