package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- shared white-box test helpers for the planner engine ---

func mkGraph() *kspGraph {
	return &kspGraph{
		Adj:       map[string][]kspEdge{},
		Nodes:     map[string]kspNodeInfo{},
		LinkIndex: map[string][2]string{},
	}
}

func (g *kspGraph) addNode(pk, code, metroPK, metroCode, contrib string) {
	g.Nodes[pk] = kspNodeInfo{
		PK: pk, Code: code, Status: "activated", DeviceType: "switch",
		MetroPK: metroPK, MetroCode: metroCode, ContributorPK: contrib,
	}
}

// addLink adds a bidirectional WAN edge with the given link identity.
func (g *kspGraph) addLink(linkPK, a, z string, metric uint32, bw uint64) {
	ac := g.Nodes[a].ContributorPK
	zc := g.Nodes[z].ContributorPK
	g.Adj[a] = append(g.Adj[a], kspEdge{To: z, Metric: metric, BandwidthBps: bw, LinkPK: linkPK, LinkType: "WAN", SideAContributorPK: ac, SideZContributorPK: zc})
	g.Adj[z] = append(g.Adj[z], kspEdge{To: a, Metric: metric, BandwidthBps: bw, LinkPK: linkPK, LinkType: "WAN", SideAContributorPK: zc, SideZContributorPK: ac})
	g.LinkIndex[linkPK] = [2]string{a, z}
}

func TestCloneGraph_DeepCopy(t *testing.T) {
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addLink("l1", "a", "b", 10, 100)

	c := cloneGraph(g)

	// Same content.
	require.Len(t, c.Adj["a"], 1)
	assert.Equal(t, "l1", c.Adj["a"][0].LinkPK)
	assert.Equal(t, "WAN", c.Adj["a"][0].LinkType)
	assert.Equal(t, [2]string{"a", "b"}, c.LinkIndex["l1"])
	assert.Equal(t, "c1", c.Nodes["a"].ContributorPK)

	// Mutating the clone must not touch the original.
	c.Adj["a"] = append(c.Adj["a"], kspEdge{To: "b", Metric: 5, LinkPK: "l2"})
	c.Nodes["a"] = kspNodeInfo{PK: "a", Code: "CHANGED"}
	delete(c.LinkIndex, "l1")

	assert.Len(t, g.Adj["a"], 1, "original adjacency unchanged")
	assert.Equal(t, "A", g.Nodes["a"].Code, "original node unchanged")
	assert.Equal(t, [2]string{"a", "b"}, g.LinkIndex["l1"], "original index unchanged")
}

func TestLatencyToMetric(t *testing.T) {
	tests := []struct {
		name       string
		latencyNs  int64
		overrideNs int64
		want       uint32
	}{
		{"plain latency to us", 5_000_000, 0, 5000},
		{"override wins", 5_000_000, 2_000_000, 2000},
		{"sub-microsecond floors to 1", 500, 0, 1},
		{"zero floors to 1", 0, 0, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, latencyToMetric(tc.latencyNs, tc.overrideNs))
		})
	}
}

func TestDecodePayload(t *testing.T) {
	p, err := decodePlanChangePayload(PlanChange{Payload: json.RawMessage(`{"latency_ns":3000000,"bandwidth_bps":100,"link_type":"DZX","side":"a","side_a_ref":"tmp_dev_1"}`)})
	require.NoError(t, err)
	assert.Equal(t, int64(3000000), p.LatencyNs)
	assert.Equal(t, uint64(100), p.BandwidthBps)
	assert.Equal(t, "DZX", p.LinkType)
	assert.Equal(t, "a", p.Side)
	assert.Equal(t, "tmp_dev_1", p.SideARef)

	empty, err := decodePlanChangePayload(PlanChange{})
	require.NoError(t, err)
	assert.Equal(t, plannerPayload{}, empty)

	_, err = decodePlanChangePayload(PlanChange{Payload: json.RawMessage(`{bad`)})
	assert.Error(t, err)
}

func TestChangeLabel(t *testing.T) {
	assert.Equal(t, "nyc-dz01",
		refSnapshotLabel(PlanChange{OpType: OpRemoveDevice, RefDevicePK: "pk1",
			RefSnapshot: json.RawMessage(`{"device_code":"nyc-dz01"}`)}))
	assert.Equal(t, "pk-link-7",
		refSnapshotLabel(PlanChange{OpType: OpRemoveLink, RefLinkPK: "pk-link-7"}))
	assert.Equal(t, "add_device",
		refSnapshotLabel(PlanChange{OpType: OpAddDevice}))
}

func mkPlannerBase() *kspGraph {
	// a --l1(10)-- b --l2(10)-- c ; plus a parallel a--l3(20)--b
	g := mkGraph()
	g.addNode("a", "A", "m1", "M1", "c1")
	g.addNode("b", "B", "m2", "M2", "c2")
	g.addNode("c", "C", "m3", "M3", "c3")
	g.addLink("l1", "a", "b", 10, 100)
	g.addLink("l2", "b", "c", 10, 100)
	g.addLink("l3", "a", "b", 20, 50) // parallel link, distinct pk
	return g
}

func TestApplyChanges_AddLink(t *testing.T) {
	g := mkPlannerBase()
	err := applyChanges(g, []PlanChange{{
		Seq: 10, OpType: OpAddLink, LocalRef: "tmp_link_1",
		Payload: json.RawMessage(`{"side_a_device_pk":"a","side_z_device_pk":"c","latency_ns":4000000,"bandwidth_bps":250,"link_type":"WAN"}`),
	}})
	require.NoError(t, err)

	assert.Equal(t, [2]string{"a", "c"}, g.LinkIndex["tmp_link_1"])
	// bidirectional, correct metric (4000000ns -> 4000us) and bandwidth
	require.Len(t, g.Adj["a"], 3) // l1, l3, new
	var found bool
	for _, e := range g.Adj["a"] {
		if e.LinkPK == "tmp_link_1" {
			found = true
			assert.Equal(t, "c", e.To)
			assert.Equal(t, uint32(4000), e.Metric)
			assert.Equal(t, uint64(250), e.BandwidthBps)
		}
	}
	assert.True(t, found)
	assert.Len(t, g.Adj["c"], 2) // l2 + new reverse
}

func TestApplyChanges_AddLink_Guards(t *testing.T) {
	cases := map[string]string{
		"sentinel latency": `{"side_a_device_pk":"a","side_z_device_pk":"c","latency_ns":1000000000,"bandwidth_bps":10}`,
		"zero bandwidth":   `{"side_a_device_pk":"a","side_z_device_pk":"c","latency_ns":4000000,"bandwidth_bps":0}`,
		"unresolved ref":   `{"side_a_ref":"nope","side_z_device_pk":"c","latency_ns":4000000,"bandwidth_bps":10}`,
		"missing device":   `{"side_a_device_pk":"ghost","side_z_device_pk":"c","latency_ns":4000000,"bandwidth_bps":10}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			g := mkPlannerBase()
			err := applyChanges(g, []PlanChange{{Seq: 1, OpType: OpAddLink, LocalRef: "x", Payload: json.RawMessage(payload)}})
			assert.Error(t, err)
		})
	}
}

func TestApplyChanges_RemoveLink_ExactAndIdempotent(t *testing.T) {
	g := mkPlannerBase()

	// Remove only the parallel link l3; l1 must survive.
	require.NoError(t, applyChanges(g, []PlanChange{{Seq: 1, OpType: OpRemoveLink, RefLinkPK: "l3"}}))
	_, ok := g.LinkIndex["l3"]
	assert.False(t, ok)
	require.Len(t, g.Adj["a"], 1)
	assert.Equal(t, "l1", g.Adj["a"][0].LinkPK)

	// Removing an absent link is a no-op (drift: already done).
	require.NoError(t, applyChanges(g, []PlanChange{{Seq: 2, OpType: OpRemoveLink, RefLinkPK: "l3"}}))
	require.Len(t, g.Adj["a"], 1)
}

func TestApplyChanges_AddDevice(t *testing.T) {
	g := mkPlannerBase()
	err := applyChanges(g, []PlanChange{{
		Seq: 10, OpType: OpAddDevice, LocalRef: "tmp_dev_1",
		Payload: json.RawMessage(`{"contributor_pk":"c9","metro_pk":"m1","code":"new-dz01","device_type":"switch"}`),
	}})
	require.NoError(t, err)
	n, ok := g.Nodes["tmp_dev_1"]
	require.True(t, ok)
	assert.Equal(t, "new-dz01", n.Code)
	assert.Equal(t, "c9", n.ContributorPK)
	assert.Equal(t, "M1", n.MetroCode, "metro code resolved from existing m1 node")
}

func TestApplyChanges_RemoveDevice(t *testing.T) {
	// c is a leaf attached only via l2; remove l2 first, then remove c.
	g := mkPlannerBase()
	require.NoError(t, applyChanges(g, []PlanChange{
		{Seq: 10, OpType: OpRemoveLink, RefLinkPK: "l2"},
		{Seq: 20, OpType: OpRemoveDevice, RefDevicePK: "c"},
	}))
	_, ok := g.Nodes["c"]
	assert.False(t, ok)

	// Removing a device that still has attached links errors.
	g2 := mkPlannerBase()
	err := applyChanges(g2, []PlanChange{{Seq: 1, OpType: OpRemoveDevice, RefDevicePK: "b"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attached link")

	// Removing an absent device is a no-op.
	g3 := mkPlannerBase()
	require.NoError(t, applyChanges(g3, []PlanChange{{Seq: 1, OpType: OpRemoveDevice, RefDevicePK: "ghost"}}))
}

// TestApplyChanges_RemovedLocalRefNoPhantom guards against graph corruption:
// add_device(local_ref) -> remove_device(that device) -> add_link referencing
// the removed local_ref must fail cleanly, not mint a phantom node/edge.
func TestApplyChanges_RemovedLocalRefNoPhantom(t *testing.T) {
	g := mkPlannerBase()
	err := applyChanges(g, []PlanChange{
		{Seq: 10, OpType: OpAddDevice, LocalRef: "tmp_dev_1",
			Payload: json.RawMessage(`{"contributor_pk":"c9","metro_pk":"m1","code":"new-dz01","device_type":"switch"}`)},
		{Seq: 20, OpType: OpRemoveDevice, RefDevicePK: "tmp_dev_1"},
		{Seq: 30, OpType: OpAddLink, LocalRef: "tmp_link_1",
			Payload: json.RawMessage(`{"side_a_ref":"tmp_dev_1","side_z_device_pk":"c","latency_ns":4000000,"bandwidth_bps":250,"link_type":"WAN"}`)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tmp_dev_1")

	// No dangling entries for the removed device or the would-be link anywhere.
	_, hasNode := g.Nodes["tmp_dev_1"]
	assert.False(t, hasNode, "removed device must not linger in Nodes")
	_, hasAdj := g.Adj["tmp_dev_1"]
	assert.False(t, hasAdj, "removed device must not linger in Adj")
	_, hasLink := g.LinkIndex["tmp_link_1"]
	assert.False(t, hasLink, "failed add_link must not register a link")
	for from, edges := range g.Adj {
		for _, e := range edges {
			assert.NotEqual(t, "tmp_dev_1", e.To, "no edge should point at removed device (from %s)", from)
			assert.NotEqual(t, "tmp_link_1", e.LinkPK, "no edge for the failed link (from %s)", from)
		}
	}
}

func TestApplyChanges_MoveLinkEnd_ExistingTarget(t *testing.T) {
	// Move side z of l2 (b<->c) from c to a. Fixed side is b (since side "z" moves).
	g := mkPlannerBase()
	require.NoError(t, applyChanges(g, []PlanChange{{
		Seq: 10, OpType: OpMoveLinkEnd, RefLinkPK: "l2", NewDevicePK: "a",
		Payload: json.RawMessage(`{"side":"z","new_iface_name":"Ethernet9","latency_ns":6000000,"bandwidth_bps":100}`),
	}}))

	ep := g.LinkIndex["l2"]
	assert.Equal(t, [2]string{"b", "a"}, ep, "l2 now connects fixed side b to new side a")
	// c no longer carries l2
	for _, e := range g.Adj["c"] {
		assert.NotEqual(t, "l2", e.LinkPK)
	}
	// new metric applied
	for _, e := range g.Adj["b"] {
		if e.LinkPK == "l2" {
			assert.Equal(t, "a", e.To)
			assert.Equal(t, uint32(6000), e.Metric)
		}
	}
}

func TestApplyChanges_MoveLinkEnd_SideA_PreservesOrder(t *testing.T) {
	// l2 is b<->c (A=b, Z=c). Add device d, move side A of l2 from b to d.
	// Fixed side is c (Z). Result must be A=d, Z=c -> LinkIndex ["d","c"].
	g := mkPlannerBase()
	require.NoError(t, applyChanges(g, []PlanChange{
		{Seq: 10, OpType: OpAddDevice, LocalRef: "d",
			Payload: json.RawMessage(`{"metro_pk":"m1","code":"new-d","device_type":"switch","contributor_pk":"c9"}`)},
		{Seq: 20, OpType: OpMoveLinkEnd, RefLinkPK: "l2", NewDevicePK: "",
			Payload: json.RawMessage(`{"side":"a","new_device_ref":"d","latency_ns":6000000,"bandwidth_bps":100}`)},
	}))
	assert.Equal(t, [2]string{"d", "c"}, g.LinkIndex["l2"], "A endpoint moved to d, Z endpoint c fixed")

	// Old A node b no longer carries l2.
	for _, e := range g.Adj["b"] {
		assert.NotEqual(t, "l2", e.LinkPK, "old A node b must not carry l2")
	}
	// side_a/side_z contributor derivation matches the new ordering (A=d->c9, Z=c->c3).
	for _, e := range g.Adj["d"] {
		if e.LinkPK == "l2" {
			assert.Equal(t, "c", e.To)
			assert.Equal(t, uint32(6000), e.Metric)
			assert.Equal(t, "c9", e.SideAContributorPK)
			assert.Equal(t, "c3", e.SideZContributorPK)
		}
	}

	// A SECOND move on the same link must resolve endpoints from the corrected
	// LinkIndex. Move side "z" (currently c) to a -> A=d stays, Z becomes a.
	require.NoError(t, applyChanges(g, []PlanChange{{
		Seq: 30, OpType: OpMoveLinkEnd, RefLinkPK: "l2", NewDevicePK: "a",
		Payload: json.RawMessage(`{"side":"z","latency_ns":6000000,"bandwidth_bps":100}`),
	}}))
	assert.Equal(t, [2]string{"d", "a"}, g.LinkIndex["l2"], "second move: A=d fixed, Z moved c->a")
	for _, e := range g.Adj["c"] {
		assert.NotEqual(t, "l2", e.LinkPK, "c must no longer carry l2 after second move")
	}
}

func TestApplyChanges_MoveLinkEnd_ToNewDeviceRef(t *testing.T) {
	// add_device then move l2's z end onto it via new_device_ref.
	g := mkPlannerBase()
	require.NoError(t, applyChanges(g, []PlanChange{
		{Seq: 10, OpType: OpAddDevice, LocalRef: "tmp_dev_1",
			Payload: json.RawMessage(`{"metro_pk":"m1","code":"new-dz","device_type":"switch","contributor_pk":"c9"}`)},
		{Seq: 20, OpType: OpMoveLinkEnd, RefLinkPK: "l2",
			Payload: json.RawMessage(`{"side":"z","new_device_ref":"tmp_dev_1","new_iface_name":"Et1","latency_ns":7000000,"bandwidth_bps":100}`)},
	}))
	assert.Equal(t, [2]string{"b", "tmp_dev_1"}, g.LinkIndex["l2"])
}

func TestApplyChanges_MoveLinkEnd_Errors(t *testing.T) {
	g := mkPlannerBase()
	// link not found
	err := applyChanges(g, []PlanChange{{Seq: 1, OpType: OpMoveLinkEnd, RefLinkPK: "ghost", NewDevicePK: "c",
		Payload: json.RawMessage(`{"side":"a","latency_ns":5000000,"bandwidth_bps":10}`)}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// bad side
	g2 := mkPlannerBase()
	err = applyChanges(g2, []PlanChange{{Seq: 1, OpType: OpMoveLinkEnd, RefLinkPK: "l2", NewDevicePK: "a",
		Payload: json.RawMessage(`{"side":"x","latency_ns":5000000,"bandwidth_bps":10}`)}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "side must be")
}
