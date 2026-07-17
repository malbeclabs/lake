package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// unsetLatencyNs is the sentinel value that means "latency not set". A new or
// moved link that keeps this value silently drops out of the impact engine, so
// applyChanges refuses it.
const unsetLatencyNs = 1000000000

// plannerPayload is the op-specific JSONB payload of a topology_plan_changes row.
// Only the fields the impact engine reads are modeled. Per SC-1, move_link_end's
// target existing-device pk lives in the ref column (c.NewDevicePK), NOT here;
// only the temp/local reference (new_device_ref) is carried in the payload.
type plannerPayload struct {
	// add_device
	ContributorPK string `json:"contributor_pk"`
	MetroPK       string `json:"metro_pk"`
	Code          string `json:"code"`
	DeviceType    string `json:"device_type"`
	// move_link_end (target existing device is c.NewDevicePK, a column, not payload)
	Side         string `json:"side"` // "a" | "z"
	NewDeviceRef string `json:"new_device_ref"`
	NewIfaceName string `json:"new_iface_name"`
	// add_link
	SideADevicePK  string   `json:"side_a_device_pk"`
	SideARef       string   `json:"side_a_ref"`
	SideZDevicePK  string   `json:"side_z_device_pk"`
	SideZRef       string   `json:"side_z_ref"`
	SideAIfaceName string   `json:"side_a_iface_name"`
	SideZIfaceName string   `json:"side_z_iface_name"`
	LinkType       string   `json:"link_type"`
	LinkTopologies []string `json:"link_topologies"`
	// shared routing-relevant fields
	BandwidthBps     uint64 `json:"bandwidth_bps"`
	LatencyNs        int64  `json:"latency_ns"`
	MetricOverrideNs int64  `json:"metric_override_ns"`
	EstimateSource   string `json:"estimate_source"` // copied | great_circle | manual
}

// decodePlanChangePayload unmarshals a change's JSONB payload. An empty payload
// yields a zero-value struct without error. Owner Phase 2 (SC-2); Phase 4 reuses
// it, so there is exactly one payload decoder in the package.
func decodePlanChangePayload(c PlanChange) (plannerPayload, error) {
	var p plannerPayload
	if len(c.Payload) == 0 {
		return p, nil
	}
	if err := json.Unmarshal(c.Payload, &p); err != nil {
		return p, fmt.Errorf("decode change payload: %w", err)
	}
	return p, nil
}

// latencyToMetric converts a link latency (ns) to the engine's µs metric, using
// the metric override when present, mirroring loadTopologyGraph. Floors at 1.
func latencyToMetric(latencyNs, overrideNs int64) uint32 {
	n := latencyNs
	if overrideNs > 0 {
		n = overrideNs
	}
	m := uint32(n / 1000)
	if m == 0 {
		m = 1
	}
	return m
}

// refSnapshotFields is the subset of a change's ref_snapshot used for display.
type refSnapshotFields struct {
	DeviceCode string `json:"device_code"`
	LinkCode   string `json:"link_code"`
	Code       string `json:"code"`
}

// refSnapshotLabel returns a human label for a change, preferring the captured
// ref_snapshot identity so labels survive pk removal. Owner Phase 2 (SC-2);
// Phase 4 reuses it.
func refSnapshotLabel(c PlanChange) string {
	var s refSnapshotFields
	_ = json.Unmarshal(c.RefSnapshot, &s)
	switch {
	case s.DeviceCode != "":
		return s.DeviceCode
	case s.LinkCode != "":
		return s.LinkCode
	case s.Code != "":
		return s.Code
	case c.RefLinkPK != "":
		return c.RefLinkPK
	case c.RefDevicePK != "":
		return c.RefDevicePK
	case c.LocalRef != "":
		return c.LocalRef
	default:
		return string(c.OpType)
	}
}

// buildPlannerGraph loads the live baseline topology for env into an in-memory
// kspGraph (all activated links, algo-0 behavior). Reuses loadTopologyGraph.
func (a *API) buildPlannerGraph(ctx context.Context, env string) (*kspGraph, error) {
	if env != "" {
		ctx = ContextWithEnv(ctx, DZEnv(env))
	}
	return a.loadTopologyGraph(ctx, "")
}

// applyChanges mutates g in place by applying the ordered plan changes. It is a
// pure function of (g, changes): no I/O. Create/move ops that carry invalid
// routing data (sentinel latency, zero bandwidth, unresolvable refs) return an
// error rather than silently dropping the edge; removals of already-absent
// entities are idempotent no-ops (drift == already done).
func applyChanges(g *kspGraph, changes []PlanChange) error {
	localRefs := map[string]string{} // local_ref -> node key
	for _, c := range changes {
		p, err := decodePlanChangePayload(c)
		if err != nil {
			return fmt.Errorf("change seq %d: %w", c.Seq, err)
		}
		switch c.OpType {
		case OpAddLink:
			aKey, err := resolveEndpoint(g, localRefs, p.SideADevicePK, p.SideARef)
			if err != nil {
				return fmt.Errorf("add_link seq %d side a: %w", c.Seq, err)
			}
			zKey, err := resolveEndpoint(g, localRefs, p.SideZDevicePK, p.SideZRef)
			if err != nil {
				return fmt.Errorf("add_link seq %d side z: %w", c.Seq, err)
			}
			if err := guardLinkMetrics(p); err != nil {
				return fmt.Errorf("add_link seq %d: %w", c.Seq, err)
			}
			linkPK := c.LocalRef
			if linkPK == "" {
				linkPK = fmt.Sprintf("tmp_link_%d", c.Seq)
			}
			addLinkEdge(g, linkPK, p.LinkType, aKey, zKey,
				latencyToMetric(p.LatencyNs, p.MetricOverrideNs), p.BandwidthBps)
		case OpRemoveLink:
			removeLinkByPK(g, c.RefLinkPK) // idempotent no-op if absent
		case OpAddDevice:
			if c.LocalRef == "" {
				return fmt.Errorf("add_device seq %d: missing local_ref", c.Seq)
			}
			key := c.LocalRef
			g.Nodes[key] = kspNodeInfo{
				PK: key, Code: p.Code, Status: "planned", DeviceType: p.DeviceType,
				MetroPK: p.MetroPK, MetroCode: metroCodeForPK(g, p.MetroPK),
				ContributorPK: p.ContributorPK,
			}
			localRefs[c.LocalRef] = key
		case OpRemoveDevice:
			key := c.RefDevicePK
			if _, ok := g.Nodes[key]; !ok {
				continue // already gone (drift: already done)
			}
			if len(g.Adj[key]) > 0 {
				return fmt.Errorf("remove_device seq %d: device %s still has %d attached link(s); remove or move them first",
					c.Seq, key, len(g.Adj[key]))
			}
			delete(g.Nodes, key)
			delete(g.Adj, key)
			// Purge any local_ref(s) that resolved to this node so a later op
			// referencing it fails cleanly in resolveEndpoint instead of minting
			// a phantom edge to a node no longer in g.Nodes.
			for ref, resolved := range localRefs {
				if resolved == key {
					delete(localRefs, ref)
				}
			}
		case OpMoveLinkEnd:
			if err := applyMoveLinkEnd(g, localRefs, c, p); err != nil {
				return err
			}
		default:
			// Other op types are added in later tasks.
			return fmt.Errorf("seq %d: unhandled op_type %q", c.Seq, c.OpType)
		}
	}
	return nil
}

// resolveEndpoint resolves a link endpoint to a node key, preferring an explicit
// device pk, otherwise a sibling create op's local_ref.
func resolveEndpoint(g *kspGraph, localRefs map[string]string, pk, ref string) (string, error) {
	if pk != "" {
		if _, ok := g.Nodes[pk]; !ok {
			return "", fmt.Errorf("device %s not found", pk)
		}
		return pk, nil
	}
	if ref != "" {
		key, ok := localRefs[ref]
		if !ok {
			return "", fmt.Errorf("local_ref %s not resolved (no earlier add_device)", ref)
		}
		return key, nil
	}
	return "", fmt.Errorf("no device pk or ref provided")
}

// guardLinkMetrics rejects create/move ops whose latency or bandwidth would make
// the edge invalid or silently dropped.
func guardLinkMetrics(p plannerPayload) error {
	if p.BandwidthBps == 0 {
		return fmt.Errorf("bandwidth_bps must be > 0")
	}
	if p.LatencyNs == unsetLatencyNs {
		return fmt.Errorf("latency_ns is the unset sentinel (%d); set a real latency", unsetLatencyNs)
	}
	if p.LatencyNs <= 0 && p.MetricOverrideNs <= 0 {
		return fmt.Errorf("latency_ns must be > 0")
	}
	return nil
}

// addLinkEdge inserts a bidirectional edge with the given identity into g.
func addLinkEdge(g *kspGraph, linkPK, linkType, aKey, zKey string, metric uint32, bw uint64) {
	aC := g.Nodes[aKey].ContributorPK
	zC := g.Nodes[zKey].ContributorPK
	g.Adj[aKey] = append(g.Adj[aKey], kspEdge{
		To: zKey, Metric: metric, BandwidthBps: bw, LinkPK: linkPK, LinkType: linkType,
		SideAContributorPK: aC, SideZContributorPK: zC,
	})
	g.Adj[zKey] = append(g.Adj[zKey], kspEdge{
		To: aKey, Metric: metric, BandwidthBps: bw, LinkPK: linkPK, LinkType: linkType,
		SideAContributorPK: zC, SideZContributorPK: aC,
	})
	g.LinkIndex[linkPK] = [2]string{aKey, zKey}
}

// removeLinkByPK removes both directed edges of a link and its index entry.
// Absent link is a no-op.
func removeLinkByPK(g *kspGraph, linkPK string) {
	ep, ok := g.LinkIndex[linkPK]
	if !ok {
		return
	}
	g.Adj[ep[0]] = filterEdges(g.Adj[ep[0]], linkPK)
	g.Adj[ep[1]] = filterEdges(g.Adj[ep[1]], linkPK)
	delete(g.LinkIndex, linkPK)
}

// filterEdges returns a new slice with every edge carrying linkPK removed.
func filterEdges(edges []kspEdge, linkPK string) []kspEdge {
	out := make([]kspEdge, 0, len(edges))
	for _, e := range edges {
		if e.LinkPK != linkPK {
			out = append(out, e)
		}
	}
	return out
}

// applyMoveLinkEnd moves one endpoint of an existing link to a new device,
// keeping the link's identity and applying the (editable) latency/bandwidth the
// operator set. side "a" moves side A (fixed side = Z); side "z" moves side Z
// (fixed side = A).
func applyMoveLinkEnd(g *kspGraph, localRefs map[string]string, c PlanChange, p plannerPayload) error {
	ep, ok := g.LinkIndex[c.RefLinkPK]
	if !ok {
		return fmt.Errorf("move_link_end seq %d: link %s not found", c.Seq, c.RefLinkPK)
	}
	linkType := ""
	for _, e := range g.Adj[ep[0]] {
		if e.LinkPK == c.RefLinkPK {
			linkType = e.LinkType
			break
		}
	}
	side := strings.ToLower(p.Side)
	if side != "a" && side != "z" {
		return fmt.Errorf("move_link_end seq %d: side must be 'a' or 'z'", c.Seq)
	}
	// SC-1/SC-3: the target existing-device pk is the c.NewDevicePK COLUMN, not
	// payload; only new_device_ref (a sibling add_device's local ref) is in payload.
	newKey, err := resolveEndpoint(g, localRefs, c.NewDevicePK, p.NewDeviceRef)
	if err != nil {
		return fmt.Errorf("move_link_end seq %d: %w", c.Seq, err)
	}
	if err := guardLinkMetrics(p); err != nil {
		return fmt.Errorf("move_link_end seq %d: %w", c.Seq, err)
	}
	if p.LinkType != "" {
		linkType = p.LinkType
	}
	// Preserve A/Z ordering: side "a" replaces the A endpoint (new device becomes
	// A, old Z stays Z); side "z" replaces the Z endpoint (old A stays A, new
	// device becomes Z). addLinkEdge treats its first key as A and second as Z,
	// so the order must match to keep LinkIndex and the side_a/side_z
	// contributor derivation correct for later ops on the same link.
	aKey, zKey := ep[0], newKey // side "z": A fixed, Z moves
	if side == "a" {
		aKey, zKey = newKey, ep[1] // A moves, Z fixed
	}
	removeLinkByPK(g, c.RefLinkPK)
	addLinkEdge(g, c.RefLinkPK, linkType, aKey, zKey,
		latencyToMetric(p.LatencyNs, p.MetricOverrideNs), p.BandwidthBps)
	return nil
}

// metroCodeForPK finds the metro code for a metro pk by scanning existing nodes.
// Returns "" if unknown (a lone new metro with no other devices).
func metroCodeForPK(g *kspGraph, metroPK string) string {
	if metroPK == "" {
		return ""
	}
	for _, n := range g.Nodes {
		if n.MetroPK == metroPK && n.MetroCode != "" {
			return n.MetroCode
		}
	}
	return ""
}
