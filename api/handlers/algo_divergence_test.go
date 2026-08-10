package handlers

import "testing"

// buildGraph makes a bidirectional graph from "a-b:metricUs" style edges,
// putting each device in a metro of the same name so a device path reads as a
// metro path.
func buildGraph(edges map[[2]string]uint32) *kspGraph {
	g := &kspGraph{Adj: make(map[string][]kspEdge), Nodes: make(map[string]kspNodeInfo)}
	node := func(pk string) {
		if _, ok := g.Nodes[pk]; !ok {
			g.Nodes[pk] = kspNodeInfo{PK: pk, Code: pk, MetroPK: pk, MetroCode: pk}
		}
	}
	for pair, metric := range edges {
		a, b := pair[0], pair[1]
		node(a)
		node(b)
		g.Adj[a] = append(g.Adj[a], kspEdge{To: b, Metric: metric})
		g.Adj[b] = append(g.Adj[b], kspEdge{To: a, Metric: metric})
	}
	return g
}

func findPair(pairs []AlgoDivergencePair, from, to string) *AlgoDivergencePair {
	for i := range pairs {
		if pairs[i].FromMetro == from && pairs[i].ToMetro == to {
			return &pairs[i]
		}
	}
	return nil
}

// fra and tyo have a direct link that carries multicast only, so unicast has
// to go the long way round through lon. This is the shape the report exists
// to find: a link nobody tagged that quietly moved a route.
func TestDivergingPairsReportsTheDetour(t *testing.T) {
	// lon-tyo is priced so both link sets agree on it, leaving fra-tyo as the
	// only pair the missing tag moves.
	all := buildGraph(map[[2]string]uint32{
		{"fra", "tyo"}: 134_000,
		{"fra", "lon"}: 11_000,
		{"lon", "tyo"}: 140_000,
	})
	tagged := buildGraph(map[[2]string]uint32{
		{"fra", "lon"}: 11_000,
		{"lon", "tyo"}: 140_000,
	})

	pairs, multicastPairs := divergingPairs(all, tagged)

	if multicastPairs != 3 {
		t.Fatalf("multicast pairs = %d, want 3", multicastPairs)
	}
	if len(pairs) != 1 {
		t.Fatalf("diverging pairs = %d, want 1: %+v", len(pairs), pairs)
	}

	p := pairs[0]
	if p.FromMetro != "fra" || p.ToMetro != "tyo" {
		t.Fatalf("diverging pair = %s-%s, want fra-tyo", p.FromMetro, p.ToMetro)
	}
	if p.MulticastMs != 134 || p.UnicastMs != 151 {
		t.Errorf("multicast=%.2f unicast=%.2f, want 134 and 151", p.MulticastMs, p.UnicastMs)
	}
	if p.DeltaMs != 17 {
		t.Errorf("delta = %.2f ms, want 17", p.DeltaMs)
	}
	if !p.UnicastReachable {
		t.Error("pair reports unicast unreachable, but the detour exists")
	}
	// The path is what lets a reader audit the number, so it must be the
	// unicast one and not a copy of the multicast path.
	if got := p.UnicastPath; len(got) != 3 || got[1] != "lon" {
		t.Errorf("unicast path = %v, want fra-lon-tyo", got)
	}
	if got := p.MulticastPath; len(got) != 2 {
		t.Errorf("multicast path = %v, want the direct fra-tyo", got)
	}
}

// A pair unicast cannot reach at all is worse than any delta, so it must be
// reported rather than skipped for having no unicast figure to subtract.
func TestDivergingPairsReportsUnreachable(t *testing.T) {
	all := buildGraph(map[[2]string]uint32{
		{"fra", "tyo"}: 134_000,
		{"fra", "lon"}: 11_000,
	})
	tagged := buildGraph(map[[2]string]uint32{
		{"fra", "lon"}: 11_000,
	})

	pairs, _ := divergingPairs(all, tagged)

	fraTyo := findPair(pairs, "fra", "tyo")
	lonTyo := findPair(pairs, "lon", "tyo")
	if fraTyo == nil || lonTyo == nil {
		t.Fatalf("want both tyo pairs reported, got %+v", pairs)
	}
	for _, p := range []*AlgoDivergencePair{fraTyo, lonTyo} {
		if p.UnicastReachable {
			t.Errorf("%s-%s reports reachable, but tyo has no tagged link", p.FromMetro, p.ToMetro)
		}
		if p.UnicastMs != 0 || p.DeltaMs != 0 {
			t.Errorf("%s-%s carries unicast figures it cannot have: %+v", p.FromMetro, p.ToMetro, p)
		}
	}
	// Unreachable sorts ahead of any delta.
	if pairs[0].UnicastReachable {
		t.Error("a reachable pair sorted above an unreachable one")
	}
}

// Identical link sets must produce an empty report, not a list of zero deltas.
func TestDivergingPairsSilentWhenSetsMatch(t *testing.T) {
	edges := map[[2]string]uint32{
		{"fra", "lon"}: 11_000,
		{"lon", "tyo"}: 200_000,
	}
	pairs, multicastPairs := divergingPairs(buildGraph(edges), buildGraph(edges))

	if len(pairs) != 0 {
		t.Errorf("diverging pairs = %+v, want none", pairs)
	}
	if multicastPairs != 3 {
		t.Errorf("multicast pairs = %d, want 3", multicastPairs)
	}
}

// An extra tagged link that no best path uses changes nothing, so a pair that
// is merely routed differently at the same cost is not a divergence.
func TestDivergingPairsIgnoresEqualCostAlternatives(t *testing.T) {
	all := buildGraph(map[[2]string]uint32{
		{"fra", "lon"}: 10_000,
		{"fra", "ams"}: 5_000,
		{"ams", "lon"}: 5_000,
	})
	tagged := buildGraph(map[[2]string]uint32{
		{"fra", "ams"}: 5_000,
		{"ams", "lon"}: 5_000,
	})

	pairs, _ := divergingPairs(all, tagged)
	if len(pairs) != 0 {
		t.Errorf("diverging pairs = %+v, want none: both sets reach fra-lon in 10 ms", pairs)
	}
}
