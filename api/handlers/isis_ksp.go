package handlers

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/config"
)

// Graph types for in-memory shortest path computation.

type kspEdge struct {
	To           string
	Metric       uint32
	BandwidthBps uint64
}

type kspGraph struct {
	Adj   map[string][]kspEdge   // adjacency list
	Nodes map[string]kspNodeInfo // node metadata
}

type kspNodeInfo struct {
	PK         string
	Code       string
	Status     string
	DeviceType string
	MetroPK    string
	MetroCode  string
}

type kspPath struct {
	Nodes       []string
	TotalMetric uint32
}

// loadTopologyGraph loads the device/link topology from Neo4j into memory.
// Edge weights use committed_rtt_ns (converted to microseconds) as the metric.
func loadTopologyGraph(ctx context.Context) (*kspGraph, error) {
	session := config.Neo4jSession(ctx)
	defer session.Close(ctx)

	g := &kspGraph{
		Adj:   make(map[string][]kspEdge),
		Nodes: make(map[string]kspNodeInfo),
	}

	// Load all links with committed latency and device/metro info.
	// Links are bidirectional, so we add edges in both directions.
	edgeCypher := `
		MATCH (a:Device)-[:CONNECTS]-(l:Link)-[:CONNECTS]-(b:Device)
		WHERE a.pk < b.pk AND l.status = 'activated'
		OPTIONAL MATCH (a)-[:LOCATED_IN]->(mA:Metro)
		OPTIONAL MATCH (b)-[:LOCATED_IN]->(mB:Metro)
		RETURN a.pk AS a_pk, b.pk AS b_pk,
		       l.committed_rtt_ns AS committed_rtt_ns,
		       coalesce(l.bandwidth, 0) AS bandwidth_bps,
		       a.code AS a_code, a.status AS a_status, a.device_type AS a_type,
		       mA.pk AS a_metro_pk, mA.code AS a_metro_code,
		       b.code AS b_code, b.status AS b_status, b.device_type AS b_type,
		       mB.pk AS b_metro_pk, mB.code AS b_metro_code
	`

	result, err := session.Run(ctx, edgeCypher, nil)
	if err != nil {
		return nil, fmt.Errorf("loading topology graph: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("collecting topology graph: %w", err)
	}

	for _, rec := range records {
		aPK := asString(recGet(rec, "a_pk"))
		bPK := asString(recGet(rec, "b_pk"))
		// Convert committed_rtt_ns to microseconds for uint32 metric
		committedNs := asInt64(recGet(rec, "committed_rtt_ns"))
		metric := uint32(committedNs / 1000)
		if metric == 0 {
			metric = 1
		}
		bwBps := uint64(asInt64(recGet(rec, "bandwidth_bps")))

		// Bidirectional edges
		g.Adj[aPK] = append(g.Adj[aPK], kspEdge{To: bPK, Metric: metric, BandwidthBps: bwBps})
		g.Adj[bPK] = append(g.Adj[bPK], kspEdge{To: aPK, Metric: metric, BandwidthBps: bwBps})

		if _, ok := g.Nodes[aPK]; !ok {
			g.Nodes[aPK] = kspNodeInfo{
				PK:         aPK,
				Code:       asString(recGet(rec, "a_code")),
				Status:     asString(recGet(rec, "a_status")),
				DeviceType: asString(recGet(rec, "a_type")),
				MetroPK:    asString(recGet(rec, "a_metro_pk")),
				MetroCode:  asString(recGet(rec, "a_metro_code")),
			}
		}
		if _, ok := g.Nodes[bPK]; !ok {
			g.Nodes[bPK] = kspNodeInfo{
				PK:         bPK,
				Code:       asString(recGet(rec, "b_code")),
				Status:     asString(recGet(rec, "b_status")),
				DeviceType: asString(recGet(rec, "b_type")),
				MetroPK:    asString(recGet(rec, "b_metro_pk")),
				MetroCode:  asString(recGet(rec, "b_metro_code")),
			}
		}
	}

	// Sort adjacency lists for deterministic traversal order
	for _, edges := range g.Adj {
		slices.SortFunc(edges, func(a, b kspEdge) int {
			if a.Metric != b.Metric {
				if a.Metric < b.Metric {
					return -1
				}
				return 1
			}
			if a.To < b.To {
				return -1
			}
			if a.To > b.To {
				return 1
			}
			return 0
		})
	}

	slog.Info("loaded topology graph", "nodes", len(g.Nodes), "edges", len(records))
	return g, nil
}

// recGet is a helper to extract a value from a neo4j record by key.
func recGet(rec interface{ Get(string) (any, bool) }, key string) any {
	v, _ := rec.Get(key)
	return v
}

// dijkstra finds the shortest path from source to target using edge metrics,
// with support for excluding specific nodes and edges.
// excludeNodes and excludeEdges are sets of items to skip.
// excludeEdges keys are "fromPK->toPK".
func dijkstra(g *kspGraph, source, target string, excludeNodes map[string]bool, excludeEdges map[string]bool) *kspPath {
	if excludeNodes[source] || excludeNodes[target] {
		return nil
	}

	dist := make(map[string]uint32)
	prev := make(map[string]string)
	dist[source] = 0

	h := &dijkHeap{{node: source, cost: 0}}
	heap.Init(h)

	for h.Len() > 0 {
		cur := heap.Pop(h).(dijkItem)
		if cur.cost > dist[cur.node] {
			continue
		}
		if cur.node == target {
			break
		}

		for _, edge := range g.Adj[cur.node] {
			if excludeNodes[edge.To] {
				continue
			}
			edgeKey := cur.node + "->" + edge.To
			if excludeEdges[edgeKey] {
				continue
			}

			newCost := cur.cost + edge.Metric
			if old, ok := dist[edge.To]; !ok || newCost < old {
				dist[edge.To] = newCost
				prev[edge.To] = cur.node
				heap.Push(h, dijkItem{node: edge.To, cost: newCost})
			}
		}
	}

	if _, ok := dist[target]; !ok {
		return nil
	}

	// Reconstruct path
	var nodes []string
	for n := target; n != ""; n = prev[n] {
		nodes = append(nodes, n)
		if n == source {
			break
		}
	}
	if len(nodes) == 0 || nodes[len(nodes)-1] != source {
		return nil
	}

	// Reverse
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}

	return &kspPath{
		Nodes:       nodes,
		TotalMetric: dist[target],
	}
}

// yenKSP implements Yen's K-Shortest Paths algorithm.
// Returns up to k shortest (lowest total metric) loopless paths from source to target.
func yenKSP(g *kspGraph, source, target string, k int) []kspPath {
	// Find the first shortest path using Dijkstra
	first := dijkstra(g, source, target, nil, nil)
	if first == nil {
		return nil
	}

	A := []kspPath{*first} // k-shortest paths found so far
	B := &candidateHeap{}  // candidate paths
	heap.Init(B)
	seen := make(map[string]bool) // dedup candidates by node sequence

	for i := 1; i < k; i++ {
		prevPath := A[i-1]

		for j := 0; j < len(prevPath.Nodes)-1; j++ {
			spurNode := prevPath.Nodes[j]
			rootPath := prevPath.Nodes[:j+1]

			// Calculate root path cost
			var rootCost uint32
			for ri := 0; ri < len(rootPath)-1; ri++ {
				rootCost += edgeMetric(g, rootPath[ri], rootPath[ri+1])
			}

			// Exclude edges from spur node that are used by existing shortest paths
			// with the same root path prefix
			excludeEdges := make(map[string]bool)
			for _, p := range A {
				if len(p.Nodes) > j && pathPrefixMatch(p.Nodes, rootPath) {
					excludeEdges[p.Nodes[j]+"->"+p.Nodes[j+1]] = true
				}
			}

			// Exclude nodes in root path (except spur node) to prevent loops
			excludeNodes := make(map[string]bool)
			for _, n := range rootPath[:j] {
				excludeNodes[n] = true
			}

			// Find spur path
			spurPath := dijkstra(g, spurNode, target, excludeNodes, excludeEdges)
			if spurPath == nil {
				continue
			}

			// Combine root + spur
			totalNodes := make([]string, len(rootPath)-1)
			copy(totalNodes, rootPath[:len(rootPath)-1])
			totalNodes = append(totalNodes, spurPath.Nodes...)
			totalCost := rootCost + spurPath.TotalMetric

			key := strings.Join(totalNodes, ",")
			if seen[key] {
				continue
			}
			seen[key] = true

			heap.Push(B, kspPath{Nodes: totalNodes, TotalMetric: totalCost})
		}

		if B.Len() == 0 {
			break
		}

		best := heap.Pop(B).(kspPath)
		A = append(A, best)
	}

	return A
}

func edgeMetric(g *kspGraph, from, to string) uint32 {
	for _, e := range g.Adj[from] {
		if e.To == to {
			return e.Metric
		}
	}
	return 1
}

func pathPrefixMatch(path, prefix []string) bool {
	if len(path) < len(prefix) {
		return false
	}
	for i, n := range prefix {
		if path[i] != n {
			return false
		}
	}
	return true
}

// kspToSinglePaths converts Yen's output to the API response format.
func kspToSinglePaths(g *kspGraph, paths []kspPath) []SinglePath {
	result := make([]SinglePath, 0, len(paths))

	for _, p := range paths {
		hops := make([]MultiPathHop, len(p.Nodes))
		for i, nodePK := range p.Nodes {
			info := g.Nodes[nodePK]
			hops[i] = MultiPathHop{
				DevicePK:   info.PK,
				DeviceCode: info.Code,
				Status:     info.Status,
				DeviceType: info.DeviceType,
				MetroPK:    info.MetroPK,
				MetroCode:  info.MetroCode,
			}
			// Set edge metric on each hop (metric of the edge arriving at this node)
			if i > 0 {
				hops[i].EdgeMetric = edgeMetric(g, p.Nodes[i-1], p.Nodes[i])
			}
		}

		result = append(result, SinglePath{
			Path:        hops,
			TotalMetric: p.TotalMetric,
			HopCount:    len(p.Nodes) - 1,
		})
	}

	return result
}

// findKShortestPaths loads the graph and runs Yen's algorithm.
func findKShortestPaths(ctx context.Context, fromPK, toPK string, k int) ([]SinglePath, error) {
	start := time.Now()
	g, err := loadTopologyGraph(ctx)
	if err != nil {
		return nil, err
	}
	loadDur := time.Since(start)

	pathStart := time.Now()
	paths := yenKSP(g, fromPK, toPK, k)
	pathDur := time.Since(pathStart)

	slog.Info("Yen's KSP completed", "k", k, "found", len(paths), "graphLoad", loadDur, "pathfind", pathDur)

	if len(paths) == 0 {
		return nil, nil
	}

	return kspToSinglePaths(g, paths), nil
}

// --- Priority queue implementations ---

type dijkItem struct {
	node string
	cost uint32
}

type dijkHeap []dijkItem

func (h dijkHeap) Len() int { return len(h) }
func (h dijkHeap) Less(i, j int) bool {
	if h[i].cost != h[j].cost {
		return h[i].cost < h[j].cost
	}
	return h[i].node < h[j].node // stable tie-break by node PK
}
func (h dijkHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *dijkHeap) Push(x any)   { *h = append(*h, x.(dijkItem)) }
func (h *dijkHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type candidateHeap []kspPath

func (h candidateHeap) Len() int { return len(h) }
func (h candidateHeap) Less(i, j int) bool {
	if h[i].TotalMetric != h[j].TotalMetric {
		return h[i].TotalMetric < h[j].TotalMetric
	}
	// Stable tie-break: fewer hops first, then lexicographic node order
	if len(h[i].Nodes) != len(h[j].Nodes) {
		return len(h[i].Nodes) < len(h[j].Nodes)
	}
	for k := range h[i].Nodes {
		if h[i].Nodes[k] != h[j].Nodes[k] {
			return h[i].Nodes[k] < h[j].Nodes[k]
		}
	}
	return false
}
func (h candidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *candidateHeap) Push(x any)   { *h = append(*h, x.(kspPath)) }
func (h *candidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// metroPairPath holds the best path between two metros.
type metroPairPath struct {
	FromMetroPK   string
	FromMetroCode string
	ToMetroPK     string
	ToMetroCode   string
	Path          *kspPath
	HopCount      int
	BottleneckBps uint64
}

// computeMetroPairPaths finds the shortest path between every pair of metros
// using the in-memory topology graph with committed latency as edge weights.
func computeMetroPairPaths(g *kspGraph) []metroPairPath {
	// Group device PKs by metro
	metroDevices := make(map[string][]string) // metroCode -> []devicePK
	metroInfo := make(map[string]struct {
		pk   string
		code string
	})

	for pk, info := range g.Nodes {
		if info.MetroCode == "" {
			continue
		}
		metroDevices[info.MetroCode] = append(metroDevices[info.MetroCode], pk)
		metroInfo[info.MetroCode] = struct {
			pk   string
			code string
		}{pk: info.MetroPK, code: info.MetroCode}
	}

	// Collect sorted metro codes for deterministic ordering
	metroCodes := make([]string, 0, len(metroDevices))
	for code := range metroDevices {
		metroCodes = append(metroCodes, code)
	}
	slices.Sort(metroCodes)

	var results []metroPairPath

	// For each unique metro pair, find the best path across all device pairs
	for i := 0; i < len(metroCodes); i++ {
		for j := i + 1; j < len(metroCodes); j++ {
			m1Code := metroCodes[i]
			m2Code := metroCodes[j]
			m1Info := metroInfo[m1Code]
			m2Info := metroInfo[m2Code]

			var bestPath *kspPath
			for _, d1 := range metroDevices[m1Code] {
				for _, d2 := range metroDevices[m2Code] {
					p := dijkstra(g, d1, d2, nil, nil)
					if p != nil && (bestPath == nil || p.TotalMetric < bestPath.TotalMetric) {
						bestPath = p
					}
				}
			}

			if bestPath == nil {
				continue
			}

			// Compute hop count and bottleneck bandwidth
			hopCount := len(bestPath.Nodes) - 1
			var bottleneckBps uint64 = 0
			for k := 0; k < hopCount; k++ {
				bw := edgeBandwidth(g, bestPath.Nodes[k], bestPath.Nodes[k+1])
				if bw > 0 && (bottleneckBps == 0 || bw < bottleneckBps) {
					bottleneckBps = bw
				}
			}

			results = append(results, metroPairPath{
				FromMetroPK:   m1Info.pk,
				FromMetroCode: m1Info.code,
				ToMetroPK:     m2Info.pk,
				ToMetroCode:   m2Info.code,
				Path:          bestPath,
				HopCount:      hopCount,
				BottleneckBps: bottleneckBps,
			})
		}
	}

	return results
}

// computeMetroPathDetail finds the best path between two specific metros
// and returns the hop-by-hop breakdown.
func computeMetroPathDetail(g *kspGraph, fromCode, toCode string) *metroPairPath {
	// Find devices in each metro
	var fromDevices, toDevices []string
	var fromMetroPK, toMetroPK string
	for pk, info := range g.Nodes {
		if info.MetroCode == fromCode {
			fromDevices = append(fromDevices, pk)
			fromMetroPK = info.MetroPK
		} else if info.MetroCode == toCode {
			toDevices = append(toDevices, pk)
			toMetroPK = info.MetroPK
		}
	}

	if len(fromDevices) == 0 || len(toDevices) == 0 {
		return nil
	}

	var bestPath *kspPath
	for _, d1 := range fromDevices {
		for _, d2 := range toDevices {
			p := dijkstra(g, d1, d2, nil, nil)
			if p != nil && (bestPath == nil || p.TotalMetric < bestPath.TotalMetric) {
				bestPath = p
			}
		}
	}

	if bestPath == nil {
		return nil
	}

	hopCount := len(bestPath.Nodes) - 1
	var bottleneckBps uint64 = 0
	for k := 0; k < hopCount; k++ {
		bw := edgeBandwidth(g, bestPath.Nodes[k], bestPath.Nodes[k+1])
		if bw > 0 && (bottleneckBps == 0 || bw < bottleneckBps) {
			bottleneckBps = bw
		}
	}

	return &metroPairPath{
		FromMetroPK:   fromMetroPK,
		FromMetroCode: fromCode,
		ToMetroPK:     toMetroPK,
		ToMetroCode:   toCode,
		Path:          bestPath,
		HopCount:      hopCount,
		BottleneckBps: bottleneckBps,
	}
}

// edgeBandwidth returns the bandwidth of the edge from→to in the graph.
func edgeBandwidth(g *kspGraph, from, to string) uint64 {
	for _, e := range g.Adj[from] {
		if e.To == to {
			return e.BandwidthBps
		}
	}
	return 0
}
