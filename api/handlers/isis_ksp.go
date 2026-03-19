package handlers

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/config"
)

// Graph types for in-memory shortest path computation.

type kspEdge struct {
	To     string
	Metric uint32
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

// loadISISGraph loads the full ISIS adjacency graph from Neo4j into memory.
func loadISISGraph(ctx context.Context) (*kspGraph, error) {
	session := config.Neo4jSession(ctx)
	defer session.Close(ctx)

	g := &kspGraph{
		Adj:   make(map[string][]kspEdge),
		Nodes: make(map[string]kspNodeInfo),
	}

	// Load all edges with metrics and metro info
	edgeCypher := `
		MATCH (from:Device)-[r:ISIS_ADJACENT]->(to:Device)
		OPTIONAL MATCH (from)-[:LOCATED_IN]->(mFrom:Metro)
		OPTIONAL MATCH (to)-[:LOCATED_IN]->(mTo:Metro)
		RETURN from.pk AS from_pk, to.pk AS to_pk, r.metric AS metric,
		       from.code AS from_code, from.status AS from_status, from.device_type AS from_type,
		       mFrom.pk AS from_metro_pk, mFrom.code AS from_metro_code,
		       to.code AS to_code, to.status AS to_status, to.device_type AS to_type,
		       mTo.pk AS to_metro_pk, mTo.code AS to_metro_code
	`

	result, err := session.Run(ctx, edgeCypher, nil)
	if err != nil {
		return nil, fmt.Errorf("loading ISIS graph edges: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("collecting ISIS graph edges: %w", err)
	}

	for _, rec := range records {
		fromPK := asString(recGet(rec, "from_pk"))
		toPK := asString(recGet(rec, "to_pk"))
		metric := uint32(asInt64(recGet(rec, "metric")))
		if metric == 0 {
			metric = 1
		}

		g.Adj[fromPK] = append(g.Adj[fromPK], kspEdge{To: toPK, Metric: metric})

		if _, ok := g.Nodes[fromPK]; !ok {
			g.Nodes[fromPK] = kspNodeInfo{
				PK:         fromPK,
				Code:       asString(recGet(rec, "from_code")),
				Status:     asString(recGet(rec, "from_status")),
				DeviceType: asString(recGet(rec, "from_type")),
				MetroPK:    asString(recGet(rec, "from_metro_pk")),
				MetroCode:  asString(recGet(rec, "from_metro_code")),
			}
		}
		if _, ok := g.Nodes[toPK]; !ok {
			g.Nodes[toPK] = kspNodeInfo{
				PK:         toPK,
				Code:       asString(recGet(rec, "to_code")),
				Status:     asString(recGet(rec, "to_status")),
				DeviceType: asString(recGet(rec, "to_type")),
				MetroPK:    asString(recGet(rec, "to_metro_pk")),
				MetroCode:  asString(recGet(rec, "to_metro_code")),
			}
		}
	}

	slog.Info("loaded ISIS graph", "nodes", len(g.Nodes), "edges", len(records))
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
	g, err := loadISISGraph(ctx)
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

func (h dijkHeap) Len() int           { return len(h) }
func (h dijkHeap) Less(i, j int) bool { return h[i].cost < h[j].cost }
func (h dijkHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dijkHeap) Push(x any)        { *h = append(*h, x.(dijkItem)) }
func (h *dijkHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type candidateHeap []kspPath

func (h candidateHeap) Len() int           { return len(h) }
func (h candidateHeap) Less(i, j int) bool { return h[i].TotalMetric < h[j].TotalMetric }
func (h candidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *candidateHeap) Push(x any)        { *h = append(*h, x.(kspPath)) }
func (h *candidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
