package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ISISNode represents a device node in the ISIS topology graph
type ISISNode struct {
	Data ISISNodeData `json:"data"`
}

type ISISNodeData struct {
	ID         string `json:"id"`
	Label      string `json:"label"`
	Status     string `json:"status"`
	DeviceType string `json:"deviceType"`
	MetroPK    string `json:"metroPK,omitempty"`
	SystemID   string `json:"systemId,omitempty"`
	RouterID   string `json:"routerId,omitempty"`
}

// ISISEdge represents an adjacency edge in the ISIS topology graph
type ISISEdge struct {
	Data ISISEdgeData `json:"data"`
}

type ISISEdgeData struct {
	ID           string   `json:"id"`
	Source       string   `json:"source"`
	Target       string   `json:"target"`
	Metric       uint32   `json:"metric,omitempty"`
	AdjSIDs      []uint32 `json:"adjSids,omitempty"`
	NeighborAddr string   `json:"neighborAddr,omitempty"`
}

// ISISTopologyResponse is the response for the ISIS topology endpoint
type ISISTopologyResponse struct {
	Nodes []ISISNode `json:"nodes"`
	Edges []ISISEdge `json:"edges"`
	Error string     `json:"error,omitempty"`
}

// GetISISTopology returns the full ISIS topology graph from ClickHouse
func (a *API) GetISISTopology(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()

	response := ISISTopologyResponse{
		Nodes: []ISISNode{},
		Edges: []ISISEdge{},
	}

	// Get ISIS devices joined with device metadata
	deviceQuery := `
		SELECT
			id.device_pk AS pk,
			COALESCE(d.code, id.hostname) AS code,
			COALESCE(d.status, '') AS status,
			COALESCE(d.device_type, '') AS device_type,
			id.system_id AS system_id,
			id.router_id AS router_id,
			COALESCE(d.metro_pk, '') AS metro_pk
		FROM isis_devices_current id
		LEFT JOIN dz_devices_current d ON id.device_pk = d.pk
		WHERE id.device_pk != ''
	`
	deviceRows, err := a.envDB(ctx).Query(ctx, deviceQuery)
	if err != nil {
		logError("ISIS topology device query error", "error", err)
		response.Error = "Failed to query ISIS devices"
		writeJSON(w, response)
		return
	}
	defer deviceRows.Close()

	for deviceRows.Next() {
		var pk, code, status, deviceType, systemID, routerID, metroPK string
		if err := deviceRows.Scan(&pk, &code, &status, &deviceType, &systemID, &routerID, &metroPK); err != nil {
			logError("ISIS topology device scan error", "error", err)
			continue
		}
		response.Nodes = append(response.Nodes, ISISNode{
			Data: ISISNodeData{
				ID:         pk,
				Label:      code,
				Status:     status,
				DeviceType: deviceType,
				SystemID:   systemID,
				RouterID:   routerID,
				MetroPK:    metroPK,
			},
		})
	}

	// Get ISIS adjacencies with neighbor device_pk resolved via hostname
	adjQuery := `
		SELECT
			a.device_pk AS from_pk,
			nd.device_pk AS to_pk,
			a.metric AS metric,
			a.neighbor_addr AS neighbor_addr,
			a.adj_sids AS adj_sids
		FROM isis_adjacencies_current a
		JOIN isis_devices_current nd ON a.neighbor_system_id = concat(nd.hostname, '.00')
		WHERE a.device_pk != '' AND nd.device_pk != ''
	`
	adjRows, err := a.envDB(ctx).Query(ctx, adjQuery)
	if err != nil {
		logError("ISIS topology adjacency query error", "error", err)
		response.Error = "Failed to query ISIS adjacencies"
		writeJSON(w, response)
		return
	}
	defer adjRows.Close()

	for adjRows.Next() {
		var fromPK, toPK, neighborAddr, adjSidsJSON string
		var metric int64
		if err := adjRows.Scan(&fromPK, &toPK, &metric, &neighborAddr, &adjSidsJSON); err != nil {
			logError("ISIS topology adjacency scan error", "error", err)
			continue
		}

		var adjSIDs []uint32
		if adjSidsJSON != "" && adjSidsJSON != "[]" {
			var sids []json.Number
			if json.Unmarshal([]byte(adjSidsJSON), &sids) == nil {
				for _, s := range sids {
					if v, err := s.Int64(); err == nil {
						adjSIDs = append(adjSIDs, uint32(v))
					}
				}
			}
		}

		response.Edges = append(response.Edges, ISISEdge{
			Data: ISISEdgeData{
				ID:           fromPK + "->" + toPK,
				Source:       fromPK,
				Target:       toPK,
				Metric:       uint32(metric),
				NeighborAddr: neighborAddr,
				AdjSIDs:      adjSIDs,
			},
		})
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	writeJSON(w, response)
}

// Helper functions

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asInt64(v any) int64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func asFloat64(v any) float64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logError("failed to encode response", "error", err)
	}
}

// PathHop represents a hop in a path
type PathHop struct {
	DevicePK   string `json:"devicePK"`
	DeviceCode string `json:"deviceCode"`
	Status     string `json:"status"`
	DeviceType string `json:"deviceType"`
}

// PathResponse is the response for the path endpoint
type PathResponse struct {
	Path        []PathHop `json:"path"`
	TotalMetric uint32    `json:"totalMetric"`
	HopCount    int       `json:"hopCount"`
	Error       string    `json:"error,omitempty"`
}

// GetISISPath finds the shortest path between two devices using ISIS metrics
func (a *API) GetISISPath(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	fromPK := r.URL.Query().Get("from")
	toPK := r.URL.Query().Get("to")

	if fromPK == "" || toPK == "" {
		writeJSON(w, PathResponse{Error: "from and to parameters are required"})
		return
	}

	if fromPK == toPK {
		writeJSON(w, PathResponse{Error: "from and to must be different devices"})
		return
	}

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	// Find shortest path with total ISIS metric
	cypher := `
		MATCH (a:Device {pk: $from_pk}), (b:Device {pk: $to_pk})
		MATCH path = shortestPath((a)-[:ISIS_ADJACENT*]->(b))
		WITH path, reduce(total = 0, r IN relationships(path) | total + coalesce(r.metric, 0)) AS total_metric
		RETURN [n IN nodes(path) | {
			pk: n.pk,
			code: n.code,
			status: n.status,
			device_type: n.device_type
		}] AS devices,
		total_metric
	`

	result, err := session.Run(ctx, cypher, map[string]any{
		"from_pk": fromPK,
		"to_pk":   toPK,
	})
	if err != nil {
		logError("ISIS path query error", "error", err)
		writeJSON(w, PathResponse{Error: "Failed to find path: " + err.Error()})
		return
	}

	record, err := result.Single(ctx)
	if err != nil {
		logError("ISIS path no result", "error", err)
		writeJSON(w, PathResponse{Error: "No path found between devices"})
		return
	}

	devicesVal, _ := record.Get("devices")
	totalMetric, _ := record.Get("total_metric")

	path := parsePathHops(devicesVal)

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	writeJSON(w, PathResponse{
		Path:        path,
		TotalMetric: uint32(asInt64(totalMetric)),
		HopCount:    len(path) - 1,
	})
}

func parsePathHops(v any) []PathHop {
	if v == nil {
		return []PathHop{}
	}
	arr, ok := v.([]any)
	if !ok {
		return []PathHop{}
	}
	hops := make([]PathHop, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		hops = append(hops, PathHop{
			DevicePK:   asString(m["pk"]),
			DeviceCode: asString(m["code"]),
			Status:     asString(m["status"]),
			DeviceType: asString(m["device_type"]),
		})
	}
	return hops
}

// TopologyDiscrepancy represents a mismatch between configured and ISIS topology
type TopologyDiscrepancy struct {
	Type        string `json:"type"` // "missing_isis", "partial_isis", "extra_isis"
	LinkPK      string `json:"linkPK,omitempty"`
	LinkCode    string `json:"linkCode,omitempty"`
	LinkStatus  string `json:"linkStatus,omitempty"` // "activated", "soft-drained", "provisioning"
	DeviceAPK   string `json:"deviceAPK"`
	DeviceACode string `json:"deviceACode"`
	DeviceBPK   string `json:"deviceBPK"`
	DeviceBCode string `json:"deviceBCode"`
	ISISMetric  uint32 `json:"isisMetric,omitempty"`
	Details     string `json:"details"`
}

// TopologyCompareResponse is the response for the topology compare endpoint
type TopologyCompareResponse struct {
	ConfiguredLinks int                   `json:"configuredLinks"`
	ISISAdjacencies int                   `json:"isisAdjacencies"`
	MatchedLinks    int                   `json:"matchedLinks"`
	Discrepancies   []TopologyDiscrepancy `json:"discrepancies"`
	Error           string                `json:"error,omitempty"`
}

// GetTopologyCompare compares configured links vs ISIS adjacencies using ClickHouse
func (a *API) GetTopologyCompare(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()

	response := TopologyCompareResponse{
		Discrepancies: []TopologyDiscrepancy{},
	}

	db := a.envDB(ctx)

	// Build ISIS adjacency data keyed by link_pk.
	// Each adjacency is directional (device_pk -> neighbor), so a fully matched
	// link should have two adjacencies with its link_pk (one per direction).
	type adjDirection struct {
		fromPK       string
		toPK         string
		metric       int64
		neighborAddr string
	}
	// link_pk -> list of directional adjacencies
	linkAdjMap := make(map[string][]adjDirection)
	// Adjacencies with no link_pk (extra — not matched to any configured link)
	var unmatchedAdjs []adjDirection

	adjQuery := `
		SELECT
			a.device_pk AS from_pk,
			nd.device_pk AS to_pk,
			a.link_pk,
			a.metric,
			a.neighbor_addr
		FROM isis_adjacencies_current a
		JOIN isis_devices_current nd ON a.neighbor_system_id = concat(nd.hostname, '.00')
		WHERE a.device_pk != '' AND nd.device_pk != ''
	`
	adjRows, err := db.Query(ctx, adjQuery)
	if err != nil {
		logError("topology compare adjacency query error", "error", err)
		response.Error = "Failed to query ISIS adjacencies"
		writeJSON(w, response)
		return
	}
	defer adjRows.Close()

	totalAdjs := 0
	for adjRows.Next() {
		var fromPK, toPK, linkPK, neighborAddr string
		var metric int64
		if err := adjRows.Scan(&fromPK, &toPK, &linkPK, &metric, &neighborAddr); err != nil {
			logError("topology compare adjacency scan error", "error", err)
			continue
		}
		totalAdjs++
		dir := adjDirection{fromPK: fromPK, toPK: toPK, metric: metric, neighborAddr: neighborAddr}
		if linkPK == "" {
			unmatchedAdjs = append(unmatchedAdjs, dir)
		} else {
			linkAdjMap[linkPK] = append(linkAdjMap[linkPK], dir)
		}
	}

	response.ISISAdjacencies = totalAdjs

	// Get configured links
	type linkInfo struct {
		pk, code, status string
		committedRttNs   int64
		tunnelNet        string
		deviceAPK        string
		deviceACode      string
		deviceBPK        string
		deviceBCode      string
	}

	linkQuery := `
		SELECT
			l.pk, l.code, l.status, l.committed_rtt_ns, l.tunnel_net,
			l.side_a_pk, COALESCE(da.code, '') AS side_a_code,
			l.side_z_pk, COALESCE(dz.code, '') AS side_z_code
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
	`
	linkRows, err := db.Query(ctx, linkQuery)
	if err != nil {
		logError("topology compare link query error", "error", err)
		response.Error = "Failed to query configured links"
		writeJSON(w, response)
		return
	}
	defer linkRows.Close()

	var links []linkInfo
	for linkRows.Next() {
		var li linkInfo
		if err := linkRows.Scan(&li.pk, &li.code, &li.status, &li.committedRttNs, &li.tunnelNet, &li.deviceAPK, &li.deviceACode, &li.deviceBPK, &li.deviceBCode); err != nil {
			logError("topology compare link scan error", "error", err)
			continue
		}
		links = append(links, li)
	}

	response.ConfiguredLinks = len(links)

	// Build sibling tunnel_net set: tunnel_nets that have at least one link with an adjacency.
	// If a link is missing its adjacency but a sibling on the same /31 has one,
	// it's a data quality issue (duplicate tunnel_net), not a real missing adjacency.
	siblingTunnelNets := make(map[string]bool)
	for _, li := range links {
		if li.tunnelNet != "" && len(linkAdjMap[li.pk]) > 0 {
			siblingTunnelNets[li.tunnelNet] = true
		}
	}

	// Compare each configured link against its ISIS adjacencies
	for _, li := range links {
		adjs := linkAdjMap[li.pk]
		hasForward := false
		hasReverse := false

		for _, a := range adjs {
			if a.fromPK == li.deviceAPK && a.toPK == li.deviceBPK {
				hasForward = true
			} else if a.fromPK == li.deviceBPK && a.toPK == li.deviceAPK {
				hasReverse = true
			} else {
				// Adjacency matched by link_pk — count as matched regardless of direction
				hasForward = true
				hasReverse = true
			}
		}

		effectiveStatus := li.status
		if li.committedRttNs == committedRttProvisioningNs {
			effectiveStatus = "provisioning"
		}

		// Normalize device order for consistent display
		devAPK, devACode := li.deviceAPK, li.deviceACode
		devBPK, devBCode := li.deviceBPK, li.deviceBCode
		if devAPK > devBPK {
			devAPK, devACode, devBPK, devBCode = devBPK, devBCode, devAPK, devACode
		}

		if hasForward || hasReverse {
			response.MatchedLinks++
			continue
		}

		// No adjacency — check if this is a sibling tunnel_net false positive
		if li.tunnelNet != "" && siblingTunnelNets[li.tunnelNet] {
			// Another link on the same /31 has the adjacency — data quality issue, not missing
			response.MatchedLinks++
			continue
		}

		response.Discrepancies = append(response.Discrepancies, TopologyDiscrepancy{
			Type:        "missing_isis",
			LinkPK:      li.pk,
			LinkCode:    li.code,
			LinkStatus:  effectiveStatus,
			DeviceAPK:   devAPK,
			DeviceACode: devACode,
			DeviceBPK:   devBPK,
			DeviceBCode: devBCode,
			Details:     "Link has no ISIS adjacency in either direction",
		})
	}

	// Extra ISIS adjacencies: those with no link_pk
	isisDeviceCodes := make(map[string]string)
	codeRows, err := db.Query(ctx, `SELECT id.device_pk, COALESCE(d.code, id.hostname) FROM isis_devices_current id LEFT JOIN dz_devices_current d ON id.device_pk = d.pk WHERE id.device_pk != ''`)
	if err == nil {
		defer codeRows.Close()
		for codeRows.Next() {
			var pk, code string
			if codeRows.Scan(&pk, &code) == nil {
				isisDeviceCodes[pk] = code
			}
		}
	}

	// Dedupe extra adjacencies by sorted device pair
	seenExtraPairs := make(map[[2]string]bool)
	for _, adj := range unmatchedAdjs {
		a, b := adj.fromPK, adj.toPK
		if a > b {
			a, b = b, a
		}
		if seenExtraPairs[[2]string{a, b}] {
			continue
		}
		seenExtraPairs[[2]string{a, b}] = true

		response.Discrepancies = append(response.Discrepancies, TopologyDiscrepancy{
			Type:        "extra_isis",
			DeviceAPK:   adj.fromPK,
			DeviceACode: isisDeviceCodes[adj.fromPK],
			DeviceBPK:   adj.toPK,
			DeviceBCode: isisDeviceCodes[adj.toPK],
			ISISMetric:  uint32(adj.metric),
			Details:     "ISIS adjacency exists (neighbor: " + adj.neighborAddr + ") but no configured link found",
		})
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	writeJSON(w, response)
}

func asBool(v any) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// ImpactDevice represents a device that would be affected by a failure
type ImpactDevice struct {
	PK         string `json:"pk"`
	Code       string `json:"code"`
	Status     string `json:"status"`
	DeviceType string `json:"deviceType"`
}

// MetroImpact represents the impact of a device failure on a metro
type MetroImpact struct {
	PK               string `json:"pk"`
	Code             string `json:"code"`
	Name             string `json:"name"`
	TotalDevices     int    `json:"totalDevices"`     // Total ISIS devices in this metro
	RemainingDevices int    `json:"remainingDevices"` // Devices still reachable after failure
	IsolatedDevices  int    `json:"isolatedDevices"`  // Devices that become unreachable
}

// FailureImpactPath represents a path affected by device failure
type FailureImpactPath struct {
	FromPK       string `json:"fromPK"`
	FromCode     string `json:"fromCode"`
	ToPK         string `json:"toPK"`
	ToCode       string `json:"toCode"`
	BeforeHops   int    `json:"beforeHops"`
	BeforeMetric uint32 `json:"beforeMetric"`
	AfterHops    int    `json:"afterHops,omitempty"`   // 0 if no alternate path
	AfterMetric  uint32 `json:"afterMetric,omitempty"` // 0 if no alternate path
	HasAlternate bool   `json:"hasAlternate"`
}

// FailureImpactResponse is the response for the failure impact endpoint
type FailureImpactResponse struct {
	DevicePK           string              `json:"devicePK"`
	DeviceCode         string              `json:"deviceCode"`
	UnreachableDevices []ImpactDevice      `json:"unreachableDevices"`
	UnreachableCount   int                 `json:"unreachableCount"`
	AffectedPaths      []FailureImpactPath `json:"affectedPaths"`
	AffectedPathCount  int                 `json:"affectedPathCount"`
	MetroImpact        []MetroImpact       `json:"metroImpact"`
	Error              string              `json:"error,omitempty"`
}

// GetFailureImpact returns devices that would become unreachable if a device goes down
func (a *API) GetFailureImpact(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Get device PK from URL path
	devicePK := r.PathValue("pk")
	if devicePK == "" {
		writeJSON(w, FailureImpactResponse{Error: "device pk is required"})
		return
	}

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := FailureImpactResponse{
		DevicePK:           devicePK,
		UnreachableDevices: []ImpactDevice{},
		AffectedPaths:      []FailureImpactPath{},
		MetroImpact:        []MetroImpact{},
	}

	// First get the device code
	deviceCypher := `MATCH (d:Device {pk: $pk}) RETURN d.code AS code`
	deviceResult, err := session.Run(ctx, deviceCypher, map[string]any{"pk": devicePK})
	if err != nil {
		logError("failure impact device query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}
	if deviceRecord, err := deviceResult.Single(ctx); err == nil {
		code, _ := deviceRecord.Get("code")
		response.DeviceCode = asString(code)
	}

	// Find devices that would become unreachable if this device goes down
	// Strategy: Find a reference device (most connected, not the target), then find all devices
	// reachable from it without going through the target. Unreachable = ISIS devices not in that set.
	impactCypher := `
		// First, find a good reference device (most ISIS adjacencies, not the target)
		MATCH (ref:Device)-[:ISIS_ADJACENT]-()
		WHERE ref.pk <> $device_pk AND ref.isis_system_id IS NOT NULL
		WITH ref, count(*) AS adjCount
		ORDER BY adjCount DESC
		LIMIT 1

		// Find all devices reachable from reference without going through target
		CALL {
			WITH ref
			MATCH (target:Device {pk: $device_pk})
			MATCH path = (ref)-[:ISIS_ADJACENT*0..20]-(reachable:Device)
			WHERE reachable.isis_system_id IS NOT NULL
			  AND NONE(n IN nodes(path) WHERE n.pk = $device_pk)
			RETURN DISTINCT reachable
		}

		// Find all ISIS devices
		WITH collect(reachable.pk) AS reachablePKs
		MATCH (d:Device)
		WHERE d.isis_system_id IS NOT NULL
		  AND d.pk <> $device_pk
		  AND NOT d.pk IN reachablePKs
		RETURN d.pk AS pk,
		       d.code AS code,
		       d.status AS status,
		       d.device_type AS device_type
	`

	impactResult, err := session.Run(ctx, impactCypher, map[string]any{
		"device_pk": devicePK,
	})
	if err != nil {
		logError("failure impact query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	impactRecords, err := impactResult.Collect(ctx)
	if err != nil {
		logError("failure impact collect error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	for _, record := range impactRecords {
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		status, _ := record.Get("status")
		deviceType, _ := record.Get("device_type")

		response.UnreachableDevices = append(response.UnreachableDevices, ImpactDevice{
			PK:         asString(pk),
			Code:       asString(code),
			Status:     asString(status),
			DeviceType: asString(deviceType),
		})
	}

	response.UnreachableCount = len(response.UnreachableDevices)

	// Calculate metro-level impact
	// Build a set of unreachable device PKs for quick lookup
	unreachablePKs := make(map[string]bool)
	for _, device := range response.UnreachableDevices {
		unreachablePKs[device.PK] = true
	}
	// The failing device itself also counts as unavailable
	unreachablePKs[devicePK] = true

	// Query all metros with ISIS devices and their device counts
	metroCypher := `
		MATCH (m:Metro)<-[:LOCATED_IN]-(d:Device)
		WHERE d.isis_system_id IS NOT NULL
		RETURN m.pk AS metro_pk,
		       m.code AS metro_code,
		       m.name AS metro_name,
		       collect(d.pk) AS device_pks
	`
	metroResult, err := session.Run(ctx, metroCypher, map[string]any{})
	if err != nil {
		logError("failure impact metro query error", "error", err)
		// Don't fail the whole response, just log the error
	} else {
		metroRecords, err := metroResult.Collect(ctx)
		if err != nil {
			logError("failure impact metro collect error", "error", err)
		} else {
			for _, record := range metroRecords {
				metroPK, _ := record.Get("metro_pk")
				metroCode, _ := record.Get("metro_code")
				metroName, _ := record.Get("metro_name")
				devicePKsRaw, _ := record.Get("device_pks")

				devicePKsList, ok := devicePKsRaw.([]any)
				if !ok {
					continue
				}

				totalDevices := len(devicePKsList)
				isolatedCount := 0
				for _, pk := range devicePKsList {
					if unreachablePKs[asString(pk)] {
						isolatedCount++
					}
				}

				// Only include metros where at least one device is affected
				if isolatedCount > 0 {
					response.MetroImpact = append(response.MetroImpact, MetroImpact{
						PK:               asString(metroPK),
						Code:             asString(metroCode),
						Name:             asString(metroName),
						TotalDevices:     totalDevices,
						RemainingDevices: totalDevices - isolatedCount,
						IsolatedDevices:  isolatedCount,
					})
				}
			}
		}
	}

	// Find affected paths - paths that currently use this device as an intermediate hop
	// For each affected path, calculate before (through device) and after (rerouted) metrics
	affectedCypher := `
		// Get the failing device
		MATCH (target:Device {pk: $device_pk})
		WHERE target.isis_system_id IS NOT NULL

		// Find devices that have shortest paths going through target
		// These are neighbors of target where going through target is part of their shortest path
		MATCH (target)-[r1:ISIS_ADJACENT]-(neighbor1:Device)
		WHERE neighbor1.isis_system_id IS NOT NULL
		WITH target, neighbor1, r1.metric AS metric1
		MATCH (target)-[r2:ISIS_ADJACENT]-(neighbor2:Device)
		WHERE neighbor2.isis_system_id IS NOT NULL
		  AND neighbor2.pk > neighbor1.pk  // Avoid duplicate pairs
		WITH target, neighbor1, neighbor2, metric1, r2.metric AS metric2

		// Calculate path through target device
		WITH neighbor1, neighbor2, target,
		     metric1 + metric2 AS throughTargetMetric

		// Find shortest path between neighbors NOT going through target
		OPTIONAL MATCH altPath = shortestPath((neighbor1)-[:ISIS_ADJACENT*]-(neighbor2))
		WHERE NONE(n IN nodes(altPath) WHERE n.pk = target.pk)
		WITH neighbor1, neighbor2, target, throughTargetMetric,
		     CASE WHEN altPath IS NOT NULL THEN length(altPath) ELSE 0 END AS altHops,
		     CASE WHEN altPath IS NOT NULL
		          THEN reduce(total = 0, rel IN relationships(altPath) | total + coalesce(rel.metric, 0))
		          ELSE 0 END AS altMetric

		// Only include paths where the path through target is actually the current best path:
		// 1. No alternate exists (altHops = 0), so removing target disconnects these devices, OR
		// 2. Path through target has lower metric than alternate, so it's currently preferred
		WHERE altHops = 0 OR (altHops > 0 AND throughTargetMetric < altMetric)
		RETURN neighbor1.pk AS from_pk,
		       neighbor1.code AS from_code,
		       neighbor2.pk AS to_pk,
		       neighbor2.code AS to_code,
		       2 AS before_hops,
		       throughTargetMetric AS before_metric,
		       altHops AS after_hops,
		       altMetric AS after_metric,
		       altHops > 0 AS has_alternate
		ORDER BY (altMetric - throughTargetMetric) DESC
		LIMIT 20
	`

	affectedResult, err := session.Run(ctx, affectedCypher, map[string]any{
		"device_pk": devicePK,
	})
	if err != nil {
		logError("failure impact affected paths query error", "error", err)
		// Don't fail the whole response, just log the error
	} else {
		affectedRecords, err := affectedResult.Collect(ctx)
		if err != nil {
			logError("failure impact affected paths collect error", "error", err)
		} else {
			for _, record := range affectedRecords {
				fromPK, _ := record.Get("from_pk")
				fromCode, _ := record.Get("from_code")
				toPK, _ := record.Get("to_pk")
				toCode, _ := record.Get("to_code")
				beforeHops, _ := record.Get("before_hops")
				beforeMetric, _ := record.Get("before_metric")
				afterHops, _ := record.Get("after_hops")
				afterMetric, _ := record.Get("after_metric")
				hasAlternate, _ := record.Get("has_alternate")

				response.AffectedPaths = append(response.AffectedPaths, FailureImpactPath{
					FromPK:       asString(fromPK),
					FromCode:     asString(fromCode),
					ToPK:         asString(toPK),
					ToCode:       asString(toCode),
					BeforeHops:   int(asInt64(beforeHops)),
					BeforeMetric: uint32(asInt64(beforeMetric)),
					AfterHops:    int(asInt64(afterHops)),
					AfterMetric:  uint32(asInt64(afterMetric)),
					HasAlternate: asBool(hasAlternate),
				})
			}
		}
	}
	response.AffectedPathCount = len(response.AffectedPaths)

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Info("failure impact", "device", response.DeviceCode, "unreachable", response.UnreachableCount, "affected_paths", response.AffectedPathCount, "metros_impacted", len(response.MetroImpact), "duration", duration)

	writeJSON(w, response)
}

// MultiPathHop represents a hop in a path with edge metric information
type MultiPathHop struct {
	DevicePK        string  `json:"devicePK"`
	DeviceCode      string  `json:"deviceCode"`
	Status          string  `json:"status"`
	DeviceType      string  `json:"deviceType"`
	MetroPK         string  `json:"metroPK,omitempty"`
	MetroCode       string  `json:"metroCode,omitempty"`
	EdgeMetric      uint32  `json:"edgeMetric,omitempty"`      // ISIS metric to reach this hop from previous
	EdgeMeasuredMs  float64 `json:"edgeMeasuredMs,omitempty"`  // measured RTT in ms to reach this hop
	EdgeJitterMs    float64 `json:"edgeJitterMs,omitempty"`    // measured jitter in ms
	EdgeLossPct     float64 `json:"edgeLossPct,omitempty"`     // packet loss percentage
	EdgeSampleCount int64   `json:"edgeSampleCount,omitempty"` // number of samples for confidence
}

// SinglePath represents one path in a multi-path response
type SinglePath struct {
	Path              []MultiPathHop `json:"path"`
	TotalMetric       uint32         `json:"totalMetric"`
	HopCount          int            `json:"hopCount"`
	MeasuredLatencyMs float64        `json:"measuredLatencyMs,omitempty"` // sum of measured RTT along path
	TotalSamples      int64          `json:"totalSamples,omitempty"`      // min samples across hops
}

// MultiPathResponse is the response for the K-shortest paths endpoint
type MultiPathResponse struct {
	Paths []SinglePath `json:"paths"`
	From  string       `json:"from"`
	To    string       `json:"to"`
	Error string       `json:"error,omitempty"`
}

// GetISISPaths finds K-shortest paths between two devices using Yen's algorithm in-memory.
// Paths are ranked by lowest total ISIS metric (latency proxy), then enriched with measured latency.
func (a *API) GetISISPaths(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	fromPK := r.URL.Query().Get("from")
	toPK := r.URL.Query().Get("to")
	kStr := r.URL.Query().Get("k")

	if fromPK == "" || toPK == "" {
		writeJSON(w, MultiPathResponse{Error: "from and to parameters are required"})
		return
	}

	if fromPK == toPK {
		writeJSON(w, MultiPathResponse{Error: "from and to must be different devices"})
		return
	}

	k := 5 // default
	if kStr != "" {
		if parsed, err := strconv.Atoi(kStr); err == nil && parsed > 0 && parsed <= 25 {
			k = parsed
		}
	}

	start := time.Now()

	response := MultiPathResponse{
		From:  fromPK,
		To:    toPK,
		Paths: []SinglePath{},
	}

	// Load graph into memory and run Yen's k-shortest paths algorithm.
	// This is faster than Neo4j's allSimplePaths which has combinatorial explosion at high depths.
	paths, err := a.findKShortestPaths(ctx, fromPK, toPK, k)
	if err != nil {
		logError("KSP error", "error", err)
		response.Error = "Failed to find paths: " + err.Error()
		writeJSON(w, response)
		return
	}

	if len(paths) == 0 {
		response.Error = "No paths found between devices"
		writeJSON(w, response)
		return
	}

	response.Paths = paths

	// Enrich paths with measured latency from ClickHouse
	if err := a.enrichPathsWithMeasuredLatency(ctx, &response); err != nil {
		logError("enrichPathsWithMeasuredLatency error", "error", err)
		response.Error = fmt.Sprintf("failed to enrich paths with measured latency: %v", err)
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)
	slog.Info("ISIS KSP completed", "paths", len(response.Paths), "duration", duration)

	writeJSON(w, response)
}

// linkLatencyData holds measured latency data for a link
type linkLatencyData struct {
	SideAPK     string
	SideZPK     string
	AvgRttMs    float64
	AvgJitterMs float64
	LossPct     float64
	SampleCount uint64
}

// enrichPathsWithMeasuredLatency queries ClickHouse for measured latency and adds it to path hops
func (a *API) enrichPathsWithMeasuredLatency(ctx context.Context, response *MultiPathResponse) error {
	if len(response.Paths) == 0 {
		return nil
	}

	// Query ClickHouse for measured latency per link, including device endpoints
	query := `
		SELECT
			l.side_a_pk,
			l.side_z_pk,
			round(sum(r.a_avg_rtt_us * r.a_samples + r.z_avg_rtt_us * r.z_samples) / greatest(sum(r.a_samples + r.z_samples), 1) / 1000.0, 3) AS avg_rtt_ms,
			round(sum(r.a_avg_jitter_us * r.a_samples + r.z_avg_jitter_us * r.z_samples) / greatest(sum(r.a_samples + r.z_samples), 1) / 1000.0, 3) AS avg_jitter_ms,
			sum(r.a_loss_pct * r.a_samples + r.z_loss_pct * r.z_samples) / greatest(sum(r.a_samples + r.z_samples), 1) AS loss_pct,
			sum(r.a_samples + r.z_samples) AS sample_count
		FROM dz_links_current l
		JOIN link_rollup_5m r FINAL ON l.pk = r.link_pk
		WHERE r.bucket_ts >= now() - INTERVAL 3 HOUR
		  AND l.side_a_pk != ''
		  AND l.side_z_pk != ''
		GROUP BY l.side_a_pk, l.side_z_pk
	`

	rows, err := a.envDB(ctx).Query(ctx, query)
	if err != nil {
		return fmt.Errorf("enrichPathsWithMeasuredLatency query error: %w", err)
	}
	defer rows.Close()

	// Build lookup map: "deviceA:deviceB" -> latency data
	// Store both directions since links are bidirectional
	latencyMap := make(map[string]linkLatencyData)
	for rows.Next() {
		var data linkLatencyData
		if err := rows.Scan(&data.SideAPK, &data.SideZPK, &data.AvgRttMs, &data.AvgJitterMs, &data.LossPct, &data.SampleCount); err != nil {
			return fmt.Errorf("enrichPathsWithMeasuredLatency scan error: %w", err)
		}
		// Store in both directions
		latencyMap[data.SideAPK+":"+data.SideZPK] = data
		latencyMap[data.SideZPK+":"+data.SideAPK] = data
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("enrichPathsWithMeasuredLatency rows error: %w", err)
	}

	// Update each path with measured latency
	for pathIdx := range response.Paths {
		path := &response.Paths[pathIdx]
		var totalMeasuredMs float64
		var minSamples int64 = -1

		for hopIdx := 1; hopIdx < len(path.Path); hopIdx++ {
			prevDevice := path.Path[hopIdx-1].DevicePK
			currDevice := path.Path[hopIdx].DevicePK
			key := prevDevice + ":" + currDevice

			if data, ok := latencyMap[key]; ok {
				path.Path[hopIdx].EdgeMeasuredMs = data.AvgRttMs
				path.Path[hopIdx].EdgeJitterMs = data.AvgJitterMs
				path.Path[hopIdx].EdgeLossPct = data.LossPct
				path.Path[hopIdx].EdgeSampleCount = int64(data.SampleCount)
				totalMeasuredMs += data.AvgRttMs
				if minSamples < 0 || int64(data.SampleCount) < minSamples {
					minSamples = int64(data.SampleCount)
				}
			}
		}

		if totalMeasuredMs > 0 {
			path.MeasuredLatencyMs = totalMeasuredMs
		}
		if minSamples > 0 {
			path.TotalSamples = minSamples
		}
	}
	return nil
}

// CriticalLink represents a link that is critical for network connectivity
type CriticalLink struct {
	SourcePK    string `json:"sourcePK"`
	SourceCode  string `json:"sourceCode"`
	TargetPK    string `json:"targetPK"`
	TargetCode  string `json:"targetCode"`
	Metric      uint32 `json:"metric"`
	Criticality string `json:"criticality"` // "critical", "important", "redundant"
}

// CriticalLinksResponse is the response for the critical links endpoint
type CriticalLinksResponse struct {
	Links []CriticalLink `json:"links"`
	Error string         `json:"error,omitempty"`
}

// GetCriticalLinks returns links that are critical for network connectivity
// Critical links are identified based on node degrees and connectivity patterns
func (a *API) GetCriticalLinks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := CriticalLinksResponse{
		Links: []CriticalLink{},
	}

	// Efficient approach: for each edge, check the degree of both endpoints
	// - If either endpoint has degree 1, this is a critical link (leaf edge)
	// - If min(degreeA, degreeB) == 2, it's important (limited redundancy)
	// - Otherwise it's redundant (well-connected)
	cypher := `
		MATCH (a:Device)-[r:ISIS_ADJACENT]-(b:Device)
		WHERE a.isis_system_id IS NOT NULL
		  AND b.isis_system_id IS NOT NULL
		  AND id(a) < id(b)
		WITH a, b, min(r.metric) AS metric  // Deduplicate multiple edges between same nodes
		// Count neighbors for each endpoint
		OPTIONAL MATCH (a)-[:ISIS_ADJACENT]-(na:Device)
		WHERE na.isis_system_id IS NOT NULL
		WITH a, b, metric, count(DISTINCT na) AS degreeA
		OPTIONAL MATCH (b)-[:ISIS_ADJACENT]-(nb:Device)
		WHERE nb.isis_system_id IS NOT NULL
		WITH a, b, metric, degreeA, count(DISTINCT nb) AS degreeB
		RETURN a.pk AS sourcePK,
		       a.code AS sourceCode,
		       b.pk AS targetPK,
		       b.code AS targetCode,
		       metric,
		       degreeA,
		       degreeB
		ORDER BY CASE
		  WHEN degreeA = 1 OR degreeB = 1 THEN 0
		  WHEN degreeA = 2 OR degreeB = 2 THEN 1
		  ELSE 2
		END, metric DESC
	`

	result, err := session.Run(ctx, cypher, nil)
	if err != nil {
		logError("critical links query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	records, err := result.Collect(ctx)
	if err != nil {
		logError("critical links collect error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	for _, record := range records {
		sourcePK, _ := record.Get("sourcePK")
		sourceCode, _ := record.Get("sourceCode")
		targetPK, _ := record.Get("targetPK")
		targetCode, _ := record.Get("targetCode")
		metric, _ := record.Get("metric")
		degreeA, _ := record.Get("degreeA")
		degreeB, _ := record.Get("degreeB")

		dA := asInt64(degreeA)
		dB := asInt64(degreeB)
		minDegree := dA
		if dB < dA {
			minDegree = dB
		}

		// Determine criticality based on minimum degree
		var criticality string
		if minDegree <= 1 {
			criticality = "critical" // At least one endpoint has only this connection
		} else if minDegree == 2 {
			criticality = "important" // Limited redundancy
		} else {
			criticality = "redundant" // Well-connected endpoints
		}

		response.Links = append(response.Links, CriticalLink{
			SourcePK:    asString(sourcePK),
			SourceCode:  asString(sourceCode),
			TargetPK:    asString(targetPK),
			TargetCode:  asString(targetCode),
			Metric:      uint32(asInt64(metric)),
			Criticality: criticality,
		})
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	criticalCount := 0
	importantCount := 0
	for _, link := range response.Links {
		if link.Criticality == "critical" {
			criticalCount++
		} else if link.Criticality == "important" {
			importantCount++
		}
	}
	slog.Info("critical links query completed", "links", len(response.Links), "critical", criticalCount, "important", importantCount, "duration", duration)

	writeJSON(w, response)
}

// RedundancyIssue represents a single redundancy issue in the network
type RedundancyIssue struct {
	Type        string `json:"type"`        // "leaf_device", "critical_link", "single_exit_metro", "no_backup_device"
	Severity    string `json:"severity"`    // "critical", "warning", "info"
	EntityPK    string `json:"entityPK"`    // PK of affected entity
	EntityCode  string `json:"entityCode"`  // Code/name of affected entity
	EntityType  string `json:"entityType"`  // "device", "link", "metro"
	Description string `json:"description"` // Human-readable description
	Impact      string `json:"impact"`      // Impact description
	// Extra fields for links
	TargetPK   string `json:"targetPK,omitempty"`
	TargetCode string `json:"targetCode,omitempty"`
	// Extra fields for context
	MetroPK   string `json:"metroPK,omitempty"`
	MetroCode string `json:"metroCode,omitempty"`
}

// RedundancyReportResponse is the response for the redundancy report endpoint
type RedundancyReportResponse struct {
	Issues  []RedundancyIssue `json:"issues"`
	Summary RedundancySummary `json:"summary"`
	Error   string            `json:"error,omitempty"`
}

type RedundancySummary struct {
	TotalIssues      int `json:"totalIssues"`
	CriticalCount    int `json:"criticalCount"`
	WarningCount     int `json:"warningCount"`
	InfoCount        int `json:"infoCount"`
	LeafDevices      int `json:"leafDevices"`
	CriticalLinks    int `json:"criticalLinks"`
	SingleExitMetros int `json:"singleExitMetros"`
}

// GetRedundancyReport returns a comprehensive redundancy analysis report
func (a *API) GetRedundancyReport(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := RedundancyReportResponse{
		Issues: []RedundancyIssue{},
	}

	// 1. Find leaf devices (devices with only 1 ISIS neighbor)
	leafCypher := `
		MATCH (d:Device)
		WHERE d.isis_system_id IS NOT NULL
		OPTIONAL MATCH (d)-[:ISIS_ADJACENT]-(n:Device)
		WHERE n.isis_system_id IS NOT NULL
		WITH d, count(DISTINCT n) AS neighborCount
		WHERE neighborCount = 1
		OPTIONAL MATCH (d)-[:LOCATED_IN]->(m:Metro)
		RETURN d.pk AS pk,
		       d.code AS code,
		       m.pk AS metroPK,
		       m.code AS metroCode
		ORDER BY d.code
	`

	leafResult, err := session.Run(ctx, leafCypher, nil)
	if err != nil {
		logError("redundancy report leaf devices query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	leafRecords, err := leafResult.Collect(ctx)
	if err != nil {
		logError("redundancy report leaf devices collect error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	for _, record := range leafRecords {
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		metroPK, _ := record.Get("metroPK")
		metroCode, _ := record.Get("metroCode")

		response.Issues = append(response.Issues, RedundancyIssue{
			Type:        "leaf_device",
			Severity:    "critical",
			EntityPK:    asString(pk),
			EntityCode:  asString(code),
			EntityType:  "device",
			Description: "Device has only one ISIS neighbor",
			Impact:      "If the single neighbor fails, this device loses connectivity to the network",
			MetroPK:     asString(metroPK),
			MetroCode:   asString(metroCode),
		})
	}

	// 2. Find critical links (links where at least one endpoint has only this connection)
	criticalLinksCypher := `
		MATCH (a:Device)-[r:ISIS_ADJACENT]-(b:Device)
		WHERE a.isis_system_id IS NOT NULL
		  AND b.isis_system_id IS NOT NULL
		  AND id(a) < id(b)
		WITH a, b, min(r.metric) AS metric
		OPTIONAL MATCH (a)-[:ISIS_ADJACENT]-(na:Device)
		WHERE na.isis_system_id IS NOT NULL
		WITH a, b, metric, count(DISTINCT na) AS degreeA
		OPTIONAL MATCH (b)-[:ISIS_ADJACENT]-(nb:Device)
		WHERE nb.isis_system_id IS NOT NULL
		WITH a, b, metric, degreeA, count(DISTINCT nb) AS degreeB
		WHERE degreeA = 1 OR degreeB = 1
		RETURN a.pk AS sourcePK,
		       a.code AS sourceCode,
		       b.pk AS targetPK,
		       b.code AS targetCode,
		       degreeA,
		       degreeB
		ORDER BY sourceCode
	`

	criticalResult, err := session.Run(ctx, criticalLinksCypher, nil)
	if err != nil {
		logError("redundancy report critical links query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	criticalRecords, err := criticalResult.Collect(ctx)
	if err != nil {
		logError("redundancy report critical links collect error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	for _, record := range criticalRecords {
		sourcePK, _ := record.Get("sourcePK")
		sourceCode, _ := record.Get("sourceCode")
		targetPK, _ := record.Get("targetPK")
		targetCode, _ := record.Get("targetCode")

		response.Issues = append(response.Issues, RedundancyIssue{
			Type:        "critical_link",
			Severity:    "critical",
			EntityPK:    asString(sourcePK),
			EntityCode:  asString(sourceCode),
			EntityType:  "link",
			TargetPK:    asString(targetPK),
			TargetCode:  asString(targetCode),
			Description: "Link connects a leaf device to the network",
			Impact:      "If this link fails, one or both devices lose network connectivity",
		})
	}

	// 3. Find single-exit metros (metros where only one device has external connections)
	singleExitCypher := `
		MATCH (m:Metro)<-[:LOCATED_IN]-(d:Device)
		WHERE d.isis_system_id IS NOT NULL
		MATCH (d)-[:ISIS_ADJACENT]-(n:Device)
		WHERE n.isis_system_id IS NOT NULL
		OPTIONAL MATCH (n)-[:LOCATED_IN]->(nm:Metro)
		WITH m, d, n, nm
		WHERE nm IS NULL OR nm.pk <> m.pk
		WITH m, count(DISTINCT d) AS exitDeviceCount
		WHERE exitDeviceCount = 1
		RETURN m.pk AS pk,
		       m.code AS code,
		       m.name AS name
		ORDER BY m.code
	`

	singleExitResult, err := session.Run(ctx, singleExitCypher, nil)
	if err != nil {
		logError("redundancy report single-exit metros query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	singleExitRecords, err := singleExitResult.Collect(ctx)
	if err != nil {
		logError("redundancy report single-exit metros collect error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	for _, record := range singleExitRecords {
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		name, _ := record.Get("name")

		displayName := asString(name)
		if displayName == "" {
			displayName = asString(code)
		}

		response.Issues = append(response.Issues, RedundancyIssue{
			Type:        "single_exit_metro",
			Severity:    "warning",
			EntityPK:    asString(pk),
			EntityCode:  displayName,
			EntityType:  "metro",
			Description: "Metro has only one device with external connections",
			Impact:      "If that device fails, the entire metro loses external connectivity",
		})
	}

	// Build summary
	criticalCount := 0
	warningCount := 0
	infoCount := 0
	leafDeviceCount := 0
	criticalLinkCount := 0
	singleExitMetroCount := 0

	for _, issue := range response.Issues {
		switch issue.Severity {
		case "critical":
			criticalCount++
		case "warning":
			warningCount++
		case "info":
			infoCount++
		}

		switch issue.Type {
		case "leaf_device":
			leafDeviceCount++
		case "critical_link":
			criticalLinkCount++
		case "single_exit_metro":
			singleExitMetroCount++
		}
	}

	response.Summary = RedundancySummary{
		TotalIssues:      len(response.Issues),
		CriticalCount:    criticalCount,
		WarningCount:     warningCount,
		InfoCount:        infoCount,
		LeafDevices:      leafDeviceCount,
		CriticalLinks:    criticalLinkCount,
		SingleExitMetros: singleExitMetroCount,
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Info("redundancy report completed", "issues", len(response.Issues), "critical", criticalCount, "warning", warningCount, "info", infoCount, "duration", duration)

	writeJSON(w, response)
}

// MetroConnectivity represents connectivity between two metros
type MetroConnectivity struct {
	FromMetroPK      string  `json:"fromMetroPK"`
	FromMetroCode    string  `json:"fromMetroCode"`
	FromMetroName    string  `json:"fromMetroName"`
	ToMetroPK        string  `json:"toMetroPK"`
	ToMetroCode      string  `json:"toMetroCode"`
	ToMetroName      string  `json:"toMetroName"`
	PathCount        int     `json:"pathCount"`
	MinHops          int     `json:"minHops"`
	MinMetric        int64   `json:"minMetric"`
	BottleneckBwGbps float64 `json:"bottleneckBwGbps,omitempty"` // min bandwidth along best path
}

// MetroConnectivityResponse is the response for the metro connectivity endpoint
type MetroConnectivityResponse struct {
	Metros       []MetroInfo         `json:"metros"`
	Connectivity []MetroConnectivity `json:"connectivity"`
	Error        string              `json:"error,omitempty"`
}

// MetroInfo is a lightweight metro representation for the matrix
type MetroInfo struct {
	PK   string `json:"pk"`
	Code string `json:"code"`
	Name string `json:"name"`
}

// GetMetroConnectivity returns the connectivity matrix between all metros
func (a *API) GetMetroConnectivity(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()

	response := MetroConnectivityResponse{
		Metros:       []MetroInfo{},
		Connectivity: []MetroConnectivity{},
	}

	// Helper to run Neo4j query with retry
	runNeo4jQuery := func(cypher string) ([]*neo4jdriver.Record, error) {
		cfg := dberror.DefaultRetryConfig()
		return dberror.Retry(ctx, cfg, func() ([]*neo4jdriver.Record, error) {
			session := a.neo4jSession(ctx)
			defer session.Close(ctx)

			result, err := session.Run(ctx, cypher, nil)
			if err != nil {
				return nil, err
			}
			return result.Collect(ctx)
		})
	}

	// First, get metros that have at least one device with max_users > 0
	// This filters out metros that are not user-facing (e.g., internal infrastructure)
	validMetroPKs := make(map[string]bool)
	chQuery := `
		SELECT DISTINCT metro_pk
		FROM dz_devices_current
		WHERE metro_pk != '' AND max_users > 0
	`
	chRows, err := a.DB.Query(ctx, chQuery)
	if err != nil {
		logError("metro connectivity ClickHouse query error", "error", err)
		response.Error = dberror.UserMessage(err)
		writeJSON(w, response)
		return
	}
	for chRows.Next() {
		var metroPK string
		if err := chRows.Scan(&metroPK); err != nil {
			continue
		}
		validMetroPKs[metroPK] = true
	}
	chRows.Close()

	// Get all metros that have ISIS-enabled devices
	metroCypher := `
		MATCH (m:Metro)<-[:LOCATED_IN]-(d:Device)
		WHERE d.isis_system_id IS NOT NULL
		WITH m, count(d) AS deviceCount
		WHERE deviceCount > 0
		RETURN m.pk AS pk, m.code AS code, m.name AS name
		ORDER BY m.code
	`

	metroRecords, err := runNeo4jQuery(metroCypher)
	if err != nil {
		logError("metro connectivity metro query error", "error", err)
		response.Error = dberror.UserMessage(err)
		writeJSON(w, response)
		return
	}

	metroMap := make(map[string]MetroInfo)
	for _, record := range metroRecords {
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		name, _ := record.Get("name")

		pkStr := asString(pk)
		// Skip metros that don't have any devices with max_users > 0
		if !validMetroPKs[pkStr] {
			continue
		}

		metro := MetroInfo{
			PK:   pkStr,
			Code: asString(code),
			Name: asString(name),
		}
		response.Metros = append(response.Metros, metro)
		metroMap[metro.PK] = metro
	}

	// For each pair of metros, find the best path between any devices in those metros
	// This query finds the shortest path between any two ISIS devices in different metros
	// and calculates bottleneck bandwidth (min bandwidth along path)
	connectivityCypher := `
		MATCH (m1:Metro)<-[:LOCATED_IN]-(d1:Device)
		MATCH (m2:Metro)<-[:LOCATED_IN]-(d2:Device)
		WHERE m1.pk < m2.pk
		  AND d1.isis_system_id IS NOT NULL
		  AND d2.isis_system_id IS NOT NULL
		WITH m1, m2, d1, d2
		MATCH path = shortestPath((d1)-[:ISIS_ADJACENT*]-(d2))
		WITH m1, m2,
		     length(path) AS hops,
		     reduce(total = 0, r IN relationships(path) | total + coalesce(r.metric, 0)) AS metric,
		     reduce(minBw = 9999999999999, r IN relationships(path) |
		       CASE WHEN coalesce(r.bandwidth_bps, 9999999999999) < minBw
		            THEN coalesce(r.bandwidth_bps, 9999999999999) ELSE minBw END) AS bottleneckBw
		WITH m1, m2, min(hops) AS minHops, min(metric) AS minMetric, count(*) AS pathCount,
		     max(bottleneckBw) AS maxBottleneckBw
		RETURN m1.pk AS fromPK, m1.code AS fromCode, m1.name AS fromName,
		       m2.pk AS toPK, m2.code AS toCode, m2.name AS toName,
		       minHops, minMetric, pathCount, maxBottleneckBw
		ORDER BY fromCode, toCode
	`

	connRecords, err := runNeo4jQuery(connectivityCypher)
	if err != nil {
		logError("metro connectivity query error", "error", err)
		response.Error = dberror.UserMessage(err)
		writeJSON(w, response)
		return
	}

	for _, record := range connRecords {
		fromPK, _ := record.Get("fromPK")
		fromCode, _ := record.Get("fromCode")
		fromName, _ := record.Get("fromName")
		toPK, _ := record.Get("toPK")
		toCode, _ := record.Get("toCode")
		toName, _ := record.Get("toName")
		minHops, _ := record.Get("minHops")
		minMetric, _ := record.Get("minMetric")
		pathCount, _ := record.Get("pathCount")
		maxBottleneckBw, _ := record.Get("maxBottleneckBw")

		fromPKStr := asString(fromPK)
		toPKStr := asString(toPK)

		// Skip connections involving metros without max_users > 0 devices
		if !validMetroPKs[fromPKStr] || !validMetroPKs[toPKStr] {
			continue
		}

		// Convert bandwidth from bps to Gbps, handle sentinel value
		bottleneckBwGbps := 0.0
		bwBps := asFloat64(maxBottleneckBw)
		if bwBps > 0 && bwBps < 9999999999999 {
			bottleneckBwGbps = bwBps / 1e9
		}

		// Add both directions (matrix is symmetric)
		conn := MetroConnectivity{
			FromMetroPK:      fromPKStr,
			FromMetroCode:    asString(fromCode),
			FromMetroName:    asString(fromName),
			ToMetroPK:        toPKStr,
			ToMetroCode:      asString(toCode),
			ToMetroName:      asString(toName),
			PathCount:        int(asInt64(pathCount)),
			MinHops:          int(asInt64(minHops)),
			MinMetric:        asInt64(minMetric),
			BottleneckBwGbps: bottleneckBwGbps,
		}
		response.Connectivity = append(response.Connectivity, conn)

		// Add reverse direction
		connReverse := MetroConnectivity{
			FromMetroPK:      toPKStr,
			FromMetroCode:    asString(toCode),
			FromMetroName:    asString(toName),
			ToMetroPK:        fromPKStr,
			ToMetroCode:      asString(fromCode),
			ToMetroName:      asString(fromName),
			PathCount:        int(asInt64(pathCount)),
			MinHops:          int(asInt64(minHops)),
			MinMetric:        asInt64(minMetric),
			BottleneckBwGbps: bottleneckBwGbps,
		}
		response.Connectivity = append(response.Connectivity, connReverse)
	}

	// Filter metros to only those that appear in at least one connectivity pair
	connectedMetros := make(map[string]bool)
	for _, conn := range response.Connectivity {
		connectedMetros[conn.FromMetroPK] = true
		connectedMetros[conn.ToMetroPK] = true
	}
	filtered := make([]MetroInfo, 0, len(response.Metros))
	for _, m := range response.Metros {
		if connectedMetros[m.PK] {
			filtered = append(filtered, m)
		}
	}
	response.Metros = filtered

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil) // Reuse existing metric for now

	slog.Info("metro connectivity completed", "metros", len(response.Metros), "connections", len(response.Connectivity), "duration", duration)

	writeJSON(w, response)
}

// MetroPathLatency represents path-based latency between two metros
type MetroPathLatency struct {
	FromMetroPK       string   `json:"fromMetroPK"`
	FromMetroCode     string   `json:"fromMetroCode"`
	ToMetroPK         string   `json:"toMetroPK"`
	ToMetroCode       string   `json:"toMetroCode"`
	PathLatencyMs     float64  `json:"pathLatencyMs"`     // Sum of link metrics along path (in ms)
	HopCount          int      `json:"hopCount"`          // Number of hops
	BottleneckBwGbps  float64  `json:"bottleneckBwGbps"`  // Min bandwidth along path (Gbps)
	InternetLatencyMs float64  `json:"internetLatencyMs"` // Internet latency for comparison (0 if not available)
	ImprovementPct    *float64 `json:"improvementPct"`    // Improvement vs internet (nil if no internet data)
}

// MetroPathLatencyResponse is the response for the metro path latency endpoint
type MetroPathLatencyResponse struct {
	Optimize string             `json:"optimize"` // "hops", "latency", or "bandwidth"
	Paths    []MetroPathLatency `json:"paths"`
	Summary  struct {
		TotalPairs        int     `json:"totalPairs"`
		PairsWithInternet int     `json:"pairsWithInternet"`
		AvgImprovementPct float64 `json:"avgImprovementPct"`
		MaxImprovementPct float64 `json:"maxImprovementPct"`
	} `json:"summary"`
	Error string `json:"error,omitempty"`
}

// GetMetroPathLatency returns path-based latency between all metro pairs
// with configurable optimization strategy (hops, latency, or bandwidth)
func (a *API) GetMetroPathLatency(w http.ResponseWriter, r *http.Request) {
	optimize := r.URL.Query().Get("optimize")
	if optimize == "" {
		optimize = "latency" // default to latency optimization
	}
	if optimize != "hops" && optimize != "latency" && optimize != "bandwidth" {
		writeJSON(w, MetroPathLatencyResponse{Error: "optimize must be 'hops', 'latency', or 'bandwidth'"})
		return
	}

	// Try cache first (cache only holds mainnet data)
	if isMainnet(r.Context()) {
		if data, err := a.readPageCache(r.Context(), "metro_path_latency:"+optimize); err == nil {
			w.Header().Set("X-Cache", "HIT")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
			return
		}
	}

	// Cache miss - fetch fresh data
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	response, err := a.FetchMetroPathLatencyData(ctx, optimize)
	if err != nil {
		logError("metro path latency error", "error", err)
		writeJSON(w, MetroPathLatencyResponse{Optimize: optimize, Paths: []MetroPathLatency{}, Error: err.Error()})
		return
	}

	writeJSON(w, response)
}

// FetchMetroPathLatencyData fetches metro path latency data for the given optimization strategy.
// Used by both the handler and the cache.
func (a *API) FetchMetroPathLatencyData(ctx context.Context, optimize string) (*MetroPathLatencyResponse, error) {
	if a.Neo4jClient == nil {
		return nil, fmt.Errorf("neo4j not available")
	}

	start := time.Now()

	// Load the in-memory topology graph using committed latency from link topology.
	// This uses Device-Link-Device (CONNECTS) relationships with committed_rtt_ns
	// as edge weights, which gives accurate latency values (unlike ISIS_ADJACENT
	// metrics which can be artificially low on transit switches).
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading topology graph: %w", err)
	}

	response := &MetroPathLatencyResponse{
		Optimize: optimize,
		Paths:    []MetroPathLatency{},
	}

	// Compute best paths between all metro pairs using in-memory Dijkstra
	metroPaths := computeMetroPairPaths(g)

	// Build map of metro paths
	pathMap := make(map[string]*MetroPathLatency)
	for _, mp := range metroPaths {
		path := &MetroPathLatency{
			FromMetroPK:      mp.FromMetroPK,
			FromMetroCode:    mp.FromMetroCode,
			ToMetroPK:        mp.ToMetroPK,
			ToMetroCode:      mp.ToMetroCode,
			PathLatencyMs:    float64(mp.Path.TotalMetric) / 1000.0, // Convert microseconds to milliseconds
			HopCount:         mp.HopCount,
			BottleneckBwGbps: float64(mp.BottleneckBps) / 1e9, // Convert bps to Gbps
		}

		// Store in map for both directions
		key1 := mp.FromMetroCode + ":" + mp.ToMetroCode
		key2 := mp.ToMetroCode + ":" + mp.FromMetroCode
		pathMap[key1] = path
		pathMap[key2] = &MetroPathLatency{
			FromMetroPK:      mp.ToMetroPK,
			FromMetroCode:    mp.ToMetroCode,
			ToMetroPK:        mp.FromMetroPK,
			ToMetroCode:      mp.FromMetroCode,
			PathLatencyMs:    path.PathLatencyMs,
			HopCount:         path.HopCount,
			BottleneckBwGbps: path.BottleneckBwGbps,
		}
	}

	// Fetch internet latency data from ClickHouse for comparison
	internetQuery := `
		SELECT
			least(ma.code, mz.code) AS metro1,
			greatest(ma.code, mz.code) AS metro2,
			round(avg(f.rtt_us) / 1000.0, 2) AS avg_rtt_ms
		FROM fact_dz_internet_metro_latency f
		JOIN dz_metros_current ma ON f.origin_metro_pk = ma.pk
		JOIN dz_metros_current mz ON f.target_metro_pk = mz.pk
		WHERE f.event_ts >= now() - INTERVAL 24 HOUR
		  AND ma.code != mz.code
		GROUP BY metro1, metro2
	`

	rows, err := a.safeQueryRows(ctx, internetQuery)
	if err != nil {
		return nil, fmt.Errorf("internet latency query failed: %w", err)
	}
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var metro1, metro2 string
			var avgRttMs float64
			if err := rows.Scan(&metro1, &metro2, &avgRttMs); err != nil {
				return nil, fmt.Errorf("failed to scan internet latency row: %w", err)
			}
			// Update both directions in pathMap
			key1 := metro1 + ":" + metro2
			key2 := metro2 + ":" + metro1
			if p, ok := pathMap[key1]; ok {
				p.InternetLatencyMs = avgRttMs
				if avgRttMs > 0 && p.PathLatencyMs > 0 {
					pct := (avgRttMs - p.PathLatencyMs) / avgRttMs * 100
					p.ImprovementPct = &pct
				}
			}
			if p, ok := pathMap[key2]; ok {
				p.InternetLatencyMs = avgRttMs
				if avgRttMs > 0 && p.PathLatencyMs > 0 {
					pct := (avgRttMs - p.PathLatencyMs) / avgRttMs * 100
					p.ImprovementPct = &pct
				}
			}
		}
	}

	// Convert map to slice and compute summary
	var totalImprovement float64
	var maxImprovement float64
	var pairsWithInternet int

	for _, path := range pathMap {
		response.Paths = append(response.Paths, *path)
		if path.ImprovementPct != nil {
			pairsWithInternet++
			totalImprovement += *path.ImprovementPct
			if *path.ImprovementPct > maxImprovement {
				maxImprovement = *path.ImprovementPct
			}
		}
	}

	response.Summary.TotalPairs = len(response.Paths)
	response.Summary.PairsWithInternet = pairsWithInternet
	if pairsWithInternet > 0 {
		response.Summary.AvgImprovementPct = totalImprovement / float64(pairsWithInternet)
	}
	response.Summary.MaxImprovementPct = maxImprovement

	duration := time.Since(start)
	slog.Info("fetchMetroPathLatencyData completed", "optimize", optimize, "paths", len(response.Paths), "duration", duration)

	return response, nil
}

// MetroPathDetailHop represents a single hop in a path
type MetroPathDetailHop struct {
	DevicePK    string  `json:"devicePK"`
	DeviceCode  string  `json:"deviceCode"`
	MetroPK     string  `json:"metroPK"`
	MetroCode   string  `json:"metroCode"`
	LinkMetric  int64   `json:"linkMetric"`  // Metric to next hop (0 for last hop)
	LinkBwGbps  float64 `json:"linkBwGbps"`  // Bandwidth to next hop (0 for last hop)
	LinkLatency float64 `json:"linkLatency"` // Latency in ms to next hop
}

// MetroPathDetailResponse is the response for the metro path detail endpoint
type MetroPathDetailResponse struct {
	FromMetroCode     string               `json:"fromMetroCode"`
	ToMetroCode       string               `json:"toMetroCode"`
	Optimize          string               `json:"optimize"`
	TotalLatencyMs    float64              `json:"totalLatencyMs"`
	TotalHops         int                  `json:"totalHops"`
	BottleneckBwGbps  float64              `json:"bottleneckBwGbps"`
	InternetLatencyMs float64              `json:"internetLatencyMs"`
	ImprovementPct    *float64             `json:"improvementPct"`
	Hops              []MetroPathDetailHop `json:"hops"`
	Error             string               `json:"error,omitempty"`
}

// GetMetroPathDetail returns detailed path breakdown between two metros
func (a *API) GetMetroPathDetail(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	fromCode := r.URL.Query().Get("from")
	toCode := r.URL.Query().Get("to")
	optimize := r.URL.Query().Get("optimize")

	if fromCode == "" || toCode == "" {
		writeJSON(w, MetroPathDetailResponse{Error: "from and to parameters are required"})
		return
	}
	if optimize == "" {
		optimize = "latency"
	}

	start := time.Now()

	response := MetroPathDetailResponse{
		FromMetroCode: fromCode,
		ToMetroCode:   toCode,
		Optimize:      optimize,
		Hops:          []MetroPathDetailHop{},
	}

	// Load in-memory topology graph with committed latency
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		logError("metro path detail graph load error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	// Find best path between the two metros
	mp := computeMetroPathDetail(g, fromCode, toCode)
	if mp == nil {
		response.Error = "No path found between metros"
		writeJSON(w, response)
		return
	}

	var totalMetric uint32
	var minBandwidth uint64

	for i, nodePK := range mp.Path.Nodes {
		info := g.Nodes[nodePK]
		var linkMetric uint32
		var linkBw uint64
		if i < len(mp.Path.Nodes)-1 {
			linkMetric = edgeMetric(g, nodePK, mp.Path.Nodes[i+1])
			linkBw = edgeBandwidth(g, nodePK, mp.Path.Nodes[i+1])
		}

		hop := MetroPathDetailHop{
			DevicePK:    info.PK,
			DeviceCode:  info.Code,
			MetroPK:     info.MetroPK,
			MetroCode:   info.MetroCode,
			LinkMetric:  int64(linkMetric),
			LinkLatency: float64(linkMetric) / 1000.0, // Convert to ms
			LinkBwGbps:  float64(linkBw) / 1e9,
		}

		response.Hops = append(response.Hops, hop)
		totalMetric += linkMetric
		if linkBw > 0 && (minBandwidth == 0 || linkBw < minBandwidth) {
			minBandwidth = linkBw
		}
	}

	response.TotalLatencyMs = float64(totalMetric) / 1000.0
	response.TotalHops = len(response.Hops) - 1
	if minBandwidth > 0 {
		response.BottleneckBwGbps = float64(minBandwidth) / 1e9
	}

	// Fetch internet latency for comparison
	internetQuery := `
		SELECT round(avg(f.rtt_us) / 1000.0, 2) AS avg_rtt_ms
		FROM fact_dz_internet_metro_latency f
		JOIN dz_metros_current ma ON f.origin_metro_pk = ma.pk
		JOIN dz_metros_current mz ON f.target_metro_pk = mz.pk
		WHERE f.event_ts >= now() - INTERVAL 24 HOUR
		  AND ((ma.code = $1 AND mz.code = $2) OR (ma.code = $2 AND mz.code = $1))
	`

	var internetLatency float64
	row := a.envDB(ctx).QueryRow(ctx, internetQuery, fromCode, toCode)
	if err := row.Scan(&internetLatency); err == nil && internetLatency > 0 {
		response.InternetLatencyMs = internetLatency
		if response.TotalLatencyMs > 0 {
			pct := (internetLatency - response.TotalLatencyMs) / internetLatency * 100
			response.ImprovementPct = &pct
		}
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	writeJSON(w, response)
}

// MetroPathsHop represents a device in a path
type MetroPathsHop struct {
	DevicePK   string `json:"devicePK"`
	DeviceCode string `json:"deviceCode"`
	MetroPK    string `json:"metroPK"`
	MetroCode  string `json:"metroCode"`
}

// MetroPath represents a single path between metros
type MetroPath struct {
	Hops        []MetroPathsHop `json:"hops"`
	TotalHops   int             `json:"totalHops"`
	TotalMetric int64           `json:"totalMetric"`
	LatencyMs   float64         `json:"latencyMs"`
}

// MetroPathsResponse is the response for metro paths endpoint
type MetroPathsResponse struct {
	FromMetroCode string      `json:"fromMetroCode"`
	ToMetroCode   string      `json:"toMetroCode"`
	Paths         []MetroPath `json:"paths"`
	Error         string      `json:"error,omitempty"`
}

// GetMetroPaths returns distinct paths between two metros
func (a *API) GetMetroPaths(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	fromPK := r.URL.Query().Get("from")
	toPK := r.URL.Query().Get("to")
	kStr := r.URL.Query().Get("k")
	if kStr == "" {
		kStr = "5"
	}
	k, _ := strconv.Atoi(kStr)
	if k <= 0 || k > 10 {
		k = 5
	}

	if fromPK == "" || toPK == "" {
		http.Error(w, "from and to metro PKs are required", http.StatusBadRequest)
		return
	}

	response := MetroPathsResponse{
		Paths: []MetroPath{},
	}

	// Load in-memory topology graph with committed latency
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	// Find metro codes and devices from the graph
	var fromDevices, toDevices []string
	for pk, info := range g.Nodes {
		if info.MetroPK == fromPK {
			response.FromMetroCode = info.MetroCode
			fromDevices = append(fromDevices, pk)
		} else if info.MetroPK == toPK {
			response.ToMetroCode = info.MetroCode
			toDevices = append(toDevices, pk)
		}
	}

	// Find k-shortest paths across all device pairs between the two metros
	type rankedPath struct {
		path kspPath
	}
	var allPaths []rankedPath
	for _, d1 := range fromDevices {
		for _, d2 := range toDevices {
			paths := yenKSP(g, d1, d2, k)
			for _, p := range paths {
				allPaths = append(allPaths, rankedPath{path: p})
			}
		}
	}

	// Sort by total metric and take top k
	slices.SortFunc(allPaths, func(a, b rankedPath) int {
		if a.path.TotalMetric != b.path.TotalMetric {
			if a.path.TotalMetric < b.path.TotalMetric {
				return -1
			}
			return 1
		}
		return len(a.path.Nodes) - len(b.path.Nodes)
	})
	if len(allPaths) > k {
		allPaths = allPaths[:k]
	}

	for _, rp := range allPaths {
		p := rp.path
		totalMetric := int64(p.TotalMetric)
		path := MetroPath{
			Hops:        []MetroPathsHop{},
			TotalHops:   len(p.Nodes) - 1,
			TotalMetric: totalMetric,
			LatencyMs:   float64(totalMetric) / 1000.0,
		}

		for _, nodePK := range p.Nodes {
			info := g.Nodes[nodePK]
			path.Hops = append(path.Hops, MetroPathsHop{
				DevicePK:   info.PK,
				DeviceCode: info.Code,
				MetroPK:    info.MetroPK,
				MetroCode:  info.MetroCode,
			})
		}

		response.Paths = append(response.Paths, path)
	}

	writeJSON(w, response)
}

// MaintenanceImpactRequest is the request body for maintenance impact analysis
type MaintenanceImpactRequest struct {
	Devices []string `json:"devices"` // Device PKs to take offline
	Links   []string `json:"links"`   // Link PKs to take offline (as "sourcePK:targetPK")
}

// MaintenanceItem represents a device or link being taken offline
type MaintenanceItem struct {
	Type                string                    `json:"type"`                          // "device" or "link"
	PK                  string                    `json:"pk"`                            // Device PK or link PK
	Code                string                    `json:"code"`                          // Device code or "sourceCode - targetCode"
	Impact              int                       `json:"impact"`                        // Number of affected paths/devices
	Disconnected        int                       `json:"disconnected"`                  // Devices that would lose connectivity
	CausesPartition     bool                      `json:"causesPartition"`               // Would this cause a network partition?
	DisconnectedDevices []string                  `json:"disconnectedDevices,omitempty"` // Device codes that would be disconnected
	AffectedPaths       []MaintenanceAffectedPath `json:"affectedPaths,omitempty"`       // Paths affected by this item
}

// MaintenanceAffectedPath represents a path that would be impacted by maintenance
type MaintenanceAffectedPath struct {
	Source       string `json:"source"`       // Source device code
	Target       string `json:"target"`       // Target device code
	SourceMetro  string `json:"sourceMetro"`  // Source metro code
	TargetMetro  string `json:"targetMetro"`  // Target metro code
	HopsBefore   int    `json:"hopsBefore"`   // Hops before maintenance
	HopsAfter    int    `json:"hopsAfter"`    // Hops after maintenance (-1 = disconnected)
	MetricBefore int    `json:"metricBefore"` // Total ISIS metric before
	MetricAfter  int    `json:"metricAfter"`  // Total ISIS metric after (-1 = disconnected)
	Status       string `json:"status"`       // "rerouted", "degraded", or "disconnected"
}

// AffectedLink represents a specific link affected by maintenance
type AffectedLink struct {
	SourceDevice string `json:"sourceDevice"` // Device code in source metro
	TargetDevice string `json:"targetDevice"` // Device code in target metro
	Status       string `json:"status"`       // "offline" (device going down) or "rerouted"
}

// AffectedMetroPair represents connectivity impact between two metros
type AffectedMetroPair struct {
	SourceMetro   string         `json:"sourceMetro"`
	TargetMetro   string         `json:"targetMetro"`
	AffectedLinks []AffectedLink `json:"affectedLinks"` // Specific links affected
	Status        string         `json:"status"`        // "reduced", "degraded", or "disconnected"
}

// MaintenanceImpactResponse is the response for maintenance impact analysis
type MaintenanceImpactResponse struct {
	Items             []MaintenanceItem         `json:"items"`                      // Items with their individual impacts
	TotalImpact       int                       `json:"totalImpact"`                // Total affected paths when all items are down
	TotalDisconnected int                       `json:"totalDisconnected"`          // Total devices that lose connectivity
	RecommendedOrder  []string                  `json:"recommendedOrder"`           // PKs in recommended maintenance order (least impact first)
	AffectedPaths     []MaintenanceAffectedPath `json:"affectedPaths,omitempty"`    // Sample of affected paths
	AffectedMetros    []AffectedMetroPair       `json:"affectedMetros,omitempty"`   // Affected metro pairs
	DisconnectedList  []string                  `json:"disconnectedList,omitempty"` // All devices that would be disconnected
	Error             string                    `json:"error,omitempty"`
}

// PostMaintenanceImpact analyzes the impact of taking multiple devices/links offline
func (a *API) PostMaintenanceImpact(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()

	// Parse request body
	var req MaintenanceImpactRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, MaintenanceImpactResponse{Error: "Invalid request body: " + err.Error()})
		return
	}

	if len(req.Devices) == 0 && len(req.Links) == 0 {
		writeJSON(w, MaintenanceImpactResponse{Error: "No devices or links specified"})
		return
	}

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := MaintenanceImpactResponse{
		Items:            []MaintenanceItem{},
		RecommendedOrder: []string{},
		AffectedPaths:    []MaintenanceAffectedPath{},
		AffectedMetros:   []AffectedMetroPair{},
		DisconnectedList: []string{},
	}

	// Collect all device PKs and link endpoints being taken offline
	offlineDevicePKs := make(map[string]bool)
	offlineLinkEndpoints := make(map[string]bool) // "sourcePK:targetPK" format

	for _, pk := range req.Devices {
		offlineDevicePKs[pk] = true
	}

	// Batch analyze all devices in a single query
	if len(req.Devices) > 0 {
		deviceItems := a.analyzeDevicesImpactBatch(ctx, session, req.Devices)
		for _, item := range deviceItems {
			response.Items = append(response.Items, item)
			for _, dc := range item.DisconnectedDevices {
				response.DisconnectedList = append(response.DisconnectedList, dc)
			}
		}
	}

	// Batch analyze all links
	if len(req.Links) > 0 {
		linkItems, err := a.analyzeLinksImpactBatch(ctx, session, req.Links)
		if err != nil {
			response.Error = fmt.Sprintf("failed to analyze links impact: %v", err)
			writeJSON(w, response)
			return
		}
		for _, item := range linkItems {
			response.Items = append(response.Items, item)
			for _, dc := range item.DisconnectedDevices {
				response.DisconnectedList = append(response.DisconnectedList, dc)
			}
		}
		// Track link endpoints for path analysis
		for _, linkPK := range req.Links {
			endpoints := a.getLinkEndpoints(ctx, linkPK)
			if endpoints != "" {
				offlineLinkEndpoints[endpoints] = true
			}
		}
	}

	// Sort items by impact (ascending) for recommended order
	sortedItems := make([]MaintenanceItem, len(response.Items))
	copy(sortedItems, response.Items)

	// Simple bubble sort by impact (least impactful first)
	for i := 0; i < len(sortedItems)-1; i++ {
		for j := 0; j < len(sortedItems)-i-1; j++ {
			if sortedItems[j].Impact > sortedItems[j+1].Impact {
				sortedItems[j], sortedItems[j+1] = sortedItems[j+1], sortedItems[j]
			}
		}
	}

	// Build recommended order
	for _, item := range sortedItems {
		response.RecommendedOrder = append(response.RecommendedOrder, item.PK)
	}

	// Calculate total impact
	for _, item := range response.Items {
		response.TotalImpact += item.Impact
		response.TotalDisconnected += item.Disconnected
	}

	// Compute affected paths with before/after routing metrics
	response.AffectedPaths = computeAffectedPathsFast(ctx, session, offlineDevicePKs, 50)

	// Compute affected metro pairs - simplified
	response.AffectedMetros = computeAffectedMetrosFast(ctx, session, offlineDevicePKs)

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Info("maintenance impact analyzed", "devices", len(req.Devices), "links", len(req.Links), "duration", duration)

	writeJSON(w, response)
}

// getLinkEndpoints returns "sourcePK:targetPK" for a link PK
func (a *API) getLinkEndpoints(ctx context.Context, linkPK string) string {
	query := `SELECT side_a_pk, side_z_pk FROM dz_links_current WHERE pk = $1`
	var sideA, sideZ string
	if err := a.envDB(ctx).QueryRow(ctx, query, linkPK).Scan(&sideA, &sideZ); err != nil {
		return ""
	}
	if sideA == "" || sideZ == "" {
		return ""
	}
	return sideA + ":" + sideZ
}

// analyzeDevicesImpactBatch computes the impact of taking multiple devices offline in a single query
func (a *API) analyzeDevicesImpactBatch(ctx context.Context, session neo4j.Session, devicePKs []string) []MaintenanceItem {
	items := make([]MaintenanceItem, 0, len(devicePKs))

	// Single query to get all device info, neighbor counts, and leaf neighbors
	cypher := `
		UNWIND $devicePKs AS devicePK
		MATCH (d:Device {pk: devicePK})
		WHERE d.isis_system_id IS NOT NULL

		// Get device code
		WITH d, devicePK

		// Count neighbors (for impact estimate)
		OPTIONAL MATCH (d)-[:ISIS_ADJACENT]-(neighbor:Device)
		WHERE neighbor.isis_system_id IS NOT NULL
		WITH d, devicePK, count(DISTINCT neighbor) AS neighborCount

		// Find leaf neighbors (degree 1) that would be disconnected
		OPTIONAL MATCH (d)-[:ISIS_ADJACENT]-(leafNeighbor:Device)
		WHERE leafNeighbor.isis_system_id IS NOT NULL
		WITH d, devicePK, neighborCount, leafNeighbor
		OPTIONAL MATCH (leafNeighbor)-[:ISIS_ADJACENT]-(leafNeighborNeighbor:Device)
		WHERE leafNeighborNeighbor.isis_system_id IS NOT NULL
		WITH d, devicePK, neighborCount, leafNeighbor, count(DISTINCT leafNeighborNeighbor) AS leafNeighborDegree
		WITH d, devicePK, neighborCount,
		     CASE WHEN leafNeighborDegree = 1 THEN leafNeighbor.code ELSE null END AS disconnectedCode

		WITH d.pk AS pk, d.code AS code, neighborCount,
		     collect(disconnectedCode) AS disconnectedCodes

		RETURN pk, code, neighborCount,
		       [x IN disconnectedCodes WHERE x IS NOT NULL] AS disconnectedDevices
	`

	result, err := session.Run(ctx, cypher, map[string]any{
		"devicePKs": devicePKs,
	})
	if err != nil {
		logError("batch device impact query error", "error", err)
		// Fallback to individual queries
		for _, pk := range devicePKs {
			items = append(items, analyzeDeviceImpact(ctx, session, pk))
		}
		return items
	}

	resultMap := make(map[string]MaintenanceItem)
	for result.Next(ctx) {
		record := result.Record()
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		neighborCount, _ := record.Get("neighborCount")
		disconnectedDevices, _ := record.Get("disconnectedDevices")

		disconnectedList := []string{}
		if arr, ok := disconnectedDevices.([]any); ok {
			for _, v := range arr {
				if s := asString(v); s != "" {
					disconnectedList = append(disconnectedList, s)
				}
			}
		}

		// Impact estimate: neighbor count squared (rough approximation of paths through this device)
		nc := int(asInt64(neighborCount))
		impact := nc * nc

		item := MaintenanceItem{
			Type:                "device",
			PK:                  asString(pk),
			Code:                asString(code),
			Impact:              impact,
			Disconnected:        len(disconnectedList),
			CausesPartition:     len(disconnectedList) > 0,
			DisconnectedDevices: disconnectedList,
		}
		resultMap[asString(pk)] = item
	}

	// Compute affected paths for each device (limit to 10 per device for performance)
	for pk, item := range resultMap {
		if item.Impact > 0 {
			offlineSet := map[string]bool{pk: true}
			paths := computeAffectedPathsFast(ctx, session, offlineSet, 10)
			item.AffectedPaths = paths
			item.Impact = len(paths) // Use actual count instead of estimate
			resultMap[pk] = item
		}
	}

	// Return items in the same order as input
	for _, pk := range devicePKs {
		if item, ok := resultMap[pk]; ok {
			items = append(items, item)
		} else {
			// Device not found in graph
			items = append(items, MaintenanceItem{
				Type: "device",
				PK:   pk,
				Code: "Unknown device",
			})
		}
	}

	return items
}

// analyzeLinksImpactBatch computes the impact of taking multiple links offline
func (a *API) analyzeLinksImpactBatch(ctx context.Context, session neo4j.Session, linkPKs []string) ([]MaintenanceItem, error) {
	items := make([]MaintenanceItem, 0, len(linkPKs))

	// First, batch lookup links from ClickHouse
	if len(linkPKs) == 0 {
		return items, nil
	}

	// Build placeholders for ClickHouse query
	linkQuery := `
		SELECT
			l.pk,
			l.code,
			COALESCE(l.side_a_pk, '') as side_a_pk,
			COALESCE(l.side_z_pk, '') as side_z_pk,
			COALESCE(da.code, '') as side_a_code,
			COALESCE(dz.code, '') as side_z_code
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		WHERE l.pk IN ($1)
	`

	// For ClickHouse we need to pass as a tuple
	rows, err := a.envDB(ctx).Query(ctx, linkQuery, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("batch link lookup error: %w", err)
	}
	defer rows.Close()

	type linkInfo struct {
		pk        string
		code      string
		sideAPK   string
		sideZPK   string
		sideACode string
		sideZCode string
	}
	linkMap := make(map[string]linkInfo)

	for rows.Next() {
		var li linkInfo
		if err := rows.Scan(&li.pk, &li.code, &li.sideAPK, &li.sideZPK, &li.sideACode, &li.sideZCode); err != nil {
			return nil, fmt.Errorf("failed to scan link info row: %w", err)
		}
		linkMap[li.pk] = li
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate link info rows: %w", err)
	}

	// Now batch query Neo4j for degree information
	var linkEndpoints []map[string]string
	for _, pk := range linkPKs {
		if li, ok := linkMap[pk]; ok && li.sideAPK != "" && li.sideZPK != "" {
			linkEndpoints = append(linkEndpoints, map[string]string{
				"pk":      pk,
				"sourceA": li.sideAPK,
				"sourceZ": li.sideZPK,
			})
		}
	}

	// Single Neo4j query for all link endpoints
	degreeCypher := `
		UNWIND $links AS link
		MATCH (s:Device {pk: link.sourceA}), (t:Device {pk: link.sourceZ})
		WHERE s.isis_system_id IS NOT NULL AND t.isis_system_id IS NOT NULL

		OPTIONAL MATCH (s)-[:ISIS_ADJACENT]-(sn:Device) WHERE sn.isis_system_id IS NOT NULL
		WITH link, s, t, count(DISTINCT sn) AS sourceDegree
		OPTIONAL MATCH (t)-[:ISIS_ADJACENT]-(tn:Device) WHERE tn.isis_system_id IS NOT NULL
		WITH link, s, t, sourceDegree, count(DISTINCT tn) AS targetDegree

		RETURN link.pk AS pk, s.code AS sourceCode, t.code AS targetCode, sourceDegree, targetDegree
	`

	degreeMap := make(map[string]struct {
		sourceCode   string
		targetCode   string
		sourceDegree int
		targetDegree int
	})

	if len(linkEndpoints) > 0 {
		result, err := session.Run(ctx, degreeCypher, map[string]any{
			"links": linkEndpoints,
		})
		if err == nil {
			for result.Next(ctx) {
				record := result.Record()
				pk, _ := record.Get("pk")
				sourceCode, _ := record.Get("sourceCode")
				targetCode, _ := record.Get("targetCode")
				sourceDegree, _ := record.Get("sourceDegree")
				targetDegree, _ := record.Get("targetDegree")

				degreeMap[asString(pk)] = struct {
					sourceCode   string
					targetCode   string
					sourceDegree int
					targetDegree int
				}{
					sourceCode:   asString(sourceCode),
					targetCode:   asString(targetCode),
					sourceDegree: int(asInt64(sourceDegree)),
					targetDegree: int(asInt64(targetDegree)),
				}
			}
		}
	}

	// Build items in order
	for _, pk := range linkPKs {
		li, hasLink := linkMap[pk]
		if !hasLink {
			items = append(items, MaintenanceItem{
				Type: "link",
				PK:   pk,
				Code: "Link not found",
			})
			continue
		}

		if li.sideAPK == "" || li.sideZPK == "" {
			items = append(items, MaintenanceItem{
				Type: "link",
				PK:   pk,
				Code: li.code + " (missing endpoints)",
			})
			continue
		}

		item := MaintenanceItem{
			Type: "link",
			PK:   pk,
			Code: li.sideACode + " - " + li.sideZCode,
		}

		if deg, ok := degreeMap[pk]; ok {
			// If either has degree 1, this link is critical
			if deg.sourceDegree == 1 || deg.targetDegree == 1 {
				item.CausesPartition = true
				if deg.sourceDegree == 1 {
					item.DisconnectedDevices = append(item.DisconnectedDevices, deg.sourceCode)
					item.Disconnected++
				}
				if deg.targetDegree == 1 {
					item.DisconnectedDevices = append(item.DisconnectedDevices, deg.targetCode)
					item.Disconnected++
				}
			}
			// Impact estimate: product of (degree-1) for each side (paths that would need rerouting)
			srcNeighbors := deg.sourceDegree - 1
			tgtNeighbors := deg.targetDegree - 1
			if srcNeighbors < 0 {
				srcNeighbors = 0
			}
			if tgtNeighbors < 0 {
				tgtNeighbors = 0
			}
			item.Impact = srcNeighbors * tgtNeighbors
		}

		items = append(items, item)
	}

	return items, nil
}

// computeAffectedPathsFast finds all paths affected by taking devices offline
// Returns paths that will be rerouted (with before/after metrics) and paths that will be disconnected
func computeAffectedPathsFast(ctx context.Context, session neo4j.Session,
	offlineDevices map[string]bool, limit int) []MaintenanceAffectedPath {

	result := []MaintenanceAffectedPath{}

	offlineDevicePKs := make([]string, 0, len(offlineDevices))
	for pk := range offlineDevices {
		offlineDevicePKs = append(offlineDevicePKs, pk)
	}

	if len(offlineDevicePKs) == 0 {
		return result
	}

	// Step 1: Find all neighbors of offline devices (these are the directly affected connections)
	// For each neighbor pair (neighbor of offline device A, neighbor of offline device B or other device),
	// compute the current shortest path and the alternate path avoiding offline devices
	cypher := `
		// Find all ISIS neighbors of offline devices
		MATCH (offline:Device)-[:ISIS_ADJACENT]-(neighbor:Device)
		WHERE offline.pk IN $offlineDevicePKs
		  AND offline.isis_system_id IS NOT NULL
		  AND neighbor.isis_system_id IS NOT NULL
		  AND NOT neighbor.pk IN $offlineDevicePKs

		WITH DISTINCT neighbor

		// For each neighbor, find paths to other devices that currently go through an offline device
		MATCH (neighbor)-[:ISIS_ADJACENT*1..2]-(other:Device)
		WHERE other.isis_system_id IS NOT NULL
		  AND other.pk <> neighbor.pk
		  AND NOT other.pk IN $offlineDevicePKs

		WITH DISTINCT neighbor, other
		WHERE neighbor.pk < other.pk  // Avoid duplicates

		// Get current shortest path
		MATCH currentPath = shortestPath((neighbor)-[:ISIS_ADJACENT*]-(other))
		WITH neighbor, other, currentPath,
		     length(currentPath) AS currentHops,
		     reduce(m = 0, r IN relationships(currentPath) | m + coalesce(r.metric, 10)) AS currentMetric,
		     any(n IN nodes(currentPath) WHERE n.pk IN $offlineDevicePKs) AS goesThruOffline

		WHERE goesThruOffline = true

		// Get metro info
		OPTIONAL MATCH (neighbor)-[:LOCATED_IN]->(nm:Metro)
		OPTIONAL MATCH (other)-[:LOCATED_IN]->(om:Metro)

		RETURN neighbor.pk AS sourcePK, neighbor.code AS sourceCode,
		       other.pk AS targetPK, other.code AS targetCode,
		       COALESCE(nm.code, 'unknown') AS sourceMetro,
		       COALESCE(om.code, 'unknown') AS targetMetro,
		       currentHops, currentMetric
		LIMIT $limit
	`

	records, err := session.Run(ctx, cypher, map[string]any{
		"offlineDevicePKs": offlineDevicePKs,
		"limit":            limit * 2, // Get more candidates, we'll filter
	})
	if err != nil {
		logError("error computing affected paths", "error", err)
		return result
	}

	// Collect paths that need alternate route computation
	type pathCandidate struct {
		sourcePK      string
		sourceCode    string
		targetPK      string
		targetCode    string
		sourceMetro   string
		targetMetro   string
		currentHops   int
		currentMetric int
	}
	candidates := []pathCandidate{}

	for records.Next(ctx) {
		record := records.Record()
		sourcePK, _ := record.Get("sourcePK")
		sourceCode, _ := record.Get("sourceCode")
		targetPK, _ := record.Get("targetPK")
		targetCode, _ := record.Get("targetCode")
		sourceMetro, _ := record.Get("sourceMetro")
		targetMetro, _ := record.Get("targetMetro")
		currentHops, _ := record.Get("currentHops")
		currentMetric, _ := record.Get("currentMetric")

		candidates = append(candidates, pathCandidate{
			sourcePK:      asString(sourcePK),
			sourceCode:    asString(sourceCode),
			targetPK:      asString(targetPK),
			targetCode:    asString(targetCode),
			sourceMetro:   asString(sourceMetro),
			targetMetro:   asString(targetMetro),
			currentHops:   int(asInt64(currentHops)),
			currentMetric: int(asInt64(currentMetric)),
		})
	}

	// Step 2: For each candidate, find alternate path avoiding offline devices
	for _, c := range candidates {
		if len(result) >= limit {
			break
		}

		path := MaintenanceAffectedPath{
			Source:       c.sourceCode,
			Target:       c.targetCode,
			SourceMetro:  c.sourceMetro,
			TargetMetro:  c.targetMetro,
			HopsBefore:   c.currentHops,
			MetricBefore: c.currentMetric,
			HopsAfter:    -1,
			MetricAfter:  -1,
			Status:       "disconnected",
		}

		// Try to find alternate path
		altCypher := `
			MATCH (source:Device {pk: $sourcePK}), (target:Device {pk: $targetPK})
			MATCH altPath = shortestPath((source)-[:ISIS_ADJACENT*]-(target))
			WHERE none(n IN nodes(altPath) WHERE n.pk IN $offlineDevicePKs)
			WITH altPath, length(altPath) AS altHops,
			     reduce(m = 0, r IN relationships(altPath) | m + coalesce(r.metric, 10)) AS altMetric
			RETURN altHops, altMetric
			LIMIT 1
		`

		altResult, err := session.Run(ctx, altCypher, map[string]any{
			"sourcePK":         c.sourcePK,
			"targetPK":         c.targetPK,
			"offlineDevicePKs": offlineDevicePKs,
		})
		if err == nil && altResult.Next(ctx) {
			record := altResult.Record()
			altHops, _ := record.Get("altHops")
			altMetric, _ := record.Get("altMetric")

			path.HopsAfter = int(asInt64(altHops))
			path.MetricAfter = int(asInt64(altMetric))

			// Classify based on degradation
			hopIncrease := path.HopsAfter - path.HopsBefore
			metricIncrease := path.MetricAfter - path.MetricBefore
			if hopIncrease > 2 || metricIncrease > 50 {
				path.Status = "degraded"
			} else {
				path.Status = "rerouted"
			}
		}

		result = append(result, path)
	}

	return result
}

// computeAffectedMetrosFast computes affected metro pairs with specific link details
func computeAffectedMetrosFast(ctx context.Context, session neo4j.Session,
	offlineDevices map[string]bool) []AffectedMetroPair {

	result := []AffectedMetroPair{}

	offlineDevicePKs := make([]string, 0, len(offlineDevices))
	for pk := range offlineDevices {
		offlineDevicePKs = append(offlineDevicePKs, pk)
	}

	if len(offlineDevicePKs) == 0 {
		return result
	}

	// Query: find ISIS adjacencies that involve offline devices, grouped by metro pair
	// Returns the specific device pairs affected
	cypher := `
		MATCH (d1:Device)-[:ISIS_ADJACENT]-(d2:Device)
		WHERE d1.pk IN $offlineDevicePKs
		  AND d1.isis_system_id IS NOT NULL
		  AND d2.isis_system_id IS NOT NULL
		  AND NOT d2.pk IN $offlineDevicePKs

		// Get metro info for both devices
		OPTIONAL MATCH (d1)-[:LOCATED_IN]->(m1:Metro)
		OPTIONAL MATCH (d2)-[:LOCATED_IN]->(m2:Metro)

		WITH COALESCE(m1.code, 'unknown') AS metro1,
		     COALESCE(m2.code, 'unknown') AS metro2,
		     d1.code AS device1,
		     d2.code AS device2,
		     d1.pk AS d1pk

		// Return individual links grouped by metro pair
		RETURN metro1, metro2, device1, device2, d1pk
		ORDER BY metro1, metro2, device1
		LIMIT 50
	`

	records, err := session.Run(ctx, cypher, map[string]any{
		"offlineDevicePKs": offlineDevicePKs,
	})
	if err != nil {
		logError("error computing affected metros fast", "error", err)
		return result
	}

	// Group links by metro pair
	type metroPairKey struct {
		metro1, metro2 string
	}
	metroPairs := make(map[metroPairKey]*AffectedMetroPair)

	for records.Next(ctx) {
		record := records.Record()
		metro1, _ := record.Get("metro1")
		metro2, _ := record.Get("metro2")
		device1, _ := record.Get("device1")
		device2, _ := record.Get("device2")

		m1 := asString(metro1)
		m2 := asString(metro2)

		// Normalize key so we don't duplicate metro pairs in different order
		key := metroPairKey{m1, m2}
		if m1 > m2 {
			key = metroPairKey{m2, m1}
		}

		pair, exists := metroPairs[key]
		if !exists {
			pair = &AffectedMetroPair{
				SourceMetro:   key.metro1,
				TargetMetro:   key.metro2,
				AffectedLinks: []AffectedLink{},
				Status:        "reduced",
			}
			metroPairs[key] = pair
		}

		// Add the affected link
		link := AffectedLink{
			SourceDevice: asString(device1),
			TargetDevice: asString(device2),
			Status:       "offline", // The source device is going offline
		}
		pair.AffectedLinks = append(pair.AffectedLinks, link)
	}

	// Convert map to slice
	for _, pair := range metroPairs {
		result = append(result, *pair)
	}

	return result
}

// analyzeDeviceImpact computes the impact of taking a single device offline
func analyzeDeviceImpact(ctx context.Context, session neo4j.Session, devicePK string) MaintenanceItem {
	item := MaintenanceItem{
		Type: "device",
		PK:   devicePK,
	}

	// Get device code
	codeCypher := `
		MATCH (d:Device {pk: $pk})
		RETURN d.code AS code
	`
	codeResult, err := session.Run(ctx, codeCypher, map[string]any{"pk": devicePK})
	if err == nil {
		if record, err := codeResult.Single(ctx); err == nil {
			if code, ok := record.Get("code"); ok {
				item.Code = asString(code)
			}
		}
	}

	// Count paths that go through this device
	pathsCypher := `
		MATCH (d:Device {pk: $pk})
		WHERE d.isis_system_id IS NOT NULL
		OPTIONAL MATCH (other:Device)
		WHERE other.isis_system_id IS NOT NULL AND other.pk <> d.pk
		OPTIONAL MATCH path = shortestPath((other)-[:ISIS_ADJACENT*]-(d))
		WITH d, count(path) AS pathCount
		RETURN pathCount
	`
	pathsResult, err := session.Run(ctx, pathsCypher, map[string]any{"pk": devicePK})
	if err == nil {
		if record, err := pathsResult.Single(ctx); err == nil {
			if pathCount, ok := record.Get("pathCount"); ok {
				item.Impact = int(asInt64(pathCount))
			}
		}
	}

	// Check if this device is critical (would disconnect others)
	// A device is critical if any of its neighbors have degree 1 (only connected to this device)
	criticalCypher := `
		MATCH (d:Device {pk: $pk})-[:ISIS_ADJACENT]-(neighbor:Device)
		WHERE d.isis_system_id IS NOT NULL AND neighbor.isis_system_id IS NOT NULL
		WITH neighbor
		MATCH (neighbor)-[:ISIS_ADJACENT]-(any:Device)
		WHERE any.isis_system_id IS NOT NULL
		WITH neighbor, count(DISTINCT any) AS degree
		WHERE degree = 1
		RETURN neighbor.code AS disconnectedCode
	`
	criticalResult, err := session.Run(ctx, criticalCypher, map[string]any{"pk": devicePK})
	if err == nil {
		for criticalResult.Next(ctx) {
			record := criticalResult.Record()
			if code, ok := record.Get("disconnectedCode"); ok {
				item.DisconnectedDevices = append(item.DisconnectedDevices, asString(code))
				item.Disconnected++
			}
		}
		item.CausesPartition = item.Disconnected > 0
	}

	return item
}

// MetroDevicePairPath represents the best path between a device pair across two metros
type MetroDevicePairPath struct {
	SourceDevicePK   string     `json:"sourceDevicePK"`
	SourceDeviceCode string     `json:"sourceDeviceCode"`
	TargetDevicePK   string     `json:"targetDevicePK"`
	TargetDeviceCode string     `json:"targetDeviceCode"`
	BestPath         SinglePath `json:"bestPath"`
}

// MetroDevicePathsResponse is the response for the metro device paths endpoint
type MetroDevicePathsResponse struct {
	FromMetroPK   string `json:"fromMetroPK"`
	FromMetroCode string `json:"fromMetroCode"`
	ToMetroPK     string `json:"toMetroPK"`
	ToMetroCode   string `json:"toMetroCode"`

	// Aggregate summary
	SourceDeviceCount int     `json:"sourceDeviceCount"`
	TargetDeviceCount int     `json:"targetDeviceCount"`
	TotalPairs        int     `json:"totalPairs"`
	MinHops           int     `json:"minHops"`
	MaxHops           int     `json:"maxHops"`
	MinLatencyMs      float64 `json:"minLatencyMs"`
	MaxLatencyMs      float64 `json:"maxLatencyMs"`
	AvgLatencyMs      float64 `json:"avgLatencyMs"`

	// All device pairs with their best path
	DevicePairs []MetroDevicePairPath `json:"devicePairs"`

	Error string `json:"error,omitempty"`
}

// GetMetroDevicePaths returns all paths between devices in two metros
// Query params: from (metro PK), to (metro PK)
func (a *API) GetMetroDevicePaths(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	fromMetroPK := r.URL.Query().Get("from")
	toMetroPK := r.URL.Query().Get("to")

	if fromMetroPK == "" || toMetroPK == "" {
		writeJSON(w, MetroDevicePathsResponse{Error: "from and to parameters are required"})
		return
	}

	if fromMetroPK == toMetroPK {
		writeJSON(w, MetroDevicePathsResponse{Error: "from and to must be different metros"})
		return
	}

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := MetroDevicePathsResponse{
		FromMetroPK: fromMetroPK,
		ToMetroPK:   toMetroPK,
		DevicePairs: []MetroDevicePairPath{},
	}

	// Get metro codes and devices in each metro
	metroCypher := `
		MATCH (m1:Metro {pk: $fromPK}), (m2:Metro {pk: $toPK})
		OPTIONAL MATCH (m1)<-[:LOCATED_IN]-(d1:Device)
		WHERE d1.isis_system_id IS NOT NULL
		WITH m1, m2, collect(d1) AS sourceDevices
		OPTIONAL MATCH (m2)<-[:LOCATED_IN]-(d2:Device)
		WHERE d2.isis_system_id IS NOT NULL
		RETURN m1.code AS fromCode, m2.code AS toCode,
		       [d IN sourceDevices | {pk: d.pk, code: d.code}] AS sourceDevices,
		       collect({pk: d2.pk, code: d2.code}) AS targetDevices
	`

	result, err := session.Run(ctx, metroCypher, map[string]any{
		"fromPK": fromMetroPK,
		"toPK":   toMetroPK,
	})
	if err != nil {
		logError("metro device paths metro query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	record, err := result.Single(ctx)
	if err != nil {
		logError("metro device paths metro query no result", "error", err)
		response.Error = "One or both metros not found"
		writeJSON(w, response)
		return
	}

	response.FromMetroCode = asString(record.Values[0])
	response.ToMetroCode = asString(record.Values[1])

	// Parse source and target devices
	type deviceInfo struct {
		PK   string
		Code string
	}
	var sourceDevices, targetDevices []deviceInfo

	if sourceList, ok := record.Values[2].([]any); ok {
		for _, item := range sourceList {
			if m, ok := item.(map[string]any); ok {
				sourceDevices = append(sourceDevices, deviceInfo{
					PK:   asString(m["pk"]),
					Code: asString(m["code"]),
				})
			}
		}
	}

	if targetList, ok := record.Values[3].([]any); ok {
		for _, item := range targetList {
			if m, ok := item.(map[string]any); ok {
				targetDevices = append(targetDevices, deviceInfo{
					PK:   asString(m["pk"]),
					Code: asString(m["code"]),
				})
			}
		}
	}

	response.SourceDeviceCount = len(sourceDevices)
	response.TargetDeviceCount = len(targetDevices)

	if len(sourceDevices) == 0 || len(targetDevices) == 0 {
		response.Error = "One or both metros have no ISIS-enabled devices"
		writeJSON(w, response)
		return
	}

	// Build list of all device pairs and find paths
	type pathResult struct {
		sourceIdx int
		targetIdx int
		path      SinglePath
		err       error
	}

	// Load graph once, find shortest path for each pair in-memory
	g, err := a.loadTopologyGraph(ctx)
	if err != nil {
		logError("metro device paths graph load error", "error", err)
		response.Error = "Failed to load graph: " + err.Error()
		writeJSON(w, response)
		return
	}

	results := make([]pathResult, 0, len(sourceDevices)*len(targetDevices))
	for i, source := range sourceDevices {
		for j, target := range targetDevices {
			kspPaths := yenKSP(g, source.PK, target.PK, 1)
			if len(kspPaths) == 0 {
				results = append(results, pathResult{sourceIdx: i, targetIdx: j, err: fmt.Errorf("no path")})
				continue
			}
			singlePaths := kspToSinglePaths(g, kspPaths[:1])
			results = append(results, pathResult{
				sourceIdx: i,
				targetIdx: j,
				path:      singlePaths[0],
			})
		}
	}

	// Build device pair paths from results
	var totalLatencyMs float64
	var pathCount int

	for _, res := range results {
		if res.err != nil {
			// No path found, skip this pair
			continue
		}

		source := sourceDevices[res.sourceIdx]
		target := targetDevices[res.targetIdx]

		response.DevicePairs = append(response.DevicePairs, MetroDevicePairPath{
			SourceDevicePK:   source.PK,
			SourceDeviceCode: source.Code,
			TargetDevicePK:   target.PK,
			TargetDeviceCode: target.Code,
			BestPath:         res.path,
		})

		// Update aggregate stats
		hops := res.path.HopCount
		latencyMs := float64(res.path.TotalMetric) / 1000.0

		if pathCount == 0 {
			response.MinHops = hops
			response.MaxHops = hops
			response.MinLatencyMs = latencyMs
			response.MaxLatencyMs = latencyMs
		} else {
			if hops < response.MinHops {
				response.MinHops = hops
			}
			if hops > response.MaxHops {
				response.MaxHops = hops
			}
			if latencyMs < response.MinLatencyMs {
				response.MinLatencyMs = latencyMs
			}
			if latencyMs > response.MaxLatencyMs {
				response.MaxLatencyMs = latencyMs
			}
		}

		totalLatencyMs += latencyMs
		pathCount++
	}

	response.TotalPairs = pathCount
	if pathCount > 0 {
		response.AvgLatencyMs = totalLatencyMs / float64(pathCount)
	}

	// Enrich paths with measured latency
	if len(response.DevicePairs) > 0 {
		multiPathResp := &MultiPathResponse{
			Paths: make([]SinglePath, len(response.DevicePairs)),
		}
		for i, pair := range response.DevicePairs {
			multiPathResp.Paths[i] = pair.BestPath
		}
		if err := a.enrichPathsWithMeasuredLatency(ctx, multiPathResp); err != nil {
			logError("enrichPathsWithMeasuredLatency error for metro paths", "error", err)
		} else {
			// Copy enriched paths back
			for i := range response.DevicePairs {
				response.DevicePairs[i].BestPath = multiPathResp.Paths[i]
			}

			// Recalculate latency stats using measured latency where available
			if len(response.DevicePairs) > 0 {
				var totalMeasured float64
				var measuredCount int
				response.MinLatencyMs = 0
				response.MaxLatencyMs = 0

				for i, pair := range response.DevicePairs {
					latencyMs := pair.BestPath.MeasuredLatencyMs
					if latencyMs == 0 {
						latencyMs = float64(pair.BestPath.TotalMetric) / 1000.0
					}
					if i == 0 || latencyMs < response.MinLatencyMs {
						response.MinLatencyMs = latencyMs
					}
					if latencyMs > response.MaxLatencyMs {
						response.MaxLatencyMs = latencyMs
					}
					totalMeasured += latencyMs
					measuredCount++
				}

				if measuredCount > 0 {
					response.AvgLatencyMs = totalMeasured / float64(measuredCount)
				}
			}
		}
	}

	// Sort device pairs by latency
	slices.SortFunc(response.DevicePairs, func(a, b MetroDevicePairPath) int {
		aLatency := a.BestPath.MeasuredLatencyMs
		if aLatency == 0 {
			aLatency = float64(a.BestPath.TotalMetric)
		}
		bLatency := b.BestPath.MeasuredLatencyMs
		if bLatency == 0 {
			bLatency = float64(b.BestPath.TotalMetric)
		}
		if aLatency < bLatency {
			return -1
		}
		if aLatency > bLatency {
			return 1
		}
		return 0
	})

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Info("GetMetroDevicePaths completed", "from", response.FromMetroCode, "to", response.ToMetroCode, "pairs", response.TotalPairs, "duration", duration)

	writeJSON(w, response)
}
