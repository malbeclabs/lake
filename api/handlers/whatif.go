package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
)

// SimulateLinkRemovalResponse is the response for simulating link removal
type SimulateLinkRemovalResponse struct {
	SourcePK            string         `json:"sourcePK"`
	SourceCode          string         `json:"sourceCode"`
	TargetPK            string         `json:"targetPK"`
	TargetCode          string         `json:"targetCode"`
	DisconnectedDevices []ImpactDevice `json:"disconnectedDevices"`
	DisconnectedCount   int            `json:"disconnectedCount"`
	AffectedPaths       []AffectedPath `json:"affectedPaths"`
	AffectedPathCount   int            `json:"affectedPathCount"`
	CausesPartition     bool           `json:"causesPartition"`
	Error               string         `json:"error,omitempty"`
}

// AffectedPath represents a path that would be affected by link removal
type AffectedPath struct {
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

// GetSimulateLinkRemoval simulates removing a link and shows the impact
func (a *API) GetSimulateLinkRemoval(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sourcePK := r.URL.Query().Get("sourcePK")
	targetPK := r.URL.Query().Get("targetPK")

	if sourcePK == "" || targetPK == "" {
		writeJSON(w, SimulateLinkRemovalResponse{Error: "sourcePK and targetPK parameters are required"})
		return
	}

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := SimulateLinkRemovalResponse{
		SourcePK:            sourcePK,
		TargetPK:            targetPK,
		DisconnectedDevices: []ImpactDevice{},
		AffectedPaths:       []AffectedPath{},
	}

	// Get device codes
	codesCypher := `
		MATCH (s:Device {pk: $source_pk})
		MATCH (t:Device {pk: $target_pk})
		RETURN s.code AS source_code, t.code AS target_code
	`
	codesResult, err := session.Run(ctx, codesCypher, map[string]any{
		"source_pk": sourcePK,
		"target_pk": targetPK,
	})
	if err != nil {
		slog.Error("simulate link removal codes query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}
	if codesRecord, err := codesResult.Single(ctx); err == nil {
		sourceCode, _ := codesRecord.Get("source_code")
		targetCode, _ := codesRecord.Get("target_code")
		response.SourceCode = asString(sourceCode)
		response.TargetCode = asString(targetCode)
	}

	// Check if removing this link would disconnect any devices
	// A device becomes disconnected if it has degree 1 (leaf node) - removing its only link disconnects it
	disconnectCypher := `
		MATCH (s:Device {pk: $source_pk}), (t:Device {pk: $target_pk})
		WHERE s.isis_system_id IS NOT NULL AND t.isis_system_id IS NOT NULL

		// Count neighbors of each endpoint
		OPTIONAL MATCH (s)-[:ISIS_ADJACENT]-(sn:Device)
		WHERE sn.isis_system_id IS NOT NULL
		WITH s, t, count(DISTINCT sn) AS sourceDegree
		OPTIONAL MATCH (t)-[:ISIS_ADJACENT]-(tn:Device)
		WHERE tn.isis_system_id IS NOT NULL
		WITH s, t, sourceDegree, count(DISTINCT tn) AS targetDegree

		// A partition occurs if one endpoint has degree 1 (it's a leaf node)
		// If both have degree > 1, there must be alternate paths (even if longer)
		WITH s, t, sourceDegree, targetDegree,
		     CASE WHEN sourceDegree = 1 OR targetDegree = 1 THEN true ELSE false END AS causesPartition
		WHERE causesPartition = true

		// Return the device(s) that would be disconnected - the leaf node(s)
		UNWIND CASE
			WHEN sourceDegree = 1 AND targetDegree = 1 THEN [s, t]
			WHEN sourceDegree = 1 THEN [s]
			WHEN targetDegree = 1 THEN [t]
			ELSE []
		END AS d
		RETURN d.pk AS pk, d.code AS code, d.status AS status, d.device_type AS device_type
	`

	disconnectResult, err := session.Run(ctx, disconnectCypher, map[string]any{
		"source_pk": sourcePK,
		"target_pk": targetPK,
	})
	if err != nil {
		slog.Error("simulate link removal disconnect query error", "error", err)
		response.Error = "failed to query disconnect impact"
	} else {
		disconnectRecords, err := disconnectResult.Collect(ctx)
		if err != nil {
			slog.Error("simulate link removal disconnect collect error", "error", err)
			response.Error = "failed to query disconnect impact"
		} else {
			slog.Debug("simulate link removal disconnect query returned records", "count", len(disconnectRecords))
			for _, record := range disconnectRecords {
				pk, _ := record.Get("pk")
				code, _ := record.Get("code")
				status, _ := record.Get("status")
				deviceType, _ := record.Get("device_type")

				response.DisconnectedDevices = append(response.DisconnectedDevices, ImpactDevice{
					PK:         asString(pk),
					Code:       asString(code),
					Status:     asString(status),
					DeviceType: asString(deviceType),
				})
			}
		}
	}
	response.DisconnectedCount = len(response.DisconnectedDevices)
	response.CausesPartition = response.DisconnectedCount > 0

	// Find affected paths - paths that currently use this link
	// Simplified query: just check direct neighbors of source and target
	affectedCypher := `
		MATCH (src:Device {pk: $source_pk}), (tgt:Device {pk: $target_pk})
		WHERE src.isis_system_id IS NOT NULL AND tgt.isis_system_id IS NOT NULL

		// Get the metric of the link being removed
		OPTIONAL MATCH (src)-[linkRel:ISIS_ADJACENT]-(tgt)
		WITH src, tgt, min(linkRel.metric) AS linkMetric

		// Get immediate neighbors of source with their link metrics
		OPTIONAL MATCH (src)-[srcRel:ISIS_ADJACENT]-(srcNeighbor:Device)
		WHERE srcNeighbor.isis_system_id IS NOT NULL AND srcNeighbor.pk <> tgt.pk
		WITH src, tgt, linkMetric, collect(DISTINCT {device: srcNeighbor, metric: srcRel.metric}) AS srcNeighborsData

		// Get immediate neighbors of target with their link metrics
		OPTIONAL MATCH (tgt)-[tgtRel:ISIS_ADJACENT]-(tgtNeighbor:Device)
		WHERE tgtNeighbor.isis_system_id IS NOT NULL AND tgtNeighbor.pk <> src.pk
		WITH src, tgt, linkMetric, srcNeighborsData, collect(DISTINCT {device: tgtNeighbor, metric: tgtRel.metric}) AS tgtNeighborsData

		// For each source neighbor, check path to target neighbors via this link
		UNWIND CASE WHEN size(srcNeighborsData) > 0 THEN srcNeighborsData ELSE [null] END AS srcData
		UNWIND CASE WHEN size(tgtNeighborsData) > 0 THEN tgtNeighborsData ELSE [null] END AS tgtData
		WITH src, tgt, linkMetric, srcData, tgtData
		WHERE srcData IS NOT NULL AND tgtData IS NOT NULL
		  AND srcData.device.pk <> tgtData.device.pk

		WITH srcData.device AS fromDevice, tgtData.device AS toDevice, src, tgt,
		     3 AS beforeHops,
		     coalesce(srcData.metric, 0) + coalesce(linkMetric, 0) + coalesce(tgtData.metric, 0) AS beforeMetric

		// Check if there's an alternate path not using the link being removed
		OPTIONAL MATCH altPath = shortestPath((fromDevice)-[:ISIS_ADJACENT*]-(toDevice))
		WHERE NONE(r IN relationships(altPath) WHERE
		      (startNode(r).pk = src.pk AND endNode(r).pk = tgt.pk) OR
		      (startNode(r).pk = tgt.pk AND endNode(r).pk = src.pk))
		WITH fromDevice, toDevice, beforeHops, beforeMetric, altPath,
		     CASE WHEN altPath IS NOT NULL THEN length(altPath) ELSE 0 END AS afterHops,
		     CASE WHEN altPath IS NOT NULL
		          THEN reduce(total = 0, r IN relationships(altPath) | total + coalesce(r.metric, 0))
		          ELSE 0 END AS afterMetric

		// Only include paths where the path through the link is actually preferred
		WHERE afterHops = 0 OR (afterHops > 0 AND beforeMetric < afterMetric)

		RETURN fromDevice.pk AS from_pk,
		       fromDevice.code AS from_code,
		       toDevice.pk AS to_pk,
		       toDevice.code AS to_code,
		       beforeHops,
		       beforeMetric,
		       afterHops,
		       afterMetric
		LIMIT 5
	`

	affectedResult, err := session.Run(ctx, affectedCypher, map[string]any{
		"source_pk": sourcePK,
		"target_pk": targetPK,
	})
	if err != nil {
		slog.Error("simulate link removal affected paths query error", "error", err)
		response.Error = "failed to query affected paths"
	} else {
		affectedRecords, err := affectedResult.Collect(ctx)
		if err != nil {
			slog.Error("simulate link removal affected paths collect error", "error", err)
			response.Error = "failed to query affected paths"
		} else {
			for _, record := range affectedRecords {
				fromPK, _ := record.Get("from_pk")
				fromCode, _ := record.Get("from_code")
				toPK, _ := record.Get("to_pk")
				toCode, _ := record.Get("to_code")
				beforeHops, _ := record.Get("beforeHops")
				beforeMetric, _ := record.Get("beforeMetric")
				afterHops, _ := record.Get("afterHops")
				afterMetric, _ := record.Get("afterMetric")

				hasAlternate := afterHops != nil && asInt64(afterHops) > 0

				response.AffectedPaths = append(response.AffectedPaths, AffectedPath{
					FromPK:       asString(fromPK),
					FromCode:     asString(fromCode),
					ToPK:         asString(toPK),
					ToCode:       asString(toCode),
					BeforeHops:   int(asInt64(beforeHops)),
					BeforeMetric: uint32(asInt64(beforeMetric)),
					AfterHops:    int(asInt64(afterHops)),
					AfterMetric:  uint32(asInt64(afterMetric)),
					HasAlternate: hasAlternate,
				})
			}
		}
	}
	response.AffectedPathCount = len(response.AffectedPaths)

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Debug("simulate link removal complete",
		"source", response.SourceCode, "target", response.TargetCode,
		"disconnected", response.DisconnectedCount, "affected_paths", response.AffectedPathCount,
		"partition", response.CausesPartition, "duration", duration)

	writeJSON(w, response)
}

// SimulateLinkAdditionResponse is the response for simulating link addition
type SimulateLinkAdditionResponse struct {
	SourcePK          string           `json:"sourcePK"`
	SourceCode        string           `json:"sourceCode"`
	TargetPK          string           `json:"targetPK"`
	TargetCode        string           `json:"targetCode"`
	Metric            uint32           `json:"metric"`
	ImprovedPaths     []ImprovedPath   `json:"improvedPaths"`
	ImprovedPathCount int              `json:"improvedPathCount"`
	RedundancyGains   []RedundancyGain `json:"redundancyGains"`
	RedundancyCount   int              `json:"redundancyCount"`
	Error             string           `json:"error,omitempty"`
}

// ImprovedPath represents a path that would be improved by adding a link
type ImprovedPath struct {
	FromPK          string `json:"fromPK"`
	FromCode        string `json:"fromCode"`
	ToPK            string `json:"toPK"`
	ToCode          string `json:"toCode"`
	BeforeHops      int    `json:"beforeHops"`
	BeforeMetric    uint32 `json:"beforeMetric"`
	AfterHops       int    `json:"afterHops"`
	AfterMetric     uint32 `json:"afterMetric"`
	HopReduction    int    `json:"hopReduction"`
	MetricReduction uint32 `json:"metricReduction"`
}

// RedundancyGain represents a device that would gain redundancy
type RedundancyGain struct {
	DevicePK   string `json:"devicePK"`
	DeviceCode string `json:"deviceCode"`
	OldDegree  int    `json:"oldDegree"`
	NewDegree  int    `json:"newDegree"`
	WasLeaf    bool   `json:"wasLeaf"` // Was a single point of failure
}

// GetSimulateLinkAddition simulates adding a link and shows the benefits
func (a *API) GetSimulateLinkAddition(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sourcePK := r.URL.Query().Get("sourcePK")
	targetPK := r.URL.Query().Get("targetPK")
	metricStr := r.URL.Query().Get("metric")

	if sourcePK == "" || targetPK == "" {
		writeJSON(w, SimulateLinkAdditionResponse{Error: "sourcePK and targetPK parameters are required"})
		return
	}

	if sourcePK == targetPK {
		writeJSON(w, SimulateLinkAdditionResponse{Error: "sourcePK and targetPK must be different"})
		return
	}

	metric := uint32(1000) // Default 1ms metric
	if metricStr != "" {
		if parsed, err := strconv.ParseUint(metricStr, 10, 32); err == nil {
			metric = uint32(parsed)
		}
	}

	start := time.Now()

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := SimulateLinkAdditionResponse{
		SourcePK:        sourcePK,
		TargetPK:        targetPK,
		Metric:          metric,
		ImprovedPaths:   []ImprovedPath{},
		RedundancyGains: []RedundancyGain{},
	}

	// Get device codes and current degrees
	codesCypher := `
		MATCH (s:Device {pk: $source_pk})
		MATCH (t:Device {pk: $target_pk})
		OPTIONAL MATCH (s)-[:ISIS_ADJACENT]-(sn:Device)
		WHERE sn.isis_system_id IS NOT NULL
		WITH s, t, count(DISTINCT sn) AS sourceDegree
		OPTIONAL MATCH (t)-[:ISIS_ADJACENT]-(tn:Device)
		WHERE tn.isis_system_id IS NOT NULL
		RETURN s.code AS source_code, t.code AS target_code,
		       sourceDegree, count(DISTINCT tn) AS targetDegree
	`
	codesResult, err := session.Run(ctx, codesCypher, map[string]any{
		"source_pk": sourcePK,
		"target_pk": targetPK,
	})
	if err != nil {
		slog.Error("simulate link addition codes query error", "error", err)
		response.Error = err.Error()
		writeJSON(w, response)
		return
	}

	var sourceDegree, targetDegree int
	if codesRecord, err := codesResult.Single(ctx); err == nil {
		sourceCode, _ := codesRecord.Get("source_code")
		targetCode, _ := codesRecord.Get("target_code")
		srcDeg, _ := codesRecord.Get("sourceDegree")
		tgtDeg, _ := codesRecord.Get("targetDegree")
		response.SourceCode = asString(sourceCode)
		response.TargetCode = asString(targetCode)
		sourceDegree = int(asInt64(srcDeg))
		targetDegree = int(asInt64(tgtDeg))
	}

	// Check if link already exists
	existsCypher := `
		MATCH (s:Device {pk: $source_pk})-[r:ISIS_ADJACENT]-(t:Device {pk: $target_pk})
		RETURN count(r) > 0 AS exists
	`
	existsResult, err := session.Run(ctx, existsCypher, map[string]any{
		"source_pk": sourcePK,
		"target_pk": targetPK,
	})
	if err == nil {
		if existsRecord, err := existsResult.Single(ctx); err == nil {
			exists, _ := existsRecord.Get("exists")
			if asBool(exists) {
				response.Error = "Link already exists between these devices"
				writeJSON(w, response)
				return
			}
		}
	}

	// Calculate redundancy gains
	// A device gains redundancy if it was a leaf (degree 1) and the new link increases its degree
	if sourceDegree == 1 {
		response.RedundancyGains = append(response.RedundancyGains, RedundancyGain{
			DevicePK:   sourcePK,
			DeviceCode: response.SourceCode,
			OldDegree:  sourceDegree,
			NewDegree:  sourceDegree + 1,
			WasLeaf:    true,
		})
	}
	if targetDegree == 1 {
		response.RedundancyGains = append(response.RedundancyGains, RedundancyGain{
			DevicePK:   targetPK,
			DeviceCode: response.TargetCode,
			OldDegree:  targetDegree,
			NewDegree:  targetDegree + 1,
			WasLeaf:    true,
		})
	}
	response.RedundancyCount = len(response.RedundancyGains)

	// Find paths that would be improved by the new link
	// We use a simpler approach: check current path between source and target,
	// and also check paths from their immediate neighbors
	improvedCypher := `
		// Get the source and target devices
		MATCH (src:Device {pk: $source_pk}), (tgt:Device {pk: $target_pk})

		// Get immediate neighbors of source (1 hop)
		OPTIONAL MATCH (src)-[:ISIS_ADJACENT]-(srcNeighbor:Device)
		WHERE srcNeighbor.isis_system_id IS NOT NULL AND srcNeighbor.pk <> tgt.pk
		WITH src, tgt, collect(DISTINCT srcNeighbor) AS srcNeighbors

		// Get immediate neighbors of target (1 hop)
		OPTIONAL MATCH (tgt)-[:ISIS_ADJACENT]-(tgtNeighbor:Device)
		WHERE tgtNeighbor.isis_system_id IS NOT NULL AND tgtNeighbor.pk <> src.pk
		WITH src, tgt, srcNeighbors, collect(DISTINCT tgtNeighbor) AS tgtNeighbors

		// Build device pairs to check: (source neighbors) -> (target neighbors)
		// Include src->tgt direct path check
		WITH src, tgt, srcNeighbors, tgtNeighbors,
		     [src] + srcNeighbors AS sourceSide,
		     [tgt] + tgtNeighbors AS targetSide

		UNWIND sourceSide AS from
		UNWIND targetSide AS to
		WITH src, tgt, from, to
		WHERE from.pk <> to.pk

		// Get current shortest path (OPTIONAL to handle disconnected graphs)
		OPTIONAL MATCH currentPath = shortestPath((from)-[:ISIS_ADJACENT*..10]-(to))
		WITH from, to, src, tgt, currentPath,
		     CASE WHEN currentPath IS NOT NULL THEN length(currentPath) ELSE 999 END AS currentHops,
		     CASE WHEN currentPath IS NOT NULL
		          THEN reduce(total = 0, r IN relationships(currentPath) | total + coalesce(r.metric, 0))
		          ELSE 999999 END AS currentMetric
		WHERE currentPath IS NOT NULL AND length(currentPath) > 2

		// Calculate path via new link: from -> src -> [new link] -> tgt -> to
		// Handle shortestPath carefully to avoid same start/end node error
		OPTIONAL MATCH p1 = shortestPath((from)-[:ISIS_ADJACENT*..10]-(src))
		WHERE from.pk <> src.pk
		OPTIONAL MATCH p2 = shortestPath((tgt)-[:ISIS_ADJACENT*..10]-(to))
		WHERE to.pk <> tgt.pk
		WITH from, to, src, tgt, currentHops, currentMetric, p1, p2,
		     from.pk = src.pk AS fromIsSrc,
		     to.pk = tgt.pk AS toIsTgt
		WITH from, to,
		     currentHops, currentMetric,
		     CASE WHEN fromIsSrc AND toIsTgt THEN 1
		          WHEN fromIsSrc AND p2 IS NOT NULL THEN 1 + length(p2)
		          WHEN toIsTgt AND p1 IS NOT NULL THEN length(p1) + 1
		          WHEN p1 IS NOT NULL AND p2 IS NOT NULL THEN length(p1) + 1 + length(p2)
		          ELSE 999 END AS viaNewLinkHops,
		     CASE WHEN fromIsSrc AND toIsTgt THEN $metric
		          WHEN fromIsSrc AND p2 IS NOT NULL
		               THEN $metric + reduce(t = 0, r IN relationships(p2) | t + coalesce(r.metric, 0))
		          WHEN toIsTgt AND p1 IS NOT NULL
		               THEN reduce(t = 0, r IN relationships(p1) | t + coalesce(r.metric, 0)) + $metric
		          WHEN p1 IS NOT NULL AND p2 IS NOT NULL
		               THEN reduce(t = 0, r IN relationships(p1) | t + coalesce(r.metric, 0)) + $metric +
		                    reduce(t = 0, r IN relationships(p2) | t + coalesce(r.metric, 0))
		          ELSE 999999 END AS viaNewLinkMetric

		// Only return if the new link provides improvement
		WHERE viaNewLinkHops < currentHops
		RETURN from.pk AS from_pk,
		       from.code AS from_code,
		       to.pk AS to_pk,
		       to.code AS to_code,
		       currentHops AS before_hops,
		       currentMetric AS before_metric,
		       viaNewLinkHops AS after_hops,
		       viaNewLinkMetric AS after_metric
		ORDER BY (currentHops - viaNewLinkHops) DESC
		LIMIT 15
	`

	improvedResult, err := session.Run(ctx, improvedCypher, map[string]any{
		"source_pk": sourcePK,
		"target_pk": targetPK,
		"metric":    int64(metric),
	})
	if err != nil {
		slog.Error("simulate link addition improved paths query error", "error", err)
		response.Error = "failed to query improved paths: " + err.Error()
	} else {
		improvedRecords, err := improvedResult.Collect(ctx)
		if err != nil {
			slog.Error("simulate link addition improved paths collect error", "error", err)
			response.Error = "failed to query improved paths: " + err.Error()
		} else {
			for _, record := range improvedRecords {
				fromPK, _ := record.Get("from_pk")
				fromCode, _ := record.Get("from_code")
				toPK, _ := record.Get("to_pk")
				toCode, _ := record.Get("to_code")
				beforeHops, _ := record.Get("before_hops")
				beforeMetric, _ := record.Get("before_metric")
				afterHops, _ := record.Get("after_hops")
				afterMetric, _ := record.Get("after_metric")

				bHops := int(asInt64(beforeHops))
				aHops := int(asInt64(afterHops))
				bMetric := uint32(asInt64(beforeMetric))
				aMetric := uint32(asInt64(afterMetric))

				response.ImprovedPaths = append(response.ImprovedPaths, ImprovedPath{
					FromPK:          asString(fromPK),
					FromCode:        asString(fromCode),
					ToPK:            asString(toPK),
					ToCode:          asString(toCode),
					BeforeHops:      bHops,
					BeforeMetric:    bMetric,
					AfterHops:       aHops,
					AfterMetric:     aMetric,
					HopReduction:    bHops - aHops,
					MetricReduction: bMetric - aMetric,
				})
			}
		}
	}
	response.ImprovedPathCount = len(response.ImprovedPaths)

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Debug("simulate link addition complete",
		"source", response.SourceCode, "target", response.TargetCode, "metric", metric,
		"improved_paths", response.ImprovedPathCount, "redundancy_gains", response.RedundancyCount, "duration", duration)

	writeJSON(w, response)
}

// WhatIfRemovalRequest is the request body for unified what-if removal analysis
type WhatIfRemovalRequest struct {
	Devices []string `json:"devices"` // Device PKs
	Links   []string `json:"links"`   // Link PKs
}

// WhatIfRemovalResponse is the response for unified what-if removal analysis
type WhatIfRemovalResponse struct {
	Items              []WhatIfRemovalItem  `json:"items"`
	TotalAffectedPaths int                  `json:"totalAffectedPaths"`
	TotalDisconnected  int                  `json:"totalDisconnected"`
	AffectedPaths      []WhatIfAffectedPath `json:"affectedPaths,omitempty"`
	DisconnectedList   []string             `json:"disconnectedList,omitempty"`
	Error              string               `json:"error,omitempty"`
}

// WhatIfRemovalItem represents impact of a single device or link removal
type WhatIfRemovalItem struct {
	Type                string               `json:"type"` // "device" or "link"
	PK                  string               `json:"pk"`   // Device PK or Link PK
	Code                string               `json:"code"` // Display name
	AffectedPaths       []WhatIfAffectedPath `json:"affectedPaths"`
	AffectedPathCount   int                  `json:"affectedPathCount"`
	DisconnectedDevices []string             `json:"disconnectedDevices"`
	DisconnectedCount   int                  `json:"disconnectedCount"`
	CausesPartition     bool                 `json:"causesPartition"`
}

// WhatIfAffectedPath represents a path affected by removal
type WhatIfAffectedPath struct {
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceMetro  string `json:"sourceMetro,omitempty"`
	TargetMetro  string `json:"targetMetro,omitempty"`
	HopsBefore   int    `json:"hopsBefore"`
	MetricBefore int    `json:"metricBefore"`
	HopsAfter    int    `json:"hopsAfter"`   // -1 if disconnected
	MetricAfter  int    `json:"metricAfter"` // -1 if disconnected
	Status       string `json:"status"`      // "rerouted", "degraded", "disconnected"
}

// PostWhatIfRemoval analyzes the impact of removing devices and/or links
func (a *API) PostWhatIfRemoval(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()

	// Parse request body
	var req WhatIfRemovalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, WhatIfRemovalResponse{Error: "Invalid request body: " + err.Error()})
		return
	}

	if len(req.Devices) == 0 && len(req.Links) == 0 {
		writeJSON(w, WhatIfRemovalResponse{Error: "No devices or links specified"})
		return
	}

	session := a.neo4jSession(ctx)
	defer session.Close(ctx)

	response := WhatIfRemovalResponse{
		Items:            []WhatIfRemovalItem{},
		AffectedPaths:    []WhatIfAffectedPath{},
		DisconnectedList: []string{},
	}

	// Analyze each device
	for _, devicePK := range req.Devices {
		item := a.analyzeDeviceRemoval(ctx, session, devicePK, 10)
		response.Items = append(response.Items, item)
		response.TotalAffectedPaths += item.AffectedPathCount
		response.TotalDisconnected += item.DisconnectedCount
		response.AffectedPaths = append(response.AffectedPaths, item.AffectedPaths...)
		response.DisconnectedList = append(response.DisconnectedList, item.DisconnectedDevices...)
	}

	// Analyze each link
	for _, linkPK := range req.Links {
		item := a.analyzeLinkRemoval(ctx, session, linkPK, 10)
		response.Items = append(response.Items, item)
		response.TotalAffectedPaths += item.AffectedPathCount
		response.TotalDisconnected += item.DisconnectedCount
		response.AffectedPaths = append(response.AffectedPaths, item.AffectedPaths...)
		response.DisconnectedList = append(response.DisconnectedList, item.DisconnectedDevices...)
	}

	// Limit total affected paths in response
	if len(response.AffectedPaths) > 50 {
		response.AffectedPaths = response.AffectedPaths[:50]
	}

	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, nil)

	slog.Debug("what-if removal complete",
		"devices", len(req.Devices), "links", len(req.Links),
		"total_paths", response.TotalAffectedPaths, "total_disconnected", response.TotalDisconnected, "duration", duration)

	writeJSON(w, response)
}

// analyzeDeviceRemoval computes the impact of removing a single device
func (a *API) analyzeDeviceRemoval(ctx context.Context, session neo4j.Session, devicePK string, pathLimit int) WhatIfRemovalItem {
	item := WhatIfRemovalItem{
		Type:                "device",
		PK:                  devicePK,
		AffectedPaths:       []WhatIfAffectedPath{},
		DisconnectedDevices: []string{},
	}

	// Get device code and neighbor info
	infoCypher := `
		MATCH (d:Device {pk: $devicePK})
		WHERE d.isis_system_id IS NOT NULL

		// Get device code
		WITH d

		// Find all neighbors
		OPTIONAL MATCH (d)-[:ISIS_ADJACENT]-(neighbor:Device)
		WHERE neighbor.isis_system_id IS NOT NULL
		WITH d, collect(DISTINCT neighbor) AS neighbors

		// Find leaf neighbors (degree 1) that would be disconnected
		UNWIND neighbors AS neighbor
		OPTIONAL MATCH (neighbor)-[:ISIS_ADJACENT]-(neighborOfNeighbor:Device)
		WHERE neighborOfNeighbor.isis_system_id IS NOT NULL
		WITH d, neighbor, count(DISTINCT neighborOfNeighbor) AS neighborDegree
		WITH d,
		     collect(CASE WHEN neighborDegree = 1 THEN neighbor.code ELSE null END) AS leafCodes

		RETURN d.code AS code,
		       [x IN leafCodes WHERE x IS NOT NULL] AS disconnectedDevices
	`

	result, err := session.Run(ctx, infoCypher, map[string]any{"devicePK": devicePK})
	if err != nil {
		slog.Error("device removal info query error", "error", err)
		item.Code = devicePK
		return item
	}

	if result.Next(ctx) {
		record := result.Record()
		code, _ := record.Get("code")
		disconnected, _ := record.Get("disconnectedDevices")

		item.Code = asString(code)
		if arr, ok := disconnected.([]any); ok {
			for _, v := range arr {
				if s := asString(v); s != "" {
					item.DisconnectedDevices = append(item.DisconnectedDevices, s)
				}
			}
		}
	}

	item.DisconnectedCount = len(item.DisconnectedDevices)
	item.CausesPartition = item.DisconnectedCount > 0

	// Get all neighbors with their ISIS metrics and metro info
	neighborsCypher := `
		MATCH (d:Device {pk: $devicePK})
		WHERE d.isis_system_id IS NOT NULL
		MATCH (d)-[r:ISIS_ADJACENT]-(neighbor:Device)
		WHERE neighbor.isis_system_id IS NOT NULL
		OPTIONAL MATCH (neighbor)-[:LOCATED_IN]->(m:Metro)
		RETURN neighbor.pk AS pk, neighbor.code AS code,
		       min(r.metric) AS metric, COALESCE(m.code, '') AS metro
	`

	neighborsResult, err := session.Run(ctx, neighborsCypher, map[string]any{"devicePK": devicePK})
	if err != nil {
		slog.Error("device removal neighbors query error", "error", err)
		return item
	}

	type neighborInfo struct {
		pk     string
		code   string
		metric int
		metro  string
	}
	var neighbors []neighborInfo

	for neighborsResult.Next(ctx) {
		record := neighborsResult.Record()
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		metric, _ := record.Get("metric")
		metro, _ := record.Get("metro")
		neighbors = append(neighbors, neighborInfo{
			pk:     asString(pk),
			code:   asString(code),
			metric: int(asInt64(metric)),
			metro:  asString(metro),
		})
	}

	// For each neighbor pair, check if ISIS would route through this device (concurrently)
	type pairResult struct {
		path     WhatIfAffectedPath
		affected bool
	}

	type pair struct {
		n1, n2 neighborInfo
	}
	var pairs []pair
	for i := 0; i < len(neighbors); i++ {
		for j := i + 1; j < len(neighbors); j++ {
			pairs = append(pairs, pair{neighbors[i], neighbors[j]})
		}
	}

	results := make([]pairResult, len(pairs))
	var wg sync.WaitGroup
	for idx, p := range pairs {
		wg.Add(1)
		go func(idx int, n1, n2 neighborInfo) {
			defer wg.Done()

			throughMetric := n1.metric + n2.metric

			altCypher := `
				MATCH (n1:Device {pk: $n1PK}), (n2:Device {pk: $n2PK})
				MATCH altPath = (n1)-[:ISIS_ADJACENT*1..6]-(n2)
				WHERE none(n IN nodes(altPath) WHERE n.pk = $devicePK)
				WITH altPath, length(altPath) AS altHops,
				     reduce(m = 0, r IN relationships(altPath) | m + coalesce(r.metric, 0)) AS altMetric
				ORDER BY altMetric ASC
				LIMIT 1
				RETURN altHops, altMetric
			`

			pairSession := a.neo4jSession(ctx)
			defer pairSession.Close(ctx)

			altResult, err := pairSession.Run(ctx, altCypher, map[string]any{
				"n1PK":     n1.pk,
				"n2PK":     n2.pk,
				"devicePK": devicePK,
			})

			path := WhatIfAffectedPath{
				Source:       n1.code,
				Target:       n2.code,
				SourceMetro:  n1.metro,
				TargetMetro:  n2.metro,
				HopsBefore:   2,
				MetricBefore: throughMetric,
				HopsAfter:    -1,
				MetricAfter:  -1,
				Status:       "disconnected",
			}

			if err == nil && altResult.Next(ctx) {
				record := altResult.Record()
				altHops, _ := record.Get("altHops")
				altMetric, _ := record.Get("altMetric")
				altMetricVal := int(asInt64(altMetric))

				if throughMetric > altMetricVal {
					results[idx] = pairResult{affected: false}
					return
				}

				path.HopsAfter = int(asInt64(altHops))
				path.MetricAfter = altMetricVal

				metricIncrease := path.MetricAfter - path.MetricBefore
				hopIncrease := path.HopsAfter - path.HopsBefore
				if hopIncrease > 2 || metricIncrease > 50 {
					path.Status = "degraded"
				} else {
					path.Status = "rerouted"
				}
			}

			results[idx] = pairResult{path: path, affected: true}
		}(idx, p.n1, p.n2)
	}
	wg.Wait()

	for _, r := range results {
		if r.affected {
			item.AffectedPaths = append(item.AffectedPaths, r.path)
			if len(item.AffectedPaths) >= pathLimit {
				break
			}
		}
	}

	item.AffectedPathCount = len(item.AffectedPaths)
	return item
}

// analyzeLinkRemoval computes the impact of removing a single link
func (a *API) analyzeLinkRemoval(ctx context.Context, session neo4j.Session, linkPK string, pathLimit int) WhatIfRemovalItem {
	item := WhatIfRemovalItem{
		Type:                "link",
		PK:                  linkPK,
		AffectedPaths:       []WhatIfAffectedPath{},
		DisconnectedDevices: []string{},
	}

	// First, resolve link PK to device endpoints via ClickHouse
	linkQuery := `
		SELECT
			COALESCE(l.side_a_pk, '') as side_a_pk,
			COALESCE(l.side_z_pk, '') as side_z_pk,
			COALESCE(da.code, '') as side_a_code,
			COALESCE(dz.code, '') as side_z_code
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		WHERE l.pk = $1
	`

	var sideAPK, sideZPK, sideACode, sideZCode string
	if err := a.envDB(ctx).QueryRow(ctx, linkQuery, linkPK).Scan(&sideAPK, &sideZPK, &sideACode, &sideZCode); err != nil {
		slog.Error("link lookup error", "link_pk", linkPK, "error", err)
		item.Code = "Link not found"
		return item
	}

	if sideAPK == "" || sideZPK == "" {
		item.Code = "Link missing endpoints"
		return item
	}

	item.Code = sideACode + " - " + sideZCode

	// Check if either endpoint is a leaf (would be disconnected)
	disconnectCypher := `
		MATCH (s:Device {pk: $sourcePK}), (t:Device {pk: $targetPK})
		WHERE s.isis_system_id IS NOT NULL AND t.isis_system_id IS NOT NULL

		// Count neighbors of each endpoint
		OPTIONAL MATCH (s)-[:ISIS_ADJACENT]-(sn:Device)
		WHERE sn.isis_system_id IS NOT NULL
		WITH s, t, count(DISTINCT sn) AS sourceDegree
		OPTIONAL MATCH (t)-[:ISIS_ADJACENT]-(tn:Device)
		WHERE tn.isis_system_id IS NOT NULL
		WITH s, t, sourceDegree, count(DISTINCT tn) AS targetDegree

		RETURN sourceDegree, targetDegree, s.code AS sourceCode, t.code AS targetCode
	`

	degResult, err := session.Run(ctx, disconnectCypher, map[string]any{
		"sourcePK": sideAPK,
		"targetPK": sideZPK,
	})
	if err != nil {
		slog.Error("link disconnect check error", "error", err)
	} else if degResult.Next(ctx) {
		record := degResult.Record()
		sourceDegree, _ := record.Get("sourceDegree")
		targetDegree, _ := record.Get("targetDegree")
		sourceCode, _ := record.Get("sourceCode")
		targetCode, _ := record.Get("targetCode")

		if asInt64(sourceDegree) == 1 {
			item.DisconnectedDevices = append(item.DisconnectedDevices, asString(sourceCode))
		}
		if asInt64(targetDegree) == 1 {
			item.DisconnectedDevices = append(item.DisconnectedDevices, asString(targetCode))
		}
	}

	item.DisconnectedCount = len(item.DisconnectedDevices)
	item.CausesPartition = item.DisconnectedCount > 0

	// Get the ISIS metric of the link being removed
	linkMetricCypher := `
		MATCH (src:Device {pk: $sourcePK})-[r:ISIS_ADJACENT]-(tgt:Device {pk: $targetPK})
		RETURN min(r.metric) AS linkMetric
	`

	var linkMetric int
	linkMetricResult, err := session.Run(ctx, linkMetricCypher, map[string]any{
		"sourcePK": sideAPK,
		"targetPK": sideZPK,
	})
	if err != nil {
		slog.Error("link metric query error", "error", err)
		return item
	}
	if linkMetricResult.Next(ctx) {
		record := linkMetricResult.Record()
		m, _ := record.Get("linkMetric")
		linkMetric = int(asInt64(m))
	}

	type neighborInfo struct {
		pk     string
		code   string
		metric int
		metro  string
	}

	// Get neighbors of source endpoint with their ISIS metrics
	srcNeighborsCypher := `
		MATCH (src:Device {pk: $sourcePK})-[r:ISIS_ADJACENT]-(n:Device)
		WHERE n.isis_system_id IS NOT NULL AND n.pk <> $targetPK
		OPTIONAL MATCH (n)-[:LOCATED_IN]->(m:Metro)
		RETURN n.pk AS pk, n.code AS code, min(r.metric) AS metric, COALESCE(m.code, '') AS metro
	`
	srcNResult, err := session.Run(ctx, srcNeighborsCypher, map[string]any{
		"sourcePK": sideAPK,
		"targetPK": sideZPK,
	})
	if err != nil {
		slog.Error("link src neighbors query error", "error", err)
		return item
	}

	var srcNeighbors []neighborInfo
	for srcNResult.Next(ctx) {
		record := srcNResult.Record()
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		metric, _ := record.Get("metric")
		metro, _ := record.Get("metro")
		srcNeighbors = append(srcNeighbors, neighborInfo{
			pk: asString(pk), code: asString(code),
			metric: int(asInt64(metric)), metro: asString(metro),
		})
	}

	// Get neighbors of target endpoint with their ISIS metrics
	tgtNeighborsCypher := `
		MATCH (tgt:Device {pk: $targetPK})-[r:ISIS_ADJACENT]-(n:Device)
		WHERE n.isis_system_id IS NOT NULL AND n.pk <> $sourcePK
		OPTIONAL MATCH (n)-[:LOCATED_IN]->(m:Metro)
		RETURN n.pk AS pk, n.code AS code, min(r.metric) AS metric, COALESCE(m.code, '') AS metro
	`
	tgtNResult, err := session.Run(ctx, tgtNeighborsCypher, map[string]any{
		"sourcePK": sideAPK,
		"targetPK": sideZPK,
	})
	if err != nil {
		slog.Error("link tgt neighbors query error", "error", err)
		return item
	}

	var tgtNeighbors []neighborInfo
	for tgtNResult.Next(ctx) {
		record := tgtNResult.Record()
		pk, _ := record.Get("pk")
		code, _ := record.Get("code")
		metric, _ := record.Get("metric")
		metro, _ := record.Get("metro")
		tgtNeighbors = append(tgtNeighbors, neighborInfo{
			pk: asString(pk), code: asString(code),
			metric: int(asInt64(metric)), metro: asString(metro),
		})
	}

	// For each (srcNeighbor, tgtNeighbor) pair, check if ISIS routes through this link (concurrently)
	type linkPair struct {
		sn, tn neighborInfo
	}
	var linkPairs []linkPair
	for _, sn := range srcNeighbors {
		for _, tn := range tgtNeighbors {
			if sn.pk != tn.pk {
				linkPairs = append(linkPairs, linkPair{sn, tn})
			}
		}
	}

	type linkPairResult struct {
		path     WhatIfAffectedPath
		affected bool
	}
	linkResults := make([]linkPairResult, len(linkPairs))
	var wg sync.WaitGroup
	for idx, lp := range linkPairs {
		wg.Add(1)
		go func(idx int, sn, tn neighborInfo) {
			defer wg.Done()

			throughMetric := sn.metric + linkMetric + tn.metric

			altCypher := `
				MATCH (n1:Device {pk: $n1PK}), (n2:Device {pk: $n2PK})
				MATCH altPath = (n1)-[:ISIS_ADJACENT*1..6]-(n2)
				WHERE NONE(r IN relationships(altPath) WHERE
				      (startNode(r).pk = $srcPK AND endNode(r).pk = $tgtPK) OR
				      (startNode(r).pk = $tgtPK AND endNode(r).pk = $srcPK))
				WITH altPath, length(altPath) AS altHops,
				     reduce(m = 0, r IN relationships(altPath) | m + coalesce(r.metric, 0)) AS altMetric
				ORDER BY altMetric ASC
				LIMIT 1
				RETURN altHops, altMetric
			`

			pairSession := a.neo4jSession(ctx)
			defer pairSession.Close(ctx)

			altResult, err := pairSession.Run(ctx, altCypher, map[string]any{
				"n1PK":  sn.pk,
				"n2PK":  tn.pk,
				"srcPK": sideAPK,
				"tgtPK": sideZPK,
			})

			path := WhatIfAffectedPath{
				Source:       sn.code,
				Target:       tn.code,
				SourceMetro:  sn.metro,
				TargetMetro:  tn.metro,
				HopsBefore:   3,
				MetricBefore: throughMetric,
				HopsAfter:    -1,
				MetricAfter:  -1,
				Status:       "disconnected",
			}

			if err == nil && altResult.Next(ctx) {
				record := altResult.Record()
				altHops, _ := record.Get("altHops")
				altMetric, _ := record.Get("altMetric")
				altMetricVal := int(asInt64(altMetric))

				if throughMetric > altMetricVal {
					linkResults[idx] = linkPairResult{affected: false}
					return
				}

				path.HopsAfter = int(asInt64(altHops))
				path.MetricAfter = altMetricVal

				metricIncrease := path.MetricAfter - path.MetricBefore
				hopIncrease := path.HopsAfter - path.HopsBefore
				if hopIncrease > 2 || metricIncrease > 50 {
					path.Status = "degraded"
				} else {
					path.Status = "rerouted"
				}
			}

			linkResults[idx] = linkPairResult{path: path, affected: true}
		}(idx, lp.sn, lp.tn)
	}
	wg.Wait()

	for _, r := range linkResults {
		if r.affected {
			item.AffectedPaths = append(item.AffectedPaths, r.path)
			if len(item.AffectedPaths) >= pathLimit {
				break
			}
		}
	}

	item.AffectedPathCount = len(item.AffectedPaths)
	return item
}
