package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// TopologyItem represents a single topology (flex-algo) with its link count.
type TopologyItem struct {
	PK             string `json:"pk"`
	Name           string `json:"name"`
	AdminGroupBit  uint8  `json:"admin_group_bit"`
	FlexAlgoNumber uint8  `json:"flex_algo_number"`
	Color          uint8  `json:"color"`
	Constraint     string `json:"constraint"`
	LinkCount      int    `json:"link_count"`
}

// GetTopologies returns all current topologies with link counts.
func (a *API) GetTopologies(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()

	// Query topology definitions
	topoQuery := `
		SELECT pk, name, admin_group_bit, flex_algo_number, color, topo_constraint
		FROM dz_topologies_current
	`
	topoRows, err := a.envDB(ctx).Query(ctx, topoQuery)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("topologies", duration, err)

	if err != nil {
		slog.Warn("topologies query failed", "error", err)
		http.Error(w, "failed to fetch topologies", http.StatusInternalServerError)
		return
	}
	defer topoRows.Close()

	var topologies []TopologyItem
	for topoRows.Next() {
		var t TopologyItem
		if err := topoRows.Scan(&t.PK, &t.Name, &t.AdminGroupBit, &t.FlexAlgoNumber, &t.Color, &t.Constraint); err != nil {
			logError("topologies row scan failed", "error", err)
			http.Error(w, "failed to scan topologies", http.StatusInternalServerError)
			return
		}
		topologies = append(topologies, t)
	}
	if err := topoRows.Err(); err != nil {
		logError("topologies rows iteration failed", "error", err)
		http.Error(w, "failed to read topologies", http.StatusInternalServerError)
		return
	}

	// Query link counts per topology (non-fatal if this fails)
	linkCounts := make(map[string]int)
	countQuery := `
		SELECT
			topo_name,
			count() AS link_count
		FROM (
			SELECT
				arrayJoin(JSONExtract(link_topologies, 'Array(String)')) AS topo_name
			FROM dz_links_current
			WHERE link_topologies != '[]' AND link_topologies != ''
		)
		GROUP BY topo_name
	`
	countRows, err := a.envDB(ctx).Query(ctx, countQuery)
	if err != nil {
		slog.Warn("topology link count query failed (non-fatal)", "error", err)
	} else {
		defer countRows.Close()
		for countRows.Next() {
			var name string
			var count uint64
			if err := countRows.Scan(&name, &count); err != nil {
				slog.Warn("topology link count scan failed (non-fatal)", "error", err)
				break
			}
			linkCounts[name] = int(count)
		}
	}

	// Merge link counts into topology items
	for i := range topologies {
		topologies[i].LinkCount = linkCounts[topologies[i].Name]
	}

	if topologies == nil {
		topologies = []TopologyItem{}
	}

	// Query total link count and drained count
	var totalLinks, drainedLinks uint64
	totalRow := a.envDB(ctx).QueryRow(ctx, `SELECT count() FROM dz_links_current`)
	if err := totalRow.Scan(&totalLinks); err != nil {
		slog.Warn("topology total link count query failed (non-fatal)", "error", err)
	}
	drainedRow := a.envDB(ctx).QueryRow(ctx, `SELECT count() FROM dz_links_current WHERE unicast_drained = 1`)
	if err := drainedRow.Scan(&drainedLinks); err != nil {
		slog.Warn("topology drained link count query failed (non-fatal)", "error", err)
	}

	resp := struct {
		Topologies       []TopologyItem `json:"topologies"`
		TotalLinkCount   int            `json:"total_link_count"`
		DrainedLinkCount int            `json:"drained_link_count"`
	}{
		Topologies:       topologies,
		TotalLinkCount:   int(totalLinks),
		DrainedLinkCount: int(drainedLinks),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logError("failed to encode topologies response", "error", err)
	}
}
