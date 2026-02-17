package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/metrics"
)

// FieldValuesResponse is the response for the field values endpoint
type FieldValuesResponse struct {
	Values []string `json:"values"`
}

// fieldConfig defines how to query distinct values for a field
type fieldConfig struct {
	table  string
	column string
}

// entityFieldConfigs maps entity+field to the SQL query config
var entityFieldConfigs = map[string]map[string]fieldConfig{
	"devices": {
		"status":      {table: "dz_devices_current", column: "status"},
		"type":        {table: "dz_devices_current", column: "device_type"},
		"metro":       {table: "dz_devices_current d JOIN dz_metros_current m ON d.metro_pk = m.pk", column: "m.code"},
		"contributor": {table: "dz_devices_current d JOIN dz_contributors_current c ON d.contributor_pk = c.pk", column: "c.code"},
	},
	"interfaces": {
		"intf": {table: "fact_dz_device_interface_counters", column: "intf"},
	},
	"links": {
		"status":      {table: "dz_links_current", column: "status"},
		"type":        {table: "dz_links_current", column: "link_type"},
		"contributor": {table: "dz_links_current l JOIN dz_contributors_current c ON l.contributor_pk = c.pk", column: "c.code"},
		"sidea":       {table: "dz_links_current l JOIN dz_devices_current d ON l.side_a_pk = d.pk", column: "d.code"},
		"sidez":       {table: "dz_links_current l JOIN dz_devices_current d ON l.side_z_pk = d.pk", column: "d.code"},
	},
	"metros": {
		// Metros don't have many filterable fields with limited values
	},
	"contributors": {
		// Contributors don't have many filterable fields with limited values
	},
	"users": {
		"status": {table: "dz_users_current", column: "status"},
		"kind":   {table: "dz_users_current", column: "kind"},
		"metro":  {table: "dz_users_current u JOIN dz_devices_current d ON u.device_pk = d.pk JOIN dz_metros_current m ON d.metro_pk = m.pk", column: "m.code"},
		"device": {table: "dz_users_current u JOIN dz_devices_current d ON u.device_pk = d.pk", column: "d.code"},
	},
	"validators": {
		"dz":      {table: "(SELECT 'yes' AS val UNION ALL SELECT 'no' AS val)", column: "val"},
		"version": {table: "solana_vote_accounts_current v JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey", column: "g.version"},
		"device":  {table: "solana_vote_accounts_current v JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey JOIN dz_users_current u ON g.gossip_ip = u.dz_ip JOIN dz_devices_current d ON u.device_pk = d.pk", column: "d.code"},
		"city":    {table: "solana_vote_accounts_current v JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey JOIN geoip_records_current geo ON g.gossip_ip = geo.ip", column: "geo.city"},
		"country": {table: "solana_vote_accounts_current v JOIN solana_gossip_nodes_current g ON v.node_pubkey = g.pubkey JOIN geoip_records_current geo ON g.gossip_ip = geo.ip", column: "geo.country"},
	},
	"gossip": {
		"dz":        {table: "(SELECT 'yes' AS val UNION ALL SELECT 'no' AS val)", column: "val"},
		"validator": {table: "(SELECT 'yes' AS val UNION ALL SELECT 'no' AS val)", column: "val"},
		"version":   {table: "solana_gossip_nodes_current", column: "version"},
		"city":      {table: "solana_gossip_nodes_current g JOIN geoip_records_current geo ON g.gossip_ip = geo.ip", column: "geo.city"},
		"country":   {table: "solana_gossip_nodes_current g JOIN geoip_records_current geo ON g.gossip_ip = geo.ip", column: "geo.country"},
		"device":    {table: "solana_gossip_nodes_current g JOIN dz_users_current u ON g.gossip_ip = u.dz_ip JOIN dz_devices_current d ON u.device_pk = d.pk", column: "d.code"},
	},
}

// quoteCSV splits a comma-separated string and returns SQL-safe quoted values.
func quoteCSV(csv string) string {
	vals := strings.Split(csv, ",")
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = fmt.Sprintf("'%s'", escapeSingleQuote(v))
	}
	return strings.Join(quoted, ",")
}

// BuildScopedFieldValuesQuery builds a scoped query for dashboard-relevant
// entity+field combos when filter params are present. Returns empty string
// when no scoping is needed (caller should use the generic query).
func BuildScopedFieldValuesQuery(entity, field string, cfg fieldConfig, r *http.Request) string {
	metro := r.URL.Query().Get("metro")
	device := r.URL.Query().Get("device")
	contributor := r.URL.Query().Get("contributor")
	linkType := r.URL.Query().Get("link_type")

	if metro == "" && device == "" && contributor == "" && linkType == "" {
		return ""
	}

	key := entity + "/" + field

	switch key {
	case "interfaces/intf":
		// Scope interface names by metro/device/contributor
		var joins, wheres []string
		joins = append(joins, "JOIN dz_devices_current d ON f.device_pk = d.pk")
		if metro != "" {
			joins = append(joins, "JOIN dz_metros_current m ON d.metro_pk = m.pk")
			wheres = append(wheres, fmt.Sprintf("m.code IN (%s)", quoteCSV(metro)))
		}
		if device != "" {
			wheres = append(wheres, fmt.Sprintf("d.code IN (%s)", quoteCSV(device)))
		}
		if contributor != "" {
			joins = append(joins, "JOIN dz_contributors_current co ON d.contributor_pk = co.pk")
			wheres = append(wheres, fmt.Sprintf("co.code IN (%s)", quoteCSV(contributor)))
		}
		if linkType != "" {
			joins = append(joins, "JOIN dz_links_current l ON f.link_pk = l.pk")
			wheres = append(wheres, fmt.Sprintf("l.link_type IN (%s)", quoteCSV(linkType)))
		}
		whereClause := "f.intf IS NOT NULL AND f.intf != '' AND f.intf NOT LIKE 'Tunnel%'"
		if len(wheres) > 0 {
			whereClause += " AND " + strings.Join(wheres, " AND ")
		}
		return fmt.Sprintf("SELECT DISTINCT f.intf AS val FROM fact_dz_device_interface_counters f %s WHERE %s ORDER BY val LIMIT 100",
			strings.Join(joins, " "), whereClause)

	case "devices/metro":
		// Scope metro values by contributor/device
		var wheres []string
		extraJoins := ""
		if contributor != "" {
			extraJoins += " JOIN dz_contributors_current co ON d.contributor_pk = co.pk"
			wheres = append(wheres, fmt.Sprintf("co.code IN (%s)", quoteCSV(contributor)))
		}
		if device != "" {
			wheres = append(wheres, fmt.Sprintf("d.code IN (%s)", quoteCSV(device)))
		}
		whereClause := "m.code IS NOT NULL AND m.code != ''"
		if len(wheres) > 0 {
			whereClause += " AND " + strings.Join(wheres, " AND ")
		}
		return fmt.Sprintf("SELECT DISTINCT m.code AS val FROM %s%s WHERE %s ORDER BY val LIMIT 100",
			cfg.table, extraJoins, whereClause)

	case "devices/contributor":
		// Scope contributor values by metro/device
		var wheres []string
		extraJoins := ""
		if metro != "" {
			extraJoins += " JOIN dz_metros_current m ON d.metro_pk = m.pk"
			wheres = append(wheres, fmt.Sprintf("m.code IN (%s)", quoteCSV(metro)))
		}
		if device != "" {
			wheres = append(wheres, fmt.Sprintf("d.code IN (%s)", quoteCSV(device)))
		}
		whereClause := "c.code IS NOT NULL AND c.code != ''"
		if len(wheres) > 0 {
			whereClause += " AND " + strings.Join(wheres, " AND ")
		}
		return fmt.Sprintf("SELECT DISTINCT c.code AS val FROM %s%s WHERE %s ORDER BY val LIMIT 100",
			cfg.table, extraJoins, whereClause)

	case "links/type":
		// Scope link types by metro/contributor
		var wheres []string
		extraJoins := ""
		if metro != "" {
			extraJoins += " JOIN dz_devices_current d ON l.side_a_pk = d.pk JOIN dz_metros_current m ON d.metro_pk = m.pk"
			wheres = append(wheres, fmt.Sprintf("m.code IN (%s)", quoteCSV(metro)))
		}
		if contributor != "" {
			if !strings.Contains(cfg.table, "dz_contributors_current") {
				extraJoins += " JOIN dz_contributors_current co ON l.contributor_pk = co.pk"
			}
			wheres = append(wheres, fmt.Sprintf("co.code IN (%s)", quoteCSV(contributor)))
		}
		whereClause := "l.link_type IS NOT NULL AND l.link_type != ''"
		if len(wheres) > 0 {
			whereClause += " AND " + strings.Join(wheres, " AND ")
		}
		// Use alias l for links table
		table := "dz_links_current l"
		return fmt.Sprintf("SELECT DISTINCT l.link_type AS val FROM %s%s WHERE %s ORDER BY val LIMIT 100",
			table, extraJoins, whereClause)
	}

	return ""
}

// GetFieldValues returns distinct values for a given entity field
func GetFieldValues(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	entity := r.URL.Query().Get("entity")
	field := r.URL.Query().Get("field")

	if entity == "" || field == "" {
		http.Error(w, "entity and field parameters are required", http.StatusBadRequest)
		return
	}

	// Look up the field config
	entityFields, ok := entityFieldConfigs[entity]
	if !ok {
		// Return empty for unknown entities
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(FieldValuesResponse{Values: []string{}})
		return
	}

	fieldCfg, ok := entityFields[field]
	if !ok {
		// Return empty for unknown fields
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(FieldValuesResponse{Values: []string{}})
		return
	}

	// Try scoped query first (dashboard filter-aware), fall back to generic
	query := BuildScopedFieldValuesQuery(entity, field, fieldCfg, r)
	if query == "" {
		query = "SELECT DISTINCT " + fieldCfg.column + " AS val FROM " + fieldCfg.table + " WHERE " + fieldCfg.column + " IS NOT NULL AND " + fieldCfg.column + " != '' ORDER BY val LIMIT 100"
	}

	start := time.Now()
	rows, err := envDB(ctx).Query(ctx, query)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)

	if err != nil {
		log.Printf("Field values query error: %v\nQuery: %s", err, query)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			log.Printf("Field values scan error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		values = append(values, val)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Field values rows error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if values == nil {
		values = []string{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(FieldValuesResponse{Values: values}); err != nil {
		log.Printf("JSON encoding error: %v", err)
	}
}
