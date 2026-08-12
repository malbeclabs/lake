package handlers

import (
	"encoding/json"
	"net/http"
)

// baseURL reconstructs the public origin for the current request. The service
// runs behind Cloudflare, which terminates TLS, so the forwarded proto is
// authoritative when present.
//
// The host comes from r.Host only. Cloudflare preserves the original Host
// header and does not set X-Forwarded-Host, so honoring that header would gain
// nothing while letting a client dictate the URLs in these documents. Both
// responses are cached with a public max-age, so a spoofed host could be
// served on to other agents by an intermediary cache.
func baseURL(r *http.Request) string {
	scheme := "http"
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	} else if r.TLS != nil {
		scheme = "https"
	}

	return scheme + "://" + r.Host
}

// MCPServerCard describes the MCP server for automated discovery. The format
// follows the conventions clients currently look for at /.well-known/mcp.json.
type MCPServerCard struct {
	Name           string     `json:"name"`
	Title          string     `json:"title"`
	Description    string     `json:"description"`
	Version        string     `json:"version"`
	Endpoint       string     `json:"endpoint"`
	Transport      string     `json:"transport"`
	Authentication string     `json:"authentication"`
	Documentation  string     `json:"documentation"`
	Capabilities   []string   `json:"capabilities"`
	Tools          []cardTool `json:"tools"`
}

type cardTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// GetMCPServerCard serves the MCP server card at /.well-known/mcp.json.
//
// The tool list mirrors createMCPServer, which only registers execute_cypher
// when Neo4j is configured on mainnet-beta. Advertising a tool that is not
// registered would send clients down a dead end, so the card is built from the
// same condition rather than hardcoded.
func (a *API) GetMCPServerCard(w http.ResponseWriter, r *http.Request) {
	base := baseURL(r)

	tools := []cardTool{
		{
			Name:        "get_schema",
			Description: "Table and column definitions. Call this before writing any query.",
		},
		{
			Name:        "execute_sql",
			Description: "Read-only SQL against ClickHouse for telemetry, latency, traffic, shreds, stake, and status data.",
		},
		{
			Name:        "read_docs",
			Description: "DoubleZero documentation for conceptual, setup, and troubleshooting questions.",
		},
		{
			Name:        "get_onboarding_runbook",
			Description: "Guided onboarding runbook for connecting to a DoubleZero service. Omit service to list. Catalog is GitHub raw docs/runbooks.md.",
		},
		{
			Name:        "check_edge_access",
			Description: "Check whether a user payer pubkey is authorized for a receiving IP via onchain access passes.",
		},
	}

	if a.Neo4jClient != nil && EnvFromContext(r.Context()) == EnvMainnet {
		tools = append(tools, cardTool{
			Name:        "execute_cypher",
			Description: "Read-only Cypher against Neo4j for topology, path finding, reachability, and multi-hop latency.",
		})
	}

	card := MCPServerCard{
		Name:           "doublezero",
		Title:          "DoubleZero Data",
		Description:    "Public telemetry and analytics for the DoubleZero network: device and link health, path latency, topology, traffic, shreds, outages, and validator stake.",
		Version:        "1.0.0",
		Endpoint:       base + "/api/mcp",
		Transport:      "streamable-http",
		Authentication: "none",
		Documentation:  base + "/docs/mcp",
		Capabilities:   []string{"tools", "resources", "prompts", "logging"},
		Tools:          tools,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_ = json.NewEncoder(w).Encode(card)
}

// linkset is an RFC 9727 API catalog document.
type linkset struct {
	Linkset []linksetEntry `json:"linkset"`
}

type linksetEntry struct {
	Anchor  string       `json:"anchor"`
	Service []linkTarget `json:"service,omitempty"`
	DescBy  []linkTarget `json:"describedby,omitempty"`
}

type linkTarget struct {
	Href  string `json:"href"`
	Type  string `json:"type,omitempty"`
	Title string `json:"title,omitempty"`
}

// GetAPICatalog serves an RFC 9727 API catalog at /.well-known/api-catalog,
// pointing agents at the MCP server and the public JSON endpoints.
func (a *API) GetAPICatalog(w http.ResponseWriter, r *http.Request) {
	base := baseURL(r)

	doc := linkset{
		Linkset: []linksetEntry{
			{
				Anchor: base + "/api/mcp",
				Service: []linkTarget{
					{Href: base + "/api/mcp", Type: "application/json", Title: "DoubleZero Data MCP server"},
				},
				DescBy: []linkTarget{
					{Href: base + "/.well-known/mcp.json", Type: "application/json", Title: "MCP server card"},
				},
			},
			{
				Anchor: base + "/api",
				Service: []linkTarget{
					{Href: base + "/api/catalog", Type: "application/json", Title: "Data catalog"},
					{Href: base + "/api/status", Type: "application/json", Title: "Current network status"},
					{Href: base + "/api/stats", Type: "application/json", Title: "Summary statistics"},
					{Href: base + "/api/dz/devices", Type: "application/json", Title: "Network devices"},
					{Href: base + "/api/dz/links", Type: "application/json", Title: "Network links"},
					{Href: base + "/api/timeline", Type: "application/json", Title: "Network event timeline"},
					{Href: base + "/api/config", Type: "application/json", Title: "Environment and feature flags"},
				},
				DescBy: []linkTarget{
					{Href: base + "/llms.txt", Type: "text/plain", Title: "Agent orientation"},
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/linkset+json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_ = json.NewEncoder(w).Encode(doc)
}
