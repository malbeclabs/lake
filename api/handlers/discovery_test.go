package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMCPServerCard(t *testing.T) {
	t.Parallel()

	api := &handlers.API{}

	t.Run("serves a card pointing at the MCP endpoint", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/.well-known/mcp.json", nil)
		req.Host = "data.doublezero.xyz"
		req.Header.Set("X-Forwarded-Proto", "https")
		rec := httptest.NewRecorder()

		api.GetMCPServerCard(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var card handlers.MCPServerCard
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &card))

		assert.Equal(t, "doublezero", card.Name)
		assert.Equal(t, "https://data.doublezero.xyz/api/mcp", card.Endpoint)
		assert.Equal(t, "https://data.doublezero.xyz/docs/mcp", card.Documentation)
		assert.Equal(t, "none", card.Authentication)
	})

	t.Run("advertises only tools that are registered", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/.well-known/mcp.json", nil)
		rec := httptest.NewRecorder()

		api.GetMCPServerCard(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		var card handlers.MCPServerCard
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &card))

		names := make([]string, 0, len(card.Tools))
		for _, tool := range card.Tools {
			names = append(names, tool.Name)
		}

		assert.Contains(t, names, "get_schema")
		assert.Contains(t, names, "execute_sql")
		assert.Contains(t, names, "read_docs")

		// execute_cypher is only registered when Neo4j is configured on
		// mainnet-beta. Neo4jClient is nil here, so advertising it would point
		// clients at a tool the server never registers.
		assert.NotContains(t, names, "execute_cypher")
	})

	t.Run("ignores a spoofed forwarded host", func(t *testing.T) {
		t.Parallel()

		// These documents are cached with a public max-age, so honoring a
		// client-supplied host would let an intermediary cache serve other
		// agents a card pointing at an attacker's endpoint.
		req := httptest.NewRequest(http.MethodGet, "/.well-known/mcp.json", nil)
		req.Host = "data.doublezero.xyz"
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("X-Forwarded-Host", "evil.example.com")
		rec := httptest.NewRecorder()

		api.GetMCPServerCard(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		var card handlers.MCPServerCard
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &card))
		assert.Equal(t, "https://data.doublezero.xyz/api/mcp", card.Endpoint)
		assert.NotContains(t, card.Endpoint, "evil.example.com")
		assert.NotContains(t, card.Documentation, "evil.example.com")
	})

	t.Run("api catalog ignores a spoofed forwarded host", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/.well-known/api-catalog", nil)
		req.Host = "data.doublezero.xyz"
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("X-Forwarded-Host", "evil.example.com")
		rec := httptest.NewRecorder()

		api.GetAPICatalog(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "evil.example.com")
	})
}

func TestGetAPICatalog(t *testing.T) {
	t.Parallel()

	api := &handlers.API{}

	req := httptest.NewRequest(http.MethodGet, "/.well-known/api-catalog", nil)
	req.Host = "data.doublezero.xyz"
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()

	api.GetAPICatalog(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/linkset+json", rec.Header().Get("Content-Type"))

	var doc struct {
		Linkset []struct {
			Anchor  string `json:"anchor"`
			Service []struct {
				Href string `json:"href"`
			} `json:"service"`
		} `json:"linkset"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
	require.NotEmpty(t, doc.Linkset)

	assert.Equal(t, "https://data.doublezero.xyz/api/mcp", doc.Linkset[0].Anchor)
	require.NotEmpty(t, doc.Linkset[0].Service)
	assert.Equal(t, "https://data.doublezero.xyz/api/mcp", doc.Linkset[0].Service[0].Href)
}
