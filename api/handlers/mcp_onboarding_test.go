package handlers_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/utils/pkg/docsfetch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubAccessPassConn implements driver.Conn.QueryRow for check_edge_access tests.
type stubAccessPassConn struct {
	driver.Conn
	row driver.Row
}

func (c *stubAccessPassConn) QueryRow(context.Context, string, ...any) driver.Row {
	return c.row
}

type stubAccessPassRow struct {
	err  error
	vals []string
}

func (r stubAccessPassRow) Err() error { return r.err }

func (r stubAccessPassRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(r.vals) == 0 {
		return sql.ErrNoRows
	}
	for i, d := range dest {
		p, ok := d.(*string)
		if !ok {
			return sql.ErrNoRows
		}
		*p = r.vals[i]
	}
	return nil
}

func (r stubAccessPassRow) ScanStruct(any) error { return r.err }

func accessPassAPI(row stubAccessPassRow) *handlers.API {
	return &handlers.API{PublicQueryDB: &stubAccessPassConn{row: row}}
}

// callToolOutput calls a tool, asserts success, and unmarshals the JSON text content
// of its (non-error) result into a map.
func callToolOutput(t *testing.T, handler http.Handler, sessionID, tool string, args map[string]any) map[string]any {
	t.Helper()
	response := callTool(t, handler, sessionID, tool, args)
	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)
	require.NotEqual(t, true, result["isError"], "unexpected tool error: %v", result)

	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)
	var output map[string]any
	require.NoError(t, json.Unmarshal([]byte(text), &output), "output: %s", text)
	return output
}

const testRunbookIndex = `# Runbooks

## Index

- ` + "`feed-a`" + ` — [Feed A + Edge Connect](feed-a-runbook.md)
- ` + "`dz-edge-subscriber`" + ` — [Edge subscriber](dz-edge-subscriber.md)
`

func withRunbookDocs(t *testing.T, files map[string]string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	prev := handlers.DocsSource
	handlers.DocsSource = &docsfetch.Client{LocalDir: dir}
	t.Cleanup(func() { handlers.DocsSource = prev })
}

func TestMCPHandler_GetOnboardingRunbook_ListsCatalog(t *testing.T) {
	withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})
	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)

	output := callToolOutput(t, handler, sessionID, "get_onboarding_runbook", map[string]any{})
	available, ok := output["available_runbooks"].([]any)
	require.True(t, ok, "expected available_runbooks array")
	assert.Contains(t, available, "feed-a")
	assert.Contains(t, available, "dz-edge-subscriber")
	assert.Empty(t, output["service"])
}

func TestMCPHandler_GetOnboardingRunbook_FromLocalDocs(t *testing.T) {
	withRunbookDocs(t, map[string]string{
		"runbooks.md":       testRunbookIndex,
		"feed-a-runbook.md": "# Feed A + Edge Connect — runbook\n\nLet an AI walk you through this page.\n",
	})

	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)

	output := callToolOutput(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "feed-a",
	})
	assert.Equal(t, "feed-a", output["service"])
	assert.Equal(t, "feed-a-runbook", output["page"])
	assert.NotEmpty(t, output["preamble"])
	runbook, ok := output["runbook"].(string)
	require.True(t, ok)
	assert.Contains(t, runbook, "Feed A + Edge Connect")
	assert.Contains(t, output["source"], "feed-a-runbook.md")
}

func TestMCPHandler_GetOnboardingRunbook_EmbeddedFallback(t *testing.T) {
	// Index is local; the runbook page is missing so loadRunbook falls back to embed.
	withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})

	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)

	output := callToolOutput(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "dz-edge-subscriber",
	})
	assert.Equal(t, "dz-edge-subscriber", output["service"])
	runbook, ok := output["runbook"].(string)
	require.True(t, ok)
	assert.Contains(t, runbook, "tiredsolid")
	assert.Equal(t, "embed:dz-edge-subscriber.md", output["source"])
}

func TestMCPHandler_GetOnboardingRunbook_UnknownService(t *testing.T) {
	withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})
	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)

	response := callTool(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "not-a-real-feed",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
}

func TestMCPHandler_GetOnboardingRunbook_InvalidService(t *testing.T) {
	withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})
	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)

	response := callTool(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "../../../etc/passwd",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
}

func TestMCPHandler_GetOnboardingRunbook_MissingIndex(t *testing.T) {
	prev := handlers.DocsSource
	handlers.DocsSource = &docsfetch.Client{}
	t.Cleanup(func() { handlers.DocsSource = prev })

	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)
	response := callTool(t, handler, sessionID, "get_onboarding_runbook", map[string]any{})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
}

func TestMCPHandler_CheckEdgeAccess_NoPassIsPending(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI(stubAccessPassRow{}))

	out := callToolOutput(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "103.106.58.157",
	})
	assert.Equal(t, "pending", out["status"])
	assert.NotContains(t, out, "mock")
	assert.Contains(t, out["message"], "no access pass")
}

func TestMCPHandler_CheckEdgeAccess_ConnectedIsActive(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI(stubAccessPassRow{
		vals: []string{"pass-pk", "0.0.0.0", "connected", "edge_seat"},
	}))

	out := callToolOutput(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "103.106.58.157",
	})
	assert.Equal(t, "active", out["status"])
	assert.Equal(t, "pass-pk", out["pass_pk"])
	assert.Equal(t, "0.0.0.0", out["client_ip"])
	assert.Equal(t, "edge_seat", out["type_tag"])
	assert.NotContains(t, out, "mock")
}

func TestMCPHandler_CheckEdgeAccess_ExpiredIsPending(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI(stubAccessPassRow{
		vals: []string{"pass-pk", "203.0.113.7", "expired", "prepaid"},
	}))

	out := callToolOutput(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "203.0.113.7",
	})
	assert.Equal(t, "pending", out["status"])
	assert.Contains(t, out["message"], "expired")
}

func TestMCPHandler_CheckEdgeAccess_InvalidIP(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI(stubAccessPassRow{}))

	response := callTool(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "not-an-ip",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "IPv4")
}

func TestMCPHandler_CheckEdgeAccess_NoClickHouse(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, &handlers.API{})

	response := callTool(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "203.0.113.7",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "clickhouse")
}

func TestMCPHandler_CheckEdgeAccess_MissingInput(t *testing.T) {
	t.Parallel()
	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)

	response := callTool(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey": "somekey",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "required")
}

func TestMCPHandler_GetOnboardingRunbook_RemoteDocs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/runbooks.md":
			_, _ = w.Write([]byte(testRunbookIndex))
		case "/feed-a-runbook.md":
			_, _ = w.Write([]byte("# remote feed-a runbook\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	prev := handlers.DocsSource
	handlers.DocsSource = &docsfetch.Client{
		HTTP: srv.Client(),
		Base: srv.URL + "/",
	}
	t.Cleanup(func() { handlers.DocsSource = prev })

	api := &handlers.API{}
	handler, sessionID := mcpSession(t, api)
	output := callToolOutput(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "feed-a",
	})
	assert.Contains(t, output["runbook"], "remote feed-a runbook")
}
