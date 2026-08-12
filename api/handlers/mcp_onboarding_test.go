package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/utils/pkg/docsfetch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// accessPassAPI stubs the doublezero CLI for check_edge_access tests: the
// runner returns stdout (the `access-pass get --json` output) or err.
func accessPassAPI(stdout string, err error) *handlers.API {
	return &handlers.API{DZCLI: func(ctx context.Context, args ...string) ([]byte, error) {
		if err != nil {
			return nil, err
		}
		return []byte(stdout), nil
	}}
}

// errAccessPassNotFound mirrors the CLI's miss error surfaced by runDoubleZeroCLI.
var errAccessPassNotFound = errors.New("doublezero access-pass get: Error: Access Pass not found")

// accessPassJSON builds a minimal `access-pass get --json` document.
func accessPassJSON(account, clientIP, status, typeTag string) string {
	doc, _ := json.Marshal(map[string]string{
		"account":   account,
		"type":      typeTag,
		"client_ip": clientIP,
		"status":    status,
	})
	return string(doc)
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

// withRunbookDocs serves files from a local httptest server and returns a docs
// client pointed at it, for injection via API.DocsSource.
func withRunbookDocs(t *testing.T, files map[string]string) *docsfetch.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		body, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	return &docsfetch.Client{
		HTTP: srv.Client(),
		Base: srv.URL + "/",
	}
}

func TestMCPHandler_GetOnboardingRunbook_ListsCatalog(t *testing.T) {
	t.Parallel()
	api := &handlers.API{DocsSource: withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})}
	handler, sessionID := mcpSession(t, api)

	output := callToolOutput(t, handler, sessionID, "get_onboarding_runbook", map[string]any{})
	available, ok := output["available_runbooks"].([]any)
	require.True(t, ok, "expected available_runbooks array")
	assert.Contains(t, available, "feed-a")
	assert.Contains(t, available, "dz-edge-subscriber")
	assert.Empty(t, output["service"])
}

func TestMCPHandler_GetOnboardingRunbook_FromLocalDocs(t *testing.T) {
	t.Parallel()
	api := &handlers.API{DocsSource: withRunbookDocs(t, map[string]string{
		"runbooks.md":       testRunbookIndex,
		"feed-a-runbook.md": "# Feed A + Edge Connect — runbook\n\nLet an AI walk you through this page.\n",
	})}
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
	t.Parallel()
	// Index is local; the runbook page is missing so loadRunbook falls back to embed.
	api := &handlers.API{DocsSource: withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})}
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
	t.Parallel()
	api := &handlers.API{DocsSource: withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})}
	handler, sessionID := mcpSession(t, api)

	response := callTool(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "not-a-real-feed",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
}

func TestMCPHandler_GetOnboardingRunbook_InvalidService(t *testing.T) {
	t.Parallel()
	api := &handlers.API{DocsSource: withRunbookDocs(t, map[string]string{"runbooks.md": testRunbookIndex})}
	handler, sessionID := mcpSession(t, api)

	response := callTool(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "../../../etc/passwd",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
}

func TestMCPHandler_GetOnboardingRunbook_MissingIndex(t *testing.T) {
	t.Parallel()
	api := &handlers.API{DocsSource: withRunbookDocs(t, nil)}
	handler, sessionID := mcpSession(t, api)
	response := callTool(t, handler, sessionID, "get_onboarding_runbook", map[string]any{})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
}

func TestMCPHandler_CheckEdgeAccess_NoPassIsPending(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI("", errAccessPassNotFound))

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
	var gotArgs []string
	api := &handlers.API{DZCLI: func(ctx context.Context, args ...string) ([]byte, error) {
		gotArgs = args
		return []byte(accessPassJSON("pass-pk", "0.0.0.0", "connected", "edge_seat: 1 feed(s)")), nil
	}}
	handler, sessionID := mcpSession(t, api)

	out := callToolOutput(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "103.106.58.157",
	})
	assert.Equal(t, "active", out["status"])
	assert.Equal(t, "pass-pk", out["pass_pk"])
	assert.Equal(t, "0.0.0.0", out["client_ip"])
	assert.Equal(t, "edge_seat: 1 feed(s)", out["type_tag"])
	assert.NotContains(t, out, "mock")
	assert.Equal(t, []string{
		"access-pass", "get",
		"--user-payer", "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"--client-ip", "103.106.58.157",
		"--json",
	}, gotArgs)
}

func TestMCPHandler_CheckEdgeAccess_RequestedIsPending(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI(
		accessPassJSON("pass-pk", "203.0.113.7", "requested", "prepaid"), nil))

	out := callToolOutput(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "203.0.113.7",
	})
	assert.Equal(t, "pending", out["status"])
	assert.Contains(t, out["message"], "waiting to be approved")
}

func TestMCPHandler_CheckEdgeAccess_ExpiredIsPending(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI(
		accessPassJSON("pass-pk", "203.0.113.7", "expired", "prepaid"), nil))

	out := callToolOutput(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "203.0.113.7",
	})
	assert.Equal(t, "pending", out["status"])
	assert.Contains(t, out["message"], "expired")
}

func TestMCPHandler_CheckEdgeAccess_InvalidIP(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI("", errAccessPassNotFound))

	response := callTool(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "not-an-ip",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "IPv4")
}

func TestMCPHandler_CheckEdgeAccess_CLIError(t *testing.T) {
	t.Parallel()
	handler, sessionID := mcpSession(t, accessPassAPI("", errors.New("doublezero CLI: executable file not found in $PATH")))

	response := callTool(t, handler, sessionID, "check_edge_access", map[string]any{
		"pubkey":    "4V83pdV5yYxKbSQWjUZFyJFQAg6unJTxaBh5UXTdAzvb",
		"public_ip": "203.0.113.7",
	})
	result, ok := response["result"].(map[string]any)
	require.True(t, ok)
	assert.True(t, result["isError"].(bool))
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "access pass lookup failed")
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
	t.Parallel()
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

	api := &handlers.API{DocsSource: &docsfetch.Client{
		HTTP: srv.Client(),
		Base: srv.URL + "/",
	}}
	handler, sessionID := mcpSession(t, api)
	output := callToolOutput(t, handler, sessionID, "get_onboarding_runbook", map[string]any{
		"service": "feed-a",
	})
	assert.Contains(t, output["runbook"], "remote feed-a runbook")
}
