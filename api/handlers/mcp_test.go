package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mcpRequest creates an MCP HTTP request with the required headers.
func mcpRequest(t *testing.T, body []byte, sessionID string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}
	return req
}

// parseSSEResponse extracts the JSON data from an SSE response.
// SSE format is: "event: message\ndata: {...}\n\n"
func parseSSEResponse(body string) (map[string]any, error) {
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "data: ") {
			jsonData := strings.TrimPrefix(line, "data: ")
			var response map[string]any
			if err := json.Unmarshal([]byte(jsonData), &response); err != nil {
				return nil, err
			}
			return response, nil
		}
	}
	// Try parsing as plain JSON if no SSE format found
	var response map[string]any
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		return nil, err
	}
	return response, nil
}

func TestMCPHandler_Initialize(t *testing.T) {
	handler := handlers.InitMCP()
	require.NotNil(t, handler)

	// MCP initialize request (JSON-RPC 2.0)
	initRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "test-client",
				"version": "1.0.0",
			},
		},
	}

	body, err := json.Marshal(initRequest)
	require.NoError(t, err)

	req := mcpRequest(t, body, "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Should get a valid response (SSE or JSON)
	assert.Equal(t, http.StatusOK, rec.Code)

	response, err := parseSSEResponse(rec.Body.String())
	require.NoError(t, err, "response: %s", rec.Body.String())

	// Check it's a valid JSON-RPC response
	assert.Equal(t, "2.0", response["jsonrpc"])
	assert.Equal(t, float64(1), response["id"])

	// Check we got a result (not an error)
	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result in response, got: %v", response)

	// Verify server info
	serverInfo, ok := result["serverInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "doublezero", serverInfo["name"])
	assert.Equal(t, "1.0.0", serverInfo["version"])
}

func TestMCPHandler_ListTools(t *testing.T) {
	handler := handlers.InitMCP()
	require.NotNil(t, handler)

	// First initialize
	initRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "test-client",
				"version": "1.0.0",
			},
		},
	}

	body, _ := json.Marshal(initRequest)
	req := mcpRequest(t, body, "")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "init response: %s", rec.Body.String())

	// Get session ID from response header
	sessionID := rec.Header().Get("Mcp-Session-Id")
	t.Logf("Session ID: %s", sessionID)

	// Now list tools
	listToolsRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
		"params":  map[string]any{},
	}

	body, _ = json.Marshal(listToolsRequest)
	req = mcpRequest(t, body, sessionID)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "list tools response: %s", rec.Body.String())

	response, err := parseSSEResponse(rec.Body.String())
	require.NoError(t, err, "response: %s", rec.Body.String())

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	tools, ok := result["tools"].([]any)
	require.True(t, ok, "expected tools array")

	// Verify we have the expected tools
	toolNames := make(map[string]bool)
	for _, tool := range tools {
		toolMap := tool.(map[string]any)
		toolNames[toolMap["name"].(string)] = true
	}

	assert.True(t, toolNames["execute_sql"], "should have execute_sql tool")
	assert.True(t, toolNames["read_docs"], "should have read_docs tool")
	assert.True(t, toolNames["get_schema"], "should have get_schema tool")
	// execute_cypher only available on mainnet with Neo4j
}

func TestMCPHandler_ListResources(t *testing.T) {
	handler := handlers.InitMCP()
	require.NotNil(t, handler)

	// First initialize
	initRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "test-client",
				"version": "1.0.0",
			},
		},
	}

	body, _ := json.Marshal(initRequest)
	req := mcpRequest(t, body, "")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "init response: %s", rec.Body.String())

	sessionID := rec.Header().Get("Mcp-Session-Id")

	// Now list resources
	listResourcesRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "resources/list",
		"params":  map[string]any{},
	}

	body, _ = json.Marshal(listResourcesRequest)
	req = mcpRequest(t, body, sessionID)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "list resources response: %s", rec.Body.String())

	response, err := parseSSEResponse(rec.Body.String())
	require.NoError(t, err, "response: %s", rec.Body.String())

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	resources, ok := result["resources"].([]any)
	require.True(t, ok, "expected resources array")

	// Verify we have the expected resources
	resourceURIs := make(map[string]bool)
	for _, resource := range resources {
		resourceMap := resource.(map[string]any)
		resourceURIs[resourceMap["uri"].(string)] = true
	}

	assert.True(t, resourceURIs["doublezero://schema"], "should have schema resource")
	assert.True(t, resourceURIs["doublezero://sql-context"], "should have sql-context resource")
	assert.True(t, resourceURIs["doublezero://cypher-context"], "should have cypher-context resource")
}

func TestMCPHandler_ListPrompts(t *testing.T) {
	handler := handlers.InitMCP()
	require.NotNil(t, handler)

	// First initialize
	initRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "test-client",
				"version": "1.0.0",
			},
		},
	}

	body, _ := json.Marshal(initRequest)
	req := mcpRequest(t, body, "")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "init response: %s", rec.Body.String())

	sessionID := rec.Header().Get("Mcp-Session-Id")

	// Now list prompts
	listPromptsRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "prompts/list",
		"params":  map[string]any{},
	}

	body, _ = json.Marshal(listPromptsRequest)
	req = mcpRequest(t, body, sessionID)

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code, "list prompts response: %s", rec.Body.String())

	response, err := parseSSEResponse(rec.Body.String())
	require.NoError(t, err, "response: %s", rec.Body.String())

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	prompts, ok := result["prompts"].([]any)
	require.True(t, ok, "expected prompts array")

	// Verify we have the expected prompts
	promptNames := make(map[string]bool)
	for _, prompt := range prompts {
		promptMap := prompt.(map[string]any)
		promptNames[promptMap["name"].(string)] = true
	}

	assert.True(t, promptNames["analyze-data"], "should have analyze-data prompt")
}

// mcpSession initializes an MCP session and returns the handler and session ID.
func mcpSession(t *testing.T) (http.Handler, string) {
	handler := handlers.InitMCP()
	require.NotNil(t, handler)

	initRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "test-client",
				"version": "1.0.0",
			},
		},
	}

	body, _ := json.Marshal(initRequest)
	req := mcpRequest(t, body, "")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	sessionID := rec.Header().Get("Mcp-Session-Id")
	return handler, sessionID
}

// callTool calls an MCP tool and returns the result.
func callTool(t *testing.T, handler http.Handler, sessionID string, toolName string, args map[string]any) map[string]any {
	callRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      100,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": args,
		},
	}

	body, _ := json.Marshal(callRequest)
	req := mcpRequest(t, body, sessionID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "tool call response: %s", rec.Body.String())

	response, err := parseSSEResponse(rec.Body.String())
	require.NoError(t, err, "response: %s", rec.Body.String())

	return response
}

func TestMCPHandler_ExecuteSQL_EmptyResults(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)

	// Create a simple test table
	ctx := t.Context()
	err := config.DB.Exec(ctx, `CREATE TABLE IF NOT EXISTS test_mcp_table (id Int32, name String) ENGINE = Memory`)
	require.NoError(t, err)

	handler, sessionID := mcpSession(t)

	// Query empty table - this should return empty arrays, not null
	response := callTool(t, handler, sessionID, "execute_sql", map[string]any{
		"query":       "SELECT * FROM test_mcp_table",
		"description": "Query empty test table",
	})

	// Check we got a result (not an error)
	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	// Parse the content
	content, ok := result["content"].([]any)
	require.True(t, ok, "expected content array")
	require.Len(t, content, 1)

	textContent := content[0].(map[string]any)
	text := textContent["text"].(string)

	var output map[string]any
	err = json.Unmarshal([]byte(text), &output)
	require.NoError(t, err, "failed to parse output: %s", text)

	// Verify columns is an array (not null)
	columns, ok := output["columns"].([]any)
	require.True(t, ok, "columns should be an array, got: %T", output["columns"])
	assert.Len(t, columns, 2) // id, name

	// Verify rows is an array (not null)
	rows, ok := output["rows"].([]any)
	require.True(t, ok, "rows should be an array, got: %T", output["rows"])
	assert.Len(t, rows, 0) // empty

	assert.Equal(t, float64(0), output["row_count"])
}

func TestMCPHandler_ExecuteSQL_WithData(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)

	ctx := t.Context()
	err := config.DB.Exec(ctx, `CREATE TABLE IF NOT EXISTS test_mcp_data (id Int32, name String) ENGINE = Memory`)
	require.NoError(t, err)
	err = config.DB.Exec(ctx, `INSERT INTO test_mcp_data VALUES (1, 'alice'), (2, 'bob')`)
	require.NoError(t, err)

	handler, sessionID := mcpSession(t)

	response := callTool(t, handler, sessionID, "execute_sql", map[string]any{
		"query":       "SELECT id, name FROM test_mcp_data ORDER BY id",
		"description": "Query test data",
	})

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	content := result["content"].([]any)
	textContent := content[0].(map[string]any)
	text := textContent["text"].(string)

	var output map[string]any
	err = json.Unmarshal([]byte(text), &output)
	require.NoError(t, err)

	columns := output["columns"].([]any)
	assert.Equal(t, []any{"id", "name"}, columns)

	rows := output["rows"].([]any)
	assert.Len(t, rows, 2)
	assert.Equal(t, float64(2), output["row_count"])
}

func TestMCPHandler_ExecuteSQL_InvalidQuery(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)

	handler, sessionID := mcpSession(t)

	response := callTool(t, handler, sessionID, "execute_sql", map[string]any{
		"query":       "SELECT * FROM nonexistent_table_xyz",
		"description": "Query nonexistent table",
	})

	// MCP returns errors inside result with isError: true
	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)
	assert.True(t, result["isError"].(bool), "expected isError to be true")

	content := result["content"].([]any)
	textContent := content[0].(map[string]any)
	assert.Contains(t, textContent["text"].(string), "query failed")
}

func TestMCPHandler_ExecuteCypher_EmptyResults(t *testing.T) {
	apitesting.SetupTestNeo4j(t, testNeo4jDB)

	handler, sessionID := mcpSession(t)

	// Query for non-existent nodes - this should return empty arrays, not null
	response := callTool(t, handler, sessionID, "execute_cypher", map[string]any{
		"query":       "MATCH (n:NonExistentLabel12345) RETURN n.name as name",
		"description": "Query non-existent nodes",
	})

	// Check we got a result (not an error)
	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	content, ok := result["content"].([]any)
	require.True(t, ok, "expected content array")
	require.Len(t, content, 1)

	textContent := content[0].(map[string]any)
	text := textContent["text"].(string)

	var output map[string]any
	err := json.Unmarshal([]byte(text), &output)
	require.NoError(t, err, "failed to parse output: %s", text)

	// Verify columns is an array (not null) - THIS WAS THE BUG
	columns, ok := output["columns"].([]any)
	require.True(t, ok, "columns should be an array, got: %T (%v)", output["columns"], output["columns"])
	assert.Len(t, columns, 0) // empty when no results

	// Verify rows is an array (not null)
	rows, ok := output["rows"].([]any)
	require.True(t, ok, "rows should be an array, got: %T", output["rows"])
	assert.Len(t, rows, 0)

	assert.Equal(t, float64(0), output["row_count"])
}

func TestMCPHandler_ExecuteCypher_WithData(t *testing.T) {
	seedFunc := func(ctx context.Context, session neo4j.Session) error {
		_, err := session.Run(ctx, `
			CREATE (a:MCPTestNode {name: 'node1', value: 100})
			CREATE (b:MCPTestNode {name: 'node2', value: 200})
		`, nil)
		return err
	}
	apitesting.SetupTestNeo4jWithData(t, testNeo4jDB, seedFunc)

	handler, sessionID := mcpSession(t)

	response := callTool(t, handler, sessionID, "execute_cypher", map[string]any{
		"query":       "MATCH (n:MCPTestNode) RETURN n.name as name, n.value as value ORDER BY n.name",
		"description": "Query test nodes",
	})

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	content := result["content"].([]any)
	textContent := content[0].(map[string]any)
	text := textContent["text"].(string)

	var output map[string]any
	err := json.Unmarshal([]byte(text), &output)
	require.NoError(t, err)

	columns := output["columns"].([]any)
	assert.Equal(t, []any{"name", "value"}, columns)

	rows := output["rows"].([]any)
	assert.Len(t, rows, 2)
	assert.Equal(t, float64(2), output["row_count"])

	// Verify row content
	row1 := rows[0].(map[string]any)
	assert.Equal(t, "node1", row1["name"])
	assert.Equal(t, float64(100), row1["value"])
}

func TestMCPHandler_GetSchema(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)

	// Create a test table so schema has something to return
	ctx := t.Context()
	err := config.DB.Exec(ctx, `CREATE TABLE IF NOT EXISTS test_schema_table (id Int32) ENGINE = Memory`)
	require.NoError(t, err)

	handler, sessionID := mcpSession(t)

	response := callTool(t, handler, sessionID, "get_schema", map[string]any{})

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	content := result["content"].([]any)
	textContent := content[0].(map[string]any)
	text := textContent["text"].(string)

	var output map[string]any
	err = json.Unmarshal([]byte(text), &output)
	require.NoError(t, err)

	schema, ok := output["schema"].(string)
	require.True(t, ok, "expected schema string")
	assert.Contains(t, schema, "test_schema_table")
}

func TestMCPHandler_ReadDocs(t *testing.T) {
	handler, sessionID := mcpSession(t)

	response := callTool(t, handler, sessionID, "read_docs", map[string]any{
		"page": "index",
	})

	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)

	content := result["content"].([]any)
	textContent := content[0].(map[string]any)
	text := textContent["text"].(string)

	var output map[string]any
	err := json.Unmarshal([]byte(text), &output)
	require.NoError(t, err)

	assert.Equal(t, "index", output["page"])
	docContent, ok := output["content"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, docContent)
}

func TestMCPHandler_ReadDocs_InvalidPage(t *testing.T) {
	handler, sessionID := mcpSession(t)

	// Use a page name with path traversal attempt - should be rejected by format validation
	response := callTool(t, handler, sessionID, "read_docs", map[string]any{
		"page": "../../../etc/passwd",
	})

	// MCP returns errors inside result with isError: true
	result, ok := response["result"].(map[string]any)
	require.True(t, ok, "expected result, got: %v", response)
	assert.True(t, result["isError"].(bool), "expected isError to be true")

	content := result["content"].([]any)
	textContent := content[0].(map[string]any)
	assert.Contains(t, textContent["text"].(string), "invalid page name")
}
