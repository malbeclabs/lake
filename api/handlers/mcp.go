package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	commonprompts "github.com/malbeclabs/lake/agent/pkg/workflow/prompts"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCP server instance (initialized once)
var mcpHandler http.Handler

// InitMCP initializes the MCP server and returns the HTTP handler.
// This should be called once during startup.
func InitMCP() http.Handler {
	if mcpHandler != nil {
		return mcpHandler
	}

	mcpHandler = mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return createMCPServer(r)
	}, nil)

	return mcpHandler
}

// createMCPServer creates a new MCP server instance for each request.
// The server is configured based on the request context (env, auth).
func createMCPServer(r *http.Request) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "doublezero",
		Version: "1.0.0",
	}, nil)

	ctx := r.Context()
	env := EnvFromContext(ctx)

	// Register tools
	registerExecuteSQLTool(server, r)
	registerReadDocsTool(server)
	registerGetSchemaTool(server, r)

	// Only add Cypher tool for mainnet-beta (where Neo4j is available)
	if config.Neo4jClient != nil && env == EnvMainnet {
		registerExecuteCypherTool(server, r)
	}

	// Register resources
	registerSchemaResource(server, r)
	registerSQLContextResource(server)
	registerCypherContextResource(server)

	// Register prompts
	registerAnalyzeDataPrompt(server)

	return server
}

// ExecuteSQLInput is the input for the execute_sql tool.
type ExecuteSQLInput struct {
	Query       string `json:"query" jsonschema:"The SQL query to execute against ClickHouse"`
	Description string `json:"description,omitempty" jsonschema:"Brief description of what this query does (e.g., 'Count validators by metro')"`
}

// ExecuteSQLOutput is the output from the execute_sql tool.
type ExecuteSQLOutput struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	ElapsedMs int64    `json:"elapsed_ms"`
}

func registerExecuteSQLTool(server *mcp.Server, r *http.Request) {
	// Capture env and IP from original request for use in handler
	env := EnvFromContext(r.Context())
	ip := GetIPFromRequest(r)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "execute_sql",
		Description: "Execute a SQL query directly against the ClickHouse database. Returns raw query results. Use this when you already know the exact SQL query you want to run. For natural language questions, use ask_question instead. Always provide a brief 'description' parameter summarizing what the query does.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ExecuteSQLInput) (*mcp.CallToolResult, ExecuteSQLOutput, error) {
		// Check rate limit
		if errMsg := CheckRateLimit(QueryRateLimiter, ip); errMsg != "" {
			return nil, ExecuteSQLOutput{}, errors.New(errMsg)
		}

		// Transfer env to handler context (r.Context() may be canceled in streamable HTTP)
		ctx = ContextWithEnv(ctx, env)

		query := strings.TrimSpace(input.Query)
		if query == "" {
			return nil, ExecuteSQLOutput{}, errors.New("query is required")
		}

		query = strings.TrimSuffix(query, ";")

		start := time.Now()

		queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		db := envDB(ctx)

		rows, err := db.Query(queryCtx, query)
		duration := time.Since(start)
		if err != nil {
			metrics.RecordClickHouseQuery(duration, err)
			return nil, ExecuteSQLOutput{}, fmt.Errorf("query failed: %w", err)
		}
		defer rows.Close()

		// Get column info
		columnTypes := rows.ColumnTypes()
		columns := make([]string, len(columnTypes))
		for i, ct := range columnTypes {
			columns[i] = ct.Name()
		}

		// Collect rows
		var resultRows [][]any
		for rows.Next() {
			values := make([]any, len(columnTypes))
			for i, ct := range columnTypes {
				values[i] = reflect.New(ct.ScanType()).Interface()
			}

			if err := rows.Scan(values...); err != nil {
				metrics.RecordClickHouseQuery(duration, err)
				return nil, ExecuteSQLOutput{}, fmt.Errorf("scan failed: %w", err)
			}

			row := make([]any, len(values))
			for i, v := range values {
				row[i] = reflect.ValueOf(v).Elem().Interface()
			}
			resultRows = append(resultRows, row)
		}

		if err := rows.Err(); err != nil {
			metrics.RecordClickHouseQuery(duration, err)
			return nil, ExecuteSQLOutput{}, fmt.Errorf("rows error: %w", err)
		}

		metrics.RecordClickHouseQuery(duration, nil)

		// Convert to JSON-safe values
		safeRows := make([][]any, len(resultRows))
		for i, row := range resultRows {
			safeRow := make([]any, len(row))
			for j, v := range row {
				safeRow[j] = toJSONSafe(v)
			}
			safeRows[i] = safeRow
		}

		return nil, ExecuteSQLOutput{
			Columns:   columns,
			Rows:      safeRows,
			RowCount:  len(safeRows),
			ElapsedMs: duration.Milliseconds(),
		}, nil
	})
}

// ExecuteCypherInput is the input for the execute_cypher tool.
type ExecuteCypherInput struct {
	Query       string `json:"query" jsonschema:"The Cypher query to execute against Neo4j"`
	Description string `json:"description,omitempty" jsonschema:"Brief description of what this query does (e.g., 'Find shortest path between metros')"`
}

// ExecuteCypherOutput is the output from the execute_cypher tool.
type ExecuteCypherOutput struct {
	Columns   []string         `json:"columns"`
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"row_count"`
	ElapsedMs int64            `json:"elapsed_ms"`
}

func registerExecuteCypherTool(server *mcp.Server, r *http.Request) {
	// Capture IP from original request for rate limiting
	ip := GetIPFromRequest(r)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "execute_cypher",
		Description: "Execute a Cypher query against the Neo4j graph database. Use this for topology questions, path finding, reachability analysis, relationship traversal, and latency between metros (finds the network path since SQL only has directly-connected pairs). Only available on mainnet-beta. Always provide a brief 'description' parameter summarizing what the query does.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ExecuteCypherInput) (*mcp.CallToolResult, ExecuteCypherOutput, error) {
		// Check rate limit
		if errMsg := CheckRateLimit(QueryRateLimiter, ip); errMsg != "" {
			return nil, ExecuteCypherOutput{}, errors.New(errMsg)
		}

		query := strings.TrimSpace(input.Query)
		if query == "" {
			return nil, ExecuteCypherOutput{}, errors.New("query is required")
		}

		if config.Neo4jClient == nil {
			return nil, ExecuteCypherOutput{}, errors.New("Neo4j is not available in this environment")
		}

		start := time.Now()

		queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		session := config.Neo4jSession(queryCtx)
		defer session.Close(queryCtx)

		result, err := session.Run(queryCtx, query, nil)
		duration := time.Since(start)
		if err != nil {
			return nil, ExecuteCypherOutput{}, fmt.Errorf("query failed: %w", err)
		}

		records, err := result.Collect(queryCtx)
		if err != nil {
			return nil, ExecuteCypherOutput{}, fmt.Errorf("collect failed: %w", err)
		}

		// Extract columns from first record (initialize to empty slice, not nil)
		columns := []string{}
		if len(records) > 0 {
			columns = records[0].Keys
		}

		// Convert records to rows
		rows := make([]map[string]any, len(records))
		for i, record := range records {
			row := make(map[string]any)
			for _, key := range record.Keys {
				val, _ := record.Get(key)
				row[key] = neo4jValueToJSON(val)
			}
			rows[i] = row
		}

		return nil, ExecuteCypherOutput{
			Columns:   columns,
			Rows:      rows,
			RowCount:  len(rows),
			ElapsedMs: duration.Milliseconds(),
		}, nil
	})
}

// neo4jValueToJSON converts Neo4j values to JSON-serializable types.
func neo4jValueToJSON(v any) any {
	if v == nil {
		return nil
	}

	// Handle common Neo4j types
	switch val := v.(type) {
	case map[string]any:
		result := make(map[string]any)
		for k, v := range val {
			result[k] = neo4jValueToJSON(v)
		}
		return result
	case []any:
		result := make([]any, len(val))
		for i, v := range val {
			result[i] = neo4jValueToJSON(v)
		}
		return result
	default:
		// Try JSON marshal/unmarshal to normalize
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		var result any
		if json.Unmarshal(data, &result) != nil {
			return fmt.Sprintf("%v", v)
		}
		return result
	}
}

// ReadDocsInput is the input for the read_docs tool.
type ReadDocsInput struct {
	Page string `json:"page" jsonschema:"The documentation page to read"`
}

// ReadDocsOutput is the output from the read_docs tool.
type ReadDocsOutput struct {
	Page    string `json:"page"`
	Content string `json:"content"`
}

// docsBaseURL is the base URL for fetching raw documentation from GitHub.
const docsBaseURL = "https://raw.githubusercontent.com/malbeclabs/docs/main/docs/"

func registerReadDocsTool(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "read_docs",
		Description: "Read DoubleZero documentation to answer questions about concepts, architecture, setup, troubleshooting, or how the network works. Use this when users ask 'what is DZ', 'how do I set up', 'why isn't X working', or similar conceptual/procedural questions. Available pages include: index, architecture, setup, troubleshooting, connect, connect-multicast, contribute, contribute-overview, contribute-operations, users-overview, paying-fees, multicast-admin.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input ReadDocsInput) (*mcp.CallToolResult, ReadDocsOutput, error) {
		page := strings.TrimSpace(input.Page)
		if page == "" {
			return nil, ReadDocsOutput{}, errors.New("page is required")
		}

		// Validate page name format to prevent path traversal
		// Allow alphanumeric, hyphens, and spaces (some docs use spaces in names)
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\- ]*$`).MatchString(page) {
			return nil, ReadDocsOutput{}, fmt.Errorf("invalid page name: %s", page)
		}

		// Fetch docs
		url := docsBaseURL + page + ".md"
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, ReadDocsOutput{}, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			return nil, ReadDocsOutput{}, fmt.Errorf("failed to fetch docs: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, ReadDocsOutput{}, fmt.Errorf("docs page not found: %s (status %d)", page, resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, ReadDocsOutput{}, fmt.Errorf("failed to read response: %w", err)
		}

		content := string(body)

		// Truncate if too long
		if len(content) > 10000 {
			content = content[:10000] + "\n\n... (truncated)"
		}

		return nil, ReadDocsOutput{
			Page:    page,
			Content: content,
		}, nil
	})
}

// GetSchemaInput is the input for the get_schema tool.
type GetSchemaInput struct {
	// No required inputs - schema is fetched for current environment
}

// GetSchemaOutput is the output from the get_schema tool.
type GetSchemaOutput struct {
	Schema      string `json:"schema"`
	Environment string `json:"environment"`
}

func registerGetSchemaTool(server *mcp.Server, r *http.Request) {
	// Capture env and IP from original request for use in handler
	env := EnvFromContext(r.Context())
	ip := GetIPFromRequest(r)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_schema",
		Description: "Get the database schema for the current environment. Returns all available tables, columns, types, and view definitions from ClickHouse. Use this to understand what data is available before writing SQL queries.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GetSchemaInput) (*mcp.CallToolResult, GetSchemaOutput, error) {
		// Check rate limit
		if errMsg := CheckRateLimit(QueryRateLimiter, ip); errMsg != "" {
			return nil, GetSchemaOutput{}, errors.New(errMsg)
		}

		// Transfer env to handler context (r.Context() may be canceled in streamable HTTP)
		ctx = ContextWithEnv(ctx, env)

		schemaFetcher := NewDBSchemaFetcher()
		schema, err := schemaFetcher.FetchSchema(ctx)
		if err != nil {
			return nil, GetSchemaOutput{}, fmt.Errorf("failed to fetch schema: %w", err)
		}

		return nil, GetSchemaOutput{
			Schema:      schema,
			Environment: string(env),
		}, nil
	})
}

// registerSchemaResource registers the dynamic schema as an MCP resource.
func registerSchemaResource(server *mcp.Server, r *http.Request) {
	// Capture env from original request for use in handler
	env := EnvFromContext(r.Context())

	server.AddResource(&mcp.Resource{
		URI:         "doublezero://schema",
		Name:        "Database Schema",
		Description: "Dynamic database schema from ClickHouse including all tables, columns, types, and view definitions",
		MIMEType:    "text/plain",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		// Transfer env to handler context (r.Context() may be canceled in streamable HTTP)
		ctx = ContextWithEnv(ctx, env)

		schemaFetcher := NewDBSchemaFetcher()
		schema, err := schemaFetcher.FetchSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch schema: %w", err)
		}
		content := fmt.Sprintf("# DoubleZero Database Schema (%s)\n\n%s", env, schema)

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      "doublezero://schema",
					MIMEType: "text/plain",
					Text:     content,
				},
			},
		}, nil
	})
}

// registerSQLContextResource registers the SQL context as an MCP resource.
func registerSQLContextResource(server *mcp.Server) {
	server.AddResource(&mcp.Resource{
		URI:         "doublezero://sql-context",
		Name:        "SQL Context",
		Description: "SQL patterns, business rules, and ClickHouse-specific query guidelines for the DoubleZero database",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		content, err := commonprompts.PromptsFS.ReadFile("SQL_CONTEXT.md")
		if err != nil {
			return nil, fmt.Errorf("failed to read SQL context: %w", err)
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      "doublezero://sql-context",
					MIMEType: "text/markdown",
					Text:     string(content),
				},
			},
		}, nil
	})
}

// registerCypherContextResource registers the Cypher context as an MCP resource.
func registerCypherContextResource(server *mcp.Server) {
	server.AddResource(&mcp.Resource{
		URI:         "doublezero://cypher-context",
		Name:        "Cypher Context",
		Description: "Cypher query patterns and Neo4j graph database guidelines for topology and path queries",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		content, err := commonprompts.PromptsFS.ReadFile("CYPHER_CONTEXT.md")
		if err != nil {
			// Cypher context is optional (only available on mainnet)
			return &mcp.ReadResourceResult{
				Contents: []*mcp.ResourceContents{
					{
						URI:      "doublezero://cypher-context",
						MIMEType: "text/markdown",
						Text:     "# Cypher Context\n\nCypher context is not available in this environment.",
					},
				},
			}, nil
		}

		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      "doublezero://cypher-context",
					MIMEType: "text/markdown",
					Text:     string(content),
				},
			},
		}, nil
	})
}

// registerAnalyzeDataPrompt registers a prompt for data analysis.
func registerAnalyzeDataPrompt(server *mcp.Server) {
	server.AddPrompt(&mcp.Prompt{
		Name:        "analyze-data",
		Description: "Analyze DoubleZero network data by asking a natural language question. References resources for schema and query patterns.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "question",
				Description: "The data question to analyze (e.g., 'how many validators are on DZ?', 'show network health')",
				Required:    true,
			},
		},
	}, func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		question := ""
		if req.Params != nil && req.Params.Arguments != nil {
			if q, ok := req.Params.Arguments["question"]; ok {
				question = q
			}
		}

		promptText := fmt.Sprintf(`You are a data analyst for the DoubleZero (DZ) network.

To answer the question below:

1. Read the doublezero://schema resource to understand available tables and columns
2. Read the doublezero://sql-context resource for SQL patterns, ClickHouse specifics, and business rules
3. Use the execute_sql tool to run queries against ClickHouse
4. For topology or path-finding questions, also read doublezero://cypher-context and use the execute_cypher tool

Question: %s

Please analyze this question and provide a data-driven answer.`, question)

		return &mcp.GetPromptResult{
			Description: "Analyze DoubleZero data: " + question,
			Messages: []*mcp.PromptMessage{
				{
					Role: "user",
					Content: &mcp.TextContent{
						Text: promptText,
					},
				},
			},
		}, nil
	})
}
