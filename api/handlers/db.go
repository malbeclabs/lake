package handlers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/malbeclabs/lake/api/metrics"
)

// Cached schema to avoid querying ClickHouse system tables on every agent request.
var (
	schemaCache    string
	schemaCacheAt  time.Time
	schemaCacheMu  sync.RWMutex
	schemaCacheTTL = 60 * time.Second
)

// DBQuerier implements workflow.Querier using a ClickHouse connection.
type DBQuerier struct {
	db driver.Conn
}

// NewDBQuerier creates a new DBQuerier.
func (a *API) NewDBQuerier() *DBQuerier {
	return &DBQuerier{db: a.DB}
}

// Query executes a SQL query and returns the result.
// Agent queries always run against the mainnet database. To query other
// environments, use fully-qualified table names (e.g., lake_devnet.dim_devices_current).
func (q *DBQuerier) Query(ctx context.Context, sql string) (workflow.QueryResult, error) {
	sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")

	start := time.Now()
	rows, err := q.db.Query(ctx, sql)
	duration := time.Since(start)
	if err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return workflow.QueryResult{SQL: sql, Error: err.Error()}, nil
	}
	defer rows.Close()

	// Get column info
	columnTypes := rows.ColumnTypes()
	columns := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ct.Name()
	}

	// Collect all rows as maps
	var resultRows []map[string]any
	for rows.Next() {
		// Create properly typed values based on column types
		values := make([]any, len(columnTypes))
		for i, ct := range columnTypes {
			values[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(values...); err != nil {
			metrics.RecordClickHouseQuery(duration, err)
			return workflow.QueryResult{SQL: sql, Error: fmt.Sprintf("scan error: %v", err)}, nil
		}

		// Dereference pointers and build map
		row := make(map[string]any)
		for i, col := range columns {
			row[col] = reflect.ValueOf(values[i]).Elem().Interface()
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return workflow.QueryResult{SQL: sql, Error: err.Error()}, nil
	}

	metrics.RecordClickHouseQuery(duration, nil)

	// Sanitize rows to replace NaN/Inf values with nil (JSON-safe)
	workflow.SanitizeRows(resultRows)

	result := workflow.QueryResult{
		SQL:     sql,
		Columns: columns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}
	result.Formatted = formatQueryResult(result)

	return result, nil
}

// formatQueryResult creates a human-readable format of the query result.
func formatQueryResult(result workflow.QueryResult) string {
	if result.Error != "" {
		return fmt.Sprintf("Error: %s", result.Error)
	}

	if len(result.Rows) == 0 {
		return "Query returned no results."
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Results (%d rows):\n", len(result.Rows)))
	sb.WriteString("Columns: " + strings.Join(result.Columns, " | ") + "\n")
	sb.WriteString(strings.Repeat("-", 40) + "\n")

	// Limit output to first 50 rows
	maxRows := min(50, len(result.Rows))

	for i := range maxRows {
		row := result.Rows[i]
		var values []string
		for _, col := range result.Columns {
			// Use workflow.FormatValue to properly handle pointer types (e.g., ClickHouse Decimals)
			values = append(values, workflow.FormatValue(row[col]))
		}
		sb.WriteString(strings.Join(values, " | ") + "\n")
	}

	if len(result.Rows) > 50 {
		sb.WriteString(fmt.Sprintf("... and %d more rows\n", len(result.Rows)-50))
	}

	return sb.String()
}

// DBSchemaFetcher implements workflow.SchemaFetcher using a ClickHouse connection.
type DBSchemaFetcher struct {
	db       driver.Conn
	database string
}

// NewDBSchemaFetcher creates a new DBSchemaFetcher.
func (a *API) NewDBSchemaFetcher() *DBSchemaFetcher {
	return &DBSchemaFetcher{db: a.DB, database: a.Database}
}

// FetchSchema retrieves table columns and view definitions from ClickHouse.
// Schema is always fetched from the mainnet database.
// Results are cached for 60 seconds to avoid redundant queries under load.
func (f *DBSchemaFetcher) FetchSchema(ctx context.Context) (string, error) {
	// Check cache first
	schemaCacheMu.RLock()
	if schemaCache != "" && time.Since(schemaCacheAt) < schemaCacheTTL {
		cached := schemaCache
		schemaCacheMu.RUnlock()
		return cached, nil
	}
	schemaCacheMu.RUnlock()

	// Fetch columns from mainnet database
	start := time.Now()
	rows, err := f.db.Query(ctx, `
		SELECT
			table,
			name,
			type
		FROM system.columns
		WHERE database = $1
		  AND table NOT LIKE 'stg_%'
		  AND table != '_env_lock'
		ORDER BY table, position
	`, f.database)
	duration := time.Since(start)
	if err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return "", fmt.Errorf("failed to fetch columns: %w", err)
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(duration, nil)

	type columnInfo struct {
		Table string
		Name  string
		Type  string
	}
	var columns []columnInfo
	for rows.Next() {
		var c columnInfo
		if err := rows.Scan(&c.Table, &c.Name, &c.Type); err != nil {
			return "", err
		}
		columns = append(columns, c)
	}

	// Fetch view names to label them in the output
	start = time.Now()
	viewRows, err := f.db.Query(ctx, `
		SELECT name
		FROM system.tables
		WHERE database = $1
		  AND engine = 'View'
		  AND name NOT LIKE 'stg_%'
		  AND name != '_env_lock'
	`, f.database)
	duration = time.Since(start)
	if err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return "", fmt.Errorf("failed to fetch views: %w", err)
	}
	defer viewRows.Close()
	metrics.RecordClickHouseQuery(duration, nil)

	views := make(map[string]bool)
	for viewRows.Next() {
		var name string
		if err := viewRows.Scan(&name); err != nil {
			return "", err
		}
		views[name] = true
	}

	// Format schema as readable text
	var sb strings.Builder
	currentTable := ""
	for _, col := range columns {
		if col.Table != currentTable {
			if currentTable != "" {
				sb.WriteString("\n")
			}
			currentTable = col.Table
			if views[col.Table] {
				sb.WriteString(col.Table + " (VIEW):\n")
			} else {
				sb.WriteString(col.Table + ":\n")
			}
		}
		sb.WriteString("  - " + col.Name + " (" + compactType(col.Type) + ")\n")
	}

	result := sb.String()

	// Update cache
	schemaCacheMu.Lock()
	schemaCache = result
	schemaCacheAt = time.Now()
	schemaCacheMu.Unlock()

	return result, nil
}

// compactType simplifies ClickHouse type names for readability.
// e.g. "Nullable(String)" → "String", "LowCardinality(Nullable(String))" → "String",
// "DateTime64(3)" → "DateTime64", "Array(Nullable(UInt64))" → "Array(UInt64)"
var reTypeWrapper = regexp.MustCompile(`(?:Nullable|LowCardinality)\(([^()]*(?:\([^()]*\))?[^()]*)\)`)

func compactType(t string) string {
	// Strip Nullable and LowCardinality wrappers (may be nested)
	for reTypeWrapper.MatchString(t) {
		t = reTypeWrapper.ReplaceAllString(t, "$1")
	}
	// Remove precision from DateTime64(N)
	t = strings.Replace(t, "DateTime64(3)", "DateTime64", 1)
	t = strings.Replace(t, "DateTime64(9)", "DateTime64", 1)
	return t
}
