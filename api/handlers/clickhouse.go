package handlers

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/config"
)

// safeQueryRows wraps config.DB.Query and guarantees that when an error is
// returned the rows value is nil.  clickhouse-go may hand back a non-nil
// driver.Rows with nil internals on query timeout, which causes a panic if
// the caller touches rows.Next() or rows.Close().
// See https://github.com/ClickHouse/clickhouse-go/issues/761
func safeQueryRows(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := config.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
