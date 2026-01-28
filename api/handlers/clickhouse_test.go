package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/config"
)

// panicRows implements driver.Rows but panics on any method call.
// This proves that safeQueryRows never touches rows when an error is returned.
type panicRows struct{}

func (panicRows) Columns() []string          { panic("Columns called on bad rows") }
func (panicRows) ColumnTypes() []driver.ColumnType { panic("ColumnTypes called on bad rows") }
func (panicRows) Next() bool                 { panic("Next called on bad rows") }
func (panicRows) Scan(dest ...any) error     { panic("Scan called on bad rows") }
func (panicRows) ScanStruct(dest any) error  { panic("ScanStruct called on bad rows") }
func (panicRows) Totals(dest ...any) error   { panic("Totals called on bad rows") }
func (panicRows) Close() error               { panic("Close called on bad rows") }
func (panicRows) Err() error                 { panic("Err called on bad rows") }

// mockConn implements the minimal driver.Conn interface needed for Query.
type mockConn struct {
	driver.Conn
	rows driver.Rows
	err  error
}

func (m *mockConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return m.rows, m.err
}

func TestSafeQueryRows_ErrorReturnsNilRows(t *testing.T) {
	orig := config.DB
	defer func() { config.DB = orig }()

	config.DB = &mockConn{
		rows: panicRows{}, // would panic if touched
		err:  errors.New("query timeout"),
	}

	rows, err := safeQueryRows(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if rows != nil {
		t.Fatal("expected nil rows on error, got non-nil")
	}
}

func TestSafeQueryRows_SuccessPassesRows(t *testing.T) {
	orig := config.DB
	defer func() { config.DB = orig }()

	// Use a non-nil sentinel to verify pass-through (we won't call methods on it)
	sentinel := panicRows{}
	config.DB = &mockConn{
		rows: sentinel,
		err:  nil,
	}

	rows, err := safeQueryRows(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows == nil {
		t.Fatal("expected non-nil rows on success")
	}
}
