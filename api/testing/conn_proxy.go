package apitesting

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnProxy implements driver.Conn and dispatches to per-test connections.
// Each test registers its connection with RegisterForTest(t, conn). Lookups
// walk the test name hierarchy (subtest → parent → grandparent) so that
// sub-tests automatically inherit the parent's connection.
type ConnProxy struct {
	Default   driver.Conn
	overrides sync.Map // test name (string) -> driver.Conn
	// seqFallback is set by non-parallel tests whose handlers spawn
	// background goroutines (e.g. MCP, errgroup).
	seqFallback atomic.Pointer[driver.Conn]
}

// DatabaseProxy stores per-test database name overrides.
type DatabaseProxy struct {
	Default     string
	overrides   sync.Map // test name (string) -> string
	seqFallback atomic.Pointer[string]
}

// currentTest stores the *testing.T for the current goroutine so that
// conn() can resolve which test is calling without requiring the caller
// to pass *testing.T explicitly. This is set via BindTest.
var currentTest sync.Map // goroutine ID -> *testing.T

// BindTest associates the current goroutine with a test so that ConnProxy
// lookups can resolve the correct connection. Call this at the start of
// each test function that uses t.Parallel().
func BindTest(t *testing.T) {
	gid := goroutineID()
	currentTest.Store(gid, t)
	t.Cleanup(func() { currentTest.Delete(gid) })
}

// RegisterForTest stores a connection override keyed by the test's name.
func (p *ConnProxy) RegisterForTest(t *testing.T, conn driver.Conn) {
	p.overrides.Store(t.Name(), conn)
	t.Cleanup(func() { p.overrides.Delete(t.Name()) })
}

// SetSequentialFallback sets a connection that any goroutine can use as fallback.
func (p *ConnProxy) SetSequentialFallback(conn driver.Conn) {
	p.seqFallback.Store(&conn)
}

// ClearSequentialFallback clears the sequential fallback.
func (p *ConnProxy) ClearSequentialFallback() {
	p.seqFallback.Store(nil)
}

func (p *ConnProxy) conn() driver.Conn {
	// Try to find the test for this goroutine and walk up the name hierarchy.
	if t, ok := currentTest.Load(goroutineID()); ok {
		name := t.(*testing.T).Name()
		for name != "" {
			if v, ok := p.overrides.Load(name); ok {
				return v.(driver.Conn)
			}
			// Walk up: "TestFoo/sub/deep" → "TestFoo/sub" → "TestFoo"
			i := len(name) - 1
			for i >= 0 && name[i] != '/' {
				i--
			}
			if i < 0 {
				break
			}
			name = name[:i]
		}
	}
	if ptr := p.seqFallback.Load(); ptr != nil {
		return *ptr
	}
	if p.Default == nil {
		panic("ConnProxy: no connection found for current goroutine (missing BindTest call?)")
	}
	return p.Default
}

// RegisterForTest stores a database name override keyed by the test's name.
func (p *DatabaseProxy) RegisterForTest(t *testing.T, name string) {
	p.overrides.Store(t.Name(), name)
	t.Cleanup(func() { p.overrides.Delete(t.Name()) })
}

// SetSequentialFallback sets a database name that any goroutine can use as fallback.
func (p *DatabaseProxy) SetSequentialFallback(name string) {
	p.seqFallback.Store(&name)
}

// ClearSequentialFallback clears the sequential fallback.
func (p *DatabaseProxy) ClearSequentialFallback() {
	p.seqFallback.Store(nil)
}

// Get returns the database name for the current goroutine's test.
func (p *DatabaseProxy) Get() string {
	if t, ok := currentTest.Load(goroutineID()); ok {
		name := t.(*testing.T).Name()
		for name != "" {
			if v, ok := p.overrides.Load(name); ok {
				return v.(string)
			}
			i := len(name) - 1
			for i >= 0 && name[i] != '/' {
				i--
			}
			if i < 0 {
				break
			}
			name = name[:i]
		}
	}
	if ptr := p.seqFallback.Load(); ptr != nil {
		return *ptr
	}
	return p.Default
}

// driver.Conn interface implementation — all methods delegate to conn().

func (p *ConnProxy) Contributors() []string { return p.conn().Contributors() }
func (p *ConnProxy) ServerVersion() (*driver.ServerVersion, error) {
	return p.conn().ServerVersion()
}
func (p *ConnProxy) Select(ctx context.Context, dest any, query string, args ...any) error {
	return p.conn().Select(ctx, dest, query, args...)
}
func (p *ConnProxy) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return p.conn().Query(ctx, query, args...)
}
func (p *ConnProxy) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return p.conn().QueryRow(ctx, query, args...)
}
func (p *ConnProxy) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return p.conn().PrepareBatch(ctx, query, opts...)
}
func (p *ConnProxy) Exec(ctx context.Context, query string, args ...any) error {
	return p.conn().Exec(ctx, query, args...)
}
func (p *ConnProxy) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return p.conn().AsyncInsert(ctx, query, wait, args...) //nolint:staticcheck
}
func (p *ConnProxy) Ping(ctx context.Context) error { return p.conn().Ping(ctx) }
func (p *ConnProxy) Stats() driver.Stats            { return p.conn().Stats() }
func (p *ConnProxy) Close() error                   { return p.conn().Close() }

// goroutineID returns the current goroutine's ID by reading the test
// stored in the goroutine-local currentTest map.
func goroutineID() uint64 {
	// Use runtime.Stack to extract the goroutine ID.
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	field := buf[:n]
	field = bytes.TrimPrefix(field, []byte("goroutine "))
	field = field[:bytes.IndexByte(field, ' ')]
	id, _ := strconv.ParseUint(string(field), 10, 64)
	return id
}
