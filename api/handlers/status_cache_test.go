package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/config"
)

// errorConn implements driver.Conn and returns errors for every operation.
// This lets fetch functions run without panicking on a nil config.DB.
type errorConn struct{}

var errFake = errors.New("fake db error")

func (errorConn) Contributors() []string                                         { return nil }
func (errorConn) ServerVersion() (*driver.ServerVersion, error)                  { return nil, errFake }
func (errorConn) Select(context.Context, any, string, ...any) error              { return errFake }
func (errorConn) Query(context.Context, string, ...any) (driver.Rows, error)     { return nil, errFake }
func (errorConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row      { return &errorRow{} }
func (errorConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errFake
}
func (errorConn) Exec(context.Context, string, ...any) error            { return errFake }
func (errorConn) AsyncInsert(context.Context, string, bool, ...any) error { return errFake }
func (errorConn) Ping(context.Context) error                            { return errFake }
func (errorConn) Stats() driver.Stats                                   { return driver.Stats{} }
func (errorConn) Close() error                                          { return nil }

// errorRow implements driver.Row and returns errors.
type errorRow struct{}

func (errorRow) Err() error              { return errFake }
func (errorRow) Scan(...any) error       { return errFake }
func (errorRow) ScanStruct(any) error    { return errFake }

// newCancelledCache returns a StatusCache whose parent context is already
// cancelled, so any refresh that derives a child context will fail immediately.
// It also installs an errorConn as config.DB to prevent nil-pointer panics.
func newCancelledCache(t *testing.T) *StatusCache {
	t.Helper()
	orig := config.DB
	t.Cleanup(func() { config.DB = orig })
	config.DB = errorConn{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	return &StatusCache{
		linkHistory:      make(map[string]*LinkHistoryResponse),
		deviceHistory:    make(map[string]*DeviceHistoryResponse),
		metroPathLatency: make(map[string]*MetroPathLatencyResponse),
		ctx:              ctx,
		cancel:           cancel,
	}
}

func TestStatusCache_RefreshStatusKeepsStaleData(t *testing.T) {
	c := newCancelledCache(t)

	original := &StatusResponse{
		Network: NetworkSummary{Links: 42},
	}
	c.status = original
	c.statusLastRefresh = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	c.refreshStatus()

	if c.status != original {
		t.Error("refreshStatus overwrote stale data on error")
	}
	if !c.statusLastRefresh.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("refreshStatus updated timestamp on error")
	}
}

func TestStatusCache_RefreshTimelineKeepsStaleData(t *testing.T) {
	c := newCancelledCache(t)

	original := &TimelineResponse{
		Events: []TimelineEvent{{ID: "test-event"}},
	}
	c.timeline = original
	c.timelineLastRefresh = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	c.refreshTimeline()

	if c.timeline != original {
		t.Error("refreshTimeline overwrote stale data on error")
	}
	if !c.timelineLastRefresh.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("refreshTimeline updated timestamp on error")
	}
}

func TestStatusCache_RefreshOutagesKeepsStaleData(t *testing.T) {
	c := newCancelledCache(t)

	original := &LinkOutagesResponse{
		Outages: []LinkOutage{{ID: "test-outage"}},
	}
	c.outages = original
	c.outagesLastRefresh = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	c.refreshOutages()

	if c.outages != original {
		t.Error("refreshOutages overwrote stale data on error")
	}
	if !c.outagesLastRefresh.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Error("refreshOutages updated timestamp on error")
	}
}
