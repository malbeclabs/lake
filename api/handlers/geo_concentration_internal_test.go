package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

// deadlineConn records the deadline on the context of the first query it is
// handed, then fails it so the caller returns without needing a real result.
type deadlineConn struct {
	driver.Conn
	deadline time.Time
	hasOne   bool
	queried  bool
}

func (c *deadlineConn) Query(ctx context.Context, _ string, _ ...any) (driver.Rows, error) {
	if !c.queried {
		c.queried = true
		c.deadline, c.hasOne = ctx.Deadline()
	}
	return nil, errors.New("no db")
}

// TestFetchGeoConcentrationTakesTheCallersDeadline pins the split that keeps the
// page-cache worker off the request path's budget. The deadline used to be
// clamped inside the fetch, which the worker shares, so the lighter
// concentration query could never use more than a fraction of its 60s entry
// budget while the heavier geo_validators got all of it.
func TestFetchGeoConcentrationTakesTheCallersDeadline(t *testing.T) {
	t.Parallel()

	conn := &deadlineConn{}
	a := &API{DB: conn, DZDPDB: "dzdp"}

	want := 55 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), want)
	defer cancel()

	_, err := a.FetchGeoConcentrationData(ctx)
	require.Error(t, err)
	require.True(t, conn.hasOne, "the query must carry the caller's deadline")
	require.Greater(t, time.Until(conn.deadline), want-5*time.Second,
		"the fetch shortened the caller's deadline")
}

// TestGetGeoConcentrationOwnsTheRequestDeadline is the other half: net/http sets
// no deadline on r.Context(), so the handler has to bound the request itself.
func TestGetGeoConcentrationOwnsTheRequestDeadline(t *testing.T) {
	t.Parallel()

	conn := &deadlineConn{}
	a := &API{DB: conn, DZDPDB: "dzdp"}

	req := httptest.NewRequest(http.MethodGet, "/api/dz/geoloc/concentration", nil)
	a.GetGeoConcentration(httptest.NewRecorder(), req)

	require.True(t, conn.hasOne, "the handler must bound the request")
	require.InDelta(t, (30 * time.Second).Seconds(), time.Until(conn.deadline).Seconds(), 5,
		"request-path deadline should match its sibling handler's (geo_validators)")
}
