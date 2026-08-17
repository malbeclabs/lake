package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockErrRow is a driver.Row double whose Scan always fails.
type mockErrRow struct{ err error }

func (r mockErrRow) Err() error                { return r.err }
func (r mockErrRow) Scan(dest ...any) error    { return r.err }
func (r mockErrRow) ScanStruct(dest any) error { return r.err }

// mockErrConn is a driver.Conn double whose every query fails, so a Fetch*Data
// call runs entirely on its failure paths without a live ClickHouse. Modelled on
// mockContributorScopeConn; it lives here because the Fetch*Data functions and
// the panel bookkeeping they use are unexported.
type mockErrConn struct {
	driver.Conn
	err error
}

func (m *mockErrConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return nil, m.err
}

func (m *mockErrConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return mockErrRow{err: m.err}
}

func failingAPI() *API {
	return &API{DB: &mockErrConn{err: errors.New("ch: memory limit (total) exceeded while executing")}}
}

// mockNopRow is a driver.Row double whose Scan succeeds without writing, leaving
// the caller's already-zeroed destinations as they are.
type mockNopRow struct{}

func (mockNopRow) Err() error                { return nil }
func (mockNopRow) Scan(dest ...any) error    { return nil }
func (mockNopRow) ScanStruct(dest any) error { return nil }

// substrFailConn fails only the queries containing marker; every other query
// succeeds with no rows, so exactly one panel of a group can be failed while its
// siblings compute.
type substrFailConn struct {
	driver.Conn
	marker string
	err    error
}

func (m *substrFailConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, m.marker) {
		return nil, m.err
	}
	return &mockCodeRows{}, nil
}

func (m *substrFailConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if strings.Contains(query, m.marker) {
		return mockErrRow{err: m.err}
	}
	return mockNopRow{}
}

// priorWindowFailConn fails only the scans whose time bounds reach into the prior
// window (any bound before priorEnd, i.e. the current window's start); every other
// query returns no rows.
type priorWindowFailConn struct {
	driver.Conn
	priorEnd time.Time
	err      error
}

func (m *priorWindowFailConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	for _, a := range args {
		if ts, ok := a.(time.Time); ok && ts.Before(m.priorEnd) {
			return nil, m.err
		}
	}
	return &mockCodeRows{}, nil
}

func (m *priorWindowFailConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return mockErrRow{err: m.err}
}

// TestNHDrainPriorWindowFailureDegradesOnly pins that a failed prior-window scan
// costs the vs-prior delta only. Those two scans feed resp.Prev; the current
// figures come from their own scans, so erroring the group would blank a whole
// panel of intact numbers and stop the worker rewriting the cached blob.
func TestNHDrainPriorWindowFailureDegradesOnly(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-07-01T00:00:00Z")
	a := &API{DB: &priorWindowFailConn{
		priorEnd: start,
		err:      errors.New("ch: memory limit (total) exceeded while executing"),
	}}

	resp := a.FetchNetworkHealthDrainData(context.Background(), start, end, "")

	assert.ElementsMatch(t, []string{"link_down_events_prev", "status_changes_prev"}, resp.Degraded)
	assert.Empty(t, resp.Error, "a prior-window failure must not blank the current window")
	require.NotNil(t, resp.Prev)
}

// TestNHGroupsReportFailedPanels drives every group against a connection whose
// queries all fail and asserts the group reports WHICH panels it could not
// compute, and sets Error only when a critical panel failed. Before this, four
// groups swallowed every panel error to nil and published a failed query's zero
// as a real measurement.
func TestNHGroupsReportFailedPanels(t *testing.T) {
	a := failingAPI()
	ctx := context.Background()
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-07-01T00:00:00Z")

	t.Run("overview", func(t *testing.T) {
		resp := a.FetchNetworkHealthOverviewData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{
			"active_devices", "active_links", "active_links_prior", "metros",
			"throughput_ts", "peak_prior", "latency_vs_internet", "isis",
			"freshness", "contributors", "contributor_devices",
		}, resp.Degraded)
		assert.NotEmpty(t, resp.Error, "throughput_ts is critical")
	})

	t.Run("availability", func(t *testing.T) {
		resp := a.FetchNetworkHealthAvailabilityData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{"link_availability", "device_availability"}, resp.Degraded)
		assert.Empty(t, resp.Error, "no availability panel is headline-critical")
	})

	t.Run("latency", func(t *testing.T) {
		resp := a.FetchNetworkHealthLatencyData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{"latency_links", "sla"}, resp.Degraded)
		assert.Empty(t, resp.Error)
	})

	t.Run("capacity", func(t *testing.T) {
		resp := a.FetchNetworkHealthCapacityData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{
			"seat_capacity", "device_slots", "dia_interfaces", "top_links", "fullest_links",
		}, resp.Degraded)
		assert.Empty(t, resp.Error)
	})

	t.Run("outages", func(t *testing.T) {
		resp := a.FetchNetworkHealthOutagesData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{
			"reliability", "reliability_prior", "degraded_links",
			"outage_summary", "outage_summary_prev", "downtime_links",
			"downtime_devices", "outages_ts", "error_hotspots",
		}, resp.Degraded)
		assert.NotEmpty(t, resp.Error, "reliability is critical")
	})

	t.Run("drain", func(t *testing.T) {
		resp := a.FetchNetworkHealthDrainData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{
			"link_down_events", "status_changes", "link_down_events_prev", "status_changes_prev",
		}, resp.Degraded)
		assert.NotEmpty(t, resp.Error, "every current-window drain figure is derived from these scans")
		// The zeroed figures must never be published as facts.
		assert.Equal(t, 0, resp.DrainTiming.Drains)
	})

	t.Run("impactful", func(t *testing.T) {
		resp := a.FetchNetworkHealthImpactfulData(ctx, start, end, "")
		assert.Equal(t, []string{"impactful"}, resp.Degraded)
		assert.True(t, resp.Unavailable)
		assert.NotEmpty(t, resp.Error)
	})

	t.Run("deferred", func(t *testing.T) {
		resp := a.FetchNetworkHealthDeferredData(ctx, start, end, "")
		assert.ElementsMatch(t, []string{"status_changes", "status_changes_prev"}, resp.Degraded)
		assert.True(t, resp.UndrainUnavailable)
		assert.NotEmpty(t, resp.Error)
	})
}

// TestNHDegradedLinksFailureDegradesOnly pins that the small degraded-links scan
// is not group-critical. It feeds one sentence of the reliability panel, which
// already names degraded_links among its sources, so erroring the group would
// blank five intact panels plus the outage headline tile and stop the worker
// rewriting the cached blob.
func TestNHDegradedLinksFailureDegradesOnly(t *testing.T) {
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-07-01T00:00:00Z")
	a := &API{DB: &substrFailConn{
		marker: "a_loss_pct > 1.0",
		err:    errors.New("ch: memory limit (total) exceeded while executing"),
	}}

	resp := a.FetchNetworkHealthOutagesData(context.Background(), start, end, "")

	assert.Equal(t, []string{"degraded_links"}, resp.Degraded)
	assert.Empty(t, resp.Error, "one failed sub-query must not blank the whole group")
}

// TestNHGroupErrorNotSuppressedWhenScoped is the regression pin for the removed
// `!scoped` gates: a contributor-scoped view used to publish 0 link failures and
// 0 traffic-weighted hours from a failed query, because Error was set only on the
// network-wide path. The worker never precomputes a scoped payload, so the gate
// protected nothing and only hid the failure from the viewer.
func TestNHGroupErrorNotSuppressedWhenScoped(t *testing.T) {
	a := failingAPI()
	ctx := context.Background()
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-07-01T00:00:00Z")
	const contrib = "C1"

	overview := a.FetchNetworkHealthOverviewData(ctx, start, end, contrib)
	assert.NotEmpty(t, overview.Error, "scoped overview must report a failed throughput_ts")
	assert.Contains(t, overview.Degraded, "throughput_ts")

	outages := a.FetchNetworkHealthOutagesData(ctx, start, end, contrib)
	assert.NotEmpty(t, outages.Error, "scoped outages must report a failed reliability scan")
	assert.Contains(t, outages.Degraded, "reliability")
	assert.Equal(t, uint64(0), outages.OutageCount, "the zero must be reported as unavailable, not as a fact")

	impactful := a.FetchNetworkHealthImpactfulData(ctx, start, end, contrib)
	assert.NotEmpty(t, impactful.Error, "scoped impactful must report its failed scan")
	assert.True(t, impactful.Unavailable)
}

// TestNHFetchContribLinkPKsFailureIsNotEmptyScope pins that a failed pk lookup is
// distinguishable from a contributor that genuinely owns no links. Both stop the
// scans, but only the failure may set Error: publishing "0 drains, 0 undrains"
// for a contributor whose lookup errored states a fact that was never measured.
func TestNHFetchContribLinkPKsFailureIsNotEmptyScope(t *testing.T) {
	ctx := context.Background()
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-07-01T00:00:00Z")

	t.Run("query_failure", func(t *testing.T) {
		a := failingAPI()
		pks, ok, err := a.fetchContribLinkPKs(ctx, "C1")
		require.Error(t, err)
		assert.Nil(t, pks)
		assert.False(t, ok)

		drain := a.FetchNetworkHealthDrainData(ctx, start, end, "C1")
		assert.Equal(t, []string{"contrib_link_pks"}, drain.Degraded)
		assert.NotEmpty(t, drain.Error)

		deferred := a.FetchNetworkHealthDeferredData(ctx, start, end, "C1")
		assert.Equal(t, []string{"contrib_link_pks"}, deferred.Degraded)
		assert.True(t, deferred.UndrainUnavailable)
		assert.NotEmpty(t, deferred.Error)
	})

	t.Run("owns_no_links", func(t *testing.T) {
		a := &API{DB: &mockContributorScopeConn{}} // Query returns zero rows
		pks, ok, err := a.fetchContribLinkPKs(ctx, "CEMPTY")
		require.NoError(t, err)
		assert.Nil(t, pks)
		assert.False(t, ok, "an empty scope still must not fall back to a network-wide scan")

		drain := a.FetchNetworkHealthDrainData(ctx, start, end, "CEMPTY")
		assert.Empty(t, drain.Degraded)
		assert.Empty(t, drain.Error, "owning no links is not a failure")

		deferred := a.FetchNetworkHealthDeferredData(ctx, start, end, "CEMPTY")
		assert.Empty(t, deferred.Degraded)
		assert.Empty(t, deferred.Error)
	})

	t.Run("network_wide_runs_unfiltered", func(t *testing.T) {
		a := failingAPI()
		pks, ok, err := a.fetchContribLinkPKs(ctx, "")
		require.NoError(t, err)
		assert.Nil(t, pks)
		assert.True(t, ok)
	})
}

// levelHandler records the level of every emitted record. Panel goroutines log
// concurrently, so the tally is mutex-guarded.
type levelHandler struct {
	mu     *sync.Mutex
	levels *[]slog.Level
}

func (h levelHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h levelHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.levels = append(*h.levels, r.Level)
	return nil
}
func (h levelHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h levelHandler) WithGroup(string) slog.Handler      { return h }

// TestNHPanelFailureLogsWarnNotError is the pin for the pager regression: a
// ClickHouse memory-limit failure (code 241) does not classify transient, so the
// old logError landed at ERROR and paged on-call once per refresh cycle for a
// condition whose only remedy is a panel marked unavailable. The alert now
// belongs to the cache worker's escalator, so the handler line stays at WARN.
func TestNHPanelFailureLogsWarnNotError(t *testing.T) {
	var mu sync.Mutex
	var levels []slog.Level
	saved := slog.Default()
	slog.SetDefault(slog.New(levelHandler{mu: &mu, levels: &levels}))
	t.Cleanup(func() { slog.SetDefault(saved) })

	a := failingAPI()
	ctx := context.Background()
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-07-01T00:00:00Z")

	a.FetchNetworkHealthAvailabilityData(ctx, start, end, "")
	a.FetchNetworkHealthOutagesData(ctx, start, end, "")
	a.FetchNetworkHealthDrainData(ctx, start, end, "")

	mu.Lock()
	defer mu.Unlock()
	var errs, warns int
	for _, l := range levels {
		switch l {
		case slog.LevelError:
			errs++
		case slog.LevelWarn:
			warns++
		}
	}
	assert.Zero(t, errs, "a failed panel must not page on-call every refresh cycle")
	assert.NotZero(t, warns, "the failure must still be logged")
}

// TestFetchNetworkHealthTicketsDataWindowBounds pins the tickets window: the ops
// API has no date filter, so the current set used to run from start to now and be
// counted against outages bounded at end. Tickets created at or after end, and
// tickets whose created_at does not parse, must not land in the current window.
func TestFetchNetworkHealthTicketsDataWindowBounds(t *testing.T) {
	savedURL := opsMgmtBaseURL
	defer func() { opsMgmtBaseURL = savedURL }()
	t.Setenv("OPS_MANAGEMENT_API_KEY", "test-key")

	// Anchored to now: the ops pagination walks back from now to priorStart, and
	// a window further back than nhOpsTicketsMaxLookback is refused outright.
	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -29)

	at := func(d time.Duration) string { return end.Add(d).Format(time.RFC3339) }
	ticket := func(id, created string) map[string]any {
		return map[string]any{"id": id, "type": "incident", "status": "open", "created_at": created}
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/users") {
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"tickets": []map[string]any{
			ticket("after-end", at(5*24*time.Hour)),
			ticket("at-end", at(0)),
			ticket("cur-in", at(-15*24*time.Hour)),
			ticket("cur-edge-start", start.Format(time.RFC3339)),
			ticket("prior-edge", start.Add(-time.Second).Format(time.RFC3339)),
			ticket("prior-in", start.Add(-21*24*time.Hour).Format(time.RFC3339)),
			ticket("unparseable", "not-a-date"),
		}}})
	}))
	defer upstream.Close()
	opsMgmtBaseURL = upstream.URL

	// The outage-list scan fails on this conn, which is itself reported; the
	// assertions here are about the ticket counts only.
	a := failingAPI()
	resp := a.FetchNetworkHealthTicketsData(context.Background(), start, end, "")

	require.NotNil(t, resp.OpsTickets)
	assert.Equal(t, 2, resp.OpsTickets.Total, "only cur-in and cur-edge-start are inside [start, end)")
	require.NotNil(t, resp.Prev)
	assert.Equal(t, 2, resp.Prev.Total, "prior-in and prior-edge are inside [priorStart, start)")

	// A failed outage list zeroes the coverage figures, so the panel is named. It
	// must not error the whole group: the ticket counts beside it are correct and
	// the maintenance panel reads none of the outage list.
	assert.Contains(t, resp.Degraded, "outage_list")
	assert.Empty(t, resp.Error, "one failed sub-query must not blank the whole group")
}

// TestFetchNetworkHealthTicketsDataUndatedSurfaced pins that dropped tickets are
// reported. A ticket whose created_at does not parse is counted in no window, so
// a silent drop publishes an undercount as a fact; a whole fetch of them (an
// upstream format change) must report unavailable so the worker keeps the last
// good blob instead of caching zeros.
func TestFetchNetworkHealthTicketsDataUndatedSurfaced(t *testing.T) {
	savedURL := opsMgmtBaseURL
	defer func() { opsMgmtBaseURL = savedURL }()
	t.Setenv("OPS_MANAGEMENT_API_KEY", "test-key")

	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -29)

	serve := func(tickets []map[string]any) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.HasPrefix(r.URL.Path, "/users") {
				_, _ = w.Write([]byte(`{"data":[]}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"tickets": tickets}})
		}))
	}
	ticket := func(id, created string) map[string]any {
		return map[string]any{"id": id, "type": "incident", "status": "open", "created_at": created}
	}
	dated := ticket("dated", end.Add(-10*24*time.Hour).Format(time.RFC3339))
	// Date-only: newer than the fetch's created_at cutoff, so it survives the
	// pagination filter, but neither accepted timestamp format parses it.
	undated := func(id string, d time.Duration) map[string]any {
		return ticket(id, end.Add(d).Format("2006-01-02"))
	}

	t.Run("a material share degrades the panel", func(t *testing.T) {
		upstream := serve([]map[string]any{
			dated,
			undated("bad1", -9*24*time.Hour),
			undated("bad2", -10*24*time.Hour),
			undated("bad3", -11*24*time.Hour),
		})
		defer upstream.Close()
		opsMgmtBaseURL = upstream.URL

		resp := failingAPI().FetchNetworkHealthTicketsData(context.Background(), start, end, "")

		assert.Contains(t, resp.Degraded, "ops_tickets")
		require.NotNil(t, resp.OpsTickets)
		assert.Equal(t, 1, resp.OpsTickets.Total, "the dated ticket still counts")
	})

	// Both gates run after the contributor filter, so on a scoped view the
	// denominator is that contributor's ticket count, not the network stream the
	// 5% share was calibrated against. Below the absolute floor a single malformed
	// row must not blank a dozen correct tickets.
	t.Run("one bad row in a small fetch degrades nothing", func(t *testing.T) {
		small := []map[string]any{undated("bad", -9*24*time.Hour)}
		for i := range 12 {
			small = append(small, ticket("ok"+strconv.Itoa(i),
				end.Add(-time.Duration(i+1)*24*time.Hour).Format(time.RFC3339)))
		}
		upstream := serve(small)
		defer upstream.Close()
		opsMgmtBaseURL = upstream.URL

		resp := failingAPI().FetchNetworkHealthTicketsData(context.Background(), start, end, "")

		assert.NotContains(t, resp.Degraded, "ops_tickets",
			"1 of 13 is above the 5% share but below the absolute floor")
		assert.Empty(t, resp.Error)
		require.NotNil(t, resp.OpsTickets)
		assert.Equal(t, 12, resp.OpsTickets.Total, "the dated tickets are still published")
	})

	t.Run("a whole undated fetch reports unavailable", func(t *testing.T) {
		upstream := serve([]map[string]any{
			undated("bad1", -9*24*time.Hour), undated("bad2", -10*24*time.Hour),
			undated("bad3", -11*24*time.Hour),
		})
		defer upstream.Close()
		opsMgmtBaseURL = upstream.URL

		resp := failingAPI().FetchNetworkHealthTicketsData(context.Background(), start, end, "")

		assert.Contains(t, resp.Degraded, "ops_tickets")
		assert.NotEmpty(t, resp.Error, "a total upstream break must not publish as a quiet zero")
		assert.Nil(t, resp.OpsTickets)
	})
}

// TestNHDegradedIsNilOnSuccess pins that a healthy payload never names a panel,
// so the frontend's unavailable state cannot fire on a good window. The ops fetch
// is skipped (no API key), which is a deliberate silent empty, not a failure.
func TestNHDegradedIsNilOnSuccess(t *testing.T) {
	t.Setenv("OPS_MANAGEMENT_API_KEY", "")
	end := time.Now().UTC().Truncate(24 * time.Hour)
	resp := (&API{}).FetchNetworkHealthTicketsData(context.Background(), end.AddDate(0, 0, -30), end, "")
	assert.Nil(t, resp.Degraded)
	assert.Empty(t, resp.Error)
	assert.Nil(t, resp.OpsTickets)
}
