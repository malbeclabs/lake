package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingOpsServer stands up an ops-management API double that counts the
// requests it receives, points opsMgmtBaseURL at it for the test, and sets an
// API key so the fetchers actually run. handle serves both /tickets and /users.
func countingOpsServer(t *testing.T, handle http.HandlerFunc) (tickets, users *atomic.Int32) {
	t.Helper()
	tickets, users = &atomic.Int32{}, &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/users") {
			users.Add(1)
		} else {
			tickets.Add(1)
		}
		handle(w, r)
	}))
	t.Cleanup(srv.Close)

	saved := opsMgmtBaseURL
	opsMgmtBaseURL = srv.URL
	t.Cleanup(func() { opsMgmtBaseURL = saved })
	t.Setenv("OPS_MANAGEMENT_API_KEY", "test-key")
	return tickets, users
}

// emptyOpsHandler answers both ops endpoints with an empty result set.
func emptyOpsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if strings.HasPrefix(r.URL.Path, "/users") {
		_, _ = w.Write([]byte(`{"data":[]}`))
		return
	}
	_, _ = w.Write([]byte(`{"data":{"tickets":[]}}`))
}

// TestNHTicketsLookbackFloor pins the bound on the ops pagination. The ops API
// is newest-first with no date filter, so a window positioned far in the past
// makes the loop walk the whole stream (up to maxPages credentialed GETs) on an
// unauthenticated public endpoint. Past the floor the group reports unavailable
// instead, and a window that the page can genuinely request still fetches.
func TestNHTicketsLookbackFloor(t *testing.T) {
	t.Run("ancient window makes no upstream calls", func(t *testing.T) {
		tickets, users := countingOpsServer(t, emptyOpsHandler)

		resp := (&API{}).FetchNetworkHealthTicketsData(context.Background(),
			mustTime("1970-01-01T00:00:00Z"), mustTime("1970-04-01T00:00:00Z"), "")

		assert.Zero(t, tickets.Load(), "an out-of-range window must not page the ops API")
		assert.Zero(t, users.Load(), "an out-of-range window must not fetch the users registry")
		assert.NotEmpty(t, resp.Error, "the window must be reported unavailable, not silently truncated")
		assert.Contains(t, resp.Degraded, "ops_tickets")
		assert.Nil(t, resp.OpsTickets)
	})

	t.Run("widest reachable window still fetches", func(t *testing.T) {
		tickets, _ := countingOpsServer(t, emptyOpsHandler)

		// The widest window the page can ask for: networkHealthMaxDays ending now,
		// whose prior window doubles the span the fetch covers.
		end := time.Now().UTC().Truncate(24 * time.Hour)
		start := end.AddDate(0, 0, -networkHealthMaxDays)
		resp := failingAPI().FetchNetworkHealthTicketsData(context.Background(), start, end, "")

		assert.NotZero(t, tickets.Load(), "the floor must not refuse a window the page can request")
		assert.NotContains(t, resp.Degraded, "ops_tickets")
	})
}

// TestNHTicketsUnknownContributorSkipsOpsFetch pins that the contributor scope is
// resolved BEFORE the ops pagination. An unknown ?contributor= used to pay the
// full fetch (up to 100 sequential credentialed GETs) and only then fail on the
// scope lookup.
func TestNHTicketsUnknownContributorSkipsOpsFetch(t *testing.T) {
	tickets, users := countingOpsServer(t, emptyOpsHandler)

	a := &API{DB: &mockContributorScopeConn{rowErr: errors.New("no rows in result set")}}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	resp := a.FetchNetworkHealthTicketsData(context.Background(), end.AddDate(0, 0, -30), end, "nope")

	assert.Zero(t, tickets.Load(), "an unknown contributor must not page the ops API")
	assert.Zero(t, users.Load(), "an unknown contributor must not fetch the users registry")
	assert.Contains(t, resp.Degraded, "contributor_scope")
	assert.NotEmpty(t, resp.Error)
}

// TestNHTicketsKnownContributorStillFetches guards the reordering: moving the
// scope lookup ahead of the fetch must not stop a valid scoped request from
// running it.
func TestNHTicketsKnownContributorStillFetches(t *testing.T) {
	tickets, _ := countingOpsServer(t, emptyOpsHandler)

	a := &API{DB: &mockContributorScopeConn{pk: "pk-acme", name: "Acme", linkCodes: []string{"ams-fra"}}}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	resp := a.FetchNetworkHealthTicketsData(context.Background(), end.AddDate(0, 0, -30), end, "acme")

	assert.NotZero(t, tickets.Load(), "a known contributor must still page the ops API")
	assert.NotContains(t, resp.Degraded, "contributor_scope")
}

// TestCachedOpsUsers covers the /users memo: the registry is up to 20 paged
// credentialed requests and was re-paged on every tickets computation (every
// worker refresh cycle plus every live request).
func TestCachedOpsUsers(t *testing.T) {
	t.Run("memoizes within the TTL", func(t *testing.T) {
		_, users := countingOpsServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"pubkey":"u1","contributor_pubkey":"c1"}]}`))
		})

		a := &API{}
		first, err := a.cachedOpsUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"u1": "c1"}, first)

		second, err := a.cachedOpsUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, first, second)
		assert.Equal(t, int32(1), users.Load(), "the second call within the TTL must not re-page /users")
	})

	t.Run("does not cache an unavailable registry", func(t *testing.T) {
		var fail atomic.Bool
		fail.Store(true)
		_, users := countingOpsServer(t, func(w http.ResponseWriter, r *http.Request) {
			if fail.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"pubkey":"u1","contributor_pubkey":null}]}`))
		})

		a := &API{}
		if _, err := a.cachedOpsUsers(context.Background()); err == nil {
			t.Fatal("expected the first-page failure to be returned")
		}
		fail.Store(false)
		got, err := a.cachedOpsUsers(context.Background())
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"u1": ""}, got, "a failed fetch must not pin an empty registry")
		assert.Equal(t, int32(2), users.Load())
	})

	t.Run("collapses concurrent misses", func(t *testing.T) {
		_, users := countingOpsServer(t, func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(50 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"pubkey":"u1","contributor_pubkey":"c1"}]}`))
		})

		a := &API{}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := a.cachedOpsUsers(context.Background())
				assert.NoError(t, err)
				assert.Equal(t, "c1", got["u1"])
			}()
		}
		wg.Wait()
		assert.Equal(t, int32(1), users.Load(), "concurrent misses must collapse into one pagination")
	})

	t.Run("no api key returns an empty registry", func(t *testing.T) {
		t.Setenv("OPS_MANAGEMENT_API_KEY", "")
		got, err := (&API{}).cachedOpsUsers(context.Background())
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// TestNHTicketsUsesCachedOpsUsers pins that the tickets aggregate goes through
// the memo, so two computations in one TTL cost one registry pagination.
func TestNHTicketsUsesCachedOpsUsers(t *testing.T) {
	_, users := countingOpsServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/users") {
			_, _ = w.Write([]byte(`{"data":[{"pubkey":"u1","contributor_pubkey":"c1"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"tickets":[]}}`))
	})

	a := failingAPI()
	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -30)
	a.FetchNetworkHealthTicketsData(context.Background(), start, end, "")
	a.FetchNetworkHealthTicketsData(context.Background(), start, end, "")

	assert.Equal(t, int32(1), users.Load(), "the second computation must reuse the memoized registry")
}

// TestServeNHGroupScopedComputesLive pins the contributor contract after the
// per-contributor cache surface was removed: a scoped request always computes
// live and reports X-Cache MISS, on mainnet as well as testnet. Nothing writes a
// per-contributor blob (see api/worker/workflow.go), so the read that used to
// run here always missed. That the read is gone is compiler-enforced (no
// per-contributor key builder exists); this pins the behaviour it fed.
func TestServeNHGroupScopedComputesLive(t *testing.T) {
	for _, env := range []DZEnv{EnvMainnet, EnvTestnet} {
		t.Run(string(env), func(t *testing.T) {
			var ran atomic.Bool
			fetch := func(ctx context.Context, start, end time.Time, contrib string) any {
				ran.Store(true)
				assert.Equal(t, "dgt", contrib)
				return map[string]any{"ok": true}
			}

			a := &API{} // nil PgPool: no cache read may be attempted
			req := httptest.NewRequest(http.MethodGet, "/api/network-health/capacity?contributor=dgt&days=30", nil)
			req = req.WithContext(ContextWithEnv(req.Context(), env))
			rw := httptest.NewRecorder()
			a.serveNHGroup(rw, req, "k", fetch, nhGroupDeadline)

			assert.Equal(t, http.StatusOK, rw.Code)
			assert.Equal(t, "MISS", rw.Header().Get("X-Cache"))
			assert.Equal(t, "application/json", rw.Header().Get("Content-Type"))
			assert.True(t, ran.Load(), "a scoped request must compute live")

			var body map[string]any
			require.NoError(t, json.Unmarshal(rw.Body.Bytes(), &body))
			assert.Equal(t, true, body["ok"])
		})
	}
}
