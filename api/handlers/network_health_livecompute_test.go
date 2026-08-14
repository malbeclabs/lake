package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestServeNHGroupQueuesUnderConcurrentScopedLoad guards the fix for the
// reported bug: a contributor-scoped Network Health page load fires ~9 group
// requests at once, and every one bypasses cache (scoped is never precomputed)
// and competes for the 4-slot nhLiveComputeSem. The OLD behavior shed any
// request that couldn't grab a slot within a fixed 2s, so ~5 of 9 panels 503'd
// and the UI showed "Couldn't load this section".
//
// With "queue, don't drop", a waiter blocks for a free slot up to its own
// deadline instead of shedding after 2s. Slots free as the running queries
// finish, so every panel is eventually served. Only a request whose deadline
// expires while waiting (genuine sustained overload) or whose client
// disconnected is shed.
//
// The stub fetch holds its slot for 3s (longer than the old 2s shed window),
// so under the old code the 5 waiters would 503; under the fix they queue and
// all complete well inside the 35s deadline.
func TestServeNHGroupQueuesUnderConcurrentScopedLoad(t *testing.T) {
	a := &API{}

	fetch := func(ctx context.Context, start, end time.Time, contrib string) any {
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
		}
		return map[string]any{}
	}

	const n = 9 // overview, availability, latency, capacity, outages, drain, tickets, impactful, deferred
	var got503, got200 int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/api/network-health/capacity?contributor=dgt&days=30", nil)
			// testnet env => isMainnet false => scoped path skips the cache read
			// (no DB needed) and goes straight to the live-compute semaphore.
			req = req.WithContext(ContextWithEnv(req.Context(), EnvTestnet))
			rw := httptest.NewRecorder()
			a.serveNHGroup(rw, req, "k", fetch, nhGroupDeadline)
			switch rw.Code {
			case http.StatusServiceUnavailable:
				atomic.AddInt32(&got503, 1)
			case http.StatusOK:
				atomic.AddInt32(&got200, 1)
			}
		}()
	}
	wg.Wait()

	t.Logf("of %d concurrent scoped requests: %d served (200), %d shed (503)", n, got200, got503)
	if got503 != 0 {
		t.Fatalf("expected all %d concurrent scoped requests to queue and be served, but %d were shed with 503", n, got503)
	}
	if got200 != n {
		t.Fatalf("expected %d served (200), got %d", n, got200)
	}
}

// TestServeNHGroupShedsWithoutRunningQuery pins the shed contract every group
// endpoint inherits from serveNHGroup: a request whose deadline expires while
// queued is answered 503 and its query never starts, so a saturated pool is
// never driven deeper by the requests waiting on it.
func TestServeNHGroupShedsWithoutRunningQuery(t *testing.T) {
	// Fill every slot so the request must queue.
	for i := 0; i < cap(nhLiveComputeSem); i++ {
		nhLiveComputeSem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(nhLiveComputeSem); i++ {
			<-nhLiveComputeSem
		}
	}()

	var ran atomic.Bool
	fetch := func(ctx context.Context, start, end time.Time, contrib string) any {
		ran.Store(true)
		return map[string]any{}
	}

	a := &API{}
	// testnet env => isMainnet false => no cache read, so a nil PgPool is never
	// touched and the request goes straight to the live-compute semaphore.
	req := httptest.NewRequest(http.MethodGet, "/api/network-health/capacity?days=29", nil)
	req = req.WithContext(ContextWithEnv(req.Context(), EnvTestnet))
	rw := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		a.serveNHGroup(rw, req, "k", fetch, 100*time.Millisecond)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("queued request parked past its deadline instead of shedding")
	}

	if rw.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for a queued request past its deadline, got %d", rw.Code)
	}
	if ran.Load() {
		t.Fatal("a shed request must not launch its query")
	}
}

// TestNetworkHealthDeferredBoundsQueueWait is the regression guard for the
// /deferred handler acquiring a live-compute slot on the raw request context.
// The server sets WriteTimeout 0 and there is no request-timeout middleware, so
// a request whose context carries no deadline used to park a goroutine on the
// semaphore for as long as the client held the connection open: with the 4 slots
// saturated, queued requests accumulated at the rate they arrived and every
// freed slot was immediately re-taken.
//
// With the deadline built before the acquire (matching serveNHGroup), the wait
// is bounded by the group deadline and an over-budget request is shed with 503
// without launching a query. Both branches were separately wrong, so both are
// covered.
func TestNetworkHealthDeferredBoundsQueueWait(t *testing.T) {
	// A short stand-in for nhHeavyGroupDeadline; the real 170s is the same code
	// path, just slower to observe.
	const deadline = 150 * time.Millisecond

	for _, tc := range []struct {
		name   string
		target string
	}{
		{"network-wide", "/api/network-health/deferred?days=29"},
		{"contributor-scoped", "/api/network-health/deferred?days=29&contributor=dgt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Fill every slot so the request must queue.
			for i := 0; i < cap(nhLiveComputeSem); i++ {
				nhLiveComputeSem <- struct{}{}
			}
			defer func() {
				for i := 0; i < cap(nhLiveComputeSem); i++ {
					<-nhLiveComputeSem
				}
			}()

			// No deadline on the request context: the client is still connected
			// and waiting, which is the case that used to park forever. Cancelled
			// on the way out so a regression unblocks rather than leaking.
			reqCtx, cancel := context.WithCancel(ContextWithEnv(context.Background(), EnvTestnet))
			defer cancel()

			a := &API{}
			req := httptest.NewRequest(http.MethodGet, tc.target, nil).WithContext(reqCtx)
			rw := httptest.NewRecorder()

			done := make(chan struct{})
			go func() {
				defer close(done)
				// testnet env => isMainnet false => no cache read, so a nil
				// PgPool is never touched; the request goes straight to the
				// live-compute semaphore.
				a.serveNHDeferred(rw, req, deadline)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("queued /deferred request parked past its %s deadline instead of shedding", deadline)
			}

			if rw.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503 for a queued request past its deadline, got %d", rw.Code)
			}
			if got := rw.Header().Get("Retry-After"); got != "5" {
				t.Fatalf("expected Retry-After 5, got %q", got)
			}
			if got := rw.Header().Get("X-Cache"); got != "MISS" {
				t.Fatalf("expected X-Cache MISS, got %q", got)
			}
		})
	}
}

// TestNetworkHealthDeferredUsesHeavyDeadline pins that /deferred and /impactful
// share one budget: the handler is a thin wrapper so the deadline cannot drift
// back to a hand-copied literal.
func TestNetworkHealthDeferredUsesHeavyDeadline(t *testing.T) {
	if nhHeavyGroupDeadline <= nhGroupDeadline {
		t.Fatalf("heavy group deadline %s must exceed the fast group deadline %s", nhHeavyGroupDeadline, nhGroupDeadline)
	}
	if nhHeavyGroupDeadline != 170*time.Second {
		t.Fatalf("heavy group deadline %s must match networkHealthDeferredQuerySettings' max_execution_time of 170s", nhHeavyGroupDeadline)
	}
}

// TestNHAcquireLiveShedsWhenClientDisconnects verifies the queue still bails out
// promptly (no hang, no query launched) when the caller's context is already
// cancelled, e.g. the browser navigated away while all slots were busy.
func TestNHAcquireLiveShedsWhenClientDisconnects(t *testing.T) {
	// Fill every slot so the next acquire must wait.
	for i := 0; i < cap(nhLiveComputeSem); i++ {
		nhLiveComputeSem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(nhLiveComputeSem); i++ {
			<-nhLiveComputeSem
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // client already gone

	release, ok := nhAcquireLive(ctx)
	if ok {
		release()
		t.Fatal("expected nhAcquireLive to fail when context is cancelled and all slots are busy")
	}
}
