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
			a.serveNHGroup(rw, req,
				"k", func(string) string { return "ck" },
				fetch, nhGroupDeadline)
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
