package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/malbeclabs/doublezero/tools/solana/pkg/rpc"
)

// TestLedgerClientPoolMatchesFanOut pins the relationship between the RPC connection
// pool and the concurrency that shares the client.
//
// The SDK's per-request timeout bounds total wall time including time queued waiting
// for a free connection, not just the server's service time. So a pool smaller than
// the fan-out multiplies each request's wall time by the number of waves: with the
// SDK's default pool of 9 and this repo's default concurrency of 64, a request's wall
// time is roughly 7x service time. Under the SDK's old 5-minute default that was
// invisible; at 10s a merely slow backend produces terminal timeouts — and a timed-out
// request is not retried, because isRetryableJSONRPC rejects context errors before any
// other check.
//
// The backend delay here is deliberately well inside the 10s budget on its own but far
// outside it once multiplied by an unsized pool.
func TestLedgerClientPoolMatchesFanOut(t *testing.T) {
	t.Parallel()

	const (
		fanOut       = defaultMaxConcurrency
		serviceTime  = 150 * time.Millisecond
		perReqBudget = 10 * time.Second // the SDK default this test is defending
	)

	var inFlight, peak int64
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		time.Sleep(serviceTime)
		mu.Lock()
		inFlight--
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","result":"ok","id":1}`)
	}))
	defer srv.Close()

	// The real constructor, configured the way main.go configures it.
	client := rpc.New(srv.URL, rpc.Options{MaxConnsPerHost: fanOut})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	errs := make(chan error, fanOut)
	var wg sync.WaitGroup
	start := time.Now()
	for range fanOut {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.GetHealth(ctx)
			errs <- err
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)

	var timeouts, others int
	for err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, context.DeadlineExceeded):
			timeouts++
		default:
			others++
		}
	}

	if timeouts != 0 {
		t.Errorf("%d/%d requests timed out at %s service time — the pool is not sized for the fan-out",
			timeouts, fanOut, serviceTime)
	}
	if others != 0 {
		t.Errorf("%d/%d requests failed for a non-timeout reason", others, fanOut)
	}
	// The discriminating assertion. Timeouts alone do not distinguish a sized pool
	// from an unsized one at a short service time — 64 requests through 9 connections
	// is ~8 waves, which at 150ms still lands inside the 10s budget. What always
	// differs is how many requests reach the server at once: a sized pool puts the
	// whole fan-out in flight, an unsized one caps it at the pool size. Asserting that
	// is fast and exact, where asserting timeouts needs a service time slow enough to
	// blow the budget (>1.25s) and so a test that runs for over ten seconds.
	if peak < int64(fanOut) {
		t.Errorf("peak server concurrency %d < fan-out %d: requests queued behind the connection pool, "+
			"so each one's wall time is a multiple of service time and counts against the same %s budget",
			peak, fanOut, perReqBudget)
	}
	t.Logf("fanOut=%d serviceTime=%s elapsed=%s peakServerConcurrency=%d", fanOut, serviceTime, elapsed, peak)
}
