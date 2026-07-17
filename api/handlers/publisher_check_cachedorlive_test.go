package handlers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFetchPublisherCheckCachedOrLive_LiveDeadline verifies the live fallback
// runs under a bounded deadline. With no PgPool the cache read fails, so both a
// default-shape and a non-default-shape request fall through to the live fetch —
// and the ctx handed to that fetch must carry a deadline (publisherCheckLiveTimeout).
func TestFetchPublisherCheckCachedOrLive_LiveDeadline(t *testing.T) {
	a := &API{} // nil PgPool → readPageCache errors → live path

	cases := []struct {
		name          string
		q             string
		epochs, slots int
	}{
		{"default shape, cache miss", "", DefaultPublisherCheckEpochs, 0},
		{"non-default shape (filtered)", "dzuser1", DefaultPublisherCheckEpochs, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotDeadline bool
			var remaining time.Duration
			spy := func(ctx context.Context, _ string, _, _ int) (*PublisherCheckResponse, error) {
				dl, ok := ctx.Deadline()
				gotDeadline = ok
				remaining = time.Until(dl)
				return &PublisherCheckResponse{}, nil
			}
			_, fromCache, err := a.fetchPublisherCheckCachedOrLive(context.Background(), tc.q, tc.epochs, tc.slots, spy)
			require.NoError(t, err)
			require.False(t, fromCache, "no cache configured, so the result must come from the live path")
			require.True(t, gotDeadline, "live fetch must run under a deadline")
			// Deadline is the bounded live budget, not something larger.
			require.Positive(t, remaining)
			require.LessOrEqual(t, remaining, publisherCheckLiveTimeout,
				"live deadline must be within publisherCheckLiveTimeout")
		})
	}
}

// TestFetchPublisherCheckLive_ConcurrencyCap verifies that live runs are bounded
// by maxConcurrentPublisherCheckLive. Distinct (filtered) shapes bypass
// singleflight, so each is its own live run; the semaphore must cap how many
// execute at once.
func TestFetchPublisherCheckLive_ConcurrencyCap(t *testing.T) {
	a := &API{}

	var inflight, maxInflight int64
	release := make(chan struct{})
	var started sync.WaitGroup

	spy := func(ctx context.Context, _ string, _, _ int) (*PublisherCheckResponse, error) {
		cur := atomic.AddInt64(&inflight, 1)
		for {
			old := atomic.LoadInt64(&maxInflight)
			if cur <= old || atomic.CompareAndSwapInt64(&maxInflight, old, cur) {
				break
			}
		}
		started.Done()
		<-release // block so concurrent runs pile up against the semaphore
		atomic.AddInt64(&inflight, -1)
		return &PublisherCheckResponse{}, nil
	}

	const n = maxConcurrentPublisherCheckLive + 3
	started.Add(maxConcurrentPublisherCheckLive) // only the cap can start while blocked
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Distinct q per goroutine → distinct singleflight keys → no collapse.
			q := string(rune('a' + i))
			_, _ = a.fetchPublisherCheckLive(context.Background(), q, DefaultPublisherCheckEpochs, 1, spy)
		}()
	}

	started.Wait() // exactly the cap have entered the spy
	require.Equal(t, int64(maxConcurrentPublisherCheckLive), atomic.LoadInt64(&inflight),
		"only maxConcurrentPublisherCheckLive runs may execute at once")
	close(release)
	wg.Wait()
	require.LessOrEqual(t, atomic.LoadInt64(&maxInflight), int64(maxConcurrentPublisherCheckLive))
}

// TestFetchPublisherCheckLive_TransientRetry verifies a transient failure is
// retried once and the retry result is returned.
func TestFetchPublisherCheckLive_TransientRetry(t *testing.T) {
	a := &API{}

	var calls int64
	spy := func(ctx context.Context, _ string, _, _ int) (*PublisherCheckResponse, error) {
		if atomic.AddInt64(&calls, 1) == 1 {
			return nil, errors.New("connection refused") // classified transient by dberror
		}
		return &PublisherCheckResponse{Epoch: 7}, nil
	}

	// Filtered shape so singleflight doesn't interfere.
	resp, err := a.fetchPublisherCheckLive(context.Background(), "dzuser1", DefaultPublisherCheckEpochs, 0, spy)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(7), resp.Epoch)
	require.Equal(t, int64(2), atomic.LoadInt64(&calls), "transient failure must be retried once")
}
