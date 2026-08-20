package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/solana"
	"github.com/stretchr/testify/require"
)

// supplyBackend serves getSupply, counting calls and optionally failing or stalling.
func supplyBackend(t *testing.T, calls *atomic.Int64, delay time.Duration, fail *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		if fail != nil && fail.Load() {
			http.Error(w, "upstream is unhappy", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},`+
			`"value":{"total":600000000000000000,"circulating":500000000000000000,`+
			`"nonCirculating":100000000000000000,"nonCirculatingAccounts":[]}}}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func resetSupplyCache() {
	supplyCache.mu.Lock()
	defer supplyCache.mu.Unlock()
	supplyCache.val = nil
	supplyCache.fetchedAt = time.Time{}
}

// TestCachedSupply_ReusesWithinTTL is the reason this cache exists. getSupply costs
// ~6.4s against production while every other call in FetchLedgerData costs ~45ms, and
// it reports numbers that move once per epoch. Refetching it on every page-cache
// refresh spent 150x the cost of the other five calls combined to learn nothing.
func TestCachedSupply_ReusesWithinTTL(t *testing.T) {
	resetSupplyCache()

	var calls atomic.Int64
	srv := supplyBackend(t, &calls, 0, nil)
	client := solana.NewClient(srv.URL)
	ctx := context.Background()

	first := cachedSupply(ctx, client)
	require.NotNil(t, first)
	require.EqualValues(t, 1, calls.Load())

	for range 20 {
		got := cachedSupply(ctx, client)
		require.NotNil(t, got)
		require.Equal(t, first.Value.Total, got.Value.Total)
	}
	require.EqualValues(t, 1, calls.Load(),
		"getSupply must be fetched once per TTL, not once per refresh")
}

// TestCachedSupply_FailureKeepsLastGoodValue: a failed refresh must not drop the
// field. Staleness past the TTL is a much smaller problem than reporting nothing.
func TestCachedSupply_FailureKeepsLastGoodValue(t *testing.T) {
	resetSupplyCache()

	var calls atomic.Int64
	var fail atomic.Bool
	srv := supplyBackend(t, &calls, 0, &fail)
	client := solana.NewClient(srv.URL)
	ctx := context.Background()

	good := cachedSupply(ctx, client)
	require.NotNil(t, good)

	// Expire the entry, then make the upstream fail.
	supplyCache.mu.Lock()
	supplyCache.fetchedAt = time.Now().Add(-2 * supplyTTL)
	supplyCache.mu.Unlock()
	fail.Store(true)

	got := cachedSupply(ctx, client)
	require.NotNil(t, got, "a failed refresh must keep serving the last good value")
	require.Equal(t, good.Value.Total, got.Value.Total)
}

// TestCachedSupply_NeverFails: with no successful fetch ever, it returns nil rather
// than an error, so the caller can still report every other ledger field.
func TestCachedSupply_NeverFails(t *testing.T) {
	resetSupplyCache()

	var calls atomic.Int64
	fail := &atomic.Bool{}
	fail.Store(true)
	srv := supplyBackend(t, &calls, 0, fail)

	require.Nil(t, cachedSupply(context.Background(), solana.NewClient(srv.URL)),
		"an unfetched supply is nil, not an error")
}

// TestFetchLedgerData_SlowSupplyDoesNotSinkTheResponse is the regression.
//
// The six calls run concurrently under errgroup.WithContext, so the first error
// cancels the rest. getSupply is the only slow one — five calls finish in ~45ms and
// then wait on it. When it exceeded the client timeout it cancelled those five
// finished results and the whole ledger response failed, which took the solana ledger
// page cache down for a day.
func TestFetchLedgerData_SlowSupplyDoesNotSinkTheResponse(t *testing.T) {
	resetSupplyCache()

	var supplyCalls atomic.Int64
	slowSupply := &atomic.Bool{}
	slowSupply.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		if req.Method == "getSupply" {
			supplyCalls.Add(1)
			// Outlast the client's own timeout, as production does.
			time.Sleep(300 * time.Millisecond)
			http.Error(w, "too slow", http.StatusGatewayTimeout)
			return
		}
		// Everything else answers immediately, as production does.
		switch req.Method {
		case "getEpochInfo":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"epoch":700,"slotIndex":100,`+
				`"slotsInEpoch":432000,"absoluteSlot":300000000,"blockHeight":280000000,"transactionCount":9}}`)
		case "getRecentPerformanceSamples":
			// Ten samples, so the span clears minSampleSpanSec and the slot duration is
			// measured rather than falling back: 600s / 1500 slots = 0.4s.
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":[`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":10},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":9},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":8},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":7},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":6},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":5},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":4},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":3},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":2},`+
				`{"numTransactions":6000,"samplePeriodSecs":60,"numSlots":150,"slot":1}]}`)
		case "getInflationRate":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"total":0.05,"validator":0.045,"foundation":0.005,"epoch":700}}`)
		case "getVersion":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"solana-core":"2.0.0"}}`)
		case "getVoteAccounts":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"current":[],"delinquent":[]}}`)
		default:
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
		}
	}))
	defer srv.Close()

	got, err := FetchLedgerData(context.Background(), srv.URL, SolanaFallbackSlotDurationSec)
	require.NoError(t, err,
		"a failing getSupply must not fail the whole response; before this it cancelled "+
			"five already-finished sibling calls through errgroup.WithContext")
	require.NotNil(t, got)

	// The fields that did resolve are present and correct.
	require.EqualValues(t, 700, got.Epoch, "epoch came from a call that succeeded in ~45ms")
	require.EqualValues(t, 300000000, got.AbsoluteSlot)
	require.InDelta(t, 100.0, got.TPS, 0.01)
	require.InDelta(t, 0.4, got.SlotDurationSec, 0.0001, "600s over 1500 slots, measured not assumed")
	require.InDelta(t, float64(432000-100)*0.4, got.EpochETASec, 0.01)
	require.Equal(t, "2.0.0", got.NodeVersion)
	require.InDelta(t, 5.0, got.InflationTotal, 0.0001, "reported as a percentage")

	// Supply is the only casualty.
	require.Zero(t, got.TotalSupply, "supply is unknown, and that costs only the supply fields")
	require.Positive(t, supplyCalls.Load(), "getSupply was attempted")
}
