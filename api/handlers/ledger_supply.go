package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/malbeclabs/lake/api/solana"
)

// supplyTTL is how long a getSupply result is reused.
//
// Total and circulating supply move as inflation accrues, which happens per epoch,
// so roughly every two days. Refetching every page-cache refresh bought nothing and
// cost a great deal: measured against the production endpoint, getSupply takes ~6.4s
// while getEpochInfo, getInflationRate, getVersion and getVoteAccounts each take
// ~45ms. One call was 150x the cost of the other five combined and produced values
// that could not have changed.
//
// Five minutes is far shorter than the epoch it tracks, so the numbers stay current,
// and long enough that the expensive call runs on a small fraction of refreshes.
const supplyTTL = 5 * time.Minute

// supplyCache holds the last getSupply result. Per process rather than shared: it
// caches two slow-moving numbers, so a second API replica fetching its own copy costs
// one call per TTL and avoids any coordination.
var supplyCache struct {
	mu        sync.Mutex
	val       *solana.Supply
	fetchedAt time.Time
}

// cachedSupply returns the cached supply, refreshing it when older than supplyTTL.
//
// It never returns an error. A failed refresh keeps the previous value, and returns
// nil only when no fetch has ever succeeded. The caller treats nil as "supply
// unknown" and still reports every other field, which is the point: before this, a
// slow getSupply cancelled its sibling calls through errgroup.WithContext and took
// the whole ledger response down with it.
func cachedSupply(ctx context.Context, client *solana.Client) *solana.Supply {
	supplyCache.mu.Lock()
	defer supplyCache.mu.Unlock()

	if supplyCache.val != nil && time.Since(supplyCache.fetchedAt) < supplyTTL {
		return supplyCache.val
	}

	// Held across the call on purpose. Concurrent refreshes would each pay the ~6.4s
	// and hit the endpoint N times for the same numbers; the wait here is the same
	// wait they would have spent on their own request.
	val, err := client.GetSupply(ctx)
	if err != nil {
		// Keep serving the last good value. Staleness beyond the TTL is a far smaller
		// problem than dropping the field, and the refresh is retried next call.
		return supplyCache.val
	}

	supplyCache.val = val
	supplyCache.fetchedAt = time.Now()
	return val
}
