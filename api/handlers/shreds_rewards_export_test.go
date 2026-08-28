package handlers

import "context"

// ExportComputeShredsRewards exposes computeShredsRewards to the external test
// package, so a cached page can be compared against the live ClickHouse query it
// is meant to reproduce (TestGetShredsRewards_CachedPageMatchesLive). Going
// through the handler instead would just read the cache back.
func (a *API) ExportComputeShredsRewards(ctx context.Context, search, sortField, order string, limit, offset int) (*ShredsRewardsResponse, error) {
	return a.computeShredsRewards(ctx, search, sortField, order, limit, offset)
}

// ExportSliceCachedShredsRewards exposes sliceCachedShredsRewards, so the Go
// predicate that answers a search from the cache can be compared against the SQL
// WHERE it reproduces (TestGetShredsRewards_CachedSearchMatchesLive) without
// going near the page_cache table.
//
// That matters beyond tidiness: seedShredsRewardsCache holds a mutex until the
// test ends, so a test that seeded twice to compare both paths would deadlock
// on its second call.
func ExportSliceCachedShredsRewards(data []byte, search, sortField, order string, limit, offset int) (*ShredsRewardsResponse, bool) {
	return sliceCachedShredsRewards(data, search, sortField, order, limit, offset)
}
