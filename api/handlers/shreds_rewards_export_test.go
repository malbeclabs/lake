package handlers

import "context"

// ExportComputeShredsRewards exposes computeShredsRewards to the external test
// package, so a cached page can be compared against the live ClickHouse query it
// is meant to reproduce (TestGetShredsRewards_CachedPageMatchesLive). Going
// through the handler instead would just read the cache back.
func (a *API) ExportComputeShredsRewards(ctx context.Context, search, sortField, order string, limit, offset int) (*ShredsRewardsResponse, error) {
	return a.computeShredsRewards(ctx, search, sortField, order, limit, offset)
}
