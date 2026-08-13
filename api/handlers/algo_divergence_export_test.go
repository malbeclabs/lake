package handlers

import "context"

// ExportExcludedLinks exposes excludedLinks to the external test package, so
// its SQL and its column scans run against a real ClickHouse.
func (a *API) ExportExcludedLinks(ctx context.Context) ([]ExcludedLink, int, error) {
	return a.excludedLinks(ctx)
}
