package handlers

import (
	"context"
	"time"
)

// ExportLinkMeasuredMap exposes linkMeasuredMap to the external test package, so
// its SQL and its column scans run against a real ClickHouse.
func (a *API) ExportLinkMeasuredMap(ctx context.Context, window time.Duration) (map[string]ExportLinkMeasured, error) {
	return a.linkMeasuredMap(ctx, window)
}

type ExportLinkMeasured = linkMeasured
