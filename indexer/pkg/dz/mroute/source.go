package mroute

import "context"

// Source provides access to per-device mroute snapshot dumps written by
// doublezero-telemetry's state-collect uploader to S3.
type Source interface {
	// FetchLatest returns the most recent snapshot for every device that has
	// uploaded one within the source's lookback window. Devices with no
	// recent snapshots are silently omitted.
	FetchLatest(ctx context.Context) ([]*Dump, error)

	// Close releases any resources held by the source.
	Close() error
}
