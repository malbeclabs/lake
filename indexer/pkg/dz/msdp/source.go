package msdp

import "context"

// Source provides access to per-device MSDP snapshot dumps written by
// doublezero-telemetry's state-collect uploader. One Source handles all
// three MSDP kinds (summary, pim sa-cache, sa-cache rejected) and
// returns them keyed by kind.
type Source interface {
	// FetchLatest returns the most recent snapshot per (kind, device)
	// within the source's lookback window. The outer map key is the
	// snapshot kind; the inner slice has one entry per device that
	// uploaded that kind. Devices with no recent snapshot for a given
	// kind are silently omitted from that kind's slice.
	FetchLatest(ctx context.Context) (map[string][]*Dump, error)

	// Close releases any resources held by the source.
	Close() error
}
