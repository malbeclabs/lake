package mroute

import (
	"context"
	"fmt"
)

// Sync parses per-device dumps into mroute Rows and replaces them in
// ClickHouse. Devices that fail to parse are skipped after logging — a single
// bad dump should not prevent the rest of the fleet from being written.
func (s *Store) Sync(ctx context.Context, dumps []*Dump) error {
	var rows []Row
	for _, d := range dumps {
		if d == nil {
			continue
		}
		entries, err := Parse(d.RawJSON)
		if err != nil {
			s.log.Warn("mroute/sync: failed to parse dump, skipping device",
				"device_pubkey", d.DevicePubkey,
				"file", d.FileName,
				"err", err,
			)
			continue
		}
		for _, e := range entries {
			rows = append(rows, EntryToRow(d.DevicePubkey, e))
		}
	}

	if err := s.ReplaceEntries(ctx, rows); err != nil {
		return fmt.Errorf("mroute: sync replace: %w", err)
	}

	s.log.Info("mroute: synced to ClickHouse",
		"dumps", len(dumps),
		"entries", len(rows),
	)
	return nil
}
