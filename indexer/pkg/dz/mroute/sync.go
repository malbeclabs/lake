package mroute

import (
	"context"
	"errors"
	"fmt"
)

// Sync parses per-device dumps into mroute Rows and replaces them in
// ClickHouse.
//
// On any parse failure, Sync returns the error and writes nothing.
// ReplaceEntries uses MissingMeansDeleted=true at the dataset layer, so
// silently dropping a device from the batch would tombstone that device's
// previously-current rows — not a recoverable "skip, try again next
// snapshot" semantic. Refusing to write preserves all prior state until
// the underlying parse issue is investigated.
func (s *Store) Sync(ctx context.Context, dumps []*Dump) error {
	rows := make([]Row, 0)
	var parseErrs []error
	for _, d := range dumps {
		if d == nil {
			continue
		}
		entries, err := Parse(d.RawJSON)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("%s (%s): %w", d.DevicePubkey, d.FileName, err))
			continue
		}
		for _, e := range entries {
			rows = append(rows, EntryToRow(d.DevicePubkey, e))
		}
	}
	if len(parseErrs) > 0 {
		// Don't write — destructive for the affected devices under
		// MissingMeansDeleted=true semantics.
		return fmt.Errorf("mroute: %d/%d dumps failed to parse, refusing to write: %w",
			len(parseErrs), len(dumps), errors.Join(parseErrs...))
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
