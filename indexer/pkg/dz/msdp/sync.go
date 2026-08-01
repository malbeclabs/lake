package msdp

import (
	"context"
	"errors"
	"fmt"
)

// Sync parses per-device dumps for all three MSDP kinds and replaces
// them in ClickHouse via three separate dim writes.
//
// Fail-fast contract (same as mroute): on any parse failure across any
// kind, return an error and write nothing. The store uses
// MissingMeansDeleted=true, so silently dropping a device from one of
// the batches would tombstone that device's prior state. Refusing to
// write preserves all prior state until the underlying parse issue is
// investigated.
func (s *Store) Sync(ctx context.Context, dumpsByKind map[string][]*Dump) error {
	var parseErrs []error

	peerRows := make([]PeerRow, 0)
	for _, d := range dumpsByKind[SnapshotKindSummary] {
		if d == nil {
			continue
		}
		peers, err := ParseSummary(d.RawJSON)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("summary %s (%s): %w", d.DevicePubkey, d.FileName, err))
			continue
		}
		for _, p := range peers {
			peerRows = append(peerRows, PeerToRow(d.DevicePubkey, p))
		}
	}

	pimRows := make([]PimSACacheRow, 0)
	for _, d := range dumpsByKind[SnapshotKindPimSACache] {
		if d == nil {
			continue
		}
		entries, err := ParsePimSACache(d.RawJSON)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("pim sa-cache %s (%s): %w", d.DevicePubkey, d.FileName, err))
			continue
		}
		for _, e := range entries {
			pimRows = append(pimRows, PimSACacheToRow(d.DevicePubkey, e))
		}
	}

	saRows := make([]SACacheRow, 0)
	for _, d := range dumpsByKind[SnapshotKindSACacheRejected] {
		if d == nil {
			continue
		}
		entries, err := ParseSACacheRejected(d.RawJSON)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Errorf("sa-cache rejected %s (%s): %w", d.DevicePubkey, d.FileName, err))
			continue
		}
		for _, e := range entries {
			saRows = append(saRows, SACacheToRow(d.DevicePubkey, e))
		}
	}

	if len(parseErrs) > 0 {
		return fmt.Errorf("msdp: %d dumps failed to parse, refusing to write: %w",
			len(parseErrs), errors.Join(parseErrs...))
	}

	if err := s.ReplacePeers(ctx, peerRows); err != nil {
		return fmt.Errorf("msdp: sync peers: %w", err)
	}
	if err := s.ReplacePimSACache(ctx, pimRows); err != nil {
		return fmt.Errorf("msdp: sync pim sa-cache: %w", err)
	}
	if err := s.ReplaceSACache(ctx, saRows); err != nil {
		return fmt.Errorf("msdp: sync sa-cache: %w", err)
	}

	s.log.Info("msdp: synced to ClickHouse",
		"peers", len(peerRows),
		"pim_sa_cache", len(pimRows),
		"sa_cache", len(saRows),
	)
	return nil
}
