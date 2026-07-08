package permissionevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// ScanCursor is the newest signature already scanned for a watched program. Empty when
// nothing has been scanned yet (first run / after truncate), which triggers a full sweep.
// Used only by the full-program backfill scan.
type ScanCursor struct {
	TxSignature string
	Slot        uint64
}

// HighWaterMark is the newest signature/slot already indexed for a single Permission PDA.
// Used by the steady-state per-account watch to resume incrementally.
type HighWaterMark struct {
	TxSignature string
	Slot        uint64
}

type StoreConfig struct {
	Logger     *slog.Logger
	ClickHouse clickhouse.Client
}

func (cfg *StoreConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	return nil
}

type Store struct {
	log *slog.Logger
	cfg StoreConfig
	ds  *dataset.FactDataset
}

func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ds, err := newDataset(cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}
	return &Store{log: cfg.Logger, cfg: cfg, ds: ds}, nil
}

// GetScanCursor returns the newest signature/slot already scanned for the given program.
// Returns an empty cursor (no error) when none has been recorded yet.
func (s *Store) GetScanCursor(ctx context.Context, programPK string) (ScanCursor, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return ScanCursor{}, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	// ReplacingMergeTree: the highest updated_at row is the current cursor.
	const query = `
		SELECT last_tx_signature, last_slot
		FROM dz_permission_events_scan_cursor
		WHERE program_pk = ?
		ORDER BY updated_at DESC
		LIMIT 1
	`
	rows, err := conn.Query(ctx, query, programPK)
	if err != nil {
		return ScanCursor{}, fmt.Errorf("failed to query scan cursor: %w", err)
	}
	defer rows.Close()

	var cur ScanCursor
	if rows.Next() {
		if err := rows.Scan(&cur.TxSignature, &cur.Slot); err != nil {
			return ScanCursor{}, fmt.Errorf("failed to scan cursor row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return ScanCursor{}, fmt.Errorf("error iterating scan cursor: %w", err)
	}
	return cur, nil
}

// SetScanCursor records the newest scanned signature/slot for the given program.
func (s *Store) SetScanCursor(ctx context.Context, programPK string, cur ScanCursor) error {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	const query = `
		INSERT INTO dz_permission_events_scan_cursor
			(program_pk, last_tx_signature, last_slot, updated_at)
		VALUES (?, ?, ?, ?)
	`
	if err := conn.Exec(ctx, query, programPK, cur.TxSignature, cur.Slot, time.Now().UTC()); err != nil {
		return fmt.Errorf("failed to write scan cursor: %w", err)
	}
	return nil
}

// GetHighWaterMarks returns the newest indexed signature/slot per Permission PDA, derived
// from the fact table. Every permission event for a PDA lands in the fact table, so
// max(slot) per permission_pk is where the per-account watch resumes.
func (s *Store) GetHighWaterMarks(ctx context.Context) (map[string]HighWaterMark, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	const query = `
		SELECT permission_pk, tx_signature, slot
		FROM fact_dz_permission_events
		WHERE (permission_pk, slot) IN (
			SELECT permission_pk, max(slot)
			FROM fact_dz_permission_events
			GROUP BY permission_pk
		)
		ORDER BY permission_pk, slot DESC, tx_signature DESC
	`
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query high water marks: %w", err)
	}
	defer rows.Close()

	result := make(map[string]HighWaterMark)
	for rows.Next() {
		var pk, txSig string
		var slot uint64
		if err := rows.Scan(&pk, &txSig, &slot); err != nil {
			return nil, fmt.Errorf("failed to scan high water mark row: %w", err)
		}
		// Keep the first (newest, by ORDER BY) row per PDA.
		if _, ok := result[pk]; !ok {
			result[pk] = HighWaterMark{TxSignature: txSig, Slot: slot}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating high water marks: %w", err)
	}
	return result, nil
}

// InsertEvents writes permission event rows to ClickHouse.
func (s *Store) InsertEvents(ctx context.Context, events []PermissionEventRow) error {
	if len(events) == 0 {
		return nil
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	ingestedAt := time.Now().UTC()
	if err := s.ds.WriteBatch(ctx, conn, len(events), func(i int) ([]any, error) {
		row := events[i]
		row.IngestedAt = ingestedAt
		return schema.ToRow(row), nil
	}); err != nil {
		return fmt.Errorf("failed to write permission events: %w", err)
	}

	s.log.Info("serviceability/permission-events: inserted events", "count", len(events))
	return nil
}
