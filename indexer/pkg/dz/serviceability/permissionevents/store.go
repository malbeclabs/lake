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
type ScanCursor struct {
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
