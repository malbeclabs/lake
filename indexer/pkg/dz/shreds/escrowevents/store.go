package escrowevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
)

// HighWaterMark tracks the latest known transaction signature for an escrow.
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
}

func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		log: cfg.Logger,
		cfg: cfg,
	}, nil
}

// GetHighWaterMarks returns the latest transaction signature and slot per escrow
// from the fact table. This is used to determine where to resume fetching.
func (s *Store) GetHighWaterMarks(ctx context.Context) (map[string]HighWaterMark, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	query := `
		SELECT escrow_pk, tx_signature, slot
		FROM fact_dz_shred_escrow_events
		WHERE (escrow_pk, slot) IN (
			SELECT escrow_pk, max(slot) as max_slot
			FROM fact_dz_shred_escrow_events
			GROUP BY escrow_pk
		)
		ORDER BY escrow_pk, slot DESC
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query high water marks: %w", err)
	}
	defer rows.Close()

	result := make(map[string]HighWaterMark)
	for rows.Next() {
		var escrowPK, txSig string
		var slot uint64
		if err := rows.Scan(&escrowPK, &txSig, &slot); err != nil {
			return nil, fmt.Errorf("failed to scan high water mark: %w", err)
		}
		// Only keep the first (highest slot) entry per escrow.
		if _, exists := result[escrowPK]; !exists {
			result[escrowPK] = HighWaterMark{TxSignature: txSig, Slot: slot}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating high water marks: %w", err)
	}

	return result, nil
}

// InsertEvents writes escrow event rows to ClickHouse.
func (s *Store) InsertEvents(ctx context.Context, events []EscrowEventRow) error {
	if len(events) == 0 {
		return nil
	}

	ds, err := newDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	ingestedAt := time.Now().UTC()
	if err := ds.WriteBatch(ctx, conn, len(events), func(i int) ([]any, error) {
		row := events[i]
		row.IngestedAt = ingestedAt
		return schema.ToRow(row), nil
	}); err != nil {
		return fmt.Errorf("failed to write escrow events: %w", err)
	}

	s.log.Info("shreds/escrow-events: inserted events", "count", len(events))
	return nil
}
