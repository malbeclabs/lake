package mroute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// StoreConfig holds configuration for the mroute ClickHouse store.
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

// Store manages mroute dimension data in ClickHouse.
type Store struct {
	log *slog.Logger
	cfg StoreConfig
}

// NewStore creates a new mroute ClickHouse store.
func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{log: cfg.Logger, cfg: cfg}, nil
}

// ReplaceEntries replaces all mroute entries in ClickHouse. With
// MissingMeansDeleted=true, any (device, vrf, mode, group, source) tuple not
// present in the current batch is marked deleted in the type2 dimension.
func (s *Store) ReplaceEntries(ctx context.Context, rows []Row) error {
	s.log.Debug("mroute/store: replacing entries", "count", len(rows))

	d, err := NewEntryDataset(s.log)
	if err != nil {
		return fmt.Errorf("mroute: build dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mroute: get clickhouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(rows), func(i int) ([]any, error) {
		return entrySchema.ToRow(rows[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("mroute: write entries: %w", err)
	}
	return nil
}
