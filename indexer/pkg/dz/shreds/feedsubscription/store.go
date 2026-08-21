package feedsubscription

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

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
	return &Store{log: cfg.Logger, cfg: cfg}, nil
}

// ReplaceFeedDistributions writes a fresh dim-type-2 snapshot of the
// FeedDistribution accounts read this refresh.
//
// A nil or empty rows slice is a no-op.
//
// MissingMeansDeleted is false: a feed-month's row accumulates its all-time
// collected total, and the on-chain account can be closed once its month
// settles. A closed account drops out of getProgramAccounts, so a refresh
// that no longer sees it must not tombstone the row it already wrote.
func (s *Store) ReplaceFeedDistributions(ctx context.Context, rows []FeedDistributionRow) error {
	if len(rows) == 0 {
		return nil
	}

	s.log.Debug("shreds/feed-subscription: replacing feed distributions", "count", len(rows))

	d, err := NewFeedDistributionDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(rows), func(i int) ([]any, error) {
		return feedDistributionSchema.ToRow(rows[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: false,
	}); err != nil {
		return fmt.Errorf("failed to write to ClickHouse: %w", err)
	}
	return nil
}

// CountFeedDistributions returns how many feed-months the table currently holds.
//
// Used only to tell two indistinguishable empty fetches apart: a cluster without
// the program deployed, where zero accounts is the right answer forever, and a
// mainnet RPC that answered with nothing when rows exist. Queried rather than
// remembered in memory so the distinction survives a pod restart.
func (s *Store) CountFeedDistributions(ctx context.Context) (uint64, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, `SELECT count() FROM dim_dz_shred_feed_distributions_current`)
	if err != nil {
		return 0, fmt.Errorf("failed to count feed distributions: %w", err)
	}
	defer rows.Close()

	var count uint64
	if !rows.Next() {
		return 0, fmt.Errorf("count query returned no row: %w", rows.Err())
	}
	if err := rows.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to scan feed distribution count: %w", err)
	}
	return count, rows.Err()
}
