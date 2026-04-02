package dzshreds

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
	return &Store{
		log: cfg.Logger,
		cfg: cfg,
	}, nil
}

func (s *Store) ReplaceExecutionControllers(ctx context.Context, rows []ExecutionControllerRow) error {
	s.log.Debug("shreds/store: replacing execution controllers", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewExecutionControllerDataset, func(i int) ([]any, error) {
		return executionControllerSchema.ToRow(rows[i]), nil
	})
}

func (s *Store) ReplaceClientSeats(ctx context.Context, rows []ClientSeatRow) error {
	s.log.Debug("shreds/store: replacing client seats", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewClientSeatDataset, func(i int) ([]any, error) {
		return clientSeatSchema.ToRow(rows[i]), nil
	})
}

func (s *Store) ReplacePaymentEscrows(ctx context.Context, rows []PaymentEscrowRow) error {
	s.log.Debug("shreds/store: replacing payment escrows", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewPaymentEscrowDataset, func(i int) ([]any, error) {
		return paymentEscrowSchema.ToRow(rows[i]), nil
	})
}

func (s *Store) ReplaceMetroHistories(ctx context.Context, rows []MetroHistoryRow) error {
	s.log.Debug("shreds/store: replacing metro histories", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewMetroHistoryDataset, func(i int) ([]any, error) {
		return metroHistorySchema.ToRow(rows[i]), nil
	})
}

func (s *Store) ReplaceDeviceHistories(ctx context.Context, rows []DeviceHistoryRow) error {
	s.log.Debug("shreds/store: replacing device histories", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewDeviceHistoryDataset, func(i int) ([]any, error) {
		return deviceHistorySchema.ToRow(rows[i]), nil
	})
}

func (s *Store) ReplaceValidatorClientRewards(ctx context.Context, rows []ValidatorClientRewardsRow) error {
	s.log.Debug("shreds/store: replacing validator client rewards", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewValidatorClientRewardsDataset, func(i int) ([]any, error) {
		return validatorClientRewardsSchema.ToRow(rows[i]), nil
	})
}

func (s *Store) ReplaceShredDistributions(ctx context.Context, rows []ShredDistributionRow) error {
	s.log.Debug("shreds/store: replacing shred distributions", "count", len(rows))
	return s.writeDimension(ctx, len(rows), NewShredDistributionDataset, func(i int) ([]any, error) {
		return shredDistributionSchema.ToRow(rows[i]), nil
	})
}

func (s *Store) writeDimension(
	ctx context.Context,
	count int,
	newDataset func(*slog.Logger) (*dataset.DimensionType2Dataset, error),
	toRow func(int) ([]any, error),
) error {
	d, err := newDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, count, toRow, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write to ClickHouse: %w", err)
	}

	return nil
}
