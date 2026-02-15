package dzrevdist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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

func (s *Store) ReplaceConfig(ctx context.Context, configs []Config) error {
	s.log.Debug("revdist/store: replacing config", "count", len(configs))

	d, err := NewConfigDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(configs), func(i int) ([]any, error) {
		return configSchema.ToRow(configs[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write config to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) ReplaceDistributions(ctx context.Context, distributions []Distribution) error {
	s.log.Debug("revdist/store: replacing distributions", "count", len(distributions))

	d, err := NewDistributionDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	// Distributions use MissingMeansDeleted=false because we only fetch newly completed
	// epochs, not all epochs. Missing epochs are not deleted, they're just not re-fetched.
	if err := d.WriteBatch(ctx, conn, len(distributions), func(i int) ([]any, error) {
		return distributionSchema.ToRow(distributions[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: false,
	}); err != nil {
		return fmt.Errorf("failed to write distributions to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) ReplaceJournal(ctx context.Context, journals []Journal) error {
	s.log.Debug("revdist/store: replacing journal", "count", len(journals))

	d, err := NewJournalDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(journals), func(i int) ([]any, error) {
		return journalSchema.ToRow(journals[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write journal to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) ReplaceValidatorDeposits(ctx context.Context, deposits []ValidatorDeposit) error {
	s.log.Debug("revdist/store: replacing validator deposits", "count", len(deposits))

	d, err := NewValidatorDepositDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(deposits), func(i int) ([]any, error) {
		return validatorDepositSchema.ToRow(deposits[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write validator deposits to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) ReplaceContributorRewards(ctx context.Context, rewards []ContributorReward) error {
	s.log.Debug("revdist/store: replacing contributor rewards", "count", len(rewards))

	d, err := NewContributorRewardsDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(rewards), func(i int) ([]any, error) {
		return contributorRewardsSchema.ToRow(rewards[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write contributor rewards to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) InsertValidatorDebts(ctx context.Context, debts []ValidatorDebt) error {
	s.log.Debug("revdist/store: inserting validator debts", "count", len(debts))

	d, err := NewValidatorDebtFactDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	now := time.Now().UTC()
	if err := d.WriteBatch(ctx, conn, len(debts), func(i int) ([]any, error) {
		return []any{
			int64(debts[i].DZEpoch),
			debts[i].NodeID,
			int64(debts[i].Amount),
			now,
		}, nil
	}); err != nil {
		return fmt.Errorf("failed to write validator debts to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) InsertRewardShares(ctx context.Context, shares []RewardShare) error {
	s.log.Debug("revdist/store: inserting reward shares", "count", len(shares))

	d, err := NewRewardShareFactDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	now := time.Now().UTC()
	if err := d.WriteBatch(ctx, conn, len(shares), func(i int) ([]any, error) {
		return []any{
			int64(shares[i].DZEpoch),
			shares[i].ContributorKey,
			int64(shares[i].UnitShare),
			int64(shares[i].TotalUnitShares),
			shares[i].IsBlocked,
			int64(shares[i].EconomicBurnRate),
			now,
		}, nil
	}); err != nil {
		return fmt.Errorf("failed to write reward shares to ClickHouse: %w", err)
	}

	return nil
}
