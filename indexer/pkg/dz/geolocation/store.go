package geolocation

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

func (s *Store) GetClickHouse() clickhouse.Client {
	return s.cfg.ClickHouse
}

func (s *Store) ReplaceProbes(ctx context.Context, probes []Probe) error {
	s.log.Debug("geolocation/store: replacing probes", "count", len(probes))

	d, err := NewProbeDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(probes), func(i int) ([]any, error) {
		return probeSchema.ToRow(probes[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write probes to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) ReplaceUsers(ctx context.Context, users []User) error {
	s.log.Debug("geolocation/store: replacing users", "count", len(users))

	d, err := NewUserDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(users), func(i int) ([]any, error) {
		return userSchema.ToRow(users[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write users to ClickHouse: %w", err)
	}

	return nil
}

func (s *Store) ReplaceTargets(ctx context.Context, targets []Target) error {
	s.log.Debug("geolocation/store: replacing targets", "count", len(targets))

	d, err := NewTargetDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(targets), func(i int) ([]any, error) {
		return targetSchema.ToRow(targets[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write targets to ClickHouse: %w", err)
	}

	return nil
}
