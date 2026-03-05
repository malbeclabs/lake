package validatorsapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// StoreConfig holds configuration for the validatorsapp Store.
type StoreConfig struct {
	Logger     *slog.Logger
	ClickHouse clickhouse.Client
}

// Validate ensures the StoreConfig has all required fields.
func (cfg *StoreConfig) Validate() error {
	if cfg.Logger == nil {
		return errors.New("logger is required")
	}
	if cfg.ClickHouse == nil {
		return errors.New("clickhouse connection is required")
	}
	return nil
}

// Store handles writing validator data to ClickHouse.
type Store struct {
	log *slog.Logger
	cfg StoreConfig
}

// NewStore creates a new Store after validating the provided config.
func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		log: cfg.Logger,
		cfg: cfg,
	}, nil
}

// ReplaceValidators writes the full set of validators to ClickHouse as an SCD2 dimension.
// Validators missing from the input that exist in the table are soft-deleted.
func (s *Store) ReplaceValidators(ctx context.Context, validators []Validator) error {
	s.log.Debug("validatorsapp/store: replacing validators", "count", len(validators))

	d, err := NewValidatorDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dimension dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	err = d.WriteBatch(ctx, conn, len(validators), func(i int) ([]any, error) {
		return validatorSchema.ToRow(validators[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	})
	if err != nil {
		return fmt.Errorf("failed to write validators to ClickHouse: %w", err)
	}

	return nil
}
