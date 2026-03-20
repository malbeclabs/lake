package isis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// StoreConfig holds configuration for the ISIS ClickHouse store.
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

// Store manages ISIS dimension data in ClickHouse.
type Store struct {
	log *slog.Logger
	cfg StoreConfig
}

// NewStore creates a new ISIS ClickHouse store.
func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		log: cfg.Logger,
		cfg: cfg,
	}, nil
}

// ReplaceAdjacencies replaces all ISIS adjacency data in ClickHouse.
func (s *Store) ReplaceAdjacencies(ctx context.Context, adjacencies []Adjacency) error {
	s.log.Debug("isis/store: replacing adjacencies", "count", len(adjacencies))

	d, err := NewAdjacencyDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(adjacencies), func(i int) ([]any, error) {
		return adjacencySchema.ToRow(adjacencies[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write adjacencies to ClickHouse: %w", err)
	}

	return nil
}

// ReplaceDevices replaces all ISIS device data in ClickHouse.
func (s *Store) ReplaceDevices(ctx context.Context, devices []Device) error {
	s.log.Debug("isis/store: replacing devices", "count", len(devices))

	d, err := NewDeviceDataset(s.log)
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(devices), func(i int) ([]any, error) {
		return deviceSchema.ToRow(devices[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("failed to write devices to ClickHouse: %w", err)
	}

	return nil
}
