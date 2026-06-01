package msdp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
)

// StoreConfig holds configuration for the MSDP ClickHouse store.
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

// Store manages MSDP dimension data in ClickHouse across three tables.
type Store struct {
	log *slog.Logger
	cfg StoreConfig
}

// NewStore creates a new MSDP ClickHouse store.
func NewStore(cfg StoreConfig) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{log: cfg.Logger, cfg: cfg}, nil
}

// ReplacePeers replaces dim_dz_ip_msdp_peers in ClickHouse. With
// MissingMeansDeleted=true any (device, peer_address) tuple not in the
// batch is tombstoned.
func (s *Store) ReplacePeers(ctx context.Context, rows []PeerRow) error {
	s.log.Debug("msdp/store: replacing peers", "count", len(rows))
	d, err := NewPeerDataset(s.log)
	if err != nil {
		return fmt.Errorf("msdp: build peer dataset: %w", err)
	}
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("msdp: get clickhouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(rows), func(i int) ([]any, error) {
		return peerSchema.ToRow(rows[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("msdp: write peers: %w", err)
	}
	return nil
}

// ReplacePimSACache replaces dim_dz_ip_msdp_pim_sa_cache in ClickHouse.
func (s *Store) ReplacePimSACache(ctx context.Context, rows []PimSACacheRow) error {
	s.log.Debug("msdp/store: replacing pim sa-cache", "count", len(rows))
	d, err := NewPimSACacheDataset(s.log)
	if err != nil {
		return fmt.Errorf("msdp: build pim sa-cache dataset: %w", err)
	}
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("msdp: get clickhouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(rows), func(i int) ([]any, error) {
		return pimSACacheSchema.ToRow(rows[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("msdp: write pim sa-cache: %w", err)
	}
	return nil
}

// ReplaceSACache replaces dim_dz_ip_msdp_sa_cache in ClickHouse.
func (s *Store) ReplaceSACache(ctx context.Context, rows []SACacheRow) error {
	s.log.Debug("msdp/store: replacing sa-cache", "count", len(rows))
	d, err := NewSACacheDataset(s.log)
	if err != nil {
		return fmt.Errorf("msdp: build sa-cache dataset: %w", err)
	}
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("msdp: get clickhouse connection: %w", err)
	}
	defer conn.Close()

	if err := d.WriteBatch(ctx, conn, len(rows), func(i int) ([]any, error) {
		return saCacheSchema.ToRow(rows[i]), nil
	}, &dataset.DimensionType2DatasetWriteConfig{
		MissingMeansDeleted: true,
	}); err != nil {
		return fmt.Errorf("msdp: write sa-cache: %w", err)
	}
	return nil
}
