package pcapwarehouse

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

// Store reads and writes fact_dz_edge_pcap_file.
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

// Cursors returns the newest hour directory ingested for each recorder prefix. A
// recorder with no row has not been read yet, and its sync starts from its oldest hour.
func (s *Store) Cursors(ctx context.Context) (map[string]time.Time, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	rows, err := conn.Query(ctx, `
		SELECT recorder, recorder_ip, max(hour_ts)
		FROM fact_dz_edge_pcap_file
		GROUP BY recorder, recorder_ip
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query pcap warehouse cursors: %w", err)
	}
	defer rows.Close()
	out := make(map[string]time.Time)
	for rows.Next() {
		var host, ip string
		var hour time.Time
		if err := rows.Scan(&host, &ip, &hour); err != nil {
			return nil, fmt.Errorf("failed to scan pcap warehouse cursor: %w", err)
		}
		out[host+"-"+ip] = hour.UTC()
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pcap warehouse cursors: %w", err)
	}
	return out, nil
}

// IngestedManifests returns the manifest keys already written for a recorder in hour
// directories at or after since.
func (s *Store) IngestedManifests(ctx context.Context, rec Recorder, since time.Time) (map[string]struct{}, error) {
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT manifest_key
		FROM fact_dz_edge_pcap_file
		WHERE recorder = ? AND recorder_ip = ? AND hour_ts >= ?
	`, rec.Host, rec.IP, since.UTC())
	if err != nil {
		return nil, fmt.Errorf("failed to query ingested manifests: %w", err)
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("failed to scan ingested manifest: %w", err)
		}
		out[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ingested manifests: %w", err)
	}
	return out, nil
}

// Insert writes file rows, stamping them with one ingested_at.
func (s *Store) Insert(ctx context.Context, files []FileRow) error {
	if len(files) == 0 {
		return nil
	}
	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}
	ingestedAt := time.Now().UTC()
	if err := s.ds.WriteBatch(ctx, conn, len(files), func(i int) ([]any, error) {
		row := files[i]
		row.IngestedAt = ingestedAt
		return schema.ToRow(row), nil
	}); err != nil {
		return fmt.Errorf("failed to write pcap warehouse files: %w", err)
	}
	return nil
}
