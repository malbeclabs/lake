package dztelemlatency

import (
	"context"
	"fmt"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
)

// DeviceLinkLatencySampleHeader mirrors the on-chain latency sample header
// for a device link circuit in a given epoch.
type DeviceLinkLatencySampleHeader struct {
	OriginDevicePK     string
	TargetDevicePK     string
	LinkPK             string
	Epoch              uint64
	StartTimestampUs   uint64
	SamplingIntervalUs uint64
	LatestSampleIndex  int
}

// InternetMetroLatencySampleHeader mirrors the on-chain latency sample header
// for an internet metro circuit in a given epoch.
type InternetMetroLatencySampleHeader struct {
	OriginMetroPK      string
	TargetMetroPK      string
	DataProvider       string
	Epoch              uint64
	StartTimestampUs   uint64
	SamplingIntervalUs uint64
	LatestSampleIndex  int
}

func (s *Store) AppendDeviceLinkLatencySampleHeaders(ctx context.Context, headers []DeviceLinkLatencySampleHeader) error {
	if len(headers) == 0 {
		return nil
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	writtenAt := time.Now().UTC()
	if err := writeHeaders(ctx, conn, "fact_dz_device_link_latency_sample_header", len(headers), func(i int) []any {
		h := headers[i]
		return []any{
			writtenAt,
			h.OriginDevicePK,
			h.TargetDevicePK,
			h.LinkPK,
			int64(h.Epoch),
			int64(h.StartTimestampUs),
			h.SamplingIntervalUs,
			int32(h.LatestSampleIndex),
		}
	}); err != nil {
		return fmt.Errorf("failed to write device link latency sample headers: %w", err)
	}

	return nil
}

func (s *Store) AppendInternetMetroLatencySampleHeaders(ctx context.Context, headers []InternetMetroLatencySampleHeader) error {
	if len(headers) == 0 {
		return nil
	}

	conn, err := s.cfg.ClickHouse.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get ClickHouse connection: %w", err)
	}

	writtenAt := time.Now().UTC()
	if err := writeHeaders(ctx, conn, "fact_dz_internet_metro_latency_sample_header", len(headers), func(i int) []any {
		h := headers[i]
		return []any{
			writtenAt,
			h.OriginMetroPK,
			h.TargetMetroPK,
			h.DataProvider,
			int64(h.Epoch),
			int64(h.StartTimestampUs),
			h.SamplingIntervalUs,
			int32(h.LatestSampleIndex),
		}
	}); err != nil {
		return fmt.Errorf("failed to write internet metro latency sample headers: %w", err)
	}

	return nil
}

func writeHeaders(ctx context.Context, conn clickhouse.Connection, table string, count int, rowFn func(int) []any) error {
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for i := range count {
		if err := batch.Append(rowFn(i)...); err != nil {
			batch.Close()
			return fmt.Errorf("failed to append header row %d: %w", i, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send header batch: %w", err)
	}

	return nil
}
