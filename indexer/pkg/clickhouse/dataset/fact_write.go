package dataset

import (
	"context"
	"fmt"

	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
)

const defaultWriteBatchSize = 50_000

// WriteBatch writes a batch of fact table data to ClickHouse using PrepareBatch.
// The writeRowFn should return data in the order specified by the Columns configuration.
// Note: ingested_at should be included in writeRowFn output if required by the table schema.
// Large batches are automatically split into sub-batches of defaultWriteBatchSize rows.
func (f *FactDataset) WriteBatch(
	ctx context.Context,
	conn clickhouse.Connection,
	count int,
	writeRowFn func(int) ([]any, error),
) error {
	if count == 0 {
		return nil
	}

	batchSize := defaultWriteBatchSize
	if f.WriteBatchSize > 0 {
		batchSize = f.WriteBatchSize
	}

	f.log.Debug("writing fact batch", "table", f.schema.Name(), "count", count, "batchSize", batchSize)

	insertSQL := fmt.Sprintf("INSERT INTO %s", f.TableName())
	expectedColCount := len(f.cols)

	for start := 0; start < count; start += batchSize {
		end := min(start+batchSize, count)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during batch insert: %w", ctx.Err())
		default:
		}

		batch, err := conn.PrepareBatch(ctx, insertSQL)
		if err != nil {
			return fmt.Errorf("failed to prepare batch: %w", err)
		}

		for i := start; i < end; i++ {
			select {
			case <-ctx.Done():
				batch.Close()
				return fmt.Errorf("context cancelled during batch insert: %w", ctx.Err())
			default:
			}

			row, err := writeRowFn(i)
			if err != nil {
				batch.Close()
				return fmt.Errorf("failed to get row data %d: %w", i, err)
			}

			if len(row) != expectedColCount {
				batch.Close()
				return fmt.Errorf("row %d has %d columns, expected exactly %d", i, len(row), expectedColCount)
			}

			if err := batch.Append(row...); err != nil {
				batch.Close()
				return fmt.Errorf("failed to append row %d: %w", i, err)
			}
		}

		if err := batch.Send(); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}

		f.log.Debug("wrote fact sub-batch", "table", f.schema.Name(), "start", start, "end", end, "total", count)
	}

	return nil
}
