package dztelemusage

import (
	"context"
	"strings"
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

func TestLake_TelemetryUsage_Store_BackfillFromReader(t *testing.T) {
	t.Parallel()

	t.Run("returns empty result for empty CSV", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		result, err := store.BackfillFromReader(context.Background(), strings.NewReader(""))
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 0, result.RowsQueried)
		require.Equal(t, 0, result.RowsInserted)
	})

	t.Run("returns empty result for header-only CSV", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		csv := "time,dzd_pubkey,host,intf,in-octets,out-octets\n"
		result, err := store.BackfillFromReader(context.Background(), strings.NewReader(csv))
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 0, result.RowsQueried)
		require.Equal(t, 0, result.RowsInserted)
	})

	t.Run("inserts rows from CSV into ClickHouse", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		// Two rows for the same device/interface at different times.
		// The first row is used as baseline for non-sparse counters and is not stored.
		// Only the second row is inserted.
		csv := "time,dzd_pubkey,host,intf,in-octets,out-octets\n" +
			"2024-01-01T10:00:00Z,deviceA,host1,eth0,1000,2000\n" +
			"2024-01-01T10:05:00Z,deviceA,host1,eth0,1500,2500\n"

		result, err := store.BackfillFromReader(context.Background(), strings.NewReader(csv))
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 2, result.RowsQueried)
		require.Equal(t, 1, result.RowsInserted)

		// Verify the row landed in ClickHouse
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		rows, err := conn.Query(context.Background(),
			"SELECT count() FROM fact_dz_device_interface_counters WHERE device_pk = ?", "deviceA")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var count uint64
		require.NoError(t, rows.Scan(&count))
		require.Greater(t, count, uint64(0))
	})

	t.Run("is safe to re-run (idempotent)", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		csv := "time,dzd_pubkey,host,intf,in-octets,out-octets\n" +
			"2024-01-01T11:00:00Z,deviceB,host2,eth1,500,1000\n" +
			"2024-01-01T11:05:00Z,deviceB,host2,eth1,800,1300\n"

		result1, err := store.BackfillFromReader(context.Background(), strings.NewReader(csv))
		require.NoError(t, err)
		require.Equal(t, 1, result1.RowsInserted)

		// Running again should not error (ReplacingMergeTree handles deduplication)
		result2, err := store.BackfillFromReader(context.Background(), strings.NewReader(csv))
		require.NoError(t, err)
		require.Equal(t, 1, result2.RowsInserted)
	})
}
