package dzshreds

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStore_MetroHistories_RoundTrip proves ReplaceMetroHistories -> the
// positional staging insert -> dim_dz_shred_metro_histories_current
// round-trips every payload column against the real migrations. The staging
// insert has no column list (loadSnapshotIntoStaging appends positionally), so
// MetroHistorySchema.PayloadColumns()/ToRow() order must equal the staging
// table's physical column order — this is the test that catches drift between
// the schema and a migration. Every column gets a distinct value, and the two
// UInt8 flags get opposite values, so a swapped pair cannot pass.
func TestStore_MetroHistories_RoundTrip(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: info.Client,
	})
	require.NoError(t, err)

	want := MetroHistoryRow{
		PK:                      "metro-history-pk-1",
		ExchangeKey:             "exchange-key-1",
		IsCurrentPriceFinalized: false,
		TotalInitializedDevices: 7,
		CurrentEpoch:            1234,
		CurrentUSDCPriceDollars: 23,
		RetransmitOnlyEnabled:   true,
	}
	require.NoError(t, store.ReplaceMetroHistories(t.Context(), []MetroHistoryRow{want}))

	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT pk, exchange_key, is_current_price_finalized, total_initialized_devices,
			current_epoch, current_usdc_price_dollars, retransmit_only_enabled
		FROM dim_dz_shred_metro_histories_current
	`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "expected one row in _current")
	var got MetroHistoryRow
	var priceFinalized, retransmitOnly uint8
	require.NoError(t, rows.Scan(
		&got.PK, &got.ExchangeKey, &priceFinalized, &got.TotalInitializedDevices,
		&got.CurrentEpoch, &got.CurrentUSDCPriceDollars, &retransmitOnly,
	))
	got.IsCurrentPriceFinalized = priceFinalized != 0
	got.RetransmitOnlyEnabled = retransmitOnly != 0
	assert.Equal(t, want, got)
	require.False(t, rows.Next(), "expected exactly one row in _current")
	require.NoError(t, rows.Err())
}

// TestMetroHistoriesStagingColumnOrder pins the positional-insert invariant
// directly: the staging table's physical column order in ClickHouse must be
// exactly internal columns + PK columns + payload columns as declared by the
// schema, because loadSnapshotIntoStaging inserts with no column list.
func TestMetroHistoriesStagingColumnOrder(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	d, err := NewMetroHistoryDataset(laketesting.NewLogger())
	require.NoError(t, err)

	expected := append(append(append([]string{}, d.InternalColumns()...),
		d.PrimaryKeyColumns()...), d.PayloadColumns()...)

	rows, err := conn.Query(t.Context(), `
		SELECT name FROM system.columns
		WHERE database = currentDatabase() AND table = ?
		ORDER BY position
	`, d.StagingTableName())
	require.NoError(t, err)
	defer rows.Close()

	var actual []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		actual = append(actual, name)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, expected, actual,
		"%s physical column order must match InternalColumns+PrimaryKeyColumns+PayloadColumns — the staging insert is positional", d.StagingTableName())
}
