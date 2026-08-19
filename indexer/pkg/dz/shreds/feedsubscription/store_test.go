package feedsubscription

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStore_FeedDistributions_RoundTrip proves ReplaceFeedDistributions -> the
// positional staging insert -> dim_dz_shred_feed_distributions_current
// round-trips every payload column against the real migrations.
// loadSnapshotIntoStaging appends values positionally with no column list, so
// PayloadColumns()/ToRow() order must equal the staging table's physical column
// order. This is the test that catches drift between the schema and the
// migration, so every column gets a distinct value: year and month cannot be
// swapped without failing, and neither can feed_key and pk.
func TestStore_FeedDistributions_RoundTrip(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: info.Client,
	})
	require.NoError(t, err)

	want := FeedDistributionRow{
		PK:            "feed-distribution-pk-1",
		FeedKey:       "feed-key-1",
		Year:          2026,
		Month:         8,
		CollectedUSDC: 2080645159,
	}
	require.NoError(t, store.ReplaceFeedDistributions(t.Context(), []FeedDistributionRow{want}))

	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(t.Context(), `
		SELECT pk, feed_key, year, month, collected_usdc
		FROM dim_dz_shred_feed_distributions_current
	`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "expected one row in _current")
	var got FeedDistributionRow
	require.NoError(t, rows.Scan(&got.PK, &got.FeedKey, &got.Year, &got.Month, &got.CollectedUSDC))
	assert.Equal(t, want, got)
	assert.False(t, rows.Next(), "expected exactly one row")
}

// Feed-month rows accumulate: a row absent from a later snapshot is not
// tombstoned, because the account it came from can be closed on chain once
// its month settles, and the row's all-time total must survive that. This
// pins both halves of the contract: a snapshot missing a previously-seen row
// keeps it (rather than dropping it as MissingMeansDeleted would), and a
// wholly empty snapshot - the shape of a transient getProgramAccounts hiccup
// - is a no-op rather than wiping the table.
func TestStore_FeedDistributions_IsAdditive(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: info.Client,
	})
	require.NoError(t, err)

	both := []FeedDistributionRow{
		{PK: "pk-a", FeedKey: "feed-a", Year: 2026, Month: 8, CollectedUSDC: 100},
		{PK: "pk-b", FeedKey: "feed-b", Year: 2026, Month: 8, CollectedUSDC: 200},
	}
	require.NoError(t, store.ReplaceFeedDistributions(t.Context(), both))

	// A snapshot that only saw one of the two accounts (the other's account
	// closed on chain, or a partial RPC page) must not tombstone the other.
	require.NoError(t, store.ReplaceFeedDistributions(t.Context(), both[:1]))

	conn, err := info.Client.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	assertBothRowsPresent := func() {
		t.Helper()
		got := map[string]uint64{}
		rows, err := conn.Query(t.Context(), `SELECT pk, collected_usdc FROM dim_dz_shred_feed_distributions_current ORDER BY pk`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var pk string
			var collected uint64
			require.NoError(t, rows.Scan(&pk, &collected))
			got[pk] = collected
		}
		assert.Equal(t, map[string]uint64{"pk-a": 100, "pk-b": 200}, got)
	}
	assertBothRowsPresent()

	// A wholly empty snapshot is the transient-empty-RPC-response case: it must
	// be a no-op, not a mass tombstone of every feed-month ever observed.
	require.NoError(t, store.ReplaceFeedDistributions(t.Context(), nil))
	assertBothRowsPresent()
}

// CountFeedDistributions is what tells a cluster without the program deployed
// apart from an RPC that answered with nothing while real revenue is stored, so
// it has to read the current view rather than the raw history.
func TestStore_CountFeedDistributions(t *testing.T) {
	info := laketesting.NewClientWithInfo(t, sharedDB)
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: info.Client,
	})
	require.NoError(t, err)

	count, err := store.CountFeedDistributions(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count, "an empty table counts zero rather than erroring")

	require.NoError(t, store.ReplaceFeedDistributions(t.Context(), []FeedDistributionRow{
		{PK: "pk-a", FeedKey: "feed-a", Year: 2026, Month: 8, CollectedUSDC: 100},
		{PK: "pk-b", FeedKey: "feed-b", Year: 2026, Month: 9, CollectedUSDC: 200},
	}))

	count, err = store.CountFeedDistributions(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), count)
}
