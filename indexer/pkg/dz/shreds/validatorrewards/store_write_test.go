package validatorrewards

import (
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

// These tests exercise the dim-type-2 write path end-to-end against a real
// ClickHouse, which the API-level tests do not (they seed via raw SQL). This
// is the path that silently broke when the migration tables omitted the `pk`
// column — the dataset framework writes internal[6] + pk + payload, so a
// missing `pk` column fails the append with "expected N arguments, got N+1".

func newStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: testClient(t),
	})
	require.NoError(t, err)
	return store
}

func TestStore_ReplaceDistribution2ZPools_RoundTrip(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	rows := []Distribution2ZPoolRow{
		{PK: Distribution2ZPoolPK(975), SubscriptionEpoch: 975, TokensReceived2Z: 8_733_030_000},
		{PK: Distribution2ZPoolPK(976), SubscriptionEpoch: 976, TokensReceived2Z: 7_735_790_000},
	}
	require.NoError(t, store.ReplaceDistribution2ZPools(ctx, rows))

	conn, err := store.cfg.ClickHouse.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var (
		count     uint64
		funded    uint64
		maxTokens uint64
	)
	res, err := conn.Query(ctx, `
		SELECT count(), countIf(tokens_received_2z > 0), max(tokens_received_2z)
		FROM dim_dz_shred_distribution_2z_pool_current`)
	require.NoError(t, err)
	defer res.Close()
	require.True(t, res.Next())
	require.NoError(t, res.Scan(&count, &funded, &maxTokens))

	require.Equal(t, uint64(2), count)
	require.Equal(t, uint64(2), funded)
	require.Equal(t, uint64(8_733_030_000), maxTokens)
}

func TestStore_ReplaceLeafDistributionStatuses_RoundTrip(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	ctx := t.Context()

	rows := []LeafDistributionStatusRow{
		{PK: LeafDistributionStatusPK(975, "node-A", 1), SubscriptionEpoch: 975, NodeID: "node-A", ClientID: 1, IsClaimable: 1, JournalMintKey: DoubleZeroMintKey},
		{PK: LeafDistributionStatusPK(975, "node-B", 1), SubscriptionEpoch: 975, NodeID: "node-B", ClientID: 1, IsClaimable: 0, JournalMintKey: DoubleZeroMintKey},
	}
	require.NoError(t, store.ReplaceLeafDistributionStatuses(ctx, rows))

	conn, err := store.cfg.ClickHouse.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var total, claimable uint64
	res, err := conn.Query(ctx, `
		SELECT count(), countIf(is_claimable = 1)
		FROM dim_dz_shred_validator_leaf_distribution_status_current
		WHERE subscription_epoch = 975`)
	require.NoError(t, err)
	defer res.Close()
	require.True(t, res.Next())
	require.NoError(t, res.Scan(&total, &claimable))

	require.Equal(t, uint64(2), total)
	require.Equal(t, uint64(1), claimable)
}
