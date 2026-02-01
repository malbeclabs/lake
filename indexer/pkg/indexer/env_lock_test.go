package indexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckClickHouseEnvLock(t *testing.T) {
	t.Parallel()

	t.Run("new lock creates entry", func(t *testing.T) {
		t.Parallel()
		ch := testClient(t)
		ctx := context.Background()

		err := checkClickHouseEnvLock(ctx, ch, "devnet")
		require.NoError(t, err)

		// Verify the lock was created
		conn, err := ch.Conn(ctx)
		require.NoError(t, err)
		rows, err := conn.Query(ctx, "SELECT dz_env FROM _env_lock")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var env string
		require.NoError(t, rows.Scan(&env))
		assert.Equal(t, "devnet", env)
	})

	t.Run("matching env succeeds", func(t *testing.T) {
		t.Parallel()
		ch := testClient(t)
		ctx := context.Background()

		// Create initial lock
		err := checkClickHouseEnvLock(ctx, ch, "testnet")
		require.NoError(t, err)

		// Same env should succeed
		err = checkClickHouseEnvLock(ctx, ch, "testnet")
		require.NoError(t, err)
	})

	t.Run("mismatched env returns error", func(t *testing.T) {
		t.Parallel()
		ch := testClient(t)
		ctx := context.Background()

		// Create initial lock for devnet
		err := checkClickHouseEnvLock(ctx, ch, "devnet")
		require.NoError(t, err)

		// Different env should fail
		err = checkClickHouseEnvLock(ctx, ch, "mainnet-beta")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "locked to env")
		assert.Contains(t, err.Error(), "devnet")
		assert.Contains(t, err.Error(), "mainnet-beta")
	})
}
