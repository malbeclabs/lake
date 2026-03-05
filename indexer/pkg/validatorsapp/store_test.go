package validatorsapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

// loadFixtureValidators serves the test fixture via httptest and returns parsed validators.
func loadFixtureValidators(t *testing.T) []Validator {
	t.Helper()
	fixture := loadTestFixture(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, "test-key")
	validators, err := client.GetValidators(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, validators)
	return validators
}

func TestStore(t *testing.T) {
	t.Parallel()

	t.Run("config validation", func(t *testing.T) {
		t.Parallel()

		t.Run("missing logger", func(t *testing.T) {
			t.Parallel()
			store, err := NewStore(StoreConfig{
				ClickHouse: nil,
			})
			require.Error(t, err)
			require.Nil(t, store)
			require.Contains(t, err.Error(), "logger is required")
		})

		t.Run("missing clickhouse", func(t *testing.T) {
			t.Parallel()
			store, err := NewStore(StoreConfig{
				Logger: laketesting.NewLogger(),
			})
			require.Error(t, err)
			require.Nil(t, store)
			require.Contains(t, err.Error(), "clickhouse connection is required")
		})
	})

	t.Run("writes real fixture data to ClickHouse", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		validators := loadFixtureValidators(t)

		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		err = store.ReplaceValidators(context.Background(), validators)
		require.NoError(t, err)

		// Verify count matches
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		rows, err := conn.Query(context.Background(), `
			WITH ranked AS (
				SELECT
					*,
					ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_validatorsapp_validators_history
			)
			SELECT count() FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var count uint64
		require.NoError(t, rows.Scan(&count))
		rows.Close()
		require.Equal(t, uint64(len(validators)), count, "should have written all fixture validators")

		// Verify multiple software_client types exist in DB
		rows, err = conn.Query(context.Background(), `
			WITH ranked AS (
				SELECT
					*,
					ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_validatorsapp_validators_history
			)
			SELECT DISTINCT software_client FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		clientTypes := make(map[string]bool)
		for rows.Next() {
			var client string
			require.NoError(t, rows.Scan(&client))
			clientTypes[client] = true
		}
		rows.Close()
		require.Greater(t, len(clientTypes), 1, "should have multiple software client types in DB")
	})

	t.Run("detects changes on second write", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		v := Validator{
			Account:         "TestAccount111111111111111111111111111111111",
			Name:            "Test Validator",
			VoteAccount:     "VoteAccount1111111111111111111111111111111",
			SoftwareVersion: "1.0.0",
			SoftwareClient:  "Agave",
			IsActive:        true,
			ActiveStake:     1000000,
			Commission:      5,
			Epoch:           100,
		}

		// First write
		err = store.ReplaceValidators(context.Background(), []Validator{v})
		require.NoError(t, err)

		// Change software version and client
		v.SoftwareVersion = "2.0.0"
		v.SoftwareClient = "Firedancer"

		// Second write
		err = store.ReplaceValidators(context.Background(), []Validator{v})
		require.NoError(t, err)

		// Verify 2 history rows for this entity
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		rows, err := conn.Query(context.Background(), `
			SELECT count()
			FROM dim_validatorsapp_validators_history
			WHERE account = ?
		`, v.Account)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var historyCount uint64
		require.NoError(t, rows.Scan(&historyCount))
		rows.Close()
		require.Equal(t, uint64(2), historyCount, "should have 2 history rows after change")
	})

	t.Run("soft-deletes removed validators", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		store, err := NewStore(StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: db,
		})
		require.NoError(t, err)

		v1 := Validator{
			Account:         "SoftDel11111111111111111111111111111111111",
			Name:            "Validator One",
			VoteAccount:     "VoteOne1111111111111111111111111111111111",
			SoftwareVersion: "1.0.0",
			SoftwareClient:  "Agave",
			IsActive:        true,
			Epoch:           100,
			StakePoolsList:  []string{},
		}
		v2 := Validator{
			Account:         "SoftDel22222222222222222222222222222222222",
			Name:            "Validator Two",
			VoteAccount:     "VoteTwo2222222222222222222222222222222222",
			SoftwareVersion: "1.0.0",
			SoftwareClient:  "Firedancer",
			IsActive:        true,
			Epoch:           100,
			StakePoolsList:  []string{},
		}

		// Write both validators. Capture the second so we can ensure the next
		// write lands in a different second (the delta query's toDateTime64(?, 3)
		// parameter binding truncates to second precision).
		firstWriteSec := time.Now().Truncate(time.Second)
		err = store.ReplaceValidators(context.Background(), []Validator{v1, v2})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return time.Now().Truncate(time.Second).After(firstWriteSec)
		}, 2*time.Second, 50*time.Millisecond, "clock should advance to next second")

		// Write only v1 (v2 should be soft-deleted via MissingMeansDeleted)
		err = store.ReplaceValidators(context.Background(), []Validator{v1})
		require.NoError(t, err)

		// Verify the tombstone was written for v2
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		// Count total history rows - should be 3 or 4:
		// v1 from first write, v2 from first write, v1 from second write (if changed), v2 tombstone
		rows, err := conn.Query(context.Background(), `
			SELECT count()
			FROM dim_validatorsapp_validators_history
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var totalCount uint64
		require.NoError(t, rows.Scan(&totalCount))
		rows.Close()
		require.GreaterOrEqual(t, totalCount, uint64(3), "should have at least 3 history rows")

		// Verify v2 has a tombstone row (is_deleted=1)
		rows, err = conn.Query(context.Background(), `
			SELECT count()
			FROM dim_validatorsapp_validators_history
			WHERE account = ? AND is_deleted = 1
		`, v2.Account)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var tombstoneCount uint64
		require.NoError(t, rows.Scan(&tombstoneCount))
		rows.Close()
		require.Equal(t, uint64(1), tombstoneCount, "v2 should have exactly 1 tombstone row")
	})
}
