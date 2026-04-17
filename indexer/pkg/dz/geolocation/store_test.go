package geolocation

import (
	"context"
	"testing"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

func TestLake_Geolocation_Store_ReplaceProbes(t *testing.T) {
	t.Parallel()

	t.Run("writes and reads back probes", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()

		store, err := NewStore(StoreConfig{Logger: log, ClickHouse: db})
		require.NoError(t, err)

		ctx := context.Background()
		probes := []Probe{
			{
				PK:                 "probe-pk-1",
				Owner:              "owner-1",
				ExchangePK:         "exchange-1",
				PublicIP:           "10.0.1.1",
				LocationOffsetPort: 9000,
				MetricsPublisherPK: "metrics-1",
				ReferenceCount:     5,
				Code:               "probe-1",
				ParentDevices:      []string{"device-a", "device-b"},
				TargetUpdateCount:  3,
			},
		}

		err = store.ReplaceProbes(ctx, probes)
		require.NoError(t, err)

		got, err := QueryCurrentProbes(ctx, log, db)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "probe-pk-1", got[0].PK)
		require.Equal(t, "owner-1", got[0].Owner)
		require.Equal(t, "10.0.1.1", got[0].PublicIP)
		require.Equal(t, uint16(9000), got[0].LocationOffsetPort)
		require.Equal(t, uint32(5), got[0].ReferenceCount)
		require.Equal(t, "probe-1", got[0].Code)
		require.Equal(t, []string{"device-a", "device-b"}, got[0].ParentDevices)
		require.Equal(t, uint32(3), got[0].TargetUpdateCount)
	})

	t.Run("tombstones removed probes on replace", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()

		store, err := NewStore(StoreConfig{Logger: log, ClickHouse: db})
		require.NoError(t, err)

		ctx := context.Background()

		err = store.ReplaceProbes(ctx, []Probe{
			{PK: "probe-a", Code: "a"},
			{PK: "probe-b", Code: "b"},
		})
		require.NoError(t, err)

		err = store.ReplaceProbes(ctx, []Probe{
			{PK: "probe-a", Code: "a-updated"},
		})
		require.NoError(t, err)

		got, err := QueryCurrentProbes(ctx, log, db)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "probe-a", got[0].PK)
		require.Equal(t, "a-updated", got[0].Code)
	})
}

func TestLake_Geolocation_Store_ReplaceUsers(t *testing.T) {
	t.Parallel()

	t.Run("writes and reads back users", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()

		store, err := NewStore(StoreConfig{Logger: log, ClickHouse: db})
		require.NoError(t, err)

		ctx := context.Background()
		users := []User{
			{
				PK:                   "user-pk-1",
				Owner:                "owner-1",
				Code:                 "user-1",
				TokenAccount:         "token-1",
				PaymentStatus:        "paid",
				Status:               "activated",
				TargetCount:          3,
				BillingRate:          1000,
				LastDeductionDzEpoch: 42,
			},
		}

		err = store.ReplaceUsers(ctx, users)
		require.NoError(t, err)

		got, err := QueryCurrentUsers(ctx, log, db)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "user-pk-1", got[0].PK)
		require.Equal(t, "user-1", got[0].Code)
		require.Equal(t, "paid", got[0].PaymentStatus)
		require.Equal(t, "activated", got[0].Status)
		require.Equal(t, uint32(3), got[0].TargetCount)
		require.Equal(t, uint64(1000), got[0].BillingRate)
		require.Equal(t, uint64(42), got[0].LastDeductionDzEpoch)
	})
}

func TestLake_Geolocation_Store_ReplaceTargets(t *testing.T) {
	t.Parallel()

	t.Run("writes and reads back targets", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)
		log := laketesting.NewLogger()

		store, err := NewStore(StoreConfig{Logger: log, ClickHouse: db})
		require.NoError(t, err)

		ctx := context.Background()
		targets := []Target{
			{
				GeolocUserPK:       "user-1",
				ProbePK:            "probe-1",
				TargetType:         "outbound",
				IP:                 "192.168.1.1",
				LocationOffsetPort: 8080,
				TargetPK:           "target-1",
			},
		}

		err = store.ReplaceTargets(ctx, targets)
		require.NoError(t, err)

		got, err := QueryCurrentTargets(ctx, log, db)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "user-1", got[0].GeolocUserPK)
		require.Equal(t, "probe-1", got[0].ProbePK)
		require.Equal(t, "outbound", got[0].TargetType)
		require.Equal(t, "192.168.1.1", got[0].IP)
		require.Equal(t, uint16(8080), got[0].LocationOffsetPort)
		require.Equal(t, "target-1", got[0].TargetPK)
	})
}
