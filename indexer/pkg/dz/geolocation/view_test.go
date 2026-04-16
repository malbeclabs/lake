package geolocation

import (
	"context"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/jonboulle/clockwork"
	geolocsdk "github.com/malbeclabs/doublezero/sdk/geolocation/go"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"
)

type MockGeolocationRPC struct {
	getGeoProbesFunc        func(context.Context) ([]geolocsdk.KeyedGeoProbe, error)
	getGeolocationUsersFunc func(context.Context) ([]geolocsdk.KeyedGeolocationUser, error)
}

func (m *MockGeolocationRPC) GetGeoProbes(ctx context.Context) ([]geolocsdk.KeyedGeoProbe, error) {
	if m.getGeoProbesFunc != nil {
		return m.getGeoProbesFunc(ctx)
	}
	return nil, nil
}

func (m *MockGeolocationRPC) GetGeolocationUsers(ctx context.Context) ([]geolocsdk.KeyedGeolocationUser, error) {
	if m.getGeolocationUsersFunc != nil {
		return m.getGeolocationUsersFunc(ctx)
	}
	return nil, nil
}

func nonEmptyGeolocationRPC() *MockGeolocationRPC {
	return &MockGeolocationRPC{
		getGeoProbesFunc: func(ctx context.Context) ([]geolocsdk.KeyedGeoProbe, error) {
			return []geolocsdk.KeyedGeoProbe{{Pubkey: solana.MustPublicKeyFromBase58("11111111111111111111111111111111")}}, nil
		},
		getGeolocationUsersFunc: func(ctx context.Context) ([]geolocsdk.KeyedGeolocationUser, error) {
			return []geolocsdk.KeyedGeolocationUser{{Pubkey: solana.MustPublicKeyFromBase58("11111111111111111111111111111111")}}, nil
		},
	}
}

func testPubkeyBytes(seed byte) [32]byte {
	var pk [32]byte
	for i := range pk {
		pk[i] = seed
	}
	return pk
}

func TestLake_Geolocation_View_Ready(t *testing.T) {
	t.Parallel()

	t.Run("returns false when not ready", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  &MockGeolocationRPC{},
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)
		require.False(t, view.Ready(), "view should not be ready before first refresh")
	})

	t.Run("returns true after successful refresh", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  nonEmptyGeolocationRPC(),
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.NoError(t, err)
		require.True(t, view.Ready(), "view should be ready after successful refresh")
	})
}

func TestLake_Geolocation_View_WaitReady(t *testing.T) {
	t.Parallel()

	t.Run("returns immediately when already ready", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  nonEmptyGeolocationRPC(),
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		err = view.WaitReady(ctx)
		require.NoError(t, err)
	})

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  &MockGeolocationRPC{},
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = view.WaitReady(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled")
	})
}

func TestLake_Geolocation_View_ConvertProbes(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain probes to domain types", func(t *testing.T) {
		t.Parallel()

		pk := solana.MustPublicKeyFromBase58("11111111111111111111111111111111")
		ownerBytes := testPubkeyBytes(1)
		exchangeBytes := testPubkeyBytes(2)
		metricsBytes := testPubkeyBytes(3)
		parentDevice := testPubkeyBytes(4)

		onchain := []geolocsdk.KeyedGeoProbe{
			{
				Pubkey: pk,
				GeoProbe: geolocsdk.GeoProbe{
					Owner:              ownerBytes,
					ExchangePK:         exchangeBytes,
					PublicIP:           [4]uint8{10, 0, 1, 1},
					LocationOffsetPort: 9000,
					MetricsPublisherPK: metricsBytes,
					ReferenceCount:     5,
					Code:               "probe-1",
					ParentDevices:      []solana.PublicKey{solana.PublicKeyFromBytes(parentDevice[:])},
					TargetUpdateCount:  3,
				},
			},
		}

		result := convertProbes(onchain)

		require.Len(t, result, 1)
		require.Equal(t, pk.String(), result[0].PK)
		require.Equal(t, solana.PublicKeyFromBytes(ownerBytes[:]).String(), result[0].Owner)
		require.Equal(t, solana.PublicKeyFromBytes(exchangeBytes[:]).String(), result[0].ExchangePK)
		require.Equal(t, "10.0.1.1", result[0].PublicIP)
		require.Equal(t, uint16(9000), result[0].LocationOffsetPort)
		require.Equal(t, solana.PublicKeyFromBytes(metricsBytes[:]).String(), result[0].MetricsPublisherPK)
		require.Equal(t, uint32(5), result[0].ReferenceCount)
		require.Equal(t, "probe-1", result[0].Code)
		require.Len(t, result[0].ParentDevices, 1)
		require.Equal(t, uint32(3), result[0].TargetUpdateCount)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		t.Parallel()

		result := convertProbes(nil)
		require.Empty(t, result)
	})
}

func TestLake_Geolocation_View_ConvertUsers(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain users to domain types", func(t *testing.T) {
		t.Parallel()

		pk := solana.MustPublicKeyFromBase58("11111111111111111111111111111111")
		ownerBytes := testPubkeyBytes(1)
		tokenAcctBytes := testPubkeyBytes(2)

		onchain := []geolocsdk.KeyedGeolocationUser{
			{
				Pubkey: pk,
				GeolocationUser: geolocsdk.GeolocationUser{
					Owner:         ownerBytes,
					Code:          "user-1",
					TokenAccount:  tokenAcctBytes,
					PaymentStatus: geolocsdk.GeolocationPaymentStatusPaid,
					Status:        geolocsdk.GeolocationUserStatusActivated,
					Billing: geolocsdk.GeolocationBillingConfig{
						FlatPerEpoch: geolocsdk.FlatPerEpochConfig{
							Rate:                 1000,
							LastDeductionDzEpoch: 42,
						},
					},
					Targets: []geolocsdk.GeolocationTarget{
						{}, {}, {},
					},
				},
			},
		}

		result := convertUsers(onchain)

		require.Len(t, result, 1)
		require.Equal(t, pk.String(), result[0].PK)
		require.Equal(t, solana.PublicKeyFromBytes(ownerBytes[:]).String(), result[0].Owner)
		require.Equal(t, "user-1", result[0].Code)
		require.Equal(t, solana.PublicKeyFromBytes(tokenAcctBytes[:]).String(), result[0].TokenAccount)
		require.Equal(t, "paid", result[0].PaymentStatus)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, uint32(3), result[0].TargetCount)
		require.Equal(t, uint64(1000), result[0].BillingRate)
		require.Equal(t, uint64(42), result[0].LastDeductionDzEpoch)
	})
}

func TestLake_Geolocation_View_ConvertTargets(t *testing.T) {
	t.Parallel()

	t.Run("fans out targets from users", func(t *testing.T) {
		t.Parallel()

		userPK := solana.MustPublicKeyFromBase58("11111111111111111111111111111111")
		probeBytes := testPubkeyBytes(1)
		targetBytes := testPubkeyBytes(2)

		onchain := []geolocsdk.KeyedGeolocationUser{
			{
				Pubkey: userPK,
				GeolocationUser: geolocsdk.GeolocationUser{
					Targets: []geolocsdk.GeolocationTarget{
						{
							TargetType:         geolocsdk.GeoLocationTargetTypeOutbound,
							IPAddress:          [4]uint8{192, 168, 1, 1},
							LocationOffsetPort: 8080,
							TargetPK:           targetBytes,
							GeoProbePK:         probeBytes,
						},
						{
							TargetType:         geolocsdk.GeoLocationTargetTypeInbound,
							IPAddress:          [4]uint8{0, 0, 0, 0},
							LocationOffsetPort: 0,
							TargetPK:           targetBytes,
							GeoProbePK:         probeBytes,
						},
					},
				},
			},
		}

		result := convertTargets(onchain)

		require.Len(t, result, 2)
		require.Equal(t, userPK.String(), result[0].GeolocUserPK)
		require.Equal(t, solana.PublicKeyFromBytes(probeBytes[:]).String(), result[0].ProbePK)
		require.Equal(t, "outbound", result[0].TargetType)
		require.Equal(t, "192.168.1.1", result[0].IP)
		require.Equal(t, uint16(8080), result[0].LocationOffsetPort)
		require.Equal(t, solana.PublicKeyFromBytes(targetBytes[:]).String(), result[0].TargetPK)

		require.Equal(t, "inbound", result[1].TargetType)
	})

	t.Run("handles users with no targets", func(t *testing.T) {
		t.Parallel()

		userPK := solana.MustPublicKeyFromBase58("11111111111111111111111111111111")
		onchain := []geolocsdk.KeyedGeolocationUser{
			{
				Pubkey:          userPK,
				GeolocationUser: geolocsdk.GeolocationUser{},
			},
		}

		result := convertTargets(onchain)
		require.Empty(t, result)
	})
}

func TestLake_Geolocation_View_TargetEntityID(t *testing.T) {
	t.Parallel()

	t.Run("produces stable IDs for same fields", func(t *testing.T) {
		t.Parallel()

		t1 := Target{
			GeolocUserPK:       "user1",
			ProbePK:            "probe1",
			TargetType:         "outbound",
			IP:                 "1.2.3.4",
			LocationOffsetPort: 8080,
			TargetPK:           "target1",
		}
		t2 := Target{
			GeolocUserPK:       "user1",
			ProbePK:            "probe1",
			TargetType:         "outbound",
			IP:                 "1.2.3.4",
			LocationOffsetPort: 8080,
			TargetPK:           "target1",
		}

		require.Equal(t, t1.EntityID(), t2.EntityID())
	})

	t.Run("produces different IDs for different fields", func(t *testing.T) {
		t.Parallel()

		t1 := Target{GeolocUserPK: "user1", ProbePK: "probe1", TargetType: "outbound", IP: "1.2.3.4", LocationOffsetPort: 8080, TargetPK: "target1"}
		t2 := Target{GeolocUserPK: "user2", ProbePK: "probe1", TargetType: "outbound", IP: "1.2.3.4", LocationOffsetPort: 8080, TargetPK: "target1"}

		require.NotEqual(t, t1.EntityID(), t2.EntityID())
	})
}

func TestLake_Geolocation_View_Refresh(t *testing.T) {
	t.Parallel()

	t.Run("stores probes, users, and targets on refresh", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		probePKBytes := testPubkeyBytes(10)
		probePK := solana.PublicKeyFromBytes(probePKBytes[:])
		ownerBytes := testPubkeyBytes(1)
		exchangeBytes := testPubkeyBytes(2)
		metricsBytes := testPubkeyBytes(3)

		userPKBytes := testPubkeyBytes(20)
		userPK := solana.PublicKeyFromBytes(userPKBytes[:])
		userOwnerBytes := testPubkeyBytes(4)
		tokenAcctBytes := testPubkeyBytes(5)
		targetPKBytes := testPubkeyBytes(6)

		rpc := &MockGeolocationRPC{
			getGeoProbesFunc: func(ctx context.Context) ([]geolocsdk.KeyedGeoProbe, error) {
				return []geolocsdk.KeyedGeoProbe{
					{
						Pubkey: probePK,
						GeoProbe: geolocsdk.GeoProbe{
							Owner:              ownerBytes,
							ExchangePK:         exchangeBytes,
							PublicIP:           [4]uint8{10, 0, 1, 1},
							LocationOffsetPort: 9000,
							MetricsPublisherPK: metricsBytes,
							ReferenceCount:     1,
							Code:               "probe-1",
						},
					},
				}, nil
			},
			getGeolocationUsersFunc: func(ctx context.Context) ([]geolocsdk.KeyedGeolocationUser, error) {
				return []geolocsdk.KeyedGeolocationUser{
					{
						Pubkey: userPK,
						GeolocationUser: geolocsdk.GeolocationUser{
							Owner:         userOwnerBytes,
							Code:          "user-1",
							TokenAccount:  tokenAcctBytes,
							PaymentStatus: geolocsdk.GeolocationPaymentStatusPaid,
							Status:        geolocsdk.GeolocationUserStatusActivated,
							Billing: geolocsdk.GeolocationBillingConfig{
								FlatPerEpoch: geolocsdk.FlatPerEpochConfig{
									Rate:                 500,
									LastDeductionDzEpoch: 10,
								},
							},
							Targets: []geolocsdk.GeolocationTarget{
								{
									TargetType:         geolocsdk.GeoLocationTargetTypeOutbound,
									IPAddress:          [4]uint8{192, 168, 1, 1},
									LocationOffsetPort: 8080,
									TargetPK:           targetPKBytes,
									GeoProbePK:         testPubkeyBytes(7),
								},
							},
						},
					},
				}, nil
			},
		}

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  rpc,
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		result, err := view.Refresh(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), result.RowsAffected) // 1 probe + 1 user + 1 target

		// Verify data via query functions.
		probes, err := QueryCurrentProbes(ctx, laketesting.NewLogger(), mockDB)
		require.NoError(t, err)
		require.Len(t, probes, 1)
		require.Equal(t, probePK.String(), probes[0].PK)
		require.Equal(t, "10.0.1.1", probes[0].PublicIP)
		require.Equal(t, "probe-1", probes[0].Code)

		users, err := QueryCurrentUsers(ctx, laketesting.NewLogger(), mockDB)
		require.NoError(t, err)
		require.Len(t, users, 1)
		require.Equal(t, userPK.String(), users[0].PK)
		require.Equal(t, "user-1", users[0].Code)
		require.Equal(t, "paid", users[0].PaymentStatus)
		require.Equal(t, "activated", users[0].Status)
		require.Equal(t, uint32(1), users[0].TargetCount)
		require.Equal(t, uint64(500), users[0].BillingRate)
		require.Equal(t, uint64(10), users[0].LastDeductionDzEpoch)

		targets, err := QueryCurrentTargets(ctx, laketesting.NewLogger(), mockDB)
		require.NoError(t, err)
		require.Len(t, targets, 1)
		require.Equal(t, userPK.String(), targets[0].GeolocUserPK)
		require.Equal(t, "outbound", targets[0].TargetType)
		require.Equal(t, "192.168.1.1", targets[0].IP)
		require.Equal(t, uint16(8080), targets[0].LocationOffsetPort)
	})

	t.Run("rejects empty probes response", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		rpc := &MockGeolocationRPC{
			getGeolocationUsersFunc: func(ctx context.Context) ([]geolocsdk.KeyedGeolocationUser, error) {
				return []geolocsdk.KeyedGeolocationUser{{Pubkey: solana.MustPublicKeyFromBase58("11111111111111111111111111111111")}}, nil
			},
		}

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  rpc,
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no probes")
	})

	t.Run("rejects empty users response", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		rpc := &MockGeolocationRPC{
			getGeoProbesFunc: func(ctx context.Context) ([]geolocsdk.KeyedGeoProbe, error) {
				return []geolocsdk.KeyedGeoProbe{{Pubkey: solana.MustPublicKeyFromBase58("11111111111111111111111111111111")}}, nil
			},
		}

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clockwork.NewFakeClock(),
			GeolocationRPC:  rpc,
			RefreshInterval: time.Second,
			ClickHouse:      mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no users")
	})
}
