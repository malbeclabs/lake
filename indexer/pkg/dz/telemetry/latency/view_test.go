package dztelemlatency

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
	mcpgeoip "github.com/malbeclabs/lake/indexer/pkg/geoip"
	"github.com/malbeclabs/lake/indexer/pkg/metrics"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/serviceability"
	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/telemetry"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
)

type mockTelemetryRPC struct{}

func (m *mockTelemetryRPC) GetDeviceLatencySamplesTail(ctx context.Context, originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
	return nil, 0, nil, telemetry.ErrAccountNotFound
}

func (m *mockTelemetryRPC) GetInternetLatencySamples(ctx context.Context, dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error) {
	return nil, telemetry.ErrAccountNotFound
}

type mockEpochRPC struct{}

func (m *mockEpochRPC) GetEpochInfo(ctx context.Context, commitment solanarpc.CommitmentType) (*solanarpc.GetEpochInfoResult, error) {
	return &solanarpc.GetEpochInfoResult{
		Epoch: 100,
	}, nil
}

type MockServiceabilityRPC struct {
	getProgramDataFunc func(context.Context) (*serviceability.ProgramData, error)
}

func (m *MockServiceabilityRPC) GetProgramData(ctx context.Context) (*serviceability.ProgramData, error) {
	if m.getProgramDataFunc != nil {
		return m.getProgramDataFunc(ctx)
	}
	// Return minimal valid data to pass validation
	return &serviceability.ProgramData{
		Contributors: []serviceability.Contributor{{PubKey: [32]byte{1}}},
		Devices:      []serviceability.Device{{PubKey: [32]byte{2}}},
		Exchanges:    []serviceability.Exchange{{PubKey: [32]byte{3}}},
	}, nil
}

type mockGeoIPStore struct {
	store *mcpgeoip.Store
	db    clickhouse.Client
}

func newMockGeoIPStore(t *testing.T) (*mockGeoIPStore, error) {
	t.Helper()
	db := testClient(t)

	store, err := mcpgeoip.NewStore(mcpgeoip.StoreConfig{
		Logger:     laketesting.NewLogger(),
		ClickHouse: db,
	})
	if err != nil {
		return nil, err
	}

	return &mockGeoIPStore{
		store: store,
		db:    db,
	}, nil
}

func TestLake_TelemetryLatency_View_Ready(t *testing.T) {
	t.Parallel()

	t.Run("returns false when not ready", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           &mockTelemetryRPC{},
			EpochRPC:               &mockEpochRPC{},
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		require.False(t, view.Ready(), "view should not be ready before first refresh")
	})
}

func TestLake_TelemetryLatency_View_WaitReady(t *testing.T) {
	t.Parallel()

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           &mockTelemetryRPC{},
			EpochRPC:               &mockEpochRPC{},
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = view.WaitReady(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled")
	})
}

func TestLake_TelemetryLatency_View_ConvertInternetLatencySamples(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain internet latency samples to domain types", func(t *testing.T) {
		t.Parallel()

		samples := &telemetry.InternetLatencySamples{
			InternetLatencySamplesHeader: telemetry.InternetLatencySamplesHeader{
				StartTimestampMicroseconds:   1_700_000_000,
				SamplingIntervalMicroseconds: 250_000,
			},
			Samples: []uint32{10000, 11000, 12000},
		}

		result := convertInternetLatencySamples(samples, testPK(1), testPK(2), "test-provider", 456)

		require.Len(t, result, 3)
		require.Equal(t, testPK(1), result[0].OriginMetroPK)
		require.Equal(t, testPK(2), result[0].TargetMetroPK)
		require.Equal(t, "test-provider", result[0].DataProvider)
		require.Equal(t, uint64(456), result[0].Epoch)
		require.Equal(t, 0, result[0].SampleIndex)
		require.WithinDuration(t, time.Unix(0, 1_700_000_000*1000), result[0].Time, time.Millisecond)
		require.Equal(t, uint32(10000), result[0].RTTMicroseconds)

		require.Equal(t, 1, result[1].SampleIndex)
		require.WithinDuration(t, time.Unix(0, (1_700_000_000+250_000)*1000), result[1].Time, time.Millisecond)
		require.Equal(t, uint32(11000), result[1].RTTMicroseconds)

		require.Equal(t, 2, result[2].SampleIndex)
		require.WithinDuration(t, time.Unix(0, (1_700_000_000+500_000)*1000), result[2].Time, time.Millisecond)
		require.Equal(t, uint32(12000), result[2].RTTMicroseconds)
	})

	t.Run("handles empty samples", func(t *testing.T) {
		t.Parallel()

		samples := &telemetry.InternetLatencySamples{
			InternetLatencySamplesHeader: telemetry.InternetLatencySamplesHeader{
				StartTimestampMicroseconds:   1_700_000_000,
				SamplingIntervalMicroseconds: 250_000,
			},
			Samples: []uint32{},
		}

		result := convertInternetLatencySamples(samples, testPK(1), testPK(2), "provider", 0)
		require.Empty(t, result)
	})
}

func TestLake_TelemetryLatency_View_Refresh_SavesToDB(t *testing.T) {
	t.Parallel()

	t.Run("saves device-link samples to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// First, set up serviceability view with devices and links
		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{
							PubKey:            devicePK1,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV1",
							PublicIp:          publicIP1,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
						{
							PubKey:            devicePK2,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV2",
							PublicIp:          publicIP2,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey:            linkPK,
							Owner:             ownerPubkey,
							Status:            serviceability.LinkStatusActivated,
							Code:              "LINK1",
							TunnelNet:         tunnelNet,
							ContributorPubKey: contributorPK,
							SideAPubKey:       devicePK1,
							SideZPubKey:       devicePK2,
							SideAIfaceName:    "eth0",
							SideZIfaceName:    "eth1",
							LinkType:          serviceability.LinkLinkTypeWAN,
							DelayNs:           1000000,
							JitterNs:          50000,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Now set up telemetry view
		mockTelemetryRPC := &mockTelemetryRPC{}
		mockEpochRPC := &mockEpochRPC{}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               mockEpochRPC,
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		// Verify samples were saved (if any telemetry data was returned)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
	})

	t.Run("saves device-link latency samples to database", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// Set up serviceability view
		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{
							PubKey:            devicePK1,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV1",
							PublicIp:          publicIP1,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
						{
							PubKey:            devicePK2,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV2",
							PublicIp:          publicIP2,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey:            linkPK,
							Owner:             ownerPubkey,
							Status:            serviceability.LinkStatusActivated,
							Code:              "LINK1",
							TunnelNet:         tunnelNet,
							ContributorPubKey: contributorPK,
							SideAPubKey:       devicePK1,
							SideZPubKey:       devicePK2,
							SideAIfaceName:    "eth0",
							SideZIfaceName:    "eth1",
							LinkType:          serviceability.LinkLinkTypeWAN,
							DelayNs:           1000000,
							JitterNs:          50000,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Set up telemetry RPC to return samples
		originPK := solana.PublicKeyFromBytes(devicePK1[:])
		targetPK := solana.PublicKeyFromBytes(devicePK2[:])
		linkPKPubKey := solana.PublicKeyFromBytes(linkPK[:])

		mockTelemetryRPC := &mockTelemetryRPCWithSamples{
			samples: map[string]*telemetry.DeviceLatencySamples{
				key(originPK, targetPK, linkPKPubKey, 100): {
					DeviceLatencySamplesHeader: telemetry.DeviceLatencySamplesHeader{
						StartTimestampMicroseconds:   1_600_000_000_000_000, // 1.6 billion seconds in microseconds
						SamplingIntervalMicroseconds: 100_000,
					},
					Samples: []uint32{5000, 6000, 7000},
				},
			},
		}

		mockEpochRPC := &mockEpochRPCWithEpoch{epoch: 100}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               mockEpochRPC,
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		// Verify samples were saved
		var sampleCount uint64
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_device_link_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&sampleCount))
		rows.Close()
		require.Equal(t, uint64(3), sampleCount)

		var originDevicePK, targetDevicePK, linkPKStr string
		var epoch int64
		var sampleIndex int32
		var sampleTime time.Time
		var rttUs int64
		rows, err = conn.Query(ctx, "SELECT origin_device_pk, target_device_pk, link_pk, epoch, sample_index, event_ts, rtt_us FROM fact_dz_device_link_latency ORDER BY sample_index LIMIT 1")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&originDevicePK, &targetDevicePK, &linkPKStr, &epoch, &sampleIndex, &sampleTime, &rttUs))
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, solana.PublicKeyFromBytes(devicePK1[:]).String(), originDevicePK)
		require.Equal(t, solana.PublicKeyFromBytes(devicePK2[:]).String(), targetDevicePK)
		require.Equal(t, solana.PublicKeyFromBytes(linkPK[:]).String(), linkPKStr)
		require.Equal(t, int64(100), epoch)
		require.Equal(t, int32(0), sampleIndex)
		require.WithinDuration(t, time.Unix(1_600_000_000, 0).UTC(), sampleTime.UTC(), time.Second)
		require.Equal(t, int64(5000), rttUs)
	})

	t.Run("reads data back from database correctly", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// Set up serviceability view
		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{
							PubKey:            devicePK1,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV1",
							PublicIp:          publicIP1,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
						{
							PubKey:            devicePK2,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV2",
							PublicIp:          publicIP2,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey:            linkPK,
							Owner:             ownerPubkey,
							Status:            serviceability.LinkStatusActivated,
							Code:              "LINK1",
							TunnelNet:         tunnelNet,
							ContributorPubKey: contributorPK,
							SideAPubKey:       devicePK1,
							SideZPubKey:       devicePK2,
							SideAIfaceName:    "eth0",
							SideZIfaceName:    "eth1",
							LinkType:          serviceability.LinkLinkTypeWAN,
							DelayNs:           1000000,
							JitterNs:          50000,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Verify we can read devices back by querying the database directly
		var deviceCount uint64
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_devices_history
			)
			SELECT COUNT(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&deviceCount))
		rows.Close()
		require.Equal(t, uint64(2), deviceCount)

		// GetDevices/GetLinks/GetContributors methods were removed - verify using the dataset API
		// Verify devices exist
		device1PK := solana.PublicKeyFromBytes(devicePK1[:])
		deviceDataset, err := dzsvc.NewDeviceDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, deviceDataset)
		device1EntityID := dataset.NewNaturalKey(device1PK.String()).ToSurrogate()
		device1, err := deviceDataset.GetCurrentRow(ctx, conn, device1EntityID)
		require.NoError(t, err)
		require.NotNil(t, device1)
		require.Equal(t, "DEV1", device1["code"])

		device2PK := solana.PublicKeyFromBytes(devicePK2[:])
		device2EntityID := dataset.NewNaturalKey(device2PK.String()).ToSurrogate()
		device2, err := deviceDataset.GetCurrentRow(ctx, conn, device2EntityID)
		require.NoError(t, err)
		require.NotNil(t, device2)
		require.Equal(t, "DEV2", device2["code"])

		// Verify links exist
		linkPKPubkey := solana.PublicKeyFromBytes(linkPK[:])
		linkDataset, err := dzsvc.NewLinkDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, linkDataset)
		linkEntityID := dataset.NewNaturalKey(linkPKPubkey.String()).ToSurrogate()
		link, err := linkDataset.GetCurrentRow(ctx, conn, linkEntityID)
		require.NoError(t, err)
		require.NotNil(t, link)
		require.Equal(t, "LINK1", link["code"])

		// Verify contributors exist
		contributorPKPubkey := solana.PublicKeyFromBytes(contributorPK[:])
		contributorDataset, err := dzsvc.NewContributorDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, contributorDataset)
		contributorEntityID := dataset.NewNaturalKey(contributorPKPubkey.String()).ToSurrogate()
		contributor, err := contributorDataset.GetCurrentRow(ctx, conn, contributorEntityID)
		require.NoError(t, err)
		require.NotNil(t, contributor)
		require.Equal(t, "CONTRIB", contributor["code"])
	})
}

type mockTelemetryRPCWithSamples struct {
	samples map[string]*telemetry.DeviceLatencySamples
}

func (m *mockTelemetryRPCWithSamples) GetDeviceLatencySamplesTail(ctx context.Context, originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
	key := key(originDevicePK, targetDevicePK, linkPK, epoch)
	if samples, ok := m.samples[key]; ok {
		return &samples.DeviceLatencySamplesHeader, 0, samples.Samples, nil
	}
	return nil, 0, nil, telemetry.ErrAccountNotFound
}

func (m *mockTelemetryRPCWithSamples) GetInternetLatencySamples(ctx context.Context, dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error) {
	return nil, telemetry.ErrAccountNotFound
}

type mockEpochRPCWithEpoch struct {
	epoch uint64
}

func (m *mockEpochRPCWithEpoch) GetEpochInfo(ctx context.Context, commitment solanarpc.CommitmentType) (*solanarpc.GetEpochInfoResult, error) {
	return &solanarpc.GetEpochInfoResult{
		Epoch: m.epoch,
	}, nil
}

func key(origin, target, link solana.PublicKey, epoch uint64) string {
	return fmt.Sprintf("%s:%s:%s:%d", origin.String(), target.String(), link.String(), epoch)
}

func TestLake_TelemetryLatency_View_IncrementalAppend(t *testing.T) {
	t.Parallel()

	t.Run("device-link samples are appended incrementally", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// Set up serviceability view
		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{
							PubKey:            devicePK1,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV1",
							PublicIp:          publicIP1,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
						{
							PubKey:            devicePK2,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV2",
							PublicIp:          publicIP2,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey:            linkPK,
							Owner:             ownerPubkey,
							Status:            serviceability.LinkStatusActivated,
							Code:              "LINK1",
							TunnelNet:         tunnelNet,
							ContributorPubKey: contributorPK,
							SideAPubKey:       devicePK1,
							SideZPubKey:       devicePK2,
							SideAIfaceName:    "eth0",
							SideZIfaceName:    "eth1",
							LinkType:          serviceability.LinkLinkTypeWAN,
							DelayNs:           1000000,
							JitterNs:          50000,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Set up telemetry RPC to return samples with NextSampleIndex
		originPK := solana.PublicKeyFromBytes(devicePK1[:])
		targetPK := solana.PublicKeyFromBytes(devicePK2[:])
		linkPKPubKey := solana.PublicKeyFromBytes(linkPK[:])

		// Mock that simulates incremental samples: data source grows from 3 to 5 samples
		// First refresh: data source has 3 samples (0-2), existingMaxIdx=-1, return all 3
		// Second refresh: data source has 5 samples (0-4), existingMaxIdx=2, return tail (3-4)
		// Recent start timestamp so the samples land inside the 4-day
		// event_ts bound in GetExistingMaxSampleIndices and the second
		// refresh exercises the bounded scan itself, not only the
		// same-process max-index carry-forward cache (which would mask a
		// broken scan here but is empty after a real restart).
		startTsMicro := uint64(time.Now().Add(-1 * time.Hour).UnixMicro())

		mockTelemetryRPC := &mockTelemetryRPCWithIncrementalSamples{
			getSamplesFunc: func(originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
				sampleKey := key(originDevicePK, targetDevicePK, linkPK, epoch)
				expectedKey := key(originPK, targetPK, linkPKPubKey, 100)
				if sampleKey != expectedKey {
					return nil, 0, nil, telemetry.ErrAccountNotFound
				}

				// Simulate data source that grows: initially 3 samples, then 5 samples
				firstBatch := []uint32{5000, 6000, 7000}             // indices 0-2
				allSamples := []uint32{5000, 6000, 7000, 8000, 9000} // indices 0-4

				if existingMaxIdx < 0 {
					// First refresh: no existing data, data source has 3 samples
					return &telemetry.DeviceLatencySamplesHeader{
						StartTimestampMicroseconds:   startTsMicro,
						SamplingIntervalMicroseconds: 100_000,
						NextSampleIndex:              3, // 3 samples (indices 0, 1, 2)
					}, 0, firstBatch, nil
				} else {
					// Subsequent refresh: data source has grown to 5 samples
					// Return only the tail (samples after existingMaxIdx)
					startIdx := existingMaxIdx + 1
					if startIdx >= len(allSamples) {
						// No new samples
						return &telemetry.DeviceLatencySamplesHeader{
							StartTimestampMicroseconds:   startTsMicro,
							SamplingIntervalMicroseconds: 100_000,
							NextSampleIndex:              uint32(len(allSamples)),
						}, startIdx, nil, nil
					}

					tail := allSamples[startIdx:]
					return &telemetry.DeviceLatencySamplesHeader{
						StartTimestampMicroseconds:   startTsMicro,
						SamplingIntervalMicroseconds: 100_000,
						NextSampleIndex:              5, // 5 samples (indices 0-4)
					}, startIdx, tail, nil
				}
			},
		}

		mockEpochRPC := &mockEpochRPCWithEpoch{epoch: 100}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               mockEpochRPC,
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		// First refresh: should insert 3 samples
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		var sampleCount uint64
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_device_link_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&sampleCount)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(3), sampleCount, "first refresh should insert 3 samples")

		// Verify the first 3 samples are correct
		var maxIdx int32
		rows, err = conn.Query(ctx, "SELECT MAX(sample_index) FROM fact_dz_device_link_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&maxIdx)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int32(2), maxIdx, "max sample_index should be 2 after first refresh")

		// Second refresh: should append only the 2 new samples (indices 3-4)
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		conn, err = db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err = conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_device_link_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&sampleCount)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(5), sampleCount, "second refresh should append 2 more samples, total 5")

		// Verify all samples are present and correct
		var rttUs int64
		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_device_link_latency WHERE sample_index = 0")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.Equal(t, int64(5000), rttUs, "sample 0 should remain unchanged")

		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_device_link_latency WHERE sample_index = 2")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.Equal(t, int64(7000), rttUs, "sample 2 should remain unchanged")

		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_device_link_latency WHERE sample_index = 3")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.Equal(t, int64(8000), rttUs, "sample 3 should be newly inserted")

		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_device_link_latency WHERE sample_index = 4")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.Equal(t, int64(9000), rttUs, "sample 4 should be newly inserted")

		// Verify max index is now 4
		rows, err = conn.Query(ctx, "SELECT MAX(sample_index) FROM fact_dz_device_link_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&maxIdx)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int32(4), maxIdx, "max sample_index should be 4 after second refresh")
	})

	t.Run("internet-metro samples are appended incrementally", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// Set up serviceability view with metros
		metroPK1 := [32]byte{1, 2, 3, 4}
		metroPK2 := [32]byte{5, 6, 7, 8}
		contributorPK := [32]byte{13, 14, 15, 16}
		ownerPubkey := [32]byte{21, 22, 23, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{PubKey: [32]byte{99}, ContributorPubKey: contributorPK, ExchangePubKey: metroPK1},
					},
					Exchanges: []serviceability.Exchange{
						{
							PubKey: metroPK1,
							Owner:  ownerPubkey,
							Code:   "NYC",
							Name:   "New York",
							Status: serviceability.ExchangeStatusActivated,
							Lat:    40.7128,
							Lng:    -74.0060,
						},
						{
							PubKey: metroPK2,
							Owner:  ownerPubkey,
							Code:   "LAX",
							Name:   "Los Angeles",
							Status: serviceability.ExchangeStatusActivated,
							Lat:    34.0522,
							Lng:    -118.2437,
						},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Set up telemetry RPC to return internet samples with NextSampleIndex
		originPK := solana.PublicKeyFromBytes(metroPK1[:])
		targetPK := solana.PublicKeyFromBytes(metroPK2[:])
		agentPK := solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112")

		// Recent start timestamp so the samples land inside the 4-day
		// event_ts bound in GetExistingInternetMaxSampleIndices and the
		// second refresh exercises the bounded scan itself, not only the
		// same-process max-index carry-forward cache (which would mask a
		// broken scan here but is empty after a real restart).
		startTsMicro := uint64(time.Now().Add(-1 * time.Hour).UnixMicro())

		var refreshCount atomic.Int64
		mockTelemetryRPC := &mockTelemetryRPCWithIncrementalInternetSamples{
			getSamplesFunc: func(dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error) {
				// Metro pairs can be in either direction, so check both orderings
				matchesForward := originLocationPK.String() == originPK.String() && targetLocationPK.String() == targetPK.String()
				matchesReverse := originLocationPK.String() == targetPK.String() && targetLocationPK.String() == originPK.String()
				if (!matchesForward && !matchesReverse) || epoch != 100 {
					return nil, telemetry.ErrAccountNotFound
				}

				count := refreshCount.Add(1)
				if count == 1 {
					// First refresh: return samples 0-1
					return &telemetry.InternetLatencySamples{
						InternetLatencySamplesHeader: telemetry.InternetLatencySamplesHeader{
							StartTimestampMicroseconds:   startTsMicro,
							SamplingIntervalMicroseconds: 250_000,
							NextSampleIndex:              2, // 2 samples (indices 0, 1)
						},
						Samples: []uint32{10000, 11000},
					}, nil
				} else {
					// Second refresh: return samples 0-3 (new samples 2-3 added)
					return &telemetry.InternetLatencySamples{
						InternetLatencySamplesHeader: telemetry.InternetLatencySamplesHeader{
							StartTimestampMicroseconds:   startTsMicro,
							SamplingIntervalMicroseconds: 250_000,
							NextSampleIndex:              4, // 4 samples (indices 0-3)
						},
						Samples: []uint32{10000, 11000, 12000, 13000},
					}, nil
				}
			},
		}

		mockEpochRPC := &mockEpochRPCWithEpoch{epoch: 100}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               mockEpochRPC,
			MaxConcurrency:         32,
			InternetLatencyAgentPK: agentPK,
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		// First refresh: should insert 2 samples
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		var sampleCount uint64
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_internet_metro_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&sampleCount)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(2), sampleCount, "first refresh should insert 2 samples")

		// Verify the first 2 samples are correct
		var maxIdx int32
		rows, err = conn.Query(ctx, "SELECT MAX(sample_index) FROM fact_dz_internet_metro_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&maxIdx)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int32(1), maxIdx, "max sample_index should be 1 after first refresh")

		// Second refresh: should append only the 2 new samples (indices 2-3)
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		conn, err = db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err = conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_internet_metro_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&sampleCount)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(4), sampleCount, "second refresh should append 2 more samples, total 4")

		// Verify all samples are present and correct
		var rttUs int64
		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_internet_metro_latency WHERE sample_index = 0")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int64(10000), rttUs, "sample 0 should remain unchanged")

		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_internet_metro_latency WHERE sample_index = 1")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int64(11000), rttUs, "sample 1 should remain unchanged")

		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_internet_metro_latency WHERE sample_index = 2")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int64(12000), rttUs, "sample 2 should be newly inserted")

		rows, err = conn.Query(ctx, "SELECT rtt_us FROM fact_dz_internet_metro_latency WHERE sample_index = 3")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&rttUs))
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int64(13000), rttUs, "sample 3 should be newly inserted")

		// Verify max index is now 3
		rows, err = conn.Query(ctx, "SELECT MAX(sample_index) FROM fact_dz_internet_metro_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&maxIdx)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, int32(3), maxIdx, "max sample_index should be 3 after second refresh")
	})
}

type mockTelemetryRPCWithIncrementalSamples struct {
	getSamplesFunc func(originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error)
}

func (m *mockTelemetryRPCWithIncrementalSamples) GetDeviceLatencySamplesTail(ctx context.Context, originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
	if m.getSamplesFunc != nil {
		return m.getSamplesFunc(originDevicePK, targetDevicePK, linkPK, epoch, existingMaxIdx)
	}
	return nil, 0, nil, telemetry.ErrAccountNotFound
}

func (m *mockTelemetryRPCWithIncrementalSamples) GetInternetLatencySamples(ctx context.Context, dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error) {
	return nil, telemetry.ErrAccountNotFound
}

type mockTelemetryRPCWithIncrementalInternetSamples struct {
	getSamplesFunc func(dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error)
}

func (m *mockTelemetryRPCWithIncrementalInternetSamples) GetDeviceLatencySamplesTail(ctx context.Context, originDevicePK, targetDevicePK, linkPK solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
	return nil, 0, nil, telemetry.ErrAccountNotFound
}

func (m *mockTelemetryRPCWithIncrementalInternetSamples) GetInternetLatencySamples(ctx context.Context, dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error) {
	if m.getSamplesFunc != nil {
		return m.getSamplesFunc(dataProviderName, originLocationPK, targetLocationPK, agentPK, epoch)
	}
	return nil, telemetry.ErrAccountNotFound
}

func TestLake_TelemetryLatency_View_Refresh_ErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("device-link refresh returns error when GetExistingMaxSampleIndices fails", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// Set up serviceability view
		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{
							PubKey:            devicePK1,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV1",
							PublicIp:          publicIP1,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
						{
							PubKey:            devicePK2,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV2",
							PublicIp:          publicIP2,
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey:            linkPK,
							Owner:             ownerPubkey,
							Status:            serviceability.LinkStatusActivated,
							Code:              "LINK1",
							TunnelNet:         tunnelNet,
							ContributorPubKey: contributorPK,
							SideAPubKey:       devicePK1,
							SideZPubKey:       devicePK2,
							SideAIfaceName:    "eth0",
							SideZIfaceName:    "eth1",
							LinkType:          serviceability.LinkLinkTypeWAN,
							DelayNs:           1000000,
							JitterNs:          50000,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Set up telemetry RPC to return samples
		originPK := solana.PublicKeyFromBytes(devicePK1[:])
		targetPK := solana.PublicKeyFromBytes(devicePK2[:])
		linkPKPubKey := solana.PublicKeyFromBytes(linkPK[:])

		mockTelemetryRPC := &mockTelemetryRPCWithSamples{
			samples: map[string]*telemetry.DeviceLatencySamples{
				key(originPK, targetPK, linkPKPubKey, 100): {
					DeviceLatencySamplesHeader: telemetry.DeviceLatencySamplesHeader{
						StartTimestampMicroseconds:   1_600_000_000_000_000,
						SamplingIntervalMicroseconds: 100_000,
					},
					Samples: []uint32{5000, 6000, 7000},
				},
			},
		}

		mockEpochRPC := &mockEpochRPCWithEpoch{epoch: 100}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               mockEpochRPC,
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		// First refresh should succeed and insert samples
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		// Verify samples were inserted
		var sampleCount uint64
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_device_link_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&sampleCount)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(3), sampleCount, "first refresh should insert 3 samples")

		// Drop the table to cause GetExistingMaxSampleIndices to fail
		err = conn.Exec(ctx, "DROP TABLE IF EXISTS fact_dz_device_link_latency")
		require.NoError(t, err)

		// Second refresh should fail when GetExistingMaxSampleIndices fails
		// The key behavior is that it should NOT insert all samples when GetExistingMaxSampleIndices fails
		_, err = view.Refresh(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get existing max indices")

		// Verify no samples were inserted after error
		// This confirms that when GetExistingMaxSampleIndices fails, we don't proceed with insertion
		// Note: Since we dropped the table, it may not exist, which is fine - it means no samples were inserted
		var count uint64
		rows, err = conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_device_link_latency")
		if err != nil {
			// Table doesn't exist, which means no samples were inserted - that's what we want
			require.Contains(t, err.Error(), "Unknown table", "table should not exist after being dropped")
			count = 0
		} else {
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(&count))
			rows.Close()
		}
		require.Equal(t, uint64(0), count, "should have no samples after error")
	})

	t.Run("internet-metro refresh returns error when GetExistingInternetMaxSampleIndices fails", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		// Set up serviceability view with metros
		metroPK1 := [32]byte{1, 2, 3, 4}
		metroPK2 := [32]byte{5, 6, 7, 8}
		contributorPK := [32]byte{13, 14, 15, 16}
		ownerPubkey := [32]byte{21, 22, 23, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Code:   "CONTRIB",
						},
					},
					Devices: []serviceability.Device{
						{PubKey: [32]byte{99}, ContributorPubKey: contributorPK, ExchangePubKey: metroPK1},
					},
					Exchanges: []serviceability.Exchange{
						{
							PubKey: metroPK1,
							Owner:  ownerPubkey,
							Code:   "NYC",
							Name:   "New York",
							Status: serviceability.ExchangeStatusActivated,
							Lat:    40.7128,
							Lng:    -74.0060,
						},
						{
							PubKey: metroPK2,
							Owner:  ownerPubkey,
							Code:   "LAX",
							Name:   "Los Angeles",
							Status: serviceability.ExchangeStatusActivated,
							Lat:    34.0522,
							Lng:    -118.2437,
						},
					},
				}, nil
			},
		}

		geoipStore, err := newMockGeoIPStore(t)
		require.NoError(t, err)
		defer geoipStore.db.Close()

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		// Set up telemetry RPC to return internet samples
		originPK := solana.PublicKeyFromBytes(metroPK1[:])
		targetPK := solana.PublicKeyFromBytes(metroPK2[:])
		agentPK := solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112")

		mockTelemetryRPC := &mockTelemetryRPCWithIncrementalInternetSamples{
			getSamplesFunc: func(dataProviderName string, originLocationPK, targetLocationPK, agentPK solana.PublicKey, epoch uint64) (*telemetry.InternetLatencySamples, error) {
				// Metro pairs can be in either direction, so check both orderings
				matchesForward := originLocationPK.String() == originPK.String() && targetLocationPK.String() == targetPK.String()
				matchesReverse := originLocationPK.String() == targetPK.String() && targetLocationPK.String() == originPK.String()
				if (!matchesForward && !matchesReverse) || epoch != 100 {
					return nil, telemetry.ErrAccountNotFound
				}

				return &telemetry.InternetLatencySamples{
					InternetLatencySamplesHeader: telemetry.InternetLatencySamplesHeader{
						StartTimestampMicroseconds:   1_700_000_000,
						SamplingIntervalMicroseconds: 250_000,
						NextSampleIndex:              2,
					},
					Samples: []uint32{10000, 11000},
				}, nil
			},
		}

		mockEpochRPC := &mockEpochRPCWithEpoch{epoch: 100}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               mockEpochRPC,
			MaxConcurrency:         32,
			InternetLatencyAgentPK: agentPK,
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		// First refresh should succeed and insert samples
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		// Verify samples were inserted
		var sampleCount uint64
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		rows, err := conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_internet_metro_latency")
		require.NoError(t, err)
		require.True(t, rows.Next())
		err = rows.Scan(&sampleCount)
		rows.Close()
		require.NoError(t, err)
		require.Equal(t, uint64(2), sampleCount, "first refresh should insert 2 samples")

		// Drop the table to cause GetExistingInternetMaxSampleIndices to fail
		err = conn.Exec(ctx, "DROP TABLE IF EXISTS fact_dz_internet_metro_latency")
		require.NoError(t, err)

		// Second refresh should fail when GetExistingInternetMaxSampleIndices fails
		// The key behavior is that it should NOT insert all samples when GetExistingInternetMaxSampleIndices fails
		_, err = view.Refresh(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get existing max indices")

		// Verify no samples were inserted after error
		// This confirms that when GetExistingInternetMaxSampleIndices fails, we don't proceed with insertion
		// Note: Since we dropped the table, it may not exist, which is fine - it means no samples were inserted
		var count uint64
		rows, err = conn.Query(ctx, "SELECT COUNT(*) FROM fact_dz_internet_metro_latency")
		if err != nil {
			// Table doesn't exist, which means no samples were inserted - that's what we want
			require.Contains(t, err.Error(), "Unknown table", "table should not exist after being dropped")
			count = 0
		} else {
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(&count))
			rows.Close()
		}
		require.Equal(t, uint64(0), count, "should have no samples after error")
	})
}

func TestTrimNullBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"version string with trailing nulls", append([]byte("1.2.3"), make([]byte, 11)...), "1.2.3"},
		{"full array with no nulls", []byte("1234567890abcdef"), "1234567890abcdef"},
		{"all zeros", make([]byte, 16), ""},
		{"single char", append([]byte("v"), make([]byte, 15)...), "v"},
		{"commit hash", append([]byte("abc123de"), make([]byte, 0)...), "abc123de"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := trimNullBytes(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLake_TelemetryLatency_View_AgentVersionCommit(t *testing.T) {
	t.Parallel()

	t.Run("agent version and commit are stored in sample headers", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{PubKey: contributorPK, Owner: ownerPubkey, Code: "CONTRIB"},
					},
					Devices: []serviceability.Device{
						{
							PubKey: devicePK1, Owner: ownerPubkey,
							Status: serviceability.DeviceStatusActivated, DeviceType: serviceability.DeviceDeviceTypeHybrid,
							Code: "DEV1", PublicIp: publicIP1,
							ContributorPubKey: contributorPK, ExchangePubKey: metroPK,
						},
						{
							PubKey: devicePK2, Owner: ownerPubkey,
							Status: serviceability.DeviceStatusActivated, DeviceType: serviceability.DeviceDeviceTypeHybrid,
							Code: "DEV2", PublicIp: publicIP2,
							ContributorPubKey: contributorPK, ExchangePubKey: metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey: linkPK, Owner: ownerPubkey,
							Status: serviceability.LinkStatusActivated, Code: "LINK1",
							TunnelNet: tunnelNet, ContributorPubKey: contributorPK,
							SideAPubKey: devicePK1, SideZPubKey: devicePK2,
							SideAIfaceName: "eth0", SideZIfaceName: "eth1",
							LinkType: serviceability.LinkLinkTypeWAN,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		originPK := solana.PublicKeyFromBytes(devicePK1[:])
		targetPK := solana.PublicKeyFromBytes(devicePK2[:])
		linkPKPubKey := solana.PublicKeyFromBytes(linkPK[:])

		// Set up agent version and commit as null-padded byte arrays
		var agentVersion [16]uint8
		copy(agentVersion[:], "1.5.0")
		var agentCommit [8]uint8
		copy(agentCommit[:], "abc123d")

		mockTelemetryRPC := &mockTelemetryRPCWithSamples{
			samples: map[string]*telemetry.DeviceLatencySamples{
				key(originPK, targetPK, linkPKPubKey, 100): {
					DeviceLatencySamplesHeader: telemetry.DeviceLatencySamplesHeader{
						StartTimestampMicroseconds:   1_600_000_000_000_000,
						SamplingIntervalMicroseconds: 100_000,
						AgentVersion:                 agentVersion,
						AgentCommit:                  agentCommit,
					},
					Samples: []uint32{5000},
				},
			},
		}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               &mockEpochRPCWithEpoch{epoch: 100},
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		var storedVersion, storedCommit string
		rows, err := conn.Query(ctx, "SELECT agent_version, agent_commit FROM fact_dz_device_link_latency_sample_header LIMIT 1")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&storedVersion, &storedCommit))
		rows.Close()

		require.Equal(t, "1.5.0", storedVersion)
		require.Equal(t, "abc123d", storedCommit)
	})

	t.Run("zero-value agent fields produce empty strings", func(t *testing.T) {
		t.Parallel()

		db := testClient(t)

		devicePK1 := [32]byte{1, 2, 3, 4}
		devicePK2 := [32]byte{5, 6, 7, 8}
		linkPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		metroPK := [32]byte{17, 18, 19, 20}
		ownerPubkey := [32]byte{21, 22, 23, 24}
		publicIP1 := [4]byte{192, 168, 1, 1}
		publicIP2 := [4]byte{192, 168, 1, 2}
		tunnelNet := [5]byte{10, 0, 0, 0, 24}

		svcMockRPC := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{PubKey: contributorPK, Owner: ownerPubkey, Code: "CONTRIB"},
					},
					Devices: []serviceability.Device{
						{
							PubKey: devicePK1, Owner: ownerPubkey,
							Status: serviceability.DeviceStatusActivated, DeviceType: serviceability.DeviceDeviceTypeHybrid,
							Code: "DEV1", PublicIp: publicIP1,
							ContributorPubKey: contributorPK, ExchangePubKey: metroPK,
						},
						{
							PubKey: devicePK2, Owner: ownerPubkey,
							Status: serviceability.DeviceStatusActivated, DeviceType: serviceability.DeviceDeviceTypeHybrid,
							Code: "DEV2", PublicIp: publicIP2,
							ContributorPubKey: contributorPK, ExchangePubKey: metroPK,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey: linkPK, Owner: ownerPubkey,
							Status: serviceability.LinkStatusActivated, Code: "LINK1",
							TunnelNet: tunnelNet, ContributorPubKey: contributorPK,
							SideAPubKey: devicePK1, SideZPubKey: devicePK2,
							SideAIfaceName: "eth0", SideZIfaceName: "eth1",
							LinkType: serviceability.LinkLinkTypeWAN,
						},
					},
					Exchanges: []serviceability.Exchange{
						{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
					},
				}, nil
			},
		}

		svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: svcMockRPC,
			RefreshInterval:   time.Second,
			ClickHouse:        db,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = svcView.Refresh(ctx)
		require.NoError(t, err)

		originPK := solana.PublicKeyFromBytes(devicePK1[:])
		targetPK := solana.PublicKeyFromBytes(devicePK2[:])
		linkPKPubKey := solana.PublicKeyFromBytes(linkPK[:])

		// Zero-value agent version and commit (V0 accounts or pre-version agents)
		mockTelemetryRPC := &mockTelemetryRPCWithSamples{
			samples: map[string]*telemetry.DeviceLatencySamples{
				key(originPK, targetPK, linkPKPubKey, 100): {
					DeviceLatencySamplesHeader: telemetry.DeviceLatencySamplesHeader{
						StartTimestampMicroseconds:   1_600_000_000_000_000,
						SamplingIntervalMicroseconds: 100_000,
						// AgentVersion and AgentCommit are zero-value
					},
					Samples: []uint32{5000},
				},
			},
		}

		view, err := NewView(ViewConfig{
			Logger:                 laketesting.NewLogger(),
			Clock:                  clockwork.NewFakeClock(),
			TelemetryRPC:           mockTelemetryRPC,
			EpochRPC:               &mockEpochRPCWithEpoch{epoch: 100},
			MaxConcurrency:         32,
			InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
			InternetDataProviders:  []string{"test-provider"},
			ClickHouse:             db,
			Serviceability:         svcView,
			RefreshInterval:        time.Second,
		})
		require.NoError(t, err)

		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		var storedVersion, storedCommit string
		rows, err := conn.Query(ctx, "SELECT agent_version, agent_commit FROM fact_dz_device_link_latency_sample_header LIMIT 1")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&storedVersion, &storedCommit))
		rows.Close()

		require.Equal(t, "", storedVersion, "zero-value AgentVersion should produce empty string")
		require.Equal(t, "", storedCommit, "zero-value AgentCommit should produce empty string")
	})
}

// TestLake_TelemetryLatency_ClassifyFetchErr pins that shutdown is decided by the
// context, not by the error's shape.
//
// The regression this guards: an http.Client timeout also satisfies
// errors.Is(err, context.DeadlineExceeded), so classifying on the error read a slow
// endpoint as shutdown and returned from the worker goroutine — before the append of
// everything already collected for that circuit. A timeout therefore discarded earlier
// epochs' samples silently, with the refresh still reporting a clean cycle.
func TestLake_TelemetryLatency_ClassifyFetchErr(t *testing.T) {
	t.Parallel()

	// The exact error an http.Client timeout produces. Constructed rather than
	// hand-written so it keeps matching whatever net/http actually returns.
	clientTimeout := func() error {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(500 * time.Millisecond)
		}))
		defer srv.Close()
		_, err := (&http.Client{Timeout: 20 * time.Millisecond}).Get(srv.URL)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded),
			"precondition: a Client.Timeout abort must look like context.DeadlineExceeded, "+
				"otherwise this test is not exercising the bug")
		return err
	}()

	live := context.Background()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want fetchOutcome
	}{
		// The regression: live context, timeout-shaped error => a real failure to count,
		// not a shutdown to swallow.
		{"client timeout on a live context", live, clientTimeout, fetchUnclassified},
		{"bare deadline exceeded on a live context", live, context.DeadlineExceeded, fetchUnclassified},

		// Genuine shutdown, whatever the error looks like.
		{"cancelled context", cancelled, errors.New("read tcp: connection reset by peer"), fetchAbort},
		{"cancelled context with timeout error", cancelled, clientTimeout, fetchAbort},
		{"cancelled context with no account", cancelled, telemetry.ErrAccountNotFound, fetchAbort},

		// Expected miss: no telemetry account for this circuit/epoch. Routine, uncounted.
		{"no telemetry account", live, telemetry.ErrAccountNotFound, fetchExpectedMiss},
		{"wrapped no account", live, fmt.Errorf("epoch 42: %w", telemetry.ErrAccountNotFound), fetchExpectedMiss},

		// Everything else is counted.
		{"rpc throttle", live, errors.New("Too many requests for a specific RPC call"), fetchUnclassified},
		{"connection reset", live, errors.New("read tcp 10.0.0.1:8899: connection reset by peer"), fetchUnclassified},
		{"decode failure", live, errors.New("failed to deserialize account data"), fetchUnclassified},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyFetchErr(tt.ctx, tt.err); got != tt.want {
				t.Errorf("classifyFetchErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

// latencyFixture builds the serviceability state the latency views read: two activated
// devices joined by one WAN link, refreshed into ClickHouse. It returns the view and the
// circuit's three pubkeys. Nine tests in this file inline the same block; new tests
// should call this rather than add a tenth copy.
func latencyFixture(t *testing.T, db clickhouse.Client) (*dzsvc.View, solana.PublicKey, solana.PublicKey, solana.PublicKey) {
	t.Helper()
	devicePK1 := [32]byte{1, 2, 3, 4}
	devicePK2 := [32]byte{5, 6, 7, 8}
	linkPK := [32]byte{9, 10, 11, 12}
	contributorPK := [32]byte{13, 14, 15, 16}
	metroPK := [32]byte{17, 18, 19, 20}
	ownerPubkey := [32]byte{21, 22, 23, 24}
	publicIP1 := [4]byte{192, 168, 1, 1}
	publicIP2 := [4]byte{192, 168, 1, 2}
	tunnelNet := [5]byte{10, 0, 0, 0, 24}

	svcMockRPC := &MockServiceabilityRPC{
		getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
			return &serviceability.ProgramData{
				Contributors: []serviceability.Contributor{
					{
						PubKey: contributorPK,
						Owner:  ownerPubkey,
						Code:   "CONTRIB",
					},
				},
				Devices: []serviceability.Device{
					{
						PubKey:            devicePK1,
						Owner:             ownerPubkey,
						Status:            serviceability.DeviceStatusActivated,
						DeviceType:        serviceability.DeviceDeviceTypeHybrid,
						Code:              "DEV1",
						PublicIp:          publicIP1,
						ContributorPubKey: contributorPK,
						ExchangePubKey:    metroPK,
					},
					{
						PubKey:            devicePK2,
						Owner:             ownerPubkey,
						Status:            serviceability.DeviceStatusActivated,
						DeviceType:        serviceability.DeviceDeviceTypeHybrid,
						Code:              "DEV2",
						PublicIp:          publicIP2,
						ContributorPubKey: contributorPK,
						ExchangePubKey:    metroPK,
					},
				},
				Links: []serviceability.Link{
					{
						PubKey:            linkPK,
						Owner:             ownerPubkey,
						Status:            serviceability.LinkStatusActivated,
						Code:              "LINK1",
						TunnelNet:         tunnelNet,
						ContributorPubKey: contributorPK,
						SideAPubKey:       devicePK1,
						SideZPubKey:       devicePK2,
						SideAIfaceName:    "eth0",
						SideZIfaceName:    "eth1",
						LinkType:          serviceability.LinkLinkTypeWAN,
						DelayNs:           1000000,
						JitterNs:          50000,
					},
				},
				Exchanges: []serviceability.Exchange{
					{PubKey: metroPK, Code: "METRO1", Name: "Test Metro"},
				},
			}, nil
		},
	}

	geoipStore, err := newMockGeoIPStore(t)
	require.NoError(t, err)
	t.Cleanup(func() { geoipStore.db.Close() })

	svcView, err := dzsvc.NewView(dzsvc.ViewConfig{
		Logger:            laketesting.NewLogger(),
		Clock:             clockwork.NewFakeClock(),
		ServiceabilityRPC: svcMockRPC,
		RefreshInterval:   time.Second,
		ClickHouse:        db,
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = svcView.Refresh(ctx)
	require.NoError(t, err)

	// Set up telemetry RPC to return samples with NextSampleIndex
	originPK := solana.PublicKeyFromBytes(devicePK1[:])
	targetPK := solana.PublicKeyFromBytes(devicePK2[:])
	linkPKPubKey := solana.PublicKeyFromBytes(linkPK[:])
	return svcView, originPK, targetPK, linkPKPubKey
}

// TestLake_TelemetryLatency_View_TimeoutKeepsEarlierEpochSamples asserts the classifier
// change at the call site, which the pure-function test cannot reach.
//
// epochsToFetch is current-first, so epoch N is fetched before N-1. A fetch error that
// looks like context.DeadlineExceeded, which every http.Client timeout does, used to
// return from the worker goroutine. That return sits before the append, so epoch N's
// samples were already in hand and got discarded, and the refresh still reported
// success.
//
// Reverting view_device.go to the error-shape branch must fail this test. No other mock
// in this file returns a non-nil fetch error, so before this the call-site behaviour was
// never exercised at all.
func TestLake_TelemetryLatency_View_TimeoutKeepsEarlierEpochSamples(t *testing.T) {
	t.Parallel()

	const env = "fixture-env"
	db := testClient(t)
	svcView, originPK, targetPK, linkPKPubKey := latencyFixture(t, db)

	// A real Client.Timeout error, so this keeps matching whatever net/http returns.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer srv.Close()
	_, timeoutErr := (&http.Client{Timeout: 20 * time.Millisecond}).Get(srv.URL)
	require.Error(t, timeoutErr)
	require.True(t, errors.Is(timeoutErr, context.DeadlineExceeded),
		"precondition: the injected error must look like context.DeadlineExceeded")

	startTsMicro := uint64(time.Now().Add(-1 * time.Hour).UnixMicro())
	rpc := &mockTelemetryRPCWithIncrementalSamples{
		getSamplesFunc: func(o, tgt, l solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
			if o != originPK || tgt != targetPK || l != linkPKPubKey {
				return nil, 0, nil, telemetry.ErrAccountNotFound
			}
			if epoch == 99 {
				return nil, 0, nil, timeoutErr
			}
			return &telemetry.DeviceLatencySamplesHeader{
				StartTimestampMicroseconds:   startTsMicro,
				SamplingIntervalMicroseconds: 100_000,
				NextSampleIndex:              3,
			}, 0, []uint32{5000, 6000, 7000}, nil
		},
	}

	view, err := NewView(ViewConfig{
		Logger:                 laketesting.NewLogger(),
		Clock:                  clockwork.NewFakeClock(),
		TelemetryRPC:           rpc,
		EpochRPC:               &mockEpochRPCWithEpoch{epoch: 100},
		MaxConcurrency:         32,
		InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
		InternetDataProviders:  []string{"test-provider"},
		ClickHouse:             db,
		Serviceability:         svcView,
		RefreshInterval:        time.Second,
		DZEnv:                  env,
	})
	require.NoError(t, err)

	before := testutil.ToFloat64(metrics.TelemetryFetchSkipped.WithLabelValues(env, "device_link"))

	ctx := context.Background()
	_, err = view.Refresh(ctx)
	require.NoError(t, err, "one circuit timing out must not fail the refresh")

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	rows, err := conn.Query(ctx, "SELECT count() FROM fact_dz_device_link_latency WHERE epoch = 100")
	require.NoError(t, err)
	require.True(t, rows.Next())
	var n uint64
	require.NoError(t, rows.Scan(&n))
	rows.Close()
	require.EqualValues(t, 3, n,
		"epoch 100's samples were collected before epoch 99 timed out and must still be written; "+
			"0 means the timeout was read as shutdown and the goroutine returned before the append")

	after := testutil.ToFloat64(metrics.TelemetryFetchSkipped.WithLabelValues(env, "device_link"))
	require.Greater(t, after, before, "the skipped fetch must be counted under dz_env=%s", env)
}

// capturingHandler records the records a logger emits, so a test can assert on log
// output as well as on metrics. The bug this exists for was invisible to a
// counter-only assertion: the accounting for the per-refresh summary sat in the wrong
// branch, so the counter still moved and the summary WARN never fired.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

// find returns the first record at the given level whose message contains msg.
func (h *capturingHandler) find(level slog.Level, msg string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, msg) {
			return r, true
		}
	}
	return slog.Record{}, false
}

// TestLake_TelemetryLatency_View_SkippedFetchesReachOneWarn asserts the operator-facing
// half of the skip accounting: a skipped fetch must produce exactly one WARN for the
// refresh, carrying the count and a real error.
//
// Asserting the counter is not enough. The accounting that feeds this summary once sat in
// a branch that only runs when err == nil, so the counter moved, the summary never fired,
// and device-link fetch failures reached no WARN at all. Every metric assertion still
// passed.
func TestLake_TelemetryLatency_View_SkippedFetchesReachOneWarn(t *testing.T) {
	t.Parallel()

	db := testClient(t)
	svcView, originPK, targetPK, linkPKPubKey := latencyFixture(t, db)

	capture := &capturingHandler{}
	injected := errors.New("Too many requests for a specific RPC call")

	rpc := &mockTelemetryRPCWithIncrementalSamples{
		getSamplesFunc: func(o, tgt, l solana.PublicKey, epoch uint64, existingMaxIdx int) (*telemetry.DeviceLatencySamplesHeader, int, []uint32, error) {
			if o != originPK || tgt != targetPK || l != linkPKPubKey {
				return nil, 0, nil, telemetry.ErrAccountNotFound
			}
			return nil, 0, nil, injected
		},
	}

	view, err := NewView(ViewConfig{
		Logger:                 slog.New(capture),
		Clock:                  clockwork.NewFakeClock(),
		TelemetryRPC:           rpc,
		EpochRPC:               &mockEpochRPCWithEpoch{epoch: 100},
		MaxConcurrency:         32,
		InternetLatencyAgentPK: solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112"),
		InternetDataProviders:  []string{"test-provider"},
		ClickHouse:             db,
		Serviceability:         svcView,
		RefreshInterval:        time.Second,
		DZEnv:                  "fixture-env",
	})
	require.NoError(t, err)

	_, err = view.Refresh(context.Background())
	require.NoError(t, err)

	rec, ok := capture.find(slog.LevelWarn, "sample fetches skipped")
	require.True(t, ok, "no summary WARN was emitted for a skipped fetch; "+
		"the per-fetch line is Debug, so without this the failure is invisible at WARN")

	var skipped int64
	var firstErr string
	rec.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "skipped":
			skipped = a.Value.Int64()
		case "first_error":
			firstErr = a.Value.String()
		}
		return true
	})
	require.Positive(t, skipped, "the summary WARN must carry a non-zero skip count")
	require.Contains(t, firstErr, "Too many requests",
		"the summary WARN must carry the real first error, not <nil>")

	// Exactly one summary line per source per refresh, not one per fetch.
	capture.mu.Lock()
	var warns int
	for _, r := range capture.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, "sample fetches skipped") {
			warns++
		}
	}
	capture.mu.Unlock()
	require.Equal(t, 1, warns, "expected one summary WARN for device-link, got %d", warns)
}
