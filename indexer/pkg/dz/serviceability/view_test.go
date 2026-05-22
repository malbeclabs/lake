package dzsvc

import (
	"context"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/jonboulle/clockwork"
	"github.com/malbeclabs/lake/indexer/pkg/clickhouse/dataset"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/require"

	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/serviceability"
)

type MockServiceabilityRPC struct {
	getProgramDataFunc func(context.Context) (*serviceability.ProgramData, error)
}

func (m *MockServiceabilityRPC) GetProgramData(ctx context.Context) (*serviceability.ProgramData, error) {
	if m.getProgramDataFunc != nil {
		return m.getProgramDataFunc(ctx)
	}
	// Return minimal valid data to pass empty snapshot validation
	return &serviceability.ProgramData{
		Contributors: []serviceability.Contributor{{PubKey: [32]byte{1}, Code: "test"}},
		Devices:      []serviceability.Device{{PubKey: [32]byte{2}, Code: "test-device", Status: serviceability.DeviceStatusActivated}},
		Exchanges:    []serviceability.Exchange{{PubKey: [32]byte{3}, Code: "test-metro", Name: "Test Metro"}},
	}, nil
}

func TestLake_Serviceability_View_Ready(t *testing.T) {
	t.Parallel()

	t.Run("returns false when not ready", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		require.False(t, view.Ready(), "view should not be ready before first refresh")
	})

	t.Run("returns true after successful refresh", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		require.True(t, view.Ready(), "view should be ready after successful refresh")
	})
}

func TestLake_Serviceability_View_WaitReady(t *testing.T) {
	t.Parallel()

	t.Run("returns immediately when already ready", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		err = view.WaitReady(ctx)
		require.NoError(t, err, "WaitReady should return immediately when already ready")
	})

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = view.WaitReady(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled")
	})
}

func TestLake_Serviceability_View_NewServiceabilityView(t *testing.T) {
	t.Parallel()

	t.Run("succeeds (tables are now created automatically by SCD2)", func(t *testing.T) {
		t.Parallel()

		// View creation should succeed (tables are created automatically by SCDTableViaCSV)
		// The failure will happen later when trying to use the database
		mockDB := testClient(t)
		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: &MockServiceabilityRPC{},
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)
		require.NotNil(t, view)
	})
}

func TestLake_Serviceability_View_ConvertContributors(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain contributors to domain types", func(t *testing.T) {
		t.Parallel()

		pk := [32]byte{1, 2, 3, 4}
		owner := [32]byte{5, 6, 7, 8}

		onchain := []serviceability.Contributor{
			{
				PubKey: pk,
				Owner:  owner,
				Status: serviceability.ContributorStatusActivated,
				Code:   "TEST",
			},
		}

		result := convertContributors(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(pk[:]).String(), result[0].PK)
		require.Equal(t, "TEST", result[0].Code)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		t.Parallel()

		result := convertContributors([]serviceability.Contributor{})
		require.Empty(t, result)
	})
}

func TestLake_Serviceability_View_ConvertDevices(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain devices to domain types", func(t *testing.T) {
		t.Parallel()

		pk := [32]byte{1, 2, 3, 4}
		owner := [32]byte{5, 6, 7, 8}
		contributorPK := [32]byte{9, 10, 11, 12}
		exchangePK := [32]byte{13, 14, 15, 16}
		publicIP := [4]byte{192, 168, 1, 1}

		onchain := []serviceability.Device{
			{
				PubKey:            pk,
				Owner:             owner,
				Status:            serviceability.DeviceStatusActivated,
				Code:              "DEV001",
				PublicIp:          publicIP,
				ContributorPubKey: contributorPK,
				ExchangePubKey:    exchangePK,
			},
		}

		result := convertDevices(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(pk[:]).String(), result[0].PK)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, "DEV001", result[0].Code)
		require.Equal(t, "192.168.1.1", result[0].PublicIP)
		require.Equal(t, solana.PublicKeyFromBytes(contributorPK[:]).String(), result[0].ContributorPK)
		require.Equal(t, solana.PublicKeyFromBytes(exchangePK[:]).String(), result[0].MetroPK)
	})
}

func TestLake_Serviceability_View_ConvertUsers(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain users to domain types", func(t *testing.T) {
		t.Parallel()

		pk := [32]byte{1, 2, 3, 4}
		owner := [32]byte{5, 6, 7, 8}

		onchain := []serviceability.User{
			{
				PubKey:   pk,
				Owner:    owner,
				Status:   serviceability.UserStatusActivated,
				UserType: serviceability.UserTypeIBRL,
			},
		}

		result := convertUsers(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(pk[:]).String(), result[0].PK)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, "ibrl", result[0].Kind)
	})
}

func TestLake_Serviceability_View_ConvertMetros(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain exchanges to domain metros", func(t *testing.T) {
		t.Parallel()

		pk := [32]byte{1, 2, 3, 4}
		owner := [32]byte{5, 6, 7, 8}

		onchain := []serviceability.Exchange{
			{
				PubKey: pk,
				Owner:  owner,
				Status: serviceability.ExchangeStatusActivated,
				Code:   "NYC",
				Name:   "New York",
				Lat:    40.7128,
				Lng:    -74.0060,
			},
		}

		result := convertMetros(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(pk[:]).String(), result[0].PK)
		require.Equal(t, "NYC", result[0].Code)
		require.Equal(t, "New York", result[0].Name)
		require.Equal(t, -74.0060, result[0].Longitude)
		require.Equal(t, 40.7128, result[0].Latitude)
	})
}

func TestLake_Serviceability_View_ConvertLinks(t *testing.T) {
	t.Parallel()

	t.Run("converts onchain links to domain types", func(t *testing.T) {
		t.Parallel()

		pk := [32]byte{1, 2, 3, 4}
		sideAPK := [32]byte{5, 6, 7, 8}
		sideZPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		// TunnelNet: [192, 168, 1, 0, 24] = 192.168.1.0/24
		tunnelNet := [5]uint8{192, 168, 1, 0, 24}

		onchain := []serviceability.Link{
			{
				PubKey:            pk,
				Status:            serviceability.LinkStatusActivated,
				Code:              "LINK001",
				TunnelNet:         tunnelNet,
				SideAPubKey:       sideAPK,
				SideZPubKey:       sideZPK,
				ContributorPubKey: contributorPK,
				SideAIfaceName:    "eth0",
				SideZIfaceName:    "eth1",
				LinkType:          serviceability.LinkLinkTypeWAN,
				DelayNs:           5000000,    // 5ms (onchain field name)
				JitterNs:          1000000,    // 1ms (onchain field name)
				Bandwidth:         1000000000, // 1 Gbps
				DelayOverrideNs:   0,          // onchain field name
			},
		}

		result := convertLinks(onchain, nil, nil)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(pk[:]).String(), result[0].PK)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, "LINK001", result[0].Code)
		require.Equal(t, "192.168.1.0/24", result[0].TunnelNet)
		require.Equal(t, solana.PublicKeyFromBytes(sideAPK[:]).String(), result[0].SideAPK)
		require.Equal(t, solana.PublicKeyFromBytes(sideZPK[:]).String(), result[0].SideZPK)
		require.Equal(t, solana.PublicKeyFromBytes(contributorPK[:]).String(), result[0].ContributorPK)
		require.Equal(t, "eth0", result[0].SideAIfaceName)
		require.Equal(t, "eth1", result[0].SideZIfaceName)
		require.Equal(t, "WAN", result[0].LinkType)
		require.Equal(t, uint64(5000000), result[0].CommittedRTTNs)
		require.Equal(t, uint64(1000000), result[0].CommittedJitterNs)
		require.Equal(t, uint64(1000000000), result[0].Bandwidth)
		require.Equal(t, uint64(0), result[0].ISISDelayOverrideNs)
	})

	t.Run("handles empty slice", func(t *testing.T) {
		t.Parallel()

		result := convertLinks([]serviceability.Link{}, nil, nil)
		require.Empty(t, result)
	})

	t.Run("extracts interface IPs from devices", func(t *testing.T) {
		t.Parallel()

		linkPK := [32]byte{1, 2, 3, 4}
		deviceAPK := [32]byte{5, 6, 7, 8}
		deviceZPK := [32]byte{9, 10, 11, 12}
		contributorPK := [32]byte{13, 14, 15, 16}
		tunnelNet := [5]uint8{192, 168, 1, 0, 24}

		links := []serviceability.Link{
			{
				PubKey:            linkPK,
				Status:            serviceability.LinkStatusActivated,
				Code:              "LINK001",
				TunnelNet:         tunnelNet,
				SideAPubKey:       deviceAPK,
				SideZPubKey:       deviceZPK,
				ContributorPubKey: contributorPK,
				SideAIfaceName:    "Port-Channel1000.2029",
				SideZIfaceName:    "Ethernet10/1",
				LinkType:          serviceability.LinkLinkTypeWAN,
			},
		}

		devices := []serviceability.Device{
			{
				PubKey: deviceAPK,
				Code:   "bom001-dz001",
				Interfaces: []serviceability.Interface{
					{
						Name:  "Loopback255",
						IpNet: [5]uint8{172, 16, 0, 79, 32}, // 172.16.0.79/32
					},
					{
						Name:  "Port-Channel1000.2029",
						IpNet: [5]uint8{172, 16, 0, 225, 31}, // 172.16.0.225/31
					},
				},
			},
			{
				PubKey: deviceZPK,
				Code:   "dz-slc-sw01",
				Interfaces: []serviceability.Interface{
					{
						Name:  "Loopback255",
						IpNet: [5]uint8{172, 16, 0, 245, 32}, // 172.16.0.245/32
					},
					{
						Name:  "Ethernet10/1",
						IpNet: [5]uint8{172, 16, 1, 32, 31}, // 172.16.1.32/31
					},
				},
			},
		}

		result := convertLinks(links, devices, nil)

		require.Len(t, result, 1)
		require.Equal(t, "172.16.0.225", result[0].SideAIP)
		require.Equal(t, "172.16.1.32", result[0].SideZIP)
	})
}

func TestLake_Serviceability_View_ConvertDeviceInterfaces(t *testing.T) {
	t.Parallel()

	t.Run("1-interface V1 device", func(t *testing.T) {
		t.Parallel()

		devicePK := [32]byte{1, 2, 3, 4}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version:       0,
						Name:          "Loopback0",
						Status:        serviceability.InterfaceStatusInvalid,
						InterfaceType: serviceability.InterfaceTypeInvalid,
						IpNet:         [5]uint8{10, 0, 0, 1, 32},
						// V2+ fields left at zero values
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(devicePK[:]).String(), result[0].DevicePK)
		require.Equal(t, "Loopback0", result[0].Intf)
		require.Equal(t, "invalid", result[0].Status)
		require.Equal(t, "invalid", result[0].InterfaceType)
		// V1 zero-value enum outputs
		require.Equal(t, "none", result[0].CYOAType)
		require.Equal(t, "none", result[0].DIAType)
		require.Equal(t, "none", result[0].LoopbackType)
		require.Equal(t, "static", result[0].RoutingMode)
		require.Equal(t, uint64(0), result[0].Bandwidth)
		require.Equal(t, uint64(0), result[0].Cir)
		require.Equal(t, uint16(0), result[0].Mtu)
		require.Equal(t, uint16(0), result[0].VlanID)
		require.Equal(t, uint16(0), result[0].NodeSegmentIdx)
		require.False(t, result[0].UserTunnelEndpoint)
	})

	t.Run("2-interface V1 device", func(t *testing.T) {
		t.Parallel()

		devicePK := [32]byte{2, 3, 4, 5}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version: 0,
						Name:    "Loopback0",
						Status:  serviceability.InterfaceStatusActivated,
						IpNet:   [5]uint8{10, 0, 0, 1, 32},
					},
					{
						Version: 0,
						Name:    "Ethernet1/1",
						Status:  serviceability.InterfaceStatusActivated,
						IpNet:   [5]uint8{172, 16, 0, 1, 31},
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		require.Len(t, result, 2)
		expectedPK := solana.PublicKeyFromBytes(devicePK[:]).String()
		require.Equal(t, expectedPK, result[0].DevicePK)
		require.Equal(t, expectedPK, result[1].DevicePK)
		require.Equal(t, "Loopback0", result[0].Intf)
		require.Equal(t, "Ethernet1/1", result[1].Intf)
	})

	t.Run("1-interface V2 device", func(t *testing.T) {
		t.Parallel()

		devicePK := [32]byte{3, 4, 5, 6}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version:            1,
						Name:               "Port-Channel1000.2029",
						Status:             serviceability.InterfaceStatusActivated,
						InterfaceType:      serviceability.InterfaceTypePhysical,
						InterfaceCYOA:      serviceability.InterfaceCYOAGREOverDIA,
						InterfaceDIA:       serviceability.InterfaceDIADIA,
						LoopbackType:       serviceability.LoopbackTypeVpnv4,
						RoutingMode:        serviceability.RoutingModeBGP,
						Bandwidth:          10_000_000_000,
						Cir:                5_000_000_000,
						Mtu:                9000,
						VlanId:             100,
						IpNet:              [5]uint8{172, 16, 0, 225, 31},
						NodeSegmentIdx:     42,
						UserTunnelEndpoint: true,
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(devicePK[:]).String(), result[0].DevicePK)
		require.Equal(t, "Port-Channel1000.2029", result[0].Intf)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, "physical", result[0].InterfaceType)
		require.Equal(t, "gre_over_dia", result[0].CYOAType)
		require.Equal(t, "dia", result[0].DIAType)
		require.Equal(t, "vpnv4", result[0].LoopbackType)
		require.Equal(t, "bgp", result[0].RoutingMode)
		require.Equal(t, uint64(10_000_000_000), result[0].Bandwidth)
		require.Equal(t, uint64(5_000_000_000), result[0].Cir)
		require.Equal(t, uint16(9000), result[0].Mtu)
		require.Equal(t, uint16(100), result[0].VlanID)
		require.Equal(t, uint16(42), result[0].NodeSegmentIdx)
		require.True(t, result[0].UserTunnelEndpoint)
	})

	t.Run("2-interface V2 device", func(t *testing.T) {
		t.Parallel()

		devicePK := [32]byte{4, 5, 6, 7}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version:       1,
						Name:          "Loopback255",
						Status:        serviceability.InterfaceStatusActivated,
						InterfaceType: serviceability.InterfaceTypeLoopback,
						LoopbackType:  serviceability.LoopbackTypeIpv4,
						RoutingMode:   serviceability.RoutingModeStatic,
						IpNet:         [5]uint8{172, 16, 0, 79, 32},
					},
					{
						Version:       1,
						Name:          "Ethernet10/1",
						Status:        serviceability.InterfaceStatusActivated,
						InterfaceType: serviceability.InterfaceTypePhysical,
						RoutingMode:   serviceability.RoutingModeBGP,
						Bandwidth:     1_000_000_000,
						Cir:           500_000_000,
						Mtu:           1500,
						VlanId:        200,
						IpNet:         [5]uint8{172, 16, 1, 32, 31},
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		require.Len(t, result, 2)
		expectedPK := solana.PublicKeyFromBytes(devicePK[:]).String()
		require.Equal(t, expectedPK, result[0].DevicePK)
		require.Equal(t, expectedPK, result[1].DevicePK)
		require.Equal(t, "Loopback255", result[0].Intf)
		require.Equal(t, "loopback", result[0].InterfaceType)
		require.Equal(t, "ipv4", result[0].LoopbackType)
		require.Equal(t, "static", result[0].RoutingMode)
		require.Equal(t, "Ethernet10/1", result[1].Intf)
		require.Equal(t, "physical", result[1].InterfaceType)
		require.Equal(t, "bgp", result[1].RoutingMode)
		require.Equal(t, uint64(1_000_000_000), result[1].Bandwidth)
		require.Equal(t, uint16(1500), result[1].Mtu)
		require.Equal(t, uint16(200), result[1].VlanID)
	})

	t.Run("1-interface V3 device with empty FlexAlgoNodeSegments", func(t *testing.T) {
		t.Parallel()

		devicePK := [32]byte{5, 6, 7, 8}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version:              3,
						Name:                 "Ethernet1/1",
						Status:               serviceability.InterfaceStatusActivated,
						InterfaceType:        serviceability.InterfaceTypePhysical,
						InterfaceCYOA:        serviceability.InterfaceCYOANone,
						InterfaceDIA:         serviceability.InterfaceDIANone,
						RoutingMode:          serviceability.RoutingModeBGP,
						Bandwidth:            100_000_000_000,
						Cir:                  50_000_000_000,
						Mtu:                  9216,
						IpNet:                [5]uint8{10, 1, 0, 1, 30},
						NodeSegmentIdx:       100,
						UserTunnelEndpoint:   true,
						FlexAlgoNodeSegments: []serviceability.FlexAlgoNodeSegment{},
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(devicePK[:]).String(), result[0].DevicePK)
		require.Equal(t, "Ethernet1/1", result[0].Intf)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, "physical", result[0].InterfaceType)
		require.Equal(t, "none", result[0].CYOAType)
		require.Equal(t, "none", result[0].DIAType)
		require.Equal(t, "bgp", result[0].RoutingMode)
		require.Equal(t, uint64(100_000_000_000), result[0].Bandwidth)
		require.Equal(t, uint64(50_000_000_000), result[0].Cir)
		require.Equal(t, uint16(9216), result[0].Mtu)
		require.Equal(t, uint16(100), result[0].NodeSegmentIdx)
		require.True(t, result[0].UserTunnelEndpoint)
	})

	t.Run("1-interface V3 device with non-empty FlexAlgoNodeSegments", func(t *testing.T) {
		t.Parallel()

		devicePK := [32]byte{6, 7, 8, 9}
		topologyPK := [32]byte{0xAA, 0xBB, 0xCC, 0xDD}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version:            3,
						Name:               "Ethernet2/1",
						Status:             serviceability.InterfaceStatusActivated,
						InterfaceType:      serviceability.InterfaceTypePhysical,
						RoutingMode:        serviceability.RoutingModeBGP,
						Bandwidth:          100_000_000_000,
						Mtu:                9216,
						NodeSegmentIdx:     200,
						UserTunnelEndpoint: false,
						FlexAlgoNodeSegments: []serviceability.FlexAlgoNodeSegment{
							{Topology: topologyPK, NodeSegmentIdx: 300},
						},
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		// FlexAlgoNodeSegments is not consumed by convertDeviceInterfaces;
		// the conversion should still succeed with the standard fields.
		require.Len(t, result, 1)
		require.Equal(t, solana.PublicKeyFromBytes(devicePK[:]).String(), result[0].DevicePK)
		require.Equal(t, "Ethernet2/1", result[0].Intf)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, uint64(100_000_000_000), result[0].Bandwidth)
		require.Equal(t, uint16(9216), result[0].Mtu)
		require.Equal(t, uint16(200), result[0].NodeSegmentIdx)
		require.False(t, result[0].UserTunnelEndpoint)
	})

	t.Run("2-interface V3 device", func(t *testing.T) {
		t.Parallel()

		// This is the case that broke production: multi-interface devices with V3 layout
		// deserialized at wrong offsets starting from the second interface when using
		// an SDK that didn't know the V3 Interface layout.
		devicePK := [32]byte{7, 8, 9, 10}
		topologyPK := [32]byte{0xDE, 0xAD, 0xBE, 0xEF}
		onchain := []serviceability.Device{
			{
				PubKey: devicePK,
				Interfaces: []serviceability.Interface{
					{
						Version:            3,
						Name:               "Loopback255",
						Status:             serviceability.InterfaceStatusActivated,
						InterfaceType:      serviceability.InterfaceTypeLoopback,
						LoopbackType:       serviceability.LoopbackTypeVpnv4,
						RoutingMode:        serviceability.RoutingModeStatic,
						IpNet:              [5]uint8{172, 16, 0, 1, 32},
						NodeSegmentIdx:     10,
						UserTunnelEndpoint: false,
						FlexAlgoNodeSegments: []serviceability.FlexAlgoNodeSegment{
							{Topology: topologyPK, NodeSegmentIdx: 110},
						},
					},
					{
						Version:            3,
						Name:               "Ethernet50/1",
						Status:             serviceability.InterfaceStatusActivated,
						InterfaceType:      serviceability.InterfaceTypePhysical,
						InterfaceCYOA:      serviceability.InterfaceCYOAGREOverFabric,
						InterfaceDIA:       serviceability.InterfaceDIANone,
						RoutingMode:        serviceability.RoutingModeBGP,
						Bandwidth:          400_000_000_000,
						Cir:                200_000_000_000,
						Mtu:                9216,
						VlanId:             500,
						IpNet:              [5]uint8{10, 0, 0, 1, 31},
						NodeSegmentIdx:     20,
						UserTunnelEndpoint: true,
						FlexAlgoNodeSegments: []serviceability.FlexAlgoNodeSegment{
							{Topology: topologyPK, NodeSegmentIdx: 120},
						},
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		require.Len(t, result, 2)
		expectedPK := solana.PublicKeyFromBytes(devicePK[:]).String()
		// First interface
		require.Equal(t, expectedPK, result[0].DevicePK)
		require.Equal(t, "Loopback255", result[0].Intf)
		require.Equal(t, "activated", result[0].Status)
		require.Equal(t, "loopback", result[0].InterfaceType)
		require.Equal(t, "vpnv4", result[0].LoopbackType)
		require.Equal(t, "static", result[0].RoutingMode)
		require.Equal(t, uint16(10), result[0].NodeSegmentIdx)
		require.False(t, result[0].UserTunnelEndpoint)
		// Second interface — the one that broke in production
		require.Equal(t, expectedPK, result[1].DevicePK)
		require.Equal(t, "Ethernet50/1", result[1].Intf)
		require.Equal(t, "activated", result[1].Status)
		require.Equal(t, "physical", result[1].InterfaceType)
		require.Equal(t, "gre_over_fabric", result[1].CYOAType)
		require.Equal(t, "none", result[1].DIAType)
		require.Equal(t, "bgp", result[1].RoutingMode)
		require.Equal(t, uint64(400_000_000_000), result[1].Bandwidth)
		require.Equal(t, uint64(200_000_000_000), result[1].Cir)
		require.Equal(t, uint16(9216), result[1].Mtu)
		require.Equal(t, uint16(500), result[1].VlanID)
		require.Equal(t, uint16(20), result[1].NodeSegmentIdx)
		require.True(t, result[1].UserTunnelEndpoint)
	})

	t.Run("empty device slice", func(t *testing.T) {
		t.Parallel()

		result := convertDeviceInterfaces([]serviceability.Device{})
		require.Empty(t, result)
	})

	t.Run("multiple devices with mixed versions", func(t *testing.T) {
		t.Parallel()

		devicePK1 := [32]byte{11, 12, 13, 14}
		devicePK2 := [32]byte{15, 16, 17, 18}
		devicePK3 := [32]byte{19, 20, 21, 22}
		topologyPK := [32]byte{0xCA, 0xFE}

		onchain := []serviceability.Device{
			{
				// V1 device with 1 interface
				PubKey: devicePK1,
				Interfaces: []serviceability.Interface{
					{
						Version: 0,
						Name:    "Loopback0",
						Status:  serviceability.InterfaceStatusActivated,
						IpNet:   [5]uint8{10, 0, 0, 1, 32},
					},
				},
			},
			{
				// V2 device with 2 interfaces
				PubKey: devicePK2,
				Interfaces: []serviceability.Interface{
					{
						Version:       1,
						Name:          "Loopback255",
						Status:        serviceability.InterfaceStatusActivated,
						InterfaceType: serviceability.InterfaceTypeLoopback,
						LoopbackType:  serviceability.LoopbackTypeIpv4,
						IpNet:         [5]uint8{172, 16, 0, 50, 32},
					},
					{
						Version:       1,
						Name:          "Ethernet1/1",
						Status:        serviceability.InterfaceStatusActivated,
						InterfaceType: serviceability.InterfaceTypePhysical,
						Bandwidth:     10_000_000_000,
						Mtu:           9000,
						IpNet:         [5]uint8{172, 16, 1, 1, 31},
					},
				},
			},
			{
				// V3 device with 2 interfaces
				PubKey: devicePK3,
				Interfaces: []serviceability.Interface{
					{
						Version:       3,
						Name:          "Loopback255",
						Status:        serviceability.InterfaceStatusActivated,
						InterfaceType: serviceability.InterfaceTypeLoopback,
						LoopbackType:  serviceability.LoopbackTypeVpnv4,
						IpNet:         [5]uint8{172, 16, 0, 99, 32},
						FlexAlgoNodeSegments: []serviceability.FlexAlgoNodeSegment{
							{Topology: topologyPK, NodeSegmentIdx: 500},
						},
					},
					{
						Version:            3,
						Name:               "Ethernet50/1",
						Status:             serviceability.InterfaceStatusActivated,
						InterfaceType:      serviceability.InterfaceTypePhysical,
						RoutingMode:        serviceability.RoutingModeBGP,
						Bandwidth:          100_000_000_000,
						Mtu:                9216,
						NodeSegmentIdx:     77,
						UserTunnelEndpoint: true,
						FlexAlgoNodeSegments: []serviceability.FlexAlgoNodeSegment{
							{Topology: topologyPK, NodeSegmentIdx: 600},
						},
					},
				},
			},
		}

		result := convertDeviceInterfaces(onchain)

		// 1 + 2 + 2 = 5 total interfaces flattened across 3 devices
		require.Len(t, result, 5)

		expectedPK1 := solana.PublicKeyFromBytes(devicePK1[:]).String()
		expectedPK2 := solana.PublicKeyFromBytes(devicePK2[:]).String()
		expectedPK3 := solana.PublicKeyFromBytes(devicePK3[:]).String()

		// Device 1 (V1): 1 interface
		require.Equal(t, expectedPK1, result[0].DevicePK)
		require.Equal(t, "Loopback0", result[0].Intf)
		require.Equal(t, "static", result[0].RoutingMode) // V1 zero-value

		// Device 2 (V2): 2 interfaces
		require.Equal(t, expectedPK2, result[1].DevicePK)
		require.Equal(t, "Loopback255", result[1].Intf)
		require.Equal(t, "ipv4", result[1].LoopbackType)
		require.Equal(t, expectedPK2, result[2].DevicePK)
		require.Equal(t, "Ethernet1/1", result[2].Intf)
		require.Equal(t, uint64(10_000_000_000), result[2].Bandwidth)
		require.Equal(t, uint16(9000), result[2].Mtu)

		// Device 3 (V3): 2 interfaces
		require.Equal(t, expectedPK3, result[3].DevicePK)
		require.Equal(t, "Loopback255", result[3].Intf)
		require.Equal(t, "vpnv4", result[3].LoopbackType)
		require.Equal(t, expectedPK3, result[4].DevicePK)
		require.Equal(t, "Ethernet50/1", result[4].Intf)
		require.Equal(t, "bgp", result[4].RoutingMode)
		require.Equal(t, uint64(100_000_000_000), result[4].Bandwidth)
		require.Equal(t, uint16(77), result[4].NodeSegmentIdx)
		require.True(t, result[4].UserTunnelEndpoint)
	})
}

// testPubkey generates a deterministic test public key from a seed byte
func testPubkey(seed byte) solana.PublicKey {
	var pk [32]byte
	for i := range pk {
		pk[i] = seed
	}
	return solana.PublicKeyFromBytes(pk[:])
}

// testPubkeyBytes generates a deterministic test public key bytes from a seed byte
func testPubkeyBytes(seed byte) [32]byte {
	var pk [32]byte
	for i := range pk {
		pk[i] = seed
	}
	return pk
}

func TestLake_Serviceability_View_Refresh(t *testing.T) {
	t.Parallel()

	t.Run("stores all data on refresh", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		// Create test data
		contributorPK := testPubkeyBytes(1)
		metroPK := testPubkeyBytes(2)
		devicePK := testPubkeyBytes(3)
		userPK := testPubkeyBytes(4)
		ownerPubkey := testPubkeyBytes(5)
		linkPK := testPubkeyBytes(6)
		sideAPK := testPubkeyBytes(7)
		sideZPK := testPubkeyBytes(8)

		rpc := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					Contributors: []serviceability.Contributor{
						{
							PubKey: contributorPK,
							Owner:  ownerPubkey,
							Status: serviceability.ContributorStatusActivated,
							Code:   "TEST",
						},
					},
					Devices: []serviceability.Device{
						{
							PubKey:            devicePK,
							Owner:             ownerPubkey,
							Status:            serviceability.DeviceStatusActivated,
							DeviceType:        serviceability.DeviceDeviceTypeHybrid,
							Code:              "DEV001",
							PublicIp:          [4]byte{192, 168, 1, 1},
							ContributorPubKey: contributorPK,
							ExchangePubKey:    metroPK,
						},
					},
					Users: []serviceability.User{
						{
							PubKey:       userPK,
							Owner:        ownerPubkey,
							Status:       serviceability.UserStatusActivated,
							UserType:     serviceability.UserTypeIBRL,
							ClientIp:     [4]byte{1, 1, 1, 1},
							DzIp:         [4]byte{10, 0, 0, 1},
							DevicePubKey: devicePK,
						},
						{
							PubKey:       testPubkeyBytes(9),
							Owner:        ownerPubkey,
							Status:       serviceability.UserStatusActivated,
							UserType:     serviceability.UserTypeIBRL,
							ClientIp:     [4]byte{8, 8, 8, 8},
							DzIp:         [4]byte{10, 0, 0, 2},
							DevicePubKey: devicePK,
						},
						{
							PubKey:       testPubkeyBytes(10),
							Owner:        ownerPubkey,
							Status:       serviceability.UserStatusActivated,
							UserType:     serviceability.UserTypeIBRL,
							ClientIp:     [4]byte{0, 0, 0, 0}, // No client IP
							DzIp:         [4]byte{10, 0, 0, 3},
							DevicePubKey: devicePK,
						},
					},
					Exchanges: []serviceability.Exchange{
						{
							PubKey: metroPK,
							Owner:  ownerPubkey,
							Status: serviceability.ExchangeStatusActivated,
							Code:   "NYC",
							Name:   "New York",
							Lat:    40.7128,
							Lng:    -74.0060,
						},
					},
					Links: []serviceability.Link{
						{
							PubKey:            linkPK,
							Status:            serviceability.LinkStatusActivated,
							Code:              "LINK001",
							TunnelNet:         [5]uint8{192, 168, 1, 0, 24},
							SideAPubKey:       sideAPK,
							SideZPubKey:       sideZPK,
							ContributorPubKey: contributorPK,
							SideAIfaceName:    "eth0",
							SideZIfaceName:    "eth1",
							LinkType:          serviceability.LinkLinkTypeWAN,
							DelayNs:           5000000, // onchain field name
							JitterNs:          1000000, // onchain field name
							Bandwidth:         1000000000,
							DelayOverrideNs:   0, // onchain field name
						},
					},
				}, nil
			},
		}

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: rpc,
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		// Verify contributors were stored by querying the database
		conn, err := mockDB.Conn(ctx)
		require.NoError(t, err)

		// Query row counts from history table (current state computed at query time)
		rows, err := conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_contributors_history
			)
			SELECT count(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var contributorCount uint64
		require.NoError(t, rows.Scan(&contributorCount))
		rows.Close()

		rows, err = conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_devices_history
			)
			SELECT count(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var deviceCount uint64
		require.NoError(t, rows.Scan(&deviceCount))
		rows.Close()

		rows, err = conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_users_history
			)
			SELECT count(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var userCount uint64
		require.NoError(t, rows.Scan(&userCount))
		rows.Close()

		rows, err = conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_metros_history
			)
			SELECT count(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var metroCount uint64
		require.NoError(t, rows.Scan(&metroCount))
		rows.Close()

		rows, err = conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_links_history
			)
			SELECT count(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var linkCount uint64
		require.NoError(t, rows.Scan(&linkCount))
		rows.Close()

		require.Equal(t, uint64(1), contributorCount, "should have 1 contributor")
		require.Equal(t, uint64(1), deviceCount, "should have 1 device")
		require.Equal(t, uint64(3), userCount, "should have 3 users")
		require.Equal(t, uint64(1), metroCount, "should have 1 metro")
		require.Equal(t, uint64(1), linkCount, "should have 1 link")

		// Note: GeoIP records are now handled by the geoip view, not the serviceability view

		// Verify specific data using the dataset API
		contributorPKStr := testPubkey(1).String()
		contributorsDataset, err := NewContributorDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, contributorsDataset)
		contributorEntityID := dataset.NewNaturalKey(contributorPKStr).ToSurrogate()
		current, err := contributorsDataset.GetCurrentRow(ctx, conn, contributorEntityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "TEST", current["code"], "contributor should have correct code")

		devicePKStr := testPubkey(3).String()
		devicesDataset, err := NewDeviceDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, devicesDataset)
		deviceEntityID := dataset.NewNaturalKey(devicePKStr).ToSurrogate()
		current, err = devicesDataset.GetCurrentRow(ctx, conn, deviceEntityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "DEV001", current["code"], "device should have correct code")

		metroPKStr := testPubkey(2).String()
		metrosDataset, err := NewMetroDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, metrosDataset)
		metroEntityID := dataset.NewNaturalKey(metroPKStr).ToSurrogate()
		current, err = metrosDataset.GetCurrentRow(ctx, conn, metroEntityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "New York", current["name"], "metro should have correct name")

		linkPKStr := testPubkey(6).String()
		linksDataset, err := NewLinkDataset(laketesting.NewLogger())
		require.NoError(t, err)
		require.NotNil(t, linksDataset)
		linkEntityID := dataset.NewNaturalKey(linkPKStr).ToSurrogate()
		current, err = linksDataset.GetCurrentRow(ctx, conn, linkEntityID)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, "LINK001", current["code"], "link should have correct code")
	})

	t.Run("handles users without client IPs", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		userPK := testPubkeyBytes(1)
		ownerPubkey := testPubkeyBytes(2)
		devicePK := testPubkeyBytes(3)

		rpc := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				return &serviceability.ProgramData{
					// Minimal data to pass empty snapshot validation
					Contributors: []serviceability.Contributor{{PubKey: [32]byte{1}, Code: "test"}},
					Devices:      []serviceability.Device{{PubKey: [32]byte{2}, Code: "test-device", Status: serviceability.DeviceStatusActivated}},
					Exchanges:    []serviceability.Exchange{{PubKey: [32]byte{3}, Code: "test-metro", Name: "Test Metro"}},
					Users: []serviceability.User{
						{
							PubKey:       userPK,
							Owner:        ownerPubkey,
							Status:       serviceability.UserStatusActivated,
							UserType:     serviceability.UserTypeIBRL,
							ClientIp:     [4]byte{0, 0, 0, 0}, // No client IP (zero IP)
							DzIp:         [4]byte{10, 0, 0, 1},
							DevicePubKey: devicePK,
						},
					},
				}, nil
			},
		}

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: rpc,
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.NoError(t, err)

		// Verify users are still stored even without geoip
		conn, err := mockDB.Conn(ctx)
		require.NoError(t, err)

		rows, err := conn.Query(ctx, `
			WITH ranked AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
				FROM dim_dz_users_history
			)
			SELECT count(*) FROM ranked WHERE rn = 1 AND is_deleted = 0
		`)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var userCount uint64
		require.NoError(t, rows.Scan(&userCount))
		rows.Close()

		require.Equal(t, uint64(1), userCount, "should have 1 user even without geoip")

		// Note: GeoIP records are now handled by the geoip view, not the serviceability view
	})

	t.Run("rejects empty snapshot to prevent mass deletion", func(t *testing.T) {
		t.Parallel()

		mockDB := testClient(t)

		rpc := &MockServiceabilityRPC{
			getProgramDataFunc: func(ctx context.Context) (*serviceability.ProgramData, error) {
				// Return empty data - simulates RPC returning no accounts
				return &serviceability.ProgramData{}, nil
			},
		}

		view, err := NewView(ViewConfig{
			Logger:            laketesting.NewLogger(),
			Clock:             clockwork.NewFakeClock(),
			ServiceabilityRPC: rpc,
			RefreshInterval:   time.Second,
			ClickHouse:        mockDB,
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = view.Refresh(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "refusing to write snapshot")
	})
}
