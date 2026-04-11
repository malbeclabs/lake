package dztelemusage

import (
	"context"
	"testing"
	"time"

	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"

	"github.com/stretchr/testify/require"
)

func TestLake_TelemetryUsage_MockInfluxDBClient_QueryIntfCounters(t *testing.T) {
	t.Parallel()

	t.Run("generates mock data for time range", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		// Insert test device data
		svcStore, err := dzsvc.NewStore(dzsvc.StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: mockDB,
		})
		require.NoError(t, err)

		devices := []dzsvc.Device{
			{
				PK:   "device1",
				Code: "DEV1",
				Interfaces: []dzsvc.Interface{
					{Name: "eth0", IP: "10.0.0.1", Status: "up"},
					{Name: "eth1", IP: "10.0.0.2", Status: "up"},
				},
			},
			{
				PK:   "device2",
				Code: "DEV2",
				Interfaces: []dzsvc.Interface{
					{Name: "Tunnel501", IP: "172.16.0.1", Status: "up"},
				},
			},
		}
		err = svcStore.ReplaceDevices(context.Background(), devices)
		require.NoError(t, err)

		client := NewMockInfluxDBClient(MockInfluxDBClientConfig{
			ClickHouse: mockDB,
			Logger:     laketesting.NewLogger(),
		})

		// Query for a 10-minute window (should generate 2 data points per interface)
		now := time.Now().UTC()
		start := now.Add(-10 * time.Minute)

		results, err := client.QueryIntfCounters(context.Background(), start, now)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		// Verify results have expected fields
		for _, row := range results {
			require.NotNil(t, row["time"])
			require.NotNil(t, row["dzd_pubkey"])
			require.NotNil(t, row["host"])
			require.NotNil(t, row["intf"])
			require.NotNil(t, row["in-octets"])
			require.NotNil(t, row["out-octets"])
			require.NotNil(t, row["in-pkts"])
			require.NotNil(t, row["out-pkts"])

			// time must be a string (RFC3339Nano parseable)
			timeStr, ok := row["time"].(string)
			require.True(t, ok, "time should be a string")
			_, err := time.Parse(time.RFC3339Nano, timeStr)
			require.NoError(t, err, "time should be parseable as RFC3339Nano: %s", timeStr)
		}

		// Verify we have data for our devices
		devicePKs := make(map[string]bool)
		for _, row := range results {
			if pk, ok := row["dzd_pubkey"].(string); ok {
				devicePKs[pk] = true
			}
		}
		require.True(t, devicePKs["device1"], "should have data for device1")
		require.True(t, devicePKs["device2"], "should have data for device2")
	})

	t.Run("handles devices without interfaces JSON", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		svcStore, err := dzsvc.NewStore(dzsvc.StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: mockDB,
		})
		require.NoError(t, err)

		devices := []dzsvc.Device{
			{
				PK:         "device3",
				Code:       "DEV3",
				Interfaces: []dzsvc.Interface{}, // Empty interfaces
			},
		}
		err = svcStore.ReplaceDevices(context.Background(), devices)
		require.NoError(t, err)

		client := NewMockInfluxDBClient(MockInfluxDBClientConfig{
			ClickHouse: mockDB,
			Logger:     laketesting.NewLogger(),
		})

		now := time.Now().UTC()
		start := now.Add(-10 * time.Minute)

		results, err := client.QueryIntfCounters(context.Background(), start, now)
		require.NoError(t, err)
		// Should still have data with default interfaces
		require.NotEmpty(t, results)
	})

	t.Run("counters increase over time", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		svcStore, err := dzsvc.NewStore(dzsvc.StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: mockDB,
		})
		require.NoError(t, err)

		devices := []dzsvc.Device{
			{
				PK:   "device4",
				Code: "DEV4",
				Interfaces: []dzsvc.Interface{
					{Name: "eth0", Status: "up"},
				},
			},
		}
		err = svcStore.ReplaceDevices(context.Background(), devices)
		require.NoError(t, err)

		client := NewMockInfluxDBClient(MockInfluxDBClientConfig{
			ClickHouse: mockDB,
			Logger:     laketesting.NewLogger(),
		})

		now := time.Now().UTC()
		start := now.Add(-30 * time.Minute) // 30 min window gives us 6 data points

		results, err := client.QueryIntfCounters(context.Background(), start, now)
		require.NoError(t, err)

		// Filter to device4
		var device4Results []map[string]any
		for _, row := range results {
			if pk, ok := row["dzd_pubkey"].(string); ok && pk == "device4" {
				device4Results = append(device4Results, row)
			}
		}
		require.GreaterOrEqual(t, len(device4Results), 2, "should have at least 2 data points")

		// Verify counters increase over time
		var prevOctets int64
		for i, row := range device4Results {
			octets, ok := row["in-octets"].(int64)
			require.True(t, ok, "in-octets should be int64")
			if i > 0 {
				require.Greater(t, octets, prevOctets, "counters should increase over time")
			}
			prevOctets = octets
		}
	})

	t.Run("time field format matches what convertRowsToUsage expects", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		svcStore, err := dzsvc.NewStore(dzsvc.StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: mockDB,
		})
		require.NoError(t, err)

		err = svcStore.ReplaceDevices(context.Background(), []dzsvc.Device{
			{PK: "device5", Code: "DEV5", Interfaces: []dzsvc.Interface{{Name: "eth0", Status: "up"}}},
		})
		require.NoError(t, err)

		client := NewMockInfluxDBClient(MockInfluxDBClientConfig{
			ClickHouse: mockDB,
			Logger:     laketesting.NewLogger(),
		})

		now := time.Now().UTC()
		results, err := client.QueryIntfCounters(context.Background(), now.Add(-10*time.Minute), now)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		for _, row := range results {
			timeVal := extractStringFromRow(row, "time")
			require.NotNil(t, timeVal, "time key must exist and be a string")

			// Must be parseable by the time formats used in convertRowsToUsage
			parsed := false
			for _, format := range []string{time.RFC3339Nano, time.RFC3339} {
				if _, err := time.Parse(format, *timeVal); err == nil {
					parsed = true
					break
				}
			}
			require.True(t, parsed, "time %q must be parseable by RFC3339Nano or RFC3339", *timeVal)
		}
	})
}

func TestLake_TelemetryUsage_MockInfluxDBClient_QueryBaselineCounter(t *testing.T) {
	t.Parallel()

	t.Run("returns empty results (no historical data in mock)", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		client := NewMockInfluxDBClient(MockInfluxDBClientConfig{
			ClickHouse: mockDB,
			Logger:     laketesting.NewLogger(),
		})

		now := time.Now().UTC()
		results, err := client.QueryBaselineCounter(context.Background(), "in-errors", now.Add(-365*24*time.Hour), now)
		require.NoError(t, err)
		require.Empty(t, results)
	})
}

func TestLake_TelemetryUsage_MockInfluxDBClient_Close(t *testing.T) {
	t.Parallel()

	t.Run("close returns no error", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		client := NewMockInfluxDBClient(MockInfluxDBClientConfig{
			ClickHouse: mockDB,
			Logger:     laketesting.NewLogger(),
		})

		err := client.Close()
		require.NoError(t, err)
	})
}

func TestLake_TelemetryUsage_MockInfluxDBClient_hashSeed(t *testing.T) {
	t.Parallel()

	t.Run("produces consistent hash for same inputs", func(t *testing.T) {
		t.Parallel()
		seed1 := hashSeed("device1", "eth0")
		seed2 := hashSeed("device1", "eth0")
		require.Equal(t, seed1, seed2)
	})

	t.Run("produces different hash for different inputs", func(t *testing.T) {
		t.Parallel()
		seed1 := hashSeed("device1", "eth0")
		seed2 := hashSeed("device1", "eth1")
		seed3 := hashSeed("device2", "eth0")
		require.NotEqual(t, seed1, seed2)
		require.NotEqual(t, seed1, seed3)
		require.NotEqual(t, seed2, seed3)
	})
}

func TestLake_TelemetryUsage_MockInfluxDBClient_getInterfaceCapacity(t *testing.T) {
	t.Parallel()

	client := &MockInfluxDBClient{
		topology: &mockTopology{
			devices:      make(map[string]*mockDevice),
			linkLookup:   make(map[string]*mockLinkInfo),
			tunnelLookup: make(map[string]int64),
		},
	}

	t.Run("tunnel interfaces have lower capacity than physical", func(t *testing.T) {
		t.Parallel()
		tunnelCap := client.getInterfaceCapacity("device1", "Tunnel501", 12345)
		ethCap := client.getInterfaceCapacity("device1", "eth0", 12345)
		require.Less(t, tunnelCap, ethCap, "tunnel should have lower capacity than physical interface")
	})

	t.Run("loopback interfaces have lower capacity than physical", func(t *testing.T) {
		t.Parallel()
		loopbackCap := client.getInterfaceCapacity("device1", "Loopback0", 12345)
		ethCap := client.getInterfaceCapacity("device1", "eth0", 12345)
		require.Less(t, loopbackCap, ethCap, "loopback should have lower capacity than physical interface")
	})

	t.Run("physical interfaces are 10 or 100 Gbps", func(t *testing.T) {
		t.Parallel()
		ethCap := client.getInterfaceCapacity("device1", "eth0", 12345)
		// Should be either 10 Gbps or 100 Gbps
		is10G := ethCap == float64(10_000_000_000)
		is100G := ethCap == float64(100_000_000_000)
		require.True(t, is10G || is100G, "physical interface should be 10G or 100G")
	})

	t.Run("interface with link bandwidth uses link capacity", func(t *testing.T) {
		t.Parallel()
		clientWithLink := &MockInfluxDBClient{
			topology: &mockTopology{
				devices: make(map[string]*mockDevice),
				linkLookup: map[string]*mockLinkInfo{
					"device1:eth0": {linkPK: "link1", linkSide: "A", bandwidthBps: int64(1_000_000_000)},
				},
				tunnelLookup: make(map[string]int64),
			},
		}
		ethCap := clientWithLink.getInterfaceCapacity("device1", "eth0", 12345)
		require.Equal(t, float64(1_000_000_000), ethCap, "should use link bandwidth")
	})
}

func TestLake_TelemetryUsage_MockInfluxDBClient_getBaseUtilization(t *testing.T) {
	t.Parallel()

	client := &MockInfluxDBClient{}

	t.Run("loopback has very low utilization", func(t *testing.T) {
		t.Parallel()
		loopbackUtil := client.getBaseUtilization("Loopback0", 12345)
		ethUtil := client.getBaseUtilization("eth0", 12345)
		require.Less(t, loopbackUtil, ethUtil, "loopback should have lower utilization")
		require.Less(t, loopbackUtil, 0.01, "loopback utilization should be under 1%")
	})

	t.Run("utilization stays under 100%", func(t *testing.T) {
		t.Parallel()
		// Try various seeds to verify reasonable values
		for seed := uint64(0); seed < 100; seed++ {
			util := client.getBaseUtilization("eth0", seed)
			require.LessOrEqual(t, util, 1.0, "utilization should be under 100%")
			require.GreaterOrEqual(t, util, 0.0, "utilization should be non-negative")
		}
	})
}
