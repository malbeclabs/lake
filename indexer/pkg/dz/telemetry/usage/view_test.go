package dztelemusage

import (
	"context"
	"testing"
	"time"

	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"

	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

type mockInfluxDBClient struct {
	queryIntfCountersFunc    func(ctx context.Context, start, end time.Time) ([]map[string]any, error)
	queryBaselineCounterFunc func(ctx context.Context, field string, lookbackStart, windowStart time.Time) ([]map[string]any, error)
	closeFunc                func() error
}

func (m *mockInfluxDBClient) QueryIntfCounters(ctx context.Context, start, end time.Time) ([]map[string]any, error) {
	if m.queryIntfCountersFunc != nil {
		return m.queryIntfCountersFunc(ctx, start, end)
	}
	return []map[string]any{}, nil
}

func (m *mockInfluxDBClient) QueryBaselineCounter(ctx context.Context, field string, lookbackStart, windowStart time.Time) ([]map[string]any, error) {
	if m.queryBaselineCounterFunc != nil {
		return m.queryBaselineCounterFunc(ctx, field, lookbackStart, windowStart)
	}
	return []map[string]any{}, nil
}

func (m *mockInfluxDBClient) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestLake_TelemetryUsage_View_ViewConfig_Validate(t *testing.T) {
	t.Parallel()

	t.Run("returns error when logger is missing", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "logger is required")
	})

	t.Run("returns error when db is missing", func(t *testing.T) {
		t.Parallel()
		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			ClickHouse:      nil,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "clickhouse connection is required")
	})

	t.Run("returns error when influxdb client is missing", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "influxdb client is required")
	})

	t.Run("returns error when bucket is empty", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			RefreshInterval: time.Second,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "influxdb bucket is required")
	})

	t.Run("returns error when refresh interval is zero", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: 0,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "refresh interval must be greater than 0")
	})

	t.Run("sets default query window when zero", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     0,
		}
		err := cfg.Validate()
		require.NoError(t, err)
		require.Equal(t, 1*time.Hour, cfg.QueryWindow)
	})

	t.Run("returns error when query chunk defeats the baseline cache", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			// refreshOverlap (5m) + 2×10m = 25m ≥ baselineCacheMaxLag (20m): the
			// capped-refresh lag must stay strictly below the cache guard's
			// backward bound.
			QueryChunk: 10 * time.Minute,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "too large for the baseline cache")
	})

	t.Run("returns error when query window defeats the empty-span age-out", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			// The age-out fires at watermark age > QueryWindow + one capped
			// span (10m with the default chunk), but the horizon skip
			// intercepts at 24h first: 23h55m leaves the age-out unreachable,
			// so a genuine source gap would pin ingest at the horizon forever.
			QueryWindow: 24*time.Hour - 5*time.Minute,
		}
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must stay below maxCatchupHorizon")
	})

	t.Run("sets default clock when nil", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			Clock:           nil,
		}
		err := cfg.Validate()
		require.NoError(t, err)
		require.NotNil(t, cfg.Clock)
	})

	t.Run("validates successfully with all required fields", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		cfg := ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     2 * time.Hour,
		}
		err := cfg.Validate()
		require.NoError(t, err)
	})
}

func TestLake_TelemetryUsage_View_NewView(t *testing.T) {
	t.Parallel()

	t.Run("returns error when config validation fails", func(t *testing.T) {
		t.Parallel()
		view, err := NewView(ViewConfig{})
		require.Error(t, err)
		require.Nil(t, view)
	})

	t.Run("creates view successfully", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)
		require.NotNil(t, view)
		require.NotNil(t, view.Store())
	})
}

func TestLake_TelemetryUsage_View_extractTunnelIDFromInterface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected *int64
	}{
		{
			name:     "valid tunnel interface",
			input:    "Tunnel501",
			expected: int64Ptr(501),
		},
		{
			name:     "valid tunnel interface with large number",
			input:    "Tunnel12345",
			expected: int64Ptr(12345),
		},
		{
			name:     "interface without Tunnel prefix",
			input:    "eth0",
			expected: nil,
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "just Tunnel prefix",
			input:    "Tunnel",
			expected: nil,
		},
		{
			name:     "Tunnel with non-numeric suffix",
			input:    "Tunnelabc",
			expected: nil,
		},
		{
			name:     "Tunnel with mixed suffix",
			input:    "Tunnel501abc",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := extractTunnelIDFromInterface(tt.input)
			if tt.expected == nil {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Equal(t, *tt.expected, *result)
			}
		})
	}
}

func TestLake_TelemetryUsage_View_buildLinkLookup(t *testing.T) {
	t.Parallel()

	t.Run("builds link lookup map successfully", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		// Insert test link data using serviceability store
		svcStore, err := dzsvc.NewStore(dzsvc.StoreConfig{
			Logger:     laketesting.NewLogger(),
			ClickHouse: mockDB,
		})
		require.NoError(t, err)

		links := []dzsvc.Link{
			{
				PK:             "link1",
				SideAPK:        "device1",
				SideAIfaceName: "eth0",
				SideZPK:        "device2",
				SideZIfaceName: "eth1",
			},
			{
				PK:             "link2",
				SideAPK:        "device3",
				SideAIfaceName: "eth0",
				SideZPK:        "device4",
				SideZIfaceName: "eth0",
			},
		}
		err = svcStore.ReplaceLinks(context.Background(), links)
		require.NoError(t, err)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		lookup, err := view.buildLinkLookup(context.Background())
		require.NoError(t, err)
		require.NotNil(t, lookup)

		// Verify side A mappings
		link1SideA, ok := lookup["device1:eth0"]
		require.True(t, ok)
		require.Equal(t, "link1", link1SideA.LinkPK)
		require.Equal(t, "A", link1SideA.LinkSide)

		link2SideA, ok := lookup["device3:eth0"]
		require.True(t, ok)
		require.Equal(t, "link2", link2SideA.LinkPK)
		require.Equal(t, "A", link2SideA.LinkSide)

		// Verify side Z mappings
		link1SideZ, ok := lookup["device2:eth1"]
		require.True(t, ok)
		require.Equal(t, "link1", link1SideZ.LinkPK)
		require.Equal(t, "Z", link1SideZ.LinkSide)

		link2SideZ, ok := lookup["device4:eth0"]
		require.True(t, ok)
		require.Equal(t, "link2", link2SideZ.LinkPK)
		require.Equal(t, "Z", link2SideZ.LinkSide)
	})

	t.Run("handles empty links table", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		// Tables are created via migrations, no need to create them here
		// This test verifies that buildLinkLookup works with empty tables
		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		lookup, err := view.buildLinkLookup(context.Background())
		require.NoError(t, err)
		require.NotNil(t, lookup)
		require.Equal(t, 0, len(lookup))
	})
}

func TestLake_TelemetryUsage_View_Ready(t *testing.T) {
	t.Parallel()

	t.Run("returns false when not ready", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		require.False(t, view.Ready())
	})

	t.Run("returns true after successful refresh", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		// With mock, we can't create tables - they're created via migrations
		// The buildLinkLookup will query the mock, which should return empty results

		clock := clockwork.NewFakeClock()

		mockInflux := &mockInfluxDBClient{}

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clock,
			ClickHouse:      mockDB,
			InfluxDB:        mockInflux,
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		require.False(t, view.Ready())

		_, err = view.Refresh(t.Context())
		require.NoError(t, err)

		require.True(t, view.Ready())
	})
}

func TestLake_TelemetryUsage_View_WaitReady(t *testing.T) {
	t.Parallel()

	t.Run("returns immediately when already ready", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		// With mock, we can't create tables - they're created via migrations

		clock := clockwork.NewFakeClock()
		mockInflux := &mockInfluxDBClient{}

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			Clock:           clock,
			ClickHouse:      mockDB,
			InfluxDB:        mockInflux,
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		_, err = view.Refresh(t.Context())
		require.NoError(t, err)

		// Should return immediately
		err = view.WaitReady(t.Context())
		require.NoError(t, err)
	})

	t.Run("handles context cancellation", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		// With mock, we can't create tables - they're created via migrations

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		err = view.WaitReady(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "context cancelled")
	})
}

func TestLake_TelemetryUsage_View_Store(t *testing.T) {
	t.Parallel()

	t.Run("returns the underlying store", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		store := view.Store()
		require.NotNil(t, store)
	})
}

func TestLake_TelemetryUsage_View_convertRowsToUsage(t *testing.T) {
	t.Parallel()

	t.Run("converts rows with tunnel ID extraction", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		// Use sparse counters (errors) so first row is not skipped
		rows := []map[string]any{
			{
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "Tunnel501",
				"model_name": "ModelX",
				"in-errors":  int64(1), // Sparse counter
			},
			{
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device2",
				"intf":       "eth0",
				"in-errors":  int64(2), // Sparse counter
			},
		}

		baselines := &CounterBaselines{
			InErrors: make(map[string]*int64),
		}

		linkLookup := map[string]LinkInfo{
			"device1:Tunnel501": {LinkPK: "link1", LinkSide: "A"},
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, linkLookup, nil)
		require.NoError(t, err)
		require.Len(t, usage, 2)

		// Check first row with tunnel ID
		require.NotNil(t, usage[0].UserTunnelID)
		require.Equal(t, int64(501), *usage[0].UserTunnelID)
		require.NotNil(t, usage[0].LinkPK)
		require.Equal(t, "link1", *usage[0].LinkPK)
		require.NotNil(t, usage[0].LinkSide)
		require.Equal(t, "A", *usage[0].LinkSide)

		// Check second row without tunnel ID
		require.Nil(t, usage[1].UserTunnelID)
		require.Nil(t, usage[1].LinkPK)
	})

	t.Run("handles first row as baseline for non-sparse counters", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000), // Non-sparse counter
			},
			{
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000), // Second row should have delta
			},
		}

		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), nil)
		require.NoError(t, err)
		// First row should be skipped (used as baseline), so only second row should be stored
		require.Len(t, usage, 1)
		require.NotNil(t, usage[0].InOctetsDelta)
		require.Equal(t, int64(1000), *usage[0].InOctetsDelta) // 2000 - 1000
	})

	t.Run("computes delta duration", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000), // Non-sparse counter: first row used as baseline
			},
			{
				"time":       now.Add(30 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
			},
			{
				"time":       now.Add(60 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
			},
		}

		baselines := &CounterBaselines{
			InErrors: make(map[string]*int64),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), nil)
		require.NoError(t, err)
		require.Len(t, usage, 2) // first row is baseline, not stored

		// First stored row should have delta_duration of 30 seconds (from baseline)
		require.NotNil(t, usage[0].DeltaDuration)
		require.InDelta(t, 30.0, *usage[0].DeltaDuration, 0.01)

		// Second stored row should have delta_duration of 30 seconds
		require.NotNil(t, usage[1].DeltaDuration)
		require.InDelta(t, 30.0, *usage[1].DeltaDuration, 0.01)
	})

	t.Run("carrier transition row does not advance lastTime", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		// Reproduce the pattern from #388: normal octets rows every ~2s,
		// with a carrier-transition-only row arriving between them ~7ms
		// before the next octets row.
		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{ // baseline row (skipped)
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
			},
			{ // normal octets row at +2s
				"time":       now.Add(2 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
			},
			{ // carrier transition only at +3.993s (no octets)
				"time":                now.Add(3993 * time.Millisecond).Format(time.RFC3339Nano),
				"dzd_pubkey":          "device1",
				"intf":                "eth0",
				"carrier-transitions": int64(777),
			},
			{ // next octets row at +4s (7ms after carrier event)
				"time":       now.Add(4 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
			},
		}

		baselines := &CounterBaselines{}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), nil)
		require.NoError(t, err)
		require.Len(t, usage, 3) // baseline skipped, 3 rows stored

		// Row 0: normal octets at +2s, delta_duration = 2s from baseline
		require.NotNil(t, usage[0].DeltaDuration)
		require.InDelta(t, 2.0, *usage[0].DeltaDuration, 0.01)
		require.NotNil(t, usage[0].InOctetsDelta)
		require.Equal(t, int64(1000), *usage[0].InOctetsDelta)

		// Row 1: carrier-transition-only at +3.993s, delta_duration = 1.993s from
		// last octets row (not from itself since it has no non-sparse counters)
		require.NotNil(t, usage[1].DeltaDuration)
		require.InDelta(t, 1.993, *usage[1].DeltaDuration, 0.01)

		// Row 2: octets at +4s — delta_duration should be ~2s from the +2s row,
		// NOT 7ms from the carrier event. This is the bug from #388.
		require.NotNil(t, usage[2].DeltaDuration)
		require.InDelta(t, 2.0, *usage[2].DeltaDuration, 0.01)
		require.NotNil(t, usage[2].InOctetsDelta)
		require.Equal(t, int64(1000), *usage[2].InOctetsDelta)
	})

	t.Run("skips already-written rows", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-errors":  int64(1),
			},
			{
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-errors":  int64(2),
			},
			{
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-errors":  int64(3),
			},
		}

		baselines := &CounterBaselines{
			InErrors: make(map[string]*int64),
		}

		// Mark the first row's timestamp as already written
		alreadyWritten := MaxTimestampsByKey{
			"device1:eth0": now,
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		// First row should be skipped (already written), so only rows 2 and 3 should be stored
		require.Len(t, usage, 2)

		// Verify we got the second and third rows (timestamps after the already-written max)
		require.Equal(t, now.Add(time.Minute), usage[0].Time)
		require.Equal(t, now.Add(2*time.Minute), usage[1].Time)
	})

	t.Run("skips all rows up to and including already-written timestamp", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-errors":  int64(1),
			},
			{
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-errors":  int64(2),
			},
			{
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-errors":  int64(3),
			},
		}

		baselines := &CounterBaselines{
			InErrors: make(map[string]*int64),
		}

		// Mark up to the second row's timestamp as already written
		alreadyWritten := MaxTimestampsByKey{
			"device1:eth0": now.Add(time.Minute),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		// First two rows should be skipped (at or before already-written timestamp), only third row stored
		require.Len(t, usage, 1)

		// Verify we got only the third row
		require.Equal(t, now.Add(2*time.Minute), usage[0].Time)
	})

	t.Run("already-written rows update lastKnownValues for subsequent delta calculations", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
				"in-errors":  int64(10),
			},
			{
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
				"in-errors":  int64(15),
			},
			{
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
				"in-errors":  int64(20),
			},
		}

		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		// Mark first two rows as already written
		alreadyWritten := MaxTimestampsByKey{
			"device1:eth0": now.Add(time.Minute),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		require.Len(t, usage, 1)

		// The third row should have deltas computed against the second row's values
		// (which were in the already-written overlap window)
		require.NotNil(t, usage[0].InOctetsDelta)
		require.Equal(t, int64(1000), *usage[0].InOctetsDelta) // 3000 - 2000
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(5), *usage[0].InErrorsDelta) // 20 - 15
	})

	t.Run("first row skip preserves sparse counter values for baseline", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				// First row has both non-sparse (octets) and sparse (errors) counters.
				// The first row is skipped for storage (used as baseline for non-sparse).
				// Sparse counter values must also be saved as baselines.
				"time":        now.Format(time.RFC3339Nano),
				"dzd_pubkey":  "device1",
				"intf":        "eth0",
				"in-octets":   int64(1000),
				"in-errors":   int64(50),
				"in-discards": int64(10),
			},
			{
				"time":        now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey":  "device1",
				"intf":        "eth0",
				"in-octets":   int64(2000),
				"in-errors":   int64(55),
				"in-discards": int64(12),
			},
		}

		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), nil)
		require.NoError(t, err)
		// First row is skipped (baseline for non-sparse), only second row stored
		require.Len(t, usage, 1)

		// Second row should have correct deltas for BOTH non-sparse and sparse counters
		// Non-sparse: 2000 - 1000 = 1000
		require.NotNil(t, usage[0].InOctetsDelta)
		require.Equal(t, int64(1000), *usage[0].InOctetsDelta)

		// Sparse: 55 - 50 = 5 (sparse values from first row were preserved as baseline)
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(5), *usage[0].InErrorsDelta)

		// Sparse: 12 - 10 = 2
		require.NotNil(t, usage[0].InDiscardsDelta)
		require.Equal(t, int64(2), *usage[0].InDiscardsDelta)
	})

	t.Run("extractInt64FromRow handles uint64 values", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"signed":   int64(42),
			"unsigned": uint64(76),
			"float":    float64(99.0),
			"str":      "123",
			"nil_val":  nil,
		}

		signed := extractInt64FromRow(row, "signed")
		require.NotNil(t, signed)
		require.Equal(t, int64(42), *signed)

		unsigned := extractInt64FromRow(row, "unsigned")
		require.NotNil(t, unsigned)
		require.Equal(t, int64(76), *unsigned)

		floatVal := extractInt64FromRow(row, "float")
		require.NotNil(t, floatVal)
		require.Equal(t, int64(99), *floatVal)

		strVal := extractInt64FromRow(row, "str")
		require.NotNil(t, strVal)
		require.Equal(t, int64(123), *strVal)

		nilVal := extractInt64FromRow(row, "nil_val")
		require.Nil(t, nilVal)

		missing := extractInt64FromRow(row, "nonexistent")
		require.Nil(t, missing)
	})

	t.Run("already-written skip with sparse counters propagates baselines across overlap boundary", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		// Simulate an overlap scenario: rows 1-3 are already written,
		// row 4 is new. Sparse counters (errors) are only present in some rows.
		rows := []map[string]any{
			{
				// Already written - has non-sparse and sparse counters
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
				"in-errors":  int64(30),
			},
			{
				// Already written - sparse counter changed
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
				"in-errors":  int64(35),
			},
			{
				// Already written - latest overlap row with errors
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
				"in-errors":  int64(40),
			},
			{
				// New row - should compute delta against row 3's values
				"time":       now.Add(3 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(4000),
				"in-errors":  int64(45),
			},
		}

		// No ClickHouse baselines — the overlap rows are the only source of baseline values
		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		alreadyWritten := MaxTimestampsByKey{
			"device1:eth0": now.Add(2 * time.Minute),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		require.Len(t, usage, 1)

		// The new row should have deltas against the last overlap row's values
		require.NotNil(t, usage[0].InOctetsDelta)
		require.Equal(t, int64(1000), *usage[0].InOctetsDelta) // 4000 - 3000
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(5), *usage[0].InErrorsDelta) // 45 - 40
	})

	t.Run("already-written rows with null sparse counters use baseline for forward-fill", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		// Simulate the real-world scenario: already-written rows have NULL for
		// sparse counters (InfluxDB doesn't include them), but the ClickHouse
		// baseline has a value. New rows also have NULL sparse counters and
		// should be forward-filled from the baseline.
		rows := []map[string]any{
			{
				// Already written - sparse counters are NULL (not present in InfluxDB)
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
				// in-errors intentionally absent
			},
			{
				// Already written
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
			},
			{
				// New row - sparse counter still NULL, should forward-fill from baseline
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
			},
			{
				// New row - sparse counter appears with new value
				"time":       now.Add(3 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(4000),
				"in-errors":  int64(10),
			},
		}

		// ClickHouse baseline has in_errors = 1 (the last known value before the window)
		baselineVal := int64(1)
		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    map[string]*int64{"device1:eth0": &baselineVal},
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		alreadyWritten := MaxTimestampsByKey{
			"device1:eth0": now.Add(time.Minute),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		require.Len(t, usage, 2)

		// First new row: in_errors should be forward-filled from baseline (1), not NULL
		require.NotNil(t, usage[0].InErrors, "in_errors should be forward-filled from baseline, not NULL")
		require.Equal(t, int64(1), *usage[0].InErrors)
		// Delta should be 0 (1 - 1)
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(0), *usage[0].InErrorsDelta)

		// Second new row: in_errors = 10, delta should be 9 (10 - 1)
		require.NotNil(t, usage[1].InErrors)
		require.Equal(t, int64(10), *usage[1].InErrors)
		require.NotNil(t, usage[1].InErrorsDelta)
		require.Equal(t, int64(9), *usage[1].InErrorsDelta)
	})

	t.Run("forward-fill works when sparse counter goes from value to explicit nil in InfluxDB", func(t *testing.T) {
		// This reproduces the production scenario where a device stops reporting
		// in-errors (goes nil in InfluxDB) while still reporting non-sparse counters.
		// The overlap window includes rows where in-errors had a value (30),
		// then transitions to nil. New rows should be forward-filled from the
		// last known value.
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		rows := []map[string]any{
			{
				// Already written - has in-errors = 30 (before the gap)
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
				"in-errors":  int64(30),
			},
			{
				// Already written - in-errors goes nil (explicit nil in map)
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
				"in-errors":  nil, // explicit nil, not absent
			},
			{
				// New row - in-errors still nil, should forward-fill from last known value (30)
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
				"in-errors":  nil,
			},
			{
				// New row - in-errors comes back with new value
				"time":       now.Add(3 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(4000),
				"in-errors":  int64(32),
			},
		}

		baselineVal := int64(30)
		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    map[string]*int64{"device1:eth0": &baselineVal},
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		alreadyWritten := MaxTimestampsByKey{
			"device1:eth0": now.Add(time.Minute),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		require.Len(t, usage, 2)

		// First new row: in_errors should be forward-filled to 30 (last known value)
		require.NotNil(t, usage[0].InErrors, "in_errors should be forward-filled, not NULL")
		require.Equal(t, int64(30), *usage[0].InErrors)
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(0), *usage[0].InErrorsDelta)

		// Second new row: in_errors = 32, delta = 32 - 30 = 2
		require.NotNil(t, usage[1].InErrors)
		require.Equal(t, int64(32), *usage[1].InErrors)
		require.NotNil(t, usage[1].InErrorsDelta)
		require.Equal(t, int64(2), *usage[1].InErrorsDelta)
	})

	t.Run("forward-fill works with baseline and no overlap rows for key", func(t *testing.T) {
		// Reproduces the production scenario where the global maxTime is ahead of this
		// key's latest event_ts, so alreadyWritten has no entry for this key.
		// The first row triggers the first-row handler (skip), and subsequent rows
		// should be forward-filled from the baseline.
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		rows := []map[string]any{
			{
				// First row for this key - has in-octets but NOT in-errors
				// Should be consumed as baseline (first-row handler)
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
				// in-errors absent - gNMI stopped reporting it
			},
			{
				// Second row - should forward-fill in-errors from baseline
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
			},
			{
				// Third row
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
			},
		}

		baselineVal := int64(55)
		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    map[string]*int64{"device1:eth0": &baselineVal},
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		// No alreadyWritten entries for this key (global maxTime is ahead)
		alreadyWritten := MaxTimestampsByKey{}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), alreadyWritten)
		require.NoError(t, err)
		// First row is consumed as baseline, so we get 2 rows
		require.Len(t, usage, 2)

		// First output row: in_errors should be forward-filled to 55
		require.NotNil(t, usage[0].InErrors, "in_errors should be forward-filled from baseline, not NULL")
		require.Equal(t, int64(55), *usage[0].InErrors)
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(0), *usage[0].InErrorsDelta)

		// Second output row: in_errors should still be 55
		require.NotNil(t, usage[1].InErrors)
		require.Equal(t, int64(55), *usage[1].InErrors)
	})

	t.Run("sparse counters with no baseline and no overlap still capture first change", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		rows := []map[string]any{
			{
				// First row with both octets and errors — first row is skipped but
				// sparse counter values should be preserved as baselines
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000),
				"in-errors":  int64(0),
			},
			{
				// Second row — errors changed from 0 to 5
				"time":       now.Add(time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(2000),
				"in-errors":  int64(5),
			},
			{
				// Third row — errors changed from 5 to 8
				"time":       now.Add(2 * time.Minute).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(3000),
				"in-errors":  int64(8),
			},
		}

		// No baselines at all — simulates first-ever indexer run
		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), nil)
		require.NoError(t, err)
		// First row skipped (baseline), rows 2 and 3 stored
		require.Len(t, usage, 2)

		// Second row: errors delta = 5 - 0 = 5 (baseline from first row preserved)
		require.NotNil(t, usage[0].InErrors)
		require.Equal(t, int64(5), *usage[0].InErrors)
		require.NotNil(t, usage[0].InErrorsDelta)
		require.Equal(t, int64(5), *usage[0].InErrorsDelta)

		// Third row: errors delta = 8 - 5 = 3
		require.NotNil(t, usage[1].InErrors)
		require.Equal(t, int64(8), *usage[1].InErrors)
		require.NotNil(t, usage[1].InErrorsDelta)
		require.Equal(t, int64(3), *usage[1].InErrorsDelta)
	})

	t.Run("stale replayed row does not inflate subsequent delta", func(t *testing.T) {
		t.Parallel()
		mockDB := testClient(t)

		view, err := NewView(ViewConfig{
			Logger:          laketesting.NewLogger(),
			ClickHouse:      mockDB,
			InfluxDB:        &mockInfluxDBClient{},
			Bucket:          "test-bucket",
			RefreshInterval: time.Second,
			QueryWindow:     time.Hour,
		})
		require.NoError(t, err)

		now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

		// Simulate the real-world scenario where the device sends a stale/replayed
		// reading with a lower octets value (counter regression), followed by a
		// real reading. Without the high-water mark fix, the next row's delta would
		// be computed against the stale low value, inflating the bps by ~200x.
		rows := []map[string]any{
			{
				// Baseline row (first row, consumed as baseline, not stored)
				"time":       now.Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000000),
			},
			{
				// Normal row: +100 bytes over 2s
				"time":       now.Add(2 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000100),
			},
			{
				// Stale/replayed row: octets regresses back to an old value (lower than previous)
				"time":       now.Add(4 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(500000), // way lower than 1000100
			},
			{
				// Real row after the stale row: should be computed against the
				// high-water mark (1000100), not the stale value (500000).
				"time":       now.Add(6 * time.Second).Format(time.RFC3339Nano),
				"dzd_pubkey": "device1",
				"intf":       "eth0",
				"in-octets":  int64(1000200),
			},
		}

		baselines := &CounterBaselines{
			InDiscards:  make(map[string]*int64),
			InErrors:    make(map[string]*int64),
			InFCSErrors: make(map[string]*int64),
			OutDiscards: make(map[string]*int64),
			OutErrors:   make(map[string]*int64),
		}

		usage, _, err := view.convertRowsToUsage(rows, baselines, make(map[string]LinkInfo), nil)
		require.NoError(t, err)
		// Row 0 (baseline) consumed, rows 1-3 stored = 3 output rows
		require.Len(t, usage, 3)

		// Row 1: normal delta = 1000100 - 1000000 = 100
		require.NotNil(t, usage[0].InOctetsDelta)
		require.Equal(t, int64(100), *usage[0].InOctetsDelta)

		// Row 2 (stale): delta is negative (regression), baseline NOT updated
		require.NotNil(t, usage[1].InOctetsDelta)
		require.Equal(t, int64(500000-1000100), *usage[1].InOctetsDelta) // negative, stored as-is

		// Row 3 (real): delta must be computed against the high-water mark (1000100),
		// NOT the stale value (500000). Without the fix this would be 1000200-500000=500200.
		require.NotNil(t, usage[2].InOctetsDelta)
		require.Equal(t, int64(1000200-1000100), *usage[2].InOctetsDelta) // 100, not 500200
	})
}

// captureIntfCounterWindows returns a View whose InfluxDB mock records the
// [start, end) of every QueryIntfCounters sub-query so tests can assert the
// span a refresh actually queried. The mock returns no rows, so Refresh
// completes without exercising the convert/insert path (not what these tests
// cover).
func captureIntfCounterWindows(t *testing.T, clock clockwork.Clock, chunk time.Duration) (*View, *[][2]time.Time) {
	t.Helper()
	windows := &[][2]time.Time{}
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			*windows = append(*windows, [2]time.Time{s, e})
			return nil, nil
		},
	}
	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clock,
		ClickHouse:      testClient(t),
		InfluxDB:        influx,
		Bucket:          "test-bucket",
		RefreshInterval: time.Second,
		QueryWindow:     time.Hour,
		QueryChunk:      chunk,
	})
	require.NoError(t, err)
	return view, windows
}

// seedMaxTime inserts a single interface-counter row at event_ts=ts so the next
// refresh reads ts as maxTime (the ingest watermark).
func seedMaxTime(t *testing.T, v *View, ts time.Time) {
	t.Helper()
	dev, intf := "seed-device", "eth0"
	err := v.store.InsertInterfaceUsage(context.Background(), []InterfaceUsage{{
		Time:     ts.UTC(),
		DevicePK: &dev,
		Intf:     &intf,
	}})
	require.NoError(t, err)
}

// span returns the overall [start, end) covered by the recorded sub-query
// windows (first start, last end).
func span(t *testing.T, windows [][2]time.Time) (time.Time, time.Time) {
	t.Helper()
	require.NotEmpty(t, windows, "expected at least one InfluxDB sub-query")
	return windows[0][0], windows[len(windows)-1][1]
}

// Regression for #708: an incremental refresh (maxTime inside the query window)
// must query PAST maxTime, not stop at it. The catch-up cap is anchored at
// maxTime, so the span is refreshOverlap behind maxTime plus one QueryChunk of
// new data. On the buggy code the cap was anchored at queryStart (= maxTime −
// overlap) and equalled the overlap, so queryEnd landed exactly at maxTime and
// no new data was ever ingested.
func TestLake_TelemetryUsage_View_Refresh_IncrementalQueriesPastMaxTime(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	// event_ts is DateTime64(3); align the seed so it round-trips exactly.
	// 30m stale: inside the 1h window, far enough behind that the catch-up cap
	// binds.
	maxTime := now.Add(-30 * time.Minute).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)

	start, end := span(t, *windows)
	require.Equal(t, maxTime.Add(-refreshOverlap).UTC(), start.UTC(),
		"refresh must re-read the overlap behind maxTime")
	require.Equal(t, maxTime.Add(maxCatchupChunks*chunk).UTC(), end.UTC(),
		"refresh must query maxCatchupChunks PAST maxTime (the #708 regression)")
	require.True(t, end.After(maxTime), "queryEnd must advance past maxTime")
	// overlap + two chunks = 3 chunk-sized sub-queries.
	require.Len(t, *windows, 3)
}

// Successive refreshes must make monotonic forward progress — the sawtooth is
// gone. A refresh behind by more than the cap advances exactly the capped two
// chunks; once within the cap it reads through to now. Re-seeding the
// watermark to the prior queryEnd stands in for the insert that advances
// maxTime in production.
func TestLake_TelemetryUsage_View_Refresh_AdvancesMonotonically(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	// event_ts is DateTime64(3); align the seed so it round-trips exactly.
	// 12m stale: beyond the 10m cap, so the first refresh is capped.
	maxTime := now.Add(-12 * time.Minute).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)
	_, end1 := span(t, *windows)
	require.Equal(t, maxTime.Add(maxCatchupChunks*chunk).UTC(), end1.UTC(),
		"a capped refresh advances exactly maxCatchupChunks chunks")

	// Simulate the insert advancing the watermark to the first refresh's end.
	*windows = nil
	seedMaxTime(t, view, end1)

	_, err = view.Refresh(t.Context())
	require.NoError(t, err)
	_, end2 := span(t, *windows)
	require.Equal(t, now.UTC(), end2.UTC(), "within the cap, a refresh reads through to now")
	require.True(t, end2.After(end1), "progress must be monotonic")
}

// The catch-up cap (the #665 memory bound) is preserved when maxTime is older
// than the query window or absent: the span covers exactly maxCatchupChunks
// chunks of new data, regardless of how far behind maxTime is. Since #718 a
// maxTime older than the query window (but within maxCatchupHorizon) anchors
// the catch-up at maxTime itself — the old jump to the window start silently
// dropped [maxTime, now−QueryWindow).
func TestLake_TelemetryUsage_View_Refresh_CatchupCapPreserved(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute

	t.Run("maxTime older than query window", func(t *testing.T) {
		t.Parallel()
		clock := clockwork.NewFakeClock()
		now := clock.Now()
		// Outside the 1h query window, inside the 24h horizon.
		maxTime := now.Add(-90 * time.Minute).Truncate(time.Millisecond)

		view, windows := captureIntfCounterWindows(t, clock, chunk)
		seedMaxTime(t, view, maxTime)

		_, err := view.Refresh(t.Context())
		require.NoError(t, err)

		start, end := span(t, *windows)
		require.Equal(t, maxTime.Add(-refreshOverlap).UTC(), start.UTC(),
			"catch-up starts at the watermark, not the window start (the #718 data loss)")
		require.Equal(t, maxTime.Add(maxCatchupChunks*chunk).UTC(), end.UTC(),
			"catch-up ingests exactly maxCatchupChunks chunks")
		// overlap + two chunks = 3 chunk-sized sub-queries.
		require.Len(t, *windows, 3)
	})

	t.Run("empty table", func(t *testing.T) {
		t.Parallel()
		clock := clockwork.NewFakeClock()
		now := clock.Now()
		windowStart := now.Add(-time.Hour)

		view, windows := captureIntfCounterWindows(t, clock, chunk)

		_, err := view.Refresh(t.Context())
		require.NoError(t, err)

		start, end := span(t, *windows)
		require.Equal(t, windowStart.UTC(), start.UTC())
		require.Equal(t, windowStart.Add(maxCatchupChunks*chunk).UTC(), end.UTC())
	})
}

// When maxTime + QueryChunk is beyond now, the refresh must clamp queryEnd to
// now rather than overshooting into the future.
func TestLake_TelemetryUsage_View_Refresh_UncappedTailClampsToNow(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	maxTime := now.Add(-3 * time.Minute) // maxTime + 5m chunk > now

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)

	_, end := span(t, *windows)
	require.Equal(t, now.UTC(), end.UTC(), "queryEnd must clamp to now, never past it")
}

// Regression for #713: while catching up (maxTime far behind now) the refresh
// must keep the overlap re-read AND extend the forward cap, covering
// [maxTime−overlap, maxTime+2·chunk). The overlap cannot be traded for new
// data even though its rows dedup out: the re-read seeds non-sparse counter
// baselines for keys whose latest row is behind the global maxTime (see
// TestLake_TelemetryUsage_View_Refresh_CatchupEmitsFirstRowOfLaggingKey).
// Convergence comes from the second chunk instead.
func TestLake_TelemetryUsage_View_Refresh_CatchupKeepsOverlapAndExtendsCap(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	// 30m stale: inside the 1h query window, far beyond the 10m cap.
	maxTime := now.Add(-30 * time.Minute).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)

	start, end := span(t, *windows)
	require.Equal(t, maxTime.Add(-refreshOverlap).UTC(), start.UTC(),
		"catch-up must keep the overlap re-read — it seeds non-sparse baselines and catches late arrivals")
	require.Equal(t, maxTime.Add(maxCatchupChunks*chunk).UTC(), end.UTC(),
		"catch-up must ingest maxCatchupChunks chunks past maxTime")
	// overlap + two chunks = 3 chunk-sized sub-queries.
	require.Len(t, *windows, 3)
}

// The throughput inequality from #713: catch-up only converges if
// net_gain / cycle_time > 1 at a MODELED cycle time, not just per-refresh
// advancement. Model the prod incident: watermark ~58m stale, each refresh
// costs ~5m of wall clock (the measured cadence). With the 2-chunk cap the net
// gain is 10m per 5m cycle, so lag must pay down ~5m per cycle until the cap
// no longer binds, then hold at the refresh cadence. On the buggy code (1-chunk
// cap) net gain equalled cycle time and this loop stayed pinned at ~58m
// forever.
func TestLake_TelemetryUsage_View_Refresh_CatchupConvergesUnderModeledCycleTime(t *testing.T) {
	t.Parallel()

	const (
		chunk     = 5 * time.Minute
		cycleTime = 5 * time.Minute // measured prod refresh cadence (#713)
	)
	clock := clockwork.NewFakeClock()
	maxTime := clock.Now().Add(-58 * time.Minute).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	// 58m → converged at −5m net per capped cycle needs ~10 cycles; run a few
	// more to verify lag holds (does not re-grow) once the cap stops binding.
	for cycle := 0; cycle < 13; cycle++ {
		*windows = nil
		_, err := view.Refresh(t.Context())
		require.NoError(t, err)

		_, end := span(t, *windows)
		require.True(t, end.After(maxTime), "watermark must advance every cycle (cycle %d)", cycle)

		// Stand-in for the insert advancing maxTime to the ingested end.
		maxTime = end
		seedMaxTime(t, view, maxTime)
		clock.Advance(cycleTime)
	}

	lag := clock.Now().Sub(maxTime)
	require.LessOrEqual(t, lag, cycleTime,
		"lag must converge to the refresh cadence and hold, got %s", lag)
}

// Regression for #718: a watermark gap larger than QueryWindow (but within
// maxCatchupHorizon) must be caught up, not skipped. On the buggy code
// queryStart jumped to now−QueryWindow and [maxTime, now−QueryWindow) was
// permanently dropped — staging lost ~30% of its interface-counter rows this
// way. Model a 2h ingest outage at the measured prod cycle time and assert the
// reads cover the whole gap contiguously until lag converges. The source has a
// row every minute across the whole timeline, so every span yields inserts:
// usage is never empty, sourceEmptyThrough is never set, and progress is
// carried by the real ClickHouse watermark alone.
func TestLake_TelemetryUsage_View_Refresh_GapBeyondQueryWindowIsCaughtUpNotSkipped(t *testing.T) {
	t.Parallel()

	const (
		chunk     = 5 * time.Minute
		cycleTime = 5 * time.Minute // measured prod refresh cadence (#713)
	)
	clock := clockwork.NewFakeClockAt(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))
	maxTime := clock.Now().Add(-2 * time.Hour)
	const dev, intf = "gap-device", "eth0"

	windows := &[][2]time.Time{}
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			*windows = append(*windows, [2]time.Time{s, e})
			var out []map[string]any
			for ts, k := maxTime.Add(-time.Hour), int64(0); ts.Before(e); ts, k = ts.Add(time.Minute), k+1 {
				if !ts.Before(s) {
					out = append(out, map[string]any{
						"time":       ts.UTC().Format(time.RFC3339Nano),
						"dzd_pubkey": dev,
						"intf":       intf,
						"in-octets":  1000 + k,
					})
				}
			}
			return out, nil
		},
	}
	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clock,
		ClickHouse:      testClient(t),
		InfluxDB:        influx,
		Bucket:          "test-bucket",
		RefreshInterval: time.Second,
		QueryWindow:     time.Hour,
		QueryChunk:      chunk,
	})
	require.NoError(t, err)
	// The row at maxTime (minute 60 on the mock's grid) is already written —
	// it defines the ingest watermark the outage left behind.
	seedUsageRow(t, view, dev, intf, maxTime, 1060)

	prevEnd := maxTime
	// A capped cycle advances the watermark ~9m (last 1m-grid row inside the
	// 10m cap) while the clock advances 5m, so 120m of lag pays down ~4m per
	// cycle: ~28 capped cycles, then a few more to verify lag holds once the
	// cap stops binding.
	for cycle := 0; cycle < 33; cycle++ {
		*windows = nil
		_, err := view.Refresh(t.Context())
		require.NoError(t, err)

		start, end := span(t, *windows)
		require.False(t, start.After(prevEnd),
			"cycle %d: read must start at or before the previous end — a later start is a silently skipped span", cycle)
		require.True(t, end.After(prevEnd), "cycle %d: watermark must advance", cycle)

		prevEnd = end
		clock.Advance(cycleTime)
	}

	lag := clock.Now().Sub(prevEnd)
	require.LessOrEqual(t, lag, cycleTime,
		"a 2h gap must fully converge to the refresh cadence, got lag %s", lag)
}

// The only remaining skip-ahead (#718): a watermark older than
// maxCatchupHorizon jumps to now−QueryWindow — logging the dropped span at
// ERROR — and the capped read proceeds from the window start.
func TestLake_TelemetryUsage_View_Refresh_SkipsOnlyBeyondCatchupHorizon(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	windowStart := now.Add(-time.Hour)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, now.Add(-maxCatchupHorizon-time.Hour).Truncate(time.Millisecond)) // 25h stale

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)

	start, end := span(t, *windows)
	require.Equal(t, windowStart.UTC(), start.UTC(),
		"beyond the horizon the refresh skips to the window start")
	require.Equal(t, windowStart.Add(maxCatchupChunks*chunk).UTC(), end.UTC(),
		"the capped span still binds after the skip")
	require.Len(t, *windows, maxCatchupChunks)
}

// Removing the jump (#718) must not let a genuine source data gap pin
// catch-up: a capped span that returned zero rows AND has aged out of
// QueryWindow held nothing for the full late-arrival window, so the next
// refresh anchors past it instead of re-reading the same span forever
// (maxTime only advances on insert).
func TestLake_TelemetryUsage_View_Refresh_EmptyCappedSpanAdvancesAnchor(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	// 2h stale: the capped span [maxTime−5m, maxTime+10m) ends well before
	// now−QueryWindow (1h), so it has aged out of the late-arrival window.
	maxTime := now.Add(-2 * time.Hour).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk) // mock returns zero rows
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)
	_, end1 := span(t, *windows)
	require.Equal(t, maxTime.Add(maxCatchupChunks*chunk).UTC(), end1.UTC())

	// maxTime is unchanged (nothing was inserted), but the aged-out capped
	// span was proven empty — the next refresh must anchor at its end, not
	// re-read it.
	*windows = nil
	_, err = view.Refresh(t.Context())
	require.NoError(t, err)

	start2, end2 := span(t, *windows)
	require.Equal(t, end1.Add(-refreshOverlap).UTC(), start2.UTC(),
		"next refresh anchors at the proven-empty span's end (minus the standard overlap)")
	require.Equal(t, end1.Add(maxCatchupChunks*chunk).UTC(), end2.UTC(),
		"gap traversal proceeds one capped span per cycle")
}

// A capped span that is empty but still within QueryWindow must NOT advance
// the anchor: it may only be empty *yet* (source stall), and a writer that
// replays buffered data with past timestamps within QueryWindow must lose
// nothing. The span is re-read every cycle until it ages out.
func TestLake_TelemetryUsage_View_Refresh_EmptyYoungCappedSpanKeepsAnchor(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	now := clock.Now()
	// 30m stale: capped (beyond the 10m cap) but the span end (maxTime+10m =
	// now−20m) is still inside the 1h late-arrival window.
	maxTime := now.Add(-30 * time.Minute).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)

	*windows = nil
	_, err = view.Refresh(t.Context())
	require.NoError(t, err)

	start2, _ := span(t, *windows)
	require.Equal(t, maxTime.Add(-refreshOverlap).UTC(), start2.UTC(),
		"an empty span still within QueryWindow must be re-read, not skipped — buffered data may yet be replayed into it")
}

// The anchor advance keys on zero INGESTIBLE rows, not zero raw rows: an
// aged-out capped span whose only rows are overlap re-reads that dedup out
// (already written) produces nothing to insert and must still advance —
// otherwise the pin the advance exists to break would persist whenever the
// overlap contains data.
func TestLake_TelemetryUsage_View_Refresh_DedupedOnlyAgedSpanAdvancesAnchor(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClockAt(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))
	now := clock.Now()
	maxTime := now.Add(-2 * time.Hour) // aged out of the 1h QueryWindow
	const dev, intf = "dedup-device", "eth0"

	// InfluxDB holds only the row a previous refresh already wrote; the
	// overlap re-read returns it and it dedups out.
	windows := &[][2]time.Time{}
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			*windows = append(*windows, [2]time.Time{s, e})
			if !maxTime.Before(s) && maxTime.Before(e) {
				return []map[string]any{{
					"time":       maxTime.UTC().Format(time.RFC3339Nano),
					"dzd_pubkey": dev,
					"intf":       intf,
					"in-octets":  int64(1000),
				}}, nil
			}
			return nil, nil
		},
	}
	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clock,
		ClickHouse:      testClient(t),
		InfluxDB:        influx,
		Bucket:          "test-bucket",
		RefreshInterval: time.Second,
		QueryWindow:     time.Hour,
		QueryChunk:      chunk,
	})
	require.NoError(t, err)
	seedUsageRow(t, view, dev, intf, maxTime, 1000)

	_, err = view.Refresh(t.Context())
	require.NoError(t, err)

	// Nothing was inserted (the only row deduped out), but the aged-out span
	// [maxTime, maxTime+10m) was proven to hold nothing ingestible — the next
	// refresh must anchor past it.
	*windows = nil
	_, err = view.Refresh(t.Context())
	require.NoError(t, err)

	start2, _ := span(t, *windows)
	require.Equal(t, maxTime.Add(maxCatchupChunks*chunk).Add(-refreshOverlap).UTC(), start2.UTC(),
		"a deduped-only aged span must advance the anchor like a zero-row one")
}

// An UNCAPPED empty window (steady state) must not advance the anchor: the
// window isn't pinned (it grows with now), and keeping the watermark
// maximizes the late-arrival re-read.
func TestLake_TelemetryUsage_View_Refresh_EmptyUncappedWindowKeepsAnchor(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClock()
	maxTime := clock.Now().Add(-3 * time.Minute).Truncate(time.Millisecond)

	view, windows := captureIntfCounterWindows(t, clock, chunk)
	seedMaxTime(t, view, maxTime)

	_, err := view.Refresh(t.Context())
	require.NoError(t, err)

	clock.Advance(2 * time.Minute)
	*windows = nil
	_, err = view.Refresh(t.Context())
	require.NoError(t, err)

	start2, _ := span(t, *windows)
	require.Equal(t, maxTime.Add(-refreshOverlap).UTC(), start2.UTC(),
		"an uncapped empty window must keep anchoring at the watermark")
}

// Regression for the #714 review: a catch-up refresh must not swallow the
// first new row of a key whose latest written row is behind the global
// maxTime — the norm for an unsynchronized multi-device fleet. Non-sparse
// counters (in-octets, …) have no ClickHouse baseline; their delta continuity
// depends on the overlap re-read returning each key's already-written rows,
// which seed lastKnownValues/firstRowSeen via the dedup path. If catch-up
// starts the read at maxTime instead of maxTime−overlap, the lagging key's
// first new row is consumed as a baseline and never inserted, silently
// undercounting its traffic across the whole recovered region.
func TestLake_TelemetryUsage_View_Refresh_CatchupEmitsFirstRowOfLaggingKey(t *testing.T) {
	t.Parallel()

	const chunk = 5 * time.Minute
	clock := clockwork.NewFakeClockAt(time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC))
	now := clock.Now()
	// Deep catch-up: 30m stale, far beyond the 10m cap.
	maxTime := now.Add(-30 * time.Minute)
	// lag-device's latest written row is 2m behind the global maxTime (set by
	// fast-device), inside the overlap.
	lagTime := maxTime.Add(-2 * time.Minute)

	const fastDev, lagDev, intf = "fast-device", "lag-device", "eth0"

	influxRow := func(ts time.Time, dev string, inOctets int64) map[string]any {
		return map[string]any{
			"time":       ts.UTC().Format(time.RFC3339Nano),
			"dzd_pubkey": dev,
			"intf":       intf,
			"in-octets":  inOctets,
		}
	}
	// InfluxDB holds the rows a previous refresh already wrote (returned by
	// the overlap re-read, they dedup out) plus one new row per device.
	influxRows := []map[string]any{
		influxRow(lagTime, lagDev, 1000),
		influxRow(maxTime, fastDev, 5000),
		influxRow(maxTime.Add(time.Minute), lagDev, 1500),
		influxRow(maxTime.Add(time.Minute), fastDev, 5600),
	}
	influx := &mockInfluxDBClient{
		queryIntfCountersFunc: func(_ context.Context, s, e time.Time) ([]map[string]any, error) {
			var out []map[string]any
			for _, r := range influxRows {
				ts, err := time.Parse(time.RFC3339Nano, r["time"].(string))
				if err != nil {
					return nil, err
				}
				if !ts.Before(s) && ts.Before(e) {
					out = append(out, r)
				}
			}
			return out, nil
		},
	}

	view, err := NewView(ViewConfig{
		Logger:          laketesting.NewLogger(),
		Clock:           clock,
		ClickHouse:      testClient(t),
		InfluxDB:        influx,
		Bucket:          "test-bucket",
		RefreshInterval: time.Second,
		QueryWindow:     time.Hour,
		QueryChunk:      chunk,
	})
	require.NoError(t, err)

	// Seed ClickHouse with what the previous refresh already wrote.
	seedUsageRow(t, view, lagDev, intf, lagTime, 1000)
	seedUsageRow(t, view, fastDev, intf, maxTime, 5000)

	_, err = view.Refresh(t.Context())
	require.NoError(t, err)

	// lag-device's new row must be inserted with its delta computed against
	// the re-read overlap row (1500−1000), not swallowed as a baseline.
	lagDeltas := queryInOctetsDeltas(t, view, lagDev, maxTime)
	require.Len(t, lagDeltas, 1, "lag-device's first new row must be emitted, not consumed as a baseline")
	delta, ok := lagDeltas[maxTime.Add(time.Minute).UTC()]
	require.True(t, ok, "expected lag-device row at maxTime+1m, got %v", lagDeltas)
	require.NotNil(t, delta)
	require.Equal(t, int64(500), *delta)

	// fast-device (the key that defines the global maxTime) also emits its new
	// row with a correct delta.
	fastDeltas := queryInOctetsDeltas(t, view, fastDev, maxTime.Add(time.Second))
	require.Len(t, fastDeltas, 1)
	delta, ok = fastDeltas[maxTime.Add(time.Minute).UTC()]
	require.True(t, ok, "expected fast-device row at maxTime+1m, got %v", fastDeltas)
	require.NotNil(t, delta)
	require.Equal(t, int64(600), *delta)
}

// seedUsageRow inserts an interface-counter row with an in-octets value, as a
// previous refresh would have written it.
func seedUsageRow(t *testing.T, v *View, dev, intf string, ts time.Time, inOctets int64) {
	t.Helper()
	err := v.store.InsertInterfaceUsage(context.Background(), []InterfaceUsage{{
		Time:     ts.UTC(),
		DevicePK: &dev,
		Intf:     &intf,
		InOctets: &inOctets,
	}})
	require.NoError(t, err)
}

// queryInOctetsDeltas returns event_ts → in_octets_delta for the device's rows
// at or after since.
func queryInOctetsDeltas(t *testing.T, v *View, dev string, since time.Time) map[time.Time]*int64 {
	t.Helper()
	ctx := context.Background()
	conn, err := v.cfg.ClickHouse.Conn(ctx)
	require.NoError(t, err)
	rows, err := conn.Query(ctx,
		"SELECT event_ts, in_octets_delta FROM fact_dz_device_interface_counters WHERE device_pk = ? AND event_ts >= ?",
		dev, since.UTC())
	require.NoError(t, err)
	defer rows.Close()
	out := map[time.Time]*int64{}
	for rows.Next() {
		var ts time.Time
		var delta *int64
		require.NoError(t, rows.Scan(&ts, &delta))
		out[ts.UTC()] = delta
	}
	require.NoError(t, rows.Err())
	return out
}
