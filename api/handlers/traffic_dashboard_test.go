package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupDashboardSchema creates Memory engine tables for the traffic dashboard endpoints.
func setupDashboardSchema(t *testing.T) {
	ctx := t.Context()

	tables := []string{
		`CREATE TABLE IF NOT EXISTS fact_dz_device_interface_counters (
			event_ts DateTime64(3),
			device_pk String,
			intf String,
			link_pk String,
			in_octets_delta Nullable(Int64),
			out_octets_delta Nullable(Int64),
			delta_duration Nullable(Float64),
			in_discards_delta Nullable(Int64),
			out_discards_delta Nullable(Int64),
			user_tunnel_id Nullable(Int64),
			in_pkts_delta Nullable(Int64),
			out_pkts_delta Nullable(Int64),
			in_errors_delta Nullable(Int64),
			out_errors_delta Nullable(Int64),
			in_fcs_errors_delta Nullable(Int64),
			carrier_transitions_delta Nullable(Int64)
		) ENGINE = Memory`,

		`CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String,
			code String,
			metro_pk String,
			contributor_pk String
		) ENGINE = Memory`,

		`CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String,
			bandwidth_bps Int64,
			link_type String,
			contributor_pk String
		) ENGINE = Memory`,

		`CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String,
			code String,
			name String
		) ENGINE = Memory`,

		`CREATE TABLE IF NOT EXISTS dz_contributors_current (
			pk String,
			code String,
			name String
		) ENGINE = Memory`,
	}

	for _, ddl := range tables {
		require.NoError(t, config.DB.Exec(ctx, ddl))
	}
}

// seedDashboardData inserts dimension and fact data for testing dashboard queries.
// Two devices in different metros with different link types, each with 3 recent samples.
func seedDashboardData(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_metros_current VALUES
		('metro-1', 'FRA', 'Frankfurt'), ('metro-2', 'AMS', 'Amsterdam')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_contributors_current VALUES
		('contrib-1', 'ACME', 'Acme Corp'), ('contrib-2', 'BETA', 'Beta Inc')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_devices_current VALUES
		('dev-1', 'ROUTER-FRA-1', 'metro-1', 'contrib-1'),
		('dev-2', 'ROUTER-AMS-1', 'metro-2', 'contrib-2')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_links_current VALUES
		('link-1', 100000000000, 'WAN', 'contrib-1'),
		('link-2', 10000000000, 'PNI', 'contrib-2')`))

	// Device 1: Port-Channel1000 on 100Gbps WAN link
	// Device 2: Ethernet1/1 on 10Gbps PNI link
	// Varying traffic levels to produce meaningful percentile spreads
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_discards_delta, out_discards_delta)
		VALUES
		(now() - INTERVAL 30 MINUTE, 'dev-1', 'Port-Channel1000', 'link-1', 300000000000, 200000000000, 30.0, 0, 0),
		(now() - INTERVAL 20 MINUTE, 'dev-1', 'Port-Channel1000', 'link-1', 350000000000, 250000000000, 30.0, 5, 2),
		(now() - INTERVAL 10 MINUTE, 'dev-1', 'Port-Channel1000', 'link-1', 100000000000, 50000000000, 30.0, 0, 0),
		(now() - INTERVAL 30 MINUTE, 'dev-2', 'Ethernet1/1', 'link-2', 18750000000, 12500000000, 30.0, 0, 0),
		(now() - INTERVAL 20 MINUTE, 'dev-2', 'Ethernet1/1', 'link-2', 22500000000, 15000000000, 30.0, 0, 1),
		(now() - INTERVAL 10 MINUTE, 'dev-2', 'Ethernet1/1', 'link-2', 7500000000, 3750000000, 30.0, 0, 0)`))
}

// --- Stress endpoint tests ---

func TestTrafficDashboardStress(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name    string
		query   string
		grouped bool
	}{
		{"utilization", "?time_range=1h&metric=utilization", false},
		{"throughput", "?time_range=1h&metric=throughput", false},
		{"packets", "?time_range=1h&metric=packets", false},
		{"group_by_metro", "?time_range=1h&group_by=metro", true},
		{"group_by_device", "?time_range=1h&group_by=device", true},
		{"group_by_link_type", "?time_range=1h&group_by=link_type", true},
		{"group_by_contributor", "?time_range=1h&group_by=contributor", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/stress"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardStress(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.StressResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			assert.NotEmpty(t, resp.EffBucket)

			if tt.grouped {
				assert.NotEmpty(t, resp.Groups, "should have group data")
				for _, g := range resp.Groups {
					assert.NotEmpty(t, g.Key)
				}
			} else {
				assert.NotEmpty(t, resp.Timestamps, "should have timestamps")
				assert.Len(t, resp.P50In, len(resp.Timestamps))
				assert.Len(t, resp.P95In, len(resp.Timestamps))
				assert.Len(t, resp.MaxIn, len(resp.Timestamps))
				assert.Len(t, resp.P50Out, len(resp.Timestamps))
				assert.Len(t, resp.P95Out, len(resp.Timestamps))
				assert.Len(t, resp.MaxOut, len(resp.Timestamps))
			}
		})
	}
}

func TestTrafficDashboardStress_Empty(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/stress?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardStress(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.StressResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Timestamps)
}

// --- Top endpoint tests ---

func TestTrafficDashboardTop(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"interface_max_util", "?time_range=1h&entity=interface&metric=max_util"},
		{"interface_p95_util", "?time_range=1h&entity=interface&metric=p95_util"},
		{"interface_avg_util", "?time_range=1h&entity=interface&metric=avg_util"},
		{"interface_max_throughput", "?time_range=1h&entity=interface&metric=max_throughput"},
		{"interface_max_in_bps", "?time_range=1h&entity=interface&metric=max_in_bps"},
		{"interface_max_out_bps", "?time_range=1h&entity=interface&metric=max_out_bps"},
		{"interface_bandwidth_bps", "?time_range=1h&entity=interface&metric=bandwidth_bps"},
		{"interface_headroom", "?time_range=1h&entity=interface&metric=headroom"},
		{"interface_dir_asc", "?time_range=1h&entity=interface&metric=max_util&dir=asc"},
		{"interface_dir_desc", "?time_range=1h&entity=interface&metric=max_util&dir=desc"},
		{"device_default", "?time_range=1h&entity=device"},
		{"device_max_util", "?time_range=1h&entity=device&metric=max_util"},
		{"device_max_throughput", "?time_range=1h&entity=device&metric=max_throughput"},
		{"device_dir_asc", "?time_range=1h&entity=device&metric=max_throughput&dir=asc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardTop(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TopResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			assert.NotEmpty(t, resp.Entities, "should return entities")
			// Verify contributor_code is populated from the join
			for _, e := range resp.Entities {
				assert.NotEmpty(t, e.ContributorCode, "contributor_code should be populated for %s %s", e.DeviceCode, e.Intf)
			}
		})
	}
}

func TestTrafficDashboardTop_Empty(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardTop(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TopResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Entities)
}

func TestTrafficDashboardTop_WithDimensionFilters(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"metro_filter", "?time_range=1h&entity=interface&metro=FRA"},
		{"link_type_filter", "?time_range=1h&entity=interface&link_type=WAN"},
		{"contributor_filter", "?time_range=1h&entity=interface&contributor=ACME"},
		{"multi_metro_filter", "?time_range=1h&entity=interface&metro=FRA,AMS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardTop(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TopResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		})
	}
}

func TestTrafficDashboardTop_WithIntfFilter(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name      string
		query     string
		wantCount int
	}{
		{"intf_filter_interface", "?time_range=1h&entity=interface&intf=Port-Channel1000", 1},
		{"intf_filter_device", "?time_range=1h&entity=device&intf=Port-Channel1000", 1},
		{"intf_filter_multi", "?time_range=1h&entity=interface&intf=Port-Channel1000,Ethernet1/1", 2},
		{"intf_filter_no_match", "?time_range=1h&entity=interface&intf=NonExistent99", 0},
		{"intf_and_metro_filter", "?time_range=1h&entity=interface&intf=Port-Channel1000&metro=FRA", 1},
		{"intf_and_wrong_metro", "?time_range=1h&entity=interface&intf=Port-Channel1000&metro=AMS", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardTop(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TopResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			assert.Len(t, resp.Entities, tt.wantCount)
		})
	}
}

func TestTrafficDashboardStress_WithIntfFilter(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"intf_filter", "?time_range=1h&metric=throughput&intf=Port-Channel1000"},
		{"intf_filter_grouped", "?time_range=1h&metric=throughput&group_by=device&intf=Port-Channel1000"},
		{"intf_filter_multi", "?time_range=1h&metric=throughput&intf=Port-Channel1000,Ethernet1/1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/stress"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardStress(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.StressResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		})
	}
}

func TestTrafficDashboardBurstiness_WithIntfFilter(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness?time_range=1h&intf=Port-Channel1000", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardBurstiness(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.BurstinessResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
}

// --- Traffic type filter tests ---

// seedTrafficTypeData inserts data with different traffic types: link, tunnel, and other.
// Multiple samples per interface to produce meaningful percentile spreads for burstiness.
func seedTrafficTypeData(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_metros_current VALUES
		('metro-1', 'FRA', 'Frankfurt')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_contributors_current VALUES
		('contrib-1', 'ACME', 'Acme Corp')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_devices_current VALUES
		('dev-1', 'ROUTER-FRA-1', 'metro-1', 'contrib-1')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_links_current VALUES
		('link-1', 100000000000, 'WAN', 'contrib-1')`))

	// Link interface: has link_pk (3 samples with varying traffic)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_discards_delta, out_discards_delta, user_tunnel_id)
		VALUES
		(now() - INTERVAL 30 MINUTE, 'dev-1', 'Ethernet1', 'link-1', 100000000000, 50000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 20 MINUTE, 'dev-1', 'Ethernet1', 'link-1', 200000000000, 100000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 10 MINUTE, 'dev-1', 'Ethernet1', 'link-1', 50000000000, 25000000000, 30.0, 0, 0, NULL)`))

	// Tunnel interface: has user_tunnel_id (3 samples with varying traffic)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_discards_delta, out_discards_delta, user_tunnel_id)
		VALUES
		(now() - INTERVAL 30 MINUTE, 'dev-1', 'Tunnel100', '', 50000000000, 25000000000, 30.0, 0, 0, 42),
		(now() - INTERVAL 20 MINUTE, 'dev-1', 'Tunnel100', '', 100000000000, 50000000000, 30.0, 0, 0, 42),
		(now() - INTERVAL 10 MINUTE, 'dev-1', 'Tunnel100', '', 25000000000, 12500000000, 30.0, 0, 0, 42)`))

	// Other interface: no link_pk, no user_tunnel_id (3 samples with varying traffic)
	// Traffic must be above 1 Mbps (p50) to pass the burstiness minimum throughput filter.
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_discards_delta, out_discards_delta, user_tunnel_id)
		VALUES
		(now() - INTERVAL 30 MINUTE, 'dev-1', 'Loopback0', '', 10000000000, 5000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 20 MINUTE, 'dev-1', 'Loopback0', '', 30000000000, 15000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 10 MINUTE, 'dev-1', 'Loopback0', '', 5000000000, 2500000000, 30.0, 0, 0, NULL)`))
}

func TestTrafficDashboardTop_WithTrafficType(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedTrafficTypeData(t)

	tests := []struct {
		name      string
		query     string
		wantIntfs []string
	}{
		{
			name:      "all_traffic",
			query:     "?time_range=1h&entity=interface",
			wantIntfs: []string{"Ethernet1", "Loopback0", "Tunnel100"},
		},
		{
			name:      "all_traffic_explicit",
			query:     "?time_range=1h&entity=interface&intf_type=all",
			wantIntfs: []string{"Ethernet1", "Loopback0", "Tunnel100"},
		},
		{
			name:      "link_only",
			query:     "?time_range=1h&entity=interface&intf_type=link",
			wantIntfs: []string{"Ethernet1"},
		},
		{
			name:      "tunnel_only",
			query:     "?time_range=1h&entity=interface&intf_type=tunnel",
			wantIntfs: []string{"Tunnel100"},
		},
		{
			name:      "other_only",
			query:     "?time_range=1h&entity=interface&intf_type=other",
			wantIntfs: []string{"Loopback0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardTop(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TopResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

			var gotIntfs []string
			for _, e := range resp.Entities {
				gotIntfs = append(gotIntfs, e.Intf)
			}
			assert.ElementsMatch(t, tt.wantIntfs, gotIntfs)
		})
	}
}

func TestTrafficDashboardStress_WithTrafficType(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedTrafficTypeData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"all_traffic", "?time_range=1h&metric=throughput"},
		{"link_only", "?time_range=1h&metric=throughput&intf_type=link"},
		{"tunnel_only", "?time_range=1h&metric=throughput&intf_type=tunnel"},
		{"other_only", "?time_range=1h&metric=throughput&intf_type=other"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/stress"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardStress(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.StressResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		})
	}
}

func TestTrafficDashboardBurstiness_WithTrafficType(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedTrafficTypeData(t)

	tests := []struct {
		name      string
		query     string
		wantIntfs []string
	}{
		{
			name:      "all_traffic",
			query:     "?time_range=1h",
			wantIntfs: []string{"Ethernet1", "Tunnel100", "Loopback0"},
		},
		{
			name:      "all_traffic_explicit",
			query:     "?time_range=1h&intf_type=all",
			wantIntfs: []string{"Ethernet1", "Tunnel100", "Loopback0"},
		},
		{
			name:      "tunnel_only",
			query:     "?time_range=1h&intf_type=tunnel",
			wantIntfs: []string{"Tunnel100"},
		},
		{
			name:      "other_only",
			query:     "?time_range=1h&intf_type=other",
			wantIntfs: []string{"Loopback0"},
		},
		{
			name:      "link_only",
			query:     "?time_range=1h&intf_type=link",
			wantIntfs: []string{"Ethernet1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardBurstiness(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.BurstinessResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

			var gotIntfs []string
			for _, e := range resp.Entities {
				gotIntfs = append(gotIntfs, e.Intf)
				// All entities should have p50_bps and p99_bps populated
				assert.Greater(t, e.P50Bps, float64(0), "p50_bps should be > 0 for %s", e.Intf)
				assert.Greater(t, e.P99Bps, float64(0), "p99_bps should be > 0 for %s", e.Intf)
			}
			assert.ElementsMatch(t, tt.wantIntfs, gotIntfs)
		})
	}
}

// --- Drilldown endpoint tests ---

func TestTrafficDashboardDrilldown(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	// Use dynamic timestamps that cover the seeded data (now-30m to now)
	now := time.Now()
	startTs := fmt.Sprintf("%d", now.Add(-1*time.Hour).Unix())
	endTs := fmt.Sprintf("%d", now.Add(1*time.Minute).Unix())

	tests := []struct {
		name   string
		query  string
		status int
	}{
		{"with_intf", "?time_range=1h&device_pk=dev-1&intf=Port-Channel1000", http.StatusOK},
		{"all_interfaces", "?time_range=1h&device_pk=dev-1", http.StatusOK},
		{"custom_time_range", "?start_time=" + startTs + "&end_time=" + endTs + "&device_pk=dev-1", http.StatusOK},
		{"missing_device_pk", "?time_range=1h", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/drilldown"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardDrilldown(rr, req)

			require.Equal(t, tt.status, rr.Code, "body: %s", rr.Body.String())

			if tt.status == http.StatusOK {
				var resp handlers.DrilldownResponse
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
				assert.NotEmpty(t, resp.Points, "should return data points")
				assert.NotEmpty(t, resp.EffBucket)
			}
		})
	}
}

func TestTrafficDashboardDrilldown_Empty(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/drilldown?time_range=1h&device_pk=nonexistent", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardDrilldown(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.DrilldownResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Points)
}

// --- Burstiness endpoint tests ---

func TestTrafficDashboardBurstiness(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"default", "?time_range=1h"},
		{"sort_burstiness", "?time_range=1h&sort=burstiness"},
		{"sort_p50_util", "?time_range=1h&sort=p50_util"},
		{"sort_p99_util", "?time_range=1h&sort=p99_util"},
		{"sort_pct_time_stressed", "?time_range=1h&sort=pct_time_stressed"},
		{"dir_asc", "?time_range=1h&sort=burstiness&dir=asc"},
		{"dir_desc", "?time_range=1h&sort=burstiness&dir=desc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardBurstiness(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.BurstinessResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			for _, e := range resp.Entities {
				assert.NotEmpty(t, e.ContributorCode, "contributor_code should be populated for %s %s", e.DeviceCode, e.Intf)
			}
		})
	}
}

// --- Scoped field values tests ---

func TestFieldValues_ScopedByDashboardFilters(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedDashboardData(t)

	tests := []struct {
		name      string
		query     string
		wantVals  []string
		wantEmpty bool
	}{
		{
			name:     "intf_unscoped",
			query:    "?entity=interfaces&field=intf",
			wantVals: []string{"Ethernet1/1", "Port-Channel1000"},
		},
		{
			name:     "intf_scoped_by_metro_FRA",
			query:    "?entity=interfaces&field=intf&metro=FRA",
			wantVals: []string{"Port-Channel1000"},
		},
		{
			name:     "intf_scoped_by_metro_AMS",
			query:    "?entity=interfaces&field=intf&metro=AMS",
			wantVals: []string{"Ethernet1/1"},
		},
		{
			name:      "intf_scoped_by_nonexistent_metro",
			query:     "?entity=interfaces&field=intf&metro=NYC",
			wantEmpty: true,
		},
		{
			name:     "intf_scoped_by_device",
			query:    "?entity=interfaces&field=intf&device=ROUTER-FRA-1",
			wantVals: []string{"Port-Channel1000"},
		},
		{
			name:     "intf_scoped_by_contributor",
			query:    "?entity=interfaces&field=intf&contributor=ACME",
			wantVals: []string{"Port-Channel1000"},
		},
		{
			name:     "intf_scoped_by_link_type",
			query:    "?entity=interfaces&field=intf&link_type=PNI",
			wantVals: []string{"Ethernet1/1"},
		},
		{
			name:     "metro_scoped_by_contributor",
			query:    "?entity=devices&field=metro&contributor=ACME",
			wantVals: []string{"FRA"},
		},
		{
			name:     "contributor_scoped_by_metro",
			query:    "?entity=devices&field=contributor&metro=AMS",
			wantVals: []string{"BETA"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/dz/field-values"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetFieldValues(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.FieldValuesResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

			if tt.wantEmpty {
				assert.Empty(t, resp.Values)
			} else {
				assert.Equal(t, tt.wantVals, resp.Values)
			}
		})
	}
}

func TestTrafficDashboardBurstiness_Empty(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardBurstiness(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.BurstinessResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Entities)
}

// --- Health endpoint tests ---

// seedHealthData inserts data with nonzero error/discard/carrier values for health testing.
func seedHealthData(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_metros_current VALUES
		('metro-1', 'FRA', 'Frankfurt'), ('metro-2', 'AMS', 'Amsterdam')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_contributors_current VALUES
		('contrib-1', 'ACME', 'Acme Corp')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_devices_current VALUES
		('dev-1', 'ROUTER-FRA-1', 'metro-1', 'contrib-1'),
		('dev-2', 'ROUTER-AMS-1', 'metro-2', 'contrib-1')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dz_links_current VALUES
		('link-1', 100000000000, 'WAN', 'contrib-1')`))

	// dev-1 Ethernet1: has errors and discards (link interface)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration,
		 in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta,
		 in_fcs_errors_delta, carrier_transitions_delta)
		VALUES
		(now() - INTERVAL 20 MINUTE, 'dev-1', 'Ethernet1', 'link-1', 100000000, 50000000, 30.0,
		 10, 5, 3, 2, 1, 0),
		(now() - INTERVAL 10 MINUTE, 'dev-1', 'Ethernet1', 'link-1', 100000000, 50000000, 30.0,
		 20, 10, 0, 0, 0, 2)`))

	// dev-2 Ethernet2: has carrier transitions only (no link)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration,
		 in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta,
		 in_fcs_errors_delta, carrier_transitions_delta)
		VALUES
		(now() - INTERVAL 20 MINUTE, 'dev-2', 'Ethernet2', '', 100000000, 50000000, 30.0,
		 0, 0, 0, 0, 0, 5)`))

	// dev-1 Loopback0: zero errors (should not appear in results)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration,
		 in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta,
		 in_fcs_errors_delta, carrier_transitions_delta)
		VALUES
		(now() - INTERVAL 10 MINUTE, 'dev-1', 'Loopback0', '', 100000000, 50000000, 30.0,
		 0, 0, 0, 0, 0, 0)`))
}

func TestTrafficDashboardHealth(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)
	seedHealthData(t)

	t.Run("default", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h", nil)
		rr := httptest.NewRecorder()

		handlers.GetTrafficDashboardHealth(rr, req)

		require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

		var resp handlers.HealthResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		// Loopback0 has zero events, so only 2 interfaces should appear
		assert.Len(t, resp.Entities, 2)
		for _, e := range resp.Entities {
			assert.Greater(t, e.TotalEvents, int64(0))
			assert.NotEmpty(t, e.ContributorCode, "contributor_code should be populated for %s %s", e.DeviceCode, e.Intf)
		}
	})

	t.Run("sort_total_errors", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h&sort=total_errors&dir=desc", nil)
		rr := httptest.NewRecorder()

		handlers.GetTrafficDashboardHealth(rr, req)

		require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

		var resp handlers.HealthResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		assert.NotEmpty(t, resp.Entities)
		// First entity should have the most errors (dev-1 Ethernet1 has 45 total errors)
		assert.Equal(t, "Ethernet1", resp.Entities[0].Intf)
	})

	t.Run("sort_total_carrier_transitions", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h&sort=total_carrier_transitions&dir=desc", nil)
		rr := httptest.NewRecorder()

		handlers.GetTrafficDashboardHealth(rr, req)

		require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

		var resp handlers.HealthResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		assert.NotEmpty(t, resp.Entities)
		// First entity should have the most carrier transitions (dev-2 Ethernet2 has 5)
		assert.Equal(t, "Ethernet2", resp.Entities[0].Intf)
	})

	t.Run("intf_type_link_only", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h&intf_type=link", nil)
		rr := httptest.NewRecorder()

		handlers.GetTrafficDashboardHealth(rr, req)

		require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

		var resp handlers.HealthResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		// Only Ethernet1 is a link interface with events > 0
		assert.Len(t, resp.Entities, 1)
		assert.Equal(t, "Ethernet1", resp.Entities[0].Intf)
	})
}

func TestTrafficDashboardHealth_Empty(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupDashboardSchema(t)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardHealth(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.HealthResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Entities)
}
