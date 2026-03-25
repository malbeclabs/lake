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

// seedDashboardData inserts dimension and rollup data for testing dashboard queries.
// Two devices in different metros with different link types, each with 3 recent 5-minute buckets.
// Uses _history tables (SCD2 pattern) since the schema comes from migrations.
func seedDashboardData(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
		('metro-1', now(), now(), generateUUIDv4(), 0, 1, 'metro-1', 'FRA', 'Frankfurt'),
		('metro-2', now(), now(), generateUUIDv4(), 0, 2, 'metro-2', 'AMS', 'Amsterdam')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_contributors_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
		('contrib-1', now(), now(), generateUUIDv4(), 0, 1, 'contrib-1', 'ACME', 'Acme Corp'),
		('contrib-2', now(), now(), generateUUIDv4(), 0, 2, 'contrib-2', 'BETA', 'Beta Inc')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users)
		VALUES
		('dev-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-1', 'active', 'router', 'ROUTER-FRA-1', '', 'contrib-1', 'metro-1', 0),
		('dev-2', now(), now(), generateUUIDv4(), 0, 2, 'dev-2', 'active', 'router', 'ROUTER-AMS-1', '', 'contrib-2', 'metro-2', 0)`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_links_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, code, tunnel_net, contributor_pk, side_a_pk, side_z_pk,
		 side_a_iface_name, side_z_iface_name, link_type, committed_rtt_ns,
		 committed_jitter_ns, bandwidth_bps, isis_delay_override_ns)
		VALUES
		('link-1', now(), now(), generateUUIDv4(), 0, 1, 'link-1', 'active', '', '', 'contrib-1', '', '', '', '', 'WAN', 0, 0, 100000000000, 0),
		('link-2', now(), now(), generateUUIDv4(), 0, 2, 'link-2', 'active', '', '', 'contrib-2', '', '', '', '', 'PNI', 0, 0, 10000000000, 0)`))

	// Device 1: Port-Channel1000 on 100Gbps WAN link — 3 rollup buckets
	// Device 2: Ethernet1/1 on 10Gbps PNI link — 3 rollup buckets
	// Rates pre-computed from original raw deltas: rate = octets_delta * 8 / delta_duration
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 30 MINUTE), 'dev-1', 'Port-Channel1000', now(), 'link-1', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 80000000000, 80000000000, 80000000000, 80000000000, 80000000000, 80000000000, 80000000000,
		 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3,
		 0, 0, 0, 0, 0, 0, 0,
		 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-1', 'Port-Channel1000', now(), 'link-1', 'A', NULL, '',
		 5, 2, 0, 0, 0, 0,
		 93333333333.3, 93333333333.3, 93333333333.3, 93333333333.3, 93333333333.3, 93333333333.3, 93333333333.3,
		 66666666666.7, 66666666666.7, 66666666666.7, 66666666666.7, 66666666666.7, 66666666666.7, 66666666666.7,
		 0, 0, 0, 0, 0, 0, 0,
		 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Port-Channel1000', now(), 'link-1', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7,
		 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3,
		 0, 0, 0, 0, 0, 0, 0,
		 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 30 MINUTE), 'dev-2', 'Ethernet1/1', now(), 'link-2', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 5000000000, 5000000000, 5000000000, 5000000000, 5000000000, 5000000000, 5000000000,
		 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3,
		 0, 0, 0, 0, 0, 0, 0,
		 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-2', 'Ethernet1/1', now(), 'link-2', 'A', NULL, '',
		 0, 1, 0, 0, 0, 0,
		 6000000000, 6000000000, 6000000000, 6000000000, 6000000000, 6000000000, 6000000000,
		 4000000000, 4000000000, 4000000000, 4000000000, 4000000000, 4000000000, 4000000000,
		 0, 0, 0, 0, 0, 0, 0,
		 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-2', 'Ethernet1/1', now(), 'link-2', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 2000000000, 2000000000, 2000000000, 2000000000, 2000000000, 2000000000, 2000000000,
		 1000000000, 1000000000, 1000000000, 1000000000, 1000000000, 1000000000, 1000000000,
		 0, 0, 0, 0, 0, 0, 0,
		 0, 0, 0, 0, 0, 0, 0)`))

	// Add more "normal" low-traffic buckets so spike detection works.
	// With 10 total buckets, the P50 stays near the low value and the
	// high bucket from the original seed becomes a spike (>2x P50).
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		SELECT
			toStartOfFiveMinutes(now() - INTERVAL (40 + number * 5) MINUTE),
			device_pk, intf, now(), link_pk, 'A', NULL, '',
			0, 0, 0, 0, 0, 0,
			low_bps, low_bps, low_bps, low_bps, low_bps, low_bps, low_bps,
			low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2,
			0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0
		FROM (
			SELECT 'dev-1' AS device_pk, 'Port-Channel1000' AS intf, 'link-1' AS link_pk, 26666666666.7 AS low_bps
			UNION ALL
			SELECT 'dev-2', 'Ethernet1/1', 'link-2', 2000000000
		) AS intfs
		CROSS JOIN numbers(7) AS n`))

	// Also seed raw fact table for sub-5m bucket queries (same data as rollup)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, ingested_at, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_discards_delta, out_discards_delta)
		VALUES
		(now() - INTERVAL 30 MINUTE, now(), 'dev-1', 'Port-Channel1000', 'link-1', 300000000000, 200000000000, 30.0, 0, 0),
		(now() - INTERVAL 20 MINUTE, now(), 'dev-1', 'Port-Channel1000', 'link-1', 350000000000, 250000000000, 30.0, 5, 2),
		(now() - INTERVAL 10 MINUTE, now(), 'dev-1', 'Port-Channel1000', 'link-1', 100000000000, 50000000000, 30.0, 0, 0),
		(now() - INTERVAL 30 MINUTE, now(), 'dev-2', 'Ethernet1/1', 'link-2', 18750000000, 12500000000, 30.0, 0, 0),
		(now() - INTERVAL 20 MINUTE, now(), 'dev-2', 'Ethernet1/1', 'link-2', 22500000000, 15000000000, 30.0, 0, 1),
		(now() - INTERVAL 10 MINUTE, now(), 'dev-2', 'Ethernet1/1', 'link-2', 7500000000, 3750000000, 30.0, 0, 0)`))
}

// --- Stress endpoint tests ---

func TestTrafficDashboardStress(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardTop(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.TopResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Entities)
}

func TestTrafficDashboardTop_WithDimensionFilters(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
// Multiple rollup buckets per interface to produce meaningful percentile spreads for burstiness.
// Uses _history tables (SCD2 pattern) since the schema comes from migrations.
func seedTrafficTypeData(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
		('metro-1', now(), now(), generateUUIDv4(), 0, 1, 'metro-1', 'FRA', 'Frankfurt')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_contributors_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
		('contrib-1', now(), now(), generateUUIDv4(), 0, 1, 'contrib-1', 'ACME', 'Acme Corp')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users)
		VALUES
		('dev-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-1', 'active', 'router', 'ROUTER-FRA-1', '', 'contrib-1', 'metro-1', 0)`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_links_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, code, tunnel_net, contributor_pk, side_a_pk, side_z_pk,
		 side_a_iface_name, side_z_iface_name, link_type, committed_rtt_ns,
		 committed_jitter_ns, bandwidth_bps, isis_delay_override_ns)
		VALUES
		('link-1', now(), now(), generateUUIDv4(), 0, 1, 'link-1', 'active', '', '', 'contrib-1', '', '', '', '', 'WAN', 0, 0, 100000000000, 0)`))

	// Users: tunnel_id 42 = ibrl kind, tunnel_id 99 = validator kind
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_users_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id)
		VALUES
		('user-1', now(), now(), generateUUIDv4(), 0, 1, 'user-1', 'pubkey1', 'active', 'ibrl', '', '10.0.0.1', 'dev-1', 42),
		('user-2', now(), now(), generateUUIDv4(), 0, 2, 'user-2', 'pubkey2', 'active', 'validator', '', '10.0.0.2', 'dev-1', 99)`))

	// Helper for rollup column list
	// Link interface: Ethernet1 on link-1 (3 buckets with varying traffic)
	// Rates: 100G*8/30=26.67G, 200G*8/30=53.33G, 50G*8/30=13.33G
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 30 MINUTE), 'dev-1', 'Ethernet1', now(), 'link-1', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7,
		 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-1', 'Ethernet1', now(), 'link-1', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3, 53333333333.3,
		 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Ethernet1', now(), 'link-1', 'A', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3,
		 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))

	// Tunnel interface for user 42 (ibrl): Tunnel100 (3 buckets)
	// Rates: 50G*8/30=13.33G, 100G*8/30=26.67G, 25G*8/30=6.67G
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 30 MINUTE), 'dev-1', 'Tunnel100', now(), '', '', 42, '',
		 0, 0, 0, 0, 0, 0,
		 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3,
		 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-1', 'Tunnel100', now(), '', '', 42, '',
		 0, 0, 0, 0, 0, 0,
		 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7, 26666666666.7,
		 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3, 13333333333.3,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Tunnel100', now(), '', '', 42, '',
		 0, 0, 0, 0, 0, 0,
		 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7, 6666666666.7,
		 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3, 3333333333.3,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))

	// Tunnel interface for user 99 (validator): Tunnel200 (3 buckets)
	// Rates: 20G*8/30=5.33G, 40G*8/30=10.67G, 10G*8/30=2.67G
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 30 MINUTE), 'dev-1', 'Tunnel200', now(), '', '', 99, '',
		 0, 0, 0, 0, 0, 0,
		 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3,
		 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-1', 'Tunnel200', now(), '', '', 99, '',
		 0, 0, 0, 0, 0, 0,
		 10666666666.7, 10666666666.7, 10666666666.7, 10666666666.7, 10666666666.7, 10666666666.7, 10666666666.7,
		 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3, 5333333333.3,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Tunnel200', now(), '', '', 99, '',
		 0, 0, 0, 0, 0, 0,
		 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7,
		 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))

	// Other interface: Loopback0, no link_pk, no user_tunnel_id (3 buckets)
	// Rates: 10G*8/30=2.67G, 30G*8/30=8G, 5G*8/30=1.33G
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 30 MINUTE), 'dev-1', 'Loopback0', now(), '', '', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7, 2666666666.7,
		 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-1', 'Loopback0', now(), '', '', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 8000000000, 8000000000, 8000000000, 8000000000, 8000000000, 8000000000, 8000000000,
		 4000000000, 4000000000, 4000000000, 4000000000, 4000000000, 4000000000, 4000000000,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Loopback0', now(), '', '', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3, 1333333333.3,
		 666666666.7, 666666666.7, 666666666.7, 666666666.7, 666666666.7, 666666666.7, 666666666.7,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))

	// Add more "normal" low-traffic buckets so spike detection works.
	// With 10 total buckets, the P50 stays near the low value and the
	// high bucket from above becomes a spike (>2x P50).
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_discards, out_discards, in_errors, out_errors, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		SELECT
			toStartOfFiveMinutes(now() - INTERVAL (40 + number * 5) MINUTE),
			device_pk, intf, now(), link_pk, link_side, user_tunnel_id, '',
			0, 0, 0, 0, 0, 0,
			low_bps, low_bps, low_bps, low_bps, low_bps, low_bps, low_bps,
			low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2, low_bps / 2,
			0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0
		FROM (
			SELECT 'dev-1' AS device_pk, 'Ethernet1' AS intf, 'link-1' AS link_pk, '' AS link_side, CAST(NULL AS Nullable(UInt64)) AS user_tunnel_id, 13333333333.3 AS low_bps
			UNION ALL
			SELECT 'dev-1', 'Tunnel100', '', '', CAST(42 AS Nullable(UInt64)), 6666666666.7
			UNION ALL
			SELECT 'dev-1', 'Tunnel200', '', '', CAST(99 AS Nullable(UInt64)), 2666666666.7
			UNION ALL
			SELECT 'dev-1', 'Loopback0', '', '', CAST(NULL AS Nullable(UInt64)), 1333333333.3
		) AS intfs
		CROSS JOIN numbers(7) AS n`))

	// Also seed raw fact table for sub-5m bucket queries
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters
		(event_ts, ingested_at, device_pk, intf, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_discards_delta, out_discards_delta, user_tunnel_id)
		VALUES
		(now() - INTERVAL 30 MINUTE, now(), 'dev-1', 'Ethernet1', 'link-1', 100000000000, 50000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 20 MINUTE, now(), 'dev-1', 'Ethernet1', 'link-1', 200000000000, 100000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 10 MINUTE, now(), 'dev-1', 'Ethernet1', 'link-1', 50000000000, 25000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 30 MINUTE, now(), 'dev-1', 'Tunnel100', '', 50000000000, 25000000000, 30.0, 0, 0, 42),
		(now() - INTERVAL 20 MINUTE, now(), 'dev-1', 'Tunnel100', '', 100000000000, 50000000000, 30.0, 0, 0, 42),
		(now() - INTERVAL 10 MINUTE, now(), 'dev-1', 'Tunnel100', '', 25000000000, 12500000000, 30.0, 0, 0, 42),
		(now() - INTERVAL 30 MINUTE, now(), 'dev-1', 'Tunnel200', '', 20000000000, 10000000000, 30.0, 0, 0, 99),
		(now() - INTERVAL 20 MINUTE, now(), 'dev-1', 'Tunnel200', '', 40000000000, 20000000000, 30.0, 0, 0, 99),
		(now() - INTERVAL 10 MINUTE, now(), 'dev-1', 'Tunnel200', '', 10000000000, 5000000000, 30.0, 0, 0, 99),
		(now() - INTERVAL 30 MINUTE, now(), 'dev-1', 'Loopback0', '', 10000000000, 5000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 20 MINUTE, now(), 'dev-1', 'Loopback0', '', 30000000000, 15000000000, 30.0, 0, 0, NULL),
		(now() - INTERVAL 10 MINUTE, now(), 'dev-1', 'Loopback0', '', 5000000000, 2500000000, 30.0, 0, 0, NULL)`))
}

func TestTrafficDashboardTop_WithTrafficType(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedTrafficTypeData(t)

	tests := []struct {
		name      string
		query     string
		wantIntfs []string
	}{
		{
			name:      "all_traffic",
			query:     "?time_range=1h&entity=interface",
			wantIntfs: []string{"Ethernet1", "Loopback0", "Tunnel100", "Tunnel200"},
		},
		{
			name:      "all_traffic_explicit",
			query:     "?time_range=1h&entity=interface&intf_type=all",
			wantIntfs: []string{"Ethernet1", "Loopback0", "Tunnel100", "Tunnel200"},
		},
		{
			name:      "link_only",
			query:     "?time_range=1h&entity=interface&intf_type=link",
			wantIntfs: []string{"Ethernet1"},
		},
		{
			name:      "tunnel_only",
			query:     "?time_range=1h&entity=interface&intf_type=tunnel",
			wantIntfs: []string{"Tunnel100", "Tunnel200"},
		},
		{
			name:      "other_only",
			query:     "?time_range=1h&entity=interface&intf_type=other",
			wantIntfs: []string{"Loopback0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedTrafficTypeData(t)

	tests := []struct {
		name      string
		query     string
		wantIntfs []string
	}{
		{
			name:      "all_traffic",
			query:     "?time_range=1h",
			wantIntfs: []string{"Ethernet1", "Tunnel100", "Tunnel200", "Loopback0"},
		},
		{
			name:      "all_traffic_explicit",
			query:     "?time_range=1h&intf_type=all",
			wantIntfs: []string{"Ethernet1", "Tunnel100", "Tunnel200", "Loopback0"},
		},
		{
			name:      "tunnel_only",
			query:     "?time_range=1h&intf_type=tunnel",
			wantIntfs: []string{"Tunnel100", "Tunnel200"},
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
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardBurstiness(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.BurstinessResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

			var gotIntfs []string
			for _, e := range resp.Entities {
				gotIntfs = append(gotIntfs, e.Intf)
				assert.Greater(t, e.P50Bps, float64(0), "p50_bps should be > 0 for %s", e.Intf)
				assert.Greater(t, e.SpikeCount, uint64(0), "spike_count should be > 0 for %s", e.Intf)
			}
			assert.ElementsMatch(t, tt.wantIntfs, gotIntfs)
		})
	}
}

// --- User kind filter tests ---
// These tests verify that user_kind filtering works across all dashboard endpoints.
// The dz_users_current table has a device_pk column which previously caused
// ClickHouse column resolution errors when joined (ambiguity between f.device_pk
// and u.device_pk in CTEs).

func TestTrafficDashboardStress_WithUserKind(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedTrafficTypeData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"user_kind_filter", "?time_range=1h&metric=throughput&intf_type=tunnel&user_kind=ibrl"},
		{"user_kind_filter_multi", "?time_range=1h&metric=throughput&intf_type=tunnel&user_kind=ibrl,validator"},
		{"user_kind_group_by", "?time_range=1h&metric=throughput&intf_type=tunnel&group_by=user_kind"},
		{"user_kind_group_by_with_filter", "?time_range=1h&metric=throughput&intf_type=tunnel&group_by=user_kind&user_kind=ibrl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/stress"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardStress(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.StressResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		})
	}
}

func TestTrafficDashboardTop_WithUserKind(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedTrafficTypeData(t)

	tests := []struct {
		name      string
		query     string
		wantIntfs []string
	}{
		{
			name:      "filter_ibrl",
			query:     "?time_range=1h&entity=interface&intf_type=tunnel&user_kind=ibrl",
			wantIntfs: []string{"Tunnel100"},
		},
		{
			name:      "filter_validator",
			query:     "?time_range=1h&entity=interface&intf_type=tunnel&user_kind=validator",
			wantIntfs: []string{"Tunnel200"},
		},
		{
			name:      "filter_both",
			query:     "?time_range=1h&entity=interface&intf_type=tunnel&user_kind=ibrl,validator",
			wantIntfs: []string{"Tunnel100", "Tunnel200"},
		},
		{
			name:      "device_level_filter",
			query:     "?time_range=1h&entity=device&intf_type=tunnel&user_kind=ibrl",
			wantIntfs: nil, // device-level has empty intf
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/top"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardTop(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TopResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

			if tt.wantIntfs != nil {
				var gotIntfs []string
				for _, e := range resp.Entities {
					gotIntfs = append(gotIntfs, e.Intf)
				}
				assert.ElementsMatch(t, tt.wantIntfs, gotIntfs)
			} else {
				assert.NotEmpty(t, resp.Entities)
			}
		})
	}
}

func TestTrafficDashboardBurstiness_WithUserKind(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedTrafficTypeData(t)

	tests := []struct {
		name      string
		query     string
		wantIntfs []string
	}{
		{
			name:      "filter_ibrl",
			query:     "?time_range=1h&intf_type=tunnel&user_kind=ibrl",
			wantIntfs: []string{"Tunnel100"},
		},
		{
			name:      "filter_validator",
			query:     "?time_range=1h&intf_type=tunnel&user_kind=validator",
			wantIntfs: []string{"Tunnel200"},
		},
		{
			name:      "filter_both",
			query:     "?time_range=1h&intf_type=tunnel&user_kind=ibrl,validator",
			wantIntfs: []string{"Tunnel100", "Tunnel200"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardBurstiness(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.BurstinessResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

			var gotIntfs []string
			for _, e := range resp.Entities {
				gotIntfs = append(gotIntfs, e.Intf)
			}
			assert.ElementsMatch(t, tt.wantIntfs, gotIntfs)
		})
	}
}

func TestTrafficDashboardHealth_WithUserKind(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedTrafficTypeData(t)

	// Health endpoint should succeed with user_kind filter (even if no events match,
	// the query must not error out due to column resolution)
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h&intf_type=tunnel&user_kind=ibrl", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardHealth(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.HealthResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
}

// --- Drilldown endpoint tests ---

func TestTrafficDashboardDrilldown(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"default", "?time_range=1h"},
		{"sort_spike_count", "?time_range=1h&sort=spike_count"},
		{"sort_max_spike_ratio", "?time_range=1h&sort=max_spike_ratio"},
		{"sort_p50_bps", "?time_range=1h&sort=p50_bps"},
		{"sort_max_spike_bps", "?time_range=1h&sort=max_spike_bps"},
		{"dir_asc", "?time_range=1h&sort=spike_count&dir=asc"},
		{"dir_desc", "?time_range=1h&sort=spike_count&dir=desc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/burstiness"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardBurstiness(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.BurstinessResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			for _, e := range resp.Entities {
				assert.NotEmpty(t, e.ContributorCode, "contributor_code should be populated for %s %s", e.DeviceCode, e.Intf)
				assert.Greater(t, e.SpikeCount, uint64(0), "spike_count should be > 0")
			}
		})
	}
}

// --- Scoped field values tests ---

func TestFieldValues_ScopedByDashboardFilters(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
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
			apitesting.BindTest(t)
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

func TestFieldValues_WithTimeRange(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	tests := []struct {
		name      string
		query     string
		wantVals  []string
		wantEmpty bool
	}{
		{
			name:     "intf_default_1day",
			query:    "?entity=interfaces&field=intf",
			wantVals: []string{"Ethernet1/1", "Port-Channel1000"},
		},
		{
			name:     "intf_with_time_range_1h",
			query:    "?entity=interfaces&field=intf&time_range=1h",
			wantVals: []string{"Ethernet1/1", "Port-Channel1000"},
		},
		{
			name:     "intf_with_time_range_7d",
			query:    "?entity=interfaces&field=intf&time_range=7d",
			wantVals: []string{"Ethernet1/1", "Port-Channel1000"},
		},
		{
			name:     "intf_scoped_with_time_range",
			query:    "?entity=interfaces&field=intf&metro=FRA&time_range=7d",
			wantVals: []string{"Port-Channel1000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

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
// Uses _history tables (SCD2 pattern) since the schema comes from migrations.
func seedHealthData(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_metros_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
		('metro-1', now(), now(), generateUUIDv4(), 0, 1, 'metro-1', 'FRA', 'Frankfurt'),
		('metro-2', now(), now(), generateUUIDv4(), 0, 2, 'metro-2', 'AMS', 'Amsterdam')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_contributors_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, pk, code, name)
		VALUES
		('contrib-1', now(), now(), generateUUIDv4(), 0, 1, 'contrib-1', 'ACME', 'Acme Corp')`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_devices_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, device_type, code, public_ip, contributor_pk, metro_pk, max_users)
		VALUES
		('dev-1', now(), now(), generateUUIDv4(), 0, 1, 'dev-1', 'active', 'router', 'ROUTER-FRA-1', '', 'contrib-1', 'metro-1', 0),
		('dev-2', now(), now(), generateUUIDv4(), 0, 2, 'dev-2', 'active', 'router', 'ROUTER-AMS-1', '', 'contrib-1', 'metro-2', 0)`))

	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO dim_dz_links_history
		(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
		 pk, status, code, tunnel_net, contributor_pk, side_a_pk, side_z_pk,
		 side_a_iface_name, side_z_iface_name, link_type, committed_rtt_ns,
		 committed_jitter_ns, bandwidth_bps, isis_delay_override_ns)
		VALUES
		('link-1', now(), now(), generateUUIDv4(), 0, 1, 'link-1', 'active', '', '', 'contrib-1', '', '', '', '', 'WAN', 0, 0, 100000000000, 0)`))

	// dev-1 Ethernet1: has errors and discards (link interface) — 2 rollup buckets
	// Bucket 1: in_errors=10, out_errors=5, in_discards=3, out_discards=2, in_fcs_errors=1
	// Bucket 2: in_errors=20, out_errors=10, carrier_transitions=2
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_errors, out_errors, in_discards, out_discards, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-1', 'Ethernet1', now(), 'link-1', 'A', NULL, '',
		 10, 5, 3, 2, 1, 0,
		 26666666, 26666666, 26666666, 26666666, 26666666, 26666666, 26666666,
		 13333333, 13333333, 13333333, 13333333, 13333333, 13333333, 13333333,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Ethernet1', now(), 'link-1', 'A', NULL, '',
		 20, 10, 0, 0, 0, 2,
		 26666666, 26666666, 26666666, 26666666, 26666666, 26666666, 26666666,
		 13333333, 13333333, 13333333, 13333333, 13333333, 13333333, 13333333,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))

	// dev-2 Ethernet2: has carrier transitions only (no link)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_errors, out_errors, in_discards, out_discards, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 20 MINUTE), 'dev-2', 'Ethernet2', now(), '', '', NULL, '',
		 0, 0, 0, 0, 0, 5,
		 26666666, 26666666, 26666666, 26666666, 26666666, 26666666, 26666666,
		 13333333, 13333333, 13333333, 13333333, 13333333, 13333333, 13333333,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))

	// dev-1 Loopback0: zero errors (should not appear in results)
	require.NoError(t, config.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
		(bucket_ts, device_pk, intf, ingested_at, link_pk, link_side, user_tunnel_id, user_pk,
		 in_errors, out_errors, in_discards, out_discards, in_fcs_errors, carrier_transitions,
		 avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		 avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		 avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		 avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps)
		VALUES
		(toStartOfFiveMinutes(now() - INTERVAL 10 MINUTE), 'dev-1', 'Loopback0', now(), '', '', NULL, '',
		 0, 0, 0, 0, 0, 0,
		 26666666, 26666666, 26666666, 26666666, 26666666, 26666666, 26666666,
		 13333333, 13333333, 13333333, 13333333, 13333333, 13333333, 13333333,
		 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)`))
}

func TestTrafficDashboardHealth(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedHealthData(t)

	t.Run("default", func(t *testing.T) {
		apitesting.BindTest(t)
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
		apitesting.BindTest(t)
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
		apitesting.BindTest(t)
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
		apitesting.BindTest(t)
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
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/health?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficDashboardHealth(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.HealthResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Empty(t, resp.Entities)
}

// --- Traffic data endpoint tests (raw + rollup paths) ---

func TestGetTrafficData_RawPath(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	// time_range=1h → 10s bucket → raw fact table
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/data?time_range=1h&agg=max", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficData(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.TrafficDataResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp.Points, "should return data from raw fact table")
	assert.NotEmpty(t, resp.Series, "should return series info")
	assert.Equal(t, "10 SECOND", resp.EffBucket)
}

func TestGetTrafficData_RollupPath(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	// time_range=12h → 10m bucket → rollup table
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/data?time_range=12h&agg=max", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficData(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.TrafficDataResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp.Points, "should return data from rollup table")
	assert.NotEmpty(t, resp.Series, "should return series info")
	assert.Equal(t, "10 MINUTE", resp.EffBucket)
}

func TestGetTrafficData_ExplicitBucket_Raw(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	// Explicit 30s bucket → raw fact table
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/data?time_range=3h&bucket=30+SECOND", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficData(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.TrafficDataResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "30 SECOND", resp.EffBucket)
}

func TestGetTrafficData_ExplicitBucket_Rollup(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	// Explicit 30m bucket → rollup table
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/data?time_range=3h&bucket=30+MINUTE", nil)
	rr := httptest.NewRecorder()

	handlers.GetTrafficData(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.TrafficDataResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "30 MINUTE", resp.EffBucket)
}

func TestGetTrafficData_Packets(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"raw", "?time_range=1h&metric=packets"},
		{"rollup", "?time_range=12h&metric=packets"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/data"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficData(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TrafficDataResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		})
	}
}

func TestGetTrafficData_AvgAgg(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"raw", "?time_range=1h&agg=avg"},
		{"rollup", "?time_range=12h&agg=avg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/data"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficData(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.TrafficDataResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		})
	}
}

// --- Discards endpoint tests (raw + rollup paths) ---

func TestGetDiscardsData_RawPath(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	// time_range=1h → raw fact table
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/discards?time_range=1h", nil)
	rr := httptest.NewRecorder()

	handlers.GetDiscardsData(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.DiscardsDataResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
}

func TestGetDiscardsData_RollupPath(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	// time_range=12h → rollup table
	req := httptest.NewRequest(http.MethodGet, "/api/traffic/discards?time_range=12h", nil)
	rr := httptest.NewRecorder()

	handlers.GetDiscardsData(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.DiscardsDataResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
}

// --- Stress endpoint raw vs rollup path tests ---

func TestTrafficDashboardStress_RawVsRollup(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"raw_throughput", "?time_range=1h&metric=throughput"},
		{"rollup_throughput", "?time_range=12h&metric=throughput"},
		{"raw_utilization", "?time_range=1h&metric=utilization"},
		{"rollup_utilization", "?time_range=12h&metric=utilization"},
		{"raw_packets", "?time_range=1h&metric=packets"},
		{"rollup_packets", "?time_range=12h&metric=packets"},
		{"raw_grouped", "?time_range=1h&metric=throughput&group_by=device"},
		{"rollup_grouped", "?time_range=12h&metric=throughput&group_by=device"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/stress"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardStress(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.StressResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			assert.NotEmpty(t, resp.EffBucket)
		})
	}
}

// --- Drilldown endpoint raw vs rollup path tests ---

func TestTrafficDashboardDrilldown_RawVsRollup(t *testing.T) {
	t.Parallel()
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	seedDashboardData(t)

	tests := []struct {
		name  string
		query string
	}{
		{"raw", "?time_range=1h&device_pk=dev-1"},
		{"rollup", "?time_range=12h&device_pk=dev-1"},
		{"raw_with_intf", "?time_range=1h&device_pk=dev-1&intf=Port-Channel1000"},
		{"rollup_with_intf", "?time_range=12h&device_pk=dev-1&intf=Port-Channel1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apitesting.BindTest(t)
			req := httptest.NewRequest(http.MethodGet, "/api/traffic/dashboard/drilldown"+tt.query, nil)
			rr := httptest.NewRecorder()

			handlers.GetTrafficDashboardDrilldown(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

			var resp handlers.DrilldownResponse
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
			assert.NotEmpty(t, resp.Points, "should return data points")
			assert.NotEmpty(t, resp.EffBucket)
		})
	}
}
