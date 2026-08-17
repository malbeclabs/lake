package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/malbeclabs/lake/indexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRollupTables(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String, code String, status String, link_type String,
			bandwidth_bps Nullable(Int64), committed_rtt_ns Int64 DEFAULT 0,
			tunnel_net String DEFAULT '', side_a_pk Nullable(String),
			side_z_pk Nullable(String), contributor_pk Nullable(String),
			side_a_iface_name Nullable(String), side_a_ip Nullable(String),
			side_z_iface_name Nullable(String), side_z_ip Nullable(String)
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String, code String, device_type String,
			metro_pk Nullable(String), contributor_pk Nullable(String),
			status String DEFAULT 'activated', public_ip String DEFAULT ''
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String, code String, name Nullable(String),
			latitude Nullable(Float64), longitude Nullable(Float64)
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_contributors_current (
			pk String, code String, name Nullable(String)
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS dz_link_status_changes (
			link_pk String, link_code String, previous_status String,
			new_status String, changed_ts DateTime,
			side_a_metro String, side_z_metro String
		) ENGINE = Memory`,
		`CREATE TABLE IF NOT EXISTS link_rollup_5m (
			bucket_ts DateTime, link_pk String, ingested_at DateTime64(3),
			a_avg_rtt_us Float64, a_min_rtt_us Float64, a_p50_rtt_us Float64,
			a_p90_rtt_us Float64, a_p95_rtt_us Float64, a_p99_rtt_us Float64,
			a_max_rtt_us Float64, a_loss_pct Float64, a_samples UInt32,
			z_avg_rtt_us Float64, z_min_rtt_us Float64, z_p50_rtt_us Float64,
			z_p90_rtt_us Float64, z_p95_rtt_us Float64, z_p99_rtt_us Float64,
			z_max_rtt_us Float64, z_loss_pct Float64, z_samples UInt32,
			status String DEFAULT '', provisioning Bool DEFAULT false,
			isis_down Bool DEFAULT false
		) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (bucket_ts, link_pk)`,
		`CREATE TABLE IF NOT EXISTS device_interface_rollup_5m (
			bucket_ts DateTime, device_pk String, intf String,
			link_pk String DEFAULT '', link_side String DEFAULT '',
			ingested_at DateTime64(3),
			in_errors UInt64, out_errors UInt64, in_fcs_errors UInt64,
			in_discards UInt64, out_discards UInt64, carrier_transitions UInt64,
			avg_in_bps Float64, min_in_bps Float64, p50_in_bps Float64,
			p90_in_bps Float64, p95_in_bps Float64, p99_in_bps Float64, max_in_bps Float64,
			avg_out_bps Float64, min_out_bps Float64, p50_out_bps Float64,
			p90_out_bps Float64, p95_out_bps Float64, p99_out_bps Float64, max_out_bps Float64,
			avg_in_pps Float64, min_in_pps Float64, p50_in_pps Float64,
			p90_in_pps Float64, p95_in_pps Float64, p99_in_pps Float64, max_in_pps Float64,
			avg_out_pps Float64, min_out_pps Float64, p50_out_pps Float64,
			p90_out_pps Float64, p95_out_pps Float64, p99_out_pps Float64, max_out_pps Float64,
			status String DEFAULT '', isis_overload Bool DEFAULT false,
			isis_unreachable Bool DEFAULT false
		) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (bucket_ts, device_pk, intf)`,
	} {
		require.NoError(t, api.DB.Exec(ctx, ddl))
	}
}

// setupIncidentViews applies the incident view definitions from the real
// migrations. Reading them here rather than copying the DDL keeps the fixture
// from drifting away from what production runs.
func setupIncidentViews(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	entries, err := indexer.ClickHouseMigrationsFS.ReadDir("db/clickhouse/migrations")
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		if strings.Contains(e.Name(), "incident_views") {
			names = append(names, e.Name())
		}
	}
	require.NotEmpty(t, names, "no incident view migrations found")
	sort.Strings(names)

	for _, name := range names {
		body, err := indexer.ClickHouseMigrationsFS.ReadFile("db/clickhouse/migrations/" + name)
		require.NoError(t, err)
		for _, stmt := range incidentViewStatements(string(body)) {
			require.NoError(t, api.DB.Exec(ctx, stmt), "migration %s", name)
		}
	}
}

// incidentViewStatements returns the CREATE statements in a migration's Up section.
func incidentViewStatements(migration string) []string {
	up := migration
	if i := strings.Index(up, "-- +goose Down"); i >= 0 {
		up = up[:i]
	}
	up = strings.TrimPrefix(strings.TrimSpace(up), "-- +goose Up")

	var out []string
	for _, stmt := range strings.Split(up, ";") {
		if strings.Contains(stmt, "CREATE OR REPLACE VIEW") {
			out = append(out, strings.TrimSpace(stmt))
		}
	}
	return out
}

func insertBaseMetadata(t *testing.T, api *handlers.API) {
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_metros_current (pk, code, name) VALUES ('metro-nyc', 'NYC', 'New York'), ('metro-lax', 'LAX', 'Los Angeles')`))
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, contributor_pk, status) VALUES ('dev-nyc-1', 'NYC-CORE-01', 'router', 'metro-nyc', 'contrib-1', 'activated'), ('dev-lax-1', 'LAX-CORE-01', 'router', 'metro-lax', 'contrib-1', 'activated')`))
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_contributors_current (pk, code, name) VALUES ('contrib-1', 'CONTRIB1', 'Contributor One')`))
}

func TestGetLinkIncidents_EmptyState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=1h", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	assert.Empty(t, resp.Active)
	assert.Empty(t, resp.Drained)
	assert.Equal(t, 0, resp.ActiveSummary.Total)
}

func TestGetLinkIncidents_PacketLoss(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	// Insert 8 consecutive 5-min buckets with high packet loss (40 min total, meets 30m default)
	now := time.Now().UTC()
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples) VALUES ($1, 'link-1', now(), 25.0, 15.0, 100, 100)`, ts))
	}
	// Healthy buckets before and after
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples) VALUES ($1, 'link-1', now(), 0, 0, 100, 100)`, baseTime.Add(-5*time.Minute)))
	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples) VALUES ($1, 'link-1', now(), 0, 0, 100, 100)`, baseTime.Add(40*time.Minute)))

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=packet_loss&threshold=10&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	require.NotEmpty(t, resp.Active, "should detect packet loss incident")
	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "packet_loss" && inc.LinkCode == "NYC-LAX-001" {
			found = true
			assert.Equal(t, "NYC", inc.SideAMetro)
			assert.Equal(t, "LAX", inc.SideZMetro)
			assert.NotNil(t, inc.PeakLossPct)
			assert.True(t, *inc.PeakLossPct >= 10.0)
			assert.False(t, inc.IsDrained)
		}
	}
	assert.True(t, found, "should find packet_loss incident for NYC-LAX-001")
}

func TestGetLinkIncidents_Errors(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	// Insert 8 consecutive 5-min error buckets via device_interface_rollup_5m
	now := time.Now().UTC()
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m (bucket_ts, device_pk, intf, link_pk, ingested_at, in_errors, out_errors) VALUES ($1, 'dev-nyc-1', 'eth0', 'link-1', now(), 15, 5)`, ts))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=errors&errors_threshold=10&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	require.NotEmpty(t, resp.Active, "should detect error incident")
	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "errors" && inc.LinkCode == "NYC-LAX-001" {
			found = true
			assert.NotNil(t, inc.PeakCount)
			assert.True(t, *inc.PeakCount >= 10)
			assert.Equal(t, "CONTRIB1", inc.ContributorCode)
		}
	}
	assert.True(t, found, "should find errors incident for NYC-LAX-001")
	assert.True(t, resp.ActiveSummary.ByType["errors"] > 0)
}

func TestGetLinkIncidents_ISISDown(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	// Insert 8 consecutive isis_down=true buckets
	now := time.Now().UTC()
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples, isis_down) VALUES ($1, 'link-1', now(), 0, 0, 100, 100, true)`, ts))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=isis_down&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "isis_down" {
			found = true
		}
	}
	assert.True(t, found, "should detect isis_down incident")
}

func TestGetLinkIncidents_TypeFilter(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	// Insert error data
	now := time.Now().UTC()
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m (bucket_ts, device_pk, intf, link_pk, ingested_at, in_errors, out_errors) VALUES ($1, 'dev-nyc-1', 'eth0', 'link-1', now(), 20, 0)`, ts))
	}

	// When filtering for packet_loss only, should NOT return errors
	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=packet_loss&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	for _, inc := range resp.Active {
		assert.NotEqual(t, "errors", inc.IncidentType, "type=packet_loss should not return errors")
	}
}

func TestGetLinkIncidents_DrainedLinksView(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1'), ('link-2', 'NYC-LAX-002', 'soft-drained', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_link_status_changes (link_pk, link_code, previous_status, new_status, changed_ts, side_a_metro, side_z_metro) VALUES ('link-2', 'NYC-LAX-002', 'activated', 'soft-drained', $1, 'NYC', 'LAX')`, now.Add(-6*time.Hour)))

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=24h", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	assert.Equal(t, 1, resp.DrainedSummary.Total, "should have 1 drained link")
	found := false
	for _, dl := range resp.Drained {
		if dl.LinkCode == "NYC-LAX-002" {
			found = true
			assert.Equal(t, "soft-drained", dl.DrainStatus)
			assert.Equal(t, "gray", dl.Readiness)
		}
	}
	assert.True(t, found, "should find drained link NYC-LAX-002")
}

func TestGetLinkIncidents_OngoingStartedBeforeWindow(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	// Insert packet loss starting 3 days ago and continuing until now (ongoing)
	now := time.Now().UTC()
	start := now.Add(-72 * time.Hour)
	for ts := start; ts.Before(now); ts = ts.Add(5 * time.Minute) {
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO link_rollup_5m (bucket_ts, link_pk, ingested_at, a_loss_pct, z_loss_pct, a_samples, z_samples) VALUES ($1, 'link-1', now(), 50.0, 0, 100, 100)`, ts))
	}

	// Query with 24h window — incident started 3 days ago but is ongoing, should still show.
	// The lookback is duration+24h=48h, so started_at will be ~48h ago (not the true 72h start).
	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=24h&type=packet_loss&threshold=10&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "packet_loss" && inc.LinkCode == "NYC-LAX-001" {
			found = true
			assert.True(t, inc.IsOngoing, "incident should be ongoing")
			// started_at should be before the 24h display window (lookback captures it)
			startedAt, _ := time.Parse(time.RFC3339, inc.StartedAt)
			assert.True(t, time.Since(startedAt) > 24*time.Hour, "started_at should be before the 24h window")
		}
	}
	assert.True(t, found, "ongoing incident that started before the window should still be visible")
}

func TestGetLinkIncidentsCSV(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES ('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')`))

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links/csv?range=1h", nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidentsCSV(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "text/csv")
	assert.Contains(t, rr.Header().Get("Content-Disposition"), "attachment")

	body := rr.Body.String()
	assert.True(t, strings.HasPrefix(body, "id,link_code,"), "CSV should start with header row")
}

func TestGetDeviceIncidents_EmptyState(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/devices?range=1h", nil)
	rr := httptest.NewRecorder()
	api.GetDeviceIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.DeviceIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	assert.Empty(t, resp.Active)
	assert.Empty(t, resp.Drained)
}

func TestGetDeviceIncidents_Errors(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	// Insert device-only interface counters (link_pk='')
	now := time.Now().UTC()
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m (bucket_ts, device_pk, intf, link_pk, ingested_at, in_errors, out_errors) VALUES ($1, 'dev-nyc-1', 'Loopback0', '', now(), 25, 0)`, ts))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/devices?range=6h&type=errors&errors_threshold=10&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetDeviceIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.DeviceIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	require.NotEmpty(t, resp.Active, "should detect device error incident")
	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "errors" && inc.DeviceCode == "NYC-CORE-01" {
			found = true
			assert.NotNil(t, inc.PeakCount)
			assert.True(t, *inc.PeakCount >= 10)
		}
	}
	assert.True(t, found, "should find errors incident for NYC-CORE-01")
}

func TestGetDeviceIncidents_ISISOverload(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIBare(t, testChDB)
	setupRollupTables(t, api)
	setupIncidentViews(t, api)
	insertBaseMetadata(t, api)
	ctx := t.Context()

	now := time.Now().UTC()
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		require.NoError(t, api.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m (bucket_ts, device_pk, intf, link_pk, ingested_at, isis_overload) VALUES ($1, 'dev-nyc-1', 'Loopback0', '', now(), true)`, ts))
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/devices?range=6h&type=isis_overload&min_duration=5", nil)
	rr := httptest.NewRecorder()
	api.GetDeviceIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.DeviceIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "isis_overload" {
			found = true
		}
	}
	assert.True(t, found, "should detect isis_overload incident")
}

// TestLinkIncidentsRollupVsRaw seeds raw latency data and rollup data,
// then verifies both source paths detect the same packet loss incident.
func TestLinkIncidentsRollupVsRaw(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	// Seed dimension data
	seedMetro(t, api, "metro-a", "NYC")
	seedMetro(t, api, "metro-z", "LAX")
	seedContributor(t, api, "contrib-1", "acme")
	seedDeviceMetadata(t, api, "dev-a", "DEV-A", "router", "contrib-1", "metro-a", 10, "activated")
	seedDeviceMetadata(t, api, "dev-z", "DEV-Z", "router", "contrib-1", "metro-z", 10, "activated")
	seedLinkMetadataAt(t, api, "link-1", "NYC-LAX-1", "WAN", "contrib-1", "dev-a", "dev-z", 10_000_000_000, 500_000, "activated",
		time.Now().Add(-24*time.Hour))

	// Seed 8 consecutive 5-minute buckets with 100% packet loss (40 minutes)
	now := time.Now().UTC().Truncate(5 * time.Minute)
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		// Raw latency: all probes are losses
		for j := range 10 {
			probeTS := ts.Add(time.Duration(j) * 20 * time.Second)
			// Direction A
			require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_dz_device_link_latency
				(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				probeTS, probeTS, int64(1), int32(i*20+j), "dev-a", "dev-z", "link-1", int64(0), true))
			// Direction Z
			require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_dz_device_link_latency
				(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				probeTS, probeTS, int64(1), int32(200+i*20+j), "dev-z", "dev-a", "link-1", int64(0), true))
		}
		// Corresponding rollup row
		seedLinkRollup(t, api, ts, "link-1", 0, 0, 100, 100, 10, 10, "activated", false, false)
	}
	// Healthy buckets before and after
	healthyBefore := baseTime.Add(-5 * time.Minute)
	healthyAfter := baseTime.Add(40 * time.Minute)
	seedLinkRollup(t, api, healthyBefore, "link-1", 100, 100, 0, 0, 10, 10, "activated", false, false)
	seedLinkRollup(t, api, healthyAfter, "link-1", 100, 100, 0, 0, 10, 10, "activated", false, false)
	for _, ts := range []time.Time{healthyBefore, healthyAfter} {
		for j := range 10 {
			probeTS := ts.Add(time.Duration(j) * 20 * time.Second)
			require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_dz_device_link_latency
				(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				probeTS, probeTS, int64(1), int32(500+j), "dev-a", "dev-z", "link-1", int64(100), false))
			require.NoError(t, api.DB.Exec(ctx, `INSERT INTO fact_dz_device_link_latency
				(event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				probeTS, probeTS, int64(1), int32(600+j), "dev-z", "dev-a", "link-1", int64(100), false))
		}
	}

	// Query rollup path
	reqRollup := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=packet_loss&threshold=10&min_duration=5", nil)
	rrRollup := httptest.NewRecorder()
	api.GetLinkIncidents(rrRollup, reqRollup)
	require.Equal(t, http.StatusOK, rrRollup.Code)

	var rollupResp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rrRollup.Body).Decode(&rollupResp))

	// Query raw path
	reqRaw := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=packet_loss&threshold=10&min_duration=5&source=raw", nil)
	rrRaw := httptest.NewRecorder()
	api.GetLinkIncidents(rrRaw, reqRaw)
	require.Equal(t, http.StatusOK, rrRaw.Code)

	var rawResp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rrRaw.Body).Decode(&rawResp))

	// Both should detect the same packet loss incident
	require.NotEmpty(t, rollupResp.Active, "rollup should detect packet loss")
	require.NotEmpty(t, rawResp.Active, "raw should detect packet loss")

	// Find the packet_loss incident in each
	var rollupInc, rawInc *handlers.LinkIncident
	for i := range rollupResp.Active {
		if rollupResp.Active[i].IncidentType == "packet_loss" {
			rollupInc = &rollupResp.Active[i]
			break
		}
	}
	for i := range rawResp.Active {
		if rawResp.Active[i].IncidentType == "packet_loss" {
			rawInc = &rawResp.Active[i]
			break
		}
	}
	require.NotNil(t, rollupInc, "rollup should have packet_loss incident")
	require.NotNil(t, rawInc, "raw should have packet_loss incident")

	// Incident timing should match
	assert.Equal(t, rollupInc.StartedAt, rawInc.StartedAt, "started_at should match")
	assert.Equal(t, rollupInc.IsOngoing, rawInc.IsOngoing, "is_ongoing should match")
	if rollupInc.EndedAt != nil && rawInc.EndedAt != nil {
		assert.Equal(t, *rollupInc.EndedAt, *rawInc.EndedAt, "ended_at should match")
	}
	assert.Equal(t, rollupInc.LinkCode, rawInc.LinkCode, "link_code should match")
}

// TestGetDeviceIncidents_NoDataGapLength checks that only a sustained gap in the
// interface rollup raises a no_data incident. The minimum runs per gap, so blips
// that the 180-minute coalesce would otherwise chain together stay suppressed.
func TestGetDeviceIncidents_NoDataGapLength(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	ctx := t.Context()

	seedMetro(t, api, "metro-a", "NYC")
	seedContributor(t, api, "contrib-1", "acme")

	// Bucket indexes each device is missing, over a continuous 3-hour run.
	devices := map[string][]int{
		"dev-gap4":  {10, 11, 12, 13}, // 20 minutes, one gap
		"dev-gap2":  {10, 11},         // 10 minutes, below the minimum
		"dev-blips": {5, 17, 29},      // three single blips an hour apart
	}
	for pk := range devices {
		seedDeviceMetadata(t, api, pk, strings.ToUpper(pk), "router", "contrib-1", "metro-a", 10, "activated")
	}

	const totalBuckets = 36
	baseTime := time.Now().UTC().Truncate(5 * time.Minute).Add(-3 * time.Hour)
	for pk, holes := range devices {
		missing := make(map[int]bool, len(holes))
		for _, h := range holes {
			missing[h] = true
		}
		for i := range totalBuckets {
			if missing[i] {
				continue
			}
			ts := baseTime.Add(time.Duration(i*5) * time.Minute)
			require.NoError(t, api.DB.Exec(ctx, `INSERT INTO device_interface_rollup_5m
				(bucket_ts, device_pk, intf, link_pk, ingested_at) VALUES ($1, $2, 'Ethernet1', '', now())`, ts, pk))
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/devices?range=6h&type=no_data", nil)
	rr := httptest.NewRecorder()
	api.GetDeviceIncidents(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.DeviceIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	byDevice := map[string][]handlers.DeviceIncident{}
	for _, inc := range resp.Active {
		if inc.IncidentType == "no_data" {
			byDevice[inc.DevicePK] = append(byDevice[inc.DevicePK], inc)
		}
	}

	require.Len(t, byDevice["dev-gap4"], 1, "a 20-minute gap raises one incident")
	assert.Empty(t, byDevice["dev-gap2"], "a 10-minute gap stays below the minimum")
	assert.Empty(t, byDevice["dev-blips"], "single-bucket blips never reach the minimum")

	gap := byDevice["dev-gap4"][0]
	assert.Equal(t, baseTime.Add(50*time.Minute).Format(time.RFC3339), gap.StartedAt)
	require.NotNil(t, gap.DurationSeconds)
	assert.Equal(t, int64(20*60), *gap.DurationSeconds, "duration matches the gap, not a coalesced span")
}

// getLinkIncidents issues a link incidents request and decodes the response.
func getLinkIncidents(t *testing.T, api *handlers.API, url string) handlers.LinkIncidentsResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	api.GetLinkIncidents(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.LinkIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return resp
}

// getDeviceIncidents issues a device incidents request and decodes the response.
func getDeviceIncidents(t *testing.T, api *handlers.API, url string) handlers.DeviceIncidentsResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	api.GetDeviceIncidents(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())

	var resp handlers.DeviceIncidentsResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return resp
}

func linkIncidentCodes(resp handlers.LinkIncidentsResponse, incidentType string) []string {
	var codes []string
	for _, inc := range resp.Active {
		if inc.IncidentType == incidentType {
			codes = append(codes, inc.LinkCode)
		}
	}
	return codes
}

func deviceIncidentCodes(resp handlers.DeviceIncidentsResponse, incidentType string) []string {
	var codes []string
	for _, inc := range resp.Active {
		if inc.IncidentType == incidentType {
			codes = append(codes, inc.DeviceCode)
		}
	}
	return codes
}

// seedTwoIncidentLinks seeds two links in disjoint metros with different contributors,
// both with a packet loss incident, so a filter that matches the wrong column or matches
// nothing at all is distinguishable from one that filters correctly.
func seedTwoIncidentLinks(t *testing.T, api *handlers.API) {
	t.Helper()

	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-lax", "LAX")
	seedMetro(t, api, "metro-fra", "FRA")
	seedMetro(t, api, "metro-sin", "SIN")
	seedContributor(t, api, "contrib-1", "CONTRIB1")
	seedContributor(t, api, "contrib-2", "CONTRIB2")
	seedDeviceMetadata(t, api, "dev-nyc", "NYC-CORE-01", "router", "contrib-1", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-lax", "LAX-CORE-01", "router", "contrib-1", "metro-lax", 10, "activated")
	seedDeviceMetadata(t, api, "dev-fra", "FRA-CORE-01", "router", "contrib-2", "metro-fra", 10, "activated")
	seedDeviceMetadata(t, api, "dev-sin", "SIN-CORE-01", "router", "contrib-2", "metro-sin", 10, "activated")
	seedLinkMetadata(t, api, "link-1", "NYC-LAX-001", "WAN", "contrib-1", "dev-nyc", "dev-lax", 10_000_000_000, 20_000_000, "activated")
	seedLinkMetadata(t, api, "link-2", "FRA-SIN-002", "WAN", "contrib-2", "dev-fra", "dev-sin", 10_000_000_000, 20_000_000, "activated")

	// 8 consecutive 5-min buckets of packet loss on both links, above the view's 10%.
	baseTime := time.Now().UTC().Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		seedLinkRollup(t, api, ts, "link-1", 100, 100, 25, 15, 10, 10, "activated", false, false)
		seedLinkRollup(t, api, ts, "link-2", 100, 100, 25, 15, 10, 10, "activated", false, false)
	}
}

// TestGetLinkIncidents_ViewPathFilters covers filtering on the default-threshold path,
// which reads link_incidents_v and has no JOINs. Referencing a JOIN alias there (cc.code)
// made every filtered incidents request fail with a ClickHouse identifier error. The API
// is built with the real migrations, so the column names are checked against the shipped
// view definition rather than a copy.
func TestGetLinkIncidents_ViewPathFilters(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoIncidentLinks(t, api)

	// No threshold overrides, so these requests take the link_incidents_v path.
	tests := []struct {
		name   string
		filter string
	}{
		{"side a metro", "metro:NYC"},
		{"side z metro", "metro:LAX"},
		{"link", "link:NYC-LAX-001"},
		{"contributor", "contributor:CONTRIB1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := getLinkIncidents(t, api, "/api/incidents/links?range=6h&type=packet_loss&min_duration=5&filter="+tt.filter)
			codes := linkIncidentCodes(resp, "packet_loss")
			assert.Contains(t, codes, "NYC-LAX-001", "filter %q should match its own link", tt.filter)
			assert.NotContains(t, codes, "FRA-SIN-002", "filter %q should exclude the other link", tt.filter)
		})
	}

	t.Run("unfiltered returns both links", func(t *testing.T) {
		codes := linkIncidentCodes(getLinkIncidents(t, api, "/api/incidents/links?range=6h&type=packet_loss&min_duration=5"), "packet_loss")
		assert.Contains(t, codes, "NYC-LAX-001")
		assert.Contains(t, codes, "FRA-SIN-002")
	})
}

// TestGetLinkIncidents_DeviceFilterFallback covers the one link filter the view cannot
// express: link_incidents_v has no device codes, so device-filtered requests fall back to
// the JOIN-based query.
func TestGetLinkIncidents_DeviceFilterFallback(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)
	seedTwoIncidentLinks(t, api)

	for _, tt := range []struct {
		name   string
		filter string
	}{
		{"side a device", "device:NYC-CORE-01"},
		{"side z device", "device:LAX-CORE-01"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			codes := linkIncidentCodes(getLinkIncidents(t, api, "/api/incidents/links?range=6h&type=packet_loss&min_duration=5&filter="+tt.filter), "packet_loss")
			assert.Contains(t, codes, "NYC-LAX-001", "filter %q should match its own link", tt.filter)
			assert.NotContains(t, codes, "FRA-SIN-002", "filter %q should exclude the other link", tt.filter)
		})
	}
}

// TestGetDeviceIncidents_ViewPathFilters is the device_incidents_v counterpart, which had
// the same alias-into-a-JOIN-less-query defect.
func TestGetDeviceIncidents_ViewPathFilters(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPI(t, testChDB)

	seedMetro(t, api, "metro-nyc", "NYC")
	seedMetro(t, api, "metro-fra", "FRA")
	seedContributor(t, api, "contrib-1", "CONTRIB1")
	seedContributor(t, api, "contrib-2", "CONTRIB2")
	seedDeviceMetadata(t, api, "dev-nyc", "NYC-CORE-01", "router", "contrib-1", "metro-nyc", 10, "activated")
	seedDeviceMetadata(t, api, "dev-fra", "FRA-CORE-01", "router", "contrib-2", "metro-fra", 10, "activated")

	// Device-only interface counters (link_pk = ''), errors on both devices.
	baseTime := time.Now().UTC().Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		seedInterfaceRollup(t, api, ts, "dev-nyc", "Loopback0", "", "", 25, 0, 1000, "up")
		seedInterfaceRollup(t, api, ts, "dev-fra", "Loopback0", "", "", 25, 0, 1000, "up")
	}

	tests := []struct {
		name   string
		filter string
	}{
		{"metro", "metro:NYC"},
		{"device", "device:NYC-CORE-01"},
		{"contributor", "contributor:CONTRIB1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := getDeviceIncidents(t, api, "/api/incidents/devices?range=6h&type=errors&min_duration=5&filter="+tt.filter)
			codes := deviceIncidentCodes(resp, "errors")
			assert.Contains(t, codes, "NYC-CORE-01", "filter %q should match its own device", tt.filter)
			assert.NotContains(t, codes, "FRA-CORE-01", "filter %q should exclude the other device", tt.filter)
		})
	}

	t.Run("unfiltered returns both devices", func(t *testing.T) {
		codes := deviceIncidentCodes(getDeviceIncidents(t, api, "/api/incidents/devices?range=6h&type=errors&min_duration=5"), "errors")
		assert.Contains(t, codes, "NYC-CORE-01")
		assert.Contains(t, codes, "FRA-CORE-01")
	})
}
