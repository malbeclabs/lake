package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupIncidentsTables(t *testing.T) {
	ctx := t.Context()

	// Links
	err := config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String,
			code String,
			status String,
			link_type String,
			bandwidth_bps Nullable(Int64),
			committed_rtt_ns Int64 DEFAULT 0,
			tunnel_net String DEFAULT '',
			side_a_pk Nullable(String),
			side_z_pk Nullable(String),
			contributor_pk Nullable(String),
			side_a_iface_name Nullable(String),
			side_a_ip Nullable(String),
			side_z_iface_name Nullable(String),
			side_z_ip Nullable(String)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Devices
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String,
			code String,
			device_type String,
			metro_pk Nullable(String),
			public_ip String
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Metros
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String,
			code String,
			name Nullable(String),
			latitude Nullable(Float64),
			longitude Nullable(Float64)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Contributors
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_contributors_current (
			pk String,
			code String,
			name Nullable(String)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Interface counters (with Nullable delta columns as used in production)
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fact_dz_device_interface_counters (
			event_ts DateTime,
			device_pk String,
			link_pk String,
			in_octets_delta UInt64,
			out_octets_delta UInt64,
			delta_duration Float64,
			user_tunnel_id Nullable(String),
			in_errors_delta Nullable(Int64),
			out_errors_delta Nullable(Int64),
			in_discards_delta Nullable(Int64),
			out_discards_delta Nullable(Int64),
			in_fcs_errors_delta Nullable(Int64),
			carrier_transitions_delta Nullable(Int64)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Latency
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fact_dz_device_link_latency (
			event_ts DateTime,
			ingested_at DateTime DEFAULT now(),
			link_pk String,
			origin_device_pk String DEFAULT '',
			target_device_pk String DEFAULT '',
			epoch Int64 DEFAULT 0,
			sample_index Int32 DEFAULT 0,
			rtt_us Float64,
			ipdv_us Float64,
			loss UInt8,
			direction Nullable(String)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Latency sample headers (for display timestamp correction)
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fact_dz_device_link_latency_sample_header (
			written_at DateTime64(3, 'UTC'),
			origin_device_pk String,
			target_device_pk String,
			link_pk String,
			epoch Int64,
			start_timestamp_us Int64,
			sampling_interval_us UInt64,
			latest_sample_index Int32
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Link status changes (for drained_since and drained periods)
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_link_status_changes (
			link_pk String,
			link_code String,
			previous_status String,
			new_status String,
			changed_ts DateTime,
			side_a_metro String,
			side_z_metro String
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Link health (needed by some shared detection functions)
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_links_health_current (
			pk String,
			avg_rtt_us Float64,
			p95_rtt_us Float64,
			committed_rtt_ns Int64,
			loss_pct Float64,
			exceeds_committed_rtt UInt8,
			has_packet_loss UInt8,
			is_dark UInt8,
			is_down UInt8
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// ISIS adjacencies (current view + history)
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS isis_adjacencies_current (
			link_pk String,
			system_id String,
			neighbor_system_id String,
			neighbor_addr String,
			device_pk String,
			hostname String,
			router_id String,
			local_addr String,
			metric Int64,
			adj_sids String
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dim_isis_adjacencies_history (
			link_pk String,
			system_id String,
			neighbor_system_id String,
			neighbor_addr String,
			snapshot_ts DateTime,
			is_deleted UInt8
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// ISIS devices (current view + history)
	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS isis_devices_current (
			device_pk String,
			system_id String,
			hostname String,
			router_id String,
			overload UInt8,
			node_unreachable UInt8,
			sequence Int64
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dim_isis_devices_history (
			device_pk String,
			system_id String,
			hostname String,
			snapshot_ts DateTime,
			is_deleted UInt8,
			overload UInt8,
			node_unreachable UInt8
		) ENGINE = Memory
	`)
	require.NoError(t, err)
}

func insertIncidentsBaseData(t *testing.T) {
	ctx := t.Context()

	err := config.DB.Exec(ctx, `
		INSERT INTO dz_metros_current (pk, code, name) VALUES
		('metro-nyc', 'NYC', 'New York'),
		('metro-lax', 'LAX', 'Los Angeles')
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dz_devices_current (pk, code, device_type, metro_pk, public_ip) VALUES
		('dev-nyc-1', 'NYC-CORE-01', 'router', 'metro-nyc', '10.0.0.1'),
		('dev-lax-1', 'LAX-CORE-01', 'router', 'metro-lax', '10.0.1.1')
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO dz_contributors_current (pk, code, name) VALUES
		('contrib-1', 'CONTRIB1', 'Contributor One')
	`)
	require.NoError(t, err)
}

func TestGetLinkIncidents_EmptyState(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=1h", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err := json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Empty(t, resp.Active)
	assert.Empty(t, resp.Drained)
	assert.Equal(t, 0, resp.ActiveSummary.Total)
	assert.Equal(t, 0, resp.DrainedSummary.Total)
}

func TestGetLinkIncidents_ErrorsDetection(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	// Insert an activated link
	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert some recent latency data so the link doesn't show as no_data
	now := time.Now().UTC()
	for i := range 5 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
			($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0)
		`, ts)
		require.NoError(t, err)
	}

	// Insert error counter data: 8 consecutive 5-min buckets above threshold (10)
	// This should create a completed incident lasting 40 minutes (meets 30m min_duration)
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
			VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 15, 5, 0, 0, 0)
		`, ts)
		require.NoError(t, err)
	}

	// Insert a quiet bucket before and after to close the incident
	err = config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
		VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 0, 0, 0, 0, 0)
	`, baseTime.Add(-5*time.Minute))
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
		VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 0, 0, 0, 0, 0)
	`, baseTime.Add(40*time.Minute))
	require.NoError(t, err)

	// Query for errors type only, with min_duration=5 to catch shorter incidents too
	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=errors&errors_threshold=10&min_duration=5", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Should detect the error incident
	require.NotEmpty(t, resp.Active, "should detect error incident")

	// Verify the incident attributes
	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "errors" && inc.LinkCode == "NYC-LAX-001" {
			found = true
			assert.Equal(t, "NYC", inc.SideAMetro)
			assert.Equal(t, "LAX", inc.SideZMetro)
			assert.Equal(t, "WAN", inc.LinkType)
			assert.Equal(t, "CONTRIB1", inc.ContributorCode)
			assert.False(t, inc.IsDrained)
			assert.NotNil(t, inc.PeakCount)
			assert.True(t, *inc.PeakCount >= 10, "peak count should be above threshold")
			break
		}
	}
	assert.True(t, found, "should find errors incident for NYC-LAX-001")

	// Summary should reflect the error
	assert.True(t, resp.ActiveSummary.ByType["errors"] > 0, "summary should count errors")
}

func TestGetLinkIncidents_DrainedLinksView(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	now := time.Now().UTC()

	// Insert one activated link and one soft-drained link
	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1'),
		('link-2', 'NYC-LAX-002', 'soft-drained', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert drain timestamp
	drainedAt := now.Add(-6 * time.Hour)
	err = config.DB.Exec(ctx, `
		INSERT INTO dz_link_status_changes (link_pk, link_code, previous_status, new_status, changed_ts, side_a_metro, side_z_metro) VALUES
		('link-2', 'NYC-LAX-002', 'activated', 'soft-drained', $1, 'NYC', 'LAX')
	`, drainedAt)
	require.NoError(t, err)

	// Insert recent latency so neither shows as no_data
	for i := range 5 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
			($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0),
			($1, 'link-2', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-2', 'dev-lax-1', 1000.0, 50.0, 0)
		`, ts)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=24h", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Drained view should include the soft-drained link
	assert.Equal(t, 1, resp.DrainedSummary.Total, "should have 1 drained link")

	found := false
	for _, dl := range resp.Drained {
		if dl.LinkCode == "NYC-LAX-002" {
			found = true
			assert.Equal(t, "soft-drained", dl.DrainStatus)
			assert.Equal(t, "gray", dl.Readiness, "no incidents = gray readiness")
			assert.Empty(t, dl.ActiveIncidents)
			assert.NotEmpty(t, dl.DrainedSince, "should have drained_since timestamp")
			break
		}
	}
	assert.True(t, found, "should find drained link NYC-LAX-002")

	// Activated link should NOT appear in drained view
	for _, dl := range resp.Drained {
		assert.NotEqual(t, "NYC-LAX-001", dl.LinkCode, "activated link should not be in drained view")
	}
}

func TestGetLinkIncidents_DrainedWithErrors(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	now := time.Now().UTC()

	// Insert a drained link
	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'hard-drained', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert recent latency so it doesn't show as no_data
	for i := range 5 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
			($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0)
		`, ts)
		require.NoError(t, err)
	}

	// Insert ongoing error counters (recent buckets above threshold)
	for i := range 3 {
		ts := now.Add(-time.Duration(i*5) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
			VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 50, 50, 0, 0, 0)
		`, ts)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=1h&type=errors&errors_threshold=10&min_duration=5", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Drained link incidents should NOT appear in active (they're drained)
	for _, inc := range resp.Active {
		assert.NotEqual(t, "NYC-LAX-001", inc.LinkCode, "drained link should not be in active list")
	}

	// But the drained link should appear in drained view with incidents
	assert.Equal(t, 1, resp.DrainedSummary.Total)
	if len(resp.Drained) > 0 {
		dl := resp.Drained[0]
		assert.Equal(t, "NYC-LAX-001", dl.LinkCode)
		assert.Equal(t, "hard-drained", dl.DrainStatus)
		// Should have active incidents or red readiness
		if len(dl.ActiveIncidents) > 0 {
			assert.Equal(t, "red", dl.Readiness)
		}
	}
}

func TestGetLinkIncidents_MinDurationFilter(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	now := time.Now().UTC()

	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert recent latency
	for i := range 5 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
			($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0)
		`, ts)
		require.NoError(t, err)
	}

	// Insert a short error spike: only 2 buckets (10 min), then quiet
	baseTime := now.Add(-1 * time.Hour)
	for i := range 2 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
			VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 20, 0, 0, 0, 0)
		`, ts)
		require.NoError(t, err)
	}
	// Quiet bucket to close
	err = config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
		VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 0, 0, 0, 0, 0)
	`, baseTime.Add(10*time.Minute))
	require.NoError(t, err)
	err = config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
		VALUES ($1, 'dev-nyc-1', 'link-1', 0, 0, 300, 0, 0, 0, 0, 0)
	`, baseTime.Add(-5*time.Minute))
	require.NoError(t, err)

	// With min_duration=30, this 10-min incident should be filtered out
	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=errors&errors_threshold=10&min_duration=30", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Short incident should be returned but not confirmed with 30m min_duration
	for _, inc := range resp.Active {
		if inc.IncidentType == "errors" && inc.LinkCode == "NYC-LAX-001" && !inc.IsOngoing {
			if inc.DurationSeconds != nil && *inc.DurationSeconds < 1800 {
				assert.False(t, inc.Confirmed, "short completed incidents should not be confirmed with min_duration=30")
			}
		}
	}

	// With min_duration=5, it should be found
	req2 := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=errors&errors_threshold=10&min_duration=5", nil)
	rr2 := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr2, req2)

	assert.Equal(t, http.StatusOK, rr2.Code)

	var resp2 handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr2.Body).Decode(&resp2)
	require.NoError(t, err)

	found := false
	for _, inc := range resp2.Active {
		if inc.IncidentType == "errors" && inc.LinkCode == "NYC-LAX-001" {
			found = true
			break
		}
	}
	assert.True(t, found, "should find short error incident with min_duration=5")
}

func TestGetLinkIncidentsCSV(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	now := time.Now().UTC()

	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert latency so link doesn't show as no_data
	for i := range 5 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
			($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0)
		`, ts)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links/csv?range=1h", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidentsCSV(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "text/csv")
	assert.Contains(t, rr.Header().Get("Content-Disposition"), "attachment")

	// Should have CSV header
	body := rr.Body.String()
	assert.True(t, strings.HasPrefix(body, "id,link_code,"), "CSV should start with header row")
}

func TestGetLinkIncidents_TypeFilter(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	now := time.Now().UTC()

	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert recent latency
	for i := range 5 {
		ts := now.Add(-time.Duration(i) * time.Minute)
		err = config.DB.Exec(ctx, `
			INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
			($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
			($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0)
		`, ts)
		require.NoError(t, err)
	}

	// Insert error data
	baseTime := now.Add(-2 * time.Hour)
	for i := range 8 {
		ts := baseTime.Add(time.Duration(i*5) * time.Minute)
		err = config.DB.Exec(ctx, fmt.Sprintf(`
			INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
			VALUES ('%s', 'dev-nyc-1', 'link-1', 0, 0, 300, 20, 0, 0, 0, 0)
		`, ts.Format("2006-01-02 15:04:05")))
		require.NoError(t, err)
	}
	// Quiet buckets
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
		VALUES ('%s', 'dev-nyc-1', 'link-1', 0, 0, 300, 0, 0, 0, 0, 0)
	`, baseTime.Add(-5*time.Minute).Format("2006-01-02 15:04:05")))
	require.NoError(t, err)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO fact_dz_device_interface_counters
		(event_ts, device_pk, link_pk, in_octets_delta, out_octets_delta, delta_duration, in_errors_delta, out_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta)
		VALUES ('%s', 'dev-nyc-1', 'link-1', 0, 0, 300, 0, 0, 0, 0, 0)
	`, baseTime.Add(40*time.Minute).Format("2006-01-02 15:04:05")))
	require.NoError(t, err)

	// When filtering for packet_loss only, should NOT return errors
	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=packet_loss&min_duration=5", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	for _, inc := range resp.Active {
		assert.NotEqual(t, "errors", inc.IncidentType, "type=packet_loss should not return errors")
	}
}

func TestGetLinkIncidents_NoDataDetection(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupIncidentsTables(t)
	insertIncidentsBaseData(t)
	ctx := t.Context()

	// Insert a link with NO recent latency data (should trigger no_data)
	err := config.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, side_a_pk, side_z_pk, contributor_pk) VALUES
		('link-1', 'NYC-LAX-001', 'activated', 'WAN', 'dev-nyc-1', 'dev-lax-1', 'contrib-1')
	`)
	require.NoError(t, err)

	// Insert latency data from 1 hour ago (stale - should trigger no_data since >15min gap)
	staleTime := time.Now().UTC().Add(-1 * time.Hour)
	err = config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_link_latency (event_ts, link_pk, origin_device_pk, rtt_us, ipdv_us, loss) VALUES
		($1, 'link-1', 'dev-nyc-1', 1000.0, 50.0, 0),
		($1, 'link-1', 'dev-lax-1', 1000.0, 50.0, 0)
	`, staleTime)
	require.NoError(t, err)

	// Insert stale header so the freshness check detects this link as no_data
	err = config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_link_latency_sample_header
			(written_at, origin_device_pk, target_device_pk, link_pk, epoch, start_timestamp_us, sampling_interval_us, latest_sample_index)
		VALUES
			($1, 'dev-nyc-1', 'dev-lax-1', 'link-1', 0, 0, 0, 0),
			($1, 'dev-lax-1', 'dev-nyc-1', 'link-1', 0, 0, 0, 0)
	`, staleTime)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/incidents/links?range=6h&type=no_data&min_duration=5", nil)
	rr := httptest.NewRecorder()
	handlers.GetLinkIncidents(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp handlers.LinkIncidentsResponse
	err = json.NewDecoder(rr.Body).Decode(&resp)
	require.NoError(t, err)

	// Should detect no_data incident
	found := false
	for _, inc := range resp.Active {
		if inc.IncidentType == "no_data" && inc.LinkCode == "NYC-LAX-001" {
			found = true
			assert.True(t, inc.IsOngoing)
			break
		}
	}
	assert.True(t, found, "should detect no_data incident for link with stale telemetry")
}
