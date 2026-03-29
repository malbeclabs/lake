package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
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

func setupIncidentViews(t *testing.T, api *handlers.API) {
	ctx := t.Context()

	linkViewDDL := `CREATE OR REPLACE VIEW link_incidents_v AS
WITH
above AS (
    SELECT link_pk AS entity_pk, bucket_ts, greatest(a_loss_pct, z_loss_pct) AS metric_value, 'packet_loss' AS incident_type
    FROM link_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND provisioning = false AND greatest(a_loss_pct, z_loss_pct) >= 10
    UNION ALL
    SELECT link_pk AS entity_pk, bucket_ts, toFloat64(1) AS metric_value, 'isis_down' AS incident_type
    FROM link_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND isis_down = true AND provisioning = false
    UNION ALL
    SELECT al.link_pk AS entity_pk, e.bucket_ts AS bucket_ts, toFloat64(1) AS metric_value, 'no_data' AS incident_type
    FROM (SELECT DISTINCT link_pk FROM link_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY - INTERVAL 1 HOUR AND provisioning = false) al
    CROSS JOIN (SELECT toDateTime(toStartOfFiveMinutes(now() - INTERVAL 8 DAY)) + number * 300 AS bucket_ts FROM numbers(toUInt64(dateDiff('second', now() - INTERVAL 8 DAY, now()) / 300))) e
    LEFT JOIN (SELECT link_pk, bucket_ts FROM link_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY) a ON al.link_pk = a.link_pk AND e.bucket_ts = a.bucket_ts
    WHERE a.bucket_ts IS NULL
    UNION ALL
    SELECT link_pk AS entity_pk, bucket_ts, toFloat64(sum(in_errors + out_errors)) AS metric_value, 'errors' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk != '' GROUP BY link_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT link_pk AS entity_pk, bucket_ts, toFloat64(sum(in_fcs_errors)) AS metric_value, 'fcs' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk != '' GROUP BY link_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT link_pk AS entity_pk, bucket_ts, toFloat64(sum(in_discards + out_discards)) AS metric_value, 'discards' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk != '' GROUP BY link_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT link_pk AS entity_pk, bucket_ts, toFloat64(sum(carrier_transitions)) AS metric_value, 'carrier' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk != '' GROUP BY link_pk, bucket_ts HAVING metric_value >= 1
),
islands AS (SELECT entity_pk, incident_type, bucket_ts, metric_value, bucket_ts - toIntervalSecond(row_number() OVER (PARTITION BY entity_pk, incident_type ORDER BY bucket_ts) * 5 * 60) AS island_grp FROM above),
raw_incidents AS (SELECT entity_pk, incident_type, island_grp, min(bucket_ts) AS started_at, max(bucket_ts) + toIntervalSecond(5 * 60) AS ended_at, max(metric_value) AS peak_value, count() AS bucket_count FROM islands GROUP BY entity_pk, incident_type, island_grp),
numbered AS (SELECT *, lagInFrame(ended_at) OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS prev_ended_at FROM raw_incidents),
coalesce_groups AS (SELECT *, sum(if(prev_ended_at IS NULL OR dateDiff('minute', prev_ended_at, started_at) >= 180, 1, 0)) OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS coalesce_grp FROM numbered),
coalesced AS (SELECT entity_pk, incident_type, min(started_at) AS started_at, max(ended_at) AS ended_at, max(peak_value) AS peak_value, sum(bucket_count) AS total_buckets FROM coalesce_groups GROUP BY entity_pk, incident_type, coalesce_grp)
SELECT c.entity_pk, c.incident_type, c.started_at,
    if(c.ended_at >= now() - toIntervalMinute(15), toDateTime('1970-01-01 00:00:00'), c.ended_at) AS ended_at,
    c.ended_at >= now() - toIntervalMinute(15) AS is_ongoing, c.peak_value, c.total_buckets,
    dateDiff('second', c.started_at, if(c.ended_at >= now() - toIntervalMinute(15), now(), c.ended_at)) AS duration_seconds,
    COALESCE(l.code, '') AS link_code, COALESCE(l.link_type, '') AS link_type, COALESCE(l.status, '') AS status,
    COALESCE(ma.code, '') AS side_a_metro, COALESCE(mz.code, '') AS side_z_metro, COALESCE(cc.code, '') AS contributor_code
FROM coalesced c
LEFT JOIN dz_links_current l ON c.entity_pk = l.pk
LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
LEFT JOIN dz_contributors_current cc ON l.contributor_pk = cc.pk
ORDER BY c.started_at DESC`

	deviceViewDDL := `CREATE OR REPLACE VIEW device_incidents_v AS
WITH
above AS (
    SELECT device_pk AS entity_pk, bucket_ts, toFloat64(sum(in_errors + out_errors)) AS metric_value, 'errors' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk = '' GROUP BY device_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT device_pk AS entity_pk, bucket_ts, toFloat64(sum(in_fcs_errors)) AS metric_value, 'fcs' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk = '' GROUP BY device_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT device_pk AS entity_pk, bucket_ts, toFloat64(sum(in_discards + out_discards)) AS metric_value, 'discards' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk = '' GROUP BY device_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT device_pk AS entity_pk, bucket_ts, toFloat64(sum(carrier_transitions)) AS metric_value, 'carrier' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk = '' GROUP BY device_pk, bucket_ts HAVING metric_value >= 1
    UNION ALL
    SELECT ad.device_pk AS entity_pk, e.bucket_ts AS bucket_ts, toFloat64(1) AS metric_value, 'no_data' AS incident_type
    FROM (SELECT DISTINCT device_pk FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY - INTERVAL 1 HOUR AND link_pk = '') ad
    CROSS JOIN (SELECT toDateTime(toStartOfFiveMinutes(now() - INTERVAL 8 DAY)) + number * 300 AS bucket_ts FROM numbers(toUInt64(dateDiff('second', now() - INTERVAL 8 DAY, now()) / 300))) e
    LEFT JOIN (SELECT device_pk, bucket_ts FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND link_pk = '' GROUP BY device_pk, bucket_ts) a ON ad.device_pk = a.device_pk AND e.bucket_ts = a.bucket_ts
    WHERE a.bucket_ts IS NULL
    UNION ALL
    SELECT device_pk AS entity_pk, bucket_ts, toFloat64(1) AS metric_value, 'isis_overload' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND isis_overload = true AND link_pk = '' GROUP BY device_pk, bucket_ts
    UNION ALL
    SELECT device_pk AS entity_pk, bucket_ts, toFloat64(1) AS metric_value, 'isis_unreachable' AS incident_type FROM device_interface_rollup_5m FINAL WHERE bucket_ts >= now() - INTERVAL 8 DAY AND isis_unreachable = true AND link_pk = '' GROUP BY device_pk, bucket_ts
),
islands AS (SELECT entity_pk, incident_type, bucket_ts, metric_value, bucket_ts - toIntervalSecond(row_number() OVER (PARTITION BY entity_pk, incident_type ORDER BY bucket_ts) * 5 * 60) AS island_grp FROM above),
raw_incidents AS (SELECT entity_pk, incident_type, island_grp, min(bucket_ts) AS started_at, max(bucket_ts) + toIntervalSecond(5 * 60) AS ended_at, max(metric_value) AS peak_value, count() AS bucket_count FROM islands GROUP BY entity_pk, incident_type, island_grp),
numbered AS (SELECT *, lagInFrame(ended_at) OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS prev_ended_at FROM raw_incidents),
coalesce_groups AS (SELECT *, sum(if(prev_ended_at IS NULL OR dateDiff('minute', prev_ended_at, started_at) >= 180, 1, 0)) OVER (PARTITION BY entity_pk, incident_type ORDER BY started_at) AS coalesce_grp FROM numbered),
coalesced AS (SELECT entity_pk, incident_type, min(started_at) AS started_at, max(ended_at) AS ended_at, max(peak_value) AS peak_value, sum(bucket_count) AS total_buckets FROM coalesce_groups GROUP BY entity_pk, incident_type, coalesce_grp)
SELECT c.entity_pk, c.incident_type, c.started_at,
    if(c.ended_at >= now() - toIntervalMinute(15), toDateTime('1970-01-01 00:00:00'), c.ended_at) AS ended_at,
    c.ended_at >= now() - toIntervalMinute(15) AS is_ongoing, c.peak_value, c.total_buckets,
    dateDiff('second', c.started_at, if(c.ended_at >= now() - toIntervalMinute(15), now(), c.ended_at)) AS duration_seconds,
    COALESCE(d.code, '') AS device_code, COALESCE(d.device_type, '') AS device_type, COALESCE(d.status, '') AS status,
    COALESCE(m.code, '') AS metro, COALESCE(cc.code, '') AS contributor_code
FROM coalesced c
LEFT JOIN dz_devices_current d ON c.entity_pk = d.pk
LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
LEFT JOIN dz_contributors_current cc ON d.contributor_pk = cc.pk
ORDER BY c.started_at DESC`

	require.NoError(t, api.DB.Exec(ctx, linkViewDDL))
	require.NoError(t, api.DB.Exec(ctx, deviceViewDDL))
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
