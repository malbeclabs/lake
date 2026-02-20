package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMulticastTables(t *testing.T) {
	ctx := t.Context()

	err := config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_multicast_groups_current (
			pk String,
			code String,
			multicast_ip Nullable(String),
			max_bandwidth Nullable(UInt64),
			status Nullable(String),
			publisher_count Nullable(UInt32),
			subscriber_count Nullable(UInt32)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_users_current (
			pk String,
			owner_pubkey Nullable(String),
			status String,
			kind String,
			client_ip Nullable(String),
			dz_ip Nullable(String),
			device_pk Nullable(String),
			tunnel_id Nullable(Int32),
			publishers String DEFAULT '[]',
			subscribers String DEFAULT '[]'
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String,
			code String,
			status String,
			device_type String,
			contributor_pk Nullable(String),
			metro_pk Nullable(String),
			public_ip Nullable(String),
			max_users Nullable(Int32),
			interfaces Nullable(String)
		) ENGINE = Memory
	`)
	require.NoError(t, err)

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

	err = config.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fact_dz_device_interface_counters (
			event_ts DateTime64(3),
			ingested_at DateTime64(3) DEFAULT now(),
			device_pk String,
			host String DEFAULT '',
			intf String DEFAULT '',
			user_tunnel_id Nullable(Int64),
			link_pk String DEFAULT '',
			link_side String DEFAULT '',
			model_name String DEFAULT '',
			serial_number String DEFAULT '',
			in_octets_delta Nullable(Int64),
			out_octets_delta Nullable(Int64),
			delta_duration Nullable(Float64)
		) ENGINE = Memory
	`)
	require.NoError(t, err)
}

func insertMulticastTestData(t *testing.T) {
	ctx := t.Context()

	// Insert metros
	err := config.DB.Exec(ctx, `
		INSERT INTO dz_metros_current (pk, code, name) VALUES
		('metro-ams', 'ams', 'Amsterdam'),
		('metro-nyc', 'nyc', 'New York')
	`)
	require.NoError(t, err)

	// Insert devices
	err = config.DB.Exec(ctx, `
		INSERT INTO dz_devices_current (pk, code, status, device_type, metro_pk) VALUES
		('dev-ams1', 'ams001-dz001', 'up', 'edge', 'metro-ams'),
		('dev-nyc1', 'nyc001-dz001', 'up', 'edge', 'metro-nyc')
	`)
	require.NoError(t, err)

	// Insert multicast group
	err = config.DB.Exec(ctx, `
		INSERT INTO dz_multicast_groups_current (pk, code, multicast_ip, max_bandwidth, status, publisher_count, subscriber_count) VALUES
		('group-1', 'test-group', '233.0.0.1', 100000000, 'activated', 0, 0)
	`)
	require.NoError(t, err)

	// Insert multicast users: one publisher, one subscriber
	err = config.DB.Exec(ctx, `
		INSERT INTO dz_users_current (pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tunnel_id, publishers, subscribers) VALUES
		('user-pub', 'pubkey-pub', 'activated', 'multicast', '10.0.0.1', '10.0.0.1', 'dev-ams1', 501, '["group-1"]', '[]'),
		('user-sub', 'pubkey-sub', 'activated', 'multicast', '10.0.0.2', '10.0.0.2', 'dev-nyc1', 502, '[]', '["group-1"]')
	`)
	require.NoError(t, err)
}

func TestGetMulticastGroups_Empty(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups", nil)
	rr := httptest.NewRecorder()
	handlers.GetMulticastGroups(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var groups []handlers.MulticastGroupListItem
	err := json.NewDecoder(rr.Body).Decode(&groups)
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestGetMulticastGroups_ReturnsRealCounts(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)
	insertMulticastTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups", nil)
	rr := httptest.NewRecorder()
	handlers.GetMulticastGroups(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var groups []handlers.MulticastGroupListItem
	err := json.NewDecoder(rr.Body).Decode(&groups)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	// The table has publisher_count=0 / subscriber_count=0, but the enrichment
	// query should compute the real counts from dz_users_current.
	assert.Equal(t, "test-group", groups[0].Code)
	assert.Equal(t, uint32(1), groups[0].PublisherCount, "should compute real publisher count from users")
	assert.Equal(t, uint32(1), groups[0].SubscriberCount, "should compute real subscriber count from users")
}

func TestGetMulticastGroup_NotFound(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/nonexistent", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("code", "nonexistent")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetMulticastGroup_ReturnsMembers(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)
	insertMulticastTestData(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("code", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var detail handlers.MulticastGroupDetail
	err := json.NewDecoder(rr.Body).Decode(&detail)
	require.NoError(t, err)

	assert.Equal(t, "test-group", detail.Code)
	assert.Equal(t, "233.0.0.1", detail.MulticastIP)
	require.Len(t, detail.Members, 2)

	// Find publisher and subscriber
	var pub, sub *handlers.MulticastMember
	for i := range detail.Members {
		switch detail.Members[i].Mode {
		case "P":
			pub = &detail.Members[i]
		case "S":
			sub = &detail.Members[i]
		}
	}

	require.NotNil(t, pub, "should have a publisher member")
	assert.Equal(t, "user-pub", pub.UserPK)
	assert.Equal(t, "ams001-dz001", pub.DeviceCode)
	assert.Equal(t, "ams", pub.MetroCode)
	assert.Equal(t, int32(501), pub.TunnelID)

	require.NotNil(t, sub, "should have a subscriber member")
	assert.Equal(t, "user-sub", sub.UserPK)
	assert.Equal(t, "nyc001-dz001", sub.DeviceCode)
	assert.Equal(t, "nyc", sub.MetroCode)
	assert.Equal(t, int32(502), sub.TunnelID)
}

func TestGetMulticastGroup_TrafficBps(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)
	insertMulticastTestData(t)

	ctx := t.Context()

	// Insert traffic counter data for both tunnels (recent, within 5 min)
	err := config.DB.Exec(ctx, `
		INSERT INTO fact_dz_device_interface_counters
			(event_ts, device_pk, user_tunnel_id, in_octets_delta, out_octets_delta, delta_duration)
		VALUES
			(now(), 'dev-ams1', 501, 1000, 50000000, 4.0),
			(now(), 'dev-ams1', 501, 1000, 50000000, 4.0),
			(now(), 'dev-nyc1', 502, 50000000, 1000, 4.0)
	`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("code", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var detail handlers.MulticastGroupDetail
	err = json.NewDecoder(rr.Body).Decode(&detail)
	require.NoError(t, err)
	require.Len(t, detail.Members, 2)

	// Find publisher and subscriber
	var pub, sub *handlers.MulticastMember
	for i := range detail.Members {
		switch detail.Members[i].Mode {
		case "P":
			pub = &detail.Members[i]
		case "S":
			sub = &detail.Members[i]
		}
	}

	require.NotNil(t, pub)
	require.NotNil(t, sub)

	// Publisher: 2 samples, each (1000 + 50000000) bytes * 8 / 4.0s
	// = 2 * 400008000 / 8.0 total_duration = sum of both
	// Actually: sum(in+out) * 8 / sum(duration) = (50001000 + 50001000) * 8 / (4+4)
	// = 100002000 * 8 / 8 = 100002000 bps
	assert.Greater(t, pub.TrafficBps, float64(0), "publisher should have traffic rate")

	// Subscriber: 1 sample, (50000000 + 1000) * 8 / 4.0 = 100002000 bps
	assert.Greater(t, sub.TrafficBps, float64(0), "subscriber should have traffic rate")
}

func TestGetMulticastGroup_TrafficBps_NoCounters(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)
	insertMulticastTestData(t)

	// Don't insert any traffic counters — traffic_bps should be 0

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/test-group", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("code", "test-group")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var detail handlers.MulticastGroupDetail
	err := json.NewDecoder(rr.Body).Decode(&detail)
	require.NoError(t, err)
	require.Len(t, detail.Members, 2)

	for _, m := range detail.Members {
		assert.Equal(t, float64(0), m.TrafficBps, "traffic_bps should be 0 when no counters exist")
	}
}

func TestGetMulticastGroup_MissingCode(t *testing.T) {
	apitesting.SetupTestClickHouse(t, testChDB)
	setupMulticastTables(t)

	req := httptest.NewRequest(http.MethodGet, "/api/dz/multicast-groups/", nil)
	rctx := chi.NewRouteContext()
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	handlers.GetMulticastGroup(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}
