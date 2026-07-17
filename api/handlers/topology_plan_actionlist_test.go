package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedActionListBaseline stands up the dz_*_current tables that
// (a *API).deriveActionList reads through FetchTopologyData. Those tables are
// VIEWs over the *_history tables in the real schema (migration
// 20250117000003_dim_current_views.sql, widened by later migrations to add
// the device capacity/user-count and link topology/interface columns
// FetchTopologyData selects). As in the Phase 2 impact-endpoint test
// (seedImpactTopology in topology_plan_impact_endpoint_test.go), drop the
// views first so "CREATE TABLE ... ENGINE = Memory" can stand up real,
// writable tables with the same names and the columns FetchTopologyData's
// queries actually reference.
func seedActionListBaseline(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_metros_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_contributors_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_devices_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_links_current`))

	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String, code String, name String, latitude Float64, longitude Float64
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_contributors_current (
			pk String, code String, name String
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String, code String, status String, device_type String,
			metro_pk String, contributor_pk String,
			unicast_users_count UInt16 DEFAULT 0,
			multicast_subscribers_count UInt16 DEFAULT 0,
			multicast_publishers_count UInt16 DEFAULT 0,
			max_unicast_users UInt16 DEFAULT 0,
			max_multicast_subscribers UInt16 DEFAULT 0,
			max_multicast_publishers UInt16 DEFAULT 0,
			interfaces String DEFAULT '[]'
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String, code String, status String, link_type String,
			bandwidth_bps Int64, side_a_pk String, side_z_pk String,
			side_a_iface_name String DEFAULT '', side_a_ip String DEFAULT '',
			side_z_iface_name String DEFAULT '', side_z_ip String DEFAULT '',
			contributor_pk String DEFAULT '',
			committed_rtt_ns Int64 DEFAULT 0, isis_delay_override_ns Int64 DEFAULT 0,
			link_topologies String DEFAULT '[]', unicast_drained UInt8 DEFAULT 0
		) ENGINE = Memory`))
}

func TestGetTopologyPlanActionList_CrossContributorMove(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	ctx := t.Context()

	seedActionListBaseline(t, api)

	// Seed a minimal live topology: DZX link l-1 (jump_ <-> teleport), plus a
	// latitude-owned device d-c the link's A-end will be moved onto.
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_metros_current (pk, code, name, latitude, longitude) VALUES
		('m-lax','LAX','Los Angeles',34.05,-118.24),
		('m-nyc','NYC','New York',40.71,-74.0)`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_contributors_current (pk, code, name) VALUES
		('c-jump','jump_','Jump'),('c-tele','teleport','Teleport'),('c-lat','latitude','Latitude')`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_devices_current (pk, code, status, device_type, metro_pk, contributor_pk) VALUES
		('d-a','lax001-dz001','activated','switch','m-lax','c-jump'),
		('d-z','nyc001-dz001','activated','switch','m-nyc','c-tele'),
		('d-c','nyc002-dz001','activated','switch','m-nyc','c-lat')`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, bandwidth_bps, side_a_pk, side_z_pk, contributor_pk, committed_rtt_ns, isis_delay_override_ns) VALUES
		('l-1','lax001-dz001:nyc001-dz001','activated','DZX',10000000000,'d-a','d-z','c-jump',5000000,0)`))

	// Seed the plan + one move_link_end change directly (Phase 1 tables).
	planID := uuid.New()
	_, err := api.PgPool.Exec(ctx, `
		INSERT INTO topology_plans (id, name, status, environment, baseline_as_of, version)
		VALUES ($1, 'DZX move', 'draft', 'mainnet-beta', now(), 1)`, planID)
	require.NoError(t, err)

	// new_device_pk rides the dedicated column (SC-1); only new_device_ref lives in payload.
	payload := `{"side":"a","new_iface_name":"Ethernet1","latency_ns":5000000,"bandwidth_bps":10000000000}`
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_changes (id, plan_id, seq, op_type, ref_link_pk, new_device_pk, payload, ref_snapshot, state, version)
		VALUES ($1, $2, 10, 'move_link_end', 'l-1', 'd-c', $3::jsonb, '{}'::jsonb, 'pending', 1)`,
		uuid.New(), planID, payload)
	require.NoError(t, err)

	// Drive the handler with the chi URL param populated.
	req := httptest.NewRequest(http.MethodGet, "/api/topology/plans/"+planID.String()+"/action-list", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", planID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()
	api.GetTopologyPlanActionList(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var al handlers.ActionList
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&al))

	byCode := map[string]handlers.ContributorActionGroup{}
	for _, g := range al.Groups {
		byCode[g.ContributorCode] = g
	}
	require.Contains(t, byCode, "jump_")
	require.Contains(t, byCode, "latitude")
	require.Contains(t, byCode, "teleport")

	want := "jump_ ↔ latitude: coordinate moving DZX link lax001-dz001:nyc001-dz001 to device nyc002-dz001"
	for _, code := range []string{"jump_", "latitude", "teleport"} {
		g := byCode[code]
		require.Len(t, g.Tasks, 1)
		assert.Equal(t, want, g.Tasks[0].Title)
		assert.Equal(t, "#ext-doublezero-"+code, g.SlackChannel)
	}
}

func TestGetTopologyPlanActionList_NotFound(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/topology/plans/"+uuid.New().String()+"/action-list", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", uuid.New().String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()
	api.GetTopologyPlanActionList(rr, req)
	require.Equal(t, http.StatusNotFound, rr.Code)
}
