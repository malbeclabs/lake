package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedImpactTopology seeds a linear topology with a parallel link:
//
//	A --l1-- B --l2-- C   (l3 is a second A-B link, parallel to l1)
//
// Metros: A=M1, B=M2, C=M3. buildPlannerGraph -> loadTopologyGraph reads the
// dz_*_current tables, so per SC-3 the test seeds those directly (as ENGINE =
// Memory tables) and NOT the *_history tables.
//
// NewTestAPIAll applies full ClickHouse migrations (needed here for the
// Postgres plan tables), which already define dz_metros_current/
// dz_devices_current/dz_links_current as VIEWs over the *_history tables
// (migration 20250117000003_dim_current_views.sql). Drop those views first so
// "CREATE TABLE ... ENGINE = Memory" can stand up real, writable tables with
// the same names for buildPlannerGraph to read.
func seedImpactTopology(t *testing.T, api *handlers.API) {
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_metros_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_devices_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_links_current`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String, code String, name String
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String, code String, status String, device_type String,
			metro_pk String, contributor_pk String
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String, code String, status String, link_type String,
			bandwidth_bps Int64, side_a_pk String, side_z_pk String,
			committed_rtt_ns Int64, isis_delay_override_ns Int64
		) ENGINE = Memory`))

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_metros_current (pk, code, name) VALUES
		('m1', 'M1', 'Metro1'),
		('m2', 'M2', 'Metro2'),
		('m3', 'M3', 'Metro3')
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_devices_current (pk, code, status, device_type, metro_pk, contributor_pk) VALUES
		('dev-a', 'A', 'activated', 'switch', 'm1', 'con-1'),
		('dev-b', 'B', 'activated', 'switch', 'm2', 'con-2'),
		('dev-c', 'C', 'activated', 'switch', 'm3', 'con-3')
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, bandwidth_bps, side_a_pk, side_z_pk, committed_rtt_ns, isis_delay_override_ns) VALUES
		('l1', 'A-B',   'activated', 'WAN', 100, 'dev-a', 'dev-b', 10000000, 0),
		('l2', 'B-C',   'activated', 'WAN', 100, 'dev-b', 'dev-c', 10000000, 0),
		('l3', 'A-B-2', 'activated', 'WAN',  50, 'dev-a', 'dev-b', 20000000, 0)
	`))
}

// seedLinearChainTopology seeds a strictly linear topology with no parallel
// backup links: A --l1-- B --l2-- C --l3-- D. Unlike seedImpactTopology (which
// has a parallel A-B link), removing an end link here isolates exactly the
// device at that end and nothing else, giving the SC-8 draft-membership
// filter (pending vs. skipped) an unambiguous before/after signal.
func seedLinearChainTopology(t *testing.T, api *handlers.API) {
	ctx := t.Context()
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_metros_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_devices_current`))
	require.NoError(t, api.DB.Exec(ctx, `DROP VIEW IF EXISTS dz_links_current`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_metros_current (
			pk String, code String, name String
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_devices_current (
			pk String, code String, status String, device_type String,
			metro_pk String, contributor_pk String
		) ENGINE = Memory`))
	require.NoError(t, api.DB.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dz_links_current (
			pk String, code String, status String, link_type String,
			bandwidth_bps Int64, side_a_pk String, side_z_pk String,
			committed_rtt_ns Int64, isis_delay_override_ns Int64
		) ENGINE = Memory`))

	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_metros_current (pk, code, name) VALUES
		('m1', 'M1', 'Metro1'), ('m2', 'M2', 'Metro2'), ('m3', 'M3', 'Metro3'), ('m4', 'M4', 'Metro4')
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_devices_current (pk, code, status, device_type, metro_pk, contributor_pk) VALUES
		('dev-a', 'A', 'activated', 'switch', 'm1', 'con-1'),
		('dev-b', 'B', 'activated', 'switch', 'm2', 'con-2'),
		('dev-c', 'C', 'activated', 'switch', 'm3', 'con-3'),
		('dev-d', 'D', 'activated', 'switch', 'm4', 'con-4')
	`))
	require.NoError(t, api.DB.Exec(ctx, `
		INSERT INTO dz_links_current (pk, code, status, link_type, bandwidth_bps, side_a_pk, side_z_pk, committed_rtt_ns, isis_delay_override_ns) VALUES
		('l1', 'A-B', 'activated', 'WAN', 100, 'dev-a', 'dev-b', 10000000, 0),
		('l2', 'B-C', 'activated', 'WAN', 100, 'dev-b', 'dev-c', 10000000, 0),
		('l3', 'C-D', 'activated', 'WAN', 100, 'dev-c', 'dev-d', 10000000, 0)
	`))
}

func insertPlan(t *testing.T, api *handlers.API, id, name, env, status string) {
	_, err := api.PgPool.Exec(t.Context(), `
		INSERT INTO topology_plans (id, name, status, environment, baseline_as_of, version, created_at, updated_at)
		VALUES ($1, $2, $3::plan_status, $4, NOW(), 1, NOW(), NOW())`,
		id, name, status, env)
	require.NoError(t, err)
}

func insertChange(t *testing.T, api *handlers.API, planID string, seq int, op, refDevicePK, refLinkPK, newDevicePK, localRef, payload, snapshot string) {
	insertChangeState(t, api, planID, seq, op, refDevicePK, refLinkPK, newDevicePK, localRef, payload, snapshot, "pending")
}

// insertChangeState is insertChange with an explicit state, for tests that
// need a non-pending change (e.g. SC-8 draft-membership filtering).
func insertChangeState(t *testing.T, api *handlers.API, planID string, seq int, op, refDevicePK, refLinkPK, newDevicePK, localRef, payload, snapshot, state string) {
	_, err := api.PgPool.Exec(t.Context(), `
		INSERT INTO topology_plan_changes
		(id, plan_id, seq, op_type, ref_device_pk, ref_link_pk, new_device_pk, local_ref, payload, ref_snapshot, state, version, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, $2, $3::plan_op_type, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::plan_change_state, 1, NOW(), NOW())`,
		planID, seq, op, refDevicePK, refLinkPK, newDevicePK, localRef, payload, snapshot, state)
	require.NoError(t, err)
}

// postImpact drives the endpoint with no ambient env set on the request, so
// EnvFromContext defaults to mainnet-beta. The handler resolves the env from the
// plan row (also mainnet-beta here) and uses it consistently for BOTH the
// baseline graph and the capacity-traffic load. In this single-env harness
// api.EnvDBs is empty, so both resolve to api.DB; a real cross-env test (where
// the plan env differs from the ambient env and each has its own ClickHouse DB)
// would need a second seeded EnvDBs entry and is impractical in this fixture.
func postImpact(t *testing.T, api *handlers.API, planID string) handlers.PlanImpactReport {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/topology/plans/"+planID+"/impact", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", planID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()
	api.PostTopologyPlanImpact(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	var rep handlers.PlanImpactReport
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&rep))
	return rep
}

func TestPostTopologyPlanImpact_RemoveLinkPartitionsAndOverlap(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	seedImpactTopology(t, api)

	planID := "11111111-1111-1111-1111-111111111111"
	insertPlan(t, api, planID, "decom bc", "mainnet-beta", "draft")
	insertChange(t, api, planID, 10, "remove_link", "", "l2", "", "",
		`{}`, `{"link_code":"B-C"}`)

	// A second active plan also touches l2 -> overlap warning.
	other := "22222222-2222-2222-2222-222222222222"
	insertPlan(t, api, other, "other plan", "mainnet-beta", "approved")
	insertChange(t, api, other, 10, "remove_link", "", "l2", "", "", `{}`, `{}`)

	rep := postImpact(t, api, planID)

	// C is isolated (its only link l2 removed).
	var isolatedC bool
	for _, p := range rep.PartitionIssues {
		if p.Type == "device_isolated" && p.EntityCode == "C" {
			isolatedC = true
		}
	}
	assert.True(t, isolatedC, "device C should be isolated")

	// Overlap on l2 flagged.
	var overlap bool
	for _, o := range rep.OverlapWarnings {
		if o.EntityPK == "l2" && o.OtherPlanID == other {
			overlap = true
		}
	}
	assert.True(t, overlap, "cross-plan overlap on l2 expected")
	assert.Empty(t, rep.DataIssues)
}

func TestPostTopologyPlanImpact_ParallelLinkNoPartition(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	seedImpactTopology(t, api)

	planID := "33333333-3333-3333-3333-333333333333"
	insertPlan(t, api, planID, "drop parallel", "mainnet-beta", "draft")
	// Remove only l3 (parallel A-B); l1 remains, so nothing is isolated.
	insertChange(t, api, planID, 10, "remove_link", "", "l3", "", "", `{}`, `{"link_code":"A-B-2"}`)

	rep := postImpact(t, api, planID)
	for _, p := range rep.PartitionIssues {
		assert.NotEqual(t, "device_isolated", p.Type, "removing a parallel link must not isolate any device")
	}
}

func TestPostTopologyPlanImpact_SentinelLatencyDataIssue(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	seedImpactTopology(t, api)

	planID := "44444444-4444-4444-4444-444444444444"
	insertPlan(t, api, planID, "bad add", "mainnet-beta", "draft")
	insertChange(t, api, planID, 10, "add_link", "", "", "", "tmp_link_1",
		`{"side_a_device_pk":"dev-a","side_z_device_pk":"dev-c","latency_ns":1000000000,"bandwidth_bps":100}`, `{}`)

	rep := postImpact(t, api, planID)
	require.NotEmpty(t, rep.DataIssues)
	assert.Contains(t, rep.DataIssues[0].Message, "sentinel")
}

// postImpactWithBody drives the impact endpoint with an inline PlanImpactRequest
// body (a live preview of an unsaved draft), unlike postImpact which relies on
// the DB fallback (a.loadPendingPlanChanges, which already filters
// state='pending' in SQL). The inline-body override path is where SC-8's
// draft-membership rule (pending-only) actually needed fixing.
func postImpactWithBody(t *testing.T, api *handlers.API, planID string, req handlers.PlanImpactRequest) handlers.PlanImpactReport {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/api/topology/plans/"+planID+"/impact", bytes.NewReader(body))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", planID)
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	rr := httptest.NewRecorder()
	api.PostTopologyPlanImpact(rr, r)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var rep handlers.PlanImpactReport
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&rep))
	return rep
}

// TestPostTopologyPlanImpact_DraftGraphExcludesSkippedChanges is the SC-8
// regression test: the draft graph is baseline + only pending changes. A
// skipped change in the same request must not be applied, even though
// applyChanges itself is state-agnostic (it applies whatever list it is
// given — filtering happens in the impact endpoint, not inside applyChanges).
func TestPostTopologyPlanImpact_DraftGraphExcludesSkippedChanges(t *testing.T) {
	api := apitesting.NewTestAPIAll(t, testChDB, testPgDB, nil, nil)
	seedLinearChainTopology(t, api)

	planID := "55555555-5555-5555-5555-555555555555"
	insertPlan(t, api, planID, "skip filter", "mainnet-beta", "draft")

	// Pending: remove l3 (C-D) -> isolates D only.
	// Skipped: remove l1 (A-B) -> if this were (wrongly) applied too, A would
	// also become isolated, since A only has l1.
	changes := []handlers.PlanChange{
		{
			Seq: 10, OpType: handlers.OpRemoveLink, RefLinkPK: "l3",
			State:       handlers.StatePending,
			Payload:     json.RawMessage(`{}`),
			RefSnapshot: json.RawMessage(`{"link_code":"C-D"}`),
		},
		{
			Seq: 20, OpType: handlers.OpRemoveLink, RefLinkPK: "l1",
			State:       handlers.StateSkipped,
			Payload:     json.RawMessage(`{}`),
			RefSnapshot: json.RawMessage(`{"link_code":"A-B"}`),
		},
	}

	rep := postImpactWithBody(t, api, planID, handlers.PlanImpactRequest{Changes: changes})

	isolated := map[string]bool{}
	for _, p := range rep.PartitionIssues {
		if p.Type == "device_isolated" {
			isolated[p.EntityCode] = true
		}
	}
	assert.True(t, isolated["D"], "pending remove_link should isolate D")
	assert.False(t, isolated["A"], "skipped remove_link must be excluded from the draft graph")
}
