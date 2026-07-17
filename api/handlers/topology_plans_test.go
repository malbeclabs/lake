package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopologyPlansSchema(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()

	account := createTestAccount(t, ctx, api)
	env := "schematest_" + uuid.New().String()[:8]

	// Insert a plan; defaults apply (status=draft, version=1).
	var planID uuid.UUID
	var status string
	var version int
	err := api.PgPool.QueryRow(ctx, `
		INSERT INTO topology_plans (name, environment, created_by_account_id)
		VALUES ($1, $2, $3)
		RETURNING id, status, version
	`, "Schema Plan", env, account.ID).Scan(&planID, &status, &version)
	require.NoError(t, err)
	assert.Equal(t, "draft", status)
	assert.Equal(t, 1, version)

	// A valid change inserts.
	linkPK := "link_abc"
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk)
		VALUES ($1, 10, 'remove_link', $2)
	`, planID, linkPK)
	require.NoError(t, err)

	// The op-type shape CHECK rejects remove_link with no ref_link_pk.
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_changes (plan_id, seq, op_type)
		VALUES ($1, 20, 'remove_link')
	`, planID)
	require.Error(t, err, "CHECK should reject remove_link with NULL ref_link_pk")

	// Partial unique index: same env+name blocked while not soft-deleted.
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plans (name, environment) VALUES ($1, $2)
	`, "Schema Plan", env)
	require.Error(t, err, "duplicate active name in same env must be rejected")

	// Soft-delete frees the name.
	_, err = api.PgPool.Exec(ctx, `UPDATE topology_plans SET deleted_at = NOW() WHERE id = $1`, planID)
	require.NoError(t, err)
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plans (name, environment) VALUES ($1, $2)
	`, "Schema Plan", env)
	require.NoError(t, err, "name reusable after soft-delete")

	// Events + issues tables accept rows (Phase 6 uses issues).
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_events (plan_id, actor_account_id, action)
		VALUES ($1, $2, 'plan.create')
	`, planID, account.ID)
	require.NoError(t, err)
	_, err = api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_issues (plan_id, github_repo, issue_number, issue_url)
		VALUES ($1, 'malbeclabs/infra', 1, 'https://github.com/malbeclabs/infra/issues/1')
	`, planID)
	require.NoError(t, err)
}

func newInternalAccount(t *testing.T, ctx context.Context, api *handlers.API) *handlers.Account {
	t.Helper()
	acc := createTestAccount(t, ctx, api)
	acc.Email = strPtr("op@doublezero.xyz")
	acc.IsInternalUser = true
	return acc
}

// planCtx returns a request context carrying an isolated env plus the account.
func planReq(t *testing.T, method, target string, body []byte, env string, acc *handlers.Account) *http.Request {
	t.Helper()
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, target, bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	r = r.WithContext(handlers.ContextWithEnv(r.Context(), handlers.DZEnv(env)))
	r = withAccount(r, acc)
	return r
}

func TestCreatePlan(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "create_" + uuid.New().String()[:8]

	body, _ := json.Marshal(handlers.CreatePlanRequest{Name: "Decom galaxy tor1"})
	req := planReq(t, http.MethodPost, "/api/topology/plans", body, env, acc)
	rr := httptest.NewRecorder()
	api.CreatePlan(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)
	var plan handlers.Plan
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&plan))
	assert.Equal(t, "Decom galaxy tor1", plan.Name)
	assert.Equal(t, handlers.StatusDraft, plan.Status)
	assert.Equal(t, env, plan.Environment)
	assert.Equal(t, 1, plan.Version)
	assert.Equal(t, "op@doublezero.xyz", plan.CreatedByEmail)
	assert.Empty(t, plan.Changes)

	// A plan.create event is recorded.
	var events int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_events WHERE plan_id=$1 AND action='plan.create'`,
		plan.ID).Scan(&events))
	assert.Equal(t, 1, events)
}

func TestCreatePlan_DuplicateName(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "dup_" + uuid.New().String()[:8]

	body, _ := json.Marshal(handlers.CreatePlanRequest{Name: "same"})
	rr := httptest.NewRecorder()
	api.CreatePlan(rr, planReq(t, http.MethodPost, "/api/topology/plans", body, env, acc))
	require.Equal(t, http.StatusCreated, rr.Code)

	rr = httptest.NewRecorder()
	api.CreatePlan(rr, planReq(t, http.MethodPost, "/api/topology/plans", body, env, acc))
	assert.Equal(t, http.StatusConflict, rr.Code)
}

func TestListPlans(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "list_" + uuid.New().String()[:8]

	// Two plans in this env; one soft-deleted; one in a different env.
	mk := func(name, e string) uuid.UUID {
		var id uuid.UUID
		require.NoError(t, api.PgPool.QueryRow(ctx,
			`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ($1,$2,$3) RETURNING id`,
			name, e, acc.ID).Scan(&id))
		return id
	}
	keep := mk("keep-a", env)
	mk("keep-b", env)
	del := mk("gone", env)
	arch := mk("archived-one", env)
	mk("other-env", "other_"+uuid.New().String()[:8])
	_, err := api.PgPool.Exec(ctx, `UPDATE topology_plans SET deleted_at=NOW() WHERE id=$1`, del)
	require.NoError(t, err)
	// Archived plans are hidden from the default shared-workspace list.
	_, err = api.PgPool.Exec(ctx, `UPDATE topology_plans SET status='archived' WHERE id=$1`, arch)
	require.NoError(t, err)
	// Give keep-a one change so change_count is exercised.
	_, err = api.PgPool.Exec(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk) VALUES ($1,10,'remove_link','l1')`, keep)
	require.NoError(t, err)

	req := planReq(t, http.MethodGet, "/api/topology/plans", nil, env, acc)
	rr := httptest.NewRecorder()
	api.ListPlans(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.PlansListResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Plans, 2)
	names := map[string]int{}
	for _, p := range resp.Plans {
		names[p.Name] = p.ChangeCount
	}
	assert.Contains(t, names, "keep-a")
	assert.Contains(t, names, "keep-b")
	assert.Equal(t, 1, names["keep-a"])
	assert.NotContains(t, names, "archived-one")

	// ?status=archived opts back into the archived plan only.
	req = planReq(t, http.MethodGet, "/api/topology/plans?status=archived", nil, env, acc)
	rr = httptest.NewRecorder()
	api.ListPlans(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	var archResp handlers.PlansListResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&archResp))
	require.Len(t, archResp.Plans, 1)
	assert.Equal(t, "archived-one", archResp.Plans[0].Name)
}

func TestCreatePlan_MissingName(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	body, _ := json.Marshal(handlers.CreatePlanRequest{Name: "  "})
	rr := httptest.NewRecorder()
	api.CreatePlan(rr, planReq(t, http.MethodPost, "/api/topology/plans", body, "x_"+uuid.New().String()[:8], acc))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetPlan_WithChanges(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "get_" + uuid.New().String()[:8]

	var planID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('g',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))
	_, err := api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk, ref_device_pk, target_date)
		VALUES ($1, 20, 'remove_link', 'lz', NULL, '2026-08-01'),
		       ($1, 10, 'remove_device', NULL, 'dz-dev', NULL)`, planID)
	require.NoError(t, err)

	req := planReq(t, http.MethodGet, "/api/topology/plans/"+planID.String(), nil, env, acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String()})
	rr := httptest.NewRecorder()
	api.GetPlan(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var plan handlers.Plan
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&plan))
	require.Len(t, plan.Changes, 2)
	// Ordered by seq: remove_device (10) then remove_link (20).
	assert.Equal(t, handlers.OpRemoveDevice, plan.Changes[0].OpType)
	assert.Equal(t, handlers.OpRemoveLink, plan.Changes[1].OpType)
	require.NotNil(t, plan.Changes[1].TargetDate)
	assert.Equal(t, "2026-08-01", *plan.Changes[1].TargetDate)
	assert.Equal(t, handlers.StatePending, plan.Changes[0].State)
}

func TestDuplicatePlan_DeepCopy(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "dupe_" + uuid.New().String()[:8]

	var srcID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, status, created_by_account_id)
		 VALUES ('orig', $1, 'approved', $2) RETURNING id`, env, acc.ID).Scan(&srcID))
	_, err := api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk, state, payload)
		VALUES ($1, 10, 'remove_link', 'lk1', 'done', '{"note":"x"}')`, srcID)
	require.NoError(t, err)

	req := planReq(t, http.MethodPost, "/api/topology/plans/"+srcID.String()+"/duplicate", nil, env, acc)
	req = withChiURLParams(req, map[string]string{"id": srcID.String()})
	rr := httptest.NewRecorder()
	api.DuplicatePlan(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)
	var copy handlers.Plan
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&copy))
	assert.Equal(t, "orig (copy)", copy.Name)
	assert.Equal(t, handlers.StatusDraft, copy.Status)
	assert.Equal(t, 1, copy.Version)
	require.NotNil(t, copy.ForkedFromPlanID)
	assert.Equal(t, srcID, *copy.ForkedFromPlanID)
	assert.NotEqual(t, srcID, copy.ID)

	require.Len(t, copy.Changes, 1)
	c := copy.Changes[0]
	assert.Equal(t, handlers.OpRemoveLink, c.OpType)
	assert.Equal(t, handlers.StatePending, c.State) // reset from 'done'
	assert.Equal(t, copy.ID, c.PlanID)
	assert.JSONEq(t, `{"note":"x"}`, string(c.Payload))
}

func TestDuplicatePlan_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	id := uuid.New()
	req := planReq(t, http.MethodPost, "/api/topology/plans/"+id.String()+"/duplicate", nil, "z_"+uuid.New().String()[:8], acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.DuplicatePlan(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetPlan_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	id := uuid.New()
	req := planReq(t, http.MethodGet, "/api/topology/plans/"+id.String(), nil, "nf_"+uuid.New().String()[:8], acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.GetPlan(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestUpdatePlan_OptimisticVersion(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "upd_" + uuid.New().String()[:8]

	var id uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('p',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&id))

	// Correct version -> 200, version bumped to 2, status changed.
	body, _ := json.Marshal(handlers.UpdatePlanRequest{Status: strPtr("approved"), Version: 1})
	req := planReq(t, http.MethodPatch, "/api/topology/plans/"+id.String(), body, env, acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.UpdatePlan(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	var plan handlers.Plan
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&plan))
	assert.Equal(t, handlers.StatusApproved, plan.Status)
	assert.Equal(t, 2, plan.Version)

	// Transitioning to approved emits the specific plan.approve action.
	var approveEvents int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_events WHERE plan_id=$1 AND action='plan.approve'`,
		id).Scan(&approveEvents))
	assert.Equal(t, 1, approveEvents)

	// Stale version -> 409.
	body, _ = json.Marshal(handlers.UpdatePlanRequest{Name: strPtr("renamed"), Version: 1})
	req = planReq(t, http.MethodPatch, "/api/topology/plans/"+id.String(), body, env, acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr = httptest.NewRecorder()
	api.UpdatePlan(rr, req)
	assert.Equal(t, http.StatusConflict, rr.Code)
}

func TestUpdatePlan_NotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	id := uuid.New()
	body, _ := json.Marshal(handlers.UpdatePlanRequest{Name: strPtr("x"), Version: 1})
	req := planReq(t, http.MethodPatch, "/api/topology/plans/"+id.String(), body, "z_"+uuid.New().String()[:8], acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.UpdatePlan(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestUpdatePlan_InvalidStatus(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	id := uuid.New()
	body, _ := json.Marshal(handlers.UpdatePlanRequest{Status: strPtr("bogus"), Version: 1})
	req := planReq(t, http.MethodPatch, "/api/topology/plans/"+id.String(), body, "z_"+uuid.New().String()[:8], acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.UpdatePlan(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestDeletePlan_SoftDelete(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "del_" + uuid.New().String()[:8]

	var id uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('d',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&id))

	req := planReq(t, http.MethodDelete, "/api/topology/plans/"+id.String(), nil, env, acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.DeletePlan(rr, req)
	require.Equal(t, http.StatusNoContent, rr.Code)

	// Row still present but soft-deleted.
	var deletedAt *time.Time
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT deleted_at FROM topology_plans WHERE id=$1`, id).Scan(&deletedAt))
	assert.NotNil(t, deletedAt)

	// Second delete -> 404 (already gone).
	req = planReq(t, http.MethodDelete, "/api/topology/plans/"+id.String(), nil, env, acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr = httptest.NewRecorder()
	api.DeletePlan(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestAddPlanChange(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "addc_" + uuid.New().String()[:8]

	var planID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('c',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))

	add := func(op handlers.PlanOpType, ref map[string]any) *httptest.ResponseRecorder {
		b := map[string]any{"op_type": op}
		for k, v := range ref {
			b[k] = v
		}
		body, _ := json.Marshal(b)
		req := planReq(t, http.MethodPost, "/api/topology/plans/"+planID.String()+"/changes", body, env, acc)
		req = withChiURLParams(req, map[string]string{"id": planID.String()})
		rr := httptest.NewRecorder()
		api.AddPlanChange(rr, req)
		return rr
	}

	// First change: seq 10.
	rr := add(handlers.OpRemoveLink, map[string]any{"ref_link_pk": "l1"})
	require.Equal(t, http.StatusCreated, rr.Code)
	var c1 handlers.PlanChange
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&c1))
	assert.Equal(t, 10, c1.Seq)
	assert.Equal(t, handlers.StatePending, c1.State)
	assert.JSONEq(t, `{}`, string(c1.Payload))

	// Second change: seq 20.
	rr = add(handlers.OpRemoveDevice, map[string]any{"ref_device_pk": "d1"})
	require.Equal(t, http.StatusCreated, rr.Code)
	var c2 handlers.PlanChange
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&c2))
	assert.Equal(t, 20, c2.Seq)

	// Shape validation: remove_link without ref_link_pk -> 400.
	rr = add(handlers.OpRemoveLink, nil)
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Unknown op_type -> 400.
	rr = add(handlers.PlanOpType("teleport"), nil)
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Adding a change touches the plan (updated_by set).
	var updatedBy *string
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT updated_by_email FROM topology_plans WHERE id=$1`, planID).Scan(&updatedBy))
	require.NotNil(t, updatedBy)
	assert.Equal(t, "op@doublezero.xyz", *updatedBy)
}

func TestAddPlanChange_PlanNotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	id := uuid.New()
	body, _ := json.Marshal(handlers.AddChangeRequest{OpType: handlers.OpRemoveLink, RefLinkPK: strPtr("l1")})
	req := planReq(t, http.MethodPost, "/api/topology/plans/"+id.String()+"/changes", body, "z_"+uuid.New().String()[:8], acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.AddPlanChange(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestUpdatePlanChange_OptimisticVersion(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "updc_" + uuid.New().String()[:8]

	var planID, changeID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('c',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk)
		 VALUES ($1, 10, 'remove_link', 'l1') RETURNING id`, planID).Scan(&changeID))

	patch := func(body []byte) *httptest.ResponseRecorder {
		req := planReq(t, http.MethodPatch,
			"/api/topology/plans/"+planID.String()+"/changes/"+changeID.String(), body, env, acc)
		req = withChiURLParams(req, map[string]string{"id": planID.String(), "changeId": changeID.String()})
		rr := httptest.NewRecorder()
		api.UpdatePlanChange(rr, req)
		return rr
	}

	// Correct version -> 200; state + target_date updated, version -> 2.
	body, _ := json.Marshal(handlers.UpdateChangeRequest{
		State:      statePtr(handlers.StateDone),
		TargetDate: strPtr("2026-09-01"),
		Version:    1,
	})
	rr := patch(body)
	require.Equal(t, http.StatusOK, rr.Code)
	var c handlers.PlanChange
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&c))
	assert.Equal(t, handlers.StateDone, c.State)
	require.NotNil(t, c.TargetDate)
	assert.Equal(t, "2026-09-01", *c.TargetDate)
	assert.Equal(t, 2, c.Version)
	// Untouched field preserved.
	assert.Equal(t, "l1", c.RefLinkPK)

	// Reordering (seq change) emits the specific change.reorder action.
	seq := 30
	body, _ = json.Marshal(handlers.UpdateChangeRequest{Seq: &seq, Version: 2})
	rr = patch(body)
	require.Equal(t, http.StatusOK, rr.Code)
	var reorderEvents int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_events WHERE plan_id=$1 AND action='change.reorder'`,
		planID).Scan(&reorderEvents))
	assert.Equal(t, 1, reorderEvents)

	// Stale version -> 409.
	body, _ = json.Marshal(handlers.UpdateChangeRequest{State: statePtr(handlers.StateSkipped), Version: 1})
	rr = patch(body)
	assert.Equal(t, http.StatusConflict, rr.Code)
}

func statePtr(s handlers.PlanChangeState) *handlers.PlanChangeState { return &s }

func TestUpdatePlanChange_RejectsInvalidShape(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "updcshape_" + uuid.New().String()[:8]

	var planID, changeID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('c',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk)
		 VALUES ($1, 10, 'remove_link', 'l1') RETURNING id`, planID).Scan(&changeID))

	// Emptying the required anchor (ref_link_pk) on a remove_link -> 400.
	body, _ := json.Marshal(handlers.UpdateChangeRequest{RefLinkPK: strPtr(""), Version: 1})
	req := planReq(t, http.MethodPatch,
		"/api/topology/plans/"+planID.String()+"/changes/"+changeID.String(), body, env, acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String(), "changeId": changeID.String()})
	rr := httptest.NewRecorder()
	api.UpdatePlanChange(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Row is unchanged: ref_link_pk still 'l1', version still 1.
	var refLink string
	var version int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT ref_link_pk, version FROM topology_plan_changes WHERE id=$1`, changeID).
		Scan(&refLink, &version))
	assert.Equal(t, "l1", refLink)
	assert.Equal(t, 1, version)
}

func TestDeletePlanChange(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "delc_" + uuid.New().String()[:8]

	var planID, changeID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('c',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk)
		 VALUES ($1, 10, 'remove_link', 'l1') RETURNING id`, planID).Scan(&changeID))

	req := planReq(t, http.MethodDelete,
		"/api/topology/plans/"+planID.String()+"/changes/"+changeID.String(), nil, env, acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String(), "changeId": changeID.String()})
	rr := httptest.NewRecorder()
	api.DeletePlanChange(rr, req)
	require.Equal(t, http.StatusNoContent, rr.Code)

	var n int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_changes WHERE id=$1`, changeID).Scan(&n))
	assert.Equal(t, 0, n)

	// Second delete -> 404.
	req = planReq(t, http.MethodDelete,
		"/api/topology/plans/"+planID.String()+"/changes/"+changeID.String(), nil, env, acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String(), "changeId": changeID.String()})
	rr = httptest.NewRecorder()
	api.DeletePlanChange(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestReorderPlanChanges(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "reorder_" + uuid.New().String()[:8]

	var planID uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('r',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))

	var id1, id2, id3 uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk) VALUES ($1,10,'remove_link','l1') RETURNING id`,
		planID).Scan(&id1))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk) VALUES ($1,20,'remove_link','l2') RETURNING id`,
		planID).Scan(&id2))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk) VALUES ($1,30,'remove_link','l3') RETURNING id`,
		planID).Scan(&id3))

	// Reverse the order: PATCHing this one-at-a-time against the live seq
	// values collides with UNIQUE(plan_id, seq) (id3's new seq of 10 is still
	// held by id1 until id1 moves). The bulk endpoint must not 500.
	body, _ := json.Marshal(handlers.ReorderPlanChangesRequest{OrderedIDs: []uuid.UUID{id3, id2, id1}})
	req := planReq(t, http.MethodPost, "/api/topology/plans/"+planID.String()+"/changes/reorder", body, env, acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String()})
	rr := httptest.NewRecorder()
	api.ReorderPlanChanges(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var plan handlers.Plan
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&plan))
	require.Len(t, plan.Changes, 3)
	assert.Equal(t, id3, plan.Changes[0].ID)
	assert.Equal(t, 10, plan.Changes[0].Seq)
	assert.Equal(t, id2, plan.Changes[1].ID)
	assert.Equal(t, 20, plan.Changes[1].Seq)
	assert.Equal(t, id1, plan.Changes[2].ID)
	assert.Equal(t, 30, plan.Changes[2].Seq)

	// The reorder touches the plan and bumps its optimistic-concurrency version.
	assert.Equal(t, 2, plan.Version)

	var events int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_events WHERE plan_id=$1 AND action='change.reorder'`,
		planID).Scan(&events))
	assert.Equal(t, 1, events)
}

func TestReorderPlanChanges_MismatchedIDs(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	acc := newInternalAccount(t, ctx, api)
	env := "reorderbad_" + uuid.New().String()[:8]

	var planID, id1, id2 uuid.UUID
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plans (name, environment, created_by_account_id) VALUES ('r',$1,$2) RETURNING id`,
		env, acc.ID).Scan(&planID))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk) VALUES ($1,10,'remove_link','l1') RETURNING id`,
		planID).Scan(&id1))
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`INSERT INTO topology_plan_changes (plan_id, seq, op_type, ref_link_pk) VALUES ($1,20,'remove_link','l2') RETURNING id`,
		planID).Scan(&id2))

	// Missing id2, plus an id that doesn't belong to the plan at all.
	body, _ := json.Marshal(handlers.ReorderPlanChangesRequest{OrderedIDs: []uuid.UUID{id1, uuid.New()}})
	req := planReq(t, http.MethodPost, "/api/topology/plans/"+planID.String()+"/changes/reorder", body, env, acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String()})
	rr := httptest.NewRecorder()
	api.ReorderPlanChanges(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Nothing changed.
	var seq1, seq2 int
	require.NoError(t, api.PgPool.QueryRow(ctx, `SELECT seq FROM topology_plan_changes WHERE id=$1`, id1).Scan(&seq1))
	require.NoError(t, api.PgPool.QueryRow(ctx, `SELECT seq FROM topology_plan_changes WHERE id=$1`, id2).Scan(&seq2))
	assert.Equal(t, 10, seq1)
	assert.Equal(t, 20, seq2)
}

func TestReorderPlanChanges_PlanNotFound(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)
	id := uuid.New()
	body, _ := json.Marshal(handlers.ReorderPlanChangesRequest{OrderedIDs: []uuid.UUID{}})
	req := planReq(t, http.MethodPost, "/api/topology/plans/"+id.String()+"/changes/reorder", body, "z_"+uuid.New().String()[:8], acc)
	req = withChiURLParams(req, map[string]string{"id": id.String()})
	rr := httptest.NewRecorder()
	api.ReorderPlanChanges(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPlansRequireInternalDomain(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := t.Context()
	env := "gate_" + uuid.New().String()[:8]

	guarded := handlers.RequireInternalDomain(http.HandlerFunc(api.ListPlans))

	// Non-internal account -> 403.
	ext := createTestAccount(t, ctx, api) // wallet account, IsInternalUser=false
	req := planReq(t, http.MethodGet, "/api/topology/plans", nil, env, ext)
	rr := httptest.NewRecorder()
	guarded.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusForbidden, rr.Code)

	// Internal account -> 200.
	internal := newInternalAccount(t, ctx, api)
	req = planReq(t, http.MethodGet, "/api/topology/plans", nil, env, internal)
	rr = httptest.NewRecorder()
	guarded.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}
