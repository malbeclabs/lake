package handlers_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeGithubAPI implements handlers.GithubIssueAPI, counting create vs update calls
// and minting stable, monotonic issue numbers.
type fakeGithubAPI struct {
	repo        string
	createCalls int
	updateCalls int
	next        int
}

func (f *fakeGithubAPI) RepoName() string { return f.repo }

func (f *fakeGithubAPI) CreateIssue(_ context.Context, _, _ string, _ []string) (*handlers.GithubIssue, error) {
	f.createCalls++
	f.next++
	return &handlers.GithubIssue{
		Number:  f.next,
		HTMLURL: fmt.Sprintf("https://github.com/%s/issues/%d", f.repo, f.next),
	}, nil
}

func (f *fakeGithubAPI) UpdateIssue(_ context.Context, number int, _, _ string) (*handlers.GithubIssue, error) {
	f.updateCalls++
	return &handlers.GithubIssue{
		Number:  number,
		HTMLURL: fmt.Sprintf("https://github.com/%s/issues/%d", f.repo, number),
	}, nil
}

func insertApprovedPlan(t *testing.T, api *handlers.API, name string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	err := api.PgPool.QueryRow(context.Background(), `
		INSERT INTO topology_plans (name, environment, status)
		VALUES ($1, 'mainnet-beta', 'approved') RETURNING id`, name).Scan(&id)
	require.NoError(t, err)
	return id
}

// sampleActionList uses non-removal ops (add_link) deliberately: removals are
// now filed as their own per-entity decom issue and excluded from the
// per-contributor body (see buildPlannedIssues), so a group whose only task is
// a removal produces no contributor issue at all. These tests exercise the
// per-contributor issue create/update/skip lifecycle, so their fixture tasks
// must be non-removal ops to keep the contributor issue in play.
func sampleActionList(planID uuid.UUID) *handlers.ActionList {
	return &handlers.ActionList{
		PlanID:      planID.String(),
		Environment: "mainnet-beta",
		Groups: []handlers.ContributorActionGroup{
			{
				ContributorPK:   "contrib-a",
				ContributorCode: "rockawayx",
				SlackChannel:    "#ext-doublezero-rockawayx",
				Tasks: []handlers.ActionTask{
					{OpType: handlers.OpAddLink, Title: "Provision link X", State: handlers.StatePending},
				},
			},
			{
				ContributorPK:   "contrib-b",
				ContributorCode: "jump",
				SlackChannel:    "#ext-doublezero-jump",
				Tasks: []handlers.ActionTask{
					{OpType: handlers.OpAddLink, Title: "Provision link X (far end)", State: handlers.StatePending},
				},
			},
		},
	}
}

func TestPreviewPlanIssues(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := context.Background()

	planID := insertApprovedPlan(t, api, "Preview plan")
	plan := &handlers.Plan{ID: planID, Name: "Preview plan", Environment: "mainnet-beta", Status: "approved"}
	al := sampleActionList(planID)

	// Pre-existing issue for rockawayx -> classified "update"; jump -> "create".
	_, err := api.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_issues
			(id, plan_id, contributor_pk, contributor_code, github_repo, issue_number, issue_url, is_parent, last_synced_at, created_at)
		VALUES (gen_random_uuid(), $1, 'contrib-a', 'rockawayx', 'malbeclabs/infra', 99,
		        'https://github.com/malbeclabs/infra/issues/99', false, NOW(), NOW())`, planID)
	require.NoError(t, err)

	previews, err := api.PreviewPlanIssues(ctx, plan, al, nil, "malbeclabs/infra", true)
	require.NoError(t, err)
	require.Len(t, previews, 3) // 2 contributors + parent

	byKey := map[string]handlers.IssuePreview{}
	for _, p := range previews {
		key := p.ContributorCode
		if p.IsParent {
			key = "__parent__"
		}
		byKey[key] = p
	}
	assert.Equal(t, "update", byKey["rockawayx"].Action)
	require.NotNil(t, byKey["rockawayx"].ExistingIssueNumber)
	assert.Equal(t, 99, *byKey["rockawayx"].ExistingIssueNumber)
	assert.Equal(t, "create", byKey["jump"].Action)
	assert.Equal(t, "create", byKey["__parent__"].Action)
	assert.Contains(t, byKey["jump"].Body, "jump")
	assert.Contains(t, byKey["jump"].Title, "Preview plan")
}

func TestSyncPlanIssuesIdempotent(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := context.Background()

	planID := insertApprovedPlan(t, api, "Decom plan idempotent")
	plan := &handlers.Plan{ID: planID, Name: "Decom plan", Environment: "mainnet-beta", Status: "approved"}
	al := sampleActionList(planID)
	gh := &fakeGithubAPI{repo: "malbeclabs/infra"}

	// First sync: one issue per contributor + one parent, all created.
	first, err := api.SyncPlanIssues(ctx, gh, plan, al, nil, true)
	require.NoError(t, err)
	require.Len(t, first, 3)
	assert.Equal(t, 3, gh.createCalls)
	assert.Equal(t, 0, gh.updateCalls)
	for _, si := range first {
		assert.Equal(t, "created", si.Action)
	}

	var count int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&count))
	assert.Equal(t, 3, count)

	// Second sync: everything updated, nothing created, no new rows.
	second, err := api.SyncPlanIssues(ctx, gh, plan, al, nil, true)
	require.NoError(t, err)
	require.Len(t, second, 3)
	assert.Equal(t, 3, gh.createCalls) // unchanged
	assert.Equal(t, 3, gh.updateCalls) // one update per issue
	for _, si := range second {
		assert.Equal(t, "updated", si.Action)
	}

	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&count))
	assert.Equal(t, 3, count) // still 3 — idempotent, no duplicates

	// Issue numbers are stable across syncs.
	assert.Equal(t, first[0].IssueNumber, second[0].IssueNumber)
	assert.Equal(t, first[2].IssueNumber, second[2].IssueNumber) // parent
}

func TestSyncPlanIssuesCreatesOnlyNewContributor(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := context.Background()

	planID := insertApprovedPlan(t, api, "Growing plan")
	plan := &handlers.Plan{ID: planID, Name: "Growing plan", Environment: "mainnet-beta", Status: "approved"}
	gh := &fakeGithubAPI{repo: "malbeclabs/infra"}

	// First sync with 2 contributors (no parent this time).
	_, err := api.SyncPlanIssues(ctx, gh, plan, sampleActionList(planID), nil, false)
	require.NoError(t, err)
	assert.Equal(t, 2, gh.createCalls)

	// Second sync adds a third contributor: exactly one new create, two updates.
	// OpAddDevice (not a removal op): removals are filed as their own decom
	// issue and excluded from the per-contributor body, so a removal-only
	// group would get no contributor issue at all.
	al := sampleActionList(planID)
	al.Groups = append(al.Groups, handlers.ContributorActionGroup{
		ContributorPK:   "contrib-c",
		ContributorCode: "teleport",
		SlackChannel:    "#ext-doublezero-teleport",
		Tasks:           []handlers.ActionTask{{OpType: handlers.OpAddDevice, Title: "Bring device online", State: handlers.StatePending}},
	})
	res, err := api.SyncPlanIssues(ctx, gh, plan, al, nil, false)
	require.NoError(t, err)
	require.Len(t, res, 3)
	assert.Equal(t, 3, gh.createCalls) // +1 for teleport only
	assert.Equal(t, 2, gh.updateCalls) // rockawayx + jump

	var count int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&count))
	assert.Equal(t, 3, count)
}

// TestSyncPlanIssuesSkipsUnassignedContributors proves FIX 3: two distinct
// contributor groups that both resolve to an empty ContributorPK (e.g. two
// different drifted contributor codes that couldn't be mapped back to a pk)
// must not both try to upsert topology_plan_issues under the same
// UNIQUE(plan_id, contributor_pk) conflict target (empty, empty), which would
// clobber one group's issue with the other's. They are skipped cleanly
// instead: no GitHub call, no row written, and the assigned contributor's
// issue is unaffected.
func TestSyncPlanIssuesSkipsUnassignedContributors(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := context.Background()

	planID := insertApprovedPlan(t, api, "Unassigned plan")
	plan := &handlers.Plan{ID: planID, Name: "Unassigned plan", Environment: "mainnet-beta", Status: "approved"}
	// Tasks use OpAddLink (not a removal op): removals are filed as their own
	// decom issue and excluded from the per-contributor body, so a
	// removal-only group would get no contributor issue regardless of this
	// test's unassigned-contributor skip logic.
	al := &handlers.ActionList{
		PlanID:      planID.String(),
		Environment: "mainnet-beta",
		Groups: []handlers.ContributorActionGroup{
			{
				ContributorPK:   "", // unknown contributor A
				ContributorCode: "unknown-a",
				Tasks:           []handlers.ActionTask{{OpType: handlers.OpAddLink, Title: "Provision link A", State: handlers.StatePending}},
			},
			{
				ContributorPK:   "", // unknown contributor B: would collide with A on (plan_id, '') pre-fix
				ContributorCode: "unknown-b",
				Tasks: []handlers.ActionTask{
					{OpType: handlers.OpAddLink, Title: "Provision link B", State: handlers.StatePending},
					{OpType: handlers.OpAddLink, Title: "Provision link B far end", State: handlers.StatePending},
				},
			},
			{
				ContributorPK:   "contrib-a",
				ContributorCode: "rockawayx",
				Tasks:           []handlers.ActionTask{{OpType: handlers.OpAddLink, Title: "Provision link X", State: handlers.StatePending}},
			},
		},
	}
	gh := &fakeGithubAPI{repo: "malbeclabs/infra"}

	results, err := api.SyncPlanIssues(ctx, gh, plan, al, nil, false)
	require.NoError(t, err)
	require.Len(t, results, 1, "only the assigned contributor gets an issue")
	assert.Equal(t, "rockawayx", results[0].ContributorCode)
	assert.Equal(t, 1, gh.createCalls, "no GitHub call for either unassigned group")

	var count int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&count))
	assert.Equal(t, 1, count, "no row written for either unassigned group, and no clobber between them")

	// Preview reports the same skip behavior without touching GitHub at all.
	previews, err := api.PreviewPlanIssues(ctx, plan, al, nil, "malbeclabs/infra", false)
	require.NoError(t, err)
	require.Len(t, previews, 1, "only the assigned contributor is previewed")
	assert.Equal(t, "rockawayx", previews[0].ContributorCode)
}

// TestPostPlanIssuesPreviewEnvMismatch proves FIX 4: by-id plan lookup is not
// env-scoped, but this path (eventually) files real GitHub issues, so a plan
// from another environment must not be reachable through a request whose
// ambient env differs.
func TestPostPlanIssuesPreviewEnvMismatch(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "tok") // t.Setenv cannot be combined with t.Parallel
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)

	planID := insertApprovedPlan(t, api, "Env mismatch preview plan") // environment: mainnet-beta

	req := planReq(t, http.MethodPost, "/api/topology/plans/"+planID.String()+"/issues/preview", nil, "testnet", acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String()})
	rr := httptest.NewRecorder()
	api.PostPlanIssuesPreview(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
}

// TestPostPlanIssuesSyncEnvMismatch is the sync-handler counterpart of
// TestPostPlanIssuesPreviewEnvMismatch: the mismatch must be caught before any
// GitHub write, so the request must fail with no fake GitHub client involved.
func TestPostPlanIssuesSyncEnvMismatch(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "tok") // t.Setenv cannot be combined with t.Parallel
	api := apitesting.NewTestAPIPg(t, testPgDB)
	acc := newInternalAccount(t, t.Context(), api)

	planID := insertApprovedPlan(t, api, "Env mismatch sync plan") // environment: mainnet-beta

	req := planReq(t, http.MethodPost, "/api/topology/plans/"+planID.String()+"/issues/sync", nil, "devnet", acc)
	req = withChiURLParams(req, map[string]string{"id": planID.String()})
	rr := httptest.NewRecorder()
	api.PostPlanIssuesSync(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
}

// TestSyncPlanIssuesDecomDeviceIdempotent proves a pending remove_device
// change produces a per-entity device_decom issue (contributor_pk stays NULL;
// the row is keyed on (plan_id, issue_kind, entity_pk) instead), and that
// re-running the sync updates that same issue rather than duplicating it.
func TestSyncPlanIssuesDecomDeviceIdempotent(t *testing.T) {
	t.Parallel()
	api := apitesting.NewTestAPIPg(t, testPgDB)
	ctx := context.Background()

	planID := insertApprovedPlan(t, api, "Decom device plan")
	plan := &handlers.Plan{
		ID: planID, Name: "Decom device plan", Environment: "mainnet-beta", Status: "approved",
		Changes: []handlers.PlanChange{
			{Seq: 1, OpType: handlers.OpRemoveDevice, RefDevicePK: "dev-x-pk", State: handlers.StatePending},
		},
	}
	baseline := &handlers.TopologyResponse{
		Devices: []handlers.Device{
			{PK: "dev-x-pk", Code: "dev-x", ContributorPK: "contrib-a", ContributorCode: "rockawayx"},
		},
	}
	al := &handlers.ActionList{PlanID: planID.String(), Environment: "mainnet-beta"}
	gh := &fakeGithubAPI{repo: "malbeclabs/infra"}

	first, err := api.SyncPlanIssues(ctx, gh, plan, al, baseline, false)
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.Equal(t, "device_decom", first[0].Kind)
	assert.Equal(t, "dev-x-pk", first[0].EntityPK)
	assert.Equal(t, "created", first[0].Action)
	assert.Equal(t, 1, gh.createCalls)

	var count int
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&count))
	assert.Equal(t, 1, count)

	var kind, entityPK string
	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT issue_kind, entity_pk FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&kind, &entityPK))
	assert.Equal(t, "device_decom", kind)
	assert.Equal(t, "dev-x-pk", entityPK)

	// Re-running the sync updates the same issue instead of duplicating it.
	second, err := api.SyncPlanIssues(ctx, gh, plan, al, baseline, false)
	require.NoError(t, err)
	require.Len(t, second, 1)
	assert.Equal(t, "updated", second[0].Action)
	assert.Equal(t, first[0].IssueNumber, second[0].IssueNumber)
	assert.Equal(t, 1, gh.createCalls) // unchanged
	assert.Equal(t, 1, gh.updateCalls)

	require.NoError(t, api.PgPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM topology_plan_issues WHERE plan_id = $1`, planID).Scan(&count))
	assert.Equal(t, 1, count) // still 1, idempotent, no duplicates
}
