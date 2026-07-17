package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingDoer is a fake httpDoer that records each request and returns a canned response.
type recordingDoer struct {
	requests []*http.Request
	bodies   []string
	respond  func(req *http.Request) (*http.Response, error)
}

func (d *recordingDoer) Do(req *http.Request) (*http.Response, error) {
	var bodyStr string
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		bodyStr = string(b)
	}
	d.requests = append(d.requests, req)
	d.bodies = append(d.bodies, bodyStr)
	return d.respond(req)
}

func githubResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestGithubClientCreateIssue(t *testing.T) {
	t.Parallel()
	doer := &recordingDoer{
		respond: func(req *http.Request) (*http.Response, error) {
			return githubResp(http.StatusCreated,
				`{"number": 42, "html_url": "https://github.com/malbeclabs/infra/issues/42"}`), nil
		},
	}
	c := newGithubClientWithDoer(doer, "tok-123", "malbeclabs/infra")

	issue, err := c.CreateIssue(context.Background(), "Title", "Body text", []string{"topology-plan"})
	require.NoError(t, err)
	assert.Equal(t, 42, issue.Number)
	assert.Equal(t, "https://github.com/malbeclabs/infra/issues/42", issue.HTMLURL)

	require.Len(t, doer.requests, 1)
	req := doer.requests[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Equal(t, "https://api.github.com/repos/malbeclabs/infra/issues", req.URL.String())
	assert.Equal(t, "Bearer tok-123", req.Header.Get("Authorization"))
	assert.Contains(t, doer.bodies[0], `"title":"Title"`)
	assert.Contains(t, doer.bodies[0], `"topology-plan"`)
}

func TestGithubClientUpdateIssue(t *testing.T) {
	t.Parallel()
	doer := &recordingDoer{
		respond: func(req *http.Request) (*http.Response, error) {
			return githubResp(http.StatusOK,
				`{"number": 7, "html_url": "https://github.com/malbeclabs/infra/issues/7"}`), nil
		},
	}
	c := newGithubClientWithDoer(doer, "tok", "malbeclabs/infra")

	issue, err := c.UpdateIssue(context.Background(), 7, "New title", "New body")
	require.NoError(t, err)
	assert.Equal(t, 7, issue.Number)

	require.Len(t, doer.requests, 1)
	assert.Equal(t, http.MethodPatch, doer.requests[0].Method)
	assert.Equal(t, "https://api.github.com/repos/malbeclabs/infra/issues/7", doer.requests[0].URL.String())
}

func TestGithubClientErrorStatus(t *testing.T) {
	t.Parallel()
	doer := &recordingDoer{
		respond: func(req *http.Request) (*http.Response, error) {
			return githubResp(http.StatusUnauthorized, `{"message":"Bad credentials"}`), nil
		},
	}
	c := newGithubClientWithDoer(doer, "bad", "malbeclabs/infra")
	_, err := c.CreateIssue(context.Background(), "t", "b", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestNewGithubClientRequiresToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	_, err := newGithubClient()
	require.ErrorIs(t, err, errGithubNotConfigured)
}

func TestNewGithubClientDefaultsRepo(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "tok")
	t.Setenv("INFRA_ISSUES_REPO", "")
	c, err := newGithubClient()
	require.NoError(t, err)
	assert.Equal(t, "malbeclabs/infra", c.RepoName())
}

func TestIssueTitleAndLabels(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "[topology-plan] My Plan: rockawayx", issueTitle("My Plan", "rockawayx"))
	assert.Equal(t, "[topology-plan] My Plan: tracking", parentIssueTitle("My Plan"))
	assert.ElementsMatch(t, []string{"topology-plan", "plan:My Plan"}, issueLabels("My Plan"))
}

func TestRenderContributorIssueBody(t *testing.T) {
	t.Parallel()
	date := "2026-08-01"
	plan := &Plan{ID: uuid.New(), Name: "Decom sea-dz01", Environment: "mainnet-beta", Status: "approved"}
	ca := ContributorActionGroup{
		ContributorPK:   "contrib-a",
		ContributorCode: "rockawayx",
		SlackChannel:    "#ext-doublezero-rockawayx",
		Tasks: []ActionTask{
			{
				OpType:               OpMoveLinkEnd,
				Title:                "Move DZX link sea-dzx1 to device sea-dz02",
				State:                StatePending,
				TargetDate:           &date,
				Note:                 "OPS-1234",
				InvolvedContributors: []string{"rockawayx", "jump"},
			},
		},
	}
	// Populate Markdown exactly as deriveActionList does in production, so the
	// issue body reuses the sanitized, checkbox-correct per-contributor markdown.
	ca.Markdown = renderGroupMarkdown(&ca)
	body := renderContributorIssueBody(plan, ca, "https://data.malbeclabs.com/topology/planner?plan=x")
	assert.Contains(t, body, "Decom sea-dz01")
	assert.Contains(t, body, "rockawayx")
	assert.Contains(t, body, "Move DZX link sea-dzx1")
	assert.Contains(t, body, "Target date: 2026-08-01")
	assert.Contains(t, body, "Coordinate with: rockawayx, jump")
	assert.Contains(t, body, "Note: OPS-1234")
	assert.Contains(t, body, "#ext-doublezero-rockawayx")
	assert.Contains(t, body, "topology/planner?plan=x")
}

// TestRenderContributorIssueBodySanitizesAndChecksDone proves the issue body
// reuses the hardened Phase 4 markdown: a note with a raw newline and a backtick
// cannot break the list/fence structure, and a done task shows a checked box.
func TestRenderContributorIssueBodySanitizesAndChecksDone(t *testing.T) {
	t.Parallel()
	plan := &Plan{ID: uuid.New(), Name: "Decom sea-dz01", Environment: "mainnet-beta", Status: "approved"}
	ca := ContributorActionGroup{
		ContributorPK:   "contrib-a",
		ContributorCode: "rockawayx",
		SlackChannel:    "#ext-doublezero-rockawayx",
		Tasks: []ActionTask{
			{
				OpType: OpMoveLinkEnd,
				Title:  "Move link",
				State:  StateDone,
				Note:   "line one\nline two `code`",
			},
		},
	}
	ca.Markdown = renderGroupMarkdown(&ca)
	body := renderContributorIssueBody(plan, ca, "https://data.malbeclabs.com/topology/planner?plan=x")

	// Done task renders a checked checkbox.
	assert.Contains(t, body, "- [x]")
	assert.NotContains(t, body, "- [ ]")
	// The raw newline in the note is collapsed to a space: the note stays on one
	// list line, so the list structure is not broken.
	assert.Contains(t, body, "Note: line one line two")
	assert.NotContains(t, body, "line one\nline two")
	// The backtick is escaped so it cannot open a stray code fence.
	assert.Contains(t, body, "\\`code\\`")
	assert.NotContains(t, body, "two `code`")
	// Exactly one task list item remains.
	assert.Equal(t, 1, strings.Count(body, "- [x]"))
}

func TestRenderParentIssueBody(t *testing.T) {
	t.Parallel()
	plan := &Plan{ID: uuid.New(), Name: "Decom sea-dz01", Environment: "mainnet-beta", Status: "approved"}
	children := []SyncedIssue{
		{IsParent: true, ContributorCode: "tracking", IssueURL: "https://github.com/malbeclabs/infra/issues/1"},
		{ContributorCode: "rockawayx", IssueURL: "https://github.com/malbeclabs/infra/issues/2"},
		// A malicious contributor code with a newline must not break the list.
		{ContributorCode: "jump\nevil", IssueURL: "https://github.com/malbeclabs/infra/issues/3"},
	}
	body := renderParentIssueBody(plan, children, "https://data.malbeclabs.com/topology/planner?plan=x")

	assert.Contains(t, body, "Decom sea-dz01")
	assert.Contains(t, body, "rockawayx: https://github.com/malbeclabs/infra/issues/2")
	// Parent row is skipped (IsParent).
	assert.NotContains(t, body, "issues/1")
	// The newline in the contributor code is collapsed, keeping one list line.
	assert.Contains(t, body, "jump evil")
	assert.NotContains(t, body, "jump\nevil")
	assert.Contains(t, body, "topology/planner?plan=x")
}

// TestUnassignedTaskCountIssues proves unassignedTaskCount only counts the
// NON-removal tasks in contributor-less groups: a removal task in a
// contributor-less group is excluded (it becomes a per-entity decom issue
// instead), and tasks in a group with a resolved contributor_pk are excluded
// entirely.
func TestUnassignedTaskCountIssues(t *testing.T) {
	t.Parallel()
	al := &ActionList{
		Groups: []ContributorActionGroup{
			{
				ContributorPK: "",
				Tasks: []ActionTask{
					{OpType: OpRemoveLink, Title: "Remove link"},
					{OpType: OpAddLink, Title: "Add link"},
				},
			},
			{
				ContributorPK:   "contrib-a",
				ContributorCode: "rockawayx",
				Tasks: []ActionTask{
					{OpType: OpAddDevice, Title: "Add device"},
				},
			},
		},
	}
	assert.Equal(t, 1, unassignedTaskCount(al))
}

func TestPlannerPlanURLDefault(t *testing.T) {
	t.Setenv("APP_BASE_URL", "")
	assert.Equal(t, "https://data.malbeclabs.com/topology/planner?plan=abc", plannerPlanURL("abc"))
}

func TestPostPlanIssuesSyncRequiresInternalUser(t *testing.T) {
	t.Parallel()
	api := &API{}
	req := httptest.NewRequest(http.MethodPost, "/api/topology/plans/abc/issues/sync", nil)
	w := httptest.NewRecorder()
	api.PostPlanIssuesSync(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestPostPlanIssuesSyncRequiresToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	api := &API{}
	acct := &Account{AccountType: AccountTypeDomain, IsInternalUser: true, Email: strPtr("t@doublezero.xyz")}
	req := httptest.NewRequest(http.MethodPost, "/api/topology/plans/abc/issues/sync", nil)
	req = req.WithContext(SetAccountInContext(req.Context(), acct))
	w := httptest.NewRecorder()
	api.PostPlanIssuesSync(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestPostPlanIssuesPreviewRequiresToken(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "")
	api := &API{}
	acct := &Account{AccountType: AccountTypeDomain, IsInternalUser: true, Email: strPtr("t@doublezero.xyz")}
	req := httptest.NewRequest(http.MethodPost, "/api/topology/plans/abc/issues/preview", nil)
	req = req.WithContext(SetAccountInContext(req.Context(), acct))
	w := httptest.NewRecorder()
	api.PostPlanIssuesPreview(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
