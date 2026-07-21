package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

const (
	githubAPIBaseURL  = "https://api.github.com"
	defaultInfraRepo  = "malbeclabs/infra"
	topologyPlanLabel = "topology-plan"

	plannerBaseURLEnv  = "APP_BASE_URL"
	defaultPlannerBase = "https://data.malbeclabs.com"
)

// errGithubNotConfigured is returned when the GitHub token env var is unset,
// which feature-gates the issue endpoints off.
var errGithubNotConfigured = errors.New("github integration not configured: set GITHUB_TOKEN")

// httpDoer is the subset of *http.Client the GitHub client needs, so tests inject a fake.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// GithubIssue is the minimal issue result returned by the GitHub REST API.
type GithubIssue struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
}

// GithubIssueAPI is the seam the sync engine calls, so tests can supply a fake.
type GithubIssueAPI interface {
	CreateIssue(ctx context.Context, title, body string, labels []string) (*GithubIssue, error)
	UpdateIssue(ctx context.Context, number int, title, body string) (*GithubIssue, error)
	RepoName() string
}

// githubClient talks to the GitHub REST API with a server-side token.
type githubClient struct {
	httpClient httpDoer
	token      string
	baseURL    string
	repo       string // "owner/repo"
}

// newGithubClient builds a client from env. Feature-gated on GITHUB_TOKEN;
// returns errGithubNotConfigured when the token is unset. Target repo is
// configurable via INFRA_ISSUES_REPO and defaults to malbeclabs/infra.
func newGithubClient() (*githubClient, error) {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return nil, errGithubNotConfigured
	}
	repo := os.Getenv("INFRA_ISSUES_REPO")
	if repo == "" {
		repo = defaultInfraRepo
	}
	return newGithubClientWithDoer(&http.Client{Timeout: 15 * time.Second}, token, repo), nil
}

// newGithubClientWithDoer builds a client with an injected HTTP doer (for tests).
func newGithubClientWithDoer(doer httpDoer, token, repo string) *githubClient {
	return &githubClient{
		httpClient: doer,
		token:      token,
		baseURL:    githubAPIBaseURL,
		repo:       repo,
	}
}

func (c *githubClient) RepoName() string { return c.repo }

func (c *githubClient) do(ctx context.Context, method, path string, payload any) (*GithubIssue, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("github returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var issue GithubIssue
	if err := json.Unmarshal(body, &issue); err != nil {
		return nil, fmt.Errorf("decode issue: %w", err)
	}
	return &issue, nil
}

// CreateIssue creates a new issue in the configured repo.
func (c *githubClient) CreateIssue(ctx context.Context, title, body string, labels []string) (*GithubIssue, error) {
	return c.do(ctx, http.MethodPost, "/repos/"+c.repo+"/issues", map[string]any{
		"title":  title,
		"body":   body,
		"labels": labels,
	})
}

// UpdateIssue updates an existing issue's title and body.
func (c *githubClient) UpdateIssue(ctx context.Context, number int, title, body string) (*GithubIssue, error) {
	return c.do(ctx, http.MethodPatch, fmt.Sprintf("/repos/%s/issues/%d", c.repo, number), map[string]any{
		"title": title,
		"body":  body,
	})
}

// issueTitle builds the per-contributor issue title. A colon separator is used
// deliberately (no em dash) so titles are copy-paste clean.
func issueTitle(planName, contributorCode string) string {
	return fmt.Sprintf("[%s] %s: %s", topologyPlanLabel, planName, contributorCode)
}

func parentIssueTitle(planName string) string {
	return fmt.Sprintf("[%s] %s: tracking", topologyPlanLabel, planName)
}

// issueLabels returns the discoverability labels: the topology-plan marker plus
// a per-plan label (GitHub auto-creates unknown labels on issue create).
func issueLabels(planName string) []string {
	return []string{topologyPlanLabel, "plan:" + planName}
}

// plannerPlanURL builds a deep link back to the plan in the planner UI.
func plannerPlanURL(planID string) string {
	base := os.Getenv(plannerBaseURLEnv)
	if base == "" {
		base = defaultPlannerBase
	}
	return fmt.Sprintf("%s/topology/planner?plan=%s", strings.TrimRight(base, "/"), planID)
}

// renderContributorIssueBody renders one contributor's action-list group as a
// GitHub issue body. It reuses the already-sanitized, checkbox-correct
// per-contributor markdown produced by Phase 4 (ContributorActionGroup.Markdown,
// SC-6, via renderGroupMarkdown) as the task list, so free-text fields
// (Title/Note/TargetDate/coordination) cannot corrupt the issue body on sync and
// a done task renders a checked box. The only extra content is a short
// plan-context header; every user-supplied value it interpolates is run through
// the same sanitizeInline helper the action-list renderer uses, so the header
// cannot be corrupted either. The body is fully regenerated on every sync, so it
// is the single source of truth (the sync overwrites it).
func renderContributorIssueBody(plan *Plan, ca ContributorActionGroup, planURL string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Plan: **%s** (`%s`)\n\n", sanitizeInline(plan.Name), sanitizeInline(plan.Environment))
	if ca.SlackChannel != "" {
		fmt.Fprintf(&b, "Coordination channel: %s\n\n", sanitizeChannel(ca.SlackChannel))
	}
	// Reuse the Phase 4 per-contributor markdown verbatim (sanitized at source).
	b.WriteString(ca.Markdown)
	if !strings.HasSuffix(ca.Markdown, "\n") {
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "\nPlan: %s\n", planURL)
	b.WriteString("\n_Generated by the DoubleZero Data topology planner; the body is overwritten on each sync._\n")
	return b.String()
}

// sanitizeChannel neutralizes a Slack channel string for inline markdown while
// preserving its leading '#' (sanitizeInline would otherwise escape a leading
// '#' as a heading marker). Only the code portion after the '#' is user-derived.
func sanitizeChannel(ch string) string {
	if strings.HasPrefix(ch, "#") {
		return "#" + sanitizeInline(ch[1:])
	}
	return sanitizeInline(ch)
}

// unassignedGroup reports whether a contributor group has no resolvable
// contributor pk. An issue cannot be routed to an unknown contributor, and
// two such groups (e.g. distinct contributor codes each resolved from a
// drifted ref_snapshot) would otherwise both write an empty contributor_pk and
// collide on UNIQUE(plan_id, contributor_pk) (ON CONFLICT would clobber one
// with the other's issue). Skip these groups instead.
func unassignedGroup(ca ContributorActionGroup) bool { return ca.ContributorPK == "" }

// unassignedTaskCount sums the NON-removal tasks in every contributor-less
// group, so the preview/sync response can surface how many tasks have no
// issue to land in. A contributor-less group's removal tasks are excluded:
// removals always become a per-entity decom issue regardless of contributor,
// so they are not actually unfiled.
func unassignedTaskCount(al *ActionList) int {
	n := 0
	for _, ca := range al.Groups {
		if !unassignedGroup(ca) {
			continue
		}
		for _, t := range ca.Tasks {
			if t.OpType != OpRemoveDevice && t.OpType != OpRemoveLink {
				n++
			}
		}
	}
	return n
}

// issueKind tags each non-parent issue a sync produces, so the preview/sync
// loop can treat per-entity decom issues and per-contributor issues
// uniformly.
type issueKind string

const (
	kindContributor issueKind = "contributor"
	kindDeviceDecom issueKind = "device_decom"
	kindLinkDecom   issueKind = "link_decom"
)

// decomIssueLabels mirrors the private infra repo's contributor-decommission
// label, plus a per-plan marker for discoverability.
func decomIssueLabels(planName string) []string {
	return []string{"contributor-decommission", "plan:" + planName}
}

// plannedIssue is one non-parent issue a sync will create/update, kind-tagged
// so the preview/sync loop treats decom (per-entity) and contributor issues
// uniformly.
type plannedIssue struct {
	Kind            issueKind
	EntityPK        string // device/link pk for decom kinds; "" for contributor
	ContributorPK   string // set for contributor kind; "" for decom
	ContributorCode string // display / routing
	Title           string
	Body            string
	Labels          []string
}

// buildPlannedIssues turns a plan + its action list + the live baseline into
// the set of non-parent issues a sync should file: one decom issue per
// pending remove_device / remove_link (per-entity, contributor-decommission
// label), plus one per-contributor issue per contributor that has NON-removal
// work (removals are covered by the decom issues, so they are filtered out of
// the contributor bodies). Order: device decoms, link decoms, then
// contributor groups (as ordered in al.Groups). Pure and DB-free so it is
// unit-testable.
func buildPlannedIssues(plan *Plan, al *ActionList, baseline *TopologyResponse, planURL string) []plannedIssue {
	var out []plannedIssue

	devTargets, linkTargets := collectDecomTargets(plan, baseline)
	for _, t := range devTargets {
		if t.DevicePK == "" {
			continue // defensive; removals always carry a ref pk
		}
		out = append(out, plannedIssue{
			Kind:            kindDeviceDecom,
			EntityPK:        t.DevicePK,
			ContributorCode: t.ContributorCode,
			Title:           renderDeviceDecomTitle(t),
			Body:            renderDeviceDecomBody(t),
			Labels:          decomIssueLabels(plan.Name),
		})
	}
	for _, t := range linkTargets {
		if t.LinkPK == "" {
			continue // defensive; removals always carry a ref pk
		}
		out = append(out, plannedIssue{
			Kind:            kindLinkDecom,
			EntityPK:        t.LinkPK,
			ContributorCode: t.OwnerContribCode,
			Title:           renderLinkDecomTitle(t),
			Body:            renderLinkDecomBody(t),
			Labels:          decomIssueLabels(plan.Name),
		})
	}

	for _, ca := range al.Groups {
		if unassignedGroup(ca) {
			continue // FIX 3: can't route an issue to an unknown contributor
		}
		var kept []ActionTask
		for _, t := range ca.Tasks {
			if t.OpType == OpRemoveDevice || t.OpType == OpRemoveLink {
				continue // covered by the per-entity decom issue instead
			}
			kept = append(kept, t)
		}
		if len(kept) == 0 {
			continue // this contributor's only work is removals: no per-contributor issue
		}
		filtered := ca
		filtered.Tasks = kept
		filtered.Markdown = renderGroupMarkdown(&filtered)
		out = append(out, plannedIssue{
			Kind:            kindContributor,
			ContributorPK:   ca.ContributorPK,
			ContributorCode: ca.ContributorCode,
			Title:           issueTitle(plan.Name, ca.ContributorCode),
			Body:            renderContributorIssueBody(plan, filtered, planURL),
			Labels:          issueLabels(plan.Name),
		})
	}

	return out
}

// SyncedIssue is a single issue result after a sync ran. It is declared here
// (not in Task 4) because renderParentIssueBody and the Task 3 preview both
// reference it; Task 4's sync engine reuses this same type. Kind/EntityPK/Title
// let a decom issue (which has no contributor code) be labeled and routed the
// same way a contributor issue is.
type SyncedIssue struct {
	Kind            string `json:"kind"`
	ContributorPK   string `json:"contributor_pk"`
	ContributorCode string `json:"contributor_code"`
	IsParent        bool   `json:"is_parent"`
	Action          string `json:"action"` // "created" | "updated"
	Title           string `json:"title"`
	EntityPK        string `json:"entity_pk,omitempty"`
	IssueNumber     int    `json:"issue_number"`
	IssueURL        string `json:"issue_url"`
	Repo            string `json:"repo"`
}

// renderParentIssueBody renders the optional single tracking issue that links
// every child issue. A decom child has no contributor code, so it is labeled
// by its own Title instead.
func renderParentIssueBody(plan *Plan, children []SyncedIssue, planURL string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Tracking issue for topology plan **%s** (`%s`).\n\n", sanitizeInline(plan.Name), sanitizeInline(plan.Environment))
	b.WriteString("Issues:\n\n")
	for _, ch := range children {
		if ch.IsParent {
			continue
		}
		label := ch.ContributorCode
		if ch.Kind != string(kindContributor) {
			label = ch.Title
		}
		fmt.Fprintf(&b, "- [ ] %s: %s\n", sanitizeInline(label), ch.IssueURL)
	}
	fmt.Fprintf(&b, "\nPlan: %s\n", planURL)
	return b.String()
}

// IssuePreview describes a single issue that a sync would create or update.
type IssuePreview struct {
	Kind                string   `json:"kind"`
	ContributorPK       string   `json:"contributor_pk"`
	ContributorCode     string   `json:"contributor_code"`
	IsParent            bool     `json:"is_parent"`
	Action              string   `json:"action"` // "create" | "update"
	Title               string   `json:"title"`
	Body                string   `json:"body"`
	Labels              []string `json:"labels"`
	EntityPK            string   `json:"entity_pk,omitempty"`
	ExistingIssueNumber *int     `json:"existing_issue_number,omitempty"`
	ExistingIssueURL    string   `json:"existing_issue_url,omitempty"`
	Repo                string   `json:"repo"`
}

// IssuesPreviewResponse is the /issues/preview response envelope.
type IssuesPreviewResponse struct {
	Repo   string         `json:"repo"`
	Issues []IssuePreview `json:"issues"`
	// UnassignedTaskCount is the number of tasks belonging to contributor
	// groups with no resolvable contributor_pk (FIX 3): these are skipped
	// rather than filed, since an issue cannot be routed to an unknown
	// contributor.
	UnassignedTaskCount int `json:"unassigned_task_count,omitempty"`
}

type existingIssue struct {
	number   int
	url      string
	isParent bool
}

// loadExistingIssues returns recorded issues for a plan: a map keyed by
// contributor_pk for per-contributor issues, a map keyed by
// "<issue_kind>|<entity_pk>" for per-entity decom issues, and the parent issue
// separately. Parent rows carry a NULL contributor_pk (which the UNIQUE index
// treats as distinct), so they are keyed on is_parent, not on either map.
func (a *API) loadExistingIssues(ctx context.Context, planID uuid.UUID) (
	byContrib map[string]existingIssue, byEntity map[string]existingIssue, parent *existingIssue, err error) {
	if a.PgPool == nil {
		return nil, nil, nil, errNoPgPool
	}
	rows, err := a.PgPool.Query(ctx, `
		SELECT contributor_pk, issue_number, issue_url, is_parent, issue_kind, entity_pk
		FROM topology_plan_issues
		WHERE plan_id = $1`, planID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("query existing issues: %w", err)
	}
	defer rows.Close()

	byContrib = make(map[string]existingIssue)
	byEntity = make(map[string]existingIssue)
	for rows.Next() {
		var (
			contribPK *string
			number    int
			url       string
			isParent  bool
			kind      string
			entityPK  *string
		)
		if err := rows.Scan(&contribPK, &number, &url, &isParent, &kind, &entityPK); err != nil {
			return nil, nil, nil, fmt.Errorf("scan existing issue: %w", err)
		}
		ei := existingIssue{number: number, url: url, isParent: isParent}
		switch {
		case isParent || (contribPK == nil && entityPK == nil):
			p := ei
			parent = &p
		case entityPK != nil:
			byEntity[kind+"|"+*entityPK] = ei
		default:
			byContrib[*contribPK] = ei
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("iterate existing issues: %w", err)
	}
	return byContrib, byEntity, parent, nil
}

// PreviewPlanIssues computes the issues a sync would create or update, without
// touching GitHub. It classifies each planned issue as create/update by
// consulting topology_plan_issues: contributor issues by contributor_pk, decom
// issues by (kind, entity_pk).
func (a *API) PreviewPlanIssues(ctx context.Context, plan *Plan, al *ActionList, baseline *TopologyResponse, repo string, includeParent bool) ([]IssuePreview, error) {
	byContrib, byEntity, parent, err := a.loadExistingIssues(ctx, plan.ID)
	if err != nil {
		return nil, err
	}
	planURL := plannerPlanURL(plan.ID.String())
	specs := buildPlannedIssues(plan, al, baseline, planURL)
	previews := make([]IssuePreview, 0, len(specs)+1)
	for _, spec := range specs {
		p := IssuePreview{
			Kind:            string(spec.Kind),
			EntityPK:        spec.EntityPK,
			ContributorPK:   spec.ContributorPK,
			ContributorCode: spec.ContributorCode,
			Title:           spec.Title,
			Body:            spec.Body,
			Labels:          spec.Labels,
			Repo:            repo,
			Action:          "create",
		}
		existing, ok := lookupExistingIssue(spec, byContrib, byEntity)
		if ok {
			p.Action = "update"
			n := existing.number
			p.ExistingIssueNumber = &n
			p.ExistingIssueURL = existing.url
		}
		previews = append(previews, p)
	}
	if includeParent {
		pp := IssuePreview{
			Kind:     "parent",
			IsParent: true,
			Title:    parentIssueTitle(plan.Name),
			Body:     renderParentIssueBody(plan, nil, planURL),
			Labels:   issueLabels(plan.Name),
			Repo:     repo,
			Action:   "create",
		}
		if parent != nil {
			pp.Action = "update"
			n := parent.number
			pp.ExistingIssueNumber = &n
			pp.ExistingIssueURL = parent.url
		}
		previews = append(previews, pp)
	}
	return previews, nil
}

// lookupExistingIssue finds the recorded issue for a planned issue spec:
// contributor issues by contributor_pk, decom issues by "<kind>|<entity_pk>".
func lookupExistingIssue(spec plannedIssue, byContrib, byEntity map[string]existingIssue) (existingIssue, bool) {
	if spec.Kind == kindContributor {
		ei, ok := byContrib[spec.ContributorPK]
		return ei, ok
	}
	ei, ok := byEntity[string(spec.Kind)+"|"+spec.EntityPK]
	return ei, ok
}

// IssuesSyncResponse is the /issues/sync response envelope.
type IssuesSyncResponse struct {
	Repo   string        `json:"repo"`
	Issues []SyncedIssue `json:"issues"`
	// UnassignedTaskCount is the number of tasks belonging to contributor
	// groups with no resolvable contributor_pk (FIX 3): these are skipped
	// rather than filed, since an issue cannot be routed to an unknown
	// contributor.
	UnassignedTaskCount int `json:"unassigned_task_count,omitempty"`
}

// SyncPlanIssues creates issues for newly-planned entities/contributors,
// updates the bodies of existing ones, and records each in
// topology_plan_issues. It is idempotent: re-running only updates and never
// duplicates (contributor issues on contributor_pk, decom issues on
// (issue_kind, entity_pk)). When includeParent is set, a single tracking issue
// linking all child issues is created/updated last (so it can reference their
// numbers).
func (a *API) SyncPlanIssues(ctx context.Context, gh GithubIssueAPI, plan *Plan, al *ActionList, baseline *TopologyResponse, includeParent bool) ([]SyncedIssue, error) {
	if a.PgPool == nil {
		return nil, errNoPgPool
	}
	byContrib, byEntity, parent, err := a.loadExistingIssues(ctx, plan.ID)
	if err != nil {
		return nil, err
	}
	planURL := plannerPlanURL(plan.ID.String())
	specs := buildPlannedIssues(plan, al, baseline, planURL)
	results := make([]SyncedIssue, 0, len(specs)+1)

	for _, spec := range specs {
		existing, ok := lookupExistingIssue(spec, byContrib, byEntity)

		var (
			issue  *GithubIssue
			action string
		)
		if ok {
			issue, err = gh.UpdateIssue(ctx, existing.number, spec.Title, spec.Body)
			action = "updated"
		} else {
			issue, err = gh.CreateIssue(ctx, spec.Title, spec.Body, spec.Labels)
			action = "created"
		}
		if err != nil {
			return nil, fmt.Errorf("sync issue %q: %w", spec.Title, err)
		}

		if spec.Kind == kindContributor {
			if err := a.upsertPlanIssue(ctx, plan.ID, spec.ContributorPK, spec.ContributorCode, gh.RepoName(), issue); err != nil {
				return nil, err
			}
		} else {
			if err := a.upsertDecomPlanIssue(ctx, plan.ID, spec.Kind, spec.EntityPK, spec.ContributorCode, gh.RepoName(), issue); err != nil {
				return nil, err
			}
		}

		results = append(results, SyncedIssue{
			Kind:            string(spec.Kind),
			EntityPK:        spec.EntityPK,
			Title:           spec.Title,
			ContributorPK:   spec.ContributorPK,
			ContributorCode: spec.ContributorCode,
			Action:          action,
			IssueNumber:     issue.Number,
			IssueURL:        issue.HTMLURL,
			Repo:            gh.RepoName(),
		})
	}

	if includeParent {
		title := parentIssueTitle(plan.Name)
		body := renderParentIssueBody(plan, results, planURL)
		var (
			issue  *GithubIssue
			action string
		)
		if parent != nil {
			issue, err = gh.UpdateIssue(ctx, parent.number, title, body)
			action = "updated"
		} else {
			issue, err = gh.CreateIssue(ctx, title, body, issueLabels(plan.Name))
			action = "created"
		}
		if err != nil {
			return nil, fmt.Errorf("sync parent issue: %w", err)
		}
		if err := a.upsertParentPlanIssue(ctx, plan.ID, gh.RepoName(), issue); err != nil {
			return nil, err
		}
		results = append(results, SyncedIssue{
			Kind:        "parent",
			IsParent:    true,
			Title:       title,
			Action:      action,
			IssueNumber: issue.Number,
			IssueURL:    issue.HTMLURL,
			Repo:        gh.RepoName(),
		})
	}
	return results, nil
}

// upsertPlanIssue inserts or updates the per-contributor tracking row. The
// UNIQUE(plan_id, contributor_pk) index makes this idempotent for a real
// contributor_pk. An empty contributor_pk is refused outright (defense in
// depth alongside the unassignedGroup skip in SyncPlanIssues): two groups
// with an empty contributor_pk would otherwise both target (plan_id, empty) and
// ON CONFLICT would clobber one with the other's issue.
func (a *API) upsertPlanIssue(ctx context.Context, planID uuid.UUID, contributorPK, contributorCode, repo string, issue *GithubIssue) error {
	if contributorPK == "" {
		return fmt.Errorf("upsert plan issue: refusing to upsert with an empty contributor_pk (would collide with another unassigned group)")
	}
	_, err := a.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_issues
			(id, plan_id, contributor_pk, contributor_code, github_repo, issue_number, issue_url, is_parent, last_synced_at, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, false, NOW(), NOW())
		ON CONFLICT (plan_id, contributor_pk)
		DO UPDATE SET
			contributor_code = EXCLUDED.contributor_code,
			github_repo      = EXCLUDED.github_repo,
			issue_number     = EXCLUDED.issue_number,
			issue_url        = EXCLUDED.issue_url,
			last_synced_at   = NOW()`,
		planID, contributorPK, contributorCode, repo, issue.Number, issue.HTMLURL)
	if err != nil {
		return fmt.Errorf("upsert plan issue: %w", err)
	}
	return nil
}

// upsertDecomPlanIssue records a per-entity decom issue, idempotent on
// (plan_id, issue_kind, entity_pk). contributor_pk stays NULL (decom rows are
// keyed by entity, not contributor, and a contributor can own several decom'd
// entities, which would collide on UNIQUE(plan_id, contributor_pk)).
func (a *API) upsertDecomPlanIssue(ctx context.Context, planID uuid.UUID, kind issueKind, entityPK, contributorCode, repo string, issue *GithubIssue) error {
	_, err := a.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_issues
			(id, plan_id, contributor_pk, contributor_code, github_repo, issue_number, issue_url, is_parent, issue_kind, entity_pk, last_synced_at, created_at)
		VALUES (gen_random_uuid(), $1, NULL, $2, $3, $4, $5, false, $6, $7, NOW(), NOW())
		ON CONFLICT (plan_id, issue_kind, entity_pk) WHERE entity_pk IS NOT NULL
		DO UPDATE SET
			contributor_code = EXCLUDED.contributor_code,
			github_repo      = EXCLUDED.github_repo,
			issue_number     = EXCLUDED.issue_number,
			issue_url        = EXCLUDED.issue_url,
			last_synced_at   = NOW()`,
		planID, contributorCode, repo, issue.Number, issue.HTMLURL, string(kind), entityPK)
	if err != nil {
		return fmt.Errorf("upsert decom plan issue: %w", err)
	}
	return nil
}

// upsertParentPlanIssue inserts or updates the parent tracking row. Parent rows
// carry a NULL contributor_pk, which the UNIQUE index treats as distinct, so
// ON CONFLICT cannot be used: update in place if a parent exists, else insert.
func (a *API) upsertParentPlanIssue(ctx context.Context, planID uuid.UUID, repo string, issue *GithubIssue) error {
	tag, err := a.PgPool.Exec(ctx, `
		UPDATE topology_plan_issues
		SET github_repo = $2, issue_number = $3, issue_url = $4, last_synced_at = NOW()
		WHERE plan_id = $1 AND is_parent = true`,
		planID, repo, issue.Number, issue.HTMLURL)
	if err != nil {
		return fmt.Errorf("update parent plan issue: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	_, err = a.PgPool.Exec(ctx, `
		INSERT INTO topology_plan_issues
			(id, plan_id, contributor_pk, contributor_code, github_repo, issue_number, issue_url, is_parent, last_synced_at, created_at)
		VALUES (gen_random_uuid(), $1, NULL, NULL, $2, $3, $4, true, NOW(), NOW())`,
		planID, repo, issue.Number, issue.HTMLURL)
	if err != nil {
		return fmt.Errorf("insert parent plan issue: %w", err)
	}
	return nil
}

// loadPlanAndActionList resolves the plan from the URL + env, optionally
// enforces that it is approved, fetches the live baseline for the plan's own
// environment once, and derives the action list from that same baseline, so
// the issue path can pass it into collectDecomTargets without a second fetch.
// requireApproved is false for the preview path (rendering issue text has no
// side effects, so drafts are fine) and true for the sync path (filing real
// GitHub issues still needs an approved plan). The returned int is the HTTP
// status to use when err is non-nil.
func (a *API) loadPlanAndActionList(ctx context.Context, r *http.Request, requireApproved bool) (*Plan, *ActionList, *TopologyResponse, int, error) {
	id := chi.URLParam(r, "id")
	if id == "" {
		return nil, nil, nil, http.StatusBadRequest, errors.New("plan id is required")
	}
	planID, err := uuid.Parse(id)
	if err != nil {
		return nil, nil, nil, http.StatusBadRequest, errors.New("invalid plan id")
	}
	plan, err := loadPlanWithChanges(ctx, a.PgPool, planID)
	if err != nil {
		return nil, nil, nil, http.StatusNotFound, errors.New("plan not found")
	}
	// FIX 4: by-id plan lookup is not env-scoped, but this path files/updates
	// real GitHub issues, so a plan from another environment must not be
	// reachable through a request whose ambient env differs (e.g. a testnet
	// request must not be able to sync issues for a mainnet-beta plan id).
	// Checked before any GitHub client call in both PostPlanIssuesPreview and
	// PostPlanIssuesSync, since both funnel through this loader.
	if reqEnv := string(EnvFromContext(ctx)); plan.Environment != reqEnv {
		return nil, nil, nil, http.StatusBadRequest, fmt.Errorf(
			"plan belongs to environment %q, not the request environment %q", plan.Environment, reqEnv)
	}
	if requireApproved && plan.Status != StatusApproved {
		return nil, nil, nil, http.StatusConflict, errors.New("plan must be approved before creating issues")
	}
	// Baseline is the live topology for the plan's own environment, not the
	// request's (mirrors (a *API) deriveActionList).
	baseCtx := ContextWithEnv(ctx, DZEnv(plan.Environment))
	baseline, err := a.FetchTopologyData(baseCtx)
	if err != nil {
		return nil, nil, nil, http.StatusInternalServerError, errors.New("failed to load baseline topology")
	}
	al := deriveActionListFromBaseline(&plan, plan.Changes, &baseline)
	return &plan, al, &baseline, http.StatusOK, nil
}

// PostPlanIssuesPreview serves POST /api/topology/plans/{id}/issues/preview.
// Internal-only; no GitHub token required. Renders the issue titles/bodies a
// manual creation flow would use, without touching GitHub and without
// requiring the plan to be approved (rendering issue text has no side
// effects). The parent tracking issue is dropped: it links child issue
// numbers that do not exist until something is actually filed on GitHub.
func (a *API) PostPlanIssuesPreview(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil || !account.IsInternalUser {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	repo := os.Getenv("INFRA_ISSUES_REPO")
	if repo == "" {
		repo = defaultInfraRepo
	}
	plan, al, baseline, code, err := a.loadPlanAndActionList(r.Context(), r, false)
	if err != nil {
		http.Error(w, err.Error(), code)
		return
	}
	previews, err := a.PreviewPlanIssues(r.Context(), plan, al, baseline, repo, false)
	if err != nil {
		log.Printf("PreviewPlanIssues: %v", err)
		http.Error(w, "failed to preview issues", http.StatusInternalServerError)
		return
	}
	writeJSON(w, IssuesPreviewResponse{Repo: repo, Issues: previews, UnassignedTaskCount: unassignedTaskCount(al)})
}

// PostPlanIssuesSync serves POST /api/topology/plans/{id}/issues/sync.
// Internal-only; feature-gated on GITHUB_TOKEN. Idempotently creates/updates the
// per-contributor issues and the parent tracking issue.
func (a *API) PostPlanIssuesSync(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil || !account.IsInternalUser {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	gh, err := newGithubClient()
	if err != nil {
		http.Error(w, "GitHub integration not configured", http.StatusServiceUnavailable)
		return
	}
	plan, al, baseline, code, err := a.loadPlanAndActionList(r.Context(), r, true)
	if err != nil {
		http.Error(w, err.Error(), code)
		return
	}
	issues, err := a.SyncPlanIssues(r.Context(), gh, plan, al, baseline, true)
	if err != nil {
		log.Printf("SyncPlanIssues: %v", err)
		http.Error(w, "failed to sync issues", http.StatusBadGateway)
		return
	}

	// Best-effort audit event; never fail the sync on a logging error.
	created, updated := 0, 0
	for _, si := range issues {
		if si.Action == "created" {
			created++
		} else {
			updated++
		}
	}
	after, _ := json.Marshal(map[string]any{"repo": gh.RepoName(), "created": created, "updated": updated})
	if _, evErr := a.PgPool.Exec(r.Context(), `
		INSERT INTO topology_plan_events (plan_id, actor_account_id, actor_email, action, after, at)
		VALUES ($1, $2, $3, 'issues.sync', $4, NOW())`,
		plan.ID, account.ID, account.Email, after); evErr != nil {
		log.Printf("SyncPlanIssues: record event: %v", evErr)
	}

	writeJSON(w, IssuesSyncResponse{Repo: gh.RepoName(), Issues: issues, UnassignedTaskCount: unassignedTaskCount(al)})
}
