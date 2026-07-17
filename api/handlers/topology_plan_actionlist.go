package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ActionList is the per-contributor action list derived from a plan's changes.
// It is the source of truth for the "Copy as markdown" export and (later) the
// per-contributor GitHub issue bodies. Canonical shape: SC-6.
type ActionList struct {
	PlanID      string                   `json:"plan_id"` // plan.ID.String()
	Environment string                   `json:"environment"`
	Groups      []ContributorActionGroup `json:"groups"`
	Markdown    string                   `json:"markdown"` // full document, server-rendered
}

// ContributorActionGroup is one contributor's slice of the action list.
// Canonical shape: SC-6, plus Markdown (server-rendered; reused byte-identical
// as the Phase 6 per-contributor GitHub issue body).
type ContributorActionGroup struct {
	ContributorPK   string       `json:"contributor_pk"`
	ContributorCode string       `json:"contributor_code"`
	SlackChannel    string       `json:"slack_channel"` // "#ext-doublezero-<code>"
	Tasks           []ActionTask `json:"tasks"`
	Markdown        string       `json:"markdown"`
}

// ActionTask is a single unit of work for a contributor, derived from one change.
// Canonical shape: SC-6.
type ActionTask struct {
	Seq                  int             `json:"seq"`
	OpType               PlanOpType      `json:"op_type"`
	Title                string          `json:"title"`
	State                PlanChangeState `json:"state"`
	TargetDate           *string         `json:"target_date"`
	Note                 string          `json:"note"`
	InvolvedContributors []string        `json:"involved_contributors"` // contributor codes; for DZX move = losing + gaining
	// remove_device surfaces current users/stake read from the baseline.
	CurrentUsers *int     `json:"current_users,omitempty"`
	StakeSol     *float64 `json:"stake_sol,omitempty"`
	StakeShare   *float64 `json:"stake_share,omitempty"`
}

// Op-specific payload decoding reuses Phase 2's shared decoder
// (`api/handlers/planner_graph.go`): `decodePlanChangePayload(c PlanChange) (plannerPayload, error)`.
// Phase 4 does NOT define its own payload decoder (SC-2). `plannerPayload` carries every
// field this file reads: ContributorPK, MetroPK, Code, Side, SideADevicePK, SideARef,
// SideZDevicePK, SideZRef, LinkType, NewDeviceRef, etc.

// changeRefSnapshot is the human identity of referenced pks captured at add-time.
// It survives pk removal and is the fallback when a referenced entity has vanished
// from the live baseline (drift).
type changeRefSnapshot struct {
	DeviceCode           string `json:"device_code"`
	LinkCode             string `json:"link_code"`
	LinkType             string `json:"link_type"`
	ContributorCode      string `json:"contributor_code"`
	MetroCode            string `json:"metro_code"`
	SideAContributorPK   string `json:"side_a_contributor_pk"`
	SideAContributorCode string `json:"side_a_contributor_code"`
	SideZContributorPK   string `json:"side_z_contributor_pk"`
	SideZContributorCode string `json:"side_z_contributor_code"`
	BandwidthBps         int64  `json:"bandwidth_bps"`
}

func parseSnapshot(raw json.RawMessage) changeRefSnapshot {
	var s changeRefSnapshot
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &s)
	}
	return s
}

// baselineIndex holds fast lookups over the live topology plus the plan's own
// create-ops (so a change can reference a not-yet-onchain entity by local_ref).
type baselineIndex struct {
	deviceByPK      map[string]Device
	linkByPK        map[string]Link
	metroByPK       map[string]Metro
	contribCodeByPK map[string]string
	addDeviceByRef  map[string]PlanChange
}

func newBaselineIndex(b *TopologyResponse, changes []PlanChange) *baselineIndex {
	idx := &baselineIndex{
		deviceByPK:      map[string]Device{},
		linkByPK:        map[string]Link{},
		metroByPK:       map[string]Metro{},
		contribCodeByPK: map[string]string{},
		addDeviceByRef:  map[string]PlanChange{},
	}
	if b != nil {
		for _, d := range b.Devices {
			idx.deviceByPK[d.PK] = d
			if d.ContributorPK != "" && d.ContributorCode != "" {
				idx.contribCodeByPK[d.ContributorPK] = d.ContributorCode
			}
		}
		for _, l := range b.Links {
			idx.linkByPK[l.PK] = l
			if l.SideAContributorPK != "" && l.SideAContributorCode != "" {
				idx.contribCodeByPK[l.SideAContributorPK] = l.SideAContributorCode
			}
			if l.SideZContributorPK != "" && l.SideZContributorCode != "" {
				idx.contribCodeByPK[l.SideZContributorPK] = l.SideZContributorCode
			}
			if l.ContributorPK != "" && l.ContributorCode != "" {
				idx.contribCodeByPK[l.ContributorPK] = l.ContributorCode
			}
		}
		for _, m := range b.Metros {
			idx.metroByPK[m.PK] = m
		}
	}
	for _, ch := range changes {
		if ch.OpType == OpAddDevice && ch.LocalRef != "" {
			idx.addDeviceByRef[ch.LocalRef] = ch
		}
	}
	return idx
}

func (b *baselineIndex) contribCode(pk string) string { return b.contribCodeByPK[pk] }

// endpoint is a resolved link endpoint: its device code and owning contributor.
type endpoint struct {
	deviceCode  string
	contribPK   string
	contribCode string
	isNew       bool
}

// resolveEndpoint resolves a link endpoint from either an existing device pk or a
// sibling create-op's local_ref.
func (b *baselineIndex) resolveEndpoint(devicePK, ref string) endpoint {
	if ref != "" {
		if add, ok := b.addDeviceByRef[ref]; ok {
			p, _ := decodePlanChangePayload(add)
			snap := parseSnapshot(add.RefSnapshot)
			code := p.Code
			if code == "" {
				code = snap.DeviceCode
			}
			// A brand-new contributor carries its code directly on the add_device
			// payload (no pk to look up yet); fall back to a baseline pk lookup,
			// then to the ref_snapshot, for an existing contributor.
			cc := p.ContributorCode
			if cc == "" {
				cc = b.contribCode(p.ContributorPK)
			}
			if cc == "" {
				cc = snap.ContributorCode
			}
			return endpoint{deviceCode: code, contribPK: p.ContributorPK, contribCode: cc, isNew: true}
		}
		return endpoint{deviceCode: ref, isNew: true}
	}
	if d, ok := b.deviceByPK[devicePK]; ok {
		return endpoint{deviceCode: d.Code, contribPK: d.ContributorPK, contribCode: d.ContributorCode}
	}
	return endpoint{deviceCode: devicePK}
}

// groupKey is the stable key a contributor group is stored under.
func groupKey(contribPK, contribCode string) string {
	if contribPK != "" {
		return contribPK
	}
	if contribCode != "" {
		return "code:" + contribCode
	}
	return "unknown"
}

type actionAccumulator struct {
	groups map[string]*ContributorActionGroup
	order  []string
}

func newActionAccumulator() *actionAccumulator {
	return &actionAccumulator{groups: map[string]*ContributorActionGroup{}}
}

func (acc *actionAccumulator) group(contribPK, contribCode string) *ContributorActionGroup {
	key := groupKey(contribPK, contribCode)
	g, ok := acc.groups[key]
	if !ok {
		channel := ""
		if contribCode != "" {
			channel = "#ext-doublezero-" + strings.ToLower(contribCode)
		}
		g = &ContributorActionGroup{
			ContributorPK:   contribPK,
			ContributorCode: contribCode,
			SlackChannel:    channel,
		}
		acc.groups[key] = g
		acc.order = append(acc.order, key)
	}
	return g
}

func (acc *actionAccumulator) addTask(contribPK, contribCode string, task ActionTask) {
	g := acc.group(contribPK, contribCode)
	g.Tasks = append(g.Tasks, task)
}

func (acc *actionAccumulator) finish(plan *Plan) *ActionList {
	al := &ActionList{
		PlanID:      plan.ID.String(),
		Environment: plan.Environment,
		Groups:      make([]ContributorActionGroup, 0, len(acc.order)),
	}
	keys := append([]string(nil), acc.order...)
	sort.SliceStable(keys, func(i, j int) bool {
		gi, gj := acc.groups[keys[i]], acc.groups[keys[j]]
		if gi.ContributorCode != gj.ContributorCode {
			return gi.ContributorCode < gj.ContributorCode
		}
		return gi.ContributorPK < gj.ContributorPK
	})
	for _, k := range keys {
		g := acc.groups[k]
		g.Markdown = renderGroupMarkdown(g)
		al.Groups = append(al.Groups, *g)
	}
	al.Markdown = renderActionListMarkdown(plan, al.Groups)
	return al
}

func baseTask(ch PlanChange, title string, involved []string) ActionTask {
	return ActionTask{
		Seq:                  ch.Seq,
		OpType:               ch.OpType,
		Title:                title,
		State:                ch.State,
		Note:                 ch.AssigneeNote,
		InvolvedContributors: involved,
		TargetDate:           ch.TargetDate,
	}
}

func uniqueNonEmpty(items []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, it := range items {
		if it == "" || seen[it] {
			continue
		}
		seen[it] = true
		out = append(out, it)
	}
	return out
}

func orFallback(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// deriveActionListFromBaseline is the pure baseline-vs-draft diff: it walks the plan's
// ordered changes and emits per-contributor tasks. skipped/superseded changes produce
// no task. It never touches the database. The exported `(a *API) deriveActionList` method
// (Task 5) wraps this after loading the baseline internally.
func deriveActionListFromBaseline(plan *Plan, changes []PlanChange, baseline *TopologyResponse) *ActionList {
	b := newBaselineIndex(baseline, changes)
	acc := newActionAccumulator()

	sorted := append([]PlanChange(nil), changes...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })

	for _, ch := range sorted {
		if ch.State == StateSkipped || ch.State == StateSuperseded {
			continue
		}
		switch ch.OpType {
		case OpAddDevice:
			deriveAddDevice(acc, b, ch)
		case OpRemoveDevice:
			deriveRemoveDevice(acc, b, ch)
		case OpRemoveLink:
			deriveRemoveLink(acc, b, ch)
		case OpAddLink:
			deriveAddLink(acc, b, ch)
		case OpMoveLinkEnd:
			deriveMoveLinkEnd(acc, b, ch)
		}
	}

	return acc.finish(plan)
}

func deriveAddDevice(acc *actionAccumulator, b *baselineIndex, ch PlanChange) {
	p, _ := decodePlanChangePayload(ch)
	snap := parseSnapshot(ch.RefSnapshot)

	code := orFallback(p.Code, snap.DeviceCode)
	// Group by contributor_code first: it's always present on the canonical
	// add_device payload (even for a brand-new contributor with no pk yet),
	// falling back to a baseline pk lookup (existing contributor) then the
	// ref_snapshot for older changes staged before this field existed.
	contribCode := p.ContributorCode
	if contribCode == "" {
		contribCode = orFallback(b.contribCode(p.ContributorPK), snap.ContributorCode)
	}

	metroCode := snap.MetroCode
	switch {
	case p.NewMetro != nil && p.NewMetro.Code != "":
		metroCode = p.NewMetro.Code
	case p.MetroPK != "":
		if m, ok := b.metroByPK[p.MetroPK]; ok {
			metroCode = m.Code
		}
	}

	title := fmt.Sprintf("Bring device %s online", code)
	if metroCode != "" {
		title = fmt.Sprintf("Bring device %s online in %s", code, metroCode)
	}
	acc.addTask(p.ContributorPK, contribCode, baseTask(ch, title, nil))
}

func deriveRemoveDevice(acc *actionAccumulator, b *baselineIndex, ch PlanChange) {
	snap := parseSnapshot(ch.RefSnapshot)

	var contribPK, contribCode, devCode string
	task := baseTask(ch, "", nil)

	if d, ok := b.deviceByPK[ch.RefDevicePK]; ok {
		contribPK, contribCode, devCode = d.ContributorPK, d.ContributorCode, d.Code
		users := int(d.UserCount)
		stakeSol := d.StakeSol
		stakeShare := d.StakeShare
		task.CurrentUsers = &users
		task.StakeSol = &stakeSol
		task.StakeShare = &stakeShare
	} else {
		devCode = snap.DeviceCode
		contribCode = snap.ContributorCode
	}
	if devCode == "" {
		devCode = ch.RefDevicePK
	}

	task.Title = fmt.Sprintf("Decommission device %s", devCode)
	acc.addTask(contribPK, contribCode, task)
}

// deriveRemoveLink emits a "Remove link" task for both endpoint contributors
// (a link's two ends can belong to different contributors).
func deriveRemoveLink(acc *actionAccumulator, b *baselineIndex, ch PlanChange) {
	snap := parseSnapshot(ch.RefSnapshot)

	var aPK, aCode, zPK, zCode, linkCode string
	if l, ok := b.linkByPK[ch.RefLinkPK]; ok {
		aPK, aCode = l.SideAContributorPK, l.SideAContributorCode
		zPK, zCode = l.SideZContributorPK, l.SideZContributorCode
		linkCode = l.Code
	} else {
		aPK, aCode = snap.SideAContributorPK, snap.SideAContributorCode
		zPK, zCode = snap.SideZContributorPK, snap.SideZContributorCode
		linkCode = snap.LinkCode
	}
	if linkCode == "" {
		linkCode = ch.RefLinkPK
	}

	involved := uniqueNonEmpty([]string{aCode, zCode})
	title := fmt.Sprintf("Remove link %s", linkCode)

	acc.addTask(aPK, aCode, baseTask(ch, title, involved))
	if groupKey(zPK, zCode) != groupKey(aPK, aCode) {
		acc.addTask(zPK, zCode, baseTask(ch, title, involved))
	}
}

// deriveAddLink emits a "Provision <type> link" task for both endpoint
// contributors, resolving each endpoint from either an existing device pk or
// a sibling create-op's local_ref.
func deriveAddLink(acc *actionAccumulator, b *baselineIndex, ch PlanChange) {
	p, _ := decodePlanChangePayload(ch)
	a := b.resolveEndpoint(p.SideADevicePK, p.SideARef)
	z := b.resolveEndpoint(p.SideZDevicePK, p.SideZRef)

	linkType := orFallback(p.LinkType, "link")
	involved := uniqueNonEmpty([]string{a.contribCode, z.contribCode})
	title := fmt.Sprintf("Provision %s link %s <-> %s", linkType, a.deviceCode, z.deviceCode)

	acc.addTask(a.contribPK, a.contribCode, baseTask(ch, title, involved))
	if groupKey(z.contribPK, z.contribCode) != groupKey(a.contribPK, a.contribCode) {
		acc.addTask(z.contribPK, z.contribCode, baseTask(ch, title, involved))
	}
}

// endpointOwner is the owning contributor of one link endpoint.
type endpointOwner struct {
	pk   string
	code string
}

// deriveMoveLinkEnd emits one coordination task shared across every contributor
// the move touches: the losing endpoint owner, the gaining (target device)
// owner, and the unchanged other-side owner. A DZX move is the case the SPEC
// calls out: it needs sign-off from both the contributor losing the endpoint
// and the contributor whose device is gaining it, plus a heads-up to whoever
// owns the link's other, unmoved side.
func deriveMoveLinkEnd(acc *actionAccumulator, b *baselineIndex, ch PlanChange) {
	p, _ := decodePlanChangePayload(ch)
	snap := parseSnapshot(ch.RefSnapshot)

	var linkCode, linkType string
	var losing, other endpointOwner
	if l, ok := b.linkByPK[ch.RefLinkPK]; ok {
		linkCode, linkType = l.Code, l.LinkType
		if strings.EqualFold(p.Side, "z") {
			losing = endpointOwner{l.SideZContributorPK, l.SideZContributorCode}
			other = endpointOwner{l.SideAContributorPK, l.SideAContributorCode}
		} else {
			losing = endpointOwner{l.SideAContributorPK, l.SideAContributorCode}
			other = endpointOwner{l.SideZContributorPK, l.SideZContributorCode}
		}
	} else {
		linkCode, linkType = snap.LinkCode, snap.LinkType
		if strings.EqualFold(p.Side, "z") {
			losing = endpointOwner{snap.SideZContributorPK, snap.SideZContributorCode}
			other = endpointOwner{snap.SideAContributorPK, snap.SideAContributorCode}
		} else {
			losing = endpointOwner{snap.SideAContributorPK, snap.SideAContributorCode}
			other = endpointOwner{snap.SideZContributorPK, snap.SideZContributorCode}
		}
	}
	if linkCode == "" {
		linkCode = ch.RefLinkPK
	}
	linkType = orFallback(linkType, "link")

	gaining := b.resolveEndpoint(ch.NewDevicePK, p.NewDeviceRef)

	title := fmt.Sprintf("%s ↔ %s: coordinate moving %s link %s to device %s",
		orFallback(losing.code, "unknown"), orFallback(gaining.contribCode, "unknown"),
		linkType, linkCode, gaining.deviceCode)

	involved := uniqueNonEmpty([]string{losing.code, gaining.contribCode, other.code})

	// The same task lands in every involved contributor's group (dedup by key).
	seen := map[string]bool{}
	addOnce := func(pk, code string) {
		if pk == "" && code == "" {
			return
		}
		key := groupKey(pk, code)
		if seen[key] {
			return
		}
		seen[key] = true
		acc.addTask(pk, code, baseTask(ch, title, involved))
	}
	addOnce(losing.pk, losing.code)
	addOnce(gaining.contribPK, gaining.contribCode)
	addOnce(other.pk, other.code)
}

// inlineSanitizer collapses CR/LF to a single space and escapes backticks. It is
// applied to every user-supplied string interpolated inline into markdown.
var inlineSanitizer = strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ", "`", "\\`")

// sanitizeInline neutralizes user-supplied text so it cannot corrupt the markdown
// list/fence structure it is interpolated into. This exact markdown is reused
// byte-for-byte as the Phase 6 GitHub issue body, so a stray newline, code fence,
// or leading block marker in a free-text note must not be able to break the
// surrounding document. It collapses CR/LF to a single space (a multi-line note
// stays one list item), escapes backticks (an unbalanced code fence can't swallow
// following content), and escapes a leading block marker so the text is not read
// as a new heading or list item.
func sanitizeInline(s string) string {
	s = inlineSanitizer.Replace(s)
	trimmed := strings.TrimLeft(s, " \t")
	if trimmed != "" {
		switch trimmed[0] {
		case '#', '-', '*', '+', '>':
			lead := len(s) - len(trimmed)
			s = s[:lead] + "\\" + trimmed
		}
	}
	return s
}

// renderGroupMarkdown renders one contributor's action group as markdown. It is
// the single source of truth reused by the Phase 6 per-contributor GitHub issue
// body, so the "Copy as markdown" export and the issues are byte-identical.
func renderGroupMarkdown(g *ContributorActionGroup) string {
	var sb strings.Builder
	header := sanitizeInline(orFallback(g.ContributorCode, "Unknown contributor"))
	sb.WriteString("## " + header)
	if g.SlackChannel != "" {
		fmt.Fprintf(&sb, " (%s)", g.SlackChannel)
	}
	sb.WriteString("\n\n")

	for _, t := range g.Tasks {
		checkbox := "[ ]"
		if t.State == StateDone {
			checkbox = "[x]"
		}
		fmt.Fprintf(&sb, "- %s **%s**\n", checkbox, sanitizeInline(t.Title))
		if t.CurrentUsers != nil {
			fmt.Fprintf(&sb, "  - Current users: %d", *t.CurrentUsers)
			if t.StakeSol != nil && t.StakeShare != nil {
				fmt.Fprintf(&sb, ", stake: %.1f SOL (%.2f%%)", *t.StakeSol, *t.StakeShare)
			}
			sb.WriteString("\n")
		}
		if len(t.InvolvedContributors) > 1 {
			fmt.Fprintf(&sb, "  - Coordinate with: %s\n", sanitizeInline(strings.Join(t.InvolvedContributors, ", ")))
		}
		if t.TargetDate != nil {
			fmt.Fprintf(&sb, "  - Target date: %s\n", sanitizeInline(*t.TargetDate))
		}
		if t.Note != "" {
			fmt.Fprintf(&sb, "  - Note: %s\n", sanitizeInline(t.Note))
		}
	}
	return sb.String()
}

// renderActionListMarkdown renders the full plan action list as markdown: a
// title followed by each contributor group's markdown, in group order.
func renderActionListMarkdown(plan *Plan, groups []ContributorActionGroup) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Topology plan: %s\n\n", sanitizeInline(plan.Name))
	if len(groups) == 0 {
		sb.WriteString("_No contributor actions in this plan._\n")
		return sb.String()
	}
	for i, g := range groups {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(g.Markdown)
	}
	return sb.String()
}

// deriveActionList loads the live topology baseline for the plan's own environment and
// returns the per-contributor action list derived from plan.Changes (SC-2). Phase 6
// calls this exact method to build the GitHub-issue bodies.
func (a *API) deriveActionList(ctx context.Context, plan Plan) (*ActionList, error) {
	// Baseline is the live topology for the plan's own environment, not the request's.
	baseCtx := ContextWithEnv(ctx, DZEnv(plan.Environment))
	baseline, err := a.FetchTopologyData(baseCtx)
	if err != nil {
		return nil, fmt.Errorf("load baseline topology: %w", err)
	}
	return deriveActionListFromBaseline(&plan, plan.Changes, &baseline), nil
}

// GetTopologyPlanActionList serves GET /api/topology/plans/{id}/action-list.
// It loads the plan + changes from Postgres and returns the derived per-contributor
// action list (including server-rendered markdown for the copy/export + GitHub-issue path).
func (a *API) GetTopologyPlanActionList(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}

	plan, err := loadPlanWithChanges(ctx, a.PgPool, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Plan not found", http.StatusNotFound)
			return
		}
		http.Error(w, internalError("Failed to load plan", err), http.StatusInternalServerError)
		return
	}

	al, err := a.deriveActionList(ctx, plan)
	if err != nil {
		http.Error(w, internalError("Failed to derive action list", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, al)
}
