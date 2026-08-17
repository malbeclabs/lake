package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertKeysAllowed marshals v to JSON and fails if it carries a key outside
// allowed. label names the struct in the failure message.
func assertKeysAllowed(t *testing.T, label string, v any, allowed map[string]bool) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("%s marshal: %v", label, err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%s unmarshal: %v", label, err)
	}
	for k := range m {
		if !allowed[k] {
			t.Errorf("%s exposes unexpected field %q; the public ops aggregate must be numbers/enums only (no free text)", label, k)
		}
	}
}

// TestNHTicketsNoFreeText enforces the public boundary structurally: the
// ops-management aggregate served to the public browser must contain only
// numeric/enum fields, never ticket titles, descriptions, reporter identities,
// or root-cause text. The guard recurses into the nested lists
// (NHNoTicketOutage, NHRootCauseCount), each populated with a sample element, so
// a free-text field added at any level fails the test.
func TestNHTicketsNoFreeText(t *testing.T) {
	allowed := map[string]bool{
		"total": true, "incidents": true, "maintenance": true,
		"sev1": true, "sev2": true, "sev3": true,
		"response_p50_min": true, "resolution_p50_min": true,
		"closed_incidents":    true,
		"self_reported_count": true, "doublezero_filed_count": true, "self_reported_pct": true,
		"maintenance_lead_p50_min": true, "maintenance_duration_p50_min": true, "closed_maintenance": true,
		"outage_count": true, "outages_with_ticket": true,
		"outages_no_ticket": true, "no_ticket_share_pct": true,
		"no_ticket_outages": true,
		"root_causes":       true,
	}
	// Populate the nested lists so their element shape is exercised too; an empty
	// NHTickets marshals them as empty arrays and would skip the recursion.
	tk := NHTickets{
		NoTicketOutages: []NHNoTicketOutage{{LinkPK: "lp", LinkCode: "ams-fra", StartTs: "2026-06-10T10:00:00Z", Hours: 1.5}},
		RootCauses:      []NHRootCauseCount{{Cause: "carrier", Count: 2, Pct: f64ptr(50)}},
	}
	assertKeysAllowed(t, "NHTickets", tk, allowed)

	// NHNoTicketOutage: link identity + start + duration only, never ticket text.
	noTicketAllowed := map[string]bool{
		"link_pk": true, "link_code": true, "start_ts": true, "hours": true,
	}
	for _, o := range tk.NoTicketOutages {
		assertKeysAllowed(t, "NHNoTicketOutage", o, noTicketAllowed)
	}

	// NHRootCauseCount: fixed enum token + counts only, never free-text cause.
	rootCauseAllowed := map[string]bool{"cause": true, "count": true, "pct": true}
	for _, rc := range tk.RootCauses {
		assertKeysAllowed(t, "NHRootCauseCount", rc, rootCauseAllowed)
	}

	// The key-name walk above cannot see a free-text VALUE, so also assert that a
	// cause produced from operator-entered upstream text stays inside the enum.
	free := "fibre cut in MRS, see ticket 4412"
	produced := computeTicketAggregates(
		[]nhRawTicket{{ID: "1", Type: "incident", Status: "open", CreatedAt: "2026-06-10T10:00:00Z", RootCause: &free}},
		nil, nil, map[string]string{}, "")
	for _, rc := range produced.RootCauses {
		if !nhRootCauseTokens[rc.Cause] && rc.Cause != nhRootCauseOther {
			t.Errorf("NHRootCauseCount published a free-text cause %q", rc.Cause)
		}
	}
}

// TestComputeTicketAggregates sanity-checks the aggregation: type/severity
// counts, closed/root-cause, and outage coverage matching.
func TestComputeTicketAggregates(t *testing.T) {
	sev1 := "sev1"
	rc := "carrier"
	// Incident "1" has a blank creator (DoubleZero-filed); "3" is filed by a user
	// belonging to the same contributor the incident is about (self-reported);
	// "4" is filed by DoubleZero staff (a user with no contributor).
	tickets := []nhRawTicket{
		{ID: "1", Type: "incident", Severity: &sev1, Status: "resolved",
			StartAt: strptr("2026-06-10T10:00:00Z"), EndAt: strptr("2026-06-10T11:00:00Z"),
			CreatedAt: "2026-06-10T10:05:00Z", RootCause: &rc, ContributorPubkey: strptr("C-x"),
			AffectedLinks: []OpsTicketEntity{{Code: "ams-fra"}}},
		{ID: "2", Type: "maintenance", Status: "completed",
			StartAt: strptr("2026-06-11T22:00:00Z"), EndAt: strptr("2026-06-11T23:00:00Z"),
			CreatedAt: "2026-06-11T09:00:00Z"},
		{ID: "3", Type: "incident", Status: "open", ContributorPubkey: strptr("C-jump"),
			CreatedAt: "2026-06-12T08:00:00Z", UserPubkey: "contrib-noc-1"},
		{ID: "4", Type: "incident", Status: "open", ContributorPubkey: strptr("C-galaxy"),
			CreatedAt: "2026-06-13T08:00:00Z", UserPubkey: "dz-staff-1"},
	}
	// contrib-noc-1 belongs to C-jump; dz-staff-1 is DoubleZero (empty contributor).
	userContrib := map[string]string{"dz-staff-1": "", "contrib-noc-1": "C-jump"}
	outages := []nhOutage{
		{linkCode: "ams-fra", start: mustTime("2026-06-10T10:10:00Z"), end: mustTime("2026-06-10T10:40:00Z")},
		{linkCode: "nyc-chi", start: mustTime("2026-06-12T00:00:00Z"), end: mustTime("2026-06-12T00:30:00Z")},
	}
	agg := computeTicketAggregates(tickets, tickets, outages, userContrib, "")
	if agg.Total != 4 || agg.Incidents != 3 || agg.Maintenance != 1 {
		t.Errorf("volume: %+v", agg)
	}
	if agg.Sev1 != 1 {
		t.Errorf("severity: %+v", agg)
	}
	if agg.ClosedIncidents != 1 {
		t.Errorf("closed incidents: %+v", agg)
	}
	if agg.ClosedMaintenance != 1 {
		t.Errorf("closed maintenance: %+v", agg)
	}
	// Self-reported: only incident "3" (contributor creator). "1" (blank creator)
	// and "4" (DoubleZero staff) are DoubleZero-filed. Pct = 1/3 incidents.
	if agg.SelfReportedCount != 1 || agg.DoubleZeroFiledCount != 2 {
		t.Errorf("self-reported counts: %+v", agg)
	}
	if agg.SelfReportedPct == nil || *agg.SelfReportedPct != 33.3 {
		t.Errorf("self-reported pct: %+v", agg.SelfReportedPct)
	}
	// The one incident with a cause carries root_cause "carrier"; the breakdown
	// is a single carrier row at 100% of caused incidents.
	if len(agg.RootCauses) != 1 || agg.RootCauses[0].Cause != "carrier" ||
		agg.RootCauses[0].Count != 1 || agg.RootCauses[0].Pct == nil || *agg.RootCauses[0].Pct != 100 {
		t.Errorf("root-cause breakdown: %+v", agg.RootCauses)
	}
	if agg.OutageCount != 2 || agg.OutagesWithTicket != 1 || agg.OutagesNoTicket != 1 {
		t.Errorf("coverage: %+v", agg)
	}
	// The actionable list exposes the one unfiled outage (nyc-chi, 30 min = 0.5h),
	// not the ams-fra outage that a ticket covers.
	if len(agg.NoTicketOutages) != 1 {
		t.Fatalf("no-ticket outages: %+v", agg.NoTicketOutages)
	}
	if agg.NoTicketOutages[0].LinkCode != "nyc-chi" || agg.NoTicketOutages[0].Hours != 0.5 ||
		agg.NoTicketOutages[0].StartTs != "2026-06-12T00:00:00Z" {
		t.Errorf("no-ticket outage item: %+v", agg.NoTicketOutages[0])
	}

	// With an empty user registry (fetch failed) the percentage is unavailable
	// (nil) rather than mislabeling every incident as DoubleZero.
	aggNoUsers := computeTicketAggregates(tickets, tickets, outages, map[string]string{}, "")
	if aggNoUsers.SelfReportedPct != nil {
		t.Errorf("self-reported pct should be nil when user registry is empty: %+v", aggNoUsers.SelfReportedPct)
	}

	// Scoped to contributor C-jump: only incident "3" is ABOUT C-jump, so
	// incidents affecting-but-not-about it ("1" about C-x, "4" about C-galaxy)
	// must not leak in. That one incident was filed by a C-jump user, so
	// self-reported is 1/1 = 100%. (This is the Teraswitch-style bug: a scoped
	// view must not count another contributor's self-filed incident.)
	aggScoped := computeTicketAggregates(tickets, tickets, outages, userContrib, "C-jump")
	if aggScoped.Incidents != 1 {
		t.Errorf("scoped incidents should be 1 (only about C-jump): %+v", aggScoped)
	}
	if aggScoped.SelfReportedCount != 1 || aggScoped.SelfReportedPct == nil || *aggScoped.SelfReportedPct != 100 {
		t.Errorf("scoped self-reported should be 1/1=100%%: count=%d pct=%v", aggScoped.SelfReportedCount, aggScoped.SelfReportedPct)
	}
	// A contributor with no self-filed incidents (scope C-x: incident "1" filed
	// by a blank/DoubleZero creator) reads 0%, never inflated.
	aggScopedX := computeTicketAggregates(tickets, tickets, outages, userContrib, "C-x")
	if aggScopedX.Incidents != 1 || aggScopedX.SelfReportedCount != 0 ||
		aggScopedX.SelfReportedPct == nil || *aggScopedX.SelfReportedPct != 0 {
		t.Errorf("scoped C-x self-reported should be 0/1=0%%: %+v", aggScopedX)
	}
}

// TestComputeTicketAggregates_CoverageSliceIsNotCounted verifies the coverage
// carve-out: a ticket filed just after the window end covers an outage that
// started inside the window, but must never enter Total/Incidents/Sev1. Without
// it the strict upper bound would report an outage as unfiled purely because its
// ticket landed minutes after midnight.
func TestComputeTicketAggregates_CoverageSliceIsNotCounted(t *testing.T) {
	sev1 := "sev1"
	inWindow := nhRawTicket{ID: "in", Type: "incident", Status: "open",
		CreatedAt: "2026-06-20T10:00:00Z"}
	lateFiled := nhRawTicket{ID: "late", Type: "incident", Severity: &sev1, Status: "open",
		StartAt: strptr("2026-06-29T23:50:00Z"), EndAt: strptr("2026-06-30T00:20:00Z"),
		CreatedAt:     "2026-06-30T00:10:00Z",
		AffectedLinks: []OpsTicketEntity{{Code: "ams-fra"}}}
	outages := []nhOutage{
		{linkCode: "ams-fra", start: mustTime("2026-06-29T23:50:00Z"), end: mustTime("2026-06-30T00:05:00Z")},
	}

	agg := computeTicketAggregates([]nhRawTicket{inWindow}, []nhRawTicket{inWindow, lateFiled},
		outages, map[string]string{}, "")

	assert.Equal(t, 1, agg.Total, "the late-filed ticket must not be counted")
	assert.Equal(t, 1, agg.Incidents)
	assert.Equal(t, 0, agg.Sev1, "the late-filed ticket's severity must not be counted")
	assert.Equal(t, 1, agg.OutagesWithTicket, "the late-filed ticket still covers the outage")
	assert.Equal(t, 0, agg.OutagesNoTicket)
	assert.Empty(t, agg.NoTicketOutages)

	// Without the coverage slice the same outage reads as unfiled.
	strict := computeTicketAggregates([]nhRawTicket{inWindow}, []nhRawTicket{inWindow},
		outages, map[string]string{}, "")
	assert.Equal(t, 1, strict.OutagesNoTicket)
}

// TestComputeTicketAggregates_UnparseableStartAtBoundedByCreatedAt pins the bound
// on the start_at fallback. A cover ticket whose start_at does not parse anchors
// its interval to the outage under test, which would otherwise match every outage
// on every link the ticket lists, across the whole fetch. The ticket's own
// created_at bounds it: an outage that began after the ticket was filed cannot be
// the one it reports.
func TestComputeTicketAggregates_UnparseableStartAtBoundedByCreatedAt(t *testing.T) {
	badStart := "2026-06-20"
	cover := nhRawTicket{ID: "cover", Type: "incident", Status: "open",
		StartAt: &badStart, CreatedAt: "2026-06-20T12:00:00Z",
		AffectedLinks: []OpsTicketEntity{{Code: "ams-fra"}}}
	outages := []nhOutage{
		{linkPK: "pk-before", linkCode: "ams-fra",
			start: mustTime("2026-06-20T09:00:00Z"), end: mustTime("2026-06-20T09:30:00Z")},
		{linkPK: "pk-after", linkCode: "ams-fra",
			start: mustTime("2026-06-25T09:00:00Z"), end: mustTime("2026-06-25T09:30:00Z")},
	}

	agg := computeTicketAggregates(nil, []nhRawTicket{cover}, outages, map[string]string{}, "")

	assert.Equal(t, 1, agg.OutagesWithTicket, "the outage that predates the filing stays covered")
	assert.Equal(t, 1, agg.OutagesNoTicket, "an outage five days after the filing is not covered by it")
	require.Len(t, agg.NoTicketOutages, 1)
	assert.Equal(t, "pk-after", agg.NoTicketOutages[0].LinkPK)
}

// TestNHSplitTicketsByWindow pins the ops-ticket window split: the upper bound at
// end is half-open, the lower bound at priorStart is inclusive, and a ticket
// whose created_at does not parse is dropped rather than defaulted into the
// current window.
func TestNHSplitTicketsByWindow(t *testing.T) {
	priorStart := mustTime("2026-05-03T00:00:00Z")
	start := mustTime("2026-06-01T00:00:00Z")
	end := mustTime("2026-06-30T00:00:00Z")

	ticket := func(id, created string) nhRawTicket {
		return nhRawTicket{ID: id, Type: "incident", CreatedAt: created}
	}

	cases := []struct {
		name       string
		created    string
		wantCur    bool
		wantPrior  bool
		wantUndate bool
	}{
		{name: "before_prior_start", created: "2026-05-02T23:59:59Z"},
		{name: "exactly_prior_start", created: "2026-05-03T00:00:00Z", wantPrior: true},
		{name: "one_second_before_start", created: "2026-05-31T23:59:59Z", wantPrior: true},
		{name: "exactly_start", created: "2026-06-01T00:00:00Z", wantCur: true},
		{name: "one_second_before_end", created: "2026-06-29T23:59:59Z", wantCur: true},
		{name: "exactly_end", created: "2026-06-30T00:00:00Z"},
		{name: "after_end", created: "2026-07-05T00:00:00Z"},
		{name: "empty_created_at", created: "", wantUndate: true},
		{name: "unparseable_created_at", created: "not-a-date", wantUndate: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sp := splitTicketsByWindow([]nhRawTicket{ticket(c.name, c.created)}, priorStart, start, end)
			assert.Equal(t, c.wantCur, len(sp.cur) == 1, "cur: %+v", sp.cur)
			assert.Equal(t, c.wantPrior, len(sp.prior) == 1, "prior: %+v", sp.prior)
			if c.wantUndate {
				assert.Equal(t, 1, sp.undated)
			} else {
				assert.Equal(t, 0, sp.undated)
			}
		})
	}

	t.Run("coverage_is_unbounded_by_filing_time", func(t *testing.T) {
		sp := splitTicketsByWindow([]nhRawTicket{
			ticket("in", "2026-06-15T00:00:00Z"),
			ticket("late", "2026-06-30T00:10:00Z"),
			ticket("much-later", "2026-08-14T09:00:00Z"),
			ticket("older-than-prior-start", "2026-04-01T00:00:00Z"),
		}, priorStart, start, end)
		require.Len(t, sp.cur, 1)
		assert.Equal(t, "in", sp.cur[0].ID)
		assert.Empty(t, sp.prior)
		assert.Len(t, sp.cover, 4, "every dated ticket can cover an outage: %+v", sp.cover)
	})

	t.Run("undated_tickets_are_not_in_cover", func(t *testing.T) {
		sp := splitTicketsByWindow([]nhRawTicket{
			ticket("in", "2026-06-15T00:00:00Z"),
			ticket("bad", "not-a-date"),
		}, priorStart, start, end)
		assert.Equal(t, 1, sp.undated)
		require.Len(t, sp.cover, 1)
		assert.Equal(t, "in", sp.cover[0].ID)
	})
}

// TestNHTicketFiledAfterWindowCoversOutage pins that filing lag is not a coverage
// bound. Coverage is matched on the ticket's incident interval, so a ticket filed
// after the window closed still covers an outage inside it and the outage must not
// be published as unfiled. The historical case is the worst one: every ticket for
// a window months in the past was filed after that window ended.
func TestNHTicketFiledAfterWindowCoversOutage(t *testing.T) {
	rfc := func(t time.Time) string { return t.UTC().Format(time.RFC3339) }

	run := func(start, end, filed time.Time) NHTickets {
		priorStart, _ := nhPriorWindow(start, end)
		outStart, outEnd := end.Add(-2*time.Hour), end.Add(-time.Hour)
		tickets := []nhRawTicket{
			{ID: "in", Type: "incident", Status: "open", CreatedAt: rfc(start.Add(time.Hour))},
			{ID: "late", Type: "incident", Status: "open", CreatedAt: rfc(filed),
				StartAt: strptr(rfc(outStart)), EndAt: strptr(rfc(outEnd)),
				AffectedLinks: []OpsTicketEntity{{Code: "ams-fra"}}},
		}
		sp := splitTicketsByWindow(tickets, priorStart, start, end)
		outs := []nhOutage{{linkPK: "pk-1", linkCode: "ams-fra", start: outStart, end: outEnd}}
		return computeTicketAggregates(sp.cur, sp.cover, outs, map[string]string{}, "")
	}

	t.Run("default_window_filed_next_morning", func(t *testing.T) {
		end := time.Now().UTC().Truncate(24 * time.Hour)
		agg := run(end.AddDate(0, 0, -30), end, end.Add(8*time.Hour))
		assert.Equal(t, 1, agg.OutagesWithTicket, "a ticket filed after midnight still covers")
		assert.Equal(t, 0, agg.OutagesNoTicket)
		assert.Empty(t, agg.NoTicketOutages)
		assert.Equal(t, 1, agg.Total, "the late-filed ticket must not be counted")
	})

	t.Run("historical_window_filed_months_later", func(t *testing.T) {
		end := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -90)
		agg := run(end.AddDate(0, 0, -30), end, time.Now().UTC())
		assert.Equal(t, 1, agg.OutagesWithTicket, "a ticket filed after a past window still covers it")
		assert.Equal(t, 0, agg.OutagesNoTicket)
		assert.Empty(t, agg.NoTicketOutages)
		assert.Equal(t, 1, agg.Total)
	})
}

// TestNHRootCauseAllowlist pins the published root-cause enum: each of the eleven
// tokens the ops-management API emits maps to itself (case/whitespace normalised)
// and anything else buckets as "other" instead of reaching the public payload
// verbatim. An allowlist short of the upstream enum relabels real causes
// ("duplicate" is the 5th most common cause in the live stream) as "other".
func TestNHRootCauseAllowlist(t *testing.T) {
	upstream := []string{
		"self_resolved", "network_external", "carrier", "fiber_cut", "duplicate",
		"configuration", "hardware", "false_positive", "software", "dz_managed",
		"human_error",
	}
	for _, token := range upstream {
		assert.Equal(t, token, nhRootCause(token), "upstream token %q must publish as itself", token)
	}
	assert.Len(t, nhRootCauseTokens, len(upstream), "the allowlist is exactly the upstream enum")

	cases := []struct{ in, want string }{
		{"Carrier", "carrier"},
		{" carrier ", "carrier"},
		{"DZ_Managed", "dz_managed"},
		{"fibre cut in MRS, see ticket 4412", nhRootCauseOther},
		{"", nhRootCauseOther},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, nhRootCause(c.in), "root cause %q", c.in)
	}
}

// TestComputeTicketAggregates_RootCauseBucketsUnknown verifies an operator-entered
// root cause is bucketed as "other" rather than published verbatim, that
// case-variants merge into one row, and that bucketing does not change the
// denominator every published share is computed against.
func TestComputeTicketAggregates_RootCauseBucketsUnknown(t *testing.T) {
	rcLower, rcUpper := "carrier", "Carrier"
	rcFree := "fibre cut in MRS, see ticket 4412"
	tickets := []nhRawTicket{
		{ID: "1", Type: "incident", Status: "open", CreatedAt: "2026-06-10T10:00:00Z", RootCause: &rcLower},
		{ID: "2", Type: "incident", Status: "open", CreatedAt: "2026-06-11T10:00:00Z", RootCause: &rcUpper},
		{ID: "3", Type: "incident", Status: "open", CreatedAt: "2026-06-12T10:00:00Z", RootCause: &rcFree},
	}
	agg := computeTicketAggregates(tickets, tickets, nil, map[string]string{}, "")

	require.Len(t, agg.RootCauses, 2, "carrier + other: %+v", agg.RootCauses)
	assert.Equal(t, "carrier", agg.RootCauses[0].Cause)
	assert.Equal(t, 2, agg.RootCauses[0].Count)
	assert.Equal(t, nhRootCauseOther, agg.RootCauses[1].Cause)
	assert.Equal(t, 1, agg.RootCauses[1].Count)

	// The denominator still counts all three caused incidents.
	require.NotNil(t, agg.RootCauses[0].Pct)
	assert.InDelta(t, 66.7, *agg.RootCauses[0].Pct, 0.05)
	require.NotNil(t, agg.RootCauses[1].Pct)
	assert.InDelta(t, 33.3, *agg.RootCauses[1].Pct, 0.05)

	for _, rc := range agg.RootCauses {
		assert.True(t, nhRootCauseTokens[rc.Cause] || rc.Cause == nhRootCauseOther,
			"published cause %q escaped the allowlist", rc.Cause)
	}
}

// TestFilterTicketsByContributor covers the two ways a ticket can belong to a
// scoped contributor: its own contributor identity (pubkey or name) matching,
// or one of its affected links/devices being owned by that contributor.
func TestFilterTicketsByContributor(t *testing.T) {
	scope := &nhContributorScope{
		pubkey:      "pk-acme",
		name:        "Acme Contributor",
		linkCodes:   map[string]struct{}{"ams-fra": {}},
		deviceCodes: map[string]struct{}{"ams-dz01": {}},
	}
	pk := "pk-acme"
	name := "Acme Contributor"
	otherPk := "pk-other"
	otherName := "Other Co"

	tickets := []nhRawTicket{
		{ID: "by-pubkey", ContributorPubkey: &pk},
		{ID: "by-name", ContributorName: &name},
		{ID: "by-link", AffectedLinks: []OpsTicketEntity{{Code: "ams-fra"}}},
		{ID: "by-device", AffectedDevices: []OpsTicketEntity{{Code: "ams-dz01"}}},
		{ID: "no-match", ContributorPubkey: &otherPk, ContributorName: &otherName,
			AffectedLinks: []OpsTicketEntity{{Code: "nyc-chi"}}},
		{ID: "empty"},
	}

	got := filterTicketsByContributor(tickets, scope)
	var ids []string
	for _, tk := range got {
		ids = append(ids, tk.ID)
	}
	assert.ElementsMatch(t, []string{"by-pubkey", "by-name", "by-link", "by-device"}, ids)
}

// mockContributorRow is a minimal driver.Row double for
// resolveContributorScope's pk/name lookup, returning a fixed pair (or an
// error).
type mockContributorRow struct {
	pk, name string
	err      error
}

func (r mockContributorRow) Err() error { return r.err }
func (r mockContributorRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*string) = r.pk
	*dest[1].(*string) = r.name
	return nil
}
func (r mockContributorRow) ScanStruct(dest any) error { return nil }

// mockCodeRows is a minimal driver.Rows double iterating over a fixed list of
// single-column string values (link/device codes).
type mockCodeRows struct {
	codes []string
	i     int
}

func (r *mockCodeRows) Next() bool { r.i++; return r.i <= len(r.codes) }
func (r *mockCodeRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.codes[r.i-1]
	return nil
}
func (r *mockCodeRows) ScanStruct(dest any) error        { return nil }
func (r *mockCodeRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *mockCodeRows) Totals(dest ...any) error         { return nil }
func (r *mockCodeRows) Columns() []string                { return []string{"code"} }
func (r *mockCodeRows) Close() error                     { return nil }
func (r *mockCodeRows) Err() error                       { return nil }
func (r *mockCodeRows) HasData() bool                    { return len(r.codes) > 0 }

// mockContributorScopeConn is a hand-built driver.Conn double for
// TestResolveContributorScope and friends. It stands in for the three queries
// resolveContributorScope issues (contributor pk/name lookup, then owned link
// codes, then owned device codes) without requiring a live ClickHouse
// instance: apitesting's DB fixtures (apitesting.NewTestAPIBare,
// insertBaseMetadata) live in a package that imports `handlers`, so they
// can't be used from this internal (package handlers) test file without an
// import cycle, and resolveContributorScope/nhContributorScope are
// unexported, so an external (package handlers_test) test can't reach them
// either.
type mockContributorScopeConn struct {
	driver.Conn
	pk, name                     string
	rowErr                       error
	linkCodes, deviceCodes       []string
	linkQueryErr, deviceQueryErr error
}

func (m *mockContributorScopeConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return mockContributorRow{pk: m.pk, name: m.name, err: m.rowErr}
}

func (m *mockContributorScopeConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "dz_links_current"):
		if m.linkQueryErr != nil {
			return nil, m.linkQueryErr
		}
		return &mockCodeRows{codes: m.linkCodes}, nil
	case strings.Contains(query, "dz_devices_current"):
		if m.deviceQueryErr != nil {
			return nil, m.deviceQueryErr
		}
		return &mockCodeRows{codes: m.deviceCodes}, nil
	default:
		return nil, fmt.Errorf("mockContributorScopeConn: unexpected query %q", query)
	}
}

// TestResolveContributorScope covers resolveContributorScope's query shape
// (pubkey/name lookup by code, then owned link codes, then owned device
// codes), plus an end-to-end-ish check that the resolved scope, fed into
// filterTicketsByContributor, keeps a ticket matched by an owned link code
// and drops an unrelated one.
func TestResolveContributorScope(t *testing.T) {
	api := &API{DB: &mockContributorScopeConn{
		pk: "pk-acme", name: "Acme Contributor",
		linkCodes:   []string{"ams-fra", "ams-lon"},
		deviceCodes: []string{"ams-dz01"},
	}}

	scope, err := api.resolveContributorScope(context.Background(), "ACME")
	require.NoError(t, err)
	assert.Equal(t, "pk-acme", scope.pubkey)
	assert.Equal(t, "Acme Contributor", scope.name)
	assert.Len(t, scope.linkCodes, 2)
	assert.Contains(t, scope.linkCodes, "ams-fra")
	assert.Contains(t, scope.linkCodes, "ams-lon")
	assert.Len(t, scope.deviceCodes, 1)
	assert.Contains(t, scope.deviceCodes, "ams-dz01")

	tickets := []nhRawTicket{
		{ID: "own-link", AffectedLinks: []OpsTicketEntity{{Code: "ams-fra"}}},
		{ID: "unrelated", AffectedLinks: []OpsTicketEntity{{Code: "nyc-chi"}}},
	}
	got := filterTicketsByContributor(tickets, scope)
	require.Len(t, got, 1)
	assert.Equal(t, "own-link", got[0].ID)
}

// TestResolveContributorScope_UnknownCode verifies an unresolvable
// contributor code surfaces as an error (the caller in the Tickets group
// treats this as best-effort and omits the ticket panel rather than failing
// the whole request).
func TestResolveContributorScope_UnknownCode(t *testing.T) {
	api := &API{DB: &mockContributorScopeConn{rowErr: errors.New("no rows")}}
	scope, err := api.resolveContributorScope(context.Background(), "NOPE")
	require.Error(t, err)
	assert.Nil(t, scope)
}

// TestResolveContributorScope_CodeQueryFailureTolerated verifies that a
// transient failure fetching the owned link/device codes does not fail scope
// resolution outright: it degrades to an empty set for that code type
// (logged via logError) rather than losing the whole scoped ticket panel.
func TestResolveContributorScope_CodeQueryFailureTolerated(t *testing.T) {
	api := &API{DB: &mockContributorScopeConn{
		pk: "pk-acme", name: "Acme Contributor",
		linkQueryErr:   errors.New("boom"),
		deviceQueryErr: errors.New("boom"),
	}}
	scope, err := api.resolveContributorScope(context.Background(), "ACME")
	require.NoError(t, err)
	assert.Equal(t, "pk-acme", scope.pubkey)
	assert.Empty(t, scope.linkCodes)
	assert.Empty(t, scope.deviceCodes)
}

// TestFetchOpsTicketsSincePartialPageTolerance verifies that a mid-pagination
// upstream failure keeps the tickets already collected instead of discarding
// the whole window (issue: the ticket panel going blank on any ops-API
// hiccup). A failure on the very first page still returns an error since
// there is nothing to salvage.
func TestFetchOpsTicketsSincePartialPageTolerance(t *testing.T) {
	savedURL := opsMgmtBaseURL
	defer func() { opsMgmtBaseURL = savedURL }()
	t.Setenv("OPS_MANAGEMENT_API_KEY", "test-key")

	since := time.Now().Add(-24 * time.Hour).UTC()
	newer := since.Add(1 * time.Hour).UTC().Format(time.RFC3339)

	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			// Return a full page (100 tickets) so the loop believes more pages
			// may follow, then the second page fails.
			tickets := make([]map[string]any, 0, 100)
			for i := 0; i < 100; i++ {
				tickets = append(tickets, map[string]any{
					"id": fmt.Sprintf("t-%d", i), "type": "incident", "status": "open",
					"created_at": newer,
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"tickets": tickets}})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("upstream unavailable"))
	}))
	defer upstream.Close()
	opsMgmtBaseURL = upstream.URL

	api := &API{}
	tickets, err := api.fetchOpsTicketsSince(context.Background(), since)
	require.NoError(t, err)
	assert.Len(t, tickets, 100)
	assert.Equal(t, 2, calls)
}

// TestFetchOpsTicketsSinceFirstPageFailureIsAnError verifies there is no
// partial data to salvage when the very first request fails.
func TestFetchOpsTicketsSinceFirstPageFailureIsAnError(t *testing.T) {
	savedURL := opsMgmtBaseURL
	defer func() { opsMgmtBaseURL = savedURL }()
	t.Setenv("OPS_MANAGEMENT_API_KEY", "test-key")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()
	opsMgmtBaseURL = upstream.URL

	api := &API{}
	tickets, err := api.fetchOpsTicketsSince(context.Background(), time.Now().Add(-24*time.Hour))
	require.Error(t, err)
	assert.Nil(t, tickets)
}

func strptr(s string) *string { return &s }

func f64ptr(f float64) *float64 { return &f }

func mustTime(s string) time.Time {
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return tm.UTC()
}

// nhJSONMap marshals v and unmarshals it back into a generic map so a test can
// assert the exact JSON key names and nesting the frontend reads.
func nhJSONMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

// TestNHGroupJSONShape pins the wire shape of each group payload's `prev` block,
// which the frontend deltas read directly. The split shipped with prev emitted
// under the wrong nesting (a wrapper object) for drain/tickets, so the frontend
// read undefined and every delta silently vanished. This asserts each group's
// prev is at the nesting api.ts expects.
func TestNHGroupJSONShape(t *testing.T) {
	// Drain: prev is the bare NHDrainTiming (no "drain_timing" wrapper).
	t.Run("drain", func(t *testing.T) {
		m := nhJSONMap(t, NHDrainGroup{
			DrainTiming: NHDrainTiming{TimeToDrainP50Min: f64ptr(12)},
			Prev:        &NHDrainTiming{TimeToDrainP50Min: f64ptr(15)},
		})
		prev, ok := m["prev"].(map[string]any)
		require.True(t, ok, "prev must be an object")
		_, hasField := prev["time_to_drain_p50_min"]
		assert.True(t, hasField, "prev must carry time_to_drain_p50_min at its top level")
		_, hasWrapper := prev["drain_timing"]
		assert.False(t, hasWrapper, "prev must NOT be wrapped in drain_timing")
	})

	// Tickets: prev is the bare NHTickets (no "ops_tickets" wrapper).
	t.Run("tickets", func(t *testing.T) {
		m := nhJSONMap(t, NHTicketsGroup{
			OpsTickets: &NHTickets{},
			Prev:       &NHTickets{Maintenance: 2, SelfReportedPct: f64ptr(50)},
		})
		prev, ok := m["prev"].(map[string]any)
		require.True(t, ok, "prev must be an object")
		_, hasPct := prev["self_reported_pct"]
		assert.True(t, hasPct, "prev must carry self_reported_pct")
		_, hasMaint := prev["maintenance"]
		assert.True(t, hasMaint, "prev must carry maintenance")
		_, hasWrapper := prev["ops_tickets"]
		assert.False(t, hasWrapper, "prev must NOT be wrapped in ops_tickets")
	})

	// Outages: prev is nested {reliability, outage_summary}; reliability carries
	// outage_count + capped_downtime_hours.
	t.Run("outages", func(t *testing.T) {
		m := nhJSONMap(t, NHOutagesGroup{
			Prev: &NHOutagesPrev{
				Reliability:   NHReliabilityPrev{OutageCount: 3, CappedDowntimeHours: 4.5},
				OutageSummary: &NHOutageSummary{},
			},
		})
		prev, ok := m["prev"].(map[string]any)
		require.True(t, ok, "prev must be an object")
		rel, ok := prev["reliability"].(map[string]any)
		require.True(t, ok, "prev.reliability must be an object")
		_, hasCount := rel["outage_count"]
		assert.True(t, hasCount, "prev.reliability must carry outage_count")
		_, hasDowntime := rel["capped_downtime_hours"]
		assert.True(t, hasDowntime, "prev.reliability must carry capped_downtime_hours")
		_, hasSummary := prev["outage_summary"]
		assert.True(t, hasSummary, "prev must carry outage_summary")
		// The dead contributor breakdown must not reappear on the wire.
		_, hasContribs := m["contributors"]
		assert.False(t, hasContribs, "outages group must not emit contributors")
	})

	// Impactful: prev carries impactful_downtime_hours.
	t.Run("impactful", func(t *testing.T) {
		m := nhJSONMap(t, NHImpactful{
			Prev: &NHImpactfulPrev{ImpactfulDowntimeHours: 7},
		})
		prev, ok := m["prev"].(map[string]any)
		require.True(t, ok, "prev must be an object")
		_, has := prev["impactful_downtime_hours"]
		assert.True(t, has, "prev must carry impactful_downtime_hours")
	})

	// Degraded is additive and omitted when empty, so a blob written before this
	// field existed still decodes, and a healthy payload does not gain a key.
	t.Run("degraded", func(t *testing.T) {
		clean := nhJSONMap(t, NHOutagesGroup{})
		_, hasClean := clean["degraded"]
		assert.False(t, hasClean, "degraded must be omitted when no panel failed")

		m := nhJSONMap(t, NHOutagesGroup{Degraded: []string{"outage_summary"}})
		got, ok := m["degraded"].([]any)
		require.True(t, ok, "degraded must be an array")
		assert.Equal(t, []any{"outage_summary"}, got)
	})

	// Overview: the headline owns only its own figures. The outage and impactful
	// tiles come from the Outages and Impactful groups, so the headline copies
	// (which were never assigned and always published 0/null) must stay retired.
	t.Run("overview headline", func(t *testing.T) {
		m := nhJSONMap(t, NHOverview{})
		headline, ok := m["headline"].(map[string]any)
		require.True(t, ok, "headline must be an object")
		assertKeysAllowed(t, "headline", headline, map[string]bool{
			"peak_bps": true, "jitter_improve_pct": true, "dz_loss_pct": true,
			"active_links": true, "active_devices": true, "active_metros": true,
			"deltas": true,
		})
		deltas, ok := headline["deltas"].(map[string]any)
		require.True(t, ok, "headline.deltas must be an object")
		assertKeysAllowed(t, "headline.deltas", deltas, map[string]bool{
			"peak_bps": true, "active_links": true,
		})
	})

	// Deferred: prev is the bare NHDeferred with time_to_undrain_p50_min.
	t.Run("deferred", func(t *testing.T) {
		m := nhJSONMap(t, NHDeferred{
			Prev: &NHDeferred{TimeToUndrainP50Min: f64ptr(9)},
		})
		prev, ok := m["prev"].(map[string]any)
		require.True(t, ok, "prev must be an object")
		_, has := prev["time_to_undrain_p50_min"]
		assert.True(t, has, "prev must carry time_to_undrain_p50_min")
	})
}

// TestComputeDrainTiming exercises the pure-Go drain/undrain pairing: drain and
// undrain counts, time-to-drain and time-drained medians/maxes, the
// drain-within-30m window (inclusive boundary), and the no-pair / no-event
// cases where the timing pointers must stay nil.
func TestComputeDrainTiming(t *testing.T) {
	// Full scenario: L1 drains 10 min after its outage and is undrained 30 min
	// later; L2 drains 60 min after its outage (outside the 30m window) and is
	// never undrained.
	t.Run("full", func(t *testing.T) {
		events := []nhEvent{
			{linkPK: "L1", start: mustTime("2026-06-10T10:00:00Z"), end: mustTime("2026-06-10T10:20:00Z")},
			{linkPK: "L2", start: mustTime("2026-06-10T12:00:00Z"), end: mustTime("2026-06-10T12:05:00Z")},
		}
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "soft-drained", ts: mustTime("2026-06-10T10:10:00Z")},
			{linkPK: "L1", prev: "soft-drained", next: "activated", ts: mustTime("2026-06-10T10:40:00Z")},
			{linkPK: "L2", prev: "activated", next: "soft-drained", ts: mustTime("2026-06-10T13:00:00Z")},
		}
		dt, matches := computeDrainTiming(events, changes)
		assert.Equal(t, 2, dt.OutageCount)
		assert.Equal(t, 2, dt.Drains)
		assert.Equal(t, 1, dt.Undrains)
		assert.Equal(t, 2, dt.EventsWithDrain)
		// ttd = {10, 60}: median 35, max 60.
		require.NotNil(t, dt.TimeToDrainP50Min)
		assert.Equal(t, float64(35), *dt.TimeToDrainP50Min)
		require.NotNil(t, dt.TimeToDrainMaxMin)
		assert.Equal(t, float64(60), *dt.TimeToDrainMaxMin)
		// drained = {30}: median and max both 30.
		require.NotNil(t, dt.TimeDrainedP50Min)
		assert.Equal(t, float64(30), *dt.TimeDrainedP50Min)
		require.NotNil(t, dt.TimeDrainedMaxMin)
		assert.Equal(t, float64(30), *dt.TimeDrainedMaxMin)
		// Only L1's drain (10:10) is within [start, end+30m]; L2's (13:00) is not.
		require.NotNil(t, dt.DrainWithin30mPct)
		assert.Equal(t, 50.0, *dt.DrainWithin30mPct)
		assert.Equal(t, 1, dt.MatchedUndrains)
		require.Len(t, matches, 1)
		assert.Equal(t, "L1", matches[0].linkPK)
	})

	// Boundary: a drain landing exactly at end+30m still counts (inclusive).
	t.Run("boundary_inclusive", func(t *testing.T) {
		events := []nhEvent{
			{linkPK: "L1", start: mustTime("2026-06-10T10:00:00Z"), end: mustTime("2026-06-10T10:20:00Z")},
		}
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "soft-drained", ts: mustTime("2026-06-10T10:50:00Z")},
		}
		dt, _ := computeDrainTiming(events, changes)
		require.NotNil(t, dt.DrainWithin30mPct)
		assert.Equal(t, 100.0, *dt.DrainWithin30mPct)
	})

	// Boundary: one second past end+30m does not count.
	t.Run("boundary_exclusive", func(t *testing.T) {
		events := []nhEvent{
			{linkPK: "L1", start: mustTime("2026-06-10T10:00:00Z"), end: mustTime("2026-06-10T10:20:00Z")},
		}
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "soft-drained", ts: mustTime("2026-06-10T10:50:01Z")},
		}
		dt, _ := computeDrainTiming(events, changes)
		require.NotNil(t, dt.DrainWithin30mPct)
		assert.Equal(t, 0.0, *dt.DrainWithin30mPct)
	})

	// A drain with no matching undrain produces no pair, so time-drained is nil.
	t.Run("no_pairs", func(t *testing.T) {
		events := []nhEvent{
			{linkPK: "L1", start: mustTime("2026-06-10T10:00:00Z"), end: mustTime("2026-06-10T10:20:00Z")},
		}
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "soft-drained", ts: mustTime("2026-06-10T10:10:00Z")},
		}
		dt, matches := computeDrainTiming(events, changes)
		assert.Equal(t, 1, dt.Drains)
		assert.Equal(t, 0, dt.Undrains)
		assert.Equal(t, 0, dt.MatchedUndrains)
		assert.Empty(t, matches)
		assert.Nil(t, dt.TimeDrainedP50Min)
		assert.Nil(t, dt.TimeDrainedMaxMin)
	})

	// hard-drained is a drained state too: an activated -> hard-drained ->
	// activated cycle is one drain and one undrain, with its duration measured.
	t.Run("hard_drain_cycle", func(t *testing.T) {
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "hard-drained", ts: mustTime("2026-06-29T09:02:00Z")},
			{linkPK: "L1", prev: "hard-drained", next: "activated", ts: mustTime("2026-06-29T09:32:00Z")},
		}
		dt, matches := computeDrainTiming(nil, changes)
		assert.Equal(t, 1, dt.Drains)
		assert.Equal(t, 1, dt.Undrains)
		assert.Equal(t, 1, dt.MatchedUndrains)
		require.Len(t, matches, 1)
		require.NotNil(t, dt.TimeDrainedP50Min)
		assert.Equal(t, float64(30), *dt.TimeDrainedP50Min)
	})

	// A change of drain LEVEL inside one drained run is neither a drain nor an
	// undrain: soft -> hard must not strand the open drain, and the duration is
	// measured from the FIRST entry into drained.
	t.Run("soft_then_hard_then_activated", func(t *testing.T) {
		t0 := mustTime("2026-06-10T10:00:00Z")
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "soft-drained", ts: t0},
			{linkPK: "L1", prev: "soft-drained", next: "hard-drained", ts: t0.Add(10 * time.Minute)},
			{linkPK: "L1", prev: "hard-drained", next: "activated", ts: t0.Add(40 * time.Minute)},
		}
		dt, matches := computeDrainTiming(nil, changes)
		assert.Equal(t, 1, dt.Drains, "a level change is not a second drain")
		assert.Equal(t, 1, dt.Undrains)
		require.Len(t, matches, 1)
		require.NotNil(t, dt.TimeDrainedP50Min)
		assert.Equal(t, float64(40), *dt.TimeDrainedP50Min, "measured from the soft drain")
	})

	// The reverse level change (hard -> soft) must not double-count the drain.
	t.Run("hard_then_soft_no_double_count", func(t *testing.T) {
		t0 := mustTime("2026-06-10T10:00:00Z")
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "hard-drained", ts: t0},
			{linkPK: "L1", prev: "hard-drained", next: "soft-drained", ts: t0.Add(5 * time.Minute)},
			{linkPK: "L1", prev: "soft-drained", next: "activated", ts: t0.Add(20 * time.Minute)},
		}
		dt, _ := computeDrainTiming(nil, changes)
		assert.Equal(t, 1, dt.Drains)
		assert.Equal(t, 1, dt.Undrains)
	})

	// An undrain whose drain opened before the window still counts as an undrain
	// (the link genuinely returned to service), with no duration sample.
	t.Run("undrain_without_opening_drain", func(t *testing.T) {
		changes := []nhChange{
			{linkPK: "L1", prev: "hard-drained", next: "activated", ts: mustTime("2026-07-23T13:51:00Z")},
		}
		dt, matches := computeDrainTiming(nil, changes)
		assert.Equal(t, 0, dt.Drains)
		assert.Equal(t, 1, dt.Undrains)
		assert.Equal(t, 0, dt.MatchedUndrains)
		assert.Empty(t, matches)
		assert.Nil(t, dt.TimeDrainedP50Min)
	})

	// A drain still open at the window edge, with a level change inside it, is
	// one drain and no undrain — never a second invented drain.
	t.Run("drain_still_open_across_levels", func(t *testing.T) {
		t0 := mustTime("2026-08-12T10:00:00Z")
		changes := []nhChange{
			{linkPK: "L1", prev: "activated", next: "soft-drained", ts: t0},
			{linkPK: "L1", prev: "soft-drained", next: "hard-drained", ts: t0.Add(15 * time.Minute)},
		}
		dt, matches := computeDrainTiming(nil, changes)
		assert.Equal(t, 1, dt.Drains)
		assert.Equal(t, 0, dt.Undrains)
		assert.Empty(t, matches)
		assert.Nil(t, dt.TimeDrainedP50Min)
	})

	// A link's first-ever transition has an empty previous status, which is not a
	// drained state, so entering soft-drained from it still counts as a drain.
	t.Run("empty_previous_status_still_drains", func(t *testing.T) {
		changes := []nhChange{
			{linkPK: "L1", prev: "", next: "soft-drained", ts: mustTime("2026-06-10T10:00:00Z")},
		}
		dt, _ := computeDrainTiming(nil, changes)
		assert.Equal(t, 1, dt.Drains)
		assert.Equal(t, 0, dt.Undrains)
	})

	// No events and no changes: every timing pointer is nil and the pct (0/0)
	// is nil rather than a divide-by-zero.
	t.Run("empty", func(t *testing.T) {
		dt, matches := computeDrainTiming(nil, nil)
		assert.Equal(t, 0, dt.OutageCount)
		assert.Empty(t, matches)
		assert.Nil(t, dt.TimeToDrainP50Min)
		assert.Nil(t, dt.TimeToDrainMaxMin)
		assert.Nil(t, dt.TimeDrainedP50Min)
		assert.Nil(t, dt.TimeDrainedMaxMin)
		assert.Nil(t, dt.DrainWithin30mPct)
	})
}

// TestNetworkHealthWindow covers the window resolver: the default 30d window is
// cacheable, an explicit custom range is not, an over-long range is clamped to
// the max span, and a sub-1-day day count floors to a single day.
func TestNetworkHealthWindow(t *testing.T) {
	day := 24 * time.Hour

	t.Run("default_30d_cacheable", func(t *testing.T) {
		start, end, cacheable := networkHealthWindow("", "", "")
		assert.True(t, cacheable, "the default window must be cacheable")
		assert.Equal(t, time.Duration(NetworkHealthDefaultDays)*day, end.Sub(start))
	})

	t.Run("custom_range_not_cacheable", func(t *testing.T) {
		start, end, cacheable := networkHealthWindow("2026-01-01", "2026-01-10", "")
		assert.False(t, cacheable, "an explicit custom range must not be cacheable")
		assert.Equal(t, 9*day, end.Sub(start))
	})

	t.Run("over_max_clamps", func(t *testing.T) {
		start, end, cacheable := networkHealthWindow("2026-01-01", "2026-12-31", "")
		assert.False(t, cacheable)
		assert.Equal(t, time.Duration(networkHealthMaxDays)*day, end.Sub(start),
			"a range longer than the max must clamp to the max span")
	})

	t.Run("sub_one_day_floors", func(t *testing.T) {
		start, end, cacheable := networkHealthWindow("", "", "0")
		assert.False(t, cacheable)
		assert.Equal(t, 1*day, end.Sub(start), "a day count below 1 must floor to a single day")
	})
}

// TestPctDelta covers the percent-change helper: a zero prior yields nil (no
// divide-by-zero), a normal case returns the right percent, and NaN/Inf inputs
// are guarded to nil.
func TestPctDelta(t *testing.T) {
	assert.Nil(t, pctDelta(5, 0), "a zero prior must return nil")

	up := pctDelta(150, 100)
	require.NotNil(t, up)
	assert.Equal(t, 50.0, *up)

	down := pctDelta(50, 100)
	require.NotNil(t, down)
	assert.Equal(t, -50.0, *down)

	assert.Nil(t, pctDelta(math.Inf(1), 1), "an Inf result must be guarded to nil")
	assert.Nil(t, pctDelta(math.NaN(), 1), "a NaN result must be guarded to nil")
}
