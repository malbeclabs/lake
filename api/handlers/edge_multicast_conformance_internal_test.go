package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeProm answers a query by matching a substring of it, so a test names the metric it is
// standing in for rather than reproducing the whole PromQL string.
type fakeProm struct {
	byMetric map[string][]PromSample
	err      error

	// errByMetric fails ONE leg while the others answer — the shape the catalog contract is
	// about.
	errByMetric map[string]error

	// delay slows each query, and lastQueryAt records when the last one ran. Only the clock
	// test uses them: an ordering assertion needs a reference point INSIDE the call, and it
	// needs the gap either side of it to be wider than the wall clock's granularity.
	delay       time.Duration
	lastQueryAt time.Time
}

func (f *fakeProm) Query(_ context.Context, query string) ([]PromSample, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.lastQueryAt = time.Now().UTC()
	if f.err != nil {
		return nil, f.err
	}
	for metric, err := range f.errByMetric {
		if strings.Contains(query, metric) {
			return nil, err
		}
	}
	for metric, samples := range f.byMetric {
		if strings.Contains(query, metric) {
			return samples, nil
		}
	}
	return nil, nil
}

func sample(v float64, kv ...string) PromSample {
	labels := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		labels[kv[i]] = kv[i+1]
	}
	return PromSample{Labels: labels, Value: v}
}

// oneValidator is the minimum a group needs to appear in the payload at all.
func oneValidator(group, host string) []PromSample {
	return []PromSample{sample(1, "multicast_group", group, "hostname", host)}
}

func fetchConformance(t *testing.T, f *fakeProm) *EdgeMulticastConformanceResponse {
	t.Helper()
	api := &API{Prom: f}
	got, err := api.FetchEdgeMulticastConformance(context.Background())
	if err != nil {
		t.Fatalf("FetchEdgeMulticastConformance: %v", err)
	}
	return got
}

func TestEdgeMulticastConformanceVerdict(t *testing.T) {
	tests := []struct {
		name string
		in   EdgeMulticastConformance
		want string
	}{
		{"a must violation outranks everything", EdgeMulticastConformance{Must: 1, Should: 9, Passes: 100, Graded: 100}, edgeMulticastConformanceViolating},
		{"a should violation is still a finding", EdgeMulticastConformance{Should: 1, Passes: 100, Graded: 100}, edgeMulticastConformanceShould},
		{"nothing came back at all", EdgeMulticastConformance{Passes: 0, Graded: 500, NA: 500}, edgeMulticastConformanceUngraded},
		{"an info finding with passes behind it is advisory", EdgeMulticastConformance{Info: 1, Passes: 100, Graded: 100}, edgeMulticastConformanceAdvisory},
		{"graded and clean", EdgeMulticastConformance{Passes: 100, Graded: 100}, edgeMulticastConformanceConforming},
		{"a validator that graded nothing at all", EdgeMulticastConformance{Instances: 1}, edgeMulticastConformanceUngraded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			if got := edgeMulticastConformanceVerdict(&in); got != tc.want {
				t.Fatalf("verdict = %q, want %q", got, tc.want)
			}
		})
	}
}

// The failure this verdict exists for. A validator can run, scrape cleanly and grade nothing —
// every rule reporting `na` because the state it needs was never reached — and report zero
// violations while doing it. Measured on a since-superseded build: 157,504 `na` and zero `pass`
// over 35 minutes, with not one must-severity check running. `conforming` there is a lie, and it
// is the lie every operator counter told at the time.
func TestEdgeMulticastConformance_NothingGradedIsNotAPass(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.3", "cmh1"),
		"dz_conformance_checks_total": {
			sample(157504, "multicast_group", "233.84.178.3", "result", "na"),
		},
	}})

	e := got.Groups["233.84.178.3"]
	if e == nil {
		t.Fatal("group missing from payload")
	}
	if e.Verdict != edgeMulticastConformanceUngraded {
		t.Fatalf("verdict = %q, want %q", e.Verdict, edgeMulticastConformanceUngraded)
	}
	if e.Passes != 0 || e.NA == 0 {
		t.Fatalf("passes = %d, na = %d; want zero passes and a non-zero na", e.Passes, e.NA)
	}
}

// checks_total carries result="violation", so a feed whose graded checks all came back as
// info-severity violations has no passes at all. On `Passes == 0` alone that rendered `ungraded`,
// whose tooltip says nothing reached a verdict — directly above a coverage line reading "0 of N
// checks passed" and a list naming the rules that fired. Something that produced a finding was
// graded, so the absence claim is the wrong one.
func TestEdgeMulticastConformance_AnInfoFindingWithNoPassesIsNotUngraded(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.3", "cmh1"),
		"dz_conformance_violations_total": {
			sample(5, "multicast_group", "233.84.178.3", "stream", "s",
				"rule_id", "TOB.QUOTE.SOURCE_COUNT", "severity", "info"),
		},
		"dz_conformance_checks_total": {
			sample(5, "multicast_group", "233.84.178.3", "result", "violation"),
		},
	}})

	e := got.Groups["233.84.178.3"]
	if e.Passes != 0 || e.Info == 0 {
		t.Fatalf("passes = %d, info = %d; want the shape this test is about", e.Passes, e.Info)
	}
	if e.Verdict != edgeMulticastConformanceAdvisory {
		t.Fatalf("verdict = %q, want %q", e.Verdict, edgeMulticastConformanceAdvisory)
	}
}

// A group whose validator is up but has produced no findings and no checks yet is a real state —
// a process that just started — and it must appear in the payload saying it graded nothing. If
// the group set were derived from the findings, a silent validator would be indistinguishable
// from an absent one.
func TestEdgeMulticastConformance_AValidatorWithNoFindingsStillAppears(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.4", "was1"),
	}})

	e := got.Groups["233.84.178.4"]
	if e == nil {
		t.Fatal("a running validator produced no group entry")
	}
	if e.Instances != 1 {
		t.Fatalf("instances = %d, want 1", e.Instances)
	}
	if e.Verdict != edgeMulticastConformanceUngraded {
		t.Fatalf("verdict = %q, want %q", e.Verdict, edgeMulticastConformanceUngraded)
	}
}

// withExemption installs a known-deviation entry for the duration of one test.
//
// The shipped list is EMPTY on purpose — see edge_multicast_conformance.go — so the mechanism has
// to be exercised against an injected entry. Testing it against whatever the list happens to hold
// would silently stop testing anything the day it is emptied, which is exactly the day it was.
func withExemption(t *testing.T, stream, ruleID string) {
	t.Helper()
	prev := edgeMulticastConformanceExemptions
	edgeMulticastConformanceExemptions = append(append([]edgeMulticastConformanceExemption{}, prev...),
		edgeMulticastConformanceExemption{stream: stream, ruleID: ruleID, why: "test"})
	t.Cleanup(func() { edgeMulticastConformanceExemptions = prev })
}

// The shipped list is empty, and that has to be asserted rather than assumed: an entry here would
// make the column read clean over a feed the deployed must-violation alert pages on, and nothing
// else in the code says so out loud.
func TestEdgeMulticastConformance_NoViolationIsExemptedByDefault(t *testing.T) {
	if len(edgeMulticastConformanceExemptions) != 0 {
		t.Fatalf("the exemption list must stay empty unless the deployed must-violation alert "+
			"exempts the same pair; got %d entries", len(edgeMulticastConformanceExemptions))
	}
}

// An exemption must not decide the verdict — and must not vanish either: a deviation that stops
// firing is a real change, and one that starts firing where it was never exempted is a finding.
func TestEdgeMulticastConformance_KnownDeviationIsExemptedNotHidden(t *testing.T) {
	withExemption(t, "kalshi_perps_tob", "MSG.WRONG_PORT_PLACEMENT")
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.3", "cmh1"),
		"dz_conformance_violations_total": {
			sample(4, "multicast_group", "233.84.178.3", "stream", "kalshi_perps_tob",
				"rule_id", "MSG.WRONG_PORT_PLACEMENT", "severity", "must"),
		},
		"dz_conformance_checks_total": {
			sample(9000, "multicast_group", "233.84.178.3", "result", "pass"),
		},
	}})

	e := got.Groups["233.84.178.3"]
	if e.Must != 0 {
		t.Fatalf("must = %d, want 0: the known deviation decided the verdict", e.Must)
	}
	if e.Exempted != 4 {
		t.Fatalf("exempted = %d, want 4: the hit was dropped instead of counted", e.Exempted)
	}
	if e.Verdict != edgeMulticastConformanceConforming {
		t.Fatalf("verdict = %q, want %q", e.Verdict, edgeMulticastConformanceConforming)
	}
}

// The exemption is per (instance, rule) and never a prefix over instances. The market-by-price
// feed does not share the top-of-book deviation — it does the opposite, and the publisher keeps a
// regression test asserting it — so a prefix matcher would blind a must-severity rule in exactly
// the place the publisher works to stay correct.
func TestEdgeMulticastConformance_ExemptionDoesNotReachAnotherInstance(t *testing.T) {
	withExemption(t, "kalshi_perps_tob", "MSG.WRONG_PORT_PLACEMENT")
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.4", "cmh1"),
		"dz_conformance_violations_total": {
			sample(2, "multicast_group", "233.84.178.4", "stream", "kalshi_perps_mbp",
				"rule_id", "MSG.WRONG_PORT_PLACEMENT", "severity", "must"),
		},
		"dz_conformance_checks_total": {
			sample(9000, "multicast_group", "233.84.178.4", "result", "pass"),
		},
	}})

	e := got.Groups["233.84.178.4"]
	if e.Must != 2 {
		t.Fatalf("must = %d, want 2: the top-of-book exemption reached the market-by-price feed", e.Must)
	}
	if e.Verdict != edgeMulticastConformanceViolating {
		t.Fatalf("verdict = %q, want %q", e.Verdict, edgeMulticastConformanceViolating)
	}
}

// Coverage loss on the snapshot rule is reported `unverifiable`, not `violation`, and on a healthy
// market-by-price feed it is the large majority of that rule's transitions. It must not read as a
// fault, and the passes beside it must still reach `conforming`.
func TestEdgeMulticastConformance_UnverifiableIsNotAFault(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.4", "cmh1"),
		"dz_conformance_checks_total": {
			sample(5811, "multicast_group", "233.84.178.4", "result", "pass"),
			sample(26341, "multicast_group", "233.84.178.4", "result", "unverifiable"),
		},
	}})

	e := got.Groups["233.84.178.4"]
	if e.Verdict != edgeMulticastConformanceConforming {
		t.Fatalf("verdict = %q, want %q", e.Verdict, edgeMulticastConformanceConforming)
	}
	if e.Graded != 5811+26341 {
		t.Fatalf("graded = %d, want %d: the denominator must carry every result", e.Graded, 5811+26341)
	}
	if e.Unverifiable != 26341 {
		t.Fatalf("unverifiable = %d, want 26341", e.Unverifiable)
	}
}

// Six validator instances scrape against ONE elections group, so the fold has to aggregate over
// instances rather than assume one per group — and the recorder set is what tells a reader how
// many vantages a verdict rests on.
func TestEdgeMulticastConformance_ManyInstancesOnOneGroupAggregate(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": {
			sample(6, "multicast_group", "233.84.178.21", "hostname", "cmh1"),
			sample(6, "multicast_group", "233.84.178.21", "hostname", "was1"),
		},
		"dz_conformance_checks_total": {
			sample(100, "multicast_group", "233.84.178.21", "result", "pass"),
		},
	}})

	e := got.Groups["233.84.178.21"]
	if e.Instances != 12 {
		t.Fatalf("instances = %d, want 12", e.Instances)
	}
	if len(e.Nodes) != 2 || e.Nodes[0] != "cmh1" || e.Nodes[1] != "was1" {
		t.Fatalf("nodes = %v, want [cmh1 was1] sorted", e.Nodes)
	}
}

// The rules the tooltip shows are ordered must-first, then by how loud they were. A reader
// scanning one line must see the severe rule, not the noisy one.
func TestEdgeMulticastConformance_TopRulesAreWorstFirst(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.20", "cmh1"),
		"dz_conformance_violations_total": {
			sample(900, "multicast_group", "233.84.178.20", "stream", "kalshi_sports_mbp_nfl",
				"rule_id", "NOISY.INFO.RULE", "severity", "info"),
			sample(3, "multicast_group", "233.84.178.20", "stream", "kalshi_sports_mbp_nfl",
				"rule_id", "MBP.SEQ.CONTINUITY", "severity", "must"),
			sample(40, "multicast_group", "233.84.178.20", "stream", "kalshi_sports_mbp_nfl",
				"rule_id", "SOME.SHOULD.RULE", "severity", "should"),
		},
	}})

	e := got.Groups["233.84.178.20"]
	if len(e.TopRules) != 3 {
		t.Fatalf("top rules = %d, want 3", len(e.TopRules))
	}
	if e.TopRules[0].RuleID != "MBP.SEQ.CONTINUITY" {
		t.Fatalf("first rule = %q, want the must-severity one", e.TopRules[0].RuleID)
	}
	if e.TopRules[1].RuleID != "SOME.SHOULD.RULE" {
		t.Fatalf("second rule = %q, want the should-severity one", e.TopRules[1].RuleID)
	}
}

// The clock the whole column ages against is taken after the queries, not before them: four round
// trips to a hosted store are not free, and stamping up front reported the payload as older than it
// is on a column whose staleness rule is the point.
// The reference point has to be INSIDE the call. An earlier version of this test took the clock
// before invoking the fetch and asserted GeneratedAt was not older than that — which holds whether
// the stamp is taken at the top of the function or the bottom, so it could not fail and tested
// nothing. Reviewed and caught; the fake now records when its last query ran, which is a point the
// two orderings fall on opposite sides of.
func TestEdgeMulticastConformance_TheClockIsTakenAfterTheQueries(t *testing.T) {
	f := &fakeProm{
		delay: time.Millisecond,
		byMetric: map[string][]PromSample{
			"dz_conformance_uptime_seconds": oneValidator("233.84.178.3", "cmh1"),
		},
	}
	got := fetchConformance(t, f)

	if f.lastQueryAt.IsZero() {
		t.Fatal("the fake ran no query; this test would assert nothing")
	}
	if got.GeneratedAt.Before(f.lastQueryAt) {
		t.Fatalf("generated_at = %v, taken before the last query at %v — the payload reports "+
			"itself older than it is", got.GeneratedAt, f.lastQueryAt)
	}
}

// A series with no multicast_group label cannot be attributed to a feed, and guessing is what the
// design refuses: an unlabelled scrape is the state before the target labels roll out, and it must
// cost the column rather than land a verdict on the wrong group.
func TestEdgeMulticastConformance_UnlabelledSeriesIsDropped(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": {sample(1, "hostname", "cmh1")},
		"dz_conformance_checks_total":   {sample(100, "result", "pass")},
	}})

	if len(got.Groups) != 0 {
		t.Fatalf("groups = %v, want none: an unlabelled series was attributed to a feed", got.Groups)
	}
}

// No metrics store is a configuration state, not a failure. It must produce a writable, empty
// payload — a nil error — so the refresher caches "nothing covers anything here" instead of
// logging a failure every cycle in an environment that was never meant to have the data.
func TestEdgeMulticastConformance_NoQuerierIsEmptyAndNotAnError(t *testing.T) {
	api := &API{}
	got, err := api.FetchEdgeMulticastConformance(context.Background())
	if err != nil {
		t.Fatalf("want no error with no querier, got %v", err)
	}
	if got == nil || len(got.Groups) != 0 {
		t.Fatalf("want an empty payload, got %+v", got)
	}
}

// A store that fails must propagate, never degrade to empty. An empty-but-valid payload is written
// over the last good cache entry, so swallowing a blip here would blank the column for a full
// refresh interval with nothing logged — the same contract kalshiTableExists keeps.
func TestEdgeMulticastConformance_AFailedQueryPropagates(t *testing.T) {
	api := &API{Prom: &fakeProm{err: errors.New("token expired")}}
	_, err := api.FetchEdgeMulticastConformance(context.Background())
	// The exact string, wrapper included: which of the four queries failed is the whole value of
	// the message, and an is-error check keeps passing after that is lost. See .cursor/BUGBOT.md.
	const want = "conformance instances: token expired"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// increase() extrapolates to the window edges, so a counter that moved once can report less than
// one. Rounding alone turns a real violation near a window edge into zero, which is a finding the
// page would never show.
func TestPromCount_ASubUnitIncreaseIsStillAnEvent(t *testing.T) {
	tests := []struct {
		in   float64
		want uint64
	}{
		{0, 0},
		{-1, 0},
		{0.3, 1},
		{0.7, 1},
		{1.03, 1},
		{4.6, 5},
	}
	for _, tc := range tests {
		if got := promCount(tc.in); got != tc.want {
			t.Fatalf("promCount(%v) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// A rule id is a finding only to a reader who knows the catalog, so the list carries the
// validator's own summary and spec link, joined by rule id.
func TestEdgeMulticastConformance_RulesCarryTheirCatalogEntry(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": oneValidator("233.84.178.3", "cmh1"),
		"dz_conformance_violations_total": {
			sample(33, "multicast_group", "233.84.178.3", "stream", "kalshi_perps_tob",
				"rule_id", "MSG.WRONG_PORT_PLACEMENT", "severity", "must", "hostname", "cmh1"),
		},
		"dz_conformance_rule_info": {
			sample(1, "rule_id", "MSG.WRONG_PORT_PLACEMENT",
				"summary", "Messages are published on the port their type belongs to",
				"spec_url", "https://example.invalid/spec#msg-wrong-port-placement"),
			sample(1, "rule_id", "SOME.OTHER.RULE", "summary", "not this one", "spec_url", "x"),
		},
	}})

	e := got.Groups["233.84.178.3"]
	if len(e.TopRules) != 1 {
		t.Fatalf("top rules = %d, want 1", len(e.TopRules))
	}
	r := e.TopRules[0]
	if r.Summary != "Messages are published on the port their type belongs to" {
		t.Fatalf("summary = %q, want the catalog entry for this rule", r.Summary)
	}
	if r.SpecURL != "https://example.invalid/spec#msg-wrong-port-placement" {
		t.Fatalf("spec_url = %q, want the catalog entry for this rule", r.SpecURL)
	}
}

// Mid-rollout the same rule is exposed by two builds at once. Picking the first lexicographically
// is arbitrary, and the point is only that it is the SAME one on every refresh — including where
// the two builds agree on the wording and differ on where it points, which the summary alone
// cannot separate.
func TestEdgeMulticastConformance_ACatalogEntryIsPickedTheSameWayEveryTime(t *testing.T) {
	const group = "233.84.178.3"
	catalog := []PromSample{
		sample(1, "rule_id", "MSG.SEQ.GAP", "summary", "Sequence numbers advance without holes",
			"spec_url", "https://example.invalid/spec@v2#msg-seq-gap"),
		sample(1, "rule_id", "MSG.SEQ.GAP", "summary", "Sequence numbers advance without holes",
			"spec_url", "https://example.invalid/spec@v1#msg-seq-gap"),
	}

	// Prometheus makes no ordering promise, so the two orders must agree.
	for _, order := range [][]PromSample{catalog, {catalog[1], catalog[0]}} {
		got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
			"dz_conformance_uptime_seconds": oneValidator(group, "cmh1"),
			"dz_conformance_violations_total": {
				sample(4, "multicast_group", group, "stream", "kalshi_perps_tob",
					"rule_id", "MSG.SEQ.GAP", "severity", "must", "hostname", "cmh1"),
			},
			"dz_conformance_rule_info": order,
		}})

		r := got.Groups[group].TopRules[0]
		if r.SpecURL != "https://example.invalid/spec@v1#msg-seq-gap" {
			t.Fatalf("spec_url = %q, want the first of the two lexicographically whatever order they arrive in", r.SpecURL)
		}
	}
}

// The catalog decorates a finding; it does not decide one. Losing it costs the description and
// nothing else — but it is never lost silently: without the WARN, a fleet whose catalog has been
// unreachable for a week is indistinguishable on the page from one whose rules carry no summary.
//
// Not parallel: swaps the global slog default.
func TestEdgeMulticastConformance_ACatalogFailureDoesNotCostTheVerdict(t *testing.T) {
	var recs []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(levelRecorder{&recs}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	api := &API{Prom: &fakeProm{
		byMetric: map[string][]PromSample{
			"dz_conformance_uptime_seconds": oneValidator("233.84.178.3", "cmh1"),
			"dz_conformance_violations_total": {
				sample(33, "multicast_group", "233.84.178.3", "stream", "kalshi_perps_tob",
					"rule_id", "MSG.WRONG_PORT_PLACEMENT", "severity", "must", "hostname", "cmh1"),
			},
		},
		errByMetric: map[string]error{"dz_conformance_rule_info": errors.New("token expired")},
	}}
	got, err := api.FetchEdgeMulticastConformance(context.Background())
	if err != nil {
		t.Fatalf("a failed catalog lookup took the payload with it: %v", err)
	}
	e := got.Groups["233.84.178.3"]
	if e == nil || e.Verdict != edgeMulticastConformanceViolating || e.Must != 33 {
		t.Fatalf("verdict/counts lost with the catalog: %+v", e)
	}
	if len(e.TopRules) != 1 || e.TopRules[0].RuleID != "MSG.WRONG_PORT_PLACEMENT" {
		t.Fatalf("the rule id must survive with no catalog behind it, got %+v", e.TopRules)
	}
	if e.TopRules[0].Summary != "" {
		t.Fatalf("summary = %q, want empty: nothing described this rule", e.TopRules[0].Summary)
	}

	var warned bool
	for _, r := range recs {
		if r.Message == "edge multicast conformance: rule catalog unavailable, rules render by id alone" {
			if r.Level != slog.LevelWarn {
				t.Fatalf("catalog failure logged at %v, want WARN: it degrades a column, it does not page", r.Level)
			}
			warned = true
		}
	}
	if !warned {
		t.Fatalf("the catalog error was swallowed; logged %d records, none of them the catalog warning", len(recs))
	}
}

// Which vantages saw a rule is a different question from how many the verdict rests on, and the
// group-level node set cannot answer it.
func TestEdgeMulticastConformance_ARuleNamesTheVantagesThatSawIt(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds": {
			sample(1, "multicast_group", "233.84.178.21", "hostname", "cmh1"),
			sample(1, "multicast_group", "233.84.178.21", "hostname", "was1"),
		},
		"dz_conformance_violations_total": {
			sample(4, "multicast_group", "233.84.178.21", "stream", "kalshi_elections_tob_house",
				"rule_id", "MSG.SEQ.GAP", "severity", "must", "hostname", "was1"),
			sample(6, "multicast_group", "233.84.178.21", "stream", "kalshi_elections_tob_senate",
				"rule_id", "MSG.SEQ.GAP", "severity", "must", "hostname", "cmh1"),
		},
	}})

	e := got.Groups["233.84.178.21"]
	r := e.TopRules[0]
	// Detections, summed across vantages — the vantages beside it are what keep it from being
	// read as an event count.
	if r.Count != 10 || e.Must != 10 {
		t.Fatalf("count = %d, must = %d, want 10 detections", r.Count, e.Must)
	}
	if len(r.Nodes) != 2 || r.Nodes[0] != "cmh1" || r.Nodes[1] != "was1" {
		t.Fatalf("nodes = %v, want [cmh1 was1] sorted", r.Nodes)
	}
	// The instance name is the only thing here that narrows a finding below the group.
	if len(r.Validators) != 2 || r.Validators[0] != "kalshi_elections_tob_house" {
		t.Fatalf("validators = %v, want both instances sorted", r.Validators)
	}
}

// A capped list that does not say what it left out reads as the whole finding.
func TestEdgeMulticastConformance_ATruncatedRuleListSaysHowManyFired(t *testing.T) {
	violations := []PromSample{}
	for i := 0; i < edgeMulticastConformanceTopRuleCap+3; i++ {
		violations = append(violations, sample(1, "multicast_group", "233.84.178.3",
			"stream", "kalshi_perps_tob", "rule_id", fmt.Sprintf("RULE.%02d", i),
			"severity", "should", "hostname", "cmh1"))
	}
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"dz_conformance_uptime_seconds":   oneValidator("233.84.178.3", "cmh1"),
		"dz_conformance_violations_total": violations,
	}})

	e := got.Groups["233.84.178.3"]
	if len(e.TopRules) != edgeMulticastConformanceTopRuleCap {
		t.Fatalf("rendered rules = %d, want the cap %d", len(e.TopRules), edgeMulticastConformanceTopRuleCap)
	}
	if e.RulesFired != edgeMulticastConformanceTopRuleCap+3 {
		t.Fatalf("rules_fired = %d, want every rule that fired, not the rendered ones", e.RulesFired)
	}
	if e.Should != uint64(edgeMulticastConformanceTopRuleCap+3) {
		t.Fatalf("should = %d, want the total over every rule including the truncated ones", e.Should)
	}
}

// --- The two grains ---------------------------------------------------------
//
// A finding lands on the publisher line when the metric named a publisher and on the group row
// when it did not. These pin both directions, plus the rollout state in which nothing is named.

// TestEdgeMulticastConformance_AnAttributedViolationLandsOnThePublisher is the case the whole
// change exists for: two publishers on one group, one of them violating, and the verdict naming
// only that one.
func TestEdgeMulticastConformance_AnAttributedViolationLandsOnThePublisher(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.3", "cmh"),
		"violations_total": {
			sample(4, "multicast_group", "233.84.178.3", "rule_id", "FRAME.SEQ_RESET_GAP",
				"severity", "must", "source_addr", "148.51.120.6"),
		},
		"checks_total": {
			sample(4, "multicast_group", "233.84.178.3", "result", "violation",
				"source_addr", "148.51.120.6"),
			sample(900, "multicast_group", "233.84.178.3", "result", "pass",
				"source_addr", "148.51.120.6"),
			sample(900, "multicast_group", "233.84.178.3", "result", "pass",
				"source_addr", "148.51.120.152"),
		},
	}})

	pubs := got.Publishers["233.84.178.3"]
	if len(pubs) != 2 {
		t.Fatalf("publisher entries: want 2, got %d", len(pubs))
	}

	bad := pubs["148.51.120.6"]
	if bad == nil {
		t.Fatal("no entry for the violating publisher")
	}
	if bad.Verdict != edgeMulticastConformanceViolating {
		t.Errorf("violating publisher: verdict %q, want %q", bad.Verdict, edgeMulticastConformanceViolating)
	}
	if bad.Must != 4 {
		t.Errorf("violating publisher: must %d, want 4", bad.Must)
	}

	// The peer did nothing wrong and must not inherit the fault. This is the reading the group
	// row could never give: one badge over two paths describes neither.
	good := pubs["148.51.120.152"]
	if good == nil {
		t.Fatal("no entry for the clean publisher")
	}
	if good.Verdict != edgeMulticastConformanceConforming {
		t.Errorf("clean publisher: verdict %q, want %q", good.Verdict, edgeMulticastConformanceConforming)
	}
	if good.Must != 0 {
		t.Errorf("clean publisher: must %d, want 0 — its peer's violation reached it", good.Must)
	}

	// And the group itself carries none of it: every finding here named a path.
	g := got.Groups["233.84.178.3"]
	if g == nil {
		t.Fatal("the group entry is missing, so the lines have no row above them")
	}
	if g.Must != 0 {
		t.Errorf("group: must %d, want 0 — an attributed finding was also charged to the group", g.Must)
	}
	// It must say NOTHING, not `ungraded`. Its own counters are zero because the split worked,
	// and `ungraded` means "the validator graded nothing" — which would render "nothing reached
	// a verdict" directly above a line reading conforming over 900 passed checks.
	if g.Verdict != "" {
		t.Errorf("group verdict %q, want none: every finding named a path, so the row has nothing to say at its own grain",
			g.Verdict)
	}
}

// TestEdgeMulticastConformance_AChannelScopedViolationStaysOnTheGroup is the other direction. A
// rule decided over state every path fills carries no address, and putting it on a line would name
// a publisher for something its peer did as much of.
func TestEdgeMulticastConformance_AChannelScopedViolationStaysOnTheGroup(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.4", "cmh"),
		"violations_total": {
			sample(2, "multicast_group", "233.84.178.4", "rule_id", "REFDATA.NEVER_REACHES_READY",
				"severity", "must", "source_addr", ""),
		},
		"checks_total": {
			sample(2, "multicast_group", "233.84.178.4", "result", "violation", "source_addr", ""),
			sample(10, "multicast_group", "233.84.178.4", "result", "pass", "source_addr", ""),
		},
	}})

	if n := len(got.Publishers["233.84.178.4"]); n != 0 {
		t.Errorf("publisher entries: want 0, got %d — a channel-scoped finding was attributed", n)
	}
	g := got.Groups["233.84.178.4"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if g.Must != 2 {
		t.Errorf("group: must %d, want 2", g.Must)
	}
	if g.Verdict != edgeMulticastConformanceViolating {
		t.Errorf("group: verdict %q, want %q", g.Verdict, edgeMulticastConformanceViolating)
	}
}

// TestEdgeMulticastConformance_AnUnlabelledFleetRendersAsItDidBefore pins the rollout state.
// Until the validators carry source_addr every series reports it empty, so every finding lands on
// the group row and the payload is the one this page has always rendered. Lake ships first.
func TestEdgeMulticastConformance_AnUnlabelledFleetRendersAsItDidBefore(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.21", "cmh"),
		"violations_total": {
			// No source_addr label at all, which is what an old build scrapes as.
			sample(3, "multicast_group", "233.84.178.21", "rule_id", "MSG.WRONG_PORT_PLACEMENT",
				"severity", "must"),
		},
		"checks_total": {
			sample(3, "multicast_group", "233.84.178.21", "result", "violation"),
			sample(50, "multicast_group", "233.84.178.21", "result", "pass"),
		},
	}})

	if got.Publishers != nil && len(got.Publishers["233.84.178.21"]) != 0 {
		t.Errorf("publisher entries: want none on an unlabelled fleet, got %d",
			len(got.Publishers["233.84.178.21"]))
	}
	g := got.Groups["233.84.178.21"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if g.Must != 3 || g.Passes != 50 {
		t.Errorf("group: must=%d passes=%d, want 3 and 50", g.Must, g.Passes)
	}
	if g.Verdict != edgeMulticastConformanceViolating {
		t.Errorf("group: verdict %q, want %q", g.Verdict, edgeMulticastConformanceViolating)
	}
}

// TestEdgeMulticastConformance_APublisherEntryAlwaysHasItsGroup: the group row carries the context
// every line's tooltip reads — which validators ran, at how many vantages, on what build — so a
// publisher entry with no group behind it would render a verdict from nowhere.
func TestEdgeMulticastConformance_APublisherEntryAlwaysHasItsGroup(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		// Deliberately no uptime sample for this group: the violation alone creates it.
		"violations_total": {
			sample(1, "multicast_group", "233.84.178.20", "rule_id", "FRAME.SEQ_RESET_GAP",
				"severity", "must", "source_addr", "148.51.120.6"),
		},
	}})

	if got.Groups["233.84.178.20"] == nil {
		t.Fatal("a publisher entry was created with no group entry behind it")
	}
	if got.Publishers["233.84.178.20"]["148.51.120.6"] == nil {
		t.Fatal("the publisher entry is missing")
	}
}

// TestEdgeMulticastConformance_AnExemptionIsChargedToTheGroup: an exemption is keyed on
// (stream, rule) and is a statement about a known deviation of the FEED. Counting it per line would
// make one waiver read differently on each path.
func TestEdgeMulticastConformance_AnExemptionIsChargedToTheGroup(t *testing.T) {
	restore := edgeMulticastConformanceExemptions
	edgeMulticastConformanceExemptions = []edgeMulticastConformanceExemption{
		{stream: "kalshi_perps_tob", ruleID: "MSG.WRONG_PORT_PLACEMENT", why: "test"},
	}
	t.Cleanup(func() { edgeMulticastConformanceExemptions = restore })

	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.3", "cmh"),
		"violations_total": {
			sample(7, "multicast_group", "233.84.178.3", "stream", "kalshi_perps_tob",
				"rule_id", "MSG.WRONG_PORT_PLACEMENT", "severity", "must",
				"source_addr", "148.51.120.6"),
		},
	}})

	g := got.Groups["233.84.178.3"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if g.Exempted != 7 {
		t.Errorf("group exempted: %d, want 7", g.Exempted)
	}
	if g.Must != 0 {
		t.Errorf("group must: %d, want 0 — an exempted violation still graded", g.Must)
	}
	// No entry at all, not an empty one. An entry with nothing in it grades `ungraded`, which
	// would put "nothing reached a verdict" on a line whose only finding was a deliberate waiver.
	if pub := got.Publishers["233.84.178.3"]["148.51.120.6"]; pub != nil {
		t.Errorf("an exempted violation minted a publisher entry (verdict %q, must %d); it should mint none",
			pub.Verdict, pub.Must)
	}
}

// TestEdgeMulticastConformance_ChannelsStayOnTheGroup: the channels a run covered describe the
// group's coverage, not one path's, and the group row is where the tooltip names them.
func TestEdgeMulticastConformance_ChannelsStayOnTheGroup(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.3", "cmh"),
		"checks_total": {
			sample(10, "multicast_group", "233.84.178.3", "result", "pass",
				"channel", "1", "source_addr", "148.51.120.6"),
			sample(10, "multicast_group", "233.84.178.3", "result", "pass",
				"channel", "101", "source_addr", "148.51.120.152"),
		},
	}})

	g := got.Groups["233.84.178.3"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if len(g.Channels) != 2 {
		t.Errorf("group channels: %v, want both", g.Channels)
	}
	for ip, pub := range got.Publishers["233.84.178.3"] {
		if len(pub.Channels) != 0 {
			t.Errorf("publisher %s carries channels %v, want none", ip, pub.Channels)
		}
	}
}

// TestEdgeMulticastConformance_AGroupThatGradedNothingAtAllIsStillUngraded is the other half of
// the suppression above, and the reason it is conditional. A validator that runs and grades
// nothing is the state `ungraded` exists to catch, and it must survive the split: here there are
// no publisher entries to have taken the findings, so the absence is real.
func TestEdgeMulticastConformance_AGroupThatGradedNothingAtAllIsStillUngraded(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.22", "cmh"),
		"checks_total": {
			sample(1000, "multicast_group", "233.84.178.22", "result", "na"),
		},
	}})

	g := got.Groups["233.84.178.22"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if g.Verdict != edgeMulticastConformanceUngraded {
		t.Errorf("group verdict %q, want %q: nothing graded this group and no line took the findings",
			g.Verdict, edgeMulticastConformanceUngraded)
	}
}

// TestEdgeMulticastConformance_AFlatCounterMintsNoPublisherEntry: with source_addr in the
// by-clause, a checks_total series flat across the window is a path the validator saw no
// datagrams from. Minting an entry for it grades `ungraded` and puts a badge on a line over
// nothing at all.
func TestEdgeMulticastConformance_AFlatCounterMintsNoPublisherEntry(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.3", "cmh"),
		"checks_total": {
			sample(900, "multicast_group", "233.84.178.3", "result", "pass",
				"source_addr", "148.51.120.6"),
			// Flat: increase() over the window is zero.
			sample(0, "multicast_group", "233.84.178.3", "result", "pass",
				"source_addr", "148.51.120.152"),
		},
	}})

	pubs := got.Publishers["233.84.178.3"]
	if _, ok := pubs["148.51.120.152"]; ok {
		t.Errorf("a flat series minted a publisher entry (verdict %q)", pubs["148.51.120.152"].Verdict)
	}
	if pubs["148.51.120.6"] == nil {
		t.Fatal("the publisher that did grade something has no entry")
	}
}

// TestCountUnattributedConformance pins what the group counts when a verdict names an address no
// line carries: counted rather than dropped, and a line with no tunnel address is not a wildcard
// that absorbs every unmatched verdict.
func TestCountUnattributedConformance(t *testing.T) {
	lines := []EdgeMulticastPublisher{
		{UserPK: "a", DZIP: "148.51.120.6"},
		{UserPK: "b", DZIP: ""}, // ledger has no tunnel address for this one
	}
	byPub := map[string]*EdgeMulticastConformance{
		"148.51.120.6":   {},
		"148.51.120.152": {}, // no line carries this
	}
	if got := countUnattributedConformance(lines, byPub); got != 1 {
		t.Errorf("unattributed: %d, want 1", got)
	}
	if got := countUnattributedConformance(lines, nil); got != 0 {
		t.Errorf("unattributed with no publisher entries: %d, want 0", got)
	}
}

// TestEdgeMulticastConformance_AGroupWithNothingConclusiveIsAlsoSilent is the second shape of the
// suppression, and the one an earlier `Graded == 0` test missed.
//
// `ungraded` is reached on `Passes == 0 && Info == 0`, which is NOT the same as having graded
// nothing: a group whose channel-scoped checks all come back `na` or `unverifiable` has
// `Graded > 0` and still asserts "nothing reached a verdict" — directly above lines that did reach
// one. This is the expected steady state after the split, not a corner case: the rules left on the
// group row are the book and reference-data ones, which decline most of their opportunities on a
// healthy feed.
func TestEdgeMulticastConformance_AGroupWithNothingConclusiveIsAlsoSilent(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.4", "cmh"),
		"checks_total": {
			// The group's own rules ran and concluded nothing.
			sample(4000, "multicast_group", "233.84.178.4", "result", "na"),
			sample(120, "multicast_group", "233.84.178.4", "result", "unverifiable"),
			// A publisher of it did reach a verdict.
			sample(900, "multicast_group", "233.84.178.4", "result", "pass",
				"source_addr", "148.51.120.6"),
		},
	}})

	g := got.Groups["233.84.178.4"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if g.Graded == 0 {
		t.Fatal("the group graded nothing, so this is not the shape under test")
	}
	if g.Verdict != "" {
		t.Errorf("group verdict %q, want none: its own checks concluded nothing while a publisher reached a verdict, so %q would sit above a line reading %q",
			g.Verdict, edgeMulticastConformanceUngraded, edgeMulticastConformanceConforming)
	}
	if pub := got.Publishers["233.84.178.4"]["148.51.120.6"]; pub == nil {
		t.Fatal("the publisher entry is missing")
	} else if pub.Verdict != edgeMulticastConformanceConforming {
		t.Errorf("publisher verdict %q, want %q", pub.Verdict, edgeMulticastConformanceConforming)
	}
}

// TestEdgeMulticastConformance_NothingConclusiveAnywhereStaysUngraded is the guard on the guard.
// Where no publisher reached a verdict either, nothing was concluded anywhere and the absence is
// real — which is the signal `ungraded` exists for and must survive the suppression.
func TestEdgeMulticastConformance_NothingConclusiveAnywhereStaysUngraded(t *testing.T) {
	got := fetchConformance(t, &fakeProm{byMetric: map[string][]PromSample{
		"uptime_seconds": oneValidator("233.84.178.4", "cmh"),
		"checks_total": {
			sample(4000, "multicast_group", "233.84.178.4", "result", "na"),
			// A publisher series that also concluded nothing must not rescue the group.
			sample(500, "multicast_group", "233.84.178.4", "result", "na",
				"source_addr", "148.51.120.6"),
		},
	}})

	g := got.Groups["233.84.178.4"]
	if g == nil {
		t.Fatal("no group entry")
	}
	if g.Verdict != edgeMulticastConformanceUngraded {
		t.Errorf("group verdict %q, want %q: nothing reached a verdict anywhere on this group",
			g.Verdict, edgeMulticastConformanceUngraded)
	}
}
