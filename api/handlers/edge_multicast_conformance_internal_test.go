package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeProm answers a query by matching a substring of it, so a test names the metric it is
// standing in for rather than reproducing the whole PromQL string.
type fakeProm struct {
	byMetric map[string][]PromSample
	err      error
}

func (f *fakeProm) Query(_ context.Context, query string) ([]PromSample, error) {
	if f.err != nil {
		return nil, f.err
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
		{"nothing passed outranks an info finding", EdgeMulticastConformance{Info: 5, Passes: 0, Graded: 500, NA: 500}, edgeMulticastConformanceUngraded},
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
