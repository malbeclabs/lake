package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// The conformance plane: what dz-conformance graded on each feed, read over PromQL.
//
// # Where this data lives, and why it is not a query against ClickHouse
//
// dz-conformance validates a feed against the edge-feed-spec rule catalog and reports through
// three sinks — slog, a --json-report file, and Prometheus metrics on loopback. There is no
// database sink. Alloy scrapes the metrics and remote-writes them to Grafana Cloud, so the only
// way to read a verdict today is PromQL. Nothing about conformance exists in ClickHouse.
//
// # The grain, which is forced rather than chosen
//
// This column renders on the GROUP ROW and never on the publisher lines, because nothing in this
// payload can name a path. The Prometheus label set carries the rule, the severity, the result and
// the scrape target's identity; it does not carry the publisher's source address. Neither does
// core.Finding upstream — it has ChannelID and no source IP — even though the engine holds the
// address and keys its own frame state on the full channel instance.
//
// That is a property of THIS SOURCE and not of the page. recorder.conformance_finding in
// malbeclabs/edge-multicast-ref keys on (source_addr, channel_id, dst_port), so when those rows
// land the same struct hangs off EdgeMulticastPublisher with source_addr as the join and the
// column moves to the lines with no change to the vocabulary below. See
// docs/superpowers/specs/2026-09-08-edge-multicast-conformance-column-design.md.
//
// # What the column must never claim
//
// A green badge here means "nothing was found wrong in what was graded", which is weaker than "this
// feed conforms". Two things make the difference material and both are carried in the payload
// rather than left to the reader: the coverage actually achieved (Passes over Graded), and the
// known deviations that are excluded from the verdict by name.
const edgeMulticastConformanceCacheKey = "edge_multicast_conformance:v1"

// The window every query below reads. Fifteen minutes, matching the other folds on this page so a
// reader comparing the Conformance and Sequence columns is comparing the same span. At Alloy's
// default 60s scrape that is 15 samples per series.
const edgeMulticastConformanceWindow = "15m"

// The environment matcher. The refresher runs with no environment in its context and always
// computes mainnet, exactly like the other two folded payloads — but unlike them this leg can say
// so to the store instead of inferring it, because the metrics carry the label. The other folds
// resolve through the multicast address, and both networks allocate out of the same
// 233.84.178.0/24.
const edgeMulticastConformanceEnv = "mainnet-beta"

// Verdicts, worst first. The order is the whole design: see edgeMulticastConformanceVerdict.
const (
	edgeMulticastConformanceViolating  = "violating"
	edgeMulticastConformanceShould     = "should"
	edgeMulticastConformanceUngraded   = "ungraded"
	edgeMulticastConformanceAdvisory   = "advisory"
	edgeMulticastConformanceConforming = "conforming"
)

// edgeMulticastConformanceExemption is one known publisher deviation, excluded from the verdict by
// exact (instance, rule) pair.
//
// **The list is empty, and that is the finding rather than an omission.**
//
// It was written from §4.3 of the 2026-08-19 conformance deploy design, which PROPOSED a PromQL
// `unless` over two (stream, rule) pairs. What actually shipped is different, and checking the
// deployed alerts against live metrics is what caught it:
//
//   - infra:grafana/alerts/dz-conformance-must-violation.json is
//     `increase(dz_conformance_violations_total{severity="must"}[5m])` with **no unless clause at
//     all**. Every must-severity violation pages, including MSG.WRONG_PORT_PLACEMENT on the
//     top-of-book feed and MBP.SNAP.RECONSTRUCTED_BOOK_MATCHES_SNAPSHOT on the book feeds.
//   - infra:grafana/alerts/dz-conformance-coverage-loss.json does carry an exemption for
//     MBP.SNAP.RECONSTRUCTED_BOOK_MATCHES_SNAPSHOT — but on dz_conformance_unverifiable_total,
//     with reason pending|cold_start, and over nine streams rather than two. That is the coverage
//     question, not the violation one, and this column reports coverage as a ratio rather than
//     alerting on it, so it needs no exemption to state it correctly.
//
// Exempting a violation here that the deployed alert pages on would make this column read
// `conforming` over a feed on-call is being woken for. **The page must never be quieter than the
// alert**, and it is the page that has to move: the alert is what somebody is carrying a phone for.
//
// So an entry here is only correct alongside a matching exemption in the deployed must-violation
// alert, added in the same change and for the same stated reason. If the alert pages on it, this
// column says so too.
//
// The mechanism stays because the case is real — a genuine known deviation should be excludable
// from both at once — and because emptying a list is a smaller change to review than deleting one
// and restoring it later. Hits are counted into Exempted and rendered either way, so an exemption
// that stops firing, or one that starts firing on an instance it was never granted for, stays
// visible rather than silent.
type edgeMulticastConformanceExemption struct {
	stream string
	ruleID string
	why    string
}

var edgeMulticastConformanceExemptions []edgeMulticastConformanceExemption

// edgeMulticastConformanceExempt reports whether this exact (instance, rule) pair is a known
// deviation.
//
// **Named instances, never a prefix.** The top-of-book deviation is not shared by the
// market-by-price feed, which does the opposite and keeps a regression test asserting it, so a
// prefix matcher would blind a must-severity rule in precisely the place the publisher works to
// stay correct.
func edgeMulticastConformanceExempt(stream, ruleID string) bool {
	for _, e := range edgeMulticastConformanceExemptions {
		if e.stream == stream && e.ruleID == ruleID {
			return true
		}
	}
	return false
}

// EdgeMulticastConformanceRule is one rule that fired, for the tooltip.
type EdgeMulticastConformanceRule struct {
	RuleID   string `json:"rule_id"`
	Severity string `json:"severity"`
	Count    uint64 `json:"count"`
}

// EdgeMulticastConformance is what the rule set graded on one group over the window.
//
// Per GROUP, for the reason at the top of this file. Counts are events in the window, not a
// running total: the queries read increase() over it.
type EdgeMulticastConformance struct {
	Verdict string `json:"verdict"`

	// Violations by severity, after the known-deviation exclusions.
	Must   uint64 `json:"must"`
	Should uint64 `json:"should"`
	Info   uint64 `json:"info"`

	// The graded denominator and its parts. Graded is the sum over every result, so
	// Passes/Graded is the coverage actually achieved rather than an assumption that silence
	// means clean.
	Passes       uint64 `json:"passes"`
	Graded       uint64 `json:"graded"`
	NA           uint64 `json:"na"`
	Unverifiable uint64 `json:"unverifiable"`

	// TopRules are the rules that fired, worst and loudest first, capped for the tooltip.
	TopRules []EdgeMulticastConformanceRule `json:"top_rules,omitempty"`

	// Exempted counts known-deviation hits. Excluded from the verdict, never from the payload.
	Exempted uint64 `json:"exempted"`

	// Instances is how many validator processes stand behind this verdict and Nodes is at how
	// many recorders they run. One vantage is the normal state on some groups, and a verdict
	// from one recorder cannot separate that recorder's own trouble from the feed's.
	Instances int      `json:"instances"`
	Nodes     []string `json:"nodes,omitempty"`

	// Channels is the set of Channel IDs graded, when the scrape carries the label. Empty is
	// "the scrape does not say", never "no channels" — a group whose validators predate the
	// label still has a verdict, it just cannot enumerate what it covered.
	Channels []string `json:"channels,omitempty"`

	// Versions is the validator build behind the verdict. More than one is a legitimate state
	// mid-rollout and the page shows them all rather than picking.
	Versions []string `json:"versions,omitempty"`
}

// EdgeMulticastConformanceResponse is the cached payload: one entry per multicast group address,
// plus the clock the column ages against.
type EdgeMulticastConformanceResponse struct {
	GeneratedAt time.Time `json:"generated_at"`
	Window      string    `json:"window"`

	// Groups is keyed on the multicast group ADDRESS, which is what the scrape target carries
	// and what the page joins on. Not the ledger code: a code that stops matching its live group
	// fails silently, and this one has been renamed once already.
	Groups map[string]*EdgeMulticastConformance `json:"groups"`
}

// edgeMulticastConformanceTopRuleCap bounds the tooltip's rule list. A feed violating dozens of
// rules at once has one story and it is not told better by naming all of them.
const edgeMulticastConformanceTopRuleCap = 6

// FetchEdgeMulticastConformance reads the conformance verdicts for every validated group.
//
// Four instant queries, folded into one entry per group. A nil querier returns an empty payload
// and no error: an environment with no Grafana credentials has no conformance data, which is a
// configuration state and not a failure.
func (a *API) FetchEdgeMulticastConformance(ctx context.Context) (*EdgeMulticastConformanceResponse, error) {
	out := &EdgeMulticastConformanceResponse{
		GeneratedAt: time.Now().UTC(),
		Window:      edgeMulticastConformanceWindow,
		Groups:      map[string]*EdgeMulticastConformance{},
	}
	if a.Prom == nil {
		return out, nil
	}

	w := edgeMulticastConformanceWindow
	env := edgeMulticastConformanceEnv

	// Which validators are running, and at how many vantages. It runs FIRST so that a validator
	// with nothing to report yet still reaches the payload: a group whose process just started
	// has no findings and no checks, and it has to render as "graded nothing" rather than
	// vanish. Every query below also creates the entry it needs, so the group set is their
	// union — what this one adds is the groups the other three would never mention.
	instances, err := a.Prom.Query(ctx, fmt.Sprintf(
		`count by (multicast_group, hostname) (dz_conformance_uptime_seconds{env=%q})`, env))
	if err != nil {
		return nil, fmt.Errorf("conformance instances: %w", err)
	}
	for _, s := range instances {
		g := s.Label("multicast_group")
		if g == "" {
			continue
		}
		e := out.group(g)
		e.Instances += int(promCount(s.Value))
		if h := s.Label("hostname"); h != "" {
			e.Nodes = appendUnique(e.Nodes, h)
		}
	}

	violations, err := a.Prom.Query(ctx, fmt.Sprintf(
		`sum by (multicast_group, stream, rule_id, severity, channel) (increase(dz_conformance_violations_total{env=%q}[%s]))`,
		env, w))
	if err != nil {
		return nil, fmt.Errorf("conformance violations: %w", err)
	}
	rules := map[string]map[string]*EdgeMulticastConformanceRule{}
	for _, s := range violations {
		g := s.Label("multicast_group")
		if g == "" {
			continue
		}
		n := promCount(s.Value)
		if n == 0 {
			continue
		}
		e := out.group(g)
		if edgeMulticastConformanceExempt(s.Label("stream"), s.Label("rule_id")) {
			e.Exempted += n
			continue
		}
		switch strings.ToLower(s.Label("severity")) {
		case "must":
			e.Must += n
		case "should":
			e.Should += n
		default:
			// Every other severity the catalog carries is advisory. Folded together on
			// purpose: the column's job is to separate "something is wrong" from "something
			// was noted", and the rule id in the tooltip is where the detail belongs.
			e.Info += n
		}
		if rules[g] == nil {
			rules[g] = map[string]*EdgeMulticastConformanceRule{}
		}
		id := s.Label("rule_id")
		if r, ok := rules[g][id]; ok {
			r.Count += n
		} else {
			rules[g][id] = &EdgeMulticastConformanceRule{
				RuleID: id, Severity: strings.ToLower(s.Label("severity")), Count: n,
			}
		}
	}

	checks, err := a.Prom.Query(ctx, fmt.Sprintf(
		`sum by (multicast_group, result, channel) (increase(dz_conformance_checks_total{env=%q}[%s]))`,
		env, w))
	if err != nil {
		return nil, fmt.Errorf("conformance checks: %w", err)
	}
	for _, s := range checks {
		g := s.Label("multicast_group")
		if g == "" {
			continue
		}
		n := promCount(s.Value)
		e := out.group(g)
		e.Graded += n
		switch strings.ToLower(s.Label("result")) {
		case "pass":
			e.Passes += n
		case "na":
			e.NA += n
		case "unverifiable":
			e.Unverifiable += n
		}
		if ch := s.Label("channel"); ch != "" {
			e.Channels = appendUnique(e.Channels, ch)
		}
	}

	builds, err := a.Prom.Query(ctx, fmt.Sprintf(
		`count by (multicast_group, version) (dz_conformance_build_info{env=%q})`, env))
	if err != nil {
		return nil, fmt.Errorf("conformance builds: %w", err)
	}
	for _, s := range builds {
		g := s.Label("multicast_group")
		if g == "" {
			continue
		}
		if v := s.Label("version"); v != "" {
			out.group(g).Versions = appendUnique(out.group(g).Versions, v)
		}
	}

	for g, e := range out.Groups {
		if rs := rules[g]; len(rs) > 0 {
			e.TopRules = topConformanceRules(rs)
		}
		sort.Strings(e.Nodes)
		sort.Strings(e.Channels)
		sort.Strings(e.Versions)
		e.Verdict = edgeMulticastConformanceVerdict(e)
	}
	return out, nil
}

// group returns the entry for a multicast address, creating it on first use.
func (r *EdgeMulticastConformanceResponse) group(addr string) *EdgeMulticastConformance {
	if r.Groups == nil {
		r.Groups = map[string]*EdgeMulticastConformance{}
	}
	if e, ok := r.Groups[addr]; ok {
		return e
	}
	e := &EdgeMulticastConformance{}
	r.Groups[addr] = e
	return e
}

// edgeMulticastConformanceVerdict grades one group, worst first.
//
// The order, and what each test is protecting:
//
//  1. A must-violation is the finding. Nothing outranks it.
//  2. A should-violation is a finding of lower severity, and still a finding.
//  3. **Nothing passed.** This is the test that earns its place. A validator can run, scrape
//     cleanly and grade nothing at all — every rule reporting `na` because the state it needs was
//     never reached — and report zero violations while doing it. Measured on a recorder running a
//     since-superseded build: 157,504 `na` and zero `pass` over 35 minutes, with one info-severity
//     rule as the only thing being graded on the feed. Not one must-severity check ran, and every
//     counter an operator watches read healthy. A verdict that cannot separate "clean" from
//     "nothing was evaluated" renders that green.
//  4. An info finding with passes behind it is worth surfacing and is not a fault.
//  5. Otherwise it graded something and found nothing wrong — which is the whole claim.
//
// The test at 3 is deliberately `Passes == 0` and not a coverage floor. A floor would fire on the
// market-by-price snapshot rule, which declines the large majority of its transitions on a healthy
// feed — roughly a quarter graded — so a coverage threshold anywhere near that would paint a
// working feed permanently amber over a property of the check rather than of the feed.
func edgeMulticastConformanceVerdict(e *EdgeMulticastConformance) string {
	switch {
	case e.Must > 0:
		return edgeMulticastConformanceViolating
	case e.Should > 0:
		return edgeMulticastConformanceShould
	case e.Passes == 0:
		return edgeMulticastConformanceUngraded
	case e.Info > 0:
		return edgeMulticastConformanceAdvisory
	default:
		return edgeMulticastConformanceConforming
	}
}

// EdgeMulticastConformanceFaulted reports whether a verdict is a finding, for the UI's colour.
// Read rather than re-derived, so the badge and the row cannot disagree about it.
func EdgeMulticastConformanceFaulted(verdict string) bool {
	return verdict == edgeMulticastConformanceViolating || verdict == edgeMulticastConformanceShould
}

// topConformanceRules orders the rules that fired: must before should before the rest, and within
// a severity the loudest first, with the rule id as the tiebreak so the list is stable between
// refreshes.
func topConformanceRules(rs map[string]*EdgeMulticastConformanceRule) []EdgeMulticastConformanceRule {
	out := make([]EdgeMulticastConformanceRule, 0, len(rs))
	for _, r := range rs {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := conformanceSeverityRank(out[i].Severity), conformanceSeverityRank(out[j].Severity)
		if si != sj {
			return si < sj
		}
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].RuleID < out[j].RuleID
	})
	if len(out) > edgeMulticastConformanceTopRuleCap {
		out = out[:edgeMulticastConformanceTopRuleCap]
	}
	return out
}

func conformanceSeverityRank(s string) int {
	switch strings.ToLower(s) {
	case "must":
		return 0
	case "should":
		return 1
	default:
		return 2
	}
}

// appendUnique appends v when the slice does not already carry it.
func appendUnique(xs []string, v string) []string {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}

// edgeMulticastConformanceFold reads the cached payload and returns the per-group verdicts with
// the clock they were computed at.
//
// Nil map on a miss or a shape mismatch, the same contract the sequence and observations folds
// carry: this signal is additive to the page and must not be able to fail it. A miss is not a rare
// state either — page_cache survives a pod restart, so a newly added key is empty from the deploy
// until the refresh chain first reaches it.
//
// Its own clock, never SequenceAsOf or ObservationsAsOf. Those are different entries written by
// different legs, and borrowing one would age this column against a payload it does not come from.
func (a *API) edgeMulticastConformanceFold(ctx context.Context) (map[string]*EdgeMulticastConformance, time.Time) {
	data, err := a.readPageCache(ctx, edgeMulticastConformanceCacheKey)
	if err != nil {
		return nil, time.Time{}
	}
	var payload EdgeMulticastConformanceResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast conformance: cache did not parse", "error", err)
		return nil, time.Time{}
	}
	if len(payload.Groups) == 0 {
		// An empty payload is a real answer — no validator covers anything in this
		// environment — but it carries no per-group facts, so there is nothing to age and the
		// column has nothing to dim. Returning a zero clock keeps the header from claiming a
		// freshness for cells that do not exist.
		return nil, time.Time{}
	}
	return payload.Groups, payload.GeneratedAt.UTC()
}
