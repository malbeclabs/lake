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
// # The grain: two of them, and the metric says which
//
// A finding sits on the PUBLISHER LINE when it names a publisher and on the GROUP ROW when it does
// not, and the `source_addr` label is what decides. dz-conformance sets it for a rule whose subject
// is one channel instance — "one path's view of one channel" — and leaves it empty for a rule
// decided over state every path of the channel fills: the book, the snapshot groups, the reference
// data set. See core.StateKind.InstanceScoped in edge-feed-spec.
//
// **An empty source_addr is a statement, not a missing value**, and it must never be resolved by
// picking whichever publisher was nearby. A validator subscribes to a GROUP, so it receives every
// path merged; a REFDATA.NEVER_REACHES_READY verdict is settled by a set both paths contributed to,
// and charging it to the path that happened to deliver the settling datagram would name a publisher
// for something its peer did as much of.
//
// # This code predates the label reaching production, on purpose
//
// Until the validators carry the new label every series reports an empty `source_addr`, every
// finding lands on the group row, and the page renders exactly as it did before. That is the
// designed rollout: lake can ship first, and lines light up as the fleet rolls. It is also why the
// cache key is NOT bumped — a payload written by the previous build has no publisher entries, which
// degrades to the old rendering rather than to a dark column.
//
// The successor is still recorder.conformance_finding in malbeclabs/edge-multicast-ref, whose rows
// key on (source_addr, channel_id, dst_port) and would reach the same two grains from a table. See
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

// conformanceRuleDoc is the catalog's description of a rule, from dz_conformance_rule_info.
type conformanceRuleDoc struct {
	summary string
	specURL string
}

// EdgeMulticastConformanceRule is one rule that fired, with what the page needs to say what it
// was. `MSG.WRONG_PORT_PLACEMENT ×33` names something only to a reader who knows the catalog.
type EdgeMulticastConformanceRule struct {
	RuleID   string `json:"rule_id"`
	Severity string `json:"severity"`
	Count    uint64 `json:"count"`

	// From the validator's own catalog, joined by rule id. Read rather than restated here: a
	// copy goes stale the first time a rule is reworded. Empty renders as the id alone.
	Summary string `json:"summary,omitempty"`
	SpecURL string `json:"spec_url,omitempty"`

	// Where it fired. Validators is the Alloy `stream` label, the dz-conformance instance name —
	// the only thing here that narrows a finding below the group, since the six elections
	// instances grade ONE address, one market each. Superseded by a `channel` label if it lands.
	Nodes      []string `json:"nodes,omitempty"`
	Validators []string `json:"validators,omitempty"`
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

	// TopRules are the rules that fired, worst and loudest first, capped.
	TopRules []EdgeMulticastConformanceRule `json:"top_rules,omitempty"`

	// RulesFired counts the rules before the cap. Without it a capped list reads as the whole
	// finding and the severity totals above would not reconcile with the rules under them.
	RulesFired int `json:"rules_fired,omitempty"`

	// Exempted counts known-deviation hits. Excluded from the verdict, never from the payload.
	Exempted uint64 `json:"exempted"`

	// Unattributed is how many publisher verdicts named an address no line on this group
	// carries, so they have no row to sit on. Counted rather than dropped, the same as a
	// recorded series whose address matches no publisher: a finding from an address the ledger
	// does not know about is a finding about something, and silently discarding it would make
	// the page quieter than the feed.
	//
	// Group entry only — a publisher entry is by definition attributed.
	Unattributed int `json:"unattributed,omitempty"`

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
	//
	// What it holds is the channel-scoped half: the findings no publisher owns, plus the context
	// that is per group either way — which validators ran, at how many vantages, on what build.
	Groups map[string]*EdgeMulticastConformance `json:"groups"`

	// Publishers is the attributed half, group address to publisher source address. Absent for a
	// group whose validators predate the label, which is the rollout state described above.
	Publishers map[string]map[string]*EdgeMulticastConformance `json:"publishers,omitempty"`
}

// edgeMulticastConformanceTopRuleCap bounds the rendered rule list. A feed violating dozens of
// rules at once has one story and naming all of them does not tell it better. Ten rather than the
// six it held as a tooltip line: it now cuts where a reader stops reading, and RulesFired says
// what was cut.
const edgeMulticastConformanceTopRuleCap = 10

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
	// Re-stamped after the queries below; set here so the empty-payload return is not zero-valued.
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
	// Samples carrying no multicast_group cannot be attributed to a feed and are dropped rather
	// than guessed at. Counted, because dropping ALL of them is a live and expected state — the
	// scrape-target label is a separate deploy — and it produces an empty payload, a false
	// showConformance and no column at all, which is pixel-identical to "no metrics store
	// configured" and to "no validator covers anything". One line separates the three.
	var dropped, seen int

	instances, err := a.Prom.Query(ctx, fmt.Sprintf(
		`count by (multicast_group, hostname) (dz_conformance_uptime_seconds{env=%q})`, env))
	if err != nil {
		return nil, fmt.Errorf("conformance instances: %w", err)
	}
	for _, s := range instances {
		g := s.Label("multicast_group")
		seen++
		if g == "" {
			dropped++
			continue
		}
		e := out.group(g)
		e.Instances += int(promCount(s.Value))
		if h := s.Label("hostname"); h != "" {
			e.Nodes = appendUnique(e.Nodes, h)
		}
	}

	// **The counts are DETECTIONS, not events**, and `hostname` is in the by-clause so a rule can
	// say at which vantages. Recorders grade the feed independently, so one violation in a wire
	// format is counted once per recorder that saw it; nothing in this plane can tell a shared
	// violation from two. Summing never under-reports, and the vantages travel with the number.
	// The group-level Nodes set cannot answer this: it says what the VERDICT rests on.
	//
	// Splitting by vantage can move a total by one or two, since promCount floors a sub-unit
	// extrapolation at one event per SERIES — the reading "detections" already implies.
	violations, err := a.Prom.Query(ctx, fmt.Sprintf(
		`sum by (multicast_group, stream, rule_id, severity, channel, hostname, source_addr) (increase(dz_conformance_violations_total{env=%q}[%s]))`,
		env, w))
	if err != nil {
		return nil, fmt.Errorf("conformance violations: %w", err)
	}
	// Rules are collected per ENTRY rather than per group, since an entry is now either a group
	// or one of its publishers and each renders its own list.
	rules := map[*EdgeMulticastConformance]map[string]*EdgeMulticastConformanceRule{}
	for _, s := range violations {
		g := s.Label("multicast_group")
		seen++
		if g == "" {
			dropped++
			continue
		}
		n := promCount(s.Value)
		if n == 0 {
			continue
		}
		// The exemption is decided BEFORE the entry is minted, and the order is load-bearing.
		// An exemption is keyed on (stream, rule) and is charged to the group either way — it
		// is a statement about a known deviation of the feed, and counting it per line would
		// make one waiver read differently on each path. Minting the publisher entry first left
		// an empty one behind, which grades `ungraded` and put "nothing reached a verdict" on a
		// line whose only finding was a deliberate waiver.
		if edgeMulticastConformanceExempt(s.Label("stream"), s.Label("rule_id")) {
			out.group(g).Exempted += n
			continue
		}
		e := out.entry(g, s.Label("source_addr"))
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
		if rules[e] == nil {
			rules[e] = map[string]*EdgeMulticastConformanceRule{}
		}
		id := s.Label("rule_id")
		r, ok := rules[e][id]
		if !ok {
			r = &EdgeMulticastConformanceRule{
				RuleID: id, Severity: strings.ToLower(s.Label("severity")),
			}
			rules[e][id] = r
		}
		r.Count += n
		if h := s.Label("hostname"); h != "" {
			r.Nodes = appendUnique(r.Nodes, h)
		}
		if v := s.Label("stream"); v != "" {
			r.Validators = appendUnique(r.Validators, v)
		}
	}

	// The denominator, split the same way the violations are: a publisher's coverage is the
	// checks that judged ITS series, and mixing in the channel-scoped ones would report a line as
	// having graded work it had no part in.
	checks, err := a.Prom.Query(ctx, fmt.Sprintf(
		`sum by (multicast_group, result, channel, source_addr) (increase(dz_conformance_checks_total{env=%q}[%s]))`,
		env, w))
	if err != nil {
		return nil, fmt.Errorf("conformance checks: %w", err)
	}
	for _, s := range checks {
		g := s.Label("multicast_group")
		seen++
		if g == "" {
			dropped++
			continue
		}
		n := promCount(s.Value)
		// Channels are a property of the group's coverage, not of one path, and the group row is
		// where the tooltip names them. Collected even from a flat series: the channel was
		// covered at some point, which is what this set reports.
		if ch := s.Label("channel"); ch != "" {
			out.group(g).Channels = appendUnique(out.group(g).Channels, ch)
		}
		// A series flat across the window graded nothing in it, and must not mint an entry: with
		// source_addr in the by-clause that is a path the validator saw no datagrams from, and an
		// entry with no checks behind it grades `ungraded` — a badge on a line over nothing at
		// all. Adding zero to an entry that already exists was always harmless; creating one is
		// not. The violations loop above skips a zero for the same reason.
		if n == 0 {
			continue
		}
		e := out.entry(g, s.Label("source_addr"))
		e.Graded += n
		switch strings.ToLower(s.Label("result")) {
		case "pass":
			e.Passes += n
		case "na":
			e.NA += n
		case "unverifiable":
			e.Unverifiable += n
		}
	}

	builds, err := a.Prom.Query(ctx, fmt.Sprintf(
		`count by (multicast_group, version) (dz_conformance_build_info{env=%q})`, env))
	if err != nil {
		return nil, fmt.Errorf("conformance builds: %w", err)
	}
	for _, s := range builds {
		g := s.Label("multicast_group")
		seen++
		if g == "" {
			dropped++
			continue
		}
		if v := s.Label("version"); v != "" {
			out.group(g).Versions = appendUnique(out.group(g).Versions, v)
		}
	}

	// The catalog behind the rules that fired. **Skipped when nothing fired** — a clean fleet has
	// nothing to describe and this is a fifth round trip — and **non-fatal**, unlike the four
	// above: it decorates a finding rather than deciding one, so blanking a computed verdict for
	// a refresh interval because a static lookup failed would cost the column its whole point.
	catalog := map[string]conformanceRuleDoc{}
	if len(rules) > 0 {
		info, err := a.Prom.Query(ctx, fmt.Sprintf(
			`count by (rule_id, summary, spec_url) (dz_conformance_rule_info{env=%q})`, env))
		if err != nil {
			slog.Warn("edge multicast conformance: rule catalog unavailable, rules render by id alone",
				"error", err)
		}
		for _, sm := range info {
			id := sm.Label("rule_id")
			if id == "" {
				continue
			}
			// Two builds mid-rollout can word one rule differently, and two can carry the same
			// wording against different spec builds. First lexicographically is arbitrary but
			// stable, so neither the text nor the link under it flips between refreshes. The
			// summary alone is not enough of a key for that: where two entries agree on it, the
			// comparison never fires and whichever link Prometheus happened to return first wins.
			cur, have := catalog[id]
			cand := conformanceRuleDoc{summary: sm.Label("summary"), specURL: sm.Label("spec_url")}
			if !have || cand.summary < cur.summary ||
				(cand.summary == cur.summary && cand.specURL < cur.specURL) {
				catalog[id] = cand
			}
		}
	}

	// Both grains are graded by the same function over the same struct, which is the point of
	// keeping one type: a publisher line and a group row cannot disagree about what `violating`
	// means, and neither can drift when the ranking changes.
	finish := func(e *EdgeMulticastConformance) {
		if rs := rules[e]; len(rs) > 0 {
			for id, r := range rs {
				if c, ok := catalog[id]; ok {
					r.Summary, r.SpecURL = c.summary, c.specURL
				}
			}
			e.RulesFired = len(rs)
			e.TopRules = topConformanceRules(rs)
		}
		sort.Strings(e.Nodes)
		sort.Strings(e.Channels)
		sort.Strings(e.Versions)
		e.Verdict = edgeMulticastConformanceVerdict(e)
	}
	for g, byPub := range out.Publishers {
		grp := out.Groups[g]
		for _, e := range byPub {
			// Who graded, carried down to the line. A publisher's verdict rests on exactly the
			// validators and vantages the group's does — these queries carry no source address
			// and never will, since a validator subscribes to the group. Copied rather than
			// read across at render time so the entry says what stands behind it.
			//
			// Channels are NOT copied: they describe the group's coverage, and a path graded on
			// one channel would read as having covered every channel the group did.
			if grp != nil {
				e.Instances = grp.Instances
				e.Nodes = append([]string(nil), grp.Nodes...)
				e.Versions = append([]string(nil), grp.Versions...)
			}
			finish(e)
		}
	}
	for g, e := range out.Groups {
		finish(e)
		// **A group with nothing conclusive of its own must not say `ungraded` over lines that
		// did reach a verdict.** That word means "the validator ran and nothing it grades
		// reached a verdict", and after the split it is what the group entry looks like on a
		// healthy feed: the rules left on it are the book and reference-data ones, which
		// decline most of their opportunities, so the row read "nothing reached a verdict"
		// directly above lines reading `conforming` over 900 passed checks.
		//
		// **The condition reads the verdict rather than re-deriving it**, and that is the whole
		// correctness argument. An earlier version tested `Graded == 0`, which is NOT the
		// condition `ungraded` is reached on — that one is `Passes == 0 && Info == 0`, so a
		// group whose channel-scoped checks all came back `na` or `unverifiable` has
		// `Graded > 0` and escaped the guard while still rendering the bad reading. Two
		// predicates for one question drift; one cannot.
		//
		// The guard keeps the signal `ungraded` exists for. A validator that runs and concludes
		// nothing is worth surfacing — but if a publisher of this group reached a verdict then
		// the validator is plainly grading, and the group's own silence is the split working
		// rather than an absence. With no publisher verdict either, nothing was concluded
		// anywhere and `ungraded` stands.
		//
		// It asks whether a publisher REACHED A VERDICT, not whether one graded: `Graded`
		// counts `na` and `unverifiable` too, so a line that concluded nothing would otherwise
		// rescue a group that concluded nothing — turning the whole group silent in exactly the
		// state the word is for. This is why the publisher entries are graded above and not
		// below: the group reads their verdicts.
		if e.Verdict == edgeMulticastConformanceUngraded && anyPublisherReachedAVerdict(out.Publishers[g]) {
			e.Verdict = ""
		}
	}

	if dropped > 0 {
		slog.Warn("edge multicast conformance: samples carry no multicast_group and were dropped",
			"dropped", dropped, "seen", seen, "groups", len(out.Groups))
	}

	// Stamped here and not before the queries. It is the clock the whole column ages against, and
	// four round trips to a hosted store are not free — taking it up front reported the payload as
	// up to a minute older than it is, on a column whose staleness rule is the point.
	out.GeneratedAt = time.Now().UTC()
	return out, nil
}

// entry returns the entry a finding belongs to: the publisher's when the metric named one, and
// the group's when it did not.
//
// The address is taken verbatim from the label and is NOT validated against the group's publisher
// set here. A finding from an address no publisher of the group carries is still a finding — the
// page counts it as unattributed rather than dropping it, exactly as the Sequence column does with
// a series whose address matches no line.
func (r *EdgeMulticastConformanceResponse) entry(addr, src string) *EdgeMulticastConformance {
	if src == "" {
		return r.group(addr)
	}
	if r.Publishers == nil {
		r.Publishers = map[string]map[string]*EdgeMulticastConformance{}
	}
	if r.Publishers[addr] == nil {
		r.Publishers[addr] = map[string]*EdgeMulticastConformance{}
	}
	if e, ok := r.Publishers[addr][src]; ok {
		return e
	}
	e := &EdgeMulticastConformance{}
	r.Publishers[addr][src] = e
	// The group must exist whenever one of its publishers does: the group row carries the
	// context every line's tooltip reads (which validators, at how many vantages, on what build),
	// and a publisher entry with no group behind it would render a verdict from nowhere.
	r.group(addr)
	return e
}

// anyPublisherReachedAVerdict reports whether any publisher of the group concluded something —
// read from the verdict each line already carries rather than re-derived from its counters, so
// the two can never disagree about what "concluded something" means.
//
// `ungraded` is the one verdict that is not a conclusion: it says the rules ran and none of them
// reached an answer. A line in that state cannot vouch for the group's silence, because it is the
// same silence one level down.
func anyPublisherReachedAVerdict(byPub map[string]*EdgeMulticastConformance) bool {
	for _, e := range byPub {
		if e.Verdict != "" && e.Verdict != edgeMulticastConformanceUngraded {
			return true
		}
	}
	return false
}

// countUnattributedConformance counts publisher verdicts naming an address no line on the group
// carries.
//
// A finding from an address the ledger does not know about is still a finding — a publisher torn
// down mid-window, or one whose tunnel address changed since the snapshot — so it is counted
// rather than dropped, exactly as a recorded series with no line is. Dropping it would make the
// page quieter than the feed.
//
// A line with no DZIP cannot match anything: it is a publisher the ledger has no tunnel address
// for, and it is not a wildcard that absorbs every unmatched verdict.
func countUnattributedConformance(
	lines []EdgeMulticastPublisher, byPub map[string]*EdgeMulticastConformance,
) int {
	if len(byPub) == 0 {
		return 0
	}
	onLine := make(map[string]struct{}, len(lines))
	for _, l := range lines {
		if l.DZIP != "" {
			onLine[l.DZIP] = struct{}{}
		}
	}
	n := 0
	for ip := range byPub {
		if _, ok := onLine[ip]; !ok {
			n++
		}
	}
	return n
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
//  3. **Nothing came back at all.** This is the test that earns its place. A validator can run,
//     scrape cleanly and grade nothing — every rule reporting `na` because the state it needs was
//     never reached — and report zero violations while doing it. Measured on a recorder running a
//     since-superseded build: 157,504 `na` and zero `pass` over 35 minutes, with one info-severity
//     rule as the only thing being graded on the feed. Not one must-severity check ran, and every
//     counter an operator watches read healthy. A verdict that cannot separate "clean" from
//     "nothing was evaluated" renders that green.
//  4. An info finding is worth surfacing and is not a fault.
//  5. Otherwise it graded something and found nothing wrong — which is the whole claim.
//
// **The test at 3 is `Passes == 0 && Info == 0`, and the second half is not redundant.**
// `dz_conformance_checks_total` carries `result="violation"`, so a feed whose graded checks all came
// back as info-severity violations has no passes at all — and on `Passes == 0` alone it rendered
// `ungraded`, whose tooltip says nothing reached a verdict, directly above a coverage line reading
// "0 of N checks passed" and a list naming the rules that fired. The badge asserted an absence over
// a present finding. Something that produced a finding was graded.
//
// It is deliberately not a coverage floor. A floor would fire on the market-by-price snapshot rule,
// which declines the large majority of its transitions on a healthy feed — roughly a quarter graded
// — so a threshold anywhere near that would paint a working feed permanently amber over a property
// of the check rather than of the feed.
func edgeMulticastConformanceVerdict(e *EdgeMulticastConformance) string {
	switch {
	case e.Must > 0:
		return edgeMulticastConformanceViolating
	case e.Should > 0:
		return edgeMulticastConformanceShould
	case e.Passes == 0 && e.Info == 0:
		return edgeMulticastConformanceUngraded
	case e.Info > 0:
		return edgeMulticastConformanceAdvisory
	default:
		return edgeMulticastConformanceConforming
	}
}

// topConformanceRules orders the rules that fired: must before should before the rest, and within
// a severity the loudest first, with the rule id as the tiebreak so the list is stable between
// refreshes.
func topConformanceRules(rs map[string]*EdgeMulticastConformanceRule) []EdgeMulticastConformanceRule {
	out := make([]EdgeMulticastConformanceRule, 0, len(rs))
	for _, r := range rs {
		// Out of a map: unsorted, these reshuffle on every refresh while saying nothing new.
		sort.Strings(r.Nodes)
		sort.Strings(r.Validators)
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
func (a *API) edgeMulticastConformanceFold(ctx context.Context) (
	map[string]*EdgeMulticastConformance, map[string]map[string]*EdgeMulticastConformance, time.Time,
) {
	data, err := a.readPageCache(ctx, edgeMulticastConformanceCacheKey)
	if err != nil {
		return nil, nil, time.Time{}
	}
	var payload EdgeMulticastConformanceResponse
	if err := json.Unmarshal(data, &payload); err != nil {
		slog.Warn("edge multicast conformance: cache did not parse", "error", err)
		return nil, nil, time.Time{}
	}
	if len(payload.Groups) == 0 {
		// An empty payload is a real answer — no validator covers anything in this
		// environment — but it carries no per-group facts, so there is nothing to age and the
		// column has nothing to dim. Returning a zero clock keeps the header from claiming a
		// freshness for cells that do not exist.
		//
		// Groups alone decides this. Every publisher entry creates its group, so there is no
		// payload carrying lines and no rows above them.
		return nil, nil, time.Time{}
	}
	// A nil Publishers map is the normal state on a fleet that has not rolled the label yet, and
	// every lookup below reads nil as "this publisher has no verdict of its own".
	return payload.Groups, payload.Publishers, payload.GeneratedAt.UTC()
}
