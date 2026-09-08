# Edge multicast conformance column — the PromQL bridge

**Date:** 2026-09-08
**Status:** Design — not yet reviewed. Implemented on `jo/edge-multicast-conformance-column`; the
column stays dark until the two out-of-band prerequisites in §1 and §2 land, which is the designed
behaviour and not a pending step in the code.
**Page:** `/dz/edge/multicast`

**Repositories.** Paths are repo-qualified wherever they are not in this one. Bare paths are
`malbeclabs/lake`. The validator and its rule catalog live in `malbeclabs/edge-feed-spec`; the
Ansible role, the Alloy scrape config and the deployment secrets live in `malbeclabs/infra`.

## Goal

Add a `Conformance` column to the edge multicast page reporting what `dz-conformance` found on each
feed, read from Grafana Cloud hosted Prometheus on the existing background refresher.

## What exists today, verified

**Nothing about conformance is in ClickHouse.** Three independent checks, all negative:

- No table matching `conform|complian|valid|check|test` in `lake`, `lake_testnet`, `lake_devnet` or
  any `telemetry_*` database. `lake` has 155 tables and none of them is this.
- No occurrence of "conformance" anywhere in this repo.
- The validator has no database sink. `edge-feed-spec:tools/conformance/report/` implements exactly
  three reporters — `slog.go`, `json.go`, `prom.go` — behind the `report.Reporter` interface
  (`report/sink.go`). Its durable outputs are an exit code, an optional `--json-report` file, and
  Prometheus metrics.

**Where the data is.** `dz-conformance` runs as a systemd instance per validated feed on the
recorders, serves `dz_conformance_*` on `127.0.0.1:<metrics_port>`, Alloy scrapes it and
remote-writes to Grafana Cloud (`prometheus-prod-13-prod-us-east-0.grafana.net`, stack `1946814`).
The dz_conformance scrape block sets no `scrape_interval`, so it runs at Alloy's default 60s — only
the validator block overrides it, at 10s.

Metrics and their labels:

| metric | labels |
| --- | --- |
| `dz_conformance_checks_total` | `feed`, `rule_id`, `result` (`pass`\|`violation`\|`suspected`\|`unverifiable`\|`na`) |
| `dz_conformance_violations_total` | `feed`, `rule_id`, `severity` |
| `dz_conformance_unverifiable_total` | `feed`, `rule_id`, `reason` |
| `dz_conformance_rule_info` | `rule_id`, `severity`, `summary`, `spec_url` |
| `dz_conformance_build_info` | `version`, `commit` |
| `dz_conformance_uptime_seconds` | — |
| `dz_conformance_transport_loss_total` | `port` |

Alloy adds `stream` (the instance name, from the scrape target), plus `hostname` and `env` from
`prometheus.relabel.common`. `env` is `mainnet-beta` on this inventory.

`severity` is on `violations_total` directly, so classifying must-vs-should needs no join against
`rule_info`.

**Deployment coverage**, from `infra:ansible/inventory/mainnet-beta/group_vars/`. 15 instances at
cmh, 14 at was, 2 at dub, covering 5 of the 15 `edge-` groups:

| group | validated |
| --- | --- |
| `edge-kalshi-perps-tob` (.3) | 3 metros |
| `edge-kalshi-perps-mbp` (.4) | 3 metros |
| `edge-kalshi-elections-pol-tob` (.21) | 6 channels, cmh + was |
| `edge-kalshi-elections-pol-mbp` (.22) | 6 channels, cmh + was |
| `edge-kalshi-sports-mbp` (.20) | channel 10 (NFL) only, cmh only |
| `edge-kalshi-sports-tob` (.17) | no — no host receives it, scoped out at deploy time |
| `edge-phoenix-*`, `edge-binance-*` | no validator |
| `edge-solana-*` (5 groups) | not applicable — Turbine, no recorded wire protocol to grade |

The Hyperliquid instances point at `233.84.178.15`, whose ledger code is **`tiredsolid`**. That does
not match `edgeMulticastGroupCodePrefix` (`"edge-"`, `edge_multicast.go:74`), so those two instances
have no row on this page to land on. Not a bug to fix here; recorded so it is not read as missing
data.

## The decision: a PromQL bridge, and what it costs

This design has the lake API query Grafana Cloud over PromQL on the background refresher. It is an
**interim**, and the successor is already specified elsewhere — see the next section, which also
records the option this design previously named as the target and now rejects.

Two costs, stated so they are not discovered later. **A new dependency class in the API**, which
today speaks only ClickHouse and Postgres; `peeringdb.go` is the closest precedent. And **history
bounded by Grafana retention**, not queryable alongside the rest of the stack.

## The successor: `recorder.conformance_finding`

`malbeclabs/edge-multicast-ref` has already specified and partly built the destination, in
`docs/superpowers/specs/2026-08-31-sequence-loss-and-conformance-rows-design.md`. Its analysis tier
derives four grains from the recorder's archive and a separate loader process writes them to a
`recorder` database. The fourth grain is `recorder.conformance_finding`: rule verdicts
(`pass` | `violation` | `unverifiable` | `na`) keyed on the **channel instance in full** —
`(source_addr, channel_id, dst_port)` — with `rule_set_version`, the evidence range, and the object
key the evidence sits in. No TTL: "a verdict's whole value is that it was recorded when the rule
ran."

That design also states its purpose in terms of this page. It names our Sequence column's data
source — a TTL-less level-grain table sorted for symbol questions, read through a remote proxy, and
therefore foldable only from a ten-minute refresher's cached payload — and calls these rows "a
better source under an existing column, not a new column". It is not a parallel effort; it is an
upstream replacement for a plane this page already renders.

**Two things it gives that this design cannot.** The publisher source address is in the key, so a
verdict can sit on the **publisher line** rather than the group row (§4). And the runner drives the
same `edge-feed-spec` catalog over **replayed archive bytes**, where lossless replay collapses most
`unverifiable` into `pass` or `violation` — the live checker's verifiability gate is open far less
often.

**Rejected, and it was named as the target in an earlier draft of this document: adding a ClickHouse
sink to `dz-conformance`.** It would be a third path to the same rows, competing with a runner that
is already designed and whose table is already created. Do not build it.

**What is not there yet, verified 2026-09-08.** Four independent reasons this cannot be the route
today, any one of which is sufficient:

- **The `recorder` database does not exist in production.** `SELECT count() FROM
  recorder.conformance_finding` returns error 81, *Database recorder does not exist* — not the
  error 497 privileges refusal that the `feeds` proxies return to the same user, so this is an
  answer and not a permissions artefact.
- **The loader is not deployed anywhere.** No reference to `dz-recorder-load` in `malbeclabs/infra`.
  Only the record path runs, on 3 hosts, pinned to `dz-recorder` 0.1.0 / `81b0331c`.
- **`conformance_finding` would be empty even if it were.** The conformance runner is the unbuilt
  half of that repo's plan 3; the loader's deriver returns `conformance_finding: Vec::new()` with
  the comment that an empty vector "is the honest statement that nothing judged this object — where
  a `pass` row would be a pass over a rule that never ran" (`dz-recorder-rows/src/derive.rs:374`).
- **The archive is local and evicted.** The recorder does not upload; staging is capped at 20–30 GiB
  per host, which perps market-by-price fills in about a day. There is no backfill window to load
  from beyond that.

**What this means for the payload.** §5's `EdgeMulticastConformance` must be expressible from either
source, so the swap replaces one fetch function and nothing else. Two consequences: the struct
carries `Channels` and `Nodes` as sets rather than assuming one validator per group, and the
group-row placement in §4 is recorded as a **property of the PromQL source**, not of the page — when
the rows land, the same struct hangs off `EdgeMulticastPublisher` with `source_addr` as the join,
and the column moves to the lines with no change to the verdict vocabulary.

Convergence worth noting, because it is evidence the vocabulary is right rather than a coincidence:
that design arrives independently at the same "an absent verdict is not a passing verdict"
distinction this document makes with `ungraded` (§4), and keys on the channel instance for the same
reason the page's Sequence column already does.

---

## 1. Prerequisite: the multicast group has to be a label — WRITTEN, not merged

Branch `jo/lake-conformance-column-prereqs` in `malbeclabs/infra` carries it, together with §2's
deployment wiring and a regression test. It ships on merge: `update_monitoring.yml` renders Alloy
from CI.

No conformance metric carries the multicast group, the channel, or the publisher source address.
The group is recoverable only by mapping the `stream` label through the Ansible inventory.

**Fix it at the scrape, not in lake.** One line in
`infra:ansible/playbooks/roles/monitoring/templates/config.alloy.j2`, in the `dz_conformance` block:

```
{ __address__ = "127.0.0.1:{{ feed.metrics_port }}", "component" = "dz_conformance",
  "stream" = "{{ feed.name }}", "multicast_group" = "{{ feed.group }}",
  "feed_kind" = "{{ feed.feed }}" },
```

Both values are already in `dz_conformance_feeds`, one field away from the ones being rendered, and
the role asserts both are present on every entry (`dz_conformance/tasks/main.yml`), so the template
cannot render an empty label.

**It carries a regression test**, `monitoring/tests/test_config_alloy_conformance_labels.py`, wired
into the Ansible workflow beside the journal-labels one. It renders the block and asserts each
target carries the group and plane its inventory entry gave, rather than that the strings appear
somewhere in the file — and its fixture puts two instances on one group, which is what the six
elections instances are. Verified to fail when the label is removed and pass when it is restored;
a test for a silent failure that cannot itself fail is worth nothing.

**Rejected: a Go map in lake from instance name to group.** It would unblock the work with no infra
change, and it is 15 entries today. It is also a second copy of a fact the inventory already holds,
and the failure it produces is silent and wrong rather than silent and empty: an instance renamed or
a group renumbered leaves lake confidently attributing a verdict to the wrong feed. `edgeNodeIPs`
and `doubleZeroOperatorWallets` are the precedent for a Go literal in this codebase, and both are
assertions about DoubleZero's own hosts that nothing else records — the opposite of this case.

**Lake must not fall back to a map when the label is missing.** A series with no `multicast_group`
is dropped and counted, and the group renders no column. Empty is the correct reading of "the
scrape config has not been rolled out yet".

**The channel is not in the key, but it is worth carrying.** Six elections instances scrape against
**one** group (`.21`), so `(multicast_group, feed_kind)` is not unique per instance and the fold
must aggregate over instances. A `channel` label would let the tooltip name which channel is
failing rather than reporting the group as a whole. `feed.name` encodes it today
(`kalshi_elections_tob_house`); an explicit label is cheaper to read than a name convention. Nice to
have, not a prerequisite.

**One-time cost of the label change:** adding a label starts new series and ends the old ones, so an
`increase()` spanning the rollout reads only the new series. Cosmetic, once, and invisible on a
15-minute window an hour after the rollout.

## 2. Prerequisite: a read token (human, out of band)

The remote-write key in `monitoring_grafana_prom_remote_write_api_key` is push-scoped. **Do not
reuse it.** This needs a Grafana Cloud access policy token with `metrics:read` on stack `1946814`.

**The deployment wiring is written** (same infra branch): three variables in the shared env patch
of `k8s/lake/prod/kustomization.yaml`, which kustomize applies to `lake-api` and `lake-worker`
together, so a var cannot reach only one of them. Verified with `kubectl kustomize`: all three land
on both deployments.

```
GRAFANA_PROM_URL   = https://prometheus-prod-13-prod-us-east-0.grafana.net/api/prom   (plain)
GRAFANA_PROM_USER  = 1946814                                                          (plain)
GRAFANA_PROM_TOKEN = secretKeyRef → lake-api-env, optional: true
```

**The URL is verified against the live endpoint**, not inferred from the push URL. Unauthenticated:
`/api/prom/api/v1/query` answers **401** and `/api/v1/query` answers **404**, so the base is the
`/api/prom` form and the client's `+ "/api/v1/query"` join is right.

**`optional: true`, unlike every other `secretKeyRef` in that block, and the difference is real.** A
missing token costs one column — the API reads it as "no metrics store configured" and the column
does not render — where a missing Postgres or Neo4j password is a dead process. Without it, rolling
the deployment before `kubectl apply -k k8s/secrets/lake-prod/` has landed the new key crash-loops
both `lake-api` and `lake-worker` over a feature neither needs in order to serve a request.

**Staging and PR previews are untouched.** Neither references `lake-api-env`; their manifests are
their own. The column stays off there, which is the designed state and not an oversight.

**What remains is the token itself**, which is a console action: a Grafana Cloud access policy with
`metrics:read` on stack `1946814`, then

```
sops set k8s/secrets/lake-prod/lake-api-env.enc.yaml \
  '["stringData"]["GRAFANA_PROM_TOKEN"]' '"<token>"'
kubectl apply -k k8s/secrets/lake-prod/
```

Apply the secret **before** the deployment rolls. `optional: true` means the wrong order costs a
refresh cycle rather than the pods, but it is still the wrong order.

## 3. The queries

Window is **15 minutes**, matching the other folds on this page. At a 60s scrape that is 15 samples.

Which validators are running, and at how many vantages. **It runs first so a validator with nothing
to report yet still reaches the payload.** A process that just started has no findings and no checks,
and it has to render as "graded nothing" rather than vanish. Every query creates the entry it needs,
so the group set is their union; what this one adds is the groups the other three would never
mention. One query rather than two: the per-host elements are both the node list and, summed, the
instance count.

```promql
count by (multicast_group, hostname) (dz_conformance_uptime_seconds{env="mainnet-beta"})
```

Violations, by severity and rule. `stream` is in the by-clause only so the exemptions below can key
on it:

```promql
sum by (multicast_group, stream, rule_id, severity, channel) (
  increase(dz_conformance_violations_total{env="mainnet-beta"}[15m])
)
```

The graded denominator:

```promql
sum by (multicast_group, result, channel) (
  increase(dz_conformance_checks_total{env="mainnet-beta"}[15m])
)
```

And the build behind a verdict, for the tooltip:

```promql
count by (multicast_group, version) (dz_conformance_build_info{env="mainnet-beta"})
```

`feed_kind` is deliberately **not** in any by-clause. Every group on this page is one plane and its
own address, so `multicast_group` alone is the key, and the page already knows the plane from the
group code. It stays a scrape label because it costs nothing and reads well in Grafana.

`channel` is requested before it exists. A by-clause naming an absent label groups every series
under the empty value rather than erroring, so the queries are forward-compatible with §1's
optional label and need no change when it lands.

**increase() does not return integers.** It extrapolates to the window edges, so a counter that
moved once inside the window reports something like 0.7 or 1.03 and never exactly 1. Rounding alone
is wrong at the bottom of that range — it turns a real violation that landed near a window edge into
zero. The rule is: at or below zero is no events, since increase() cannot produce a positive number
from a counter that did not move, and anything above zero is at least one. `promCount` in
`promql.go` owns it and `TestPromCount_ASubUnitIncreaseIsStillAnEvent` pins it.

The `env` matcher is pinned explicitly. That is stronger than the environment gate the other folds
on this page can manage: they resolve through the multicast address, and both networks allocate out
of the same `233.84.178.0/24`.

**No violation is exempted, and an earlier draft of this document had that wrong.**

That draft took §4.3 of the 2026-08-19 conformance deploy design at face value. §4.3 *proposed* a
PromQL `unless` over two `(stream, rule_id)` pairs; it is not what shipped. Checking the deployed
alerts against live metrics on 2026-09-08 found:

- `infra:grafana/alerts/dz-conformance-must-violation.json` is
  `increase(dz_conformance_violations_total{severity="must"}[5m])` with **no `unless` clause at
  all**. Every must-severity violation pages, `MSG.WRONG_PORT_PLACEMENT` and
  `MBP.SNAP.RECONSTRUCTED_BOOK_MATCHES_SNAPSHOT` included.
- `infra:grafana/alerts/dz-conformance-coverage-loss.json` does exempt
  `MBP.SNAP.RECONSTRUCTED_BOOK_MATCHES_SNAPSHOT` — but on `dz_conformance_unverifiable_total`, with
  `reason=~"pending|cold_start"`, and over **nine** streams rather than two: the six
  `kalshi_elections_mbp_*` channels as well as perps and NFL. That is the coverage question, not the
  violation one. This column reports coverage as a ratio rather than alerting on it, so it needs no
  exemption to state it correctly.

Had the draft's list shipped, the column would have read `conforming` over two feeds on-call is
being paged for. **The page must never be quieter than the alert**, and it is the page that has to
move — the alert is what somebody is carrying a phone for. So the list is empty, and an entry is
only correct alongside a matching exemption in the deployed must-violation alert, added in the same
change and for the same stated reason.

The mechanism stays, tested against an injected entry rather than the shipped list, and
`TestEdgeMulticastConformance_NoViolationIsExemptedByDefault` asserts the list is empty — otherwise
the day it was emptied is the day its tests silently stopped testing anything.

**The design's own claim about that rule is also falsified.** §4.2 says the snapshot rule reports
*unverifiable*, not violation ("the committed code reports 0 violations on that capture"). Measured
over fifteen minutes on 2026-09-08 it produced **90 must-severity violations** across the fleet —
33 on perps, 289 on NFL, and 90 across four elections channels. It produces both.

## 4. Grain and vocabulary

**The column renders on the group row and nowhere else, and this is forced rather than chosen.**
Neither the Prometheus labels nor `core.Finding` (`finding.go:149`) carry the publisher source
address — `Finding` has `ChannelID` but no source IP, even though the engine has it
(`Engine.Process(src netip.Addr, …)`, and `instanceTrack(src, port, ch)` keys per channel instance).
So nothing in this payload can name a path, and a per-publisher-line verdict is not available on
this route at any effort. Recorded here so the next reader does not try to derive one.

Getting it onto the lines means adding `SourceIP` to `Finding` and threading it through — a small
change in `edge-feed-spec`, and the natural companion to the ClickHouse sink.

Verdicts rank worst-first:

| verdict | means |
| --- | --- |
| `violating` | ≥1 `must` violation in the window, after exemptions |
| `should` | `should` violations only |
| `ungraded` | validators are running but almost nothing was graded — `na` + `unverifiable` dominate and `pass` is at or near zero |
| `advisory` | `info` findings only |
| `conforming` | passes, with measured coverage |
| *(no cell)* | no validator covers this group |

**`ungraded` is the one that earns its place.** On `v0.2.0` a Kalshi recorder read
`TOB.QUOTE.REFDATA_KNOWN` at 157,504 `na` and zero `pass` for 35 minutes, with a single `info` rule
as the only thing being graded on the feed — not one `must` check ran, and every counter an operator
watches read healthy. A verdict that cannot distinguish "clean" from "nothing was evaluated" would
have rendered that green. It is the same distinction this page already makes with `gaps_unmeasured`
and `unrecorded`.

**Rejected: a `partial` verdict for a group whose validator covers only some of its channels.**
`edge-kalshi-sports-mbp` validates 1 channel of roughly 31, which is worth telling the reader — but
this payload does not know how many channels the group has, and there is no honest way to compute
the denominator from it. Inventing one would put a number on the badge that no measurement supports.
Instead the payload carries the channel ids and recorder hostnames it actually saw, and the tooltip
says "graded on 1 channel at 1 recorder". Coverage is reported as `pass / Σ result`, which is
measured rather than assumed.

## 5. API

New file `api/handlers/edge_multicast_conformance.go`, following `edge_multicast_observations.go`.

- `const edgeMulticastConformanceCacheKey = "edge_multicast_conformance:v1"`.
- Computed in `StartKalshiBackgroundRefresher` with its own timeout, so a slow or hanging Grafana
  cannot starve the other legs. It runs **after** the observations leg, which stays first: it is the
  cheapest and the only one with no live-query fallback.
- Folded in `FetchEdgeMulticastData`, gated on `isMainnet(ctx)` like the other two folded payloads.
- **Its own `conformance_as_of` stamp.** Not `sequence_as_of`, not `observations_as_of`. Those are
  different clocks, and reusing one dims columns over the staleness of a payload they do not come
  from — `TestGetEdgeMulticast_ObservationsCarryTheirOwnAsOf` pins that pair already.
- An absent or failed payload drops the column, never the page. Note that this is not a rare state:
  `page_cache` survives a pod restart, so a newly added key is empty from deploy until the refresh
  chain first reaches it.
- Failures log at WARN, matching the five legs already in that function. Deliberately not
  `logger.Escalator`: WARN never escalates to ERROR, so this cannot page, and introducing an
  escalator for one leg of a chain where the other five use `slog.Warn` would make the file
  inconsistent for no gain in signal. If the whole chain moves to escalators, this moves with it.
- A **nil querier** makes the fetch return an empty payload with a nil error, which is written and
  read as "no validator covers anything here". That is the correct state for an environment with no
  credentials, and it is not a failure to log every cycle. A **failed query**, by contrast,
  propagates: an empty-but-valid payload is written over the last good entry, so swallowing a blip
  would blank the column for a full refresh interval with nothing logged — the contract
  `kalshiTableExists` already keeps.

Payload sketch, on `EdgeMulticastGroup`:

```go
// Conformance is what dz-conformance graded on this group in the window — nil for a group no
// validator covers, which is 10 of the 15 today. It is per GROUP and not per publisher line:
// neither the metrics nor core.Finding carry the publisher source address, so nothing here can
// name a path. See docs/superpowers/specs/2026-09-08-edge-multicast-conformance-column-design.md.
Conformance *EdgeMulticastConformance `json:"conformance,omitempty"`

type EdgeMulticastConformance struct {
    Verdict   string   `json:"verdict"`               // violating|should|ungraded|advisory|conforming
    Must      uint64   `json:"must"`                  // must-violations in the window, post-exemption
    Should    uint64   `json:"should"`
    Info      uint64   `json:"info"`
    Passes    uint64   `json:"passes"`
    Graded    uint64   `json:"graded"`                // Σ over result — the denominator
    NA        uint64   `json:"na"`
    Unverif   uint64   `json:"unverifiable"`
    TopRules  []EdgeMulticastConformanceRule `json:"top_rules,omitempty"` // must-first
    Instances int      `json:"instances"`             // validator processes behind the verdict
    Nodes     []string `json:"nodes,omitempty"`       // recorder hostnames behind the verdict
    Channels  []string `json:"channels,omitempty"`    // empty = the scrape does not say
    Exempted  uint64   `json:"exempted"`              // known-deviation hits, counted not hidden
    Versions  []string `json:"versions,omitempty"`    // >1 is a legitimate mid-rollout state
}
```

`Exempted` is counted and surfaced rather than dropped: a known deviation that stops firing, or one
that starts firing on an instance it was never exempted for, is a real signal and silence would hide
both.

## 6. Web

`Conformance` between `Heard` and `Sequence` on the group row, blank on the publisher lines — the
same treatment `Heard` already gets, for the same reason. The header carries a `per group` subtitle,
said once for the column rather than once per cell, which is what the `Ingress` header already does
for `per tunnel`.

A group no validator covers renders an **em dash**, not a badge and not a blank: nobody checked, and
that is a different statement from a clean one. Today that is 10 of the 15 groups.

The badge colours follow the ranking. `violating` red, `should` amber, `conforming` filled green —
and `ungraded` and `advisory` are both **outlined rather than filled**, the treatment `advancing`
already uses one column over, so a glance down the column cannot read either as the same clean bill
of health a `conforming` row carries.

The tooltip carries what the badge cannot, and three of its lines are not optional:

- **Known deviations are excluded and counted.** Naming them, so a green badge on `perps-tob` is not
  read as the port-placement deviation having been fixed.
- **How many vantages.** `sports-mbp` is graded at one recorder, the same caveat `GapNodes` carries
  on the Sequence column.
- **`conforming` means "nothing was found wrong in what was graded"**, with the measured coverage
  beside it — not "this feed conforms to the spec".

Staleness follows the existing rule: past `STALE_AFTER_SECS` the header carries the age in amber and
the values dim. An **absent** stamp is not staleness — that is a payload written before the API
carried this clock.

## 7. What this does not do

- No verdict on `edge-kalshi-sports-tob`, `edge-phoenix-*`, `edge-binance-*` or any `edge-solana-*`
  group. The first is a deployment gap, the last is not applicable.

  **The stated reason for that first gap is now stale.** The conformance deploy design scoped
  `sports-tob` out because "no host receives it, so it is a new join and a separate decision".
  `dz_recorder_feeds` at cmh now records `tob_edge_kalshi_sports_{mlb,ncaaf,tennis}` on channels 17,
  11 and 41, so that host is subscribed and the join already exists. Adding validator instances
  there is now an inventory change, not a network decision. Out of scope here; worth raising in
  `malbeclabs/infra`.
- No row for the Hyperliquid validators, whose group's ledger code does not match `edge-`.
- No per-path attribution (§4).
- No history beyond Grafana retention, and none queryable alongside the ClickHouse planes.

## Vocabulary

`stream` appears here only as the name of an Alloy label. In our own prose the traffic is a **feed**
and a redundant route is a **path**, per `edge-feed-spec:GLOSSARY.md`.
