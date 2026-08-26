# Claude Code Guidelines

## Project Overview

This is the **DoubleZero Data** platform (internal project name: "lake"). It's a data analytics platform for the DoubleZero (DZ) network. It ingests network telemetry and Solana validator data into ClickHouse, and provides an AI agent that answers natural language questions by generating and executing SQL queries.

**Important:** The user-facing name is "DoubleZero Data", not "Lake". Use "DoubleZero Data" in UI text and user-facing content.

The agent is the core feature - it lets users ask questions like "which validators are on DZ?" or "show network health" and get data-driven answers.

## Structure

- `agent/` - AI SQL generation agent (the main feature)
- `api/` - Go HTTP server (chi router, :8080)
- `web/` - React frontend (Vite + Bun, :5173)
- `indexer/` - Data indexing service (user-managed)
- `slack/` - Slack bot (user-managed)

## Service Management

Do NOT manage the `api` or `web` services. The user runs these separately and will restart them as needed.

## Local K8s Environment

The project has a local k3d + Tilt dev environment managed by `scripts/k8s.sh`:

```bash
./scripts/k8s.sh up          # Create cluster and start Tilt
./scripts/k8s.sh down        # Stop cluster (preserves data)
./scripts/k8s.sh down --clean  # Delete cluster and all data
./scripts/k8s.sh status      # Show cluster and pod status
./scripts/k8s.sh list        # List all lake clusters
```

The cluster name follows the pattern `lake-$USER` (e.g., `lake-snormore`). Kubeconfig is isolated at `.tmp/k8s/<cluster-name>.kubeconfig` — use it with:

```bash
KUBECONFIG=.tmp/k8s/lake-snormore.kubeconfig kubectl -n lake-dev ...
```

Services run in the `lake-dev` namespace. Tilt port-forwards them to localhost (with automatic offset if ports conflict).

### ClickHouse Databases (Local vs Remote)

The local ClickHouse has two databases:

- **`default`** — local data written by the local indexer. This is what the API queries when running locally (without `--use-remote`).
- **`lake`** — remote proxy tables (`remoteSecure()`) pointing to production ClickHouse Cloud. Created by the indexer's `--setup-remote-tables` flag.

Both databases have the same table names. When querying local data (e.g., via the local MCP at `localhost:8080/api/mcp`), the API reads from `default`. The `lake` database is only used when the API runs with `--use-remote` or when querying prod data directly.

The cloud-hosted MCP (`claude_ai_DoubleZero`) always queries production. To verify local indexer changes, use the local `doublezero` MCP (which hits the local API on `localhost:8080`).

## Commands

```bash
go run ./api/main.go      # Run API server (:8080)
cd web && bun run dev     # Run web dev server (:5173)
cd web && bun run build   # Build frontend (runs tsc first)
```

### Agent Evals

```bash
./scripts/run-evals.sh                 # Run all Anthropic evals in parallel
./scripts/run-evals.sh --show-failures # Show failure logs at end
./scripts/run-evals.sh -s              # Short mode (code validation only, no API calls)
./scripts/run-evals.sh -r 2            # Retry failed tests up to 2 times
./scripts/run-evals.sh -f 'NetworkHealth'  # Filter to specific tests
EVAL_MODEL=claude-sonnet-4-5 ./scripts/run-evals.sh  # Use a different model (default: claude-haiku-4-5)
```

Output goes to `eval-runs/<timestamp>/` with:
- `failures.log` - All failure output (check this first)
- `flaky.log` - Tests that failed initially but passed on retry (review to identify unstable behavior)
- `successes.log` - All success output
- `<TestName>.log` - Individual test logs

**When to run evals:** Only after changing agent logic (prompts, context, or code in `agent/`). Changes to `api/` or `web/` do not require evals.

**IMPORTANT:** Do not run the full eval suite without asking the user first. Running all evals takes several minutes and costs money. When you need to verify changes, run specific tests with `-f 'TestName'` or use `-s` for short mode. Only run the full suite when the user explicitly requests it.

**Short mode does not exercise prompts with the agent** — it only validates code, setup, and test infrastructure. To run all evals in short mode, prefer `go test` over the shell script as it parallelises better:
```bash
go test -tags evals -short ./agent/evals/ -v -count=1
```

**Do NOT run OllamaLocal evals.** The OllamaLocal tests skip when Ollama isn't available, which makes them appear to pass. Only run the Anthropic evals (filter with `-f 'Anthropic'` if needed).

**Evals are the source of truth for agent quality.** The agent system prompt and evals work together:

- When changing agent prompts or context: evals must continue to pass. If an eval fails, fix the agent behavior, not the expectation.
- When working on evals: the goal is to improve the agent. Add expectations that enforce better behavior, don't weaken expectations to make tests pass.

## Conventions

- TypeScript strict mode - `tsc -b` must pass before builds
- React functional components with hooks
- Tailwind CSS v4 for styling
- API client functions in `web/src/lib/api.ts`
- Go handlers in `api/handlers/`

## Makefile

- `make build` — build all packages with CGO disabled
- `make lint` — run golangci-lint with the repo's `.golangci.yaml` config
- `make fmt` — run `go fmt` on all packages
- `make test` — run all tests with race detector
- `make ci` — run build, lint, and test in sequence

## Page Cache

The API has a background page cache (`api/handlers/page_cache.go`) that pre-computes expensive ClickHouse queries so pages load instantly on first visit. It refreshes each endpoint on a configurable interval (30s–120s). Handlers check the cache first for default request parameters and return with `X-Cache: HIT`; non-default requests bypass the cache.

Add caching when a page runs expensive queries, has a common default view, and 30–60s staleness is acceptable. See publisher check or edge scoreboard handlers for reference implementations.

An entry may cache a complete result set and slice request-shaped pages out of it, so hit eligibility doesn't depend on the `limit` a client happens to send (see `sliceCachedValidators`). Cap the cached row count and fall through to the live query when the entry doesn't hold the whole set.

Refreshes are driven by a long-running Temporal workflow (`api/worker/`). `PageCacheWorkflow` schedules its activities with no `workflow.GetVersion` guards, so any change to the sequence of commands the loop emits — adding or removing an activity call, reshaping the refresh cycle — is replay-breaking against the previous deploy's run. What makes that safe is that every deploy starts a **fresh run**, via `pageCacheStartOptions` — see "Temporal Workflow Restarts on Deploy" below for the two option fields that produce it and why neither may be relaxed. Adding a cached *page* is not replay-breaking by itself: entries are enumerated inside the activities, not scheduled by the workflow.

Three operational notes. A rolling deploy still overlaps workers on one task queue, so an old pod can pick up the fresh run's first workflow task and write old-shaped history — the same wedge from a different cause; versioning the task queue by build SHA is the open follow-up. Terminating the workflow by hand does **not** restore the cache: `ExecuteWorkflow` runs only at process startup, so a manual terminate needs a pod restart after it. And prod runs `lake-api` at two replicas, each with an embedded worker, so a deploy logs two `page-cache: workflow started` lines with different `run_id`s — last one wins, and that is expected.

### Refresh cadence

An entry sets `cacheEntry.every` (`api/worker/workflow.go`) to refresh less often than every
cycle. It is a **wall-clock duration**, gated on the age of that key's own `page_cache.updated_at`
row — deliberately not a cycle count: the cycle period is configured per environment (prod ~68s,
staging ~4 min) and shortens as entries leave the every-cycle path, so a count would mean a
different staleness in each environment and drift as entries are added. The duration is a floor,
not a period — actual spacing is `[every, every + one cycle period)`.

Set it from how stale the view may be, not from how expensive it is. A day-aligned or long-window
aggregate (Network Health, the 24h scoreboard aggregates, the metro-pair latency comparison) can
take minutes; anything backing a live tail or point-in-time status stays every-cycle.

An entry whose payload reports the window it was computed over sets `dayAligned` as well, so it
refreshes as soon as its blob predates the current window. Without it, two groups on different
cadences describe different windows for up to a cadence after midnight UTC, and the Network Health
page refuses to combine payloads whose windows disagree.

A failed refresh writes nothing, so `updated_at` does not advance and the entry is due again on the
next cycle — escalation counters keep running at the cycle rate whatever the cadence. The one
exception is a refresh that writes *and* reports a problem (a degraded Network Health panel), which
is why `degradedEsc` sets `ErrorAfterDuration`.

## Temporal Workflow Restarts on Deploy

Every long-running workflow started under a fixed ID at process startup must restart on deploy, and none of them carry `workflow.GetVersion` guards — so an adopted run replays the previous deploy's history against new code, fails every workflow task, and retries forever without converging. There are four such sites: `api/worker.pageCacheStartOptions`, `dzingest.deployStartOptions`, `solingest.deployStartOptions`, and `rollup.computeRollupStartOptions`. Each sets `WorkflowIDConflictPolicy: TERMINATE_EXISTING` *and* `WorkflowExecutionErrorWhenAlreadyStarted: true`, and each logs the returned `run_id` so a start line is falsifiable. Any new workflow of this shape must do the same; per-run-ID workflows (the `admin` backfills) need none of it, since they never collide.

The conflict policy is what produces the fresh run — the server terminates the running execution as part of the start, atomically. The flag is what keeps a non-fresh start visible: without the conflict policy, the server's default is to fail an already-started start, and with the flag unset the SDK converts that failure into a handle on the still-running run and returns no error. That is how a deploy silently adopted the old run and panicked on replay for six hours. Don't relax either, and don't reintroduce a separate `TerminateWorkflow` call before the start — it is dead code once the server terminates atomically, and adds a `NotFound` on every clean start.

## Kalshi Scoreboard Feed Config

The competing feeds shown on the Kalshi scoreboard are **not** in code — they live in the
Postgres table `kalshi_scoreboard_entry` (`feed`, `label`, `display_order`, `enabled`).
Only enabled rows are raced, counted, or displayed; a feed with no row never appears.

Add, remove, or reorder a feed by changing rows — no code change, no deploy.

**These statements are run by a human operator against the target environment.** They are
recorded here as a runbook, not as something to execute automatically: do not run them, or
any other write against this table, from an agent session unless explicitly asked to.

```sql
-- stop showing a feed
UPDATE kalshi_scoreboard_entry SET enabled = FALSE, updated_at = NOW() WHERE feed = '<feed>';
-- start showing a feed
INSERT INTO kalshi_scoreboard_entry (feed, label, display_order, enabled)
VALUES ('<feed>', '<label>', <n>, TRUE)
ON CONFLICT (feed) DO UPDATE SET enabled = TRUE, label = EXCLUDED.label,
    display_order = EXCLUDED.display_order, updated_at = NOW();
```

Changes take effect on the next cache refresh with no restart: about 60s for the 1h view
(page-cache worker) and about 10min for the 24h/7d views (background refresher).

The migration creates the table **empty on purpose** — rows are environment config and are
inserted out of band, so they never live in this repository. An unseeded environment renders
an empty scoreboard, which is also the expected local-dev state.

DoubleZero's own feeds are never config rows: they are matched by the `tob_` (top-of-book) and
`mbp_` (market-by-price) prefixes, and a row for either is rejected by the loader with a WARN
(it would broaden the allow-list clause to races against unconfigured competitors, leaking
their feed ids into the payload). Both prefixes are DoubleZero's — an MBP publisher emits the
shared BBO observation on every derived top-of-book change, so it races the venue's public feed
exactly as the top-of-book feed does.

## Multicast Member Classification

Which multicast subscribers are DoubleZero's own recorders is settled in **four tiers**, and only
the top one can be changed without a deploy. In precedence order:

1. **Asserted.** An enabled row in the Postgres table `multicast_member_class` (`client_ip`,
   `class`, `label`, `note`, `enabled`), with `class` one of `recorder`, `internal_probe`,
   `customer`. Wins outright.
2. **Derived, capture host.** The Solana capture hosts in `edgeNodeIPs`, which the edge scoreboard
   already maintains for geoip. Classifies as `recorder`.
3. **Derived, operator wallet.** `doubleZeroOperatorWallets` in `edge_multicast_class.go`, an
   explicit allow-list of the ledger owner pubkeys DoubleZero runs its own boxes under. Classifies
   as `doublezero` and **never** as `recorder` — see below.
4. **Default:** `customer`, which means "nobody has said otherwise", not a verified fact. That is
   why the API also reports how many members each count actually knows about (`class_asserted` /
   `class_derived`; the wallet tier counts as derived).

Tier 3 is not the owner-based rule that was removed, and the distinction is the whole point. The
removed one *inferred*: it asked whether a member's owner published into any feed-backed group,
which every shreds validator does, so it matched 515 wallets and got `mbone` wrong 6 times out of 6.
This one *names* specific wallets, so it is an assertion in a Go literal, the same kind of thing
`edgeNodeIPs` is. Do not replace it with a rule over wallets — not the vanity `DZ` prefix, which is
unenforced and mintable, and not anything derived from what a wallet publishes or owns.

Tier 3 stops at "ours" because owner pubkey is stable but far too coarse: one wallet owns recorders,
probes and lab boxes at once. Naming the kind is the asserted tier's job, and `doublezero` is a
separate count from `recorders` on the payload so the page never claims a box records when nothing
has said it does.

Rows are keyed on `client_ip` and not on the user pk: DoubleZero user accounts are torn down and
recreated constantly, so a pk-keyed row goes stale within hours.

An asserted row wins even when it says `customer` — that is the escape hatch for a decommissioned
capture box, and it reaches both derived tiers. It matters more against the wallet tier than against
the host map: a wallet keeps matching until the ledger account itself is torn down.

**These statements are run by a human operator against the target environment.** They are recorded
here as a runbook, not as something to execute automatically: do not run them, or any other write
against this table, from an agent session unless explicitly asked to.

```sql
-- classify a box as a DoubleZero recorder
INSERT INTO multicast_member_class (client_ip, class, label)
VALUES ('<client_ip>', 'recorder', '<hostname>')
ON CONFLICT (client_ip) DO UPDATE SET class = EXCLUDED.class,
    label = EXCLUDED.label, enabled = TRUE, updated_at = NOW();

-- a decommissioned recorder handed back to a customer
UPDATE multicast_member_class SET class = 'customer', updated_at = NOW() WHERE client_ip = '<client_ip>';

-- stop asserting anything about a box (falls back to the derived signal)
UPDATE multicast_member_class SET enabled = FALSE, updated_at = NOW() WHERE client_ip = '<client_ip>';
```

Changes take effect on the next page-cache refresh (about 60s) with no restart.

The migration creates the table **empty on purpose** — which boxes are DoubleZero's is environment
config, differs between mainnet and testnet, and is inserted out of band, so it never lives in this
repository. An unseeded environment now still labels the DoubleZero boxes through the wallet tier;
what it cannot do is call any of them a `recorder` specifically. For the Kalshi and Hyperliquid
recorders an asserted row is the only way to get that far: the feeds tables identify them by
`measurement_node_id`, a hostname with no IP anywhere in that schema and no table in lake to
resolve it through.

## Edge Market Data Vocabulary

The canonical vocabulary for anything Edge market data — venues, feeds, publishers, channels — is
**[edge-feed-spec/GLOSSARY.md](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md)**,
and that file is the authority: a definition there overrides any local one. It binds specs, docs,
plans, comments, identifiers, CLI flags, config keys, metric names and log fields alike, so it
applies to Go identifiers and JSON field names in this repo, not just to prose.

The violations that keep appearing here:

| Do not write | Write | Why |
| --- | --- | --- |
| `lane` | `feed` (the traffic) or `path` (a redundant route) | banned outright |
| `source` bare | `capture source`, `source_ts`, `source IP address`, `upstream source` | several unrelated things share the prefix, so the qualifier is mandatory |
| `stream` (our traffic) | `feed` | `snapshot stream` / `delta stream` are the two exceptions |
| `arm` (any sense) | `path`, `branch`, or set/clear | banned outright; `ARM64` is a proper noun and survives |
| `epoch` (our own spans) | `era` | `Unix epoch` is the exception |
| `roster`, `active set` | `published set` | |

Two shapes recur in this codebase and are both correct: a foreign column name we do not own stays
as it is (`kalshi_bbo_observations.source`, aliased to `capture_source` in the projection), and a
ledger string is never renamed by search-and-replace — a `code` that stops matching its live group
fails silently, so renames sequence behind the ledger.

## Edge Multicast Health

**The verdict lives on the publisher line, and the group row carries none.** A badge over a feed
with one dead path and one live one describes neither, so `Health` and `Sequence` are per publisher
(`edgeMulticastPublisherHealth`); the group row identifies and counts, and its Sequence cell reports
only what no line can — series recorded from an address no publisher of the group carries. Two
consequences to keep in view: a collapsed group shows no verdict at all (lines auto-expand below
`PUBLISHER_LINES_OPEN_BELOW`), and `skewed` has no line to sit on, since capture-node parity is a
statement about a group's recorders and no single publisher owns it.

What a **collapsed** group has instead of a badge is `publisher_verdicts`, a tally of how many
lines landed in each state — a count of lines, not a verdict over them. It drives the Publishers
cell's dot, and it reads the per-line verdicts rather than the floor tally alone: without that, a
group nobody expanded would summarise itself on the counter plane and read clean while one of its
series was gapping. It is tallied before `edgeMulticastPublisherLineCap`, so what the payload
happens to carry cannot change it.

The per-publisher ranking is worst-first: `silent`, `thin`, `gapped`, `stalled`, `behind`,
`unknown`, `unrecorded`, `healthy`. The two states between the faults and `healthy` are not faults
and are not counted as such; the difference between them matters — `unknown` is no counter row at
all, `unrecorded` is a publisher clearing the floor that no recorder wrote a series for **while its
peers on the group have one**. `unknown` is graded first of the two, because a publisher nothing
measured is not "sending and unrecorded"; `TestEdgeMulticastPublisherHealth_UnknownWhenNothingMeasured`
pins it.

`unrecorded` exists because `healthy` here means the floor AND an intact series, so a publisher
with no series has only half of it. Returning `healthy` anyway put a measured, gapping feed beside
an unmeasured one and made the unmeasured one look the better of the two — observed on the Kalshi
sports pair, where `-mbp` read `gapped` and `-tob` read `healthy` for no reason other than that
nothing measures gaps on the top-of-book plane. It applies only where the group has series to be
missing from: the shreds groups run Turbine, have no recorded wire protocol at all, and `healthy`
is the whole truth there.

The group's Publishers cell counts publishers **above the floor**, which is the same thing the
lines' own status word says, so the row cannot contradict itself. It briefly counted `healthy`
instead and rendered `0/2` beside two lines that both said `publishing`. The per-line verdicts
still drive the cell's dot, so a group whose only fault is a gapped series does not read green. The
recorded planes are read **before** the counter's own absence: a series gapping at a recorder is a
finding whether or not the rate view has a row for the tunnel that sent it, and `unknown` returning
first hid exactly that — it is excluded from `Faulted()`, so the collapsed group's dot went grey
over a feed losing data. A publisher moving no bytes outranks one moving too few, and both outrank a recorded gap
— `thin` says the tunnel carries overhead and no product, a larger failure than a series that lost
some of a feed it is otherwise delivering. Two things stay out of it: **BGP status**, which keeps
its own marker beside the verdict because the ledger snapshot and the rate bucket are minutes apart
and can legitimately disagree; and **a series whose gaps were never counted**, whose zero gap count
is an absence rather than a reading — it still reaches `stalled`, graded on staleness alone, and the
`healthy` over it is the weaker claim "nothing was found wrong in what was recorded", which the
badge's tooltip spells out from `GapsUnmeasured`. It is deliberately not a verdict of its own: there
is no state between `healthy` and a fault, and minting one would paint every top-of-book line
permanently non-green over a property of the plane rather than of the path.
`behind` sits last of the faults because it is the mildest of them — the path is delivering, just
less of the feed than its peer.

### The DZD column

`edge_multicast_bgp.go` reads the DoubleZero device's own view of each publisher's BGP session from
`telemetry_<env>.bgp_neighbors_latest`. User sessions are `network_instance = 'vrf1'`, `peer_type =
'EXTERNAL'`, addressed on a link-local /31 that appears nowhere else in this schema — what makes
them addressable is `description`, which the device sets to `USER-<tunnel_id>`. With the device
pubkey that is exactly the (device, tunnel) pair every publisher line already carries.

It is shown **beside** `dz_users_current.bgp_status`, not instead of it. The ledger word is written
by the client agent and read out of a snapshot minutes old; this is the device, ~30s fresh, and it
carries what one word cannot — session uptime and `established_transitions`. A session up for an
hour after 200 flaps and one that came up once both read `up` in the ledger. Neither moves the
publisher verdict, for the reason already given above.

The **round trip** on the same column comes from a third place: the client agent writes the
smoothed BGP TCP RTT — read from the kernel's `tcp_info` for the BGP socket — into its own
serviceability `User` account as `BgpRttNs`, and lake keeps the series in **`fact_dz_user_bgp_rtt`**
(`dz_user_bgp_rtt_current` for the newest per user). It is the only measurement of the access path
that exists: `fact_dz_device_link_latency` is device-to-device across the backbone,
`fact_dz_internet_metro_latency` is metro-to-metro over the public internet, and the telemetry
mirror carries no timing of the user tunnel at all.

Three things about it are load-bearing:

- **It is a fact, not a dimension column.** `attrs_hash` covers every payload column of
  `dim_dz_users_history`, so a hashed rtt would mint a history row per user on every onchain
  rewrite — the churn `20260708000001` refused for `last_bgp_reported_at` — and an unhashed one
  would freeze at whatever value was current when some other attribute last changed. There is no
  non-hashed-column mechanism in `DimensionType2Dataset` today, and this needs none.
- **It is keyed on the onchain write**, `(user_pk, reported_at_slot)`, not on the observation. The
  agent submits only on a BGP status change or its ~6-hourly keepalive while the indexer polls
  every 60s, so the table grows with reports and `ReplacingMergeTree` collapses the re-observations.
- **It can be hours old, and that is normal.** It is a property of the path, not a live signal, and
  the UI carries its age for that reason. A report whose session was down carries a cleared rtt;
  the fact keeps it, and the page drops it rather than rendering 0.00 ms.
- **`event_ts` is the FIRST observation of a report, and the write path reads it back to keep it
  there.** The dedup version is `ingested_at`, so a poll clock stamped into `event_ts` would leave
  the surviving row carrying the newest of the 60s re-observations — a six-hour-old keepalive
  rendering as seconds old, which is the exact opposite of the point above.
  `TestLake_Serviceability_UserBGPRTT_ReobservationCollapses` asserts it.

The fact's **column order is part of the contract**: `WriteBatch` issues a bare `INSERT` with no
column list, so the migration and `dzsvc.userBGPRTTRow` must match position for position. That is
what `TestLake_Serviceability_UserBGPRTT_RowLandsInItsColumns` exists to catch.

### Row order

Groups read alphabetically by ledger code within their feed section. Publisher lines read by
**client IP**, ascending, compared as addresses and not as strings — dotted-quad text sorts
`148.51.120.152` before `148.51.120.6`, which is exactly the pair of Kalshi publishers on one
group.

That is the DISPLAY order, applied to the kept lines after `edgeMulticastPublisherLineCap` has
already truncated. Selection stays worst-first and must: sort by address before the cap and
truncation keeps an arbitrary twelve, with the faults as likely to be cut as not, while the notice
underneath still claims everything dropped was above the floor.

### Path parity

`behind` comes from `edgeMulticastPathParity`: a publisher path measured against the other paths of
the same capture source **at the same recording node**, below `edgeMulticastPathParityFloor` (98%).

Two things are in the key and one is deliberately out. The **recording node is in it**, so a
recorder that is behind on everything cancels out of the ratio instead of reading as a fault in
both paths. The **channel is not**: the two paths of a feed publish it on different channel ids —
mainnet runs a +100 offset, sports on 10-49 against 110-149, perps on 1 against 101 — so keying on
channel would put each path in a group of one and compare nothing. Counts are summed across
channels per path for the same reason.

The floor is tight because there is no legitimate spread to leave room for: measured over fifteen
minutes the two Kalshi paths agree **to the message** on all 29 sports capture sources and run
0.9985-1.0000 on perps. Compared against the **best** peer, never the mean — a mean over a pair
sinks with the faulty path and would report both at roughly 1.0 when one is broken. A path with no
peer at that node, or a pair where every path is silent, records neither a pass nor a fail.

The tight floor needs a **volume floor** under it: `edgeMulticastPathParityMinMessages` (500 over
the window), applied to the best path of the pair. Below a few hundred messages one message is a
percent or more where the floor leaves two, so 4 messages against 5 — measured on a market-by-price
instance — would read `behind`, and one failed pair marks the whole line across the 29-33 capture
sources a sports node compares. Under the floor the pair records nothing, not a pass.

This is the check that reaches what capture-node parity cannot. That one needs two recorders
(`edgeMulticastMinParityNodes`) and the sports capture runs on one, so it is inert on every sports
group; and where it can fire, its floor is half the median against a real fault of 0.3%. It is
still the only recorder-side signal, so it stays — but it no longer paints anything, since the
group verdict it feeds is not rendered.

The two checks behind it, both in `api/handlers/edge_multicast_publishers.go`, are not an
"is anyone sending" rollup:

1. **Publisher floor.** Every publisher must clear `edgeMulticastPublisherFloorBps` (1 Kbps) on its
   own tunnel counter. Below it — trickle or zero — the group reads `thin`. The feeds on this page
   run at megabits, so a kilobit is overhead with no product behind it.
2. **Capture-node parity.** Recording nodes on one group receive the same feed, so each node's
   sample count must stay within `edgeMulticastNodeParityFloor` (half) of the group's median. A node
   under it reads `skewed`.

The group-level roll-up those two checks produce is **still in the payload** as
`EdgeMulticastGroup.Health` — it is what `capture_nodes_lagging` and `skewed` feed, and the only
place parity can be reported at all — but the page no longer renders it as a badge. Its verdicts
rank worst-first: `silent` (nothing sending) → `thin` → `skewed` → `unknown` (nothing measured) →
`healthy`. A publisher fault outranks a receiver fault, and an unmeasured publisher never forces a
verdict — one device's telemetry gap is not a fault in the feed.

**"Every publisher" means every publisher, including the validator fan-in groups.** Measured on
mainnet: the two Kalshi perps groups have 2 publishers each, both above the floor; `edge-solana-shreds`
has 767, of which 605 clear the floor, 3 are thin and 149 are idle. So the shreds and root rows read
`thin` as their steady state — a validator that is not sending is normal there, and the old verdict
called the same group healthy. If that reads as noise rather than signal, the fix is a share
threshold on the verdict (not a change to the counts, which stay strict), and it needs a product
decision rather than a quiet default.

The **Sequence** column is separate from that verdict and reports the recorded wire protocol's own
counters. **A series belongs to one publisher, so the verdict sits on the publisher line**: each path
runs its own counters and one can gap while its peer is intact, which a group-level cell can only
report as "this group gapped" — naming neither the broken path nor the healthy one.

Sequencing keys on the channel instance, `(source IP address, Channel ID, destination port)`.
`kalshi_mbp_levels` carries the source address as `publisher_source_ip` — the arm axis is a column in
the capture schema on purpose — so the key here is `(source IP address, Channel ID, recording node)`,
matched against the ledger's `dz_ip` to find the line. Two folds are deliberate: the destination port
(only `Sequence Number` is per port; `Reset Count`, the manifest and the book-level counters this
column carries span the three ports) and never the recording node (two vantages are two independent
observations). A series whose address matches no publisher of the group is counted as
`unattributed` on the group roll-up rather than dropped.

Publisher lines also carry the ledger's **`bgp_status`**, and `down` renders as an error on the line.
That is not a reversal of the rule that the control-plane roll-up must not paint the row: what that
rule rejects is a worst-of over every *member*, where customers with BGP down turned every group red.
A publisher with no session cannot be sending the feed it is registered to send. It deliberately does
not move the group verdict — the ledger snapshot and the rate bucket are minutes apart, so a publisher
can read `down` while its tunnel still moved bytes, and both are shown.

That column **folds cached refresher payloads and runs no query of its own**, and it has two legs.

**Market-by-price** comes from `kalshi_l2_coverage.go`. `kalshi_mbp_levels` is level-grain and
TTL-less, and a fifteen-minute question reads most of a day through a `remoteSecure()` proxy
(~135M rows), which is why that file owns the scan on a ten-minute refresher. Re-running it on a
page that polls every 30s would be the same scan again and would let the two pages disagree about
one feed. Gap counts are **books**, never gap-marked messages — the message count is a duration
that scales with traffic.

**Top-of-book** comes from `edge_multicast_tob_sequence.go`, on the same refresher and the same
fifteen-minute window (measured: 3.8s over every `tob_` capture source). It reads
`kalshi_bbo_observations`, which carries the wire protocol's `sequence` and `reset_count` on every
row plus a `raw_meta` JSON object holding `publisher_source_ip`, `multicast_group` and `port`. The
address in `raw_meta` is the primary group key — it is the destination the datagrams carried, where
the capture source name is a convention that has been renamed once already — with the name as
fallback.

That leg **cannot count gaps, and must not pretend to**. There is no gap marker on this plane, and
the obvious substitute is wrong by construction: `kalshi_bbo_observations` holds one row per change
to the top of the book, so a wire message that did not move the BBO legitimately leaves a hole in
the numbering. Measured on mainnet, one instance carried 23,846 rows across a sequence span of
24,553 — about 3% "missing" with nothing wrong. A count-versus-span test would paint every healthy
top-of-book series permanently red. So those instances carry `gaps_measured: false`, the roll-up
counts them in `gaps_unmeasured`, and the badge reads **`advancing`** rather than `ok`: the counters
move and nothing checked them for loss. Closing that half needs the producer to emit a gap marker
for top-of-book the way it does for market-by-price; it is not work this repo can do.

The cost of both legs is staleness, so `sequence_as_of` is in the payload — the **older** of the two
legs — and the column ages against it. A cache miss costs that plane's rows, never the page.

Both folded payloads are **mainnet only**, gated on `isMainnet(ctx)` in `FetchEdgeMulticastData`.
The refresher runs with no environment in context, so it always computes mainnet, and the keys carry
no environment either — while the group key they resolve through is the multicast address, and both
networks allocate out of the same `233.84.178.0/24`. The gate closed a **live** leak, not a latent
one: testnet has three activated `edge-` groups, and `edge-solana-retrans` has sat on
`233.84.178.17` — mainnet's `edge-kalshi-sports-tob` — continuously since 2026-07-23, so testnet was
rendering mainnet Kalshi series on a Solana retransmit group. `edge-solana-root` collides too, on
`.12` (mainnet's `edge-solana-retrans-eu`), and is harmless only because both folded payloads read
Kalshi tables and have nothing to attach there.

### What the page is about

The subject is the **feed and the publishers that fill it**, not who buys it. The subscriber side
earns its column for one reason: the **DoubleZero count** beside the total, because those boxes are
the apparatus every application-plane column is measured at (Heard, Sequence, Msg/s, Peer), and a
group with none of them has no application-plane signal at all.

The column says **Subscribers**, not "Recorders", and that is not cosmetic. With
`multicast_member_class` unseeded, every DoubleZero box on it is matched by the operator-wallet
tier, which establishes whose box it is and explicitly cannot say whether it records — so
"recorders" would assert what no tier has established. The customer breakdown stays in the tooltip
and in the payload rather than on the row.

Two per-path columns come from the observations payload and cost no query: **Msg/s**, the recorded
message rate, and **Peer**, the parity ratio. Both are dropped entirely when that payload is
absent, the same rule Heard and Sequence follow — a missing cache costs the columns, never the
page. That is not a rare state: `page_cache` survives a pod restart, so a **newly added** cache key
is the one entry a deploy leaves empty, and it stays empty until the refresh chain reaches it.
`StartKalshiBackgroundRefresher` is serial with a three-minute timeout per step, which is why the
observations leg runs **first** — it is the cheapest and the only one with no live-query fallback. Msg/s sits beside the counter rate rather than
replacing it — the counter is per tunnel, minutes late, and an upper bound a multi-group publisher
shares across its groups; this is per group, from the far end, so it is what arrived rather than
what was sent, and it is blank for any feed with no recorder behind it. Neither figure is on the
group row: summing recorded rates over a group's paths would double the feed, since redundant paths
carry the same traffic, and a parity ratio means nothing until you name which path it is about.

Parity is measured on the **application plane** (`kalshi_bbo_observations`,
`slot_feed_race_summary_v2`) and cannot move to the counters. Interface counters are per tunnel: a
recorder subscribed to several groups reports the sum against each, so on mainnet the Tokyo recorder
reads 232 Mbps against a group whose entire ingress is 3.6 Mbps. Every Kalshi recorder is
multi-group, so a counter-based parity check would be permanently wrong on exactly the feeds it
exists for.

## Basemap Tiles

`web/src/lib/basemap.ts` owns the CARTO tile URL for every map surface. Two things must move
together with it:

- **The CSP in `api/main.go`** must allow the tile host in both `connect-src` and `img-src`. The
  entry is a wildcard (`https://*.basemaps.cartocdn.com`), which matches `a.basemaps.cartocdn.com`
  but **not** a bare `basemaps.cartocdn.com` — a host change that skips the CSP blocks every tile
  with no error anywhere but the browser console.
- **`CARTO_API_KEY`** on the API, surfaced to the browser through `/api/config`. It ships in the
  bundle, so it is public by construction: a plaintext deployment value, not a sops secret. Do not
  file a follow-up to "restrict it" — CARTO has no domain/referer scoping for basemap keys (the
  domain box on the request form is informational, verified by request), so nothing bounds the key
  but CARTO's fair-use quota of 5M tile requests per calendar month and their right to revoke it.
  One key is shared across environments. If the quota is ever burned by someone else's site, the
  remedy is to rotate the key, or to proxy tiles through the API so it stops shipping to browsers.

Both failure modes are silent by construction, which is why the URL lives in one module: CARTO
answers a keyless request with **HTTP 200 and a valid PNG** carrying `API KEY REQUIRED` burned
into the image, so MapLibre sees a perfectly good tile and nothing logs.

## Logging Levels

ERROR-level log lines page on-call (alerts fire on `level="ERR"` — prod → `#alerts`, staging → `#alerts-l2`). Reserve raw `.Error(...)` calls for genuinely-actionable terminal failures: process/component death, startup failures, panics, config errors.

Any log call that can carry a transient, not-found, client-cancel, or lifecycle error must go through a classification helper from `utils/pkg/logger`:

- `logger.Error(log, msg, args...)` — one-shot failures: logs transient causes (`utils/pkg/dberror.IsTransient`: connection blips, timeouts, rate limits) and disconnect-class context errors at WARN, everything else at ERROR; it never drops a line. `api/handlers.logError` wraps this and additionally skips client disconnects entirely (on a request path the caller is gone).
- `logger.Escalator` — periodic/background loops (view refreshes, workflow iterations, cache refreshes): `Fail` logs WARN below a consecutive-failure threshold and ERROR at/above (default 3, transient 10), `Reset` on success. A single blip stays at WARN; sustained failure still pages. Set `ErrorAfterDuration` when the interval between `Fail` calls is not fixed, so a count alone doesn't describe how long a failure has lasted; it escalates on elapsed time as well, never later than the count would. It reads the wall clock, so never set it from Temporal workflow code.

One failure produces at most one alert-bearing line, owned by the layer with the escalation context: a layer above an escalation-gated log (e.g. a Temporal workflow observing an activity that already self-escalates via `logger.Escalator`) must not re-report the same failure at ERROR — it doubles the page without adding information. Re-report at WARN if the outer layer carries a distinct, useful cause (e.g. a StartToClose timeout shape).

Non-actionable conditions that should never log ERROR: empty/not-found results (return a 404 instead), client disconnects, worker/pod shutdown, deploy-time dependency races, expected Temporal lifecycle events, and served-stale/degraded fallbacks (WARN/INFO).

## Git Commits

- Do NOT include "Co-Authored-By" lines in commit messages
- Use the format `component: short description` (e.g., `indexer: fix flaky staging test`, `telemetry: use CLICKHOUSE_PASS env var`)
- Keep the description lowercase (except proper nouns) and concise

## Pull Requests

- Use the `/pr-text` skill to generate PR descriptions, then use `gh pr create`
- Do not include "Generated with Claude Code" or similar footers
- PR title format: `component: short description` (same as commit messages)
- Summary bullets should be concise, ordered by importance/significance
- Focus on "what" and "why", not implementation details
- Include a "Testing Verification" section
- Don't mention table-stakes items in testing verification (e.g., "compiles cleanly", "builds successfully", "no lint errors"). Only include meaningful verification like specific test scenarios, behavioral observations, or edge cases validated.
- Group related changes together
- Mention any breaking changes or migration steps if applicable
