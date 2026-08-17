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
their feed ids into the payload). Both prefixes are DoubleZero's — an MBP source emits the
shared BBO observation on every derived top-of-book change, so it races the venue's public feed
exactly as the top-of-book lane does.

## Logging Levels

ERROR-level log lines page on-call (alerts fire on `level="ERR"` — prod → `#alerts`, staging → `#alerts-l2`). Reserve raw `.Error(...)` calls for genuinely-actionable terminal failures: process/component death, startup failures, panics, config errors.

Any log call that can carry a transient, not-found, client-cancel, or lifecycle error must go through a classification helper from `utils/pkg/logger`:

- `logger.Error(log, msg, args...)` — one-shot failures: logs transient causes (`utils/pkg/dberror.IsTransient`: connection blips, timeouts, rate limits) and disconnect-class context errors at WARN, everything else at ERROR; it never drops a line. `api/handlers.logError` wraps this and additionally skips client disconnects entirely (on a request path the caller is gone).
- `logger.Escalator` — periodic/background loops (view refreshes, workflow iterations, cache refreshes): `Fail` logs WARN below a consecutive-failure threshold and ERROR at/above (default 3, transient 10), `Reset` on success. A single blip stays at WARN; sustained failure still pages.

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
