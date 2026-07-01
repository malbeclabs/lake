# Hyperliquid BBO Scoreboard Design

**Status:** Approved · **Date:** 2026-06-29

Live scoreboard proving DoubleZero delivers Hyperliquid best-bid/offer (BBO)
market-data updates faster than competing feed providers. Displays DoubleZero's
win share, per-competitor win rate and lead time, and a per-vantage breakdown,
with a live strip of the most recent races.

This is a sibling to the **Shreds / Edge Scoreboard** feature and deliberately
reuses its patterns end to end (remote-proxied summary table → Go handler →
page cache → React page). Where this doc is silent on a mechanism, the
edge-scoreboard implementation (`api/handlers/edge_scoreboard.go`,
`web/src/components/edge-scoreboard-page.tsx`) is the reference.

## Concept

For each Hyperliquid BBO update — uniquely identified by
`(symbol, source_ts_ms, bbo_hash)` observed at a measurement vantage point —
multiple **feeds** (transport/provider sources) deliver the same logical update.
The feed with the lowest receive timestamp **wins** that race. This is the direct
analog of a shredder "slot race", with a market-data update standing in for a
Solana slot.

**DoubleZero = every feed whose name starts with `tob_`** (our top-of-book feeds
across clouds/regions). All other feeds are competitors:

| Raw feed(s)              | Rollup label  | Role                  |
|--------------------------|---------------|-----------------------|
| `tob_*` (all)            | **DoubleZero**| us                    |
| `hyperliquid_public_bbo` | Public API    | baseline (free)       |
| `hydromancer_bbo`        | Hydromancer   | paid competitor       |
| `hyperpc_shared_bbo`     | HypeRPC       | paid competitor       |
| `quicknode_l2book_bbo`   | QuickNode     | paid competitor       |

The competitor set lives as a small ordered slice of `{rawFeed, label}` in Go;
adding/renaming a competitor is a one-line change. The `tob_*` rollup is applied
in SQL via `startsWith(feed, 'tob_')`.

## Data Source

**Table:** `{feeds_db}.hyperliquid_bbo_feed_race_summary` (default db: `feeds`)

This summary table already exists on the remote ClickHouse Cloud instance
(`btjr1b5uy8…`, the **same host as shredder**) and is maintained there by an
existing refreshable materialized view (`hyperliquid_mv_bbo_feed_race_summary`,
`REFRESH EVERY 1 MINUTE APPEND`) reading from raw `feeds.hyperliquid_bbo_observations`.
**Lake does not build or own any materialized view** — it only proxies and reads
the summary, exactly as it does for `shredder.slot_feed_race_summary_v2`.

| Column                  | Type                  | Description                                                        |
|-------------------------|-----------------------|-------------------------------------------------------------------|
| event_ts                | DateTime64(9, UTC)    | Winner receive time for the race                                  |
| ingested_at             | DateTime64(9, UTC)    | Row write time (ReplacingMergeTree version column)               |
| capture_run_id          | String                | Capture session id (part of race key)                            |
| measurement_node_id     | String                | Vantage recorder, e.g. `chi-mn-recorder1`                        |
| host                    | String                | Recorder host                                                    |
| location_code           | LowCardinality(String)| Vantage metro: `chi`, `nyc`, `tyo`                               |
| feed_type               | LowCardinality(String)| Always `bbo` in v1 (gossip type exists but is unpopulated)       |
| symbol                  | LowCardinality(String)| Trading symbol, e.g. `BTC`, `ETH`, `xyz:SILVER` (~300 distinct)  |
| source_ts_ms            | UInt64                | Source-side update timestamp (part of race key)                  |
| bbo_hash                | UInt64                | Hash of the BBO payload (part of race key)                       |
| feed                    | LowCardinality(String)| The **winner** feed for the row                                  |
| loser_feed              | LowCardinality(String)| `''` for aggregate rows; the loser feed for pairwise rows        |
| total_events            | UInt64                | Always `1` (one race per row)                                    |
| events_won              | UInt64                | Always `1` (one race per row)                                    |
| lead_time_p50_ms        | Float64               | Lead in ms over `loser_feed` for this race (p50 == p95)          |
| lead_time_p95_ms        | Float64               | Same value as p50 (single sample per row)                        |
| send_lead_time_p50_ms   | Nullable(Float64)     | Send-timestamp-based lead (sparse; **unused in v1**)             |
| send_lead_time_p95_ms   | Nullable(Float64)     | Same (unused in v1)                                              |

**Key semantics (verified against live data):**
- **One row == one race.** `events_won == total_events == 1` and
  `lead_time_p50_ms == lead_time_p95_ms` for every row. So the set of `lead_time_p50_ms`
  values across rows *is* the true per-race lead-time distribution (one sample per race), and
  counting distinct race keys counts races. Query time uses the bounded-memory estimators
  `quantileTDigest` / `uniqCombined` over that distribution rather than the exact variants (see
  Design Decisions) — a sub-1% approximation, not a semantic change.
- **Aggregate rows** (`loser_feed = ''`): one per `(race key, winner feed)` —
  used for "who won" counts.
- **Pairwise rows** (`loser_feed != ''`): one per `(race key, winner, loser)` —
  carry the per-race lead of the winner over that specific loser.
- Query with `FINAL` to dedup the ReplacingMergeTree (overlapping MV append
  windows write the same race key repeatedly).
- The race key is `(capture_run_id, measurement_node_id, symbol, source_ts_ms, bbo_hash)`.

### Why summary-only is sufficient and exact for v1

Because each row is a single race with its exact lead, the summary table answers
every v1 metric exactly and cheaply, with no need to touch the 33B-row /
667 GiB raw `hyperliquid_bbo_observations` table. The raw table is required only
for deferred features (see Non-Goals).

**One semantic caveat (accepted for v1):** pairwise rows only relate the race
*winner* to each loser. When a *third* feed wins a race in which both DoubleZero
and competitor X participated, that race does not produce a direct `tob_*`-vs-`X`
pairwise row, so it is excluded from the X head-to-head. Given `tob_*` wins the
large majority of races, this is a negligible undercount. The exact fix (recompute
head-to-head from raw observations) is the deferred raw-table path.

## API

**`GET /api/dz/hyperliquid/scoreboard`**

Query params:
- `window` — `1h`, `24h`, `7d` (default: `24h`)
- `symbol` — a symbol to filter to, or omitted/`all` for the aggregate (default: all)
- `since_ts` — RFC3339 / epoch cursor for the live tail (returns races after the
  cursor; bypasses the standard cache, served from the fast-refresh cache)

Param handling mirrors the edge scoreboard: the **default request shape**
(`window=24h` or omitted, no `symbol`, no `since_ts`) is served from the page
cache (`X-Cache: HIT`); any other shape bypasses the cache and runs a bounded
live query (`X-Cache: MISS`).

### Response

```json
{
  "window": "24h",
  "symbol": null,
  "generated_at": "2026-06-29T17:00:00Z",
  "feed_type": "bbo",
  "dz_win_share_pct": 96.4,
  "total_races": 13680322,
  "competitors": [
    { "feed": "hyperliquid_public_bbo", "label": "Public API",  "dz_win_pct": 99.1, "lead_p50_ms": 2.4, "lead_p95_ms": 8.1, "races": 12300000 },
    { "feed": "hydromancer_bbo",        "label": "Hydromancer", "dz_win_pct": 94.2, "lead_p50_ms": 0.9, "lead_p95_ms": 3.7, "races": 1100000 },
    { "feed": "hyperpc_shared_bbo",     "label": "HypeRPC",     "dz_win_pct": 97.8, "lead_p50_ms": 1.6, "lead_p95_ms": 5.2, "races": 400000 },
    { "feed": "quicknode_l2book_bbo",   "label": "QuickNode",   "dz_win_pct": 95.0, "lead_p50_ms": 1.1, "lead_p95_ms": 4.4, "races": 900000 }
  ],
  "nodes": [
    {
      "measurement_node_id": "aws-tyo-mn-recorder1",
      "location_code": "tyo",
      "dz_win_share_pct": 99.2,
      "total_races": 4600000,
      "competitors": [
        { "feed": "hydromancer_bbo", "label": "Hydromancer", "dz_win_pct": 98.9, "lead_p50_ms": 0.6, "lead_p95_ms": 2.1, "races": 380000 }
      ]
    }
  ],
  "recent_races": [
    { "event_ts": "2026-06-29T16:59:59.9Z", "symbol": "BTC", "winner_feed": "tob_gcp_tyo_hl_mainnet1", "is_dz": true, "runner_up_feed": "hydromancer_bbo", "runner_up_label": "Hydromancer", "lead_ms": 1.2, "location_code": "tyo" }
  ]
}
```

Notes:
- `dz_win_share_pct` uses the dashboard's win-share definition:
  `sumIf(events_won, startsWith(feed,'tob_') AND NOT startsWith(loser_feed,'tob_'))
   / nullIf(sumIf(events_won, startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')), 0)`
  over pairwise rows (`loser_feed != '' AND feed != loser_feed`).
- Per-competitor `dz_win_pct` and `lead_p50/p95_ms` come from pairwise rows where
  exactly one side is `tob_*` and the other is that competitor; lead percentiles
  are `quantileTDigest` over `lead_time_p50_ms` for races DoubleZero won.
- `nodes[]` is the same computation grouped by `measurement_node_id` /
  `location_code` (the per-vantage breakdown).
- `recent_races[]` is the newest aggregate rows (`loser_feed=''`) ordered by
  `event_ts DESC`, joined to the closest competitor + lead.

## Caching

Registered in `api/worker/workflow.go` alongside the edge-scoreboard entries:
- **`hyperliquid_scoreboard`** — default view (all symbols, `window=24h`,
  aggregated + per-node). Standard 30s refresh.
- **`hyperliquid_scoreboard:latest`** — last N races for the live strip.
  Fast refresh (~3–5s), mirroring `edge_scoreboard:latest`.

Symbol-filtered or non-default-window requests bypass the cache and run bounded
live queries. The handler reads the cache for the default shape exactly like
`GetEdgeScoreboard`.

## Backend Changes

| File | Change |
|------|--------|
| `admin/remotetables/setup.go` | add `{"feeds", "hyperliquid_bbo_feed_race_summary"}` to `externalRemoteTables` |
| `api/config/config.go` | add `feedsDB = "feeds"`, `CLICKHOUSE_FEEDS_DB` override, `GetFeedsDB()`/`SetFeedsDB()` |
| `api/handlers/api.go` | add `FeedsDB string` field on `API` |
| `api/main.go` | init `FeedsDB: config.GetFeedsDB()`; register `GET /api/dz/hyperliquid/scoreboard` |
| `api/handlers/hyperliquid_scoreboard.go` (new) | `GetHyperliquidScoreboard`, `FetchHyperliquidScoreboardData`, response structs, rollup + competitor table |
| `api/worker/workflow.go` | register `hyperliquid_scoreboard` + `hyperliquid_scoreboard:latest` cache entries |

**Prerequisite (ops):** lake's existing remote reader (`REMOTE_CH_*`) must be
granted `SELECT` on `feeds.*` on the ClickHouse Cloud instance. This preserves the
single-credential proxy model (no new credential plumbing in
`admin/remotetables/setup.go`). The separate `feeds_reader` credential is used only
for the local seed script.

## Web Changes

| File | Change |
|------|--------|
| `web/src/App.tsx` | route `/dz/hyperliquid` → redirect to `/dz/hyperliquid/scoreboard` → `HyperliquidScoreboardPage` |
| `web/src/components/sidebar.tsx` | new **"Hyperliquid"** collapsible group under the **Edge** section, sibling to **Shreds**; child **Scoreboard**; `isHyperliquidRoute` detection |
| `web/src/components/hyperliquid-scoreboard-page.tsx` (new) | the page (header, headline, per-competitor table, per-node cards, live races strip, window + symbol selectors) |
| `web/src/lib/api.ts` | `fetchHyperliquidScoreboard(window, symbol, opts)` + `HyperliquidScoreboardResponse` / `…Competitor` / `…Node` / `…Race` types |

### Page layout

```
Hyperliquid ▸ BBO Scoreboard         window:[24h ▾]   symbol:[All ▾]

  DoubleZero wins  96.4%  of races            (all vantages, 24h)

  vs Competitor   DZ win%    median lead    p95 lead    races
  Public API       99.1%      +2.4 ms        +8.1 ms     12.3M
  Hydromancer      94.2%      +0.9 ms        +3.7 ms      1.1M
  HypeRPC          97.8%      +1.6 ms        +5.2 ms      0.4M
  QuickNode        95.0%      +1.1 ms        +4.4 ms      0.9M

  By vantage:   [chi]   [nyc]   [tyo]        (per-node cards, same stats)

  Recent races (live)   symbol  winner               lead vs runner-up
  BTC   DoubleZero (tob_gcp_tyo)     +1.2 ms vs Hydromancer
  ...                                            (polls :latest cache)
```

The page is built lean (not a fork of the ~1,950-line edge page). Shared chrome
(`PageHeader`, `Tooltip`, formatting helpers) is reused; bespoke gauge/bar
components are built locally for v1. Extracting shared scoreboard components is a
possible later refactor, intentionally deferred to avoid coupling two large files.

## Error Handling

- **Missing proxy table** (e.g., local dev without the `feeds` proxy): the handler
  returns an empty-but-valid response and the page renders a "no data yet" state —
  the same failure mode shredder has. The page-cache refresher gates its
  `feeds`-backed entries on table presence so it does not reproduce the noisy
  repeating `Database … does not exist` log loop.
- **Cache miss / transient DB error:** degrade to the last cached value when
  present; the page never returns a 500 for a data-availability problem.

## Testing

- **Go** — `api/handlers/hyperliquid_scoreboard_test.go`: seed a local
  `feeds.hyperliquid_bbo_feed_race_summary` with crafted rows (mirroring how
  `edge_scoreboard_test.go` seeds `slot_feed_race_summary_v2`) and assert:
  the `tob_*` rollup, `dz_win_share_pct`, per-competitor `dz_win_pct` and exact
  lead percentiles, and per-node grouping — all against known inputs.
- **Seed script** — `scripts/seed-hyperliquid-local.sh`: pull recent rows from
  remote `feeds` (using the `feeds_reader` credentials) into local ClickHouse for
  UI development; the analog of `seed-shredder-local.sh`.
- **Web** — `tsc -b` clean; manual smoke against seeded/proxied data.

## Non-Goals (deferred to v2+)

These require the raw `hyperliquid_bbo_observations` table and/or new surfaces and
are explicitly out of v1 scope:
- Send-vs-receive **loss attribution** (`send_timestamp_ns`: "publisher sent late"
  vs "network was slower").
- Strict three-way **head-to-head** (recomputed from raw, removing the win-share
  caveat above).
- Absolute **blocktime → receive** latency panels (p50/p90/p99).
- The **gossip** feed type (`hyperliquid_gossip_*` tables, currently empty).
- Per-symbol leaderboard, time-series charts, and historical drill-downs.

## Implementation Revisions (post-review, 2026-06-29)

Changes made while validating the page against production data:

- **Default window is `1h`, not `24h`.** The summary table has no time-based index, so a 24h
  aggregation over the (proxied) production table runs ~60s and exceeds the page-cache refresh
  deadline. Only the `1h` view is cached; `24h`/`7d` remain selectable but run as slower live
  queries. The proper fix (a time-ordered projection/materialized view on the remote summary,
  analogous to the shreds slot-range index) is deferred and tracked for the feeds owner.
- **Queries dedup with a distinct-key count over the sorting key instead of `FINAL`.** `FINAL`
  dominated query time; win rates are ratios and lead percentiles are duplicate-insensitive, so
  dropping it keeps results correct while cutting latency. The headline is derived from the
  per-node aggregation (one fewer scan), which also makes headline and per-competitor totals
  consistent.
- **Aggregations use approximate estimators (`uniqCombined`, `quantileTDigest`), not the exact
  variants.** The exact functions (`uniqExact`, `quantileExact`) buffer per-group state
  proportional to the window's race count (~7.5M matchup rows/hour → ~750 MiB of aggregation
  state at current volume). On the memory-constrained preview/dev ClickHouse — where the `feeds`
  tables are `remoteSecure()` proxies, so the GROUP BY runs on the small local instance — that
  tripped the OvercommitTracker and got the page-cache refresh killed (`code 241`) under the
  concurrent refresh load. The approximate estimators use bounded (~KB/group) memory; at
  scoreboard scale their sub-1% error is invisible (win % is a ratio, race totals and lead-time
  percentiles are display-only), and they are exact at the small cardinalities the unit tests
  assert. This does **not** reduce rows *read* — the `event_ts` filter still does not prune
  through the proxy (no time index), the separately-tracked deferred item below.
- **Recent-races window widened to 5 minutes** (the remote MV lags ~50-90s, so a 2-minute
  window intermittently came up empty). The `LIMIT` still returns only the newest races.
- **Local dev uses a `remoteSecure()` proxy to production** (`scripts/setup-feeds-remote-local.sh`,
  using the `feeds_reader` credentials) rather than static seed data, which was unrepresentative.
- **Web page restyled** to match the shreds scoreboard (full-width layout, win-rate gauge,
  per-vantage table).
