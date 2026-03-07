# Edge Scoreboard Design

**Status:** Approved · **Date:** 2026-03-07

Live scoreboard proving DZ Edge delivers Solana shreds faster than alternatives.
Displays completeness, win rates, and lead times across measurement nodes.

## Data Source

**Table:** `{shredder_db}.slot_feed_races`

| Column           | Type          | Description                                      |
|------------------|---------------|--------------------------------------------------|
| event_ts         | DateTime64(3) | Stats flush timestamp                            |
| ingested_at      | DateTime64(3) | Row write time                                   |
| node_id          | String        | Shredder instance (`<loc>-<env>-bm#`)            |
| feed_type        | String        | Always `"shred"` for now                         |
| epoch            | UInt64        | Solana epoch                                     |
| slot             | UInt64        | Solana slot number                               |
| feed             | String        | `"dz"`, `"turbine"`, `"jito"`, `"pipe"`          |
| total_shreds     | UInt64        | Total unique shreds for this slot (same all feeds)|
| shreds_won       | UInt64        | Shreds where this feed was fastest               |
| lead_time_p50_ms | Float64       | Median lead time when winning                    |
| lead_time_p95_ms | Float64       | P95 lead time when winning                       |

Key semantics: one row per `(node_id, slot, feed)`. `shreds_won` across all
feeds for a slot sums to `total_shreds`.

## API

**`GET /api/dz/edge/scoreboard`**

Query params:
- `window` — `1h`, `24h`, `7d`, `30d`, `all` (default: `24h`)

### Response

```json
{
  "window": "24h",
  "generated_at": "2026-03-07T17:00:00Z",
  "current_epoch": 810,
  "total_slots": 432000,
  "dz_slots": 19008,
  "completeness_pct": 4.4,
  "nodes": [
    {
      "node_id": "slc-qa-bm1",
      "location": "SLC",
      "metro_name": "Salt Lake City",
      "latitude": 40.48,
      "longitude": -111.91,
      "feeds": {
        "dz":      { "shreds_won": 8560417, "total_shreds": 12500000, "win_rate_pct": 68.5, "lead_p50_ms": 3.2, "lead_p95_ms": 9.8 },
        "jito":    { "shreds_won": 2100000, "total_shreds": 12500000, "win_rate_pct": 16.8, "lead_p50_ms": 5.1, "lead_p95_ms": 11.2 },
        "turbine": { "shreds_won": 1800000, "total_shreds": 12500000, "win_rate_pct": 14.4, "lead_p50_ms": 0.0, "lead_p95_ms": 0.0 },
        "pipe":    { "shreds_won": 40000,   "total_shreds": 12500000, "win_rate_pct": 0.3,  "lead_p50_ms": 4.0, "lead_p95_ms": 8.5 }
      },
      "slots_observed": 19008,
      "last_updated": "2026-03-07T16:59:00Z"
    }
  ]
}
```

### Handler design

Go handler in `api/handlers/edge_scoreboard.go`. Registered in `main.go` as:
```go
r.Get("/api/dz/edge/scoreboard", handlers.GetEdgeScoreboard)
```

Uses `config.ShredderDB` for the database name. Queries `slot_feed_races`
scoped to the time window, then JOINs `dz_metros_current` (via `envDB`) to
resolve metro name and lat/lng from the node_id location code.

### ClickHouse query

```sql
WITH
  time_filter AS (
    SELECT now() - INTERVAL 24 HOUR AS start_ts
  ),
  slot_summary AS (
    SELECT
      node_id,
      COUNT(DISTINCT slot) AS total_slots,
      COUNT(DISTINCT IF(feed = 'dz', slot, NULL)) AS dz_slots
    FROM {shredder_db}.slot_feed_races
    WHERE event_ts >= (SELECT start_ts FROM time_filter)
      AND feed_type = 'shred'
    GROUP BY node_id
  )
SELECT
  r.node_id,
  r.feed,
  SUM(r.shreds_won) AS shreds_won,
  SUM(r.total_shreds) / COUNT(DISTINCT r.slot) * COUNT(DISTINCT r.slot) AS total_shreds,
  SUM(r.shreds_won) * 1.0 / SUM(r.total_shreds) AS win_rate_pct,
  SUM(r.lead_time_p50_ms * r.shreds_won) / SUM(r.shreds_won) AS lead_p50_ms,
  SUM(r.lead_time_p95_ms * r.shreds_won) / SUM(r.shreds_won) AS lead_p95_ms,
  s.total_slots,
  s.dz_slots,
  MAX(r.event_ts) AS last_updated
FROM {shredder_db}.slot_feed_races r
JOIN slot_summary s ON r.node_id = s.node_id
WHERE r.event_ts >= (SELECT start_ts FROM time_filter)
  AND r.feed_type = 'shred'
  AND r.slot IN (
    SELECT DISTINCT slot FROM {shredder_db}.slot_feed_races
    WHERE feed = 'dz'
      AND feed_type = 'shred'
      AND event_ts >= (SELECT start_ts FROM time_filter)
      AND node_id = r.node_id
  )
GROUP BY r.node_id, r.feed, s.total_slots, s.dz_slots
ORDER BY r.node_id, r.feed
```

The Go handler:
1. Runs this query, scans rows into a flat list
2. Groups by `node_id`, nests feeds into a map
3. Parses location code from `node_id` (first segment before `-`, uppercased)
4. Queries `dz_metros_current` for matching metro codes to get name/lat/lng
5. Computes global `completeness_pct` = sum(dz_slots) / sum(total_slots)
6. Returns the structured JSON response

Lead time uses weighted average (weighted by `shreds_won`). This is an
approximation — not a true global percentile — but proportionally fair since
slots where DZ won more shreds contribute more.

## Frontend

### Route & component

- **Route:** `/dz/edge/scoreboard` in `App.tsx`
- **Component:** `web/src/components/edge-scoreboard-page.tsx`
- **Sidebar:** Trophy icon + "Scoreboard" link next to Publisher Check in DZ section

### Page layout (top to bottom)

1. **Page header** — "Edge Scoreboard" via `PageHeader`. Actions: time window
   toggle buttons (`1h`/`24h`/`7d`/`30d`/`All`) + green "LIVE" indicator.

2. **Completeness banner** — Always-visible amber info banner explaining DZ
   multicast delivers leader shreds only, defining completeness as
   `DZ shreds seen ÷ total shreds from all sources`.

3. **Summary cards** — Row of cards:
   - Avg Completeness (%)
   - DZ Win Rate (%)
   - DZ Lead Time p50 (ms)
   - DZ Lead Time p95 (ms)
   - Slots Observed

4. **Win Rate by Node** — Horizontal bar chart (Recharts `BarChart`). One bar
   per node, length = DZ win rate %. Labeled with location code and percentage.

5. **Node Map** — MapLibre map with markers at each node. Markers display DZ
   win rate % and completeness %. Coordinates from API response (sourced from
   `dz_metros_current`).

6. **Node Detail table** — Columns: Node | Completeness | DZ Win % |
   DZ Lead p50 | DZ Lead p95 | Jito Win % | Turbine Win % | Pipe Win % |
   Slots | Last Updated. Sortable.

### Data fetching

- `useQuery` with key `['edge-scoreboard', window]`
- `refetchInterval: 30_000` (30s auto-refresh)
- `staleTime: 15_000`
- Window stored in URL search params (`?window=24h`)
- API function in `web/src/lib/api.ts`

### Loading states

- Initial: skeleton components matching layout
- Refresh: stale data stays visible while refetching (React Query default)
- Error: simple message with retry

## Node location resolution

Node IDs follow `<loc>-<env>-bm#`. Location code = first segment, uppercased
for display. The API JOINs `dz_metros_current` on `code = lower(location)` to
resolve metro name, latitude, longitude. Unknown locations still appear in the
table and chart — just omitted from the map.

## Future extensibility

- **Feed type selector:** `feed_type` param ready for new feed types beyond `shred`
- **Pairwise win rates:** Placeholder for when per-competitor data is available
- **New nodes:** Auto-appear when their `node_id` shows up in `slot_feed_races`
- **New feeds:** Add feed subscription + data flows into existing schema
