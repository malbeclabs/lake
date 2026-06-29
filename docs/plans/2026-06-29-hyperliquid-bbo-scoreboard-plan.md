# Hyperliquid BBO Scoreboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a lean "Hyperliquid ▸ BBO Scoreboard" page (sibling to Shreds) that proves DoubleZero (`tob_*` feeds) delivers Hyperliquid best-bid/offer updates faster than competing providers, with a per-vantage breakdown and a live recent-races strip.

**Architecture:** Backend is a near-clone of the shredder edge-scoreboard. We proxy the remote, pre-aggregated `feeds.hyperliquid_bbo_feed_race_summary` table into lake via `remoteSecure()`, compute win-share / per-competitor / per-node metrics from it (all exact, since every row is one race), serve them through a cached `GET /api/dz/hyperliquid/scoreboard` endpoint, and render a React page. No materialized view is built in lake (the remote owns it); the raw observations table is **not** used in v1.

**Tech Stack:** Go (chi router, ClickHouse-go), ClickHouse, Temporal page-cache worker, React + Vite + TypeScript + Tailwind v4.

## Global Constraints

- User-facing product name is **"DoubleZero Data"**, not "Lake".
- DoubleZero rollup = any feed where `startsWith(feed, 'tob_')`. Competitors (raw feed → label): `hyperliquid_public_bbo`→`Public API`, `hydromancer_bbo`→`Hydromancer`, `hyperpc_shared_bbo`→`HyperPC`, `quicknode_l2book_bbo`→`QuickNode`.
- Source table: `{FeedsDB}.hyperliquid_bbo_feed_race_summary`, default db name `feeds`, overridable via `CLICKHOUSE_FEEDS_DB`. Always query with `FINAL` (ReplacingMergeTree).
- Race key columns: `(capture_run_id, measurement_node_id, symbol, source_ts_ms, bbo_hash)`.
- Window values: `1h`, `24h`, `7d`; default `24h`. Default (cacheable) request shape = no `symbol`, no `since_ts`, window omitted or `24h`.
- Win-share / per-competitor counts use the production Grafana methodology (per-pairwise-comparison; `events_won` is always `1`, so `countIf` == `sumIf(events_won, …)`). Lead percentiles use `quantileExact` over `lead_time_p50_ms` (one sample per row = exact distribution).
- Go commit style: `component: short description`, lowercase, **no Co-Authored-By footer**. Commits are SSH-signed (1Password agent is forwarded; approve the prompt).
- TypeScript strict mode: `cd web && bunx tsc -b` must pass.
- ClickHouse handler tests run against the local ClickHouse via the `apitesting` harness and require a running local ClickHouse (the k3d dev env). They are skipped/fail without it — run them in the dev environment.

---

### Task 1: Wire the `feeds` database config + API field + test harness

**Files:**
- Modify: `api/config/config.go` (after the `dzdpDB` block ~line 36, the getters ~line 84, and the env reads ~line 155)
- Modify: `api/handlers/api.go:43-45` (API struct DB fields)
- Modify: `api/main.go:362-364` (API init)
- Modify: `api/testing/api.go` (NewTestAPIBare ~26, NewTestAPI ~46, NewTestAPIAll ~82)
- Test: `api/config/config_test.go` (create)

**Interfaces:**
- Produces: `config.GetFeedsDB() string`, `config.SetFeedsDB(string)`, package var `feedsDB` (default `"feeds"`); `handlers.API.FeedsDB string` field; test APIs set `FeedsDB` to the per-test db name.

- [ ] **Step 1: Write the failing test**

Create `api/config/config_test.go`:
```go
package config

import "testing"

func TestFeedsDBDefaultAndSetter(t *testing.T) {
	SetFeedsDB("feeds")
	if got := GetFeedsDB(); got != "feeds" {
		t.Fatalf("default GetFeedsDB() = %q, want %q", got, "feeds")
	}
	SetFeedsDB("feeds_qa")
	if got := GetFeedsDB(); got != "feeds_qa" {
		t.Fatalf("after SetFeedsDB, GetFeedsDB() = %q, want %q", got, "feeds_qa")
	}
	SetFeedsDB("feeds") // restore default for other tests
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./api/config/ -run TestFeedsDBDefaultAndSetter -v`
Expected: FAIL — `undefined: SetFeedsDB` / `undefined: GetFeedsDB`.

- [ ] **Step 3: Add the config var, getters, and env override**

In `api/config/config.go`, after the `dzdpDB` declaration (line 36):
```go
// feedsDB is the ClickHouse database name for the Hyperliquid feeds tables (default: "feeds").
var feedsDB = "feeds"
```
After `SetDZDPDB` (line 84):
```go
// GetFeedsDB returns the feeds database name.
func GetFeedsDB() string {
	return feedsDB
}

// SetFeedsDB sets the feeds database name.
func SetFeedsDB(db string) {
	feedsDB = db
}
```
In `Load()`, after the `CLICKHOUSE_DZDP_DB` block (line 155):
```go
	if db := os.Getenv("CLICKHOUSE_FEEDS_DB"); db != "" {
		feedsDB = db
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./api/config/ -run TestFeedsDBDefaultAndSetter -v`
Expected: PASS.

- [ ] **Step 5: Add the `FeedsDB` field and wire it everywhere**

In `api/handlers/api.go`, in the `API` struct next to the other DB names (after line 45 `DZDPDB string`):
```go
	FeedsDB       string
```
In `api/main.go`, in the `&handlers.API{...}` literal after line 364 `DZDPDB: config.GetDZDPDB(),`:
```go
		FeedsDB:       config.GetFeedsDB(),
```
In `api/testing/api.go`, add `FeedsDB: dbName,` after the `ShredderDB: dbName,` line in **both** `NewTestAPIBare` (line 24) and `NewTestAPI` (line 44); and in `NewTestAPIAll` add `api.FeedsDB = dbName` after line 81 `api.ShredderDB = dbName`.

- [ ] **Step 6: Build to verify wiring**

Run: `go build ./api/...`
Expected: builds cleanly.

- [ ] **Step 7: Commit**

```bash
git add api/config/config.go api/config/config_test.go api/handlers/api.go api/main.go api/testing/api.go
git commit -m "api: add feeds database config and FeedsDB wiring"
```

---

### Task 2: Register the remote proxy table

**Files:**
- Modify: `admin/remotetables/setup.go` (the `externalRemoteTables` slice, ~lines 16-39)
- Test: `admin/remotetables/setup_test.go` (create if absent, else add a test)

**Interfaces:**
- Produces: `externalRemoteTables` includes `{"feeds", "hyperliquid_bbo_feed_race_summary"}`, so `Setup()` creates a local `remoteSecure()` proxy `feeds.hyperliquid_bbo_feed_race_summary`.

**Note (ops prerequisite, not code):** lake's existing remote reader (`REMOTE_CH_USER`) must have `SELECT` on `feeds.*` on the ClickHouse Cloud instance. Without it the proxy is created but queries fail; the handler degrades gracefully (Task 6). Record this as a deploy dependency.

- [ ] **Step 1: Write the failing test**

Create `admin/remotetables/setup_test.go` (adjust the package name to match `setup.go`'s package if it differs):
```go
package remotetables

import "testing"

func TestExternalRemoteTablesIncludesFeeds(t *testing.T) {
	found := false
	for _, e := range externalRemoteTables {
		if e.database == "feeds" && e.table == "hyperliquid_bbo_feed_race_summary" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("externalRemoteTables missing feeds.hyperliquid_bbo_feed_race_summary")
	}
}
```

- [ ] **Step 2: Run test to verify it fails (and confirm field names)**

Run: `go test ./admin/remotetables/ -run TestExternalRemoteTablesIncludesFeeds -v`
Expected: FAIL. If it fails to **compile** on `e.database`/`e.table`, open `admin/remotetables/setup.go`, read the struct field names used by `externalRemoteTables` entries, and update the test to match (the entries were quoted in exploration as `{"shredder", "slot_feed_race_summary_v2"}` etc.).

- [ ] **Step 3: Add the entry**

In `admin/remotetables/setup.go`, in the `externalRemoteTables` slice, add alongside the `shredder` entries:
```go
	{"feeds", "hyperliquid_bbo_feed_race_summary"},
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./admin/remotetables/ -run TestExternalRemoteTablesIncludesFeeds -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add admin/remotetables/setup.go admin/remotetables/setup_test.go
git commit -m "admin: proxy feeds.hyperliquid_bbo_feed_race_summary remote table"
```

---

### Task 3: Response types, seed helper, headline + per-competitor query

**Files:**
- Create: `api/handlers/hyperliquid_scoreboard.go`
- Create: `api/handlers/hyperliquid_scoreboard_test.go`

**Interfaces:**
- Produces:
  - Types `HyperliquidCompetitor`, `HyperliquidNode`, `HyperliquidRace`, `HyperliquidScoreboardResponse` (JSON shapes below).
  - `var hyperliquidCompetitors = []struct{ Feed, Label string }{…}` (ordered).
  - `var hyperliquidWindows = map[string]string{"1h":"1 HOUR","24h":"24 HOUR","7d":"7 DAY"}`.
  - `func (a *API) FetchHyperliquidScoreboardData(ctx context.Context, window, symbol string) (*HyperliquidScoreboardResponse, error)` — in this task it fills `DZWinSharePct`, `TotalRaces`, `Competitors`; `Nodes`/`RecentRaces` come in Tasks 4–5.
  - `func sanitizeHyperliquidSymbol(s string) string` — returns the symbol if it matches `^[A-Za-z0-9:_.-]{1,32}$`, else `""`.
- Consumes: `a.FeedsDB`, `a.envDB(ctx)` (from Task 1 / existing).

- [ ] **Step 1: Write the failing test**

Create `api/handlers/hyperliquid_scoreboard_test.go`:
```go
package handlers_test

import (
	"fmt"
	"testing"

	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createFeedsTable creates the hyperliquid_bbo_feed_race_summary table in the feeds DB.
func createFeedsTable(t *testing.T, api *handlers.API) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db)))
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.hyperliquid_bbo_feed_race_summary (
			event_ts DateTime64(9),
			ingested_at DateTime64(9) DEFAULT now64(9),
			capture_run_id String,
			measurement_node_id String,
			host String,
			location_code LowCardinality(String),
			feed_type LowCardinality(String) DEFAULT 'bbo',
			symbol LowCardinality(String),
			source_ts_ms UInt64,
			bbo_hash UInt64,
			feed LowCardinality(String),
			loser_feed LowCardinality(String) DEFAULT '',
			total_events UInt64,
			events_won UInt64,
			lead_time_p50_ms Float64 DEFAULT 0,
			lead_time_p95_ms Float64 DEFAULT 0,
			send_lead_time_p50_ms Nullable(Float64) DEFAULT NULL,
			send_lead_time_p95_ms Nullable(Float64) DEFAULT NULL
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toDate(event_ts)
		ORDER BY (measurement_node_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed)
	`, db)))
}

// pairwiseRow inserts one pairwise race row (winner feed beat loser_feed by leadMs).
func insertPairwise(t *testing.T, api *handlers.API, node, loc, symbol string, srcTs, hash uint64, winner, loser string, leadMs float64) {
	t.Helper()
	ctx := t.Context()
	db := "`" + api.FeedsDB + "`"
	require.NoError(t, api.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.hyperliquid_bbo_feed_race_summary
		(event_ts, capture_run_id, measurement_node_id, host, location_code, symbol, source_ts_ms, bbo_hash, feed, loser_feed, total_events, events_won, lead_time_p50_ms, lead_time_p95_ms)
		VALUES (now64(9), 'run1', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', 1, 1, %f, %f)
	`, db, node, node, loc, symbol, srcTs, hash, winner, loser, leadMs, leadMs)))
}

func TestHyperliquidScoreboard_HeadlineAndCompetitors(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api)

	// 3 races at node tyo: DZ (tob_*) beats Hydromancer twice, loses once.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 1000, 1, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 1.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 2000, 2, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 3.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 3000, 3, "hydromancer_bbo", "tob_gcp_tyo_hl_mainnet1", 0.5)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)

	// DZ won 2 of 3 comparable races = 66.67%.
	assert.InDelta(t, 66.67, resp.DZWinSharePct, 0.1)
	assert.EqualValues(t, 3, resp.TotalRaces)

	var hydro *handlers.HyperliquidCompetitor
	for i := range resp.Competitors {
		if resp.Competitors[i].Feed == "hydromancer_bbo" {
			hydro = &resp.Competitors[i]
		}
	}
	require.NotNil(t, hydro)
	assert.Equal(t, "Hydromancer", hydro.Label)
	assert.InDelta(t, 66.67, hydro.DZWinPct, 0.1)
	assert.EqualValues(t, 3, hydro.Races)
	// Lead p50 over the 2 DZ wins (1.0, 3.0) = 1.0 (quantileExact lower).
	assert.InDelta(t, 1.0, hydro.LeadP50Ms, 0.001)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./api/handlers/ -run TestHyperliquidScoreboard_HeadlineAndCompetitors -v`
Expected: FAIL — `undefined: api.FetchHyperliquidScoreboardData` / `undefined: handlers.HyperliquidCompetitor`.

- [ ] **Step 3: Create the handler file with types + the fetch**

Create `api/handlers/hyperliquid_scoreboard.go`:
```go
package handlers

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// hyperliquidCompetitors lists the non-DoubleZero feeds shown on the scoreboard,
// in display order, mapping each raw feed name to its label.
var hyperliquidCompetitors = []struct{ Feed, Label string }{
	{"hyperliquid_public_bbo", "Public API"},
	{"hydromancer_bbo", "Hydromancer"},
	{"hyperpc_shared_bbo", "HyperPC"},
	{"quicknode_l2book_bbo", "QuickNode"},
}

// hyperliquidWindows maps window params to ClickHouse interval expressions.
var hyperliquidWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
}

var hyperliquidSymbolRe = regexp.MustCompile(`^[A-Za-z0-9:_.-]{1,32}$`)

// sanitizeHyperliquidSymbol returns the symbol if safe to inline, else "".
func sanitizeHyperliquidSymbol(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "all") || !hyperliquidSymbolRe.MatchString(s) {
		return ""
	}
	return s
}

// HyperliquidCompetitor is DoubleZero's head-to-head record vs one competitor feed.
type HyperliquidCompetitor struct {
	Feed      string  `json:"feed"`
	Label     string  `json:"label"`
	DZWinPct  float64 `json:"dz_win_pct"`
	LeadP50Ms float64 `json:"lead_p50_ms"`
	LeadP95Ms float64 `json:"lead_p95_ms"`
	Races     uint64  `json:"races"`
}

// HyperliquidNode is the per-vantage breakdown.
type HyperliquidNode struct {
	MeasurementNodeID string                  `json:"measurement_node_id"`
	LocationCode      string                  `json:"location_code"`
	DZWinSharePct     float64                 `json:"dz_win_share_pct"`
	TotalRaces        uint64                  `json:"total_races"`
	Competitors       []HyperliquidCompetitor `json:"competitors"`
}

// HyperliquidRace is one recent race for the live strip.
type HyperliquidRace struct {
	EventTs       time.Time `json:"event_ts"`
	Symbol        string    `json:"symbol"`
	LocationCode  string    `json:"location_code"`
	WinnerFeed    string    `json:"winner_feed"`
	IsDZ          bool      `json:"is_dz"`
	RunnerUpFeed  string    `json:"runner_up_feed"`
	RunnerUpLabel string    `json:"runner_up_label"`
	LeadMs        float64   `json:"lead_ms"`
}

// HyperliquidScoreboardResponse is the API response.
type HyperliquidScoreboardResponse struct {
	Window        string                  `json:"window"`
	Symbol        string                  `json:"symbol,omitempty"`
	GeneratedAt   time.Time               `json:"generated_at"`
	FeedType      string                  `json:"feed_type"`
	DZWinSharePct float64                 `json:"dz_win_share_pct"`
	TotalRaces    uint64                  `json:"total_races"`
	Competitors   []HyperliquidCompetitor `json:"competitors"`
	Nodes         []HyperliquidNode       `json:"nodes"`
	RecentRaces   []HyperliquidRace       `json:"recent_races"`
}

// labelForFeed maps a raw competitor feed to its display label (falls back to the raw name).
func labelForFeed(feed string) string {
	for _, c := range hyperliquidCompetitors {
		if c.Feed == feed {
			return c.Label
		}
	}
	return feed
}

// FetchHyperliquidScoreboardData computes the aggregated scoreboard for a window and
// optional symbol. Empty symbol means all symbols.
func (a *API) FetchHyperliquidScoreboardData(ctx context.Context, window, symbol string) (*HyperliquidScoreboardResponse, error) {
	interval, ok := hyperliquidWindows[window]
	if !ok {
		window = "24h"
		interval = hyperliquidWindows[window]
	}
	symbol = sanitizeHyperliquidSymbol(symbol)
	symbolFilter := ""
	if symbol != "" {
		symbolFilter = fmt.Sprintf("AND symbol = '%s'", symbol)
	}
	db := fmt.Sprintf("`%s`", a.FeedsDB)

	resp := &HyperliquidScoreboardResponse{
		Window:      window,
		Symbol:      symbol,
		GeneratedAt: time.Now().UTC(),
		FeedType:    "bbo",
		Competitors: []HyperliquidCompetitor{},
		Nodes:       []HyperliquidNode{},
		RecentRaces: []HyperliquidRace{},
	}

	// Headline: DZ win share over all DZ-vs-competitor pairwise comparisons.
	headlineQ := fmt.Sprintf(`
		SELECT
			100.0 * countIf(startsWith(feed,'tob_') AND NOT startsWith(loser_feed,'tob_'))
			      / nullIf(countIf(startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')), 0) AS dz_win_share_pct,
			countIf(startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) AS total_races
		FROM %s.hyperliquid_bbo_feed_race_summary FINAL
		WHERE feed != loser_feed AND loser_feed != '' %s
		  AND event_ts >= now() - INTERVAL %s`, db, symbolFilter, interval)
	var winShare float64
	var totalRaces uint64
	if err := a.envDB(ctx).QueryRow(ctx, headlineQ).Scan(&winShare, &totalRaces); err != nil {
		return nil, err
	}
	resp.DZWinSharePct = winShare
	resp.TotalRaces = totalRaces

	// Per-competitor: win%, lead p50/p95 (over DZ wins), and total comparable races.
	compQ := fmt.Sprintf(`
		SELECT competitor,
			countIf(dz_won = 1) AS dz_wins,
			countIf(dz_won = 0) AS dz_losses,
			quantileExactIf(0.5)(lead_ms, dz_won = 1) AS lead_p50,
			quantileExactIf(0.95)(lead_ms, dz_won = 1) AS lead_p95
		FROM (
			SELECT
				if(startsWith(feed,'tob_'), loser_feed, feed) AS competitor,
				if(startsWith(feed,'tob_'), 1, 0) AS dz_won,
				lead_time_p50_ms AS lead_ms
			FROM %s.hyperliquid_bbo_feed_race_summary FINAL
			WHERE feed != loser_feed AND loser_feed != ''
			  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %s
			  AND event_ts >= now() - INTERVAL %s
		)
		GROUP BY competitor`, db, symbolFilter, interval)
	rows, err := a.envDB(ctx).Query(ctx, compQ)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type stat struct {
		wins, losses        uint64
		leadP50, leadP95    float64
	}
	byFeed := map[string]stat{}
	for rows.Next() {
		var competitor string
		var s stat
		if err := rows.Scan(&competitor, &s.wins, &s.losses, &s.leadP50, &s.leadP95); err != nil {
			return nil, err
		}
		byFeed[competitor] = s
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Emit competitors in configured order.
	for _, c := range hyperliquidCompetitors {
		s, ok := byFeed[c.Feed]
		if !ok {
			continue
		}
		races := s.wins + s.losses
		var winPct float64
		if races > 0 {
			winPct = 100.0 * float64(s.wins) / float64(races)
		}
		resp.Competitors = append(resp.Competitors, HyperliquidCompetitor{
			Feed:      c.Feed,
			Label:     c.Label,
			DZWinPct:  winPct,
			LeadP50Ms: s.leadP50,
			LeadP95Ms: s.leadP95,
			Races:     races,
		})
	}

	return resp, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./api/handlers/ -run TestHyperliquidScoreboard_HeadlineAndCompetitors -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/handlers/hyperliquid_scoreboard.go api/handlers/hyperliquid_scoreboard_test.go
git commit -m "api: hyperliquid scoreboard headline and per-competitor query"
```

---

### Task 4: Per-node (per-vantage) breakdown

**Files:**
- Modify: `api/handlers/hyperliquid_scoreboard.go` (add a query to `FetchHyperliquidScoreboardData`, populating `resp.Nodes`)
- Modify: `api/handlers/hyperliquid_scoreboard_test.go` (add a test)

**Interfaces:**
- Consumes: `FetchHyperliquidScoreboardData` from Task 3.
- Produces: `resp.Nodes []HyperliquidNode`, one per `measurement_node_id`, each with its own `DZWinSharePct`, `TotalRaces`, and ordered `Competitors`.

- [ ] **Step 1: Write the failing test**

Add to `api/handlers/hyperliquid_scoreboard_test.go`:
```go
func TestHyperliquidScoreboard_PerNode(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api)

	// tyo: DZ wins both vs QuickNode. nyc: DZ wins 1, loses 1 vs QuickNode.
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 10, 1, "tob_gcp_tyo_hl_mainnet1", "quicknode_l2book_bbo", 2.0)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 20, 2, "tob_gcp_tyo_hl_mainnet1", "quicknode_l2book_bbo", 2.0)
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 30, 3, "tob_aws_galaxy1", "quicknode_l2book_bbo", 1.0)
	insertPairwise(t, api, "nyc-rec1", "nyc", "ETH", 40, 4, "quicknode_l2book_bbo", "tob_aws_galaxy1", 1.0)

	resp, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.Len(t, resp.Nodes, 2)

	byNode := map[string]handlers.HyperliquidNode{}
	for _, n := range resp.Nodes {
		byNode[n.MeasurementNodeID] = n
	}
	assert.InDelta(t, 100.0, byNode["tyo-rec1"].DZWinSharePct, 0.1)
	assert.InDelta(t, 50.0, byNode["nyc-rec1"].DZWinSharePct, 0.1)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./api/handlers/ -run TestHyperliquidScoreboard_PerNode -v`
Expected: FAIL — `resp.Nodes` is empty (length 0, not 2).

- [ ] **Step 3: Add the per-node query**

In `FetchHyperliquidScoreboardData`, before `return resp, nil`, add:
```go
	// Per-node: same comparison, grouped by measurement node + competitor.
	nodeQ := fmt.Sprintf(`
		SELECT measurement_node_id, location_code, competitor,
			countIf(dz_won = 1) AS dz_wins,
			countIf(dz_won = 0) AS dz_losses,
			quantileExactIf(0.5)(lead_ms, dz_won = 1) AS lead_p50,
			quantileExactIf(0.95)(lead_ms, dz_won = 1) AS lead_p95
		FROM (
			SELECT measurement_node_id, location_code,
				if(startsWith(feed,'tob_'), loser_feed, feed) AS competitor,
				if(startsWith(feed,'tob_'), 1, 0) AS dz_won,
				lead_time_p50_ms AS lead_ms
			FROM %s.hyperliquid_bbo_feed_race_summary FINAL
			WHERE feed != loser_feed AND loser_feed != ''
			  AND (startsWith(feed,'tob_') != startsWith(loser_feed,'tob_')) %s
			  AND event_ts >= now() - INTERVAL %s
		)
		GROUP BY measurement_node_id, location_code, competitor`, db, symbolFilter, interval)
	nrows, err := a.envDB(ctx).Query(ctx, nodeQ)
	if err != nil {
		return nil, err
	}
	defer nrows.Close()

	type nodeAgg struct {
		loc   string
		byFeed map[string]stat
	}
	nodeMap := map[string]*nodeAgg{}
	var nodeOrder []string
	for nrows.Next() {
		var node, loc, competitor string
		var s stat
		if err := nrows.Scan(&node, &loc, &competitor, &s.wins, &s.losses, &s.leadP50, &s.leadP95); err != nil {
			return nil, err
		}
		na, ok := nodeMap[node]
		if !ok {
			na = &nodeAgg{loc: loc, byFeed: map[string]stat{}}
			nodeMap[node] = na
			nodeOrder = append(nodeOrder, node)
		}
		na.byFeed[competitor] = s
	}
	if err := nrows.Err(); err != nil {
		return nil, err
	}

	for _, node := range nodeOrder {
		na := nodeMap[node]
		n := HyperliquidNode{
			MeasurementNodeID: node,
			LocationCode:      na.loc,
			Competitors:       []HyperliquidCompetitor{},
		}
		var wins, races uint64
		for _, c := range hyperliquidCompetitors {
			s, ok := na.byFeed[c.Feed]
			if !ok {
				continue
			}
			r := s.wins + s.losses
			var winPct float64
			if r > 0 {
				winPct = 100.0 * float64(s.wins) / float64(r)
			}
			n.Competitors = append(n.Competitors, HyperliquidCompetitor{
				Feed: c.Feed, Label: c.Label, DZWinPct: winPct,
				LeadP50Ms: s.leadP50, LeadP95Ms: s.leadP95, Races: r,
			})
			wins += s.wins
			races += r
		}
		n.TotalRaces = races
		if races > 0 {
			n.DZWinSharePct = 100.0 * float64(wins) / float64(races)
		}
		resp.Nodes = append(resp.Nodes, n)
	}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./api/handlers/ -run 'TestHyperliquidScoreboard_(HeadlineAndCompetitors|PerNode)' -v`
Expected: PASS (both).

- [ ] **Step 5: Commit**

```bash
git add api/handlers/hyperliquid_scoreboard.go api/handlers/hyperliquid_scoreboard_test.go
git commit -m "api: hyperliquid scoreboard per-node breakdown"
```

---

### Task 5: Recent races (live strip) + latest fetch

**Files:**
- Modify: `api/handlers/hyperliquid_scoreboard.go`
- Modify: `api/handlers/hyperliquid_scoreboard_test.go`

**Interfaces:**
- Produces:
  - `func (a *API) fetchHyperliquidRecentRaces(ctx context.Context, symbol string, sinceTs time.Time, limit int) ([]HyperliquidRace, error)` — newest races first; `sinceTs` zero means "last 2 minutes".
  - `func (a *API) FetchHyperliquidScoreboardLatest(ctx context.Context, limit int) (*HyperliquidScoreboardResponse, error)` — a response with only `RecentRaces` populated (for the fast cache).
  - `FetchHyperliquidScoreboardData` now populates `resp.RecentRaces` (last 2 min, limit 50).

**Performance note:** the recent-races query is bounded to a short trailing window and a row limit because the summary table is large; it is refreshed on the fast cache cadence (Task 7), not per request. If it proves heavy without a symbol filter, narrow the trailing window further.

- [ ] **Step 1: Write the failing test**

Add to `api/handlers/hyperliquid_scoreboard_test.go`:
```go
func TestHyperliquidScoreboard_RecentRaces(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api)

	// A DZ-won race (BTC) and a competitor-won race (ETH), both pairwise.
	insertPairwise(t, api, "tyo-rec1", "tyo", "BTC", 100, 1, "tob_gcp_tyo_hl_mainnet1", "hydromancer_bbo", 1.5)
	insertPairwise(t, api, "tyo-rec1", "tyo", "ETH", 200, 2, "quicknode_l2book_bbo", "tob_gcp_tyo_hl_mainnet1", 0.7)

	races, err := api.FetchHyperliquidScoreboardData(t.Context(), "24h", "")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(races.RecentRaces), 2)

	bySym := map[string]handlers.HyperliquidRace{}
	for _, r := range races.RecentRaces {
		bySym[r.Symbol] = r
	}
	assert.True(t, bySym["BTC"].IsDZ)
	assert.Equal(t, "Hydromancer", bySym["BTC"].RunnerUpLabel)
	assert.InDelta(t, 1.5, bySym["BTC"].LeadMs, 0.001)
	assert.False(t, bySym["ETH"].IsDZ)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./api/handlers/ -run TestHyperliquidScoreboard_RecentRaces -v`
Expected: FAIL — `RecentRaces` empty.

- [ ] **Step 3: Add the recent-races fetch and wire it in**

Add to `api/handlers/hyperliquid_scoreboard.go`:
```go
// fetchHyperliquidRecentRaces returns the most recent races (one row per race,
// winner + closest competitor + lead). sinceTs zero -> last 2 minutes.
func (a *API) fetchHyperliquidRecentRaces(ctx context.Context, symbol string, sinceTs time.Time, limit int) ([]HyperliquidRace, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	symbol = sanitizeHyperliquidSymbol(symbol)
	symbolFilter := ""
	if symbol != "" {
		symbolFilter = fmt.Sprintf("AND symbol = '%s'", symbol)
	}
	timeFilter := "AND event_ts >= now() - INTERVAL 2 MINUTE"
	if !sinceTs.IsZero() {
		timeFilter = fmt.Sprintf("AND event_ts > toDateTime64(%d, 9)", sinceTs.Unix())
	}
	db := fmt.Sprintf("`%s`", a.FeedsDB)
	q := fmt.Sprintf(`
		SELECT
			max(event_ts) AS event_ts,
			symbol,
			location_code,
			feed AS winner_feed,
			startsWith(feed,'tob_') AS is_dz,
			argMin(loser_feed, lead_time_p50_ms) AS runner_up_feed,
			min(lead_time_p50_ms) AS lead_ms
		FROM %s.hyperliquid_bbo_feed_race_summary FINAL
		WHERE loser_feed != '' AND feed != loser_feed %s %s
		GROUP BY capture_run_id, measurement_node_id, symbol, source_ts_ms, bbo_hash, location_code, feed
		ORDER BY event_ts DESC
		LIMIT %d`, db, symbolFilter, timeFilter, limit)
	rows, err := a.envDB(ctx).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []HyperliquidRace{}
	for rows.Next() {
		var r HyperliquidRace
		var isDZ uint8
		if err := rows.Scan(&r.EventTs, &r.Symbol, &r.LocationCode, &r.WinnerFeed, &isDZ, &r.RunnerUpFeed, &r.LeadMs); err != nil {
			return nil, err
		}
		r.IsDZ = isDZ == 1
		r.RunnerUpLabel = labelForFeed(r.RunnerUpFeed)
		out = append(out, r)
	}
	return out, rows.Err()
}

// FetchHyperliquidScoreboardLatest returns only the recent-races strip (fast cache).
func (a *API) FetchHyperliquidScoreboardLatest(ctx context.Context, limit int) (*HyperliquidScoreboardResponse, error) {
	races, err := a.fetchHyperliquidRecentRaces(ctx, "", time.Time{}, limit)
	if err != nil {
		return nil, err
	}
	return &HyperliquidScoreboardResponse{
		GeneratedAt: time.Now().UTC(),
		FeedType:    "bbo",
		RecentRaces: races,
		Competitors: []HyperliquidCompetitor{},
		Nodes:       []HyperliquidNode{},
	}, nil
}
```
In `FetchHyperliquidScoreboardData`, before `return resp, nil`:
```go
	recent, err := a.fetchHyperliquidRecentRaces(ctx, symbol, time.Time{}, 50)
	if err != nil {
		return nil, err
	}
	resp.RecentRaces = recent
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./api/handlers/ -run 'TestHyperliquidScoreboard' -v`
Expected: PASS (all three).

- [ ] **Step 5: Commit**

```bash
git add api/handlers/hyperliquid_scoreboard.go api/handlers/hyperliquid_scoreboard_test.go
git commit -m "api: hyperliquid scoreboard recent races strip"
```

---

### Task 6: HTTP handler, caching, graceful degradation, route

**Files:**
- Modify: `api/handlers/hyperliquid_scoreboard.go` (add `GetHyperliquidScoreboard`, cache keys, table-exists check)
- Modify: `api/main.go` (register route, after line 604)
- Modify: `api/handlers/hyperliquid_scoreboard_test.go` (handler tests)

**Interfaces:**
- Consumes: `a.readPageCache`, `isMainnet`, `writeJSON`, `logError` (existing); `FetchHyperliquidScoreboardData`, `fetchHyperliquidRecentRaces` (Tasks 3–5).
- Produces: `func (a *API) GetHyperliquidScoreboard(w http.ResponseWriter, r *http.Request)`; cache keys `hyperliquid_scoreboard` and `hyperliquid_scoreboard:latest`; `func (a *API) hyperliquidFeedsTableExists(ctx context.Context) bool`.

- [ ] **Step 1: Write the failing test**

Add to `api/handlers/hyperliquid_scoreboard_test.go`:
```go
func TestGetHyperliquidScoreboard_Empty(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	createFeedsTable(t, api) // empty table -> empty-but-valid response

	req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetHyperliquidScoreboard(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "24h", resp.Window)
	assert.Equal(t, "MISS", rr.Header().Get("X-Cache"))
}

func TestGetHyperliquidScoreboard_MissingTable(t *testing.T) {
	api := apitesting.NewTestAPIBare(t, testChDB)
	// Do NOT create the table -> handler must degrade to empty 200, not 500.

	req := httptest.NewRequest(http.MethodGet, "/api/dz/hyperliquid/scoreboard", nil)
	rr := httptest.NewRecorder()
	api.GetHyperliquidScoreboard(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp handlers.HyperliquidScoreboardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Competitors)
}
```
Add imports `encoding/json`, `net/http`, `net/http/httptest` to the test file if not already present.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./api/handlers/ -run 'TestGetHyperliquidScoreboard' -v`
Expected: FAIL — `undefined: api.GetHyperliquidScoreboard`.

- [ ] **Step 3: Add the handler, cache keys, and table check**

Add to `api/handlers/hyperliquid_scoreboard.go` (add `"context"`, `"encoding/json"`, `"net/http"`, `"strings"` to imports as needed):
```go
// hyperliquidScoreboardCacheKey returns the cache key for the default request shape, or "".
func hyperliquidScoreboardCacheKey(r *http.Request) string {
	if r.URL.Query().Get("since_ts") != "" || r.URL.Query().Get("symbol") != "" {
		return ""
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if window != "" && window != "24h" {
		return ""
	}
	return "hyperliquid_scoreboard"
}

// hyperliquidFeedsTableExists reports whether the proxied summary table is queryable.
func (a *API) hyperliquidFeedsTableExists(ctx context.Context) bool {
	var n uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.hyperliquid_bbo_feed_race_summary", a.FeedsDB)
	if err := a.envDB(ctx).QueryRow(ctx, q).Scan(&n); err != nil {
		return false
	}
	return n == 1
}

// GetHyperliquidScoreboard serves the Hyperliquid BBO scoreboard.
func (a *API) GetHyperliquidScoreboard(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) {
		if key := hyperliquidScoreboardCacheKey(r); key != "" {
			if data, err := a.readPageCache(r.Context(), key); err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				_, _ = w.Write(data)
				return
			}
		}
	}
	w.Header().Set("X-Cache", "MISS")

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	window := strings.TrimSpace(r.URL.Query().Get("window"))
	if _, ok := hyperliquidWindows[window]; !ok {
		window = "24h"
	}
	symbol := r.URL.Query().Get("symbol")

	// Degrade gracefully if the proxy table isn't available (e.g. local dev).
	if !a.hyperliquidFeedsTableExists(ctx) {
		writeJSON(w, &HyperliquidScoreboardResponse{
			Window: window, GeneratedAt: time.Now().UTC(), FeedType: "bbo",
			Competitors: []HyperliquidCompetitor{}, Nodes: []HyperliquidNode{}, RecentRaces: []HyperliquidRace{},
		})
		return
	}

	resp, err := a.FetchHyperliquidScoreboardData(ctx, window, symbol)
	if err != nil {
		logError("HyperliquidScoreboard error", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, resp)
}
```
In `api/main.go`, after line 604 (`r.Get("/api/dz/edge/scoreboard", api.GetEdgeScoreboard)`):
```go
		r.Get("/api/dz/hyperliquid/scoreboard", api.GetHyperliquidScoreboard)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./api/handlers/ -run 'TestGetHyperliquidScoreboard|TestHyperliquidScoreboard' -v`
Expected: PASS. Then `go build ./api/...` (route compiles).

- [ ] **Step 5: Commit**

```bash
git add api/handlers/hyperliquid_scoreboard.go api/main.go api/handlers/hyperliquid_scoreboard_test.go
git commit -m "api: hyperliquid scoreboard HTTP handler, caching, and route"
```

---

### Task 7: Page-cache registration

**Files:**
- Modify: `api/worker/workflow.go` (the `entries()` slice ~line 113, and the `latestEntries` slice ~line 199)

**Interfaces:**
- Consumes: `api.FetchHyperliquidScoreboardData`, `api.FetchHyperliquidScoreboardLatest` (Tasks 3–6).
- Produces: cache entries keyed `hyperliquid_scoreboard` (30s) and `hyperliquid_scoreboard:latest` (fast).

**Note:** the handler already degrades when the table is missing; the refresher will surface a query error for the cache entry if the proxy/grant isn't in place, which is logged with backoff per the existing failure-count mechanism (`failures sync.Map`). No extra gating needed in v1.

- [ ] **Step 1: Add the standard cache entry**

In `api/worker/workflow.go`, in `entries()` immediately after the `edge scoreboard (leaders)` entry (line 113):
```go
		{"hyperliquid scoreboard", "hyperliquid_scoreboard", func(ctx context.Context) (any, error) {
			return api.FetchHyperliquidScoreboardData(ctx, "24h", "")
		}},
```

- [ ] **Step 2: Add the fast (latest) cache entry**

In `latestEntries` (after the `edge scoreboard (latest, leaders)` entry ~line 199):
```go
		{"hyperliquid scoreboard (latest)", "hyperliquid_scoreboard:latest", func(ctx context.Context) (any, error) {
			return api.FetchHyperliquidScoreboardLatest(ctx, 50)
		}},
```

- [ ] **Step 3: Build to verify**

Run: `go build ./api/...`
Expected: builds cleanly.

- [ ] **Step 4: Commit**

```bash
git add api/worker/workflow.go
git commit -m "api: register hyperliquid scoreboard page cache entries"
```

---

### Task 8: Web API client + types

**Files:**
- Modify: `web/src/lib/api.ts` (add near the edge-scoreboard client, ~line 6095)

**Interfaces:**
- Produces: TS interfaces `HyperliquidCompetitor`, `HyperliquidNode`, `HyperliquidRace`, `HyperliquidScoreboardResponse`; `fetchHyperliquidScoreboard(window, symbol?, opts?)`.

- [ ] **Step 1: Add types and fetch function**

Append to `web/src/lib/api.ts`:
```typescript
export interface HyperliquidCompetitor {
  feed: string
  label: string
  dz_win_pct: number
  lead_p50_ms: number
  lead_p95_ms: number
  races: number
}

export interface HyperliquidNode {
  measurement_node_id: string
  location_code: string
  dz_win_share_pct: number
  total_races: number
  competitors: HyperliquidCompetitor[]
}

export interface HyperliquidRace {
  event_ts: string
  symbol: string
  location_code: string
  winner_feed: string
  is_dz: boolean
  runner_up_feed: string
  runner_up_label: string
  lead_ms: number
}

export interface HyperliquidScoreboardResponse {
  window: string
  symbol?: string
  generated_at: string
  feed_type: string
  dz_win_share_pct: number
  total_races: number
  competitors: HyperliquidCompetitor[]
  nodes: HyperliquidNode[]
  recent_races: HyperliquidRace[]
}

export async function fetchHyperliquidScoreboard(
  window: string = '24h',
  symbol?: string,
): Promise<HyperliquidScoreboardResponse> {
  const params = new URLSearchParams()
  params.set('window', window)
  if (symbol && symbol !== 'all') params.set('symbol', symbol)
  const res = await apiFetch(`/api/dz/hyperliquid/scoreboard?${params}`)
  if (!res.ok) {
    throw new Error('Failed to fetch hyperliquid scoreboard')
  }
  return res.json()
}
```

- [ ] **Step 2: Type-check**

Run: `cd web && bunx tsc -b`
Expected: passes (no errors).

- [ ] **Step 3: Commit**

```bash
git add web/src/lib/api.ts
git commit -m "web: hyperliquid scoreboard api client and types"
```

---

### Task 9: Web page component

**Files:**
- Create: `web/src/components/hyperliquid-scoreboard-page.tsx`

**Interfaces:**
- Consumes: `fetchHyperliquidScoreboard`, `HyperliquidScoreboardResponse` (Task 8); existing `PageHeader` component.
- Produces: `export function HyperliquidScoreboardPage()`.

- [ ] **Step 1: Create the component**

Create `web/src/components/hyperliquid-scoreboard-page.tsx`:
```tsx
import { useEffect, useState, useCallback } from 'react'
import { PageHeader } from './page-header'
import {
  fetchHyperliquidScoreboard,
  type HyperliquidScoreboardResponse,
} from '@/lib/api'

const WINDOWS = ['1h', '24h', '7d']

function pct(n: number): string {
  return `${n.toFixed(1)}%`
}
function ms(n: number): string {
  return `+${n.toFixed(2)} ms`
}

function CompetitorTable({ competitors }: { competitors: HyperliquidScoreboardResponse['competitors'] }) {
  if (!competitors.length) {
    return <div className="text-sm text-muted-foreground">No competitor races in this window.</div>
  }
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-muted-foreground">
          <th className="py-1 pr-4 font-normal">vs Competitor</th>
          <th className="py-1 pr-4 font-normal">DZ win%</th>
          <th className="py-1 pr-4 font-normal">median lead</th>
          <th className="py-1 pr-4 font-normal">p95 lead</th>
          <th className="py-1 pr-4 font-normal">races</th>
        </tr>
      </thead>
      <tbody>
        {competitors.map((c) => (
          <tr key={c.feed} className="border-t border-border/50">
            <td className="py-1 pr-4">{c.label}</td>
            <td className="py-1 pr-4 tabular-nums">{pct(c.dz_win_pct)}</td>
            <td className="py-1 pr-4 tabular-nums">{ms(c.lead_p50_ms)}</td>
            <td className="py-1 pr-4 tabular-nums">{ms(c.lead_p95_ms)}</td>
            <td className="py-1 pr-4 tabular-nums">{c.races.toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export function HyperliquidScoreboardPage() {
  const [window, setWindow] = useState('24h')
  const [data, setData] = useState<HyperliquidScoreboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      setData(await fetchHyperliquidScoreboard(window))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [window])

  // Initial + window-change load, then poll every 15s for fresh recent races.
  useEffect(() => {
    load()
    const id = setInterval(load, 15000)
    return () => clearInterval(id)
  }, [load])

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader title="Hyperliquid · BBO Scoreboard" />

      <div className="flex items-center gap-2">
        <span className="text-sm text-muted-foreground">window:</span>
        {WINDOWS.map((w) => (
          <button
            key={w}
            onClick={() => setWindow(w)}
            className={`rounded px-2 py-1 text-sm ${w === window ? 'bg-primary text-primary-foreground' : 'bg-muted'}`}
          >
            {w}
          </button>
        ))}
      </div>

      {error && <div className="text-sm text-red-500">{error}</div>}
      {!data && !error && <div className="text-sm text-muted-foreground">Loading…</div>}

      {data && (
        <>
          <div className="rounded-lg border border-border p-4">
            <div className="text-2xl font-semibold tabular-nums">
              DoubleZero wins {pct(data.dz_win_share_pct)} of races
            </div>
            <div className="text-sm text-muted-foreground">
              all vantages · {data.window} · {data.total_races.toLocaleString()} comparisons
            </div>
          </div>

          <div className="rounded-lg border border-border p-4">
            <div className="mb-2 text-sm font-medium">DoubleZero vs competitors</div>
            <CompetitorTable competitors={data.competitors} />
          </div>

          <div className="rounded-lg border border-border p-4">
            <div className="mb-3 text-sm font-medium">By vantage</div>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
              {data.nodes.map((n) => (
                <div key={n.measurement_node_id} className="rounded border border-border/60 p-3">
                  <div className="mb-1 flex items-baseline justify-between">
                    <span className="font-medium uppercase">{n.location_code}</span>
                    <span className="tabular-nums">{pct(n.dz_win_share_pct)}</span>
                  </div>
                  <div className="mb-2 text-xs text-muted-foreground">
                    {n.measurement_node_id} · {n.total_races.toLocaleString()} races
                  </div>
                  <CompetitorTable competitors={n.competitors} />
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-lg border border-border p-4">
            <div className="mb-2 text-sm font-medium">Recent races (live)</div>
            <table className="w-full text-sm">
              <tbody>
                {data.recent_races.map((r, i) => (
                  <tr key={`${r.symbol}-${r.event_ts}-${i}`} className="border-t border-border/50">
                    <td className="py-1 pr-4 font-medium">{r.symbol}</td>
                    <td className="py-1 pr-4">
                      {r.is_dz ? (
                        <span className="text-emerald-500">DoubleZero ({r.winner_feed})</span>
                      ) : (
                        <span className="text-muted-foreground">{r.winner_feed}</span>
                      )}
                    </td>
                    <td className="py-1 pr-4 tabular-nums">
                      {ms(r.lead_ms)} vs {r.runner_up_label}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}
```
**Before writing:** confirm the `PageHeader` import path and props by opening `web/src/components/edge-scoreboard-page.tsx` (it imports `PageHeader` near line 16) — match its exact import path and prop names; adjust the `<PageHeader … />` usage if its API differs.

- [ ] **Step 2: Type-check**

Run: `cd web && bunx tsc -b`
Expected: passes. Fix any `PageHeader` prop/import mismatches surfaced here.

- [ ] **Step 3: Commit**

```bash
git add web/src/components/hyperliquid-scoreboard-page.tsx
git commit -m "web: hyperliquid scoreboard page component"
```

---

### Task 10: Routing + sidebar navigation

**Files:**
- Modify: `web/src/App.tsx` (import ~line 69; routes ~line 737)
- Modify: `web/src/components/sidebar.tsx` (route detection ~line 109-119; Edge section ~line 583-585)

**Interfaces:**
- Consumes: `HyperliquidScoreboardPage` (Task 9).
- Produces: routes `/dz/hyperliquid` (redirect) and `/dz/hyperliquid/scoreboard`; a "Hyperliquid" group in the Edge sidebar section.

- [ ] **Step 1: Add the import and routes**

In `web/src/App.tsx`, near line 69 (after the `EdgeScoreboardPage` import):
```tsx
import { HyperliquidScoreboardPage } from './components/hyperliquid-scoreboard-page'
```
After the shreds routes (after line 737, the `/dz/edge/scoreboard` redirect):
```tsx
            <Route path="/dz/hyperliquid" element={<Navigate to="/dz/hyperliquid/scoreboard" replace />} />
            <Route path="/dz/hyperliquid/scoreboard" element={<HyperliquidScoreboardPage />} />
```

- [ ] **Step 2: Add route detection in the sidebar**

In `web/src/components/sidebar.tsx`, near the other `is…Route` consts (~line 109-119):
```tsx
  const isHyperliquidScoreboardRoute = location.pathname === '/dz/hyperliquid/scoreboard'
  const isHyperliquidRoute = location.pathname.startsWith('/dz/hyperliquid')
```

- [ ] **Step 3: Add the nav group in the Edge section**

In `web/src/components/sidebar.tsx`, inside the Edge section `<div className="space-y-1">`, after the Shreds block closes (after line 583 `</>` `)}`), add a sibling block. The `Link` and class-helper names (`navItemClass`, `navItemExpandedClass`, `subNavItemClass`) match the Shreds block above:
```tsx
            <Link to="/dz/hyperliquid/scoreboard" className={isHyperliquidRoute ? navItemExpandedClass : navItemClass(false)}>
              <Activity className="h-4 w-4" />
              Hyperliquid
            </Link>
            {isHyperliquidRoute && (
              <>
                <Link to="/dz/hyperliquid/scoreboard" className={subNavItemClass(isHyperliquidScoreboardRoute)}>
                  Scoreboard
                </Link>
              </>
            )}
```
Ensure an icon is imported: the Shreds entry uses `Puzzle` from `lucide-react` (see the import block at the top of `sidebar.tsx`). Add `Activity` to that existing `lucide-react` import (or reuse an already-imported icon if `Activity` is taken).

- [ ] **Step 4: Type-check and verify the route renders**

Run: `cd web && bunx tsc -b`
Expected: passes. Then with the dev env up, open `http://localhost:5173/dz/hyperliquid/scoreboard` (or the Tailscale URL) and confirm the page renders and the "Hyperliquid" nav item appears under Edge with the Scoreboard child when active.

- [ ] **Step 5: Commit**

```bash
git add web/src/App.tsx web/src/components/sidebar.tsx
git commit -m "web: hyperliquid scoreboard route and sidebar nav"
```

---

### Task 11: Local seed script (developer convenience)

**Files:**
- Create: `scripts/seed-hyperliquid-local.sh`

**Interfaces:**
- Consumes: `FEEDS_CH_*` env vars in `.env` (remote `feeds_reader` creds) and local ClickHouse (`CLICKHOUSE_ADDR_TCP`).
- Produces: a local `feeds.hyperliquid_bbo_feed_race_summary` table populated with recent rows for UI development without the production proxy.

- [ ] **Step 1: Create the script**

Create `scripts/seed-hyperliquid-local.sh` (mirrors `scripts/seed-shredder-local.sh`):
```bash
#!/usr/bin/env bash
# Seed local ClickHouse with recent rows from remote feeds.hyperliquid_bbo_feed_race_summary.
# Reads credentials from .env (FEEDS_CH_*). Usage: ./scripts/seed-hyperliquid-local.sh [MINUTES]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
[[ -f "$ROOT_DIR/.env" ]] && { set -a; source "$ROOT_DIR/.env"; set +a; }

REMOTE_HOST="${FEEDS_CH_HOST:?set FEEDS_CH_HOST in .env}"
REMOTE_HTTPS_PORT="${FEEDS_CH_PORT:-8443}"
REMOTE_DB="${FEEDS_CH_DB:-feeds}"
REMOTE_USER="${FEEDS_CH_USER:?set FEEDS_CH_USER in .env}"
REMOTE_PASS="${FEEDS_CH_PASSWORD:?set FEEDS_CH_PASSWORD in .env}"
MINUTES="${1:-10}"

LOCAL_HOST="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
LOCAL_ADDR="${LOCAL_HOST%%:*}"
LOCAL_PORT="${LOCAL_HOST##*:}"

COLS="event_ts, ingested_at, capture_run_id, measurement_node_id, host, location_code, feed_type, symbol, source_ts_ms, bbo_hash, feed, loser_feed, total_events, events_won, lead_time_p50_ms, lead_time_p95_ms, send_lead_time_p50_ms, send_lead_time_p95_ms"

echo "==> Creating local feeds.hyperliquid_bbo_feed_race_summary..."
clickhouse client --host "$LOCAL_ADDR" --port "$LOCAL_PORT" --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS feeds;
DROP TABLE IF EXISTS feeds.hyperliquid_bbo_feed_race_summary;
CREATE TABLE feeds.hyperliquid_bbo_feed_race_summary (
  event_ts DateTime64(9), ingested_at DateTime64(9) DEFAULT now64(9),
  capture_run_id String, measurement_node_id String, host String,
  location_code LowCardinality(String), feed_type LowCardinality(String) DEFAULT 'bbo',
  symbol LowCardinality(String), source_ts_ms UInt64, bbo_hash UInt64,
  feed LowCardinality(String), loser_feed LowCardinality(String) DEFAULT '',
  total_events UInt64, events_won UInt64,
  lead_time_p50_ms Float64 DEFAULT 0, lead_time_p95_ms Float64 DEFAULT 0,
  send_lead_time_p50_ms Nullable(Float64) DEFAULT NULL, send_lead_time_p95_ms Nullable(Float64) DEFAULT NULL
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(event_ts)
ORDER BY (measurement_node_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed);
SQL

echo "==> Fetching last ${MINUTES}m from remote..."
curl -sS "https://${REMOTE_HOST}:${REMOTE_HTTPS_PORT}/?database=${REMOTE_DB}" \
  --user "${REMOTE_USER}:${REMOTE_PASS}" \
  --data-binary "SELECT ${COLS} FROM hyperliquid_bbo_feed_race_summary WHERE event_ts >= now() - INTERVAL ${MINUTES} MINUTE FORMAT TabSeparated" \
  > /tmp/hl_feed_race.tsv

ROWS=$(wc -l < /tmp/hl_feed_race.tsv | tr -d ' ')
echo "==> Inserting ${ROWS} rows locally..."
clickhouse client --host "$LOCAL_ADDR" --port "$LOCAL_PORT" \
  --query "INSERT INTO feeds.hyperliquid_bbo_feed_race_summary (${COLS}) FORMAT TabSeparated" \
  < /tmp/hl_feed_race.tsv
rm -f /tmp/hl_feed_race.tsv
echo "==> Done. Seeded ${ROWS} rows into feeds.hyperliquid_bbo_feed_race_summary"
```

- [ ] **Step 2: Make it executable and smoke-test**

```bash
chmod +x scripts/seed-hyperliquid-local.sh
./scripts/seed-hyperliquid-local.sh 5
```
Expected: prints a row count and "Done". (Requires `FEEDS_CH_*` in `.env` and the local ClickHouse running.) Then load the page locally and confirm data renders.

- [ ] **Step 3: Commit**

```bash
git add scripts/seed-hyperliquid-local.sh
git commit -m "scripts: seed local hyperliquid feed race summary"
```

---

## Self-Review

**1. Spec coverage:**
- Proxy + config (`CLICKHOUSE_FEEDS_DB`, `FeedsDB`) → Tasks 1–2. ✓
- `tob_*` rollup + competitor labels → Task 3 (`hyperliquidCompetitors`, `startsWith(feed,'tob_')`). ✓
- Headline DZ win-share (Grafana formula) → Task 3. ✓
- Per-competitor win% + exact lead p50/p95 → Task 3. ✓
- Per-node breakdown → Task 4. ✓
- Recent-races live strip + fast cache → Tasks 5, 7. ✓
- Endpoint `/api/dz/hyperliquid/scoreboard`, default-shape caching, `X-Cache` → Task 6. ✓
- Page cache `hyperliquid_scoreboard` (30s) + `:latest` (fast) → Task 7. ✓
- Window set `1h/24h/7d`, default `24h` → Tasks 3, 6, 8. ✓
- Symbol filter (default all, cache bypass) + sanitization → Tasks 3, 6, 8. ✓
- Graceful degradation on missing proxy table → Task 6 (`hyperliquidFeedsTableExists`). ✓
- Web nav (sibling group under Edge), route, page → Tasks 9–10. ✓
- Go handler tests + local seed script → Tasks 3–6, 11. ✓
- Non-goals (raw-table features, gossip, charts) → not implemented, by design. ✓

**2. Placeholder scan:** No `TBD`/`TODO`/"handle errors appropriately"; every code step shows complete code. Two steps ask the implementer to confirm a neighboring file's exact identifiers before writing (`externalRemoteTables` field names in Task 2; `PageHeader` props and sidebar class/icon names in Tasks 9–10) — these are verification steps with a concrete fallback, not placeholders.

**3. Type consistency:** `HyperliquidScoreboardResponse` / `HyperliquidCompetitor` / `HyperliquidNode` / `HyperliquidRace` field names and JSON tags match between Go (Tasks 3–5) and TS (Task 8). `FetchHyperliquidScoreboardData(ctx, window, symbol)`, `FetchHyperliquidScoreboardLatest(ctx, limit)`, and `fetchHyperliquidRecentRaces(ctx, symbol, sinceTs, limit)` signatures are consistent across Tasks 3–7. Cache keys `hyperliquid_scoreboard` / `hyperliquid_scoreboard:latest` match between Task 6 (read) and Task 7 (write).

## Open Dependency

Before the proxy (Task 2) yields data in staging/prod, lake's remote reader (`REMOTE_CH_USER`) must be granted `SELECT` on `feeds.*` on the ClickHouse Cloud instance. Local development uses `scripts/seed-hyperliquid-local.sh` instead and does not need the grant.
