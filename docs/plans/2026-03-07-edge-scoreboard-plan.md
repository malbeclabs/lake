# Edge Scoreboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a live scoreboard page showing DZ Edge shred delivery win rates, completeness, and lead times across measurement nodes.

**Architecture:** Single Go API endpoint queries `slot_feed_races` in the shredder ClickHouse database, JOINs `dz_metros_current` for geo coordinates. React frontend renders summary cards, bar chart, world map, and detail table with 30s auto-refresh.

**Tech Stack:** Go (chi router, ClickHouse driver), React 19, TypeScript, Recharts, MapLibre GL, Tailwind CSS v4, React Query v5.

---

### Task 1: API Handler — Response Types and Route Registration

**Files:**
- Create: `api/handlers/edge_scoreboard.go`
- Modify: `api/main.go:451` (after publisher-check route)

**Step 1: Create the handler file with response structs and stub**

Create `api/handlers/edge_scoreboard.go`:

```go
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/metrics"
)

type EdgeScoreboardFeedStats struct {
	ShredsWon   uint64  `json:"shreds_won"`
	TotalShreds uint64  `json:"total_shreds"`
	WinRatePct  float64 `json:"win_rate_pct"`
	LeadP50Ms   float64 `json:"lead_p50_ms"`
	LeadP95Ms   float64 `json:"lead_p95_ms"`
}

type EdgeScoreboardNode struct {
	NodeID        string                             `json:"node_id"`
	Location      string                             `json:"location"`
	MetroName     string                             `json:"metro_name"`
	Latitude      float64                            `json:"latitude"`
	Longitude     float64                            `json:"longitude"`
	Feeds         map[string]*EdgeScoreboardFeedStats `json:"feeds"`
	SlotsObserved uint64                             `json:"slots_observed"`
	LastUpdated   time.Time                          `json:"last_updated"`
}

type EdgeScoreboardResponse struct {
	Window          string                `json:"window"`
	GeneratedAt     time.Time             `json:"generated_at"`
	CurrentEpoch    uint64                `json:"current_epoch"`
	TotalSlots      uint64                `json:"total_slots"`
	DZSlots         uint64                `json:"dz_slots"`
	CompletenessPct float64               `json:"completeness_pct"`
	Nodes           []EdgeScoreboardNode  `json:"nodes"`
}

func GetEdgeScoreboard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, EdgeScoreboardResponse{
		Window:      "24h",
		GeneratedAt: time.Now().UTC(),
		Nodes:       []EdgeScoreboardNode{},
	})
}
```

**Step 2: Register the route in main.go**

In `api/main.go`, add after line 451 (after the publisher-check route):

```go
r.Get("/api/dz/edge/scoreboard", handlers.GetEdgeScoreboard)
```

**Step 3: Verify it compiles**

Run: `cd /Users/amcconnell/src/git/lake && go build ./api/...`
Expected: Success (no errors)

**Step 4: Commit**

```
git add api/handlers/edge_scoreboard.go api/main.go
git commit -m "web: add edge scoreboard API stub"
```

---

### Task 2: API Handler — ClickHouse Query and Response Assembly

**Files:**
- Modify: `api/handlers/edge_scoreboard.go`

**Step 1: Write the test file with test data setup**

Create `api/handlers/edge_scoreboard_test.go`:

```go
package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	apitesting "github.com/malbeclabs/lake/api/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createSlotFeedRacesTable(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	err := config.DB.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", config.ShredderDB))
	require.NoError(t, err)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.slot_feed_races (
			event_ts DateTime64(3),
			ingested_at DateTime64(3) DEFAULT now(),
			node_id String,
			feed_type String,
			epoch UInt64,
			slot UInt64,
			feed String,
			total_shreds UInt64,
			shreds_won UInt64,
			lead_time_p50_ms Float64,
			lead_time_p95_ms Float64
		) ENGINE = ReplacingMergeTree(ingested_at)
		PARTITION BY toYYYYMM(event_ts)
		ORDER BY (node_id, slot, feed)
	`, "`"+config.ShredderDB+"`"))
	require.NoError(t, err)
}

func insertEdgeScoreboardTestData(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	createSlotFeedRacesTable(t)

	// Create metros for location resolution
	err := config.DB.Exec(ctx, `
		INSERT INTO dim_dz_metros_history
			(entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash,
			 pk, code, name, latitude, longitude)
		VALUES
			('metro-slc', now(), now(), generateUUIDv4(), 0, 1,
			 'metro-slc', 'slc', 'Salt Lake City', 40.76, -111.89),
			('metro-fra', now(), now(), generateUUIDv4(), 0, 2,
			 'metro-fra', 'fra', 'Frankfurt', 50.11, 8.68)
	`)
	require.NoError(t, err)

	// Insert slot_feed_races data for two nodes
	// Node slc-qa-bm1: slot 100 with dz winning most shreds
	// Node fra-qa-bm1: slot 100 with dz winning some shreds
	// Also slot 200 with no dz (jito vs turbine only)
	table := fmt.Sprintf("`%s`.slot_feed_races", config.ShredderDB)
	err = config.DB.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
			(event_ts, node_id, feed_type, epoch, slot, feed, total_shreds, shreds_won, lead_time_p50_ms, lead_time_p95_ms)
		VALUES
			(now(), 'slc-qa-bm1', 'shred', 800, 100, 'dz',      3000, 2000, 3.0, 9.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 100, 'turbine',  3000,  800, 0.0, 0.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 100, 'jito',     3000,  200, 5.0, 11.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 200, 'turbine',  2500, 1000, 0.0, 0.0),
			(now(), 'slc-qa-bm1', 'shred', 800, 200, 'jito',     2500, 1500, 6.0, 10.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 100, 'dz',       3000, 1500, 2.0, 8.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 100, 'turbine',  3000, 1000, 0.0, 0.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 100, 'jito',     3000,  500, 4.0, 10.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 200, 'turbine',  2500,  800, 0.0, 0.0),
			(now(), 'fra-qa-bm1', 'shred', 800, 200, 'jito',     2500, 1700, 7.0, 12.0)
	`, table))
	require.NoError(t, err)
}

func TestGetEdgeScoreboard_Empty(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createSlotFeedRacesTable(t)

	req := httptest.NewRequest("GET", "/api/dz/edge/scoreboard", nil)
	w := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "24h", resp.Window)
	assert.Empty(t, resp.Nodes)
}

func TestGetEdgeScoreboard_WithData(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertEdgeScoreboardTestData(t)

	req := httptest.NewRequest("GET", "/api/dz/edge/scoreboard?window=24h", nil)
	w := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, "24h", resp.Window)
	assert.Equal(t, uint64(800), resp.CurrentEpoch)

	// total_slots: each node has 2 slots (100, 200). Global = 2 per node.
	// dz_slots: each node has 1 DZ slot (100). Global = 1 per node.
	// completeness = dz_slots / total_slots across all nodes
	assert.Equal(t, uint64(4), resp.TotalSlots)  // 2 per node * 2 nodes
	assert.Equal(t, uint64(2), resp.DZSlots)      // 1 per node * 2 nodes
	assert.InDelta(t, 50.0, resp.CompletenessPct, 0.1) // 2/4 = 50%

	// Should have 2 nodes
	assert.Len(t, resp.Nodes, 2)

	// Find SLC node
	var slc *handlers.EdgeScoreboardNode
	for i := range resp.Nodes {
		if resp.Nodes[i].Location == "SLC" {
			slc = &resp.Nodes[i]
			break
		}
	}
	require.NotNil(t, slc, "SLC node not found")
	assert.Equal(t, "Salt Lake City", slc.MetroName)
	assert.InDelta(t, 40.76, slc.Latitude, 0.01)
	assert.Equal(t, uint64(1), slc.SlotsObserved) // 1 DZ slot

	// SLC DZ feed: 2000 won out of 3000 total = 66.7%
	dzFeed := slc.Feeds["dz"]
	require.NotNil(t, dzFeed)
	assert.Equal(t, uint64(2000), dzFeed.ShredsWon)
	assert.Equal(t, uint64(3000), dzFeed.TotalShreds)
	assert.InDelta(t, 66.67, dzFeed.WinRatePct, 0.1)
	assert.InDelta(t, 3.0, dzFeed.LeadP50Ms, 0.01)
	assert.InDelta(t, 9.0, dzFeed.LeadP95Ms, 0.01)
}

func TestGetEdgeScoreboard_WindowParam(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	insertEdgeScoreboardTestData(t)

	for _, window := range []string{"1h", "24h", "7d", "30d", "all"} {
		req := httptest.NewRequest("GET", "/api/dz/edge/scoreboard?window="+window, nil)
		w := httptest.NewRecorder()
		handlers.GetEdgeScoreboard(w, req)
		assert.Equal(t, http.StatusOK, w.Code, "window=%s", window)

		var resp handlers.EdgeScoreboardResponse
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "window=%s", window)
		assert.Equal(t, window, resp.Window)
	}
}

func TestGetEdgeScoreboard_InvalidWindow(t *testing.T) {
	apitesting.SetupTestClickHouseWithMigrations(t, testChDB)
	createSlotFeedRacesTable(t)

	req := httptest.NewRequest("GET", "/api/dz/edge/scoreboard?window=invalid", nil)
	w := httptest.NewRecorder()
	handlers.GetEdgeScoreboard(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp handlers.EdgeScoreboardResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "24h", resp.Window) // falls back to default
}
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/amcconnell/src/git/lake && go test ./api/handlers/ -run TestGetEdgeScoreboard -v -count=1`
Expected: Tests fail (stub returns empty response, no query logic yet)

**Step 3: Implement the full handler**

Replace the `GetEdgeScoreboard` function in `api/handlers/edge_scoreboard.go`:

```go
var validWindows = map[string]string{
	"1h":  "1 HOUR",
	"24h": "24 HOUR",
	"7d":  "7 DAY",
	"30d": "30 DAY",
	"all": "",
}

func GetEdgeScoreboard(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	window := r.URL.Query().Get("window")
	if window == "" {
		window = "24h"
	}
	interval, ok := validWindows[window]
	if !ok {
		window = "24h"
		interval = "24 HOUR"
	}

	start := time.Now()
	db := envDB(ctx)
	raceTable := fmt.Sprintf("`%s`.slot_feed_races", config.ShredderDB)

	// Build time filter clause
	timeFilter := ""
	if interval != "" {
		timeFilter = fmt.Sprintf("AND event_ts >= now() - INTERVAL %s", interval)
	}

	// Query 1: per-node slot counts (all slots, not just DZ)
	slotQuery := fmt.Sprintf(`
		SELECT
			node_id,
			COUNT(DISTINCT slot) AS total_slots,
			countIf(feed = 'dz') AS dz_row_count,
			COUNT(DISTINCT IF(feed = 'dz', slot, NULL)) AS dz_slots,
			max(epoch) AS current_epoch
		FROM %s
		WHERE feed_type = 'shred' %s
		GROUP BY node_id
	`, raceTable, timeFilter)

	slotRows, err := db.Query(ctx, slotQuery)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		log.Printf("EdgeScoreboard slot query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type nodeSlotInfo struct {
		totalSlots   uint64
		dzSlots      uint64
		currentEpoch uint64
	}
	nodeSlots := make(map[string]*nodeSlotInfo)
	var globalTotalSlots, globalDZSlots, globalEpoch uint64

	for slotRows.Next() {
		var nodeID string
		var totalSlots, dzRowCount, dzSlots, epoch uint64
		if err := slotRows.Scan(&nodeID, &totalSlots, &dzRowCount, &dzSlots, &epoch); err != nil {
			log.Printf("EdgeScoreboard slot scan error: %v", err)
			continue
		}
		nodeSlots[nodeID] = &nodeSlotInfo{totalSlots: totalSlots, dzSlots: dzSlots, currentEpoch: epoch}
		globalTotalSlots += totalSlots
		globalDZSlots += dzSlots
		if epoch > globalEpoch {
			globalEpoch = epoch
		}
	}
	slotRows.Close()

	if len(nodeSlots) == 0 {
		writeJSON(w, EdgeScoreboardResponse{
			Window:      window,
			GeneratedAt: time.Now().UTC(),
			Nodes:       []EdgeScoreboardNode{},
		})
		return
	}

	// Query 2: per-node, per-feed aggregates (only DZ-participating slots)
	feedQuery := fmt.Sprintf(`
		SELECT
			r.node_id,
			r.feed,
			SUM(r.shreds_won) AS shreds_won,
			SUM(r.total_shreds) AS total_shreds,
			IF(SUM(r.shreds_won) > 0,
				SUM(r.lead_time_p50_ms * r.shreds_won) / SUM(r.shreds_won), 0) AS lead_p50_ms,
			IF(SUM(r.shreds_won) > 0,
				SUM(r.lead_time_p95_ms * r.shreds_won) / SUM(r.shreds_won), 0) AS lead_p95_ms,
			MAX(r.event_ts) AS last_updated
		FROM %s r
		WHERE r.feed_type = 'shred' %s
			AND r.slot IN (
				SELECT DISTINCT slot FROM %s
				WHERE feed = 'dz' AND feed_type = 'shred' %s
					AND node_id = r.node_id
			)
		GROUP BY r.node_id, r.feed
		ORDER BY r.node_id, r.feed
	`, raceTable, timeFilter, raceTable, timeFilter)

	feedRows, err := db.Query(ctx, feedQuery)
	duration = time.Since(start)
	metrics.RecordClickHouseQuery(duration, err)
	if err != nil {
		log.Printf("EdgeScoreboard feed query error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	nodeMap := make(map[string]*EdgeScoreboardNode)
	for feedRows.Next() {
		var nodeID, feed string
		var shredsWon, totalShreds uint64
		var leadP50, leadP95 float64
		var lastUpdated time.Time

		if err := feedRows.Scan(&nodeID, &feed, &shredsWon, &totalShreds, &leadP50, &leadP95, &lastUpdated); err != nil {
			log.Printf("EdgeScoreboard feed scan error: %v", err)
			continue
		}

		node, exists := nodeMap[nodeID]
		if !exists {
			loc := strings.ToUpper(strings.SplitN(nodeID, "-", 2)[0])
			info := nodeSlots[nodeID]
			node = &EdgeScoreboardNode{
				NodeID:        nodeID,
				Location:      loc,
				Feeds:         make(map[string]*EdgeScoreboardFeedStats),
				SlotsObserved: info.dzSlots,
				LastUpdated:   lastUpdated,
			}
			nodeMap[nodeID] = node
		}

		var winRate float64
		if totalShreds > 0 {
			winRate = float64(shredsWon) * 100.0 / float64(totalShreds)
		}

		node.Feeds[feed] = &EdgeScoreboardFeedStats{
			ShredsWon:   shredsWon,
			TotalShreds: totalShreds,
			WinRatePct:  winRate,
			LeadP50Ms:   leadP50,
			LeadP95Ms:   leadP95,
		}

		if lastUpdated.After(node.LastUpdated) {
			node.LastUpdated = lastUpdated
		}
	}
	feedRows.Close()

	// Query 3: resolve metro coords from location codes
	locCodes := make([]string, 0, len(nodeMap))
	for _, node := range nodeMap {
		locCodes = append(locCodes, strings.ToLower(node.Location))
	}
	if len(locCodes) > 0 {
		metroQuery := `SELECT code, name, latitude, longitude FROM dz_metros_current WHERE code IN (?)`
		metroRows, err := db.Query(ctx, metroQuery, locCodes)
		if err == nil {
			metros := make(map[string]struct {
				name string
				lat  float64
				lng  float64
			})
			for metroRows.Next() {
				var code, name string
				var lat, lng float64
				if err := metroRows.Scan(&code, &name, &lat, &lng); err == nil {
					metros[strings.ToUpper(code)] = struct {
						name string
						lat  float64
						lng  float64
					}{name, lat, lng}
				}
			}
			metroRows.Close()

			for _, node := range nodeMap {
				if m, ok := metros[node.Location]; ok {
					node.MetroName = m.name
					node.Latitude = m.lat
					node.Longitude = m.lng
				}
			}
		}
	}

	// Assemble response
	nodes := make([]EdgeScoreboardNode, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, *node)
	}

	var completenessPct float64
	if globalTotalSlots > 0 {
		completenessPct = float64(globalDZSlots) * 100.0 / float64(globalTotalSlots)
	}

	writeJSON(w, EdgeScoreboardResponse{
		Window:          window,
		GeneratedAt:     time.Now().UTC(),
		CurrentEpoch:    globalEpoch,
		TotalSlots:      globalTotalSlots,
		DZSlots:         globalDZSlots,
		CompletenessPct: completenessPct,
		Nodes:           nodes,
	})
}
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/amcconnell/src/git/lake && go test ./api/handlers/ -run TestGetEdgeScoreboard -v -count=1`
Expected: All 4 tests pass

**Step 5: Commit**

```
git add api/handlers/edge_scoreboard.go api/handlers/edge_scoreboard_test.go
git commit -m "api: implement edge scoreboard endpoint"
```

---

### Task 3: Frontend — API Client Types and Fetch Function

**Files:**
- Modify: `web/src/lib/api.ts` (append at end, before final closing)

**Step 1: Add TypeScript types and fetch function**

Append to `web/src/lib/api.ts`:

```typescript
// Edge Scoreboard

export interface EdgeScoreboardFeedStats {
  shreds_won: number
  total_shreds: number
  win_rate_pct: number
  lead_p50_ms: number
  lead_p95_ms: number
}

export interface EdgeScoreboardNode {
  node_id: string
  location: string
  metro_name: string
  latitude: number
  longitude: number
  feeds: Record<string, EdgeScoreboardFeedStats>
  slots_observed: number
  last_updated: string
}

export interface EdgeScoreboardResponse {
  window: string
  generated_at: string
  current_epoch: number
  total_slots: number
  dz_slots: number
  completeness_pct: number
  nodes: EdgeScoreboardNode[]
}

export async function fetchEdgeScoreboard(window: string = '24h'): Promise<EdgeScoreboardResponse> {
  const params = new URLSearchParams()
  if (window !== '24h') params.set('window', window)
  const qs = params.toString()
  const res = await apiFetch(`/api/dz/edge/scoreboard${qs ? `?${qs}` : ''}`)
  if (!res.ok) {
    throw new Error('Failed to fetch edge scoreboard')
  }
  return res.json()
}
```

**Step 2: Verify TypeScript compiles**

Run: `cd /Users/amcconnell/src/git/lake/web && npx tsc -b --noEmit`
Expected: No type errors

**Step 3: Commit**

```
git add web/src/lib/api.ts
git commit -m "web: add edge scoreboard API types and fetch function"
```

---

### Task 4: Frontend — Scoreboard Page Component (Summary Cards + Banner)

**Files:**
- Create: `web/src/components/edge-scoreboard-page.tsx`
- Modify: `web/src/App.tsx:693` (after publisher-check route)
- Modify: `web/src/App.tsx` imports (top of file)

**Step 1: Create the page component**

Create `web/src/components/edge-scoreboard-page.tsx`:

```tsx
import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Trophy, Info } from 'lucide-react'
import { fetchEdgeScoreboard, type EdgeScoreboardResponse, type EdgeScoreboardNode } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { PageHeader } from './page-header'

const WINDOWS = ['1h', '24h', '7d', '30d', 'all'] as const
type Window = (typeof WINDOWS)[number]

function formatPct(v: number): string {
  return v.toFixed(1) + '%'
}

function formatMs(v: number): string {
  if (v < 1) return '<1ms'
  if (v >= 1000) return (v / 1000).toFixed(1) + 's'
  return v.toFixed(1) + 'ms'
}

function formatNumber(v: number): string {
  return v.toLocaleString()
}

export function EdgeScoreboardPage() {
  useDocumentTitle('Edge Scoreboard')
  const [searchParams, setSearchParams] = useSearchParams()
  const window = (searchParams.get('window') || '24h') as Window

  const { data, isLoading, error } = useQuery({
    queryKey: ['edge-scoreboard', window],
    queryFn: () => fetchEdgeScoreboard(window),
    refetchInterval: 30_000,
    staleTime: 15_000,
  })

  const setWindow = (w: Window) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (w === '24h') p.delete('window'); else p.set('window', w)
      return p
    })
  }

  // Compute global DZ stats from all nodes
  const globalDZ = useMemo(() => {
    if (!data?.nodes.length) return null
    let shredsWon = 0, totalShreds = 0, weightedP50 = 0, weightedP95 = 0
    for (const node of data.nodes) {
      const dz = node.feeds['dz']
      if (dz) {
        shredsWon += dz.shreds_won
        totalShreds += dz.total_shreds
        weightedP50 += dz.lead_p50_ms * dz.shreds_won
        weightedP95 += dz.lead_p95_ms * dz.shreds_won
      }
    }
    return {
      winRate: totalShreds > 0 ? (shredsWon / totalShreds) * 100 : 0,
      leadP50: shredsWon > 0 ? weightedP50 / shredsWon : 0,
      leadP95: shredsWon > 0 ? weightedP95 / shredsWon : 0,
    }
  }, [data])

  if (error) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
          <p className="text-red-500">Failed to load scoreboard data.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Trophy}
          title="Edge Scoreboard"
          actions={
            <div className="flex items-center gap-3">
              <div className="flex rounded-md border border-border overflow-hidden text-sm">
                {WINDOWS.map(w => (
                  <button
                    key={w}
                    onClick={() => setWindow(w)}
                    className={`px-3 py-1.5 transition-colors ${
                      window === w
                        ? 'bg-primary text-primary-foreground'
                        : 'hover:bg-muted'
                    }`}
                  >
                    {w === 'all' ? 'All' : w.toUpperCase()}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-1.5 text-sm text-emerald-500">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
                </span>
                LIVE
              </div>
            </div>
          }
        />

        {/* Completeness explanation banner */}
        <div className="mb-6 rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm">
          <div className="flex items-start gap-2">
            <Info className="h-4 w-4 text-amber-500 mt-0.5 shrink-0" />
            <p className="text-muted-foreground">
              <span className="font-medium text-amber-500">Coverage note:</span>{' '}
              DZ multicast delivers leader shreds only — not the full shred universe.{' '}
              <span className="font-medium">Completeness</span> = DZ shreds seen ÷ total shreds from all sources for a given
              slot. Win rates are measured within that covered subset. A high win rate + high completeness = the strongest signal.
            </p>
          </div>
        </div>

        {isLoading ? (
          <ScoreboardSkeleton />
        ) : data ? (
          <div className="space-y-6">
            {/* Summary cards */}
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
              <SummaryCard label="Avg Completeness" value={formatPct(data.completeness_pct)} />
              <SummaryCard label="DZ Win Rate" value={globalDZ ? formatPct(globalDZ.winRate) : '—'} />
              <SummaryCard label="DZ Lead p50" value={globalDZ ? formatMs(globalDZ.leadP50) : '—'} />
              <SummaryCard label="DZ Lead p95" value={globalDZ ? formatMs(globalDZ.leadP95) : '—'} />
              <SummaryCard label="Slots Observed" value={formatNumber(data.dz_slots)} />
            </div>

            {/* Bar chart and map will be added in subsequent tasks */}

            {/* Node detail table */}
            <NodeDetailTable nodes={data.nodes} />
          </div>
        ) : null}
      </div>
    </div>
  )
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <p className="text-xs text-muted-foreground mb-1">{label}</p>
      <p className="text-2xl font-semibold">{value}</p>
    </div>
  )
}

function NodeDetailTable({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const sorted = useMemo(() =>
    [...nodes].sort((a, b) => {
      const aWin = a.feeds['dz']?.win_rate_pct ?? 0
      const bWin = b.feeds['dz']?.win_rate_pct ?? 0
      return bWin - aWin
    }),
    [nodes]
  )

  return (
    <div className="rounded-lg border border-border overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-muted/50">
            <th className="px-4 py-3 text-left font-medium">Node</th>
            <th className="px-4 py-3 text-right font-medium">Completeness</th>
            <th className="px-4 py-3 text-right font-medium">DZ Win %</th>
            <th className="px-4 py-3 text-right font-medium">DZ Lead p50</th>
            <th className="px-4 py-3 text-right font-medium">DZ Lead p95</th>
            <th className="px-4 py-3 text-right font-medium">Jito Win %</th>
            <th className="px-4 py-3 text-right font-medium">Turbine Win %</th>
            <th className="px-4 py-3 text-right font-medium">Pipe Win %</th>
            <th className="px-4 py-3 text-right font-medium">Slots</th>
            <th className="px-4 py-3 text-right font-medium">Last Updated</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(node => {
            const dz = node.feeds['dz']
            const jito = node.feeds['jito']
            const turbine = node.feeds['turbine']
            const pipe = node.feeds['pipe']
            const nodeCompleteness = node.slots_observed > 0
              ? formatPct((node.slots_observed / (node.slots_observed + (node.feeds['turbine']?.total_shreds ? 1 : 0))) * 100)
              : '—'
            return (
              <tr key={node.node_id} className="border-b border-border last:border-0 hover:bg-muted/30">
                <td className="px-4 py-3">
                  <div className="font-medium">{node.location}</div>
                  <div className="text-xs text-muted-foreground">{node.metro_name}</div>
                </td>
                <td className="px-4 py-3 text-right tabular-nums">{formatPct(node.slots_observed > 0 ? (dz?.total_shreds ?? 0) / (dz?.total_shreds ?? 1) * (node.slots_observed / node.slots_observed) * 100 : 0)}</td>
                <td className="px-4 py-3 text-right tabular-nums font-medium text-emerald-500">{dz ? formatPct(dz.win_rate_pct) : '—'}</td>
                <td className="px-4 py-3 text-right tabular-nums">{dz ? formatMs(dz.lead_p50_ms) : '—'}</td>
                <td className="px-4 py-3 text-right tabular-nums">{dz ? formatMs(dz.lead_p95_ms) : '—'}</td>
                <td className="px-4 py-3 text-right tabular-nums">{jito ? formatPct(jito.win_rate_pct) : '—'}</td>
                <td className="px-4 py-3 text-right tabular-nums">{turbine ? formatPct(turbine.win_rate_pct) : '—'}</td>
                <td className="px-4 py-3 text-right tabular-nums">{pipe ? formatPct(pipe.win_rate_pct) : '—'}</td>
                <td className="px-4 py-3 text-right tabular-nums">{formatNumber(node.slots_observed)}</td>
                <td className="px-4 py-3 text-right text-muted-foreground text-xs">
                  {new Date(node.last_updated).toLocaleString()}
                </td>
              </tr>
            )
          })}
          {sorted.length === 0 && (
            <tr>
              <td colSpan={10} className="px-4 py-8 text-center text-muted-foreground">
                No data available for the selected time window.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

function ScoreboardSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="rounded-lg border border-border bg-card p-4">
            <div className="h-3 w-20 bg-muted rounded mb-2" />
            <div className="h-7 w-16 bg-muted rounded" />
          </div>
        ))}
      </div>
      <div className="h-64 rounded-lg bg-muted" />
      <div className="h-96 rounded-lg bg-muted" />
    </div>
  )
}
```

**Step 2: Add route and import in App.tsx**

Add import near line 57 (after PublisherCheckPage import):

```tsx
import { EdgeScoreboardPage } from './components/edge-scoreboard-page'
```

Add route after line 693 (after publisher-check route):

```tsx
<Route path="/dz/edge/scoreboard" element={<EdgeScoreboardPage />} />
```

**Step 3: Verify TypeScript compiles**

Run: `cd /Users/amcconnell/src/git/lake/web && npx tsc -b --noEmit`
Expected: No type errors

**Step 4: Commit**

```
git add web/src/components/edge-scoreboard-page.tsx web/src/App.tsx
git commit -m "web: add edge scoreboard page with summary cards and detail table"
```

---

### Task 5: Frontend — Sidebar Navigation Link

**Files:**
- Modify: `web/src/components/sidebar.tsx:89` (add route detection)
- Modify: `web/src/components/sidebar.tsx:30` (add Trophy import)
- Modify: `web/src/components/sidebar.tsx:532` (add link after Publisher Check)

**Step 1: Add Trophy to the lucide-react import**

In `web/src/components/sidebar.tsx`, add `Trophy` to the lucide-react import at the top of the file (around line 30).

**Step 2: Add route detection**

Near line 89 (where `isPublisherCheckRoute` is defined), add:

```tsx
const isScoreboardRoute = location.pathname === '/dz/edge/scoreboard'
```

**Step 3: Add sidebar link**

After line 532 (after the Publisher Check `</Link>` closing tag), add:

```tsx
<Link to="/dz/edge/scoreboard" className={navItemClass(isScoreboardRoute)}>
  <Trophy className="h-4 w-4" />
  Scoreboard
</Link>
```

**Step 4: Verify TypeScript compiles**

Run: `cd /Users/amcconnell/src/git/lake/web && npx tsc -b --noEmit`
Expected: No type errors

**Step 5: Commit**

```
git add web/src/components/sidebar.tsx
git commit -m "web: add scoreboard link to sidebar"
```

---

### Task 6: Frontend — Win Rate by Node Bar Chart

**Files:**
- Modify: `web/src/components/edge-scoreboard-page.tsx`

**Step 1: Add Recharts bar chart between summary cards and detail table**

Add import at the top of `edge-scoreboard-page.tsx`:

```tsx
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'
```

Add the chart component:

```tsx
function WinRateChart({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const chartData = useMemo(() =>
    [...nodes]
      .map(n => ({
        location: n.location,
        winRate: n.feeds['dz']?.win_rate_pct ?? 0,
        leadP50: n.feeds['dz']?.lead_p50_ms ?? 0,
      }))
      .sort((a, b) => b.winRate - a.winRate),
    [nodes]
  )

  if (chartData.length === 0) return null

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="text-sm font-medium mb-4">Win Rate by Node</h3>
      <ResponsiveContainer width="100%" height={Math.max(200, chartData.length * 48)}>
        <BarChart data={chartData} layout="vertical" margin={{ left: 8, right: 24, top: 4, bottom: 4 }}>
          <XAxis type="number" domain={[0, 100]} tickFormatter={v => `${v}%`} className="text-xs" />
          <YAxis type="category" dataKey="location" width={48} className="text-xs" />
          <Tooltip
            formatter={(value: number) => [`${value.toFixed(1)}%`, 'DZ Win Rate']}
            contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))' }}
          />
          <Bar dataKey="winRate" radius={[0, 4, 4, 0]}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.winRate >= 50 ? 'hsl(var(--chart-2))' : 'hsl(var(--chart-4))'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
```

Then insert `<WinRateChart nodes={data.nodes} />` between the summary cards grid and the `<NodeDetailTable>` in the main render.

**Step 2: Verify TypeScript compiles**

Run: `cd /Users/amcconnell/src/git/lake/web && npx tsc -b --noEmit`
Expected: No type errors

**Step 3: Commit**

```
git add web/src/components/edge-scoreboard-page.tsx
git commit -m "web: add win rate by node bar chart to scoreboard"
```

---

### Task 7: Frontend — Node Map

**Files:**
- Modify: `web/src/components/edge-scoreboard-page.tsx`

**Step 1: Add MapLibre map with node markers**

Add import:

```tsx
import { useRef, useEffect } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
```

Note: Check that `maplibre-gl` is already in `web/package.json` dependencies (it is, used by topology page). If not, run `cd web && bun add maplibre-gl`.

Add the map component:

```tsx
function NodeMap({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)

  const nodesWithCoords = nodes.filter(n => n.latitude !== 0 || n.longitude !== 0)

  useEffect(() => {
    if (!mapContainer.current || nodesWithCoords.length === 0) return
    if (mapRef.current) {
      mapRef.current.remove()
      mapRef.current = null
    }

    const map = new maplibregl.Map({
      container: mapContainer.current,
      style: {
        version: 8,
        sources: {
          'simple-tiles': {
            type: 'raster',
            tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '&copy; OpenStreetMap contributors',
          },
        },
        layers: [{
          id: 'simple-tiles',
          type: 'raster',
          source: 'simple-tiles',
        }],
      },
      center: [0, 30],
      zoom: 1.5,
      attributionControl: false,
    })

    map.addControl(new maplibregl.NavigationControl(), 'top-right')

    for (const node of nodesWithCoords) {
      const dz = node.feeds['dz']
      const winRate = dz?.win_rate_pct ?? 0
      const color = winRate >= 50 ? '#22c55e' : '#f59e0b'

      const el = document.createElement('div')
      el.className = 'flex flex-col items-center'
      el.innerHTML = `
        <div style="background:${color};color:white;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;white-space:nowrap;">
          ${node.location} ${winRate.toFixed(0)}%
        </div>
      `

      new maplibregl.Marker({ element: el })
        .setLngLat([node.longitude, node.latitude])
        .setPopup(new maplibregl.Popup({ offset: 25 }).setHTML(`
          <div style="font-size:13px">
            <strong>${node.location}</strong> — ${node.metro_name}<br/>
            DZ Win Rate: ${winRate.toFixed(1)}%<br/>
            Slots: ${node.slots_observed.toLocaleString()}
          </div>
        `))
        .addTo(map)
    }

    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [nodesWithCoords])

  if (nodesWithCoords.length === 0) return null

  return (
    <div className="rounded-lg border border-border overflow-hidden">
      <div ref={mapContainer} className="h-[350px] w-full" />
    </div>
  )
}
```

Insert `<NodeMap nodes={data.nodes} />` between the `<WinRateChart>` and `<NodeDetailTable>` in the main render.

**Step 2: Verify TypeScript compiles**

Run: `cd /Users/amcconnell/src/git/lake/web && npx tsc -b --noEmit`
Expected: No type errors

**Step 3: Commit**

```
git add web/src/components/edge-scoreboard-page.tsx
git commit -m "web: add node map to scoreboard"
```

---

### Task 8: Fix Node Completeness in Detail Table

**Files:**
- Modify: `web/src/components/edge-scoreboard-page.tsx`

The detail table's completeness column in Task 4 has a placeholder calculation. Now that we have the full picture, fix it.

**Step 1: Pass the global response data to the table**

Change `<NodeDetailTable nodes={data.nodes} />` to:

```tsx
<NodeDetailTable nodes={data.nodes} totalSlots={data.total_slots} nodeCount={data.nodes.length} />
```

Update the component to compute per-node completeness from the API response. The API returns `slots_observed` (DZ slots for that node) and `total_slots` (global). For per-node completeness, we need per-node total slots. Since the API currently returns global totals, compute per-node completeness as `node.slots_observed / (total_slots / nodeCount)` as an approximation, OR better yet, add `total_slots` to the per-node API response.

Actually — the cleaner fix is to add a `total_slots` field to `EdgeScoreboardNode` in the Go handler (from the `nodeSlots` map). Update the Go struct:

```go
type EdgeScoreboardNode struct {
	// ... existing fields ...
	TotalSlots    uint64  `json:"total_slots"`
	// ...
}
```

Set it when building the node in the handler:

```go
node = &EdgeScoreboardNode{
	// ... existing ...
	TotalSlots:    info.totalSlots,
	SlotsObserved: info.dzSlots,
	// ...
}
```

Update the TypeScript interface in `api.ts`:

```typescript
export interface EdgeScoreboardNode {
  // ... existing ...
  total_slots: number
  // ...
}
```

Then the table completeness column becomes:

```tsx
<td className="px-4 py-3 text-right tabular-nums">
  {node.total_slots > 0 ? formatPct((node.slots_observed / node.total_slots) * 100) : '—'}
</td>
```

**Step 2: Verify TypeScript compiles and Go tests pass**

Run: `cd /Users/amcconnell/src/git/lake && go test ./api/handlers/ -run TestGetEdgeScoreboard -v -count=1`
Run: `cd /Users/amcconnell/src/git/lake/web && npx tsc -b --noEmit`
Expected: Both pass

**Step 3: Commit**

```
git add api/handlers/edge_scoreboard.go api/handlers/edge_scoreboard_test.go web/src/lib/api.ts web/src/components/edge-scoreboard-page.tsx
git commit -m "web: add per-node completeness to scoreboard detail table"
```

---

### Task 9: Manual Verification

**Step 1: Start the API server (if not running)**

The user manages the API and web servers. Ask them to restart both to pick up the new handler and frontend code.

**Step 2: Navigate to the scoreboard page**

Open `http://localhost:5173/#/dz/edge/scoreboard` in the browser.

**Step 3: Verify**

- [ ] Page loads without errors
- [ ] Sidebar shows "Scoreboard" link with Trophy icon under DZ section
- [ ] Summary cards display (may show 0 / — if no data in local shredder DB)
- [ ] Time window toggle switches between 1h/24h/7d/30d/All
- [ ] URL updates with `?window=` param
- [ ] Completeness banner is visible
- [ ] Bar chart renders (when data is available)
- [ ] Map renders with node markers (when data is available)
- [ ] Detail table renders with columns
- [ ] LIVE indicator pulses green
- [ ] No console errors

**Step 4: Commit any fixes from manual testing**

---

### Task 10: Go Lint and Build Check

**Step 1: Run linter**

Run: `cd /Users/amcconnell/src/git/lake && make lint`
Expected: No new lint errors

**Step 2: Run full build**

Run: `cd /Users/amcconnell/src/git/lake && make build`
Expected: Build succeeds

**Step 3: Run frontend build**

Run: `cd /Users/amcconnell/src/git/lake/web && bun run build`
Expected: Build succeeds (tsc + vite)

**Step 4: Fix any issues and commit**
