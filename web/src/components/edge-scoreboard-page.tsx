import { useMemo, useRef, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Trophy, Info } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import {
  fetchEdgeScoreboard,
  type EdgeScoreboardNode,
  type EdgeScoreboardFeedStats,
  type EdgeScoreboardSlotRace,
} from '@/lib/api'
import { cn } from '@/lib/utils'
import { PageHeader } from './page-header'

const VALID_WINDOWS = ['1h', '24h', '7d', '30d', 'all'] as const
type TimeWindow = (typeof VALID_WINDOWS)[number]

function isValidWindow(v: string | null): v is TimeWindow {
  return v !== null && (VALID_WINDOWS as readonly string[]).includes(v)
}

function formatPct(v: number): string {
  return `${v.toFixed(1)}%`
}

function formatMs(v: number): string {
  if (v < 0.1) return '<0.1ms'
  if (v >= 1000) return `${(v / 1000).toFixed(1)}s`
  return `${v.toFixed(1)}ms`
}

function formatNumber(v: number): string {
  return v.toLocaleString()
}

function formatStake(sol: number): string {
  if (sol >= 1_000_000) return `${(sol / 1_000_000).toFixed(1)}M SOL`
  if (sol >= 1_000) return `${(sol / 1_000).toFixed(0)}K SOL`
  return `${sol.toFixed(0)} SOL`
}

/**
 * Get the tightest DZ lead time across all losers (min p50/p95).
 * Each lead time is already aggregated via quantile() on the server.
 * We pick the minimum across competitors — the closest race — as the
 * representative summary value. This avoids averaging percentiles.
 */
function getDzLead(feed: EdgeScoreboardFeedStats | undefined): { p50: number; p95: number } {
  if (!feed?.lead_times?.length) return { p50: 0, p95: 0 }
  let minP50 = Infinity
  let minP95 = Infinity
  for (const lt of feed.lead_times) {
    if (lt.p50_ms < minP50) minP50 = lt.p50_ms
    if (lt.p95_ms < minP95) minP95 = lt.p95_ms
  }
  return { p50: minP50 === Infinity ? 0 : minP50, p95: minP95 === Infinity ? 0 : minP95 }
}

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

function ScoreboardSkeleton() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <Skeleton className="h-8 w-64 mb-8" />
        <Skeleton className="h-16 mb-6" />
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4 mb-8">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-24" />
          ))}
        </div>
        <Skeleton className="h-[400px] rounded-lg" />
      </div>
    </div>
  )
}

function SummaryCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="text-sm text-muted-foreground mb-1">{label}</div>
      <div className="text-2xl font-semibold tabular-nums">{value}</div>
      {sub && <div className="text-xs text-muted-foreground mt-1">{sub}</div>}
    </div>
  )
}

const FEED_COLORS: Record<string, string> = {
  dz: '#22c55e',
  jito: '#3b82f6',
  turbine: '#f59e0b',
  pipe: '#a855f7',
}

const FEED_LABELS: Record<string, string> = {
  dz: 'Edge Direct',
  jito: 'Jito',
  turbine: 'Turbine',
  pipe: 'Pipe',
}

const SLOTS_PER_EPOCH = 432_000

function EpochProgress({ epoch, slot }: { epoch: number; slot: number }) {
  const slotInEpoch = slot % SLOTS_PER_EPOCH
  const pct = (slotInEpoch / SLOTS_PER_EPOCH) * 100

  return (
    <div className="rounded-lg border border-border bg-card p-4 mb-6">
      <div className="flex items-center justify-between mb-2">
        <div className="text-sm font-medium">
          Epoch {epoch.toLocaleString()}
        </div>
        <div className="text-sm text-muted-foreground tabular-nums">
          Slot {slotInEpoch.toLocaleString()} / {SLOTS_PER_EPOCH.toLocaleString()} ({pct.toFixed(1)}%)
        </div>
      </div>
      <div className="h-2 bg-muted rounded-full overflow-hidden">
        <div
          className="h-full bg-emerald-500 rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

/** Height per node row shared between Win Rate and Recent Slots charts. */
const NODE_ROW_HEIGHT = 72

function WinRateChart({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const chartData = useMemo(() => {
    const feedSet = new Set<string>()
    for (const n of nodes) {
      for (const f of Object.keys(n.feeds)) feedSet.add(f)
    }
    const feeds = [...feedSet].sort((a, b) => {
      if (a === 'dz') return -1
      if (b === 'dz') return 1
      return a.localeCompare(b)
    })

    const nodeRows = [...nodes]
      .sort((a, b) => (b.stake_sol ?? 0) - (a.stake_sol ?? 0))
      .map(n => {
        const row: Record<string, string | number> = { location: n.location }
        const rawSum = feeds.reduce((s, f) => s + (n.feeds[f]?.win_rate_pct ?? 0), 0)
        const scale = rawSum > 0 ? 100 / rawSum : 0
        for (const f of feeds) {
          row[f] = Math.round(((n.feeds[f]?.win_rate_pct ?? 0) * scale) * 10) / 10
          row[`${f}_shreds`] = n.feeds[f]?.shreds_won ?? 0
        }
        return { nodeId: n.node_id, location: n.location, data: [row] }
      })

    return { nodeRows, feeds }
  }, [nodes])

  if (chartData.nodeRows.length === 0) return null

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium">Win Rate by Node</h3>
        <div className="flex items-center gap-3">
          {chartData.feeds.map(f => (
            <div key={f} className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: FEED_COLORS[f] ?? '#6b7280' }} />
              {FEED_LABELS[f] ?? f}
            </div>
          ))}
        </div>
      </div>
      {chartData.nodeRows.map(nr => (
        <div key={nr.nodeId} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
          <div className="w-12 shrink-0 text-xs text-muted-foreground text-right pr-2">{nr.location}</div>
          <div className="flex-1 h-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={nr.data} layout="vertical" barSize={56} margin={{ top: 2, right: 24, bottom: 2, left: 0 }}>
                <XAxis type="number" domain={[0, 100]} hide />
                <YAxis type="category" hide dataKey="location" />
                <Tooltip
                  formatter={(value: number | string | undefined, name?: string, props?: { payload?: Record<string, number> }) => {
                    const shreds = props?.payload?.[`${name}_shreds`] ?? 0
                    return [
                      `${Number(value ?? 0).toFixed(1)}% (${Number(shreds).toLocaleString()} shreds)`,
                      FEED_LABELS[name ?? ''] ?? name ?? '',
                    ]
                  }}
                  contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #333', borderRadius: '6px', color: '#e5e5e5' }}
                  wrapperStyle={{ zIndex: 10 }}
                />
                {chartData.feeds.map((f, i) => (
                  <Bar
                    key={f}
                    dataKey={f}
                    stackId="winrate"
                    fill={FEED_COLORS[f] ?? '#6b7280'}
                    radius={i === chartData.feeds.length - 1 ? [0, 4, 4, 0] : undefined}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      ))}
      {/* Static X-axis labels */}
      <div className="flex items-center" style={{ paddingLeft: 48 }}>
        <div className="flex-1 flex justify-between pr-6 text-xs text-muted-foreground">
          <span>0%</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span>
        </div>
      </div>
    </div>
  )
}

function RecentSlotsChart({ slots, nodes }: { slots: EdgeScoreboardSlotRace[]; nodes: EdgeScoreboardNode[] }) {
  const chartData = useMemo(() => {
    if (!slots.length || !nodes.length) return { nodeCharts: [], feeds: [] as string[], slotCount: 0 }

    const validNodeIds = new Set(nodes.map(n => n.node_id))
    const filtered = slots.filter(s => validNodeIds.has(s.node_id))

    const feedSet = new Set<string>()
    for (const s of filtered) feedSet.add(s.feed)
    const feeds = [...feedSet].sort((a, b) => a === 'dz' ? -1 : b === 'dz' ? 1 : a.localeCompare(b))

    // Group: node -> slot -> feed -> win_pct
    const byNode = new Map<string, Map<number, Record<string, number>>>()
    for (const s of filtered) {
      let nodeMap = byNode.get(s.node_id)
      if (!nodeMap) { nodeMap = new Map(); byNode.set(s.node_id, nodeMap) }
      let row = nodeMap.get(s.slot)
      if (!row) { row = {}; nodeMap.set(s.slot, row) }
      row[s.feed] = s.win_pct
    }

    const slotNumbers = [...new Set(filtered.map(s => s.slot))].sort((a, b) => a - b)

    // Sort nodes by stake descending (matching Win Rate chart)
    const sortedNodes = [...nodes].sort((a, b) => (b.stake_sol ?? 0) - (a.stake_sol ?? 0))

    const nodeCharts = sortedNodes
      .filter(n => byNode.has(n.node_id))
      .map(n => {
        const slotMap = byNode.get(n.node_id)!
        const data = slotNumbers.map((slot, idx) => {
          const feedPcts = slotMap.get(slot) ?? {}
          const row: Record<string, number> = { idx }
          for (const f of feeds) row[f] = feedPcts[f] ?? 0
          return row
        })
        return { nodeId: n.node_id, location: n.location, data }
      })

    return { nodeCharts, feeds, slotCount: slotNumbers.length }
  }, [slots, nodes])

  if (!slots.length) return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="text-sm font-medium mb-4">Recent DZ Leader Slots — Win Rate per Slot</h3>
      <div className="text-sm text-muted-foreground text-center py-12">No recent slot data available.</div>
    </div>
  )

  const { nodeCharts, feeds, slotCount } = chartData

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium">Recent DZ Leader Slots — Win Rate per Slot</h3>
        <div className="flex items-center gap-3">
          {feeds.map(f => (
            <div key={f} className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: FEED_COLORS[f] ?? '#6b7280' }} />
              {FEED_LABELS[f] ?? f}
            </div>
          ))}
        </div>
      </div>
      {nodeCharts.map(nc => (
        <div key={nc.nodeId} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
          <div className="w-12 shrink-0 text-xs text-muted-foreground text-right pr-2">{nc.location}</div>
          <div className="flex-1 h-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={nc.data} margin={{ top: 2, right: 0, bottom: 2, left: 0 }}>
                <XAxis dataKey="idx" hide />
                <YAxis domain={[0, 100]} hide allowDataOverflow />
                <Tooltip
                  formatter={(value: number | undefined, name?: string) => [
                    `${(value ?? 0).toFixed(1)}%`,
                    FEED_LABELS[name ?? ''] ?? name ?? '',
                  ]}
                  labelFormatter={() => ''}
                  contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #333', borderRadius: '6px', color: '#e5e5e5' }}
                />
                {feeds.map(f => (
                  <Bar key={f} dataKey={f} stackId="s" fill={FEED_COLORS[f] ?? '#6b7280'} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      ))}
      <div className="text-xs text-muted-foreground text-center mt-1">
        {slotCount} most recent DZ leader slots
      </div>
    </div>
  )
}

function NodeMap({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)

  const nodesWithCoords = useMemo(() =>
    nodes.filter(n => n.latitude !== 0 || n.longitude !== 0),
    [nodes]
  )

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
          carto: {
            type: 'raster',
            tiles: ['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
          },
        },
        layers: [{
          id: 'carto-tiles',
          type: 'raster',
          source: 'carto',
          minzoom: 0,
          maxzoom: 22,
        }],
      },
      center: [0, 30],
      zoom: 1,
      attributionControl: false,
    })

    map.addControl(new maplibregl.NavigationControl(), 'top-right')

    // Fit map to show all node markers
    const bounds = new maplibregl.LngLatBounds()
    for (const node of nodesWithCoords) {
      bounds.extend([node.longitude, node.latitude])
    }
    map.fitBounds(bounds, { padding: 60, maxZoom: 8 })

    for (const node of nodesWithCoords) {
      const dz = node.feeds['dz']
      const winRate = dz?.win_rate_pct ?? 0
      const color = winRate >= 50 ? '#22c55e' : '#f59e0b'

      const el = document.createElement('div')
      el.style.cssText = `background:${color};color:white;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;white-space:nowrap;cursor:pointer;`
      el.textContent = `${node.location} ${winRate.toFixed(0)}%`

      new maplibregl.Marker({ element: el })
        .setLngLat([node.longitude, node.latitude])
        .setPopup(new maplibregl.Popup({ offset: 25 }).setHTML(
          `<div style="font-size:13px;color:#1a1a2e">` +
          `<strong>${node.location}</strong> — ${node.metro_name}<br/>` +
          `DZ Win Rate: ${winRate.toFixed(1)}%<br/>` +
          `Slots: ${node.slots_observed.toLocaleString()}` +
          `</div>`
        ))
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

export function EdgeScoreboardPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const rawWindow = searchParams.get('window')
  const window: TimeWindow = isValidWindow(rawWindow) ? rawWindow : '24h'

  const { data, isLoading, error } = useQuery({
    queryKey: ['edge-scoreboard', window],
    queryFn: () => fetchEdgeScoreboard(window),
    refetchInterval: 30_000,
    staleTime: 15_000,
  })

  const setWindow = (w: TimeWindow) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (w === '24h') p.delete('window')
      else p.set('window', w)
      return p
    })
  }

  // Aggregate global DZ stats across all nodes
  const globalStats = useMemo(() => {
    if (!data?.nodes) return null

    let dzShredsWon = 0
    let dzTotalShreds = 0
    let weightedLeadP50 = 0
    let weightedLeadP95 = 0
    let totalSlots = 0

    for (const node of data.nodes) {
      const dz = node.feeds['dz']
      if (!dz) continue
      dzShredsWon += dz.shreds_won
      dzTotalShreds += dz.total_shreds
      const lead = getDzLead(dz)
      weightedLeadP50 += lead.p50 * node.slots_observed
      weightedLeadP95 += lead.p95 * node.slots_observed
      totalSlots += node.slots_observed
    }

    return {
      winRate: dzTotalShreds > 0 ? (dzShredsWon / dzTotalShreds) * 100 : 0,
      leadP50: totalSlots > 0 ? weightedLeadP50 / totalSlots : 0,
      leadP95: totalSlots > 0 ? weightedLeadP95 / totalSlots : 0,
      totalSlots,
      avgCompleteness:
        data.nodes.length > 0
          ? data.nodes.reduce((sum, n) => sum + (n.total_slots > 0 ? n.slots_observed / n.total_slots : 0), 0) /
            data.nodes.length *
            100
          : 0,
    }
  }, [data?.nodes])

  // Sort nodes by stake weight descending
  const sortedNodes = useMemo(() => {
    if (!data?.nodes) return []
    return [...data.nodes].sort((a, b) => (b.stake_sol ?? 0) - (a.stake_sol ?? 0))
  }, [data?.nodes])

  if (isLoading) return <ScoreboardSkeleton />

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-500 mb-2">Failed to load edge scoreboard</div>
          <div className="text-sm text-muted-foreground">
            {error instanceof Error ? error.message : 'Unknown error'}
          </div>
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
              <div className="flex items-center rounded-md border border-border text-sm">
                {(['1h', '24h', '7d', '30d', 'all'] as const).map((w) => (
                  <button
                    key={w}
                    type="button"
                    onClick={() => setWindow(w)}
                    className={cn(
                      'px-3 py-1.5 transition-colors',
                      window === w
                        ? 'bg-primary text-primary-foreground'
                        : 'hover:bg-muted'
                    )}
                  >
                    {w === 'all' ? 'All' : w}
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

        {/* Completeness banner */}
        <div className="mb-6 rounded-lg bg-amber-500/10 border border-amber-500/20 px-4 py-3 text-sm text-amber-200">
          <div className="flex items-start gap-2">
            <Info className="h-4 w-4 mt-0.5 shrink-0 text-amber-500" />
            <span>
              <span className="font-medium text-amber-500">Completeness</span> — DZ delivers leader shreds only. Completeness measures the percentage of total leader slots observed by each edge node during the selected window.
            </span>
          </div>
        </div>

        {/* Epoch progress */}
        {data && data.current_epoch > 0 && (
          <EpochProgress epoch={data.current_epoch} slot={data.current_slot} />
        )}

        {/* Summary cards */}
        {globalStats && (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4 mb-8">
            <SummaryCard label="Edge Direct Completeness" value={formatPct(globalStats.avgCompleteness)} />
            <SummaryCard label="Edge Direct Win Rate" value={formatPct(globalStats.winRate)} />
            <SummaryCard label="Edge Direct Lead (p50)" value={formatMs(globalStats.leadP50)} />
            <SummaryCard label="Edge Direct Lead (p95)" value={formatMs(globalStats.leadP95)} />
            <SummaryCard label="Slots Observed" value={formatNumber(globalStats.totalSlots)} />
          </div>
        )}

        {/* Charts row */}
        {data?.nodes && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <WinRateChart nodes={data.nodes} />
            <RecentSlotsChart slots={data.recent_slots ?? []} nodes={data.nodes} />
          </div>
        )}

        {/* Node detail table */}
        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium">Node</th>
                  <th className="px-4 py-3 font-medium text-right">Completeness</th>
                  <th className="px-4 py-3 font-medium text-right">Edge Direct Win %</th>
                  <th className="px-4 py-3 font-medium text-right">vs Jito<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-4 py-3 font-medium text-right">vs Turbine<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-4 py-3 font-medium text-right">vs Pipe<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-4 py-3 font-medium">Sources Measured</th>
                  <th className="px-4 py-3 font-medium text-right">Slots</th>
                  <th className="px-4 py-3 font-medium text-right">Last Updated</th>
                </tr>
              </thead>
              <tbody>
                {sortedNodes.length === 0 ? (
                  <tr>
                    <td colSpan={9} className="px-4 py-12 text-center text-muted-foreground">
                      No data available for the selected time window.
                    </td>
                  </tr>
                ) : (
                  sortedNodes.map((node) => (
                    <NodeRow key={node.node_id} node={node} />
                  ))
                )}
              </tbody>
            </table>
          </div>
        {/* Map */}
        {data?.nodes && (
          <div className="mt-8">
            <NodeMap nodes={data.nodes} />
          </div>
        )}
        </div>
      </div>
    </div>
  )
}

function NodeRow({ node }: { node: EdgeScoreboardNode }) {
  const dz = node.feeds['dz']
  const completeness = node.total_slots > 0 ? (node.slots_observed / node.total_slots) * 100 : 0

  // Build lead time lookup: loser_feed -> { p50, p95 }
  const dzLeadByFeed: Record<string, { p50: number; p95: number }> = {}
  if (dz?.lead_times) {
    for (const lt of dz.lead_times) {
      dzLeadByFeed[lt.loser_feed] = { p50: lt.p50_ms, p95: lt.p95_ms }
    }
  }

  const updated = new Date(node.last_updated)
  const timeStr = updated.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })

  return (
    <tr className="border-b border-border last:border-b-0 hover:bg-muted/50 transition-colors">
      <td className="px-4 py-3">
        <div className="text-sm font-medium">{node.location}</div>
        <div className="text-xs text-muted-foreground">{node.metro_name}</div>
        {node.stake_sol > 0 && <div className="text-xs text-muted-foreground">{formatStake(node.stake_sol)} staked</div>}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">{formatPct(completeness)}</td>
      <td className="px-4 py-3 text-right tabular-nums text-sm text-green-500">
        {dz ? formatPct(dz.win_rate_pct) : '—'}
      </td>
      {['jito', 'turbine', 'pipe'].map(f => {
        const lt = dzLeadByFeed[f]
        return (
          <td key={f} className="px-4 py-3 text-right tabular-nums text-sm">
            {lt ? <>{formatMs(lt.p50)} <span className="text-muted-foreground">({formatMs(lt.p95)})</span></> : '—'}
          </td>
        )
      })}
      <td className="px-4 py-3 text-sm">
        <div className="flex flex-col gap-1">
          {Object.keys(node.feeds).sort((a, b) => a === 'dz' ? -1 : b === 'dz' ? 1 : a.localeCompare(b)).map(f => (
            <span key={f} className="inline-flex items-center rounded px-1.5 py-0.5 text-xs font-medium w-fit" style={{ backgroundColor: `${FEED_COLORS[f] ?? '#6b7280'}20`, color: FEED_COLORS[f] ?? '#6b7280' }}>
              {FEED_LABELS[f] ?? f}
            </span>
          ))}
        </div>
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {formatNumber(node.slots_observed)}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm text-muted-foreground">
        {timeStr}
      </td>
    </tr>
  )
}
