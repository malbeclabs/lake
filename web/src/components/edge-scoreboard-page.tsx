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
  if (v < 0.01) return '<0.01ms'
  if (v >= 1000) return `${(v / 1000).toFixed(2)}s`
  return `${v.toFixed(2)}ms`
}

function formatNumber(v: number): string {
  return v.toLocaleString()
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

function WinRateChart({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const chartData = useMemo(() => {
    // Collect all feed names across nodes
    const feedSet = new Set<string>()
    for (const n of nodes) {
      for (const f of Object.keys(n.feeds)) feedSet.add(f)
    }
    // Sort feeds: dz first, then alphabetical
    const feeds = [...feedSet].sort((a, b) => {
      if (a === 'dz') return -1
      if (b === 'dz') return 1
      return a.localeCompare(b)
    })

    const data = [...nodes]
      .map(n => {
        const row: Record<string, string | number> = { location: n.location }
        for (const f of feeds) {
          row[f] = n.feeds[f]?.win_rate_pct ?? 0
        }
        return row
      })
      .sort((a, b) => (Number(b['dz'] ?? 0)) - (Number(a['dz'] ?? 0)))

    return { data, feeds }
  }, [nodes])

  if (chartData.data.length === 0) return null

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
      <ResponsiveContainer width="100%" height={Math.max(200, chartData.data.length * 48)}>
        <BarChart data={chartData.data} layout="vertical" margin={{ left: 8, right: 24, top: 4, bottom: 4 }}>
          <XAxis type="number" domain={[0, 100]} tickFormatter={(v: number) => `${v}%`} fontSize={12} />
          <YAxis type="category" dataKey="location" width={48} fontSize={12} />
          <Tooltip
            formatter={(value: number | string | undefined, name?: string) => [
              `${Number(value ?? 0).toFixed(1)}%`,
              FEED_LABELS[name ?? ''] ?? name ?? '',
            ]}
            contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', borderRadius: '6px' }}
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

  // Sort nodes by DZ win rate descending
  const sortedNodes = useMemo(() => {
    if (!data?.nodes) return []
    return [...data.nodes].sort((a, b) => {
      const aRate = a.feeds['dz']?.win_rate_pct ?? 0
      const bRate = b.feeds['dz']?.win_rate_pct ?? 0
      return bRate - aRate
    })
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
            <NodeMap nodes={data.nodes} />
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
                  <th className="px-4 py-3 font-medium text-right">DZ Win %</th>
                  <th className="px-4 py-3 font-medium text-right">DZ Lead p50</th>
                  <th className="px-4 py-3 font-medium text-right">DZ Lead p95</th>
                  <th className="px-4 py-3 font-medium text-right">Jito Win %</th>
                  <th className="px-4 py-3 font-medium text-right">Turbine Win %</th>
                  <th className="px-4 py-3 font-medium text-right">Pipe Win %</th>
                  <th className="px-4 py-3 font-medium text-right">Slots</th>
                  <th className="px-4 py-3 font-medium text-right">Last Updated</th>
                </tr>
              </thead>
              <tbody>
                {sortedNodes.length === 0 ? (
                  <tr>
                    <td colSpan={10} className="px-4 py-12 text-center text-muted-foreground">
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
        </div>
      </div>
    </div>
  )
}

function NodeRow({ node }: { node: EdgeScoreboardNode }) {
  const dz = node.feeds['dz']
  const jito = node.feeds['jito']
  const turbine = node.feeds['turbine']
  const pipe = node.feeds['pipe']
  const dzLead = getDzLead(dz)
  const completeness = node.total_slots > 0 ? (node.slots_observed / node.total_slots) * 100 : 0

  const updated = new Date(node.last_updated)
  const timeStr = updated.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })

  return (
    <tr className="border-b border-border last:border-b-0 hover:bg-muted/50 transition-colors">
      <td className="px-4 py-3">
        <div className="text-sm font-medium">{node.location}</div>
        <div className="text-xs text-muted-foreground">{node.metro_name}</div>
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">{formatPct(completeness)}</td>
      <td className="px-4 py-3 text-right tabular-nums text-sm text-green-500">
        {dz ? formatPct(dz.win_rate_pct) : '—'}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {dz ? formatMs(dzLead.p50) : '—'}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {dz ? formatMs(dzLead.p95) : '—'}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {jito ? formatPct(jito.win_rate_pct) : '—'}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {turbine ? formatPct(turbine.win_rate_pct) : '—'}
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {pipe ? formatPct(pipe.win_rate_pct) : '—'}
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
