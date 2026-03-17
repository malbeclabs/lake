import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { Trophy, Info } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import {
  fetchEdgeScoreboard,
  type EdgeScoreboardNode,
  type EdgeScoreboardSlotRace,
  type EdgeScoreboardLeader,
} from '@/lib/api'
import { cn } from '@/lib/utils'
import { useTheme } from '@/hooks/use-theme'
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
  dz: 'Edge',
  jito: 'Jito Shredstream',
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
          Solana Epoch {epoch.toLocaleString()}
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function WinRateTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-[#1a1a2e] border border-[#333] rounded-md px-3 py-2 text-xs shadow-lg">
      <div className="text-[#e5e5e5] font-medium mb-1.5">{label}</div>
      <table className="border-spacing-0">
        <thead>
          <tr className="text-[#777]">
            <th className="pr-3 py-0.5 text-left font-normal">Feed</th>
            <th className="pr-3 py-0.5 text-right font-normal">Win %</th>
            <th className="py-0.5 text-right font-normal">Shreds</th>
          </tr>
        </thead>
        <tbody>
          {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
          {payload.map((entry: any) => {
            const feed = entry.dataKey ?? ''
            const shreds = entry.payload?.[`${feed}_shreds`] ?? 0
            return (
              <tr key={feed}>
                <td className="pr-3 py-0.5 font-medium" style={{ color: FEED_COLORS[feed] ?? '#6b7280' }}>
                  {FEED_LABELS[feed] ?? feed}
                </td>
                <td className="pr-3 py-0.5 text-right font-mono text-[#e5e5e5]">
                  {Number(entry.value ?? 0).toFixed(1)}%
                </td>
                <td className="py-0.5 text-right font-mono text-[#999]">
                  {Number(shreds).toLocaleString()}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function makeSlotRaceTooltip(slotLeaders?: Record<string, EdgeScoreboardLeader>) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return function SlotRaceTooltip({ active, payload }: any) {
    if (!active || !payload?.length) return null
    const slot = payload[0]?.payload?.slot
    const leader = slot != null ? slotLeaders?.[String(slot)] : undefined
    return (
      <div className="bg-[#1a1a2e] border border-[#333] rounded-md px-3 py-2 text-xs shadow-lg">
        {slot != null && <div className="text-[#e5e5e5] font-medium mb-1.5 font-mono">Slot {slot.toLocaleString()}</div>}
        {leader && (
          <div className="mb-1.5 text-[#999] border-b border-[#333] pb-1.5">
            <div className="text-[#e5e5e5]">{leader.name || leader.pubkey.slice(0, 8) + '...'}</div>
            {leader.ip && <div><span className="text-[#666]">IP </span><span className="font-mono">{leader.ip}</span></div>}
            {leader.asn_org && <div><span className="text-[#666]">Host </span>{leader.asn_org}</div>}
            {leader.city && <div><span className="text-[#666]">Loc </span>{leader.city}{leader.country ? `, ${leader.country}` : ''}</div>}
          </div>
        )}
        <table className="border-spacing-0">
          <thead>
            <tr className="text-[#777]">
              <th className="pr-3 py-0.5 text-left font-normal">Feed</th>
              <th className="py-0.5 text-right font-normal">Win %</th>
            </tr>
          </thead>
          <tbody>
            {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
            {payload.map((entry: any) => {
              const feed = entry.dataKey ?? ''
              return (
                <tr key={feed}>
                  <td className="pr-3 py-0.5 font-medium" style={{ color: FEED_COLORS[feed] ?? '#6b7280' }}>
                    {FEED_LABELS[feed] ?? feed}
                  </td>
                  <td className="py-0.5 text-right font-mono text-[#e5e5e5]">
                    {(entry.value ?? 0).toFixed(1)}%
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }
}

/** Height per node row shared between Win Rate and Recent Slots charts. */
const NODE_ROW_HEIGHT = 72

function NodeLabel({ node }: { node: EdgeScoreboardNode }) {
  const [show, setShow] = useState(false)
  const hasGossip = !!node.gossip_pubkey

  return (
    <div
      className="relative w-12 shrink-0 text-xs text-muted-foreground text-right pr-2 cursor-pointer"
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      {hasGossip ? (
        <Link to={`/solana/gossip-nodes/${node.gossip_pubkey}`} className="hover:text-accent transition-colors">
          {node.location}
        </Link>
      ) : (
        node.location
      )}
      {show && (node.gossip_ip || node.asn_org) && (
        <div className="absolute left-full top-1/2 -translate-y-1/2 ml-2 z-20 bg-popover border border-border rounded-lg shadow-lg p-3 text-xs whitespace-nowrap text-left text-foreground">
          {node.gossip_ip && (
            <div className="flex gap-2"><span className="text-muted-foreground">IP</span><span className="font-mono">{node.gossip_ip}</span></div>
          )}
          {node.asn_org && (
            <div className="flex gap-2"><span className="text-muted-foreground">Host</span><span>{node.asn_org}</span></div>
          )}
          {node.asn != null && node.asn > 0 && (
            <div className="flex gap-2"><span className="text-muted-foreground">ASN</span><span>AS{node.asn}</span></div>
          )}
          {node.city && (
            <div className="flex gap-2"><span className="text-muted-foreground">Location</span><span>{node.city}{node.country ? `, ${node.country}` : ''}</span></div>
          )}
          {hasGossip && (
            <div className="flex gap-2"><span className="text-muted-foreground">Pubkey</span><span className="font-mono">{node.gossip_pubkey!.slice(0, 8)}...{node.gossip_pubkey!.slice(-4)}</span></div>
          )}
        </div>
      )}
    </div>
  )
}

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
          row[`${f}_raw`] = n.feeds[f]?.win_rate_pct ?? 0
        }
        return { node: n, data: [row] }
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
        <div key={nr.node.node_id} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
          <NodeLabel node={nr.node} />
          <div className="flex-1 h-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={nr.data} layout="vertical" barSize={62} margin={{ top: 2, right: 24, bottom: 2, left: 0 }}>
                <XAxis type="number" domain={[0, 100]} hide />
                <YAxis type="category" hide dataKey="location" />
                <Tooltip content={WinRateTooltip} wrapperStyle={{ zIndex: 10 }} />
                {chartData.feeds.map((f, i) => (
                  <Bar
                    key={f}
                    dataKey={f}
                    stackId="winrate"
                    fill={FEED_COLORS[f] ?? '#6b7280'}
                    radius={i === chartData.feeds.length - 1 ? [0, 4, 4, 0] : undefined}
                    label={f === 'dz' ? ((props: { x?: number; y?: number; width?: number; height?: number; [k: string]: unknown }) => {
                      const raw = Number(nr.data[0]['dz_raw'] ?? 0)
                      return (
                        <text x={(props.x ?? 0) + (props.width ?? 0) / 2} y={(props.y ?? 0) + (props.height ?? 0) / 2} fill="#fff" fontSize={12} fontWeight={600} textAnchor="middle" dominantBaseline="central">
                          {`${raw.toFixed(1)}%`}
                        </text>
                      )
                    }) as unknown as boolean : undefined}
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

function RecentSlotsChart({ slots, nodes, slotLeaders }: { slots: EdgeScoreboardSlotRace[]; nodes: EdgeScoreboardNode[]; slotLeaders?: Record<string, EdgeScoreboardLeader> }) {
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
          const row: Record<string, number> = { idx, slot }
          for (const f of feeds) row[f] = feedPcts[f] ?? 0
          return row
        })
        return { node: n, data }
      })

    return { nodeCharts, feeds, slotCount: slotNumbers.length }
  }, [slots, nodes])

  if (!slots.length) return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="text-sm font-medium mb-4">Recent Edge Leader Slots — Win Rate per Slot</h3>
      <div className="text-sm text-muted-foreground text-center py-12">No recent slot data available.</div>
    </div>
  )

  const { nodeCharts, feeds, slotCount } = chartData

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium">Recent Edge Leader Slots — Win Rate per Slot</h3>
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
        <div key={nc.node.node_id} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
          <NodeLabel node={nc.node} />
          <div className="flex-1 h-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={nc.data} margin={{ top: 6, right: 0, bottom: 6, left: 0 }}>
                <XAxis dataKey="idx" hide />
                <YAxis domain={[0, 100]} hide allowDataOverflow />
                <Tooltip content={makeSlotRaceTooltip(slotLeaders)} wrapperStyle={{ zIndex: 10 }} />
                {feeds.map(f => (
                  <Bar key={f} dataKey={f} stackId="s" fill={FEED_COLORS[f] ?? '#6b7280'} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      ))}
      <div className="text-xs text-muted-foreground text-center mt-1">
        {slotCount} most recent Edge leader slots
      </div>
    </div>
  )
}

function NodeMap({ nodes }: { nodes: EdgeScoreboardNode[] }) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const mapRef = useRef<maplibregl.Map | null>(null)
  const { resolvedTheme } = useTheme()

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

    const tileVariant = resolvedTheme === 'dark' ? 'dark_all' : 'light_all'
    const map = new maplibregl.Map({
      container: mapContainer.current,
      style: {
        version: 8,
        sources: {
          carto: {
            type: 'raster',
            tiles: [`https://a.basemaps.cartocdn.com/${tileVariant}/{z}/{x}/{y}.png`],
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
          `Edge Win Rate: ${winRate.toFixed(1)}%<br/>` +
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
  }, [nodesWithCoords, resolvedTheme])

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
  const window: TimeWindow = isValidWindow(rawWindow) ? rawWindow : '1h'

  const { data, isLoading, error } = useQuery({
    queryKey: ['edge-scoreboard', window],
    queryFn: () => fetchEdgeScoreboard(window),
    refetchInterval: 30_000,
    staleTime: 15_000,
  })

  const setWindow = (w: TimeWindow) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (w === '1h') p.delete('window')
      else p.set('window', w)
      return p
    })
  }

  // Aggregate global Edge stats across all nodes
  const globalStats = useMemo(() => {
    if (!data?.nodes) return null

    let dzShredsWon = 0
    let dzTotalShreds = 0
    let totalSlots = 0

    // Per-competitor weighted lead times
    const competitors = ['jito', 'turbine'] as const
    const weightedP50: Record<string, number> = {}
    const weightedP95: Record<string, number> = {}
    const competitorSlots: Record<string, number> = {}
    for (const c of competitors) {
      weightedP50[c] = 0
      weightedP95[c] = 0
      competitorSlots[c] = 0
    }

    for (const node of data.nodes) {
      const dz = node.feeds['dz']
      if (!dz) continue
      dzShredsWon += dz.shreds_won
      dzTotalShreds += dz.total_shreds
      totalSlots += node.slots_observed

      if (dz.lead_times) {
        for (const lt of dz.lead_times) {
          if (lt.loser_feed in weightedP50) {
            weightedP50[lt.loser_feed] += lt.p50_ms * node.slots_observed
            weightedP95[lt.loser_feed] += lt.p95_ms * node.slots_observed
            competitorSlots[lt.loser_feed] += node.slots_observed
          }
        }
      }
    }

    const leads: Record<string, { p50: number; p95: number }> = {}
    for (const c of competitors) {
      if (competitorSlots[c] > 0) {
        leads[c] = {
          p50: weightedP50[c] / competitorSlots[c],
          p95: weightedP95[c] / competitorSlots[c],
        }
      }
    }

    return {
      winRate: dzTotalShreds > 0 ? (dzShredsWon / dzTotalShreds) * 100 : 0,
      leads,
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

        {/* Completeness info */}
        <div className="mb-6 rounded-lg bg-muted/50 border border-border px-4 py-3 text-sm text-muted-foreground">
          <div className="flex items-start gap-2">
            <Info className="h-4 w-4 mt-0.5 shrink-0" />
            <div className="space-y-1.5">
              <div><a href="https://doublezero.xyz/dz-edge" target="_blank" rel="noopener noreferrer" className="font-medium text-foreground hover:underline">Edge</a> delivers Solana leader shreds only and publishers are encouraged to not send retransmit shreds. Completeness measures the percentage of total leader slots observed by each edge node during the selected window.</div>
              <div><Link to="/dz/publisher-check" className="font-medium text-foreground hover:underline">Publisher Check</Link> tracks the publishers contributing to Solana shreds over Edge.</div>
            </div>
          </div>
        </div>

        {/* Epoch progress */}
        {data && data.current_epoch > 0 && (
          <EpochProgress epoch={data.current_epoch} slot={data.current_slot} />
        )}

        {/* Summary cards */}
        {globalStats && (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-8">
            <SummaryCard label="Edge Completeness" value={formatPct(globalStats.avgCompleteness)} />
            <SummaryCard label="Edge Win Rate" value={formatPct(globalStats.winRate)} />
            {Object.entries(globalStats.leads).map(([competitor, lead]) => (
              <SummaryCard
                key={competitor}
                label={`vs ${FEED_LABELS[competitor] ?? competitor}`}
                value={formatMs(lead.p50)}
                sub={`p95: ${formatMs(lead.p95)}`}
              />
            ))}
            <SummaryCard label="Slots Observed" value={formatNumber(globalStats.totalSlots)} sub={`${formatNumber(data?.nodes?.length ? Math.round(globalStats.totalSlots / data.nodes.length) : 0)} avg/host`} />
          </div>
        )}

        {/* Charts row */}
        {data?.nodes && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <WinRateChart nodes={data.nodes} />
            <RecentSlotsChart slots={data.recent_slots ?? []} nodes={data.nodes} slotLeaders={data.slot_leaders} />
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
                  <th className="px-4 py-3 font-medium text-right">Edge Win %</th>
                  <th className="px-4 py-3 font-medium text-right">vs Jito Shredstream<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-4 py-3 font-medium text-right">vs Turbine<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-4 py-3 font-medium">Sources Measured</th>
                  <th className="px-4 py-3 font-medium text-right">Slots</th>
                  <th className="px-4 py-3 font-medium text-right">Last Updated</th>
                </tr>
              </thead>
              <tbody>
                {sortedNodes.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-12 text-center text-muted-foreground">
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

        {/* Map */}
        {data?.nodes && (
          <div className="mt-8">
            <NodeMap nodes={data.nodes} />
          </div>
        )}
      </div>
    </div>
  )
}

function NodeRow({ node }: { node: EdgeScoreboardNode }) {
  const [showTooltip, setShowTooltip] = useState(false)
  const cellRef = useRef<HTMLDivElement>(null)
  const [tooltipAbove, setTooltipAbove] = useState(true)
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
  const hasGossip = !!node.gossip_pubkey

  return (
    <tr className="border-b border-border last:border-b-0 hover:bg-muted/50 transition-colors">
      <td className="px-4 py-3">
        <div ref={cellRef} className="relative" onMouseEnter={() => {
          if (cellRef.current) {
            const rect = cellRef.current.getBoundingClientRect()
            setTooltipAbove(rect.top > 150)
          }
          setShowTooltip(true)
        }} onMouseLeave={() => setShowTooltip(false)}>
          {hasGossip ? (
            <Link to={`/solana/gossip-nodes/${node.gossip_pubkey}`} className="text-sm font-medium hover:text-accent transition-colors">
              {node.location}
            </Link>
          ) : (
            <div className="text-sm font-medium">{node.location}</div>
          )}
          <div className="text-xs text-muted-foreground">{node.metro_name}</div>
          {node.stake_sol > 0 && <div className="text-xs text-muted-foreground">{formatStake(node.stake_sol)} staked</div>}
          {showTooltip && (node.gossip_ip || node.asn_org) && (
            <div className={cn("absolute left-0 z-20 bg-popover border border-border rounded-lg shadow-lg p-3 text-xs whitespace-nowrap", tooltipAbove ? "bottom-full mb-1" : "top-full mt-1")}>
              {node.gossip_ip && (
                <div className="flex gap-2"><span className="text-muted-foreground">IP</span><span className="font-mono">{node.gossip_ip}</span></div>
              )}
              {node.asn_org && (
                <div className="flex gap-2"><span className="text-muted-foreground">Host</span><span>{node.asn_org}</span></div>
              )}
              {node.asn != null && node.asn > 0 && (
                <div className="flex gap-2"><span className="text-muted-foreground">ASN</span><span>AS{node.asn}</span></div>
              )}
              {node.city && (
                <div className="flex gap-2"><span className="text-muted-foreground">Location</span><span>{node.city}{node.country ? `, ${node.country}` : ''}</span></div>
              )}
              {hasGossip && (
                <div className="flex gap-2"><span className="text-muted-foreground">Pubkey</span><span className="font-mono">{node.gossip_pubkey!.slice(0, 8)}...{node.gossip_pubkey!.slice(-4)}</span></div>
              )}
            </div>
          )}
        </div>
      </td>
      <td className="px-4 py-3 text-right tabular-nums text-sm">{formatPct(completeness)}</td>
      <td className="px-4 py-3 text-right tabular-nums text-sm text-green-500">
        {dz ? formatPct(dz.win_rate_pct) : '—'}
      </td>
      {['jito', 'turbine'].map(f => {
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
