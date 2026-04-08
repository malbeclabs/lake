import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { Trophy, Loader2 } from 'lucide-react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
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
  dz_rebop: '#14b8a6',
  jito: '#3b82f6',
  turbine: '#f59e0b',
  provider_one: '#ef4444',
  pipe: '#a855f7',
}

const FEED_LABELS: Record<string, string> = {
  dz: 'Edge Leaders',
  dz_rebop: 'Edge Retransmits',
  jito: 'Jito Shredstream',
  turbine: 'Turbine',
  provider_one: 'Provider One',
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

function WinRateBar({
  node,
  feeds,
  data,
}: {
  node: EdgeScoreboardNode
  feeds: string[]
  data: Record<string, string | number>
}) {
  const [hoveredFeed, setHoveredFeed] = useState<string | null>(null)
  const [mousePos, setMousePos] = useState<{ x: number; y: number } | null>(null)

  return (
    <div
      className="relative flex-1 h-14"
      onMouseLeave={() => { setMousePos(null); setHoveredFeed(null) }}
      onMouseMove={(e) => setMousePos({ x: e.clientX, y: e.clientY })}
    >
      <div className="relative flex h-full rounded overflow-hidden">
        {feeds.map((f) => {
          const pct = Number(data[f] ?? 0)
          if (pct < 0.1) return null
          const raw = Number(data[`${f}_raw`] ?? 0)
          return (
            <div
              key={f}
              style={{ width: `${pct}%`, backgroundColor: FEED_COLORS[f] ?? '#6b7280', minWidth: 0 }}
              className="relative flex items-center justify-center overflow-hidden"
              onMouseEnter={() => setHoveredFeed(f)}
            >
              {(f === 'dz' || f === 'dz_rebop') && raw >= 2 && (
                <span className="text-white text-xs font-semibold whitespace-nowrap select-none">
                  {raw.toFixed(1)}%
                </span>
              )}
              {hoveredFeed === f && (
                <div className="absolute inset-0 bg-white/15 ring-1 ring-inset ring-white/40 pointer-events-none" />
              )}
            </div>
          )
        })}
      </div>
      {mousePos && (
        <div
          className="fixed z-20 bg-[#1a1a2e] border border-[#333] rounded-md px-3 py-2 text-xs shadow-lg pointer-events-none"
          style={{ left: mousePos.x + 10, top: mousePos.y - 60 }}
        >
          <div className="text-[#e5e5e5] font-medium mb-1.5">{node.location}</div>
          <table className="border-spacing-0">
            <thead>
              <tr className="text-[#777]">
                <th className="pr-3 py-0.5 text-left font-normal">Feed</th>
                <th className="pr-3 py-0.5 text-right font-normal">Win %</th>
                <th className="py-0.5 text-right font-normal">Shreds</th>
              </tr>
            </thead>
            <tbody>
              {feeds.map((f) => {
                const raw = Number(data[`${f}_raw`] ?? 0)
                const shreds = Number(data[`${f}_shreds`] ?? 0)
                return (
                  <tr key={f}>
                    <td className="pr-3 py-0.5 font-medium" style={{ color: FEED_COLORS[f] ?? '#6b7280' }}>
                      {FEED_LABELS[f] ?? f}
                    </td>
                    <td className="pr-3 py-0.5 text-right font-mono text-[#e5e5e5]">{raw.toFixed(1)}%</td>
                    <td className="py-0.5 text-right font-mono text-[#999]">{shreds.toLocaleString()}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

/** Height per node row shared between Win Rate and Recent Slots charts. */
const NODE_ROW_HEIGHT = 72

function NodeLabel({ node, label }: { node: EdgeScoreboardNode; label: string }) {
  const [show, setShow] = useState(false)
  const hasGossip = !!node.gossip_pubkey

  return (
    <div
      className="relative w-14 shrink-0 text-xs text-muted-foreground text-right pr-2 cursor-pointer"
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      {hasGossip ? (
        <Link to={`/solana/gossip-nodes/${node.gossip_pubkey}`} className="hover:text-accent transition-colors">
          {label}
        </Link>
      ) : (
        label
      )}
      {show && (node.name || node.gossip_ip || node.asn_org) && (
        <div className="absolute left-full top-1/2 -translate-y-1/2 ml-2 z-20 bg-popover border border-border rounded-lg shadow-lg p-3 text-xs whitespace-nowrap text-left text-foreground">
          {node.name && (
            <div className="font-medium mb-1">{node.name}</div>
          )}
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

// nodeDisplayLabel returns a disambiguated label for a node. When multiple nodes
// share the same metro location (e.g. "ams-mn-bm1" and "ams-mn-bm2" both map to "AMS"),
// appends the trailing index from the host name so the UI shows "AMS-1" / "AMS-2".
function nodeDisplayLabel(node: EdgeScoreboardNode, nodes: EdgeScoreboardNode[]): string {
  const hasDuplicate = nodes.some(n => n.host !== node.host && n.location === node.location)
  if (!hasDuplicate) return node.location
  const suffix = node.host.split('-').pop()?.match(/\d+$/)?.[0]
  return suffix ? `${node.location}-${suffix}` : node.host
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
      .sort((a, b) => a.host.localeCompare(b.host))
      .map((n) => {
        const row: Record<string, string | number> = { location: n.location }
        const rawSum = feeds.reduce((s, f) => s + (n.feeds[f]?.win_rate_pct ?? 0), 0)
        const scale = rawSum > 0 ? 100 / rawSum : 0
        for (const f of feeds) {
          row[f] = Math.round(((n.feeds[f]?.win_rate_pct ?? 0) * scale) * 10) / 10
          row[`${f}_shreds`] = n.feeds[f]?.shreds_won ?? 0
          row[`${f}_raw`] = n.feeds[f]?.win_rate_pct ?? 0
        }
        return { node: n, data: row }
      })

    return { nodeRows, feeds }
  }, [nodes])

  if (chartData.nodeRows.length === 0) return null

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="mb-4">
        <h3 className="text-sm font-medium">Win Rate by Node</h3>
        <div className="flex items-center justify-end gap-3 mt-1">
          {chartData.feeds.map((f) => (
            <div key={f} className="flex items-center gap-1 text-[10px] text-muted-foreground">
              <span className="inline-block w-2 h-2 rounded-sm shrink-0" style={{ backgroundColor: FEED_COLORS[f] ?? '#6b7280' }} />
              {FEED_LABELS[f] ?? f}
            </div>
          ))}
        </div>
      </div>
      {chartData.nodeRows.map((nr) => (
        <div key={nr.node.host} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
          <NodeLabel node={nr.node} label={nodeDisplayLabel(nr.node, nodes)} />
          <WinRateBar node={nr.node} feeds={chartData.feeds} data={nr.data} />
        </div>
      ))}
      <div className="flex items-center" style={{ paddingLeft: 48 }}>
        <div className="flex-1 flex justify-between pr-6 text-xs text-muted-foreground">
          <span>0%</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span>
        </div>
      </div>
    </div>
  )
}

function SlotRaceNodeChart({
  slotData,
  feeds,
  slotLeaders,
}: {
  slotData: Array<Record<string, number>>
  feeds: string[]
  slotLeaders?: Record<string, EdgeScoreboardLeader>
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const slotDataRef = useRef(slotData)
  slotDataRef.current = slotData
  const slotLeadersRef = useRef(slotLeaders)
  slotLeadersRef.current = slotLeaders
  const setHoverRef = useRef<((idx: number | null, vx: number, vy: number) => void) | null>(null)
  const hoveredIdxRef = useRef<number | null>(null)

  const [hover, setHover] = useState<{ idx: number; vx: number; vy: number } | null>(null)
  setHoverRef.current = (idx, vx, vy) => setHover(idx == null ? null : { idx, vx, vy })

  useEffect(() => {
    if (!containerRef.current || !slotData.length) return

    const n = slotData.length
    const height = NODE_ROW_HEIGHT - 4

    const xData = Float64Array.from({ length: n }, (_, i) => i)
    const yDummy = new Float64Array(n)
    const uData: uPlot.AlignedData = [xData, yDummy]

    const opts: uPlot.Options = {
      width: containerRef.current.offsetWidth,
      height,
      series: [{}, { show: false }],
      scales: {
        x: { time: false, range: () => [-0.5, n - 0.5] },
        y: { range: () => [0, 100] },
      },
      axes: [{ show: false }, { show: false }],
      padding: [2, 2, 2, 2],
      cursor: { points: { show: false } },
      legend: { show: false },
      hooks: {
        draw: [
          (u) => {
            const ctx = u.ctx
            ctx.save()
            const cumulative = new Float64Array(n)
            for (const feed of feeds) {
              ctx.fillStyle = FEED_COLORS[feed] ?? '#6b7280'
              for (let i = 0; i < n; i++) {
                const val = slotDataRef.current[i][feed] ?? 0
                if (!val) continue
                const prev = cumulative[i]
                const x1 = Math.round(u.valToPos(i - 0.4, 'x', true))
                const x2 = Math.round(u.valToPos(i + 0.4, 'x', true))
                const y1 = Math.round(u.valToPos(prev + val, 'y', true))
                const y2 = Math.round(u.valToPos(prev, 'y', true))
                if (y2 > y1 && x2 > x1) ctx.fillRect(x1, y1, x2 - x1, y2 - y1)
                cumulative[i] += val
              }
            }
            // Highlight hovered column
            const hIdx = hoveredIdxRef.current
            if (hIdx != null && hIdx >= 0 && hIdx < n) {
              const x1 = Math.round(u.valToPos(hIdx - 0.4, 'x', true))
              const x2 = Math.round(u.valToPos(hIdx + 0.4, 'x', true))
              const y1 = Math.round(u.valToPos(100, 'y', true))
              const y2 = Math.round(u.valToPos(0, 'y', true))
              const w = x2 - x1
              const h = y2 - y1
              ctx.fillStyle = 'rgba(255, 255, 255, 0.12)'
              ctx.fillRect(x1, y1, w, h)
              ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)'
              ctx.lineWidth = 1
              ctx.strokeRect(x1 + 0.5, y1 + 0.5, w - 1, h - 1)
            }
            ctx.restore()
          },
        ],
        setCursor: [
          (u) => {
            const idx = u.cursor.idx
            if (idx == null || idx < 0 || idx >= slotDataRef.current.length) {
              hoveredIdxRef.current = null
              u.redraw(false)
              setHoverRef.current?.(null, 0, 0)
              return
            }
            hoveredIdxRef.current = idx
            u.redraw(false)
            const rect = u.over.getBoundingClientRect()
            const vx = rect.left + (u.cursor.left ?? 0)
            const vy = rect.top + (u.cursor.top ?? 0)
            setHoverRef.current?.(idx, vx, vy)
          },
        ],
      },
    }

    plotRef.current?.destroy()
    plotRef.current = new uPlot(opts, uData, containerRef.current)

    const ro = new ResizeObserver((entries) => {
      if (plotRef.current) plotRef.current.setSize({ width: entries[0].contentRect.width, height })
    })
    ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [slotData, feeds])

  const hoveredSlot = hover != null ? slotData[hover.idx] : null
  const hoveredLeader = hoveredSlot ? slotLeadersRef.current?.[String(hoveredSlot['slot'])] : undefined
  const xPos = hover != null && hover.vx + 10 + 180 > window.innerWidth ? hover.vx - 190 : (hover?.vx ?? 0) + 10

  return (
    <div className="relative flex-1 h-full min-w-0 overflow-hidden">
      <div ref={containerRef} />
      {hover && hoveredSlot && (
        <div
          className="fixed z-50 bg-[#1a1a2e] border border-[#333] rounded-md px-3 py-2 text-xs shadow-lg pointer-events-none"
          style={{ left: xPos, top: Math.max(0, hover.vy - 60) }}
        >
          <div className="font-mono font-semibold text-[#e5e5e5] mb-1.5">
            Slot {Number(hoveredSlot['slot']).toLocaleString()}
          </div>
          {hoveredLeader && (
            <div className="mb-1.5 pb-1.5 border-b border-[#333] text-[#999]">
              {hoveredLeader.name && <div className="text-[#e5e5e5]">{hoveredLeader.name}</div>}
              <div className="font-mono text-[#aaa]">{hoveredLeader.pubkey.slice(0, 8)}...{hoveredLeader.pubkey.slice(-4)}</div>
              {hoveredLeader.ip && <div><span className="text-[#666]">IP </span><span className="font-mono">{hoveredLeader.ip}</span></div>}
              {hoveredLeader.asn_org && <div><span className="text-[#666]">Host </span>{hoveredLeader.asn_org}</div>}
              {hoveredLeader.city && <div><span className="text-[#666]">Loc </span>{hoveredLeader.city}{hoveredLeader.country ? `, ${hoveredLeader.country}` : ''}</div>}
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
              {feeds.map((f) => (
                <tr key={f}>
                  <td className="pr-3 py-0.5 font-semibold" style={{ color: FEED_COLORS[f] ?? '#6b7280' }}>
                    {FEED_LABELS[f] ?? f}
                  </td>
                  <td className="py-0.5 text-right font-mono text-[#e5e5e5]">
                    {(hoveredSlot[f] ?? 0).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function RecentSlotsChart({
  slots,
  nodes,
  slotLeaders,
}: {
  slots: EdgeScoreboardSlotRace[]
  nodes: EdgeScoreboardNode[]
  slotLeaders?: Record<string, EdgeScoreboardLeader>
}) {
  const chartData = useMemo(() => {
    if (!slots.length || !nodes.length) return { nodeCharts: [], feeds: [] as string[], slotCount: 0 }

    const validNodeIds = new Set(nodes.map((n) => n.host))
    const filtered = slots.filter((s) => validNodeIds.has(s.host))

    const feedSet = new Set<string>()
    for (const s of filtered) feedSet.add(s.feed)
    const feeds = [...feedSet].sort((a, b) => (a === 'dz' ? -1 : b === 'dz' ? 1 : a.localeCompare(b)))

    const byNode = new Map<string, Map<number, Record<string, number>>>()
    for (const s of filtered) {
      let nodeMap = byNode.get(s.host)
      if (!nodeMap) {
        nodeMap = new Map()
        byNode.set(s.host, nodeMap)
      }
      let row = nodeMap.get(s.slot)
      if (!row) {
        row = {}
        nodeMap.set(s.slot, row)
      }
      row[s.feed] = s.win_pct
    }

    const slotNumbers = [...new Set(filtered.map((s) => s.slot))].sort((a, b) => a - b)
    const sortedNodes = [...nodes].sort((a, b) => a.host.localeCompare(b.host))

    const nodeCharts = sortedNodes
      .filter((n) => byNode.has(n.host))
      .map((n) => {
        const slotMap = byNode.get(n.host)!
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

  if (!slots.length)
    return (
      <div className="rounded-lg border border-border bg-card p-4">
        <h3 className="text-sm font-medium mb-4">Recent Edge Leader Slots — Win Rate per Slot</h3>
        <div className="text-sm text-muted-foreground text-center py-12">No recent slot data available.</div>
      </div>
    )

  const { nodeCharts, feeds, slotCount } = chartData

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="mb-4">
        <h3 className="text-sm font-medium">Recent Edge Leader Slots — Win Rate per Slot</h3>
        <div className="flex items-center justify-end gap-3 mt-1">
          {feeds.map((f) => (
            <div key={f} className="flex items-center gap-1 text-[10px] text-muted-foreground">
              <span className="inline-block w-2 h-2 rounded-sm shrink-0" style={{ backgroundColor: FEED_COLORS[f] ?? '#6b7280' }} />
              {FEED_LABELS[f] ?? f}
            </div>
          ))}
        </div>
      </div>
      {nodeCharts.map((nc) => (
        <div key={nc.node.host} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
          <NodeLabel node={nc.node} label={nodeDisplayLabel(nc.node, nodes)} />
          <SlotRaceNodeChart slotData={nc.data} feeds={feeds} slotLeaders={slotLeaders} />
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
      interactive: false,
    })

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

  const leadersOnly = searchParams.get('leaders_only') === 'true'

  const [showLoader, setShowLoader] = useState(false)
  const [showShimmer, setShowShimmer] = useState(false)
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 5_000)
    return () => clearInterval(id)
  }, [])

  const showTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const hideTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['edge-scoreboard', leadersOnly],
    queryFn: () => fetchEdgeScoreboard('1h', leadersOnly),
    refetchInterval: 30_000,
    staleTime: 15_000,
    placeholderData: keepPreviousData,
  })

  const freshness = useMemo(() => {
    if (!data?.generated_at) return null
    const ageSec = Math.round((now - new Date(data.generated_at).getTime()) / 1000)
    if (ageSec < 5) return 'just now'
    if (ageSec < 60) return `${ageSec}s ago`
    return `${Math.round(ageSec / 60)}m ago`
  }, [data?.generated_at, now])

  const slotDuration = useMemo(() => {
    if (!data?.global_total_slots) return null
    const m = Math.round(data.global_total_slots * 0.4 / 60)
    return m < 1 ? '~<1m' : `~${m}m`
  }, [data?.global_total_slots])

  const setLeadersOnly = (v: boolean) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (v) p.set('leaders_only', 'true')
      else p.delete('leaders_only')
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
    const competitors = ['jito', 'turbine', 'provider_one'] as const
    const weightedP50: Record<string, number> = {}
    const weightedP95: Record<string, number> = {}
    const competitorSlots: Record<string, number> = {}
    for (const c of competitors) {
      weightedP50[c] = 0
      weightedP95[c] = 0
      competitorSlots[c] = 0
    }

    for (const node of data.nodes) {
      // Normalize per-node win rates to sum to 100% (same as the bar chart),
      // then take the combined DZ fraction. This avoids inflating the average
      // when dz_rebop has disproportionately large raw values.
      const rawSum = Object.values(node.feeds).reduce((s, f) => s + (f?.win_rate_pct ?? 0), 0)
      const scale = rawSum > 0 ? 100 / rawSum : 0
      const dzPct = ((node.feeds['dz']?.win_rate_pct ?? 0) + (node.feeds['dz_rebop']?.win_rate_pct ?? 0)) * scale
      dzShredsWon += dzPct
      dzTotalShreds++
      totalSlots += node.slots_observed

      const dz = node.feeds['dz']
      if (dz?.lead_times) {
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
      winRate: dzTotalShreds > 0 ? dzShredsWon / dzTotalShreds : 0,
      leads,
      totalSlots,
      avgCompleteness: data.completeness_pct,
    }
  }, [data?.nodes])

  // Sort nodes by stake weight descending
  const sortedNodes = useMemo(() => {
    if (!data?.nodes) return []
    return [...data.nodes].sort((a, b) => a.host.localeCompare(b.host))
  }, [data?.nodes])

  useEffect(() => {
    if (!isLoading) {
      setShowLoader(false)
      return
    }
    const t = setTimeout(() => setShowLoader(true), 200)
    return () => clearTimeout(t)
  }, [isLoading])

  // Show shimmer when switching views, but only if data takes >200ms to arrive.
  // Fast cache hits skip the shimmer entirely.
  useEffect(() => {
    showTimerRef.current = setTimeout(() => {
      showTimerRef.current = null
      setShowShimmer(true)
      hideTimerRef.current = setTimeout(() => {
        hideTimerRef.current = null
        setShowShimmer(false)
      }, 1500)
    }, 200)
    return () => {
      if (showTimerRef.current) { clearTimeout(showTimerRef.current); showTimerRef.current = null }
      if (hideTimerRef.current) { clearTimeout(hideTimerRef.current); hideTimerRef.current = null }
      setShowShimmer(false)
    }
  }, [leadersOnly, window])

  // Cancel the debounce if data arrives before the 200ms threshold.
  useEffect(() => {
    if (showTimerRef.current) {
      clearTimeout(showTimerRef.current)
      showTimerRef.current = null
    }
  }, [data])

  if (isLoading && showLoader && !data) return (
    <div className="flex-1 flex items-center justify-center bg-background">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
    </div>
  )

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
      <div className="w-full px-4 sm:px-8 py-8">
        <PageHeader
          icon={Trophy}
          title="Edge Scoreboard"
          subtitle={
            data && freshness ? (
              <span className="text-sm text-muted-foreground">
                {data.global_total_slots.toLocaleString()} slots ({slotDuration}) · updated {freshness}
              </span>
            ) : undefined
          }
          actions={
            <div className="flex items-center gap-3">
              <div className="flex items-center rounded-md border border-border text-sm">
                {([
                  [false, 'All Slots'] as const,
                  [true, 'Edge Leaders'] as const,
                ]).map(([v, label]) => (
                  <button
                    key={String(v)}
                    type="button"
                    onClick={() => setLeadersOnly(v)}
                    className={cn(
                      'px-3 py-1.5 transition-colors',
                      leadersOnly === v
                        ? 'bg-primary text-primary-foreground'
                        : 'hover:bg-muted'
                    )}
                  >
                    {label}
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

        {/* Loading shimmer */}
        <div className="h-0.5 w-full overflow-hidden rounded-full mb-4">
          {showShimmer && (
            <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
          )}
        </div>

        {/* Epoch progress */}
        {data && data.current_epoch > 0 && (
          <EpochProgress epoch={data.current_epoch} slot={data.current_slot} />
        )}

        {/* Summary cards */}
        {globalStats && (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-8">
            <SummaryCard label="Edge Leaders Completeness" value={formatPct(globalStats.avgCompleteness)} />
            <SummaryCard label="Edge Win Rate" value={formatPct(globalStats.winRate)} />
            <SummaryCard label="Slots Observed" value={formatNumber(globalStats.totalSlots)} sub={`${formatNumber(data?.nodes?.length ? Math.round(globalStats.totalSlots / data.nodes.length) : 0)} avg/host`} />
            {Object.entries(globalStats.leads).map(([competitor, lead]) => (
              <SummaryCard
                key={competitor}
                label={`vs ${FEED_LABELS[competitor] ?? competitor}`}
                value={formatMs(lead.p50)}
                sub={`p95: ${formatMs(lead.p95)}`}
              />
            ))}
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
                    <NodeRow key={node.host} node={node} label={nodeDisplayLabel(node, data?.nodes ?? [])} totalDZLeaderSlots={data?.total_dz_leader_slots ?? 0} />
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

function NodeRow({ node, label, totalDZLeaderSlots }: { node: EdgeScoreboardNode; label: string; totalDZLeaderSlots: number }) {
  const [showTooltip, setShowTooltip] = useState(false)
  const cellRef = useRef<HTMLDivElement>(null)
  const [tooltipAbove, setTooltipAbove] = useState(true)
  const dz = node.feeds['dz']
  const completeness = totalDZLeaderSlots > 0 ? (node.dz_leader_slots / totalDZLeaderSlots) * 100 : 0

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
              {label}
            </Link>
          ) : (
            <div className="text-sm font-medium">{label}</div>
          )}
          <div className="text-xs text-muted-foreground">{node.metro_name}</div>
          {node.stake_sol > 0 && <div className="text-xs text-muted-foreground">{formatStake(node.stake_sol)} staked</div>}
          {showTooltip && (node.name || node.gossip_ip || node.asn_org) && (
            <div className={cn("absolute left-0 z-20 bg-popover border border-border rounded-lg shadow-lg p-3 text-xs whitespace-nowrap", tooltipAbove ? "bottom-full mb-1" : "top-full mt-1")}>
              {node.name && (
                <div className="font-medium mb-1">{node.name}</div>
              )}
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
