import { useState, useMemo, useRef, useEffect, useLayoutEffect, useCallback } from 'react'
import { useDrag } from '@use-gesture/react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { Trophy, Loader2, ChevronsRight, Play, Square } from 'lucide-react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'

import {
  fetchEdgeScoreboard,
  type EdgeScoreboardNode,
  type EdgeScoreboardSlotRace,
  type EdgeScoreboardSlotBucket,
  type EdgeScoreboardLeader,
} from '@/lib/api'
import { cn } from '@/lib/utils'
import { PageHeader } from './page-header'
import { Section } from './traffic-dashboard/section'

const VALID_WINDOWS = ['1h', '24h', '3d', '7d'] as const
type TimeWindow = (typeof VALID_WINDOWS)[number]

function isValidWindow(v: string | null): v is TimeWindow {
  return v !== null && (VALID_WINDOWS as readonly string[]).includes(v)
}

function formatPct(v: number): string {
  return v >= 100 ? '100%' : `${v.toFixed(1)}%`
}

function formatMs(v: number): string {
  if (v < 0.1) return '<0.1ms'
  if (v >= 1000) return `${(v / 1000).toFixed(1)}s`
  return `${v.toFixed(1)}ms`
}


function windowLabel(w: TimeWindow): string {
  const labels: Record<TimeWindow, string> = {
    '1h': 'past 1 hour',
    '24h': 'past 24 hours',
    '3d': 'past 3 days',
    '7d': 'past 7 days',
  }
  return labels[w] ?? w
}

function formatStake(sol: number): string {
  if (sol >= 1_000_000) return `${(sol / 1_000_000).toFixed(1)}M SOL`
  if (sol >= 1_000) return `${(sol / 1_000).toFixed(0)}K SOL`
  return `${sol.toFixed(0)} SOL`
}




function WinRateGauge({ value }: { value: number }) {
  const size = 160
  const r = 65
  const cx = size / 2
  const cy = size / 2
  const circ = 2 * Math.PI * r
  const arc = circ * 0.75
  const gap = circ - arc
  const fill = arc * (Math.min(100, Math.max(0, value)) / 100)
  return (
    <div className="relative flex items-center justify-center shrink-0" style={{ width: size, height: size }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="absolute inset-0">
        <circle cx={cx} cy={cy} r={r} fill="none" strokeWidth={4} stroke="currentColor" className="text-muted-foreground/25" strokeDasharray={`${arc} ${gap}`} strokeLinecap="round" transform={`rotate(-225, ${cx}, ${cy})`} />
        <circle cx={cx} cy={cy} r={r} fill="none" strokeWidth={4} stroke="#10b981" strokeDasharray={`${fill} ${circ - fill}`} strokeLinecap="round" transform={`rotate(-225, ${cx}, ${cy})`} />
      </svg>
      <div className="flex flex-col items-center z-10">
        <div className="text-2xl font-semibold tabular-nums">{value.toFixed(1)}%</div>
        <div className="text-xs text-muted-foreground mt-0.5 text-center">DZ Edge<br/>Win Rate</div>
      </div>
    </div>
  )
}

const FEED_COLORS: Record<string, string> = {
  dz_edge: '#10b981',
  dz: '#10b981',
  dz_rebop: '#10b981',
  jito: '#374151',
  turbine: '#1f2937',
  pipe: '#e879f9',
  other: '#6b7280',
}

const FEED_LABELS: Record<string, string> = {
  dz_edge: 'DZ Edge',
  dz: 'DZ Edge Leaders',
  dz_rebop: 'DZ Edge Retransmits',
  jito: 'Jito Shredstream',
  turbine: 'Turbine',
  pipe: 'Pipe',
  other: 'Other',
}

// Feeds considered "DZ" for simplified view grouping.
const DZ_FEED_KEYS = new Set(['dz_edge', 'dz', 'dz_rebop'])

// Map a raw feed name to the key used in the chart data.
// In simplified mode, all DZ feeds collapse to 'dz_edge' and competitors to 'other'.
function feedKeyForMode(feed: string, granular: boolean): string | null {
  if (granular) return feed in FEED_COLORS ? feed : null
  if (DZ_FEED_KEYS.has(feed)) return 'dz_edge'
  if (feed in FEED_COLORS) return 'other'
  return null
}

// Priority for feed ordering in chart (lower = rendered first / bottom of stack).
function feedSortPriority(f: string): number {
  if (f === 'dz_edge') return 0
  if (f === 'dz') return 1
  if (f === 'dz_rebop') return 2
  if (f === 'other') return 10
  return 5
}


/** Height per node row in the Recent Slots chart. */
const NODE_ROW_HEIGHT = 36
const NODE_CHART_HEIGHT = 24

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
      {show && (
        <div className="absolute left-full top-1/2 -translate-y-1/2 ml-2 z-20 bg-popover border border-border rounded-lg shadow-lg p-3 text-xs whitespace-nowrap text-left text-foreground">
          {node.metro_name && (
            <div className="font-medium mb-1">{node.metro_name}</div>
          )}
          <div className="flex gap-2"><span className="text-muted-foreground">Host</span><span className="font-mono">{node.host}</span></div>
          {node.gossip_ip && (
            <div className="flex gap-2"><span className="text-muted-foreground">IP</span><span className="font-mono">{node.gossip_ip}</span></div>
          )}
          {node.asn_org && (
            <div className="flex gap-2"><span className="text-muted-foreground">Org</span><span>{node.asn_org}</span></div>
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


function BucketedNodeChart({ data, feeds, bucketSize }: { data: Array<Record<string, number | null>>; feeds: string[]; bucketSize?: number }) {
  const height = NODE_CHART_HEIGHT
  const [hover, setHover] = useState<{ idx: number; x: number; y: number } | null>(null)
  const n = data.length

  // viewBox coords: x = 0..n-1, y = 0..100 (0 = top = 100% win rate)
  const paths = useMemo(() => {
    if (!n) return []
    const cumulative = new Array(n).fill(0)

    return feeds.map(feed => {
      const color = FEED_COLORS[feed] ?? '#6b7280'
      type Seg = { i: number; top: number; bot: number }[]
      const segments: Seg[] = []
      let cur: Seg = []
      for (let i = 0; i < n; i++) {
        const val = data[i][feed] as number | null
        if (val != null && val > 0) {
          cur.push({ i, top: cumulative[i] + val, bot: cumulative[i] })
        } else {
          if (cur.length) { segments.push(cur); cur = [] }
        }
      }
      if (cur.length) segments.push(cur)
      for (let i = 0; i < n; i++) cumulative[i] += (data[i][feed] as number | null) ?? 0

      const svgPaths = segments.map(seg => {
        const top = seg.map(p => `${p.i},${(100 - p.top).toFixed(2)}`).join(' L')
        const bot = [...seg].reverse().map(p => `${p.i},${(100 - p.bot).toFixed(2)}`).join(' L')
        return `M${top} L${bot} Z`
      })
      const strokePaths = segments.map(seg =>
        'M' + seg.map(p => `${p.i},${(100 - p.top).toFixed(2)}`).join(' L')
      )
      return { feed, color, svgPaths, strokePaths }
    })
  }, [data, feeds, n])

  const hovered = hover != null ? data[hover.idx] : null
  const hoveredSlot = hovered?.slot as number | undefined
  const vbWidth = Math.max(1, n - 1)

  return (
    <div className="flex-1 min-w-0 relative rounded overflow-hidden">
      <svg
        width="100%"
        height={height}
        viewBox={`0 0 ${vbWidth} 100`}
        preserveAspectRatio="none"
        style={{ display: 'block' }}
        onMouseMove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect()
          const mx = e.clientX - rect.left
          const idx = Math.max(0, Math.min(n - 1, Math.round((mx / rect.width) * (n - 1))))
          setHover({ idx, x: e.clientX, y: e.clientY })
        }}
        onMouseLeave={() => setHover(null)}
      >
        {paths.map(({ feed, color, svgPaths, strokePaths }) => (
          <g key={feed}>
            {svgPaths.map((d, i) => <path key={i} d={d} fill={color} />)}
            {strokePaths.map((d, i) => <path key={i} d={d} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" vectorEffect="non-scaling-stroke" />)}
          </g>
        ))}
        {hover != null && (
          <line
            x1={hover.idx} x2={hover.idx}
            y1={0} y2={100}
            stroke="rgba(255,255,255,0.4)" strokeWidth={1} strokeDasharray="3 3" vectorEffect="non-scaling-stroke"
          />
        )}
      </svg>
      {hover && hovered && (
        <div
          className="fixed z-50 bg-[#1a1a2e] border border-[#333] rounded-md px-3 py-2 text-xs shadow-lg pointer-events-none"
          style={{ left: hover.x + 10, top: hover.y - 60 }}
        >
          {hoveredSlot != null && (
            <div className="font-mono font-semibold text-[#e5e5e5] mb-1.5">
              {bucketSize
                ? `Slots ${hoveredSlot.toLocaleString()} – ${(hoveredSlot + bucketSize - 1).toLocaleString()}`
                : `Slot ${hoveredSlot.toLocaleString()}`}
            </div>
          )}
          {[...feeds].reverse().map(f => {
            const val = hovered[f] as number | null
            return val != null ? (
              <div key={f} className="flex justify-between gap-4">
                <span style={{ color: FEED_COLORS[f] }}>{FEED_LABELS[f] ?? f}</span>
                <span className="text-[#e5e5e5] font-mono">{val.toFixed(1)}%</span>
              </div>
            ) : null
          })}
        </div>
      )}
    </div>
  )
}

type SlotHoverInfo = {
  slot: number
  leader?: EdgeScoreboardLeader
  feedData: Record<string, number | null>
  hoveredFeed?: string | null
}

// Module-level ref: only one chart instance can own hover at a time.
// When a chart gets a valid setCursor, it claims ownership. The scroll-restore
// effect only fires for the owner, so moving to another row clears the previous one.
let activeChartId: string | null = null

function SlotRaceNodeChart({
  slotData,
  feeds,
  slotLeaders,
  animated = true,
  dragging = false,
  liveScrollOffset = 0,
  viewSlotCount,
  onHover,
}: {
  slotData: Array<Record<string, number | null>>
  feeds: string[]
  slotLeaders?: Record<string, EdgeScoreboardLeader>
  animated?: boolean
  dragging?: boolean
  liveScrollOffset?: number
  viewSlotCount: number
  onHover?: (info: SlotHoverInfo | null) => void
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const slotDataRef = useRef(slotData)
  slotDataRef.current = slotData
  const slotLeadersRef = useRef(slotLeaders)
  slotLeadersRef.current = slotLeaders
  const feedsRef = useRef(feeds)
  feedsRef.current = feeds
  const setHoverRef = useRef<((idx: number | null, vx: number, vy: number) => void) | null>(null)
  const hoveredIdxRef = useRef<number | null>(null)
  const animOffsetRef = useRef(0)
  const rafRef = useRef<number | null>(null)
  const animatedRef = useRef(animated)
  animatedRef.current = animated
  const draggingRef = useRef(dragging)
  draggingRef.current = dragging
  const prevRightSlotRef = useRef<number | null>(null)
  // Track cursor position so we can recompute the hovered idx as the chart scrolls
  // under a stationary mouse (translateX moves the canvas, uPlot doesn't re-fire setCursor).
  const liveScrollOffsetRef = useRef(liveScrollOffset)
  liveScrollOffsetRef.current = liveScrollOffset
  const lastHoverVxRef = useRef<number | null>(null)
  const lastHoverVyRef = useRef<number | null>(null)
  // Stable id for this chart instance — used to claim/check activeChartId ownership.
  const chartIdRef = useRef(`chart-${Math.random()}`)
  const lastNotifiedSlotRef = useRef<number | null>(null)
  const lastNotifiedFeedRef = useRef<string | null | undefined>(undefined)

  const onHoverRef = useRef(onHover)
  onHoverRef.current = onHover

  // Coalesce notifyHover calls within a single rAF frame so competing paths
  // (setCursor and scroll-restore) don't race each other with different indices.
  // Only the last scheduled index per frame reaches updateInfoBar.
  const pendingNotifyIdxRef = useRef<number | null>(null)
  const notifyRafRef = useRef<number | null>(null)
  const notifyHover = (idx: number) => {
    pendingNotifyIdxRef.current = idx
    if (notifyRafRef.current !== null) return  // already scheduled; last write wins
    notifyRafRef.current = requestAnimationFrame(() => {
      notifyRafRef.current = null
      const pendingIdx = pendingNotifyIdxRef.current
      pendingNotifyIdxRef.current = null
      if (pendingIdx === null) return
      const slot = slotDataRef.current[pendingIdx]
      if (!slot) return
      const slotNum = Number(slot['slot'])
      const leader = slotLeadersRef.current?.[String(slot['slot'])]
      const feedData: Record<string, number | null> = {}
      for (const key of Object.keys(slot)) {
        if (key !== 'slot') feedData[key] = slot[key] as number | null
      }

      // Compute which feed segment is under the cursor from Y position
      let hoveredFeed: string | null = null
      const plot = plotRef.current
      if (plot && lastHoverVyRef.current !== null) {
        const rect = plot.over.getBoundingClientRect()
        const canvasY = lastHoverVyRef.current - rect.top
        if (canvasY >= 0 && canvasY <= rect.height) {
          const yVal = plot.posToVal(canvasY, 'y')
          let cumulative = 0
          for (const f of feedsRef.current) {
            const val = (slot[f] as number | null) ?? 0
            cumulative += val
            if (yVal <= cumulative) {
              hoveredFeed = f
              break
            }
          }
        }
      }

      // Skip if both slot and hovered feed are unchanged
      if (slotNum === lastNotifiedSlotRef.current && hoveredFeed === lastNotifiedFeedRef.current) return
      lastNotifiedSlotRef.current = slotNum
      lastNotifiedFeedRef.current = hoveredFeed

      onHoverRef.current?.({ slot: slotNum, leader, feedData, hoveredFeed })
    })
  }

  setHoverRef.current = (idx, vx, vy) => {
    if (idx == null) {
      // Don't clear here — phantom mouseleave events resolve immediately.
    } else {
      activeChartId = chartIdRef.current  // claim ownership
      lastHoverVxRef.current = vx
      lastHoverVyRef.current = vy
      hoveredIdxRef.current = idx
      notifyHover(idx)
    }
  }

  // Re-initialize uPlot when feeds or viewSlotCount change. The scale is fixed to viewSlotCount
  // slots — draw hook reads slotDataRef.current directly and handles any count.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (!containerRef.current || !slotData.length) return

    const n = viewSlotCount
    const height = NODE_CHART_HEIGHT

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
      padding: [0, 0, 0, 0],
      cursor: { points: { show: false }, x: false, y: false },
      select: { show: false, left: 0, top: 0, width: 0, height: 0 },
      legend: { show: false },
      hooks: {
        draw: [
          (u) => {
            const ctx = u.ctx
            ctx.save()
            // uPlot may set globalAlpha < 1 for series focus/dim — reset to fully opaque.
            ctx.globalAlpha = 1
            // Clip to the plot area so animated bars don't overflow
            ctx.beginPath()
            ctx.rect(u.bbox.left, u.bbox.top, u.bbox.width, u.bbox.height)
            ctx.clip()
            // Apply slide-in offset during data refresh animation
            ctx.translate(animOffsetRef.current, 0)
            const currentData = slotDataRef.current
            const currentN = currentData.length
            const cumulative = new Float64Array(currentN)

            {
              // Stacked bar chart for individual slot mode
              for (const feed of feeds) {
                ctx.fillStyle = FEED_COLORS[feed] ?? '#6b7280'
                for (let i = 0; i < currentN; i++) {
                  const val = currentData[i][feed] ?? 0
                  if (!val) continue
                  const prev = cumulative[i]
                  const x1 = Math.floor(u.valToPos(i - 0.5, 'x', true))
                  const x2 = Math.ceil(u.valToPos(i + 0.5, 'x', true))
                  const y1 = Math.floor(u.valToPos(prev + val, 'y', true))
                  const y2 = Math.ceil(u.valToPos(prev, 'y', true))
                  if (y2 > y1 && x2 > x1) ctx.fillRect(x1, y1, x2 - x1, y2 - y1)
                  cumulative[i] += val
                }
              }
              // Highlight hovered column (skip during drag — position is misleading while translating)
              const hIdx = hoveredIdxRef.current
              if (!draggingRef.current && hIdx != null && hIdx >= 0 && hIdx < currentN) {
                const x1 = Math.floor(u.valToPos(hIdx - 0.5, 'x', true))
                const x2 = Math.ceil(u.valToPos(hIdx + 0.5, 'x', true))
                const y1 = Math.floor(u.valToPos(100, 'y', true))
                const y2 = Math.ceil(u.valToPos(0, 'y', true))
                const w = x2 - x1
                const h = y2 - y1
                ctx.fillStyle = 'rgba(255, 255, 255, 0.12)'
                ctx.fillRect(x1, y1, w, h)
                ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)'
                ctx.lineWidth = 1
                ctx.strokeRect(x1 + 0.5, y1 + 0.5, w - 1, h - 1)
              }
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
            setHoverRef.current?.(idx, 0, 0)
          },
        ],
      },
    }

    plotRef.current?.destroy()
    plotRef.current = new uPlot(opts, uData, containerRef.current)

    const onOverMove = (e: MouseEvent) => {
      lastHoverVxRef.current = e.clientX
      lastHoverVyRef.current = e.clientY
    }
    plotRef.current.over.addEventListener('mousemove', onOverMove)

    // Don't set borderRadius on the canvas — the parent div's overflow-hidden+rounded
    // already clips the corners, and canvas borderRadius causes GPU compositing seams.

    const ro = new ResizeObserver((entries) => {
      if (plotRef.current) plotRef.current.setSize({ width: entries[0].contentRect.width, height })
    })
    ro.observe(containerRef.current)

    return () => {
      ro.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [feeds, viewSlotCount])

  // Animate bars sliding in from the right on data refresh.
  // useLayoutEffect so the canvas redraws synchronously with the DOM before the browser paints.
  // Without this, translateX (from scrollOffset state) updates in one frame and the canvas
  // content updates in the next (useEffect fires after paint), causing a one-frame jitter that
  // is especially visible with narrow bars (high slot counts like 200 or 300).
  useLayoutEffect(() => {
    const plot = plotRef.current
    if (!plot || !slotData.length) return

    if (rafRef.current != null) cancelAnimationFrame(rafRef.current)

    // Only animate when the rightmost slot actually changes (a real new slot arrived).
    const rightSlot = slotData.at(-1)?.['slot'] as number | undefined ?? null
    const rightSlotChanged = rightSlot !== prevRightSlotRef.current
    prevRightSlotRef.current = rightSlot

    // Skip canvas slide animation during drag (the outer translateX handles position)
    // and in live tailing mode (the drain translateX handles movement).
    if (!animatedRef.current || !rightSlotChanged || draggingRef.current) {
      animOffsetRef.current = 0
      plot.redraw(false)
      return
    }

    // Slide-in offset: use one slot-width, but cap at 4px so bucketed mode
    // (fewer, wider buckets) doesn't animate a large gap on the left.
    const slotPx = Math.min(plot.valToPos(1, 'x', true) - plot.valToPos(0, 'x', true), 4)
    const duration = 350
    const startTime = performance.now()
    animOffsetRef.current = slotPx

    const tick = (now: number) => {
      const t = Math.min((now - startTime) / duration, 1)
      const eased = 1 - (1 - t) ** 3  // cubic ease-out
      animOffsetRef.current = slotPx * (1 - eased)
      plot.redraw(false)
      if (t < 1) {
        rafRef.current = requestAnimationFrame(tick)
      } else {
        animOffsetRef.current = 0
        rafRef.current = null
      }
    }
    rafRef.current = requestAnimationFrame(tick)

    return () => {
      if (rafRef.current != null) {
        cancelAnimationFrame(rafRef.current)
        rafRef.current = null
      }
    }
  }, [slotData])

  // When the chart scrolls (translateX shifts the canvas left), a stationary mouse effectively
  // moves right in canvas coordinates. Recompute the hovered idx from the current screen position
  // of u.over so the tooltip tracks the bar that is visually under the cursor.
  useEffect(() => {
    const plot = plotRef.current
    if (!plot || lastHoverVxRef.current === null) return
    if (activeChartId !== chartIdRef.current) {
      if (hoveredIdxRef.current !== null) {
        hoveredIdxRef.current = null
        plot.redraw(false)
      }
      lastHoverVxRef.current = null
      return
    }
    // Compute canvas-relative x directly from clientX + live bounding rect.
    // This is always exact regardless of drain events or scroll delta accumulation.
    const rect = plot.over.getBoundingClientRect()
    const canvasX = lastHoverVxRef.current - rect.left
    if (canvasX < 0 || canvasX > rect.width) return
    const xVal = plot.posToVal(canvasX, 'x')
    const idx = Math.round(xVal)
    if (idx < 0 || idx >= slotDataRef.current.length) return
    // Only redraw when the bar index actually changes — no floating-point oscillation possible.
    if (idx === hoveredIdxRef.current) return
    hoveredIdxRef.current = idx
    plot.redraw(false)
    notifyHover(idx)
  }, [liveScrollOffset])  // eslint-disable-line react-hooks/exhaustive-deps

  // Clear hover when mouse moves outside this chart's container.
  // Note: do NOT guard on activeChartId here — when moving row-to-row, the new row
  // claims activeChartId before this handler fires, causing the old row's highlight
  // to get stuck. Always clear if mouse is genuinely outside this chart.
  useEffect(() => {
    const onDocMove = (e: MouseEvent) => {
      if (!containerRef.current) return
      if (!containerRef.current.contains(e.target as Node)) {
        if (activeChartId === chartIdRef.current) activeChartId = null
        if (hoveredIdxRef.current !== null) {
          lastHoverVxRef.current = null
          lastNotifiedSlotRef.current = null
          lastNotifiedFeedRef.current = undefined
          hoveredIdxRef.current = null
          plotRef.current?.redraw(false)
        }
      }
    }
    document.addEventListener('mousemove', onDocMove, { passive: true })
    return () => document.removeEventListener('mousemove', onDocMove)
  }, [])

  return (
    <div className="relative flex-1 min-w-0 overflow-hidden rounded">
      <div ref={containerRef} />
    </div>
  )
}

const LIVE_BUFFER_SIZE = 200
const MAX_BUFFER_SLOTS = 2000

// Returns the windowSize slots whose right edge is at `endSlot`.
// When endSlot is null, uses `liveEdge` as the anchor (the drain-controlled live edge).
// liveEdge=0 means "no anchor" — uses the buffer's newest slot (non-live mode).
function computeViewByEnd(
  buffer: EdgeScoreboardSlotRace[],
  endSlot: number | null,
  liveEdge?: number,
  extraLeft: number = 0,
  windowSize: number = LIVE_BUFFER_SIZE,
): EdgeScoreboardSlotRace[] {
  const slotNums = [...new Set(buffer.map(r => r.slot))].sort((a, b) => a - b)
  if (!slotNums.length) return []
  // liveEdge=0 is treated as unset (use buffer newest).
  const anchor = endSlot ?? (liveEdge || null)
  let rightIdx = slotNums.length - 1
  if (anchor != null) {
    while (rightIdx > 0 && slotNums[rightIdx] > anchor) rightIdx--
  }
  const leftIdx = Math.max(0, rightIdx - windowSize + 1 - extraLeft)
  const keep = new Set(slotNums.slice(leftIdx, rightIdx + 1))
  return buffer.filter(r => keep.has(r.slot))
}

function RecentSlotsChart({
  slots,
  nodes,
  slotLeaders,
  leadersOnly,
  slotBuckets,
  slotBucketSize,
  window,
  bucketed,
  setBucketed,
  live,
  setLive,
  viewSlotCount,
  setViewSlotCount,
  bare,
  granular,
  scrollToLiveRef,
  toggleLiveRef,
  onViewEndSlotChange,
}: {
  slots: EdgeScoreboardSlotRace[]
  nodes: EdgeScoreboardNode[]
  slotLeaders?: Record<string, EdgeScoreboardLeader>
  leadersOnly?: boolean
  slotBuckets?: EdgeScoreboardSlotBucket[]
  slotBucketSize?: number
  window?: TimeWindow
  bucketed: boolean
  setBucketed: (v: boolean) => void
  live: boolean
  setLive: (v: boolean) => void
  viewSlotCount: number
  setViewSlotCount: (n: number) => void
  bare?: boolean
  granular?: boolean
  scrollToLiveRef?: React.RefObject<(() => void) | null>
  toggleLiveRef?: React.RefObject<(() => void) | null>
  onViewEndSlotChange?: (slot: number | null) => void
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const viewSlotCountRef = useRef(viewSlotCount)
  viewSlotCountRef.current = viewSlotCount

  // Live mode: fetch 300 slots on activate (show first 100 immediately, queue
  // the rest for animation), then poll every 5s with a since_slot cursor so
  // only new slots are fetched. A sliding window keeps slot count fixed at
  // LIVE_BUFFER_SIZE so uPlot never re-initialises and the slide animation fires.
  const liveMaxSlotRef = useRef(0)
  const liveQueueRef = useRef<EdgeScoreboardSlotRace[][]>([])
  // liveEdge: the slot number drain considers the right edge of the live window.
  // Only advances via drain, so computeViewByEnd(buf, null, liveEdge) and
  // scrollToLive both target the same value — eliminating the jump on transition.
  const [liveEdge, setLiveEdge] = useState<number>(0)
  const liveEdgeRef = useRef<number>(0)
  const [liveLeaders, setLiveLeaders] = useState<Record<string, EdgeScoreboardLeader> | undefined>(undefined)
  const slotBufferRef = useRef<EdgeScoreboardSlotRace[]>([])
  // viewEndSlot: the slot number anchoring the right edge of the visible window.
  // null = live (show up to liveEdge). Absolute slot number means the view is
  // stable when the buffer grows on either end — no offset math needed.
  const [viewEndSlot, setViewEndSlotRaw] = useState<number | null>(null)
  const viewEndSlotRef = useRef<number | null>(null)
  const setViewEndSlot = (slot: number | null) => {
    setViewEndSlotRaw(slot)
    onViewEndSlotChange?.(slot)
  }

  // Refs so the live effect can read current prop values without re-running.
  const slotsRef = useRef(slots)
  slotsRef.current = slots
  const slotLeadersRef = useRef(slotLeaders)
  slotLeadersRef.current = slotLeaders

  // Track query params from the last live-effect seed so we can detect when they change.
  // When leadersOnly or window changes, slotsRef.current holds stale data (wrong filter),
  // so we must fetch fresh instead of seeding from the cached query data.
  const prevLiveParamsRef = useRef<{ leadersOnly?: boolean; window?: string }>({})

  useEffect(() => {
    if (!live || bucketed) {
      liveMaxSlotRef.current = 0
      liveQueueRef.current = []
      liveEdgeRef.current = 0
      setLiveEdge(0)
      setLiveLeaders(undefined)
      viewEndSlotRef.current = null
      setViewEndSlot(null)
      prefetchedBoundariesRef.current = new Set()
      return
    }

    let cancelled = false

    // Group races by slot, preserving order.
    const bySlotOrdered = (races: EdgeScoreboardSlotRace[]) => {
      const map = new Map<number, EdgeScoreboardSlotRace[]>()
      const nums: number[] = []
      for (const r of races) {
        if (!map.has(r.slot)) { map.set(r.slot, []); nums.push(r.slot) }
        map.get(r.slot)!.push(r)
      }
      return { map, nums: nums.sort((a, b) => a - b) }
    }

    // Seed the live buffer from a set of slot races (initial load path).
    // Puts the vast majority of slots into the immediate buffer (chart starts near live edge)
    // and queues only a small tail so the animation is visually active right away.
    const INITIAL_QUEUE_SLOTS = 75 // ~30s of animation at 400ms/slot, matching server cache refresh cycle
    const seedBuffer = (races: EdgeScoreboardSlotRace[], leaders?: Record<string, EdgeScoreboardLeader>) => {
      const { map, nums } = bySlotOrdered(races)
      liveMaxSlotRef.current = nums.at(-1) ?? 0
      const splitIdx = Math.max(0, nums.length - INITIAL_QUEUE_SLOTS)
      const immediate = nums.slice(0, splitIdx)
      const toQueue = nums.slice(splitIdx)
      const immediateSlot = immediate.at(-1) ?? nums.at(-1) ?? 0
      slotBufferRef.current = immediate.flatMap(s => map.get(s)!)
      liveEdgeRef.current = immediateSlot
      setLiveEdge(immediateSlot)
      liveQueueRef.current = toQueue.map(s => map.get(s)!)
      if (leaders) setLiveLeaders(leaders)
    }

    const loadSlots = (data: Awaited<ReturnType<typeof fetchEdgeScoreboard>>, prevMax: number) => {
      const { map, nums } = bySlotOrdered(data.recent_slots)
      const newNums = prevMax > 0 ? nums.filter(n => n > prevMax) : nums
      if (!newNums.length) return
      liveMaxSlotRef.current = nums.at(-1) ?? liveMaxSlotRef.current
      liveQueueRef.current.push(...newNums.map(s => map.get(s)!))
      if (data.slot_leaders) setLiveLeaders(prev => ({ ...prev, ...data.slot_leaders }))
    }

    // Seed immediately from the React Query data if already available, otherwise fetch.
    // This eliminates a duplicate network round-trip on initial load and when re-entering live mode.
    // If leadersOnly or window changed, slotsRef.current holds stale data (wrong filter),
    // so we must fetch fresh to avoid seeding the buffer with mismatched slots.
    const liveParamsChanged = prevLiveParamsRef.current.leadersOnly !== leadersOnly || prevLiveParamsRef.current.window !== window
    prevLiveParamsRef.current = { leadersOnly, window }
    if (!liveParamsChanged && slotsRef.current.length > 0) {
      seedBuffer(slotsRef.current, slotLeadersRef.current)
    } else {
      fetchEdgeScoreboard(window, leadersOnly).then(data => {
        if (cancelled) return
        seedBuffer(data.recent_slots, data.slot_leaders ?? undefined)
      }).catch(() => {})
    }

    // Poll every 5s. The server cache refreshes every 30s (~75 new slots each time).
    // At 400ms/slot drain rate, 75 slots take ~30s — matching the cache cycle. Polling
    // at 5s means we catch a fresh cache within 5s of it arriving rather than up to 10s.
    const poll = () => {
      const prevMax = liveMaxSlotRef.current
      fetchEdgeScoreboard(window, leadersOnly).then(data => {
        if (cancelled) return
        loadSlots(data, prevMax)
      }).catch(() => {})
    }
    const pollInterval = setInterval(poll, 5_000)

    // Single rAF loop drives both the scroll animation and the drain.
    // scrollOffset advances at slotPx/400ms (constant velocity). When it rolls over
    // slotPx, we pop the next slot from the queue and call setLiveEdge + setScrollOffset
    // in the same rAF callback — React 18 auto-batches them into one render so the
    // rollover is seamless: old slot exits at screen -slotPx, new slot appears at
    // screen 199*slotPx (right mask fade zone), all positions continuous.
    let scrollOff = 0
    let drainTimer = 0
    let lastTime: number | null = null
    let drainRafId = 0
    const tick = (now: number) => {
      if (cancelled) return
      // Cap dt to one slot interval so a long background-tab pause doesn't cause a
      // burst of rapid drains on return (rAF is throttled in background tabs).
      const dt = lastTime === null ? 0 : Math.min(now - lastTime, 400)
      lastTime = now
      const slotPx = Math.max(1, ((containerRef.current?.offsetWidth ?? 260) - 130) / viewSlotCountRef.current)
      const inTail = !isDraggingRef.current && viewEndSlotRef.current === null
      if (inTail) {
        // Only advance when there's something to drain — prevents scroll from oscillating
        // back to 0 when the queue is empty (e.g. right after init or between polls).
        if (liveQueueRef.current.length > 0) {
          scrollOff += (slotPx / 400) * dt
          if (scrollOff >= slotPx) {
            const races = liveQueueRef.current.shift()
            if (races) {
              const newBuf = [...slotBufferRef.current, ...races]
              const bufNums = [...new Set(newBuf.map(r => r.slot))].sort((a, b) => a - b)
              const keepBuf = new Set(bufNums.slice(-MAX_BUFFER_SLOTS))
              slotBufferRef.current = newBuf.filter(r => keepBuf.has(r.slot))
              const slotNum = races[0]?.slot
              scrollOff -= slotPx
              if (slotNum) { liveEdgeRef.current = slotNum; setLiveEdge(slotNum) }
            } else {
              scrollOff = 0
            }
          }
        } else if (scrollOff !== 0) {
          scrollOff = 0
        }
        setScrollOffset(scrollOff)
      } else {
        if (scrollOff !== 0) { scrollOff = 0; setScrollOffset(0) }
        // Still drain queue at 400ms pace so liveEdge stays current even when frozen/dragging.
        // This ensures clicking Live/>> after a drag returns to the actual head, not a frozen slot.
        drainTimer += dt
        if (drainTimer >= 400) {
          drainTimer -= 400
          const races = liveQueueRef.current.shift()
          if (races) {
            const newBuf = [...slotBufferRef.current, ...races]
            const bufNums = [...new Set(newBuf.map(r => r.slot))].sort((a, b) => a - b)
            const keepBuf = new Set(bufNums.slice(-MAX_BUFFER_SLOTS))
            slotBufferRef.current = newBuf.filter(r => keepBuf.has(r.slot))
            const slotNum = races[0]?.slot
            if (slotNum) { liveEdgeRef.current = slotNum }
          }
        }
      }
      drainRafId = requestAnimationFrame(tick)
    }
    drainRafId = requestAnimationFrame(tick)

    return () => {
      cancelled = true
      clearInterval(pollInterval)
      cancelAnimationFrame(drainRafId)
      liveMaxSlotRef.current = 0
      liveQueueRef.current = []
      // Don't clear the buffer — non-live mode will overwrite it with slots next render.
      liveEdgeRef.current = 0
    }
  }, [live, bucketed, window, leadersOnly])

  // If React Query data arrives after the live effect started but the live effect's
  // own fetch hasn't returned yet, seed the buffer now so the chart isn't blank.
  const FALLBACK_QUEUE_SLOTS = 75
  useEffect(() => {
    if (!live || bucketed || !slots.length || slotBufferRef.current.length > 0 || liveEdgeRef.current > 0) return
    const map = new Map<number, EdgeScoreboardSlotRace[]>()
    const nums: number[] = []
    for (const r of slots) {
      if (!map.has(r.slot)) { map.set(r.slot, []); nums.push(r.slot) }
      map.get(r.slot)!.push(r)
    }
    nums.sort((a, b) => a - b)
    liveMaxSlotRef.current = nums.at(-1) ?? 0
    const splitIdx = Math.max(0, nums.length - FALLBACK_QUEUE_SLOTS)
    slotBufferRef.current = nums.slice(0, splitIdx).flatMap(s => map.get(s)!)
    const immediateSlot = nums[splitIdx - 1] ?? nums.at(-1) ?? 0
    liveEdgeRef.current = immediateSlot
    setLiveEdge(immediateSlot)
    liveQueueRef.current = nums.slice(splitIdx).map(s => map.get(s)!)
    if (slotLeaders) setLiveLeaders(slotLeaders)
  }, [slots, live, bucketed, slotLeaders])

  // In non-live per-slot mode, keep the buffer in sync with the query result so
  // the scroll system works the same way as live mode.
  if (!live && !bucketed) {
    slotBufferRef.current = slots
  }

  // Drag-to-scroll: click/touch and drag left/right to navigate the timeline with momentum.
  const momentumStopRef = useRef<{ stop: () => void } | null>(null)
  const liveRef = useRef(live)
  liveRef.current = live
  // Info bar DOM refs — updated directly to avoid React re-render flicker.
  // Info bar: 2 lines, always visible, zero layout shift.
  // Line 1 (feeds): color swatch + label + live % — replaces the standalone legend.
  // Line 2 (slot): slot number (fixed) + single leader text span (variable content, no show/hide).
  // When null is passed (mouse left), fall back to defaultInfoRef (most-recent slot).
  const infoSlotRef = useRef<HTMLSpanElement>(null)
  const infoLeaderNameRef = useRef<HTMLSpanElement>(null)
  const infoLeaderRef = useRef<HTMLSpanElement>(null)
  const infoFeedValueRefs = useRef<Map<string, HTMLSpanElement>>(new Map())
  const infoFeedBarFillRefs = useRef<Map<string, HTMLDivElement>>(new Map())
  const infoFeedLegendItemRefs = useRef<Map<string, HTMLDivElement>>(new Map())
  const defaultInfoRef = useRef<SlotHoverInfo | null>(null)
  const isHoveredRef = useRef(false)

  const applyInfoBar = useCallback((info: SlotHoverInfo | null) => {
    if (!info) return
    if (infoSlotRef.current) infoSlotRef.current.textContent = `Slot ${info.slot.toLocaleString()}`
    if (infoLeaderNameRef.current) {
      const l = info.leader
      infoLeaderNameRef.current.textContent = l?.name ?? (l ? `${l.pubkey.slice(0, 8)}…${l.pubkey.slice(-4)}` : '')
    }
    if (infoLeaderRef.current) {
      const l = info.leader
      const parts: string[] = []
      if (l?.name) parts.push(`${l.pubkey.slice(0, 8)}…${l.pubkey.slice(-4)}`)
      if (l?.city) parts.push(`${l.city}${l.country ? `, ${l.country}` : ''}`)
      if (l?.asn_org) parts.push(l.asn_org)
      infoLeaderRef.current.textContent = parts.join(' · ')
    }
    for (const [f, span] of infoFeedValueRefs.current) { const v = info.feedData[f] ?? 0; span.textContent = v >= 100 ? '100%' : `${v.toFixed(1)}%` }
    for (const [f, bar] of infoFeedBarFillRefs.current) { bar.style.width = `${Math.min(100, info.feedData[f] ?? 0)}%` }
    // Always emphasize the winning feed (highest value at this slot)
    const winnerFeed = Object.entries(info.feedData).reduce<string | null>(
      (best, [f, v]) => (v != null && (best == null || v > (info.feedData[best] ?? 0)) ? f : best), null
    )
    for (const [f, el] of infoFeedLegendItemRefs.current) {
      el.style.opacity = winnerFeed ? (f === winnerFeed ? '1' : '0.5') : ''
      el.style.fontWeight = f === winnerFeed ? '500' : ''
    }
  }, [])

  const updateInfoBar = useCallback((info: SlotHoverInfo | null) => {
    if (info) {
      isHoveredRef.current = true
      applyInfoBar(info)
    } else {
      isHoveredRef.current = false
      applyInfoBar(defaultInfoRef.current)
    }
  }, [applyInfoBar])

  // Ref to the chart rows container — used to clear hover info when mouse leaves the area.
  const chartRowsRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const onDocMove = (e: MouseEvent) => {
      if (chartRowsRef.current && !chartRowsRef.current.contains(e.target as Node)) {
        updateInfoBar(null)
      }
    }
    document.addEventListener('mousemove', onDocMove, { passive: true })
    return () => document.removeEventListener('mousemove', onDocMove)
  }, [updateInfoBar])

  const [isDragging, setIsDragging] = useState(false)
  const isDraggingRef = useRef(isDragging)
  isDraggingRef.current = isDragging
  const [overscrollPx, setOverscrollPx] = useState(0)
  // Sub-slot fractional offset applied as CSS translate during inertia so bars glide
  // smoothly between slot boundaries without triggering activeSlots recomputation.
  const [inertiaFracPx, setInertiaFracPx] = useState(0)
  const [isInertia, setIsInertia] = useState(false)
  const [isScrollingToLive, setIsScrollingToLive] = useState(false)
  // scrollOffset: 0→slotPx at constant velocity, driven by a single rAF loop that also
  // pops the drain queue at rollover. Both setScrollOffset+setLiveEdge fire in the same
  // rAF callback so React batches them into one render — the rollover is seamless.
  const [scrollOffset, setScrollOffset] = useState(0)


  const prefetchingRef = useRef(false)
  const [isPrefetching, setIsPrefetching] = useState(false)
  const prefetchedBoundariesRef = useRef(new Set<number>())

  // Animate smoothly to the live edge, then snap to tailing mode.
  // Handles two cases:
  //   1. liveEdge already known (already live, or re-enabling): animate immediately.
  //   2. liveEdge unknown (first activation): pin current position, start live mode,
  //      then wait for drain to set liveEdgeRef before starting the tween.
  const scrollToLiveAnimRef = useRef<number | null>(null)
  const scrollToLive = () => {
    momentumStopRef.current?.stop()
    momentumStopRef.current = null
    setInertiaFracPx(0)
    setIsInertia(false)

    if (scrollToLiveAnimRef.current !== null) {
      cancelAnimationFrame(scrollToLiveAnimRef.current)
      scrollToLiveAnimRef.current = null
    }

    const slotNums = [...new Set(slotBufferRef.current.map(r => r.slot))].sort((a, b) => a - b)

    // Effective start: use pinned position if available, else the buffer's newest slot.
    // This gives us a concrete starting slot even when viewEndSlot is null (tailing state).
    const effectiveStart = viewEndSlotRef.current ?? slotNums.at(-1) ?? null

    // Sync liveEdge state with ref before transitioning to tailing so activeSlots
    // doesn't anchor to a stale state value (ref is kept current by non-tail drain).
    const syncLiveEdge = () => {
      if (liveEdgeRef.current > 0) setLiveEdge(liveEdgeRef.current)
    }

    if (effectiveStart === null) {
      // No data at all — just snap.
      viewEndSlotRef.current = null
      syncLiveEdge()
      setViewEndSlot(null)
      if (!liveRef.current) setLive(true)
      return
    }

    setIsScrollingToLive(true)

    // Pin the current view BEFORE activating live mode so the content doesn't jump
    // when the drain first fires and liveEdge advances past the current buffer head.
    if (viewEndSlotRef.current !== effectiveStart) {
      viewEndSlotRef.current = effectiveStart
      setViewEndSlot(effectiveStart)
    }

    // Start live mode (no-op if already live) so drain + SSE begin.
    if (!liveRef.current) setLive(true)

    // If liveEdge is already known and we're already at it, snap and done.
    if (liveEdgeRef.current > 0 && effectiveStart >= liveEdgeRef.current) {
      viewEndSlotRef.current = null
      syncLiveEdge()
      setViewEndSlot(null)
      setIsScrollingToLive(false)
      return
    }

    const startSlot = effectiveStart
    // If liveEdge is known, we can set the target now; otherwise we wait in the rAF loop.
    let targetSlot: number | null = liveEdgeRef.current > 0 ? liveEdgeRef.current : null
    let animStartTime: number | null = targetSlot !== null ? performance.now() : null
    const waitStart = performance.now()
    const WAIT_TIMEOUT_MS = 2000

    const tick = (now: number) => {
      if (targetSlot === null) {
        // Waiting for drain to produce the first liveEdge value.
        const liveEdge = liveEdgeRef.current
        if (liveEdge > 0) {
          if (startSlot >= liveEdge) {
            // Already at target, done.
            scrollToLiveAnimRef.current = null
            viewEndSlotRef.current = null
            syncLiveEdge()
            setViewEndSlot(null)
            setIsScrollingToLive(false)
            return
          }
          targetSlot = liveEdge
          animStartTime = now
        } else if (now - waitStart > WAIT_TIMEOUT_MS) {
          // Timed out — snap to live.
          scrollToLiveAnimRef.current = null
          viewEndSlotRef.current = null
          syncLiveEdge()
          setViewEndSlot(null)
          setIsScrollingToLive(false)
          return
        } else {
          scrollToLiveAnimRef.current = requestAnimationFrame(tick)
          return
        }
      }

      const distance = targetSlot - startSlot
      // Scale duration by distance; clamp so short hops feel snappy, long ones stay smooth.
      const duration = Math.min(700, Math.max(200, distance * 8))
      const t = Math.min(1, (now - animStartTime!) / duration)
      // Ease-out cubic: fast start, smooth deceleration.
      const eased = 1 - Math.pow(1 - t, 3)
      const current = startSlot + distance * eased

      if (t < 1) {
        viewEndSlotRef.current = current
        setViewEndSlot(current)
        scrollToLiveAnimRef.current = requestAnimationFrame(tick)
      } else {
        scrollToLiveAnimRef.current = null
        viewEndSlotRef.current = null
        syncLiveEdge()
        setViewEndSlot(null)
        setIsScrollingToLive(false)
      }
    }

    scrollToLiveAnimRef.current = requestAnimationFrame(tick)
  }

  const toggleLive = () => {
    if (live && viewEndSlot === null) {
      viewEndSlotRef.current = liveEdgeRef.current
      setViewEndSlot(liveEdgeRef.current)
    } else {
      scrollToLive()
    }
  }

  // Expose scrollToLive and toggleLive to parent for header controls rendering.
  if (scrollToLiveRef) scrollToLiveRef.current = scrollToLive
  if (toggleLiveRef) toggleLiveRef.current = toggleLive

  // Captured at the start of each drag gesture so we can compute position from
  // cumulative movement rather than accumulating per-frame incremental deltas.
  const dragStartSlotRef = useRef<number>(0)

  useDrag(
    ({ movement: [mx], velocity: [vx], direction: [dirX], first, last, active }) => {
      const slotNums = () => [...new Set(slotBufferRef.current.map(r => r.slot))].sort((a, b) => a - b)
      const px = () => Math.max(1, ((containerRef.current?.offsetWidth ?? 260) - 130) / viewSlotCountRef.current)

      if (active) {
        momentumStopRef.current?.stop()
        momentumStopRef.current = null
        setInertiaFracPx(0)

        const nums = slotNums()
        const liveEdge = liveEdgeRef.current || (nums.at(-1) ?? 0)
        const oldestSlot = nums[0] ?? 0
        const minEnd = oldestSlot + viewSlotCount - 1

        if (first) {
          // Capture the slot position at drag start so rawEnd = startSlot - totalMovement/px.
          // This makes overscroll accumulate correctly — delta-based math resets each frame
          // when viewEndSlotRef is frozen at minEnd during overscroll.
          dragStartSlotRef.current = viewEndSlotRef.current ?? liveEdge
        }

        // Use cumulative movement from gesture start, not incremental delta.
        // drag right (mx > 0) = back in time = rawEnd decreases
        const rawEnd = dragStartSlotRef.current - mx / px()

        if (rawEnd < minEnd) {
          // Past the left edge: anchor data at minEnd (no content change), grow CSS transform.
          const rawOverflowPx = (minEnd - rawEnd) * px()
          setOverscrollPx(Math.min(350, rawOverflowPx * 0.35))
          viewEndSlotRef.current = minEnd
          setViewEndSlot(minEnd)
        } else if (rawEnd > liveEdge) {
          // Past the right (live) edge: anchor at liveEdge, grow CSS transform leftward.
          const rawOverflowPx = (rawEnd - liveEdge) * px()
          setOverscrollPx(-Math.min(350, rawOverflowPx * 0.35))
          viewEndSlotRef.current = liveEdge
          setViewEndSlot(liveEdge)
        } else {
          setOverscrollPx(0)
          viewEndSlotRef.current = rawEnd
          setViewEndSlot(rawEnd)
        }
        setIsDragging(true)
      }

      if (last) {
        setIsDragging(false)
        setOverscrollPx(0)  // CSS transition snaps back

        const nums = slotNums()
        const liveEdge = liveEdgeRef.current || (nums.at(-1) ?? 0)
        const oldestSlot = nums[0] ?? 0
        const minEnd = oldestSlot + viewSlotCount - 1
        const currentEnd = viewEndSlotRef.current ?? liveEdge

        // If the user released at/past the live edge, animate smoothly into live.
        if (currentEnd >= liveEdge) {
          scrollToLive()
          return
        }

        // velocity[0] = px/ms magnitude, direction[0] = sign.
        // drag right → slot decreases → inertia velocity is negative
        const slotVelocityPerSecond = -(vx * dirX) / px() * 1000

        if (Math.abs(slotVelocityPerSecond) > 1) {
          // Custom rAF-based inertia with sub-slot CSS translate for smooth deceleration.
          // The fractional offset (inertiaFracPx) is updated every frame — since it's not
          // in activeSlots deps, those re-renders only update the CSS transform (cheap).
          // setViewEndSlot only fires at slot boundaries (expensive but infrequent).
          const timeConstant = 600
          const power = 0.8
          const target = Math.max(minEnd, Math.min(liveEdge,
            currentEnd + slotVelocityPerSecond / 1000 * power * timeConstant))
          const from = currentEnd
          const startTime = performance.now()
          let lastCommitted = Math.floor(currentEnd)
          let rafHandle: number | null = null

          const tick = () => {
            const elapsed = performance.now() - startTime
            const decay = Math.exp(-elapsed / timeConstant)
            const value = target + (from - target) * decay
            const clamped = Math.max(minEnd, Math.min(liveEdge, value))

            const slotPxNow = Math.max(1, ((containerRef.current?.offsetWidth ?? 260) - 130) / viewSlotCountRef.current)
            const committed = Math.floor(clamped)
            const fracPx = -(clamped - committed) * slotPxNow

            viewEndSlotRef.current = clamped

            // Only re-render with new slot data at boundaries; sub-slot moves use cheap fracPx re-render
            if (committed !== lastCommitted) {
              lastCommitted = committed
              setViewEndSlot(committed)
            }
            setInertiaFracPx(fracPx)

            // Stop when close enough to target
            const remainingSlots = Math.abs(clamped - target)
            const velocitySlotsSec = Math.abs((from - target) / timeConstant * decay * 1000)
            if (remainingSlots < 0.5 || velocitySlotsSec < 0.5) {
              const finalSlot = Math.round(clamped)
              viewEndSlotRef.current = finalSlot
              setViewEndSlot(finalSlot)
              setInertiaFracPx(0)
              setIsInertia(false)
              momentumStopRef.current = null
              if (finalSlot >= liveEdgeRef.current - 1) scrollToLive()
              return
            }

            rafHandle = requestAnimationFrame(tick)
          }

          setIsInertia(true)
          rafHandle = requestAnimationFrame(tick)
          momentumStopRef.current = {
            stop: () => {
              if (rafHandle !== null) cancelAnimationFrame(rafHandle)
              setInertiaFracPx(0)
              setIsInertia(false)
            }
          }
        }
      }
    },
    {
      target: containerRef,
      axis: 'x',
      pointer: { capture: true, touch: true },
      filterTaps: true,
      enabled: !bucketed,
    }
  )

  // Prefetch older slots when user scrolls near the buffer start (infinite scroll backwards).
  useEffect(() => {
    if (bucketed || prefetchingRef.current || viewEndSlot === null) return
    const buffer = slotBufferRef.current
    if (!buffer.length) return
    const slotNums = [...new Set(buffer.map(r => r.slot))].sort((a, b) => a - b)
    const oldestSlot = slotNums[0]
    // Trigger when the left edge of the view is within 150 slots of the oldest data
    if (viewEndSlot > oldestSlot + viewSlotCount + 150) return
    if (prefetchedBoundariesRef.current.has(oldestSlot)) return
    prefetchingRef.current = true
    setIsPrefetching(true)
    prefetchedBoundariesRef.current.add(oldestSlot)
    fetchEdgeScoreboard(window, leadersOnly, { beforeSlot: oldestSlot, limit: 500 }).then(data => {
      if (!data.recent_slots.length) return
      // Prepend older slots. viewEndSlot is an absolute slot number so the view
      // is unaffected by buffer growth — no offset adjustment needed.
      const existingSlots = new Set(slotBufferRef.current.map(r => r.slot))
      const newRaces = data.recent_slots.filter((r: EdgeScoreboardSlotRace) => !existingSlots.has(r.slot))
      if (newRaces.length) slotBufferRef.current = [...newRaces, ...slotBufferRef.current]
    }).catch(() => {
      prefetchedBoundariesRef.current.delete(oldestSlot)
    }).finally(() => {
      prefetchingRef.current = false
      setIsPrefetching(false)
    })
  }, [viewEndSlot, bucketed, window, leadersOnly])

  // Memoized on liveEdge/viewEndSlot so 60fps scrollOffset re-renders don't recompute it.
  // slotBufferRef is a ref that only changes when liveEdge advances, so this is correct.
  const activeSlots = useMemo(() => {
    if (bucketed) return slots
    const buf = slotBufferRef.current
    if (!buf.length) return live ? [] : slots
    return computeViewByEnd(buf, viewEndSlot, liveEdge, 0, viewSlotCount)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bucketed, liveEdge, viewEndSlot, live, slots, viewSlotCount])

  const chartData = useMemo(() => {
    if (!activeSlots.length || !nodes.length) return { nodeCharts: [], feeds: [] as string[], slotCount: 0 }

    const validNodeIds = new Set(nodes.map((n) => n.host))
    const filtered = activeSlots.filter((s) => validNodeIds.has(s.host))
    const feedKey = (f: string) => feedKeyForMode(f, granular ?? false)

    const feedSet = new Set<string>()
    for (const s of filtered) { const k = feedKey(s.feed); if (k) feedSet.add(k) }
    const feeds = [...feedSet].sort((a, b) => feedSortPriority(a) - feedSortPriority(b))

    const byNode = new Map<string, Map<number, Record<string, number>>>()
    for (const s of filtered) {
      const k = feedKey(s.feed)
      if (!k) continue
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
      row[k] = (row[k] ?? 0) + s.win_pct  // accumulate (handles merged feeds in simplified mode)
    }

    // Use all slots from activeSlots as the shared x-axis so every node's data array
    // has the same length as the canvas (LIVE_BUFFER_SIZE). Nodes with no data for a
    // given slot get zeros, which draw no bars. This ensures uPlot cursor.idx always
    // maps to a valid slotData entry, so hover/tooltip works across the full canvas.
    const allSlotNumbers = [...new Set(activeSlots.map((s) => s.slot))].sort((a, b) => a - b)
    const sortedNodes = [...nodes].sort((a, b) => a.host.localeCompare(b.host))

    const nodeCharts = sortedNodes
      .filter((n) => byNode.has(n.host))
      .map((n) => {
        const slotMap = byNode.get(n.host)!
        const data = allSlotNumbers.map((slot, idx) => {
          const feedPcts = slotMap.get(slot) ?? {}
          const total = feeds.reduce((sum, f) => sum + (feedPcts[f] ?? 0), 0)
          const scale = total > 0 ? 100 / total : 1
          const row: Record<string, number> = { idx, slot }
          for (const f of feeds) row[f] = Math.round((feedPcts[f] ?? 0) * scale * 10) / 10
          return row
        })
        return { node: n, data }
      })
    const slotNumbers = allSlotNumbers

    return { nodeCharts, feeds, slotCount: slotNumbers.length }
  }, [activeSlots, nodes, granular])

  const bucketedChartData = useMemo(() => {
    if (!slotBuckets || !slotBuckets.length || !nodes.length) {
      return { nodeCharts: [], feeds: [] as string[], slotCount: 0, bucketSize: 0 }
    }

    const validNodeIds = new Set(nodes.map((n) => n.host))
    const filtered = slotBuckets.filter((b) => validNodeIds.has(b.host))

    const feedKey = (f: string) => feedKeyForMode(f, granular ?? false)
    const feedSet = new Set<string>()
    for (const b of filtered) { const k = feedKey(b.feed); if (k) feedSet.add(k) }
    const feeds = [...feedSet].sort((a, b) => feedSortPriority(a) - feedSortPriority(b))

    // Use the actual API-returned buckets as the x-axis. No theoretical range computation —
    // the window filter on the API side already scopes the data. Aggregating to at most 200
    // display buckets keeps the chart readable for long windows.
    const maxDisplayBuckets = 200

    const allRawBuckets = [...new Set(filtered.map((b) => b.slot_bucket))].sort((a, b) => a - b)
    const apiBucketSize = slotBucketSize ?? (allRawBuckets.length >= 2 ? allRawBuckets[1] - allRawBuckets[0] : 1)

    // Clip to the common bucket range across all nodes so all charts start and end at the same point.
    // Without this, nodes with earlier or later data produce partial first/last buckets with
    // misleading win rates (visible as spikes at the chart edges).
    const perNodeMin = new Map<string, number>()
    const perNodeMax = new Map<string, number>()
    for (const b of filtered) {
      if (perNodeMin.get(b.host) === undefined || b.slot_bucket < perNodeMin.get(b.host)!) perNodeMin.set(b.host, b.slot_bucket)
      if (b.slot_bucket > (perNodeMax.get(b.host) ?? 0)) perNodeMax.set(b.host, b.slot_bucket)
    }
    const maxFirstBucket = Math.max(...perNodeMin.values())
    const minLastBucket = Math.min(...perNodeMax.values())
    const rawBuckets = allRawBuckets.filter(b => b >= maxFirstBucket && b <= minLastBucket)

    const groupSize = Math.max(1, Math.ceil(rawBuckets.length / maxDisplayBuckets))
    const displayBucketSize = groupSize * apiBucketSize
    const displayBucketStarts = rawBuckets.filter((_, i) => i % groupSize === 0)
    const apiBucketIndex = new Map(rawBuckets.map((slot, i) => [slot, i]))

    const sortedNodes = [...nodes].sort((a, b) => a.host.localeCompare(b.host))

    const bucketedNodeCharts = sortedNodes
      .filter((n) => filtered.some((b) => b.host === n.host))
      .map((n) => {
        // Aggregate raw counts per display bucket per feed
        const displayData = new Map<number, { feedWon: Map<string, number>; bucketTotal: number }>()
        for (const b of filtered) {
          if (b.host !== n.host) continue
          // Skip buckets outside our computed range to avoid polluting the wrong display bucket
          const apiIdx = apiBucketIndex.get(b.slot_bucket)
          if (apiIdx === undefined) continue
          const displayStart = rawBuckets[Math.floor(apiIdx / groupSize) * groupSize]
          let agg = displayData.get(displayStart)
          if (!agg) {
            agg = { feedWon: new Map(), bucketTotal: 0 }
            displayData.set(displayStart, agg)
          }
          const bk = feedKey(b.feed)
          if (bk) agg.feedWon.set(bk, (agg.feedWon.get(bk) ?? 0) + b.feed_won)
          agg.bucketTotal += b.feed_won // bucket_total = sum of all feed_won across feeds
        }

        const data = displayBucketStarts.map((slot, idx) => {
          const agg = displayData.get(slot)
          const total = agg?.bucketTotal ?? 0
          const row: Record<string, number | null> = { idx, slot }
          for (const f of feeds) {
            const feedWon = agg?.feedWon.get(f) ?? 0
            const pct = total > 0 ? Math.round((feedWon / total) * 1000) / 10 : 0
            row[f] = pct > 0 ? pct : null
          }
          return row
        })
        return { node: n, data }
      })

    return { nodeCharts: bucketedNodeCharts, feeds, slotCount: displayBucketStarts.length, bucketSize: displayBucketSize }
  }, [slotBuckets, slotBucketSize, nodes, window, granular])

  const activeData = bucketed ? bucketedChartData : chartData
  const { nodeCharts, feeds, slotCount } = activeData
  const activeBucketSize = bucketed ? bucketedChartData.bucketSize : undefined

  // Keep defaultInfoRef up-to-date with the most-recent visible slot so the
  // info bar always shows live data even when nothing is hovered.
  useEffect(() => {
    if (bucketed || !activeSlots.length) return
    const lastSlot = activeSlots.at(-1)
    if (!lastSlot) return
    const slotNum = lastSlot.slot
    const slotRaces = activeSlots.filter(s => s.slot === slotNum)
    const fk = (f: string) => feedKeyForMode(f, granular ?? false)

    // Sum feed wins per node first (handles merged feeds in simplified mode), then average across nodes.
    const nodeFeeds: Record<string, Record<string, number>> = {}
    for (const r of slotRaces) {
      const k = fk(r.feed)
      if (!k) continue
      if (!nodeFeeds[r.host]) nodeFeeds[r.host] = {}
      nodeFeeds[r.host][k] = (nodeFeeds[r.host][k] ?? 0) + r.win_pct
    }
    const nodeList = Object.values(nodeFeeds)
    const feedData: Record<string, number | null> = {}
    for (const f of feeds) {
      const sum = nodeList.reduce((acc, nf) => acc + (nf[f] ?? 0), 0)
      feedData[f] = nodeList.length > 0 ? sum / nodeList.length : 0
    }

    // Normalize so values sum to 100%, matching the stacked chart visualization.
    const total = feeds.reduce((sum, f) => sum + (feedData[f] ?? 0), 0)
    if (total > 0) {
      const scale = 100 / total
      for (const f of feeds) feedData[f] = Math.round((feedData[f] ?? 0) * scale * 10) / 10
    }
    const leaders = live ? (liveLeaders ?? slotLeaders) : slotLeaders
    const leader = leaders?.[String(slotNum)]
    const info: SlotHoverInfo = { slot: slotNum, leader, feedData }
    defaultInfoRef.current = info
    if (!isHoveredRef.current) applyInfoBar(info)
  }, [activeSlots, feeds, slotLeaders, liveLeaders, live, bucketed, granular, applyInfoBar])

  if (!slots.length)
    return (
      <div className={bare ? undefined : "rounded-lg border border-border bg-card p-4"}>
        {!bare && <h3 className="text-sm font-medium mb-4">Recent DZ Edge Leader Slots — Win Rate per Slot</h3>}
        <div className="text-sm text-muted-foreground text-center py-12">No recent slot data available.</div>
      </div>
    )

  return (
    <div
      ref={containerRef}
      className={bare ? "pt-2" : "rounded-lg border border-border bg-card p-4"}
      style={{
        touchAction: bucketed ? undefined : 'pan-y',
        cursor: bucketed ? undefined : isDragging ? 'grabbing' : 'grab',
        userSelect: 'none',
      }}
      onPointerDown={bucketed ? undefined : (e) => {
        // Only capture for drag intent — skip interactive children so their click events fire normally.
        if (!(e.target as Element).closest('button, a, input, select, [role="button"]')) {
          e.currentTarget.setPointerCapture(e.pointerId)
        }
      }}
    >
      {!bare && <div className="mb-4">
        <div className={cn("flex items-center", bare ? "justify-end" : "justify-between")}>
          {!bare && <h3 className="text-sm font-medium flex items-center gap-2">
            {bucketed ? 'Win Rate Trend' : 'Win Rate per Slot'}
            {live && (
              <span className="relative flex items-center">
                {isPrefetching ? (
                  <Loader2 size={12} className="animate-spin text-emerald-500/50" />
                ) : viewEndSlot === null ? (
                  <>
                    <span className="animate-ping absolute inline-flex h-2 w-2 rounded-full bg-emerald-400 opacity-75" />
                    <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
                  </>
                ) : (
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500/30" />
                )}
              </span>
            )}
            {(() => {
              const visibleNums = [...new Set(activeSlots.map(r => r.slot))].sort((a, b) => a - b)
              const minSlot = visibleNums[0]
              const maxSlot = visibleNums.at(-1)
              const liveSlot = liveMaxSlotRef.current || maxSlot
              const fmtSlot = (s: number) => s.toLocaleString()
              const fmtAgo = (slotDelta: number) => {
                const sec = Math.round(slotDelta / 2.5)
                return sec < 5 ? 'now' : sec < 60 ? `~${sec}s ago` : `~${Math.round(sec / 60)}m ago`
              }
              const slotRange = minSlot && maxSlot && minSlot !== maxSlot
                ? `${fmtSlot(minSlot)} – ${fmtSlot(maxSlot)}`
                : minSlot ? `${fmtSlot(minSlot)}` : null
              // Only show time-ago when paused (scrolled to a historical position).
              // In live-tailing mode the value reflects animation queue depth, not data age
              // (the page cache is only ~30s stale), so it's misleading to show it there.
              const timeNote = liveSlot && maxSlot && viewEndSlot !== null
                ? `${fmtAgo(liveSlot - minSlot)} – ${fmtAgo(liveSlot - maxSlot)}`
                : null
              if (!slotRange) return null
              return (
                <span className="text-xs font-normal text-muted-foreground">
                  {slotRange}{timeNote ? <span className="text-[#555] ml-1">· {timeNote}</span> : null}
                </span>
              )
            })()}
          </h3>}
          <div className="flex items-center gap-2 -mt-2">
            {!bucketed && live && viewEndSlot !== null && (
              <button
                onClick={scrollToLive}
                className="text-emerald-400 hover:text-emerald-300 transition-colors"
              >
                <ChevronsRight size={16} />
              </button>
            )}
            {!bucketed && (
              <button
                onClick={() => {
                  if (live && viewEndSlot === null) {
                    // Currently live-tailing → pause
                    viewEndSlotRef.current = liveEdgeRef.current
                    setViewEndSlot(liveEdgeRef.current)
                  } else {
                    // Not tailing (paused, or coming back from Trend) → start/resume
                    scrollToLive()
                  }
                }}
                className={cn(
                  'text-xs px-2.5 h-[26px] rounded-md border transition-colors',
                  live && viewEndSlot === null
                    ? 'border-emerald-500 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20'
                    : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
                )}
              >
                <span className="flex items-center gap-1.5 whitespace-nowrap">
                  Live
                  {live && viewEndSlot === null
                    ? <Square size={10} className="fill-current shrink-0" />
                    : <Play size={10} className="fill-current shrink-0" />
                  }
                </span>
              </button>
            )}
            <div className="inline-flex rounded-md border border-border overflow-hidden text-xs h-[26px]">
              <button
                onClick={() => { setBucketed(false); scrollToLive() }}
                className={cn(
                  'px-2.5 transition-colors',
                  !bucketed ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                )}
              >
                Per Slot
              </button>
              <button
                onClick={() => { setBucketed(true); setLive(false) }}
                className={cn(
                  'px-2.5 transition-colors border-l border-border',
                  bucketed ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                )}
              >
                Trend
              </button>
            </div>
          </div>
        </div>
      </div>}
      <div className="flex gap-0">
        <div ref={chartRowsRef} className="relative flex-1 min-w-0">
        {/* Left-edge indicator: fixed at the chart boundary, shows while overscrolling or fetching */}
        {!bucketed && (overscrollPx > 0 || isPrefetching) && (
          <div className="absolute left-[130px] inset-y-0 flex items-center pointer-events-none z-10">
            {isPrefetching
              ? <Loader2 size={14} className="animate-spin text-muted-foreground/60" />
              : <ChevronsRight size={14} className="text-muted-foreground/40 rotate-180" />
            }
          </div>
        )}
        {nodeCharts.map((nc) => (
          <div key={nc.node.host} style={{ height: NODE_ROW_HEIGHT }} className="flex items-center">
            {/* Label stays fixed */}
            <NodeLabel node={nc.node} label={nodeDisplayLabel(nc.node, nodes)} />
            {/* Mask wrapper stays fixed — fade zones always at the visual edges */}
            <div
              className="flex-1 min-w-0 overflow-hidden"
              style={{ maskImage: 'linear-gradient(to right, transparent 0%, black 1%, black 99%, transparent 100%)' }}
            >
            {/* Chart area: overscroll from drag + smooth scroll offset from rAF drain */}
            <div
              className="flex"
              style={{
                transform: `translateX(${overscrollPx + inertiaFracPx - (live && viewEndSlot === null && !isDragging ? scrollOffset : 0)}px)`,
                transition: (isDragging || isInertia || (live && viewEndSlot === null)) ? undefined : 'transform 0.15s cubic-bezier(0.2, 0, 0, 1)',
                willChange: 'transform',
              }}
            >
              {bucketed
                ? <BucketedNodeChart data={nc.data} feeds={feeds} bucketSize={activeBucketSize} />
                : <SlotRaceNodeChart slotData={nc.data} feeds={feeds} slotLeaders={live && !bucketed ? (liveLeaders ?? slotLeaders) : slotLeaders} animated={viewEndSlot !== null} dragging={isDragging || isInertia || isScrollingToLive} liveScrollOffset={live && viewEndSlot === null && !isDragging ? scrollOffset : 0} viewSlotCount={viewSlotCount} onHover={updateInfoBar} />}
            </div>
            </div>{/* end mask wrapper */}
          </div>
        ))}
        </div>{/* end chart rows */}
        {/* Right info panel */}
        <div className="w-60 shrink-0 border-l border-border flex flex-col px-5 py-5">
          {!bucketed && (
            <span ref={infoSlotRef} className="text-xs font-medium tabular-nums mb-4" />
          )}
          <div className="flex flex-col gap-3">
            {feeds.map((f) => (
              <div key={f} ref={el => { if (el) infoFeedLegendItemRefs.current.set(f, el) }} className="flex flex-col gap-1 transition-opacity duration-150">
                <div className="flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: FEED_COLORS[f] ?? '#6b7280' }} />
                  <span className="text-xs text-muted-foreground flex-1 truncate">{FEED_LABELS[f] ?? f}</span>
                  <span ref={el => { if (el) infoFeedValueRefs.current.set(f, el) }} className="text-xs font-medium tabular-nums w-[5ch] text-right shrink-0">—</span>
                </div>
                <div className="h-1 rounded-full bg-muted-foreground/20 overflow-hidden">
                  <div ref={el => { if (el) infoFeedBarFillRefs.current.set(f, el) }} className="h-full rounded-full transition-all duration-150" style={{ backgroundColor: FEED_COLORS[f] ?? '#6b7280', width: '0%' }} />
                </div>
              </div>
            ))}
          </div>
          {!bucketed && (
            <div className="mt-5 flex flex-col gap-0.5">
              <span className="text-[10px] uppercase tracking-wider text-muted-foreground/60 mb-1">Slot Leader</span>
              <span ref={infoLeaderNameRef} className="text-sm font-medium leading-snug truncate" />
              <span ref={infoLeaderRef} className="text-xs text-muted-foreground leading-snug mt-0.5" />
            </div>
          )}
        </div>
      </div>{/* end flex container */}
      {!bucketed && (
        <div className="flex items-center justify-end gap-1 mt-1">
          <span className="text-[10px] text-muted-foreground mr-1">Slots</span>
          {[50, 100, 200, 300, 500].map(n => (
            <button
              key={n}
              onClick={() => setViewSlotCount(n)}
              className={cn(
                'text-[10px] px-1.5 h-[18px] rounded transition-colors',
                viewSlotCount === n
                  ? 'bg-muted text-foreground'
                  : 'text-muted-foreground hover:text-foreground'
              )}
            >
              {n}
            </button>
          ))}
        </div>
      )}
      {bucketed && !bare && (
        <div className="text-xs text-muted-foreground text-center mt-1">
          {(() => {
            const totalSlots = slotCount * (activeBucketSize ?? 1)
            const sec = Math.round(totalSlots / 2.5)
            const timeEst = sec < 60 ? `~${sec}s` : sec < 3600 ? `~${Math.round(sec / 60)}m` : `~${Math.round(sec / 3600)}h`
            return `~${totalSlots.toLocaleString()} slots · ${timeEst} · last ${window ?? 'all'}`
          })()}
        </div>
      )}
    </div>
  )
}

export function EdgeScoreboardPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const rawWindow = searchParams.get('window')
  const activeWindow: TimeWindow = isValidWindow(rawWindow) ? rawWindow : '24h'

  const leadersOnly = searchParams.get('leaders_only') === 'true'
  const bucketed = searchParams.get('trend') === '1'
  const setBucketed = (v: boolean) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (v) p.set('trend', '1')
      else p.delete('trend')
      return p
    })
  }

  const granular = searchParams.get('granular') === '1'
  const setGranular = (v: boolean) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (v) p.set('granular', '1')
      else p.delete('granular')
      return p
    })
  }

  const rawSlotCount = parseInt(searchParams.get('slot_count') ?? '500')
  const viewSlotCount = [50, 100, 200, 300, 500].includes(rawSlotCount) ? rawSlotCount : 500
  const setViewSlotCount = (n: number) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (n === 500) p.delete('slot_count')
      else p.set('slot_count', String(n))
      return p
    })
  }

  const [live, setLive] = useState(true)
  const [viewEndSlot, setViewEndSlot] = useState<number | null>(null)
  const scrollToLiveRef = useRef<(() => void) | null>(null)
  const toggleLiveRef = useRef<(() => void) | null>(null)

  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 5_000)
    return () => clearInterval(id)
  }, [])

  const [showLoader, setShowLoader] = useState(false)
  const [showShimmer, setShowShimmer] = useState(false)

  const showTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['edge-scoreboard', activeWindow, leadersOnly],
    queryFn: () => fetchEdgeScoreboard(activeWindow, leadersOnly),
    refetchInterval: 30_000,
    staleTime: 15_000,
    placeholderData: keepPreviousData,
  })

  // Derive recent slots snapshot synchronously from `data` so both charts update in the same
  // render cycle. Using state+useEffect caused a one-render lag between the bucketed chart
  // (reads data.slot_buckets) and the slot chart (read stableRecent), making them visibly
  // update at different times when switching views. keepPreviousData already handles the
  // transition — data never goes blank — so the generated_at guard is not needed.
  const stableRecent = useMemo(() => {
    if (!data?.recent_slots?.length) return null
    return { slots: data.recent_slots, leaders: data.slot_leaders }
  }, [data])

  const freshness = useMemo(() => {
    if (!data?.generated_at) return null
    const ageSec = Math.round((now - new Date(data.generated_at).getTime()) / 1000)
    if (ageSec < 5) return 'just now'
    if (ageSec < 60) return `${ageSec}s ago`
    return `${Math.round(ageSec / 60)}m ago`
  }, [data?.generated_at, now])

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
      dzShredsWon += node.feeds['dz_edge']?.win_rate_pct ?? 0
      dzTotalShreds++

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

  // Show shimmer while fetching, debounced 200ms so instant cache hits skip it entirely.
  // Shimmer stays on until isFetching clears — no fixed duration.
  useEffect(() => {
    if (!isFetching) {
      if (showTimerRef.current) { clearTimeout(showTimerRef.current); showTimerRef.current = null }
      setShowShimmer(false)
      return
    }
    showTimerRef.current = setTimeout(() => {
      showTimerRef.current = null
      setShowShimmer(true)
    }, 200)
    return () => {
      if (showTimerRef.current) { clearTimeout(showTimerRef.current); showTimerRef.current = null }
    }
  }, [isFetching])

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
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Trophy}
          title="Edge Scoreboard"
          subtitle={
            <span className="text-xs text-muted-foreground/50 flex items-center gap-2">
              <span>{windowLabel(activeWindow)}</span>
              {freshness && <span>· updated {freshness}</span>}
            </span>
          }
          actions={
            <div className="flex items-center gap-3">
              <div className="flex items-center gap-2 text-sm">
                {([
                  [false, 'All Slots', 'Win rates across all observed slots, showing how often each feed (DZ, Jito, Turbine) delivered shreds first.'] as const,
                  [true, 'DZ Edge Leaders', 'Win rates scoped to slots where the scheduled validator leader was publishing via DZ Edge.'] as const,
                ]).map(([v, label, tooltip]) => (
                  <div key={String(v)} className="relative group">
                    {v === true && leadersOnly !== true && (
                      <span className="absolute inset-0 rounded-md ring-1 ring-emerald-400/40 shadow-[0_0_8px_2px_rgba(52,211,153,0.2)] animate-pulse pointer-events-none" />
                    )}
                    <button
                      type="button"
                      onClick={() => setLeadersOnly(v)}
                      className={cn(
                        'px-3 py-1.5 rounded-md border border-border transition-colors',
                        leadersOnly === v
                          ? 'bg-primary text-primary-foreground border-primary'
                          : 'hover:bg-muted'
                      )}
                    >
                      {label}
                    </button>
                    <span className="pointer-events-none absolute top-full left-1/2 -translate-x-1/2 mt-2 z-30 w-64 rounded-lg border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-lg whitespace-normal opacity-0 group-hover:opacity-100 transition-opacity">
                      {tooltip}
                    </span>
                  </div>
                ))}
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


        {/* Hero stats */}
        {data && globalStats && (
          <div className="flex gap-0 mb-8 bg-card border border-border rounded-lg overflow-hidden">
            {/* Left: description + publisher stats */}
            <div className="flex-1 p-6 flex flex-col justify-between min-w-0">
              <p className="text-sm text-muted-foreground leading-relaxed">
                DZ Edge nodes race against Jito Shredstream and Turbine to deliver shreds to validators first, measuring real-world network propagation performance.
              </p>
              <div className="border-t border-border pt-4 mt-4 flex items-center gap-6">
                <div>
                  <div className="text-xs text-muted-foreground mb-1">Publishing Shreds</div>
                  <div className="text-2xl font-semibold tabular-nums">{data.publishing_count.toLocaleString()}</div>
                </div>
                <div className="border-l border-border pl-6">
                  <div className="text-xs text-muted-foreground mb-1">Publisher Stake Weight</div>
                  <div className="text-2xl font-semibold tabular-nums">{formatPct(data.publishing_stake_pct)}</div>
                </div>
              </div>
            </div>

            {/* Middle: metrics */}
            <div className="border-l border-border flex-1 p-6 flex flex-col justify-center gap-4 min-w-0">
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-sm text-muted-foreground">Publisher Stake Weight</span>
                  <span className="text-sm font-medium tabular-nums ml-4 shrink-0">{formatPct(data.publishing_stake_pct)}</span>
                </div>
                <div className="h-1 rounded-full bg-muted-foreground/25 overflow-hidden">
                  <div className="h-full rounded-full bg-emerald-500 transition-all duration-500" style={{ width: `${Math.min(100, data.publishing_stake_pct)}%` }} />
                </div>
              </div>
              {Object.entries(globalStats.leads).map(([competitor, lead]) => (
                <div key={competitor}>
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">DZ Edge vs {FEED_LABELS[competitor] ?? competitor}</span>
                    <span className="text-sm font-medium tabular-nums ml-4 shrink-0">{formatMs(lead.p50)}</span>
                  </div>
                  <div className="text-xs text-muted-foreground mt-0.5">p95: {formatMs(lead.p95)}</div>
                </div>
              ))}
            </div>

            {/* Right: gauge */}
            <div className="border-l border-border px-8 flex items-center justify-center shrink-0">
              <WinRateGauge value={globalStats.winRate} />
            </div>
          </div>
        )}

        {/* Win Rate by Slot chart */}
        {data?.nodes && (
          <div className="mb-6">
            <Section
              title="Win Rate by Slot"
              defaultOpen={true}
              action={
                <div className="flex items-center gap-2">
                  <div className="inline-flex rounded-md border border-border overflow-hidden text-xs h-[26px]">
                    <button
                      onClick={() => setGranular(false)}
                      className={cn('px-2.5 transition-colors', !granular ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground')}
                    >
                      DZ vs Other
                    </button>
                    <button
                      onClick={() => setGranular(true)}
                      className={cn('px-2.5 transition-colors border-l border-border', granular ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground')}
                    >
                      All Feeds
                    </button>
                  </div>
                  <div className="w-px h-4 bg-border mx-1" />
                  {!bucketed && live && viewEndSlot !== null && (
                    <button
                      onClick={() => scrollToLiveRef.current?.()}
                      className="text-[#10b981] hover:text-[#059669] transition-colors"
                    >
                      <ChevronsRight size={16} />
                    </button>
                  )}
                  {!bucketed && (
                    <button
                      onClick={() => toggleLiveRef.current?.()}
                      className={cn(
                        'text-xs px-2.5 h-[26px] rounded-md border transition-colors',
                        live && viewEndSlot === null
                          ? 'border-[#10b981] bg-[#10b981]/10 text-[#10b981] hover:bg-[#059669]/20'
                          : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
                      )}
                    >
                      <span className="flex items-center gap-1.5 whitespace-nowrap">
                        Live
                        {live && viewEndSlot === null
                          ? <Square size={10} className="fill-current shrink-0" />
                          : <Play size={10} className="fill-current shrink-0" />
                        }
                      </span>
                    </button>
                  )}
                  <div className="inline-flex rounded-md border border-border overflow-hidden text-xs h-[26px]">
                    <button
                      onClick={() => { setBucketed(false); scrollToLiveRef.current?.() }}
                      className={cn(
                        'px-2.5 transition-colors',
                        !bucketed ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                      )}
                    >
                      Per Slot
                    </button>
                    <button
                      onClick={() => { setBucketed(true); setLive(false) }}
                      className={cn(
                        'px-2.5 transition-colors border-l border-border',
                        bucketed ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                      )}
                    >
                      Trend
                    </button>
                  </div>
                </div>
              }
            >
              <RecentSlotsChart
                slots={data.recent_slots ?? []}
                nodes={data.nodes}
                slotLeaders={stableRecent?.leaders}
                leadersOnly={leadersOnly}
                slotBuckets={data.slot_buckets}
                slotBucketSize={data.slot_bucket_size}
                window={activeWindow}
                bucketed={bucketed}
                setBucketed={setBucketed}
                live={live}
                setLive={setLive}
                viewSlotCount={viewSlotCount}
                setViewSlotCount={setViewSlotCount}
                bare
                granular={granular}
                scrollToLiveRef={scrollToLiveRef}
                toggleLiveRef={toggleLiveRef}
                onViewEndSlotChange={setViewEndSlot}
              />
            </Section>
          </div>
        )}

        {/* Node detail table */}
        <div className="border border-border rounded-lg overflow-hidden bg-card mb-6">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium">Node</th>
                  <th className="px-4 py-3 font-medium text-right">DZ Edge Win Rate %</th>
                  <th className="px-4 py-3 font-medium text-right">vs Jito Shredstream<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-4 py-3 font-medium text-right">vs Turbine<span className="block font-normal text-xs">p50 (p95)</span></th>
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
                    <NodeRow key={node.host} node={node} label={nodeDisplayLabel(node, data?.nodes ?? [])} />
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

function NodeRow({ node, label }: { node: EdgeScoreboardNode; label: string }) {
  const [showTooltip, setShowTooltip] = useState(false)
  const cellRef = useRef<HTMLDivElement>(null)
  const [tooltipAbove, setTooltipAbove] = useState(true)
  const dz = node.feeds['dz']
  const edgeFirstArrival = node.feeds['dz_edge']?.win_rate_pct ?? 0

  // Build lead time lookup: loser_feed -> { p50, p95 }
  const dzLeadByFeed: Record<string, { p50: number; p95: number }> = {}
  if (dz?.lead_times) {
    for (const lt of dz.lead_times) {
      dzLeadByFeed[lt.loser_feed] = { p50: lt.p50_ms, p95: lt.p95_ms }
    }
  }

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
      <td className="px-4 py-3 text-right tabular-nums text-sm">
        {dz ? (
          <>
            {formatPct(edgeFirstArrival)}
            <div className="h-1 rounded-full bg-muted-foreground/25 overflow-hidden mt-1.5">
              <div className="h-full rounded-full bg-emerald-500 transition-all duration-500" style={{ width: `${Math.min(100, edgeFirstArrival)}%` }} />
            </div>
          </>
        ) : '—'}
      </td>
      {['jito', 'turbine'].map(f => {
        const lt = dzLeadByFeed[f]
        return (
          <td key={f} className="px-4 py-3 text-right tabular-nums text-sm">
            {lt ? <>{formatMs(lt.p50)} <span className="text-muted-foreground">({formatMs(lt.p95)})</span></> : '—'}
          </td>
        )
      })}
    </tr>
  )
}
