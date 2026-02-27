import { useState, useMemo, useCallback, useRef, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams, Link } from 'react-router-dom'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { Loader2, Radio, AlertCircle, ArrowLeft, ChevronUp, ChevronDown, X, Info, Search } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid } from 'recharts'
import { fetchMulticastGroup, fetchMulticastGroupMembers, fetchMulticastGroupTraffic, fetchMulticastGroupMemberCounts, type MulticastMember } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { InlineFilter } from '@/components/inline-filter'
import { Pagination } from '@/components/pagination'

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function formatAxisBps(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)}T`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)}G`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)}M`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)}K`
  return `${bps.toFixed(0)}`
}

function formatPps(pps: number): string {
  if (pps === 0) return '—'
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(1)} Gpps`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)} Mpps`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(1)} Kpps`
  return `${pps.toFixed(0)} pps`
}

type TrafficMetric = 'throughput' | 'packets'

function formatStake(sol: number): string {
  if (sol === 0) return '—'
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M SOL`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(1)}K SOL`
  return `${sol.toFixed(0)} SOL`
}

function formatTime(timeStr: string): string {
  const d = new Date(timeStr)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

function formatSlotDelta(slotDelta: number): string {
  const seconds = Math.abs(slotDelta) * 0.4
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${(seconds / 3600).toFixed(1)}h`
}

function leaderTimingText(member: MulticastMember): string | null {
  if (!member.current_slot) return null
  if (member.is_leader) return 'Leading now'
  const parts: string[] = []
  if (member.last_leader_slot != null) {
    parts.push(`Leader ${formatSlotDelta(member.current_slot - member.last_leader_slot)} ago`)
  }
  if (member.next_leader_slot != null) {
    parts.push(`Next in ${formatSlotDelta(member.next_leader_slot - member.current_slot)}`)
  }
  return parts.length > 0 ? parts.join(' · ') : null
}

const TRAFFIC_COLORS = [
  '#7c5cbf', '#4a8fe7', '#3dad6f', '#d4854a', '#2ba3a8', '#c4a23d', '#c45fa0', '#6ba8f2',
]

const TIME_RANGES = ['1h', '6h', '12h', '24h'] as const
const BUCKET_OPTIONS = ['auto', '2s', '10s', '30s', '1m', '2m', '5m', '10m'] as const

function MulticastTrafficChart({ groupCode, members, activeTab, onHoverMember }: {
  groupCode: string
  members: MulticastMember[]
  activeTab: 'publishers' | 'subscribers'
  onHoverMember?: (seriesKey: string | null) => void
}) {
  const [timeRange, setTimeRange] = useState<string>('1h')
  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<string>('auto')

  const autoBucketLabel: Record<string, string> = { '1h': '10s', '6h': '2m', '12h': '5m', '24h': '10m' }

  const bucketSeconds = bucket === 'auto' ? undefined : bucket.endsWith('m')
    ? String(parseInt(bucket) * 60)
    : String(parseInt(bucket))

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['multicast-traffic', groupCode, timeRange, bucket],
    queryFn: () => fetchMulticastGroupTraffic(groupCode, timeRange, bucketSeconds),
    refetchInterval: 30000,
  })

  // Build a lookup from device_pk+tunnel_id to display info
  const seriesInfo = useMemo(() => {
    const map = new Map<string, { ownerPubkey: string; nodePubkey: string; code: string; tunnelId: number; mode: string }>()
    for (const m of members) {
      if (m.tunnel_id > 0) {
        const key = `${m.device_pk}_${m.tunnel_id}`
        if (!map.has(key)) {
          const effectiveMode = m.mode === 'P+S' ? 'P' : m.mode
          map.set(key, {
            ownerPubkey: m.owner_pubkey,
            nodePubkey: m.node_pubkey,
            code: m.device_code || m.device_pk.slice(0, 8),
            tunnelId: m.tunnel_id,
            mode: effectiveMode,
          })
        }
      }
    }
    return map
  }, [members])

  const { chartData, seriesKeys } = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return { chartData: [], seriesKeys: [] as string[] }

    const showPubs = activeTab === 'publishers'
    const keys = new Set<string>()
    const timeMap = new Map<string, Record<string, string | number>>()

    for (const p of trafficData) {
      const isPub = p.mode === 'P'
      if (isPub !== showPubs) continue

      const seriesKey = `${p.device_pk}_${p.tunnel_id}`
      keys.add(seriesKey)

      let row = timeMap.get(p.time)
      if (!row) {
        row = { time: p.time } as Record<string, string | number>
        timeMap.set(p.time, row)
      }
      // Device counters: in = arriving at device (publisher sends), out = leaving device (to subscriber)
      if (metric === 'throughput') {
        row[seriesKey] = showPubs ? p.in_bps : p.out_bps
      } else {
        row[seriesKey] = showPubs ? p.in_pps : p.out_pps
      }
    }

    for (const row of timeMap.values()) {
      for (const k of keys) {
        if (!(k in row)) row[k] = 0
      }
    }

    // Only include series that have a matching member
    const memberKeys = new Set(members.filter(m => m.tunnel_id > 0).map(m => `${m.device_pk}_${m.tunnel_id}`))
    const filteredKeys = [...keys].filter(k => memberKeys.has(k)).sort()

    const data = [...timeMap.values()].sort((a, b) =>
      String(a.time).localeCompare(String(b.time))
    )
    return { chartData: data, seriesKeys: filteredKeys }
  }, [trafficData, activeTab, metric, members])

  const getSeriesColor = (key: string) => {
    const idx = seriesKeys.indexOf(key)
    return TRAFFIC_COLORS[idx % TRAFFIC_COLORS.length]
  }

  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const legend = useChartLegend()

  // Legend: resizable, searchable, sortable
  const LEGEND_HEADER_HEIGHT = 60 // header + column headers + padding
  const LEGEND_ROW_HEIGHT = 22
  const LEGEND_HANDLE_HEIGHT = 12
  const [legendMaxHeight, setLegendMaxHeight] = useState(256)
  const legendContainerRef = useRef<HTMLDivElement>(null)
  const [legendSearchExpanded, setLegendSearchExpanded] = useState(false)
  const [legendSearchText, setLegendSearchText] = useState('')
  const legendSearchInputRef = useRef<HTMLInputElement>(null)
  const [legendSortBy, setLegendSortBy] = useState<'name' | 'value'>('value')
  const [legendSortDir, setLegendSortDir] = useState<'asc' | 'desc'>('desc')

  const handleLegendResizeStart = (e: React.MouseEvent) => {
    e.preventDefault()
    const startY = e.clientY
    const startHeight = legendMaxHeight
    const handleMouseMove = (e: MouseEvent) => {
      const newHeight = Math.max(128, Math.min(640, startHeight + (e.clientY - startY)))
      setLegendMaxHeight(newHeight)
    }
    const handleMouseUp = () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
    document.body.style.cursor = 'ns-resize'
    document.body.style.userSelect = 'none'
  }

  const handleLegendResizeDoubleClick = () => {
    if (legendMaxHeight <= 138) {
      setLegendMaxHeight(256)
    } else {
      setLegendMaxHeight(128)
    }
  }

  // Notify parent of hovered member's device_pk
  const prevHoveredKey = useRef<string | null>(null)
  if (legend.hoveredSeries !== prevHoveredKey.current) {
    prevHoveredKey.current = legend.hoveredSeries
    if (legend.hoveredSeries) {
      onHoverMember?.(legend.hoveredSeries)
    } else {
      onHoverMember?.(null)
    }
  }

  // Snap-to-peak: find the index with the highest value in a window around the hovered point.
  // Window scales with data density — 5% of total points in each direction, clamped to [5, 150].
  const effectiveIdx = useMemo(() => {
    if (hoveredIdx === null) return null
    if (chartData.length === 0) return hoveredIdx

    const peakWindow = Math.min(150, Math.max(5, Math.round(chartData.length * 0.05)))
    const lo = Math.max(0, hoveredIdx - peakWindow)
    const hi = Math.min(chartData.length - 1, hoveredIdx + peakWindow)
    let bestIdx = hoveredIdx
    let bestVal = -Infinity

    for (let i = lo; i <= hi; i++) {
      const row = chartData[i]
      let rowMax = 0
      for (const key of seriesKeys) {
        const val = (row[key] as number) ?? 0
        rowMax = Math.max(rowMax, val)
      }
      if (rowMax > bestVal) {
        bestVal = rowMax
        bestIdx = i
      }
    }
    return bestIdx
  }, [hoveredIdx, chartData, seriesKeys])

  // Latest values (last data point) — used for stable sort order
  const latestValues = useMemo(() => {
    if (chartData.length === 0) return new Map<string, number>()
    const row = chartData[chartData.length - 1]
    const map = new Map<string, number>()
    for (const key of seriesKeys) {
      map.set(key, (row[key] as number) ?? 0)
    }
    return map
  }, [chartData, seriesKeys])

  // Display values (hovered or latest) — shown in legend Rate column
  const displayValues = useMemo(() => {
    if (effectiveIdx === null || chartData.length === 0) return latestValues
    const row = chartData[effectiveIdx]
    const map = new Map<string, number>()
    for (const key of seriesKeys) {
      map.set(key, (row[key] as number) ?? 0)
    }
    return map
  }, [chartData, seriesKeys, effectiveIdx, latestValues])

  const hoveredTime = useMemo(() => {
    if (hoveredIdx === null || hoveredIdx >= chartData.length) return null
    const t = chartData[hoveredIdx].time as string
    if (!t) return null
    const d = new Date(t)
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })
  }, [hoveredIdx, chartData])

  // Compute Y-axis domain based on visible series only
  const yDomain = useMemo((): [number, number] => {
    if (chartData.length === 0) return [0, 0]
    const visibleKeys = legend.selectedSeries.size === 0
      ? seriesKeys
      : seriesKeys.filter(k => legend.selectedSeries.has(k))
    if (visibleKeys.length === 0) return [0, 0]
    let max = 0
    for (const row of chartData) {
      for (const k of visibleKeys) {
        const v = (row[k] as number) ?? 0
        if (v > max) max = v
      }
    }
    return [0, max || 1]
  }, [chartData, seriesKeys, legend.selectedSeries])

  const fmtValue = metric === 'throughput' ? formatBps : formatPps
  const fmtAxis = (v: number) => formatAxisBps(v)

  // Legend: filter by search text, then sort
  const legendFilteredKeys = useMemo(() => {
    let keys = seriesKeys
    if (legendSearchText) {
      const needle = legendSearchText.toLowerCase()
      keys = keys.filter(key => {
        const info = seriesInfo.get(key)
        const ownerPk = info?.ownerPubkey ?? ''
        const nodePk = info?.nodePubkey ?? ''
        const code = info?.code ?? ''
        return ownerPk.toLowerCase().includes(needle) ||
          nodePk.toLowerCase().includes(needle) ||
          code.toLowerCase().includes(needle) ||
          key.toLowerCase().includes(needle)
      })
    }
    const sorted = [...keys].sort((a, b) => {
      if (legendSortBy === 'value') {
        const va = latestValues.get(a) ?? 0
        const vb = latestValues.get(b) ?? 0
        return legendSortDir === 'desc' ? vb - va : va - vb
      }
      const infoA = seriesInfo.get(a)
      const infoB = seriesInfo.get(b)
      const labelA = infoA?.ownerPubkey ?? a
      const labelB = infoB?.ownerPubkey ?? b
      return legendSortDir === 'asc' ? labelA.localeCompare(labelB) : labelB.localeCompare(labelA)
    })
    return sorted
  }, [seriesKeys, legendSearchText, legendSortBy, legendSortDir, seriesInfo, latestValues])

  const legendHeight = useMemo(() => {
    const contentHeight = LEGEND_HEADER_HEIGHT + legendFilteredKeys.length * LEGEND_ROW_HEIGHT + LEGEND_HANDLE_HEIGHT
    return Math.min(legendMaxHeight, contentHeight)
  }, [legendMaxHeight, legendFilteredKeys.length])

  return (
    <div className="border border-border rounded-lg p-4 bg-card">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">
          Traffic ({activeTab})
          {hoveredTime && <span className="ml-2 text-foreground tabular-nums">{hoveredTime}</span>}
        </h3>
        <div className="flex items-center gap-2">
          <select
            value={metric}
            onChange={e => setMetric(e.target.value as TrafficMetric)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            <option value="throughput">bps</option>
            <option value="packets">pps</option>
          </select>
          <select
            value={bucket}
            onChange={e => setBucket(e.target.value)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {BUCKET_OPTIONS.map(b => (
              <option key={b} value={b}>{b === 'auto' ? `auto (${autoBucketLabel[timeRange] || '30s'})` : b}</option>
            ))}
          </select>
          <select
            value={timeRange}
            onChange={e => setTimeRange(e.target.value)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {TIME_RANGES.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin mr-2" />
          Loading traffic data...
        </div>
      )}

      {!isLoading && chartData.length === 0 && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          No traffic data available
        </div>
      )}

      {!isLoading && chartData.length > 0 && (
        <div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={chartData}
                onMouseMove={(state) => {
                  if (state?.activeTooltipIndex != null) setHoveredIdx(Number(state.activeTooltipIndex))
                }}
                onMouseLeave={() => setHoveredIdx(null)}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />
                <XAxis
                  dataKey="time"
                  tick={{ fontSize: 9 }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={formatTime}
                />
                <YAxis
                  tick={{ fontSize: 9 }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={fmtAxis}
                  width={45}
                  domain={yDomain}
                  allowDataOverflow={true}
                />
                <RechartsTooltip
                  content={() => null}
                  cursor={{ stroke: 'var(--muted-foreground)', strokeWidth: 1, strokeDasharray: '4 2' }}
                />
                {seriesKeys.map(key => (
                  <Line
                    key={key}
                    type="monotone"
                    dataKey={key}
                    stroke={getSeriesColor(key)}
                    strokeWidth={1.5}
                    strokeOpacity={legend.getOpacity(key)}
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
          {seriesKeys.length > 0 && (
            <div ref={legendContainerRef} className="relative mt-2" style={{ height: `${legendHeight}px` }}>
              <div className="flex flex-col h-full text-xs">
                {/* Sticky header */}
                <div className="flex-none px-2 pt-2">
                  <div className="flex items-center gap-2 mb-1.5">
                    <div className="text-xs font-medium whitespace-nowrap">
                      Series ({legendFilteredKeys.length}/{seriesKeys.length})
                    </div>
                    {legendSearchExpanded ? (
                      <div className="relative flex-1">
                        <input
                          ref={legendSearchInputRef}
                          type="text"
                          value={legendSearchText}
                          onChange={(e) => setLegendSearchText(e.target.value)}
                          onBlur={() => { if (!legendSearchText) setLegendSearchExpanded(false) }}
                          placeholder="Filter by owner, node, device..."
                          className="w-full px-1.5 py-0.5 pr-6 text-xs bg-transparent border-b border-border focus:outline-none focus:border-foreground placeholder:text-muted-foreground/60"
                        />
                        {legendSearchText && (
                          <button
                            onClick={() => { setLegendSearchText(''); legendSearchInputRef.current?.focus() }}
                            className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10"
                            aria-label="Clear search"
                          >
                            <X className="h-3 w-3" />
                          </button>
                        )}
                      </div>
                    ) : (
                      <button
                        onClick={() => { setLegendSearchExpanded(true); setTimeout(() => legendSearchInputRef.current?.focus(), 0) }}
                        className="text-muted-foreground hover:text-foreground"
                        aria-label="Search series"
                      >
                        <Search className="h-3.5 w-3.5" />
                      </button>
                    )}
                    <button
                      onClick={() => legend.setSelectedSeries(new Set())}
                      className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
                    >
                      All
                    </button>
                    <button
                      onClick={() => legend.setSelectedSeries(new Set(['__none__']))}
                      className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
                    >
                      None
                    </button>
                    <div className="relative group flex-shrink-0">
                      <Info className="h-3 w-3 text-muted-foreground/50 group-hover:text-muted-foreground cursor-help" />
                      <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-1 hidden group-hover:block z-50 pointer-events-none">
                        <div className="bg-[var(--popover)] text-[var(--popover-foreground)] border border-[var(--border)] rounded-md px-2 py-1.5 text-[10px] leading-relaxed whitespace-nowrap shadow-md">
                          <div><strong>Click</strong> — solo select</div>
                          <div><strong>{navigator.platform.includes('Mac') ? 'Cmd' : 'Ctrl'}+click</strong> — toggle</div>
                        </div>
                      </div>
                    </div>
                  </div>
                  {/* Column headers */}
                  <div className="flex items-center gap-4 px-1 mb-1">
                    <div className="w-2.5" />
                    <button
                      onClick={() => { setLegendSortBy('name'); setLegendSortDir(legendSortBy === 'name' ? (legendSortDir === 'asc' ? 'desc' : 'asc') : 'asc') }}
                      className="flex-1 min-w-0 flex items-center gap-0.5 text-xs text-muted-foreground hover:text-foreground font-medium"
                    >
                      Owner
                      {legendSortBy === 'name' && (legendSortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
                    </button>
                    <div className="w-20 text-right text-xs text-muted-foreground font-medium whitespace-nowrap">Node</div>
                    <button
                      onClick={() => { setLegendSortBy('value'); setLegendSortDir(legendSortBy === 'value' ? (legendSortDir === 'asc' ? 'desc' : 'asc') : 'desc') }}
                      className="w-20 flex items-center justify-end gap-0.5 text-xs text-muted-foreground hover:text-foreground font-medium"
                    >
                      Rate
                      {legendSortBy === 'value' && (legendSortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
                    </button>
                  </div>
                </div>
                {/* Scrollable items */}
                <div className="flex-1 overflow-y-auto px-2 pb-2">
                  <div className="space-y-0.5">
                    {legendFilteredKeys.map((key) => {
                      const i = seriesKeys.indexOf(key)
                      const info = seriesInfo.get(key)
                      const val = displayValues.get(key)
                      const opacity = legend.getOpacity(key)
                      const isSelected = legend.selectedSeries.size === 0 || legend.selectedSeries.has(key)
                      const ownerLabel = info?.ownerPubkey
                        ? `${info.ownerPubkey.slice(0, 4)}..${info.ownerPubkey.slice(-4)}`
                        : key.split('_')[0].slice(0, 8)
                      const nodeLabel = info?.nodePubkey
                        ? `${info.nodePubkey.slice(0, 4)}..${info.nodePubkey.slice(-4)}`
                        : '—'
                      return (
                        <div
                          key={key}
                          className="flex items-center gap-4 px-1 py-0.5 rounded cursor-pointer select-none transition-opacity hover:bg-muted/60"
                          style={{ opacity: Math.max(opacity, 0.3) }}
                          onClick={(e) => legend.handleClick(key, e)}
                          onMouseEnter={() => legend.handleMouseEnter(key)}
                          onMouseLeave={() => legend.handleMouseLeave()}
                        >
                          <div
                            className="w-2.5 h-2.5 rounded-sm flex-shrink-0 transition-colors"
                            style={{ backgroundColor: isSelected ? TRAFFIC_COLORS[i % TRAFFIC_COLORS.length] : 'var(--border)' }}
                          />
                          <div className="flex-1 min-w-0 text-foreground truncate font-mono">
                            {ownerLabel}
                            <span className="text-muted-foreground ml-2">{info?.code ?? key.split('_')[0].slice(0, 8)}{info?.tunnelId ? ` / ${info.tunnelId}` : ''}</span>
                          </div>
                          <div className="w-20 text-right tabular-nums font-mono text-muted-foreground">{nodeLabel}</div>
                          <div className="w-20 text-right tabular-nums">{val !== undefined && opacity > 0 ? fmtValue(val) : '—'}</div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              </div>
              {/* Resize handle */}
              <div
                onMouseDown={handleLegendResizeStart}
                onDoubleClick={handleLegendResizeDoubleClick}
                className="absolute bottom-0 left-0 right-0 h-3 cursor-ns-resize hover:bg-muted transition-colors flex items-center justify-center"
              >
                <div className="w-12 h-1 bg-border rounded-full" />
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const MEMBER_COUNT_TIME_RANGES = ['1h', '6h', '12h', '24h', '7d', '30d'] as const

function MemberCountChart({ groupCode }: { groupCode: string }) {
  const [timeRange, setTimeRange] = useState<string>('7d')
  const [hiddenSeries, setHiddenSeries] = useState<Set<string>>(new Set())

  const toggleSeries = (key: string) => {
    setHiddenSeries(prev => {
      const next = new Set(prev)
      if (next.has(key)) {
        next.delete(key)
      } else {
        next.add(key)
      }
      return next
    })
  }

  const { data: countData, isLoading } = useQuery({
    queryKey: ['multicast-member-counts', groupCode, timeRange],
    queryFn: () => fetchMulticastGroupMemberCounts(groupCode, timeRange),
    refetchInterval: 30000,
  })

  const chartData = useMemo(() => {
    if (!countData || countData.length === 0) return []
    const raw = countData.map(p => ({
      time: new Date(p.time).getTime(),
      publishers: p.publisher_count,
      subscribers: p.subscriber_count,
    }))
    if (raw.length < 2) return raw.map(p => ({ ...p, time: new Date(p.time).toISOString() }))
    // Densify: fill in points at regular intervals so tooltip hovers smoothly.
    // With stepAfter, inserted points carry the last value forward and don't change the visual.
    const first = raw[0].time
    const last = raw[raw.length - 1].time
    const span = last - first
    const maxPoints = 200
    const interval = Math.max(span / maxPoints, 60000) // at least 1 min apart
    const dense: { time: string; publishers: number; subscribers: number }[] = []
    let ri = 0
    let curPub = raw[0].publishers
    let curSub = raw[0].subscribers
    for (let t = first; t <= last; t += interval) {
      // Advance past any raw points at or before t
      while (ri < raw.length && raw[ri].time <= t) {
        curPub = raw[ri].publishers
        curSub = raw[ri].subscribers
        ri++
      }
      dense.push({ time: new Date(t).toISOString(), publishers: curPub, subscribers: curSub })
    }
    // Ensure the last raw point is included
    if (dense.length === 0 || new Date(dense[dense.length - 1].time).getTime() < last) {
      dense.push({ time: new Date(last).toISOString(), publishers: raw[raw.length - 1].publishers, subscribers: raw[raw.length - 1].subscribers })
    }
    return dense
  }, [countData])

  const yDomain = useMemo((): [number, number] => {
    if (chartData.length === 0) return [0, 1]
    const showPub = !hiddenSeries.has('publishers')
    const showSub = !hiddenSeries.has('subscribers')
    if (!showPub && !showSub) return [0, 1]
    let min = Infinity
    let max = 0
    for (const row of chartData) {
      if (showPub) {
        if (row.publishers > max) max = row.publishers
        if (row.publishers < min) min = row.publishers
      }
      if (showSub) {
        if (row.subscribers > max) max = row.subscribers
        if (row.subscribers < min) min = row.subscribers
      }
    }
    if (!isFinite(min)) min = 0
    // Add padding so flat lines aren't hugged to the edges
    const range = max - min
    const pad = range > 0 ? Math.ceil(range * 0.2) : Math.max(1, Math.ceil(max * 0.1))
    return [Math.max(0, min - pad), max + pad]
  }, [chartData, hiddenSeries])

  return (
    <div className="border border-border rounded-lg p-4 bg-card mt-6">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">Members Over Time</h3>
        <select
          value={timeRange}
          onChange={e => setTimeRange(e.target.value)}
          className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
        >
          {MEMBER_COUNT_TIME_RANGES.map(r => (
            <option key={r} value={r}>{r}</option>
          ))}
        </select>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center h-40 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin mr-2" />
          Loading member counts...
        </div>
      )}

      {!isLoading && chartData.length === 0 && (
        <div className="flex items-center justify-center h-40 text-sm text-muted-foreground">
          No member count data available
        </div>
      )}

      {!isLoading && chartData.length > 0 && (
        <div>
          <ResponsiveContainer width="100%" height={160} className="outline-none [&_svg]:outline-none [&_*]:outline-none">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 9 }}
                tickLine={false}
                axisLine={false}
                tickFormatter={formatTime}
              />
              <YAxis
                tick={{ fontSize: 9 }}
                tickLine={false}
                axisLine={false}
                width={35}
                domain={yDomain}
                allowDecimals={false}
              />
              <RechartsTooltip
                contentStyle={{
                  backgroundColor: 'var(--card)',
                  border: '1px solid var(--border)',
                  borderRadius: '6px',
                  fontSize: '12px',
                }}
                labelFormatter={(label) => {
                  const d = new Date(label)
                  return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
                }}
              />
              <Line
                type="stepAfter"
                dataKey="publishers"
                name="Publishers"
                stroke="#7c5cbf"
                strokeWidth={1.5}
                strokeOpacity={hiddenSeries.has('publishers') ? 0 : 1}
                dot={false}
                isAnimationActive={false}
              />
              <Line
                type="stepAfter"
                dataKey="subscribers"
                name="Subscribers"
                stroke="#4a8fe7"
                strokeWidth={1.5}
                strokeOpacity={hiddenSeries.has('subscribers') ? 0 : 1}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
          <div className="flex items-center justify-center gap-4 mt-2 text-xs text-muted-foreground">
            <button
              type="button"
              className="flex items-center gap-1.5 cursor-pointer hover:opacity-80 transition-opacity"
              style={{ opacity: hiddenSeries.has('publishers') ? 0.4 : 1 }}
              onClick={() => toggleSeries('publishers')}
            >
              <div className="w-3 h-0.5 rounded-full" style={{ backgroundColor: '#7c5cbf' }} />
              <span style={{ textDecoration: hiddenSeries.has('publishers') ? 'line-through' : 'none' }}>Publishers</span>
            </button>
            <button
              type="button"
              className="flex items-center gap-1.5 cursor-pointer hover:opacity-80 transition-opacity"
              style={{ opacity: hiddenSeries.has('subscribers') ? 0.4 : 1 }}
              onClick={() => toggleSeries('subscribers')}
            >
              <div className="w-3 h-0.5 rounded-full" style={{ backgroundColor: '#4a8fe7' }} />
              <span style={{ textDecoration: hiddenSeries.has('subscribers') ? 'line-through' : 'none' }}>Subscribers</span>
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

type MemberSortField = 'owner_pubkey' | 'node_pubkey' | 'device_code' | 'metro_name' | 'dz_ip' | 'tunnel_id' | 'stake_sol' | 'leader_schedule'
type SortDirection = 'asc' | 'desc'

const DEFAULT_PAGE_SIZE = 10
const PAGE_SIZE_OPTIONS = [10, 20, 50, 100]

const validMemberFilterFields = ['device', 'metro', 'owner']

const memberFieldPrefixes = [
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'metro:', description: 'Filter by metro name' },
  { prefix: 'owner:', description: 'Filter by owner pubkey' },
]

const memberAutocompleteFields = ['device', 'metro']

function parseMemberSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

function parseMemberFilter(filter: string): { field: string; value: string } {
  const colonIndex = filter.indexOf(':')
  if (colonIndex > 0) {
    const field = filter.slice(0, colonIndex).toLowerCase()
    const value = filter.slice(colonIndex + 1)
    if (validMemberFilterFields.includes(field) && value) {
      return { field, value }
    }
  }
  return { field: 'all', value: filter }
}

function getMemberSearchValue(member: MulticastMember, field: string): string {
  switch (field) {
    case 'device':
      return `${member.device_code} ${member.device_pk}`
    case 'metro':
      return `${member.metro_name} ${member.metro_code}`
    case 'owner':
      return member.owner_pubkey
    default:
      return ''
  }
}

export function MulticastGroupDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = (searchParams.get('tab') === 'subscribers' ? 'subscribers' : 'publishers') as 'publishers' | 'subscribers'
  const sortField = (searchParams.get('sort') || 'stake_sol') as MemberSortField
  const sortDirection = (searchParams.get('dir') || 'desc') as SortDirection
  const page = parseInt(searchParams.get('page') || '1')
  const pageSizeParam = parseInt(searchParams.get('limit') || '0')
  const pageSize = PAGE_SIZE_OPTIONS.includes(pageSizeParam) ? pageSizeParam : DEFAULT_PAGE_SIZE
  const offset = (page - 1) * pageSize
  const [liveFilter, setLiveFilter] = useState('')
  const [hoveredSeriesKey, setHoveredSeriesKey] = useState<string | null>(null)

  const setActiveTab = useCallback((tab: 'publishers' | 'subscribers') => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (tab === 'publishers') { p.delete('tab') } else { p.set('tab', tab) }
      p.delete('page') // Reset to page 1
      return p
    })
  }, [setSearchParams])

  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      const curSize = parseInt(newParams.get('limit') || '0')
      const effectiveSize = PAGE_SIZE_OPTIONS.includes(curSize) ? curSize : DEFAULT_PAGE_SIZE
      const newPage = Math.floor(newOffset / effectiveSize) + 1
      if (newPage <= 1) {
        newParams.delete('page')
      } else {
        newParams.set('page', String(newPage))
      }
      return newParams
    })
  }, [setSearchParams])

  const setPageSize = useCallback((size: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (size === DEFAULT_PAGE_SIZE) {
        p.delete('limit')
      } else {
        p.set('limit', String(size))
      }
      p.delete('page') // Reset to page 1
      return p
    })
  }, [setSearchParams])

  const handleSort = (field: MemberSortField) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      let newField = field
      let newDir: SortDirection = 'desc'
      if (sortField === field) {
        if (sortDirection === 'desc') {
          newDir = 'asc'
        } else {
          // Already asc — reset to default
          newField = 'stake_sol'
          newDir = 'desc'
        }
      }
      if (newField === 'stake_sol' && newDir === 'desc') {
        p.delete('sort')
        p.delete('dir')
      } else {
        p.set('sort', newField)
        p.set('dir', newDir)
      }
      p.delete('page') // Reset to page 1
      return p
    })
  }

  const SortIcon = ({ field }: { field: MemberSortField }) => {
    if (sortField !== field) return null
    return sortDirection === 'asc'
      ? <ChevronUp className="h-3 w-3" />
      : <ChevronDown className="h-3 w-3" />
  }

  const sortAria = (field: MemberSortField) => {
    if (sortField !== field) return 'none' as const
    return sortDirection === 'asc' ? 'ascending' as const : 'descending' as const
  }

  // Filter state from URL
  const searchParam = searchParams.get('search') || ''
  const searchFilters = parseMemberSearchFilters(searchParam)
  const allFilters = liveFilter ? [...searchFilters, liveFilter] : searchFilters

  // First filter is sent server-side, rest applied client-side
  const serverFilterRaw = allFilters[0] || ''
  const serverFilter = serverFilterRaw ? parseMemberFilter(serverFilterRaw) : null
  const clientFilters = allFilters.slice(1)

  const removeFilter = useCallback((filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      if (newFilters.length === 0) {
        newParams.delete('search')
      } else {
        newParams.set('search', newFilters.join(','))
      }
      newParams.delete('page') // Reset to page 1
      return newParams
    })
  }, [searchFilters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('search')
      newParams.delete('page')
      return newParams
    })
  }, [setSearchParams])

  // Reset page when filters change
  const prevFilterRef = useRef(JSON.stringify(allFilters))
  useEffect(() => {
    const key = JSON.stringify(allFilters)
    if (prevFilterRef.current === key) return
    prevFilterRef.current = key
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('page')
      return newParams
    })
  }, [allFilters, setSearchParams])

  // Group metadata query
  const { data: group, isLoading: groupLoading, error: groupError } = useQuery({
    queryKey: ['multicast-group', pk],
    queryFn: () => fetchMulticastGroup(pk!),
    enabled: !!pk,
    refetchInterval: 30000,
  })

  // Members query (server-side pagination)
  const { data: membersResponse, isLoading: membersLoading, isFetching: membersFetching } = useQuery({
    queryKey: ['multicast-group-members', pk, activeTab, offset, pageSize, sortField, sortDirection, serverFilterRaw],
    queryFn: () => fetchMulticastGroupMembers(
      pk!,
      pageSize,
      offset,
      sortField,
      sortDirection,
      activeTab,
      serverFilter?.field,
      serverFilter?.value
    ),
    enabled: !!pk,
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })

  useDocumentTitle(group?.code || 'Multicast Group')

  const publisherCount = membersResponse?.publisher_count ?? 0
  const subscriberCount = membersResponse?.subscriber_count ?? 0

  // Apply client-side filters (filters beyond the first)
  const activeMembers = useMemo(() => {
    const members = membersResponse?.items ?? []
    if (clientFilters.length === 0) return members

    const matchesSingleFilter = (member: MulticastMember, filterRaw: string): boolean => {
      const filter = parseMemberFilter(filterRaw)
      const needle = filter.value.trim().toLowerCase()
      if (!needle) return true

      if (filter.field === 'all') {
        const textFields = ['device', 'metro', 'owner']
        return textFields.some(f => getMemberSearchValue(member, f).toLowerCase().includes(needle))
      }

      return getMemberSearchValue(member, filter.field).toLowerCase().includes(needle)
    }

    // Group filters by field: OR within same field, AND across different fields
    const grouped = new Map<string, string[]>()
    for (const f of clientFilters) {
      const { field } = parseMemberFilter(f)
      const existing = grouped.get(field) ?? []
      existing.push(f)
      grouped.set(field, existing)
    }
    return members.filter(member =>
      Array.from(grouped.values()).every(group =>
        group.some(f => matchesSingleFilter(member, f))
      )
    )
  }, [membersResponse, clientFilters])

  if (groupLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (groupError || !group) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Multicast group not found</div>
          <button
            onClick={() => navigate('/dz/multicast-groups')}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to multicast groups
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">
        {/* Back button */}
        <button
          onClick={() => navigate('/dz/multicast-groups')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to multicast groups
        </button>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <Radio className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium font-mono">{group.code}</h1>
            <div className="text-sm text-muted-foreground font-mono">{group.multicast_ip}</div>
          </div>
        </div>

        {/* Members filter + tabs */}
        <div className="flex items-center gap-2 mb-3">
          <InlineFilter
            fieldPrefixes={memberFieldPrefixes}
            entity="multicast-members"
            autocompleteFields={memberAutocompleteFields}
            placeholder="Filter members..."
            onLiveFilterChange={setLiveFilter}
            filterParams={pk ? { group: pk } : undefined}
          />
          {searchFilters.map((filter, idx) => (
            <button
              key={`${filter}-${idx}`}
              onClick={() => removeFilter(filter)}
              className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
            >
              {filter}
              <X className="h-3 w-3" />
            </button>
          ))}
          {searchFilters.length > 1 && (
            <button
              onClick={clearAllFilters}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              Clear all
            </button>
          )}
        </div>

        {/* Members table */}
        <div className="border border-border rounded-lg bg-card mb-6">
          <div className="flex border-b border-border">
            <button
              onClick={() => setActiveTab('publishers')}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors -mb-px ${
                activeTab === 'publishers'
                  ? 'border-purple-500 text-purple-500'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              Publishers ({publisherCount})
            </button>
            <button
              onClick={() => setActiveTab('subscribers')}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors -mb-px ${
                activeTab === 'subscribers'
                  ? 'border-purple-500 text-purple-500'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              Subscribers ({subscriberCount})
            </button>
            {membersFetching && (
              <div className="flex items-center ml-2">
                <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
              </div>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('owner_pubkey')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('owner_pubkey')}>
                      Owner
                      <SortIcon field="owner_pubkey" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('node_pubkey')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('node_pubkey')}>
                      Node
                      <SortIcon field="node_pubkey" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('device_code')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('device_code')}>
                      Device
                      <SortIcon field="device_code" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('metro_name')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('metro_name')}>
                      Metro
                      <SortIcon field="metro_name" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('dz_ip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('dz_ip')}>
                      DZ IP
                      <SortIcon field="dz_ip" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('tunnel_id')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('tunnel_id')}>
                      Tunnel
                      <SortIcon field="tunnel_id" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('stake_sol')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('stake_sol')}>
                      Stake
                      <SortIcon field="stake_sol" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('leader_schedule')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('leader_schedule')}>
                      Leader Schedule
                      <SortIcon field="leader_schedule" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {membersLoading && !membersResponse && (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-muted-foreground">
                      <Loader2 className="h-5 w-5 animate-spin mx-auto" />
                    </td>
                  </tr>
                )}
                {activeMembers.map((member) => (
                  <tr
                    key={member.user_pk}
                    className={`border-b border-border last:border-b-0 hover:bg-muted transition-colors ${hoveredSeriesKey === `${member.device_pk}_${member.tunnel_id}` ? 'bg-muted' : ''}`}
                  >
                    <td className="px-4 py-3 text-sm font-mono">
                      {member.owner_pubkey ? (
                        <Link
                          to={`/dz/users/${member.user_pk}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {member.owner_pubkey.slice(0, 4)}..{member.owner_pubkey.slice(-4)}
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono">
                      {member.node_pubkey ? (
                        <Link
                          to={`/solana/gossip-nodes/${member.node_pubkey}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {member.node_pubkey.slice(0, 4)}..{member.node_pubkey.slice(-4)}
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {member.device_pk ? (
                        <Link
                          to={`/dz/devices/${member.device_pk}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline font-mono"
                        >
                          {member.device_code || member.device_pk.slice(0, 8)}
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {member.metro_pk ? (
                        <Link
                          to={`/dz/metros/${member.metro_pk}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {member.metro_name || member.metro_code}
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono text-muted-foreground">
                      {member.dz_ip || '—'}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground font-mono">
                      {member.tunnel_id > 0 ? member.tunnel_id : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {member.stake_sol > 0 ? formatStake(member.stake_sol) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {member.is_leader ? (
                        <span className="inline-flex items-center px-1.5 py-0.5 rounded-full bg-amber-500/15 text-amber-500 font-medium text-xs">
                          Leading now
                        </span>
                      ) : (
                        (() => {
                          const timing = leaderTimingText(member)
                          return timing ? (
                            <span className="text-muted-foreground">{timing}</span>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )
                        })()
                      )}
                    </td>
                  </tr>
                ))}
                {!membersLoading && activeMembers.length === 0 && (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-muted-foreground">
                      No {activeTab} found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {membersResponse && (
            <Pagination
              total={membersResponse.total}
              limit={pageSize}
              offset={offset}
              onOffsetChange={setOffset}
              pageSizeOptions={PAGE_SIZE_OPTIONS}
              onPageSizeChange={setPageSize}
            />
          )}
        </div>

        {/* Traffic chart — uses all members (not just current page) for series labels */}
        {pk && group && group.members.length > 0 && (
          <MulticastTrafficChart
            groupCode={pk}
            members={group.members}
            activeTab={activeTab}
            onHoverMember={setHoveredSeriesKey}
          />
        )}

        {/* Member count chart */}
        {pk && <MemberCountChart groupCode={pk} />}
      </div>
    </div>
  )
}
