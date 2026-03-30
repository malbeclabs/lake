import { useState, useMemo, useCallback, useRef, useEffect } from 'react'
import { useParams, useNavigate, useSearchParams, Link } from 'react-router-dom'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { Loader2, Radio, AlertCircle, ArrowLeft, ChevronUp, ChevronDown, X, Info, Search, RefreshCw } from 'lucide-react'
import uPlot from 'uplot'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { formatChartAxisRate, formatChartAxisPps } from '@/components/topology/utils'
import { fetchMulticastGroup, fetchMulticastGroupMembers, fetchMulticastGroupTraffic, fetchMulticastGroupMemberCounts, fetchMulticastGroupShredStats, type MulticastMember } from '@/lib/api'
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

const TIME_RANGES = ['1h', '3h', '6h', '12h', '24h', '3d', '7d', '14d', '30d'] as const
const BUCKET_OPTIONS = ['auto', '10 SECOND', '30 SECOND', '1 MINUTE', '5 MINUTE', '10 MINUTE', '15 MINUTE', '30 MINUTE', '1 HOUR', '4 HOUR', '12 HOUR', '1 DAY'] as const
const BUCKET_LABELS: Record<string, string> = {
  'auto': 'Auto', '10 SECOND': '10s', '30 SECOND': '30s', '1 MINUTE': '1m', '5 MINUTE': '5m',
  '10 MINUTE': '10m', '15 MINUTE': '15m', '30 MINUTE': '30m', '1 HOUR': '1h',
  '4 HOUR': '4h', '12 HOUR': '12h', '1 DAY': '1d',
}
function resolveAutoBucket(timeRange: string): string {
  switch (timeRange) {
    case '1h': return '10 SECOND'
    case '3h': return '30 SECOND'
    case '6h': return '1 MINUTE'
    case '12h': return '10 MINUTE'
    case '24h': return '15 MINUTE'
    case '3d': return '30 MINUTE'
    case '7d': return '4 HOUR'
    case '14d': return '12 HOUR'
    case '30d': return '1 DAY'
    default: return '5 MINUTE'
  }
}

function MulticastTrafficChart({ groupCode, members, activeTab, onHoverMember, onSelectMember }: {
  groupCode: string
  members: MulticastMember[]
  activeTab: 'publishers' | 'subscribers'
  onHoverMember?: (seriesKey: string | null) => void
  onSelectMember?: (keys: Set<string>) => void
}) {
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<string>('1h')
  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<string>('auto')

  const effectiveBucket = bucket === 'auto' ? resolveAutoBucket(timeRange) : bucket
  const autoBucketLabel = BUCKET_LABELS[resolveAutoBucket(timeRange)] || '5m'

  const { data: trafficData, isFetching } = useQuery({
    queryKey: ['multicast-traffic', groupCode, timeRange, effectiveBucket],
    queryFn: () => fetchMulticastGroupTraffic(groupCode, timeRange, effectiveBucket),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
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

  const { uplotData, seriesKeys } = useMemo(() => {
    if (!trafficData || trafficData.length === 0)
      return { uplotData: [new Float64Array(0)] as uPlot.AlignedData, seriesKeys: [] as string[] }

    const showPubs = activeTab === 'publishers'
    const keySet = new Set<string>()
    const timeMap = new Map<string, Map<string, number>>()

    for (const p of trafficData) {
      const isPub = p.mode === 'P'
      if (isPub !== showPubs) continue

      const seriesKey = `${p.device_pk}_${p.tunnel_id}`
      keySet.add(seriesKey)

      let byKey = timeMap.get(p.time)
      if (!byKey) {
        byKey = new Map()
        timeMap.set(p.time, byKey)
      }
      const value = metric === 'throughput'
        ? (showPubs ? p.in_bps : p.out_bps)
        : (showPubs ? p.in_pps : p.out_pps)
      byKey.set(seriesKey, value)
    }

    const memberKeys = new Set(
      members.filter(m => m.tunnel_id > 0).map(m => `${m.device_pk}_${m.tunnel_id}`)
    )
    const keys = [...keySet].filter(k => memberKeys.has(k)).sort()
    const sortedTimes = [...timeMap.keys()].sort()
    const timestamps = new Float64Array(sortedTimes.map(t => new Date(t).getTime() / 1000))
    const columns = keys.map(key =>
      new Float64Array(sortedTimes.map(t => timeMap.get(t)?.get(key) ?? 0))
    )

    return {
      uplotData: [timestamps, ...columns] as uPlot.AlignedData,
      seriesKeys: keys,
    }
  }, [trafficData, activeTab, metric, members])

  const legend = useChartLegend()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const focusedKeyRef = useRef<string | null>(null)

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

  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const displayValues = useMemo(() => {
    const map = new Map<string, number>()
    if (!uplotData[0] || uplotData[0].length === 0) return map
    const idx = hoveredIdx != null && hoveredIdx < uplotData[0].length
      ? hoveredIdx
      : uplotData[0].length - 1
    for (let i = 0; i < seriesKeys.length; i++) {
      map.set(seriesKeys[i], (uplotData[i + 1] as number[])?.[idx] ?? 0)
    }
    return map
  }, [uplotData, seriesKeys, hoveredIdx])

  const fmtValue = metric === 'throughput' ? formatBps : formatPps

  const handleCursorIdx = useCallback((idx: number | null) => {
    setHoveredIdx(idx)
  }, [])

  const handleFocusSeries = useCallback((seriesIdx: number | null) => {
    if (seriesIdx != null && seriesIdx > 0 && seriesIdx <= seriesKeys.length) {
      const key = seriesKeys[seriesIdx - 1]
      focusedKeyRef.current = key
      legend.handleMouseEnter(key)
    } else {
      focusedKeyRef.current = null
      legend.handleMouseLeave()
    }
  }, [seriesKeys, legend])

  const uplotSeries = useMemo((): uPlot.Series[] => {
    const s: uPlot.Series[] = [{}]
    for (let i = 0; i < seriesKeys.length; i++) {
      s.push({
        label: seriesKeys[i],
        stroke: TRAFFIC_COLORS[i % TRAFFIC_COLORS.length],
        width: 1.5,
        points: { show: false },
      })
    }
    return s
  }, [seriesKeys])

  const uplotAxes = useMemo((): uPlot.Axis[] => [
    {},
    {
      values: (_u: uPlot, vals: number[]) =>
        vals.map(v => metric === 'throughput' ? formatChartAxisRate(v) : formatChartAxisPps(v)),
    },
  ], [metric])

  const chartScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const { plotRef } = useUPlotChart({
    containerRef: chartContainerRef,
    data: uplotData,
    series: uplotSeries,
    height: 224,
    axes: uplotAxes,
    scales: chartScales,
    onCursorIdx: handleCursorIdx,
    onFocusSeries: handleFocusSeries,
  })

  useUPlotLegendSync(plotRef, legend, seriesKeys)

  // Click on chart: select focused series, or clear selection
  const legendRef = useRef(legend)
  legendRef.current = legend
  const handleChartClick = useCallback((e: React.MouseEvent) => {
    const leg = legendRef.current
    if (focusedKeyRef.current) {
      leg.handleClick(focusedKeyRef.current, e)
    } else if (leg.selectedSeries.size > 0) {
      leg.setSelectedSeries(new Set())
    }
  }, [])

  // Propagate selection to parent
  const prevSelectedRef = useRef<Set<string>>(legend.selectedSeries)
  useEffect(() => {
    if (prevSelectedRef.current !== legend.selectedSeries) {
      prevSelectedRef.current = legend.selectedSeries
      onSelectMember?.(legend.selectedSeries)
    }
  }, [legend.selectedSeries, onSelectMember])

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
        const va = displayValues.get(a) ?? 0
        const vb = displayValues.get(b) ?? 0
        return legendSortDir === 'desc' ? vb - va : va - vb
      }
      const infoA = seriesInfo.get(a)
      const infoB = seriesInfo.get(b)
      const labelA = infoA?.ownerPubkey ?? a
      const labelB = infoB?.ownerPubkey ?? b
      return legendSortDir === 'asc' ? labelA.localeCompare(labelB) : labelB.localeCompare(labelA)
    })
    return sorted
  }, [seriesKeys, legendSearchText, legendSortBy, legendSortDir, seriesInfo, displayValues])

  const legendHeight = useMemo(() => {
    const contentHeight = LEGEND_HEADER_HEIGHT + legendFilteredKeys.length * LEGEND_ROW_HEIGHT + LEGEND_HANDLE_HEIGHT
    return Math.min(legendMaxHeight, contentHeight)
  }, [legendMaxHeight, legendFilteredKeys.length])

  return (
    <div className="border border-border rounded-lg p-4 bg-card group/chart">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-muted-foreground">
            Traffic ({activeTab})
          </h3>
          {isFetching ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
          ) : (
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: ['multicast-traffic', groupCode] })}
              className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground"
              title="Refresh"
            >
              <RefreshCw className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
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
              <option key={b} value={b}>{b === 'auto' ? `Auto (${autoBucketLabel})` : BUCKET_LABELS[b] || b}</option>
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
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-2">
        {isFetching && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>

      {!trafficData && !isFetching && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          No traffic data available
        </div>
      )}

      {(trafficData || isFetching) && (
        <div>
          <div
            ref={chartContainerRef}
            className="h-56"
            onClick={handleChartClick}
          />
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
                    <div className="w-48 text-right text-xs text-muted-foreground font-medium whitespace-nowrap">DZ ID</div>
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
                      const dzIdLabel = info?.ownerPubkey || '—'
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
                          <div className="w-48 text-right truncate tabular-nums font-mono text-muted-foreground select-text" title={dzIdLabel}>{dzIdLabel}</div>
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

type ShredMetric = 'unique_shreds' | 'total_packets' | 'data_shreds' | 'coding_shreds' | 'leader_slots' | 'repair_slots'

const SHRED_METRIC_LABELS: Record<ShredMetric, string> = {
  unique_shreds: 'Unique Shreds',
  total_packets: 'Total Packets',
  data_shreds: 'Data Shreds',
  coding_shreds: 'Coding Shreds',
  leader_slots: 'Leader Slots',
  repair_slots: 'Repair Slots',
}

function formatCount(v: number): string {
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`
  return v.toFixed(0)
}

function formatRate(v: number): string {
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B/s`
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M/s`
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K/s`
  if (v >= 1) return `${v.toFixed(1)}/s`
  if (v > 0) return `${v.toFixed(2)}/s`
  return '0/s'
}

function bucketToSeconds(bucket: string): number {
  const match = bucket.match(/^(\d+)\s+(\w+)$/)
  if (!match) return 60
  const n = parseInt(match[1])
  switch (match[2]) {
    case 'SECOND': return n
    case 'MINUTE': return n * 60
    case 'HOUR': return n * 3600
    case 'DAY': return n * 86400
    default: return 60
  }
}

function ShredStatsChart({ groupCode, members, onHoverMember, onSelectMember }: {
  groupCode: string
  members: MulticastMember[]
  onHoverMember?: (seriesKey: string | null) => void
  onSelectMember?: (keys: Set<string>) => void
}) {
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<string>('1h')
  const [metric, setMetric] = useState<ShredMetric>('unique_shreds')
  const [bucket, setBucket] = useState<string>('auto')
  const [rateMode, setRateMode] = useState(true)

  const effectiveBucket = bucket === 'auto' ? resolveAutoBucket(timeRange) : bucket
  const autoBucketLabel = BUCKET_LABELS[resolveAutoBucket(timeRange)] || '5m'
  const bucketSeconds = bucketToSeconds(effectiveBucket)

  const { data: shredData, isFetching } = useQuery({
    queryKey: ['multicast-shred-stats', groupCode, timeRange, effectiveBucket],
    queryFn: () => fetchMulticastGroupShredStats(groupCode, timeRange, effectiveBucket),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })

  // Build a lookup from dz_user_pubkey (user_pk) to display info and member table keys
  const userInfo = useMemo(() => {
    const map = new Map<string, { ownerPubkey: string; code: string }>()
    for (const m of members) {
      if (m.mode === 'P' || m.mode === 'P+S') {
        if (!map.has(m.user_pk)) {
          map.set(m.user_pk, {
            ownerPubkey: m.owner_pubkey,
            code: m.device_code || m.device_pk.slice(0, 8),
          })
        }
      }
    }
    return map
  }, [members])

  // Map user_pk -> device_pk_tunnel_id (for table highlighting)
  const userPkToMemberKeys = useMemo(() => {
    const map = new Map<string, string[]>()
    for (const m of members) {
      if ((m.mode === 'P' || m.mode === 'P+S') && m.tunnel_id > 0) {
        const existing = map.get(m.user_pk) ?? []
        existing.push(`${m.device_pk}_${m.tunnel_id}`)
        map.set(m.user_pk, existing)
      }
    }
    return map
  }, [members])

  const { uplotData, seriesKeys } = useMemo(() => {
    if (!shredData || shredData.length === 0)
      return { uplotData: [new Float64Array(0)] as uPlot.AlignedData, seriesKeys: [] as string[] }

    const keySet = new Set<string>()
    const timeMap = new Map<string, Map<string, number>>()

    for (const p of shredData) {
      keySet.add(p.dz_user_pubkey)

      let byKey = timeMap.get(p.time)
      if (!byKey) {
        byKey = new Map()
        timeMap.set(p.time, byKey)
      }
      const raw = p[metric] as number
      byKey.set(p.dz_user_pubkey, rateMode ? raw / bucketSeconds : raw)
    }

    const keys = [...keySet].sort()
    const sortedTimes = [...timeMap.keys()].sort()
    const timestamps = new Float64Array(sortedTimes.map(t => new Date(t).getTime() / 1000))
    const columns = keys.map(key =>
      new Float64Array(sortedTimes.map(t => timeMap.get(t)?.get(key) ?? 0))
    )

    return {
      uplotData: [timestamps, ...columns] as uPlot.AlignedData,
      seriesKeys: keys,
    }
  }, [shredData, metric, rateMode, bucketSeconds])

  const legend = useChartLegend()
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const focusedKeyRef = useRef<string | null>(null)

  // Legend state
  const LEGEND_HEADER_HEIGHT = 60
  const LEGEND_ROW_HEIGHT = 22
  const LEGEND_HANDLE_HEIGHT = 12
  const [legendMaxHeight, setLegendMaxHeight] = useState(256)
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

  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const displayValues = useMemo(() => {
    const map = new Map<string, number>()
    if (!uplotData[0] || uplotData[0].length === 0) return map
    const idx = hoveredIdx != null && hoveredIdx < uplotData[0].length
      ? hoveredIdx
      : uplotData[0].length - 1
    for (let i = 0; i < seriesKeys.length; i++) {
      map.set(seriesKeys[i], (uplotData[i + 1] as number[])?.[idx] ?? 0)
    }
    return map
  }, [uplotData, seriesKeys, hoveredIdx])

  const handleCursorIdx = useCallback((idx: number | null) => {
    setHoveredIdx(idx)
  }, [])

  const handleFocusSeries = useCallback((seriesIdx: number | null) => {
    if (seriesIdx != null && seriesIdx > 0 && seriesIdx <= seriesKeys.length) {
      const key = seriesKeys[seriesIdx - 1]
      focusedKeyRef.current = key
      legend.handleMouseEnter(key)
    } else {
      focusedKeyRef.current = null
      legend.handleMouseLeave()
    }
  }, [seriesKeys, legend])

  const uplotSeries = useMemo((): uPlot.Series[] => {
    const s: uPlot.Series[] = [{}]
    for (let i = 0; i < seriesKeys.length; i++) {
      s.push({
        label: seriesKeys[i],
        stroke: TRAFFIC_COLORS[i % TRAFFIC_COLORS.length],
        width: 1.5,
        points: { show: false },
      })
    }
    return s
  }, [seriesKeys])

  const fmtValue = rateMode ? formatRate : formatCount

  const uplotAxes = useMemo((): uPlot.Axis[] => [
    {},
    {
      values: (_u: uPlot, vals: number[]) => vals.map(v => rateMode ? formatRate(v) : formatCount(v)),
    },
  ], [rateMode])

  const chartScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const { plotRef } = useUPlotChart({
    containerRef: chartContainerRef,
    data: uplotData,
    series: uplotSeries,
    height: 224,
    axes: uplotAxes,
    scales: chartScales,
    onCursorIdx: handleCursorIdx,
    onFocusSeries: handleFocusSeries,
  })

  useUPlotLegendSync(plotRef, legend, seriesKeys)

  const legendRef = useRef(legend)
  legendRef.current = legend
  const handleChartClick = useCallback((e: React.MouseEvent) => {
    const leg = legendRef.current
    if (focusedKeyRef.current) {
      leg.handleClick(focusedKeyRef.current, e)
    } else if (leg.selectedSeries.size > 0) {
      leg.setSelectedSeries(new Set())
    }
  }, [])

  // Propagate hover to parent (translate user_pk -> device_pk_tunnel_id)
  const prevHoveredKey = useRef<string | null>(null)
  if (legend.hoveredSeries !== prevHoveredKey.current) {
    prevHoveredKey.current = legend.hoveredSeries
    if (legend.hoveredSeries) {
      const memberKeys = userPkToMemberKeys.get(legend.hoveredSeries)
      onHoverMember?.(memberKeys?.[0] ?? null)
    } else {
      onHoverMember?.(null)
    }
  }

  // Propagate selection to parent (translate user_pks -> device_pk_tunnel_ids)
  const prevSelectedRef = useRef<Set<string>>(legend.selectedSeries)
  useEffect(() => {
    if (prevSelectedRef.current !== legend.selectedSeries) {
      prevSelectedRef.current = legend.selectedSeries
      if (legend.selectedSeries.size === 0) {
        onSelectMember?.(new Set())
      } else {
        const memberKeySet = new Set<string>()
        for (const userPk of legend.selectedSeries) {
          const keys = userPkToMemberKeys.get(userPk)
          if (keys) keys.forEach(k => memberKeySet.add(k))
        }
        onSelectMember?.(memberKeySet)
      }
    }
  }, [legend.selectedSeries, userPkToMemberKeys, onSelectMember])

  // Legend: filter by search text, then sort
  const legendFilteredKeys = useMemo(() => {
    let keys = seriesKeys
    if (legendSearchText) {
      const needle = legendSearchText.toLowerCase()
      keys = keys.filter(key => {
        const info = userInfo.get(key)
        const ownerPk = info?.ownerPubkey ?? ''
        const code = info?.code ?? ''
        return ownerPk.toLowerCase().includes(needle) ||
          code.toLowerCase().includes(needle) ||
          key.toLowerCase().includes(needle)
      })
    }
    const sorted = [...keys].sort((a, b) => {
      if (legendSortBy === 'value') {
        const va = displayValues.get(a) ?? 0
        const vb = displayValues.get(b) ?? 0
        return legendSortDir === 'desc' ? vb - va : va - vb
      }
      const infoA = userInfo.get(a)
      const infoB = userInfo.get(b)
      const labelA = infoA?.ownerPubkey ?? a
      const labelB = infoB?.ownerPubkey ?? b
      return legendSortDir === 'asc' ? labelA.localeCompare(labelB) : labelB.localeCompare(labelA)
    })
    return sorted
  }, [seriesKeys, legendSearchText, legendSortBy, legendSortDir, userInfo, displayValues])

  const legendHeight = useMemo(() => {
    const contentHeight = LEGEND_HEADER_HEIGHT + legendFilteredKeys.length * LEGEND_ROW_HEIGHT + LEGEND_HANDLE_HEIGHT
    return Math.min(legendMaxHeight, contentHeight)
  }, [legendMaxHeight, legendFilteredKeys.length])

  return (
    <div className="border border-border rounded-lg p-4 bg-card group/chart">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-muted-foreground">
            Shred Stats (publishers)
          </h3>
          {isFetching ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
          ) : (
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: ['multicast-shred-stats', groupCode] })}
              className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground"
              title="Refresh"
            >
              <RefreshCw className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
        <div className="flex items-center gap-2">
          <select
            value={metric}
            onChange={e => setMetric(e.target.value as ShredMetric)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {(Object.keys(SHRED_METRIC_LABELS) as ShredMetric[]).map(m => (
              <option key={m} value={m}>{SHRED_METRIC_LABELS[m]}</option>
            ))}
          </select>
          <select
            value={rateMode ? 'rate' : 'total'}
            onChange={e => setRateMode(e.target.value === 'rate')}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            <option value="rate">Rate (/s)</option>
            <option value="total">Total</option>
          </select>
          <select
            value={bucket}
            onChange={e => setBucket(e.target.value)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {BUCKET_OPTIONS.map(b => (
              <option key={b} value={b}>{b === 'auto' ? `Auto (${autoBucketLabel})` : BUCKET_LABELS[b] || b}</option>
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
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-2">
        {isFetching && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>

      {!shredData && !isFetching && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          No shred stats data available
        </div>
      )}

      {(shredData || isFetching) && (
        <div>
          <div
            ref={chartContainerRef}
            className="h-56"
            onClick={handleChartClick}
          />
          {seriesKeys.length > 0 && (
            <div className="relative mt-2" style={{ height: `${legendHeight}px` }}>
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
                          placeholder="Filter by owner, device..."
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
                    <div className="w-48 text-right text-xs text-muted-foreground font-medium whitespace-nowrap">DZ ID</div>
                    <button
                      onClick={() => { setLegendSortBy('value'); setLegendSortDir(legendSortBy === 'value' ? (legendSortDir === 'asc' ? 'desc' : 'asc') : 'desc') }}
                      className="w-20 flex items-center justify-end gap-0.5 text-xs text-muted-foreground hover:text-foreground font-medium"
                    >
                      Value
                      {legendSortBy === 'value' && (legendSortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
                    </button>
                  </div>
                </div>
                {/* Scrollable items */}
                <div className="flex-1 overflow-y-auto px-2 pb-2">
                  <div className="space-y-0.5">
                    {legendFilteredKeys.map((key) => {
                      const i = seriesKeys.indexOf(key)
                      const info = userInfo.get(key)
                      const val = displayValues.get(key)
                      const opacity = legend.getOpacity(key)
                      const isSelected = legend.selectedSeries.size === 0 || legend.selectedSeries.has(key)
                      const ownerLabel = info?.ownerPubkey
                        ? `${info.ownerPubkey.slice(0, 4)}..${info.ownerPubkey.slice(-4)}`
                        : key.slice(0, 8)
                      const dzIdLabel = info?.ownerPubkey || key
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
                            <span className="text-muted-foreground ml-2">{info?.code ?? key.slice(0, 8)}</span>
                          </div>
                          <div className="w-48 text-right truncate tabular-nums font-mono text-muted-foreground select-text" title={dzIdLabel}>{dzIdLabel}</div>
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

function MemberCountChart({ groupCode }: { groupCode: string }) {
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<string>('7d')
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: countData, isFetching } = useQuery({
    queryKey: ['multicast-member-counts', groupCode, timeRange],
    queryFn: () => fetchMulticastGroupMemberCounts(groupCode, timeRange),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })

  const steppedPaths = useMemo(() => uPlot.paths.stepped?.({ align: 1 }), [])

  const { uplotData, uplotSeries } = useMemo(() => {
    if (!countData || countData.length === 0) {
      return { uplotData: [[]] as uPlot.AlignedData, uplotSeries: [] as uPlot.Series[] }
    }

    const timestamps = countData.map(p => new Date(p.time).getTime() / 1000)
    const pubs = countData.map(p => p.publisher_count)
    const subs = countData.map(p => p.subscriber_count)

    return {
      uplotData: [timestamps, pubs, subs] as uPlot.AlignedData,
      uplotSeries: [
        {},
        { label: 'Publishers', stroke: '#7c5cbf', width: 1.5, points: { show: false }, paths: steppedPaths },
        { label: 'Subscribers', stroke: '#4a8fe7', width: 1.5, points: { show: false }, paths: steppedPaths },
      ] as uPlot.Series[],
    }
  }, [countData, steppedPaths])

  const chartScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map(v => String(Math.round(v))) },
  ], [])

  useUPlotChart({
    containerRef: chartRef,
    data: uplotData,
    series: uplotSeries,
    height: 160,
    axes,
    scales: chartScales,
  })

  return (
    <div className="border border-border rounded-lg p-4 bg-card group/chart">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-muted-foreground">Members Over Time</h3>
          {isFetching ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
          ) : (
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: ['multicast-member-counts', groupCode] })}
              className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground"
              title="Refresh"
            >
              <RefreshCw className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
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
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-2">
        {isFetching && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>

      {!countData && !isFetching && (
        <div className="flex items-center justify-center h-40 text-sm text-muted-foreground">
          No member count data available
        </div>
      )}

      {(countData || isFetching) && (
        <div>
          <div ref={chartRef} className="h-40" />
          <div className="flex items-center justify-center gap-4 mt-2 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.5">
              <div className="w-3 h-0.5 rounded-full" style={{ backgroundColor: '#7c5cbf' }} />
              Publishers
            </span>
            <span className="flex items-center gap-1.5">
              <div className="w-3 h-0.5 rounded-full" style={{ backgroundColor: '#4a8fe7' }} />
              Subscribers
            </span>
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

// Base58 character class (no 0, O, I, l)
const BASE58_RE = /^[1-9A-HJ-NP-Za-km-z]{8,}$/

function toMemberFilterParam(filter: string): string {
  const colonIndex = filter.indexOf(':')
  if (colonIndex > 0) {
    const field = filter.slice(0, colonIndex).toLowerCase()
    const value = filter.slice(colonIndex + 1)
    if (validMemberFilterFields.includes(field) && value) {
      return `${field}:${value}`
    }
  }
  // Auto-detect pubkeys
  if (BASE58_RE.test(filter)) {
    return `owner:${filter}`
  }
  return `all:${filter}`
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
  const [selectedSeriesKeys, setSelectedSeriesKeys] = useState<Set<string>>(new Set())

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

  // Convert all filters to "field:value" params for the API
  const filterParams = useMemo(
    () => allFilters.map(toMemberFilterParam),
    [allFilters]
  )
  const filterKey = filterParams.join(',')

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
  const prevFilterRef = useRef(filterKey)
  useEffect(() => {
    if (prevFilterRef.current === filterKey) return
    prevFilterRef.current = filterKey
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('page')
      return newParams
    })
  }, [filterKey, setSearchParams])

  // Group metadata query
  const { data: group, isLoading: groupLoading, error: groupError } = useQuery({
    queryKey: ['multicast-group', pk],
    queryFn: () => fetchMulticastGroup(pk!),
    enabled: !!pk,
    refetchInterval: 30000,
  })

  // Members query (server-side pagination)
  const { data: membersResponse, isLoading: membersLoading, isFetching: membersFetching } = useQuery({
    queryKey: ['multicast-group-members', pk, activeTab, offset, pageSize, sortField, sortDirection, filterKey],
    queryFn: () => fetchMulticastGroupMembers(
      pk!,
      pageSize,
      offset,
      sortField,
      sortDirection,
      activeTab,
      filterParams.length > 0 ? filterParams : undefined
    ),
    enabled: !!pk,
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })

  useDocumentTitle(group?.code || 'Multicast Group')

  const publisherCount = membersResponse?.publisher_count ?? 0
  const subscriberCount = membersResponse?.subscriber_count ?? 0

  const activeMembers = membersResponse?.items ?? []

  // Selected members not on current page — surfaced at top of table
  const surfacedMembers = useMemo(() => {
    if (selectedSeriesKeys.size === 0 || !group) return []
    const activeKeys = new Set(activeMembers.map(m => `${m.device_pk}_${m.tunnel_id}`))
    const modeFilter = activeTab === 'publishers' ? 'P' : 'S'
    return group.members.filter(m => {
      const key = `${m.device_pk}_${m.tunnel_id}`
      if (!selectedSeriesKeys.has(key)) return false
      if (activeKeys.has(key)) return false
      // Only surface members matching the active tab
      return m.mode === modeFilter || m.mode === 'P+S'
    })
  }, [selectedSeriesKeys, group, activeMembers, activeTab])

  // Scroll to first selected row when selection changes
  const selectedRowRef = useRef<HTMLTableRowElement>(null)
  const scrollContainerRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (selectedSeriesKeys.size > 0) {
      // Defer to next frame so surfaced rows are rendered before scrolling
      requestAnimationFrame(() => {
        const row = selectedRowRef.current
        const container = scrollContainerRef.current
        if (!row || !container) return
        // Manually scroll just the overflow-auto container (scrollIntoView
        // also scrolls overflow:hidden ancestors, breaking the app layout)
        const rowTop = row.offsetTop
        const containerScroll = container.scrollTop
        const containerHeight = container.clientHeight
        if (rowTop < containerScroll || rowTop > containerScroll + containerHeight - row.offsetHeight) {
          container.scrollTo({ top: Math.max(0, rowTop - 80), behavior: 'smooth' })
        }
      })
    }
  }, [selectedSeriesKeys])

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

  const renderMemberCells = (member: MulticastMember) => (
    <>
      <td className="px-4 py-3 text-sm font-mono">
        {member.owner_pubkey ? (
          <Link to={`/dz/users/${member.user_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
            {member.owner_pubkey.slice(0, 4)}..{member.owner_pubkey.slice(-4)}
          </Link>
        ) : '—'}
      </td>
      <td className="px-4 py-3 text-sm font-mono">
        {member.node_pubkey ? (
          <Link to={`/solana/gossip-nodes/${member.node_pubkey}`} className="text-blue-600 dark:text-blue-400 hover:underline">
            {member.node_pubkey.slice(0, 4)}..{member.node_pubkey.slice(-4)}
          </Link>
        ) : '—'}
      </td>
      <td className="px-4 py-3 text-sm">
        {member.device_pk ? (
          <Link to={`/dz/devices/${member.device_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
            {member.device_code || member.device_pk.slice(0, 8)}
          </Link>
        ) : '—'}
      </td>
      <td className="px-4 py-3 text-sm">
        {member.metro_pk ? (
          <Link to={`/dz/metros/${member.metro_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
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
    </>
  )

  return (
    <div ref={scrollContainerRef} className="flex-1 overflow-auto">
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
                {surfacedMembers.map((member, i) => (
                  <tr
                    key={`surfaced-${member.user_pk}`}
                    ref={i === 0 ? selectedRowRef : undefined}
                    className="border-b border-border bg-muted border-l-2 border-l-purple-500"
                  >
                    {renderMemberCells(member)}
                  </tr>
                ))}
                {activeMembers.map((member) => {
                  const memberSeriesKey = `${member.device_pk}_${member.tunnel_id}`
                  const isHovered = hoveredSeriesKey === memberSeriesKey
                  const isSelected = selectedSeriesKeys.size > 0 && selectedSeriesKeys.has(memberSeriesKey)
                  return (
                  <tr
                    key={member.user_pk}
                    ref={isSelected && surfacedMembers.length === 0 ? selectedRowRef : undefined}
                    className={`border-b border-border last:border-b-0 hover:bg-muted transition-colors ${isSelected ? 'bg-muted border-l-2 border-l-purple-500' : isHovered ? 'bg-muted' : ''}`}
                  >
                    {renderMemberCells(member)}
                  </tr>
                  )
                })}
                {!membersLoading && activeMembers.length === 0 && surfacedMembers.length === 0 && (
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

        <div className="space-y-6">
          {/* Shred stats chart — only for groups with shred stats */}
          {pk && group && group.has_shred_stats && activeTab === 'publishers' && group.members.length > 0 && (
            <ShredStatsChart
              groupCode={pk}
              members={group.members}
              onHoverMember={setHoveredSeriesKey}
              onSelectMember={setSelectedSeriesKeys}
            />
          )}

          {/* Traffic chart — uses all members (not just current page) for series labels */}
          {pk && group && group.members.length > 0 && (
            <MulticastTrafficChart
              groupCode={pk}
              members={group.members}
              activeTab={activeTab}
              onHoverMember={setHoveredSeriesKey}
              onSelectMember={setSelectedSeriesKeys}
            />
          )}

          {/* Member count chart */}
          {pk && <MemberCountChart groupCode={pk} />}
        </div>
      </div>
    </div>
  )
}
