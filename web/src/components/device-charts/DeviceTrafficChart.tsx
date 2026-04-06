import { useMemo, useRef, useState, useCallback, useEffect } from 'react'
import uPlot from 'uplot'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { formatHoveredTime } from '@/components/topology/utils'
import type { DeviceMetricsResponse, DeviceInterfaceTraffic } from '@/lib/api'

interface DeviceTrafficChartProps {
  data: DeviceMetricsResponse
  className?: string
  loading?: boolean
  highlightTimeRange?: { start: number; end: number } | null
  onCursorTime?: (time: number | null) => void
}

type AggMode = 'avg' | 'peak'

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316']

function formatBps(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e9) return `${(abs / 1e9).toFixed(1)} Gbps`
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(1)} Mbps`
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)} Kbps`
  return `${abs.toFixed(0)} bps`
}

function formatBpsAxis(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e9) return `${(abs / 1e9).toFixed(1)}G`
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(1)}M`
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)}K`
  return `${abs.toFixed(0)}`
}

type InterfaceCategory = 'userTunnel' | 'link' | 'cyoa' | 'other'

function classifyInterface(intf: DeviceInterfaceTraffic): InterfaceCategory {
  if (intf.user_pk) return 'userTunnel'
  if (intf.cyoa_type && intf.cyoa_type !== 'none' && intf.cyoa_type !== '') return 'cyoa'
  if (intf.link_pk) return 'link'
  return 'other'
}

const categoryLabels: Record<InterfaceCategory, string> = {
  userTunnel: 'User Tunnel Traffic',
  link: 'Link Interface Traffic',
  cyoa: 'CYOA Interface Traffic',
  other: 'Other Interface Traffic',
}

const categoryOrder: InterfaceCategory[] = ['userTunnel', 'link', 'cyoa', 'other']

interface CategoryChartProps {
  title: string
  interfaces: string[]
  bucketTimestamps: number[]
  /** Map from "intf:ts" -> { inBps, outBps, maxInBps, maxOutBps } */
  dataMap: Map<string, DeviceInterfaceTraffic>
  /** Display labels per interface (e.g. "Ethernet1 (Side A)") */
  interfaceLabels?: Map<string, string>
  /** Interface type metadata for badges */
  interfaceTypes?: Map<string, { cyoa_type?: string; link_pk?: string }>
  aggMode: AggMode
  loading?: boolean
  className?: string
  bucketSeconds: number
  highlightTimeRange?: { start: number; end: number } | null
  onCursorTime?: (time: number | null) => void
}

function CategoryChart({ title, interfaces, bucketTimestamps, dataMap, interfaceLabels, interfaceTypes, aggMode, loading, className, bucketSeconds, highlightTimeRange, onCursorTime }: CategoryChartProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const chartRef = useRef<HTMLDivElement>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const uPlotDataRef = useRef<uPlot.AlignedData>([[]])
  const handleCursorIdx = useCallback((idx: number | null) => {
    setHoveredIdx(idx)
    if (onCursorTime) {
      const ts = idx != null ? (uPlotDataRef.current[0] as number[])?.[idx] ?? null : null
      onCursorTime(ts)
    }
  }, [onCursorTime])

  const legend = useChartLegend()

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => formatBpsAxis(v)) },
  ], [])

  const { uPlotData, uPlotSeries, seriesKeys } = useMemo(() => {
    if (bucketTimestamps.length === 0 || interfaces.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[], seriesKeys: [] as string[] }
    }

    const arrays: (number | null)[][] = [bucketTimestamps as unknown as (number | null)[]]
    const series: uPlot.Series[] = [{}]
    const keys: string[] = []

    for (let i = 0; i < interfaces.length; i++) {
      const intf = interfaces[i]
      const color = COLORS[i % COLORS.length]
      const inVals: (number | null)[] = []
      const outVals: (number | null)[] = []

      for (const ts of bucketTimestamps) {
        const key = `${intf}:${ts}`
        const d = dataMap.get(key)
        if (d) {
          const inVal = aggMode === 'peak' ? d.max_in_bps : d.in_bps
          const outVal = aggMode === 'peak' ? d.max_out_bps : d.out_bps
          inVals.push(inVal || null)
          outVals.push(outVal ? -outVal : null)
        } else {
          inVals.push(null)
          outVals.push(null)
        }
      }

      arrays.push(inVals)
      arrays.push(outVals)
      series.push({ label: `${intf}:in`, stroke: color, width: 1.5, points: { show: false } })
      series.push({ label: `${intf}:out`, stroke: color, width: 1.5, dash: [4, 2], points: { show: false } })
      keys.push(intf, intf) // both in/out map to same legend key
    }

    return { uPlotData: arrays as uPlot.AlignedData, uPlotSeries: series, seriesKeys: keys }
  }, [bucketTimestamps, interfaces, dataMap, aggMode])

  uPlotDataRef.current = uPlotData

  const highlightTimeRangeRef = useRef(highlightTimeRange)
  highlightTimeRangeRef.current = highlightTimeRange
  const isDarkRef = useRef(isDark)
  isDarkRef.current = isDark

  const drawHooks = useMemo(() => [(u: uPlot) => {
    const range = highlightTimeRangeRef.current
    if (!range) return
    const ctx = u.ctx
    const left = u.valToPos(range.start, 'x', true)
    const right = u.valToPos(range.end, 'x', true)
    if (left == null || right == null) return
    const top = u.bbox.top
    const height = u.bbox.height
    ctx.save()
    ctx.fillStyle = isDarkRef.current ? 'rgba(255, 255, 255, 0.08)' : 'rgba(0, 0, 0, 0.06)'
    ctx.fillRect(left, top, right - left, height)
    ctx.restore()
  }], [])

  const { plotRef } = useUPlotChart({
    containerRef: chartRef,
    data: uPlotData,
    series: uPlotSeries,
    height: 144,
    axes,
    scales,
    onCursorIdx: handleCursorIdx,
    drawHooks,
  })

  // Redraw when highlight range changes
  const prevHighlightRef = useRef(highlightTimeRange)
  useEffect(() => {
    if (prevHighlightRef.current !== highlightTimeRange) {
      prevHighlightRef.current = highlightTimeRange
      plotRef.current?.redraw()
    }
  }, [highlightTimeRange, plotRef])

  useUPlotLegendSync(plotRef, legend, seriesKeys)

  const visibleSeries = useMemo(() => {
    if (legend.selectedSeries.has('__none__')) return new Set<string>()
    if (legend.selectedSeries.size > 0) return legend.selectedSeries
    return new Set(interfaces)
  }, [legend.selectedSeries, interfaces])

  const hoveredTime = useMemo(() =>
    formatHoveredTime(uPlotData[0] as ArrayLike<number>, hoveredIdx, bucketSeconds < 60),
    [uPlotData, hoveredIdx, bucketSeconds])

  const hasAnyData = uPlotData[0].length > 0 && uPlotData.slice(1).some(
    (s) => (s as (number | null)[]).some((v) => v != null))

  // Active interfaces: those with any non-null in or out data
  const activeInterfaces = useMemo(() => {
    return interfaces.filter((_, i) => {
      const inSeries = uPlotData[i * 2 + 1] as (number | null)[]
      const outSeries = uPlotData[i * 2 + 2] as (number | null)[]
      return (inSeries?.some((v) => v != null) || outSeries?.some((v) => v != null))
    })
  }, [interfaces, uPlotData])

  if (!hasAnyData) {
    return (
      <div className={className}>
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
          <span>{title}</span>
        </div>
        <div className="text-xs text-muted-foreground/60 pt-3 pb-6 text-center">No data for this time range</div>
      </div>
    )
  }

  return (
    <div className={`${className ?? ''} group/chart`}>
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
          <span>{title}</span>
          {loading && <Loader2 className="h-3 w-3 animate-spin" />}
        </div>
      </div>
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
        {loading && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>
      <div ref={chartRef} className="h-36" />
      {/* Per-interface legend */}
      <div className="flex flex-col text-xs px-2 pt-1 pb-2">
        <div className="flex items-center px-1 mb-1">
          <span className="text-xs text-muted-foreground flex-1 min-w-0">Interface</span>
          <span className="text-xs text-muted-foreground w-24 text-right whitespace-nowrap">Max</span>
          <span className="text-xs text-muted-foreground w-24 text-right whitespace-nowrap">{hoveredTime ?? 'Value'}</span>
        </div>
        <div className="max-h-32 overflow-y-auto space-y-0.5">
          {activeInterfaces.map((intf) => {
            const i = interfaces.indexOf(intf)
            const color = COLORS[i % COLORS.length]
            const isVisible = visibleSeries.has(intf)

            // Current value (hovered or latest)
            const idx = hoveredIdx != null && hoveredIdx < bucketTimestamps.length ? hoveredIdx : bucketTimestamps.length - 1
            const inVal = (uPlotData[i * 2 + 1] as (number | null)[])?.[idx]
            const outVal = (uPlotData[i * 2 + 2] as (number | null)[])?.[idx]

            // Max value across range
            let maxIn = 0, maxOut = 0
            const inS = uPlotData[i * 2 + 1] as (number | null)[]
            const outS = uPlotData[i * 2 + 2] as (number | null)[]
            if (inS) for (const v of inS) if (v != null && v > maxIn) maxIn = v
            if (outS) for (const v of outS) if (v != null && Math.abs(v) > maxOut) maxOut = Math.abs(v)

            return (
              <div key={intf}>
                <div
                  className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${isVisible ? '' : 'opacity-40'}`}
                  onClick={(e) => legend.handleClick(intf, e)}
                  onMouseEnter={() => legend.handleMouseEnter(intf)}
                  onMouseLeave={legend.handleMouseLeave}
                >
                  <div className="flex items-center gap-1.5 min-w-0 flex-1">
                    <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: color }} />
                    <span className="font-mono text-foreground truncate">{interfaceLabels?.get(intf) ?? intf} Rx</span>
                    {(() => {
                      const t = interfaceTypes?.get(intf)
                      if (!t) return null
                      return <>
                        {t.cyoa_type && t.cyoa_type !== 'none' && t.cyoa_type !== '' && (
                          <span className="px-1 py-0.5 rounded text-[9px] leading-none bg-amber-500/15 text-amber-400 flex-shrink-0">CYOA</span>
                        )}
                        {(!t.cyoa_type || t.cyoa_type === 'none' || t.cyoa_type === '') && t.link_pk && (
                          <span className="px-1 py-0.5 rounded text-[9px] leading-none bg-blue-500/15 text-blue-400 flex-shrink-0">link</span>
                        )}
                        {(!t.cyoa_type || t.cyoa_type === 'none' || t.cyoa_type === '') && !t.link_pk && intf.startsWith('Loopback') && (
                          <span className="px-1 py-0.5 rounded text-[9px] leading-none bg-purple-500/15 text-purple-400 flex-shrink-0">lo</span>
                        )}
                      </>
                    })()}
                  </div>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
                    {formatBps(maxIn)}
                  </span>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
                    {inVal != null ? formatBps(inVal) : '--'}
                  </span>
                </div>
                <div
                  className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${isVisible ? '' : 'opacity-40'}`}
                  onClick={(e) => legend.handleClick(intf, e)}
                  onMouseEnter={() => legend.handleMouseEnter(intf)}
                  onMouseLeave={legend.handleMouseLeave}
                >
                  <div className="flex items-center gap-1.5 min-w-0 flex-1">
                    <svg className="w-2.5 h-2.5 flex-shrink-0" viewBox="0 0 10 10">
                      <line x1="0" y1="5" x2="10" y2="5" stroke={color} strokeWidth="3" strokeDasharray="3 2" />
                    </svg>
                    <span className="font-mono text-foreground truncate">{interfaceLabels?.get(intf) ?? intf} Tx</span>
                  </div>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
                    {formatBps(maxOut)}
                  </span>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
                    {outVal != null ? formatBps(Math.abs(outVal)) : '--'}
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

export function DeviceTrafficChart({ data, className, loading, highlightTimeRange, onCursorTime }: DeviceTrafficChartProps) {
  const [aggMode, setAggMode] = useState<AggMode>('avg')

  // Collect all interface data across buckets, classify, and build lookup
  const { categories, bucketTimestamps, dataMap, interfaceLabels, interfaceTypes } = useMemo(() => {
    const catIntfs: Record<InterfaceCategory, Set<string>> = {
      userTunnel: new Set(),
      link: new Set(),
      cyoa: new Set(),
      other: new Set(),
    }
    const map = new Map<string, DeviceInterfaceTraffic>()
    const labels = new Map<string, string>()
    const types = new Map<string, { cyoa_type?: string; link_pk?: string }>()
    const timestamps: number[] = []

    const nonCollecting = data.buckets.filter((b) => !b.status?.collecting)
    for (const b of nonCollecting) {
      const ts = new Date(b.ts).getTime() / 1000
      timestamps.push(ts)
      if (b.interfaces) {
        for (const intf of b.interfaces) {
          const cat = classifyInterface(intf)
          catIntfs[cat].add(intf.intf)
          map.set(`${intf.intf}:${ts}`, intf)
          if (!types.has(intf.intf)) {
            types.set(intf.intf, { cyoa_type: intf.cyoa_type, link_pk: intf.link_pk })
          }
          // Build label for link interfaces: "Ethernet1 (link-code, Side A)"
          if (intf.link_pk && !labels.has(intf.intf)) {
            const parts: string[] = []
            if (intf.link_code) parts.push(intf.link_code)
            if (intf.link_side) parts.push(`Side ${intf.link_side.toUpperCase()}`)
            if (parts.length > 0) labels.set(intf.intf, `${intf.intf} (${parts.join(', ')})`)
          }
        }
      }
    }

    const cats = categoryOrder
      .filter((c) => catIntfs[c].size > 0)
      .map((c) => ({
        key: c,
        title: categoryLabels[c],
        interfaces: Array.from(catIntfs[c]).sort(),
      }))

    return { categories: cats, bucketTimestamps: timestamps, dataMap: map, interfaceLabels: labels, interfaceTypes: types }
  }, [data])

  if (categories.length === 0) {
    return (
      <div className={className}>
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
          <span>Traffic</span>
        </div>
        <div className="text-xs text-muted-foreground/60 pt-3 pb-6 text-center">No data for this time range</div>
      </div>
    )
  }

  const filters = (
    <div className="flex items-center gap-1.5">
      <select
        value={aggMode}
        onChange={e => setAggMode(e.target.value as AggMode)}
        className="text-xs bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground cursor-pointer"
      >
        <option value="avg">Avg</option>
        <option value="peak">Peak</option>
      </select>
    </div>
  )

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-end">
        {filters}
      </div>
      {categories.map((cat) => (
        <CategoryChart
          key={cat.key}
          title={cat.title}
          interfaces={cat.interfaces}
          bucketTimestamps={bucketTimestamps}
          dataMap={dataMap}
          interfaceLabels={interfaceLabels}
          interfaceTypes={interfaceTypes}
          aggMode={aggMode}
          loading={loading}
          className={className}
          bucketSeconds={data.bucket_seconds}
          highlightTimeRange={highlightTimeRange}
          onCursorTime={onCursorTime}
        />
      ))}
    </div>
  )
}
