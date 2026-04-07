import { useMemo, useRef, useState, useCallback, useEffect } from 'react'
import uPlot from 'uplot'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { formatHoveredTime } from '@/components/topology/utils'
import type { DeviceMetricsResponse, DeviceInterfaceTraffic } from '@/lib/api'

interface DeviceInterfaceIssuesChartProps {
  data: DeviceMetricsResponse
  className?: string
  loading?: boolean
  highlightTimeRange?: { start: number; end: number } | null
  onCursorTime?: (time: number | null) => void
}

function formatCount(value: number): string {
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toString()
}

const COLORS = [
  '#ef4444', '#f97316', '#f59e0b', '#8b5cf6', '#06b6d4', '#10b981',
  '#ec4899', '#6366f1', '#14b8a6', '#d946ef', '#84cc16', '#fb923c',
]

function typeBadge(intf: DeviceInterfaceTraffic) {
  if (intf.cyoa_type && intf.cyoa_type !== 'none' && intf.cyoa_type !== '') {
    return { label: 'CYOA', className: 'bg-amber-500/15 text-amber-400' }
  }
  if (intf.link_pk) {
    return { label: 'link', className: 'bg-blue-500/15 text-blue-400' }
  }
  if (intf.intf.startsWith('Loopback')) {
    return { label: 'lo', className: 'bg-purple-500/15 text-purple-400' }
  }
  return null
}

export function DeviceInterfaceIssuesChart({ data, className, loading, highlightTimeRange, onCursorTime }: DeviceInterfaceIssuesChartProps) {
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

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => formatCount(Math.abs(v))) },
  ], [])

  // Collect per-interface issue totals to find which interfaces have issues
  const { intfIssues, intfMeta } = useMemo(() => {
    const totals = new Map<string, number>()
    const meta = new Map<string, DeviceInterfaceTraffic>()
    const buckets = data.buckets.filter((b) => !b.status?.collecting)
    for (const b of buckets) {
      if (!b.interfaces) continue
      for (const intf of b.interfaces) {
        const total = (intf.in_errors ?? 0) + (intf.out_errors ?? 0) + (intf.in_fcs_errors ?? 0) +
          (intf.in_discards ?? 0) + (intf.out_discards ?? 0) + (intf.carrier_transitions ?? 0)
        if (total === 0) continue
        totals.set(intf.intf, (totals.get(intf.intf) ?? 0) + total)
        if (!meta.has(intf.intf)) meta.set(intf.intf, intf)
      }
    }
    // Sort by total descending
    const sorted = [...totals.entries()].sort((a, b) => b[1] - a[1]).map(([intf]) => intf)
    return { intfIssues: sorted, intfMeta: meta }
  }, [data])

  const { uPlotData, uPlotSeries, seriesKeys } = useMemo(() => {
    const buckets = data.buckets.filter((b) => !b.status?.collecting)
    if (buckets.length === 0 || intfIssues.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[], seriesKeys: [] as string[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)

    // Build per-bucket lookup: intf -> counters
    const bucketIndex = buckets.map((b) => {
      const map = new Map<string, DeviceInterfaceTraffic>()
      if (b.interfaces) {
        for (const intf of b.interfaces) map.set(intf.intf, intf)
      }
      return map
    })

    const dataArrays: (number | null)[][] = [timestamps as unknown as (number | null)[]]
    const series: uPlot.Series[] = [{}]
    const keys: string[] = []

    for (let i = 0; i < intfIssues.length; i++) {
      const intfName = intfIssues[i]
      const color = COLORS[i % COLORS.length]

      // Rx: in_errors + in_fcs_errors + in_discards + carrier_transitions
      const rxVals: (number | null)[] = bucketIndex.map((m) => {
        const d = m.get(intfName)
        if (!d) return null
        const v = (d.in_errors ?? 0) + (d.in_fcs_errors ?? 0) + (d.in_discards ?? 0) + (d.carrier_transitions ?? 0)
        return v > 0 ? v : null
      })

      // Tx: out_errors + out_discards (negated)
      const txVals: (number | null)[] = bucketIndex.map((m) => {
        const d = m.get(intfName)
        if (!d) return null
        const v = (d.out_errors ?? 0) + (d.out_discards ?? 0)
        return v > 0 ? -v : null
      })

      dataArrays.push(rxVals, txVals)
      series.push(
        { label: `${intfName} rx`, stroke: color, width: 1.5, points: { show: true, size: 4 } },
        { label: `${intfName} tx`, stroke: color, width: 1.5, points: { show: true, size: 4 }, dash: [4, 4] },
      )
      keys.push(intfName, intfName)
    }

    return {
      uPlotData: dataArrays as uPlot.AlignedData,
      uPlotSeries: series,
      seriesKeys: keys,
    }
  }, [data, intfIssues])

  const legend = useChartLegend()

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

  // Visible series state
  const visibleSeries = useMemo(() => {
    if (legend.selectedSeries.has('__none__')) return new Set<string>()
    if (legend.selectedSeries.size > 0) return legend.selectedSeries
    return new Set(intfIssues)
  }, [legend.selectedSeries, intfIssues])

  const hoveredTime = useMemo(() =>
    formatHoveredTime(uPlotData[0] as ArrayLike<number>, hoveredIdx, data.bucket_seconds < 60),
    [uPlotData, hoveredIdx])

  const hasAnyData = uPlotData[0].length > 0 && uPlotData.slice(1).some(
    (s) => (s as (number | null)[]).some((v) => v != null))

  if (!hasAnyData) {
    return (
      <div className={className}>
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
          <span>Interface Issues</span>
        </div>
        <div className="text-xs text-muted-foreground/60 pt-3 pb-6 text-center">No data for this time range</div>
      </div>
    )
  }

  return (
    <div className={`${className ?? ''} group/chart`}>
      <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
        <span>Interface Issues</span>
        {loading && <Loader2 className="h-3 w-3 animate-spin" />}
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
          <span className="text-xs text-muted-foreground w-20 text-right whitespace-nowrap">Max</span>
          <span className="text-xs text-muted-foreground w-20 text-right whitespace-nowrap">{hoveredTime ?? 'Value'}</span>
        </div>
        <div className="max-h-32 overflow-y-auto space-y-0.5">
          {intfIssues.map((intfName, i) => {
            const color = COLORS[i % COLORS.length]
            const isVisible = visibleSeries.has(intfName)
            const meta = intfMeta.get(intfName)
            const badge = meta ? typeBadge(meta) : null

            // Current values
            const idx = hoveredIdx != null && hoveredIdx < (uPlotData[0] as number[]).length
              ? hoveredIdx : (uPlotData[0] as number[]).length - 1
            const rxVal = (uPlotData[i * 2 + 1] as (number | null)[])?.[idx]
            const txVal = (uPlotData[i * 2 + 2] as (number | null)[])?.[idx]

            // Max values
            let maxRx = 0, maxTx = 0
            const rxS = uPlotData[i * 2 + 1] as (number | null)[]
            const txS = uPlotData[i * 2 + 2] as (number | null)[]
            if (rxS) for (const v of rxS) if (v != null && v > maxRx) maxRx = v
            if (txS) for (const v of txS) if (v != null && Math.abs(v) > maxTx) maxTx = Math.abs(v)

            return (
              <div key={intfName}>
                <div
                  className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${isVisible ? '' : 'opacity-40'}`}
                  onClick={(e) => legend.handleClick(intfName, e)}
                  onMouseEnter={() => legend.handleMouseEnter(intfName)}
                  onMouseLeave={legend.handleMouseLeave}
                >
                  <div className="flex items-center gap-1.5 min-w-0 flex-1">
                    <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: color }} />
                    <span className="font-mono text-foreground truncate">{intfName} Rx</span>
                    {badge && (
                      <span className={`px-1 py-0.5 rounded text-[9px] leading-none flex-shrink-0 ${badge.className}`}>{badge.label}</span>
                    )}
                  </div>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-20 text-right">
                    {formatCount(maxRx)}
                  </span>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-20 text-right">
                    {rxVal != null ? formatCount(rxVal) : '--'}
                  </span>
                </div>
                <div
                  className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${isVisible ? '' : 'opacity-40'}`}
                  onClick={(e) => legend.handleClick(intfName, e)}
                  onMouseEnter={() => legend.handleMouseEnter(intfName)}
                  onMouseLeave={legend.handleMouseLeave}
                >
                  <div className="flex items-center gap-1.5 min-w-0 flex-1">
                    <svg className="w-2.5 h-2.5 flex-shrink-0" viewBox="0 0 10 10">
                      <line x1="0" y1="5" x2="10" y2="5" stroke={color} strokeWidth="3" strokeDasharray="3 2" />
                    </svg>
                    <span className="font-mono text-foreground truncate">{intfName} Tx</span>
                  </div>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-20 text-right">
                    {formatCount(maxTx)}
                  </span>
                  <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-20 text-right">
                    {txVal != null ? formatCount(Math.abs(txVal)) : '--'}
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
