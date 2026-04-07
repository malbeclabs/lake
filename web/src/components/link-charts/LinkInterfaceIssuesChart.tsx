import { useMemo, useRef, useState, useCallback, useEffect } from 'react'
import uPlot from 'uplot'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { formatHoveredTime } from '@/components/topology/utils'
import type { LinkMetricsResponse } from '@/lib/api'

interface LinkInterfaceIssuesChartProps {
  data: LinkMetricsResponse
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

const SIDE_A_COLOR = '#06b6d4' // cyan
const SIDE_Z_COLOR = '#f59e0b' // amber

interface SideSeries {
  key: string
  label: string
  color: string
  dashed: boolean
  extract: (b: LinkMetricsResponse['buckets'][number]) => number | null
}

export function LinkInterfaceIssuesChart({ data, className, loading, highlightTimeRange, onCursorTime }: LinkInterfaceIssuesChartProps) {
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

  const sideALabel = 'Side A'
  const sideZLabel = 'Side Z'

  // Define all possible series, then filter to those with data
  const allSideSeries: SideSeries[] = useMemo(() => [
    { key: 'a_errors_rx', label: `${sideALabel} errors (rx)`, color: SIDE_A_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_a_in_errors ?? 0; return v > 0 ? v : null } },
    { key: 'a_errors_tx', label: `${sideALabel} errors (tx)`, color: SIDE_A_COLOR, dashed: true,
      extract: (b) => { const v = b.traffic?.side_a_out_errors ?? 0; return v > 0 ? -v : null } },
    { key: 'a_fcs', label: `${sideALabel} fcs (rx)`, color: SIDE_A_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_a_in_fcs_errors ?? 0; return v > 0 ? v : null } },
    { key: 'a_discards_rx', label: `${sideALabel} discards (rx)`, color: SIDE_A_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_a_in_discards ?? 0; return v > 0 ? v : null } },
    { key: 'a_discards_tx', label: `${sideALabel} discards (tx)`, color: SIDE_A_COLOR, dashed: true,
      extract: (b) => { const v = b.traffic?.side_a_out_discards ?? 0; return v > 0 ? -v : null } },
    { key: 'a_carrier', label: `${sideALabel} carrier transitions`, color: SIDE_A_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_a_carrier_transitions ?? 0; return v > 0 ? v : null } },
    { key: 'z_errors_rx', label: `${sideZLabel} errors (rx)`, color: SIDE_Z_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_z_in_errors ?? 0; return v > 0 ? v : null } },
    { key: 'z_errors_tx', label: `${sideZLabel} errors (tx)`, color: SIDE_Z_COLOR, dashed: true,
      extract: (b) => { const v = b.traffic?.side_z_out_errors ?? 0; return v > 0 ? -v : null } },
    { key: 'z_fcs', label: `${sideZLabel} fcs (rx)`, color: SIDE_Z_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_z_in_fcs_errors ?? 0; return v > 0 ? v : null } },
    { key: 'z_discards_rx', label: `${sideZLabel} discards (rx)`, color: SIDE_Z_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_z_in_discards ?? 0; return v > 0 ? v : null } },
    { key: 'z_discards_tx', label: `${sideZLabel} discards (tx)`, color: SIDE_Z_COLOR, dashed: true,
      extract: (b) => { const v = b.traffic?.side_z_out_discards ?? 0; return v > 0 ? -v : null } },
    { key: 'z_carrier', label: `${sideZLabel} carrier transitions`, color: SIDE_Z_COLOR, dashed: false,
      extract: (b) => { const v = b.traffic?.side_z_carrier_transitions ?? 0; return v > 0 ? v : null } },
  ], [sideALabel, sideZLabel])

  const { uPlotData, uPlotSeries, activeSeries, seriesKeys } = useMemo(() => {
    const buckets = data.buckets
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[], activeSeries: [] as SideSeries[], seriesKeys: [] as string[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)

    // Build data arrays and filter out all-null series
    const active: SideSeries[] = []
    const dataArrays: (number | null)[][] = [timestamps as unknown as (number | null)[]]
    const series: uPlot.Series[] = [{}]
    const keys: string[] = []

    for (const s of allSideSeries) {
      const vals = buckets.map(s.extract)
      if (vals.every((v) => v === null)) continue
      active.push(s)
      dataArrays.push(vals)
      series.push({
        label: s.label,
        stroke: s.color,
        width: 1.5,
        points: { show: true, size: 4 },
        ...(s.dashed ? { dash: [4, 4] } : {}),
      })
      keys.push(s.key)
    }

    return {
      uPlotData: dataArrays as uPlot.AlignedData,
      uPlotSeries: series,
      activeSeries: active,
      seriesKeys: keys,
    }
  }, [data, allSideSeries])

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

  const displayValues = useMemo(() => {
    const map = new Map<string, string>()
    if (uPlotData[0].length === 0) return map
    let defaultIdx = uPlotData[0].length - 1
    for (let j = defaultIdx; j >= 0; j--) {
      if (activeSeries.some((_, si) => (uPlotData[si + 1] as (number | null)[])?.[j] != null)) { defaultIdx = j; break }
    }
    const idx = hoveredIdx != null && hoveredIdx < uPlotData[0].length ? hoveredIdx : defaultIdx
    for (let i = 0; i < activeSeries.length; i++) {
      const val = (uPlotData[i + 1] as (number | null)[])?.[idx]
      map.set(activeSeries[i].key, val != null ? formatCount(Math.abs(val)) : '--')
    }
    return map
  }, [uPlotData, hoveredIdx, activeSeries])

  const maxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (uPlotData[0].length === 0) return map
    for (let i = 0; i < activeSeries.length; i++) {
      const s = uPlotData[i + 1] as (number | null)[]
      let max = 0
      if (s) for (const v of s) if (v != null && Math.abs(v) > max) max = Math.abs(v)
      map.set(activeSeries[i].key, formatCount(max))
    }
    return map
  }, [uPlotData, activeSeries])

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
      {/* Per-series legend */}
      <div className="flex flex-col text-xs px-2 pt-1 pb-2">
        <div className="flex items-center px-1 mb-1">
          <span className="text-xs text-muted-foreground flex-1 min-w-0">Series</span>
          <span className="text-xs text-muted-foreground w-16 text-right whitespace-nowrap">Max</span>
          <span className="text-xs text-muted-foreground w-16 text-right whitespace-nowrap">{hoveredTime ?? 'Value'}</span>
        </div>
        <div className="max-h-32 overflow-y-auto space-y-0.5">
          {activeSeries.map((s) => {
            const isVisible = !legend.selectedSeries.has('__none__') &&
              (legend.selectedSeries.size === 0 || legend.selectedSeries.has(s.key))
            return (
              <div
                key={s.key}
                className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${isVisible ? '' : 'opacity-40'}`}
                onClick={(e) => legend.handleClick(s.key, e)}
                onMouseEnter={() => legend.handleMouseEnter(s.key)}
                onMouseLeave={legend.handleMouseLeave}
              >
                <div className="flex items-center gap-1.5 min-w-0 flex-1">
                  {s.dashed ? (
                    <svg className="w-2.5 h-2.5 flex-shrink-0" viewBox="0 0 10 10">
                      <line x1="0" y1="5" x2="10" y2="5" stroke={s.color} strokeWidth="3" strokeDasharray="3 2" />
                    </svg>
                  ) : (
                    <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: s.color }} />
                  )}
                  <span className="text-foreground truncate">{s.label}</span>
                </div>
                <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-16 text-right">
                  {maxValues.get(s.key) ?? '--'}
                </span>
                <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-16 text-right">
                  {displayValues.get(s.key) ?? '--'}
                </span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
