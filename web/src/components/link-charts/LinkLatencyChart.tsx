import { useMemo, useRef, useState, useCallback, useEffect } from 'react'
import uPlot from 'uplot'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { type ChartLegendSeries } from '@/components/topology/ChartLegend'
import { ChartLegendTable } from '@/components/topology/ChartLegendTable'
import { formatHoveredTime } from '@/components/topology/utils'
import type { LinkMetricsResponse } from '@/lib/api'

interface LinkLatencyChartProps {
  data: LinkMetricsResponse
  className?: string
  loading?: boolean
  highlightTimeRange?: { start: number; end: number } | null
  onCursorTime?: (time: number | null) => void
}

type AggMode = 'avg' | 'p50' | 'p90' | 'p95' | 'p99' | 'min' | 'max'

const AGG_OPTIONS: { value: AggMode; label: string }[] = [
  { value: 'avg', label: 'Avg' },
  { value: 'p50', label: 'P50' },
  { value: 'p90', label: 'P90' },
  { value: 'p95', label: 'P95' },
  { value: 'p99', label: 'P99' },
  { value: 'min', label: 'Min' },
  { value: 'max', label: 'Max' },
]

function getRttField(side: 'a' | 'z', mode: AggMode): string {
  return `${side}_${mode}_rtt_us`
}

function getRttValue(latency: Record<string, number>, side: 'a' | 'z', mode: AggMode): number {
  const field = getRttField(side, mode)
  return (latency as Record<string, number>)[field] ?? 0
}

export function LinkLatencyChart({ data, className, loading, highlightTimeRange, onCursorTime }: LinkLatencyChartProps) {
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
  const [aggMode, setAggMode] = useState<AggMode>('avg')

  // Colors
  const aToZColor = isDark ? '#22c55e' : '#16a34a'
  const zToAColor = isDark ? '#3b82f6' : '#2563eb'
  const committedColor = isDark ? '#6b7280' : '#9ca3af'

  const hasCommittedRtt = data.committed_rtt_us > 0

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => `${v.toFixed(1)} ms`) },
  ], [])

  const seriesKeys = useMemo(() => {
    const keys = ['aToZ', 'zToA']
    if (hasCommittedRtt) keys.push('committed')
    return keys
  }, [hasCommittedRtt])

  const { uPlotData, uPlotSeries } = useMemo(() => {
    const buckets = data.buckets.filter((b) => !b.status?.collecting)
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)
    const aToZ = buckets.map((b) => {
      if (!b.latency) return null
      const v = getRttValue(b.latency as unknown as Record<string, number>, 'a', aggMode)
      return v > 0 ? v / 1000 : null
    })
    const zToA = buckets.map((b) => {
      if (!b.latency) return null
      const v = getRttValue(b.latency as unknown as Record<string, number>, 'z', aggMode)
      return v > 0 ? v / 1000 : null
    })

    const series: uPlot.Series[] = [
      {},
      { label: 'aToZ', stroke: aToZColor, width: 1.5, points: { show: false } },
      { label: 'zToA', stroke: zToAColor, width: 1.5, points: { show: false } },
    ]

    const dataArrays: (number | null)[][] = [aToZ, zToA]

    if (hasCommittedRtt) {
      const committedMs = data.committed_rtt_us / 1000
      dataArrays.push(buckets.map(() => committedMs))
      series.push(
        { label: 'committed', stroke: committedColor, width: 1, dash: [4, 4], points: { show: false } },
      )
    }

    return {
      uPlotData: [timestamps, ...dataArrays] as uPlot.AlignedData,
      uPlotSeries: series,
    }
  }, [data, aggMode, aToZColor, zToAColor, committedColor, hasCommittedRtt])

  const legend = useChartLegend()
  const legendSeries: ChartLegendSeries[] = useMemo(() => {
    const items: ChartLegendSeries[] = [
      { key: 'aToZ', color: aToZColor, label: 'A \u2192 Z' },
      { key: 'zToA', color: zToAColor, label: 'Z \u2192 A' },
    ]
    if (hasCommittedRtt) {
      items.push({ key: 'committed', color: committedColor, label: 'Committed RTT', dashed: true })
    }
    return items
  }, [aToZColor, zToAColor, committedColor, hasCommittedRtt])

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
      if (seriesKeys.some((_, si) => (uPlotData[si + 1] as (number | null)[])?.[j] != null)) { defaultIdx = j; break }
    }
    const idx = hoveredIdx != null && hoveredIdx < uPlotData[0].length ? hoveredIdx : defaultIdx
    for (let i = 0; i < seriesKeys.length; i++) {
      const val = (uPlotData[i + 1] as (number | null)[])?.[idx]
      map.set(seriesKeys[i], val != null ? `${val.toFixed(2)} ms` : '--')
    }
    return map
  }, [uPlotData, seriesKeys, hoveredIdx])

  const maxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (uPlotData[0].length === 0) return map
    for (let i = 0; i < seriesKeys.length; i++) {
      const s = uPlotData[i + 1] as (number | null)[]
      let max = 0
      if (s) for (const v of s) if (v != null && v > max) max = v
      map.set(seriesKeys[i], `${max.toFixed(2)} ms`)
    }
    return map
  }, [uPlotData, seriesKeys])

  const hoveredTime = useMemo(() =>
    formatHoveredTime(uPlotData[0] as ArrayLike<number>, hoveredIdx, data.bucket_seconds < 60),
    [uPlotData, hoveredIdx])

  const hasAnyData = uPlotData[0].length > 0 && uPlotData.slice(1).some(
    (s) => (s as (number | null)[]).some((v) => v != null))

  if (!hasAnyData) {
    return (
      <div className={className}>
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
          <span>Round-Trip Time</span>
        </div>
        <div className="text-xs text-muted-foreground/60 pt-3 pb-6 text-center">No data for this time range</div>
      </div>
    )
  }

  return (
    <div className={`${className ?? ''} group/chart`}>
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
          <span>Round-Trip Time</span>
          {loading && <Loader2 className="h-3 w-3 animate-spin" />}
        </div>
        <select
          value={aggMode}
          onChange={e => setAggMode(e.target.value as AggMode)}
          className="text-xs bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground cursor-pointer"
        >
          {AGG_OPTIONS.map(o => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
      </div>
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
        {loading && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>
      <div ref={chartRef} className="h-36" />
      <ChartLegendTable series={legendSeries} legend={legend} values={displayValues} maxValues={maxValues} hoveredTime={hoveredTime} />
    </div>
  )
}
