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

interface LinkPacketLossChartProps {
  data: LinkMetricsResponse
  className?: string
  loading?: boolean
  highlightTimeRange?: { start: number; end: number } | null
  onCursorTime?: (time: number | null) => void
}

export function LinkPacketLossChart({ data, className, loading, highlightTimeRange, onCursorTime }: LinkPacketLossChartProps) {
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

  // Colors
  const sideAColor = isDark ? '#10b981' : '#059669'
  const sideZColor = isDark ? '#3b82f6' : '#2563eb'
  const avgColor = isDark ? '#a855f7' : '#9333ea'

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: {
      auto: true,
      range: (_u, _min, max) => {
        if (!isFinite(max) || max <= 0) return [0, 5] as [number, number]
        return [0, Math.min(Math.ceil(max * 1.1), 100)] as [number, number]
      },
    },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => `${v.toFixed(1)}%`) },
  ], [])

  const { uPlotData, uPlotSeries } = useMemo(() => {
    const buckets = data.buckets
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)
    const sideA = buckets.map((b) => b.latency?.a_loss_pct ?? null)
    const sideZ = buckets.map((b) => b.latency?.z_loss_pct ?? null)
    const avg = buckets.map((b) => b.latency ? Math.max(b.latency.a_loss_pct, b.latency.z_loss_pct) : null)

    const series: uPlot.Series[] = [
      {},
      { label: 'sideA', stroke: sideAColor, width: 1.5, points: { show: false } },
      { label: 'sideZ', stroke: sideZColor, width: 1.5, points: { show: false } },
      { label: 'avg', stroke: avgColor, width: 1.5, dash: [4, 2], points: { show: false } },
    ]

    return {
      uPlotData: [timestamps, sideA, sideZ, avg] as uPlot.AlignedData,
      uPlotSeries: series,
    }
  }, [data, sideAColor, sideZColor, avgColor])

  const legend = useChartLegend()
  const legendSeries: ChartLegendSeries[] = useMemo(() => [
    { key: 'sideA', color: sideAColor, label: 'Side A' },
    { key: 'sideZ', color: sideZColor, label: 'Side Z' },
    { key: 'avg', color: avgColor, label: 'Average', dashed: true },
  ], [sideAColor, sideZColor, avgColor])

  const seriesKeys = ['sideA', 'sideZ', 'avg']

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

  // Redraw when highlight range changes (skip if chart not yet initialized)
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
      map.set(seriesKeys[i], val != null ? `${val.toFixed(2)}%` : '--')
    }
    return map
  }, [uPlotData, hoveredIdx])

  const maxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (uPlotData[0].length === 0) return map
    for (let i = 0; i < seriesKeys.length; i++) {
      const s = uPlotData[i + 1] as (number | null)[]
      let max = 0
      if (s) for (const v of s) if (v != null && v > max) max = v
      map.set(seriesKeys[i], `${max.toFixed(2)}%`)
    }
    return map
  }, [uPlotData])

  const hoveredTime = useMemo(() =>
    formatHoveredTime(uPlotData[0] as ArrayLike<number>, hoveredIdx, data.bucket_seconds < 60),
    [uPlotData, hoveredIdx])

  const hasAnyData = uPlotData[0].length > 0 && uPlotData.slice(1).some(
    (s) => (s as (number | null)[]).some((v) => v != null))

  if (!hasAnyData) {
    return (
      <div className={className}>
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
          <span>Packet Loss</span>
        </div>
        <div className="text-xs text-muted-foreground/60 pt-3 pb-6 text-center">No data for this time range</div>
      </div>
    )
  }

  return (
    <div className={`${className ?? ''} group/chart`}>
      <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
        <span>Packet Loss</span>
        {loading && <Loader2 className="h-3 w-3 animate-spin" />}
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
