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
import type { LinkMetricsResponse, LinkMetricsTraffic } from '@/lib/api'

interface LinkTrafficChartProps {
  data: LinkMetricsResponse
  className?: string
  loading?: boolean
  highlightTimeRange?: { start: number; end: number } | null
  onCursorTime?: (time: number | null) => void
}

type AggMode = 'avg' | 'p50' | 'p90' | 'p95' | 'p99' | 'max'
type MetricMode = 'bps' | 'pps'

function formatBps(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e9) return `${(abs / 1e9).toFixed(1)} Gbps`
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(1)} Mbps`
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)} Kbps`
  return `${abs.toFixed(0)} bps`
}

function formatPps(value: number): string {
  const abs = Math.abs(value)
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(1)} Mpps`
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)} Kpps`
  return `${abs.toFixed(0)} pps`
}

function getTrafficValue(t: LinkMetricsTraffic, side: 'a' | 'z', dir: 'in' | 'out', agg: AggMode, metric: MetricMode): number {
  const suffix = metric === 'bps' ? 'bps' : 'pps'
  let key: string
  if (agg === 'max') {
    key = `side_${side}_max_${dir}_${suffix}`
  } else if (agg === 'avg') {
    key = `side_${side}_${dir}_${suffix}`
  } else {
    key = `side_${side}_${agg}_${dir}_${suffix}`
  }
  return (t as unknown as Record<string, number>)[key] ?? 0
}

export function LinkTrafficChart({ data, className, loading, highlightTimeRange, onCursorTime }: LinkTrafficChartProps) {
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
  const [bidir, setBidir] = useState(true)
  const [aggMode, setAggMode] = useState<AggMode>('avg')
  const [metricMode, setMetricMode] = useState<MetricMode>('bps')

  const fmt = metricMode === 'bps' ? formatBps : formatPps

  // Colors — A green, Z blue
  const aColor = isDark ? '#22c55e' : '#16a34a'
  const zColor = isDark ? '#3b82f6' : '#2563eb'
  const aOutColor = isDark ? '#86efac' : '#4ade80'
  const zOutColor = isDark ? '#93c5fd' : '#60a5fa'

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => fmt(v)) },
  ], [fmt])

  const seriesKeys = useMemo(() =>
    bidir ? ['aRx', 'aTx', 'zRx', 'zTx'] : ['aIn', 'aOut', 'zIn', 'zOut'],
    [bidir])

  const { uPlotData, uPlotSeries } = useMemo(() => {
    const buckets = data.buckets.filter((b) => !b.status?.collecting)
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)
    const val = (t: LinkMetricsTraffic | undefined, side: 'a' | 'z', dir: 'in' | 'out') =>
      t ? getTrafficValue(t, side, dir, aggMode, metricMode) || null : null

    if (bidir) {
      const aRx = buckets.map((b) => val(b.traffic, 'a', 'in'))
      const aTx = buckets.map((b) => { const v = val(b.traffic, 'a', 'out'); return v ? -v : null })
      const zRx = buckets.map((b) => val(b.traffic, 'z', 'in'))
      const zTx = buckets.map((b) => { const v = val(b.traffic, 'z', 'out'); return v ? -v : null })

      return {
        uPlotData: [timestamps, aRx, aTx, zRx, zTx] as uPlot.AlignedData,
        uPlotSeries: [
          {},
          { label: 'aRx', stroke: aColor, width: 1.5, points: { show: false } },
          { label: 'aTx', stroke: aColor, width: 1.5, dash: [4, 2], points: { show: false } },
          { label: 'zRx', stroke: zColor, width: 1.5, points: { show: false } },
          { label: 'zTx', stroke: zColor, width: 1.5, dash: [4, 2], points: { show: false } },
        ] as uPlot.Series[],
      }
    }

    const aIn = buckets.map((b) => val(b.traffic, 'a', 'in'))
    const aOut = buckets.map((b) => val(b.traffic, 'a', 'out'))
    const zIn = buckets.map((b) => val(b.traffic, 'z', 'in'))
    const zOut = buckets.map((b) => val(b.traffic, 'z', 'out'))

    return {
      uPlotData: [timestamps, aIn, aOut, zIn, zOut] as uPlot.AlignedData,
      uPlotSeries: [
        {},
        { label: 'aIn', stroke: aColor, width: 1.5, points: { show: false } },
        { label: 'aOut', stroke: aOutColor, width: 1.5, dash: [4, 2], points: { show: false } },
        { label: 'zIn', stroke: zColor, width: 1.5, points: { show: false } },
        { label: 'zOut', stroke: zOutColor, width: 1.5, dash: [4, 2], points: { show: false } },
      ] as uPlot.Series[],
    }
  }, [data, bidir, aggMode, metricMode, aColor, aOutColor, zColor, zOutColor])

  const legend = useChartLegend()
  const legendSeries: ChartLegendSeries[] = useMemo(() =>
    bidir
      ? [
          { key: 'aRx', color: aColor, label: 'Side A Rx' },
          { key: 'aTx', color: aColor, label: 'Side A Tx', dashed: true },
          { key: 'zRx', color: zColor, label: 'Side Z Rx' },
          { key: 'zTx', color: zColor, label: 'Side Z Tx', dashed: true },
        ]
      : [
          { key: 'aIn', color: aColor, label: 'Side A In' },
          { key: 'aOut', color: aOutColor, label: 'Side A Out', dashed: true },
          { key: 'zIn', color: zColor, label: 'Side Z In' },
          { key: 'zOut', color: zOutColor, label: 'Side Z Out', dashed: true },
        ],
    [bidir, aColor, aOutColor, zColor, zOutColor])

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
      map.set(seriesKeys[i], val != null ? fmt(val) : '--')
    }
    return map
  }, [uPlotData, seriesKeys, hoveredIdx, fmt])

  const maxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (uPlotData[0].length === 0) return map
    for (let i = 0; i < seriesKeys.length; i++) {
      const s = uPlotData[i + 1] as (number | null)[]
      let max = 0
      if (s) for (const v of s) if (v != null && Math.abs(v) > max) max = Math.abs(v)
      map.set(seriesKeys[i], fmt(max))
    }
    return map
  }, [uPlotData, seriesKeys, fmt])

  const hoveredTime = useMemo(() =>
    formatHoveredTime(uPlotData[0] as ArrayLike<number>, hoveredIdx, data.bucket_seconds < 60),
    [uPlotData, hoveredIdx])

  const hasAnyData = uPlotData[0].length > 0 && uPlotData.slice(1).some(
    (s) => (s as (number | null)[]).some((v) => v != null))

  if (!hasAnyData) {
    return (
      <div className={className}>
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
          <span>Traffic</span>
        </div>
        <div className="text-xs text-muted-foreground/60 pt-3 pb-6 text-center">No data for this time range</div>
      </div>
    )
  }

  return (
    <div className={`${className ?? ''} group/chart`}>
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
          <span>Traffic</span>
          {loading && <Loader2 className="h-3 w-3 animate-spin" />}
        </div>
        <div className="flex items-center gap-1.5">
          <select
            value={aggMode}
            onChange={e => setAggMode(e.target.value as AggMode)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground cursor-pointer"
          >
            <option value="avg">Avg</option>
            <option value="p50">P50</option>
            <option value="p90">P90</option>
            <option value="p95">P95</option>
            <option value="p99">P99</option>
            <option value="max">Max</option>
          </select>
          <select
            value={metricMode}
            onChange={e => setMetricMode(e.target.value as MetricMode)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground cursor-pointer"
          >
            <option value="bps">bps</option>
            <option value="pps">pps</option>
          </select>
          <button
            onClick={() => setBidir(!bidir)}
            className="text-[10px] text-muted-foreground hover:text-foreground border border-border rounded px-1.5 py-0.5 transition-colors"
          >
            {bidir ? 'Rx/Tx ±' : 'All +'}
          </button>
        </div>
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
