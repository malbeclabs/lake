import { useMemo, useRef, useState, useCallback } from 'react'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { Loader2, RefreshCw } from 'lucide-react'
import uPlot from 'uplot'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { type ChartLegendSeries } from './ChartLegend'
import { ChartLegendTable } from './ChartLegendTable'
import { fetchLatencyHistory, formatHoveredTime, type TimeRange, type BucketSize, type TrafficView } from './utils'

const AGG_OPTIONS: { value: TrafficView; label: string }[] = [
  { value: 'peak', label: 'Max' },
  { value: 'p99', label: 'P99' },
  { value: 'p95', label: 'P95' },
  { value: 'p90', label: 'P90' },
  { value: 'p50', label: 'P50' },
  { value: 'avg', label: 'Avg' },
  { value: 'min', label: 'Min' },
]

function RefreshButton({ fetching, onClick }: { fetching: boolean; onClick: () => void }) {
  if (fetching) return <Loader2 className="h-3 w-3 animate-spin" />
  return (
    <button onClick={onClick} className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground" title="Refresh">
      <RefreshCw className="h-3 w-3" />
    </button>
  )
}

interface LatencyChartsProps {
  linkPk: string
  timeRange?: TimeRange
  bucket?: BucketSize
  className?: string
}

export function LatencyCharts({ linkPk, timeRange, bucket, className }: LatencyChartsProps) {
  const queryClient = useQueryClient()
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const effectiveRange = timeRange ?? { preset: '24h' as const }
  const [aggMode, setAggMode] = useState<TrafficView>('avg')
  const aggParam = aggMode === 'peak' ? 'max' : aggMode

  const { data: latencyData, isFetching, error } = useQuery({
    queryKey: ['topology-latency', linkPk, effectiveRange, bucket, aggParam],
    queryFn: () => fetchLatencyHistory(linkPk, effectiveRange, bucket, aggParam),
    refetchInterval: 60000,
    retry: 2,
    placeholderData: keepPreviousData,
  })

  const rttChartRef = useRef<HTMLDivElement>(null)
  const jitterChartRef = useRef<HTMLDivElement>(null)

  const [rttHoveredIdx, setRttHoveredIdx] = useState<number | null>(null)
  const [jitterHoveredIdx, setJitterHoveredIdx] = useState<number | null>(null)
  const handleRttCursorIdx = useCallback((idx: number | null) => setRttHoveredIdx(idx), [])
  const handleJitterCursorIdx = useCallback((idx: number | null) => setJitterHoveredIdx(idx), [])

  const chartScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  // Colors
  const rttAAvgColor = isDark ? '#22c55e' : '#16a34a'
  const rttAP95Color = isDark ? '#86efac' : '#4ade80'
  const rttZAvgColor = isDark ? '#3b82f6' : '#2563eb'
  const rttZP95Color = isDark ? '#93c5fd' : '#60a5fa'
  const jitterAColor = isDark ? '#a855f7' : '#9333ea'
  const jitterZColor = isDark ? '#f97316' : '#ea580c'

  const hasDirectionalData = latencyData?.some(
    (d) => (d.avgRttAtoZMs && d.avgRttAtoZMs > 0) || (d.avgRttZtoAMs && d.avgRttZtoAMs > 0)
  ) ?? false

  const aggLabel = AGG_OPTIONS.find(o => o.value === aggMode)?.label || 'Avg'

  // RTT legend
  const rttKeys = useMemo(() =>
    hasDirectionalData
      ? ['avgRttAtoZMs', 'p95RttAtoZMs', 'avgRttZtoAMs', 'p95RttZtoAMs']
      : ['avgRttMs', 'p95RttMs'],
    [hasDirectionalData]
  )
  const rttLegend = useChartLegend()
  const rttLegendSeries: ChartLegendSeries[] = useMemo(() =>
    hasDirectionalData
      ? [
          { key: 'avgRttAtoZMs', color: rttAAvgColor, label: 'Avg A' },
          { key: 'p95RttAtoZMs', color: rttAP95Color, label: `${aggLabel} A`, dashed: true },
          { key: 'avgRttZtoAMs', color: rttZAvgColor, label: 'Avg Z' },
          { key: 'p95RttZtoAMs', color: rttZP95Color, label: `${aggLabel} Z`, dashed: true },
        ]
      : [
          { key: 'avgRttMs', color: rttAAvgColor, label: 'Avg' },
          { key: 'p95RttMs', color: rttAP95Color, label: aggLabel, dashed: true },
        ],
    [hasDirectionalData, rttAAvgColor, rttAP95Color, rttZAvgColor, rttZP95Color, aggLabel]
  )

  // Jitter legend
  const jitterKeys = useMemo(() =>
    hasDirectionalData
      ? ['jitterAtoZMs', 'jitterZtoAMs']
      : ['avgJitter'],
    [hasDirectionalData]
  )
  const jitterLegend = useChartLegend()
  const jitterLegendSeries: ChartLegendSeries[] = useMemo(() =>
    hasDirectionalData
      ? [
          { key: 'jitterAtoZMs', color: jitterAColor, label: 'From A' },
          { key: 'jitterZtoAMs', color: jitterZColor, label: 'From Z' },
        ]
      : [
          { key: 'avgJitter', color: jitterAColor, label: 'Jitter' },
        ],
    [hasDirectionalData, jitterAColor, jitterZColor]
  )

  // RTT columnar data
  const { rttUPlotData, rttUPlotSeries } = useMemo(() => {
    if (!latencyData || latencyData.length === 0) {
      return { rttUPlotData: [[]] as uPlot.AlignedData, rttUPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = latencyData.map((d) => new Date(d.time).getTime() / 1000)
    const series: uPlot.Series[] = [{}]

    if (hasDirectionalData) {
      series.push(
        { label: 'avgRttAtoZMs', stroke: rttAAvgColor, width: 1.5, points: { show: false } },
        { label: 'p95RttAtoZMs', stroke: rttAP95Color, width: 1.5, dash: [4, 2], points: { show: false } },
        { label: 'avgRttZtoAMs', stroke: rttZAvgColor, width: 1.5, points: { show: false } },
        { label: 'p95RttZtoAMs', stroke: rttZP95Color, width: 1.5, dash: [4, 2], points: { show: false } },
      )
      return {
        rttUPlotData: [timestamps, latencyData.map(d => d.avgRttAtoZMs || null), latencyData.map(d => d.p95RttAtoZMs || null), latencyData.map(d => d.avgRttZtoAMs || null), latencyData.map(d => d.p95RttZtoAMs || null)] as uPlot.AlignedData,
        rttUPlotSeries: series,
      }
    }

    series.push(
      { label: 'avgRttMs', stroke: rttAAvgColor, width: 1.5, points: { show: false } },
      { label: 'p95RttMs', stroke: rttAP95Color, width: 1.5, dash: [4, 2], points: { show: false } },
    )
    return {
      rttUPlotData: [timestamps, latencyData.map(d => d.avgRttMs || null), latencyData.map(d => d.p95RttMs || null)] as uPlot.AlignedData,
      rttUPlotSeries: series,
    }
  }, [latencyData, hasDirectionalData, rttAAvgColor, rttAP95Color, rttZAvgColor, rttZP95Color])

  // Jitter columnar data
  const { jitterUPlotData, jitterUPlotSeries } = useMemo(() => {
    if (!latencyData || latencyData.length === 0) {
      return { jitterUPlotData: [[]] as uPlot.AlignedData, jitterUPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = latencyData.map((d) => new Date(d.time).getTime() / 1000)
    const series: uPlot.Series[] = [{}]

    if (hasDirectionalData) {
      series.push(
        { label: 'jitterAtoZMs', stroke: jitterAColor, width: 1.5, points: { show: false } },
        { label: 'jitterZtoAMs', stroke: jitterZColor, width: 1.5, points: { show: false } },
      )
      return {
        jitterUPlotData: [timestamps, latencyData.map(d => d.jitterAtoZMs || null), latencyData.map(d => d.jitterZtoAMs || null)] as uPlot.AlignedData,
        jitterUPlotSeries: series,
      }
    }

    series.push(
      { label: 'avgJitter', stroke: jitterAColor, width: 1.5, points: { show: false } },
    )
    return {
      jitterUPlotData: [timestamps, latencyData.map(d => d.avgJitter || null)] as uPlot.AlignedData,
      jitterUPlotSeries: series,
    }
  }, [latencyData, hasDirectionalData, jitterAColor, jitterZColor])

  const msAxes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => `${v.toFixed(1)} ms`) },
  ], [])

  // Charts
  const { plotRef: rttPlotRef} = useUPlotChart({
    containerRef: rttChartRef,
    data: rttUPlotData,
    series: rttUPlotSeries,
    height: 144,
    axes: msAxes,
    scales: chartScales,
    onCursorIdx: handleRttCursorIdx,
  })

  const { plotRef: jitterPlotRef} = useUPlotChart({
    containerRef: jitterChartRef,
    data: jitterUPlotData,
    series: jitterUPlotSeries,
    height: 144,
    axes: msAxes,
    scales: chartScales,
    onCursorIdx: handleJitterCursorIdx,
  })

  // Legend sync
  useUPlotLegendSync(rttPlotRef, rttLegend, rttKeys)
  useUPlotLegendSync(jitterPlotRef, jitterLegend, jitterKeys)

  // Display values
  const rttDisplayValues = useMemo(() => {
    const map = new Map<string, string>()
    if (rttUPlotData[0].length === 0) return map
    const idx = rttHoveredIdx != null && rttHoveredIdx < rttUPlotData[0].length ? rttHoveredIdx : rttUPlotData[0].length - 1
    for (let i = 0; i < rttKeys.length; i++) {
      const val = (rttUPlotData[i + 1] as (number | null)[])?.[idx]
      map.set(rttKeys[i], val != null ? `${val.toFixed(2)} ms` : '—')
    }
    return map
  }, [rttUPlotData, rttKeys, rttHoveredIdx])

  const jitterDisplayValues = useMemo(() => {
    const map = new Map<string, string>()
    if (jitterUPlotData[0].length === 0) return map
    const idx = jitterHoveredIdx != null && jitterHoveredIdx < jitterUPlotData[0].length ? jitterHoveredIdx : jitterUPlotData[0].length - 1
    for (let i = 0; i < jitterKeys.length; i++) {
      const val = (jitterUPlotData[i + 1] as (number | null)[])?.[idx]
      map.set(jitterKeys[i], val != null ? `${val.toFixed(2)} ms` : '—')
    }
    return map
  }, [jitterUPlotData, jitterKeys, jitterHoveredIdx])

  const rttMaxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (rttUPlotData[0].length === 0) return map
    for (let i = 0; i < rttKeys.length; i++) {
      const series = rttUPlotData[i + 1] as (number | null)[]
      let max = 0
      if (series) for (const v of series) if (v != null && v > max) max = v
      map.set(rttKeys[i], `${max.toFixed(2)} ms`)
    }
    return map
  }, [rttUPlotData, rttKeys])

  const jitterMaxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (jitterUPlotData[0].length === 0) return map
    for (let i = 0; i < jitterKeys.length; i++) {
      const series = jitterUPlotData[i + 1] as (number | null)[]
      let max = 0
      if (series) for (const v of series) if (v != null && v > max) max = v
      map.set(jitterKeys[i], `${max.toFixed(2)} ms`)
    }
    return map
  }, [jitterUPlotData, jitterKeys])

  const rttHoveredTime = useMemo(() =>
    formatHoveredTime(rttUPlotData[0] as ArrayLike<number>, rttHoveredIdx),
    [rttUPlotData, rttHoveredIdx])
  const jitterHoveredTime = useMemo(() =>
    formatHoveredTime(jitterUPlotData[0] as ArrayLike<number>, jitterHoveredIdx),
    [jitterUPlotData, jitterHoveredIdx])

  if (error && !latencyData) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load latency data — the request may have timed out
      </div>
    )
  }

  const refreshLatency = () => queryClient.invalidateQueries({ queryKey: ['topology-latency'] })

  return (
    <div className="space-y-6">
      <div className={`${className} group/chart`}>
        <div className="flex items-center justify-between mb-1">
          <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
            <span>Round-Trip Time</span>
            <RefreshButton fetching={isFetching} onClick={refreshLatency} />
          </div>
          <select
            value={aggMode}
            onChange={e => setAggMode(e.target.value as TrafficView)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground cursor-pointer"
          >
            {AGG_OPTIONS.map(o => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>
        <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
          {isFetching && (
            <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
          )}
        </div>
        <div ref={rttChartRef} className="h-36" />
        <ChartLegendTable series={rttLegendSeries} legend={rttLegend} values={rttDisplayValues} maxValues={rttMaxValues} hoveredTime={rttHoveredTime} />
      </div>

      <div className={`${className} group/chart`}>
        <div className="flex items-center justify-between mb-1">
          <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
            <span>Jitter</span>
            <RefreshButton fetching={isFetching} onClick={refreshLatency} />
          </div>
          <select
            value={aggMode}
            onChange={e => setAggMode(e.target.value as TrafficView)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground cursor-pointer"
          >
            {AGG_OPTIONS.map(o => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>
        <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
          {isFetching && (
            <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
          )}
        </div>
        <div ref={jitterChartRef} className="h-36" />
        <ChartLegendTable series={jitterLegendSeries} legend={jitterLegend} values={jitterDisplayValues} maxValues={jitterMaxValues} hoveredTime={jitterHoveredTime} />
      </div>
    </div>
  )
}
