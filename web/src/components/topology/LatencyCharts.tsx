import { useMemo, useRef, useState, useCallback } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { type ChartLegendSeries } from './ChartLegend'
import { ChartLegendTable } from './ChartLegendTable'
import { fetchLatencyHistory, formatHoveredTime, type TimeRange, type BucketSize } from './utils'

interface LatencyChartsProps {
  linkPk: string
  timeRange?: TimeRange
  bucket?: BucketSize
  /** Additional CSS classes for the outer wrapper */
  className?: string
}

export function LatencyCharts({ linkPk, timeRange, bucket, className }: LatencyChartsProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const effectiveRange = timeRange ?? { preset: '24h' as const }

  const { data: latencyData, isLoading, error } = useQuery({
    queryKey: ['topology-latency', linkPk, effectiveRange, bucket],
    queryFn: () => fetchLatencyHistory(linkPk, effectiveRange, bucket),
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
          { key: 'p95RttAtoZMs', color: rttAP95Color, label: 'P95 A', dashed: true },
          { key: 'avgRttZtoAMs', color: rttZAvgColor, label: 'Avg Z' },
          { key: 'p95RttZtoAMs', color: rttZP95Color, label: 'P95 Z', dashed: true },
        ]
      : [
          { key: 'avgRttMs', color: rttAAvgColor, label: 'Avg' },
          { key: 'p95RttMs', color: rttAP95Color, label: 'P95', dashed: true },
        ],
    [hasDirectionalData, rttAAvgColor, rttAP95Color, rttZAvgColor, rttZP95Color]
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
          { key: 'avgJitter', color: jitterAColor, label: 'Avg Jitter' },
        ],
    [hasDirectionalData, jitterAColor, jitterZColor]
  )

  // RTT columnar data with Unix timestamp x-axis
  const { rttUPlotData, rttUPlotSeries } = useMemo(() => {
    if (!latencyData || latencyData.length === 0) {
      return { rttUPlotData: [[]] as uPlot.AlignedData, rttUPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = latencyData.map((d) => new Date(d.time).getTime() / 1000)
    const series: uPlot.Series[] = [{}]

    if (hasDirectionalData) {
      const avgAtoZ = latencyData.map((d) => d.avgRttAtoZMs || null)
      const p95AtoZ = latencyData.map((d) => d.p95RttAtoZMs || null)
      const avgZtoA = latencyData.map((d) => d.avgRttZtoAMs || null)
      const p95ZtoA = latencyData.map((d) => d.p95RttZtoAMs || null)

      series.push(
        { label: 'avgRttAtoZMs', stroke: rttAAvgColor, width: 1.5, points: { show: false } },
        { label: 'p95RttAtoZMs', stroke: rttAP95Color, width: 1.5, dash: [4, 2], points: { show: false } },
        { label: 'avgRttZtoAMs', stroke: rttZAvgColor, width: 1.5, points: { show: false } },
        { label: 'p95RttZtoAMs', stroke: rttZP95Color, width: 1.5, dash: [4, 2], points: { show: false } },
      )

      return {
        rttUPlotData: [timestamps, avgAtoZ, p95AtoZ, avgZtoA, p95ZtoA] as uPlot.AlignedData,
        rttUPlotSeries: series,
      }
    }

    const avgRtt = latencyData.map((d) => d.avgRttMs || null)
    const p95Rtt = latencyData.map((d) => d.p95RttMs || null)

    series.push(
      { label: 'avgRttMs', stroke: rttAAvgColor, width: 1.5, points: { show: false } },
      { label: 'p95RttMs', stroke: rttAP95Color, width: 1.5, dash: [4, 2], points: { show: false } },
    )

    return {
      rttUPlotData: [timestamps, avgRtt, p95Rtt] as uPlot.AlignedData,
      rttUPlotSeries: series,
    }
  }, [latencyData, hasDirectionalData, rttAAvgColor, rttAP95Color, rttZAvgColor, rttZP95Color])

  // Jitter columnar data with Unix timestamp x-axis
  const { jitterUPlotData, jitterUPlotSeries } = useMemo(() => {
    if (!latencyData || latencyData.length === 0) {
      return { jitterUPlotData: [[]] as uPlot.AlignedData, jitterUPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = latencyData.map((d) => new Date(d.time).getTime() / 1000)
    const series: uPlot.Series[] = [{}]

    if (hasDirectionalData) {
      const jitterAtoZ = latencyData.map((d) => d.jitterAtoZMs || null)
      const jitterZtoA = latencyData.map((d) => d.jitterZtoAMs || null)

      series.push(
        { label: 'jitterAtoZMs', stroke: jitterAColor, width: 1.5, points: { show: false } },
        { label: 'jitterZtoAMs', stroke: jitterZColor, width: 1.5, points: { show: false } },
      )

      return {
        jitterUPlotData: [timestamps, jitterAtoZ, jitterZtoA] as uPlot.AlignedData,
        jitterUPlotSeries: series,
      }
    }

    const avgJitter = latencyData.map((d) => d.avgJitter || null)

    series.push(
      { label: 'avgJitter', stroke: jitterAColor, width: 1.5, points: { show: false } },
    )

    return {
      jitterUPlotData: [timestamps, avgJitter] as uPlot.AlignedData,
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
    onCursorIdx: handleRttCursorIdx,
  })

  const { plotRef: jitterPlotRef} = useUPlotChart({
    containerRef: jitterChartRef,
    data: jitterUPlotData,
    series: jitterUPlotSeries,
    height: 144,
    axes: msAxes,
    onCursorIdx: handleJitterCursorIdx,
  })

  // Legend sync
  useUPlotLegendSync(rttPlotRef, rttLegend, rttKeys)
  useUPlotLegendSync(jitterPlotRef, jitterLegend, jitterKeys)

  // Display values: hovered or latest
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

  // Max values across the time range
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

  if (isLoading && !latencyData) {
    return <div className="h-36 animate-pulse bg-muted/50 rounded" />
  }

  if (error) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load latency data — the request may have timed out
      </div>
    )
  }

  if (!latencyData || latencyData.length === 0) return null

  return (
    <div className="space-y-6">
      <div className={className}>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Round-Trip Time</div>
        <div ref={rttChartRef} className="h-36" />
        <ChartLegendTable series={rttLegendSeries} legend={rttLegend} values={rttDisplayValues} maxValues={rttMaxValues} hoveredTime={rttHoveredTime} />
      </div>

      <div className={className}>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Jitter</div>
        <div ref={jitterChartRef} className="h-36" />
        <ChartLegendTable series={jitterLegendSeries} legend={jitterLegend} values={jitterDisplayValues} maxValues={jitterMaxValues} hoveredTime={jitterHoveredTime} />
      </div>
    </div>
  )
}
