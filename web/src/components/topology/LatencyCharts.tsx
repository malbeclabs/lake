import { useMemo, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { ChartLegend, type ChartLegendSeries } from './ChartLegend'
import { fetchLatencyHistory, type TimeRange, type BucketSize } from './utils'

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
  })

  const rttChartRef = useRef<HTMLDivElement>(null)
  const jitterChartRef = useRef<HTMLDivElement>(null)

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
      const avgAtoZ = latencyData.map((d) => d.avgRttAtoZMs ?? null)
      const p95AtoZ = latencyData.map((d) => d.p95RttAtoZMs ?? null)
      const avgZtoA = latencyData.map((d) => d.avgRttZtoAMs ?? null)
      const p95ZtoA = latencyData.map((d) => d.p95RttZtoAMs ?? null)

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

    const avgRtt = latencyData.map((d) => d.avgRttMs ?? null)
    const p95Rtt = latencyData.map((d) => d.p95RttMs ?? null)

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
      const jitterAtoZ = latencyData.map((d) => d.jitterAtoZMs ?? null)
      const jitterZtoA = latencyData.map((d) => d.jitterZtoAMs ?? null)

      series.push(
        { label: 'jitterAtoZMs', stroke: jitterAColor, width: 1.5, points: { show: false } },
        { label: 'jitterZtoAMs', stroke: jitterZColor, width: 1.5, points: { show: false } },
      )

      return {
        jitterUPlotData: [timestamps, jitterAtoZ, jitterZtoA] as uPlot.AlignedData,
        jitterUPlotSeries: series,
      }
    }

    const avgJitter = latencyData.map((d) => d.avgJitter ?? null)

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
  })

  const { plotRef: jitterPlotRef} = useUPlotChart({
    containerRef: jitterChartRef,
    data: jitterUPlotData,
    series: jitterUPlotSeries,
    height: 144,
    axes: msAxes,
  })

  // Legend sync
  useUPlotLegendSync(rttPlotRef, rttLegend, rttKeys)
  useUPlotLegendSync(jitterPlotRef, jitterLegend, jitterKeys)



  if (isLoading) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Loading latency data...
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load latency data
      </div>
    )
  }

  if (!latencyData || latencyData.length === 0) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        No latency data available for this time range
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className={className}>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Round-Trip Time</div>
        <div ref={rttChartRef} className="h-36" />
        <ChartLegend series={rttLegendSeries} legend={rttLegend} />
      </div>

      <div className={className}>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Jitter</div>
        <div ref={jitterChartRef} className="h-36" />
        <ChartLegend series={jitterLegendSeries} legend={jitterLegend} />
      </div>
    </div>
  )
}
