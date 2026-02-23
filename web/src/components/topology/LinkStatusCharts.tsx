import { useMemo, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { ChartLegend, type ChartLegendSeries } from './ChartLegend'
import { fetchSingleLinkHistory } from '@/lib/api'
import type { LinkHourStatus } from '@/lib/api'
import type { BucketSize } from './utils'
import { bucketSizeToSeconds, presetToSeconds, resolveAutoBucket, type TimeRangePreset } from './utils'

interface LinkStatusChartsProps {
  linkPk: string
  timeRange?: string
  bucket?: BucketSize
  /** Additional CSS classes for the outer wrapper */
  className?: string
}

function hasPacketLossData(hours: LinkHourStatus[]): boolean {
  return hours.some(h => h.avg_loss_pct > 0)
}

function hasInterfaceIssueData(hours: LinkHourStatus[]): boolean {
  return hours.some(h =>
    (h.side_a_in_errors ?? 0) > 0 || (h.side_a_out_errors ?? 0) > 0 ||
    (h.side_z_in_errors ?? 0) > 0 || (h.side_z_out_errors ?? 0) > 0 ||
    (h.side_a_in_discards ?? 0) > 0 || (h.side_a_out_discards ?? 0) > 0 ||
    (h.side_z_in_discards ?? 0) > 0 || (h.side_z_out_discards ?? 0) > 0 ||
    (h.side_a_carrier_transitions ?? 0) > 0 || (h.side_z_carrier_transitions ?? 0) > 0
  )
}

function formatCount(value: number): string {
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toString()
}

export function LinkStatusCharts({ linkPk, timeRange = '24h', bucket, className }: LinkStatusChartsProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const packetLossChartRef = useRef<HTMLDivElement>(null)
  const interfaceIssuesChartRef = useRef<HTMLDivElement>(null)

  // Convert bucket size to bucket count for the API
  const bucketCount = useMemo(() => {
    const effectiveBucket = (!bucket || bucket === 'auto')
      ? resolveAutoBucket(timeRange as TimeRangePreset)
      : bucket
    const rangeS = presetToSeconds(timeRange as TimeRangePreset)
    const bucketS = bucketSizeToSeconds(effectiveBucket)
    return Math.ceil(rangeS / bucketS)
  }, [bucket, timeRange])

  const { data: historyData, isLoading, error } = useQuery({
    queryKey: ['single-link-history', linkPk, timeRange, bucketCount],
    queryFn: () => fetchSingleLinkHistory(linkPk, timeRange, bucketCount),
    refetchInterval: 60000,
    retry: false,
  })

  // Colors
  const lossColor = isDark ? '#a855f7' : '#9333ea'
  const sideAColor = isDark ? '#10b981' : '#059669'
  const sideZColor = isDark ? '#3b82f6' : '#2563eb'
  const errorColor = isDark ? '#ef4444' : '#dc2626'
  const discardColor = isDark ? '#f59e0b' : '#d97706'
  const carrierColor = isDark ? '#8b5cf6' : '#7c3aed'

  // Packet loss data
  const { packetLossUPlotData, packetLossSeries } = useMemo(() => {
    if (!historyData?.hours || historyData.hours.length === 0) {
      return { packetLossUPlotData: [[]] as uPlot.AlignedData, packetLossSeries: [] as uPlot.Series[] }
    }

    const timestamps = historyData.hours.map((h) => new Date(h.hour).getTime() / 1000)
    const total = historyData.hours.map((h) => h.avg_loss_pct)
    const sideA = historyData.hours.map((h) => h.side_a_loss_pct ?? 0)
    const sideZ = historyData.hours.map((h) => h.side_z_loss_pct ?? 0)

    const series: uPlot.Series[] = [
      {},
      { label: 'total', stroke: lossColor, width: 1.5, dash: [4, 2], points: { show: false } },
      { label: 'sideA', stroke: sideAColor, width: 1.5, points: { show: false } },
      { label: 'sideZ', stroke: sideZColor, width: 1.5, points: { show: false } },
    ]

    return {
      packetLossUPlotData: [timestamps, total, sideA, sideZ] as uPlot.AlignedData,
      packetLossSeries: series,
    }
  }, [historyData, lossColor, sideAColor, sideZColor])

  // Interface issues data
  const { issuesUPlotData, issuesSeries } = useMemo(() => {
    if (!historyData?.hours || historyData.hours.length === 0) {
      return { issuesUPlotData: [[]] as uPlot.AlignedData, issuesSeries: [] as uPlot.Series[] }
    }

    const timestamps = historyData.hours.map((h) => new Date(h.hour).getTime() / 1000)
    const errors = historyData.hours.map((h) =>
      (h.side_a_in_errors ?? 0) + (h.side_a_out_errors ?? 0) +
      (h.side_z_in_errors ?? 0) + (h.side_z_out_errors ?? 0)
    )
    const discards = historyData.hours.map((h) =>
      (h.side_a_in_discards ?? 0) + (h.side_a_out_discards ?? 0) +
      (h.side_z_in_discards ?? 0) + (h.side_z_out_discards ?? 0)
    )
    const carrier = historyData.hours.map((h) =>
      (h.side_a_carrier_transitions ?? 0) + (h.side_z_carrier_transitions ?? 0)
    )

    const series: uPlot.Series[] = [
      {},
      { label: 'errors', stroke: errorColor, width: 1.5, points: { show: true, size: 4 } },
      { label: 'discards', stroke: discardColor, width: 1.5, points: { show: true, size: 4 } },
      { label: 'carrier', stroke: carrierColor, width: 1.5, points: { show: true, size: 4 } },
    ]

    return {
      issuesUPlotData: [timestamps, errors, discards, carrier] as uPlot.AlignedData,
      issuesSeries: series,
    }
  }, [historyData, errorColor, discardColor, carrierColor])

  const showPacketLoss = historyData?.hours && hasPacketLossData(historyData.hours)
  const showInterfaceIssues = historyData?.hours && hasInterfaceIssueData(historyData.hours)

  // Legends
  const packetLossLegend = useChartLegend()
  const packetLossLegendSeries: ChartLegendSeries[] = useMemo(() => [
    { key: 'total', color: lossColor, label: 'Average', dashed: true },
    { key: 'sideA', color: sideAColor, label: 'Side A' },
    { key: 'sideZ', color: sideZColor, label: 'Side Z' },
  ], [lossColor, sideAColor, sideZColor])

  const interfaceIssueLegend = useChartLegend()
  const interfaceIssueLegendSeries: ChartLegendSeries[] = useMemo(() => [
    { key: 'errors', color: errorColor, label: 'Errors' },
    { key: 'discards', color: discardColor, label: 'Discards' },
    { key: 'carrier', color: carrierColor, label: 'Carrier' },
  ], [errorColor, discardColor, carrierColor])

  // Axes
  const pctAxes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => `${v.toFixed(1)}%`) },
  ], [])

  const countAxes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => formatCount(v)) },
  ], [])

  // Charts
  const { plotRef: packetLossPlotRef} = useUPlotChart({
    containerRef: packetLossChartRef,
    data: packetLossUPlotData,
    series: packetLossSeries,
    height: 144,
    axes: pctAxes,
  })

  const { plotRef: issuesPlotRef} = useUPlotChart({
    containerRef: interfaceIssuesChartRef,
    data: issuesUPlotData,
    series: issuesSeries,
    height: 144,
    axes: countAxes,
  })

  // Legend sync
  useUPlotLegendSync(packetLossPlotRef, packetLossLegend, ['total', 'sideA', 'sideZ'])
  useUPlotLegendSync(issuesPlotRef, interfaceIssueLegend, ['errors', 'discards', 'carrier'])

  if (isLoading) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Loading link status data...
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load link status data
      </div>
    )
  }

  if (!historyData?.hours || historyData.hours.length === 0) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        No link status data available
      </div>
    )
  }

  if (!showPacketLoss && !showInterfaceIssues) {
    return null
  }

  return (
    <div className="space-y-6">
      {showPacketLoss && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Packet Loss ({timeRange})
          </div>
          <div ref={packetLossChartRef} className="h-36" />
          <ChartLegend series={packetLossLegendSeries} legend={packetLossLegend} />
        </div>
      )}

      {showInterfaceIssues && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Interface Issues ({timeRange})
          </div>
          <div ref={interfaceIssuesChartRef} className="h-36" />
          <ChartLegend series={interfaceIssueLegendSeries} legend={interfaceIssueLegend} />
        </div>
      )}
    </div>
  )
}
