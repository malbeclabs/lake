import { useMemo, useRef, useState, useCallback } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { type ChartLegendSeries } from './ChartLegend'
import { ChartLegendTable } from './ChartLegendTable'
import { fetchSingleLinkHistory } from '@/lib/api'
import type { LinkHourStatus } from '@/lib/api'
import type { BucketSize } from './utils'
import { bucketSizeToSeconds, formatHoveredTime, presetToSeconds, resolveAutoBucket, type TimeRangePreset } from './utils'

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
    (h.side_a_in_fcs_errors ?? 0) > 0 || (h.side_z_in_fcs_errors ?? 0) > 0 ||
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

  const [lossHoveredIdx, setLossHoveredIdx] = useState<number | null>(null)
  const [issuesHoveredIdx, setIssuesHoveredIdx] = useState<number | null>(null)
  const handleLossCursorIdx = useCallback((idx: number | null) => setLossHoveredIdx(idx), [])
  const handleIssuesCursorIdx = useCallback((idx: number | null) => setIssuesHoveredIdx(idx), [])

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
    placeholderData: keepPreviousData,
  })

  // Colors
  const lossColor = isDark ? '#a855f7' : '#9333ea'
  const sideAColor = isDark ? '#10b981' : '#059669'
  const sideZColor = isDark ? '#3b82f6' : '#2563eb'
  const errorColor = isDark ? '#ef4444' : '#dc2626'
  const fcsColor = isDark ? '#f97316' : '#ea580c'
  const discardColor = isDark ? '#f59e0b' : '#d97706'
  const carrierColor = isDark ? '#8b5cf6' : '#7c3aed'

  // Packet loss data
  const { packetLossUPlotData, packetLossSeries } = useMemo(() => {
    if (!historyData?.hours || historyData.hours.length === 0) {
      return { packetLossUPlotData: [[]] as uPlot.AlignedData, packetLossSeries: [] as uPlot.Series[] }
    }

    // Exclude the current incomplete bucket so the chart line doesn't drop to zero
    const completedHours = historyData.hours.filter((h) => !h.collecting)
    const timestamps = completedHours.map((h) => new Date(h.hour).getTime() / 1000)
    const total = completedHours.map((h) => h.avg_loss_pct)
    const sideA = completedHours.map((h) => h.side_a_loss_pct ?? 0)
    const sideZ = completedHours.map((h) => h.side_z_loss_pct ?? 0)

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

    const completedHours2 = historyData.hours.filter((h) => !h.collecting)
    const timestamps = completedHours2.map((h) => new Date(h.hour).getTime() / 1000)
    const errors = completedHours2.map((h) => {
      const v = (h.side_a_in_errors ?? 0) + (h.side_a_out_errors ?? 0) +
        (h.side_z_in_errors ?? 0) + (h.side_z_out_errors ?? 0)
      return v > 0 ? v : null
    })
    const fcs = completedHours2.map((h) => {
      const v = (h.side_a_in_fcs_errors ?? 0) + (h.side_z_in_fcs_errors ?? 0)
      return v > 0 ? v : null
    })
    const discards = completedHours2.map((h) => {
      const v = (h.side_a_in_discards ?? 0) + (h.side_a_out_discards ?? 0) +
        (h.side_z_in_discards ?? 0) + (h.side_z_out_discards ?? 0)
      return v > 0 ? v : null
    })
    const carrier = completedHours2.map((h) => {
      const v = (h.side_a_carrier_transitions ?? 0) + (h.side_z_carrier_transitions ?? 0)
      return v > 0 ? v : null
    })

    const series: uPlot.Series[] = [
      {},
      { label: 'errors', stroke: errorColor, width: 1.5, points: { show: true, size: 4 } },
      { label: 'fcs', stroke: fcsColor, width: 1.5, points: { show: true, size: 4 } },
      { label: 'discards', stroke: discardColor, width: 1.5, points: { show: true, size: 4 } },
      { label: 'carrier', stroke: carrierColor, width: 1.5, points: { show: true, size: 4 } },
    ]

    return {
      issuesUPlotData: [timestamps, errors, fcs, discards, carrier] as uPlot.AlignedData,
      issuesSeries: series,
    }
  }, [historyData, errorColor, fcsColor, discardColor, carrierColor])

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
    { key: 'fcs', color: fcsColor, label: 'FCS Errors' },
    { key: 'discards', color: discardColor, label: 'Discards' },
    { key: 'carrier', color: carrierColor, label: 'Carrier' },
  ], [errorColor, fcsColor, discardColor, carrierColor])

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
    onCursorIdx: handleLossCursorIdx,
  })

  const { plotRef: issuesPlotRef} = useUPlotChart({
    containerRef: interfaceIssuesChartRef,
    data: issuesUPlotData,
    series: issuesSeries,
    height: 144,
    axes: countAxes,
    onCursorIdx: handleIssuesCursorIdx,
  })

  // Legend sync
  useUPlotLegendSync(packetLossPlotRef, packetLossLegend, ['total', 'sideA', 'sideZ'])
  useUPlotLegendSync(issuesPlotRef, interfaceIssueLegend, ['errors', 'fcs', 'discards', 'carrier'])

  // Display values: hovered or latest
  const lossDisplayValues = useMemo(() => {
    const map = new Map<string, string>()
    if (packetLossUPlotData[0].length === 0) return map
    const idx = lossHoveredIdx != null && lossHoveredIdx < packetLossUPlotData[0].length ? lossHoveredIdx : packetLossUPlotData[0].length - 1
    const keys = ['total', 'sideA', 'sideZ']
    for (let i = 0; i < keys.length; i++) {
      const val = (packetLossUPlotData[i + 1] as (number | null)[])?.[idx]
      map.set(keys[i], val != null ? `${val.toFixed(2)}%` : '—')
    }
    return map
  }, [packetLossUPlotData, lossHoveredIdx])

  const issuesDisplayValues = useMemo(() => {
    const map = new Map<string, string>()
    if (issuesUPlotData[0].length === 0) return map
    const idx = issuesHoveredIdx != null && issuesHoveredIdx < issuesUPlotData[0].length ? issuesHoveredIdx : issuesUPlotData[0].length - 1
    const keys = ['errors', 'fcs', 'discards', 'carrier']
    for (let i = 0; i < keys.length; i++) {
      const val = (issuesUPlotData[i + 1] as (number | null)[])?.[idx]
      map.set(keys[i], val != null ? formatCount(val) : '—')
    }
    return map
  }, [issuesUPlotData, issuesHoveredIdx])

  // Max values across the time range
  const lossMaxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (packetLossUPlotData[0].length === 0) return map
    const keys = ['total', 'sideA', 'sideZ']
    for (let i = 0; i < keys.length; i++) {
      const series = packetLossUPlotData[i + 1] as (number | null)[]
      let max = 0
      if (series) for (const v of series) if (v != null && v > max) max = v
      map.set(keys[i], `${max.toFixed(2)}%`)
    }
    return map
  }, [packetLossUPlotData])

  const issuesMaxValues = useMemo(() => {
    const map = new Map<string, string>()
    if (issuesUPlotData[0].length === 0) return map
    const keys = ['errors', 'fcs', 'discards', 'carrier']
    for (let i = 0; i < keys.length; i++) {
      const series = issuesUPlotData[i + 1] as (number | null)[]
      let max = 0
      if (series) for (const v of series) if (v != null && v > max) max = v
      map.set(keys[i], formatCount(max))
    }
    return map
  }, [issuesUPlotData])

  const lossHoveredTime = useMemo(() =>
    formatHoveredTime(packetLossUPlotData[0] as ArrayLike<number>, lossHoveredIdx),
    [packetLossUPlotData, lossHoveredIdx])
  const issuesHoveredTime = useMemo(() =>
    formatHoveredTime(issuesUPlotData[0] as ArrayLike<number>, issuesHoveredIdx),
    [issuesUPlotData, issuesHoveredIdx])

  if (isLoading && !historyData) return null
  if (error || !historyData?.hours || historyData.hours.length === 0) return null

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
          <ChartLegendTable series={packetLossLegendSeries} legend={packetLossLegend} values={lossDisplayValues} maxValues={lossMaxValues} hoveredTime={lossHoveredTime} />
        </div>
      )}

      {showInterfaceIssues && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Interface Issues ({timeRange})
          </div>
          <div ref={interfaceIssuesChartRef} className="h-36" />
          <ChartLegendTable series={interfaceIssueLegendSeries} legend={interfaceIssueLegend} values={issuesDisplayValues} maxValues={issuesMaxValues} hoveredTime={issuesHoveredTime} />
        </div>
      )}
    </div>
  )
}
