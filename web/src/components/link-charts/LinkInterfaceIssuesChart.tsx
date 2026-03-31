import { useMemo, useRef, useState, useCallback } from 'react'
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

interface LinkInterfaceIssuesChartProps {
  data: LinkMetricsResponse
  className?: string
  loading?: boolean
}

function formatCount(value: number): string {
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toString()
}

export function LinkInterfaceIssuesChart({ data, className, loading }: LinkInterfaceIssuesChartProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const chartRef = useRef<HTMLDivElement>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const handleCursorIdx = useCallback((idx: number | null) => setHoveredIdx(idx), [])

  // Colors matching LinkStatusCharts
  const errorColor = isDark ? '#ef4444' : '#dc2626'
  const fcsColor = isDark ? '#f97316' : '#ea580c'
  const discardColor = isDark ? '#f59e0b' : '#d97706'
  const carrierColor = isDark ? '#8b5cf6' : '#7c3aed'

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => formatCount(v)) },
  ], [])

  const { uPlotData, uPlotSeries } = useMemo(() => {
    const buckets = data.buckets.filter((b) => !b.status?.collecting)
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)
    const errors = buckets.map((b) => {
      if (!b.traffic) return null
      const t = b.traffic
      const v = t.side_a_in_errors + t.side_a_out_errors + t.side_z_in_errors + t.side_z_out_errors
      return v > 0 ? v : null
    })
    const fcs = buckets.map((b) => {
      if (!b.traffic) return null
      const t = b.traffic
      const v = t.side_a_in_fcs_errors + t.side_z_in_fcs_errors
      return v > 0 ? v : null
    })
    const discards = buckets.map((b) => {
      if (!b.traffic) return null
      const t = b.traffic
      const v = t.side_a_in_discards + t.side_a_out_discards + t.side_z_in_discards + t.side_z_out_discards
      return v > 0 ? v : null
    })
    const carrier = buckets.map((b) => {
      if (!b.traffic) return null
      const t = b.traffic
      const v = t.side_a_carrier_transitions + t.side_z_carrier_transitions
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
      uPlotData: [timestamps, errors, fcs, discards, carrier] as uPlot.AlignedData,
      uPlotSeries: series,
    }
  }, [data, errorColor, fcsColor, discardColor, carrierColor])

  const legend = useChartLegend()
  const legendSeries: ChartLegendSeries[] = useMemo(() => [
    { key: 'errors', color: errorColor, label: 'Errors' },
    { key: 'fcs', color: fcsColor, label: 'FCS Errors' },
    { key: 'discards', color: discardColor, label: 'Discards' },
    { key: 'carrier', color: carrierColor, label: 'Carrier' },
  ], [errorColor, fcsColor, discardColor, carrierColor])

  const seriesKeys = ['errors', 'fcs', 'discards', 'carrier']

  const { plotRef } = useUPlotChart({
    containerRef: chartRef,
    data: uPlotData,
    series: uPlotSeries,
    height: 144,
    axes,
    scales,
    onCursorIdx: handleCursorIdx,
  })

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
      map.set(seriesKeys[i], val != null ? formatCount(val) : '--')
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
      map.set(seriesKeys[i], formatCount(max))
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
          <span>Interface Issues</span>
        </div>
        <div className="text-xs text-muted-foreground/60 py-6 text-center">No data for this time range</div>
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
      <ChartLegendTable series={legendSeries} legend={legend} values={displayValues} maxValues={maxValues} hoveredTime={hoveredTime} />
    </div>
  )
}
