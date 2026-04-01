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
import type { DeviceMetricsResponse, DeviceMetricsTraffic } from '@/lib/api'

interface DeviceTrafficChartProps {
  data: DeviceMetricsResponse
  className?: string
  loading?: boolean
}

type AggMode = 'avg' | 'peak'
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

function getTrafficValue(t: DeviceMetricsTraffic, dir: 'in' | 'out', agg: AggMode, metric: MetricMode): number {
  const prefix = agg === 'peak' ? (metric === 'bps' ? `max_${dir}_bps` : `max_${dir}_pps`) :
    (metric === 'bps' ? `${dir}_bps` : `${dir}_pps`)
  return (t as unknown as Record<string, number>)[prefix] ?? 0
}

export function DeviceTrafficChart({ data, className, loading }: DeviceTrafficChartProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const chartRef = useRef<HTMLDivElement>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const handleCursorIdx = useCallback((idx: number | null) => setHoveredIdx(idx), [])
  const [bidir, setBidir] = useState(true)
  const [aggMode, setAggMode] = useState<AggMode>('avg')
  const [metricMode, setMetricMode] = useState<MetricMode>('bps')

  const fmt = metricMode === 'bps' ? formatBps : formatPps

  const inColor = isDark ? '#22c55e' : '#16a34a'
  const outColor = isDark ? '#3b82f6' : '#2563eb'

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => fmt(v)) },
  ], [fmt])

  const seriesKeys = useMemo(() => bidir ? ['rx', 'tx'] : ['in', 'out'], [bidir])

  const { uPlotData, uPlotSeries } = useMemo(() => {
    const buckets = data.buckets.filter((b) => !b.status?.collecting)
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[] }
    }

    const timestamps = buckets.map((b) => new Date(b.ts).getTime() / 1000)
    const val = (t: DeviceMetricsTraffic | undefined, dir: 'in' | 'out') =>
      t ? getTrafficValue(t, dir, aggMode, metricMode) || null : null

    if (bidir) {
      const rx = buckets.map((b) => val(b.traffic, 'in'))
      const tx = buckets.map((b) => { const v = val(b.traffic, 'out'); return v ? -v : null })

      return {
        uPlotData: [timestamps, rx, tx] as uPlot.AlignedData,
        uPlotSeries: [
          {},
          { label: 'rx', stroke: inColor, width: 1.5, points: { show: false } },
          { label: 'tx', stroke: outColor, width: 1.5, dash: [4, 2], points: { show: false } },
        ] as uPlot.Series[],
      }
    }

    const inVals = buckets.map((b) => val(b.traffic, 'in'))
    const outVals = buckets.map((b) => val(b.traffic, 'out'))

    return {
      uPlotData: [timestamps, inVals, outVals] as uPlot.AlignedData,
      uPlotSeries: [
        {},
        { label: 'in', stroke: inColor, width: 1.5, points: { show: false } },
        { label: 'out', stroke: outColor, width: 1.5, dash: [4, 2], points: { show: false } },
      ] as uPlot.Series[],
    }
  }, [data, bidir, aggMode, metricMode, inColor, outColor])

  const legend = useChartLegend()
  const legendSeries: ChartLegendSeries[] = useMemo(() =>
    bidir
      ? [
          { key: 'rx', color: inColor, label: 'Rx' },
          { key: 'tx', color: outColor, label: 'Tx', dashed: true },
        ]
      : [
          { key: 'in', color: inColor, label: 'In' },
          { key: 'out', color: outColor, label: 'Out', dashed: true },
        ],
    [bidir, inColor, outColor])

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
            <option value="peak">Peak</option>
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
