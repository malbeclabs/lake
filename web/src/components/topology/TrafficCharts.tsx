import { useState, useMemo, useRef } from 'react'
import { Loader2, RefreshCw } from 'lucide-react'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { ChartLegend, type ChartLegendSeries } from './ChartLegend'
import { fetchTrafficHistory, formatChartAxisRate, formatChartAxisPps, bucketLabels, resolveAutoBucket, type TimeRange, type TimeRangePreset, type BucketSize, type TrafficMetric, type TrafficView } from './utils'
import { TrafficFilters } from './TimeRangeSelector'

const COLOR = '#3b82f6'

function RefreshButton({ fetching, onClick }: { fetching: boolean; onClick: () => void }) {
  if (fetching) return <Loader2 className="h-3 w-3 animate-spin" />
  return (
    <button onClick={onClick} className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground" title="Refresh">
      <RefreshCw className="h-3 w-3" />
    </button>
  )
}

interface TrafficChartsProps {
  entityType: 'link' | 'device' | 'validator'
  entityPk: string
  timeRange?: TimeRange
  className?: string
}

export function TrafficCharts({ entityType, entityPk, timeRange, className }: TrafficChartsProps) {
  const queryClient = useQueryClient()
  const effectiveRange = timeRange ?? { preset: '24h' as const }

  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<BucketSize>('auto')
  const [trafficView, setTrafficView] = useState<TrafficView>('peak')

  const chartRef = useRef<HTMLDivElement>(null)

  const { data: trafficData, isFetching, error } = useQuery({
    queryKey: ['topology-traffic', entityType, entityPk, effectiveRange, bucket, metric],
    queryFn: () => fetchTrafficHistory(entityType, entityPk, effectiveRange, bucket, metric),
    refetchInterval: 60000,
    retry: 2,
    placeholderData: keepPreviousData,
  })

  const isPps = metric === 'packets'
  const axisFormatter = isPps ? formatChartAxisPps : formatChartAxisRate

  const chartScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  // Build columnar uPlot data
  const chartData = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return [[]] as uPlot.AlignedData

    const timestamps = trafficData.map(row => new Date(row.time).getTime() / 1000)
    const avgIn = trafficData.map(row => row.avgIn)
    const avgOut = trafficData.map(row => -row.avgOut)
    const peakIn = trafficData.map(row => row.peakIn)
    const peakOut = trafficData.map(row => -row.peakOut)

    return [timestamps, avgIn, avgOut, peakIn, peakOut] as uPlot.AlignedData
  }, [trafficData])

  const series = useMemo((): uPlot.Series[] => {
    const isAvg = trafficView === 'avg'
    return [
      {},
      { label: 'Avg In', stroke: COLOR, width: 1.5, points: { show: false }, show: isAvg },
      { label: 'Avg Out', stroke: COLOR, width: 1.5, dash: [4, 2], points: { show: false }, show: isAvg },
      { label: 'Peak In', stroke: COLOR, width: 1.5, points: { show: false }, show: !isAvg },
      { label: 'Peak Out', stroke: COLOR, width: 1.5, dash: [4, 2], points: { show: false }, show: !isAvg },
    ]
  }, [trafficView])

  const legend = useChartLegend()

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => axisFormatter(Math.abs(v))) },
  ], [axisFormatter])

  const { plotRef } = useUPlotChart({
    containerRef: chartRef,
    data: chartData,
    series,
    height: 176,
    axes,
    scales: chartScales,
  })

  useUPlotLegendSync(plotRef, legend, trafficView === 'avg'
    ? ['in', 'out', 'in', 'out']
    : ['in', 'out', 'in', 'out']
  )

  const legendSeries = useMemo((): ChartLegendSeries[] => [
    { key: 'in', color: COLOR, label: 'In' },
    { key: 'out', color: COLOR, label: 'Out', dashed: true },
  ], [])

  const effectiveBucketLabel = bucket === 'auto'
    ? bucketLabels[resolveAutoBucket(effectiveRange.preset as TimeRangePreset)]
    : undefined

  const aggLabel = trafficView === 'avg' ? 'Avg' : trafficView === 'peak' ? 'Max' : trafficView.toUpperCase()
  const metricLabel = isPps ? 'Packet Rate' : 'Traffic Rate'

  if (error && !trafficData) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load traffic data — the request may have timed out
      </div>
    )
  }

  return (
    <div className={`${className} group/chart`}>
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
          <span>{aggLabel} {metricLabel}</span>
          <RefreshButton fetching={isFetching} onClick={() => queryClient.invalidateQueries({ queryKey: ['topology-traffic'] })} />
        </div>
        <TrafficFilters
          bucket={bucket}
          onBucketChange={setBucket}
          metric={metric}
          onMetricChange={setMetric}
          effectiveBucketLabel={effectiveBucketLabel}
          trafficView={trafficView}
          onTrafficViewChange={setTrafficView}
        />
      </div>
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
        {isFetching && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>
      <div ref={chartRef} className="h-44" />
      <ChartLegend series={legendSeries} legend={legend} />
    </div>
  )
}
