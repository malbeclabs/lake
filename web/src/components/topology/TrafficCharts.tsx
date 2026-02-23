import { useState, useMemo, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { ChartLegend, type ChartLegendSeries } from './ChartLegend'
import { fetchTrafficHistory, formatChartAxisRate, formatChartAxisPps, bucketLabels, resolveAutoBucket, type TimeRange, type TimeRangePreset, type BucketSize, type TrafficMetric } from './utils'
import { TrafficFilters } from './TimeRangeSelector'

const COLOR = '#3b82f6'

interface TrafficChartsProps {
  entityType: 'link' | 'device' | 'validator'
  entityPk: string
  timeRange?: TimeRange
  /** Additional CSS classes for the outer wrapper */
  className?: string
}

export function TrafficCharts({ entityType, entityPk, timeRange, className }: TrafficChartsProps) {
  const effectiveRange = timeRange ?? { preset: '24h' as const }


  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<BucketSize>('auto')
  const [trafficView, setTrafficView] = useState<'avg' | 'peak'>('avg')

  const chartRef = useRef<HTMLDivElement>(null)

  const { data: trafficData, isLoading, error } = useQuery({
    queryKey: ['topology-traffic', entityType, entityPk, effectiveRange, bucket, metric],
    queryFn: () => fetchTrafficHistory(entityType, entityPk, effectiveRange, bucket, metric),
    refetchInterval: 60000,
    retry: 2,
  })

  const isPps = metric === 'packets'
  const axisFormatter = isPps ? formatChartAxisPps : formatChartAxisRate

  // Build columnar uPlot data: timestamps, avg in, avg out (negated), peak in, peak out (negated)
  const chartData = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return [[]] as uPlot.AlignedData

    const timestamps = trafficData.map(row => new Date(row.time).getTime() / 1000)
    const avgIn = trafficData.map(row => row.avgIn)
    const avgOut = trafficData.map(row => -row.avgOut)
    const peakIn = trafficData.map(row => row.peakIn)
    const peakOut = trafficData.map(row => -row.peakOut)

    return [timestamps, avgIn, avgOut, peakIn, peakOut] as uPlot.AlignedData
  }, [trafficData])

  // Series config: show avg or peak based on trafficView
  const series = useMemo((): uPlot.Series[] => {
    const isAvg = trafficView === 'avg'
    return [
      {}, // x-axis
      { label: 'Avg In', stroke: COLOR, width: 1.5, points: { show: false }, show: isAvg },
      { label: 'Avg Out', stroke: COLOR, width: 1.5, dash: [4, 2], points: { show: false }, show: isAvg },
      { label: 'Peak In', stroke: COLOR, width: 1.5, points: { show: false }, show: !isAvg },
      { label: 'Peak Out', stroke: COLOR, width: 1.5, dash: [4, 2], points: { show: false }, show: !isAvg },
    ]
  }, [trafficView])

  // Legend keys: in and out
  const legend = useChartLegend()

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    {
      values: (_u: uPlot, vals: number[]) => vals.map((v) => axisFormatter(Math.abs(v))),
    },
  ], [axisFormatter])

  const { plotRef } = useUPlotChart({
    containerRef: chartRef,
    data: chartData,
    series,
    height: 176,
    axes,
  })

  // Sync legend visibility to chart series
  useUPlotLegendSync(plotRef, legend, trafficView === 'avg'
    ? ['in', 'out', 'in', 'out'] // avg in, avg out, peak in (hidden), peak out (hidden)
    : ['in', 'out', 'in', 'out'] // avg in (hidden), avg out (hidden), peak in, peak out
  )

  // Legend display values
  const legendSeries = useMemo((): ChartLegendSeries[] => [
    { key: 'in', color: COLOR, label: 'In' },
    { key: 'out', color: COLOR, label: 'Out', dashed: true },
  ], [])

  const effectiveBucketLabel = bucket === 'auto'
    ? bucketLabels[resolveAutoBucket(effectiveRange.preset as TimeRangePreset)]
    : undefined

  const metricLabel = isPps ? 'Packet Rate' : 'Traffic Rate'

  if (isLoading) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Loading traffic data...
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load traffic data
      </div>
    )
  }

  if (!trafficData || trafficData.length === 0) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        No traffic data available
      </div>
    )
  }

  return (
    <div className={className}>
      <div className="flex items-center justify-between mb-2">
        <div className="text-xs text-muted-foreground uppercase tracking-wider">
          {trafficView === 'avg' ? 'Avg' : 'Peak'} {metricLabel}
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
      <div ref={chartRef} className="h-44" />
      <ChartLegend series={legendSeries} legend={legend} />
    </div>
  )
}
