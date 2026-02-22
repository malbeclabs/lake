import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid, ReferenceLine } from 'recharts'
import { fetchTrafficHistory, formatChartAxisRate, formatChartTooltipRate, formatChartAxisPps, formatChartTooltipPps, bucketLabels, resolveAutoBucket, type TimeRange, type TimeRangePreset, type BucketSize, type TrafficMetric } from './utils'
import { TrafficFilters, getTimeRangeLabel } from './TimeRangeSelector'

const COLOR = '#3b82f6'

interface TrafficChartsProps {
  entityType: 'link' | 'device' | 'validator'
  entityPk: string
  timeRange?: TimeRange
}

export function TrafficCharts({ entityType, entityPk, timeRange }: TrafficChartsProps) {
  const effectiveRange = timeRange ?? { preset: '24h' as const }
  const rangeLabel = getTimeRangeLabel(effectiveRange)

  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<BucketSize>('auto')
  const [snapToPeak, setSnapToPeak] = useState(false)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['topology-traffic', entityType, entityPk, effectiveRange, bucket, metric],
    queryFn: () => fetchTrafficHistory(entityType, entityPk, effectiveRange, bucket, metric),
    refetchInterval: 60000,
  })

  const isPps = metric === 'packets'
  const axisFormatter = isPps ? formatChartAxisPps : formatChartAxisRate
  const tooltipFormatter = isPps ? formatChartTooltipPps : formatChartTooltipRate

  // Build mirrored chart data: negate out values
  const { avgData, peakData } = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return { avgData: [], peakData: [] }
    return {
      avgData: trafficData.map(row => ({
        time: row.time,
        in: row.avgIn,
        out: -row.avgOut,
      })),
      peakData: trafficData.map(row => ({
        time: row.time,
        in: row.peakIn,
        out: -row.peakOut,
      })),
    }
  }, [trafficData])

  // Snap-to-peak: find index with highest absolute value in a window around hovered point
  const effectiveIdx = useMemo(() => {
    if (hoveredIdx === null) return null
    if (!snapToPeak || avgData.length === 0) return hoveredIdx

    const peakWindow = Math.min(150, Math.max(5, Math.round(avgData.length * 0.05)))
    const lo = Math.max(0, hoveredIdx - peakWindow)
    const hi = Math.min(avgData.length - 1, hoveredIdx + peakWindow)
    let bestIdx = hoveredIdx
    let bestVal = -Infinity

    for (let i = lo; i <= hi; i++) {
      const row = avgData[i]
      const rowMax = Math.max(Math.abs(row.in), Math.abs(row.out))
      if (rowMax > bestVal) {
        bestVal = rowMax
        bestIdx = i
      }
    }
    return bestIdx
  }, [hoveredIdx, snapToPeak, avgData])

  const legendValues = useMemo(() => {
    const getRow = (data: typeof avgData) => {
      if (data.length === 0) return { in: '—', out: '—' }
      const row = effectiveIdx !== null && effectiveIdx < data.length
        ? data[effectiveIdx]
        : data[data.length - 1]
      return {
        in: tooltipFormatter(Math.abs(row.in)),
        out: tooltipFormatter(Math.abs(row.out)),
      }
    }
    return { avg: getRow(avgData), peak: getRow(peakData) }
  }, [avgData, peakData, effectiveIdx, tooltipFormatter])

  if (isLoading) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Loading traffic data...
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

  const renderChart = (label: string, data: typeof avgData, values: { in: string; out: string }) => (
    <div>
      <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
        {label}
      </div>
      <div className="h-44 relative">
        <span className="absolute top-0 left-0 text-[10px] text-muted-foreground/60 pointer-events-none z-10">▲ In</span>
        <span className="absolute bottom-5 left-0 text-[10px] text-muted-foreground/60 pointer-events-none z-10">▼ Out</span>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={data}
            onMouseMove={(state) => {
              if (state?.activeTooltipIndex != null) setHoveredIdx(Number(state.activeTooltipIndex))
            }}
            onMouseLeave={() => setHoveredIdx(null)}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 9 }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              tick={{ fontSize: 9 }}
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => axisFormatter(Math.abs(v))}
              width={40}
            />
            <ReferenceLine y={0} stroke="var(--border)" strokeWidth={1} />
            <RechartsTooltip
              content={() => null}
              cursor={{ stroke: 'var(--muted-foreground)', strokeWidth: 1, strokeDasharray: '4 2' }}
            />
            <Line
              type="monotone"
              dataKey="in"
              stroke={COLOR}
              strokeWidth={1.5}
              dot={false}
              name="In"
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="out"
              stroke={COLOR}
              strokeWidth={1.5}
              strokeDasharray="4 2"
              dot={false}
              name="Out"
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div className="flex items-center gap-4 mt-1.5 text-xs text-muted-foreground px-1">
        <div className="flex items-center gap-1.5">
          <span className="w-2.5 h-0.5 rounded-sm flex-shrink-0" style={{ backgroundColor: COLOR, display: 'inline-block' }} />
          <span>In</span>
          <span className="text-foreground tabular-nums font-mono">{values.in}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="w-2.5 h-0.5 rounded-sm flex-shrink-0 border-t border-dashed" style={{ borderColor: COLOR, display: 'inline-block' }} />
          <span>Out</span>
          <span className="text-foreground tabular-nums font-mono">{values.out}</span>
        </div>
      </div>
    </div>
  )

  const metricLabel = isPps ? 'Packet Rate' : 'Traffic Rate'

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">Traffic ({rangeLabel})</h3>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setSnapToPeak(v => !v)}
            className={`text-xs rounded px-1.5 py-1 cursor-pointer transition-colors inline-flex items-center gap-1 border text-foreground ${
              snapToPeak
                ? 'bg-muted border-border'
                : 'bg-transparent border-border hover:bg-muted/50'
            }`}
            title="Snap hover to nearest peak value"
          >
            snap to peak
          </button>
          <TrafficFilters
            bucket={bucket}
            onBucketChange={setBucket}
            metric={metric}
            onMetricChange={setMetric}
            effectiveBucketLabel={bucket === 'auto' ? bucketLabels[resolveAutoBucket(effectiveRange.preset as TimeRangePreset)] : undefined}
          />
        </div>
      </div>

      {renderChart(`Avg ${metricLabel}`, avgData, legendValues.avg)}
      {renderChart(`Peak ${metricLabel}`, peakData, legendValues.peak)}
    </div>
  )
}
