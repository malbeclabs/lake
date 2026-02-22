import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid } from 'recharts'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { ChartLegend, type ChartLegendSeries } from './ChartLegend'
import { fetchLatencyHistory, type TimeRange } from './utils'
import { getTimeRangeLabel } from './TimeRangeSelector'

interface LatencyChartsProps {
  linkPk: string
  timeRange?: TimeRange
}

export function LatencyCharts({ linkPk, timeRange }: LatencyChartsProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const effectiveRange = timeRange ?? { preset: '24h' as const }

  const { data: latencyData, isLoading } = useQuery({
    queryKey: ['topology-latency', linkPk, effectiveRange],
    queryFn: () => fetchLatencyHistory(linkPk, effectiveRange),
    refetchInterval: 60000,
  })

  // Colors for per-direction data
  const rttAAvgColor = isDark ? '#22c55e' : '#16a34a' // green for A avg
  const rttAP95Color = isDark ? '#86efac' : '#4ade80' // light green for A p95
  const rttZAvgColor = isDark ? '#3b82f6' : '#2563eb' // blue for Z avg
  const rttZP95Color = isDark ? '#93c5fd' : '#60a5fa' // light blue for Z p95
  const jitterAColor = isDark ? '#a855f7' : '#9333ea' // purple for A
  const jitterZColor = isDark ? '#f97316' : '#ea580c' // orange for Z

  // Check if we have per-direction data
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
  const rttLegend = useChartLegend(rttKeys)
  const rttSeries: ChartLegendSeries[] = useMemo(() =>
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
  const jitterLegend = useChartLegend(jitterKeys)
  const jitterSeries: ChartLegendSeries[] = useMemo(() =>
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

  const rangeLabel = getTimeRangeLabel(effectiveRange)

  return (
    <div className="space-y-4">
      {isLoading ? (
        <div className="text-sm text-muted-foreground text-center py-4">
          Loading latency data...
        </div>
      ) : !latencyData || latencyData.length === 0 ? (
        <div className="text-sm text-muted-foreground text-center py-4">
          No latency data available for this time range
        </div>
      ) : (
        <>
      {/* RTT Chart - Per Direction */}
      <div>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Round-Trip Time by Direction ({rangeLabel})
        </div>
        <div className="h-36">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={latencyData}>
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
                tickFormatter={(v) => `${v.toFixed(1)}`}
                width={35}
                unit="ms"
              />
              <RechartsTooltip
                contentStyle={{
                  backgroundColor: 'var(--card)',
                  border: '1px solid var(--border)',
                  borderRadius: '6px',
                  fontSize: '11px',
                }}
                formatter={(value, name) => {
                  const labels: Record<string, string> = {
                    avgRttAtoZMs: 'Avg from A',
                    p95RttAtoZMs: 'P95 from A',
                    avgRttZtoAMs: 'Avg from Z',
                    p95RttZtoAMs: 'P95 from Z',
                    avgRttMs: 'Avg',
                    p95RttMs: 'P95',
                  }
                  return [`${Number(value ?? 0).toFixed(2)} ms`, labels[name ?? ''] || name || '']
                }}
              />
              {hasDirectionalData ? (
                <>
                  <Line
                    type="monotone"
                    dataKey="avgRttAtoZMs"
                    stroke={rttAAvgColor}
                    strokeWidth={1.5}
                    strokeOpacity={rttLegend.getOpacity('avgRttAtoZMs')}
                    dot={false}
                    name="avgRttAtoZMs"
                  />
                  <Line
                    type="monotone"
                    dataKey="p95RttAtoZMs"
                    stroke={rttAP95Color}
                    strokeWidth={1.5}
                    strokeOpacity={rttLegend.getOpacity('p95RttAtoZMs')}
                    strokeDasharray="4 2"
                    dot={false}
                    name="p95RttAtoZMs"
                  />
                  <Line
                    type="monotone"
                    dataKey="avgRttZtoAMs"
                    stroke={rttZAvgColor}
                    strokeWidth={1.5}
                    strokeOpacity={rttLegend.getOpacity('avgRttZtoAMs')}
                    dot={false}
                    name="avgRttZtoAMs"
                  />
                  <Line
                    type="monotone"
                    dataKey="p95RttZtoAMs"
                    stroke={rttZP95Color}
                    strokeWidth={1.5}
                    strokeOpacity={rttLegend.getOpacity('p95RttZtoAMs')}
                    strokeDasharray="4 2"
                    dot={false}
                    name="p95RttZtoAMs"
                  />
                </>
              ) : (
                <>
                  <Line
                    type="monotone"
                    dataKey="avgRttMs"
                    stroke={rttAAvgColor}
                    strokeWidth={1.5}
                    strokeOpacity={rttLegend.getOpacity('avgRttMs')}
                    dot={false}
                    name="avgRttMs"
                  />
                  <Line
                    type="monotone"
                    dataKey="p95RttMs"
                    stroke={rttAP95Color}
                    strokeWidth={1.5}
                    strokeOpacity={rttLegend.getOpacity('p95RttMs')}
                    strokeDasharray="4 2"
                    dot={false}
                    name="p95RttMs"
                  />
                </>
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
        <ChartLegend series={rttSeries} legend={rttLegend} />
      </div>

      {/* Jitter Chart - Per Direction */}
      <div>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Jitter by Direction ({rangeLabel})
        </div>
        <div className="h-36">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={latencyData}>
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
                tickFormatter={(v) => `${v.toFixed(1)}`}
                width={35}
                unit="ms"
              />
              <RechartsTooltip
                contentStyle={{
                  backgroundColor: 'var(--card)',
                  border: '1px solid var(--border)',
                  borderRadius: '6px',
                  fontSize: '11px',
                }}
                formatter={(value, name) => {
                  const label = name === 'jitterAtoZMs' ? 'From A' : name === 'jitterZtoAMs' ? 'From Z' : 'Jitter'
                  return [`${Number(value ?? 0).toFixed(2)} ms`, label]
                }}
              />
              {hasDirectionalData ? (
                <>
                  <Line
                    type="monotone"
                    dataKey="jitterAtoZMs"
                    stroke={jitterAColor}
                    strokeWidth={1.5}
                    strokeOpacity={jitterLegend.getOpacity('jitterAtoZMs')}
                    dot={false}
                    name="jitterAtoZMs"
                  />
                  <Line
                    type="monotone"
                    dataKey="jitterZtoAMs"
                    stroke={jitterZColor}
                    strokeWidth={1.5}
                    strokeOpacity={jitterLegend.getOpacity('jitterZtoAMs')}
                    dot={false}
                    name="jitterZtoAMs"
                  />
                </>
              ) : (
                <Line
                  type="monotone"
                  dataKey="avgJitter"
                  stroke={jitterAColor}
                  strokeWidth={1.5}
                  strokeOpacity={jitterLegend.getOpacity('avgJitter')}
                  dot={false}
                  name="Jitter"
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
        <ChartLegend series={jitterSeries} legend={jitterLegend} />
      </div>
        </>
      )}
    </div>
  )
}
