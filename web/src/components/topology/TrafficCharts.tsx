import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid } from 'recharts'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { ChartLegend, type ChartLegendSeries } from './ChartLegend'
import { fetchTrafficHistory, formatChartAxisRate, formatChartTooltipRate, type TimeRange } from './utils'
import { getTimeRangeLabel } from './TimeRangeSelector'

interface TrafficChartsProps {
  entityType: 'link' | 'device' | 'validator'
  entityPk: string
  timeRange?: TimeRange
}

export function TrafficCharts({ entityType, entityPk, timeRange }: TrafficChartsProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const effectiveRange = timeRange ?? { preset: '24h' as const }
  const rangeLabel = getTimeRangeLabel(effectiveRange)

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['topology-traffic', entityType, entityPk, effectiveRange],
    queryFn: () => fetchTrafficHistory(entityType, entityPk, effectiveRange),
    refetchInterval: 60000,
  })

  const chartColor = isDark ? '#60a5fa' : '#2563eb'
  const chartColorSecondary = isDark ? '#f97316' : '#ea580c'

  const avgKeys = useMemo(() => ['avgIn', 'avgOut'], [])
  const peakKeys = useMemo(() => ['peakIn', 'peakOut'], [])
  const avgLegend = useChartLegend(avgKeys)
  const peakLegend = useChartLegend(peakKeys)

  const avgSeries: ChartLegendSeries[] = useMemo(() => [
    { key: 'avgIn', color: chartColor, label: 'In' },
    { key: 'avgOut', color: chartColorSecondary, label: 'Out' },
  ], [chartColor, chartColorSecondary])

  const peakSeries: ChartLegendSeries[] = useMemo(() => [
    { key: 'peakIn', color: chartColor, label: 'In' },
    { key: 'peakOut', color: chartColorSecondary, label: 'Out' },
  ], [chartColor, chartColorSecondary])

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

  return (
    <div className="space-y-4">
      {/* Average Traffic Chart */}
      <div>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Avg Traffic Rate ({rangeLabel})
        </div>
        <div className="h-36">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={trafficData}>
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
                tickFormatter={(v) => formatChartAxisRate(v)}
                width={40}
              />
              <RechartsTooltip
                contentStyle={{
                  backgroundColor: 'var(--card)',
                  border: '1px solid var(--border)',
                  borderRadius: '6px',
                  fontSize: '11px',
                }}
                formatter={(value) => formatChartTooltipRate(value as number)}
              />
              <Line
                type="monotone"
                dataKey="avgIn"
                stroke={chartColor}
                strokeWidth={1.5}
                strokeOpacity={avgLegend.getOpacity('avgIn')}
                dot={false}
                name="In"
              />
              <Line
                type="monotone"
                dataKey="avgOut"
                stroke={chartColorSecondary}
                strokeWidth={1.5}
                strokeOpacity={avgLegend.getOpacity('avgOut')}
                dot={false}
                name="Out"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <ChartLegend series={avgSeries} legend={avgLegend} />
      </div>

      {/* Peak Traffic Chart */}
      <div>
        <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
          Peak Traffic Rate ({rangeLabel})
        </div>
        <div className="h-36">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={trafficData}>
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
                tickFormatter={(v) => formatChartAxisRate(v)}
                width={40}
              />
              <RechartsTooltip
                contentStyle={{
                  backgroundColor: 'var(--card)',
                  border: '1px solid var(--border)',
                  borderRadius: '6px',
                  fontSize: '11px',
                }}
                formatter={(value) => formatChartTooltipRate(value as number)}
              />
              <Line
                type="monotone"
                dataKey="peakIn"
                stroke={chartColor}
                strokeWidth={1.5}
                strokeOpacity={peakLegend.getOpacity('peakIn')}
                dot={false}
                name="In"
              />
              <Line
                type="monotone"
                dataKey="peakOut"
                stroke={chartColorSecondary}
                strokeWidth={1.5}
                strokeOpacity={peakLegend.getOpacity('peakOut')}
                dot={false}
                name="Out"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <ChartLegend series={peakSeries} legend={peakLegend} />
      </div>
    </div>
  )
}
