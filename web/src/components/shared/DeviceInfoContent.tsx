import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import type { DeviceInterface } from '@/lib/api'
import { fetchDeviceMetrics } from '@/lib/api'
import { DeviceHealthTimeline } from '@/components/device-charts/DeviceHealthTimeline'
import { DeviceTrafficChart } from '@/components/device-charts/DeviceTrafficChart'
import { DeviceInterfaceIssuesChart } from '@/components/device-charts/DeviceInterfaceIssuesChart'
import { toDeviceMetricsParams } from '@/components/shared/metrics-params'
import { TimeRangeSelector } from '@/components/topology/TimeRangeSelector'
import type { TimeRange } from '@/components/topology/utils'

// Shared device info type that both topology and device page can use
export interface DeviceInfoData {
  pk: string
  code: string
  deviceType: string
  status: string
  metroPk: string
  metroName: string
  contributorPk: string
  contributorCode: string
  userCount: number
  validatorCount: number
  stakeSol: number
  stakeShare: number
  interfaces: DeviceInterface[]
}

interface DeviceInfoContentProps {
  device: DeviceInfoData
  /** Compact mode for sidebar panels */
  compact?: boolean
  /** Controlled time range (when managed by parent) */
  timeRange?: TimeRange
  /** Callback when time range changes (when managed by parent) */
  onTimeRangeChange?: (range: TimeRange) => void
  /** Hide the status row (when rendered separately by parent) */
  hideStatusRow?: boolean
  /** Hide charts (when rendered separately by parent) */
  hideCharts?: boolean
}

function formatStake(sol: number): string {
  if (sol === 0) return '—'
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M SOL`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(1)}K SOL`
  return `${sol.toFixed(0)} SOL`
}

function formatStakeShare(share: number): string {
  if (share === 0) return '—'
  return `${share.toFixed(2)}%`
}

/**
 * Shared component for displaying device information.
 * Used by both the topology panel and the device detail page.
 */
export function DeviceInfoContent({
  device,
  compact = false,
  timeRange: controlledTimeRange,
  onTimeRangeChange,
  hideStatusRow = false,
  hideCharts = false,
}: DeviceInfoContentProps) {
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{ start: number; end: number } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  const [internalTimeRange, setInternalTimeRange] = useState<TimeRange>({ preset: '24h' })

  const timeRange = controlledTimeRange ?? internalTimeRange
  const setTimeRange = onTimeRangeChange ?? setInternalTimeRange

  const metricsParams = useMemo(() => toDeviceMetricsParams(timeRange), [timeRange])

  const { data: metrics, isFetching: metricsFetching } = useQuery({
    queryKey: ['deviceMetrics', device.pk, metricsParams],
    queryFn: () => fetchDeviceMetrics(device.pk, metricsParams),
    enabled: !hideCharts,
  })

  const cardClass = "rounded-lg border border-border p-4"
  const stats = [
    { label: 'Type', value: device.deviceType },
    {
      label: 'Contributor',
      value: device.contributorPk ? (
        <Link to={`/dz/contributors/${device.contributorPk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
          {device.contributorCode}
        </Link>
      ) : (
        device.contributorCode || '—'
      ),
    },
    {
      label: 'Metro',
      value: device.metroPk ? (
        <Link to={`/dz/metros/${device.metroPk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
          {device.metroName}
        </Link>
      ) : (
        device.metroName || '—'
      ),
    },
    { label: 'Users', value: String(device.userCount) },
    { label: 'Validators', value: String(device.validatorCount) },
    { label: 'Stake', value: formatStake(device.stakeSol) },
    { label: 'Stake Share', value: formatStakeShare(device.stakeShare) },
  ]

  // Sort interfaces: activated first, then by name
  const sortedInterfaces = [...(device.interfaces || [])].sort((a, b) => {
    if (a.status === 'activated' && b.status !== 'activated') return -1
    if (a.status !== 'activated' && b.status === 'activated') return 1
    return a.name.localeCompare(b.name)
  })

  // Compact mode: optimized for sidebar panels
  if (compact) {
    return (
      <div className="space-y-4">
        {/* Stats grid - 2 columns for sidebar */}
        <div className="grid grid-cols-2 gap-2">
          {stats.map((stat, i) => (
            <div key={i} className="text-center p-2 bg-muted/30 rounded-lg">
              <div className="text-base font-medium tabular-nums tracking-tight">
                {stat.value}
              </div>
              <div className="text-xs text-muted-foreground">{stat.label}</div>
            </div>
          ))}
        </div>

        {/* Interfaces */}
        {sortedInterfaces.length > 0 && (
          <div>
            <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
              Physical Interfaces ({sortedInterfaces.length})
            </div>
            <div className="space-y-1 max-h-48 overflow-y-auto">
              {sortedInterfaces.map((iface, i) => (
                <div
                  key={i}
                  className="flex items-center justify-between p-2 bg-muted/30 rounded text-xs font-mono"
                >
                  <span className="truncate flex-1 mr-2" title={iface.name}>
                    {iface.name}
                  </span>
                  <span className="text-muted-foreground whitespace-nowrap">
                    {iface.ip || '—'}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Time range selector */}
        {!hideCharts && (
          <div className="flex items-center justify-end gap-2">
            <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
          </div>
        )}

        {/* Charts from unified metrics endpoint */}
        {!hideCharts && metrics && (
          <div className="space-y-4">
            {!hideStatusRow && <DeviceHealthTimeline data={metrics} onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />}
            <DeviceInterfaceIssuesChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <DeviceTrafficChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
          </div>
        )}
      </div>
    )
  }

  // Wide mode: optimized for full-page view on desktop
  return (
    <div className="space-y-6">
      {/* Stats grid - responsive columns */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-2">
        {stats.map((stat, i) => (
          <div key={i} className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums tracking-tight">
              {stat.value}
            </div>
            <div className="text-xs text-muted-foreground">{stat.label}</div>
          </div>
        ))}
      </div>

      {/* Interfaces - horizontal row below stats */}
      {sortedInterfaces.length > 0 && (
        <div>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Physical Interfaces ({sortedInterfaces.length})
          </div>
          <div className="flex flex-wrap gap-2">
            {sortedInterfaces.map((iface, i) => (
              <div
                key={i}
                className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-muted/30 rounded text-xs font-mono"
                title={`${iface.name} — ${iface.ip || 'no IP'}`}
              >
                <span>{iface.name}</span>
                {iface.ip && (
                  <span className="text-muted-foreground">{iface.ip}</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Time range selector (only shown when not controlled by parent and charts visible) */}
      {!hideCharts && !controlledTimeRange && (
        <div className="flex items-center justify-end gap-2">
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
        </div>
      )}

      {!hideCharts && metrics && (
        <div className="space-y-4">
          {!hideStatusRow && <DeviceHealthTimeline data={metrics} onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />}
          <DeviceInterfaceIssuesChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
          <DeviceTrafficChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
        </div>
      )}
    </div>
  )
}

