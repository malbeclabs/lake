import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useEffect, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { CheckCircle2, AlertTriangle, History, Info, ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchDeviceHistory, fetchDeviceMetrics } from '@/lib/api'
import type { DeviceHistory, DeviceHourStatus } from '@/lib/api'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'
import { DeviceInterfaceIssuesChart } from '@/components/device-charts/DeviceInterfaceIssuesChart'

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

function DeviceTimelineSkeleton() {
  return (
    <div className="border border-border rounded-lg">
      <div className="px-4 py-2.5 bg-muted/50 border-b border-border flex items-center gap-2 rounded-t-lg">
        <Skeleton className="h-4 w-4 rounded" />
        <Skeleton className="h-5 w-48" />
        <div className="ml-auto">
          <Skeleton className="h-6 w-48 rounded-lg" />
        </div>
      </div>
      <div className="px-4 py-2 border-b border-border bg-muted/30 flex items-center gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-3 w-16" />
        ))}
      </div>
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} className="px-4 py-3 border-b border-border last:border-b-0">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0 w-5" />
            <div className="flex-shrink-0 w-44 space-y-1.5">
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-3 w-20" />
            </div>
            <div className="flex-1 min-w-0">
              <Skeleton className="h-6 w-full rounded-sm" />
              <div className="flex justify-between mt-1">
                <Skeleton className="h-2.5 w-10" />
                <Skeleton className="h-2.5 w-6" />
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}

type TimeRange = '3h' | '6h' | '12h' | '24h' | '3d' | '7d'

interface DeviceStatusTimelinesProps {
  timeRange?: string
  onTimeRangeChange?: (range: TimeRange) => void
  issueFilters?: string[]
  healthFilters?: string[]
  devicesWithIssues?: Map<string, string[]>  // Map of device code -> issue reasons (from filter time range)
  devicesWithHealth?: Map<string, string>    // Map of device code -> health status (from filter time range)
  expandedDevicePk?: string                  // Device PK to auto-expand (from URL param)
}

function DeviceInfoPopover({ device }: { device: DeviceHistory }) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="relative inline-block">
      <button
        className="text-muted-foreground hover:text-foreground transition-colors p-0.5 -m-0.5"
        onMouseEnter={() => setIsOpen(true)}
        onMouseLeave={() => setIsOpen(false)}
        onClick={() => setIsOpen(!isOpen)}
      >
        <Info className="h-3.5 w-3.5" />
      </button>
      {isOpen && (
        <div
          className="absolute left-0 top-full mt-1 z-50 bg-popover border border-border rounded-lg shadow-lg p-3 min-w-[200px]"
          onMouseEnter={() => setIsOpen(true)}
          onMouseLeave={() => setIsOpen(false)}
        >
          <div className="space-y-2 text-xs">
            <div>
              <div className="text-muted-foreground">Metro</div>
              <div className="font-medium">{device.metro || '—'}</div>
            </div>
            <div>
              <div className="text-muted-foreground">Type</div>
              <div className="font-medium capitalize">{device.device_type?.replace(/_/g, ' ')}</div>
            </div>
            {device.max_users > 0 && (
              <div>
                <div className="text-muted-foreground">Max Users</div>
                <div className="font-medium">{device.max_users}</div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// Status colors and labels for timeline
const statusColors: Record<string, string> = {
  healthy: 'bg-green-500',
  degraded: 'bg-amber-500',
  unhealthy: 'bg-red-500',
  no_data: 'bg-transparent border border-gray-200 dark:border-gray-700',
  disabled: 'bg-gray-500 dark:bg-gray-700',
}

const statusLabels: Record<string, string> = {
  healthy: 'Healthy',
  degraded: 'Degraded',
  unhealthy: 'Unhealthy',
  no_data: 'No Data',
  disabled: 'Disabled',
}

function formatDate(isoString: string): string {
  const date = new Date(isoString)
  return date.toLocaleDateString([], { month: 'short', day: 'numeric' })
}

function formatTimeRange(isoString: string, bucketMinutes: number = 60): string {
  const start = new Date(isoString)
  const end = new Date(start.getTime() + bucketMinutes * 60 * 1000)
  const startTime = start.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  const endTime = end.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  if (start.getDate() !== end.getDate()) {
    return `${formatDate(isoString)} ${startTime} — ${formatDate(end.toISOString())} ${endTime}`
  }
  return `${formatDate(isoString)} ${startTime} — ${endTime}`
}

interface DeviceStatusTimelineProps {
  hours: DeviceHourStatus[]
  bucketMinutes?: number
  timeRange?: string
}

function getEffectiveDeviceStatus(hour: DeviceHourStatus): string {
  if (hour.status !== 'healthy' && hour.status !== 'degraded') {
    // If already unhealthy/disabled/no_data, keep it
    if (hour.isis_unreachable) return 'unhealthy'
    return hour.status
  }
  // ISIS unreachable → unhealthy, overload → at least degraded
  if (hour.isis_unreachable) return 'unhealthy'
  if (hour.isis_overload && hour.status === 'healthy') return 'degraded'
  return hour.status
}

function DeviceStatusTimeline({ hours, bucketMinutes = 60, timeRange = '24h' }: DeviceStatusTimelineProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)

  const timeLabels: Record<string, string> = {
    '1h': '1h ago',
    '3h': '3h ago',
    '6h': '6h ago',
    '12h': '12h ago',
    '24h': '24h ago',
    '3d': '3d ago',
    '7d': '7d ago',
  }
  const timeLabel = timeLabels[timeRange] || '24h ago'

  return (
    <div className="relative">
      <div className="flex gap-[2px]">
        {hours.map((hour, index) => {
          const effectiveStatus = getEffectiveDeviceStatus(hour)
          const prevStatus = index > 0 ? getEffectiveDeviceStatus(hours[index - 1]) : undefined
          return (
          <div
            key={hour.hour}
            className="relative flex-1 min-w-0"
            onMouseEnter={() => setHoveredIndex(index)}
            onMouseLeave={() => setHoveredIndex(null)}
          >
            <div className="relative w-full h-6 rounded-sm overflow-hidden cursor-pointer transition-opacity hover:opacity-80">
              <div className={`absolute inset-0 ${
                hour.collecting && effectiveStatus === 'no_data'
                  ? (prevStatus && prevStatus !== 'no_data' ? statusColors[prevStatus] : 'bg-transparent border border-gray-200/40 dark:border-gray-700/40')
                  : statusColors[effectiveStatus]
              }`} />
              {hour.collecting && (effectiveStatus !== 'no_data' || (prevStatus && prevStatus !== 'no_data')) && (
                <div className="absolute inset-0 bg-gradient-to-r from-transparent via-transparent to-background" />
              )}
            </div>

            {/* Tooltip */}
            {hoveredIndex === index && (
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 z-50">
                <div className="bg-popover border border-border rounded-lg shadow-lg p-3 whitespace-nowrap text-sm">
                  <div className="font-medium mb-1">
                    {formatTimeRange(hour.hour, bucketMinutes)}
                  </div>
                  <div className={`text-xs mb-2 ${
                    effectiveStatus === 'healthy' ? 'text-green-600 dark:text-green-400' :
                    effectiveStatus === 'degraded' ? 'text-amber-600 dark:text-amber-400' :
                    effectiveStatus === 'unhealthy' ? 'text-red-600 dark:text-red-400' :
                    'text-muted-foreground'
                  }`}>
                    {statusLabels[effectiveStatus]}
                    {hour.collecting && <span className="text-muted-foreground ml-1">(In progress)</span>}
                  </div>
                  {hour.status !== 'no_data' && (
                    <div className="space-y-1 text-muted-foreground">
                      {(hour.in_errors > 0 || hour.out_errors > 0) && (
                        <div className="flex justify-between gap-4">
                          <span>Errors:</span>
                          <span className="font-mono">
                            {(hour.in_errors + hour.out_errors).toLocaleString()}
                            <span className="text-xs ml-1">
                              (in: {hour.in_errors.toLocaleString()}, out: {hour.out_errors.toLocaleString()})
                            </span>
                          </span>
                        </div>
                      )}
                      {hour.in_fcs_errors > 0 && (
                        <div className="flex justify-between gap-4">
                          <span>FCS Errors:</span>
                          <span className="font-mono">{hour.in_fcs_errors.toLocaleString()}</span>
                        </div>
                      )}
                      {(hour.in_discards > 0 || hour.out_discards > 0) && (
                        <div className="flex justify-between gap-4">
                          <span>Discards:</span>
                          <span className="font-mono">
                            {(hour.in_discards + hour.out_discards).toLocaleString()}
                            <span className="text-xs ml-1">
                              (in: {hour.in_discards.toLocaleString()}, out: {hour.out_discards.toLocaleString()})
                            </span>
                          </span>
                        </div>
                      )}
                      {hour.carrier_transitions > 0 && (
                        <div className="flex justify-between gap-4">
                          <span>Carrier Transitions:</span>
                          <span className="font-mono">{hour.carrier_transitions.toLocaleString()}</span>
                        </div>
                      )}
                      {hour.max_users > 0 && (
                        <div className="flex justify-between gap-4">
                          <span>Utilization:</span>
                          <span className="font-mono">
                            {hour.utilization_pct.toFixed(1)}%
                            <span className="text-xs ml-1">
                              ({hour.current_users}/{hour.max_users})
                            </span>
                          </span>
                        </div>
                      )}
                      {hour.no_probes && (
                        <div className="flex justify-between gap-4">
                          <span className="text-red-500">Not sending latency probes</span>
                        </div>
                      )}
                      {hour.isis_overload && (
                        <div className="flex justify-between gap-4">
                          <span className="text-red-500">ISIS Overload</span>
                        </div>
                      )}
                      {hour.isis_unreachable && (
                        <div className="flex justify-between gap-4">
                          <span className="text-red-500">ISIS Unreachable</span>
                        </div>
                      )}
                      {hour.in_errors === 0 && hour.out_errors === 0 &&
                       hour.in_fcs_errors === 0 &&
                       hour.in_discards === 0 && hour.out_discards === 0 &&
                       hour.carrier_transitions === 0 && !hour.no_probes && hour.max_users === 0 &&
                       !hour.isis_overload && !hour.isis_unreachable && (
                        <div className="text-xs">No issues detected</div>
                      )}
                    </div>
                  )}
                </div>
                {/* Arrow */}
                <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-[1px]">
                  <div className="border-8 border-transparent border-t-border" />
                  <div className="absolute top-0 left-1/2 -translate-x-1/2 border-[7px] border-transparent border-t-popover" />
                </div>
              </div>
            )}
          </div>
          )
        })}
      </div>

      {/* Time labels */}
      <div className="flex justify-between mt-1 text-[10px] text-muted-foreground">
        <span>{timeLabel}</span>
        <span>Now</span>
      </div>
    </div>
  )
}

function useBucketCount() {
  const [buckets, setBuckets] = useState(72)

  useEffect(() => {
    const updateBuckets = () => {
      const width = window.innerWidth
      if (width < 640) {
        setBuckets(24) // mobile
      } else if (width < 1024) {
        setBuckets(48) // tablet
      } else {
        setBuckets(72) // desktop
      }
    }

    updateBuckets()
    window.addEventListener('resize', updateBuckets)
    return () => window.removeEventListener('resize', updateBuckets)
  }, [])

  return buckets
}

const cardClass = "rounded-lg border border-border p-4"

// Device row component with expand/collapse
interface DeviceRowProps {
  device: DeviceHistory
  devicesWithIssues?: Map<string, string[]>
  bucketMinutes?: number
  dataTimeRange?: string
  metricsTimeRange: string
  initiallyExpanded?: boolean
}

function DeviceRow({ device, devicesWithIssues, bucketMinutes, dataTimeRange, metricsTimeRange, initiallyExpanded = false }: DeviceRowProps) {
  const [expanded, setExpanded] = useState(initiallyExpanded)

  // Expand when initiallyExpanded prop changes to true
  useEffect(() => {
    if (initiallyExpanded) {
      setExpanded(true)
    }
  }, [initiallyExpanded])

  const { data: metrics, isFetching: metricsFetching } = useQuery({
    queryKey: ['deviceMetrics', device.pk, { range: metricsTimeRange }],
    queryFn: () => fetchDeviceMetrics(device.pk, { range: metricsTimeRange }),
    enabled: expanded,
  })

  const issueReasons = devicesWithIssues && devicesWithIssues.size > 0
    ? (devicesWithIssues.get(device.code) ?? [])
    : (device.issue_reasons ?? [])

  return (
    <div id={`device-row-${device.pk}`} className="border-b border-border last:border-b-0">
      <div
        className="px-4 py-3 transition-colors cursor-pointer hover:bg-muted/30"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-start gap-4">
          {/* Expand/collapse indicator */}
          <div className="flex-shrink-0 w-5 pt-0.5">
            {expanded ? (
              <ChevronUp className="h-4 w-4 text-muted-foreground" />
            ) : (
              <ChevronDown className="h-4 w-4 text-muted-foreground" />
            )}
          </div>

          {/* Device info */}
          <div className="flex-shrink-0 w-44">
            <div className="flex items-center gap-1.5">
              <Link
                to={`/dz/devices/${device.pk}`}
                className="font-mono text-sm truncate hover:underline"
                title={device.code}
                onClick={(e) => e.stopPropagation()}
              >
                {device.code}
              </Link>
              <DeviceInfoPopover device={device} />
            </div>
            <div className="text-xs text-muted-foreground">
              {device.contributor}{device.metro && ` · ${device.metro}`}
            </div>
            {issueReasons.length > 0 && (
              <div className="flex flex-wrap gap-1 mt-1">
                {issueReasons.includes('interface_errors') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-fuchsia-500/15 text-fuchsia-600 dark:text-fuchsia-400">
                    Interface Errors
                  </span>
                )}
                {issueReasons.includes('fcs_errors') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(249, 115, 22, 0.15)', color: '#ea580c' }}>
                    FCS Errors
                  </span>
                )}
                {issueReasons.includes('discards') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-rose-500/15 text-rose-600 dark:text-rose-400">
                    Discards
                  </span>
                )}
                {issueReasons.includes('carrier_transitions') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-orange-500/15 text-orange-600 dark:text-orange-400">
                    Carrier Transitions
                  </span>
                )}
                {issueReasons.includes('drained') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-slate-500/15 text-slate-600 dark:text-slate-400">
                    Drained
                  </span>
                )}
                {issueReasons.includes('no_data') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(236, 72, 153, 0.15)', color: '#db2777' }}>No Data</span>
                )}
                {issueReasons.includes('isis_overload') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-red-600/15 text-red-700 dark:text-red-400">ISIS Overload</span>
                )}
                {issueReasons.includes('isis_unreachable') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-red-800/15 text-red-800 dark:text-red-400">ISIS Unreachable</span>
                )}
              </div>
            )}
          </div>

          {/* Timeline */}
          <div className="flex-1 min-w-0">
            <DeviceStatusTimeline
              hours={device.hours}
              bucketMinutes={bucketMinutes}
              timeRange={dataTimeRange}
            />
          </div>
        </div>
      </div>

      {/* Expanded charts — aligned with the timeline column */}
      {expanded && (
        <div className="px-4 pb-4 pt-0">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0 w-5" />
            <div className="flex-shrink-0 w-44" />
            <div className="flex-1 min-w-0 space-y-4">
              {metrics && (() => {
                const hasIssues = metrics.buckets.some(b => b.traffic && (
                  b.traffic.in_errors + b.traffic.out_errors > 0 ||
                  b.traffic.in_fcs_errors > 0 ||
                  b.traffic.in_discards + b.traffic.out_discards > 0 ||
                  b.traffic.carrier_transitions > 0
                ))
                if (!hasIssues) return null
                return <DeviceInterfaceIssuesChart data={metrics} loading={metricsFetching} className={cardClass} />
              })()}
              {!metrics && metricsFetching && (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export function DeviceStatusTimelines({
  timeRange = '24h',
  onTimeRangeChange,
  issueFilters = ['interface_errors', 'fcs_errors', 'discards', 'carrier_transitions', 'drained', 'isis_overload', 'isis_unreachable'],
  healthFilters = ['healthy', 'degraded', 'unhealthy', 'disabled'],
  devicesWithIssues,
  devicesWithHealth,
  expandedDevicePk,
}: DeviceStatusTimelinesProps) {
  const timeRangeOptions: { value: TimeRange; label: string }[] = [
    { value: '3h', label: '3h' },
    { value: '6h', label: '6h' },
    { value: '12h', label: '12h' },
    { value: '24h', label: '24h' },
    { value: '3d', label: '3d' },
    { value: '7d', label: '7d' },
  ]
  const buckets = useBucketCount()

  const { data, isLoading, isPlaceholderData, error } = useQuery({
    queryKey: ['device-history', timeRange, buckets],
    queryFn: () => fetchDeviceHistory(timeRange, buckets),
    refetchInterval: 60_000, // Refresh every minute
    staleTime: 30_000,
    placeholderData: keepPreviousData,
  })

  // Helper to check if a device matches health filters
  const deviceMatchesHealthFilters = (device: DeviceHistory): boolean => {
    if (devicesWithHealth && devicesWithHealth.size > 0) {
      const health = devicesWithHealth.get(device.code)
      if (health) {
        const filterHealth = health === 'no_data' ? 'unhealthy' : health
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return healthFilters.includes(filterHealth as any)
      }
      return false
    }

    // Fallback: check device's own hours data
    if (!device.hours || device.hours.length === 0) return false
    return device.hours.some(hour => {
      const status = hour.status
      if (status === 'healthy' && healthFilters.includes('healthy')) return true
      if (status === 'degraded' && healthFilters.includes('degraded')) return true
      if (status === 'unhealthy' && healthFilters.includes('unhealthy')) return true
      if (status === 'disabled' && healthFilters.includes('disabled')) return true
      if (status === 'no_data' && healthFilters.includes('unhealthy')) return true
      return false
    })
  }

  // Check which issue filters are selected
  const issueTypesSelected = issueFilters.filter(f => f !== 'no_issues')
  const noIssuesSelected = issueFilters.includes('no_issues')

  // Filter and sort devices by recency of issues
  const filteredDevices = useMemo(() => {
    if (!data?.devices) return []

    const filtered = data.devices.filter(device => {
      const issueReasons = devicesWithIssues && devicesWithIssues.size > 0
        ? (devicesWithIssues.get(device.code) ?? [])
        : (device.issue_reasons ?? [])
      const hasIssues = issueReasons.length > 0

      // Devices with only no_data or no_probes are shown based on health filter (no separate issue toggle)
      const hasOnlyNoData = issueReasons.length > 0 && issueReasons.every(r => r === 'no_data' || r === 'no_probes')
      const matchesIssue = hasOnlyNoData
        ? true
        : hasIssues
          ? issueReasons.some(reason => issueTypesSelected.includes(reason))
          : noIssuesSelected

      const matchesHealth = deviceMatchesHealthFilters(device)

      return matchesIssue && matchesHealth
    })

    // Sort by: 1) most recent issue, 2) severity of that issue, 3) total issue count, 4) alphabetical
    const statusSeverity = (status: string): number => {
      switch (status) {
        case 'unhealthy': return 4
        case 'degraded': return 3
        case 'disabled': return 2
        case 'no_data': return 1
        default: return 0
      }
    }

    return filtered.sort((a, b) => {
      const getLatestIssue = (device: DeviceHistory): { index: number; severity: number } => {
        if (!device.hours) return { index: -1, severity: 0 }
        for (let i = device.hours.length - 1; i >= 0; i--) {
          const sev = statusSeverity(device.hours[i].status)
          if (sev > 0) return { index: i, severity: sev }
        }
        return { index: -1, severity: 0 }
      }

      const issueCount = (device: DeviceHistory): number => {
        if (!device.hours) return 0
        return device.hours.filter(h => statusSeverity(h.status) > 0).length
      }

      const aIssue = getLatestIssue(a)
      const bIssue = getLatestIssue(b)

      // Most recent issue first
      if (aIssue.index !== bIssue.index) return bIssue.index - aIssue.index
      // Higher severity first
      if (aIssue.severity !== bIssue.severity) return bIssue.severity - aIssue.severity
      // More total issues first
      const aCount = issueCount(a)
      const bCount = issueCount(b)
      if (aCount !== bCount) return bCount - aCount
      // Alphabetical fallback
      return a.code.localeCompare(b.code)
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.devices, issueFilters, healthFilters, noIssuesSelected, issueTypesSelected, devicesWithIssues, devicesWithHealth])

  const showSkeleton = useDelayedLoading(isLoading && !data)

  if (isLoading && !data) {
    return showSkeleton ? <DeviceTimelineSkeleton /> : null
  }

  if (error) {
    return (
      <div className="border border-border rounded-lg p-6 text-center">
        <AlertTriangle className="h-8 w-8 text-amber-500 mx-auto mb-2" />
        <div className="text-sm text-muted-foreground">Unable to load device history</div>
      </div>
    )
  }

  if (filteredDevices.length === 0) {
    return (
      <div className="border border-border rounded-lg p-6 text-center">
        <CheckCircle2 className="h-8 w-8 text-green-500 mx-auto mb-2" />
        <div className="text-sm text-muted-foreground">
          {data?.devices.length === 0
            ? 'No devices available in the selected time range'
            : 'No devices match the selected filters'}
        </div>
      </div>
    )
  }

  return (
    <div id="device-status-history" className={`border border-border rounded-lg transition-opacity${isPlaceholderData ? ' opacity-60' : ''}`}>
      <div className="px-4 py-2.5 bg-muted/50 border-b border-border flex items-center gap-2 rounded-t-lg">
        {isPlaceholderData
          ? <Loader2 className="h-4 w-4 text-muted-foreground animate-spin" />
          : <History className="h-4 w-4 text-muted-foreground" />
        }
        <h3 className="font-medium">
          Device Status History
          <span className="text-sm text-muted-foreground font-normal ml-1">
            ({filteredDevices.length} device{filteredDevices.length !== 1 ? 's' : ''})
          </span>
        </h3>
        {onTimeRangeChange && (
          <div className="inline-flex rounded-lg border border-border bg-background/50 p-0.5 ml-auto">
            {timeRangeOptions.map((opt) => (
              <button
                key={opt.value}
                onClick={() => onTimeRangeChange(opt.value)}
                className={`px-2.5 py-0.5 text-xs rounded-md transition-colors ${
                  timeRange === opt.value
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="px-4 py-2 border-b border-border bg-muted/30 flex items-center gap-4 text-xs text-muted-foreground">
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-green-500" />
          <span>Healthy</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-amber-500" />
          <span>Degraded</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-red-500" />
          <span>Unhealthy</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-transparent border border-gray-200 dark:border-gray-700" />
          <span>No Data</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-gray-500 dark:bg-gray-700" />
          <span>Disabled</span>
        </div>
      </div>

      <div>
        {filteredDevices.map((device) => (
          <DeviceRow
            key={device.code}
            device={device}
            devicesWithIssues={devicesWithIssues}
            bucketMinutes={data?.bucket_minutes}
            dataTimeRange={data?.time_range}
            metricsTimeRange={timeRange}
            initiallyExpanded={device.pk === expandedDevicePk}
          />
        ))}
      </div>
    </div>
  )
}
