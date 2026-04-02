import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useEffect, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { CheckCircle2, AlertTriangle, History, Info, ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchBulkDeviceMetrics } from '@/lib/api'
import type { DeviceMetricsResponse } from '@/lib/api'
import { DeviceHealthTimeline } from '@/components/device-charts/DeviceHealthTimeline'
import { DeviceInterfaceIssuesChart } from '@/components/device-charts/DeviceInterfaceIssuesChart'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'

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

interface DerivedDeviceInfo {
  pk: string
  code: string
  deviceType: string
  contributor: string
  metro: string
  maxUsers: number
  issueReasons: string[]
  isDown: boolean
  drainStatus: string
  isisOverload: boolean
  isisUnreachable: boolean
  health: string  // worst health across buckets
}

function deriveDeviceInfo(metrics: DeviceMetricsResponse): DerivedDeviceInfo {
  const issueReasons = new Set<string>()
  let worstHealth = 'healthy'
  let drainStatus = ''
  let isisOverload = false
  let isisUnreachable = false

  const healthPriority: Record<string, number> = {
    unhealthy: 4,
    degraded: 3,
    disabled: 2,
    no_data: 1,
    healthy: 0,
  }

  for (const b of metrics.buckets) {
    if (b.status) {
      const bHealth = b.status.health || 'no_data'
      if ((healthPriority[bHealth] ?? 0) > (healthPriority[worstHealth] ?? 0)) {
        worstHealth = bHealth
      }
      if (b.status.drain_status) {
        drainStatus = b.status.drain_status
        issueReasons.add('drained')
      }
      if (b.status.isis_overload) {
        isisOverload = true
        issueReasons.add('isis_overload')
      }
      if (b.status.isis_unreachable) {
        isisUnreachable = true
        issueReasons.add('isis_unreachable')
      }
      if (b.status.no_probes) {
        issueReasons.add('no_probes')
      }
      if (!b.status.collecting && bHealth === 'no_data') {
        issueReasons.add('no_data')
      }
    }

    if (b.traffic) {
      const t = b.traffic
      if (t.in_errors + t.out_errors > 0) issueReasons.add('interface_errors')
      if (t.in_fcs_errors > 0) issueReasons.add('fcs_errors')
      if (t.in_discards + t.out_discards > 0) issueReasons.add('discards')
      if (t.carrier_transitions > 0) issueReasons.add('carrier_transitions')
    }
  }

  // Check if device is down: look at latest non-collecting bucket
  let isDown = false
  for (let i = metrics.buckets.length - 1; i >= 0; i--) {
    const b = metrics.buckets[i]
    if (b.status && !b.status.collecting) {
      if (b.status.health === 'unhealthy' && b.status.no_probes) {
        isDown = true
      }
      break
    }
  }

  return {
    pk: metrics.device_pk,
    code: metrics.device_code,
    deviceType: metrics.device_type,
    contributor: metrics.contributor_code,
    metro: metrics.metro,
    maxUsers: metrics.max_users ?? 0,
    issueReasons: Array.from(issueReasons),
    isDown,
    drainStatus,
    isisOverload,
    isisUnreachable,
    health: worstHealth,
  }
}

function DeviceInfoPopover({ deviceMetrics }: { deviceMetrics: DeviceMetricsResponse }) {
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
              <div className="font-medium">{deviceMetrics.metro || '\u2014'}</div>
            </div>
            <div>
              <div className="text-muted-foreground">Type</div>
              <div className="font-medium capitalize">{deviceMetrics.device_type?.replace(/_/g, ' ')}</div>
            </div>
            {(deviceMetrics.max_users ?? 0) > 0 && (
              <div>
                <div className="text-muted-foreground">Max Users</div>
                <div className="font-medium">{deviceMetrics.max_users}</div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const cardClass = "rounded-lg border border-border p-4"

interface DeviceRowProps {
  deviceMetrics: DeviceMetricsResponse
  derivedInfo: DerivedDeviceInfo
  devicesWithIssues?: Map<string, string[]>
  initiallyExpanded?: boolean
}

function DeviceRow({ deviceMetrics, derivedInfo, devicesWithIssues, initiallyExpanded = false }: DeviceRowProps) {
  const [expanded, setExpanded] = useState(initiallyExpanded)
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{ start: number; end: number } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  // Expand when initiallyExpanded prop changes to true
  useEffect(() => {
    if (initiallyExpanded) {
      setExpanded(true)
    }
  }, [initiallyExpanded])

  const issueReasons = devicesWithIssues && devicesWithIssues.size > 0
    ? (devicesWithIssues.get(derivedInfo.code) ?? [])
    : derivedInfo.issueReasons

  const nowMinutes = Math.floor(Date.now() / 60000)
  const recentIssues = useMemo(() => {
    const recent = new Set<string>()
    const cutoff = nowMinutes * 60 - 30 * 60
    for (const b of deviceMetrics.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < cutoff) continue
      if (b.traffic) {
        const t = b.traffic
        if (t.in_errors + t.out_errors > 0) recent.add('interface_errors')
        if (t.in_fcs_errors > 0) recent.add('fcs_errors')
        if (t.in_discards + t.out_discards > 0) recent.add('discards')
        if (t.carrier_transitions > 0) recent.add('carrier_transitions')
      }
      if (b.status?.drain_status) recent.add('drained')
      if (b.status && b.status.health === 'no_data') recent.add('no_data')
      if (b.status?.isis_overload) recent.add('isis_overload')
      if (b.status?.isis_unreachable) recent.add('isis_unreachable')
    }
    return recent
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deviceMetrics, nowMinutes])

  const hoveredIssues = useMemo(() => {
    if (!hoveredTimeRange) return null
    const issues = new Set<string>()
    for (const b of deviceMetrics.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < hoveredTimeRange.start || ts >= hoveredTimeRange.end) continue
      if (b.traffic) {
        const t = b.traffic
        if (t.in_errors + t.out_errors > 0) issues.add('interface_errors')
        if (t.in_fcs_errors > 0) issues.add('fcs_errors')
        if (t.in_discards + t.out_discards > 0) issues.add('discards')
        if (t.carrier_transitions > 0) issues.add('carrier_transitions')
      }
      if (b.status?.drain_status) issues.add('drained')
      if (b.status && b.status.health === 'no_data') issues.add('no_data')
      if (b.status?.isis_overload) issues.add('isis_overload')
      if (b.status?.isis_unreachable) issues.add('isis_unreachable')
    }
    return issues
  }, [hoveredTimeRange, deviceMetrics])

  const isBadgeActive = (issue: string) => {
    if (hoveredIssues) return hoveredIssues.has(issue)
    return recentIssues.has(issue)
  }

  const dimBadgeClass = 'bg-muted-foreground/10 text-muted-foreground/50'

  const recentHealth = useMemo(() => {
    const cutoff = nowMinutes * 60 - 30 * 60
    let worstHealth = 'healthy'
    let isisIssue = false
    const priority: Record<string, number> = { unhealthy: 3, degraded: 2, no_data: 1, healthy: 0 }
    for (const b of deviceMetrics.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < cutoff) continue
      if (b.status) {
        if (b.status.isis_overload || b.status.isis_unreachable) isisIssue = true
        const h = b.status.health || 'healthy'
        if ((priority[h] ?? 0) > (priority[worstHealth] ?? 0)) worstHealth = h
      }
    }
    return { health: worstHealth, isisIssue }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deviceMetrics, nowMinutes])

  const leftBorderColor = recentHealth.isisIssue
    ? 'border-l-gray-500'
    : recentHealth.health === 'unhealthy'
      ? 'border-l-red-500'
      : recentHealth.health === 'degraded'
        ? 'border-l-amber-500'
        : 'border-l-transparent'

  const hasExpandableContent = issueReasons.some(r =>
    r === 'interface_errors' || r === 'fcs_errors' || r === 'discards' || r === 'carrier_transitions'
  )

  return (
    <div id={`device-row-${derivedInfo.pk}`} className={`border-b border-border last:border-b-0 border-l-2 ${leftBorderColor}`}>
      <div
        className={`px-4 py-3 transition-colors ${hasExpandableContent ? 'cursor-pointer hover:bg-muted/30' : ''}`}
        onClick={hasExpandableContent ? () => setExpanded(!expanded) : undefined}
      >
        <div className="flex items-start gap-4">
          {/* Expand/collapse indicator */}
          <div className="flex-shrink-0 w-5 pt-0.5">
            {hasExpandableContent ? (
              expanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />
            ) : (
              <div className="w-4" />
            )}
          </div>

          {/* Device info */}
          <div className="flex-shrink-0 w-44">
            <div className="flex items-center gap-1.5">
              <Link
                to={`/dz/devices/${derivedInfo.pk}`}
                className="font-mono text-sm truncate hover:underline"
                title={derivedInfo.code}
                onClick={(e) => e.stopPropagation()}
              >
                {derivedInfo.code}
              </Link>
              <DeviceInfoPopover deviceMetrics={deviceMetrics} />
            </div>
            <div className="text-xs text-muted-foreground">
              {derivedInfo.contributor}{derivedInfo.metro && ` \u00b7 ${derivedInfo.metro}`}
            </div>
            {issueReasons.length > 0 && (
              <div className="flex flex-wrap gap-1 mt-1">
                {issueReasons.includes('interface_errors') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('interface_errors') ? 'bg-fuchsia-500/15 text-fuchsia-600 dark:text-fuchsia-400' : dimBadgeClass}`}>
                    Interface Errors
                  </span>
                )}
                {issueReasons.includes('fcs_errors') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('fcs_errors') ? '' : dimBadgeClass}`} style={isBadgeActive('fcs_errors') ? { backgroundColor: 'rgba(249, 115, 22, 0.15)', color: '#ea580c' } : undefined}>
                    FCS Errors
                  </span>
                )}
                {issueReasons.includes('discards') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('discards') ? 'bg-rose-500/15 text-rose-600 dark:text-rose-400' : dimBadgeClass}`}>
                    Discards
                  </span>
                )}
                {issueReasons.includes('carrier_transitions') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('carrier_transitions') ? 'bg-orange-500/15 text-orange-600 dark:text-orange-400' : dimBadgeClass}`}>
                    Carrier Transitions
                  </span>
                )}
                {issueReasons.includes('drained') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('drained') ? 'bg-slate-500/15 text-slate-600 dark:text-slate-400' : dimBadgeClass}`}>
                    Drained
                  </span>
                )}
                {issueReasons.includes('no_data') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('no_data') ? '' : dimBadgeClass}`} style={isBadgeActive('no_data') ? { backgroundColor: 'rgba(236, 72, 153, 0.15)', color: '#db2777' } : undefined}>No Data</span>
                )}
                {issueReasons.includes('isis_overload') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('isis_overload') ? 'bg-red-600/15 text-red-700 dark:text-red-400' : dimBadgeClass}`}>ISIS Overload</span>
                )}
                {issueReasons.includes('isis_unreachable') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('isis_unreachable') ? 'bg-red-800/15 text-red-800 dark:text-red-400' : dimBadgeClass}`}>ISIS Unreachable</span>
                )}
              </div>
            )}
          </div>

          {/* Timeline */}
          <div className="flex-1 min-w-0">
            <DeviceHealthTimeline data={deviceMetrics} hideBadges onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />
          </div>
        </div>
      </div>

      {/* Expanded charts */}
      {expanded && (
        <div className="px-4 pb-4 pt-2 space-y-4">
          {(() => {
            const hasIssues = deviceMetrics.buckets.some(b => b.traffic && (
              b.traffic.in_errors + b.traffic.out_errors > 0 ||
              b.traffic.in_fcs_errors > 0 ||
              b.traffic.in_discards + b.traffic.out_discards > 0 ||
              b.traffic.carrier_transitions > 0
            ))
            if (!hasIssues) return null
            return <DeviceInterfaceIssuesChart data={deviceMetrics} loading={false} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
          })()}
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

  const { data, isLoading, isPlaceholderData, error } = useQuery({
    queryKey: ['bulk-device-metrics', timeRange],
    queryFn: () => fetchBulkDeviceMetrics({ range: timeRange, include: ['status', 'traffic'], hasIssues: true }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    placeholderData: keepPreviousData,
  })

  // Convert the Record<string, DeviceMetricsResponse> into an array with derived info
  const devicesArray = useMemo(() => {
    if (!data?.devices) return []
    return Object.values(data.devices).map(metrics => ({
      metrics,
      derived: deriveDeviceInfo(metrics),
    }))
  }, [data?.devices])

  // Helper to check if a device matches health filters
  const deviceMatchesHealthFilters = (derived: DerivedDeviceInfo): boolean => {
    if (devicesWithHealth && devicesWithHealth.size > 0) {
      const health = devicesWithHealth.get(derived.code)
      if (health) {
        const filterHealth = health === 'no_data' ? 'unhealthy' : health
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return healthFilters.includes(filterHealth as any)
      }
      return false
    }

    // Fallback: use worst health derived from buckets
    const h = derived.health
    if (h === 'healthy' && healthFilters.includes('healthy')) return true
    if (h === 'degraded' && healthFilters.includes('degraded')) return true
    if ((h === 'unhealthy' || h === 'no_data') && healthFilters.includes('unhealthy')) return true
    if (h === 'disabled' && healthFilters.includes('disabled')) return true
    return false
  }

  // Check which issue filters are selected
  const issueTypesSelected = issueFilters.filter(f => f !== 'no_issues')
  const noIssuesSelected = issueFilters.includes('no_issues')

  // Filter and sort devices by recency of issues
  const filteredDevices = useMemo(() => {
    if (devicesArray.length === 0) return []

    const filtered = devicesArray.filter(({ derived }) => {
      const issueReasons = devicesWithIssues && devicesWithIssues.size > 0
        ? (devicesWithIssues.get(derived.code) ?? [])
        : derived.issueReasons
      const hasIssues = issueReasons.length > 0

      // Devices with only no_data or no_probes are shown based on health filter (no separate issue toggle)
      const hasOnlyNoData = issueReasons.length > 0 && issueReasons.every(r => r === 'no_data' || r === 'no_probes')
      const matchesIssue = hasOnlyNoData
        ? true
        : hasIssues
          ? issueReasons.some(reason => issueTypesSelected.includes(reason))
          : noIssuesSelected

      const matchesHealth = deviceMatchesHealthFilters(derived)

      return matchesIssue && matchesHealth
    })

    // Sort by: 1) recent severity (worst in last 6 buckets), 2) overall worst severity,
    // 3) most recent issue timestamp, 4) total issue count, 5) alphabetical.
    const statusSeverity = (health: string): number => {
      switch (health) {
        case 'unhealthy': return 4
        case 'degraded': return 3
        case 'disabled': return 2
        case 'no_data': return 1
        default: return 0
      }
    }

    const RECENT_BUCKETS = 6

    return filtered.sort((a, b) => {
      const getSortKey = (item: { metrics: DeviceMetricsResponse }): { recent: number; worst: number; latestTs: string; count: number } => {
        const buckets = item.metrics.buckets
        if (!buckets || buckets.length === 0) return { recent: 0, worst: 0, latestTs: '', count: 0 }
        let worst = 0
        let recent = 0
        let latestTs = ''
        let count = 0
        const recentStart = Math.max(0, buckets.length - RECENT_BUCKETS)
        for (let i = 0; i < buckets.length; i++) {
          const bk = buckets[i]
          const health = bk.status?.health ?? 'no_data'
          const sev = statusSeverity(health)
          if (sev > 0) {
            count++
            if (sev > worst) worst = sev
            if (i >= recentStart && sev > recent) recent = sev
            if (bk.ts > latestTs) latestTs = bk.ts
          }
        }
        return { recent, worst, latestTs, count }
      }

      const aInfo = getSortKey(a)
      const bInfo = getSortKey(b)

      if (aInfo.recent !== bInfo.recent) return bInfo.recent - aInfo.recent
      if (aInfo.worst !== bInfo.worst) return bInfo.worst - aInfo.worst
      if (aInfo.latestTs !== bInfo.latestTs) return aInfo.latestTs < bInfo.latestTs ? 1 : -1
      if (aInfo.count !== bInfo.count) return bInfo.count - aInfo.count
      return a.derived.code.localeCompare(b.derived.code)
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [devicesArray, issueFilters, healthFilters, noIssuesSelected, issueTypesSelected, devicesWithIssues, devicesWithHealth])

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
          {devicesArray.length === 0
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
        {filteredDevices.map(({ metrics, derived }) => (
          <DeviceRow
            key={derived.code}
            deviceMetrics={metrics}
            derivedInfo={derived}
            devicesWithIssues={devicesWithIssues}
            initiallyExpanded={derived.pk === expandedDevicePk}
          />
        ))}
      </div>
    </div>
  )
}
