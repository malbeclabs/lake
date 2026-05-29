import React, { useState, useMemo } from 'react'
import { Info } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import type { DeviceInterface } from '@/lib/api'
import { fetchDeviceControllerCalls, fetchDeviceMetrics } from '@/lib/api'
import { DeviceHealthTimeline } from '@/components/device-charts/DeviceHealthTimeline'
import { DeviceTrafficChart } from '@/components/device-charts/DeviceTrafficChart'
import { DeviceInterfaceIssuesChart } from '@/components/device-charts/DeviceInterfaceIssuesChart'
import { DeviceControllerCallsChart } from '@/components/device-charts/DeviceControllerCallsChart'
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
  maxUsers: number
  unicastUsersCount: number
  multicastSubscribersCount: number
  multicastPublishersCount: number
  maxUnicastUsers: number
  maxMulticastSubscribers: number
  maxMulticastPublishers: number
  validatorCount: number | null
  stakeSol: number | null
  stakeShare: number | null
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

function formatBandwidth(bps: number): string {
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(0)}G`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(0)}M`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(0)}K`
  return `${bps}`
}

function InterfaceTypeBadges({ iface }: { iface: DeviceInterface }) {
  const badges: { label: string; className: string }[] = []
  if (iface.interface_type === 'loopback') {
    badges.push({
      label: 'loopback',
      className: 'bg-purple-500/15 text-purple-400',
    })
    if (iface.loopback_type && iface.loopback_type !== 'none') {
      badges.push({
        label: iface.loopback_type,
        className: 'bg-purple-500/10 text-purple-400/80',
      })
    }
  }
  if (iface.cyoa_type && iface.cyoa_type !== 'none') {
    badges.push({
      label: iface.cyoa_type.replace(/_/g, ' '),
      className: 'bg-amber-500/15 text-amber-400',
    })
  }
  if (iface.dia_type && iface.dia_type !== 'none') {
    badges.push({
      label: 'DIA',
      className: 'bg-orange-500/15 text-orange-400',
    })
  }
  if (iface.routing_mode && iface.routing_mode !== 'static') {
    badges.push({
      label: iface.routing_mode.toUpperCase(),
      className: 'bg-blue-500/15 text-blue-400',
    })
  }
  if (iface.bandwidth && iface.bandwidth > 0) {
    badges.push({
      label: formatBandwidth(iface.bandwidth),
      className: 'bg-green-500/15 text-green-400',
    })
  }
  if (badges.length === 0) return null
  return (
    <span className="inline-flex gap-1">
      {badges.map((b, i) => (
        <span key={i} className={`px-1 py-0.5 rounded text-[10px] leading-none ${b.className}`}>
          {b.label}
        </span>
      ))}
    </span>
  )
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

function InlineSkeleton() {
  return <span className="inline-block align-middle h-4 w-12 rounded bg-muted animate-pulse" />
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
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{
    start: number
    end: number
  } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  const [internalTimeRange, setInternalTimeRange] = useState<TimeRange>({
    preset: '24h',
  })

  const timeRange = controlledTimeRange ?? internalTimeRange
  const setTimeRange = onTimeRangeChange ?? setInternalTimeRange

  const metricsParams = useMemo(() => toDeviceMetricsParams(timeRange), [timeRange])

  const { data: metrics, isFetching: metricsFetching } = useQuery({
    queryKey: ['deviceMetrics', device.pk, metricsParams],
    queryFn: () => fetchDeviceMetrics(device.pk, metricsParams),
    enabled: !hideCharts,
  })

  const { data: controllerCalls, isFetching: controllerCallsFetching } = useQuery({
    queryKey: ['deviceControllerCalls', device.pk, metricsParams],
    queryFn: () => fetchDeviceControllerCalls(device.pk, metricsParams),
    enabled: !hideCharts,
  })

  const cardClass = "rounded-lg border border-border p-4"
  const isTransit = device.deviceType === 'transit'
  const usersPct = device.maxUsers > 0 ? Math.min(100, (device.userCount / device.maxUsers) * 100) : null
  const stats: { label: string; value: React.ReactNode; bar?: number | null }[] = [
    { label: 'Type', value: device.deviceType },
    {
      label: 'Contributor',
      value: device.contributorPk ? (
        <Link
          to={`/dz/contributors/${device.contributorPk}`}
          className="text-blue-600 dark:text-blue-400 hover:underline"
        >
          {device.contributorCode}
        </Link>
      ) : (
        device.contributorCode || '—'
      ),
    },
    {
      label: 'Metro',
      value: device.metroPk ? (
        <Link
          to={`/dz/metros/${device.metroPk}`}
          className="text-blue-600 dark:text-blue-400 hover:underline"
        >
          {device.metroName}
        </Link>
      ) : (
        device.metroName || '—'
      ),
    },
    ...(!isTransit ? [{ label: 'Users', bar: usersPct, value: (
      <span className="tabular-nums">
        {device.userCount}
        {device.maxUsers > 0
          ? <span className="text-muted-foreground"> / {device.maxUsers}</span>
          : <span className="text-muted-foreground"> / 0</span>
        }
      </span>
    ) }] : []),
    { label: 'Validators', value: device.validatorCount === null ? <InlineSkeleton /> : String(device.validatorCount) },
    { label: 'Stake', value: device.stakeSol === null ? <InlineSkeleton /> : formatStake(device.stakeSol) },
    { label: 'Stake Share', value: device.stakeShare === null ? <InlineSkeleton /> : formatStakeShare(device.stakeShare) },
  ]

  const deviceAvailable = device.maxUsers > 0 ? Math.max(0, device.maxUsers - device.userCount) : 0
  const derivedTooltip = device.maxUsers > 0
    ? `max_users (${device.maxUsers}) − connected (${device.userCount}) = ${deviceAvailable}`
    : ''
  const effUnicast = device.maxUnicastUsers > 0 ? device.maxUnicastUsers : device.unicastUsersCount + deviceAvailable
  const effSubs = device.maxMulticastSubscribers > 0 ? device.maxMulticastSubscribers : device.multicastSubscribersCount + deviceAvailable
  const effPubs = device.maxMulticastPublishers > 0 ? device.maxMulticastPublishers : device.multicastPublishersCount + deviceAvailable
  const userCapacityCards = [
    { count: device.unicastUsersCount, rawMax: device.maxUnicastUsers, effectiveMax: effUnicast, label: 'Unicast Users' },
    { count: device.multicastSubscribersCount, rawMax: device.maxMulticastSubscribers, effectiveMax: effSubs, label: 'Subscribers' },
    { count: device.multicastPublishersCount, rawMax: device.maxMulticastPublishers, effectiveMax: effPubs, label: 'Publishers' },
  ].map(({ count, rawMax, effectiveMax, label }) => {
    const isDerived = rawMax === 0
    const displayMax = Math.max(count, effectiveMax)
    const available = displayMax > count ? displayMax - count : 0
    const pct = displayMax > 0 ? Math.min(100, (count / displayMax) * 100) : null
    const fillColor = pct !== null ? (pct >= 90 ? 'bg-red-500/50' : pct >= 70 ? 'bg-amber-500/40' : 'bg-blue-500/30') : ''
    return { count, available, max: displayMax, isDerived, label, pct, fillColor, derivedTooltip: isDerived ? derivedTooltip : '' }
  })

  // Sort interfaces: activated first, then by type (physical, loopback), then by name
  const sortedInterfaces = [...(device.interfaces || [])].sort((a, b) => {
    if (a.status === 'activated' && b.status !== 'activated') return -1
    if (a.status !== 'activated' && b.status === 'activated') return 1
    // Physical before loopback
    const typeOrder = (t?: string) => (t === 'physical' ? 0 : t === 'loopback' ? 1 : 2)
    const typeA = typeOrder(a.interface_type)
    const typeB = typeOrder(b.interface_type)
    if (typeA !== typeB) return typeA - typeB
    return a.name.localeCompare(b.name)
  })

  // Compact mode: optimized for sidebar panels
  if (compact) {
    return (
      <div className="space-y-4">
        {/* Stats grid - 2 columns for sidebar */}
        <div className="grid grid-cols-2 gap-2">
          {stats.map((stat, i) => {
            const pct = stat.bar ?? null
            const fillColor = pct !== null ? (pct >= 90 ? 'bg-red-500/50' : pct >= 70 ? 'bg-amber-500/40' : 'bg-blue-500/30') : ''
            return (
              <div key={i} className="overflow-hidden rounded-lg bg-muted/30">
                <div className="p-2 text-center">
                  <div className="text-base font-medium tabular-nums tracking-tight">{stat.value}</div>
                  <div className="text-xs text-muted-foreground">{stat.label}</div>
                </div>
                {pct !== null && (
                  <div className="relative h-1 bg-muted/60">
                    <div className={`absolute inset-y-0 left-0 ${fillColor}`} style={{ width: `${pct}%` }} />
                  </div>
                )}
              </div>
            )
          })}
        </div>

        {/* Unicast / Multicast cards */}
        {!isTransit && (
          <div className="grid grid-cols-3 gap-2">
            {userCapacityCards.map(({ count, max, isDerived, label, pct, fillColor, derivedTooltip: tooltip }) => (
              <div key={label} className="overflow-hidden rounded-lg bg-muted/30">
                <div className="p-2 text-center">
                  <div className="text-sm font-medium tabular-nums">
                    {count}
                    {max > 0 ? (
                      isDerived
                        ? <span className="text-muted-foreground/50 inline-flex items-center gap-0.5" title={tooltip}>/{max}<Info className="h-2.5 w-2.5" /></span>
                        : <span className="text-muted-foreground">/{max}</span>
                    ) : <span className="text-muted-foreground">/0</span>}
                  </div>
                  <div className="text-xs text-muted-foreground">{label}</div>
                </div>
                {pct !== null && (
                  <div className="relative h-1 bg-muted/60">
                    <div className={`absolute inset-y-0 left-0 ${fillColor}`} style={{ width: `${pct}%` }} />
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        {/* Interfaces */}
        {sortedInterfaces.length > 0 && (
          <div>
            <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
              Interfaces ({sortedInterfaces.length})
            </div>
            <div className="space-y-1 max-h-48 overflow-y-auto">
              {sortedInterfaces.map((iface, i) => (
                <div
                  key={i}
                  className="flex items-center justify-between p-2 bg-muted/30 rounded text-xs font-mono"
                >
                  <span className="truncate flex-1 mr-2" title={iface.name}>
                    {iface.name}
                    <InterfaceTypeBadges iface={iface} />
                  </span>
                  <span className="text-muted-foreground whitespace-nowrap">{iface.ip || '—'}</span>
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
            <DeviceControllerCallsChart
              data={controllerCalls}
              loading={controllerCallsFetching}
              className={cardClass}
              compact={compact}
              onBarHover={setHoveredTimeRange}
              highlightedTime={chartHoveredTime}
            />
            {!hideStatusRow && (
              <DeviceHealthTimeline
                data={metrics}
                onBarHover={setHoveredTimeRange}
                highlightedTime={chartHoveredTime}
              />
            )}
            <DeviceInterfaceIssuesChart
              data={metrics}
              loading={metricsFetching}
              className={cardClass}
              highlightTimeRange={hoveredTimeRange}
              onCursorTime={setChartHoveredTime}
            />
            <DeviceTrafficChart
              data={metrics}
              loading={metricsFetching}
              className={cardClass}
              highlightTimeRange={hoveredTimeRange}
              onCursorTime={setChartHoveredTime}
            />
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
        {stats.map((stat, i) => {
          const pct = stat.bar ?? null
          const fillColor = pct !== null ? (pct >= 90 ? 'bg-red-500/50' : pct >= 70 ? 'bg-amber-500/40' : 'bg-blue-500/30') : ''
          return (
            <div key={i} className="overflow-hidden rounded-lg bg-muted/30">
              <div className="p-3 text-center">
                <div className="text-base font-medium tabular-nums tracking-tight">{stat.value}</div>
                <div className="text-xs text-muted-foreground">{stat.label}</div>
              </div>
              {pct !== null && (
                <div className="relative h-1 bg-muted/60">
                  <div className={`absolute inset-y-0 left-0 ${fillColor}`} style={{ width: `${pct}%` }} />
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* Unicast / Multicast cards — Used + Available as one split card per type */}
      {!isTransit && (
        <div className="grid grid-cols-3 gap-2">
          {userCapacityCards.map(({ count, available, max, isDerived, label, pct, fillColor, derivedTooltip: tooltip }) => (
            <div key={label} className="overflow-hidden rounded-lg border border-border">
              <div className="grid grid-cols-2 divide-x divide-border">
                <div className="p-2.5 text-center bg-muted/30">
                  <div className="text-sm font-medium tabular-nums">{count}</div>
                  <div className="text-xs text-muted-foreground">{label}</div>
                </div>
                <div className="p-2.5 text-center bg-muted/10">
                  <div className="text-sm font-medium tabular-nums text-muted-foreground">
                    {max > 0 ? available : 0}
                  </div>
                  <div className="text-xs text-muted-foreground/60 inline-flex items-center gap-0.5">
                    Available
                    {isDerived && max > 0 && <span title={tooltip}><Info className="h-2.5 w-2.5" /></span>}
                  </div>
                </div>
              </div>
              {pct !== null && (
                <div className="relative h-1 bg-muted/60">
                  <div className={`absolute inset-y-0 left-0 ${fillColor}`} style={{ width: `${pct}%` }} />
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Interfaces - horizontal row below stats */}
      {sortedInterfaces.length > 0 && (
        <div>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Interfaces ({sortedInterfaces.length})
          </div>
          <div className="flex flex-col gap-1.5">
            {sortedInterfaces.map((iface, i) => (
              <div
                key={i}
                className="flex flex-wrap items-center gap-1.5 px-2.5 py-1.5 bg-muted/30 rounded text-xs font-mono"
                title={`${iface.name} — ${iface.ip || 'no IP'}`}
              >
                <span>{iface.name}</span>
                {iface.ip && <span className="text-muted-foreground">{iface.ip}</span>}
                <InterfaceTypeBadges iface={iface} />
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
          <DeviceControllerCallsChart
            data={controllerCalls}
            loading={controllerCallsFetching}
            className={cardClass}
            onBarHover={setHoveredTimeRange}
            highlightedTime={chartHoveredTime}
          />
          {!hideStatusRow && (
            <DeviceHealthTimeline
              data={metrics}
              onBarHover={setHoveredTimeRange}
              highlightedTime={chartHoveredTime}
            />
          )}
          <DeviceInterfaceIssuesChart
            data={metrics}
            loading={metricsFetching}
            className={cardClass}
            highlightTimeRange={hoveredTimeRange}
            onCursorTime={setChartHoveredTime}
          />
          <DeviceTrafficChart
            data={metrics}
            loading={metricsFetching}
            className={cardClass}
            highlightTimeRange={hoveredTimeRange}
            onCursorTime={setChartHoveredTime}
          />
        </div>
      )}
    </div>
  )
}
