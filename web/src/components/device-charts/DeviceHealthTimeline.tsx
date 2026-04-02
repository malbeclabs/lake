import { useState, useMemo, useCallback, useLayoutEffect } from 'react'
import type { DeviceMetricsResponse, DeviceMetricsBucket } from '@/lib/api'

interface DeviceHealthTimelineProps {
  data: DeviceMetricsResponse
  className?: string
  hideBadges?: boolean
  onBarHover?: (range: { start: number; end: number } | null) => void
  highlightedTime?: number | null  // unix seconds
}

const healthColors: Record<string, string> = {
  healthy: 'bg-green-500',
  degraded: 'bg-amber-500',
  unhealthy: 'bg-red-500',
  disabled: 'bg-gray-400',
  no_data: 'bg-transparent border border-gray-200 dark:border-gray-700',
}

const healthLabels: Record<string, string> = {
  healthy: 'Healthy',
  degraded: 'Degraded',
  unhealthy: 'Unhealthy',
  disabled: 'Disabled',
  no_data: 'No Data',
}

const healthPriority: Record<string, number> = {
  unhealthy: 3,
  degraded: 2,
  disabled: 1,
  no_data: 1,
  healthy: 0,
}

function worstHealth(a: string, b: string): string {
  return (healthPriority[a] ?? 0) >= (healthPriority[b] ?? 0) ? a : b
}

interface MergedBar {
  ts: string
  spanSeconds: number
  health: string
  collecting: boolean
  drainStatus: string
  isisOverload: boolean
  isisUnreachable: boolean
  noProbes: boolean
  // Aggregated metrics for tooltip
  totalErrors: number
  totalFcsErrors: number
  totalDiscards: number
  totalCarrier: number
  hasTraffic: boolean
  missingTraffic: boolean
}

function aggregateBar(group: DeviceMetricsBucket[], bucketSeconds: number): MergedBar {
  let health = 'healthy'
  let collecting = false
  let drainStatus = ''
  let isisOverload = false
  let isisUnreachable = false
  let noProbes = false
  let totalErrors = 0
  let totalFcsErrors = 0
  let totalDiscards = 0
  let totalCarrier = 0
  let hasTraffic = false
  let missingTraffic = false
  let nonCollectingCount = 0

  for (const b of group) {
    health = worstHealth(health, b.status?.health ?? 'no_data')
    if (b.status?.collecting) collecting = true
    if (b.status?.drain_status) drainStatus = b.status.drain_status
    if (b.status?.isis_overload) isisOverload = true
    if (b.status?.isis_unreachable) isisUnreachable = true
    if (b.status?.no_probes) noProbes = true

    if (!b.status?.collecting) {
      nonCollectingCount++
      if (!b.traffic) missingTraffic = true
    }

    if (b.traffic) {
      hasTraffic = true
      totalErrors += b.traffic.in_errors + b.traffic.out_errors
      totalFcsErrors += b.traffic.in_fcs_errors
      totalDiscards += b.traffic.in_discards + b.traffic.out_discards
      totalCarrier += b.traffic.carrier_transitions
    }
  }

  return {
    ts: group[0].ts,
    spanSeconds: group.length * bucketSeconds,
    health,
    collecting,
    drainStatus,
    isisOverload,
    isisUnreachable,
    noProbes,
    totalErrors,
    totalFcsErrors,
    totalDiscards,
    totalCarrier,
    hasTraffic,
    missingTraffic: missingTraffic && nonCollectingCount > 0,
  }
}

function mergeBuckets(buckets: DeviceMetricsBucket[], bucketSeconds: number, maxBars: number): MergedBar[] {
  if (buckets.length <= maxBars) {
    return buckets.map((b) => aggregateBar([b], bucketSeconds))
  }

  const groupSize = Math.ceil(buckets.length / maxBars)
  const bars: MergedBar[] = []
  for (let i = 0; i < buckets.length; i += groupSize) {
    bars.push(aggregateBar(buckets.slice(i, i + groupSize), bucketSeconds))
  }
  return bars
}

function markTrailingCollecting(bars: MergedBar[]): void {
  const now = Date.now()
  for (let i = bars.length - 1; i >= 0; i--) {
    const barEnd = new Date(bars[i].ts).getTime() + bars[i].spanSeconds * 1000
    if (now - barEnd > 10 * 60 * 1000) break
    bars[i].missingTraffic = false
    if (bars[i].health === 'no_data' && !bars[i].hasTraffic) {
      bars[i].collecting = true
    }
  }
}

function getReasons(bar: MergedBar): string[] {
  const reasons: string[] = []

  if (bar.isisOverload) reasons.push('ISIS overload')
  if (bar.isisUnreachable) reasons.push('ISIS unreachable')
  if (bar.noProbes) reasons.push('Not sending latency probes')
  if (bar.missingTraffic) reasons.push('No traffic data')
  if (bar.health === 'no_data') return reasons

  const intfIssues: string[] = []
  if (bar.totalErrors > 0) intfIssues.push(`${bar.totalErrors} interface errors`)
  if (bar.totalFcsErrors > 0) intfIssues.push(`${bar.totalFcsErrors} FCS errors`)
  if (bar.totalDiscards > 0) intfIssues.push(`${bar.totalDiscards} discards`)
  if (bar.totalCarrier > 0) intfIssues.push(`${bar.totalCarrier} carrier transitions`)
  if (intfIssues.length > 0) reasons.push(intfIssues.join(', '))

  return reasons
}

function formatTimeRange(ts: string, spanSeconds: number): string {
  const start = new Date(ts)
  const end = new Date(start.getTime() + spanSeconds * 1000)
  const timeOpts: Intl.DateTimeFormatOptions = { hour: '2-digit', minute: '2-digit', ...(spanSeconds < 60 ? { second: '2-digit' } : {}) }
  const startTime = start.toLocaleTimeString([], timeOpts)
  const endTime = end.toLocaleTimeString([], timeOpts)
  const startDate = start.toLocaleDateString([], { month: 'short', day: 'numeric' })
  if (start.getDate() !== end.getDate()) {
    const endDate = end.toLocaleDateString([], { month: 'short', day: 'numeric' })
    return `${startDate} ${startTime} — ${endDate} ${endTime}`
  }
  return `${startDate} ${startTime} — ${endTime}`
}

const BAR_WIDTH_PX = 8
const MIN_BARS = 24
const MAX_BARS = 192

function useContainerBars() {
  const [maxBars, setMaxBars] = useState(MAX_BARS)
  const [el, setEl] = useState<HTMLDivElement | null>(null)
  const containerRef = useCallback((node: HTMLDivElement | null) => setEl(node), [])

  useLayoutEffect(() => {
    if (!el) return
    const measure = () => {
      const width = el.getBoundingClientRect().width
      const count = Math.floor(width / BAR_WIDTH_PX)
      setMaxBars(Math.max(MIN_BARS, Math.min(MAX_BARS, count)))
    }
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(el)
    return () => observer.disconnect()
  }, [el])

  return { containerRef, maxBars }
}

export function DeviceHealthTimeline({ data, className, hideBadges, onBarHover, highlightedTime }: DeviceHealthTimelineProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)
  const { containerRef, maxBars } = useContainerBars()

  const bars = useMemo(() => {
    const merged = mergeBuckets(data.buckets, data.bucket_seconds, maxBars)
    markTrailingCollecting(merged)
    return merged
  }, [data.buckets, data.bucket_seconds, maxBars])

  const highlightedBarIndex = useMemo(() => {
    if (highlightedTime == null) return -1
    let bestIdx = -1
    let bestDist = Infinity
    for (let i = 0; i < bars.length; i++) {
      const start = new Date(bars[i].ts).getTime() / 1000
      const end = start + bars[i].spanSeconds
      if (highlightedTime >= start && highlightedTime < end) return i
      const mid = start + bars[i].spanSeconds / 2
      const dist = Math.abs(highlightedTime - mid)
      if (dist < bestDist) { bestDist = dist; bestIdx = i }
    }
    return bestDist < bars[0]?.spanSeconds ? bestIdx : -1
  }, [highlightedTime, bars])

  const badges = useMemo(() => {
    const found = new Set<string>()
    for (const b of data.buckets) {
      if (b.traffic) {
        const t = b.traffic
        if (t.in_errors + t.out_errors > 0) found.add('Errors')
        if (t.in_fcs_errors > 0) found.add('FCS')
        if (t.in_discards + t.out_discards > 0) found.add('Discards')
        if (t.carrier_transitions > 0) found.add('Carrier')
      }
      if (b.status?.isis_overload) found.add('ISIS Overload')
      if (b.status?.isis_unreachable) found.add('ISIS Unreachable')
    }
    return Array.from(found)
  }, [data.buckets])

  // Recent badges: active in last 30 minutes
  const nowMinutes = Math.floor(Date.now() / 60000)
  const recentBadges = useMemo(() => {
    const recent = new Set<string>()
    const cutoff = nowMinutes * 60 - 30 * 60
    for (const b of data.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < cutoff) continue
      if (b.traffic) {
        const t = b.traffic
        if (t.in_errors + t.out_errors > 0) recent.add('Errors')
        if (t.in_fcs_errors > 0) recent.add('FCS')
        if (t.in_discards + t.out_discards > 0) recent.add('Discards')
        if (t.carrier_transitions > 0) recent.add('Carrier')
      }
      if (b.status?.isis_overload) recent.add('ISIS Overload')
      if (b.status?.isis_unreachable) recent.add('ISIS Unreachable')
    }
    return recent
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data.buckets, nowMinutes])

  const hoveredBarBadges = useMemo(() => {
    const activeIdx = hoveredIndex ?? highlightedBarIndex
    if (activeIdx == null || activeIdx < 0 || activeIdx >= bars.length) return null
    const bar = bars[activeIdx]
    const active = new Set<string>()
    if (bar.totalErrors > 0) active.add('Errors')
    if (bar.totalFcsErrors > 0) active.add('FCS')
    if (bar.totalDiscards > 0) active.add('Discards')
    if (bar.totalCarrier > 0) active.add('Carrier')
    if (bar.isisOverload) active.add('ISIS Overload')
    if (bar.isisUnreachable) active.add('ISIS Unreachable')
    return active
  }, [hoveredIndex, highlightedBarIndex, bars])

  const isBadgeActive = (badge: string) => {
    if (hoveredBarBadges) return hoveredBarBadges.has(badge)
    return recentBadges.has(badge)
  }

  const labels = useMemo(() => {
    const rangeMap: Record<string, string> = {
      '1h': '1h ago', '6h': '6h ago', '12h': '12h ago',
      '24h': '24h ago', '3d': '3d ago', '7d': '7d ago',
    }
    if (rangeMap[data.time_range]) {
      return { startLabel: rangeMap[data.time_range], endLabel: 'Now' as string }
    }
    const fmt = (ts: string) => {
      const d = new Date(ts)
      return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
    }
    const buckets = data.buckets
    if (buckets.length === 0) return { startLabel: '', endLabel: '' }
    const lastBucket = buckets[buckets.length - 1]
    const endMs = new Date(lastBucket.ts).getTime() + data.bucket_seconds * 1000
    return { startLabel: fmt(buckets[0].ts), endMs }
  }, [data.time_range, data.buckets, data.bucket_seconds])

  const [nowMs] = useState(() => Date.now())
  const startLabel = labels.startLabel
  const endLabel = 'endLabel' in labels
    ? labels.endLabel
    : nowMs - labels.endMs < 5 * 60 * 1000
      ? 'Now'
      : new Date(labels.endMs).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })

  if (bars.length === 0) return null

  return (
    <div ref={containerRef} className={className}>
      <div className="relative">
        <div className="flex gap-[2px]">
          {bars.map((bar, index) => {
            const reasons = getReasons(bar)
            const displayHealth = bar.health === 'healthy' && bar.missingTraffic
              ? 'degraded' : bar.health
            const prevBar = index > 0 ? bars[index - 1] : undefined
            const prevHealth = prevBar
              ? (prevBar.health === 'healthy' && prevBar.missingTraffic ? 'degraded' : prevBar.health)
              : undefined
            return (
              <div
                key={bar.ts}
                className={`relative flex-1 min-w-0 ${highlightedBarIndex === index ? 'rounded-sm z-10 ring-1 ring-foreground/40' : ''}`}
                onMouseEnter={() => {
                  setHoveredIndex(index)
                  onBarHover?.({
                    start: new Date(bar.ts).getTime() / 1000,
                    end: new Date(bar.ts).getTime() / 1000 + bar.spanSeconds,
                  })
                }}
                onMouseLeave={() => {
                  setHoveredIndex(null)
                  onBarHover?.(null)
                }}
              >
                <div className="relative w-full h-6 rounded-sm overflow-hidden cursor-pointer transition-opacity hover:opacity-80">
                  <div
                    className={`absolute inset-0 ${
                      bar.collecting && displayHealth === 'no_data'
                        ? (prevHealth && prevHealth !== 'no_data' ? healthColors[prevHealth] : 'bg-transparent border border-gray-200/40 dark:border-gray-700/40')
                        : (healthColors[displayHealth] ?? healthColors['no_data'])
                    }`}
                  />
                  {bar.collecting && (displayHealth !== 'no_data' || (prevHealth && prevHealth !== 'no_data')) && (
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-transparent to-background" />
                  )}
                </div>

                {hoveredIndex === index && (
                  <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 z-50">
                    <div className="bg-popover border border-border rounded-lg shadow-lg px-2.5 py-2 whitespace-nowrap">
                      <div className="text-[11px] font-medium text-foreground/80 mb-0.5">
                        {formatTimeRange(bar.ts, bar.spanSeconds)}
                      </div>
                      <div className={`text-xs ${
                        displayHealth === 'healthy' ? 'text-green-600 dark:text-green-400' :
                        displayHealth === 'degraded' ? 'text-amber-600 dark:text-amber-400' :
                        displayHealth === 'unhealthy' ? 'text-red-600 dark:text-red-400' :
                        displayHealth === 'disabled' ? 'text-gray-600 dark:text-gray-400' :
                        'text-muted-foreground'
                      }`}>
                        {healthLabels[displayHealth] || displayHealth}
                        {bar.collecting && <span className="text-muted-foreground ml-1">(In progress)</span>}
                        {bar.drainStatus && <span className="text-muted-foreground ml-1">({bar.drainStatus})</span>}
                        {reasons.length === 1 && <span className="text-muted-foreground"> — {reasons[0]}</span>}
                      </div>
                      {reasons.length > 1 && (
                        <div className="text-xs text-muted-foreground mt-1.5 space-y-0.5">
                          {reasons.map((reason, i) => (
                            <div key={i}>• {reason}</div>
                          ))}
                        </div>
                      )}
                    </div>
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

        <div className="flex justify-between mt-1 text-[10px] text-muted-foreground">
          <span>{startLabel}</span>
          <span>{endLabel}</span>
        </div>
      </div>

      {!hideBadges && badges.length > 0 && (
        <div className="flex gap-1.5 mt-2 flex-wrap">
          {badges.map((badge) => {
            const dimClass = 'bg-muted-foreground/10 text-muted-foreground/40'
            const colorMap: Record<string, string> = {
              'Errors': 'bg-fuchsia-500/15 text-fuchsia-600 dark:text-fuchsia-400',
              'FCS': 'bg-orange-500/15 text-orange-700 dark:text-orange-400',
              'Discards': 'bg-rose-500/15 text-rose-600 dark:text-rose-400',
              'Carrier': 'bg-orange-500/15 text-orange-600 dark:text-orange-400',
              'ISIS Overload': 'bg-red-600/15 text-red-700 dark:text-red-400',
              'ISIS Unreachable': 'bg-red-800/15 text-red-800 dark:text-red-400',
            }
            return (
            <span
              key={badge}
              className={`text-[10px] font-medium px-1.5 py-0.5 rounded transition-all ${isBadgeActive(badge) ? (colorMap[badge] ?? 'bg-muted text-muted-foreground') : dimClass}`}
            >
              {badge}
            </span>
            )
          })}
        </div>
      )}
    </div>
  )
}
