import { useState, useMemo, useCallback, useLayoutEffect } from 'react'
import type { LinkMetricsResponse, LinkMetricsBucket } from '@/lib/api'

interface LinkHealthTimelineProps {
  data: LinkMetricsResponse
  className?: string
  hideBadges?: boolean
  onBarHover?: (range: { start: number; end: number } | null) => void
  highlightedTime?: number | null  // unix seconds
}

// Hard-drained: dark stripes over health color
const hardDrainedStripeStyle: React.CSSProperties = {
  backgroundImage: 'repeating-linear-gradient(135deg, rgba(60,60,60,0.85), rgba(60,60,60,0.85) 5px, transparent 5px, transparent 7px)',
}

// Soft-drained: light/white stripes over health color
const softDrainedStripeStyle: React.CSSProperties = {
  backgroundImage: 'repeating-linear-gradient(135deg, rgba(255,255,255,0.6), rgba(255,255,255,0.6) 5px, transparent 5px, transparent 7px)',
}

function getDrainStripeStyle(drainStatus?: string): React.CSSProperties | undefined {
  if (drainStatus === 'hard-drained') return hardDrainedStripeStyle
  if (drainStatus === 'soft-drained') return softDrainedStripeStyle
  return undefined
}

const healthColors: Record<string, string> = {
  healthy: 'bg-green-500',
  degraded: 'bg-amber-500',
  unhealthy: 'bg-red-500',
  down: 'bg-gray-500 dark:bg-gray-700',
  no_data: 'bg-transparent border border-gray-200 dark:border-gray-700',
}

const healthLabels: Record<string, string> = {
  healthy: 'Healthy',
  degraded: 'Degraded',
  unhealthy: 'Unhealthy',
  down: 'Down',
  no_data: 'No Data',
}

const healthPriority: Record<string, number> = {
  unhealthy: 3,
  degraded: 2,
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
  // Aggregated metrics for tooltip
  maxLossPct: number
  avgLatencyUs: number
  totalErrors: number
  totalFcsErrors: number
  totalDiscards: number
  totalCarrier: number
  isisDown: boolean
  drainStatus: string
  samples: number
  hasLatency: boolean
  hasTraffic: boolean
  missingLatency: boolean
  serverReasons: string[]
  missingTraffic: boolean
}

function aggregateBar(group: LinkMetricsBucket[], bucketSeconds: number, latencyIncluded = true, trafficIncluded = true): MergedBar {
  let health = 'healthy'
  let collecting = false
  let maxLossPct = 0
  let latencySum = 0
  let latencyCount = 0
  let totalErrors = 0
  let totalFcsErrors = 0
  let totalDiscards = 0
  let totalCarrier = 0
  let isisDown = false
  let drainStatus = ''
  let samples = 0
  let hasLatency = false
  let hasTraffic = false
  let missingLatency = false
  let missingTraffic = false
  let nonCollectingCount = 0

  for (const b of group) {
    health = worstHealth(health, b.status?.health ?? 'no_data')
    if (b.status?.collecting) collecting = true
    if (b.status?.isis_down) isisDown = true
    if (b.status?.drain_status) drainStatus = b.status.drain_status

    if (!b.status?.collecting) {
      nonCollectingCount++
      if (latencyIncluded && !b.latency) missingLatency = true
      if (trafficIncluded && !b.traffic) missingTraffic = true
    }

    if (b.latency) {
      hasLatency = true
      const loss = Math.max(b.latency.a_loss_pct, b.latency.z_loss_pct)
      if (loss > maxLossPct) maxLossPct = loss
      const totalSamples = b.latency.a_samples + b.latency.z_samples
      if (totalSamples > 0) {
        const avgLat = (b.latency.a_avg_rtt_us * b.latency.a_samples + b.latency.z_avg_rtt_us * b.latency.z_samples) / totalSamples
        latencySum += avgLat
        latencyCount++
      }
      samples += totalSamples
    }

    if (b.traffic) {
      hasTraffic = true
      const t = b.traffic
      totalErrors += t.side_a_in_errors + t.side_a_out_errors + t.side_z_in_errors + t.side_z_out_errors
      totalFcsErrors += t.side_a_in_fcs_errors + t.side_z_in_fcs_errors
      totalDiscards += t.side_a_in_discards + t.side_a_out_discards + t.side_z_in_discards + t.side_z_out_discards
      totalCarrier += t.side_a_carrier_transitions + t.side_z_carrier_transitions
    }
  }

  return {
    ts: group[0].ts,
    spanSeconds: group.length * bucketSeconds,
    health,
    collecting,
    maxLossPct,
    avgLatencyUs: latencyCount > 0 ? latencySum / latencyCount : 0,
    totalErrors,
    totalFcsErrors,
    totalDiscards,
    totalCarrier,
    isisDown,
    drainStatus,
    samples,
    hasLatency,
    hasTraffic,
    missingLatency: missingLatency && nonCollectingCount > 0,
    missingTraffic: missingTraffic && nonCollectingCount > 0,
    serverReasons: Array.from(new Set(group.flatMap(b => b.status?.reasons ?? []))),
  }
}

// Check if any bucket in the dataset has latency/traffic data.
// If none do, the field wasn't included in the request — don't flag as missing.
function hasAnyLatency(buckets: LinkMetricsBucket[]): boolean {
  return buckets.some(b => b.latency != null)
}

function hasAnyTraffic(buckets: LinkMetricsBucket[]): boolean {
  return buckets.some(b => b.traffic != null)
}

function mergeBuckets(buckets: LinkMetricsBucket[], bucketSeconds: number, maxBars: number): MergedBar[] {
  const latencyIncluded = hasAnyLatency(buckets)
  const trafficIncluded = hasAnyTraffic(buckets)

  if (buckets.length <= maxBars) {
    return buckets.map((b) => aggregateBar([b], bucketSeconds, latencyIncluded, trafficIncluded))
  }

  const groupSize = Math.ceil(buckets.length / maxBars)
  const bars: MergedBar[] = []
  for (let i = 0; i < buckets.length; i += groupSize) {
    bars.push(aggregateBar(buckets.slice(i, i + groupSize), bucketSeconds, latencyIncluded, trafficIncluded))
  }
  return bars
}

// Mark trailing no_data bars as collecting if they're within the rollup lag
// window (near now). The backend only marks the very last bucket as collecting,
// but rollup data typically lags by 5-10 minutes.
// Mark trailing bars near now as collecting to account for rollup lag.
// The backend only marks the very last bucket, but data typically lags 5-10 min.
// Also suppress missingLatency/missingTraffic in the lag window since partial
// data there is expected, not an incident signal.
function markTrailingCollecting(bars: MergedBar[]): void {
  const now = Date.now()
  for (let i = bars.length - 1; i >= 0; i--) {
    const barEnd = new Date(bars[i].ts).getTime() + bars[i].spanSeconds * 1000
    if (now - barEnd > 10 * 60 * 1000) break
    // Suppress missing-data flags in the lag window
    bars[i].missingLatency = false
    bars[i].missingTraffic = false
    if (bars[i].health === 'no_data' && !bars[i].hasLatency && !bars[i].hasTraffic) {
      bars[i].collecting = true
    }
  }
}

function getReasons(bar: MergedBar, committedRttUs: number): string[] {
  // Use server-provided reasons when available (covers latency-only scenarios
  // where the client doesn't have latency data in the response)
  if (bar.serverReasons.length > 0) return bar.serverReasons

  const reasons: string[] = []

  if (bar.isisDown) reasons.push('ISIS down')
  if (bar.missingLatency) reasons.push('No latency data')
  if (bar.missingTraffic) reasons.push('No traffic data')
  if (bar.health === 'no_data') return reasons

  if (bar.maxLossPct >= 95) reasons.push('Extended packet loss (≥95%)')
  else if (bar.maxLossPct >= 25) reasons.push(`Severe packet loss (${bar.maxLossPct.toFixed(1)}%)`)
  else if (bar.maxLossPct >= 1) reasons.push(`Moderate packet loss (${bar.maxLossPct.toFixed(1)}%)`)
  else if (bar.maxLossPct > 0) reasons.push(`Minor packet loss (${bar.maxLossPct.toFixed(2)}%)`)

  if (committedRttUs > 0 && bar.avgLatencyUs > 0) {
    const overPct = ((bar.avgLatencyUs - committedRttUs) / committedRttUs) * 100
    if (overPct >= 100) reasons.push(`High latency (${overPct.toFixed(0)}% over SLA)`)
    else if (overPct >= 20) reasons.push(`Elevated latency (${overPct.toFixed(0)}% over SLA)`)
  }

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

// Target ~6px per bar (including 2px gap), with floor/ceiling
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

export function LinkHealthTimeline({ data, className, hideBadges, onBarHover, highlightedTime }: LinkHealthTimelineProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)
  const { containerRef, maxBars } = useContainerBars()

  const bars = useMemo(() => {
    const merged = mergeBuckets(data.buckets, data.bucket_seconds, maxBars)
    markTrailingCollecting(merged)
    return merged
  }, [data.buckets, data.bucket_seconds, maxBars])

  const highlightedBarIndex = useMemo(() => {
    if (highlightedTime == null) return -1
    // Find bar containing the time, or closest bar
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

  // Detect issue badges across all buckets
  const badges = useMemo(() => {
    const found = new Set<string>()
    for (const b of data.buckets) {
      if (b.latency) {
        if (b.latency.a_loss_pct > 0 || b.latency.z_loss_pct > 0) found.add('Loss')
      }
      if (b.traffic) {
        const t = b.traffic
        if (t.side_a_in_errors + t.side_a_out_errors + t.side_z_in_errors + t.side_z_out_errors > 0) found.add('Errors')
        if (t.side_a_in_fcs_errors + t.side_z_in_fcs_errors > 0) found.add('FCS')
        if (t.side_a_in_discards + t.side_a_out_discards + t.side_z_in_discards + t.side_z_out_discards > 0) found.add('Discards')
        if (t.side_a_carrier_transitions + t.side_z_carrier_transitions > 0) found.add('Carrier')
      }
      if (b.status?.isis_down) found.add('ISIS Down')
    }
    return Array.from(found)
  }, [data.buckets])

  // Recent badges: active in last 30 minutes
  // Use a coarse timestamp (floored to minutes) so this doesn't change every render
  const nowMinutes = Math.floor(Date.now() / 60000)
  const recentBadges = useMemo(() => {
    const recent = new Set<string>()
    const cutoff = nowMinutes * 60 - 30 * 60
    for (const b of data.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < cutoff) continue
      if (b.status?.reasons) {
        for (const r of b.status.reasons) {
          if (r.includes('packet loss')) recent.add('Loss')
          if (r.includes('latency')) recent.add('Latency')
          if (r.includes('interface error')) recent.add('Errors')
          if (r.includes('discard')) recent.add('Discards')
          if (r.includes('carrier')) recent.add('Carrier')
        }
      }
      if (b.traffic) {
        const t = b.traffic
        if (t.side_a_in_errors + t.side_a_out_errors + t.side_z_in_errors + t.side_z_out_errors > 0) recent.add('Errors')
        if (t.side_a_in_fcs_errors + t.side_z_in_fcs_errors > 0) recent.add('FCS')
        if (t.side_a_in_discards + t.side_a_out_discards + t.side_z_in_discards + t.side_z_out_discards > 0) recent.add('Discards')
        if (t.side_a_carrier_transitions + t.side_z_carrier_transitions > 0) recent.add('Carrier')
      }
      if (b.status?.isis_down) recent.add('ISIS Down')
    }
    return recent
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data.buckets, nowMinutes])

  // Hovered bar badges
  const hoveredBarBadges = useMemo(() => {
    const activeIdx = hoveredIndex ?? highlightedBarIndex
    if (activeIdx == null || activeIdx < 0 || activeIdx >= bars.length) return null
    const bar = bars[activeIdx]
    const active = new Set<string>()
    if (bar.maxLossPct > 0) active.add('Loss')
    if (bar.totalErrors > 0) active.add('Errors')
    if (bar.totalFcsErrors > 0) active.add('FCS')
    if (bar.totalDiscards > 0) active.add('Discards')
    if (bar.totalCarrier > 0) active.add('Carrier')
    if (bar.isisDown) active.add('ISIS Down')
    // Also check server reasons
    for (const r of bar.serverReasons) {
      if (r.includes('packet loss')) active.add('Loss')
      if (r.includes('interface error')) active.add('Errors')
      if (r.includes('discard')) active.add('Discards')
      if (r.includes('carrier')) active.add('Carrier')
    }
    return active
  }, [hoveredIndex, highlightedBarIndex, bars])

  const isBadgeActive = (badge: string) => {
    if (hoveredBarBadges) return hoveredBarBadges.has(badge)
    return recentBadges.has(badge)
  }

  // Time labels
  const labels = useMemo(() => {
    const rangeMap: Record<string, string> = {
      '1h': '1h ago', '6h': '6h ago', '12h': '12h ago',
      '24h': '24h ago', '3d': '3d ago', '7d': '7d ago',
    }
    if (rangeMap[data.time_range]) {
      return { startLabel: rangeMap[data.time_range], endLabel: 'Now' as string }
    }
    // Custom range — derive from actual bucket timestamps
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
            const reasons = getReasons(bar, data.committed_rtt_us)
            // Override health: ISIS down → 'down' (grey), missing latency → 'degraded'
            const displayHealth = bar.isisDown
              ? 'down'
              : bar.health === 'healthy' && bar.missingLatency
                ? 'degraded' : bar.health
            const prevBar = index > 0 ? bars[index - 1] : undefined
            const prevHealth = prevBar
              ? (prevBar.isisDown ? 'down' : prevBar.health === 'healthy' && prevBar.missingLatency ? 'degraded' : prevBar.health)
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
                    style={getDrainStripeStyle(bar.drainStatus)}
                  />
                  {bar.collecting && (displayHealth !== 'no_data' || (prevHealth && prevHealth !== 'no_data')) && (
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-transparent to-background" />
                  )}
                </div>

                {/* Tooltip */}
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
                        displayHealth === 'down' ? 'text-gray-600 dark:text-gray-400' :
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
          <span>{startLabel}</span>
          <span>{endLabel}</span>
        </div>
      </div>

      {/* Issue badges */}
      {!hideBadges && badges.length > 0 && (
        <div className="flex gap-1.5 mt-2 flex-wrap">
          {badges.map((badge) => {
            const dimClass = 'bg-muted-foreground/10 text-muted-foreground/40'
            const colorMap: Record<string, string> = {
              'Loss': 'bg-purple-500/15 text-purple-700 dark:text-purple-400',
              'Errors': 'bg-red-500/15 text-red-700 dark:text-red-400',
              'FCS': 'bg-orange-500/15 text-orange-700 dark:text-orange-400',
              'Discards': 'bg-teal-500/15 text-teal-700 dark:text-teal-400',
              'Carrier': 'bg-yellow-500/15 text-yellow-700 dark:text-yellow-400',
              'ISIS Down': 'bg-rose-500/15 text-rose-700 dark:text-rose-400',
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
