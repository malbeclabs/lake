import { useCallback, useLayoutEffect, useMemo, useState } from 'react'
import type {
  DeviceControllerCallsBucket,
  DeviceControllerCallsResponse,
  DeviceControllerCallStatus,
} from '@/lib/api'

interface DeviceControllerCallsChartProps {
  data?: DeviceControllerCallsResponse
  loading?: boolean
  className?: string
  compact?: boolean
  onBarHover?: (range: { start: number; end: number } | null) => void
  highlightedTime?: number | null
}

interface MergedCallBar {
  ts: string
  spanSeconds: number
  calls: number
  minutesWithCalls: number
  status: DeviceControllerCallStatus
  gapSeconds?: number
}

interface MetricTileProps {
  label: string
  value: string
  title?: string
  valueClassName?: string
}

const statusColors: Record<DeviceControllerCallStatus, string> = {
  calling: 'bg-green-500',
  stopped: 'bg-red-500',
  recovered: 'bg-blue-500',
  no_data: 'bg-muted/20 ring-1 ring-inset ring-border',
  not_expected: 'bg-muted',
}

const statusTextColors: Record<DeviceControllerCallStatus, string> = {
  calling: 'text-green-600 dark:text-green-400',
  stopped: 'text-red-600 dark:text-red-400',
  recovered: 'text-blue-600 dark:text-blue-400',
  no_data: 'text-muted-foreground',
  not_expected: 'text-muted-foreground',
}

const statusBadgeColors: Record<DeviceControllerCallStatus, string> = {
  calling: 'bg-green-500/10 text-green-700 dark:text-green-400',
  stopped: 'bg-red-500/10 text-red-700 dark:text-red-400',
  recovered: 'bg-blue-500/10 text-blue-700 dark:text-blue-400',
  no_data: 'bg-muted text-muted-foreground',
  not_expected: 'bg-muted text-muted-foreground',
}

const statusLabels: Record<DeviceControllerCallStatus, string> = {
  calling: 'Calling',
  stopped: 'Stopped',
  recovered: 'Recovered',
  no_data: 'No data',
  not_expected: 'Not expected',
}

const statusPriority: Record<DeviceControllerCallStatus, number> = {
  stopped: 4,
  recovered: 3,
  not_expected: 2,
  no_data: 1,
  calling: 0,
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

function worstStatus(a: DeviceControllerCallStatus, b: DeviceControllerCallStatus): DeviceControllerCallStatus {
  return statusPriority[a] >= statusPriority[b] ? a : b
}

function aggregateBar(group: DeviceControllerCallsBucket[], bucketSeconds: number): MergedCallBar {
  let status: DeviceControllerCallStatus = 'calling'
  let calls = 0
  let minutesWithCalls = 0
  let gapSeconds: number | undefined
  for (const bucket of group) {
    status = worstStatus(status, bucket.status)
    calls += bucket.calls
    minutesWithCalls += bucket.minutes_with_calls
    if (bucket.gap_seconds !== undefined) gapSeconds = bucket.gap_seconds
  }
  return {
    ts: group[0].ts,
    spanSeconds: group.length * bucketSeconds,
    calls,
    minutesWithCalls,
    status,
    gapSeconds,
  }
}

function mergeBuckets(
  buckets: DeviceControllerCallsBucket[],
  bucketSeconds: number,
  maxBars: number
): MergedCallBar[] {
  if (buckets.length <= maxBars) {
    return buckets.map((bucket) => aggregateBar([bucket], bucketSeconds))
  }

  const groupSize = Math.ceil(buckets.length / maxBars)
  const bars: MergedCallBar[] = []
  for (let i = 0; i < buckets.length; i += groupSize) {
    bars.push(aggregateBar(buckets.slice(i, i + groupSize), bucketSeconds))
  }
  return bars
}

function formatTimeRange(ts: string, spanSeconds: number): string {
  const start = new Date(ts)
  const end = new Date(start.getTime() + spanSeconds * 1000)
  const timeOpts: Intl.DateTimeFormatOptions = {
    hour: '2-digit',
    minute: '2-digit',
    ...(spanSeconds < 60 ? { second: '2-digit' } : {}),
  }
  const startTime = start.toLocaleTimeString([], timeOpts)
  const endTime = end.toLocaleTimeString([], timeOpts)
  const startDate = start.toLocaleDateString([], { month: 'short', day: 'numeric' })
  if (start.getDate() !== end.getDate()) {
    const endDate = end.toLocaleDateString([], { month: 'short', day: 'numeric' })
    return `${startDate} ${startTime} to ${endDate} ${endTime}`
  }
  return `${startDate} ${startTime} to ${endTime}`
}

function formatTimestamp(ts: string): string {
  return new Date(ts).toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`
  const minutes = seconds / 60
  if (minutes < 60) return `${Math.round(minutes)}m`
  const hours = minutes / 60
  if (hours < 48) return `${hours.toFixed(hours < 10 ? 1 : 0)}h`
  return `${(hours / 24).toFixed(1)}d`
}

function formatCount(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return String(value)
}

function rangeLabels(data: DeviceControllerCallsResponse): { startLabel: string; endLabel: string } {
  const rangeMap: Record<string, string> = {
    '1h': '1h ago',
    '3h': '3h ago',
    '6h': '6h ago',
    '12h': '12h ago',
    '24h': '24h ago',
    '3d': '3d ago',
    '7d': '7d ago',
    '14d': '14d ago',
    '30d': '30d ago',
  }
  if (rangeMap[data.time_range]) return { startLabel: rangeMap[data.time_range], endLabel: 'Now' }
  return { startLabel: formatTimestamp(data.from), endLabel: formatTimestamp(data.to) }
}

function selectedRangeMinutes(data: DeviceControllerCallsResponse): number {
  const start = new Date(data.from).getTime()
  const end = new Date(data.to).getTime()
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return 0
  return Math.max(1, Math.ceil((end - start) / 60_000))
}

function formatMinuteCoverage(data: DeviceControllerCallsResponse): string {
  if (!data.source_available) return 'No source'
  const totalMinutes = selectedRangeMinutes(data)
  if (totalMinutes === 0) return formatCount(data.minutes_with_calls)
  const pct = Math.min(100, Math.round((data.minutes_with_calls / totalMinutes) * 100))
  return `${formatCount(data.minutes_with_calls)} / ${formatCount(totalMinutes)} min (${pct}%)`
}

function statusSummary(data: DeviceControllerCallsResponse): string {
  if (!data.source_available) {
    return 'Controller history is not available for this environment.'
  }
  switch (data.last_status) {
    case 'calling':
      return 'Controller calls are arriving in the selected range.'
    case 'stopped':
      return `No controller calls for at least ${data.alert_threshold_minutes}m after steady prior history.`
    case 'recovered':
      return 'Controller calls resumed after a stopped period in this range.'
    case 'not_expected':
      return 'This device is not expected to call the controller.'
    case 'no_data':
    default:
      return 'No controller calls were observed in the selected range.'
  }
}

function barLabel(bar: MergedCallBar): string {
  const parts = [
    formatTimeRange(bar.ts, bar.spanSeconds),
    statusLabels[bar.status],
    `${formatCount(bar.calls)} calls`,
  ]
  if (bar.minutesWithCalls > 0) parts.push(`${formatCount(bar.minutesWithCalls)} min with calls`)
  if (bar.gapSeconds !== undefined && bar.calls === 0) parts.push(`${formatDuration(bar.gapSeconds)} gap`)
  return parts.join(', ')
}

function MetricTile({ label, value, title, valueClassName }: MetricTileProps) {
  return (
    <div className="min-w-0 border border-border/60 bg-muted/20 px-2.5 py-2">
      <div className="text-[11px] text-muted-foreground">{label}</div>
      <div className={`truncate font-mono text-xs tabular-nums ${valueClassName ?? ''}`} title={title ?? value}>
        {value}
      </div>
    </div>
  )
}

function LoadingSkeleton({ className }: { className?: string }) {
  return (
    <div className={className} aria-busy="true" aria-label="Loading controller calls">
      <div className="animate-pulse space-y-3">
        <div className="flex items-center justify-between">
          <div className="h-3 w-32 bg-muted" />
          <div className="h-5 w-16 bg-muted" />
        </div>
        <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="h-12 bg-muted/50" />
          ))}
        </div>
        <div className="flex gap-[2px]">
          {Array.from({ length: 64 }).map((_, i) => (
            <div key={i} className="h-7 flex-1 bg-muted/50" />
          ))}
        </div>
      </div>
    </div>
  )
}

export function DeviceControllerCallsChart({
  data,
  loading,
  className,
  compact,
  onBarHover,
  highlightedTime,
}: DeviceControllerCallsChartProps) {
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)
  const { containerRef, maxBars } = useContainerBars()

  const bars = useMemo(() => {
    if (!data) return []
    return mergeBuckets(data.buckets, data.bucket_seconds, maxBars)
  }, [data, maxBars])

  const highlightedBarIndex = useMemo(() => {
    if (highlightedTime == null || bars.length === 0) return -1
    let bestIdx = -1
    let bestDist = Infinity
    for (let i = 0; i < bars.length; i++) {
      const start = new Date(bars[i].ts).getTime() / 1000
      const end = start + bars[i].spanSeconds
      if (highlightedTime >= start && highlightedTime < end) return i
      const mid = start + bars[i].spanSeconds / 2
      const dist = Math.abs(highlightedTime - mid)
      if (dist < bestDist) {
        bestDist = dist
        bestIdx = i
      }
    }
    return bestDist < (bars[0]?.spanSeconds ?? 0) ? bestIdx : -1
  }, [highlightedTime, bars])

  if (loading && !data) return <LoadingSkeleton className={className} />
  if (!data) return null

  const labels = rangeLabels(data)
  const lastStatus = data.last_status ?? 'no_data'
  const summary = statusSummary(data)
  const gapAlertSeconds = data.alert_threshold_minutes * 60
  const currentGapIsAlert = data.current_gap_seconds !== undefined && data.current_gap_seconds >= gapAlertSeconds
  const lastCallValue = data.last_call_at
    ? compact && data.current_gap_seconds !== undefined
      ? `${formatDuration(data.current_gap_seconds)} ago`
      : formatTimestamp(data.last_call_at)
    : data.source_available
      ? 'No calls observed'
      : 'Source unavailable'
  const lastCallTitle = data.last_call_at
    ? `${formatTimestamp(data.last_call_at)}${data.current_gap_seconds !== undefined ? `, ${formatDuration(data.current_gap_seconds)} current gap` : ''}`
    : lastCallValue
  const currentGapValue = data.current_gap_seconds !== undefined
    ? formatDuration(data.current_gap_seconds)
    : data.source_available
      ? 'Unknown'
      : 'No source'
  const callsLabel = data.time_range ? `Calls in ${data.time_range}` : 'Calls in range'
  const barHeightClass = compact ? 'h-5' : 'h-7'

  return (
    <div ref={containerRef} className={className}>
      <div className="mb-3 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2 text-[11px] font-medium uppercase tracking-[0.14em] text-muted-foreground">
            <span>Controller calls</span>
            <span className="bg-muted/60 px-1.5 py-0.5 text-[10px] normal-case tracking-normal text-muted-foreground">
              GetConfig
            </span>
            {!data.source_available && (
              <span className="bg-muted/60 px-1.5 py-0.5 text-[10px] normal-case tracking-normal text-muted-foreground">
                source unavailable
              </span>
            )}
          </div>
          {!compact && (
            <div className="mt-1 max-w-[72ch] text-xs text-muted-foreground">
              {data.source_available
                ? `Controller GetConfig successes from controller history. Stopped follows the ${data.alert_threshold_minutes}m no-call alert after ${data.history_window_hours}h of prior history.`
                : 'Controller history is not available for this environment. No-data buckets are not outages.'}
            </div>
          )}
        </div>
        <span
          className={`shrink-0 px-2 py-0.5 text-[11px] font-medium ${statusBadgeColors[lastStatus]}`}
          title={summary}
        >
          {statusLabels[lastStatus]}
        </span>
      </div>

      <div className={`mb-3 grid gap-2 ${compact ? 'grid-cols-2' : 'grid-cols-2 lg:grid-cols-4'}`}>
        <MetricTile label="Last call" value={lastCallValue} title={lastCallTitle} />
        {!compact && (
          <MetricTile
            label="Current gap"
            value={currentGapValue}
            valueClassName={currentGapIsAlert ? statusTextColors.stopped : data.current_gap_seconds !== undefined ? statusTextColors.calling : undefined}
          />
        )}
        <MetricTile label={callsLabel} value={data.source_available ? formatCount(data.total_calls) : 'No source'} />
        {!compact && (
          <MetricTile
            label="Minutes with calls"
            value={formatMinuteCoverage(data)}
            title={data.source_available ? `${data.minutes_with_calls} minutes with calls out of ${selectedRangeMinutes(data)} selected minutes` : 'Controller history source unavailable'}
          />
        )}
      </div>

      {bars.length === 0 ? (
        <div className="border border-border/60 bg-muted/20 px-3 py-4 text-xs text-muted-foreground">
          No controller-call buckets were returned for this range.
        </div>
      ) : (
        <div className="relative">
          <div className="flex gap-[2px]" role="img" aria-label={`Controller calls timeline, ${statusLabels[lastStatus].toLowerCase()}`}>
            {bars.map((bar, index) => {
              const active = hoveredIndex === index
              const start = new Date(bar.ts).getTime() / 1000
              return (
                <div
                  key={`${bar.ts}-${index}`}
                  className={`relative min-w-0 flex-1 ${highlightedBarIndex === index ? 'z-10 ring-1 ring-foreground/40' : ''}`}
                  title={barLabel(bar)}
                  onMouseEnter={() => {
                    setHoveredIndex(index)
                    onBarHover?.({ start, end: start + bar.spanSeconds })
                  }}
                  onMouseLeave={() => {
                    setHoveredIndex(null)
                    onBarHover?.(null)
                  }}
                >
                  <div className={`relative w-full cursor-pointer overflow-hidden transition-opacity hover:opacity-80 ${barHeightClass}`}>
                    <div className={`absolute inset-0 ${statusColors[bar.status]}`} />
                  </div>
                  {active && (
                    <div className="absolute bottom-full left-1/2 z-50 mb-2 -translate-x-1/2">
                      <div className="whitespace-nowrap border border-border bg-popover px-2.5 py-2 shadow-lg">
                        <div className="mb-0.5 text-[11px] font-medium text-foreground/80">
                          {formatTimeRange(bar.ts, bar.spanSeconds)}
                        </div>
                        <div className={`text-xs ${statusTextColors[bar.status]}`}>
                          {statusLabels[bar.status]}
                          <span className="text-muted-foreground"> · {formatCount(bar.calls)} calls</span>
                          {bar.minutesWithCalls > 0 && (
                            <span className="text-muted-foreground"> · {formatCount(bar.minutesWithCalls)} min</span>
                          )}
                          {bar.gapSeconds !== undefined && bar.calls === 0 && (
                            <span className="text-muted-foreground"> · {formatDuration(bar.gapSeconds)} gap</span>
                          )}
                        </div>
                      </div>
                      <div className="absolute left-1/2 top-full -mt-[1px] -translate-x-1/2">
                        <div className="border-8 border-transparent border-t-border" />
                        <div className="absolute left-1/2 top-0 -translate-x-1/2 border-[7px] border-transparent border-t-popover" />
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
          <div className="mt-1 flex justify-between text-[10px] text-muted-foreground">
            <span>{labels.startLabel}</span>
            <span>{labels.endLabel}</span>
          </div>
        </div>
      )}

      {!compact && (
        <div className="mt-2 flex flex-wrap gap-1.5 text-[10px] font-medium">
          {(['calling', 'stopped', 'recovered', 'no_data'] as DeviceControllerCallStatus[]).map((status) => (
            <span key={status} className={`px-1.5 py-0.5 ${statusBadgeColors[status]}`}>
              {statusLabels[status]}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}
