import { useState, useRef } from 'react'
import { createPortal } from 'react-dom'
import { ChevronDown } from 'lucide-react'
import type { TimeRange, TimeRangePreset, BucketSize, TrafficMetric, TrafficView } from './utils'
import { bucketLabels, TIME_RANGE_OPTIONS } from './utils'

const BUCKET_OPTIONS: { value: BucketSize; label: string }[] = Object.entries(bucketLabels).map(
  ([value, label]) => ({ value: value as BucketSize, label }),
)

const METRIC_OPTIONS: { value: TrafficMetric; label: string }[] = [
  { value: 'throughput', label: 'bps' },
  { value: 'packets', label: 'pps' },
]

const TRAFFIC_VIEW_OPTIONS: { value: TrafficView; label: string }[] = [
  { value: 'peak', label: 'Max' },
  { value: 'p99', label: 'P99' },
  { value: 'p95', label: 'P95' },
  { value: 'p90', label: 'P90' },
  { value: 'p50', label: 'P50' },
  { value: 'avg', label: 'Avg' },
  { value: 'min', label: 'Min' },
]

function cn(...classes: (string | false | undefined)[]) {
  return classes.filter(Boolean).join(' ')
}

export function SmallDropdown<T extends string>({
  value,
  displayLabel,
  options,
  onChange,
}: {
  value: T
  displayLabel?: string
  options: { value: T; label: string }[]
  onChange: (v: T) => void
}) {
  const [isOpen, setIsOpen] = useState(false)
  const [menuStyle, setMenuStyle] = useState<React.CSSProperties>({} as React.CSSProperties)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const selectedLabel = displayLabel ?? options.find((o) => o.value === value)?.label ?? value

  const handleOpen = () => {
    if (!isOpen && buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect()
      const margin = 8
      const onRightHalf = rect.left > window.innerWidth / 2
      setMenuStyle(
        onRightHalf
          ? {
              position: 'fixed',
              top: rect.bottom + 4,
              right: Math.max(margin, window.innerWidth - rect.right),
            }
          : {
              position: 'fixed',
              top: rect.bottom + 4,
              left: Math.max(margin, rect.left),
            },
      )
    }
    setIsOpen(!isOpen)
  }

  return (
    <div className="relative inline-block">
      <button
        ref={buttonRef}
        onClick={handleOpen}
        className="flex w-full! items-center gap-1 px-3 py-1.5 text-xs border border-border rounded-md bg-background hover:bg-muted transition-colors"
      >
        <span>{selectedLabel}</span>
        <ChevronDown className="h-3 w-3 text-muted-foreground" />
      </button>
      {isOpen &&
        createPortal(
          <>
            <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
            <div
              style={menuStyle}
              className="z-50 bg-popover border border-border rounded-md shadow-lg py-1"
            >
              {options.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => {
                    onChange(opt.value)
                    setIsOpen(false)
                  }}
                  className={cn(
                    'block w-full min-w-24 text-left px-3 py-1.5 text-xs whitespace-nowrap transition-colors',
                    opt.value === value ? 'bg-accent text-accent-foreground' : 'hover:bg-muted',
                  )}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </>,
          document.body,
        )}
    </div>
  )
}

export function TimeRangeSelector({
  value,
  onChange,
}: {
  value: TimeRange
  onChange: (range: TimeRange) => void
}) {
  const [showCustom, setShowCustom] = useState(value.preset === 'custom')
  const [customFrom, setCustomFrom] = useState(value.from || '')
  const [customTo, setCustomTo] = useState(value.to || '')

  const handlePresetChange = (preset: string) => {
    if (preset === 'custom') {
      setShowCustom(true)
    } else {
      setShowCustom(false)
      onChange({ preset: preset as TimeRangePreset })
    }
  }

  const handleApplyCustom = () => {
    if (customFrom && customTo) {
      onChange({ preset: 'custom', from: customFrom, to: customTo })
    }
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <SmallDropdown
          value={value.preset}
          options={TIME_RANGE_OPTIONS as { value: string; label: string }[]}
          onChange={handlePresetChange}
        />
      </div>
      {showCustom && (
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <div className="flex items-center gap-1">
            <span className="text-muted-foreground">From:</span>
            <input
              type="text"
              placeholder="yyyy-mm-dd-hh:mm:ss"
              value={customFrom}
              onChange={(e) => setCustomFrom(e.target.value)}
              className="px-2 py-1 rounded border border-[var(--border)] bg-transparent w-40 font-mono text-xs"
            />
          </div>
          <div className="flex items-center gap-1">
            <span className="text-muted-foreground">To:</span>
            <input
              type="text"
              placeholder="yyyy-mm-dd-hh:mm:ss"
              value={customTo}
              onChange={(e) => setCustomTo(e.target.value)}
              className="px-2 py-1 rounded border border-[var(--border)] bg-transparent w-40 font-mono text-xs"
            />
          </div>
          <button
            onClick={handleApplyCustom}
            disabled={!customFrom || !customTo}
            className="px-2 py-1 text-xs rounded bg-[var(--primary)] text-[var(--primary-foreground)] disabled:opacity-50"
          >
            Apply
          </button>
        </div>
      )}
    </div>
  )
}

/** Bucket size + metric + traffic view selectors for traffic charts */
export function TrafficFilters({
  bucket,
  onBucketChange,
  metric,
  onMetricChange,
  effectiveBucketLabel,
  trafficView,
  onTrafficViewChange,
}: {
  bucket?: BucketSize
  onBucketChange?: (bucket: BucketSize) => void
  metric?: TrafficMetric
  onMetricChange?: (metric: TrafficMetric) => void
  effectiveBucketLabel?: string
  trafficView?: TrafficView
  onTrafficViewChange?: (view: TrafficView) => void
}) {
  const bucketDisplayLabel =
    bucket === 'auto' && effectiveBucketLabel ? `Auto (${effectiveBucketLabel})` : undefined

  return (
    <div className="flex items-center gap-2">
      {bucket && onBucketChange && (
        <SmallDropdown
          value={bucket}
          displayLabel={bucketDisplayLabel}
          options={BUCKET_OPTIONS}
          onChange={onBucketChange}
        />
      )}
      {metric && onMetricChange && (
        <SmallDropdown value={metric} options={METRIC_OPTIONS} onChange={onMetricChange} />
      )}
      {trafficView && onTrafficViewChange && (
        <SmallDropdown
          value={trafficView}
          options={TRAFFIC_VIEW_OPTIONS}
          onChange={onTrafficViewChange}
        />
      )}
    </div>
  )
}
