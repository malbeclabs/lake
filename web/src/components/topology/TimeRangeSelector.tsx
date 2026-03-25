import { useState } from 'react'
import { ChevronDown } from 'lucide-react'
import type { TimeRange, TimeRangePreset, BucketSize, TrafficMetric, TrafficView } from './utils'
import { bucketLabels, TIME_RANGE_OPTIONS } from './utils'


const BUCKET_OPTIONS: { value: BucketSize; label: string }[] = Object.entries(bucketLabels).map(
  ([value, label]) => ({ value: value as BucketSize, label })
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

function SmallDropdown<T extends string>({
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
  const selectedLabel = displayLabel ?? options.find(o => o.value === value)?.label ?? value

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-1 px-2 py-1 text-xs border border-border rounded-md bg-background hover:bg-muted transition-colors"
      >
        <span>{selectedLabel}</span>
        <ChevronDown className="h-3 w-3 text-muted-foreground" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[120px]">
            {options.map(opt => (
              <button
                key={opt.value}
                onClick={() => { onChange(opt.value); setIsOpen(false) }}
                className={cn(
                  'w-full text-left px-3 py-1.5 text-xs transition-colors',
                  opt.value === value
                    ? 'bg-accent text-accent-foreground'
                    : 'hover:bg-muted'
                )}
              >
                {opt.label}
              </button>
            ))}
          </div>
        </>
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
        <select
          value={value.preset}
          onChange={(e) => handlePresetChange(e.target.value)}
          className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
        >
          {TIME_RANGE_OPTIONS.map((opt) => (
            <option key={opt.value} value={opt.value}>{opt.label}</option>
          ))}
        </select>
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
  const bucketDisplayLabel = bucket === 'auto' && effectiveBucketLabel
    ? `Auto (${effectiveBucketLabel})`
    : undefined

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
        <SmallDropdown
          value={metric}
          options={METRIC_OPTIONS}
          onChange={onMetricChange}
        />
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
