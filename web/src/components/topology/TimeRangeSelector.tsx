import { useState } from 'react'
import type { TimeRange, TimeRangePreset } from './utils'

export const TIME_RANGE_OPTIONS: { value: TimeRangePreset; label: string }[] = [
  { value: '15m', label: '15 min' },
  { value: '30m', label: '30 min' },
  { value: '1h', label: '1 hour' },
  { value: '3h', label: '3 hours' },
  { value: '6h', label: '6 hours' },
  { value: '12h', label: '12 hours' },
  { value: '24h', label: '24 hours' },
  { value: '2d', label: '2 days' },
  { value: '7d', label: '7 days' },
  { value: 'custom', label: 'Custom' },
]

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

/** Get a human-readable label for a time range */
export function getTimeRangeLabel(timeRange: TimeRange): string {
  if (timeRange.preset === 'custom') return 'Custom Range'
  const opt = TIME_RANGE_OPTIONS.find(o => o.value === timeRange.preset)
  return opt?.label || '24 hours'
}

/** Convert a TimeRange preset to the simple string the status APIs expect */
export function timeRangeToString(timeRange: TimeRange): string {
  if (timeRange.preset === 'custom') return '24h'
  return timeRange.preset
}
