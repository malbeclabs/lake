import { useState } from 'react'
import { ChevronDown, X } from 'lucide-react'
import { useDashboard, type TimeRange } from './dashboard-context'
import { cn } from '@/lib/utils'

const timeRangeOptions: { value: TimeRange; label: string }[] = [
  { value: '1h', label: '1 hour' },
  { value: '3h', label: '3 hours' },
  { value: '6h', label: '6 hours' },
  { value: '12h', label: '12 hours' },
  { value: '24h', label: '24 hours' },
  { value: '3d', label: '3 days' },
  { value: '7d', label: '7 days' },
  { value: '14d', label: '14 days' },
  { value: '30d', label: '30 days' },
]

const metricOptions: { value: 'utilization' | 'throughput'; label: string }[] = [
  { value: 'utilization', label: 'Utilization' },
  { value: 'throughput', label: 'Throughput' },
]

function Dropdown<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string
  value: T
  options: { value: T; label: string }[]
  onChange: (v: T) => void
}) {
  const [isOpen, setIsOpen] = useState(false)
  const selectedLabel = options.find(o => o.value === value)?.label ?? value

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-border rounded-md bg-background hover:bg-muted transition-colors"
      >
        <span className="text-muted-foreground">{label}:</span>
        <span className="font-medium">{selectedLabel}</span>
        <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute left-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[140px]">
            {options.map(opt => (
              <button
                key={opt.value}
                onClick={() => { onChange(opt.value); setIsOpen(false) }}
                className={cn(
                  'w-full text-left px-3 py-1.5 text-sm transition-colors',
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

function FilterBadge({ label, onRemove }: { label: string; onRemove: () => void }) {
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-accent text-accent-foreground rounded-full">
      {label}
      <button onClick={onRemove} className="hover:text-foreground">
        <X className="h-3 w-3" />
      </button>
    </span>
  )
}

export function DashboardFilters() {
  const {
    timeRange, setTimeRange,
    metric, setMetric,
    metroFilter, setMetroFilter,
    deviceFilter, setDeviceFilter,
    linkTypeFilter, setLinkTypeFilter,
    contributorFilter, setContributorFilter,
    clearFilters,
  } = useDashboard()

  const hasFilters = metroFilter.length > 0 || deviceFilter.length > 0 ||
    linkTypeFilter.length > 0 || contributorFilter.length > 0

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-3 flex-wrap">
        <Dropdown label="Time" value={timeRange} options={timeRangeOptions} onChange={setTimeRange} />
        <Dropdown label="Metric" value={metric} options={metricOptions} onChange={setMetric} />
      </div>
      {hasFilters && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted-foreground">Filters:</span>
          {metroFilter.map(v => (
            <FilterBadge key={`metro-${v}`} label={`Metro: ${v}`} onRemove={() => setMetroFilter(metroFilter.filter(f => f !== v))} />
          ))}
          {deviceFilter.map(v => (
            <FilterBadge key={`device-${v}`} label={`Device: ${v}`} onRemove={() => setDeviceFilter(deviceFilter.filter(f => f !== v))} />
          ))}
          {linkTypeFilter.map(v => (
            <FilterBadge key={`lt-${v}`} label={`Link: ${v}`} onRemove={() => setLinkTypeFilter(linkTypeFilter.filter(f => f !== v))} />
          ))}
          {contributorFilter.map(v => (
            <FilterBadge key={`cont-${v}`} label={`Contributor: ${v}`} onRemove={() => setContributorFilter(contributorFilter.filter(f => f !== v))} />
          ))}
          <button
            onClick={clearFilters}
            className="text-xs text-muted-foreground hover:text-foreground underline"
          >
            Clear all
          </button>
        </div>
      )}
    </div>
  )
}
