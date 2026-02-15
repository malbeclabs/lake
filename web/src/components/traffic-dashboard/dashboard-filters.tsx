import { useState, useRef, useEffect, useCallback, useMemo } from 'react'
import { useQuery, useQueryClient, useIsFetching } from '@tanstack/react-query'
import { ChevronDown, X, Search, Filter, Loader2, RotateCcw, RefreshCw } from 'lucide-react'
import { useDashboard, type TimeRange, type IntfType, type RefreshInterval, refreshIntervalLabels } from './dashboard-context'
import { cn } from '@/lib/utils'
import { fetchFieldValues } from '@/lib/api'

const presetTimeRangeOptions: { value: TimeRange; label: string }[] = [
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

const metricOptions: { value: 'utilization' | 'throughput' | 'packets'; label: string }[] = [
  { value: 'throughput', label: 'Throughput' },
  { value: 'packets', label: 'Packets' },
  { value: 'utilization', label: 'Utilization' },
]

const intfTypeOptions: { value: IntfType; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'link', label: 'Link' },
  { value: 'tunnel', label: 'Tunnel' },
  { value: 'other', label: 'Other' },
]

function Dropdown<T extends string>({
  label,
  value,
  options,
  onChange,
  disabled = false,
}: {
  label: string
  value: T
  options: { value: T; label: string }[]
  onChange: (v: T) => void
  disabled?: boolean
}) {
  const [isOpen, setIsOpen] = useState(false)
  const selectedLabel = options.find(o => o.value === value)?.label ?? value

  return (
    <div className="relative inline-block">
      <button
        onClick={() => !disabled && setIsOpen(!isOpen)}
        className={cn(
          'flex items-center gap-1.5 px-3 py-1.5 text-sm border border-border rounded-md bg-background transition-colors',
          disabled ? 'opacity-50 cursor-not-allowed' : 'hover:bg-muted'
        )}
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
    <button
      onClick={onRemove}
      className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
    >
      {label}
      <X className="h-3 w-3" />
    </button>
  )
}

const DEBOUNCE_MS = 150

const fieldPrefixes = [
  { prefix: 'metro:', description: 'Filter by metro code', contextKey: 'metro' },
  { prefix: 'device:', description: 'Filter by device code', contextKey: 'device' },
  { prefix: 'intf:', description: 'Filter by interface name', contextKey: 'intf' },
  { prefix: 'link:', description: 'Filter by link type', contextKey: 'link_type' },
  { prefix: 'contributor:', description: 'Filter by contributor', contextKey: 'contributor' },
] as const

type ContextKey = typeof fieldPrefixes[number]['contextKey']

const autocompleteConfig: Record<string, { entity: string; field: string } | null> = {
  'metro': { entity: 'devices', field: 'metro' },
  'device': null,
  'intf': { entity: 'interfaces', field: 'intf' },
  'link': { entity: 'links', field: 'type' },
  'contributor': { entity: 'devices', field: 'contributor' },
}

function DashboardSearch() {
  const {
    metroFilter, setMetroFilter,
    deviceFilter, setDeviceFilter,
    linkTypeFilter, setLinkTypeFilter,
    contributorFilter, setContributorFilter,
    intfFilter, setIntfFilter,
  } = useDashboard()

  // Build scope filters to pass to field-values API so autocomplete
  // results are scoped to the active dashboard filters.
  const scopeFilters = useMemo(() => {
    const f: Record<string, string> = {}
    if (metroFilter.length > 0) f.metro = metroFilter.join(',')
    if (deviceFilter.length > 0) f.device = deviceFilter.join(',')
    if (linkTypeFilter.length > 0) f.link_type = linkTypeFilter.join(',')
    if (contributorFilter.length > 0) f.contributor = contributorFilter.join(',')
    if (intfFilter.length > 0) f.intf = intfFilter.join(',')
    return Object.keys(f).length > 0 ? f : undefined
  }, [metroFilter, deviceFilter, linkTypeFilter, contributorFilter, intfFilter])

  const [query, setQuery] = useState('')
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const [selectedIndex, setSelectedIndex] = useState(-1)
  const inputRef = useRef<HTMLInputElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(query), DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  // Parse field:value from query
  const fieldValueMatch = useMemo(() => {
    const colonIndex = query.indexOf(':')
    if (colonIndex <= 0) return null
    const field = query.slice(0, colonIndex).toLowerCase()
    const value = query.slice(colonIndex + 1).toLowerCase()
    const prefixEntry = fieldPrefixes.find(p => p.prefix === field + ':')
    if (!prefixEntry) return null
    return { field, value, contextKey: prefixEntry.contextKey }
  }, [query])

  // Autocomplete query config
  const acConfig = fieldValueMatch ? autocompleteConfig[fieldValueMatch.field] : null

  const { data: fieldValuesData, isLoading: fieldValuesLoading } = useQuery({
    queryKey: ['field-values', acConfig?.entity, acConfig?.field, scopeFilters],
    queryFn: () => fetchFieldValues(acConfig!.entity, acConfig!.field, scopeFilters),
    enabled: acConfig !== null && acConfig !== undefined,
    staleTime: 60000,
  })

  const filteredFieldValues = useMemo(() => {
    if (!fieldValueMatch || !fieldValuesData) return []
    const needle = fieldValueMatch.value
    if (!needle) return fieldValuesData
    return fieldValuesData.filter(v => v.toLowerCase().includes(needle))
  }, [fieldValueMatch, fieldValuesData])

  const matchingPrefixes = useMemo(() => {
    if (query.length === 0 || query.includes(':')) return []
    return fieldPrefixes.filter(p =>
      p.prefix.toLowerCase().startsWith(query.toLowerCase())
    )
  }, [query])

  const showAllPrefixes = query.length === 0 && isFocused

  const commitToDashboard = useCallback((contextKey: ContextKey, value: string) => {
    const setters: Record<ContextKey, { get: string[]; set: (f: string[]) => void }> = {
      metro: { get: metroFilter, set: setMetroFilter },
      device: { get: deviceFilter, set: setDeviceFilter },
      intf: { get: intfFilter, set: setIntfFilter },
      link_type: { get: linkTypeFilter, set: setLinkTypeFilter },
      contributor: { get: contributorFilter, set: setContributorFilter },
    }
    const { get, set } = setters[contextKey]
    if (!get.includes(value)) {
      set([...get, value])
    }
    setQuery('')
    inputRef.current?.focus()
  }, [metroFilter, setMetroFilter, deviceFilter, setDeviceFilter, intfFilter, setIntfFilter, linkTypeFilter, setLinkTypeFilter, contributorFilter, setContributorFilter])

  const commitFilter = useCallback((filterStr: string) => {
    const colonIndex = filterStr.indexOf(':')
    if (colonIndex > 0) {
      const field = filterStr.slice(0, colonIndex).toLowerCase()
      const value = filterStr.slice(colonIndex + 1)
      const prefixEntry = fieldPrefixes.find(p => p.prefix === field + ':')
      if (prefixEntry && value) {
        commitToDashboard(prefixEntry.contextKey, value)
        return
      }
    }
    // Free text: commit as device filter
    if (filterStr) {
      commitToDashboard('device', filterStr)
    }
  }, [commitToDashboard])

  // Build dropdown items
  type DropdownItem =
    | { type: 'prefix'; prefix: string; description: string }
    | { type: 'field-value'; field: string; value: string; contextKey: ContextKey }
    | { type: 'apply-filter' }

  const items: DropdownItem[] = useMemo(() => {
    const result: DropdownItem[] = []

    if (fieldValueMatch && filteredFieldValues.length > 0) {
      result.push(...filteredFieldValues.map(v => ({
        type: 'field-value' as const,
        field: fieldValueMatch.field,
        value: v,
        contextKey: fieldValueMatch.contextKey,
      })))
    } else if (query.length >= 1 && !showAllPrefixes && matchingPrefixes.length === 0) {
      result.push({ type: 'apply-filter' })
    }

    if (showAllPrefixes) {
      result.push(...fieldPrefixes.map(p => ({
        type: 'prefix' as const,
        prefix: p.prefix,
        description: p.description,
      })))
    } else if (matchingPrefixes.length > 0 && filteredFieldValues.length === 0) {
      result.push(...matchingPrefixes.map(p => ({
        type: 'prefix' as const,
        prefix: p.prefix,
        description: p.description,
      })))
    }

    return result
  }, [query, filteredFieldValues, fieldValueMatch, showAllPrefixes, matchingPrefixes])

  useEffect(() => {
    setSelectedIndex(-1)
  }, [debouncedQuery, matchingPrefixes.length, showAllPrefixes])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    const isDropdownOpen = isFocused && items.length > 0

    switch (e.key) {
      case 'ArrowDown':
        if (isDropdownOpen) {
          e.preventDefault()
          setSelectedIndex(prev => Math.min(prev + 1, items.length - 1))
        }
        break
      case 'ArrowUp':
        if (isDropdownOpen) {
          e.preventDefault()
          setSelectedIndex(prev => Math.max(prev - 1, -1))
        }
        break
      case 'Enter': {
        e.preventDefault()
        const indexToUse = selectedIndex >= 0 ? selectedIndex : 0
        if (indexToUse < items.length) {
          const item = items[indexToUse]
          if (item.type === 'prefix') {
            setQuery(item.prefix)
          } else if (item.type === 'field-value') {
            commitToDashboard(item.contextKey, item.value)
          } else if (item.type === 'apply-filter' && query.trim()) {
            commitFilter(query.trim())
          }
        } else if (query.trim()) {
          commitFilter(query.trim())
        }
        break
      }
      case 'Tab':
        if (selectedIndex >= 0 && selectedIndex < items.length) {
          const item = items[selectedIndex]
          if (item.type === 'prefix') {
            e.preventDefault()
            setQuery(item.prefix)
          }
        }
        break
      case 'Escape':
        e.preventDefault()
        setQuery('')
        inputRef.current?.blur()
        break
    }
  }, [items, selectedIndex, query, commitFilter, commitToDashboard, isFocused])

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsFocused(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const showDropdown = isFocused && items.length > 0

  return (
    <div ref={containerRef} className="relative">
      <div className="flex items-center gap-1.5 px-2 py-1 text-sm border border-border rounded-md bg-background hover:bg-muted/50 focus-within:ring-1 focus-within:ring-ring transition-colors">
        <Search className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => setIsFocused(true)}
          placeholder="Filter..."
          className="w-32 bg-transparent border-0 focus:outline-none placeholder:text-muted-foreground text-sm"
        />
        {fieldValuesLoading && (
          <Loader2 className="h-3.5 w-3.5 text-muted-foreground animate-spin" />
        )}
      </div>

      {showDropdown && (
        <div className="absolute top-full right-0 mt-1 w-64 max-h-64 overflow-y-auto bg-card border border-border rounded-lg shadow-lg z-40">
          {showAllPrefixes && (
            <div className="px-3 py-1.5 text-xs text-muted-foreground border-b border-border flex items-center gap-1">
              <Filter className="h-3 w-3" />
              Filter by field
            </div>
          )}

          {items.map((item, index) => {
            if (item.type === 'apply-filter') {
              return (
                <button
                  key="apply-filter"
                  onClick={() => commitFilter(query.trim())}
                  className={cn(
                    'w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-muted transition-colors',
                    index === selectedIndex && 'bg-muted'
                  )}
                >
                  <Filter className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                  <span>Filter by "<span className="font-medium">{query}</span>"</span>
                </button>
              )
            }

            if (item.type === 'field-value') {
              return (
                <button
                  key={`field-value-${item.value}`}
                  onClick={() => commitToDashboard(item.contextKey, item.value)}
                  className={cn(
                    'w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-muted transition-colors',
                    index === selectedIndex && 'bg-muted'
                  )}
                >
                  <span className="flex-1 truncate">{item.value}</span>
                  <span className="text-xs px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
                    {item.field}
                  </span>
                </button>
              )
            }

            if (item.type === 'prefix') {
              return (
                <button
                  key={item.prefix}
                  onClick={() => {
                    setQuery(item.prefix)
                    inputRef.current?.focus()
                  }}
                  className={cn(
                    'w-full flex flex-col gap-0.5 px-3 py-2 text-left text-sm hover:bg-muted transition-colors',
                    index === selectedIndex && 'bg-muted'
                  )}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{item.prefix.slice(0, -1)}</span>
                    <span className="text-xs px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
                      filter
                    </span>
                  </div>
                  <span className="text-xs text-muted-foreground">{item.description}</span>
                </button>
              )
            }

            return null
          })}
        </div>
      )}
    </div>
  )
}

function formatCustomLabel(start: number, end: number): string {
  const fmt = (ts: number) => {
    const d = new Date(ts * 1000)
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }) +
      ' ' + d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })
  }
  return `${fmt(start)} – ${fmt(end)}`
}

function toLocalDatetimeString(ts: number): string {
  const d = new Date(ts * 1000)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
}

function TimeRangeDropdown() {
  const { timeRange, setTimeRange, customStart, customEnd, setCustomRange, clearCustomRange } = useDashboard()
  const [isOpen, setIsOpen] = useState(false)
  const [showCustomPicker, setShowCustomPicker] = useState(false)
  const [customStartInput, setCustomStartInput] = useState('')
  const [customEndInput, setCustomEndInput] = useState('')

  const isCustom = timeRange === 'custom' && customStart != null && customEnd != null

  const selectedLabel = isCustom
    ? formatCustomLabel(customStart!, customEnd!)
    : presetTimeRangeOptions.find(o => o.value === timeRange)?.label ?? timeRange

  const handlePresetSelect = useCallback((v: TimeRange) => {
    setTimeRange(v)
    setIsOpen(false)
    setShowCustomPicker(false)
  }, [setTimeRange])

  const handleCustomClick = useCallback(() => {
    const now = Math.floor(Date.now() / 1000)
    const start = customStart ?? now - 3600
    const end = customEnd ?? now
    setCustomStartInput(toLocalDatetimeString(start))
    setCustomEndInput(toLocalDatetimeString(end))
    setShowCustomPicker(true)
  }, [customStart, customEnd])

  const handleCustomApply = useCallback(() => {
    const start = Math.floor(new Date(customStartInput).getTime() / 1000)
    const end = Math.floor(new Date(customEndInput).getTime() / 1000)
    if (!isNaN(start) && !isNaN(end) && end > start) {
      setCustomRange(start, end)
      setIsOpen(false)
      setShowCustomPicker(false)
    }
  }, [customStartInput, customEndInput, setCustomRange])

  return (
    <div className="flex items-center gap-1.5">
      <div className="relative inline-block">
        <button
          onClick={() => { setIsOpen(!isOpen); if (!isOpen) setShowCustomPicker(false) }}
          className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-border rounded-md bg-background hover:bg-muted transition-colors"
        >
          <span className="text-muted-foreground">Time:</span>
          <span className="font-medium max-w-[200px] truncate">{selectedLabel}</span>
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
        </button>
        {isOpen && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => { setIsOpen(false); setShowCustomPicker(false) }} />
            <div className="absolute left-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[200px]">
              {presetTimeRangeOptions.map(opt => (
                <button
                  key={opt.value}
                  onClick={() => handlePresetSelect(opt.value)}
                  className={cn(
                    'w-full text-left px-3 py-1.5 text-sm transition-colors',
                    opt.value === timeRange && !isCustom
                      ? 'bg-accent text-accent-foreground'
                      : 'hover:bg-muted'
                  )}
                >
                  {opt.label}
                </button>
              ))}
              <div className="border-t border-border my-1" />
              <button
                onClick={handleCustomClick}
                className={cn(
                  'w-full text-left px-3 py-1.5 text-sm transition-colors',
                  isCustom ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'
                )}
              >
                Custom range...
              </button>
              {showCustomPicker && (
                <div className="px-3 py-2 border-t border-border space-y-2">
                  <div>
                    <label className="text-xs text-muted-foreground">Start</label>
                    <input
                      type="datetime-local"
                      value={customStartInput}
                      onChange={e => setCustomStartInput(e.target.value)}
                      className="w-full mt-0.5 px-2 py-1 text-sm border border-border rounded bg-background"
                    />
                  </div>
                  <div>
                    <label className="text-xs text-muted-foreground">End</label>
                    <input
                      type="datetime-local"
                      value={customEndInput}
                      onChange={e => setCustomEndInput(e.target.value)}
                      className="w-full mt-0.5 px-2 py-1 text-sm border border-border rounded bg-background"
                    />
                  </div>
                  <button
                    onClick={handleCustomApply}
                    className="w-full px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground rounded hover:bg-primary/90 transition-colors"
                  >
                    Apply
                  </button>
                </div>
              )}
            </div>
          </>
        )}
      </div>
      {isCustom && (
        <button
          onClick={clearCustomRange}
          className="flex items-center gap-1 px-2 py-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
          title="Reset zoom"
        >
          <RotateCcw className="h-3 w-3" />
          Reset
        </button>
      )}
    </div>
  )
}

const refreshIntervalOptions: { value: RefreshInterval; label: string }[] = Object.entries(refreshIntervalLabels).map(
  ([value, label]) => ({ value: value as RefreshInterval, label })
)

function RefreshIntervalDropdown() {
  const { refreshInterval, setRefreshInterval } = useDashboard()

  return (
    <Dropdown
      label="Refresh"
      value={refreshInterval}
      options={refreshIntervalOptions}
      onChange={setRefreshInterval}
    />
  )
}

export function DashboardFilters() {
  const { metric, setMetric, intfType, setIntfType } = useDashboard()
  const queryClient = useQueryClient()
  const isFetching = useIsFetching()

  const handleManualRefresh = () => {
    queryClient.invalidateQueries()
  }

  return (
    <div className="flex items-center gap-3 flex-wrap">
      <TimeRangeDropdown />
      <Dropdown label="Metric" value={metric} options={metricOptions} onChange={setMetric} />
      <Dropdown label="Intf Type" value={intfType} options={intfTypeOptions} onChange={setIntfType} />
      <RefreshIntervalDropdown />
      <button
        onClick={handleManualRefresh}
        className="flex items-center gap-1.5 px-2 py-1.5 text-sm border border-border rounded-md bg-background hover:bg-muted transition-colors"
        title="Refresh now"
      >
        <RefreshCw className={cn('h-3.5 w-3.5', isFetching > 0 && 'animate-spin')} />
      </button>
      <DashboardSearch />
    </div>
  )
}

export function DashboardFilterBadges() {
  const {
    metroFilter, setMetroFilter,
    deviceFilter, setDeviceFilter,
    linkTypeFilter, setLinkTypeFilter,
    contributorFilter, setContributorFilter,
    intfFilter, setIntfFilter,
    clearFilters,
  } = useDashboard()

  const hasFilters = metroFilter.length > 0 || deviceFilter.length > 0 ||
    linkTypeFilter.length > 0 || contributorFilter.length > 0 || intfFilter.length > 0

  if (!hasFilters) return null

  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span className="text-xs text-muted-foreground">Filters:</span>
      {metroFilter.map(v => (
        <FilterBadge key={`metro-${v}`} label={`Metro: ${v}`} onRemove={() => setMetroFilter(metroFilter.filter(f => f !== v))} />
      ))}
      {deviceFilter.map(v => (
        <FilterBadge key={`device-${v}`} label={`Device: ${v}`} onRemove={() => setDeviceFilter(deviceFilter.filter(f => f !== v))} />
      ))}
      {intfFilter.map(v => (
        <FilterBadge key={`intf-${v}`} label={`Intf: ${v}`} onRemove={() => setIntfFilter(intfFilter.filter(f => f !== v))} />
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
  )
}
