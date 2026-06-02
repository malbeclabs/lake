import { useState, useRef, useEffect, useCallback, useMemo } from 'react'
import { Search, Filter, X } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { CalendarFilters } from './use-maintenance-events'
import type { CalendarView } from './date-utils'

const FIELD_PREFIXES = [
  { prefix: 'contributor:', description: 'Filter by contributor name' },
  { prefix: 'status:', description: 'Filter by status' },
  { prefix: 'metro:', description: 'Filter by metro code' },
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'link:', description: 'Filter by link code' },
]

const STATUS_VALUES = [
  'planned', 'in-progress', 'open', 'acknowledged',
  'investigating', 'mitigating', 'monitoring',
  'completed', 'resolved', 'closed',
]

const VIEWS: { value: CalendarView; label: string }[] = [
  { value: 'day', label: 'Day' },
  { value: 'week', label: 'Week' },
  { value: '2week', label: '2 weeks' },
  { value: 'month', label: 'Month' },
]

// ── chip helpers ──────────────────────────────────────────────────────────────

interface FilterChip { field: string; value: string; label: string }

function filtersToChips(f: CalendarFilters): FilterChip[] {
  const chips: FilterChip[] = []
  if (f.search) chips.push({ field: 'search', value: f.search, label: f.search })
  for (const v of f.contributors) chips.push({ field: 'contributor', value: v, label: `contributor:${v}` })
  if (f.status) chips.push({ field: 'status', value: f.status, label: `status:${f.status}` })
  for (const v of f.metros) chips.push({ field: 'metro', value: v, label: `metro:${v}` })
  for (const v of f.devices) chips.push({ field: 'device', value: v, label: `device:${v}` })
  for (const v of f.links) chips.push({ field: 'link', value: v, label: `link:${v}` })
  return chips
}

function removeChip(f: CalendarFilters, chip: FilterChip): CalendarFilters {
  if (chip.field === 'search') return { ...f, search: '' }
  if (chip.field === 'status') return { ...f, status: '' }
  if (chip.field === 'contributor') { const s = new Set(f.contributors); s.delete(chip.value); return { ...f, contributors: s } }
  if (chip.field === 'metro') { const s = new Set(f.metros); s.delete(chip.value); return { ...f, metros: s } }
  if (chip.field === 'device') { const s = new Set(f.devices); s.delete(chip.value); return { ...f, devices: s } }
  if (chip.field === 'link') { const s = new Set(f.links); s.delete(chip.value); return { ...f, links: s } }
  return f
}

function applyToken(f: CalendarFilters, token: string): CalendarFilters {
  const colonIdx = token.indexOf(':')
  if (colonIdx <= 0) return { ...f, search: token }
  const field = token.slice(0, colonIdx).toLowerCase()
  const value = token.slice(colonIdx + 1)
  if (field === 'contributor') { const s = new Set(f.contributors); s.add(value); return { ...f, contributors: s } }
  if (field === 'status') return { ...f, status: value }
  if (field === 'metro') { const s = new Set(f.metros); s.add(value); return { ...f, metros: s } }
  if (field === 'device') { const s = new Set(f.devices); s.add(value); return { ...f, devices: s } }
  if (field === 'link') { const s = new Set(f.links); s.add(value); return { ...f, links: s } }
  return { ...f, search: token }
}

// ── CalendarInlineFilter ──────────────────────────────────────────────────────

interface InlineFilterProps {
  filters: CalendarFilters
  onChange: (f: CalendarFilters) => void
  allContributors: string[]
  allMetros: string[]
  allDevices: string[]
  allLinks: string[]
}

type DropdownItem =
  | { type: 'prefix'; prefix: string; description: string }
  | { type: 'field-value'; field: string; value: string }
  | { type: 'apply-filter' }

function CalendarInlineFilter({
  filters, onChange, allContributors, allMetros, allDevices, allLinks,
}: InlineFilterProps) {
  const [query, setQuery] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const [selectedIndex, setSelectedIndex] = useState(-1)
  const inputRef = useRef<HTMLInputElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  const fieldValueMatch = useMemo(() => {
    const colonIdx = query.indexOf(':')
    if (colonIdx <= 0) return null
    return { field: query.slice(0, colonIdx).toLowerCase(), value: query.slice(colonIdx + 1) }
  }, [query])

  const fieldValues = useMemo(() => {
    if (!fieldValueMatch) return []
    const { field, value } = fieldValueMatch
    const list: Record<string, string[]> = {
      contributor: allContributors,
      status: STATUS_VALUES,
      metro: allMetros,
      device: allDevices,
      link: allLinks,
    }
    const src = list[field] ?? []
    return value ? src.filter(v => v.toLowerCase().includes(value.toLowerCase())) : src
  }, [fieldValueMatch, allContributors, allMetros, allDevices, allLinks])

  const matchingPrefixes = useMemo(() => {
    if (!query || query.includes(':')) return []
    return FIELD_PREFIXES.filter(p => p.prefix.toLowerCase().startsWith(query.toLowerCase()))
  }, [query])

  const showAllPrefixes = isFocused && query.length === 0

  const items: DropdownItem[] = useMemo(() => {
    const out: DropdownItem[] = []
    if (fieldValues.length > 0 && fieldValueMatch) {
      out.push(...fieldValues.map(v => ({ type: 'field-value' as const, field: fieldValueMatch.field, value: v })))
    } else if (query.length >= 1 && !query.endsWith(':') && fieldValues.length === 0 && matchingPrefixes.length === 0) {
      out.push({ type: 'apply-filter' })
    }
    if (showAllPrefixes) {
      out.push(...FIELD_PREFIXES.map(p => ({ type: 'prefix' as const, ...p })))
    } else if (matchingPrefixes.length > 0 && fieldValues.length === 0) {
      out.push(...matchingPrefixes.map(p => ({ type: 'prefix' as const, ...p })))
    }
    return out
  }, [query, fieldValues, fieldValueMatch, showAllPrefixes, matchingPrefixes])

  useEffect(() => { setSelectedIndex(-1) }, [query])

  const commit = useCallback((token: string) => {
    onChange(applyToken(filters, token))
    setQuery('')
    inputRef.current?.focus()
  }, [filters, onChange])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    const isOpen = isFocused && items.length > 0
    switch (e.key) {
      case 'ArrowDown':
        if (isOpen) { e.preventDefault(); setSelectedIndex(p => Math.min(p + 1, items.length - 1)) }
        break
      case 'ArrowUp':
        if (isOpen) { e.preventDefault(); setSelectedIndex(p => Math.max(p - 1, -1)) }
        break
      case 'Enter': {
        e.preventDefault()
        if (selectedIndex >= 0 && selectedIndex < items.length) {
          const item = items[selectedIndex]
          if (item.type === 'prefix') setQuery(item.prefix)
          else if (item.type === 'field-value') commit(`${item.field}:${item.value}`)
          else if (item.type === 'apply-filter' && query.trim()) commit(query.trim())
        } else if (query.trim()) {
          commit(query.trim())
        }
        break
      }
      case 'Tab':
        if (selectedIndex >= 0) {
          const item = items[selectedIndex]
          if (item?.type === 'prefix') { e.preventDefault(); setQuery(item.prefix) }
        }
        break
      case 'Escape':
        e.preventDefault()
        setQuery('')
        inputRef.current?.blur()
        break
    }
  }, [items, selectedIndex, query, commit, isFocused])

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) setIsFocused(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const chips = filtersToChips(filters)
  const showDropdown = isFocused && items.length > 0

  return (
    <div ref={containerRef} className="relative flex items-center gap-1.5 flex-wrap">
      {/* Input */}
      <div className="flex items-center gap-1.5 px-2.5 h-[30px] border border-border bg-[var(--input)] hover:bg-muted/50 focus-within:border-border/80 transition-colors">
        <Search className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => setIsFocused(true)}
          placeholder="Filter maintenance…"
          className="w-44 bg-transparent border-0 focus:outline-none placeholder:text-muted-foreground text-sm"
        />
      </div>

      {/* Active filter chips */}
      {chips.map(chip => (
        <div
          key={`${chip.field}:${chip.value}`}
          className="flex items-center gap-1 h-[30px] px-2 text-xs border border-border/70 bg-accent/10 text-foreground"
        >
          <span className="font-mono max-w-[180px] truncate">{chip.label}</span>
          <button
            type="button"
            onClick={() => onChange(removeChip(filters, chip))}
            className="ml-0.5 text-muted-foreground hover:text-foreground"
          >
            <X className="h-3 w-3" />
          </button>
        </div>
      ))}

      {/* Dropdown */}
      {showDropdown && (
        <div className="absolute top-full left-0 mt-1 w-72 max-h-72 overflow-y-auto bg-card border border-border shadow-lg z-40">
          {showAllPrefixes && (
            <div className="px-3 py-1.5 text-xs text-muted-foreground border-b border-border flex items-center gap-1">
              <Filter className="h-3 w-3" />
              Filter by field
            </div>
          )}

          {items.map((item, idx) => {
            if (item.type === 'apply-filter') {
              return (
                <button
                  key="apply"
                  onClick={() => commit(query.trim())}
                  className={cn('w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-muted transition-colors', idx === selectedIndex && 'bg-muted')}
                >
                  <Filter className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                  <span>Filter by "<span className="font-medium">{query}</span>"</span>
                </button>
              )
            }
            if (item.type === 'field-value') {
              return (
                <button
                  key={`fv-${item.value}`}
                  onClick={() => commit(`${item.field}:${item.value}`)}
                  className={cn('w-full flex items-center gap-2 px-3 py-2 text-left text-sm hover:bg-muted transition-colors', idx === selectedIndex && 'bg-muted')}
                >
                  <span className="flex-1 truncate">{item.value}</span>
                  <span className="text-xs px-1.5 py-0.5 bg-muted text-muted-foreground">{item.field}</span>
                </button>
              )
            }
            if (item.type === 'prefix') {
              return (
                <button
                  key={item.prefix}
                  onClick={() => { setQuery(item.prefix); inputRef.current?.focus() }}
                  className={cn('w-full flex flex-col gap-0.5 px-3 py-2 text-left text-sm hover:bg-muted transition-colors', idx === selectedIndex && 'bg-muted')}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{item.prefix.slice(0, -1)}</span>
                    <span className="text-xs px-1.5 py-0.5 bg-muted text-muted-foreground">filter</span>
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

// ── MaintenanceFilters (toolbar) ──────────────────────────────────────────────

interface FiltersProps {
  filters: CalendarFilters
  onFiltersChange: (f: CalendarFilters) => void
  hasActiveFilters: boolean
  allContributors: string[]
  allMetros: string[]
  allDevices: string[]
  allLinks: string[]
  view: CalendarView
  onViewChange: (v: CalendarView) => void
}

export function MaintenanceFilters({
  filters,
  onFiltersChange,
  hasActiveFilters,
  allContributors,
  allMetros,
  allDevices,
  allLinks,
  view,
  onViewChange,
}: FiltersProps) {
  function clearAll() {
    onFiltersChange({
      search: '',
      contributors: new Set(),
      metros: new Set(),
      devices: new Set(),
      links: new Set(),
      status: '',
    })
  }

  return (
    <div className="flex items-center gap-2 px-4 py-2 border-b border-border bg-card flex-wrap">
      <CalendarInlineFilter
        filters={filters}
        onChange={onFiltersChange}
        allContributors={allContributors}
        allMetros={allMetros}
        allDevices={allDevices}
        allLinks={allLinks}
      />

      {hasActiveFilters && (
        <button
          type="button"
          onClick={clearAll}
          className="flex items-center gap-1.5 h-[30px] px-2.5 text-sm text-muted-foreground hover:text-foreground border border-border/50 hover:border-border transition-colors"
        >
          <X className="h-3.5 w-3.5" />
          Clear filters
        </button>
      )}

      {/* View toggle — right-aligned */}
      <div className="ml-auto flex border border-border overflow-hidden">
        {VIEWS.map((v) => (
          <button
            key={v.value}
            type="button"
            onClick={() => onViewChange(v.value)}
            className={cn(
              'h-[30px] px-3 text-sm transition-colors border-r border-border last:border-r-0',
              view === v.value
                ? 'bg-muted text-foreground font-medium'
                : 'text-muted-foreground hover:text-foreground bg-[var(--input)]'
            )}
          >
            {v.label}
          </button>
        ))}
      </div>
    </div>
  )
}
