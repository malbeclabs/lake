/* eslint-disable react-refresh/only-export-components */
import { useState, useRef, useEffect, useCallback, useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Search, X, Server, Link2, MapPin, Building2, Filter, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'
import { fetchFieldValues } from '@/lib/api'
import type { SearchEntityType } from '@/lib/api'

const DEBOUNCE_MS = 150

// Only show entity types relevant to status filtering
const statusEntityTypes: SearchEntityType[] = ['device', 'link', 'metro', 'contributor']

const entityIcons: Record<string, React.ElementType> = {
  device: Server,
  link: Link2,
  metro: MapPin,
  contributor: Building2,
}

const fieldPrefixes = [
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'link:', description: 'Filter by link code' },
  { prefix: 'metro:', description: 'Filter by metro' },
  { prefix: 'contributor:', description: 'Filter by contributor' },
]

// Autocomplete config: maps field name to the entity/field to query and optional minChars
const autocompleteConfig: Record<string, { entity: string; field: string; minChars: number }> = {
  device: { entity: 'devices', field: 'code', minChars: 2 },
  link: { entity: 'links', field: 'code', minChars: 2 },
  metro: { entity: 'devices', field: 'metro', minChars: 0 },
  contributor: { entity: 'devices', field: 'contributor', minChars: 0 },
}

export interface StatusFilter {
  type: SearchEntityType
  value: string
  label: string
}

export function parseStatusFilters(searchParam: string): StatusFilter[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean).map(f => {
    const [type, ...rest] = f.split(':')
    const value = rest.join(':')
    if (type && value && statusEntityTypes.includes(type as SearchEntityType)) {
      return { type: type as SearchEntityType, value, label: value }
    }
    // Plain value without type prefix - treat as device code
    return { type: 'device' as SearchEntityType, value: f, label: f }
  })
}

export function serializeStatusFilters(filters: StatusFilter[]): string {
  return filters.map(f => `${f.type}:${f.value}`).join(',')
}

// Hook to get current filters for use in other components
export function useStatusFilters(): StatusFilter[] {
  const [searchParams] = useSearchParams()
  const searchParam = searchParams.get('filter') || ''
  return parseStatusFilters(searchParam)
}

// Hook to add a filter (used by other components)
export function useAddStatusFilter() {
  const [searchParams, setSearchParams] = useSearchParams()

  return useCallback((type: SearchEntityType, value: string, label?: string) => {
    const currentParam = searchParams.get('filter') || ''
    const filters = parseStatusFilters(currentParam)

    const newFilter: StatusFilter = { type, value, label: label || value }
    const exists = filters.some(f => f.type === newFilter.type && f.value === newFilter.value)

    if (!exists) {
      const newFilters = [...filters, newFilter]
      setSearchParams(prev => {
        prev.set('filter', serializeStatusFilters(newFilters))
        return prev
      })
    }
  }, [searchParams, setSearchParams])
}

// Inline filter input with autocomplete dropdown for status page
function StatusInlineFilter({ onCommit }: { onCommit: (type: string, value: string) => void }) {
  const [query, setQuery] = useState('')
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const [selectedIndex, setSelectedIndex] = useState(-1)
  const inputRef = useRef<HTMLInputElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  // Debounce query for autocomplete
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(query)
    }, DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  // Parse field:value from query
  const fieldValueMatch = useMemo(() => {
    const colonIndex = query.indexOf(':')
    if (colonIndex <= 0) return null
    const field = query.slice(0, colonIndex).toLowerCase()
    const value = query.slice(colonIndex + 1).toLowerCase()
    const config = autocompleteConfig[field]
    if (!config) return null
    return { field, value, entity: config.entity, acField: config.field, minChars: config.minChars }
  }, [query])

  const meetsMinChars = fieldValueMatch != null && (fieldValueMatch.value.length >= fieldValueMatch.minChars)

  // Fetch field values when a valid field prefix is detected
  const { data: fieldValuesData, isLoading: fieldValuesLoading } = useQuery({
    queryKey: ['field-values', fieldValueMatch?.entity, fieldValueMatch?.acField],
    queryFn: () => fetchFieldValues(fieldValueMatch!.entity, fieldValueMatch!.acField),
    enabled: fieldValueMatch !== null && meetsMinChars,
    staleTime: 60000,
  })

  // Filter field values based on what user has typed after the colon
  const filteredFieldValues = useMemo(() => {
    if (!fieldValueMatch || !fieldValuesData) return []
    const needle = fieldValueMatch.value
    if (!needle) return fieldValuesData
    return fieldValuesData.filter(v => v.toLowerCase().includes(needle))
  }, [fieldValueMatch, fieldValuesData])

  // Check if query matches any field prefix
  const matchingPrefixes = useMemo(() => {
    if (query.length === 0 || query.includes(':')) return []
    return fieldPrefixes.filter(p =>
      p.prefix.toLowerCase().startsWith(query.toLowerCase())
    )
  }, [query])

  const showAllPrefixes = query.length === 0 && isFocused

  // Build dropdown items
  type DropdownItem =
    | { type: 'prefix'; prefix: string; description: string }
    | { type: 'field-value'; field: string; value: string }
    | { type: 'type-more'; minChars: number }

  const items: DropdownItem[] = useMemo(() => {
    const result: DropdownItem[] = []

    if (fieldValueMatch && !meetsMinChars && fieldValueMatch.minChars > 0) {
      result.push({ type: 'type-more', minChars: fieldValueMatch.minChars })
    }

    if (filteredFieldValues.length > 0 && fieldValueMatch) {
      result.push(...filteredFieldValues.map(v => ({
        type: 'field-value' as const,
        field: fieldValueMatch.field,
        value: v,
      })))
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
  }, [filteredFieldValues, fieldValueMatch, showAllPrefixes, matchingPrefixes, meetsMinChars])

  // Reset selection when items change
  useEffect(() => {
    setSelectedIndex(-1)
  }, [debouncedQuery, matchingPrefixes.length, showAllPrefixes])

  const commitFieldValue = useCallback((field: string, value: string) => {
    onCommit(field, value)
    setQuery('')
    inputRef.current?.focus()
  }, [onCommit])

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
            commitFieldValue(item.field, item.value)
          }
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
  }, [items, selectedIndex, commitFieldValue, isFocused])

  // Close dropdown when clicking outside
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
      <div className="flex items-center gap-1.5 px-2 py-1 text-xs border border-border rounded-md bg-background hover:bg-muted/50 focus-within:ring-1 focus-within:ring-ring transition-colors">
        <Search className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => setIsFocused(true)}
          placeholder="Filter..."
          className="w-28 bg-transparent border-0 focus:outline-none placeholder:text-muted-foreground text-xs"
        />
        {fieldValuesLoading && (
          <Loader2 className="h-3 w-3 text-muted-foreground animate-spin" />
        )}
      </div>

      {showDropdown && (
        <div className="absolute top-full left-0 mt-1 w-64 max-h-64 overflow-y-auto bg-card border border-border rounded-lg shadow-lg z-40">
          {showAllPrefixes && (
            <div className="px-3 py-1.5 text-xs text-muted-foreground border-b border-border flex items-center gap-1">
              <Filter className="h-3 w-3" />
              Filter by field
            </div>
          )}

          {items.map((item, index) => {
            if (item.type === 'type-more') {
              return (
                <div
                  key="type-more"
                  className="px-3 py-2 text-xs text-muted-foreground"
                >
                  Type at least {item.minChars} characters to see suggestions
                </div>
              )
            }

            if (item.type === 'field-value') {
              return (
                <button
                  key={`field-value-${item.value}`}
                  onClick={() => commitFieldValue(item.field, item.value)}
                  className={cn(
                    'w-full flex items-center gap-2 px-3 py-2 text-left text-xs hover:bg-muted transition-colors',
                    index === selectedIndex && 'bg-muted'
                  )}
                >
                  <span className="flex-1 truncate">{item.value}</span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
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
                    'w-full flex flex-col gap-0.5 px-3 py-2 text-left text-xs hover:bg-muted transition-colors',
                    index === selectedIndex && 'bg-muted'
                  )}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{item.prefix.slice(0, -1)}</span>
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
                      filter
                    </span>
                  </div>
                  <span className="text-[10px] text-muted-foreground">{item.description}</span>
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

interface StatusFiltersProps {
  className?: string
}

// Compact filter display with inline search input and filter tags
export function StatusFilters({ className }: StatusFiltersProps) {
  const [searchParams, setSearchParams] = useSearchParams()
  const searchParam = searchParams.get('filter') || ''
  const filters = parseStatusFilters(searchParam)

  const removeFilter = useCallback((filterToRemove: StatusFilter) => {
    const newFilters = filters.filter(f => !(f.type === filterToRemove.type && f.value === filterToRemove.value))
    setSearchParams(prev => {
      if (newFilters.length === 0) {
        prev.delete('filter')
      } else {
        prev.set('filter', serializeStatusFilters(newFilters))
      }
      return prev
    })
  }, [filters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      prev.delete('filter')
      return prev
    })
  }, [setSearchParams])

  const addFilter = useCallback((type: string, value: string) => {
    const newFilter: StatusFilter = { type: type as SearchEntityType, value, label: value }
    const exists = filters.some(f => f.type === newFilter.type && f.value === newFilter.value)
    if (!exists) {
      const newFilters = [...filters, newFilter]
      setSearchParams(prev => {
        prev.set('filter', serializeStatusFilters(newFilters))
        return prev
      })
    }
  }, [filters, setSearchParams])

  return (
    <div className={className}>
      <div className="flex items-center gap-2 flex-wrap">
        {/* Filter tags */}
        {filters.map((filter, idx) => {
          const Icon = entityIcons[filter.type] || Server
          return (
            <button
              key={`${filter.type}-${filter.value}-${idx}`}
              onClick={() => removeFilter(filter)}
              className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
            >
              <Icon className="h-3 w-3" />
              {filter.label}
              <X className="h-3 w-3" />
            </button>
          )
        })}

        {/* Clear all */}
        {filters.length > 1 && (
          <button
            onClick={clearAllFilters}
            className="text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            Clear all
          </button>
        )}

        {/* Inline filter input */}
        <StatusInlineFilter onCommit={addFilter} />
      </div>
    </div>
  )
}
