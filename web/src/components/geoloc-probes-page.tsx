import { useEffect, useMemo, useState, useCallback, useRef } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Loader2, MapPin, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { fetchGeolocProbes } from '@/lib/api'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'

const PAGE_SIZE = 100

type SortField = 'code' | 'owner' | 'ip' | 'exchange' | 'references' | 'updates'
type SortDirection = 'asc' | 'desc'

function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

const validFilterFields = ['code', 'owner', 'ip', 'exchange', 'references', 'updates']

const fieldPrefixes = [
  { prefix: 'code:', description: 'Filter by probe code' },
  { prefix: 'owner:', description: 'Filter by owner' },
  { prefix: 'ip:', description: 'Filter by public IP' },
  { prefix: 'exchange:', description: 'Filter by exchange' },
  { prefix: 'references:', description: 'Filter by reference count' },
  { prefix: 'updates:', description: 'Filter by update count' },
]

const autocompleteFields = ['code', 'owner', 'exchange']

function toFilterParam(filter: string): string {
  const colonIndex = filter.indexOf(':')
  if (colonIndex > 0) {
    const field = filter.slice(0, colonIndex).toLowerCase()
    const value = filter.slice(colonIndex + 1)
    if (validFilterFields.includes(field) && value) {
      return `${field}:${value}`
    }
  }
  return `all:${filter}`
}

export function GeolocProbesPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [liveFilter, setLiveFilter] = useState('')

  const page = parseInt(searchParams.get('page') || '1')
  const offset = (page - 1) * PAGE_SIZE
  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
      if (newPage <= 1) { newParams.delete('page') } else { newParams.set('page', String(newPage)) }
      return newParams
    })
  }, [setSearchParams])

  const sortField = (searchParams.get('sort') || 'code') as SortField
  const sortDirection = (searchParams.get('dir') || 'asc') as SortDirection

  const searchParam = searchParams.get('search') || ''
  const searchFilters = parseSearchFilters(searchParam)

  const allFilters = liveFilter
    ? [...searchFilters, liveFilter]
    : searchFilters

  const removeFilter = useCallback((filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      if (newFilters.length === 0) {
        newParams.delete('search')
      } else {
        newParams.set('search', newFilters.join(','))
      }
      return newParams
    })
  }, [searchFilters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('search')
      return newParams
    })
  }, [setSearchParams])

  const filterParams = useMemo(() => allFilters.map(toFilterParam), [allFilters])
  const filterKey = filterParams.join(',')

  const { data: response, isLoading, error } = useQuery({
    queryKey: ['geoloc-probes', offset, sortField, sortDirection, filterKey],
    queryFn: () => fetchGeolocProbes(PAGE_SIZE, offset, sortField, sortDirection, filterParams.length > 0 ? filterParams : undefined),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })
  const probes = response?.items ?? []

  const handleSort = (field: SortField) => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      if (sortField === field) {
        newParams.set('dir', sortDirection === 'asc' ? 'desc' : 'asc')
      } else {
        newParams.set('sort', field)
        newParams.set('dir', 'asc')
      }
      return newParams
    })
  }

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null
    return sortDirection === 'asc'
      ? <ChevronUp className="h-3 w-3" />
      : <ChevronDown className="h-3 w-3" />
  }

  const sortAria = (field: SortField) => {
    if (sortField !== field) return 'none'
    return sortDirection === 'asc' ? 'ascending' : 'descending'
  }

  const prevFilterRef = useRef(JSON.stringify(allFilters))
  useEffect(() => {
    const key = JSON.stringify(allFilters)
    if (prevFilterRef.current === key) return
    prevFilterRef.current = key
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('page')
      return newParams
    })
  }, [allFilters, setSearchParams])

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load geolocation probes</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={MapPin}
          title="Geolocation Probes"
          count={response?.total || 0}
          actions={
            <>
              {searchFilters.map((filter, idx) => (
                <button
                  key={`${filter}-${idx}`}
                  onClick={() => removeFilter(filter)}
                  className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
                >
                  {filter}
                  <X className="h-3 w-3" />
                </button>
              ))}
              {searchFilters.length > 1 && (
                <button
                  onClick={clearAllFilters}
                  className="text-xs text-muted-foreground hover:text-foreground transition-colors"
                >
                  Clear all
                </button>
              )}
              <InlineFilter
                fieldPrefixes={fieldPrefixes}
                entity="geoloc-probes"
                autocompleteFields={autocompleteFields}
                placeholder="Filter geolocation probes..."
                onLiveFilterChange={setLiveFilter}
              />
            </>
          }
        />

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('code')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('code')}>
                      Code
                      <SortIcon field="code" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('owner')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('owner')}>
                      Owner
                      <SortIcon field="owner" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('ip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('ip')}>
                      Public IP
                      <SortIcon field="ip" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('exchange')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('exchange')}>
                      Exchange
                      <SortIcon field="exchange" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('references')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('references')}>
                      References
                      <SortIcon field="references" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('updates')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('updates')}>
                      Updates
                      <SortIcon field="updates" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {probes.map((probe) => (
                  <tr
                    key={probe.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted transition-colors"
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={probe.code} className="font-mono text-sm" />
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={probe.owner} className="font-mono text-sm text-muted-foreground">
                        <span className="min-w-0 break-all" title={probe.owner}>{probe.owner.length > 12 ? `${probe.owner.slice(0, 6)}…${probe.owner.slice(-4)}` : probe.owner}</span>
                      </CopyableText>
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={probe.public_ip} className="font-mono text-sm text-muted-foreground" />
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={probe.exchange_pk} className="font-mono text-sm text-muted-foreground">
                        <span className="min-w-0 break-all" title={probe.exchange_pk}>{probe.exchange_pk.length > 12 ? `${probe.exchange_pk.slice(0, 6)}…${probe.exchange_pk.slice(-4)}` : probe.exchange_pk}</span>
                      </CopyableText>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {probe.reference_count > 0 ? probe.reference_count : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {probe.target_update_count > 0 ? probe.target_update_count : <span className="text-muted-foreground">—</span>}
                    </td>
                  </tr>
                ))}
                {probes.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                      No geolocation probes found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {response && (
            <Pagination
              total={response.total ?? 0}
              limit={PAGE_SIZE}
              offset={offset}
              onOffsetChange={setOffset}
            />
          )}
        </div>
      </div>
    </div>
  )
}
