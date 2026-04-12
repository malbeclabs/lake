import { useEffect, useMemo, useState, useCallback, useRef } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Loader2, Radio, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { fetchMulticastGroups } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'

const PAGE_SIZE = 100

type SortField = 'code' | 'ip' | 'status' | 'publishers' | 'subscribers'
type SortDirection = 'asc' | 'desc'

function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

const validFilterFields = ['code', 'ip', 'status', 'publishers', 'subscribers']

const fieldPrefixes = [
  { prefix: 'code:', description: 'Filter by group code' },
  { prefix: 'ip:', description: 'Filter by multicast IP' },
  { prefix: 'status:', description: 'Filter by status' },
  { prefix: 'publishers:', description: 'Filter by publisher count (e.g., >5)' },
  { prefix: 'subscribers:', description: 'Filter by subscriber count (e.g., >10)' },
]

const autocompleteFields = ['status']

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

export function MulticastGroupsPage() {
  const navigate = useNavigate()
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
    queryKey: ['multicast-groups', offset, sortField, sortDirection, filterKey],
    queryFn: () => fetchMulticastGroups(PAGE_SIZE, offset, sortField, sortDirection, filterParams.length > 0 ? filterParams : undefined),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })
  const groups = response?.items ?? []

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
          <div className="text-lg font-medium mb-2">Unable to load multicast groups</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Radio}
          title="Multicast Groups"
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
                entity="multicast_groups"
                autocompleteFields={autocompleteFields}
                placeholder="Filter multicast groups..."
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
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('ip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('ip')}>
                      Multicast IP
                      <SortIcon field="ip" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('status')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('status')}>
                      Status
                      <SortIcon field="status" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('publishers')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('publishers')}>
                      Publishers
                      <SortIcon field="publishers" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('subscribers')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('subscribers')}>
                      Subscribers
                      <SortIcon field="subscribers" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {groups.map((group) => (
                  <tr
                    key={group.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/multicast-groups/${group.pk}`, navigate)}
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={group.code} className="font-mono text-sm" />
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={group.multicast_ip} className="text-sm font-mono text-muted-foreground" />
                    </td>
                    <td className="px-4 py-3 text-sm capitalize">
                      {group.status}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {group.publisher_count > 0 ? group.publisher_count : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {group.subscriber_count > 0 ? group.subscriber_count : <span className="text-muted-foreground">—</span>}
                    </td>
                  </tr>
                ))}
                {groups.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                      No multicast groups found
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
