import { useEffect, useMemo, useState, useCallback, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Loader2, Radio, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { fetchMulticastGroups } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'

const PAGE_SIZE = 100

type SortField = 'code' | 'multicast_ip' | 'status' | 'publishers' | 'subscribers'
type SortDirection = 'asc' | 'desc'

type NumericFilter = {
  op: '>' | '>=' | '<' | '<=' | '='
  value: number
}

const numericSearchFields: SortField[] = ['publishers', 'subscribers']

function parseNumericFilter(input: string): NumericFilter | null {
  const match = input.trim().match(/^(>=|<=|>|<|==|=)\s*(-?\d+(?:\.\d+)?)$/)
  if (!match) return null
  const op = match[1] === '==' ? '=' : (match[1] as NumericFilter['op'])
  return { op, value: Number(match[2]) }
}

function matchesNumericFilter(value: number, filter: NumericFilter): boolean {
  switch (filter.op) {
    case '>':
      return value > filter.value
    case '>=':
      return value >= filter.value
    case '<':
      return value < filter.value
    case '<=':
      return value <= filter.value
    case '=':
      return value === filter.value
  }
}

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

const autocompleteFields: string[] = []

function parseFilter(filter: string): { field: string; value: string } {
  const colonIndex = filter.indexOf(':')
  if (colonIndex > 0) {
    const field = filter.slice(0, colonIndex).toLowerCase()
    const value = filter.slice(colonIndex + 1)
    if (validFilterFields.includes(field) && value) {
      return { field, value }
    }
  }
  return { field: 'all', value: filter }
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

  const activeFilterRaw = liveFilter || searchFilters[0] || ''

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

  const { data: groups, isLoading, error } = useQuery({
    queryKey: ['multicast-groups'],
    queryFn: () => fetchMulticastGroups(),
    refetchInterval: 30000,
  })

  const filteredGroups = useMemo(() => {
    if (!groups) return []
    if (!activeFilterRaw) return groups

    const filter = parseFilter(activeFilterRaw)
    const searchField = filter.field as SortField | 'all' | 'ip'
    const needle = filter.value.trim().toLowerCase()
    if (!needle) return groups

    const numericFilter = parseNumericFilter(filter.value)
    if (searchField !== 'all' && numericFilter && numericSearchFields.includes(searchField as SortField)) {
      const getNumericValue = (group: typeof groups[number]) => {
        switch (searchField) {
          case 'publishers':
            return group.publisher_count
          case 'subscribers':
            return group.subscriber_count
          default:
            return 0
        }
      }
      return groups.filter(group => matchesNumericFilter(getNumericValue(group), numericFilter))
    }

    const getSearchValue = (group: typeof groups[number], field: string) => {
      switch (field) {
        case 'code':
          return group.code
        case 'ip':
          return group.multicast_ip
        case 'status':
          return group.status
        default:
          return ''
      }
    }

    if (searchField === 'all') {
      return groups.filter(group => {
        const textFields = ['code', 'ip', 'status']
        return textFields.some(field => getSearchValue(group, field).toLowerCase().includes(needle))
      })
    }

    return groups.filter(group => getSearchValue(group, searchField).toLowerCase().includes(needle))
  }, [groups, activeFilterRaw])

  const sortedGroups = useMemo(() => {
    if (!filteredGroups) return []
    const seen = new Set<string>()
    const unique = filteredGroups.filter(g => {
      if (seen.has(g.pk)) return false
      seen.add(g.pk)
      return true
    })
    const sorted = [...unique].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'code':
          cmp = a.code.localeCompare(b.code)
          break
        case 'multicast_ip':
          cmp = a.multicast_ip.localeCompare(b.multicast_ip)
          break
        case 'status':
          cmp = a.status.localeCompare(b.status)
          break
        case 'publishers':
          cmp = a.publisher_count - b.publisher_count
          break
        case 'subscribers':
          cmp = a.subscriber_count - b.subscriber_count
          break
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
    return sorted
  }, [filteredGroups, sortField, sortDirection])

  const pagedGroups = useMemo(
    () => sortedGroups.slice(offset, offset + PAGE_SIZE),
    [sortedGroups, offset]
  )

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

  const prevFilterRef = useRef(activeFilterRaw)
  useEffect(() => {
    if (prevFilterRef.current === activeFilterRaw) return
    prevFilterRef.current = activeFilterRaw
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('page')
      return newParams
    })
  }, [activeFilterRaw, setSearchParams])

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
          count={groups?.length || 0}
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
                entity="multicast groups"
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
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('multicast_ip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('multicast_ip')}>
                      Multicast IP
                      <SortIcon field="multicast_ip" />
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
                {pagedGroups.map((group) => (
                  <tr
                    key={group.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/multicast-groups/${group.pk}`, navigate)}
                  >
                    <td className="px-4 py-3">
                      <span className="font-mono text-sm">{group.code}</span>
                    </td>
                    <td className="px-4 py-3 text-sm font-mono text-muted-foreground">
                      {group.multicast_ip}
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
                {sortedGroups.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-4 py-8 text-center text-muted-foreground">
                      No multicast groups found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {groups && (
            <Pagination
              total={sortedGroups.length}
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
