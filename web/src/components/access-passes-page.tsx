import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { AlertCircle, ChevronDown, ChevronUp, KeyRound, Loader2, X } from 'lucide-react'
import { fetchAccessPasses } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'

const PAGE_SIZE = 100

type SortField = 'type' | 'status' | 'connections' | 'client_ip'
type SortDirection = 'asc' | 'desc'

const TYPE_TAG_COLORS: Record<string, string> = {
  prepaid: 'bg-zinc-500/10 text-zinc-600 dark:text-zinc-400 border-zinc-500/20',
  solana_validator: 'bg-violet-500/10 text-violet-600 dark:text-violet-400 border-violet-500/20',
  solana_rpc: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
  others: 'bg-yellow-500/10 text-yellow-600 dark:text-yellow-400 border-yellow-500/20',
  edge_seat: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
}

function TypeTagBadge({ tag }: { tag: string }) {
  const colors = TYPE_TAG_COLORS[tag] ?? 'bg-muted text-muted-foreground border-border'
  const label = tag.replace(/_/g, ' ')
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${colors}`}>
      {label}
    </span>
  )
}

const validFilterFields = ['type', 'status', 'owner', 'client_ip', 'user_payer', 'pub_group', 'sub_group']

const fieldPrefixes = [
  { prefix: 'type:', description: 'Filter by type (e.g., solana_validator)' },
  { prefix: 'status:', description: 'Filter by status (e.g., connected)' },
  { prefix: 'owner:', description: 'Filter by owner pubkey' },
  { prefix: 'client_ip:', description: 'Filter by client IP' },
  { prefix: 'user_payer:', description: 'Filter by user payer pubkey' },
  { prefix: 'pub_group:', description: 'Filter by publisher multicast group code or PK' },
  { prefix: 'sub_group:', description: 'Filter by subscriber multicast group code or PK' },
]

const autocompleteFields = ['type', 'status']

function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

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

export function AccessPassesPage() {
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

  const sortField = (searchParams.get('sort') || 'type') as SortField
  const sortDirection = (searchParams.get('dir') || 'asc') as SortDirection

  const searchParam = searchParams.get('search') || ''
  const searchFilters = parseSearchFilters(searchParam)

  const allFilters = liveFilter ? [...searchFilters, liveFilter] : searchFilters

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
    queryKey: ['access-passes', offset, sortField, sortDirection, filterKey],
    queryFn: () => fetchAccessPasses(PAGE_SIZE, offset, sortField, sortDirection, filterParams.length > 0 ? filterParams : undefined),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })
  const passes = response?.items ?? []

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
    return sortDirection === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
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
          <div className="text-lg font-medium mb-2">Unable to load access passes</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={KeyRound}
          title="Access Passes"
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
                entity="access_passes"
                autocompleteFields={autocompleteFields}
                placeholder="Filter access passes..."
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
                  <th className="px-4 py-3 font-medium">Owner</th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('type')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('type')}>
                      Type
                      <SortIcon field="type" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('status')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('status')}>
                      Status
                      <SortIcon field="status" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('client_ip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('client_ip')}>
                      Client IP
                      <SortIcon field="client_ip" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('connections')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('connections')}>
                      Connections
                      <SortIcon field="connections" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium">Associated</th>
                  <th className="px-4 py-3 font-medium">Multicast</th>
                </tr>
              </thead>
              <tbody>
                {passes.map((ap) => (
                  <tr
                    key={ap.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/access-passes/${ap.pk}`, navigate)}
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <span className="font-mono text-sm text-muted-foreground">
                        {ap.owner_pubkey ? `${ap.owner_pubkey.slice(0, 6)}…${ap.owner_pubkey.slice(-4)}` : '—'}
                      </span>
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <TypeTagBadge tag={ap.type_tag} />
                    </td>
                    <td className="px-4 py-3 text-sm capitalize">
                      {ap.status}
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <span className="font-mono text-sm">{ap.client_ip || <span className="text-muted-foreground">—</span>}</span>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {ap.connection_count > 0 ? ap.connection_count : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      {ap.associated_pubkey ? (
                        <span className="font-mono text-sm text-muted-foreground">
                          {`${ap.associated_pubkey.slice(0, 6)}…${ap.associated_pubkey.slice(-4)}`}
                        </span>
                      ) : (
                        <span className="text-muted-foreground text-sm">—</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      {ap.first_pub_code || ap.first_sub_code ? (
                        <span className="inline-flex items-center gap-1 flex-wrap">
                          {ap.first_pub_code && (
                            <span className="inline-flex items-center px-1.5 py-0.5 rounded border font-mono text-xs bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20">
                              P:{ap.first_pub_code}
                            </span>
                          )}
                          {ap.first_sub_code && (
                            <span className="inline-flex items-center px-1.5 py-0.5 rounded border font-mono text-xs bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/20">
                              S:{ap.first_sub_code}
                            </span>
                          )}
                        </span>
                      ) : (
                        <span className="text-muted-foreground text-sm">—</span>
                      )}
                    </td>
                  </tr>
                ))}
                {passes.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
                      No access passes found
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
