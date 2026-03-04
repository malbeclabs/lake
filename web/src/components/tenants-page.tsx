import { useEffect, useMemo, useState, useCallback, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Loader2, Layers, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { fetchAllPaginated, fetchTenants } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'

const PAGE_SIZE = 100

type SortField = 'code' | 'vrf_id'
type SortDirection = 'asc' | 'desc'

function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

const validFilterFields = ['code']

const tenantFieldPrefixes = [
  { prefix: 'code:', description: 'Filter by tenant code' },
]

const tenantAutocompleteFields: string[] = []

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

function BoolBadge({ value }: { value: boolean }) {
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
      value
        ? 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20'
        : 'text-muted-foreground'
    }`}>
      {value ? 'yes' : 'no'}
    </span>
  )
}

function truncatePk(pk: string): string {
  if (pk.length <= 12) return pk
  return `${pk.slice(0, 6)}…${pk.slice(-4)}`
}

export function TenantsPage() {
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
      if (newFilters.length === 0) { newParams.delete('search') } else { newParams.set('search', newFilters.join(',')) }
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

  const { data: response, isLoading, error } = useQuery({
    queryKey: ['tenants', 'all'],
    queryFn: () => fetchAllPaginated(fetchTenants, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const tenants = response?.items

  const filteredTenants = useMemo(() => {
    if (!tenants) return []
    if (!activeFilterRaw) return tenants

    const filter = parseFilter(activeFilterRaw)
    const needle = filter.value.trim().toLowerCase()
    if (!needle) return tenants

    if (filter.field === 'code') {
      return tenants.filter(t => t.code.toLowerCase().includes(needle))
    }
    // all fields
    return tenants.filter(t =>
      t.code.toLowerCase().includes(needle) ||
      t.pk.toLowerCase().includes(needle)
    )
  }, [tenants, activeFilterRaw])

  const sortedTenants = useMemo(() => {
    if (!filteredTenants) return []
    const seen = new Set<string>()
    const unique = filteredTenants.filter(t => {
      if (seen.has(t.pk)) return false
      seen.add(t.pk)
      return true
    })
    return [...unique].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'code':
          cmp = a.code.localeCompare(b.code)
          break
        case 'vrf_id':
          cmp = a.vrf_id - b.vrf_id
          break
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
  }, [filteredTenants, sortField, sortDirection])

  const pagedTenants = useMemo(
    () => sortedTenants.slice(offset, offset + PAGE_SIZE),
    [sortedTenants, offset]
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
          <div className="text-lg font-medium mb-2">Unable to load tenants</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Layers}
          title="Tenants"
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
                fieldPrefixes={tenantFieldPrefixes}
                entity="tenants"
                autocompleteFields={tenantAutocompleteFields}
                placeholder="Filter tenants..."
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
                  <th className="px-4 py-3 font-medium">PK</th>
                  <th className="px-4 py-3 font-medium">Owner</th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('vrf_id')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('vrf_id')}>
                      VRF ID
                      <SortIcon field="vrf_id" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-center">Metro Routing</th>
                  <th className="px-4 py-3 font-medium text-center">Route Liveness</th>
                </tr>
              </thead>
              <tbody>
                {pagedTenants.map((tenant) => (
                  <tr
                    key={tenant.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/tenants/${tenant.pk}`, navigate)}
                  >
                    <td className="px-4 py-3">
                      <span className="font-mono text-sm">{tenant.code || '—'}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="font-mono text-xs text-muted-foreground" title={tenant.pk}>
                        {truncatePk(tenant.pk)}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="font-mono text-xs text-muted-foreground" title={tenant.owner_pubkey}>
                        {truncatePk(tenant.owner_pubkey)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {tenant.vrf_id}
                    </td>
                    <td className="px-4 py-3 text-center">
                      <BoolBadge value={tenant.metro_routing} />
                    </td>
                    <td className="px-4 py-3 text-center">
                      <BoolBadge value={tenant.route_liveness} />
                    </td>
                  </tr>
                ))}
                {sortedTenants.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                      No tenants found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {response && (
            <Pagination
              total={sortedTenants.length}
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
