import { useEffect, useState, useCallback, useMemo, useRef } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Loader2, Users, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { Link } from 'react-router-dom'
import { fetchUsers } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'

const PAGE_SIZE = 100

const statusColors: Record<string, string> = {
  activated: 'text-muted-foreground',
  provisioning: 'text-blue-600 dark:text-blue-400',
  'soft-drained': 'text-amber-600 dark:text-amber-400',
  drained: 'text-amber-600 dark:text-amber-400',
  suspended: 'text-red-600 dark:text-red-400',
  pending: 'text-amber-600 dark:text-amber-400',
}

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function truncatePubkey(pubkey: string): string {
  if (!pubkey || pubkey.length <= 12) return pubkey || '—'
  return `${pubkey.slice(0, 6)}...${pubkey.slice(-4)}`
}

type SortField = 'owner' | 'kind' | 'dzip' | 'clientip' | 'device' | 'metro' | 'tenant' | 'status' | 'in' | 'out'
type SortDirection = 'asc' | 'desc'

// Valid filter field names as accepted by the API
const validFilterFields = ['owner', 'kind', 'dzip', 'clientip', 'device', 'metro', 'tenant', 'status', 'in', 'out']

const userFieldPrefixes = [
  { prefix: 'owner:', description: 'Filter by owner pubkey' },
  { prefix: 'kind:', description: 'Filter by user kind' },
  { prefix: 'clientip:', description: 'Filter by client IP' },
  { prefix: 'dzip:', description: 'Filter by DZ IP' },
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'metro:', description: 'Filter by metro' },
  { prefix: 'tenant:', description: 'Filter by tenant code' },
  { prefix: 'status:', description: 'Filter by status' },
  { prefix: 'in:', description: 'Filter by inbound traffic (e.g., >1gbps)' },
  { prefix: 'out:', description: 'Filter by outbound traffic (e.g., >1gbps)' },
]

const userAutocompleteFields: (string | { field: string; minChars: number })[] = ['status', 'kind', 'metro', { field: 'device', minChars: 2 }]

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

export function UsersPage() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [liveFilter, setLiveFilter] = useState('')

  const showDeleted = searchParams.get('deleted') === '1'
  const toggleDeleted = useCallback(() => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      if (showDeleted) { newParams.delete('deleted') } else { newParams.set('deleted', '1') }
      newParams.delete('page')
      return newParams
    })
  }, [showDeleted, setSearchParams])

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

  const sortField = (searchParams.get('sort') || 'owner') as SortField
  const sortDirection = (searchParams.get('dir') || 'asc') as SortDirection

  const searchParam = searchParams.get('search') || ''
  const searchFilters = parseSearchFilters(searchParam)

  const allFilters = liveFilter ? [...searchFilters, liveFilter] : searchFilters
  const filterParams = useMemo(() => allFilters.map(toFilterParam), [allFilters])
  const filterKey = filterParams.join(',')

  const { data: response, isLoading, error } = useQuery({
    queryKey: ['users', offset, sortField, sortDirection, filterKey, showDeleted],
    queryFn: () => fetchUsers(
      PAGE_SIZE,
      offset,
      sortField,
      sortDirection,
      filterParams.length > 0 ? filterParams : undefined,
      showDeleted
    ),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })

  const users = response?.items ?? []

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

  const handleSort = (field: SortField) => {
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      if (sortField === field) {
        newParams.set('dir', sortDirection === 'asc' ? 'desc' : 'asc')
      } else {
        newParams.set('sort', field)
        newParams.set('dir', 'asc')
      }
      newParams.delete('page')
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

  // Reset to first page when filter changes
  const prevFilterRef = useRef(filterKey)
  useEffect(() => {
    if (prevFilterRef.current === filterKey) return
    prevFilterRef.current = filterKey
    setSearchParams(prev => {
      const newParams = new URLSearchParams(prev)
      newParams.delete('page')
      return newParams
    })
  }, [filterKey, setSearchParams])

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
          <div className="text-lg font-medium mb-2">Unable to load users</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1400px] mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Users}
          title="Users"
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
              <button
                onClick={toggleDeleted}
                className={`text-xs px-2 py-1 rounded-md border transition-colors ${
                  showDeleted
                    ? 'bg-gray-500/15 text-gray-500 border-gray-500/30 hover:bg-gray-500/25'
                    : 'border-border text-muted-foreground hover:text-foreground hover:bg-muted'
                }`}
              >
                {showDeleted ? 'Hide deleted' : 'Show deleted'}
              </button>
              <InlineFilter
                fieldPrefixes={userFieldPrefixes}
                entity="users"
                autocompleteFields={userAutocompleteFields}
                placeholder="Filter users..."
                onLiveFilterChange={setLiveFilter}
              />
            </>
          }
        />

        {/* Table */}
        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('owner')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('owner')}>
                      Owner
                      <SortIcon field="owner" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('kind')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('kind')}>
                      Kind
                      <SortIcon field="kind" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('clientip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('clientip')}>
                      Client IP
                      <SortIcon field="clientip" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('dzip')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('dzip')}>
                      DZ IP
                      <SortIcon field="dzip" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('device')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('device')}>
                      Device
                      <SortIcon field="device" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('metro')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('metro')}>
                      Metro
                      <SortIcon field="metro" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('tenant')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('tenant')}>
                      Tenant
                      <SortIcon field="tenant" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('status')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('status')}>
                      Status
                      <SortIcon field="status" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('in')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('in')}>
                      In
                      <SortIcon field="in" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('out')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('out')}>
                      Out
                      <SortIcon field="out" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {users.map((user) => (
                  <tr
                    key={user.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/users/${user.pk}`, navigate)}
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={user.owner_pubkey} className="font-mono text-sm">
                        <span title={user.owner_pubkey}>{truncatePubkey(user.owner_pubkey)}</span>
                      </CopyableText>
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground">
                      {user.kind || '—'}
                    </td>
                    <td className="px-4 py-3 text-sm whitespace-nowrap">
                      {user.client_ip ? <CopyableText text={user.client_ip} className="font-mono" /> : <span className="font-mono text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm whitespace-nowrap">
                      {user.dz_ip ? <CopyableText text={user.dz_ip} className="font-mono" /> : <span className="font-mono text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm whitespace-nowrap">
                      {user.device_code ? <CopyableText text={user.device_code} className="font-mono" /> : <span className="font-mono text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground">
                      {user.metro_name || user.metro_code || '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {user.tenant_code
                        ? <Link to={`/dz/tenants/${user.tenant_pk}`} className="font-mono hover:underline" onClick={e => e.stopPropagation()}>{user.tenant_code}</Link>
                        : <span className="text-muted-foreground">—</span>
                      }
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {user.is_deleted
                        ? <span className="text-xs px-1.5 py-0.5 rounded font-medium bg-gray-500/15 text-gray-500">Deleted</span>
                        : <span className={`capitalize ${statusColors[user.status] || ''}`}>{user.status}</span>
                      }
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatBps(user.in_bps)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatBps(user.out_bps)}
                    </td>
                  </tr>
                ))}
                {users.length === 0 && (
                  <tr>
                    <td colSpan={10} className="px-4 py-8 text-center text-muted-foreground">
                      No users found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {response && (
            <Pagination
              total={response.total}
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
