import { useEffect, useMemo, useState, useCallback, useRef } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { Loader2, Building2, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { fetchLocations, fetchPeeringDBFacility } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'

const PAGE_SIZE = 100

type SortField = 'code' | 'name' | 'country' | 'loc_id' | 'metro' | 'devices' | 'users'
type SortDirection = 'asc' | 'desc'

function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

const validFilterFields = ['code', 'name', 'country', 'status', 'loc_id', 'metro', 'devices', 'users']

const locationFieldPrefixes = [
  { prefix: 'code:', description: 'Filter by location code' },
  { prefix: 'name:', description: 'Filter by location name' },
  { prefix: 'country:', description: 'Filter by country code (e.g., US)' },
  { prefix: 'status:', description: 'Filter by status (activated, pending, suspended)' },
  { prefix: 'loc_id:', description: 'Filter by PeeringDB location ID' },
  { prefix: 'metro:', description: 'Filter by metro code' },
  { prefix: 'devices:', description: 'Filter by device count (e.g., >0)' },
  { prefix: 'users:', description: 'Filter by user count' },
]

const locationAutocompleteFields: string[] = []

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

function PeeringDBCell({ locId }: { locId: number }) {
  const { data, isLoading } = useQuery({
    queryKey: ['peeringdb', locId],
    queryFn: () => fetchPeeringDBFacility(locId),
    staleTime: 1000 * 60 * 60,
    enabled: locId > 0,
  })

  if (locId === 0) return <span className="text-muted-foreground">—</span>
  if (isLoading) return <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />

  return (
    <div className="flex items-center gap-2">
      {data?.logoUrl && (
        <img
          src={data.logoUrl}
          alt=""
          className="h-6 w-auto object-contain flex-shrink-0"
          onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
        />
      )}
      <div className="min-w-0">
        <div className="text-sm truncate">{data?.orgName || '—'}</div>
        {data?.aka && (
          <div className="text-xs text-muted-foreground truncate">{data.aka}</div>
        )}
      </div>
    </div>
  )
}

export function LocationsPage() {
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
    queryKey: ['locations', offset, sortField, sortDirection, filterKey],
    queryFn: () => fetchLocations(PAGE_SIZE, offset, sortField, sortDirection, filterParams.length > 0 ? filterParams : undefined),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })
  const locations = response?.items ?? []

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
    if (sortField !== field) return 'none' as const
    return sortDirection === 'asc' ? 'ascending' as const : 'descending' as const
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
          <div className="text-lg font-medium mb-2">Unable to load locations</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Building2}
          title="Locations"
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
                fieldPrefixes={locationFieldPrefixes}
                entity="locations"
                autocompleteFields={locationAutocompleteFields}
                placeholder="Filter locations..."
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
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('name')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('name')}>
                      Name
                      <SortIcon field="name" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('country')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('country')}>
                      Country
                      <SortIcon field="country" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('metro')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('metro')}>
                      Metro
                      <SortIcon field="metro" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('devices')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('devices')}>
                      Devices
                      <SortIcon field="devices" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('users')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('users')}>
                      Users
                      <SortIcon field="users" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right">Unicast Avail.</th>
                  <th className="px-4 py-3 font-medium text-right">Subs. Avail.</th>
                  <th className="px-4 py-3 font-medium text-right">Pubs. Avail.</th>
                  <th className="px-4 py-3 font-medium">Organization</th>
                </tr>
              </thead>
              <tbody>
                {locations.map((location) => (
                  <tr
                    key={location.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/locations/${encodeURIComponent(location.pk)}`, navigate)}
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={location.code} className="font-mono text-sm" />
                    </td>
                    <td className="px-4 py-3 text-sm max-w-xs truncate">
                      {location.name || '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {location.country || <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {location.metro_pk
                        ? <Link to={`/dz/metros/${location.metro_pk}`} className="font-mono text-foreground/85 hover:text-foreground hover:underline" onClick={e => e.stopPropagation()}>{location.metro_code}</Link>
                        : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {location.device_count > 0 ? location.device_count : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right relative">
                      {location.max_users > 0 && <div className="absolute inset-y-0 left-0 right-0 pointer-events-none bg-muted/30 border-r border-muted-foreground/20" />}
                      {(() => {
                        const pct = location.max_users > 0 ? Math.min(100, (location.user_count / location.max_users) * 100) : 0
                        const fillColor = pct >= 90 ? 'bg-red-500/25' : pct >= 70 ? 'bg-amber-500/20' : 'bg-blue-500/15'
                        return pct > 0 ? <div className={`absolute inset-y-0 left-0 pointer-events-none ${fillColor}`} style={{ width: `${pct}%` }} /> : null
                      })()}
                      <span className="relative">
                        {location.user_count > 0 || location.max_users > 0 ? (
                          <>{location.user_count}{location.max_users > 0 && <span className="text-muted-foreground">/{location.max_users}</span>}</>
                        ) : <span className="text-muted-foreground">—</span>}
                      </span>
                    </td>
                    {[
                      { count: location.unicast_users_count, max: location.max_unicast_users },
                      { count: location.multicast_subscribers_count, max: location.max_multicast_subscribers },
                      { count: location.multicast_publishers_count, max: location.max_multicast_publishers },
                    ].map(({ count, max }, i) => {
                      const available = max > count ? max - count : 0
                      return (
                        <td key={i} className="px-4 py-3 text-sm tabular-nums text-right">
                          {count === 0 && max === 0
                            ? <span className="text-muted-foreground">—</span>
                            : <span>{available}</span>
                          }
                        </td>
                      )
                    })}
                    <td className="px-4 py-3 max-w-sm" onClick={e => e.stopPropagation()}>
                      <PeeringDBCell locId={location.loc_id} />
                    </td>
                  </tr>
                ))}
                {locations.length === 0 && (
                  <tr>
                    <td colSpan={10} className="px-4 py-8 text-center text-muted-foreground">
                      No locations found
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
