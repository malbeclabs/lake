import { useEffect, useMemo, useState, useCallback, useRef } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Loader2, Link2, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import { fetchLinks } from '@/lib/api'
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

function formatLatency(us: number): string {
  if (us === 0) return '—'
  if (us >= 1000) return `${(us / 1000).toFixed(1)} ms`
  return `${us.toFixed(0)} µs`
}

function formatPercent(pct: number): string {
  if (pct === 0) return '—'
  return `${pct.toFixed(1)}%`
}

function getUtilizationColor(pct: number): string {
  if (pct >= 80) return 'text-red-600 dark:text-red-400'
  if (pct >= 60) return 'text-amber-600 dark:text-amber-400'
  if (pct > 0) return 'text-green-600 dark:text-green-400'
  return 'text-muted-foreground'
}

type SortField =
  | 'code'
  | 'type'
  | 'contributor'
  | 'sidea'
  | 'sidez'
  | 'status'
  | 'bandwidth'
  | 'in'
  | 'out'
  | 'utilin'
  | 'utilout'
  | 'latency'
  | 'jitter'
  | 'loss'

type SortDirection = 'asc' | 'desc'

// Parse search filters from URL param
function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

// Valid filter fields for links
const validFilterFields = ['code', 'type', 'contributor', 'sidea', 'sidez', 'status', 'bandwidth', 'in', 'out', 'utilin', 'utilout', 'latency', 'jitter', 'loss']

// Field prefixes for inline filter
const linkFieldPrefixes = [
  { prefix: 'code:', description: 'Filter by link code' },
  { prefix: 'type:', description: 'Filter by link type' },
  { prefix: 'contributor:', description: 'Filter by contributor' },
  { prefix: 'sidea:', description: 'Filter by side A device' },
  { prefix: 'sidez:', description: 'Filter by side Z device' },
  { prefix: 'status:', description: 'Filter by status' },
  { prefix: 'bandwidth:', description: 'Filter by bandwidth (e.g., >10gbps)' },
  { prefix: 'in:', description: 'Filter by inbound traffic (e.g., >1gbps)' },
  { prefix: 'out:', description: 'Filter by outbound traffic (e.g., >1gbps)' },
  { prefix: 'utilin:', description: 'Filter by inbound utilization % (e.g., >50)' },
  { prefix: 'utilout:', description: 'Filter by outbound utilization % (e.g., >50)' },
]

// Fields that support autocomplete
const linkAutocompleteFields = ['status', 'type', 'contributor', 'sidea', 'sidez']

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

export function LinksPage() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [liveFilter, setLiveFilter] = useState('')

  // Derive pagination from URL
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

  // Get sort config from URL (default: code asc)
  const sortField = (searchParams.get('sort') || 'code') as SortField
  const sortDirection = (searchParams.get('dir') || 'asc') as SortDirection

  // Get search filters from URL
  const searchParam = searchParams.get('search') || ''
  const searchFilters = parseSearchFilters(searchParam)

  // Combine committed filters with live filter
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
    queryKey: ['links', offset, sortField, sortDirection, filterKey],
    queryFn: () => fetchLinks(PAGE_SIZE, offset, sortField, sortDirection, filterParams.length > 0 ? filterParams : undefined),
    refetchInterval: 30000,
    placeholderData: keepPreviousData,
  })
  const links = response?.items ?? []

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
          <div className="text-lg font-medium mb-2">Unable to load links</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1800px] mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Link2}
          title="Links"
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
                fieldPrefixes={linkFieldPrefixes}
                entity="links"
                autocompleteFields={linkAutocompleteFields}
                placeholder="Filter links..."
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
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('code')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('code')}>
                      Code
                      <SortIcon field="code" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('type')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('type')}>
                      Type
                      <SortIcon field="type" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('contributor')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('contributor')}>
                      Contributor
                      <SortIcon field="contributor" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('sidea')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('sidea')}>
                      Side A
                      <SortIcon field="sidea" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('sidez')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('sidez')}>
                      Side Z
                      <SortIcon field="sidez" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium" aria-sort={sortAria('status')}>
                    <button className="inline-flex items-center gap-1" type="button" onClick={() => handleSort('status')}>
                      Status
                      <SortIcon field="status" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('bandwidth')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('bandwidth')}>
                      Bandwidth
                      <SortIcon field="bandwidth" />
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
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('utilin')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('utilin')}>
                      Util In
                      <SortIcon field="utilin" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('utilout')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('utilout')}>
                      Util Out
                      <SortIcon field="utilout" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('latency')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('latency')}>
                      Latency
                      <SortIcon field="latency" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('jitter')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('jitter')}>
                      Jitter
                      <SortIcon field="jitter" />
                    </button>
                  </th>
                  <th className="px-4 py-3 font-medium text-right" aria-sort={sortAria('loss')}>
                    <button className="inline-flex items-center gap-1 justify-end w-full" type="button" onClick={() => handleSort('loss')}>
                      Loss
                      <SortIcon field="loss" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {links.map((link) => (
                  <tr
                    key={link.pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
                    onClick={(e) => handleRowClick(e, `/dz/links/${link.pk}`, navigate)}
                  >
                    <td className="px-4 py-3 whitespace-nowrap">
                      <CopyableText text={link.code} className="font-mono text-sm" />
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground">
                      {link.link_type}
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground">
                      {link.contributor_code || '—'}
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground whitespace-nowrap">
                      {link.side_a_code ? <CopyableText text={link.side_a_code} className="font-mono" /> : <span className="font-mono">—</span>}
                      {link.side_a_metro && (
                        <span className="ml-1 text-xs">({link.side_a_metro})</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground whitespace-nowrap">
                      {link.side_z_code ? <CopyableText text={link.side_z_code} className="font-mono" /> : <span className="font-mono">—</span>}
                      {link.side_z_metro && (
                        <span className="ml-1 text-xs">({link.side_z_metro})</span>
                      )}
                    </td>
                    <td className={`px-4 py-3 text-sm capitalize ${statusColors[link.status] || ''}`}>
                      {link.status}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatBps(link.bandwidth_bps)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatBps(link.in_bps)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatBps(link.out_bps)}
                    </td>
                    <td className={`px-4 py-3 text-sm tabular-nums text-right ${getUtilizationColor(link.utilization_in)}`}>
                      {formatPercent(link.utilization_in)}
                    </td>
                    <td className={`px-4 py-3 text-sm tabular-nums text-right ${getUtilizationColor(link.utilization_out)}`}>
                      {formatPercent(link.utilization_out)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatLatency(link.latency_us)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {formatLatency(link.jitter_us)}
                    </td>
                    <td className={`px-4 py-3 text-sm tabular-nums text-right ${link.loss_percent > 0 ? 'text-red-600 dark:text-red-400' : 'text-muted-foreground'}`}>
                      {formatPercent(link.loss_percent)}
                    </td>
                  </tr>
                ))}
                {links.length === 0 && (
                  <tr>
                    <td colSpan={14} className="px-4 py-8 text-center text-muted-foreground">
                      No links found
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
