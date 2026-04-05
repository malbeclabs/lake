import { useMemo, useCallback, useState, useRef, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams, useNavigate, Link } from 'react-router-dom'
import { Loader2, Puzzle, AlertCircle, ChevronDown, ChevronUp, ChevronRight, X, ExternalLink, Filter, Copy, Check, RefreshCw, Info } from 'lucide-react'
import {
  fetchShredClientSeats,
  fetchShredDevices,
  fetchShredFunders,
  fetchShredEscrowEvents,
  fetchShredsOverview,
  type ShredClientSeat,
  type ShredFunder,
} from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { Pagination } from './pagination'
import { InlineFilter } from './inline-filter'
import { PageHeader } from './page-header'

const PAGE_SIZE = 100

function truncatePK(pk: string) {
  if (pk.length <= 12) return pk
  return pk.slice(0, 6) + '...' + pk.slice(-4)
}

// --- Shared helpers ---

type SortDirection = 'asc' | 'desc'

type NumericFilter = { op: '>' | '>=' | '<' | '<=' | '='; value: number }

function parseNumericFilter(input: string): NumericFilter | null {
  const match = input.trim().match(/^(>=|<=|>|<|==|=)\s*(-?\d+(?:\.\d+)?)$/)
  if (!match) return null
  const op = match[1] === '==' ? '=' : (match[1] as NumericFilter['op'])
  return { op, value: Number(match[2]) }
}

function matchesNumericFilter(value: number, filter: NumericFilter): boolean {
  switch (filter.op) {
    case '>': return value > filter.value
    case '>=': return value >= filter.value
    case '<': return value < filter.value
    case '<=': return value <= filter.value
    case '=': return value === filter.value
  }
}

function parseSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

function parseFilter(filter: string, validFields: string[]): { field: string; value: string } {
  const colonIndex = filter.indexOf(':')
  if (colonIndex > 0) {
    const field = filter.slice(0, colonIndex).toLowerCase()
    const value = filter.slice(colonIndex + 1)
    if (validFields.includes(field) && value) {
      return { field, value }
    }
  }
  return { field: 'all', value: filter }
}

function usePageState() {
  const [searchParams, setSearchParams] = useSearchParams()
  const page = parseInt(searchParams.get('page') || '1')
  const offset = (page - 1) * PAGE_SIZE
  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
      if (newPage <= 1) { p.delete('page') } else { p.set('page', String(newPage)) }
      return p
    })
  }, [setSearchParams])

  const sortField = searchParams.get('sort') || ''
  const sortDirection = (searchParams.get('dir') || 'desc') as SortDirection
  const handleSort = useCallback((field: string) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (prev.get('sort') === field) { p.set('dir', prev.get('dir') === 'asc' ? 'desc' : 'asc') }
      else { p.set('sort', field); p.set('dir', 'desc') }
      return p
    })
  }, [setSearchParams])

  const searchParam = searchParams.get('search') || ''
  const searchFilters = parseSearchFilters(searchParam)
  const [liveFilter, setLiveFilter] = useState('')

  const removeFilter = useCallback((filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (newFilters.length === 0) { p.delete('search') } else { p.set('search', newFilters.join(',')) }
      return p
    })
  }, [searchFilters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete('search')
      return p
    })
  }, [setSearchParams])

  // Reset page on filter change
  const allFilters = liveFilter ? [...searchFilters, liveFilter] : searchFilters
  const prevFilterRef = useRef(JSON.stringify(allFilters))
  useEffect(() => {
    const key = JSON.stringify(allFilters)
    if (prevFilterRef.current === key) return
    prevFilterRef.current = key
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete('page')
      return p
    })
  }, [allFilters, setSearchParams])

  return {
    searchParams, setSearchParams,
    offset, setOffset,
    sortField, sortDirection, handleSort,
    searchFilters, liveFilter, setLiveFilter, allFilters,
    removeFilter, clearAllFilters,
  }
}

function SortHeader({ field, label, align, currentSort, currentDir, onSort }: {
  field: string; label: string; align?: 'right'; currentSort: string; currentDir: SortDirection; onSort: (f: string) => void
}) {
  return (
    <th className={`px-4 py-3 font-medium ${align === 'right' ? 'text-right' : ''}`}>
      <button
        className={`inline-flex items-center gap-1 ${align === 'right' ? 'justify-end w-full' : ''}`}
        onClick={() => onSort(field)}
      >
        {label}
        {currentSort === field && (currentDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
      </button>
    </th>
  )
}

function FilterActions({ searchFilters, removeFilter, clearAllFilters, setLiveFilter, fieldPrefixes, entity, placeholder, autocompleteFields = [] }: {
  searchFilters: string[]; removeFilter: (f: string) => void; clearAllFilters: () => void
  setLiveFilter: (f: string) => void
  fieldPrefixes: { prefix: string; description: string }[]; entity: string; placeholder: string
  autocompleteFields?: (string | { field: string; minChars: number })[]
}) {
  return (
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
        <button onClick={clearAllFilters} className="text-xs text-muted-foreground hover:text-foreground transition-colors">
          Clear all
        </button>
      )}
      <InlineFilter
        fieldPrefixes={fieldPrefixes}
        entity={entity}
        autocompleteFields={autocompleteFields}
        placeholder={placeholder}
        onLiveFilterChange={setLiveFilter}
      />
    </>
  )
}

function CopyIcon({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 2000) }}
      className="inline-flex items-center justify-center h-4 w-4 text-muted-foreground opacity-0 group-hover/cell:opacity-100 hover:text-foreground transition-opacity cursor-pointer"
      title={copied ? 'Copied!' : 'Copy'}
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
    </button>
  )
}

function LoadingState() {
  return (
    <div className="flex-1 flex items-center justify-center">
      <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
    </div>
  )
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="flex-1 flex items-center justify-center">
      <div className="text-center">
        <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
        <div className="text-lg font-medium mb-2">Unable to load data</div>
        <div className="text-sm text-muted-foreground">{message}</div>
      </div>
    </div>
  )
}

// --- Seats Page ---

const seatFieldPrefixes = [
  { prefix: 'seat:', description: 'Filter by seat key' },
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'metro:', description: 'Filter by metro code' },
  { prefix: 'ip:', description: 'Filter by client IP' },
  { prefix: 'funder:', description: 'Filter by funder key' },
  { prefix: 'tenure:', description: 'Filter by tenure (e.g., >1)' },
  { prefix: 'epoch:', description: 'Filter by active epoch (e.g., =950)' },
  { prefix: 'balance:', description: 'Filter by USDC balance (e.g., >0)' },
  { prefix: 'prepaid:', description: 'Filter by prepaid epochs (e.g., >5)' },
]

function prepaidEpochs(seat: ShredClientSeat): number {
  if (seat.price_per_epoch_dollars <= 0 || seat.total_usdc_balance === 0) return 0
  return Math.floor((seat.total_usdc_balance / 1e6) / seat.price_per_epoch_dollars)
}

type SeatStatus = 'active' | 'expiring' | 'pending' | 'expired' | 'closed'

function getSeatStatus(seat: ShredClientSeat, currentSolanaEpoch: number): SeatStatus {
  if (seat.escrow_count === 0) return 'closed'
  if (seat.active_epoch < currentSolanaEpoch) {
    return prepaidEpochs(seat) >= 1 ? 'pending' : 'expired'
  }
  const prepaid = prepaidEpochs(seat)
  if (prepaid < 2) return 'expiring'
  return 'active'
}

const seatStatusConfig: Record<SeatStatus, { label: string; className: string }> = {
  active: {
    label: 'Active',
    className: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
  },
  expiring: {
    label: 'Expiring',
    className: 'bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20',
  },
  pending: {
    label: 'Pending',
    className: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
  },
  expired: {
    label: 'Expired',
    className: 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20',
  },
  closed: {
    label: 'Closed',
    className: 'bg-gray-500/10 text-gray-500 dark:text-gray-400 border-gray-500/20',
  },
}

/** Extends a boolean flag to stay true for at least `minMs` after it turns on. */
function useDebouncedFetching(isFetching: boolean, minMs = 800): boolean {
  const [visible, setVisible] = useState(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (isFetching) {
      if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null }
      setVisible(true)
    } else if (visible) {
      timerRef.current = setTimeout(() => setVisible(false), minMs)
    }
    return () => { if (timerRef.current) clearTimeout(timerRef.current) }
  }, [isFetching, visible, minMs])

  return visible
}

function SeatStatusBadge({ status }: { status: SeatStatus }) {
  const config = seatStatusConfig[status]
  return (
    <span className={`inline-flex items-center text-xs px-2 py-0.5 rounded-lg border whitespace-nowrap ${config.className}`}>
      {config.label}
    </span>
  )
}

export function ShredsSeatsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const [showStatusInfo, setShowStatusInfo] = useState(false)

  // Fetch overview for current Solana epoch (used for status badges)
  const { data: overview } = useQuery({
    queryKey: ['shreds-overview'],
    queryFn: fetchShredsOverview,
    refetchInterval: 30_000,
  })
  const currentSolanaEpoch = overview?.current_solana_epoch ?? 0

  // Pagination from URL.
  const page = parseInt(searchParams.get('page') || '1')
  const offset = (page - 1) * PAGE_SIZE

  // Sort from URL.
  const sortBy = searchParams.get('sort') || 'last_activity'
  const sortDir = (searchParams.get('dir') || 'desc') as SortDirection

  // Status filter from URL (comma-separated, default: active,expiring,pending).
  const statusParam = searchParams.get('status') || 'active,expiring,pending'
  const activeStatuses = useMemo(() => new Set(statusParam.split(',').filter(Boolean)), [statusParam])

  const toggleStatus = useCallback((status: string) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const current = new Set((prev.get('status') || 'active,expiring,pending').split(',').filter(Boolean))
      if (current.has(status)) { current.delete(status) } else { current.add(status) }
      const val = Array.from(current).join(',')
      if (val === 'active,expiring,pending') { p.delete('status') } else { p.set('status', val) }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  // Filters from URL.
  const searchParam = searchParams.get('search') || ''
  const searchFilters = useMemo(() => parseSearchFilters(searchParam), [searchParam])

  // Build server-side filter params.
  const serverFilters = useMemo(() => {
    return searchFilters.length > 0 ? searchFilters : undefined
  }, [searchFilters])

  const { data, isLoading, isFetching: rawFetching, error, refetch } = useQuery({
    queryKey: ['shred-client-seats', offset, sortBy, sortDir, statusParam, searchParam],
    queryFn: () => fetchShredClientSeats({
      limit: PAGE_SIZE,
      offset,
      sortBy,
      sortDir,
      status: statusParam,
      filters: serverFilters,
    }),
    placeholderData: keepPreviousData,
    refetchInterval: 30000,
  })
  const isFetching = useDebouncedFetching(rawFetching)

  const items = data?.items ?? []
  const total = data?.total ?? 0

  // URL state setters.
  const handleSort = useCallback((field: string) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (prev.get('sort') === field) { p.set('dir', prev.get('dir') === 'asc' ? 'desc' : 'asc') }
      else { p.set('sort', field); p.set('dir', 'desc') }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
      if (newPage <= 1) { p.delete('page') } else { p.set('page', String(newPage)) }
      return p
    })
  }, [setSearchParams])

  const removeFilter = useCallback((filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (newFilters.length === 0) { p.delete('search') } else { p.set('search', newFilters.join(',')) }
      p.delete('page')
      return p
    })
  }, [searchFilters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete('search')
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  if (isLoading && !data) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="px-4 sm:px-8 py-8">
        <PageHeader
          icon={Puzzle}
          title="Shred Subscribers"
          count={total}
          subtitle={
            <button
              onClick={() => refetch()}
              className="text-muted-foreground hover:text-foreground transition-colors"
              title="Refresh"
            >
              {isFetching
                ? <Loader2 className="h-4 w-4 animate-spin" />
                : <RefreshCw className="h-4 w-4" />}
            </button>
          }
          actions={
            <FilterActions
              searchFilters={searchFilters} removeFilter={removeFilter} clearAllFilters={clearAllFilters}
              setLiveFilter={() => {}}
              fieldPrefixes={seatFieldPrefixes} entity="shred-seats" placeholder="Filter seats..."
              autocompleteFields={['device', 'metro', 'ip', 'funder']}
            />
          }
        />

        <div className="flex items-center gap-2 mb-3">
          {([
            { key: 'active', label: 'Active', onClass: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20', dotClass: 'bg-green-500' },
            { key: 'expiring', label: 'Expiring', onClass: 'bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20', dotClass: 'bg-amber-500' },
            { key: 'pending', label: 'Pending', onClass: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20', dotClass: 'bg-blue-500' },
            { key: 'inactive', label: 'Expired', onClass: 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20', dotClass: 'bg-red-500' },
            { key: 'closed', label: 'Closed', onClass: 'bg-gray-500/10 text-gray-600 dark:text-gray-400 border-gray-500/20', dotClass: 'bg-gray-500' },
          ] as const).map(({ key, label, onClass, dotClass }) => {
            const on = activeStatuses.has(key)
            return (
              <button
                key={key}
                onClick={() => toggleStatus(key)}
                className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border transition-colors ${
                  on ? onClass : 'bg-muted text-muted-foreground border-border opacity-50'
                }`}
              >
                <div className={`h-1.5 w-1.5 rounded-full ${on ? dotClass : 'bg-muted-foreground'}`} />
                {label}
              </button>
            )
          })}
          <button
            onClick={() => setShowStatusInfo(!showStatusInfo)}
            className={`p-1 rounded-md transition-colors ${showStatusInfo ? 'text-foreground bg-muted' : 'text-muted-foreground hover:text-foreground'}`}
            title="Status definitions"
          >
            <Info className="h-3.5 w-3.5" />
          </button>
        </div>
        {showStatusInfo && (
          <div className="mb-3 border border-border rounded-lg bg-muted/50 overflow-hidden">
            <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 px-5 py-4 text-sm">
              <span className="inline-flex items-center gap-1.5 font-medium text-green-600 dark:text-green-400">
                <div className="h-1.5 w-1.5 rounded-full bg-green-500" /> Active
              </span>
              <span className="text-muted-foreground">Allocated for the current epoch with 2+ epochs of prepaid balance</span>
              <span className="inline-flex items-center gap-1.5 font-medium text-amber-600 dark:text-amber-400">
                <div className="h-1.5 w-1.5 rounded-full bg-amber-500" /> Expiring
              </span>
              <span className="text-muted-foreground">Active but less than 2 epochs of balance remaining</span>
              <span className="inline-flex items-center gap-1.5 font-medium text-blue-600 dark:text-blue-400">
                <div className="h-1.5 w-1.5 rounded-full bg-blue-500" /> Pending
              </span>
              <span className="text-muted-foreground">Funded with enough for at least 1 epoch but not yet allocated</span>
              <span className="inline-flex items-center gap-1.5 font-medium text-red-600 dark:text-red-400">
                <div className="h-1.5 w-1.5 rounded-full bg-red-500" /> Expired
              </span>
              <span className="text-muted-foreground">Not active and insufficient balance to renew</span>
              <span className="inline-flex items-center gap-1.5 font-medium text-gray-500 dark:text-gray-400">
                <div className="h-1.5 w-1.5 rounded-full bg-gray-500" /> Closed
              </span>
              <span className="text-muted-foreground">No payment escrow attached</span>
            </div>
          </div>
        )}

        <div className="relative border border-border rounded-lg overflow-hidden bg-card">
          {isFetching && data && (
            <div className="absolute inset-x-0 top-0 h-0.5 overflow-hidden z-10">
              <div className="h-full w-1/3 bg-primary/60 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
            </div>
          )}
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium">Seat</th>
                  <SortHeader field="device" label="Device" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="metro" label="Metro" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="ip" label="Client IP" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <th className="px-4 py-3 font-medium">Status</th>
                  <SortHeader field="tenure" label="Tenure" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="balance" label="Balance (USDC)" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="prepaid" label="Prepaid Epochs" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="last_activity" label="Last Activity" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                </tr>
              </thead>
              <tbody>
                {items.map((seat) => {
                  const status = getSeatStatus(seat, currentSolanaEpoch)
                  return (
                  <tr key={seat.pk} className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors" onClick={(e) => handleRowClick(e, `/dz/shreds/activity?search=seat:${seat.pk}`, navigate)}>
                    <td className="px-4 py-3 font-mono text-xs group/cell" title={seat.pk}>
                      <span className="inline-flex items-center gap-1">
                        {truncatePK(seat.pk)}
                        <CopyIcon text={seat.pk} />
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm whitespace-nowrap group/cell">
                      <span className="inline-flex items-center gap-1">
                        <Link to={`/dz/devices/${seat.device_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={seat.device_key}>
                          {seat.device_code || truncatePK(seat.device_key)}
                        </Link>
                        <CopyIcon text={seat.device_key} />
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {seat.metro_pk ? (
                        <Link to={`/dz/metros/${seat.metro_pk}`} className="text-blue-500 hover:underline font-mono text-xs" title={seat.metro_pk}>
                          {seat.metro_code || truncatePK(seat.metro_pk)}
                        </Link>
                      ) : <span className="text-muted-foreground">{'\u2014'}</span>}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono group/cell">
                      <span className="inline-flex items-center gap-1">
                        {seat.user_pk ? (
                          <Link to={`/dz/users/${seat.user_pk}`} className="text-blue-500 hover:underline" title={seat.user_pk}>
                            {seat.client_ip}
                          </Link>
                        ) : seat.client_ip}
                        <CopyIcon text={seat.client_ip} />
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <SeatStatusBadge status={status} />
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{seat.tenure_epochs}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {`$${(seat.total_usdc_balance / 1e6).toFixed(2)}`}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {prepaidEpochs(seat)}
                    </td>
                    <td className="px-4 py-3">
                      <Link
                        to={`/dz/shreds/activity?search=seat:${seat.pk}`}
                        className="inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors whitespace-nowrap"
                      >
                        {seat.last_activity ? new Date(seat.last_activity).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '\u2014'}
                        <ChevronRight className="h-3 w-3" />
                      </Link>
                    </td>
                  </tr>
                  )
                })}
                {items.length === 0 && (
                  <tr><td colSpan={10} className="px-4 py-8 text-center text-muted-foreground">No subscribers found</td></tr>
                )}
              </tbody>
            </table>
          </div>
          {total > PAGE_SIZE && (
            <Pagination total={total} limit={PAGE_SIZE} offset={offset} onOffsetChange={setOffset} />
          )}
        </div>
      </div>

    </div>
  )
}

// --- Devices Page ---

const deviceFieldPrefixes = [
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'metro:', description: 'Filter by metro code' },
  { prefix: 'price:', description: 'Filter by price/epoch (e.g., >0)' },
  { prefix: 'granted:', description: 'Filter by granted seats (e.g., >0)' },
  { prefix: 'available:', description: 'Filter by available seats (e.g., >10)' },
]

export function ShredsDevicesPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const page = parseInt(searchParams.get('page') || '1')
  const offset = (page - 1) * PAGE_SIZE
  const sortBy = searchParams.get('sort') || 'granted'
  const sortDir = (searchParams.get('dir') || 'desc') as SortDirection
  const searchParam = searchParams.get('search') || ''
  const searchFilters = useMemo(() => parseSearchFilters(searchParam), [searchParam])
  const serverFilters = useMemo(() => searchFilters.length > 0 ? searchFilters : undefined, [searchFilters])

  const { data, isLoading, isFetching: rawFetchingDevices, error, refetch: refetchDevices } = useQuery({
    queryKey: ['shred-devices', offset, sortBy, sortDir, searchParam],
    queryFn: () => fetchShredDevices({
      limit: PAGE_SIZE,
      offset,
      sortBy,
      sortDir,
      filters: serverFilters,
    }),
    placeholderData: keepPreviousData,
    refetchInterval: 30000,
  })
  const isFetchingDevices = useDebouncedFetching(rawFetchingDevices)

  const items = data?.items ?? []
  const total = data?.total ?? 0

  const handleSort = useCallback((field: string) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (prev.get('sort') === field) { p.set('dir', prev.get('dir') === 'asc' ? 'desc' : 'asc') }
      else { p.set('sort', field); p.set('dir', 'desc') }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
      if (newPage <= 1) { p.delete('page') } else { p.set('page', String(newPage)) }
      return p
    })
  }, [setSearchParams])

  const removeFilter = useCallback((filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (newFilters.length === 0) { p.delete('search') } else { p.set('search', newFilters.join(',')) }
      p.delete('page')
      return p
    })
  }, [searchFilters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete('search')
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  if (isLoading && !data) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Puzzle}
          title="Shred Devices"
          count={total}
          subtitle={
            <button onClick={() => refetchDevices()} className="text-muted-foreground hover:text-foreground transition-colors" title="Refresh">
              {isFetchingDevices ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            </button>
          }
          actions={
            <FilterActions
              searchFilters={searchFilters} removeFilter={removeFilter} clearAllFilters={clearAllFilters}
              setLiveFilter={() => {}}
              fieldPrefixes={deviceFieldPrefixes} entity="shred-devices" placeholder="Filter devices..."
              autocompleteFields={['device', 'metro']}
            />
          }
        />

        <div className="relative border border-border rounded-lg overflow-hidden bg-card">
          {isFetchingDevices && data && (
            <div className="absolute inset-x-0 top-0 h-0.5 overflow-hidden z-10">
              <div className="h-full w-1/3 bg-primary/60 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
            </div>
          )}
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <SortHeader field="device" label="Device" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="metro" label="Metro" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="price" label="Price / Epoch" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="granted" label="Granted" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="capacity" label="Capacity" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="available" label="Available" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                </tr>
              </thead>
              <tbody>
                {items.map((d) => (
                  <tr key={d.device_key} className="border-b border-border last:border-b-0 hover:bg-muted transition-colors">
                    <td className="px-4 py-3 text-sm group/cell">
                      <span className="inline-flex items-center gap-1">
                        <Link to={`/dz/devices/${d.device_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={d.device_key}>
                          {d.device_code || truncatePK(d.device_key)}
                        </Link>
                        <CopyIcon text={d.device_key} />
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <Link to={`/dz/metros/${d.metro_exchange_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={d.metro_exchange_key}>
                        {d.metro_code || truncatePK(d.metro_exchange_key)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">${d.total_price_dollars}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{d.granted_seats}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{d.capacity}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {d.available_seats > 0 ? d.available_seats : <span className="text-red-500">0</span>}
                    </td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">No devices found</td></tr>
                )}
              </tbody>
            </table>
          </div>
          {total > PAGE_SIZE && (
            <Pagination total={total} limit={PAGE_SIZE} offset={offset} onOffsetChange={setOffset} />
          )}
        </div>
      </div>
    </div>
  )
}

// --- Funders Page ---

const funderFilterFields = ['funder', 'active', 'inactive', 'closed']
const funderFieldPrefixes = [
  { prefix: 'funder:', description: 'Filter by funder key' },
  { prefix: 'active:', description: 'Filter by active seats (e.g., >0)' },
  { prefix: 'inactive:', description: 'Filter by inactive seats (e.g., >0)' },
  { prefix: 'closed:', description: 'Filter by closed seats (e.g., >0)' },
]

function matchesFunderFilter(f: ShredFunder, filter: string): boolean {
  const { field, value } = parseFilter(filter, funderFilterFields)
  const needle = value.toLowerCase()

  if (field === 'all') {
    return f.funding_authority_key.toLowerCase().includes(needle)
  }

  switch (field) {
    case 'funder': return f.funding_authority_key.toLowerCase().includes(needle)
    case 'active': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(f.active_seats, nf) : false }
    case 'inactive': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(f.inactive_seats, nf) : false }
    case 'closed': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(f.closed_seats, nf) : false }
    default: return true
  }
}

export function ShredsFundersPage() {
  const ps = usePageState()
  const sortField = ps.sortField || 'active'

  const { data, isLoading, isFetching: rawFetchingFunders, error, refetch: refetchFunders } = useQuery({
    queryKey: ['shred-funders'],
    queryFn: fetchShredFunders,
    refetchInterval: 30000,
  })
  const isFetchingFunders = useDebouncedFetching(rawFetchingFunders)

  const filtered = useMemo(() => {
    if (!data) return []
    if (ps.allFilters.length === 0) return data
    const grouped = new Map<string, string[]>()
    for (const f of ps.allFilters) {
      const { field } = parseFilter(f, funderFilterFields)
      grouped.set(field, [...(grouped.get(field) ?? []), f])
    }
    return data.filter(f => Array.from(grouped.values()).every(group => group.some(fl => matchesFunderFilter(f, fl))))
  }, [data, ps.allFilters])

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'funder': cmp = a.funding_authority_key.localeCompare(b.funding_authority_key); break
        case 'active': cmp = a.active_seats - b.active_seats; break
        case 'inactive': cmp = a.inactive_seats - b.inactive_seats; break
        case 'closed': cmp = a.closed_seats - b.closed_seats; break
      }
      return ps.sortDirection === 'asc' ? cmp : -cmp
    })
  }, [filtered, sortField, ps.sortDirection])

  if (isLoading) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Puzzle}
          title="Shred Funders"
          count={sorted.length}
          subtitle={
            <button onClick={() => refetchFunders()} className="text-muted-foreground hover:text-foreground transition-colors" title="Refresh">
              {isFetchingFunders ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            </button>
          }
          actions={
            <FilterActions
              searchFilters={ps.searchFilters} removeFilter={ps.removeFilter} clearAllFilters={ps.clearAllFilters}
              setLiveFilter={ps.setLiveFilter}
              fieldPrefixes={funderFieldPrefixes} entity="shred-funders" placeholder="Filter funders..."
            />
          }
        />

        <div className="relative border border-border rounded-lg overflow-hidden bg-card">
          {isFetchingFunders && data && (
            <div className="absolute inset-x-0 top-0 h-0.5 overflow-hidden z-10">
              <div className="h-full w-1/3 bg-primary/60 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
            </div>
          )}
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <SortHeader field="funder" label="Funder" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="active" label="Active Seats" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="inactive" label="Inactive Seats" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="closed" label="Closed Seats" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                </tr>
              </thead>
              <tbody>
                {sorted.map((f) => (
                  <tr key={f.funding_authority_key} className="border-b border-border last:border-b-0 hover:bg-muted transition-colors">
                    <td className="px-4 py-3 font-mono text-xs" title={f.funding_authority_key}>{truncatePK(f.funding_authority_key)}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{f.active_seats}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {f.inactive_seats > 0 ? f.inactive_seats : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {f.closed_seats > 0 ? f.closed_seats : <span className="text-muted-foreground">—</span>}
                    </td>
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr><td colSpan={4} className="px-4 py-8 text-center text-muted-foreground">No funders found</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  )
}

// --- Escrow Events Page ---

const eventFieldPrefixes = [
  { prefix: 'type:', description: 'Filter by event type (fund, close, batch_allocate, ...)' },
  { prefix: 'escrow:', description: 'Filter by escrow key' },
  { prefix: 'seat:', description: 'Filter by client seat key' },
  { prefix: 'signer:', description: 'Filter by transaction signer' },
  { prefix: 'epoch:', description: 'Filter by epoch (e.g., =42)' },
  { prefix: 'status:', description: 'Filter by status (ok, failed)' },
]

function formatUSDC(raw: number | null): string {
  if (raw === null) return '\u2014'
  return '$' + (raw / 1_000_000).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

const eventTypeBadgeColors: Record<string, string> = {
  fund: 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20',
  close: 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20',
  batch_allocate: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
  batch_settle: 'bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20',
  allocate_seat: 'bg-cyan-500/10 text-cyan-600 dark:text-cyan-400 border-cyan-500/20',
  ack_allocate: 'bg-cyan-500/10 text-cyan-600 dark:text-cyan-400 border-cyan-500/20',
  reject_allocate: 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20',
  withdraw_seat: 'bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/20',
  ack_withdraw: 'bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/20',
  initialize_seat: 'bg-purple-500/10 text-purple-600 dark:text-purple-400 border-purple-500/20',
  initialize_escrow: 'bg-purple-500/10 text-purple-600 dark:text-purple-400 border-purple-500/20',
  set_price_override: 'bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20',
  unknown: 'bg-gray-500/10 text-gray-600 dark:text-gray-400 border-gray-500/20',
}

const eventTypeLabels: Record<string, string> = {
  fund: 'Fund',
  close: 'Close',
  batch_allocate: 'Batch Allocate',
  batch_settle: 'Batch Settle',
  allocate_seat: 'Instant Allocate',
  ack_allocate: 'Ack Allocate',
  reject_allocate: 'Reject Allocate',
  withdraw_seat: 'Instant Withdraw',
  ack_withdraw: 'Ack Withdraw',
  initialize_seat: 'Init Seat',
  initialize_escrow: 'Init Escrow',
  set_price_override: 'Set Price',
  unknown: 'Unknown',
}

type TimeRangePreset = '24h' | '3d' | '7d' | '14d' | '30d'
const timeRangePresets: { value: TimeRangePreset | 'custom'; label: string }[] = [
  { value: '24h', label: '24h' },
  { value: '3d', label: '3d' },
  { value: '7d', label: '7d' },
  { value: '14d', label: '14d' },
  { value: '30d', label: '30d' },
  { value: 'custom', label: 'Custom' },
]

function toLocalDatetimeString(ts: number): string {
  const d = new Date(ts * 1000)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
}

export function ShredsEscrowEventsPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  // Pagination from URL.
  const page = parseInt(searchParams.get('page') || '1')
  const offset = (page - 1) * PAGE_SIZE

  // Sort from URL.
  const sortBy = searchParams.get('sort') || 'time'
  const sortDir = (searchParams.get('dir') || 'desc') as SortDirection

  // Time range from URL.
  const timeRange = (searchParams.get('range') || '7d') as TimeRangePreset | 'custom'
  const customStart = searchParams.get('start_time') || ''
  const customEnd = searchParams.get('end_time') || ''

  // Include internal toggle from URL.
  const includeInternal = searchParams.get('internal') === 'true'

  // Filters from URL.
  const searchParam = searchParams.get('search') || ''
  const searchFilters = useMemo(() => parseSearchFilters(searchParam), [searchParam])

  // Build server-side filter params.
  const serverFilters = useMemo(() => {
    return searchFilters.length > 0 ? searchFilters : undefined
  }, [searchFilters])

  // Build time range params for API.
  const timeParams = useMemo(() => {
    if (timeRange === 'custom' && customStart && customEnd) {
      return { startTime: parseInt(customStart), endTime: parseInt(customEnd) }
    }
    return { range: timeRange }
  }, [timeRange, customStart, customEnd])

  const { data, isLoading, isFetching: rawFetchingEvents, error, refetch: refetchEvents } = useQuery({
    queryKey: ['shred-escrow-events', offset, sortBy, sortDir, timeParams, searchParam, includeInternal],
    queryFn: () => fetchShredEscrowEvents({
      limit: PAGE_SIZE,
      offset,
      sortBy,
      sortDir,
      ...timeParams,
      filters: serverFilters,
      includeInternal,
    }),
    placeholderData: keepPreviousData,
    refetchInterval: 30000,
  })
  const isFetchingEvents = useDebouncedFetching(rawFetchingEvents)

  const items = data?.items ?? []
  const total = data?.total ?? 0

  // URL state setters.
  const handleSort = useCallback((field: string) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (prev.get('sort') === field) { p.set('dir', prev.get('dir') === 'asc' ? 'desc' : 'asc') }
      else { p.set('sort', field); p.set('dir', 'desc') }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleTimeRange = useCallback((range: TimeRangePreset | 'custom') => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.set('range', range)
      if (range !== 'custom') {
        p.delete('start_time')
        p.delete('end_time')
      } else if (!prev.get('start_time')) {
        // Default custom to last 7 days.
        const now = Math.floor(Date.now() / 1000)
        p.set('start_time', String(now - 7 * 86400))
        p.set('end_time', String(now))
      }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleCustomStart = useCallback((value: string) => {
    const ts = Math.floor(new Date(value).getTime() / 1000)
    if (isNaN(ts)) return
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.set('start_time', String(ts))
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleCustomEnd = useCallback((value: string) => {
    const ts = Math.floor(new Date(value).getTime() / 1000)
    if (isNaN(ts)) return
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.set('end_time', String(ts))
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const setOffset = useCallback((newOffset: number) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
      if (newPage <= 1) { p.delete('page') } else { p.set('page', String(newPage)) }
      return p
    })
  }, [setSearchParams])

  const removeFilter = useCallback((filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (newFilters.length === 0) { p.delete('search') } else { p.set('search', newFilters.join(',')) }
      p.delete('page')
      return p
    })
  }, [searchFilters, setSearchParams])

  const addFilter = useCallback((filter: string) => {
    if (searchFilters.includes(filter)) return
    const newFilters = [...searchFilters, filter]
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.set('search', newFilters.join(','))
      p.delete('page')
      return p
    })
  }, [searchFilters, setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete('search')
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const handleIncludeInternal = useCallback((value: boolean) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (value) { p.set('internal', 'true') } else { p.delete('internal') }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  const maxDate = new Date().toISOString().slice(0, 16)

  if (isLoading && !data) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Puzzle}
          title="Shred Activity"
          count={total}
          subtitle={
            <button onClick={() => refetchEvents()} className="text-muted-foreground hover:text-foreground transition-colors" title="Refresh">
              {isFetchingEvents ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            </button>
          }
          actions={
            <FilterActions
              searchFilters={searchFilters} removeFilter={removeFilter} clearAllFilters={clearAllFilters}
              setLiveFilter={() => {}}
              fieldPrefixes={eventFieldPrefixes} entity="escrow-events" placeholder="Filter events..."
              autocompleteFields={['type', 'seat', 'signer', 'status']}
            />
          }
        />

        {/* Time range selector */}
        <div className="flex items-center gap-3 mb-4">
          <div className="inline-flex rounded-md border border-border bg-background p-0.5">
            {timeRangePresets.map(option => (
              <button
                key={option.value}
                onClick={() => handleTimeRange(option.value)}
                className={`px-2.5 py-1 text-sm rounded transition-colors ${
                  timeRange === option.value
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {option.label}
              </button>
            ))}
          </div>

          {timeRange === 'custom' && (
            <div className="inline-flex items-center gap-2">
              <input
                type="datetime-local"
                value={customStart ? toLocalDatetimeString(parseInt(customStart)) : ''}
                max={maxDate}
                onChange={(e) => handleCustomStart(e.target.value)}
                className="px-2 py-1 text-sm border border-border rounded-md bg-background"
              />
              <span className="text-muted-foreground text-sm">to</span>
              <input
                type="datetime-local"
                value={customEnd ? toLocalDatetimeString(parseInt(customEnd)) : ''}
                max={maxDate}
                onChange={(e) => handleCustomEnd(e.target.value)}
                className="px-2 py-1 text-sm border border-border rounded-md bg-background"
              />
            </div>
          )}

          <div className="flex-1" />

          <button
            onClick={() => handleIncludeInternal(!includeInternal)}
            className="inline-flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            <span>Internal</span>
            <div className={`relative w-7 h-4 rounded-full transition-colors ${
              includeInternal ? 'bg-primary' : 'bg-muted-foreground/30'
            }`}>
              <div className={`absolute top-0.5 w-3 h-3 rounded-full bg-white shadow transition-transform ${
                includeInternal ? 'translate-x-3.5' : 'translate-x-0.5'
              }`} />
            </div>
          </button>
        </div>

        <div className="relative border border-border rounded-lg overflow-hidden bg-card">
          {isFetchingEvents && data && (
            <div className="absolute inset-x-0 top-0 h-0.5 overflow-hidden z-10">
              <div className="h-full w-1/3 bg-primary/60 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
            </div>
          )}
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <SortHeader field="time" label="Time" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="type" label="Event" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <th className="px-4 py-3 font-medium">Seat</th>
                  <th className="px-4 py-3 font-medium">Client IP</th>
                  <th className="px-4 py-3 font-medium">Signer</th>
                  <SortHeader field="amount" label="Amount (USDC)" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="balance" label="Balance (USDC)" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader field="epoch" label="Epoch" align="right" currentSort={sortBy} currentDir={sortDir} onSort={handleSort} />
                  <th className="px-4 py-3 font-medium">Tx</th>
                </tr>
              </thead>
              <tbody>
                {items.map((e) => (
                  <tr key={`${e.tx_signature}-${e.event_type}-${e.escrow_pk}`} className="border-b border-border last:border-b-0 hover:bg-muted transition-colors">
                    <td className="px-4 py-3 text-xs text-muted-foreground whitespace-nowrap">
                      {new Date(e.event_ts).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex items-center text-xs px-2 py-0.5 rounded-lg border ${eventTypeBadgeColors[e.event_type] || eventTypeBadgeColors.unknown}`}>
                        {eventTypeLabels[e.event_type] || e.event_type}
                      </span>
                      {e.status === 'failed' && (
                        <span className="ml-1 inline-flex items-center text-xs px-1.5 py-0.5 rounded-md bg-red-500/10 text-red-600 dark:text-red-400 border border-red-500/20">
                          FAILED
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3 font-mono text-xs" title={e.client_seat_pk}>
                      <span className="inline-flex items-center gap-1.5 group/cell">
                        <Link
                          to={`/dz/shreds/subscribers?search=seat:${e.client_seat_pk}&status=active,expiring,pending,inactive,closed`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {truncatePK(e.client_seat_pk)}
                        </Link>
                        {!searchFilters.some(f => f.startsWith('seat:')) && (
                          <button
                            onClick={() => addFilter(`seat:${e.client_seat_pk}`)}
                            className="text-muted-foreground hover:text-foreground opacity-0 group-hover/cell:opacity-100 transition-opacity p-0.5"
                            title="Filter by this seat"
                          >
                            <Filter className="h-3 w-3" />
                          </button>
                        )}
                      </span>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs">
                      <span className="inline-flex items-center gap-1.5 group/cell">
                        {e.client_ip || <span className="text-muted-foreground">{'\u2014'}</span>}
                        {e.client_ip && <CopyIcon text={e.client_ip} />}
                      </span>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs" title={e.signer}>
                      <span className="inline-flex items-center gap-1.5 group/cell">
                        {e.signer ? truncatePK(e.signer) : <span className="text-muted-foreground">{'\u2014'}</span>}
                        {e.signer && <CopyIcon text={e.signer} />}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {e.amount_usdc !== null ? formatUSDC(e.amount_usdc) : <span className="text-muted-foreground">{'\u2014'}</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {formatUSDC(e.balance_after_usdc ?? 0)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {e.epoch !== null ? e.epoch : <span className="text-muted-foreground">{'\u2014'}</span>}
                    </td>
                    <td className="px-4 py-3">
                      <a
                        href={e.solscan_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline"
                        title={e.tx_signature}
                      >
                        {truncatePK(e.tx_signature)}
                        <ExternalLink className="h-3 w-3" />
                      </a>
                    </td>
                  </tr>
                ))}
                {items.length === 0 && (
                  <tr><td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">No activity found</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
        {total > PAGE_SIZE && (
          <Pagination total={total} limit={PAGE_SIZE} offset={offset} onOffsetChange={setOffset} />
        )}
      </div>
    </div>
  )
}
