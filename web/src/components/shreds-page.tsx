import { useMemo, useCallback, useState, useRef, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { Loader2, Coins, AlertCircle, ChevronDown, ChevronUp, X } from 'lucide-react'
import {
  fetchShredsOverview,
  fetchAllPaginated,
  fetchShredClientSeats,
  fetchShredDeviceHistories,
  fetchShredMetroHistories,
  fetchShredFunders,
  type ShredClientSeat,
  type ShredDeviceHistory,
  type ShredMetroHistory,
  type ShredFunder,
} from '@/lib/api'
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

function FilterActions({ searchFilters, removeFilter, clearAllFilters, setLiveFilter, fieldPrefixes, entity, placeholder }: {
  searchFilters: string[]; removeFilter: (f: string) => void; clearAllFilters: () => void
  setLiveFilter: (f: string) => void
  fieldPrefixes: { prefix: string; description: string }[]; entity: string; placeholder: string
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
        autocompleteFields={[]}
        placeholder={placeholder}
        onLiveFilterChange={setLiveFilter}
      />
    </>
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

const seatFilterFields = ['device', 'metro', 'ip', 'funder', 'tenure', 'epoch', 'balance', 'prepaid']
const seatFieldPrefixes = [
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

function matchesSeatFilter(seat: ShredClientSeat, filter: string): boolean {
  const { field, value } = parseFilter(filter, seatFilterFields)
  const needle = value.toLowerCase()

  if (field === 'all') {
    return (seat.device_code || seat.device_key).toLowerCase().includes(needle)
      || (seat.metro_code || seat.metro_pk).toLowerCase().includes(needle)
      || seat.client_ip.includes(needle)
      || seat.funding_authority_key.toLowerCase().includes(needle)
  }

  switch (field) {
    case 'device': return (seat.device_code || seat.device_key).toLowerCase().includes(needle)
    case 'metro': return (seat.metro_code || seat.metro_pk).toLowerCase().includes(needle)
    case 'ip': return seat.client_ip.includes(needle)
    case 'funder': return seat.funding_authority_key.toLowerCase().includes(needle)
    case 'tenure': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(seat.tenure_epochs, nf) : false }
    case 'epoch': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(seat.active_epoch, nf) : false }
    case 'balance': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(seat.total_usdc_balance / 1e6, nf) : false }
    case 'prepaid': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(prepaidEpochs(seat), nf) : false }
    default: return true
  }
}

export function ShredsSeatsPage() {
  const ps = usePageState()
  const sortField = ps.sortField || 'active_epoch'
  const showActive = ps.searchParams.get('active') !== '0'
  const showInactive = ps.searchParams.get('inactive') === '1'
  const showClosed = ps.searchParams.get('closed') === '1'

  const toggleParam = useCallback((key: string, current: boolean, defaultOn = false) => {
    ps.setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (defaultOn) { if (current) { p.set(key, '0') } else { p.delete(key) } }
      else { if (current) { p.delete(key) } else { p.set(key, '1') } }
      p.delete('page')
      return p
    })
  }, [ps.setSearchParams])

  const { data: overview } = useQuery({
    queryKey: ['shreds-overview'],
    queryFn: fetchShredsOverview,
    refetchInterval: 30000,
  })
  const currentEpoch = overview?.current_solana_epoch ?? 0

  const { data, isLoading, error } = useQuery({
    queryKey: ['shred-client-seats', 'all'],
    queryFn: () => fetchAllPaginated(fetchShredClientSeats, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const filtered = useMemo(() => {
    if (!data?.items) return []
    // Deduplicate
    const seen = new Set<string>()
    const unique = data.items.filter(s => { if (seen.has(s.pk)) return false; seen.add(s.pk); return true })

    return unique.filter(s => {
      // Status filtering
      const isActive = currentEpoch === 0 || s.active_epoch >= currentEpoch
      const isClosed = s.escrow_count === 0
      if (!showActive && isActive && !isClosed) return false
      if (!showInactive && !isActive && !isClosed) return false
      if (!showClosed && isClosed) return false

      // Text/numeric filtering
      if (ps.allFilters.length === 0) return true
      const grouped = new Map<string, string[]>()
      for (const f of ps.allFilters) {
        const { field } = parseFilter(f, seatFilterFields)
        grouped.set(field, [...(grouped.get(field) ?? []), f])
      }
      return Array.from(grouped.values()).every(group => group.some(f => matchesSeatFilter(s, f)))
    })
  }, [data, showActive, showInactive, showClosed, currentEpoch, ps.allFilters])

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'device': cmp = (a.device_code || a.device_key).localeCompare(b.device_code || b.device_key); break
        case 'metro': cmp = (a.metro_code || a.metro_pk).localeCompare(b.metro_code || b.metro_pk); break
        case 'ip': cmp = a.client_ip.localeCompare(b.client_ip); break
        case 'tenure': cmp = a.tenure_epochs - b.tenure_epochs; break
        case 'active_epoch': cmp = Number(a.active_epoch) - Number(b.active_epoch); break
        case 'funder': cmp = a.funding_authority_key.localeCompare(b.funding_authority_key); break
        case 'balance': cmp = a.total_usdc_balance - b.total_usdc_balance; break
        case 'prepaid': cmp = prepaidEpochs(a) - prepaidEpochs(b); break
      }
      return ps.sortDirection === 'asc' ? cmp : -cmp
    })
  }, [filtered, sortField, ps.sortDirection])

  const paged = useMemo(() => sorted.slice(ps.offset, ps.offset + PAGE_SIZE), [sorted, ps.offset])

  if (isLoading) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Coins}
          title="Shred Seats"
          count={sorted.length}
          actions={
            <FilterActions
              searchFilters={ps.searchFilters} removeFilter={ps.removeFilter} clearAllFilters={ps.clearAllFilters}
              setLiveFilter={ps.setLiveFilter}
              fieldPrefixes={seatFieldPrefixes} entity="shred-seats" placeholder="Filter seats..."
            />
          }
        />

        <div className="flex items-center gap-2 mb-3">
          <button
            onClick={() => toggleParam('active', showActive, true)}
            className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border transition-colors ${
              showActive
                ? 'bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20'
                : 'bg-muted text-muted-foreground border-border opacity-50'
            }`}
          >
            <div className={`h-1.5 w-1.5 rounded-full ${showActive ? 'bg-green-500' : 'bg-muted-foreground'}`} />
            Active
          </button>
          <button
            onClick={() => toggleParam('inactive', showInactive)}
            className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border transition-colors ${
              showInactive
                ? 'bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20'
                : 'bg-muted text-muted-foreground border-border opacity-50'
            }`}
          >
            <div className={`h-1.5 w-1.5 rounded-full ${showInactive ? 'bg-amber-500' : 'bg-muted-foreground'}`} />
            Inactive
          </button>
          <button
            onClick={() => toggleParam('closed', showClosed)}
            className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border transition-colors ${
              showClosed
                ? 'bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20'
                : 'bg-muted text-muted-foreground border-border opacity-50'
            }`}
          >
            <div className={`h-1.5 w-1.5 rounded-full ${showClosed ? 'bg-red-500' : 'bg-muted-foreground'}`} />
            Closed
          </button>
        </div>

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <SortHeader field="device" label="Device" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="metro" label="Metro" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="ip" label="Client IP" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="tenure" label="Tenure" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="active_epoch" label="Active Epoch" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="funder" label="Funder" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="balance" label="Balance (USDC)" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="prepaid" label="Prepaid Epochs" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                </tr>
              </thead>
              <tbody>
                {paged.map((seat) => (
                  <tr key={seat.pk} className="border-b border-border last:border-b-0 hover:bg-muted transition-colors">
                    <td className="px-4 py-3 text-sm">
                      <Link to={`/dz/devices/${seat.device_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={seat.device_key}>
                        {seat.device_code || truncatePK(seat.device_key)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {seat.metro_pk ? (
                        <Link to={`/dz/metros/${seat.metro_pk}`} className="text-blue-500 hover:underline font-mono text-xs" title={seat.metro_pk}>
                          {seat.metro_code || truncatePK(seat.metro_pk)}
                        </Link>
                      ) : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono">
                      {seat.user_pk ? (
                        <Link to={`/dz/users/${seat.user_pk}`} className="text-blue-500 hover:underline" title={seat.user_pk}>
                          {seat.client_ip}
                        </Link>
                      ) : seat.client_ip}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{seat.tenure_epochs}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{seat.active_epoch}</td>
                    <td className="px-4 py-3 font-mono text-xs" title={seat.funding_authority_key}>
                      {truncatePK(seat.funding_authority_key)}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {seat.total_usdc_balance > 0 ? `$${(seat.total_usdc_balance / 1e6).toFixed(2)}` : <span className="text-muted-foreground">—</span>}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">
                      {(() => {
                        const epochs = prepaidEpochs(seat)
                        if (epochs <= 0) return <span className="text-muted-foreground">—</span>
                        return epochs
                      })()}
                    </td>
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr><td colSpan={8} className="px-4 py-8 text-center text-muted-foreground">No seats found</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <Pagination total={sorted.length} limit={PAGE_SIZE} offset={ps.offset} onOffsetChange={ps.setOffset} />
        </div>
      </div>
    </div>
  )
}

// --- Devices Page ---

const deviceFilterFields = ['device', 'metro', 'granted', 'available']
const deviceFieldPrefixes = [
  { prefix: 'device:', description: 'Filter by device code' },
  { prefix: 'metro:', description: 'Filter by metro code' },
  { prefix: 'granted:', description: 'Filter by granted seats (e.g., >0)' },
  { prefix: 'available:', description: 'Filter by available seats (e.g., >10)' },
]

function matchesDeviceFilter(d: ShredDeviceHistory, filter: string): boolean {
  const { field, value } = parseFilter(filter, deviceFilterFields)
  const needle = value.toLowerCase()

  if (field === 'all') {
    return (d.device_code || d.device_key).toLowerCase().includes(needle)
      || (d.metro_code || d.metro_exchange_key).toLowerCase().includes(needle)
  }

  switch (field) {
    case 'device': return (d.device_code || d.device_key).toLowerCase().includes(needle)
    case 'metro': return (d.metro_code || d.metro_exchange_key).toLowerCase().includes(needle)
    case 'granted': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(d.active_granted_seats, nf) : false }
    case 'available': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(d.active_total_available_seats, nf) : false }
    default: return true
  }
}

export function ShredsDevicesPage() {
  const ps = usePageState()
  const sortField = ps.sortField || 'granted'

  const { data, isLoading, error } = useQuery({
    queryKey: ['shred-device-histories', 'all'],
    queryFn: () => fetchAllPaginated(fetchShredDeviceHistories, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const filtered = useMemo(() => {
    if (!data?.items) return []
    const seen = new Set<string>()
    const unique = data.items.filter(d => { if (seen.has(d.pk)) return false; seen.add(d.pk); return true })
    if (ps.allFilters.length === 0) return unique
    const grouped = new Map<string, string[]>()
    for (const f of ps.allFilters) {
      const { field } = parseFilter(f, deviceFilterFields)
      grouped.set(field, [...(grouped.get(field) ?? []), f])
    }
    return unique.filter(d => Array.from(grouped.values()).every(group => group.some(f => matchesDeviceFilter(d, f))))
  }, [data, ps.allFilters])

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'device': cmp = (a.device_code || a.device_key).localeCompare(b.device_code || b.device_key); break
        case 'metro': cmp = (a.metro_code || a.metro_exchange_key).localeCompare(b.metro_code || b.metro_exchange_key); break
        case 'granted': cmp = a.active_granted_seats - b.active_granted_seats; break
        case 'available': cmp = a.active_total_available_seats - b.active_total_available_seats; break
      }
      return ps.sortDirection === 'asc' ? cmp : -cmp
    })
  }, [filtered, sortField, ps.sortDirection])

  const paged = useMemo(() => sorted.slice(ps.offset, ps.offset + PAGE_SIZE), [sorted, ps.offset])

  if (isLoading) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Coins}
          title="Shred Devices"
          count={sorted.length}
          actions={
            <FilterActions
              searchFilters={ps.searchFilters} removeFilter={ps.removeFilter} clearAllFilters={ps.clearAllFilters}
              setLiveFilter={ps.setLiveFilter}
              fieldPrefixes={deviceFieldPrefixes} entity="shred-devices" placeholder="Filter devices..."
            />
          }
        />

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <SortHeader field="device" label="Device" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="metro" label="Metro" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="granted" label="Granted Seats" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="available" label="Available Seats" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                </tr>
              </thead>
              <tbody>
                {paged.map((d) => (
                  <tr key={d.pk} className="border-b border-border last:border-b-0 hover:bg-muted transition-colors">
                    <td className="px-4 py-3 text-sm">
                      <Link to={`/dz/devices/${d.device_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={d.device_key}>
                        {d.device_code || truncatePK(d.device_key)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <Link to={`/dz/metros/${d.metro_exchange_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={d.metro_exchange_key}>
                        {d.metro_code || truncatePK(d.metro_exchange_key)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{d.active_granted_seats}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{d.active_total_available_seats}</td>
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr><td colSpan={4} className="px-4 py-8 text-center text-muted-foreground">No devices found</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <Pagination total={sorted.length} limit={PAGE_SIZE} offset={ps.offset} onOffsetChange={ps.setOffset} />
        </div>
      </div>
    </div>
  )
}

// --- Metros Page ---

const metroFilterFields = ['metro', 'devices', 'price']
const metroFieldPrefixes = [
  { prefix: 'metro:', description: 'Filter by metro code' },
  { prefix: 'devices:', description: 'Filter by device count (e.g., >5)' },
  { prefix: 'price:', description: 'Filter by USDC price (e.g., >0)' },
]

function matchesMetroFilter(m: ShredMetroHistory, filter: string): boolean {
  const { field, value } = parseFilter(filter, metroFilterFields)
  const needle = value.toLowerCase()

  if (field === 'all') {
    return (m.metro_code || m.exchange_key).toLowerCase().includes(needle)
  }

  switch (field) {
    case 'metro': return (m.metro_code || m.exchange_key).toLowerCase().includes(needle)
    case 'devices': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(m.total_initialized_devices, nf) : false }
    case 'price': { const nf = parseNumericFilter(value); return nf ? matchesNumericFilter(m.current_usdc_price_dollars, nf) : false }
    default: return true
  }
}

export function ShredsMetrosPage() {
  const ps = usePageState()
  const sortField = ps.sortField || 'devices'

  const { data, isLoading, error } = useQuery({
    queryKey: ['shred-metro-histories', 'all'],
    queryFn: () => fetchAllPaginated(fetchShredMetroHistories, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const filtered = useMemo(() => {
    if (!data?.items) return []
    const seen = new Set<string>()
    const unique = data.items.filter(m => { if (seen.has(m.pk)) return false; seen.add(m.pk); return true })
    if (ps.allFilters.length === 0) return unique
    const grouped = new Map<string, string[]>()
    for (const f of ps.allFilters) {
      const { field } = parseFilter(f, metroFilterFields)
      grouped.set(field, [...(grouped.get(field) ?? []), f])
    }
    return unique.filter(m => Array.from(grouped.values()).every(group => group.some(f => matchesMetroFilter(m, f))))
  }, [data, ps.allFilters])

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'metro': cmp = (a.metro_code || a.exchange_key).localeCompare(b.metro_code || b.exchange_key); break
        case 'devices': cmp = a.total_initialized_devices - b.total_initialized_devices; break
        case 'price': cmp = a.current_usdc_price_dollars - b.current_usdc_price_dollars; break
      }
      return ps.sortDirection === 'asc' ? cmp : -cmp
    })
  }, [filtered, sortField, ps.sortDirection])

  const paged = useMemo(() => sorted.slice(ps.offset, ps.offset + PAGE_SIZE), [sorted, ps.offset])

  if (isLoading) return <LoadingState />
  if (error) return <ErrorState message={error?.message || 'Unknown error'} />

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Coins}
          title="Shred Metros"
          count={sorted.length}
          actions={
            <FilterActions
              searchFilters={ps.searchFilters} removeFilter={ps.removeFilter} clearAllFilters={ps.clearAllFilters}
              setLiveFilter={ps.setLiveFilter}
              fieldPrefixes={metroFieldPrefixes} entity="shred-metros" placeholder="Filter metros..."
            />
          }
        />

        <div className="border border-border rounded-lg overflow-hidden bg-card">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <SortHeader field="metro" label="Metro" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="devices" label="Devices" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                  <SortHeader field="price" label="Price (USDC)" align="right" currentSort={sortField} currentDir={ps.sortDirection} onSort={ps.handleSort} />
                </tr>
              </thead>
              <tbody>
                {paged.map((m) => (
                  <tr key={m.pk} className="border-b border-border last:border-b-0 hover:bg-muted transition-colors">
                    <td className="px-4 py-3 text-sm">
                      <Link to={`/dz/metros/${m.exchange_key}`} className="text-blue-500 hover:underline font-mono text-xs" title={m.exchange_key}>
                        {m.metro_code || truncatePK(m.exchange_key)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">{m.total_initialized_devices}</td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right">${m.current_usdc_price_dollars}</td>
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr><td colSpan={3} className="px-4 py-8 text-center text-muted-foreground">No metros found</td></tr>
                )}
              </tbody>
            </table>
          </div>
          <Pagination total={sorted.length} limit={PAGE_SIZE} offset={ps.offset} onOffsetChange={ps.setOffset} />
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

  const { data, isLoading, error } = useQuery({
    queryKey: ['shred-funders'],
    queryFn: fetchShredFunders,
    refetchInterval: 30000,
  })

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
          icon={Coins}
          title="Shred Funders"
          count={sorted.length}
          actions={
            <FilterActions
              searchFilters={ps.searchFilters} removeFilter={ps.removeFilter} clearAllFilters={ps.clearAllFilters}
              setLiveFilter={ps.setLiveFilter}
              fieldPrefixes={funderFieldPrefixes} entity="shred-funders" placeholder="Filter funders..."
            />
          }
        />

        <div className="border border-border rounded-lg overflow-hidden bg-card">
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
