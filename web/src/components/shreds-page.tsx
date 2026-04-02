import { useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { Loader2, Coins, ChevronDown, ChevronUp } from 'lucide-react'
import {
  fetchShredsOverview,
  fetchAllPaginated,
  fetchShredClientSeats,
  fetchShredDeviceHistories,
  fetchShredMetroHistories,
  fetchShredFunders,
} from '@/lib/api'
import { Pagination } from './pagination'
import { PageHeader } from './page-header'

const PAGE_SIZE = 100

function truncatePK(pk: string) {
  if (pk.length <= 12) return pk
  return pk.slice(0, 6) + '...' + pk.slice(-4)
}

// --- Overview Section ---

// --- Shared sort helpers ---

type SortDirection = 'asc' | 'desc'

function useSort<T extends string>(defaultField: T, defaultDir: SortDirection = 'desc') {
  const [searchParams, setSearchParams] = useSearchParams()
  const sortField = (searchParams.get('sort') || defaultField) as T
  const sortDirection = (searchParams.get('dir') || defaultDir) as SortDirection

  const handleSort = useCallback((field: T) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (sortField === field) { p.set('dir', sortDirection === 'asc' ? 'desc' : 'asc') }
      else { p.set('sort', field); p.set('dir', 'desc') }
      return p
    })
  }, [sortField, sortDirection, setSearchParams])

  const SortIcon = useCallback(({ field }: { field: T }) => {
    if (sortField !== field) return null
    return sortDirection === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
  }, [sortField, sortDirection])

  const sortAria = useCallback((field: T) => {
    if (sortField !== field) return 'none' as const
    return sortDirection === 'asc' ? 'ascending' as const : 'descending' as const
  }, [sortField, sortDirection])

  return { sortField, sortDirection, handleSort, SortIcon, sortAria }
}

function SortButton<T extends string>({ field, label, align, handleSort, SortIcon }: {
  field: T; label: string; align?: 'right'; handleSort: (f: T) => void; SortIcon: React.FC<{ field: T }>
}) {
  return (
    <button
      className={`inline-flex items-center gap-1 ${align === 'right' ? 'justify-end w-full' : ''}`}
      onClick={() => handleSort(field)}
    >
      {label} <SortIcon field={field} />
    </button>
  )
}

// --- Client Seats Page ---

type SeatSortField = 'device_code' | 'client_ip' | 'tenure_epochs' | 'active_epoch' | 'funder' | 'balance' | 'escrow_count'

function ClientSeatsTab() {
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

  const { sortField, sortDirection, handleSort, SortIcon } = useSort<SeatSortField>('active_epoch')
  const showActive = searchParams.get('active') !== '0'
  const showInactive = searchParams.get('inactive') === '1'
  const showClosed = searchParams.get('closed') === '1'

  const { data: overview, isLoading: overviewLoading } = useQuery({
    queryKey: ['shreds-overview'],
    queryFn: fetchShredsOverview,
    refetchInterval: 30000,
  })
  const currentEpoch = overview?.current_solana_epoch ?? 0

  const { data, isLoading: seatsLoading } = useQuery({
    queryKey: ['shred-client-seats', 'all'],
    queryFn: () => fetchAllPaginated(fetchShredClientSeats, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const filtered = useMemo(() => {
    if (!data?.items) return []
    return data.items.filter(s => {
      const isActive = currentEpoch === 0 || s.active_epoch >= currentEpoch
      const isClosed = s.escrow_count === 0
      if (!showActive && isActive && !isClosed) return false
      if (!showInactive && !isActive && !isClosed) return false
      if (!showClosed && isClosed) return false
      return true
    })
  }, [data, showActive, showInactive, showClosed, currentEpoch])

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'device_code': cmp = (a.device_code || a.device_key).localeCompare(b.device_code || b.device_key); break
        case 'client_ip': cmp = a.client_ip.localeCompare(b.client_ip); break
        case 'tenure_epochs': cmp = a.tenure_epochs - b.tenure_epochs; break
        case 'active_epoch': cmp = Number(a.active_epoch) - Number(b.active_epoch); break
        case 'funder': cmp = a.funding_authority_key.localeCompare(b.funding_authority_key); break
        case 'balance': cmp = a.total_usdc_balance - b.total_usdc_balance; break
        case 'escrow_count': cmp = a.escrow_count - b.escrow_count; break
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
  }, [filtered, sortField, sortDirection])

  const paged = useMemo(() => sorted.slice(offset, offset + PAGE_SIZE), [sorted, offset])

  const toggleParam = useCallback((key: string, current: boolean, defaultOn = false) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (defaultOn) {
        if (current) { p.set(key, '0') } else { p.delete(key) }
      } else {
        if (current) { p.delete(key) } else { p.set(key, '1') }
      }
      p.delete('page')
      return p
    })
  }, [setSearchParams])

  if (seatsLoading || overviewLoading) {
    return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
  }

  return (
    <>
      <div className="flex items-center gap-4 mb-3">
        <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer select-none">
          <input type="checkbox" checked={showActive} onChange={() => toggleParam('active', showActive, true)} className="rounded" />
          Show active
        </label>
        <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer select-none">
          <input type="checkbox" checked={showInactive} onChange={() => toggleParam('inactive', showInactive)} className="rounded" />
          Show inactive
        </label>
        <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer select-none">
          <input type="checkbox" checked={showClosed} onChange={() => toggleParam('closed', showClosed)} className="rounded" />
          Show closed
        </label>
        <span className="text-xs text-muted-foreground">{sorted.length} seats</span>
      </div>
      <div className="border border-border rounded-lg overflow-hidden bg-card">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-sm text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-3 font-medium"><SortButton field="device_code" label="Device" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium"><SortButton field="client_ip" label="Client IP" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="tenure_epochs" label="Tenure" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="active_epoch" label="Active Epoch" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium"><SortButton field="funder" label="Funder" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="balance" label="Balance (USDC)" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
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
              </tr>
            ))}
            {sorted.length === 0 && (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">No client seats found</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <Pagination total={sorted.length} limit={PAGE_SIZE} offset={offset} onOffsetChange={setOffset} />
    </div>
    </>
  )
}

// --- Device Histories Tab ---

type DeviceSortField = 'device_code' | 'metro_code' | 'granted_seats' | 'available_seats'

function DeviceHistoriesTab() {
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

  const { sortField, sortDirection, handleSort, SortIcon } = useSort<DeviceSortField>('granted_seats')

  const { data, isLoading } = useQuery({
    queryKey: ['shred-device-histories', 'all'],
    queryFn: () => fetchAllPaginated(fetchShredDeviceHistories, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const sorted = useMemo(() => {
    if (!data?.items) return []
    return [...data.items].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'device_code': cmp = (a.device_code || a.device_key).localeCompare(b.device_code || b.device_key); break
        case 'metro_code': cmp = (a.metro_code || a.metro_exchange_key).localeCompare(b.metro_code || b.metro_exchange_key); break
        case 'granted_seats': cmp = a.active_granted_seats - b.active_granted_seats; break
        case 'available_seats': cmp = a.active_total_available_seats - b.active_total_available_seats; break
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
  }, [data, sortField, sortDirection])

  const paged = useMemo(() => sorted.slice(offset, offset + PAGE_SIZE), [sorted, offset])

  if (isLoading) {
    return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
  }

  return (
    <div className="border border-border rounded-lg overflow-hidden bg-card">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="text-sm text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-3 font-medium"><SortButton field="device_code" label="Device" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium"><SortButton field="metro_code" label="Metro" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="granted_seats" label="Granted Seats" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="available_seats" label="Available Seats" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
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
              <tr><td colSpan={4} className="px-4 py-8 text-center text-muted-foreground">No device histories found</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <Pagination total={sorted.length} limit={PAGE_SIZE} offset={offset} onOffsetChange={setOffset} />
    </div>
  )
}

// --- Metro Histories Tab ---

type MetroSortField = 'metro_code' | 'devices' | 'price'

function MetroHistoriesTab() {
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

  const { sortField, sortDirection, handleSort, SortIcon } = useSort<MetroSortField>('devices')

  const { data, isLoading } = useQuery({
    queryKey: ['shred-metro-histories', 'all'],
    queryFn: () => fetchAllPaginated(fetchShredMetroHistories, PAGE_SIZE),
    refetchInterval: 30000,
  })

  const sorted = useMemo(() => {
    if (!data?.items) return []
    return [...data.items].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'metro_code': cmp = (a.metro_code || a.exchange_key).localeCompare(b.metro_code || b.exchange_key); break
        case 'devices': cmp = a.total_initialized_devices - b.total_initialized_devices; break
        case 'price': cmp = a.current_usdc_price_dollars - b.current_usdc_price_dollars; break
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
  }, [data, sortField, sortDirection])

  const paged = useMemo(() => sorted.slice(offset, offset + PAGE_SIZE), [sorted, offset])

  if (isLoading) {
    return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
  }

  return (
    <div className="border border-border rounded-lg overflow-hidden bg-card">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="text-sm text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-3 font-medium"><SortButton field="metro_code" label="Metro" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="devices" label="Devices" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="price" label="Price (USDC)" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
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
              <tr><td colSpan={3} className="px-4 py-8 text-center text-muted-foreground">No metro histories found</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <Pagination total={sorted.length} limit={PAGE_SIZE} offset={offset} onOffsetChange={setOffset} />
    </div>
  )
}

// --- Funders Tab ---

type FunderSortField = 'funder' | 'active_seats' | 'inactive_seats' | 'closed_seats'

function FundersTab() {
  const { sortField, sortDirection, handleSort, SortIcon } = useSort<FunderSortField>('active_seats')

  const { data, isLoading } = useQuery({
    queryKey: ['shred-funders'],
    queryFn: fetchShredFunders,
    refetchInterval: 30000,
  })

  const sorted = useMemo(() => {
    if (!data) return []
    return [...data].sort((a, b) => {
      let cmp = 0
      switch (sortField) {
        case 'funder': cmp = a.funding_authority_key.localeCompare(b.funding_authority_key); break
        case 'active_seats': cmp = a.active_seats - b.active_seats; break
        case 'inactive_seats': cmp = a.inactive_seats - b.inactive_seats; break
        case 'closed_seats': cmp = a.closed_seats - b.closed_seats; break
      }
      return sortDirection === 'asc' ? cmp : -cmp
    })
  }, [data, sortField, sortDirection])

  if (isLoading) {
    return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
  }

  return (
    <div className="border border-border rounded-lg overflow-hidden bg-card">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="text-sm text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-3 font-medium"><SortButton field="funder" label="Funder" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="active_seats" label="Active Seats" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="inactive_seats" label="Inactive Seats" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
              <th className="px-4 py-3 font-medium text-right"><SortButton field="closed_seats" label="Closed Seats" align="right" handleSort={handleSort} SortIcon={SortIcon} /></th>
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
  )
}

// --- Page Exports ---

function PageWrapper({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader icon={Coins} title={title} />
        {children}
      </div>
    </div>
  )
}

export function ShredsSeatsPage() {
  return <PageWrapper title="Shred Seats"><ClientSeatsTab /></PageWrapper>
}

export function ShredsFundersPage() {
  return <PageWrapper title="Shred Funders"><FundersTab /></PageWrapper>
}

export function ShredsDevicesPage() {
  return <PageWrapper title="Shred Devices"><DeviceHistoriesTab /></PageWrapper>
}

export function ShredsMetrosPage() {
  return <PageWrapper title="Shred Metros"><MetroHistoriesTab /></PageWrapper>
}
