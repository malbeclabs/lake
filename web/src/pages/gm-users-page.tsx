import { useMemo, useRef, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { Activity, ChevronUp, ChevronDown } from 'lucide-react'
import uPlot from 'uplot'
import { fetchGMUsers, fetchGMUsersSummary, type GMUserItem, type GMTimePoint } from '@/lib/api'
import { handleRowClick } from '@/lib/utils'
import { PageHeader } from '@/components/page-header'
import { StatCard } from '@/components/stat-card'
import { Search } from 'lucide-react'
import { Pagination } from '@/components/pagination'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useTheme } from '@/hooks/use-theme'

const PAGE_SIZE = 100

const RANGE_OPTIONS = [
  { label: '1h', value: '1h' },
  { label: '3h', value: '3h' },
  { label: '6h', value: '6h' },
  { label: '12h', value: '12h' },
  { label: '24h', value: '24h' },
  { label: '3d', value: '3d' },
  { label: '7d', value: '7d' },
]

function truncatePubkey(pk: string, len = 8): string {
  if (pk.length <= len * 2) return pk
  return `${pk.slice(0, len)}...${pk.slice(-len)}`
}

function formatRtt(ms: number): string {
  if (!ms || ms === 0) return '-'
  return `${ms.toFixed(1)} ms`
}

function formatPct(pct: number): string {
  if (pct === 0) return '-'
  return `${pct.toFixed(1)}%`
}

function timePointsToUPlotData(points: GMTimePoint[]): uPlot.AlignedData {
  if (points.length === 0) return [[], [], []]
  const ts = points.map(p => new Date(p.time).getTime() / 1000)
  const dz = points.map(p => p.dz_value)
  const pi = points.map(p => p.pi_value)
  return [ts, dz, pi]
}

function DualLineChart({ data, height, yLabel, yFormat }: {
  data: uPlot.AlignedData
  height: number
  yLabel: string
  yFormat?: (v: number) => string
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const { resolvedTheme } = useTheme()
  const dzColor = resolvedTheme === 'dark' ? '#22d3ee' : '#0891b2'
  const piColor = resolvedTheme === 'dark' ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.3)'

  const series: uPlot.Series[] = useMemo(() => [
    {},
    { label: 'DoubleZero', stroke: dzColor, width: 2 },
    { label: 'Public Internet', stroke: piColor, width: 2, dash: [6, 4] },
  ], [dzColor, piColor])

  const axes: uPlot.Axis[] = useMemo(() => [
    {},
    {
      label: yLabel,
      values: (_u: uPlot, vals: number[]) => vals.map(v => yFormat ? yFormat(v) : String(v)),
    },
  ], [yLabel, yFormat])

  useUPlotChart({ containerRef, data, series, height, axes })

  return (
    <div>
      <div className="flex items-center gap-4 mb-2 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: dzColor }} />
          DoubleZero
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: piColor, borderTop: `2px dashed ${piColor}`, height: 0 }} />
          Public Internet
        </span>
      </div>
      <div ref={containerRef} className="w-full" />
    </div>
  )
}

interface ParsedFilter { field: string; value: string }

function parseFilter(f: string): ParsedFilter {
  const idx = f.indexOf(':')
  if (idx === -1) return { field: '', value: f }
  return { field: f.slice(0, idx), value: f.slice(idx + 1) }
}

function matchesFilter(item: GMUserItem, filter: ParsedFilter): boolean {
  const val = filter.value.toLowerCase()
  const field = filter.field?.toLowerCase()
  if (!field) {
    return (
      item.user_pubkey.toLowerCase().includes(val) ||
      item.metro.toLowerCase().includes(val) ||
      item.country.toLowerCase().includes(val) ||
      item.city.toLowerCase().includes(val) ||
      item.asn_org.toLowerCase().includes(val) ||
      item.dzd_metro.toLowerCase().includes(val) ||
      item.target_ip.toLowerCase().includes(val)
    )
  }
  switch (field) {
    case 'pubkey': return item.user_pubkey.toLowerCase().includes(val)
    case 'metro': return item.metro.toLowerCase().includes(val)
    case 'country': return item.country.toLowerCase().includes(val)
    case 'city': return item.city.toLowerCase().includes(val)
    case 'asn': return item.asn_org.toLowerCase().includes(val)
    case 'ip': return item.target_ip.toLowerCase().includes(val)
    case 'dzd_metro': return item.dzd_metro.toLowerCase().includes(val)
    default: return false
  }
}

export function GMUsersPage() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()

  const range = searchParams.get('range') || '24h'
  const sortBy = searchParams.get('sort') || 'dz_availability_pct'
  const sortDir = (searchParams.get('dir') || 'desc') as 'asc' | 'desc'
  const page = parseInt(searchParams.get('page') || '1')
  const search = searchParams.get('search') || ''

  const setParam = useCallback((key: string, value: string, resetPage = true) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set(key, value)
      if (resetPage && key !== 'page') next.delete('page')
      return next
    })
  }, [setSearchParams])

  const { data: listData, isLoading: listLoading } = useQuery({
    queryKey: ['gm-users', range],
    queryFn: () => fetchGMUsers(range),
    refetchInterval: 60000,
  })

  const { data: summary, isLoading: summaryLoading } = useQuery({
    queryKey: ['gm-users-summary', range],
    queryFn: () => fetchGMUsersSummary(range),
    refetchInterval: 60000,
  })

  const filters = useMemo(() => {
    if (!search) return []
    return search.split(',').map(s => s.trim()).filter(Boolean).map(parseFilter)
  }, [search])

  const filtered = useMemo(() => {
    if (!listData?.items) return []
    let items = listData.items
    if (filters.length > 0) {
      items = items.filter(item => filters.every(f => matchesFilter(item, f)))
    }
    return items
  }, [listData, filters])

  const sorted = useMemo(() => {
    const copy = [...filtered]
    copy.sort((a, b) => {
      const av = (a as unknown as Record<string, unknown>)[sortBy]
      const bv = (b as unknown as Record<string, unknown>)[sortBy]
      if (typeof av === 'number' && typeof bv === 'number') {
        return sortDir === 'asc' ? av - bv : bv - av
      }
      const as = String(av ?? '')
      const bs = String(bv ?? '')
      return sortDir === 'asc' ? as.localeCompare(bs) : bs.localeCompare(as)
    })
    return copy
  }, [filtered, sortBy, sortDir])

  const offset = (page - 1) * PAGE_SIZE
  const pageItems = sorted.slice(offset, offset + PAGE_SIZE)

  const availData = useMemo(() => timePointsToUPlotData(summary?.availability_ts ?? []), [summary])
  const rttData = useMemo(() => timePointsToUPlotData(summary?.rtt_ts ?? []), [summary])

  function toggleSort(field: string) {
    if (sortBy === field) {
      setParam('dir', sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSearchParams(prev => {
        const next = new URLSearchParams(prev)
        next.set('sort', field)
        next.set('dir', 'desc')
        next.delete('page')
        return next
      })
    }
  }

  function SortIcon({ field }: { field: string }) {
    if (sortBy !== field) return null
    return sortDir === 'asc'
      ? <ChevronUp className="h-3 w-3 inline ml-0.5" />
      : <ChevronDown className="h-3 w-3 inline ml-0.5" />
  }

  const loading = listLoading || summaryLoading

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1400px] mx-auto px-6 py-6">
        <PageHeader
          icon={Activity}
          title="GM Users"
          count={sorted.length}
          actions={
            <div className="flex items-center gap-2">
              <div className="flex items-center rounded-md border border-border overflow-hidden text-sm">
                {RANGE_OPTIONS.map(opt => (
                  <button
                    key={opt.value}
                    onClick={() => setParam('range', opt.value)}
                    className={`px-2.5 py-1.5 transition-colors ${range === opt.value ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}`}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>
          }
        />

        {/* Stat cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <StatCard label="Total Users" value={summary?.total_users} format="number" />
          <StatCard label="DZ Availability" value={summary?.dz_available_pct} format="percent" decimals={1} />
          <StatCard label="DZ Better RTT" value={summary?.dz_better_rtt_pct} format="percent" decimals={1} />
          <StatCard label="Median RTT Delta (ms)" value={summary?.median_rtt_delta_ms} format="number" decimals={1} />
        </div>

        {/* Summary charts */}
        {!summaryLoading && summary && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
            <div className="border border-border rounded-lg p-4">
              <h3 className="text-sm font-medium mb-3">Availability (%)</h3>
              <DualLineChart data={availData} height={180} yLabel="%" yFormat={v => `${v.toFixed(0)}%`} />
            </div>
            <div className="border border-border rounded-lg p-4">
              <h3 className="text-sm font-medium mb-3">RTT (ms)</h3>
              <DualLineChart data={rttData} height={180} yLabel="ms" yFormat={v => `${v.toFixed(1)}`} />
            </div>
          </div>
        )}

        {/* Filter */}
        <div className="mb-4 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <input
            type="text"
            value={search}
            onChange={(e) => setParam('search', e.target.value)}
            placeholder="Filter users... (e.g. metro:Dallas, country:US)"
            className="w-full pl-9 pr-4 py-2 text-sm border border-border rounded-md bg-background focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </div>

        {/* Table */}
        <div className="border border-border rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/50 text-left">
                  <th className="px-4 py-3 font-medium cursor-pointer" onClick={() => toggleSort('user_pubkey')}>Pubkey<SortIcon field="user_pubkey" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer" onClick={() => toggleSort('metro')}>Metro<SortIcon field="metro" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer" onClick={() => toggleSort('country')}>Country<SortIcon field="country" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer text-right" onClick={() => toggleSort('dz_availability_pct')}>DZ Avail<SortIcon field="dz_availability_pct" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer text-right" onClick={() => toggleSort('pi_availability_pct')}>PI Avail<SortIcon field="pi_availability_pct" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer text-right" onClick={() => toggleSort('dz_rtt_ms')}>DZ RTT<SortIcon field="dz_rtt_ms" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer text-right" onClick={() => toggleSort('pi_rtt_ms')}>PI RTT<SortIcon field="pi_rtt_ms" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer text-right" onClick={() => toggleSort('rtt_delta_ms')}>Delta<SortIcon field="rtt_delta_ms" /></th>
                  <th className="px-4 py-3 font-medium cursor-pointer text-right" onClick={() => toggleSort('packet_loss_pct')}>Pkt Loss<SortIcon field="packet_loss_pct" /></th>
                </tr>
              </thead>
              <tbody>
                {loading && pageItems.length === 0 ? (
                  <tr><td colSpan={9} className="px-4 py-12 text-center text-muted-foreground">Loading...</td></tr>
                ) : pageItems.length === 0 ? (
                  <tr><td colSpan={9} className="px-4 py-12 text-center text-muted-foreground">No users found</td></tr>
                ) : (
                  pageItems.map(u => (
                    <tr
                      key={u.user_pubkey}
                      className="border-b border-border hover:bg-muted/30 cursor-pointer transition-colors"
                      onClick={(e) => handleRowClick(e, `/gm/users/${u.user_pubkey}`, navigate)}
                    >
                      <td className="px-4 py-2.5 font-mono text-xs">{truncatePubkey(u.user_pubkey)}</td>
                      <td className="px-4 py-2.5">{u.metro || '-'}</td>
                      <td className="px-4 py-2.5">{u.country || '-'}</td>
                      <td className="px-4 py-2.5 text-right tabular-nums">{formatPct(u.dz_availability_pct)}</td>
                      <td className="px-4 py-2.5 text-right tabular-nums">{formatPct(u.pi_availability_pct)}</td>
                      <td className="px-4 py-2.5 text-right tabular-nums">{formatRtt(u.dz_rtt_ms)}</td>
                      <td className="px-4 py-2.5 text-right tabular-nums">{formatRtt(u.pi_rtt_ms)}</td>
                      <td className={`px-4 py-2.5 text-right tabular-nums ${u.rtt_delta_ms > 0 ? 'text-green-500' : u.rtt_delta_ms < 0 ? 'text-red-500' : ''}`}>
                        {u.rtt_delta_ms === 0 ? '-' : `${u.rtt_delta_ms > 0 ? '+' : ''}${u.rtt_delta_ms.toFixed(1)}`}
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums">{formatPct(u.packet_loss_pct)}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {sorted.length > PAGE_SIZE && (
          <div className="mt-4">
            <Pagination
              total={sorted.length}
              limit={PAGE_SIZE}
              offset={offset}
              onOffsetChange={(newOffset) => {
                const newPage = Math.floor(newOffset / PAGE_SIZE) + 1
                setParam('page', String(newPage), false)
              }}
            />
          </div>
        )}
      </div>
    </div>
  )
}
