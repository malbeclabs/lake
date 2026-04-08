import { useState, useMemo, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchDashboardBurstiness, type DashboardBurstinessEntity } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { cn } from '@/lib/utils'

function formatRate(val: number): string {
  if (val >= 1e12) return (val / 1e12).toFixed(1) + ' Tbps'
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gbps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mbps'
  if (val >= 1e3) return (val / 1e3).toFixed(1) + ' Kbps'
  return val.toFixed(0) + ' bps'
}

function formatRatio(val: number): string {
  return val.toFixed(1) + 'x'
}

function formatTimeAgo(isoStr: string): string {
  const d = new Date(isoStr)
  const now = Date.now()
  const diffMs = now - d.getTime()
  if (diffMs < 0) return 'just now'
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  return `${days}d ago`
}

function spikeColor(ratio: number): string {
  if (ratio >= 5) return 'bg-red-500/15 text-red-400 border-red-500/20'
  if (ratio >= 3) return 'bg-yellow-500/15 text-yellow-400 border-yellow-500/20'
  return 'bg-blue-500/15 text-blue-400 border-blue-500/20'
}

const bpsFilterOptions = [
  { value: 0, label: 'None' },
  { value: 1_000_000, label: '1 Mbps' },
  { value: 10_000_000, label: '10 Mbps' },
  { value: 100_000_000, label: '100 Mbps' },
  { value: 1_000_000_000, label: '1 Gbps' },
]

const pageSizeOptions = [10, 20, 50]

type SortField = 'spike_count' | 'max_spike_ratio' | 'p50_bps' | 'max_spike_bps'

function SpikeTable({
  entities,
  state,
  sortField,
  sortDir,
  handleSort,
  isPlaceholderData,
}: {
  entities: DashboardBurstinessEntity[]
  state: ReturnType<typeof useDashboard>
  sortField: SortField
  sortDir: 'asc' | 'desc'
  handleSort: (field: SortField) => void
  isPlaceholderData: boolean
}) {
  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null
    return sortDir === 'asc'
      ? <ChevronUp className="h-3 w-3" />
      : <ChevronDown className="h-3 w-3" />
  }

  const sortAria = (field: SortField) => {
    if (sortField !== field) return 'none' as const
    return sortDir === 'asc' ? 'ascending' as const : 'descending' as const
  }

  if (entities.length === 0) {
    return (
      <div className="py-4 text-center text-sm text-muted-foreground">
        No spikes detected
      </div>
    )
  }

  return (
    <div className={cn('overflow-x-auto transition-opacity', isPlaceholderData && 'opacity-50')}>
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Interface</th>
            <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
            <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Contributor</th>
            <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('spike_count')}>
              <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('spike_count')}>
                Spikes <SortIcon field="spike_count" />
              </button>
            </th>
            <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('max_spike_ratio')}>
              <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('max_spike_ratio')}>
                Worst Spike <SortIcon field="max_spike_ratio" />
              </button>
            </th>
            <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('p50_bps')}>
              <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('p50_bps')}>
                Baseline (P50) <SortIcon field="p50_bps" />
              </button>
            </th>
            <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('max_spike_bps')}>
              <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('max_spike_bps')}>
                Peak Spike <SortIcon field="max_spike_bps" />
              </button>
            </th>
            <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">Last Spike</th>
          </tr>
        </thead>
        <tbody>
          {entities.map((e, i) => {
            const isSelected = state.selectedEntity?.devicePk === e.device_pk &&
              state.selectedEntity?.intf === e.intf
            return (
              <tr
                key={`${e.device_pk}-${e.intf}-${i}`}
                onClick={() => {
                  if (isSelected) {
                    state.selectEntity(null)
                    state.setReferenceLines(e.device_pk, e.intf, null)
                  } else {
                    state.selectEntity({
                      devicePk: e.device_pk,
                      deviceCode: e.device_code,
                      intf: e.intf,
                    })
                    state.setReferenceLines(e.device_pk, e.intf, {
                      p50_bps: e.p50_bps,
                      p99_bps: e.max_spike_bps,
                      direction: e.peak_direction,
                    })
                  }
                }}
                className={cn(
                  'border-b border-border/50 cursor-pointer transition-colors',
                  isSelected ? 'bg-blue-500/10 ring-1 ring-blue-500/30' : 'hover:bg-muted/50'
                )}
              >
                <td className="py-1.5 px-2 font-mono">
                  {e.device_code} <span className="text-muted-foreground">{e.intf}</span>
                  <span className="text-[10px] text-muted-foreground ml-1">{e.peak_direction === 'rx' ? 'Rx' : 'Tx'}</span>
                </td>
                <td className="py-1.5 px-2">{e.metro_code}</td>
                <td className="py-1.5 px-2">{e.contributor_code}</td>
                <td className="py-1.5 px-2 text-right font-mono">
                  {e.spike_count}
                  <span className="text-[10px] text-muted-foreground ml-1">/ {e.total_buckets}</span>
                </td>
                <td className="py-1.5 px-2 text-right">
                  <span className={cn('px-1.5 py-0.5 rounded text-xs border', spikeColor(e.max_spike_ratio))}>
                    {formatRatio(e.max_spike_ratio)}
                  </span>
                </td>
                <td className="py-1.5 px-2 text-right font-mono">
                  {formatRate(e.p50_bps)}
                </td>
                <td className="py-1.5 px-2 text-right font-mono">
                  {formatRate(e.max_spike_bps)}
                </td>
                <td className="py-1.5 px-2 text-right text-muted-foreground">
                  {e.last_spike_time ? formatTimeAgo(e.last_spike_time) : '\u2014'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export function BurstinessPanel() {
  const state = useDashboard()
  const [limit, setLimit] = useState(10)
  const [sortField, setSortField] = useState<SortField>('max_spike_ratio')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [minBps, setMinBps] = useState(10_000_000)
  const [minPeakBps, setMinPeakBps] = useState(0)
  const [activeTab, setActiveTab] = useState<'link' | 'tunnel' | 'cyoa' | 'other'>('link')
  const [page, setPage] = useState(0)

  const isAllMode = state.intfType === 'all'

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDir('desc')
    }
    setPage(0)
  }

  const baseParams = useMemo(() => ({
    ...dashboardFilterParams(state),
    sort: sortField,
    dir: sortDir,
    limit,
    offset: page * limit,
    min_bps: minBps,
    min_peak_bps: minPeakBps,
  }), [state, sortField, sortDir, limit, page, minBps, minPeakBps])

  // Single query for when a specific type is selected
  const singleQuery = useQuery({
    queryKey: ['dashboard-burstiness', baseParams],
    queryFn: () => fetchDashboardBurstiness(baseParams),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
    placeholderData: keepPreviousData,
    enabled: !isAllMode,
  })

  // Three parallel queries for "all" mode
  const linkQuery = useQuery({
    queryKey: ['dashboard-burstiness', { ...baseParams, intf_type: 'link' }],
    queryFn: () => fetchDashboardBurstiness({ ...baseParams, intf_type: 'link' }),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
    placeholderData: keepPreviousData,
    enabled: isAllMode,
  })

  const tunnelQuery = useQuery({
    queryKey: ['dashboard-burstiness', { ...baseParams, intf_type: 'tunnel' }],
    queryFn: () => fetchDashboardBurstiness({ ...baseParams, intf_type: 'tunnel' }),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
    placeholderData: keepPreviousData,
    enabled: isAllMode,
  })

  const cyoaQuery = useQuery({
    queryKey: ['dashboard-burstiness', { ...baseParams, intf_type: 'cyoa' }],
    queryFn: () => fetchDashboardBurstiness({ ...baseParams, intf_type: 'cyoa' }),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
    placeholderData: keepPreviousData,
    enabled: isAllMode,
  })

  const otherQuery = useQuery({
    queryKey: ['dashboard-burstiness', { ...baseParams, intf_type: 'other' }],
    queryFn: () => fetchDashboardBurstiness({ ...baseParams, intf_type: 'other' }),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
    placeholderData: keepPreviousData,
    enabled: isAllMode,
  })

  // Sync reference lines from query results when the selected entity matches
  // a spike detection row (e.g., after page refresh where URL has a selection
  // but reference lines state was lost).
  const selectedEntity = state.selectedEntity
  const allEntities = useMemo(() => {
    if (isAllMode) {
      return [
        ...(linkQuery.data?.entities ?? []),
        ...(tunnelQuery.data?.entities ?? []),
        ...(cyoaQuery.data?.entities ?? []),
        ...(otherQuery.data?.entities ?? []),
      ]
    }
    return singleQuery.data?.entities ?? []
  }, [isAllMode, linkQuery.data, tunnelQuery.data, cyoaQuery.data, otherQuery.data, singleQuery.data])

  useEffect(() => {
    if (!selectedEntity || state.referenceLines.size > 0) return
    const match = allEntities.find(
      e => e.device_pk === selectedEntity.devicePk && e.intf === selectedEntity.intf
    )
    if (match) {
      state.setReferenceLines(match.device_pk, match.intf, {
        p50_bps: match.p50_bps,
        p99_bps: match.max_spike_bps,
        direction: match.peak_direction,
      })
    }
  }, [selectedEntity, allEntities, state.referenceLines.size, state.setReferenceLines])

  const isLoading = isAllMode
    ? linkQuery.isLoading || tunnelQuery.isLoading || cyoaQuery.isLoading || otherQuery.isLoading
    : singleQuery.isLoading

  const renderControls = (total: number) => {
    const totalPages = Math.max(1, Math.ceil(total / limit))
    const start = page * limit + 1
    const end = Math.min((page + 1) * limit, total)

    return (
      <div className="flex items-center justify-between mt-2">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          {total > 0 ? (
            <span>{start}{'\u2013'}{end} of {total}</span>
          ) : (
            <span>No results</span>
          )}
          {totalPages > 1 && (
            <div className="flex items-center gap-1">
              <button
                onClick={() => setPage(p => Math.max(0, p - 1))}
                disabled={page === 0}
                className={cn('px-1.5 py-0.5 rounded transition-colors', page === 0 ? 'opacity-30' : 'hover:bg-muted/50')}
              >
                {'\u2039'}
              </button>
              <button
                onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                disabled={page >= totalPages - 1}
                className={cn('px-1.5 py-0.5 rounded transition-colors', page >= totalPages - 1 ? 'opacity-30' : 'hover:bg-muted/50')}
              >
                {'\u203A'}
              </button>
            </div>
          )}
        </div>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <label className="flex items-center gap-1">
            <span className="text-foreground/60">Baseline</span>
            <select
              value={minBps}
              onChange={e => { setMinBps(Number(e.target.value)); setPage(0) }}
              className="bg-muted border-none rounded px-1.5 py-0.5 text-xs text-foreground cursor-pointer"
            >
              {bpsFilterOptions.map(opt => (
                <option key={opt.value} value={opt.value}>{opt.label}</option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1">
            <span className="text-foreground/60">Peak</span>
            <select
              value={minPeakBps}
              onChange={e => { setMinPeakBps(Number(e.target.value)); setPage(0) }}
              className="bg-muted border-none rounded px-1.5 py-0.5 text-xs text-foreground cursor-pointer"
            >
              {bpsFilterOptions.map(opt => (
                <option key={opt.value} value={opt.value}>{opt.label}</option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1">
            <span className="text-foreground/60">Show</span>
            <select
              value={limit}
              onChange={e => { setLimit(Number(e.target.value)); setPage(0) }}
              className="bg-muted border-none rounded px-1.5 py-0.5 text-xs text-foreground cursor-pointer"
            >
              {pageSizeOptions.map(n => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </label>
        </div>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="h-[200px] flex items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!isAllMode) {
    const entities = singleQuery.data?.entities ?? []
    const total = singleQuery.data?.total ?? 0
    if (entities.length === 0 && page === 0) {
      return (
        <div className="h-[200px] flex items-center justify-center text-sm text-muted-foreground">
          No spikes detected
        </div>
      )
    }
    return (
      <>
        <SpikeTable
          entities={entities}
          state={state}
          sortField={sortField}
          sortDir={sortDir}
          handleSort={handleSort}
          isPlaceholderData={singleQuery.isPlaceholderData}
        />
        {renderControls(total)}
      </>
    )
  }

  // "All" mode — show tabbed sections
  const tabs = [
    { key: 'link' as const, label: 'Links', query: linkQuery },
    { key: 'tunnel' as const, label: 'User Tunnels', query: tunnelQuery },
    { key: 'cyoa' as const, label: 'CYOA', query: cyoaQuery },
    { key: 'other' as const, label: 'Other', query: otherQuery },
  ]

  const allEmpty = tabs.every(t => (t.query.data?.total ?? 0) === 0 && (t.query.data?.entities ?? []).length === 0)
  if (allEmpty && page === 0) {
    return (
      <div className="h-[200px] flex items-center justify-center text-sm text-muted-foreground">
        No spikes detected
      </div>
    )
  }

  const activeQuery = tabs.find(t => t.key === activeTab)?.query
  const activeEntities = activeQuery?.data?.entities ?? []
  const activeTotal = activeQuery?.data?.total ?? 0
  const activePlaceholder = activeQuery?.isPlaceholderData ?? false

  return (
    <>
      <div className="flex items-center gap-1 mb-2 border-b border-border">
        {tabs.map(({ key, label, query }) => {
          const total = query.data?.total ?? 0
          return (
            <button
              key={key}
              onClick={() => { setActiveTab(key); setPage(0) }}
              className={cn(
                'px-3 py-1.5 text-xs font-medium transition-colors relative -mb-px',
                activeTab === key
                  ? 'text-foreground border-b-2 border-foreground'
                  : 'text-muted-foreground hover:text-foreground/70'
              )}
            >
              {label}
              {total > 0 && (
                <span className={cn(
                  'ml-1.5 text-[10px]',
                  activeTab === key ? 'text-muted-foreground' : 'text-muted-foreground/60'
                )}>
                  {total}
                </span>
              )}
            </button>
          )
        })}
      </div>
      <SpikeTable
        entities={activeEntities}
        state={state}
        sortField={sortField}
        sortDir={sortDir}
        handleSort={handleSort}
        isPlaceholderData={activePlaceholder}
      />
      {renderControls(activeTotal)}
    </>
  )
}
