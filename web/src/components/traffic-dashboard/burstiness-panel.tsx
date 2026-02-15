import { useState, useMemo } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchDashboardBurstiness } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { cn } from '@/lib/utils'

function formatPercent(val: number): string {
  return (val * 100).toFixed(1) + '%'
}

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

function burstColor(val: number, isLink: boolean): string {
  const high = isLink ? 0.5 : 3
  const med = isLink ? 0.3 : 1.5
  if (val >= high) return 'bg-red-500/15 text-red-400 border-red-500/20'
  if (val >= med) return 'bg-yellow-500/15 text-yellow-400 border-yellow-500/20'
  return 'bg-blue-500/15 text-blue-400 border-blue-500/20'
}

type SortField = 'burstiness' | 'p50_util' | 'p99_util' | 'pct_time_stressed' | 'p50_bps' | 'p99_bps'

export function BurstinessPanel() {
  const state = useDashboard()
  const [limit, setLimit] = useState(20)
  const [sortField, setSortField] = useState<SortField>('burstiness')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDir('desc')
    }
  }

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

  const params = useMemo(() => ({
    ...dashboardFilterParams(state),
    sort: sortField,
    dir: sortDir,
    limit,
  }), [state, sortField, sortDir, limit])

  const { data, isLoading, isFetching } = useQuery({
    queryKey: ['dashboard-burstiness', params],
    queryFn: () => fetchDashboardBurstiness(params),
    staleTime: 30_000,
    placeholderData: keepPreviousData,
  })

  const entities = data?.entities ?? []

  return (
    <div>
      {isLoading ? (
        <div className="h-[200px] flex items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : entities.length === 0 ? (
        <div className="h-[200px] flex items-center justify-center text-sm text-muted-foreground">
          No bursty interfaces detected
        </div>
      ) : (
        <>
          <div className={cn('overflow-x-auto transition-opacity', isFetching && 'opacity-50')}>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Interface</th>
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('p50_util')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('p50_util')}>
                      Typical (P50) <SortIcon field="p50_util" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('p99_util')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('p99_util')}>
                      Peak (P99) <SortIcon field="p99_util" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('burstiness')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('burstiness')}>
                      Spike Gap <SortIcon field="burstiness" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('pct_time_stressed')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('pct_time_stressed')}>
                      % Time &ge; 80% <SortIcon field="pct_time_stressed" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {entities.map((e, i) => {
                  const isSelected = state.selectedEntity?.devicePk === e.device_pk &&
                    state.selectedEntity?.intf === e.intf
                  const isLink = e.bandwidth_bps > 0
                  return (
                    <tr
                      key={`${e.device_pk}-${e.intf}-${i}`}
                      onClick={() => state.selectEntity({
                        devicePk: e.device_pk,
                        deviceCode: e.device_code,
                        intf: e.intf,
                      })}
                      className={cn(
                        'border-b border-border/50 cursor-pointer transition-colors',
                        isSelected ? 'bg-blue-500/10 ring-1 ring-blue-500/30' : 'hover:bg-muted/50'
                      )}
                    >
                      <td className="py-1.5 px-2 font-mono">
                        {e.device_code} <span className="text-muted-foreground">{e.intf}</span>
                      </td>
                      <td className="py-1.5 px-2">{e.metro_code}</td>
                      <td className="py-1.5 px-2 text-right font-mono">
                        {isLink ? formatPercent(e.p50_util) : formatRate(e.p50_bps)}
                      </td>
                      <td className="py-1.5 px-2 text-right font-mono">
                        {isLink ? formatPercent(e.p99_util) : formatRate(e.p99_bps)}
                      </td>
                      <td className="py-1.5 px-2 text-right">
                        <span className={cn('px-1.5 py-0.5 rounded text-xs border', burstColor(e.burstiness, isLink))}>
                          {isLink ? formatPercent(e.burstiness) : formatRatio(e.burstiness)}
                        </span>
                      </td>
                      <td className="py-1.5 px-2 text-right font-mono">
                        {isLink ? formatPercent(e.pct_time_stressed) : '\u2014'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
          <div className="flex items-center justify-between mt-2">
            <span className="text-xs text-muted-foreground">
              Showing top {entities.length}
            </span>
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <span>Show</span>
              {[10, 20, 50].map(n => (
                <button
                  key={n}
                  onClick={() => setLimit(n)}
                  className={cn(
                    'px-1.5 py-0.5 rounded transition-colors',
                    limit === n ? 'bg-muted text-foreground font-medium' : 'hover:bg-muted/50'
                  )}
                >
                  {n}
                </button>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  )
}
