import { useState, useMemo } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchDashboardTop, type DashboardTopParams } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { cn } from '@/lib/utils'

function formatRate(val: number): string {
  if (val >= 1e12) return (val / 1e12).toFixed(1) + ' Tbps'
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gbps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mbps'
  if (val >= 1e3) return (val / 1e3).toFixed(0) + ' Kbps'
  return val.toFixed(0) + ' bps'
}

function formatPercent(val: number): string {
  return (val * 100).toFixed(1) + '%'
}

function utilBadgeClass(val: number): string {
  if (val >= 0.8) return 'bg-red-500/15 text-red-400 border-red-500/20'
  if (val >= 0.5) return 'bg-yellow-500/15 text-yellow-400 border-yellow-500/20'
  return 'bg-green-500/15 text-green-400 border-green-500/20'
}

type SortField = 'bandwidth_bps' | 'p95_util' | 'headroom'

export function CapacityPanel() {
  const state = useDashboard()
  const [sortField, setSortField] = useState<SortField>('p95_util')
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
    entity: 'interface' as const,
    metric: sortField as DashboardTopParams['metric'],
    dir: sortDir,
    limit: 10,
  }), [state, sortField, sortDir])

  const { data, isLoading, isFetching } = useQuery({
    queryKey: ['dashboard-capacity', params],
    queryFn: () => fetchDashboardTop(params),
    staleTime: 60_000,
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
          No data available
        </div>
      ) : (
        <div className={cn('overflow-x-auto transition-opacity', isFetching && 'opacity-50')}>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Interface</th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Contributor</th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Link</th>
                <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('bandwidth_bps')}>
                  <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('bandwidth_bps')}>
                    Capacity <SortIcon field="bandwidth_bps" />
                  </button>
                </th>
                <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('p95_util')}>
                  <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('p95_util')}>
                    P95 Util <SortIcon field="p95_util" />
                  </button>
                </th>
                <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('headroom')}>
                  <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('headroom')}>
                    Headroom <SortIcon field="headroom" />
                  </button>
                </th>
              </tr>
            </thead>
            <tbody>
              {entities.map((e, i) => {
                const headroom = e.bandwidth_bps > 0
                  ? (1 - e.p95_util) * e.bandwidth_bps
                  : 0
                const isSelected = state.selectedEntity?.devicePk === e.device_pk &&
                  state.selectedEntity?.intf === e.intf
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
                    <td className="py-1.5 px-2">{e.contributor_code}</td>
                    <td className="py-1.5 px-2">{e.link_type}</td>
                    <td className="py-1.5 px-2 text-right font-mono">
                      {e.bandwidth_bps > 0 ? formatRate(e.bandwidth_bps) : '\u2014'}
                    </td>
                    <td className="py-1.5 px-2 text-right">
                      <span className={cn('px-1.5 py-0.5 rounded text-xs border', utilBadgeClass(e.p95_util))}>
                        {formatPercent(e.p95_util)}
                      </span>
                    </td>
                    <td className="py-1.5 px-2 text-right font-mono">
                      {headroom > 0 ? formatRate(headroom) : '\u2014'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
