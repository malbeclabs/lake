import { useState, useMemo } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchDashboardHealth } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { cn } from '@/lib/utils'

function formatCount(val: number): string {
  if (val >= 1_000_000) return (val / 1_000_000).toFixed(1) + 'M'
  if (val >= 1_000) return (val / 1_000).toFixed(1) + 'K'
  return val.toLocaleString()
}

function severityColor(val: number): string {
  if (val >= 1000) return 'bg-red-500/15 text-red-400 border-red-500/20'
  if (val >= 100) return 'bg-yellow-500/15 text-yellow-400 border-yellow-500/20'
  return 'bg-blue-500/15 text-blue-400 border-blue-500/20'
}

type SortField = 'total_events' | 'total_errors' | 'total_discards' | 'total_fcs_errors' | 'total_carrier_transitions'

export function HealthPanel() {
  const state = useDashboard()
  const [limit, setLimit] = useState(10)
  const [sortField, setSortField] = useState<SortField>('total_events')
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
    queryKey: ['dashboard-health', params],
    queryFn: () => fetchDashboardHealth(params),
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
          No interface errors detected
        </div>
      ) : (
        <>
          <div className={cn('overflow-x-auto transition-opacity', isFetching && 'opacity-50')}>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Interface</th>
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Contributor</th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('total_errors')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('total_errors')}>
                      Errors <SortIcon field="total_errors" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('total_discards')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('total_discards')}>
                      Discards <SortIcon field="total_discards" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('total_fcs_errors')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('total_fcs_errors')}>
                      FCS Errors <SortIcon field="total_fcs_errors" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('total_carrier_transitions')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('total_carrier_transitions')}>
                      Carrier Trans <SortIcon field="total_carrier_transitions" />
                    </button>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('total_events')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('total_events')}>
                      Total <SortIcon field="total_events" />
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {entities.map((e, i) => {
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
                      <td className="py-1.5 px-2 text-right font-mono">{formatCount(e.total_errors)}</td>
                      <td className="py-1.5 px-2 text-right font-mono">{formatCount(e.total_discards)}</td>
                      <td className="py-1.5 px-2 text-right font-mono">{formatCount(e.total_fcs_errors)}</td>
                      <td className="py-1.5 px-2 text-right font-mono">{formatCount(e.total_carrier_transitions)}</td>
                      <td className="py-1.5 px-2 text-right">
                        <span className={cn('px-1.5 py-0.5 rounded text-xs border font-mono', severityColor(e.total_events))}>
                          {formatCount(e.total_events)}
                        </span>
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
              <span className="font-medium text-foreground/60">Show</span>
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
