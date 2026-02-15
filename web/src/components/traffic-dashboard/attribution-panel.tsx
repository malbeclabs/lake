import { useState, useMemo, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchDashboardTop, type DashboardTopEntity, type DashboardTopParams } from '@/lib/api'
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

type SortField = 'max_util' | 'p95_util' | 'avg_util' | 'max_throughput' | 'max_in_bps' | 'max_out_bps'

function TopTable({
  entityType,
}: {
  entityType: 'device' | 'interface'
}) {
  const state = useDashboard()
  const isDevice = entityType === 'device'
  const isUtil = state.metric === 'utilization'
  const showUtilCols = !isDevice && isUtil
  const [sortField, setSortField] = useState<SortField>(
    (isDevice || !isUtil) ? 'max_in_bps' : 'max_util'
  )
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [limit, setLimit] = useState(10)

  // Reset sort when metric mode changes so we don't sort by a hidden column
  useEffect(() => {
    setSortField((isDevice || state.metric !== 'utilization') ? 'max_in_bps' : 'max_util')
    setSortDir('desc')
  }, [state.metric, isDevice])

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
    entity: entityType,
    metric: sortField as DashboardTopParams['metric'],
    dir: sortDir,
    limit,
  }), [state, entityType, sortField, sortDir, limit])

  const { data, isLoading, isFetching } = useQuery({
    queryKey: ['dashboard-top', params],
    queryFn: () => fetchDashboardTop(params),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
    placeholderData: keepPreviousData,
  })

  const handleRowClick = (entity: DashboardTopEntity) => {
    state.selectEntity({
      devicePk: entity.device_pk,
      deviceCode: entity.device_code,
      intf: entityType === 'interface' ? entity.intf : undefined,
    })
  }

  const entities = data?.entities ?? []

  return (
    <div>
      {isLoading ? (
        <div className="h-[300px] flex items-center justify-center">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        </div>
      ) : entities.length === 0 ? (
        <div className="h-[300px] flex items-center justify-center text-sm text-muted-foreground">
          No data
        </div>
      ) : (
        <div className={cn('overflow-x-auto transition-opacity', isFetching && 'opacity-50')}>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">
                  {isDevice ? 'Device' : 'Interface'}
                </th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Contributor</th>
                {showUtilCols && (
                  <>
                    <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Link</th>
                    <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('max_util')}>
                      <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('max_util')}>
                        Max <SortIcon field="max_util" />
                      </button>
                    </th>
                    <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('p95_util')}>
                      <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('p95_util')}>
                        P95 <SortIcon field="p95_util" />
                      </button>
                    </th>
                    <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('avg_util')}>
                      <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('avg_util')}>
                        Avg <SortIcon field="avg_util" />
                      </button>
                    </th>
                  </>
                )}
                {showUtilCols ? (
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('max_throughput')}>
                    <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('max_throughput')}>
                      Peak <SortIcon field="max_throughput" />
                    </button>
                  </th>
                ) : (
                  <>
                    <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('max_in_bps')}>
                      <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('max_in_bps')}>
                        Peak Rx <SortIcon field="max_in_bps" />
                      </button>
                    </th>
                    <th className="text-right py-1.5 px-2 font-medium text-muted-foreground" aria-sort={sortAria('max_out_bps')}>
                      <button className="inline-flex items-center gap-0.5" onClick={() => handleSort('max_out_bps')}>
                        Peak Tx <SortIcon field="max_out_bps" />
                      </button>
                    </th>
                  </>
                )}
              </tr>
            </thead>
            <tbody>
              {entities.map((e, i) => {
                const isSelected = state.selectedEntity?.devicePk === e.device_pk &&
                  (isDevice || state.selectedEntity?.intf === e.intf)
                return (
                  <tr
                    key={`${e.device_pk}-${e.intf}-${i}`}
                    onClick={() => handleRowClick(e)}
                    className={cn(
                      'border-b border-border/50 cursor-pointer transition-colors',
                      isSelected ? 'bg-blue-500/10 ring-1 ring-blue-500/30' : 'hover:bg-muted/50'
                    )}
                  >
                    <td className="py-1.5 px-2 font-mono">
                      {!isDevice
                        ? <span title={`${e.device_code} ${e.intf}`}>{e.device_code} <span className="text-muted-foreground">{e.intf}</span></span>
                        : e.device_code
                      }
                    </td>
                    <td className="py-1.5 px-2">{e.metro_code}</td>
                    <td className="py-1.5 px-2">{e.contributor_code}</td>
                    {showUtilCols && (
                      <>
                        <td className="py-1.5 px-2">{e.link_type}</td>
                        <td className="py-1.5 px-2">
                          <span className={cn('px-1.5 py-0.5 rounded text-xs border', utilBadgeClass(e.max_util))}>
                            {formatPercent(e.max_util)}
                          </span>
                        </td>
                        <td className="py-1.5 px-2">
                          <span className={cn('px-1.5 py-0.5 rounded text-xs border', utilBadgeClass(e.p95_util))}>
                            {formatPercent(e.p95_util)}
                          </span>
                        </td>
                        <td className="py-1.5 px-2">
                          <span className={cn('px-1.5 py-0.5 rounded text-xs border', utilBadgeClass(e.avg_util))}>
                            {formatPercent(e.avg_util)}
                          </span>
                        </td>
                      </>
                    )}
                    {showUtilCols ? (
                      <td className="py-1.5 px-2 font-mono text-right">
                        {formatRate(Math.max(e.max_in_bps, e.max_out_bps))}
                      </td>
                    ) : (
                      <>
                        <td className="py-1.5 px-2 font-mono text-right">
                          {formatRate(e.max_in_bps)}
                        </td>
                        <td className="py-1.5 px-2 font-mono text-right">
                          {formatRate(e.max_out_bps)}
                        </td>
                      </>
                    )}
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
      {!isLoading && entities.length > 0 && (
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
      )}
    </div>
  )
}

export function TopDevicesPanel() {
  return <TopTable entityType="device" />
}

export function TopInterfacesPanel() {
  return <TopTable entityType="interface" />
}
