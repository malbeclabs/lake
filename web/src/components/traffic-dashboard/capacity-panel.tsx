import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { fetchDashboardTop } from '@/lib/api'
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

export function CapacityPanel() {
  const state = useDashboard()

  const params = useMemo(() => ({
    ...dashboardFilterParams(state),
    entity: 'interface' as const,
    metric: 'p95_util' as const,
    limit: 20,
  }), [state])

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-capacity', params],
    queryFn: () => fetchDashboardTop(params),
    staleTime: 60_000,
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
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Interface</th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
                <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Link</th>
                <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">Capacity</th>
                <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">P95 Util</th>
                <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">Headroom</th>
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
                    <td className="py-1.5 px-2">{e.link_type}</td>
                    <td className="py-1.5 px-2 text-right font-mono">
                      {e.bandwidth_bps > 0 ? formatRate(e.bandwidth_bps) : '—'}
                    </td>
                    <td className="py-1.5 px-2 text-right">
                      <span className={cn('px-1.5 py-0.5 rounded text-xs border', utilBadgeClass(e.p95_util))}>
                        {formatPercent(e.p95_util)}
                      </span>
                    </td>
                    <td className="py-1.5 px-2 text-right font-mono">
                      {headroom > 0 ? formatRate(headroom) : '—'}
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
