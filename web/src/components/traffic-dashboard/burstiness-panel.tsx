import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { fetchDashboardBurstiness } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { cn } from '@/lib/utils'

function formatPercent(val: number): string {
  return (val * 100).toFixed(1) + '%'
}

function burstColor(val: number): string {
  if (val >= 0.5) return 'bg-red-500/15 text-red-400 border-red-500/20'
  if (val >= 0.3) return 'bg-yellow-500/15 text-yellow-400 border-yellow-500/20'
  return 'bg-blue-500/15 text-blue-400 border-blue-500/20'
}

export function BurstinessPanel() {
  const state = useDashboard()
  const [limit, setLimit] = useState(20)

  const params = useMemo(() => ({
    ...dashboardFilterParams(state),
    limit,
  }), [state, limit])

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-burstiness', params],
    queryFn: () => fetchDashboardBurstiness(params),
    staleTime: 30_000,
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
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Interface</th>
                  <th className="text-left py-1.5 px-2 font-medium text-muted-foreground">Metro</th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">Typical (P50)</th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">Peak (P99)</th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">Spike Gap</th>
                  <th className="text-right py-1.5 px-2 font-medium text-muted-foreground">% Time &ge; 80%</th>
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
                      <td className="py-1.5 px-2 text-right font-mono">{formatPercent(e.p50_util)}</td>
                      <td className="py-1.5 px-2 text-right font-mono">{formatPercent(e.p99_util)}</td>
                      <td className="py-1.5 px-2 text-right">
                        <span className={cn('px-1.5 py-0.5 rounded text-xs border', burstColor(e.burstiness))}>
                          {formatPercent(e.burstiness)}
                        </span>
                      </td>
                      <td className="py-1.5 px-2 text-right font-mono">{formatPercent(e.pct_time_stressed)}</td>
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
