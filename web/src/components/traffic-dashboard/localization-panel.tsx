import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, Loader2 } from 'lucide-react'
import { fetchDashboardStress } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { cn } from '@/lib/utils'

const groupByOptions = [
  { value: 'metro', label: 'Metro' },
  { value: 'device', label: 'Device' },
  { value: 'link_type', label: 'Link Type' },
  { value: 'contributor', label: 'Contributor' },
]

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

function formatPps(val: number): string {
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gpps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mpps'
  if (val >= 1e3) return (val / 1e3).toFixed(1) + ' Kpps'
  return val.toFixed(0) + ' pps'
}

function utilColor(val: number): string {
  if (val >= 0.8) return 'bg-red-500/20 dark:bg-red-500/40 text-red-400'
  if (val >= 0.5) return 'bg-yellow-500/20 dark:bg-yellow-500/40 text-yellow-400'
  return 'bg-green-500/20 dark:bg-green-500/40 text-green-400'
}

export function LocalizationPanel() {
  const state = useDashboard()
  const [dropdownOpen, setDropdownOpen] = useState(false)
  const [limit, setLimit] = useState(10)

  const params = useMemo(() => ({
    ...dashboardFilterParams(state),
    metric: state.metric,
    group_by: state.groupBy,
  }), [state])

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-stress-grouped', params],
    queryFn: () => fetchDashboardStress(params),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
  })

  // Compute aggregate stats per group (average of the time series)
  const { groupStats, totalGroups } = useMemo(() => {
    if (!data?.groups?.length) return { groupStats: [], totalGroups: 0 }
    const all = data.groups.map(g => {
      const p95 = g.p95_in.map((v, i) => Math.max(v, g.p95_out[i] ?? 0))
      const n = p95.length || 1
      const avgP95 = p95.reduce((a, b) => a + b, 0) / n
      const maxP95 = Math.max(...p95)
      const totalStressed = g.stressed_count.reduce((a, b) => a + b, 0)
      return {
        key: g.key,
        label: g.label,
        avgP95,
        maxP95,
        totalStressed,
      }
    }).sort((a, b) => b.avgP95 - a.avgP95)
    return { groupStats: all.slice(0, limit), totalGroups: all.length }
  }, [data, limit])

  const isUtil = state.metric === 'utilization'
  const fmtVal = isUtil ? formatPercent : state.metric === 'packets' ? formatPps : formatRate
  const maxBar = Math.max(...groupStats.map(g => g.avgP95), 0.01)

  const handleGroupClick = (key: string) => {
    const dim = state.groupBy
    switch (dim) {
      case 'metro':
        if (state.metroFilter.includes(key)) {
          state.setMetroFilter(state.metroFilter.filter(f => f !== key))
        } else {
          state.setMetroFilter([...state.metroFilter, key])
        }
        break
      case 'device':
        if (state.deviceFilter.includes(key)) {
          state.setDeviceFilter(state.deviceFilter.filter(f => f !== key))
        } else {
          state.setDeviceFilter([...state.deviceFilter, key])
        }
        break
      case 'link_type':
        if (state.linkTypeFilter.includes(key)) {
          state.setLinkTypeFilter(state.linkTypeFilter.filter(f => f !== key))
        } else {
          state.setLinkTypeFilter([...state.linkTypeFilter, key])
        }
        break
      case 'contributor':
        if (state.contributorFilter.includes(key)) {
          state.setContributorFilter(state.contributorFilter.filter(f => f !== key))
        } else {
          state.setContributorFilter([...state.contributorFilter, key])
        }
        break
    }
  }

  const selectedLabel = groupByOptions.find(o => o.value === state.groupBy)?.label ?? state.groupBy

  return (
    <div>
      <div className="flex items-center justify-end mb-3">
        <div className="relative inline-block">
          <button
            onClick={() => setDropdownOpen(!dropdownOpen)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-border rounded-md bg-background hover:bg-muted transition-colors"
          >
            Group by: <span className="font-medium">{selectedLabel}</span>
            <ChevronDown className="h-3 w-3" />
          </button>
          {dropdownOpen && (
            <>
              <div className="fixed inset-0 z-40" onClick={() => setDropdownOpen(false)} />
              <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[120px]">
                {groupByOptions.map(opt => (
                  <button
                    key={opt.value}
                    onClick={() => { state.setGroupBy(opt.value); setDropdownOpen(false) }}
                    className={cn(
                      'w-full text-left px-3 py-1.5 text-xs transition-colors',
                      opt.value === state.groupBy ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'
                    )}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </>
          )}
        </div>
      </div>

      {isLoading ? (
        <div className="h-[200px] flex items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : groupStats.length === 0 ? (
        <div className="h-[200px] flex items-center justify-center text-sm text-muted-foreground">
          No data available
        </div>
      ) : (
        <div className="space-y-1.5">
          {groupStats.map(g => (
            <button
              key={g.key}
              onClick={() => handleGroupClick(g.key)}
              className="w-full flex items-center gap-3 px-2 py-1.5 rounded hover:bg-muted/50 transition-colors text-left group"
            >
              <span className="w-24 text-xs font-medium truncate" title={g.label}>
                {g.label || g.key}
              </span>
              <div className="flex-1 h-5 bg-muted/30 rounded-sm overflow-hidden relative">
                <div
                  className={cn('h-full rounded-sm transition-all', isUtil ? utilColor(g.avgP95) : 'bg-blue-500/20 dark:bg-blue-500/40 text-blue-400')}
                  style={{ width: `${(g.avgP95 / maxBar) * 100}%` }}
                />
              </div>
              <span className="w-20 text-xs text-right font-mono">
                {fmtVal(g.avgP95)}
              </span>
              {isUtil && g.totalStressed > 0 && (
                <span className="text-xs text-red-400 w-16 text-right">
                  {g.totalStressed} stressed
                </span>
              )}
            </button>
          ))}
        </div>
      )}
      {!isLoading && groupStats.length > 0 && (
        <div className="flex items-center justify-between mt-2">
          <span className="text-xs text-muted-foreground">
            Showing top {groupStats.length}{totalGroups > limit ? ` of ${totalGroups}` : ''}
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
