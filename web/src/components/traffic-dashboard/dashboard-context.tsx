import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'

export type TimeRange = '1h' | '3h' | '6h' | '12h' | '24h' | '3d' | '7d' | '14d' | '30d'

export interface SelectedEntity {
  devicePk: string
  deviceCode: string
  intf?: string
}

export interface DashboardState {
  timeRange: TimeRange
  threshold: number
  metric: 'utilization' | 'throughput'
  groupBy: string

  // Dimension filters
  metroFilter: string[]
  deviceFilter: string[]
  linkTypeFilter: string[]
  contributorFilter: string[]

  // Selections
  selectedEntity: SelectedEntity | null
  pinnedEntities: SelectedEntity[]

  // Actions
  setTimeRange: (tr: TimeRange) => void
  setThreshold: (t: number) => void
  setMetric: (m: 'utilization' | 'throughput') => void
  setGroupBy: (g: string) => void
  setMetroFilter: (f: string[]) => void
  setDeviceFilter: (f: string[]) => void
  setLinkTypeFilter: (f: string[]) => void
  setContributorFilter: (f: string[]) => void
  selectEntity: (e: SelectedEntity | null) => void
  pinEntity: (e: SelectedEntity) => void
  unpinEntity: (e: SelectedEntity) => void
  clearFilters: () => void
}

const DashboardContext = createContext<DashboardState | null>(null)

export function DashboardProvider({ children }: { children: ReactNode }) {
  const [timeRange, setTimeRange] = useState<TimeRange>(() => {
    const saved = localStorage.getItem('traffic-dashboard-time-range')
    return (saved as TimeRange) || '12h'
  })
  const [threshold, setThreshold] = useState(0.8)
  const [metric, setMetric] = useState<'utilization' | 'throughput'>('throughput')
  const [groupBy, setGroupBy] = useState('device')

  const [metroFilter, setMetroFilter] = useState<string[]>([])
  const [deviceFilter, setDeviceFilter] = useState<string[]>([])
  const [linkTypeFilter, setLinkTypeFilter] = useState<string[]>([])
  const [contributorFilter, setContributorFilter] = useState<string[]>([])

  const [selectedEntity, setSelectedEntity] = useState<SelectedEntity | null>(null)
  const [pinnedEntities, setPinnedEntities] = useState<SelectedEntity[]>([])

  const handleSetTimeRange = useCallback((tr: TimeRange) => {
    setTimeRange(tr)
    localStorage.setItem('traffic-dashboard-time-range', tr)
  }, [])

  const selectEntity = useCallback((e: SelectedEntity | null) => {
    setSelectedEntity(e)
  }, [])

  const pinEntity = useCallback((e: SelectedEntity) => {
    setPinnedEntities(prev => {
      const key = e.devicePk + (e.intf || '')
      if (prev.some(p => p.devicePk + (p.intf || '') === key)) return prev
      return [...prev, e]
    })
  }, [])

  const unpinEntity = useCallback((e: SelectedEntity) => {
    setPinnedEntities(prev =>
      prev.filter(p => p.devicePk + (p.intf || '') !== e.devicePk + (e.intf || ''))
    )
  }, [])

  const clearFilters = useCallback(() => {
    setMetroFilter([])
    setDeviceFilter([])
    setLinkTypeFilter([])
    setContributorFilter([])
    setSelectedEntity(null)
    setPinnedEntities([])
  }, [])

  return (
    <DashboardContext.Provider
      value={{
        timeRange, threshold, metric, groupBy,
        metroFilter, deviceFilter, linkTypeFilter, contributorFilter,
        selectedEntity, pinnedEntities,
        setTimeRange: handleSetTimeRange, setThreshold, setMetric, setGroupBy,
        setMetroFilter, setDeviceFilter, setLinkTypeFilter, setContributorFilter,
        selectEntity, pinEntity, unpinEntity, clearFilters,
      }}
    >
      {children}
    </DashboardContext.Provider>
  )
}

export function useDashboard() {
  const ctx = useContext(DashboardContext)
  if (!ctx) throw new Error('useDashboard must be used within DashboardProvider')
  return ctx
}

// Helper to build common query params from dashboard state
export function dashboardFilterParams(state: DashboardState): Record<string, string> {
  const params: Record<string, string> = {
    time_range: state.timeRange,
    threshold: String(state.threshold),
  }
  if (state.metroFilter.length > 0) params.metro = state.metroFilter.join(',')
  if (state.deviceFilter.length > 0) params.device = state.deviceFilter.join(',')
  if (state.linkTypeFilter.length > 0) params.link_type = state.linkTypeFilter.join(',')
  if (state.contributorFilter.length > 0) params.contributor = state.contributorFilter.join(',')
  return params
}
