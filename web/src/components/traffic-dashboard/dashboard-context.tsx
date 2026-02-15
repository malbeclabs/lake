import { createContext, useContext, useMemo, useCallback, type ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'

export type TimeRange = '1h' | '3h' | '6h' | '12h' | '24h' | '3d' | '7d' | '14d' | '30d' | 'custom'
export type IntfType = 'all' | 'link' | 'tunnel' | 'other'

const validIntfTypes: Set<string> = new Set(['all', 'link', 'tunnel', 'other'])

const validTimeRanges: Set<string> = new Set(['1h', '3h', '6h', '12h', '24h', '3d', '7d', '14d', '30d'])

export interface SelectedEntity {
  devicePk: string
  deviceCode: string
  intf?: string
}

export interface DashboardState {
  timeRange: TimeRange
  threshold: number
  metric: 'utilization' | 'throughput' | 'packets'
  groupBy: string
  intfType: IntfType

  // Dimension filters
  metroFilter: string[]
  deviceFilter: string[]
  linkTypeFilter: string[]
  contributorFilter: string[]
  intfFilter: string[]

  // Custom time range (unix seconds)
  customStart: number | null
  customEnd: number | null

  // Selections
  selectedEntity: SelectedEntity | null
  pinnedEntities: SelectedEntity[]

  // Actions
  setTimeRange: (tr: TimeRange) => void
  setThreshold: (t: number) => void
  setMetric: (m: 'utilization' | 'throughput' | 'packets') => void
  setGroupBy: (g: string) => void
  setIntfType: (t: IntfType) => void
  setMetroFilter: (f: string[]) => void
  setDeviceFilter: (f: string[]) => void
  setLinkTypeFilter: (f: string[]) => void
  setContributorFilter: (f: string[]) => void
  setIntfFilter: (f: string[]) => void
  setCustomRange: (start: number, end: number) => void
  clearCustomRange: () => void
  selectEntity: (e: SelectedEntity | null) => void
  pinEntity: (e: SelectedEntity) => void
  unpinEntity: (e: SelectedEntity) => void
  clearFilters: () => void
}

const DashboardContext = createContext<DashboardState | null>(null)

// Serialize a SelectedEntity to URL-safe string: devicePk~deviceCode or devicePk~deviceCode~intf
function serializeEntity(e: SelectedEntity): string {
  return e.intf ? `${e.devicePk}~${e.deviceCode}~${e.intf}` : `${e.devicePk}~${e.deviceCode}`
}

// Parse a serialized entity string back to SelectedEntity
function parseEntity(s: string): SelectedEntity | null {
  const parts = s.split('~')
  if (parts.length < 2) return null
  return {
    devicePk: parts[0],
    deviceCode: parts[1],
    intf: parts.length >= 3 ? parts.slice(2).join('~') : undefined,
  }
}

// Parse a comma-separated list, filtering out empty strings
function parseList(param: string | null): string[] {
  if (!param) return []
  return param.split(',').filter(Boolean)
}

export function DashboardProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()

  // Derive all state from URL search params
  const timeRange = useMemo<TimeRange>(() => {
    // If custom start/end are present, it's a custom range
    if (searchParams.has('start_time') && searchParams.has('end_time')) return 'custom'
    const param = searchParams.get('time_range')
    if (param && validTimeRanges.has(param)) return param as TimeRange
    return '12h'
  }, [searchParams])

  const intfType = useMemo<IntfType>(() => {
    const param = searchParams.get('intf_type')
    if (param && validIntfTypes.has(param)) return param as IntfType
    return 'all'
  }, [searchParams])

  const groupBy = useMemo(() => searchParams.get('group_by') ?? 'device', [searchParams])

  // Force away from utilization when intf_type is non-link (utilization requires bandwidth)
  const metric = useMemo<'utilization' | 'throughput' | 'packets'>(() => {
    const param = searchParams.get('metric')
    if (param === 'packets') return 'packets'
    if (param === 'utilization') {
      if (intfType === 'tunnel' || intfType === 'other') return 'throughput'
      return 'utilization'
    }
    if (param === 'throughput') return 'throughput'
    return 'throughput'
  }, [searchParams, intfType])

  const threshold = useMemo(() => {
    const param = searchParams.get('threshold')
    if (param != null) {
      const n = parseFloat(param)
      if (!isNaN(n)) return n
    }
    return 0.8
  }, [searchParams])

  const customStart = useMemo<number | null>(() => {
    const param = searchParams.get('start_time')
    if (param != null) {
      const n = parseFloat(param)
      if (!isNaN(n)) return n
    }
    return null
  }, [searchParams])

  const customEnd = useMemo<number | null>(() => {
    const param = searchParams.get('end_time')
    if (param != null) {
      const n = parseFloat(param)
      if (!isNaN(n)) return n
    }
    return null
  }, [searchParams])

  const metroFilter = useMemo(() => parseList(searchParams.get('metro')), [searchParams])
  const deviceFilter = useMemo(() => parseList(searchParams.get('device')), [searchParams])
  const linkTypeFilter = useMemo(() => parseList(searchParams.get('link_type')), [searchParams])
  const contributorFilter = useMemo(() => parseList(searchParams.get('contributor')), [searchParams])
  const intfFilter = useMemo(() => parseList(searchParams.get('intf')), [searchParams])

  const selectedEntity = useMemo<SelectedEntity | null>(() => {
    const param = searchParams.get('sel')
    if (!param) return null
    return parseEntity(param)
  }, [searchParams])

  const pinnedEntities = useMemo<SelectedEntity[]>(() => {
    const param = searchParams.get('pinned')
    if (!param) return []
    return param.split(',').map(parseEntity).filter((e): e is SelectedEntity => e !== null)
  }, [searchParams])

  // Setters — each updates URL params, deleting param when at default value

  const handleSetTimeRange = useCallback((tr: TimeRange) => {
    setSearchParams(prev => {
      if (tr !== 'custom') {
        prev.delete('start_time')
        prev.delete('end_time')
      }
      if (tr === '12h') {
        prev.delete('time_range')
      } else {
        prev.set('time_range', tr)
      }
      return prev
    })
  }, [setSearchParams])

  const setThresholdAction = useCallback((t: number) => {
    setSearchParams(prev => {
      if (t === 0.8) {
        prev.delete('threshold')
      } else {
        prev.set('threshold', String(t))
      }
      return prev
    })
  }, [setSearchParams])

  const setMetricAction = useCallback((m: 'utilization' | 'throughput' | 'packets') => {
    setSearchParams(prev => {
      if (m === 'throughput') {
        prev.delete('metric')
      } else {
        prev.set('metric', m)
      }
      return prev
    })
  }, [setSearchParams])

  const setGroupByAction = useCallback((g: string) => {
    setSearchParams(prev => {
      if (g === 'device') {
        prev.delete('group_by')
      } else {
        prev.set('group_by', g)
      }
      return prev
    })
  }, [setSearchParams])

  const setIntfTypeAction = useCallback((t: IntfType) => {
    setSearchParams(prev => {
      if (t === 'all') {
        prev.delete('intf_type')
      } else {
        prev.set('intf_type', t)
      }
      return prev
    })
  }, [setSearchParams])

  const setListParam = useCallback((key: string, values: string[]) => {
    setSearchParams(prev => {
      if (values.length === 0) {
        prev.delete(key)
      } else {
        prev.set(key, values.join(','))
      }
      return prev
    })
  }, [setSearchParams])

  const setMetroFilter = useCallback((f: string[]) => setListParam('metro', f), [setListParam])
  const setDeviceFilter = useCallback((f: string[]) => setListParam('device', f), [setListParam])
  const setLinkTypeFilter = useCallback((f: string[]) => setListParam('link_type', f), [setListParam])
  const setContributorFilter = useCallback((f: string[]) => setListParam('contributor', f), [setListParam])
  const setIntfFilter = useCallback((f: string[]) => setListParam('intf', f), [setListParam])

  const setCustomRange = useCallback((start: number, end: number) => {
    setSearchParams(prev => {
      prev.set('start_time', String(start))
      prev.set('end_time', String(end))
      prev.delete('time_range')
      return prev
    })
  }, [setSearchParams])

  const clearCustomRange = useCallback(() => {
    setSearchParams(prev => {
      prev.delete('start_time')
      prev.delete('end_time')
      // Revert to default time range (12h) — delete param since it's the default
      prev.delete('time_range')
      return prev
    })
  }, [setSearchParams])

  const selectEntity = useCallback((e: SelectedEntity | null) => {
    setSearchParams(prev => {
      if (e === null) {
        prev.delete('sel')
      } else {
        prev.set('sel', serializeEntity(e))
      }
      return prev
    })
  }, [setSearchParams])

  const pinEntity = useCallback((e: SelectedEntity) => {
    setSearchParams(prev => {
      const existing = parseList(prev.get('pinned'))
      const serialized = serializeEntity(e)
      if (existing.includes(serialized)) return prev
      const updated = [...existing, serialized]
      prev.set('pinned', updated.join(','))
      return prev
    })
  }, [setSearchParams])

  const unpinEntity = useCallback((e: SelectedEntity) => {
    setSearchParams(prev => {
      const existing = parseList(prev.get('pinned'))
      const serialized = serializeEntity(e)
      const updated = existing.filter(s => s !== serialized)
      if (updated.length === 0) {
        prev.delete('pinned')
      } else {
        prev.set('pinned', updated.join(','))
      }
      return prev
    })
  }, [setSearchParams])

  const clearFilters = useCallback(() => {
    setSearchParams(prev => {
      prev.delete('metro')
      prev.delete('device')
      prev.delete('link_type')
      prev.delete('contributor')
      prev.delete('intf')
      prev.delete('sel')
      prev.delete('pinned')
      return prev
    })
  }, [setSearchParams])

  return (
    <DashboardContext.Provider
      value={{
        timeRange, threshold, metric, groupBy, intfType,
        metroFilter, deviceFilter, linkTypeFilter, contributorFilter, intfFilter,
        customStart, customEnd,
        selectedEntity, pinnedEntities,
        setTimeRange: handleSetTimeRange, setThreshold: setThresholdAction, setMetric: setMetricAction, setGroupBy: setGroupByAction,
        setIntfType: setIntfTypeAction,
        setMetroFilter, setDeviceFilter, setLinkTypeFilter, setContributorFilter, setIntfFilter,
        setCustomRange, clearCustomRange,
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
    threshold: String(state.threshold),
  }
  if (state.customStart != null && state.customEnd != null) {
    params.start_time = String(state.customStart)
    params.end_time = String(state.customEnd)
  } else {
    params.time_range = state.timeRange
  }
  if (state.intfType !== 'all') params.intf_type = state.intfType
  if (state.metroFilter.length > 0) params.metro = state.metroFilter.join(',')
  if (state.deviceFilter.length > 0) params.device = state.deviceFilter.join(',')
  if (state.linkTypeFilter.length > 0) params.link_type = state.linkTypeFilter.join(',')
  if (state.contributorFilter.length > 0) params.contributor = state.contributorFilter.join(',')
  if (state.intfFilter.length > 0) params.intf = state.intfFilter.join(',')
  return params
}
