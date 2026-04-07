import { useState, useEffect, useRef, useMemo, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { ChevronDown, Network, Sigma } from 'lucide-react'
import { fetchTrafficData, fetchTopology, type TrafficPoint, type SeriesInfo } from '@/lib/api'
import { TrafficChart } from '@/components/traffic-chart-uplot'
import { DashboardProvider, useDashboard, dashboardFilterParams, resolveAutoBucket } from '@/components/traffic-dashboard/dashboard-context'
import { DashboardFilters, DashboardFilterBadges } from '@/components/traffic-dashboard/dashboard-filters'
import { PageHeader } from '@/components/page-header'

export interface LinkLookupInfo {
  pk: string
  code: string
  bandwidth_bps: number
  side_a_pk: string
  side_a_code: string
  side_z_pk: string
  side_z_code: string
}

// Lazy chart wrapper that only renders when in viewport
function LazyChart({ children, height = 600 }: { children: React.ReactNode; height?: number }) {
  const [isVisible, setIsVisible] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true)
          observer.disconnect()
        }
      },
      { rootMargin: '100px' } // Start loading 100px before visible
    )

    if (ref.current) {
      observer.observe(ref.current)
    }

    return () => observer.disconnect()
  }, [])

  return (
    <div ref={ref} style={{ minHeight: height }}>
      {isVisible ? children : (
        <div className="animate-pulse bg-muted rounded h-full" />
      )}
    </div>
  )
}

type AggMethod = 'max' | 'avg' | 'min' | 'p50' | 'p90' | 'p95' | 'p99'

const aggLabels: Record<AggMethod, string> = {
  'max': 'Max',
  'p99': 'P99',
  'p95': 'P95',
  'p90': 'P90',
  'p50': 'P50',
  'avg': 'Average',
  'min': 'Min',
}

type ChartSection = 'tunnel-stacked' | 'tunnel' | 'link-stacked' | 'link' | 'cyoa-stacked' | 'cyoa' | 'other-stacked' | 'other' | 'discards'

const ALL_KNOWN_SECTIONS: ChartSection[] = ['tunnel-stacked', 'tunnel', 'link-stacked', 'link', 'cyoa-stacked', 'cyoa', 'other-stacked', 'other', 'discards']

type IntfCategory = 'tunnel' | 'link' | 'cyoa' | 'other'

const SECTION_CATEGORY: Partial<Record<ChartSection, IntfCategory>> = {
  'tunnel-stacked': 'tunnel', 'tunnel': 'tunnel',
  'link-stacked': 'link', 'link': 'link',
  'cyoa-stacked': 'cyoa', 'cyoa': 'cyoa',
  'other-stacked': 'other', 'other': 'other',
}

type Layout = '1x4' | '2x2'

const layoutLabels: Record<Layout, string> = {
  '1x4': '1',
  '2x2': '2',
}



function AggSelector({
  value,
  onChange,
}: {
  value: AggMethod
  onChange: (value: AggMethod) => void
}) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
      >
        Agg: {aggLabels[value]}
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[140px]">
            {(['max', 'p99', 'p95', 'p90', 'p50', 'avg', 'min'] as AggMethod[]).map((agg) => (
              <button
                key={agg}
                onClick={() => {
                  onChange(agg)
                  setIsOpen(false)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  value === agg
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                }`}
              >
                {aggLabels[agg]}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function LayoutSelector({
  value,
  onChange,
}: {
  value: Layout
  onChange: (value: Layout) => void
}) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
      >
        Columns: {layoutLabels[value]}
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[180px]">
            {(['1x4', '2x2'] as Layout[]).map((layout) => (
              <button
                key={layout}
                onClick={() => {
                  onChange(layout)
                  setIsOpen(false)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  value === layout
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                }`}
              >
                {layoutLabels[layout]}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// Compute percentile from a sorted array (linear interpolation)
function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  if (sorted.length === 1) return sorted[0]
  const idx = (p / 100) * (sorted.length - 1)
  const lo = Math.floor(idx)
  const hi = Math.ceil(idx)
  if (lo === hi) return sorted[lo]
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo)
}

// Aggregate per-interface traffic data into P50/P95/Max statistical series
function aggregateTrafficData(
  points: TrafficPoint[],
  series: SeriesInfo[],
): { points: TrafficPoint[]; series: SeriesInfo[] } {
  if (!points.length) return { points: [], series: [] }

  // Group values by timestamp
  const byTime = new Map<string, { inVals: number[]; outVals: number[] }>()
  for (const p of points) {
    let entry = byTime.get(p.time)
    if (!entry) {
      entry = { inVals: [], outVals: [] }
      byTime.set(p.time, entry)
    }
    entry.inVals.push(p.in_bps)
    entry.outVals.push(p.out_bps)
  }

  // Pick a representative device_pk from the first series for the synthetic points
  const refDevicePk = series[0]?.device ?? 'aggregate'

  const stats = ['P50', 'P95', 'Max'] as const
  const aggPoints: TrafficPoint[] = []

  for (const [time, { inVals, outVals }] of byTime) {
    const sortedIn = [...inVals].sort((a, b) => a - b)
    const sortedOut = [...outVals].sort((a, b) => a - b)

    const computed = {
      P50: { in: percentile(sortedIn, 50), out: percentile(sortedOut, 50) },
      P95: { in: percentile(sortedIn, 95), out: percentile(sortedOut, 95) },
      Max: { in: sortedIn[sortedIn.length - 1] ?? 0, out: sortedOut[sortedOut.length - 1] ?? 0 },
    }

    for (const stat of stats) {
      aggPoints.push({
        time,
        device_pk: refDevicePk,
        device: stat,
        intf: '',
        in_bps: computed[stat].in,
        out_bps: computed[stat].out,
        in_discards: 0, out_discards: 0, in_errors: 0, out_errors: 0,
        in_fcs_errors: 0, carrier_transitions: 0,
      })
    }
  }

  const aggSeries: SeriesInfo[] = stats.flatMap(stat => [
    { key: `${stat} (in)`, device: stat, intf: '', direction: 'in', mean: 0 },
    { key: `${stat} (out)`, device: stat, intf: '', direction: 'out', mean: 0 },
  ])

  return { points: aggPoints, series: aggSeries }
}

// Aggregate per-interface traffic data into a single Total series (sum across interfaces).
// Used for stacked charts where percentile lines don't make sense.
function aggregateTrafficDataTotal(
  points: TrafficPoint[],
  series: SeriesInfo[],
): { points: TrafficPoint[]; series: SeriesInfo[] } {
  if (!points.length) return { points: [], series: [] }

  const byTime = new Map<string, { inSum: number; outSum: number }>()
  for (const p of points) {
    let entry = byTime.get(p.time)
    if (!entry) {
      entry = { inSum: 0, outSum: 0 }
      byTime.set(p.time, entry)
    }
    entry.inSum += p.in_bps
    entry.outSum += p.out_bps
  }

  const refDevicePk = series[0]?.device ?? 'aggregate'
  const aggPoints: TrafficPoint[] = []
  for (const [time, { inSum, outSum }] of byTime) {
    aggPoints.push({
      time,
      device_pk: refDevicePk,
      device: 'Total',
      intf: '',
      in_bps: inSum,
      out_bps: outSum,
      in_discards: 0, out_discards: 0, in_errors: 0, out_errors: 0,
      in_fcs_errors: 0, carrier_transitions: 0,
    })
  }

  const aggSeries: SeriesInfo[] = [
    { key: 'Total (in)', device: 'Total', intf: '', direction: 'in', mean: 0 },
    { key: 'Total (out)', device: 'Total', intf: '', direction: 'out', mean: 0 },
  ]

  return { points: aggPoints, series: aggSeries }
}

function TrafficPageContent() {
  const dashboardState = useDashboard()
  const { timeRange, intfType, metric, customStart, customEnd } = dashboardState

  const timeRangeSeconds = useMemo(() => {
    if (customStart && customEnd) return customEnd - customStart
    const map: Record<string, number> = {
      '1h': 3600, '3h': 10800, '6h': 21600, '12h': 43200, '24h': 86400,
      '3d': 259200, '7d': 604800, '14d': 1209600, '30d': 2592000,
    }
    return map[timeRange] || 86400
  }, [timeRange, customStart, customEnd])

  const [searchParams, setSearchParams] = useSearchParams()

  const aggMethod = useMemo<AggMethod>(() => {
    const param = searchParams.get('agg')
    if (param && param in aggLabels) return param as AggMethod
    return 'max'
  }, [searchParams])

  const setAggMethod = useCallback((m: AggMethod) => {
    setSearchParams(prev => {
      if (m === 'max') { prev.delete('agg') } else { prev.set('agg', m) }
      return prev
    })
  }, [setSearchParams])

  // aggregate param: absent = auto, 'on' = force all on, 'off' = force all off
  const aggregateOverride = useMemo<boolean | null>(() => {
    const param = searchParams.get('aggregate')
    if (param === 'on') return true
    if (param === 'off') return false
    return null
  }, [searchParams])

  const setAggregateOverride = useCallback((v: boolean | null) => {
    setSearchParams(prev => {
      if (v === null) { prev.delete('aggregate') }
      else { prev.set('aggregate', v ? 'on' : 'off') }
      return prev
    })
  }, [setSearchParams])

  const [aggregateProcessing, setAggregateProcessing] = useState(false)

  const toggleAggregate = useCallback(() => {
    setAggregateProcessing(true)
    // Defer the actual change so the processing state renders first
    requestAnimationFrame(() => {
      const current = searchParams.get('aggregate')
      if (current === null) setAggregateOverride(false)
      else if (current === 'off') setAggregateOverride(true)
      else setAggregateOverride(null)
    })
  }, [searchParams, setAggregateOverride])

  const [layout, setLayout] = useState<Layout>('2x2')
  const [bidirectional, setBidirectional] = useState(true)

  // Load layout from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('traffic-layout')
    if (saved && (saved === '1x4' || saved === '2x2')) {
      setLayout(saved as Layout)
    }
  }, [])

  // Save layout to localStorage
  const handleLayoutChange = (newLayout: Layout) => {
    setLayout(newLayout)
    localStorage.setItem('traffic-layout', newLayout)
  }

  // Compute actual bucket size to send to API
  const actualBucketSize = useMemo(() => {
    if (dashboardState.bucket === 'auto') {
      return resolveAutoBucket(timeRange)
    }
    return dashboardState.bucket
  }, [dashboardState.bucket, timeRange])


  // Determine which chart categories to show based on intf type filter
  const showCategory: Record<IntfCategory, boolean> = {
    tunnel: intfType === 'all' || intfType === 'tunnel',
    link: intfType === 'all' || intfType === 'link',
    cyoa: intfType === 'all' || intfType === 'cyoa',
    other: intfType === 'all' || intfType === 'other',
  }

  // Build dimension filter params from dashboard state.
  // When intfType is 'all', intf_type is omitted and the per-chart tunnel_only
  // param handles the category split. When a specific type is selected
  // (link/tunnel/other), intf_type is passed through so the server filters to
  // only that interface type.
  const filterParams = useMemo(() => {
    return dashboardFilterParams(dashboardState)
  }, [dashboardState])

  // Single query: when intfType is 'all', fetch everything and split client-side.
  // When intfType is specific, filterParams already has intf_type so the server filters.
  const {
    data: allTrafficData,
    isLoading: trafficLoading,
    isFetching: trafficFetching,
    error: trafficError,
  } = useQuery({
    queryKey: ['traffic-intf', timeRange, actualBucketSize, aggMethod, filterParams, metric],
    queryFn: () => fetchTrafficData(timeRange, null, actualBucketSize, aggMethod, filterParams, metric),
    staleTime: 30000,
    refetchInterval: dashboardState.refetchInterval,
  })

  // Build a lookup from series metadata to classify each device+intf pair
  const intfCategoryMap = useMemo(() => {
    const map = new Map<string, IntfCategory>()
    if (!allTrafficData) return map
    for (const s of allTrafficData.series) {
      if (s.direction !== 'in') continue // only need one direction per intf
      const key = `${s.device}:${s.intf}`
      if (s.intf.startsWith('Tunnel')) {
        map.set(key, 'tunnel')
      } else if (s.cyoa_type && s.cyoa_type !== 'none' && s.cyoa_type !== '') {
        map.set(key, 'cyoa')
      } else if (s.link_pk) {
        map.set(key, 'link')
      } else {
        map.set(key, 'other')
      }
    }
    return map
  }, [allTrafficData])

  // Per-category interface counts for auto-detect
  const categoryCounts = useMemo(() => {
    const counts: Record<IntfCategory, number> = { tunnel: 0, link: 0, cyoa: 0, other: 0 }
    for (const cat of intfCategoryMap.values()) {
      counts[cat]++
    }
    return counts
  }, [intfCategoryMap])

  // Reset override when filters change the interface count (but not on initial data load)
  const totalCount = intfCategoryMap.size
  const prevTotalCount = useRef(totalCount)
  useEffect(() => {
    if (prevTotalCount.current !== 0 && totalCount !== prevTotalCount.current && aggregateOverride !== null) {
      setAggregateOverride(null)
    }
    prevTotalCount.current = totalCount
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [totalCount])

  // Resolve whether a given category should be aggregated
  const shouldAggregate = useCallback((cat: IntfCategory): boolean => {
    if (aggregateOverride !== null) return aggregateOverride
    const count = intfType !== 'all' ? intfCategoryMap.size : categoryCounts[cat]
    return count > 10
  }, [aggregateOverride, intfType, intfCategoryMap.size, categoryCounts])

  // For the sigma button display: true = forced on, false = forced off, null = auto
  const anyAutoAggregate = useMemo(() =>
    Object.values(categoryCounts).some(c => c > 10),
  [categoryCounts])

  // Split data by interface category client-side
  const categoryData = useMemo(() => {
    if (!allTrafficData || intfType !== 'all') {
      return { tunnel: allTrafficData, link: allTrafficData, cyoa: allTrafficData, other: allTrafficData }
    }
    const filterByCategory = (cat: IntfCategory) => {
      const match = (device: string, intf: string) => (intfCategoryMap.get(`${device}:${intf}`) ?? 'other') === cat
      return {
        ...allTrafficData,
        points: allTrafficData.points.filter(p => match(p.device, p.intf)),
        series: allTrafficData.series.filter(s => match(s.device, s.intf)),
        discards_series: allTrafficData.discards_series.filter(s => match(s.device, s.intf)),
      }
    }
    return {
      tunnel: filterByCategory('tunnel'),
      link: filterByCategory('link'),
      cyoa: filterByCategory('cyoa'),
      other: filterByCategory('other'),
    }
  }, [allTrafficData, intfType, intfCategoryMap])

  // When aggregate mode is on, compute stats across interfaces per category.
  // Non-stacked charts get P50/P95/Max lines; stacked charts get a single Total sum.
  const aggHelper = useCallback((data: typeof allTrafficData, fn: typeof aggregateTrafficData) => {
    if (!data) return data
    const { points, series } = fn(data.points, data.series)
    return { ...data, points, series }
  }, [])

  const aggregatedCategoryData = useMemo(() => {
    const maybeAgg = (cat: IntfCategory) =>
      shouldAggregate(cat) ? aggHelper(categoryData[cat], aggregateTrafficData) : categoryData[cat]
    return { tunnel: maybeAgg('tunnel'), link: maybeAgg('link'), cyoa: maybeAgg('cyoa'), other: maybeAgg('other') }
  }, [categoryData, shouldAggregate, aggHelper])

  const aggregatedCategoryDataStacked = useMemo(() => {
    const maybeAgg = (cat: IntfCategory) =>
      shouldAggregate(cat) ? aggHelper(categoryData[cat], aggregateTrafficDataTotal) : categoryData[cat]
    return { tunnel: maybeAgg('tunnel'), link: maybeAgg('link'), cyoa: maybeAgg('cyoa'), other: maybeAgg('other') }
  }, [categoryData, shouldAggregate, aggHelper])

  // Keep processing indicator visible until the browser is idle (charts done painting)
  useEffect(() => {
    if (!aggregateProcessing) return
    // requestIdleCallback fires after the browser has finished layout/paint work
    if ('requestIdleCallback' in window) {
      const id = requestIdleCallback(() => setAggregateProcessing(false))
      return () => cancelIdleCallback(id)
    }
    // Fallback: wait a generous amount for chart rendering
    const t = setTimeout(() => setAggregateProcessing(false), 2000)
    return () => clearTimeout(t)
  }, [aggregateProcessing])

  // Fetch topology data for link metadata
  const {
    data: topologyData,
  } = useQuery({
    queryKey: ['topology'],
    queryFn: () => fetchTopology(),
    staleTime: 60000,
    refetchInterval: dashboardState.refetchInterval,
  })

  // Derive interface counters (errors/discards/fcs/carrier) from the traffic response.
  // Transforms counter fields into TrafficPoint-compatible format for the TrafficChart.
  const countersData = useMemo(() => {
    if (!allTrafficData) return null
    // Sum all counter types per interface to find which interfaces have any events
    const intfTotals = new Map<string, { device: string; devicePk: string; intf: string; total: number }>()
    for (const p of allTrafficData.points) {
      const total = p.in_discards + p.out_discards + p.in_errors + p.out_errors + p.in_fcs_errors + p.carrier_transitions
      if (total === 0) continue
      const key = `${p.device}-${p.intf}`
      const existing = intfTotals.get(key)
      if (existing) {
        existing.total += total
      } else {
        intfTotals.set(key, { device: p.device, devicePk: p.device_pk, intf: p.intf, total })
      }
    }
    if (intfTotals.size === 0) return null

    // Build points using combined counters as in/out values:
    // in = in_discards + in_errors + in_fcs_errors + carrier_transitions
    // out = out_discards + out_errors (negated for bidirectional)
    const points = allTrafficData.points
      .filter(p => {
        const total = p.in_discards + p.out_discards + p.in_errors + p.out_errors + p.in_fcs_errors + p.carrier_transitions
        return total > 0
      })
      .map(p => ({
        ...p,
        in_bps: p.in_discards + p.in_errors + p.in_fcs_errors + p.carrier_transitions,
        out_bps: p.out_discards + p.out_errors,
      }))

    // Build a lookup for link_pk/cyoa_type from the traffic series
    const seriesMeta = new Map<string, { link_pk?: string; cyoa_type?: string }>()
    for (const s of allTrafficData.series) {
      if (s.direction === 'in') {
        seriesMeta.set(`${s.device}-${s.intf}`, { link_pk: s.link_pk, cyoa_type: s.cyoa_type })
      }
    }

    // Build series info sorted by total events
    const sorted = [...intfTotals.entries()].sort((a, b) => b[1].total - a[1].total)
    const series = sorted.flatMap(([, info]) => {
      const meta = seriesMeta.get(`${info.device}-${info.intf}`)
      return [
        { key: `${info.device}-${info.intf} (in)`, device: info.device, intf: info.intf, direction: 'in' as const, mean: 0, link_pk: meta?.link_pk, cyoa_type: meta?.cyoa_type },
        { key: `${info.device}-${info.intf} (out)`, device: info.device, intf: info.intf, direction: 'out' as const, mean: 0, link_pk: meta?.link_pk, cyoa_type: meta?.cyoa_type },
      ]
    })

    return { points, series }
  }, [allTrafficData])
  const countersLoading = trafficLoading
  const countersFetching = trafficFetching

  // Build link lookup: device_pk + interface -> link info
  const linkLookup = useMemo(() => {
    if (!topologyData?.links) return new Map<string, LinkLookupInfo>()

    const map = new Map<string, LinkLookupInfo>()
    for (const link of topologyData.links) {
      const linkInfo: LinkLookupInfo = {
        pk: link.pk,
        code: link.code,
        bandwidth_bps: link.bandwidth_bps,
        side_a_pk: link.side_a_pk,
        side_a_code: link.side_a_code,
        side_z_pk: link.side_z_pk,
        side_z_code: link.side_z_code,
      }

      // Map side A device+interface to link
      map.set(`${link.side_a_pk}:${link.side_a_iface_name}`, linkInfo)
      // Map side Z device+interface to link
      map.set(`${link.side_z_pk}:${link.side_z_iface_name}`, linkInfo)
    }

    return map
  }, [topologyData])

  // Check if a section should be shown based on intf type
  const isSectionAllowed = (section: ChartSection): boolean => {
    const cat = SECTION_CATEGORY[section]
    if (cat) return showCategory[cat]
    return true // discards always shown
  }

  // Render a chart section
  const renderChartSection = (section: ChartSection) => {
    if (!isSectionAllowed(section)) return null

    // Handle counters chart (errors/discards/fcs/carrier) separately
    if (section === 'discards') {
      if (!countersData && !countersLoading) {
        return (
          <div key={section} className="border border-border rounded-lg p-4 flex items-center justify-center h-[400px]">
            <p className="text-sm text-muted-foreground">No errors or discards in the selected time range</p>
          </div>
        )
      }
      return (
        <div key={section} className="border border-border rounded-lg p-4">
          <LazyChart key={`${section}-${layout}`}>
            <TrafficChart
              title="Interface Errors & Discards"
              data={countersData?.points || []}
              series={countersData?.series || []}
              bidirectional={bidirectional}
              onTimeRangeSelect={dashboardState.setCustomRange}
              metric="counters"
              loading={countersFetching}
              timeRangeSeconds={timeRangeSeconds}
            />
          </LazyChart>
        </div>
      )
    }

    const cat = SECTION_CATEGORY[section]
    if (!cat) return null
    const stacked = section.endsWith('-stacked')

    const categoryLabels: Record<IntfCategory, string> = {
      tunnel: 'Tunnel',
      link: 'Link',
      cyoa: 'CYOA',
      other: 'Other',
    }
    const typeLabel = categoryLabels[cat]
    const metricLabel = metric === 'packets' ? 'Packets' : 'Traffic'
    const catAggregated = shouldAggregate(cat)
    const title = catAggregated
      ? `${typeLabel} ${metricLabel}${stacked ? ' Total' : ' Summary'}${stacked ? ' (stacked)' : ''}`
      : `${typeLabel} ${metricLabel} Per Device & Interface${stacked ? ' (stacked)' : ''}`

    const data = stacked ? aggregatedCategoryDataStacked[cat] : aggregatedCategoryData[cat]
    const fetching = trafficFetching
    const error = trafficError

    // Count original interfaces for this category (in-direction series = one per interface)
    const rawSeries = categoryData[cat]?.series
    const originalIntfCount = rawSeries ? rawSeries.filter(s => s.direction === 'in').length : 0

    return (
      <div key={section} className="border border-border rounded-lg p-4">
        <LazyChart key={section}>
          {error ? (
            <div className="flex flex-col space-y-2">
              <div className="flex items-center gap-2">
                <h3 className="text-lg font-semibold">{title}</h3>
              </div>
              <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
                <p className="text-muted-foreground">Error: {(error as Error).message || String(error)}</p>
              </div>
            </div>
          ) : (
            <TrafficChart
              title={title}
              data={data?.points || []}
              series={data?.series || []}
              stacked={stacked}
              linkLookup={linkLookup}
              bidirectional={bidirectional}
              onTimeRangeSelect={dashboardState.setCustomRange}
              metric={metric}
              loading={fetching}
              timeRangeSeconds={timeRangeSeconds}
              legendHeader={catAggregated ? `Summary of ${originalIntfCount} interfaces` : undefined}
            />
          )}
        </LazyChart>
      </div>
    )
  }

  const gridClass = layout === '2x2' ? 'grid grid-cols-1 lg:grid-cols-2 gap-6' : 'space-y-6'

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Sticky header */}
      <div className="flex-none bg-background border-b border-border px-4 sm:px-8 pt-6 pb-4 z-10">
        <div className="[&>div]:mb-0">
          <PageHeader
            icon={Network}
            title="Interfaces"
            actions={<DashboardFilters excludeMetrics={['utilization']} />}
          />
        </div>
        <div className="flex items-center gap-3 mt-3">
          <div className="flex items-center gap-3 flex-shrink-0 ml-auto">
            <DashboardFilterBadges />
            <button
              onClick={toggleAggregate}
              className={`px-2 border rounded-md transition-colors inline-flex items-center justify-center h-[34px] gap-1 ${
                aggregateOverride === true
                  ? 'border-foreground/30 text-foreground bg-muted'
                  : aggregateOverride === null && anyAutoAggregate
                    ? 'border-foreground/20 text-foreground/70 bg-muted/50'
                    : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
              }`}
              title={
                aggregateOverride === true
                  ? 'All charts aggregated. Click to auto-detect.'
                  : aggregateOverride === false
                    ? 'All charts per-interface. Click to aggregate all.'
                    : 'Auto: charts with >10 interfaces are aggregated. Click to show all per-interface.'
              }
            >
              <Sigma className={`h-4 w-4 transition-opacity ${aggregateProcessing ? 'opacity-30 animate-pulse' : ''}`} />
              {aggregateOverride === null && <span className="text-[9px] leading-none opacity-60">A</span>}
            </button>
            <button
              onClick={() => setBidirectional(!bidirectional)}
              className={`px-3 py-1.5 text-sm border rounded-md transition-colors inline-flex items-center gap-1.5 ${
                bidirectional
                  ? 'border-foreground/30 text-foreground bg-muted'
                  : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
              }`}
              title={bidirectional ? 'Rx and Tx are shown separately (Rx up, Tx down). Click to combine into a single line per interface.' : 'Rx and Tx are combined into a single line per interface. Click to split into separate Rx (up) and Tx (down).'}
            >
              {bidirectional ? 'Rx / Tx' : 'Rx+Tx'}
            </button>
            <LayoutSelector value={layout} onChange={handleLayoutChange} />
            <AggSelector value={aggMethod} onChange={setAggMethod} />
          </div>
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-auto px-4 sm:px-8 py-6">
        {/* Truncation warning */}
        {(allTrafficData?.truncated || allTrafficData?.truncated) && (
          <div className="mb-4 px-4 py-3 bg-yellow-500/10 border border-yellow-500/30 rounded-md text-sm text-yellow-700 dark:text-yellow-200">
            Results were truncated due to data volume. Try a larger bucket size or shorter time range to see all data.
          </div>
        )}

        {/* Charts */}
        <div className={gridClass}>
          {ALL_KNOWN_SECTIONS.map(section => renderChartSection(section))}
        </div>
      </div>
    </div>
  )
}

export function TrafficPage() {
  return (
    <DashboardProvider defaultTimeRange="24h">
      <TrafficPageContent />
    </DashboardProvider>
  )
}
