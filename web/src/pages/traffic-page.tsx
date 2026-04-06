import { useState, useEffect, useRef, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, Network } from 'lucide-react'
import { fetchTrafficData, fetchTopology } from '@/lib/api'
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

  const [aggMethod, setAggMethod] = useState<AggMethod>('max')
  const [layout, setLayout] = useState<Layout>('1x4')
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
            <p className="text-green-600 dark:text-green-400">No errors or discards in the selected time range</p>
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
    const title = `${typeLabel} ${metricLabel} Per Device & Interface${stacked ? ' (stacked)' : ''}`

    const data = categoryData[cat]
    const fetching = trafficFetching
    const error = trafficError

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
