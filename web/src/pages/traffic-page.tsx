import { useState, useEffect, useRef, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, GripVertical, Check, RefreshCw, ArrowUpDown } from 'lucide-react'
import { fetchTrafficData, fetchTopology, fetchDiscardsData } from '@/lib/api'
import { TrafficChart } from '@/components/traffic-chart-uplot'
import { DiscardsChart } from '@/components/discards-chart'
import { DashboardProvider, useDashboard, dashboardFilterParams } from '@/components/traffic-dashboard/dashboard-context'
import { DashboardFilters, DashboardFilterBadges } from '@/components/traffic-dashboard/dashboard-filters'

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

type BucketSize = '2 SECOND' | '30 SECOND' | '1 MINUTE' | '5 MINUTE' | '10 MINUTE' | '15 MINUTE' | '30 MINUTE' | '1 HOUR' | 'auto'

const bucketLabels: Record<BucketSize, string> = {
  '2 SECOND': '2 seconds',
  '30 SECOND': '30 seconds',
  '1 MINUTE': '1 minute',
  '5 MINUTE': '5 minutes',
  '10 MINUTE': '10 minutes',
  '15 MINUTE': '15 minutes',
  '30 MINUTE': '30 minutes',
  '1 HOUR': '1 hour',
  'auto': 'Auto',
}

type AggMethod = 'max' | 'avg'

const aggLabels: Record<AggMethod, string> = {
  'max': 'Max',
  'avg': 'Average',
}

type ChartSection = 'non-tunnel-stacked' | 'non-tunnel' | 'tunnel-stacked' | 'tunnel' | 'discards'

function chartSectionLabels(intfType: string): Record<ChartSection, string> {
  const nonTunnelLabel = intfType === 'link' ? 'Link' : intfType === 'other' ? 'Other' : 'Non-Tunnel'
  return {
    'non-tunnel-stacked': `${nonTunnelLabel} Traffic (stacked)`,
    'non-tunnel': `${nonTunnelLabel} Traffic`,
    'tunnel-stacked': 'Tunnel Traffic (stacked)',
    'tunnel': 'Tunnel Traffic',
    'discards': 'Interface Discards',
  }
}

// All known chart sections (used to detect new sections added after localStorage was saved)
const ALL_KNOWN_SECTIONS: ChartSection[] = ['non-tunnel-stacked', 'non-tunnel', 'tunnel-stacked', 'tunnel', 'discards']

const TUNNEL_SECTIONS: Set<ChartSection> = new Set(['tunnel-stacked', 'tunnel'])
const NON_TUNNEL_SECTIONS: Set<ChartSection> = new Set(['non-tunnel-stacked', 'non-tunnel', 'discards'])

type Layout = '1x4' | '2x2'

const layoutLabels: Record<Layout, string> = {
  '1x4': '1',
  '2x2': '2',
}

type RefreshInterval = 'never' | '30s' | '1m' | '10m' | '30m'

const refreshIntervalLabels: Record<RefreshInterval, string> = {
  'never': 'Never',
  '30s': '30 seconds',
  '1m': '1 minute',
  '10m': '10 minutes',
  '30m': '30 minutes',
}

const refreshIntervalMs: Record<RefreshInterval, number | false> = {
  'never': false,
  '30s': 30000,
  '1m': 60000,
  '10m': 600000,
  '30m': 1800000,
}


function BucketSelector({
  bucketValue,
  aggValue,
  effectiveBucket,
  onBucketChange,
  onAggChange,
}: {
  bucketValue: BucketSize
  aggValue: AggMethod
  effectiveBucket?: string
  onBucketChange: (value: BucketSize) => void
  onAggChange: (value: AggMethod) => void
}) {
  const [isOpen, setIsOpen] = useState(false)

  // Show effective bucket: for "auto" show the resolved value, for manual show if the API adjusted it
  const effectiveLabel = effectiveBucket ? (bucketLabels[effectiveBucket as BucketSize] || effectiveBucket) : undefined
  let displayBucket: string
  if (bucketValue === 'auto' && effectiveLabel) {
    displayBucket = `Auto (${effectiveLabel})`
  } else if (effectiveBucket && effectiveBucket !== bucketValue) {
    displayBucket = `${bucketLabels[bucketValue]} → ${effectiveLabel}`
  } else {
    displayBucket = bucketLabels[bucketValue]
  }

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
      >
        Coalesce: {displayBucket} ({aggLabels[aggValue]})
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[200px]">
            <div className="px-3 py-1.5 text-xs font-semibold text-muted-foreground border-b border-border">
              Bucket Size
            </div>
            {(['2 SECOND', '30 SECOND', '1 MINUTE', '5 MINUTE', '10 MINUTE', '15 MINUTE', '30 MINUTE', '1 HOUR', 'auto'] as BucketSize[]).map((bucket) => (
              <button
                key={bucket}
                onClick={() => {
                  onBucketChange(bucket)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  bucketValue === bucket
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                }`}
              >
                {bucketLabels[bucket]}
              </button>
            ))}
            <div className="px-3 py-1.5 text-xs font-semibold text-muted-foreground border-t border-b border-border mt-1">
              Aggregation
            </div>
            {(['max', 'avg'] as AggMethod[]).map((agg) => (
              <button
                key={agg}
                onClick={() => {
                  onAggChange(agg)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  aggValue === agg
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

function ShowGraphsSelector({
  sections,
  visibleSections,
  onReorder,
  onToggle,
  labels,
}: {
  sections: ChartSection[]
  visibleSections: Set<ChartSection>
  onReorder: (sections: ChartSection[]) => void
  onToggle: (section: ChartSection) => void
  labels: Record<ChartSection, string>
}) {
  const [isOpen, setIsOpen] = useState(false)
  const [draggedIndex, setDraggedIndex] = useState<number | null>(null)

  const handleDragStart = (e: React.DragEvent, index: number) => {
    setDraggedIndex(index)
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleDragOver = (e: React.DragEvent, index: number) => {
    e.preventDefault()
    if (draggedIndex === null || draggedIndex === index) return

    const newSections = [...sections]
    const draggedSection = newSections[draggedIndex]
    newSections.splice(draggedIndex, 1)
    newSections.splice(index, 0, draggedSection)

    onReorder(newSections)
    setDraggedIndex(index)
  }

  const handleDragEnd = () => {
    setDraggedIndex(null)
  }

  const visibleCount = Array.from(visibleSections).filter(s => sections.includes(s)).length

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
      >
        Show Graphs ({visibleCount}/{sections.length})
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[280px]">
            {sections.map((section, index) => (
              <div
                key={section}
                draggable
                onDragStart={(e) => handleDragStart(e, index)}
                onDragOver={(e) => handleDragOver(e, index)}
                onDragEnd={handleDragEnd}
                className={`flex items-center gap-2 px-3 py-1.5 text-sm transition-colors cursor-move ${
                  draggedIndex === index ? 'opacity-50' : ''
                }`}
              >
                <GripVertical className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                <button
                  onClick={() => onToggle(section)}
                  className="flex-1 flex items-center justify-between text-left hover:bg-muted/50 -m-1.5 p-1.5 rounded"
                >
                  <span className="text-foreground">{labels[section]}</span>
                  {visibleSections.has(section) && <Check className="h-4 w-4 flex-shrink-0" />}
                </button>
              </div>
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

function RefreshIntervalSelector({
  value,
  onChange,
}: {
  value: RefreshInterval
  onChange: (value: RefreshInterval) => void
}) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
      >
        Auto-refresh: {refreshIntervalLabels[value]}
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[180px]">
            {(['never', '30s', '1m', '10m', '30m'] as RefreshInterval[]).map((interval) => (
              <button
                key={interval}
                onClick={() => {
                  onChange(interval)
                  setIsOpen(false)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  value === interval
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                }`}
              >
                {refreshIntervalLabels[interval]}
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
  const { timeRange, intfType } = dashboardState

  const [bucketSize, setBucketSize] = useState<BucketSize>('auto')
  const [aggMethod, setAggMethod] = useState<AggMethod>('max')
  const [chartSections, setChartSections] = useState<ChartSection[]>([
    'non-tunnel-stacked',
    'non-tunnel',
    'tunnel-stacked',
    'tunnel',
    'discards',
  ])
  const [visibleSections, setVisibleSections] = useState<Set<ChartSection>>(
    new Set(['non-tunnel-stacked', 'non-tunnel', 'tunnel-stacked', 'tunnel', 'discards'])
  )
  const [layout, setLayout] = useState<Layout>('1x4')
  const [refreshInterval, setRefreshInterval] = useState<RefreshInterval>('never')
  const [bidirectional, setBidirectional] = useState(true)

  // Load chart sections order from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('traffic-chart-sections')
    if (saved) {
      try {
        const sections = JSON.parse(saved) as ChartSection[]
        // Add any new sections that weren't in localStorage
        const missingSections = ALL_KNOWN_SECTIONS.filter(s => !sections.includes(s))
        if (missingSections.length > 0) {
          const updatedSections = [...sections, ...missingSections]
          setChartSections(updatedSections)
          localStorage.setItem('traffic-chart-sections', JSON.stringify(updatedSections))
        } else {
          setChartSections(sections)
        }
      } catch {
        // Ignore invalid data
      }
    }
  }, [])

  // Load visible sections from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('traffic-visible-sections')
    if (saved) {
      try {
        const sections = JSON.parse(saved) as ChartSection[]
        // Add any new sections that weren't in localStorage (make them visible by default)
        const missingSections = ALL_KNOWN_SECTIONS.filter(s => !sections.includes(s))
        if (missingSections.length > 0) {
          const updatedSections = [...sections, ...missingSections]
          setVisibleSections(new Set(updatedSections))
          localStorage.setItem('traffic-visible-sections', JSON.stringify(updatedSections))
        } else {
          setVisibleSections(new Set(sections))
        }
      } catch {
        // Ignore invalid data
      }
    }
  }, [])

  // Load layout from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('traffic-layout')
    if (saved && (saved === '1x4' || saved === '2x2')) {
      setLayout(saved as Layout)
    }
  }, [])

  // Load refresh interval from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('traffic-refresh-interval')
    if (saved && saved in refreshIntervalLabels) {
      setRefreshInterval(saved as RefreshInterval)
    }
  }, [])

  // Save chart sections order to localStorage
  const handleSectionsReorder = (sections: ChartSection[]) => {
    setChartSections(sections)
    localStorage.setItem('traffic-chart-sections', JSON.stringify(sections))
  }

  // Toggle section visibility
  const handleSectionToggle = (section: ChartSection) => {
    const newVisible = new Set(visibleSections)
    if (newVisible.has(section)) {
      newVisible.delete(section)
    } else {
      newVisible.add(section)
    }
    setVisibleSections(newVisible)
    localStorage.setItem('traffic-visible-sections', JSON.stringify(Array.from(newVisible)))
  }

  // Save layout to localStorage
  const handleLayoutChange = (newLayout: Layout) => {
    setLayout(newLayout)
    localStorage.setItem('traffic-layout', newLayout)
  }

  // Save refresh interval to localStorage
  const handleRefreshIntervalChange = (interval: RefreshInterval) => {
    setRefreshInterval(interval)
    localStorage.setItem('traffic-refresh-interval', interval)
  }

  // Compute actual bucket size to send to API
  const actualBucketSize = useMemo(() => {
    if (bucketSize === 'auto') {
      // Auto-select based on time range
      switch (timeRange) {
        case '1h':
        case '3h':
        case '6h':
        case '12h':
          return '30 SECOND'
        case '24h':
          return '1 MINUTE'
        case '3d':
        case '7d':
        case '14d':
        case '30d':
          return '5 MINUTE'
        default:
          return '30 SECOND'
      }
    }
    return bucketSize
  }, [bucketSize, timeRange])

  // Get refetch interval based on selected refresh interval
  const refetchIntervalValue = useMemo(() => {
    return refreshIntervalMs[refreshInterval]
  }, [refreshInterval])

  // Determine which chart categories to show based on intf type filter
  const showTunnelCharts = intfType === 'all' || intfType === 'tunnel'
  const showNonTunnelCharts = intfType === 'all' || intfType === 'link' || intfType === 'other'

  // Build dimension filter params from dashboard state.
  // When intfType is 'all', intf_type is omitted and the per-chart tunnel_only
  // param handles the tunnel/non-tunnel split. When a specific type is selected
  // (link/tunnel/other), intf_type is passed through so the server filters to
  // only that interface type.
  const filterParams = useMemo(() => {
    return dashboardFilterParams(dashboardState)
  }, [dashboardState])

  // Fetch tunnel data (only when tunnel charts are visible)
  const {
    data: tunnelData,
    isLoading: tunnelLoading,
    error: tunnelError,
    refetch: refetchTunnel,
  } = useQuery({
    queryKey: ['traffic-intf', timeRange, true, actualBucketSize, aggMethod, filterParams],
    queryFn: () => fetchTrafficData(timeRange, true, actualBucketSize, aggMethod, filterParams),
    staleTime: 30000,
    refetchInterval: refetchIntervalValue,
    enabled: showTunnelCharts,
  })

  // Fetch non-tunnel data (only when non-tunnel charts are visible)
  const {
    data: nonTunnelData,
    isLoading: nonTunnelLoading,
    error: nonTunnelError,
    refetch: refetchNonTunnel,
  } = useQuery({
    queryKey: ['traffic-intf', timeRange, false, actualBucketSize, aggMethod, filterParams],
    queryFn: () => fetchTrafficData(timeRange, false, actualBucketSize, aggMethod, filterParams),
    staleTime: 30000,
    refetchInterval: refetchIntervalValue,
    enabled: showNonTunnelCharts,
  })

  // Fetch topology data for link metadata
  const {
    data: topologyData,
    refetch: refetchTopology,
  } = useQuery({
    queryKey: ['topology'],
    queryFn: () => fetchTopology(),
    staleTime: 60000,
    refetchInterval: refetchIntervalValue,
  })

  // Fetch discards data (only when non-tunnel charts are visible)
  const {
    data: discardsData,
    isLoading: discardsLoading,
    refetch: refetchDiscards,
  } = useQuery({
    queryKey: ['discards-intf', timeRange, actualBucketSize, filterParams],
    queryFn: () => fetchDiscardsData(timeRange, actualBucketSize, filterParams),
    staleTime: 30000,
    refetchInterval: refetchIntervalValue,
    enabled: showNonTunnelCharts,
  })

  // Manual refresh handler
  const handleManualRefresh = () => {
    if (showTunnelCharts) refetchTunnel()
    if (showNonTunnelCharts) refetchNonTunnel()
    refetchTopology()
    if (showNonTunnelCharts) refetchDiscards()
  }

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
    if (TUNNEL_SECTIONS.has(section)) return showTunnelCharts
    if (NON_TUNNEL_SECTIONS.has(section)) return showNonTunnelCharts
    return true
  }

  // Render a chart section
  const renderChartSection = (section: ChartSection) => {
    if (!visibleSections.has(section)) return null
    if (!isSectionAllowed(section)) return null

    // Handle discards chart separately
    if (section === 'discards') {
      return (
        <div key={section} className="border border-border rounded-lg p-4">
          <LazyChart key={`${section}-${layout}`}>
            <DiscardsChart
              title="Interface Discards"
              data={discardsData?.points || []}
              series={discardsData?.series || []}
              isLoading={discardsLoading}
            />
          </LazyChart>
        </div>
      )
    }

    let isTunnel = false
    let stacked = false

    switch (section) {
      case 'non-tunnel-stacked':
        isTunnel = false
        stacked = true
        break
      case 'non-tunnel':
        isTunnel = false
        stacked = false
        break
      case 'tunnel-stacked':
        isTunnel = true
        stacked = true
        break
      case 'tunnel':
        isTunnel = true
        stacked = false
        break
    }

    // Build title based on intf type filter
    const typeLabel = isTunnel
      ? 'Tunnel'
      : intfType === 'link' ? 'Link' : intfType === 'other' ? 'Other' : 'Non-Tunnel'
    const title = `${typeLabel} Traffic Per Device & Interface${stacked ? ' (stacked)' : ''}`

    const data = isTunnel ? tunnelData : nonTunnelData
    const loading = isTunnel ? tunnelLoading : nonTunnelLoading
    const error = isTunnel ? tunnelError : nonTunnelError

    return (
      <div key={section} className="border border-border rounded-lg p-4">
        <LazyChart key={section}>
          {loading ? (
            <div className="flex flex-col space-y-2">
              <h3 className="text-lg font-semibold">{title}</h3>
              <div className="animate-pulse bg-muted rounded h-[400px]" />
            </div>
          ) : error ? (
            <div className="flex flex-col space-y-2">
              <h3 className="text-lg font-semibold">{title}</h3>
              <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
                <p className="text-muted-foreground">Error: {(error as Error).message || String(error)}</p>
              </div>
            </div>
          ) : data ? (
            <TrafficChart
              title={title}
              data={data.points}
              series={data.series}
              stacked={stacked}
              linkLookup={linkLookup}
              bidirectional={bidirectional}
            />
          ) : (
            <div className="flex flex-col space-y-2">
              <h3 className="text-lg font-semibold">{title}</h3>
              <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
                <p className="text-muted-foreground">No data available</p>
              </div>
            </div>
          )}
        </LazyChart>
      </div>
    )
  }

  const gridClass = layout === '2x2' ? 'grid grid-cols-1 lg:grid-cols-2 gap-6' : 'space-y-6'

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        {/* Header */}
        <div className="flex flex-col gap-3 mb-6">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <h1 className="text-2xl font-bold">Interfaces</h1>
            <DashboardFilters />
          </div>
          <DashboardFilterBadges />
          <div className="flex items-center gap-3 flex-wrap justify-end">
            <button
              onClick={() => setBidirectional(!bidirectional)}
              className={`px-3 py-1.5 text-sm border rounded-md transition-colors inline-flex items-center gap-1.5 ${
                bidirectional
                  ? 'border-foreground/30 text-foreground bg-muted'
                  : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
              }`}
              title={bidirectional ? 'Switch to all-positive view' : 'Switch to bidirectional (Rx up / Tx down)'}
            >
              <ArrowUpDown className="h-4 w-4" />
              Rx/Tx
            </button>
            <ShowGraphsSelector
              sections={chartSections}
              visibleSections={visibleSections}
              onReorder={handleSectionsReorder}
              onToggle={handleSectionToggle}
              labels={chartSectionLabels(intfType)}
            />
            <LayoutSelector value={layout} onChange={handleLayoutChange} />
            <BucketSelector
              bucketValue={bucketSize}
              aggValue={aggMethod}
              effectiveBucket={tunnelData?.effective_bucket ?? nonTunnelData?.effective_bucket}
              onBucketChange={setBucketSize}
              onAggChange={setAggMethod}
            />
            <RefreshIntervalSelector value={refreshInterval} onChange={handleRefreshIntervalChange} />
            <button
              onClick={handleManualRefresh}
              className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
              title="Refresh now"
            >
              <RefreshCw className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* Truncation warning */}
        {(tunnelData?.truncated || nonTunnelData?.truncated) && (
          <div className="mb-4 px-4 py-3 bg-yellow-500/10 border border-yellow-500/30 rounded-md text-sm text-yellow-700 dark:text-yellow-200">
            Results were truncated due to data volume. Try a larger bucket size or shorter time range to see all data.
          </div>
        )}

        {/* Charts */}
        <div className={gridClass}>
          {chartSections.map(section => renderChartSection(section))}
        </div>
      </div>
    </div>
  )
}

export function TrafficPage() {
  return (
    <DashboardProvider defaultTimeRange="6h">
      <TrafficPageContent />
    </DashboardProvider>
  )
}
