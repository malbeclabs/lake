import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, Activity } from 'lucide-react'
import { fetchLinkLatencyData, type TrafficPoint, type SeriesInfo } from '@/lib/api'
import { TrafficChart } from '@/components/traffic-chart-uplot'
import { DashboardProvider, useDashboard, dashboardFilterParams, resolveAutoBucket } from '@/components/traffic-dashboard/dashboard-context'
import { DashboardFilters, DashboardFilterBadges } from '@/components/traffic-dashboard/dashboard-filters'
import { PageHeader } from '@/components/page-header'

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

type LatencyMetric = 'rtt' | 'jitter' | 'loss'

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

function MetricSelector({
  value,
  onChange,
}: {
  value: LatencyMetric
  onChange: (value: LatencyMetric) => void
}) {
  const options: { value: LatencyMetric; label: string }[] = [
    { value: 'rtt', label: 'RTT' },
    { value: 'jitter', label: 'Jitter' },
    { value: 'loss', label: 'Packet Loss' },
  ]

  return (
    <div className="flex items-center gap-1">
      {options.map((opt) => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`px-3 py-1.5 text-sm border rounded-md transition-colors ${
            value === opt.value
              ? 'border-foreground/30 text-foreground bg-muted'
              : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}

const DIRECTION_LABELS = { in: 'A→Z', out: 'Z→A' }

function LinkLatencyPageContent() {
  const dashboardState = useDashboard()
  const { timeRange, customStart, customEnd } = dashboardState

  const timeRangeSeconds = useMemo(() => {
    if (customStart && customEnd) return customEnd - customStart
    const map: Record<string, number> = {
      '1h': 3600, '3h': 10800, '6h': 21600, '12h': 43200, '24h': 86400,
      '3d': 259200, '7d': 604800, '14d': 1209600, '30d': 2592000,
    }
    return map[timeRange] || 86400
  }, [timeRange, customStart, customEnd])

  const [aggMethod, setAggMethod] = useState<AggMethod>('avg')
  const [latencyMetric, setLatencyMetric] = useState<LatencyMetric>('rtt')

  const actualBucketSize = useMemo(() => {
    if (dashboardState.bucket === 'auto') {
      return resolveAutoBucket(timeRange)
    }
    return dashboardState.bucket
  }, [dashboardState.bucket, timeRange])

  const filterParams = useMemo(() => {
    return dashboardFilterParams(dashboardState)
  }, [dashboardState])

  const {
    data: latencyData,
    isFetching,
    error,
  } = useQuery({
    queryKey: ['link-latency-data', timeRange, actualBucketSize, aggMethod, filterParams],
    queryFn: () => fetchLinkLatencyData(timeRange, actualBucketSize, aggMethod, filterParams),
    staleTime: 30000,
    refetchInterval: dashboardState.refetchInterval,
  })

  // Transform link latency data into TrafficPoint + SeriesInfo format
  // for the current metric (RTT, Jitter, or Loss)
  const chartData = useMemo(() => {
    if (!latencyData) return null

    const points: TrafficPoint[] = latencyData.points.map((p) => {
      let inVal: number, outVal: number
      switch (latencyMetric) {
        case 'rtt':
          inVal = p.rtt_a_to_z_ms
          outVal = p.rtt_z_to_a_ms
          break
        case 'jitter':
          inVal = p.jitter_a_to_z_ms
          outVal = p.jitter_z_to_a_ms
          break
        case 'loss':
          inVal = p.loss_a_pct
          outVal = p.loss_z_pct
          break
      }
      return {
        time: p.time,
        device_pk: p.link_pk,
        device: p.link_code,
        intf: '',
        in_bps: inVal,
        out_bps: outVal,
        in_discards: 0,
        out_discards: 0,
        in_errors: 0,
        out_errors: 0,
        in_fcs_errors: 0,
        carrier_transitions: 0,
      }
    })

    // Build series sorted by mean RTT (highest first for visibility)
    const sorted = [...latencyData.series].sort((a, b) => b.mean_rtt_ms - a.mean_rtt_ms)
    const series: SeriesInfo[] = sorted.flatMap((s) => [
      { key: `${s.link_code} (in)`, device: s.link_code, intf: '', direction: 'in' as const, mean: s.mean_rtt_ms },
      { key: `${s.link_code} (out)`, device: s.link_code, intf: '', direction: 'out' as const, mean: s.mean_rtt_ms },
    ])

    return { points, series }
  }, [latencyData, latencyMetric])

  const metricLabels: Record<LatencyMetric, string> = {
    rtt: 'Round-Trip Time',
    jitter: 'Jitter',
    loss: 'Packet Loss',
  }

  const chartMetric = latencyMetric === 'loss' ? 'loss' as const : latencyMetric === 'jitter' ? 'jitter' as const : 'latency' as const

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Sticky header */}
      <div className="flex-none bg-background border-b border-border px-4 sm:px-8 pt-6 pb-4 z-10">
        <div className="[&>div]:mb-0">
          <PageHeader
            icon={Activity}
            title="Link Latency"
            actions={<DashboardFilters hideMetric hideIntfType />}
          />
        </div>
        <div className="flex items-center gap-3 mt-3">
          <DashboardFilterBadges />
          <div className="flex items-center gap-3 flex-shrink-0 ml-auto">
            <MetricSelector value={latencyMetric} onChange={setLatencyMetric} />
            <AggSelector value={aggMethod} onChange={setAggMethod} />
          </div>
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-auto px-4 sm:px-8 py-6">
        <div className="border border-border rounded-lg p-4">
          {error ? (
            <div className="flex flex-col space-y-2">
              <div className="flex items-center gap-2">
                <h3 className="text-lg font-semibold">{metricLabels[latencyMetric]} Per Link</h3>
              </div>
              <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
                <p className="text-muted-foreground">Error: {(error as Error).message || String(error)}</p>
              </div>
            </div>
          ) : (
            <TrafficChart
              title={`${metricLabels[latencyMetric]} Per Link`}
              data={chartData?.points || []}
              series={chartData?.series || []}
              bidirectional={true}
              onTimeRangeSelect={dashboardState.setCustomRange}
              metric={chartMetric}
              loading={isFetching}
              timeRangeSeconds={timeRangeSeconds}
              directionLabels={DIRECTION_LABELS}
            />
          )}
        </div>
      </div>
    </div>
  )
}

export function LinkLatencyPage() {
  return (
    <DashboardProvider defaultTimeRange="24h">
      <LinkLatencyPageContent />
    </DashboardProvider>
  )
}
