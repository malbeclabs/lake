import { useState, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, RefreshCw, Server, AlertCircle, ArrowLeft } from 'lucide-react'
import { CopyableText } from '@/components/copyable-text'
import { fetchDevice, fetchDeviceMetrics } from '@/lib/api'
import { DeviceInfoContent } from '@/components/shared/DeviceInfoContent'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { deviceDetailToInfo } from '@/components/shared/device-info-converters'
import { toDeviceMetricsParams } from '@/components/shared/metrics-params'
import { DeviceHealthTimeline } from '@/components/device-charts/DeviceHealthTimeline'
import { DeviceInterfaceIssuesChart } from '@/components/device-charts/DeviceInterfaceIssuesChart'
import { DeviceTrafficChart } from '@/components/device-charts/DeviceTrafficChart'
import { TimeRangeSelector } from '@/components/topology/TimeRangeSelector'
import type { TimeRange } from '@/components/topology/utils'

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

const statusColors: Record<string, string> = {
  activated: 'text-muted-foreground',
  provisioning: 'text-blue-600 dark:text-blue-400',
  maintenance: 'text-amber-600 dark:text-amber-400',
  offline: 'text-red-600 dark:text-red-400',
}

export function DeviceDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<TimeRange>({ preset: '24h' })
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{ start: number; end: number } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  const { data: device, isLoading, error } = useQuery({
    queryKey: ['device', pk],
    queryFn: () => fetchDevice(pk!),
    enabled: !!pk,
  })

  const metricsParams = useMemo(() => toDeviceMetricsParams(timeRange), [timeRange])

  const { data: metrics, isLoading: metricsLoading, isFetching: metricsFetching } = useQuery({
    queryKey: ['deviceMetrics', pk, metricsParams],
    queryFn: () => fetchDeviceMetrics(pk!, metricsParams),
    enabled: !!pk,
  })

  useDocumentTitle(device?.code || 'Device')

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !device) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Device not found</div>
          <Link
            to="/dz/devices"
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to devices
          </Link>
        </div>
      </div>
    )
  }

  const deviceInfo = deviceDetailToInfo(device)

  return (
    <div className="flex-1 overflow-auto">
      {/* Header section - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pt-8">
        {/* Back button */}
        <Link
          to="/dz/devices"
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to devices
        </Link>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <Server className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium font-mono">
              <CopyableText text={device.code} />
            </h1>
            <div className="flex items-center gap-3">
              <span className="text-sm text-muted-foreground font-mono">
                <CopyableText text={device.pk} />
              </span>
              <span className={`text-sm capitalize ${statusColors[device.status] || 'text-muted-foreground'}`}>
                {device.status}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Device stats - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-6">
        {/* Device-specific info cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium font-mono">{device.public_ip || '—'}</div>
            <div className="text-xs text-muted-foreground">Public IP</div>
          </div>
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium">
              <span className="text-muted-foreground text-xs">In:</span> {formatBps(device.in_bps)}
              <span className="mx-2 text-muted-foreground">|</span>
              <span className="text-muted-foreground text-xs">Out:</span> {formatBps(device.out_bps)}
            </div>
            <div className="text-xs text-muted-foreground">Current Traffic</div>
          </div>
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium">
              {device.current_users} / {device.max_users} users
              {device.max_users > 0 && (
                <span className="text-muted-foreground text-xs ml-1">
                  ({((device.current_users / device.max_users) * 100).toFixed(0)}%)
                </span>
              )}
            </div>
            <div className="text-xs text-muted-foreground">Capacity</div>
          </div>
        </div>

        {/* Shared device info (stats grid + interfaces) */}
        <DeviceInfoContent device={deviceInfo} hideStatusRow hideCharts />
      </div>

      {/* Filters + charts */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-8 space-y-6">
        <div className="flex justify-end gap-2 items-center">
          <button
            onClick={() => queryClient.invalidateQueries({ queryKey: ['deviceMetrics'] })}
            disabled={metricsFetching}
            className="text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
            title="Refresh"
          >
            {metricsFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          </button>
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
        </div>

        {metricsLoading && (
          <div className="space-y-4">
            <div className="animate-pulse bg-muted rounded h-6 w-full" />
            {[1, 2].map((i) => (
              <div key={i} className="rounded-lg border border-border p-4 space-y-2">
                <div className="animate-pulse bg-muted rounded h-4 w-32" />
                <div className="animate-pulse bg-muted rounded h-36 w-full" />
              </div>
            ))}
          </div>
        )}
        {metrics && (
          <div className="space-y-4">
            <DeviceHealthTimeline data={metrics} onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />
            <DeviceInterfaceIssuesChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <DeviceTrafficChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
          </div>
        )}
      </div>
    </div>
  )
}
