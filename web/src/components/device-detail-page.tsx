import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Server, AlertCircle, ArrowLeft } from 'lucide-react'
import { CopyableText } from '@/components/copyable-text'
import { fetchDevice } from '@/lib/api'
import { DeviceInfoContent } from '@/components/shared/DeviceInfoContent'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { deviceDetailToInfo } from '@/components/shared/device-info-converters'
import { SingleDeviceStatusRow } from '@/components/single-device-status-row'
import { InterfaceCharts } from '@/components/topology/InterfaceCharts'
import { TimeRangeSelector } from '@/components/topology/TimeRangeSelector'
import type { TimeRange } from '@/components/topology/utils'
import { timeRangeToString } from '@/components/topology/utils'

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
  const navigate = useNavigate()
  const [timeRange, setTimeRange] = useState<TimeRange>({ preset: '24h' })

  const { data: device, isLoading, error } = useQuery({
    queryKey: ['device', pk],
    queryFn: () => fetchDevice(pk!),
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
          <button
            onClick={() => navigate('/dz/devices')}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to devices
          </button>
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
        <button
          onClick={() => navigate('/dz/devices')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to devices
        </button>

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

      {/* Filters + status row + charts */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-8 space-y-6">
        <div className="flex justify-end">
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
        </div>

        {/* Status row */}
        <SingleDeviceStatusRow devicePk={device.pk} timeRange={timeRangeToString(timeRange)} />

        {/* Interface charts (traffic + health) */}
        <InterfaceCharts entityType="device" entityPk={device.pk} timeRange={timeRange} className="rounded-lg border border-border p-4" />
      </div>
    </div>
  )
}
