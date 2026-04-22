import { useState, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, RefreshCw, Server, AlertCircle, ArrowLeft } from 'lucide-react'
import { CopyableText } from '@/components/copyable-text'
import { fetchDevice, fetchDeviceMetrics, fetchDeviceValidatorStats } from '@/lib/api'
import { DeviceInfoContent } from '@/components/shared/DeviceInfoContent'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useBackLink } from '@/hooks/use-back-link'
import { OpsPanel } from '@/components/ops/OpsPanel'
import { CreateIncidentModal } from '@/components/ops/CreateIncidentModal'
import { deviceDetailToInfo } from '@/components/shared/device-info-converters'
import { toDeviceMetricsParams } from '@/components/shared/metrics-params'
import { DeviceHealthTimeline } from '@/components/device-charts/DeviceHealthTimeline'
import { DeviceInterfaceIssuesChart } from '@/components/device-charts/DeviceInterfaceIssuesChart'
import { DeviceTrafficChart } from '@/components/device-charts/DeviceTrafficChart'
import { TimeRangeSelector } from '@/components/topology/TimeRangeSelector'
import type { TimeRange } from '@/components/topology/utils'
import { useActiveOpsTickets, useOpsTicketHistory } from '@/hooks/use-ops-tickets'
import { useIsOpsUser } from '@/hooks/use-is-ops-user'
import type { OpsTicket } from '@/lib/ops-api'
import type { TicketWindow } from '@/components/ops/TicketOverlay'

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

const statusColors: Record<string, string> = {
  activated: 'bg-muted/60 text-muted-foreground',
  provisioning: 'bg-blue-500/10 text-blue-600 dark:text-blue-400',
  maintenance: 'bg-amber-500/10 text-amber-600 dark:text-amber-400',
  offline: 'bg-red-500/10 text-red-600 dark:text-red-400',
}

export function DeviceDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const back = useBackLink({ to: '/dz/devices', label: 'devices' })
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<TimeRange>({ preset: '24h' })
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{
    start: number
    end: number
  } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)
  const [showCreateIncident, setShowCreateIncident] = useState(false)
  const isOpsUser = useIsOpsUser()
  const [showIncidents, setShowIncidents] = useState(true)
  const [showMaintenance, setShowMaintenance] = useState(true)

  const {
    data: device,
    isLoading,
    error,
  } = useQuery({
    queryKey: ['device', pk],
    queryFn: () => fetchDevice(pk!),
    enabled: !!pk,
  })

  const { data: validatorStats } = useQuery({
    queryKey: ['deviceValidatorStats', pk],
    queryFn: () => fetchDeviceValidatorStats(pk!),
    enabled: !!pk,
  })

  const metricsParams = useMemo(() => toDeviceMetricsParams(timeRange), [timeRange])

  const {
    data: metrics,
    isLoading: metricsLoading,
    isFetching: metricsFetching,
  } = useQuery({
    queryKey: ['deviceMetrics', pk, metricsParams],
    queryFn: () => fetchDeviceMetrics(pk!, metricsParams),
    enabled: !!pk,
  })

  const { data: activeTicketsData } = useActiveOpsTickets()
  const { data: ticketHistoryData } = useOpsTicketHistory(pk ?? '', undefined, 'device')
  // Combine active tickets for this device + recently closed history
  const tickets: OpsTicket[] = [
    ...(activeTicketsData?.tickets ?? []).filter(
      t => pk && (t.device_pubkey.includes(pk) || (t.affected_devices?.some(d => d.pubkey === pk) ?? false))
    ),
    ...(ticketHistoryData?.tickets ?? []),
  ]

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
            to={back.to}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to {back.label}
          </Link>
        </div>
      </div>
    )
  }

  const deviceInfo = deviceDetailToInfo(device, undefined, validatorStats)

  return (
    <div className="flex-1 overflow-auto">
      {/* Header section - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pt-8">
        {/* Back button */}
        <Link
          to={back.to}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to {back.label}
        </Link>

        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-end gap-2 sm:gap-5 mb-8">
          <div className="flex items-center gap-3">
            <Server className="hidden sm:block h-8 w-8 text-muted-foreground shrink-0" />
            <div>
              <h1 className="text-2xl font-medium font-mono">
                <CopyableText text={device.code} />
              </h1>
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-sm text-muted-foreground font-mono">
                  <CopyableText text={device.pk} />
                </span>
              </div>
            </div>
          </div>
          <div>
            {' '}
            <span
              className={`text-xs capitalize px-2 py-0.5 rounded-full ${statusColors[device.status] || 'bg-muted/60 text-muted-foreground'}`}
            >
              {device.status}
            </span>
          </div>
        </div>
      </div>

      {/* Device stats - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-6">
        {/* Device-specific info cards */}
        <div className="grid grid-cols-2 gap-4 mb-6">
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium font-mono">{device.public_ip || '—'}</div>
            <div className="text-xs text-muted-foreground">Public IP</div>
          </div>
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-sm sm:text-base font-medium">
              <span className="text-muted-foreground text-xs">In:</span> {formatBps(device.in_bps)}
              <span className="mx-2 text-muted-foreground">|</span>
              <span className="text-muted-foreground text-xs">Out:</span>{' '}
              {formatBps(device.out_bps)}
            </div>
            <div className="text-xs text-muted-foreground">Current Traffic</div>
          </div>
        </div>

        {/* Shared device info (stats grid + interfaces) */}
        <DeviceInfoContent device={deviceInfo} hideStatusRow hideCharts />
        <div className="mt-6">
          <OpsPanel
            entityPk={device.pk}
            entityCode={device.code}
            entityType="device"
            contributorCode={device.contributor_code}
            isDown={device.status === 'down'}
            onCreateIncident={() => setShowCreateIncident(true)}
          />
        </div>
        {showCreateIncident && (
          <CreateIncidentModal
            entityCode={device.code}
            entityType="device"
            entityPk={device.pk}
            contributorCode={device.contributor_code}
            contributorPk={device.contributor_pk}
            onClose={() => setShowCreateIncident(false)}
            onSuccess={() => setShowCreateIncident(false)}
          />
        )}
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
            {metricsFetching ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
          </button>
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
          {isOpsUser && tickets.some(t => t.type === 'incident') && (
            <button
              type="button"
              onClick={() => setShowIncidents(v => !v)}
              className={`text-[11px] font-medium px-2 py-1 border transition-colors ${
                showIncidents
                  ? 'border-red-600/60 bg-red-500/10 text-red-700 dark:border-red-800/60 dark:bg-red-900/20 dark:text-red-300'
                  : 'border-border text-muted-foreground hover:text-foreground'
              }`}
            >
              Incidents
            </button>
          )}
          {isOpsUser && tickets.some(t => t.type === 'maintenance') && (
            <button
              type="button"
              onClick={() => setShowMaintenance(v => !v)}
              className={`text-[11px] font-medium px-2 py-1 border transition-colors ${
                showMaintenance
                  ? 'border-blue-600/60 bg-blue-500/10 text-blue-700 dark:border-blue-700/60 dark:bg-blue-900/20 dark:text-blue-300'
                  : 'border-border text-muted-foreground hover:text-foreground'
              }`}
            >
              Maintenance
            </button>
          )}
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
            <DeviceHealthTimeline
              data={metrics}
              onBarHover={setHoveredTimeRange}
              highlightedTime={chartHoveredTime}
              incidentWindows={showIncidents ? tickets
                .filter(t => t.type === 'incident')
                .map(t => ({
                  startAt: t.start_at ?? new Date().toISOString(),
                  endAt: t.end_at,
                  type: 'incident' as const,
                  id: t.id,
                  humanReadableId: t.human_readable_id,
                  title: t.title,
                  status: t.status,
                  slackUrl: t.slack_message_url,
                } as TicketWindow)) : []}
              maintenanceWindows={showMaintenance ? tickets
                .filter(t => t.type === 'maintenance' && t.start_at)
                .map(t => ({
                  startAt: t.start_at!,
                  endAt: t.end_at,
                  type: 'maintenance' as const,
                  id: t.id,
                  humanReadableId: t.human_readable_id,
                  title: t.title,
                  status: t.status,
                  slackUrl: t.slack_message_url,
                } as TicketWindow)) : []}
            />
            <DeviceInterfaceIssuesChart
              data={metrics}
              loading={metricsFetching}
              className="rounded-lg border border-border p-4"
              highlightTimeRange={hoveredTimeRange}
              onCursorTime={setChartHoveredTime}
              tickets={tickets}
              showIncidents={showIncidents}
              showMaintenance={showMaintenance}
            />
            <DeviceTrafficChart
              data={metrics}
              loading={metricsFetching}
              className="rounded-lg border border-border p-4"
              highlightTimeRange={hoveredTimeRange}
              onCursorTime={setChartHoveredTime}
              tickets={tickets}
              showIncidents={showIncidents}
              showMaintenance={showMaintenance}
            />
          </div>
        )}
      </div>
    </div>
  )
}
