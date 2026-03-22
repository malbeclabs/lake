import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useMemo, useEffect } from 'react'
import { useSearchParams, useNavigate, useLocation, Link } from 'react-router-dom'
import { ShieldAlert, Settings, ExternalLink, Info, Download, ChevronDown, RefreshCw } from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  fetchLinkIncidents,
  fetchDeviceIncidents,
  type LinkIncident,
  type DrainedLinkInfo,
  type DeviceIncident,
  type DrainedDeviceInfo,
  type IncidentTimeRange,
} from '@/lib/api'
import { StatusFilters, useStatusFilters } from '@/components/status-search-bar'
import { PageHeader } from '@/components/page-header'

const timeRanges: { value: IncidentTimeRange; label: string }[] = [
  { value: '3h', label: '3h' },
  { value: '6h', label: '6h' },
  { value: '12h', label: '12h' },
  { value: '24h', label: '24h' },
  { value: '3d', label: '3d' },
  { value: '7d', label: '7d' },
  { value: '30d', label: '30d' },
]

function RelativeTime({ timestamp }: { timestamp: number }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 10000)
    return () => clearInterval(id)
  }, [])
  const seconds = Math.floor((now - timestamp) / 1000)
  if (seconds < 5) return <>just now</>
  if (seconds < 60) return <>{seconds}s ago</>
  const minutes = Math.floor(seconds / 60)
  return <>{minutes}m ago</>
}

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

function IncidentsContentSkeleton() {
  return (
    <>
      <div className="grid grid-cols-3 sm:grid-cols-5 gap-3 mb-6">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-[72px]" />
        ))}
      </div>
      <Skeleton className="h-[400px] rounded-lg" />
    </>
  )
}

function formatDuration(seconds: number | undefined): string {
  if (seconds === undefined) return '-'
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) {
    const hours = Math.floor(seconds / 3600)
    const mins = Math.floor((seconds % 3600) / 60)
    return mins > 0 ? `${hours}h ${mins}m` : `${hours}h`
  }
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const mins = Math.floor((seconds % 3600) / 60)
  const parts = [`${days}d`]
  if (hours > 0) parts.push(`${hours}h`)
  if (mins > 0 && days < 7) parts.push(`${mins}m`)
  return parts.join(' ')
}


type IncidentScope = 'links' | 'devices'

function formatTimeAgo(isoString: string): string {
  if (isoString === 'unknown') return 'Unknown'
  const date = new Date(isoString)
  if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'Unknown'
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffSecs = Math.floor(diffMs / 1000)

  if (diffSecs < 60) return `${diffSecs}s ago`
  if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`

  const days = Math.floor(diffSecs / 86400)
  const hours = Math.floor((diffSecs % 86400) / 3600)
  const minutes = Math.floor((diffSecs % 3600) / 60)

  if (days > 0) {
    const parts = [`${days}d`]
    if (hours > 0) parts.push(`${hours}h`)
    if (minutes > 0 && days < 7) parts.push(`${minutes}m`)
    return `${parts.join(' ')} ago`
  }
  const parts = [`${hours}h`]
  if (minutes > 0) parts.push(`${minutes}m`)
  return `${parts.join(' ')} ago`
}

function formatTimestamp(isoString: string): string {
  if (isoString === 'unknown') return 'Unknown'
  const date = new Date(isoString)
  if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'Unknown'
  return date.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function IncidentTypeBadge({ type }: { type: string }) {
  const config: Record<string, { label: string; className: string }> = {
    packet_loss: {
      label: 'packet loss',
      className: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
    },
    errors: {
      label: 'errors',
      className: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    },
    discards: {
      label: 'discards',
      className: 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200',
    },
    fcs: {
      label: 'fcs errors',
      className: 'bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-200',
    },
    carrier: {
      label: 'carrier transitions',
      className: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    },
    no_data: {
      label: 'no data',
      className: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    },
    isis_down: {
      label: 'ISIS down',
      className: 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200',
    },
    isis_overload: {
      label: 'ISIS overload',
      className: 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200',
    },
    isis_unreachable: {
      label: 'ISIS unreachable',
      className: 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200',
    },
  }
  const c = config[type] || { label: type, className: 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200' }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${c.className}`}>
      {c.label}
    </span>
  )
}

function IncidentSection({
  title,
  description,
  count,
  defaultOpen = true,
  children,
}: {
  title: string
  description: string
  count: number
  defaultOpen?: boolean
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <div className="border border-border rounded-lg bg-card">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-muted/30 transition-colors rounded-t-lg"
      >
        <ChevronDown
          className={cn(
            'h-4 w-4 shrink-0 text-muted-foreground transition-transform',
            !open && '-rotate-90'
          )}
        />
        <h2 className="text-sm font-semibold">{title}</h2>
        <span className="px-1.5 py-0.5 text-xs rounded-full bg-muted-foreground/10 text-muted-foreground tabular-nums">
          {count}
        </span>
        <span className="text-xs text-muted-foreground">{description}</span>
      </button>
      {open && count > 0 && (
        <div className="px-4 pb-4 overflow-hidden">
          {children}
        </div>
      )}
    </div>
  )
}

function DrainedBadge() {
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
      drained
    </span>
  )
}


function ReadinessDot({ readiness }: { readiness: string }) {
  const colors: Record<string, string> = {
    red: 'bg-red-500',
    yellow: 'bg-yellow-500',
    green: 'bg-green-500',
    gray: 'bg-gray-400',
  }
  const labels: Record<string, string> = {
    red: 'Active issues',
    yellow: 'Recently clear',
    green: 'Clear 30m+',
    gray: 'No issues in range',
  }
  return (
    <span className="inline-flex items-center gap-1.5" title={labels[readiness] || readiness}>
      <span className={`inline-flex rounded-full h-2.5 w-2.5 ${colors[readiness] || 'bg-gray-400'}`} />
      <span className="text-xs text-muted-foreground">{labels[readiness]}</span>
    </span>
  )
}

function dedupeIncidentTypes(incidents: { incident_type: string }[]): string[] {
  const seen = new Set<string>()
  const result: string[] = []
  for (const inc of incidents) {
    if (!seen.has(inc.incident_type)) {
      seen.add(inc.incident_type)
      result.push(inc.incident_type)
    }
  }
  return result
}

export function IncidentsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const location = useLocation()

  // Scope from path
  const scope: IncidentScope = location.pathname === '/incidents/devices' ? 'devices' : 'links'

  // Parse URL params with defaults
  const range = (searchParams.get('range') as IncidentTimeRange) || '24h'
  const threshold = parseInt(searchParams.get('threshold') || '10') || 10
  const errorsThreshold = parseInt(searchParams.get('errors_threshold') || '1') || 1
  const fcsThreshold = parseInt(searchParams.get('fcs_threshold') || '1') || 1
  const discardsThreshold = parseInt(searchParams.get('discards_threshold') || '1') || 1
  const carrierThreshold = parseInt(searchParams.get('carrier_threshold') || '1') || 1
  const typeParam = searchParams.get('type') || ''
  const selectedTypes = useMemo(() => {
    if (!typeParam || typeParam === 'all') return new Set<string>()
    return new Set(typeParam.split(',').filter(Boolean))
  }, [typeParam])
  const minDuration = parseInt(searchParams.get('min_duration') || '30') || 30
  const coalesceGap = parseInt(searchParams.get('coalesce_gap') || '180') || 180
  const view = (searchParams.get('view') as 'active' | 'drained') || 'active'
  const filterParam = searchParams.get('filter') || ''
  const showLinkInterfaces = searchParams.get('link_interfaces') === 'true'

  const [showSettings, setShowSettings] = useState(false)

  // Local settings state — only applied on "Apply"
  const [localSettings, setLocalSettings] = useState({
    threshold: String(threshold),
    errors_threshold: String(errorsThreshold),
    fcs_threshold: String(fcsThreshold),
    discards_threshold: String(discardsThreshold),
    carrier_threshold: String(carrierThreshold),
    min_duration: String(minDuration),
    coalesce_gap: String(coalesceGap),
  })

  // Sync local state when URL params change externally
  const settingsKey = `${threshold}-${errorsThreshold}-${fcsThreshold}-${discardsThreshold}-${carrierThreshold}-${minDuration}-${coalesceGap}`
  const [lastSettingsKey, setLastSettingsKey] = useState(settingsKey)
  if (settingsKey !== lastSettingsKey) {
    setLastSettingsKey(settingsKey)
    setLocalSettings({
      threshold: String(threshold),
      errors_threshold: String(errorsThreshold),
      fcs_threshold: String(fcsThreshold),
      discards_threshold: String(discardsThreshold),
      carrier_threshold: String(carrierThreshold),
      min_duration: String(minDuration),
      coalesce_gap: String(coalesceGap),
    })
  }

  const applySettings = () => {
    updateParams(localSettings)
  }

  const settingsDirty =
    localSettings.threshold !== String(threshold) ||
    localSettings.errors_threshold !== String(errorsThreshold) ||
    localSettings.fcs_threshold !== String(fcsThreshold) ||
    localSettings.discards_threshold !== String(discardsThreshold) ||
    localSettings.carrier_threshold !== String(carrierThreshold) ||
    localSettings.min_duration !== String(minDuration) ||
    localSettings.coalesce_gap !== String(coalesceGap)

  const toggleType = (t: string) => {
    const next = new Set(selectedTypes)
    if (next.has(t)) {
      next.delete(t)
    } else {
      next.add(t)
    }
    updateParams({ type: next.size === 0 ? undefined : Array.from(next).join(',') })
  }

  const filters = useStatusFilters()

  const updateParams = (updates: Record<string, string | undefined>) => {
    const newParams = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(updates)) {
      if (value && value !== getDefaultValue(key)) {
        newParams.set(key, value)
      } else {
        newParams.delete(key)
      }
    }
    setSearchParams(newParams)
  }

  const getDefaultValue = (key: string): string => {
    switch (key) {
      case 'range': return '24h'
      case 'threshold': return '10'
      case 'errors_threshold': return '1'
      case 'fcs_threshold': return '1'
      case 'discards_threshold': return '1'
      case 'carrier_threshold': return '1'
      case 'min_duration': return '30'
      case 'coalesce_gap': return '180'
      case 'type': return ''
      case 'view': return 'active'
      default: return ''
    }
  }

  const linkQuery = useQuery({
    queryKey: ['linkIncidents', range, threshold, errorsThreshold, fcsThreshold, discardsThreshold, carrierThreshold, minDuration, coalesceGap, filterParam],
    queryFn: () => fetchLinkIncidents({
      range,
      threshold,
      errors_threshold: errorsThreshold,
      fcs_threshold: fcsThreshold,
      discards_threshold: discardsThreshold,
      carrier_threshold: carrierThreshold,
      min_duration: minDuration,
      coalesce_gap: coalesceGap,
      filter: filterParam || undefined,
    }),
    refetchInterval: 60000,
    placeholderData: keepPreviousData,
    enabled: scope === 'links',
  })

  const deviceQuery = useQuery({
    queryKey: ['deviceIncidents', range, errorsThreshold, fcsThreshold, discardsThreshold, carrierThreshold, minDuration, coalesceGap, filterParam, showLinkInterfaces],
    queryFn: () => fetchDeviceIncidents({
      range,
      errors_threshold: errorsThreshold,
      fcs_threshold: fcsThreshold,
      discards_threshold: discardsThreshold,
      carrier_threshold: carrierThreshold,
      min_duration: minDuration,
      coalesce_gap: coalesceGap,
      filter: filterParam || undefined,
      link_interfaces: showLinkInterfaces || undefined,
    }),
    refetchInterval: 60000,
    placeholderData: keepPreviousData,
    enabled: scope === 'devices',
  })

  const activeQuery = scope === 'links' ? linkQuery : deviceQuery
  const isLoading = activeQuery.isLoading
  const isFetching = activeQuery.isFetching
  const error = activeQuery.error
  const hasData = activeQuery.data != null
  const dataUpdatedAt = activeQuery.dataUpdatedAt
  const linkData = linkQuery.data
  const deviceData = deviceQuery.data

  // Unfiltered summaries for the stat cards (always show all counts)
  const allDrainedSummary = scope === 'links'
    ? linkData?.drained_summary || { total: 0, with_incidents: 0, ready: 0, not_ready: 0 }
    : deviceData?.drained_summary || { total: 0, with_incidents: 0, ready: 0, not_ready: 0 }

  // Client-side type filtering
  const hasTypeFilter = selectedTypes.size > 0
  const activeIncidents = useMemo(() => {
    if (scope === 'links') return linkData?.active || []
    return [] as LinkIncident[]
  }, [scope, linkData?.active])

  const activeDeviceIncidents = useMemo(() => {
    if (scope === 'devices') return deviceData?.active || []
    return [] as DeviceIncident[]
  }, [scope, deviceData?.active])

  const drainedLinks = useMemo(() => {
    const all = linkData?.drained || []
    if (!hasTypeFilter) return all
    return all.map(dl => ({
      ...dl,
      active_incidents: dl.active_incidents.filter(i => selectedTypes.has(i.incident_type)),
      recent_incidents: dl.recent_incidents.filter(i => selectedTypes.has(i.incident_type)),
    })).filter(dl => dl.active_incidents.length > 0 || dl.recent_incidents.length > 0)
  }, [linkData?.drained, hasTypeFilter, selectedTypes])

  const drainedDevices = useMemo(() => {
    const all = deviceData?.drained || []
    if (!hasTypeFilter) return all
    return all.map(dd => ({
      ...dd,
      active_incidents: dd.active_incidents.filter(i => selectedTypes.has(i.incident_type)),
      recent_incidents: dd.recent_incidents.filter(i => selectedTypes.has(i.incident_type)),
    })).filter(dd => dd.active_incidents.length > 0 || dd.recent_incidents.length > 0)
  }, [deviceData?.drained, hasTypeFilter, selectedTypes])

  // Sort state for active view
  type SortField = 'started_at' | 'ended_at' | 'duration'
  const [sortField, setSortField] = useState<SortField>('started_at')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  // Generic sort helper for incidents of either type
  type SortableIncident = { started_at: string; is_ongoing: boolean; duration_seconds?: number }
  const sortIncidents = <T extends SortableIncident>(items: T[]): T[] => {
    return [...items].sort((a, b) => {
      if (sortField === 'started_at') {
        const aTime = new Date(a.started_at).getTime()
        const bTime = new Date(b.started_at).getTime()
        return sortDir === 'asc' ? aTime - bTime : bTime - aTime
      } else if (sortField === 'ended_at') {
        const aEnd = a.is_ongoing ? Infinity : new Date(a.started_at).getTime() + (a.duration_seconds || 0) * 1000
        const bEnd = b.is_ongoing ? Infinity : new Date(b.started_at).getTime() + (b.duration_seconds || 0) * 1000
        return sortDir === 'asc' ? aEnd - bEnd : bEnd - aEnd
      } else {
        const aDur = a.is_ongoing ? Infinity : (a.duration_seconds || 0)
        const bDur = b.is_ongoing ? Infinity : (b.duration_seconds || 0)
        return sortDir === 'asc' ? aDur - bDur : bDur - aDur
      }
    })
  }

  // Split link incidents by status category
  type IncidentStatus = 'ongoing' | 'detecting' | 'resolved' | 'transient'
  const getStatus = (i: { is_ongoing: boolean; confirmed: boolean }): IncidentStatus => {
    if (i.is_ongoing && i.confirmed) return 'ongoing'
    if (i.is_ongoing) return 'detecting'
    if (i.confirmed) return 'resolved'
    return 'transient'
  }

  const splitByStatus = <T extends { is_ongoing: boolean; confirmed: boolean; started_at: string; duration_seconds?: number }>(items: T[]) => {
    const ongoing: T[] = []
    const detecting: T[] = []
    const resolved: T[] = []
    const transient: T[] = []
    for (const i of items) {
      switch (getStatus(i)) {
        case 'ongoing': ongoing.push(i); break
        case 'detecting': detecting.push(i); break
        case 'resolved': resolved.push(i); break
        case 'transient': transient.push(i); break
      }
    }
    return {
      ongoing: sortIncidents(ongoing),
      detecting: sortIncidents(detecting),
      resolved: sortIncidents(resolved),
      transient: sortIncidents(transient),
    }
  }

  const linkIncidentsByStatus = useMemo(
    () => splitByStatus(activeIncidents),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [activeIncidents, sortField, sortDir],
  )

  const deviceIncidentsByStatus = useMemo(
    () => splitByStatus(activeDeviceIncidents),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [activeDeviceIncidents, sortField, sortDir],
  )

  // Compute counts from all active data (no toggle filtering)
  const filteredByType = useMemo(() => {
    if (scope === 'links') {
      const all = linkData?.active || []
      const byType: Record<string, number> = { packet_loss: 0, errors: 0, fcs: 0, discards: 0, carrier: 0, no_data: 0, isis_down: 0 }
      let ongoing = 0
      for (const i of all) {
        byType[i.incident_type] = (byType[i.incident_type] || 0) + 1
        if (i.is_ongoing && i.confirmed) ongoing++
      }
      return { byType, ongoing }
    } else {
      const all = deviceData?.active || []
      const byType: Record<string, number> = { errors: 0, fcs: 0, discards: 0, carrier: 0, no_data: 0, isis_overload: 0, isis_unreachable: 0 }
      let ongoing = 0
      for (const i of all) {
        byType[i.incident_type] = (byType[i.incident_type] || 0) + 1
        if (i.is_ongoing && i.confirmed) ongoing++
      }
      return { byType, ongoing }
    }
  }, [scope, linkData?.active, deviceData?.active])

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDir('desc')
    }
  }

  const exportUrl = useMemo(() => {
    const params = new URLSearchParams()
    params.set('range', range)
    if (scope === 'links') params.set('threshold', threshold.toString())
    params.set('errors_threshold', errorsThreshold.toString())
    params.set('fcs_threshold', fcsThreshold.toString())
    params.set('discards_threshold', discardsThreshold.toString())
    params.set('carrier_threshold', carrierThreshold.toString())
    params.set('min_duration', minDuration.toString())
    params.set('coalesce_gap', coalesceGap.toString())
    if (filterParam) params.set('filter', filterParam)
    if (scope === 'devices' && showLinkInterfaces) params.set('link_interfaces', 'true')
    const base = scope === 'devices' ? '/api/incidents/devices/csv' : '/api/incidents/links/csv'
    return `${base}?${params.toString()}`
  }, [scope, range, threshold, errorsThreshold, fcsThreshold, discardsThreshold, carrierThreshold, minDuration, coalesceGap, filterParam, showLinkInterfaces])

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={ShieldAlert}
          title="Incidents"
          actions={
            <a
              href={exportUrl}
              className="inline-flex items-center gap-2 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground border border-border rounded-md hover:bg-muted transition-colors"
            >
              <Download className="h-4 w-4" />
              Export CSV
            </a>
          }
        />

        {/* Scope toggle */}
        <div className="flex items-center gap-2 mb-4">
          <div className="flex items-center gap-1 bg-muted rounded-md p-1">
            <button
              onClick={() => navigate('/incidents/links')}
              className={`px-3 py-1 text-sm rounded transition-colors ${
                scope === 'links'
                  ? 'bg-background text-foreground shadow-sm'
                  : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              Links
            </button>
            <button
              onClick={() => navigate('/incidents/devices')}
              className={`px-3 py-1 text-sm rounded transition-colors ${
                scope === 'devices'
                  ? 'bg-background text-foreground shadow-sm'
                  : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              Devices
            </button>
          </div>
        </div>

        {/* Filters row */}
        <div className="flex flex-wrap items-center gap-4 mb-4">
          {/* Time range */}
          <div className="flex items-center gap-1 bg-muted rounded-md p-1">
            {timeRanges.map((tr) => (
              <button
                key={tr.value}
                onClick={() => updateParams({ range: tr.value })}
                className={`px-3 py-1 text-sm rounded transition-colors ${
                  range === tr.value
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {tr.label}
              </button>
            ))}
          </div>

          <StatusFilters />

          {/* Settings toggle */}
          <button
            onClick={() => setShowSettings(!showSettings)}
            className={`p-2 rounded transition-colors ${
              showSettings
                ? 'bg-muted text-foreground'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted'
            }`}
            title="Threshold settings"
          >
            <Settings className="h-4 w-4" />
          </button>
        </div>

        {/* Settings panel */}
        {showSettings && (
          <div className="flex flex-wrap items-center gap-6 mb-4 p-4 bg-muted/50 rounded-lg border border-border">
            {scope === 'links' && (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Packet Loss:</span>
                <input
                  type="number"
                  value={localSettings.threshold}
                  onChange={(e) => setLocalSettings(s => ({ ...s, threshold: e.target.value }))}
                  className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                  min={1}
                  max={100}
                />
                <span className="text-sm text-muted-foreground">%</span>
              </div>
            )}
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Errors:</span>
              <input
                type="number"
                value={localSettings.errors_threshold}
                onChange={(e) => setLocalSettings(s => ({ ...s, errors_threshold: e.target.value }))}
                className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                min={1}
              />
              <span className="text-sm text-muted-foreground">/5m</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">FCS:</span>
              <input
                type="number"
                value={localSettings.fcs_threshold}
                onChange={(e) => setLocalSettings(s => ({ ...s, fcs_threshold: e.target.value }))}
                className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                min={1}
              />
              <span className="text-sm text-muted-foreground">/5m</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Discards:</span>
              <input
                type="number"
                value={localSettings.discards_threshold}
                onChange={(e) => setLocalSettings(s => ({ ...s, discards_threshold: e.target.value }))}
                className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                min={1}
              />
              <span className="text-sm text-muted-foreground">/5m</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Carrier:</span>
              <input
                type="number"
                value={localSettings.carrier_threshold}
                onChange={(e) => setLocalSettings(s => ({ ...s, carrier_threshold: e.target.value }))}
                className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                min={1}
              />
              <span className="text-sm text-muted-foreground">/5m</span>
            </div>
            <div className="w-px h-6 bg-border" />
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Min Duration:</span>
              <input
                type="number"
                value={localSettings.min_duration}
                onChange={(e) => setLocalSettings(s => ({ ...s, min_duration: e.target.value }))}
                className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                min={5}
                step={5}
              />
              <span className="text-sm text-muted-foreground">min</span>
              <span className="text-xs text-muted-foreground/60">({Math.max(1, Math.floor(parseInt(localSettings.min_duration || '30') / 5))} × 5m buckets)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Coalesce Gap:</span>
              <input
                type="number"
                value={localSettings.coalesce_gap}
                onChange={(e) => setLocalSettings(s => ({ ...s, coalesce_gap: e.target.value }))}
                className="w-16 px-2 py-1 text-sm bg-background border border-border rounded"
                min={0}
                step={5}
              />
              <span className="text-sm text-muted-foreground">min</span>
            </div>
            <button
              onClick={applySettings}
              disabled={!settingsDirty}
              className={`px-4 py-1.5 text-sm rounded transition-colors ${
                settingsDirty
                  ? 'bg-primary text-primary-foreground hover:bg-primary/90'
                  : 'bg-muted text-muted-foreground cursor-not-allowed'
              }`}
            >
              Apply
            </button>
          </div>
        )}

        {isLoading ? <IncidentsContentSkeleton /> : error && !hasData ? (
          <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
            <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium mb-2">Unable to load incidents</h3>
            <p className="text-sm text-muted-foreground mb-4">
              {(error as Error).message || 'Something went wrong. The API server may be unavailable.'}
            </p>
            <button
              onClick={() => activeQuery.refetch()}
              className="px-4 py-2 text-sm border border-border rounded-md hover:bg-muted transition-colors"
            >
              Retry
            </button>
          </div>
        ) : (<>
        {/* Data freshness indicator */}
        <div className="flex items-center gap-2 mb-4 text-xs text-muted-foreground">
          <button
            onClick={() => activeQuery.refetch()}
            className="inline-flex items-center gap-1.5 hover:text-foreground transition-colors"
            title="Refresh now"
          >
            <RefreshCw className={cn('h-3 w-3', isFetching && 'animate-spin')} />
            {dataUpdatedAt ? (
              <span>Updated <RelativeTime timestamp={dataUpdatedAt} /></span>
            ) : (
              <span>Refreshing...</span>
            )}
          </button>
          {error && (
            <span className="text-yellow-600 dark:text-yellow-400">· Refresh failed, showing previous data</span>
          )}
        </div>
        {/* Type stat cards — clickable multi-select filters */}
        <div className={`grid gap-3 mb-6 ${scope === 'links' ? 'grid-cols-4 sm:grid-cols-7' : 'grid-cols-4 sm:grid-cols-7'}`}>
          {([
            ...(scope === 'links' ? [{ key: 'packet_loss', label: 'Packet Loss' }] : []),
            { key: 'errors', label: 'Errors' },
            { key: 'fcs', label: 'FCS Errors' },
            { key: 'discards', label: 'Discards' },
            { key: 'carrier', label: 'Carrier' },
            { key: 'no_data', label: 'No Data' },
            ...(scope === 'links' ? [{ key: 'isis_down', label: 'ISIS Down' }] : []),
            ...(scope === 'devices' ? [
              { key: 'isis_overload', label: 'ISIS Overload' },
              { key: 'isis_unreachable', label: 'ISIS Unreachable' },
            ] : []),
          ] as { key: string; label: string }[]).map(({ key, label }) => {
            const count = filteredByType.byType[key] || 0
            const isSelected = selectedTypes.has(key)
            return (
              <button
                key={key}
                onClick={() => toggleType(key)}
                className={`text-center p-3 rounded-lg border transition-colors ${
                  isSelected
                    ? 'border-primary bg-primary/5 ring-1 ring-primary'
                    : 'border-border hover:border-muted-foreground/30'
                }`}
              >
                <div className="text-2xl font-medium tabular-nums tracking-tight">
                  {count}
                </div>
                <div className="text-xs text-muted-foreground">{label}</div>
              </button>
            )
          })}
        </div>

        {/* View tabs */}
        <div className="flex items-center gap-1 bg-muted rounded-md p-1 w-fit mb-6">
          <button
            onClick={() => updateParams({ view: 'active' })}
            className={`px-4 py-1.5 text-sm rounded transition-colors ${
              view === 'active'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            Activated
            {filteredByType.ongoing > 0 && (
              <span className="ml-1.5 px-1.5 py-0.5 text-xs rounded-full bg-red-500/10 text-red-600 dark:text-red-400">
                {filteredByType.ongoing}
              </span>
            )}
          </button>
          <button
            onClick={() => updateParams({ view: 'drained' })}
            className={`px-4 py-1.5 text-sm rounded transition-colors ${
              view === 'drained'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            Drained
            {allDrainedSummary.total > 0 && (
              <span className="ml-1.5 px-1.5 py-0.5 text-xs rounded-full bg-muted-foreground/10 text-muted-foreground">
                {allDrainedSummary.total}
              </span>
            )}
          </button>
        </div>

        {/* Active view */}
        {view === 'active' && (
          <>
            {(() => {
              const isEmpty = scope === 'links' ? activeIncidents.length === 0 : activeDeviceIncidents.length === 0
              if (isEmpty) {
                return (
                  <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
                    <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
                    <h3 className="text-lg font-medium mb-2">No active incidents</h3>
                    <p className="text-sm text-muted-foreground">
                      {filters.length > 0 ? 'No incidents match the selected filters ' : 'No incidents '}
                      on non-drained {scope === 'links' ? 'links' : 'devices'} in the selected time range.
                    </p>
                  </div>
                )
              }
              const byStatus = scope === 'links' ? linkIncidentsByStatus : deviceIncidentsByStatus
              const sections: { key: string; title: string; description: string; defaultOpen: boolean; incidents: (LinkIncident | DeviceIncident)[] }[] = [
                { key: 'ongoing', title: 'Ongoing', description: 'Confirmed active incidents', defaultOpen: true, incidents: byStatus.ongoing },
                { key: 'detecting', title: 'Detecting', description: 'Recently started incidents not yet confirmed', defaultOpen: true, incidents: byStatus.detecting },
                { key: 'resolved', title: 'Resolved', description: 'Confirmed incidents that have ended', defaultOpen: true, incidents: byStatus.resolved },
                { key: 'transient', title: 'Transient', description: 'Brief incidents that ended before being confirmed', defaultOpen: true, incidents: byStatus.transient },
              ]
              return (
                <>
                  {scope === 'devices' && (
                    <div className="flex items-center gap-6 mb-3 justify-end">
                      <button
                        type="button"
                        role="switch"
                        aria-checked={showLinkInterfaces}
                        onClick={() => updateParams({ link_interfaces: showLinkInterfaces ? undefined : 'true' })}
                        className="flex items-center gap-2 text-sm text-muted-foreground"
                      >
                        <span
                          className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                            showLinkInterfaces ? 'bg-primary' : 'bg-muted-foreground/30'
                          }`}
                        >
                          <span
                            className={`inline-block h-4 w-4 transform rounded-full bg-background shadow transition-transform ${
                              showLinkInterfaces ? 'translate-x-4' : 'translate-x-0.5'
                            }`}
                          />
                        </span>
                        Show link interfaces
                        <span className="relative group">
                          <Info className="h-3.5 w-3.5 text-muted-foreground/50" />
                          <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 px-2 py-1 text-xs bg-popover text-popover-foreground border border-border rounded shadow-md whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50">
                            Include interfaces already tracked in the Links view
                          </span>
                        </span>
                      </button>
                    </div>
                  )}
                  <div className="flex flex-col gap-3">
                    {sections.map(({ key, title, description, defaultOpen, incidents: sectionIncidents }) => (
                      <IncidentSection
                        key={key}
                        title={title}
                        description={description}
                        count={sectionIncidents.length}
                        defaultOpen={defaultOpen}
                      >
                        {sectionIncidents.length === 0 ? null : scope === 'links' ? (
                          <ActiveIncidentsTable
                            incidents={sectionIncidents as LinkIncident[]}
                            sortField={sortField}
                            sortDir={sortDir}
                            toggleSort={toggleSort}
                            coalesceGapMinutes={coalesceGap}
                            typeFilter={selectedTypes}
                          />
                        ) : (
                          <ActiveDeviceIncidentsTable
                            incidents={sectionIncidents as DeviceIncident[]}
                            sortField={sortField}
                            sortDir={sortDir}
                            toggleSort={toggleSort}
                            typeFilter={selectedTypes}
                          />
                        )}
                      </IncidentSection>
                    ))}
                  </div>
                </>
              )
            })()}
          </>
        )}

        {/* Drained view */}
        {view === 'drained' && (
          <>
            {(() => {
              const isEmpty = scope === 'links' ? drainedLinks.length === 0 : drainedDevices.length === 0
              const entity = scope === 'links' ? 'links' : 'devices'
              if (isEmpty) {
                return (
                  <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
                    <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
                    <h3 className="text-lg font-medium mb-2">No drained {entity}</h3>
                    <p className="text-sm text-muted-foreground">
                      {hasTypeFilter ? `No drained ${entity} have issues of the selected type(s).` : `No ${entity} are currently drained.`}
                    </p>
                  </div>
                )
              }
              return scope === 'links' ? (
                <DrainedLinksTable drainedLinks={drainedLinks} />
              ) : (
                <DrainedDevicesTable drainedDevices={drainedDevices} />
              )
            })()}
          </>
        )}
        </>)}
      </div>
    </div>
  )
}

type GroupedLinkIncident = {
  link_pk: string
  link_code: string
  link_type: string
  side_a_metro: string
  side_z_metro: string
  contributor_code: string
  is_drained: boolean
  started_at: string
  is_ongoing: boolean
  duration_seconds?: number
  incidents: LinkIncident[]
}

function groupIncidentsByLink(incidents: LinkIncident[], coalesceGapMinutes: number): GroupedLinkIncident[] {
  const gapMs = coalesceGapMinutes * 60 * 1000

  // First group by link
  const byLink = new Map<string, LinkIncident[]>()
  for (const inc of incidents) {
    const existing = byLink.get(inc.link_pk)
    if (existing) existing.push(inc)
    else byLink.set(inc.link_pk, [inc])
  }

  const result: GroupedLinkIncident[] = []
  for (const incs of byLink.values()) {
    // Sort by start time
    incs.sort((a, b) => new Date(a.started_at).getTime() - new Date(b.started_at).getTime())

    // Cluster into temporal groups: incidents overlap or are within coalesceGap of each other
    const clusters: LinkIncident[][] = []
    for (const inc of incs) {
      const incStart = new Date(inc.started_at).getTime()
      // Try to add to the last cluster
      if (clusters.length > 0) {
        const lastCluster = clusters[clusters.length - 1]
        // Find the latest end time in the cluster
        let clusterEnd = -Infinity
        for (const c of lastCluster) {
          if (c.is_ongoing) {
            clusterEnd = Infinity
            break
          }
          const end = c.ended_at ? new Date(c.ended_at).getTime() : new Date(c.started_at).getTime() + (c.duration_seconds || 0) * 1000
          if (end > clusterEnd) clusterEnd = end
        }
        if (incStart <= clusterEnd + gapMs) {
          lastCluster.push(inc)
          continue
        }
      }
      clusters.push([inc])
    }

    // Convert each cluster to a grouped incident
    for (const cluster of clusters) {
      const earliest = cluster.reduce((a, b) =>
        new Date(a.started_at).getTime() < new Date(b.started_at).getTime() ? a : b
      )
      const anyOngoing = cluster.some(i => i.is_ongoing)
      const maxDuration = anyOngoing ? undefined : Math.max(...cluster.map(i => i.duration_seconds || 0))
      result.push({
        link_pk: earliest.link_pk,
        link_code: earliest.link_code,
        link_type: earliest.link_type,
        side_a_metro: earliest.side_a_metro,
        side_z_metro: earliest.side_z_metro,
        contributor_code: earliest.contributor_code,
        is_drained: cluster.some(i => i.is_drained),
        started_at: earliest.started_at,
        is_ongoing: anyOngoing,
        duration_seconds: maxDuration,
        incidents: cluster,
      })
    }
  }
  return result
}

type GroupedDeviceIncident = {
  device_pk: string
  device_code: string
  device_type: string
  metro: string
  contributor_code: string
  is_drained: boolean
  started_at: string
  is_ongoing: boolean
  duration_seconds?: number
  incidents: DeviceIncident[]
}

function groupIncidentsByDevice(incidents: DeviceIncident[]): GroupedDeviceIncident[] {
  const byDevice = new Map<string, DeviceIncident[]>()
  for (const inc of incidents) {
    const existing = byDevice.get(inc.device_pk)
    if (existing) existing.push(inc)
    else byDevice.set(inc.device_pk, [inc])
  }

  const result: GroupedDeviceIncident[] = []
  for (const incs of byDevice.values()) {
    incs.sort((a, b) => new Date(a.started_at).getTime() - new Date(b.started_at).getTime())
    const earliest = incs[0]
    const anyOngoing = incs.some(i => i.is_ongoing)
    const maxDuration = anyOngoing ? undefined : Math.max(...incs.map(i => i.duration_seconds || 0))
    result.push({
      device_pk: earliest.device_pk,
      device_code: earliest.device_code,
      device_type: earliest.device_type,
      metro: earliest.metro,
      contributor_code: earliest.contributor_code,
      is_drained: incs.some(i => i.is_drained),
      started_at: earliest.started_at,
      is_ongoing: anyOngoing,
      duration_seconds: maxDuration,
      incidents: incs,
    })
  }
  return result
}

function ActiveIncidentsTable({
  incidents,
  sortField,
  sortDir,
  toggleSort,
  coalesceGapMinutes = 180,
  typeFilter,
}: {
  incidents: LinkIncident[]
  sortField: string
  sortDir: string
  toggleSort: (field: 'started_at' | 'ended_at' | 'duration') => void
  coalesceGapMinutes?: number
  typeFilter?: Set<string>
}) {
  // Stable timestamp for computing ongoing durations — avoids calling Date.now() during render
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const renderTimestamp = useMemo(() => Date.now(), [incidents])

  const grouped = useMemo(() => {
    let groups = groupIncidentsByLink(incidents, coalesceGapMinutes)
    // Filter rows to those containing at least one incident of a selected type
    if (typeFilter && typeFilter.size > 0) {
      groups = groups.filter(g => g.incidents.some(i => typeFilter.has(i.incident_type)))
    }
    return groups.sort((a, b) => {
      if (sortField === 'started_at') {
        const aTime = new Date(a.started_at).getTime()
        const bTime = new Date(b.started_at).getTime()
        return sortDir === 'asc' ? aTime - bTime : bTime - aTime
      } else if (sortField === 'ended_at') {
        const aEnd = a.is_ongoing ? Infinity : new Date(a.started_at).getTime() + (a.duration_seconds || 0) * 1000
        const bEnd = b.is_ongoing ? Infinity : new Date(b.started_at).getTime() + (b.duration_seconds || 0) * 1000
        return sortDir === 'asc' ? aEnd - bEnd : bEnd - aEnd
      } else {
        const aDur = a.is_ongoing ? Infinity : (a.duration_seconds || 0)
        const bDur = b.is_ongoing ? Infinity : (b.duration_seconds || 0)
        return sortDir === 'asc' ? aDur - bDur : bDur - aDur
      }
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [incidents, coalesceGapMinutes, sortField, sortDir, typeFilter])

  const sortIcon = (field: string) => sortField === field ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''

  return (
    <div className="overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium">Link</th>
            <th className="text-left px-4 py-3 font-medium">Type</th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('started_at')}>
              Started{sortIcon('started_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('ended_at')}>
              Ended{sortIcon('ended_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('duration')}>
              Duration{sortIcon('duration')}
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {grouped.map((group) => {
            // Aggregate incidents by type: one badge per type with combined peak values
            const byType = new Map<string, { peakLossPct?: number; peakCount?: number }>()
            const allInterfaces = new Set<string>()
            for (const inc of group.incidents) {
              for (const iface of inc.affected_interfaces || []) allInterfaces.add(iface)
              const existing = byType.get(inc.incident_type)
              if (existing) {
                if (inc.peak_loss_pct != null) existing.peakLossPct = Math.max(existing.peakLossPct ?? 0, inc.peak_loss_pct)
                if (inc.peak_count != null) existing.peakCount = (existing.peakCount ?? 0) + inc.peak_count
              } else {
                byType.set(inc.incident_type, {
                  peakLossPct: inc.peak_loss_pct ?? undefined,
                  peakCount: inc.peak_count ?? undefined,
                })
              }
            }
            const interfaces = Array.from(allInterfaces)
            return (
              <tr key={`${group.link_pk}-${group.started_at}`} className="hover:bg-muted/30">
                <td className="px-4 py-3">
                  <Link
                    to={`/dz/links/${encodeURIComponent(group.link_pk)}`}
                    state={{ backLabel: 'incidents' }}
                    className="text-primary hover:underline inline-flex items-center gap-1"
                  >
                    {group.link_code}
                    <ExternalLink className="h-3 w-3" />
                  </Link>
                  <div className="text-xs text-muted-foreground">
                    {group.contributor_code} · {group.link_type}
                    <span className="mx-1">·</span>
                    <span className="font-mono">{group.side_a_metro} &rarr; {group.side_z_metro}</span>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1.5 flex-wrap">
                    {Array.from(byType.entries()).map(([type, agg]) => (
                      <span key={type} className="inline-flex items-center gap-1">
                        <IncidentTypeBadge type={type} />
                        {type === 'packet_loss' && agg.peakLossPct != null && (
                          <span className="text-xs text-muted-foreground">
                            ({agg.peakLossPct.toFixed(0)}%)
                          </span>
                        )}
                        {agg.peakCount != null && type !== 'packet_loss' && (
                          <span className="text-xs text-muted-foreground">
                            ({agg.peakCount})
                          </span>
                        )}
                      </span>
                    ))}
                    {group.is_drained && <DrainedBadge />}
                  </div>
                  {interfaces.length > 0 && (
                    <div className="text-xs text-muted-foreground mt-0.5 font-mono">
                      {interfaces.join(', ')}
                    </div>
                  )}
                </td>
                <td className="px-4 py-3">
                  <div>{formatTimeAgo(group.started_at)}</div>
                  <div className="text-xs text-muted-foreground">{formatTimestamp(group.started_at)}</div>
                </td>
                <td className="px-4 py-3">
                  {group.is_ongoing ? (
                    <div className="text-muted-foreground">ongoing</div>
                  ) : group.duration_seconds != null && (() => {
                    const endedIso = new Date(new Date(group.started_at).getTime() + group.duration_seconds! * 1000).toISOString()
                    return (
                      <>
                        <div>{formatTimeAgo(endedIso)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(endedIso)}</div>
                      </>
                    )
                  })()}
                </td>
                <td className="px-4 py-3">
                  {group.is_ongoing
                    ? formatDuration(Math.floor((renderTimestamp - new Date(group.started_at).getTime()) / 1000))
                    : formatDuration(group.duration_seconds)
                  }
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function DrainedLinksTable({ drainedLinks }: { drainedLinks: DrainedLinkInfo[] }) {
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm table-fixed">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium w-[24%]">Link</th>
            <th className="text-left px-4 py-3 font-medium w-[10%]">Route</th>
            <th className="text-left px-4 py-3 font-medium w-[11%]">Drain Status</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Issues</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Started</th>
            <th className="text-left px-4 py-3 font-medium w-[9%] whitespace-nowrap">Clear For</th>
            <th className="text-left px-4 py-3 font-medium w-[14%]">Readiness</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {drainedLinks.map((dl) => (
            <tr key={dl.link_pk} className="hover:bg-muted/30">
              <td className="px-4 py-3 truncate">
                <Link
                  to={`/dz/links/${encodeURIComponent(dl.link_pk)}`}
                  state={{ backLabel: 'incidents' }}
                  className="text-primary hover:underline inline-flex items-center gap-1 max-w-full"
                  title={dl.link_code}
                >
                  <span className="truncate">{dl.link_code}</span>
                  <ExternalLink className="h-3 w-3 shrink-0" />
                </Link>
                <div className="text-xs text-muted-foreground truncate">{dl.contributor_code} · {dl.link_type}</div>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="font-mono">
                  {dl.side_a_metro} &rarr; {dl.side_z_metro}
                </span>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
                  {dl.drain_status}
                </span>
              </td>
              <td className="px-4 py-3">
                {(() => {
                  const allIncidents = [...dl.active_incidents, ...dl.recent_incidents]
                  if (allIncidents.length === 0) {
                    return <span className="text-muted-foreground">-</span>
                  }
                  const types = dedupeIncidentTypes(allIncidents)
                  return (
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {types.map((type) => (
                        <IncidentTypeBadge key={type} type={type} />
                      ))}
                    </div>
                  )
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {(() => {
                  const allIncidents = [...dl.active_incidents, ...dl.recent_incidents]
                  if (allIncidents.length > 0) {
                    const earliest = allIncidents.reduce((a, b) =>
                      new Date(a.started_at) < new Date(b.started_at) ? a : b
                    )
                    return (
                      <>
                        <div>{formatTimeAgo(earliest.started_at)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(earliest.started_at)}</div>
                      </>
                    )
                  }
                  if (dl.drained_since) {
                    return (
                      <>
                        <div className="text-muted-foreground">{formatTimeAgo(dl.drained_since)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(dl.drained_since)}</div>
                      </>
                    )
                  }
                  return <span className="text-muted-foreground">-</span>
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {dl.clear_for_seconds != null ? (
                  formatDuration(dl.clear_for_seconds)
                ) : dl.active_incidents.length > 0 ? (
                  <span className="text-red-600 dark:text-red-400">-</span>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <ReadinessDot readiness={dl.readiness} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ActiveDeviceIncidentsTable({
  incidents,
  sortField,
  sortDir,
  toggleSort,
  typeFilter,
}: {
  incidents: DeviceIncident[]
  sortField: string
  sortDir: string
  toggleSort: (field: 'started_at' | 'ended_at' | 'duration') => void
  typeFilter?: Set<string>
}) {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const renderTimestamp = useMemo(() => Date.now(), [incidents])

  const grouped = useMemo(() => {
    let groups = groupIncidentsByDevice(incidents)
    if (typeFilter && typeFilter.size > 0) {
      groups = groups.filter(g => g.incidents.some(i => typeFilter.has(i.incident_type)))
    }
    return groups.sort((a, b) => {
      if (sortField === 'started_at') {
        const aTime = new Date(a.started_at).getTime()
        const bTime = new Date(b.started_at).getTime()
        return sortDir === 'asc' ? aTime - bTime : bTime - aTime
      } else if (sortField === 'ended_at') {
        const aEnd = a.is_ongoing ? Infinity : new Date(a.started_at).getTime() + (a.duration_seconds || 0) * 1000
        const bEnd = b.is_ongoing ? Infinity : new Date(b.started_at).getTime() + (b.duration_seconds || 0) * 1000
        return sortDir === 'asc' ? aEnd - bEnd : bEnd - aEnd
      } else {
        const aDur = a.is_ongoing ? Infinity : (a.duration_seconds || 0)
        const bDur = b.is_ongoing ? Infinity : (b.duration_seconds || 0)
        return sortDir === 'asc' ? aDur - bDur : bDur - aDur
      }
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [incidents, sortField, sortDir, typeFilter])

  const sortIcon = (field: string) => sortField === field ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''

  return (
    <div className="overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium">Device</th>
            <th className="text-left px-4 py-3 font-medium">Type</th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('started_at')}>
              Started{sortIcon('started_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('ended_at')}>
              Ended{sortIcon('ended_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('duration')}>
              Duration{sortIcon('duration')}
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {grouped.map((group) => {
            const byType = new Map<string, { peakCount?: number }>()
            const allInterfaces = new Set<string>()
            for (const inc of group.incidents) {
              for (const iface of inc.affected_interfaces || []) allInterfaces.add(iface)
              const existing = byType.get(inc.incident_type)
              if (existing) {
                if (inc.peak_count != null) existing.peakCount = (existing.peakCount ?? 0) + inc.peak_count
              } else {
                byType.set(inc.incident_type, {
                  peakCount: inc.peak_count ?? undefined,
                })
              }
            }
            const interfaces = Array.from(allInterfaces)
            return (
            <tr key={group.device_pk + group.started_at} className="hover:bg-muted/30">
              <td className="px-4 py-3">
                <Link
                  to={`/dz/devices/${encodeURIComponent(group.device_pk)}`}
                  className="text-primary hover:underline inline-flex items-center gap-1"
                >
                  {group.device_code}
                  <ExternalLink className="h-3 w-3" />
                </Link>
                <div className="text-xs text-muted-foreground">
                  {group.contributor_code} · {group.device_type}
                  {group.metro && <><span className="mx-1">·</span><span className="font-mono">{group.metro}</span></>}
                </div>
              </td>
              <td className="px-4 py-3">
                <div className="flex items-center gap-1.5 flex-wrap">
                  {Array.from(byType.entries()).map(([type, agg]) => (
                    <span key={type} className="inline-flex items-center gap-1">
                      <IncidentTypeBadge type={type} />
                      {agg.peakCount != null && (
                        <span className="text-xs text-muted-foreground">
                          ({agg.peakCount})
                        </span>
                      )}
                    </span>
                  ))}
                  {group.is_drained && <DrainedBadge />}
                </div>
                {interfaces.length > 0 && (
                  <div className="text-xs text-muted-foreground mt-0.5 font-mono">
                    {interfaces.join(', ')}
                  </div>
                )}
              </td>
              <td className="px-4 py-3">
                <div>{formatTimeAgo(group.started_at)}</div>
                <div className="text-xs text-muted-foreground">{formatTimestamp(group.started_at)}</div>
              </td>
              <td className="px-4 py-3">
                {group.is_ongoing ? (
                  <div className="text-muted-foreground">ongoing</div>
                ) : group.duration_seconds != null && (() => {
                  const endedIso = new Date(new Date(group.started_at).getTime() + group.duration_seconds! * 1000).toISOString()
                  return (
                    <>
                      <div>{formatTimeAgo(endedIso)}</div>
                      <div className="text-xs text-muted-foreground">{formatTimestamp(endedIso)}</div>
                    </>
                  )
                })()}
              </td>
              <td className="px-4 py-3">
                {group.is_ongoing
                  ? formatDuration(Math.floor((renderTimestamp - new Date(group.started_at).getTime()) / 1000))
                  : formatDuration(group.duration_seconds)
                }
              </td>
            </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function DrainedDevicesTable({ drainedDevices }: { drainedDevices: DrainedDeviceInfo[] }) {
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm table-fixed">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium w-[24%]">Device</th>
            <th className="text-left px-4 py-3 font-medium w-[10%]">Metro</th>
            <th className="text-left px-4 py-3 font-medium w-[11%]">Drain Status</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Issues</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Started</th>
            <th className="text-left px-4 py-3 font-medium w-[9%] whitespace-nowrap">Clear For</th>
            <th className="text-left px-4 py-3 font-medium w-[14%]">Readiness</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {drainedDevices.map((dd) => (
            <tr key={dd.device_pk} className="hover:bg-muted/30">
              <td className="px-4 py-3 truncate">
                <Link
                  to={`/dz/devices/${encodeURIComponent(dd.device_pk)}`}
                  className="text-primary hover:underline inline-flex items-center gap-1 max-w-full"
                  title={dd.device_code}
                >
                  <span className="truncate">{dd.device_code}</span>
                  <ExternalLink className="h-3 w-3 shrink-0" />
                </Link>
                <div className="text-xs text-muted-foreground truncate">{dd.contributor_code} · {dd.device_type}</div>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="font-mono">{dd.metro}</span>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
                  {dd.drain_status}
                </span>
              </td>
              <td className="px-4 py-3">
                {(() => {
                  const allIncidents = [...dd.active_incidents, ...dd.recent_incidents]
                  if (allIncidents.length === 0) {
                    return <span className="text-muted-foreground">-</span>
                  }
                  const types = dedupeIncidentTypes(allIncidents)
                  return (
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {types.map((type) => (
                        <IncidentTypeBadge key={type} type={type} />
                      ))}
                    </div>
                  )
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {(() => {
                  const allIncidents = [...dd.active_incidents, ...dd.recent_incidents]
                  if (allIncidents.length > 0) {
                    const earliest = allIncidents.reduce((a, b) =>
                      new Date(a.started_at) < new Date(b.started_at) ? a : b
                    )
                    return (
                      <>
                        <div>{formatTimeAgo(earliest.started_at)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(earliest.started_at)}</div>
                      </>
                    )
                  }
                  if (dd.drained_since) {
                    return (
                      <>
                        <div className="text-muted-foreground">{formatTimeAgo(dd.drained_since)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(dd.drained_since)}</div>
                      </>
                    )
                  }
                  return <span className="text-muted-foreground">-</span>
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {dd.clear_for_seconds != null ? (
                  formatDuration(dd.clear_for_seconds)
                ) : dd.active_incidents.length > 0 ? (
                  <span className="text-red-600 dark:text-red-400">-</span>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <ReadinessDot readiness={dd.readiness} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
