import { useQuery } from '@tanstack/react-query'
import { useState, useMemo } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { ShieldAlert, Settings, ExternalLink, Info, Download } from 'lucide-react'
import {
  fetchLinkIncidents,
  type LinkIncident,
  type DrainedLinkInfo,
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

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

function IncidentsPageSkeleton() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
        <div className="mb-6">
          <Skeleton className="h-8 w-48" />
        </div>
        <div className="mb-6">
          <Skeleton className="h-10 w-full max-w-lg" />
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-20" />
          ))}
        </div>
        <Skeleton className="h-[400px] rounded-lg" />
      </div>
    </div>
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
  return hours > 0 ? `${days}d ${hours}h` : `${days}d`
}

// Client-side confirmed check: ongoing incident is confirmed if elapsed time >= minDuration
function isConfirmed(incident: LinkIncident, minDurationMin: number): boolean {
  if (!incident.is_ongoing) return true
  const elapsedSecs = (Date.now() - new Date(incident.started_at).getTime()) / 1000
  return elapsedSecs >= minDurationMin * 60
}

function formatTimeAgo(isoString: string): string {
  if (isoString === 'unknown') return 'Unknown'
  const date = new Date(isoString)
  if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'Unknown'
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffSecs = Math.floor(diffMs / 1000)

  if (diffSecs < 60) return `${diffSecs}s ago`
  if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`
  if (diffSecs < 86400) return `${Math.floor(diffSecs / 3600)}h ago`
  return `${Math.floor(diffSecs / 86400)}d ago`
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
    carrier: {
      label: 'carrier',
      className: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    },
    no_data: {
      label: 'no data',
      className: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    },
  }
  const c = config[type] || { label: type, className: 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200' }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${c.className}`}>
      {c.label}
    </span>
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

function dedupeIncidentTypes(incidents: LinkIncident[]): string[] {
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

  // Parse URL params with defaults
  const range = (searchParams.get('range') as IncidentTimeRange) || '24h'
  const threshold = parseInt(searchParams.get('threshold') || '10') || 10
  const errorsThreshold = parseInt(searchParams.get('errors_threshold') || '10') || 10
  const discardsThreshold = parseInt(searchParams.get('discards_threshold') || '10') || 10
  const carrierThreshold = parseInt(searchParams.get('carrier_threshold') || '1') || 1
  const typeParam = searchParams.get('type') || ''
  const selectedTypes = useMemo(() => {
    if (!typeParam || typeParam === 'all') return new Set<string>()
    return new Set(typeParam.split(',').filter(Boolean))
  }, [typeParam])
  const minDuration = parseInt(searchParams.get('min_duration') || '30') || 30
  const coalesceGap = parseInt(searchParams.get('coalesce_gap') || '720') || 720
  const view = (searchParams.get('view') as 'active' | 'drained') || 'active'
  const filterParam = searchParams.get('filter') || ''

  const [showSettings, setShowSettings] = useState(false)

  // Local settings state — only applied on "Apply"
  const [localSettings, setLocalSettings] = useState({
    threshold: String(threshold),
    errors_threshold: String(errorsThreshold),
    discards_threshold: String(discardsThreshold),
    carrier_threshold: String(carrierThreshold),
    min_duration: String(minDuration),
    coalesce_gap: String(coalesceGap),
  })

  // Sync local state when URL params change externally
  const settingsKey = `${threshold}-${errorsThreshold}-${discardsThreshold}-${carrierThreshold}-${minDuration}-${coalesceGap}`
  const [lastSettingsKey, setLastSettingsKey] = useState(settingsKey)
  if (settingsKey !== lastSettingsKey) {
    setLastSettingsKey(settingsKey)
    setLocalSettings({
      threshold: String(threshold),
      errors_threshold: String(errorsThreshold),
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
      case 'errors_threshold': return '10'
      case 'discards_threshold': return '10'
      case 'carrier_threshold': return '1'
      case 'min_duration': return '30'
      case 'coalesce_gap': return '720'
      case 'type': return ''
      case 'view': return 'active'
      default: return ''
    }
  }

  const { data, isLoading, error } = useQuery({
    queryKey: ['linkIncidents', range, threshold, errorsThreshold, discardsThreshold, carrierThreshold, minDuration, coalesceGap, filterParam],
    queryFn: () => fetchLinkIncidents({
      range,
      threshold,
      errors_threshold: errorsThreshold,
      discards_threshold: discardsThreshold,
      carrier_threshold: carrierThreshold,
      min_duration: minDuration,
      coalesce_gap: coalesceGap,
      filter: filterParam || undefined,
    }),
    refetchInterval: 60000,
  })

  // Unfiltered summaries for the stat cards (always show all counts)
  const allDrainedSummary = data?.drained_summary || { total: 0, with_incidents: 0, ready: 0, not_ready: 0 }

  // Client-side type filtering
  const hasTypeFilter = selectedTypes.size > 0
  const activeIncidents = useMemo(() => {
    const all = data?.active || []
    if (!hasTypeFilter) return all
    return all.filter(i => selectedTypes.has(i.incident_type))
  }, [data?.active, hasTypeFilter, selectedTypes])

  const drainedLinks = useMemo(() => {
    const all = data?.drained || []
    if (!hasTypeFilter) return all
    // Filter drained links: only show those with at least one incident matching the type filter
    return all.map(dl => ({
      ...dl,
      active_incidents: dl.active_incidents.filter(i => selectedTypes.has(i.incident_type)),
      recent_incidents: dl.recent_incidents.filter(i => selectedTypes.has(i.incident_type)),
    })).filter(dl => dl.active_incidents.length > 0 || dl.recent_incidents.length > 0)
  }, [data?.drained, hasTypeFilter, selectedTypes])

  // Sort state for active view
  const [sortField, setSortField] = useState<'started_at' | 'duration'>('started_at')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [pinOngoing, setPinOngoing] = useState(true)
  const [showDetecting, setShowDetecting] = useState(true)

  const sortedActiveIncidents = useMemo(() => {
    const compare = (a: LinkIncident, b: LinkIncident) => {
      if (sortField === 'started_at') {
        const aTime = new Date(a.started_at).getTime()
        const bTime = new Date(b.started_at).getTime()
        return sortDir === 'asc' ? aTime - bTime : bTime - aTime
      } else {
        const aDur = a.is_ongoing ? Infinity : (a.duration_seconds || 0)
        const bDur = b.is_ongoing ? Infinity : (b.duration_seconds || 0)
        return sortDir === 'asc' ? aDur - bDur : bDur - aDur
      }
    }

    if (!pinOngoing) {
      return [...activeIncidents].sort(compare)
    }
    const ongoing = activeIncidents.filter(i => i.is_ongoing).sort(compare)
    const notOngoing = activeIncidents.filter(i => !i.is_ongoing).sort(compare)
    return [...ongoing, ...notOngoing]
  }, [activeIncidents, sortField, sortDir, pinOngoing])

  const displayedActiveIncidents = useMemo(() => {
    if (showDetecting) return sortedActiveIncidents
    return sortedActiveIncidents.filter(i => !i.is_ongoing || isConfirmed(i, minDuration))
  }, [sortedActiveIncidents, showDetecting, minDuration])

  // Compute counts from filtered data (respects showDetecting toggle)
  const filteredByType = useMemo(() => {
    const all = data?.active || []
    const visible = showDetecting ? all : all.filter(i => !i.is_ongoing || isConfirmed(i, minDuration))
    const byType: Record<string, number> = { packet_loss: 0, errors: 0, discards: 0, carrier: 0, no_data: 0 }
    let ongoing = 0
    for (const i of visible) {
      byType[i.incident_type] = (byType[i.incident_type] || 0) + 1
      if (i.is_ongoing && isConfirmed(i, minDuration)) ongoing++
    }
    return { byType, ongoing }
  }, [data?.active, showDetecting, minDuration])

  const toggleSort = (field: 'started_at' | 'duration') => {
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
    params.set('threshold', threshold.toString())
    params.set('errors_threshold', errorsThreshold.toString())
    params.set('discards_threshold', discardsThreshold.toString())
    params.set('carrier_threshold', carrierThreshold.toString())
    params.set('min_duration', minDuration.toString())
    params.set('coalesce_gap', coalesceGap.toString())
    if (filterParam) params.set('filter', filterParam)
    return `/api/incidents/links/csv?${params.toString()}`
  }, [range, threshold, errorsThreshold, discardsThreshold, carrierThreshold, minDuration, coalesceGap, filterParam])

  if (isLoading) {
    return <IncidentsPageSkeleton />
  }

  if (error) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
          <PageHeader icon={ShieldAlert} title="Incidents" />
          <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
            <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium mb-2">Unable to load incidents</h3>
            <p className="text-sm text-muted-foreground mb-4">
              {(error as Error).message || 'Something went wrong. The API server may be unavailable.'}
            </p>
            <button
              onClick={() => window.location.reload()}
              className="px-4 py-2 text-sm border border-border rounded-md hover:bg-muted transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    )
  }

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
            <button className="px-3 py-1 text-sm rounded bg-background text-foreground shadow-sm">
              Links
            </button>
            <button
              className="px-3 py-1 text-sm rounded text-muted-foreground cursor-not-allowed opacity-50"
              title="Coming soon"
              disabled
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

        {/* Type stat cards — clickable multi-select filters */}
        <div className="grid grid-cols-3 sm:grid-cols-5 gap-3 mb-6">
          {([
            { key: 'packet_loss', label: 'Packet Loss' },
            { key: 'errors', label: 'Errors' },
            { key: 'discards', label: 'Discards' },
            { key: 'carrier', label: 'Carrier' },
            { key: 'no_data', label: 'No Data' },
          ] as const).map(({ key, label }) => {
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
            {activeIncidents.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
                <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
                <h3 className="text-lg font-medium mb-2">No active incidents</h3>
                <p className="text-sm text-muted-foreground">
                  {filters.length > 0 ? 'No incidents match the selected filters ' : 'No incidents '}
                  on non-drained links in the selected time range.
                </p>
              </div>
            ) : (
              <>
                <div className="flex items-center justify-between mb-3">
                  <button
                    type="button"
                    role="switch"
                    aria-checked={pinOngoing}
                    onClick={() => setPinOngoing(!pinOngoing)}
                    className="flex items-center gap-2 text-sm text-muted-foreground"
                  >
                    <span
                      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                        pinOngoing ? 'bg-primary' : 'bg-muted-foreground/30'
                      }`}
                    >
                      <span
                        className={`inline-block h-4 w-4 transform rounded-full bg-background shadow transition-transform ${
                          pinOngoing ? 'translate-x-4' : 'translate-x-0.5'
                        }`}
                      />
                    </span>
                    Pin ongoing to top
                  </button>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={showDetecting}
                    onClick={() => setShowDetecting(!showDetecting)}
                    className="flex items-center gap-2 text-sm text-muted-foreground"
                  >
                    <span
                      className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                        showDetecting ? 'bg-primary' : 'bg-muted-foreground/30'
                      }`}
                    >
                      <span
                        className={`inline-block h-4 w-4 transform rounded-full bg-background shadow transition-transform ${
                          showDetecting ? 'translate-x-4' : 'translate-x-0.5'
                        }`}
                      />
                    </span>
                    Show detecting
                    <span className="relative group">
                      <Info className="h-3.5 w-3.5 text-muted-foreground/50" />
                      <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 px-2 py-1 text-xs bg-popover text-popover-foreground border border-border rounded shadow-md whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50">
                        Above threshold but under min duration
                      </span>
                    </span>
                  </button>
                </div>
                <ActiveIncidentsTable
                  incidents={displayedActiveIncidents}
                  sortField={sortField}
                  sortDir={sortDir}
                  toggleSort={toggleSort}
                  minDuration={minDuration}
                />
              </>
            )}
          </>
        )}

        {/* Drained view */}
        {view === 'drained' && (
          <>
            {drainedLinks.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
                <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
                <h3 className="text-lg font-medium mb-2">No drained links</h3>
                <p className="text-sm text-muted-foreground">
                  {hasTypeFilter ? 'No drained links have issues of the selected type(s).' : 'No links are currently drained.'}
                </p>
              </div>
            ) : (
              <DrainedLinksTable drainedLinks={drainedLinks} />
            )}
          </>
        )}
      </div>
    </div>
  )
}

function ActiveIncidentsTable({
  incidents,
  sortField,
  sortDir,
  toggleSort,
  minDuration,
}: {
  incidents: LinkIncident[]
  sortField: string
  sortDir: string
  toggleSort: (field: 'started_at' | 'duration') => void
  minDuration: number
}) {
  // Stable timestamp for computing ongoing durations — avoids calling Date.now() during render
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const renderTimestamp = useMemo(() => Date.now(), [incidents])

  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium">Link</th>
            <th className="text-left px-4 py-3 font-medium">Route</th>
            <th className="text-left px-4 py-3 font-medium">Type</th>
            <th
              className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground"
              onClick={() => toggleSort('started_at')}
            >
              Started{' '}
              {sortField === 'started_at' && (
                <span className="text-xs">{sortDir === 'asc' ? '↑' : '↓'}</span>
              )}
            </th>
            <th
              className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground"
              onClick={() => toggleSort('duration')}
            >
              Duration{' '}
              {sortField === 'duration' && (
                <span className="text-xs">{sortDir === 'asc' ? '↑' : '↓'}</span>
              )}
            </th>
            <th className="text-left px-4 py-3 font-medium">Status</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {incidents.map((incident) => (
            <tr key={incident.id} className="hover:bg-muted/30">
              <td className="px-4 py-3">
                <Link
                  to={`/dz/links/${encodeURIComponent(incident.link_pk)}`}
                  className="text-primary hover:underline inline-flex items-center gap-1"
                >
                  {incident.link_code}
                  <ExternalLink className="h-3 w-3" />
                </Link>
                <div className="text-xs text-muted-foreground">{incident.contributor_code} · {incident.link_type}</div>
              </td>
              <td className="px-4 py-3">
                <span className="font-mono">
                  {incident.side_a_metro} &rarr; {incident.side_z_metro}
                </span>
              </td>
              <td className="px-4 py-3">
                <div className="flex items-center gap-1.5 flex-wrap">
                  <IncidentTypeBadge type={incident.incident_type} />
                  {incident.is_drained && <DrainedBadge />}
                  {incident.incident_type === 'packet_loss' && incident.peak_loss_pct != null && (
                    <span className="text-xs text-muted-foreground">
                      ({incident.peak_loss_pct.toFixed(0)}%)
                    </span>
                  )}
                  {incident.peak_count != null && incident.incident_type !== 'packet_loss' && (
                    <span className="text-xs text-muted-foreground">
                      ({incident.peak_count})
                    </span>
                  )}
                </div>
              </td>
              <td className="px-4 py-3">
                <div>{formatTimeAgo(incident.started_at)}</div>
                <div className="text-xs text-muted-foreground">
                  {formatTimestamp(incident.started_at)}
                </div>
              </td>
              <td className="px-4 py-3">
                {incident.is_ongoing
                  ? formatDuration(Math.floor((renderTimestamp - new Date(incident.started_at).getTime()) / 1000))
                  : formatDuration(incident.duration_seconds)
                }
              </td>
              <td className="px-4 py-3">
                {incident.is_ongoing && isConfirmed(incident, minDuration) ? (
                  <span className="inline-flex items-center gap-1.5 text-red-600 dark:text-red-400">
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
                    </span>
                    Ongoing
                  </span>
                ) : incident.is_ongoing ? (
                  <span className="inline-flex items-center gap-1.5 text-yellow-600 dark:text-yellow-400">
                    <span className="relative flex h-2 w-2">
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-yellow-500"></span>
                    </span>
                    Detecting
                  </span>
                ) : (
                  <span className="text-muted-foreground">Resolved</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DrainedLinksTable({ drainedLinks }: { drainedLinks: DrainedLinkInfo[] }) {
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium">Link</th>
            <th className="text-left px-4 py-3 font-medium">Route</th>
            <th className="text-left px-4 py-3 font-medium">Drain Status</th>
            <th className="text-left px-4 py-3 font-medium">Issues</th>
            <th className="text-left px-4 py-3 font-medium">Started</th>
            <th className="text-left px-4 py-3 font-medium">Clear For</th>
            <th className="text-left px-4 py-3 font-medium">Readiness</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {drainedLinks.map((dl) => (
            <tr key={dl.link_pk} className="hover:bg-muted/30">
              <td className="px-4 py-3">
                <Link
                  to={`/dz/links/${encodeURIComponent(dl.link_pk)}`}
                  className="text-primary hover:underline inline-flex items-center gap-1"
                >
                  {dl.link_code}
                  <ExternalLink className="h-3 w-3" />
                </Link>
                <div className="text-xs text-muted-foreground">{dl.contributor_code} · {dl.link_type}</div>
              </td>
              <td className="px-4 py-3">
                <span className="font-mono">
                  {dl.side_a_metro} &rarr; {dl.side_z_metro}
                </span>
              </td>
              <td className="px-4 py-3">
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
                      <span className="text-xs text-muted-foreground">({allIncidents.length})</span>
                    </div>
                  )
                })()}
              </td>
              <td className="px-4 py-3">
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
                        <div className="text-xs text-muted-foreground">drained {formatTimestamp(dl.drained_since)}</div>
                      </>
                    )
                  }
                  return <span className="text-muted-foreground">-</span>
                })()}
              </td>
              <td className="px-4 py-3">
                {dl.clear_for_seconds != null ? (
                  formatDuration(dl.clear_for_seconds)
                ) : dl.active_incidents.length > 0 ? (
                  <span className="text-red-600 dark:text-red-400">-</span>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </td>
              <td className="px-4 py-3">
                <ReadinessDot readiness={dl.readiness} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
