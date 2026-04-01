import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useEffect, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { CheckCircle2, AlertTriangle, History, Info, ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchLinkHistory, fetchLinkMetrics } from '@/lib/api'
import type { LinkHistory } from '@/lib/api'
import { LinkPacketLossChart as LinkPacketLossDetailChart } from '@/components/link-charts/LinkPacketLossChart'
import { LinkInterfaceIssuesChart } from '@/components/link-charts/LinkInterfaceIssuesChart'
import { StatusTimeline } from './status-timeline'
import { getEffectiveStatus } from '@/lib/link-status'
import { CriticalityBadge } from './criticality-badge'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

function LinkTimelineSkeleton() {
  return (
    <div className="border border-border rounded-lg">
      <div className="px-4 py-2.5 bg-muted/50 border-b border-border flex items-center gap-2 rounded-t-lg">
        <Skeleton className="h-4 w-4 rounded" />
        <Skeleton className="h-5 w-40" />
        <div className="ml-auto">
          <Skeleton className="h-6 w-48 rounded-lg" />
        </div>
      </div>
      <div className="px-4 py-2 border-b border-border bg-muted/30 flex items-center gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-3 w-16" />
        ))}
      </div>
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} className="px-4 py-3 border-b border-border last:border-b-0">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0 w-5" />
            <div className="flex-shrink-0 w-44 space-y-1.5">
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-3 w-20" />
            </div>
            <div className="flex-1 min-w-0">
              <Skeleton className="h-6 w-full rounded-sm" />
              <div className="flex justify-between mt-1">
                <Skeleton className="h-2.5 w-10" />
                <Skeleton className="h-2.5 w-6" />
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}

type TimeRange = '3h' | '6h' | '12h' | '24h' | '3d' | '7d'

interface LinkStatusTimelinesProps {
  timeRange?: string
  onTimeRangeChange?: (range: TimeRange) => void
  issueFilters?: string[]
  healthFilters?: string[]
  showDrained?: boolean
  onShowDrainedChange?: (show: boolean) => void
  showProvisioning?: boolean
  onShowProvisioningChange?: (show: boolean) => void
  linksWithIssues?: Map<string, string[]>  // Map of link code -> issue reasons (from filter time range)
  linksWithHealth?: Map<string, string>    // Map of link code -> health status (from filter time range)
  criticalityMap?: Map<string, 'critical' | 'important' | 'redundant'>  // Map of link code -> criticality level
}

function formatBandwidth(bps: number): string {
  if (bps >= 1_000_000_000) {
    return `${(bps / 1_000_000_000).toFixed(0)} Gbps`
  } else if (bps >= 1_000_000) {
    return `${(bps / 1_000_000).toFixed(0)} Mbps`
  } else if (bps >= 1_000) {
    return `${(bps / 1_000).toFixed(0)} Kbps`
  }
  return `${bps} bps`
}

function LinkInfoPopover({ link, criticality }: { link: LinkHistory; criticality?: 'critical' | 'important' | 'redundant' }) {
  const [isOpen, setIsOpen] = useState(false)

  const criticalityInfo = {
    critical: {
      label: 'Single Point of Failure',
      description: 'One endpoint has no other connections.',
      className: 'text-red-500',
    },
    important: {
      label: 'Limited Redundancy',
      description: 'Each endpoint has only 2 connections.',
      className: 'text-amber-500',
    },
    redundant: {
      label: 'Well Connected',
      description: 'Both endpoints have 3+ connections.',
      className: 'text-green-500',
    },
  }

  return (
    <div className="relative inline-block">
      <button
        className="text-muted-foreground hover:text-foreground transition-colors p-0.5 -m-0.5"
        onMouseEnter={() => setIsOpen(true)}
        onMouseLeave={() => setIsOpen(false)}
        onClick={() => setIsOpen(!isOpen)}
      >
        <Info className="h-3.5 w-3.5" />
      </button>
      {isOpen && (
        <div
          className="absolute left-0 top-full mt-1 z-50 bg-popover border border-border rounded-lg shadow-lg p-3 min-w-[220px]"
          onMouseEnter={() => setIsOpen(true)}
          onMouseLeave={() => setIsOpen(false)}
        >
          <div className="space-y-2 text-xs">
            <div>
              <div className="text-muted-foreground">Route</div>
              <div className="font-medium">{link.side_a_metro} — {link.side_z_metro}</div>
            </div>
            <div>
              <div className="text-muted-foreground">Devices</div>
              <div className="font-mono text-[11px]">
                <div>{link.side_a_device}</div>
                <div>{link.side_z_device}</div>
              </div>
            </div>
            <div className="flex gap-4">
              <div>
                <div className="text-muted-foreground">Type</div>
                <div className="font-medium">{link.link_type}</div>
              </div>
              {link.bandwidth_bps > 0 && (
                <div>
                  <div className="text-muted-foreground">Bandwidth</div>
                  <div className="font-medium">{formatBandwidth(link.bandwidth_bps)}</div>
                </div>
              )}
            </div>
            {link.committed_rtt_us > 0 && (
              <div>
                <div className="text-muted-foreground">Committed RTT</div>
                <div className="font-medium">{(link.committed_rtt_us / 1000).toFixed(2)} ms</div>
              </div>
            )}
            {criticality && (
              <div className="pt-2 mt-2 border-t border-border">
                <div className="text-muted-foreground">Redundancy</div>
                <div className={`font-medium ${criticalityInfo[criticality].className}`}>
                  {criticalityInfo[criticality].label}
                </div>
                <div className="text-muted-foreground mt-1">
                  {criticalityInfo[criticality].description}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function useBucketCount() {
  const [buckets, setBuckets] = useState(72)

  useEffect(() => {
    const updateBuckets = () => {
      const width = window.innerWidth
      if (width < 640) {
        setBuckets(24) // mobile
      } else if (width < 1024) {
        setBuckets(48) // tablet
      } else {
        setBuckets(72) // desktop
      }
    }

    updateBuckets()
    window.addEventListener('resize', updateBuckets)
    return () => window.removeEventListener('resize', updateBuckets)
  }, [])

  return buckets
}

const cardClass = "rounded-lg border border-border p-4"

interface LinkRowProps {
  link: LinkHistory
  linksWithIssues?: Map<string, string[]>
  criticalityMap?: Map<string, 'critical' | 'important' | 'redundant'>
  bucketMinutes?: number
  dataTimeRange?: string
  metricsTimeRange: string
}

function LinkRow({ link, linksWithIssues, criticalityMap, bucketMinutes = 60, dataTimeRange, metricsTimeRange }: LinkRowProps) {
  const [expanded, setExpanded] = useState(false)

  const { data: metrics, isFetching: metricsFetching } = useQuery({
    queryKey: ['linkMetrics', link.pk, { range: metricsTimeRange }],
    queryFn: () => fetchLinkMetrics(link.pk, { range: metricsTimeRange }),
    enabled: expanded,
  })

  const issueReasons = linksWithIssues
    ? (linksWithIssues.get(link.code) ?? [])
    : (link.issue_reasons ?? [])

  return (
    <div id={`link-row-${link.code}`} className="border-b border-border last:border-b-0">
      <div
        className="px-4 py-3 transition-colors cursor-pointer hover:bg-muted/30"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-start gap-4">
          {/* Expand/collapse indicator */}
          <div className="flex-shrink-0 w-5 pt-0.5">
            {expanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
          </div>

          {/* Link info */}
          <div className="flex-shrink-0 w-52 sm:w-60 lg:w-68">
            <div className="flex items-center gap-1.5">
              <Link to={`/dz/links/${link.pk}`} state={{ backLabel: 'status' }} className="font-mono text-sm truncate hover:underline" title={link.code} onClick={(e) => e.stopPropagation()}>
                {link.code}
              </Link>
              <LinkInfoPopover link={link} criticality={criticalityMap?.get(link.code)} />
              {criticalityMap?.get(link.code) && criticalityMap.get(link.code) !== 'redundant' && (
                <CriticalityBadge criticality={criticalityMap.get(link.code)!} />
              )}
            </div>
            <div className="text-xs text-muted-foreground">
              {link.link_type}{link.contributor && ` · ${link.contributor}`} · {link.side_a_metro} ↔ {link.side_z_metro}
            </div>
            {(link.is_down || link.drain_status || link.provisioning || issueReasons.length > 0) && (
              <div className="flex flex-wrap gap-1 mt-1">
                {link.is_down && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-gray-900/15 text-gray-900 dark:bg-gray-400/20 dark:text-gray-300">Down</span>
                )}
                {link.drain_status && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-gray-900/15 text-gray-900 dark:bg-gray-400/20 dark:text-gray-300">{link.drain_status === 'hard-drained' ? 'Hard Drained' : 'Soft Drained'}</span>
                )}
                {link.provisioning && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-blue-500/15 text-blue-700 dark:bg-blue-400/20 dark:text-blue-300">Provisioning</span>
                )}
                {issueReasons.includes('packet_loss') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(168, 85, 247, 0.15)', color: '#9333ea' }}>Loss</span>
                )}
                {issueReasons.includes('high_latency') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(59, 130, 246, 0.15)', color: '#2563eb' }}>High Latency</span>
                )}
                {issueReasons.includes('high_utilization') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(99, 102, 241, 0.15)', color: '#4f46e5' }}>High Utilization</span>
                )}
                {issueReasons.includes('no_data') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(236, 72, 153, 0.15)', color: '#db2777' }}>No Data</span>
                )}
                {issueReasons.includes('interface_errors') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(239, 68, 68, 0.15)', color: '#dc2626' }}>Errors</span>
                )}
                {issueReasons.includes('fcs_errors') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(249, 115, 22, 0.15)', color: '#ea580c' }}>FCS Errors</span>
                )}
                {issueReasons.includes('discards') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(20, 184, 166, 0.15)', color: '#0d9488' }}>Discards</span>
                )}
                {issueReasons.includes('carrier_transitions') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium" style={{ backgroundColor: 'rgba(234, 179, 8, 0.15)', color: '#ca8a04' }}>Carrier Transitions</span>
                )}
                {issueReasons.includes('missing_adjacency') && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-rose-600/15 text-rose-700 dark:text-rose-400">ISIS Down</span>
                )}
              </div>
            )}
          </div>

          {/* Timeline */}
          <div className="flex-1 min-w-0">
            <StatusTimeline
              hours={link.hours}
              committedRttUs={link.committed_rtt_us}
              bucketMinutes={bucketMinutes}
              timeRange={dataTimeRange}
            />
          </div>
        </div>
      </div>

      {/* Expanded charts — aligned with the timeline column */}
      {expanded && (
        <div className="px-4 pb-4 pt-0">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0 w-5" />
            <div className="flex-shrink-0 w-52 sm:w-60 lg:w-68" />
            <div className="flex-1 min-w-0 space-y-4">
              {metrics && (() => {
                const hasLoss = metrics.buckets.some(b => b.latency && (b.latency.a_loss_pct > 0 || b.latency.z_loss_pct > 0))
                const hasIssues = metrics.buckets.some(b => b.traffic && (
                  b.traffic.side_a_in_errors + b.traffic.side_a_out_errors + b.traffic.side_z_in_errors + b.traffic.side_z_out_errors > 0 ||
                  b.traffic.side_a_in_fcs_errors + b.traffic.side_z_in_fcs_errors > 0 ||
                  b.traffic.side_a_in_discards + b.traffic.side_a_out_discards + b.traffic.side_z_in_discards + b.traffic.side_z_out_discards > 0 ||
                  b.traffic.side_a_carrier_transitions + b.traffic.side_z_carrier_transitions > 0
                ))
                if (!hasLoss && !hasIssues) return null
                return (
                  <>
                    {hasLoss && <LinkPacketLossDetailChart data={metrics} loading={metricsFetching} className={cardClass} />}
                    {hasIssues && <LinkInterfaceIssuesChart data={metrics} loading={metricsFetching} className={cardClass} />}
                  </>
                )
              })()}
              {!metrics && metricsFetching && (
                <div className="flex justify-center py-8">
                  <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export function LinkStatusTimelines({
  timeRange = '24h',
  onTimeRangeChange,
  issueFilters = ['packet_loss', 'high_latency', 'high_utilization', 'interface_errors', 'fcs_errors', 'discards', 'carrier_transitions', 'missing_adjacency'],
  healthFilters = ['healthy', 'degraded', 'unhealthy'],
  showDrained = false,
  onShowDrainedChange,
  showProvisioning = false,
  onShowProvisioningChange,
  linksWithIssues,
  linksWithHealth,
  criticalityMap,
}: LinkStatusTimelinesProps) {
  const timeRangeOptions: { value: TimeRange; label: string }[] = [
    { value: '3h', label: '3h' },
    { value: '6h', label: '6h' },
    { value: '12h', label: '12h' },
    { value: '24h', label: '24h' },
    { value: '3d', label: '3d' },
    { value: '7d', label: '7d' },
  ]
  const buckets = useBucketCount()

  const { data, isLoading, isPlaceholderData, error } = useQuery({
    queryKey: ['link-history', timeRange, buckets],
    queryFn: () => fetchLinkHistory(timeRange, buckets),
    refetchInterval: 60_000, // Refresh every minute
    staleTime: 30_000,
    placeholderData: keepPreviousData,
  })

  // Helper to check if a link matches health filters
  // Uses linksWithHealth (from filter time range) if provided, otherwise falls back to link's own hours
  const linkMatchesHealthFilters = (link: LinkHistory): boolean => {
    // If we have health data from the filter time range, use it
    if (linksWithHealth && linksWithHealth.size > 0) {
      const health = linksWithHealth.get(link.code)
      if (health) {
        // Map no_data and down to unhealthy for filter matching (not separate filter options)
        const filterHealth = (health === 'no_data' || health === 'down') ? 'unhealthy' : health
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return healthFilters.includes(filterHealth as any)
      }
      // Link not in filter data - check if it exists in history
      return false
    }

    // Fallback: check link's own hours data
    if (!link.hours || link.hours.length === 0) return false
    return link.hours.some(hour => {
      const status = hour.status
      if (status === 'healthy' && healthFilters.includes('healthy')) return true
      if (status === 'degraded' && healthFilters.includes('degraded')) return true
      if (status === 'unhealthy' && healthFilters.includes('unhealthy')) return true
      if (status === 'no_data' && healthFilters.includes('unhealthy')) return true // no_data maps to unhealthy
      return false
    })
  }

  // Check which issue filters are selected
  const issueTypesSelected = issueFilters.filter(f => f !== 'no_issues')
  const noIssuesSelected = issueFilters.includes('no_issues')
  const noDataSelected = issueFilters.includes('no_data')

  // Filter and sort links by recency of issues
  const filteredLinks = useMemo(() => {
    if (!data?.links) return []

    // Filter by issue reasons (from filter time range if provided) AND health status
    const filtered = data.links.filter(link => {
      // Use linksWithIssues if provided - if link not in map, it has no issues in the filter time range
      // Only fall back to link.issue_reasons if linksWithIssues is not provided at all
      const issueReasons = linksWithIssues && linksWithIssues.size > 0
        ? (linksWithIssues.get(link.code) ?? [])
        : (link.issue_reasons ?? [])
      const hasIssues = issueReasons.length > 0

      // When no_data filter is off, exclude links whose only issue is no_data
      // UNLESS the link has non-healthy buckets (unhealthy/degraded from missing data)
      if (!noDataSelected && issueReasons.length === 1 && issueReasons[0] === 'no_data') {
        const hasNonHealthyBuckets = link.hours?.some(h =>
          !h.collecting && (h.status === 'unhealthy' || h.status === 'degraded')
        )
        if (!hasNonHealthyBuckets) {
          return false
        }
      }

      // Hide currently drained links unless showDrained is enabled
      if (link.drain_status && !showDrained) {
        return false
      }

      // Hide provisioning links unless showProvisioning is enabled
      if (link.provisioning && !showProvisioning) {
        return false
      }

      const matchesIssue = hasIssues
        ? issueReasons.some(reason => issueTypesSelected.includes(reason)) ||
          (issueReasons.length === 1 && issueReasons[0] === 'no_data' && link.hours?.some(h =>
            !h.collecting && (h.status === 'unhealthy' || h.status === 'degraded')
          ))
        : noIssuesSelected

      // Must match at least one health filter
      const matchesHealth = linkMatchesHealthFilters(link)

      return matchesIssue && matchesHealth
    })

    // Sort by: 1) recent severity (worst in last 6 buckets), 2) overall worst severity,
    // 3) most recent issue timestamp, 4) total issue count, 5) alphabetical.
    // Uses getEffectiveStatus to account for ISIS down, interface issues, and latency
    // overages that aren't reflected in the raw status field.
    // "Recent severity" ensures a link that's down right now sorts above one that had
    // a brief issue 12 hours ago, even if both have the same worst-ever severity.
    const statusSeverity = (status: string): number => {
      switch (status) {
        case 'down':
        case 'unhealthy': return 3
        case 'degraded': return 2
        case 'no_data': return 1
        default: return 0
      }
    }

    const RECENT_BUCKETS = 6

    return filtered.sort((a, b) => {
      const getSortKey = (link: LinkHistory): { recent: number; worst: number; latestTs: string; count: number } => {
        if (!link.hours) return { recent: 0, worst: 0, latestTs: '', count: 0 }
        let worst = 0
        let recent = 0
        let latestTs = ''
        let count = 0
        const recentStart = Math.max(0, link.hours.length - RECENT_BUCKETS)
        for (let i = 0; i < link.hours.length; i++) {
          const sev = statusSeverity(getEffectiveStatus(link.hours[i]))
          if (sev > 0) {
            count++
            if (sev > worst) worst = sev
            if (i >= recentStart && sev > recent) recent = sev
            if (link.hours[i].hour > latestTs) latestTs = link.hours[i].hour
          }
        }
        return { recent, worst, latestTs, count }
      }

      const aInfo = getSortKey(a)
      const bInfo = getSortKey(b)

      // Recent severity first (what's happening now)
      if (aInfo.recent !== bInfo.recent) return bInfo.recent - aInfo.recent
      // Overall worst severity
      if (aInfo.worst !== bInfo.worst) return bInfo.worst - aInfo.worst
      // Most recent issue first (by timestamp, not index)
      if (aInfo.latestTs !== bInfo.latestTs) return aInfo.latestTs < bInfo.latestTs ? 1 : -1
      // More total issues first
      if (aInfo.count !== bInfo.count) return bInfo.count - aInfo.count
      // Alphabetical fallback
      return a.code.localeCompare(b.code)
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.links, issueFilters, healthFilters, noIssuesSelected, issueTypesSelected, showDrained, showProvisioning, linksWithIssues, linksWithHealth])

  const drainedCount = useMemo(() => {
    if (!data?.links) return 0
    return data.links.filter(link => link.drain_status).length
  }, [data?.links])

  const provisioningCount = useMemo(() => {
    if (!data?.links) return 0
    return data.links.filter(link => link.provisioning).length
  }, [data?.links])

  const showSkeleton = useDelayedLoading(isLoading && !data)

  if (isLoading && !data) {
    return showSkeleton ? <LinkTimelineSkeleton /> : null
  }

  if (error) {
    return (
      <div className="border border-border rounded-lg p-6 text-center">
        <AlertTriangle className="h-8 w-8 text-amber-500 mx-auto mb-2" />
        <div className="text-sm text-muted-foreground">Unable to load link history</div>
      </div>
    )
  }

  if (filteredLinks.length === 0) {
    return (
      <div className="border border-border rounded-lg p-6 text-center">
        <CheckCircle2 className="h-8 w-8 text-green-500 mx-auto mb-2" />
        <div className="text-sm text-muted-foreground">
          {data?.links.length === 0
            ? 'No links available in the selected time range'
            : 'No links match the selected filters'}
        </div>
      </div>
    )
  }

  return (
    <div className={`border border-border rounded-lg transition-opacity${isPlaceholderData ? ' opacity-60' : ''}`}>
      <div className="px-4 py-2.5 bg-muted/50 border-b border-border flex items-center gap-2 rounded-t-lg">
        {isPlaceholderData
          ? <Loader2 className="h-4 w-4 text-muted-foreground animate-spin" />
          : <History className="h-4 w-4 text-muted-foreground" />
        }
        <h3 className="font-medium">
          Link Status History
          <span className="text-sm text-muted-foreground font-normal ml-1">
            ({filteredLinks.length} link{filteredLinks.length !== 1 ? 's' : ''})
          </span>
        </h3>
        <div className="flex items-center gap-2 ml-auto">
          {onShowDrainedChange && (
            <button
              onClick={() => onShowDrainedChange(!showDrained)}
              className="flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md border border-border bg-background/50 transition-colors hover:bg-muted/50"
            >
              <div className={`w-3 h-3 rounded-sm transition-colors ${showDrained ? 'bg-primary' : 'bg-muted-foreground/20 border border-muted-foreground/30'}`}>
                {showDrained && (
                  <svg viewBox="0 0 12 12" className="w-3 h-3 text-primary-foreground">
                    <path d="M3.5 6L5.5 8L8.5 4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </div>
              <span className={showDrained ? 'text-foreground' : 'text-muted-foreground'}>Drained ({drainedCount})</span>
            </button>
          )}
          {onShowProvisioningChange && (
            <button
              onClick={() => onShowProvisioningChange(!showProvisioning)}
              className="flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md border border-border bg-background/50 transition-colors hover:bg-muted/50"
            >
              <div className={`w-3 h-3 rounded-sm transition-colors ${showProvisioning ? 'bg-primary' : 'bg-muted-foreground/20 border border-muted-foreground/30'}`}>
                {showProvisioning && (
                  <svg viewBox="0 0 12 12" className="w-3 h-3 text-primary-foreground">
                    <path d="M3.5 6L5.5 8L8.5 4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
              </div>
              <span className={showProvisioning ? 'text-foreground' : 'text-muted-foreground'}>Provisioning ({provisioningCount})</span>
            </button>
          )}
          {onTimeRangeChange && (
            <div className="inline-flex rounded-lg border border-border bg-background/50 p-0.5">
              {timeRangeOptions.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => onTimeRangeChange(opt.value)}
                  className={`px-2.5 py-0.5 text-xs rounded-md transition-colors ${
                    timeRange === opt.value
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Legend */}
      <div className="px-4 py-2 border-b border-border bg-muted/30 flex items-center gap-4 text-xs text-muted-foreground">
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-green-500" />
          <span>Healthy</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-amber-500" />
          <span>Degraded</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-red-500" />
          <span>Unhealthy</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-transparent border border-gray-200 dark:border-gray-700" />
          <span>No Data</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-sm bg-muted-foreground/20 border border-muted-foreground/30" style={{ backgroundImage: 'repeating-linear-gradient(135deg, rgba(120,120,120,0.9), rgba(120,120,120,0.9) 1.5px, transparent 1.5px, transparent 3px)' }} />
          <span>Drained</span>
        </div>
      </div>

      <div>
        {filteredLinks.map((link) => (
          <LinkRow
            key={link.code}
            link={link}
            linksWithIssues={linksWithIssues}
            criticalityMap={criticalityMap}
            bucketMinutes={data?.bucket_minutes}
            dataTimeRange={data?.time_range}
            metricsTimeRange={timeRange}
          />
        ))}
      </div>
    </div>
  )
}
