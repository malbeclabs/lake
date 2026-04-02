import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { CheckCircle2, AlertTriangle, History, Info, ChevronDown, ChevronUp, Loader2 } from 'lucide-react'
import { fetchBulkLinkMetrics, fetchLinkMetrics } from '@/lib/api'
import type { LinkMetricsResponse, LinkMetricsBucket } from '@/lib/api'
import { LinkPacketLossChart as LinkPacketLossDetailChart } from '@/components/link-charts/LinkPacketLossChart'
import { LinkInterfaceIssuesChart } from '@/components/link-charts/LinkInterfaceIssuesChart'
import { LinkHealthTimeline } from '@/components/link-charts/LinkHealthTimeline'
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

interface DerivedLinkInfo {
  pk: string
  code: string
  linkType: string
  contributor: string
  sideAMetro: string
  sideZMetro: string
  bandwidthBps: number
  committedRttUs: number
  issueReasons: string[]
  isDown: boolean
  drainStatus: string
  provisioning: boolean
  health: string  // worst health across buckets
}

function deriveLinkInfo(metrics: LinkMetricsResponse): DerivedLinkInfo {
  const issueReasons = new Set<string>()
  let worstHealth = 'healthy'
  let isDown = false
  let drainStatus = ''
  let provisioning = false

  const healthPriority: Record<string, number> = {
    unhealthy: 4,
    down: 3,
    degraded: 2,
    no_data: 1,
    healthy: 0,
  }

  for (const b of metrics.buckets) {
    // Status-derived info
    if (b.status) {
      const bHealth = b.status.health || 'no_data'
      if ((healthPriority[bHealth] ?? 0) > (healthPriority[worstHealth] ?? 0)) {
        worstHealth = bHealth
      }
      if (b.status.isis_down) {
        issueReasons.add('missing_adjacency')
      }
      if (b.status.drain_status) {
        drainStatus = b.status.drain_status
      }
      if (b.status.provisioning) {
        provisioning = true
      }
    }

    // Latency-derived issues
    if (b.latency) {
      if (b.latency.a_loss_pct > 0 || b.latency.z_loss_pct > 0) {
        issueReasons.add('packet_loss')
      }
      if (metrics.committed_rtt_us > 0) {
        const avgRtt = (b.latency.a_avg_rtt_us + b.latency.z_avg_rtt_us) / 2
        if (avgRtt > metrics.committed_rtt_us * 1.2) {
          issueReasons.add('high_latency')
        }
      }
    }

    // Traffic-derived issues
    if (b.traffic) {
      const t = b.traffic
      if (t.side_a_in_errors + t.side_a_out_errors + t.side_z_in_errors + t.side_z_out_errors > 0) {
        issueReasons.add('interface_errors')
      }
      if (t.side_a_in_fcs_errors + t.side_z_in_fcs_errors > 0) {
        issueReasons.add('fcs_errors')
      }
      if (t.side_a_in_discards + t.side_a_out_discards + t.side_z_in_discards + t.side_z_out_discards > 0) {
        issueReasons.add('discards')
      }
      if (t.side_a_carrier_transitions + t.side_z_carrier_transitions > 0) {
        issueReasons.add('carrier_transitions')
      }
      if (t.utilization_in_pct > 80 || t.utilization_out_pct > 80) {
        issueReasons.add('high_utilization')
      }
    }

    // No data detection: non-collecting bucket with no_data health
    if (b.status && !b.status.collecting && b.status.health === 'no_data') {
      issueReasons.add('no_data')
    }
  }

  // Check if the link is down: look at the latest non-collecting bucket
  for (let i = metrics.buckets.length - 1; i >= 0; i--) {
    const b = metrics.buckets[i]
    if (b.status && !b.status.collecting) {
      if (b.status.health === 'down' || b.status.isis_down) {
        isDown = true
      }
      break
    }
  }

  return {
    pk: metrics.link_pk,
    code: metrics.link_code,
    linkType: metrics.link_type,
    contributor: metrics.contributor_code,
    sideAMetro: metrics.side_a_metro,
    sideZMetro: metrics.side_z_metro,
    bandwidthBps: metrics.bandwidth_bps,
    committedRttUs: metrics.committed_rtt_us,
    issueReasons: Array.from(issueReasons),
    isDown,
    drainStatus,
    provisioning,
    health: worstHealth,
  }
}

function LinkInfoPopover({ linkMetrics, criticality }: { linkMetrics: LinkMetricsResponse; criticality?: 'critical' | 'important' | 'redundant' }) {
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
              <div className="font-medium">{linkMetrics.side_a_metro} — {linkMetrics.side_z_metro}</div>
            </div>
            <div className="flex gap-4">
              <div>
                <div className="text-muted-foreground">Type</div>
                <div className="font-medium">{linkMetrics.link_type}</div>
              </div>
              {linkMetrics.bandwidth_bps > 0 && (
                <div>
                  <div className="text-muted-foreground">Bandwidth</div>
                  <div className="font-medium">{formatBandwidth(linkMetrics.bandwidth_bps)}</div>
                </div>
              )}
            </div>
            {linkMetrics.committed_rtt_us > 0 && (
              <div>
                <div className="text-muted-foreground">Committed RTT</div>
                <div className="font-medium">{(linkMetrics.committed_rtt_us / 1000).toFixed(2)} ms</div>
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

// Map server-provided reasons and traffic data to badge issue keys.
function addBucketIssues(b: LinkMetricsBucket, issues: Set<string>) {
  // Use server reasons (covers latency-based issues even without latency data)
  if (b.status?.reasons) {
    for (const r of b.status.reasons) {
      if (r.includes('packet loss')) issues.add('packet_loss')
      if (r.includes('latency')) issues.add('high_latency')
      if (r.includes('interface error')) issues.add('interface_errors')
      if (r.includes('discard')) issues.add('discards')
      if (r.includes('carrier')) issues.add('carrier_transitions')
      if (r.includes('One-sided')) issues.add('no_data')
    }
  }
  // Also check traffic data directly (for FCS errors and utilization not in reasons)
  if (b.traffic) {
    const t = b.traffic
    if (t.side_a_in_errors + t.side_a_out_errors + t.side_z_in_errors + t.side_z_out_errors > 0) issues.add('interface_errors')
    if (t.side_a_in_fcs_errors + t.side_z_in_fcs_errors > 0) issues.add('fcs_errors')
    if (t.side_a_in_discards + t.side_a_out_discards + t.side_z_in_discards + t.side_z_out_discards > 0) issues.add('discards')
    if (t.side_a_carrier_transitions + t.side_z_carrier_transitions > 0) issues.add('carrier_transitions')
    if (t.utilization_in_pct > 80 || t.utilization_out_pct > 80) issues.add('high_utilization')
  }
  if (b.status && !b.status.collecting && b.status.health === 'no_data') issues.add('no_data')
  if (b.status?.isis_down) issues.add('missing_adjacency')
}

const cardClass = "rounded-lg border border-border p-4"

interface LinkRowProps {
  linkMetrics: LinkMetricsResponse
  derivedInfo: DerivedLinkInfo
  linksWithIssues?: Map<string, string[]>
  criticalityMap?: Map<string, 'critical' | 'important' | 'redundant'>
  metricsTimeRange: string
}

function LinkRow({ linkMetrics, derivedInfo, linksWithIssues, criticalityMap, metricsTimeRange }: LinkRowProps) {
  const [expanded, setExpanded] = useState(false)
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{ start: number; end: number } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  // Fetch full metrics (with latency) on expand for packet loss chart
  const { data: fullMetrics, isFetching: metricsFetching } = useQuery({
    queryKey: ['linkMetrics', derivedInfo.pk, { range: metricsTimeRange }],
    queryFn: () => fetchLinkMetrics(derivedInfo.pk, { range: metricsTimeRange }),
    enabled: expanded,
  })

  const issueReasons = linksWithIssues
    ? (linksWithIssues.get(derivedInfo.code) ?? [])
    : derivedInfo.issueReasons

  const nowMinutes = Math.floor(Date.now() / 60000)
  const recentIssues = useMemo(() => {
    const recent = new Set<string>()
    const cutoff = nowMinutes * 60 - 30 * 60
    for (const b of linkMetrics.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < cutoff) continue
      addBucketIssues(b, recent)
    }
    return recent
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [linkMetrics, nowMinutes])

  const hoveredIssues = useMemo(() => {
    if (!hoveredTimeRange) return null
    const issues = new Set<string>()
    for (const b of linkMetrics.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < hoveredTimeRange.start || ts >= hoveredTimeRange.end) continue
      addBucketIssues(b, issues)
    }
    return issues
  }, [hoveredTimeRange, linkMetrics])

  const isBadgeActive = (issue: string) => {
    if (hoveredIssues) return hoveredIssues.has(issue)
    return recentIssues.has(issue)
  }

  const dimBadgeClass = 'bg-muted-foreground/10 text-muted-foreground/50'

  // Worst health in recent window (last 30 min) for left border indicator
  const recentHealth = useMemo(() => {
    const cutoff = nowMinutes * 60 - 30 * 60
    let worstHealth = 'healthy'
    let isisDown = false
    const priority: Record<string, number> = { unhealthy: 3, degraded: 2, no_data: 1, healthy: 0 }
    for (const b of linkMetrics.buckets) {
      const ts = new Date(b.ts).getTime() / 1000
      if (ts < cutoff) continue
      if (b.status) {
        if (b.status.isis_down) isisDown = true
        const h = b.status.health || 'healthy'
        if ((priority[h] ?? 0) > (priority[worstHealth] ?? 0)) worstHealth = h
      }
    }
    return { health: worstHealth, isisDown }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [linkMetrics, nowMinutes])

  const leftBorderColor = recentHealth.isisDown
    ? 'border-l-gray-500'
    : recentHealth.health === 'unhealthy'
      ? 'border-l-red-500'
      : recentHealth.health === 'degraded'
        ? 'border-l-amber-500'
        : 'border-l-transparent'

  // Has expandable content: packet loss or interface issues
  const hasExpandableContent = issueReasons.some(r =>
    r === 'packet_loss' || r === 'interface_errors' || r === 'fcs_errors' || r === 'discards' || r === 'carrier_transitions'
  )

  return (
    <div id={`link-row-${derivedInfo.code}`} className={`border-b border-border last:border-b-0 border-l-2 ${leftBorderColor}`}>
      <div
        className={`px-4 py-3 transition-colors ${hasExpandableContent ? 'cursor-pointer hover:bg-muted/30' : ''}`}
        onClick={hasExpandableContent ? () => setExpanded(!expanded) : undefined}
      >
        <div className="flex items-start gap-4">
          {/* Expand/collapse indicator */}
          <div className="flex-shrink-0 w-5 pt-0.5">
            {hasExpandableContent ? (
              expanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />
            ) : (
              <div className="w-4" />
            )}
          </div>

          {/* Link info */}
          <div className="flex-shrink-0 w-52 sm:w-60 lg:w-68">
            <div className="flex items-center gap-1.5">
              <Link to={`/dz/links/${derivedInfo.pk}`} state={{ backLabel: 'status' }} className="font-mono text-sm truncate hover:underline" title={derivedInfo.code} onClick={(e) => e.stopPropagation()}>
                {derivedInfo.code}
              </Link>
              <LinkInfoPopover linkMetrics={linkMetrics} criticality={criticalityMap?.get(derivedInfo.code)} />
            </div>
            <div className="text-xs text-muted-foreground">
              {derivedInfo.linkType}{derivedInfo.contributor && ` · ${derivedInfo.contributor}`} · {derivedInfo.sideAMetro} ↔ {derivedInfo.sideZMetro}
            </div>
            {(derivedInfo.isDown || derivedInfo.drainStatus || derivedInfo.provisioning || issueReasons.length > 0) && (
              <div className="flex flex-wrap gap-1 mt-1">
                {derivedInfo.isDown && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-gray-900/15 text-gray-900 dark:bg-gray-400/20 dark:text-gray-300">Down</span>
                )}
                {derivedInfo.drainStatus && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-gray-900/15 text-gray-900 dark:bg-gray-400/20 dark:text-gray-300">{derivedInfo.drainStatus === 'hard-drained' ? 'Hard Drained' : 'Soft Drained'}</span>
                )}
                {derivedInfo.provisioning && (
                  <span className="text-[10px] px-1.5 py-0.5 rounded font-medium bg-blue-500/15 text-blue-700 dark:bg-blue-400/20 dark:text-blue-300">Provisioning</span>
                )}
                {issueReasons.includes('packet_loss') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('packet_loss') ? '' : dimBadgeClass}`} style={isBadgeActive('packet_loss') ? { backgroundColor: 'rgba(168, 85, 247, 0.15)', color: '#9333ea' } : undefined}>Loss</span>
                )}
                {issueReasons.includes('high_latency') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('high_latency') ? '' : dimBadgeClass}`} style={isBadgeActive('high_latency') ? { backgroundColor: 'rgba(59, 130, 246, 0.15)', color: '#2563eb' } : undefined}>High Latency</span>
                )}
                {issueReasons.includes('high_utilization') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('high_utilization') ? '' : dimBadgeClass}`} style={isBadgeActive('high_utilization') ? { backgroundColor: 'rgba(99, 102, 241, 0.15)', color: '#4f46e5' } : undefined}>High Utilization</span>
                )}
                {issueReasons.includes('no_data') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('no_data') ? '' : dimBadgeClass}`} style={isBadgeActive('no_data') ? { backgroundColor: 'rgba(236, 72, 153, 0.15)', color: '#db2777' } : undefined}>No Data</span>
                )}
                {issueReasons.includes('interface_errors') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('interface_errors') ? '' : dimBadgeClass}`} style={isBadgeActive('interface_errors') ? { backgroundColor: 'rgba(239, 68, 68, 0.15)', color: '#dc2626' } : undefined}>Errors</span>
                )}
                {issueReasons.includes('fcs_errors') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('fcs_errors') ? '' : dimBadgeClass}`} style={isBadgeActive('fcs_errors') ? { backgroundColor: 'rgba(249, 115, 22, 0.15)', color: '#ea580c' } : undefined}>FCS Errors</span>
                )}
                {issueReasons.includes('discards') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('discards') ? '' : dimBadgeClass}`} style={isBadgeActive('discards') ? { backgroundColor: 'rgba(20, 184, 166, 0.15)', color: '#0d9488' } : undefined}>Discards</span>
                )}
                {issueReasons.includes('carrier_transitions') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('carrier_transitions') ? '' : dimBadgeClass}`} style={isBadgeActive('carrier_transitions') ? { backgroundColor: 'rgba(234, 179, 8, 0.15)', color: '#ca8a04' } : undefined}>Carrier Transitions</span>
                )}
                {issueReasons.includes('missing_adjacency') && (
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium transition-all ${isBadgeActive('missing_adjacency') ? 'bg-rose-600/15 text-rose-700 dark:text-rose-400' : dimBadgeClass}`}>ISIS Down</span>
                )}
              </div>
            )}
          </div>

          {/* Timeline */}
          <div className="flex-1 min-w-0">
            <LinkHealthTimeline data={linkMetrics} hideBadges onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />
          </div>
        </div>
      </div>

      {/* Expanded charts */}
      {expanded && (
        <div className="px-4 pb-4 pt-2 space-y-4">
          {/* Packet loss chart — needs latency data from per-link fetch */}
          {fullMetrics && (() => {
            const hasLoss = fullMetrics.buckets.some(b => b.latency && (b.latency.a_loss_pct > 0 || b.latency.z_loss_pct > 0))
            return hasLoss ? <LinkPacketLossDetailChart data={fullMetrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} /> : null
          })()}
          {/* Interface issues chart — uses bulk data (has traffic) */}
          {(() => {
            const hasIssues = linkMetrics.buckets.some(b => b.traffic && (
              b.traffic.side_a_in_errors + b.traffic.side_a_out_errors + b.traffic.side_z_in_errors + b.traffic.side_z_out_errors > 0 ||
              b.traffic.side_a_in_fcs_errors + b.traffic.side_z_in_fcs_errors > 0 ||
              b.traffic.side_a_in_discards + b.traffic.side_a_out_discards + b.traffic.side_z_in_discards + b.traffic.side_z_out_discards > 0 ||
              b.traffic.side_a_carrier_transitions + b.traffic.side_z_carrier_transitions > 0
            ))
            return hasIssues ? <LinkInterfaceIssuesChart data={linkMetrics} loading={false} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} /> : null
          })()}
          {!fullMetrics && metricsFetching && (
            <div className="flex justify-center py-4">
              <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
            </div>
          )}
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

  const { data, isLoading, isPlaceholderData, error } = useQuery({
    queryKey: ['bulk-link-metrics', timeRange],
    queryFn: () => fetchBulkLinkMetrics({ range: timeRange, include: ['status', 'traffic'], hasIssues: true }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    placeholderData: keepPreviousData,
  })

  // Convert the Record<string, LinkMetricsResponse> into an array with derived info
  const linksArray = useMemo(() => {
    if (!data?.links) return []
    return Object.values(data.links).map(metrics => ({
      metrics,
      derived: deriveLinkInfo(metrics),
    }))
  }, [data?.links])

  // Helper to check if a link matches health filters
  const linkMatchesHealthFilters = (derived: DerivedLinkInfo): boolean => {
    if (linksWithHealth && linksWithHealth.size > 0) {
      const health = linksWithHealth.get(derived.code)
      if (health) {
        const filterHealth = (health === 'no_data' || health === 'down') ? 'unhealthy' : health
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return healthFilters.includes(filterHealth as any)
      }
      return false
    }

    // Fallback: use worst health derived from buckets
    const h = derived.health
    if (h === 'healthy' && healthFilters.includes('healthy')) return true
    if (h === 'degraded' && healthFilters.includes('degraded')) return true
    if ((h === 'unhealthy' || h === 'no_data' || h === 'down') && healthFilters.includes('unhealthy')) return true
    return false
  }

  const issueTypesSelected = issueFilters.filter(f => f !== 'no_issues')
  const noIssuesSelected = issueFilters.includes('no_issues')
  const noDataSelected = issueFilters.includes('no_data')

  const filteredLinks = useMemo(() => {
    if (linksArray.length === 0) return []

    const filtered = linksArray.filter(({ metrics, derived }) => {
      const issueReasons = linksWithIssues && linksWithIssues.size > 0
        ? (linksWithIssues.get(derived.code) ?? [])
        : derived.issueReasons
      const hasIssues = issueReasons.length > 0

      // When no_data filter is off, exclude links whose only issue is no_data
      if (!noDataSelected && issueReasons.length === 1 && issueReasons[0] === 'no_data') {
        const hasNonHealthyBuckets = metrics.buckets.some(b =>
          b.status && !b.status.collecting && (b.status.health === 'unhealthy' || b.status.health === 'degraded')
        )
        if (!hasNonHealthyBuckets) {
          return false
        }
      }

      if (derived.drainStatus && !showDrained) {
        return false
      }

      if (derived.provisioning && !showProvisioning) {
        return false
      }

      // Drained/provisioning links pass when their toggle is on, regardless of issue/health filters
      if ((derived.drainStatus && showDrained) || (derived.provisioning && showProvisioning)) {
        return true
      }

      const matchesIssue = hasIssues
        ? issueReasons.some(reason => issueTypesSelected.includes(reason)) ||
          (issueReasons.length === 1 && issueReasons[0] === 'no_data' && metrics.buckets.some(b =>
            b.status && !b.status.collecting && (b.status.health === 'unhealthy' || b.status.health === 'degraded')
          ))
        : noIssuesSelected

      const matchesHealth = linkMatchesHealthFilters(derived)

      return matchesIssue && matchesHealth
    })

    // Sort by: 1) recent severity (worst in last 6 buckets), 2) overall worst severity,
    // 3) most recent issue timestamp, 4) total issue count, 5) alphabetical.
    const statusSeverity = (health: string, isisDown: boolean): number => {
      if (isisDown) return 3
      switch (health) {
        case 'down':
        case 'unhealthy': return 3
        case 'degraded': return 2
        case 'no_data': return 1
        default: return 0
      }
    }

    const RECENT_BUCKETS = 6

    return filtered.sort((a, b) => {
      const getSortKey = (item: { metrics: LinkMetricsResponse }): { recent: number; worst: number; latestTs: string; count: number } => {
        const buckets = item.metrics.buckets
        if (!buckets || buckets.length === 0) return { recent: 0, worst: 0, latestTs: '', count: 0 }
        let worst = 0
        let recent = 0
        let latestTs = ''
        let count = 0
        const recentStart = Math.max(0, buckets.length - RECENT_BUCKETS)
        for (let i = 0; i < buckets.length; i++) {
          const bk = buckets[i]
          const health = bk.status?.health ?? 'no_data'
          const isisDown = bk.status?.isis_down ?? false
          const sev = statusSeverity(health, isisDown)
          if (sev > 0) {
            count++
            if (sev > worst) worst = sev
            if (i >= recentStart && sev > recent) recent = sev
            if (bk.ts > latestTs) latestTs = bk.ts
          }
        }
        return { recent, worst, latestTs, count }
      }

      const aInfo = getSortKey(a)
      const bInfo = getSortKey(b)

      if (aInfo.recent !== bInfo.recent) return bInfo.recent - aInfo.recent
      if (aInfo.worst !== bInfo.worst) return bInfo.worst - aInfo.worst
      if (aInfo.latestTs !== bInfo.latestTs) return aInfo.latestTs < bInfo.latestTs ? 1 : -1
      if (aInfo.count !== bInfo.count) return bInfo.count - aInfo.count
      return a.derived.code.localeCompare(b.derived.code)
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [linksArray, issueFilters, healthFilters, noIssuesSelected, issueTypesSelected, showDrained, showProvisioning, linksWithIssues, linksWithHealth])

  const drainedCount = useMemo(() => {
    return linksArray.filter(({ derived }) => derived.drainStatus).length
  }, [linksArray])

  const provisioningCount = useMemo(() => {
    return linksArray.filter(({ derived }) => derived.provisioning).length
  }, [linksArray])

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
          {linksArray.length === 0
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
        {filteredLinks.map(({ metrics, derived }) => (
          <LinkRow
            key={derived.code}
            linkMetrics={metrics}
            derivedInfo={derived}
            linksWithIssues={linksWithIssues}
            criticalityMap={criticalityMap}
            metricsTimeRange={timeRange}
          />
        ))}
      </div>
    </div>
  )
}
