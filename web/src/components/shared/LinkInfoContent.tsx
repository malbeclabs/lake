import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { fetchLinkMetrics } from '@/lib/api'
import { LinkHealthTimeline } from '@/components/link-charts/LinkHealthTimeline'
import { LinkPacketLossChart } from '@/components/link-charts/LinkPacketLossChart'
import { LinkInterfaceIssuesChart } from '@/components/link-charts/LinkInterfaceIssuesChart'
import { LinkTrafficChart } from '@/components/link-charts/LinkTrafficChart'
import { LinkLatencyChart } from '@/components/link-charts/LinkLatencyChart'
import { LinkJitterChart } from '@/components/link-charts/LinkJitterChart'
import { toLinkMetricsParams } from '@/components/shared/metrics-params'
import { TimeRangeSelector, TrafficFilters } from '@/components/topology/TimeRangeSelector'
import type { TimeRange, BucketSize, TimeRangePreset } from '@/components/topology/utils'
import { resolveAutoBucket, bucketLabels } from '@/components/topology/utils'

// Shared link info type that both topology and link page can use
export interface LinkInfoData {
  pk: string
  code: string
  status: string
  linkType: string
  bandwidthBps: number
  sideAPk: string
  sideACode: string
  sideAMetro: string
  sideAIfaceName: string
  sideAIP: string
  sideZPk: string
  sideZCode: string
  sideZMetro: string
  sideZIfaceName: string
  sideZIP: string
  contributorPk: string
  contributorCode: string
  inBps: number
  outBps: number
  utilizationIn: number
  utilizationOut: number
  latencyUs: number
  jitterUs: number
  latencyAtoZUs: number
  jitterAtoZUs: number
  latencyZtoAUs: number
  jitterZtoAUs: number
  lossPercent: number
  peakInBps?: number
  peakOutBps?: number
  committedRttNs?: number
  isisDelayOverrideNs?: number
}

interface LinkInfoContentProps {
  link: LinkInfoData
  /** Compact mode for sidebar panels */
  compact?: boolean
  /** Hide status row (to be rendered separately at page level) */
  hideStatusRow?: boolean
  /** Hide charts section (to be rendered separately at page level) */
  hideCharts?: boolean
  /** Controlled time range (when managed by parent) */
  timeRange?: TimeRange
  /** Callback when time range changes (when managed by parent) */
  onTimeRangeChange?: (range: TimeRange) => void
}

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function formatLatencyUs(us: number): string {
  if (us === 0) return '—'
  if (us >= 1000) return `${(us / 1000).toFixed(2)} ms`
  return `${us.toFixed(0)} µs`
}

function formatLatencyNs(ns: number): string {
  if (ns === 0) return '—'
  const us = ns / 1000
  if (us >= 1000) return `${(us / 1000).toFixed(2)} ms`
  return `${us.toFixed(0)} µs`
}

function formatPercent(value: number): string {
  if (value === 0) return '—'
  return `${value.toFixed(2)}%`
}

const statusColors: Record<string, string> = {
  activated: 'text-muted-foreground',
  provisioning: 'text-blue-600 dark:text-blue-400',
  maintenance: 'text-amber-600 dark:text-amber-400',
  offline: 'text-red-600 dark:text-red-400',
}

/**
 * Shared component for displaying link information.
 * Used by both the topology panel and the link detail page.
 */
export function LinkInfoContent({ link, compact = false, hideStatusRow = false, hideCharts = false, timeRange: controlledTimeRange, onTimeRangeChange }: LinkInfoContentProps) {
  const [internalTimeRange, setInternalTimeRange] = useState<TimeRange>({ preset: '24h' })
  const timeRange = controlledTimeRange ?? internalTimeRange
  const setTimeRange = onTimeRangeChange ?? setInternalTimeRange

  // Check if we have directional latency data
  const hasDirectionalData = link.latencyAtoZUs > 0 || link.latencyZtoAUs > 0

  const [hoveredTimeRange, setHoveredTimeRange] = useState<{ start: number; end: number } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  const [bucket, setBucket] = useState<BucketSize>('auto')

  const metricsParams = useMemo(() => toLinkMetricsParams(timeRange, bucket), [timeRange, bucket])

  const { data: metrics, isFetching: metricsFetching } = useQuery({
    queryKey: ['linkMetrics', link.pk, metricsParams],
    queryFn: () => fetchLinkMetrics(link.pk, metricsParams),
    enabled: !hideCharts,
  })

  const effectiveBucketLabel = bucket === 'auto'
    ? bucketLabels[resolveAutoBucket(timeRange.preset as TimeRangePreset)]
    : undefined

  const cardClass = "rounded-lg border border-border p-4"

  // Compact mode: optimized for sidebar panels
  if (compact) {
    return (
      <div className="space-y-4">
        {/* Endpoints with per-direction latency */}
        <div className="grid grid-cols-2 gap-3">
          <div className="p-2 bg-muted/30 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">A-Side</div>
            <div className="text-sm font-medium">
              <Link to={`/dz/devices/${link.sideAPk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
                {link.sideACode}
              </Link>
            </div>
            {link.sideAIfaceName && (
              <div className="text-xs text-muted-foreground font-mono mt-0.5">{link.sideAIfaceName}</div>
            )}
            {link.sideAIP && (
              <div className="text-xs text-muted-foreground font-mono">{link.sideAIP}</div>
            )}
            {hasDirectionalData && (
              <div className="mt-2 pt-2 border-t border-muted/50">
                <div className="text-xs text-muted-foreground">RTT from A</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.latencyAtoZUs)}</div>
                <div className="text-xs text-muted-foreground mt-1">Jitter from A</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.jitterAtoZUs)}</div>
              </div>
            )}
          </div>
          <div className="p-2 bg-muted/30 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">Z-Side</div>
            <div className="text-sm font-medium">
              <Link to={`/dz/devices/${link.sideZPk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
                {link.sideZCode}
              </Link>
            </div>
            {link.sideZIfaceName && (
              <div className="text-xs text-muted-foreground font-mono mt-0.5">{link.sideZIfaceName}</div>
            )}
            {link.sideZIP && (
              <div className="text-xs text-muted-foreground font-mono">{link.sideZIP}</div>
            )}
            {hasDirectionalData && (
              <div className="mt-2 pt-2 border-t border-muted/50">
                <div className="text-xs text-muted-foreground">RTT from Z</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.latencyZtoAUs)}</div>
                <div className="text-xs text-muted-foreground mt-1">Jitter from Z</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.jitterZtoAUs)}</div>
              </div>
            )}
          </div>
        </div>

        {/* Committed latency */}
        {link.committedRttNs !== undefined && (
          <div className="text-center p-2 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums tracking-tight">
              {link.isisDelayOverrideNs !== undefined && link.isisDelayOverrideNs > 0 ? (
                <div>
                  <div className="line-through text-muted-foreground text-sm">{formatLatencyNs(link.committedRttNs)}</div>
                  <div>{formatLatencyNs(link.isisDelayOverrideNs)}</div>
                </div>
              ) : (
                formatLatencyNs(link.committedRttNs)
              )}
            </div>
            <div className="text-xs text-muted-foreground">
              {link.isisDelayOverrideNs !== undefined && link.isisDelayOverrideNs > 0 ? 'Committed Latency (override)' : 'Committed Latency'}
            </div>
          </div>
        )}

        {/* Combined latency (average of both directions) - shown when no directional data */}
        {!hasDirectionalData && (
          <div className="grid grid-cols-2 gap-2">
            <div className="text-center p-2 bg-muted/30 rounded-lg">
              <div className="text-base font-medium tabular-nums tracking-tight">{formatLatencyUs(link.latencyUs)}</div>
              <div className="text-xs text-muted-foreground">Latency</div>
            </div>
            <div className="text-center p-2 bg-muted/30 rounded-lg">
              <div className="text-base font-medium tabular-nums tracking-tight">{formatLatencyUs(link.jitterUs)}</div>
              <div className="text-xs text-muted-foreground">Jitter</div>
            </div>
          </div>
        )}

        {/* Stats grid - 2 columns for sidebar */}
        <div className="grid grid-cols-2 gap-2">
          <div className="text-center p-2 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums tracking-tight">{formatPercent(link.lossPercent)}</div>
            <div className="text-xs text-muted-foreground">Packet Loss</div>
          </div>
          <div className="text-center p-2 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums tracking-tight">{formatBps(link.bandwidthBps)}</div>
            <div className="text-xs text-muted-foreground">Bandwidth</div>
          </div>
          <div className="text-center p-2 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums tracking-tight">{formatBps(link.inBps)}</div>
            <div className="text-xs text-muted-foreground">Current In</div>
          </div>
          <div className="text-center p-2 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums tracking-tight">{formatBps(link.outBps)}</div>
            <div className="text-xs text-muted-foreground">Current Out</div>
          </div>
          <div className="text-center p-2 bg-muted/30 rounded-lg col-span-2">
            <div className="text-base font-medium tabular-nums tracking-tight">
              {link.contributorPk ? (
                <Link to={`/dz/contributors/${link.contributorPk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
                  {link.contributorCode}
                </Link>
              ) : (
                link.contributorCode || '—'
              )}
            </div>
            <div className="text-xs text-muted-foreground">Contributor</div>
          </div>
        </div>

        {/* Time range and bucket selectors */}
        <div className="flex items-center justify-end gap-2">
          <TrafficFilters
            bucket={bucket}
            onBucketChange={setBucket}
            effectiveBucketLabel={effectiveBucketLabel}
          />
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
        </div>

        {/* Charts from unified metrics endpoint */}
        {metrics && (
          <div className="space-y-4">
            <LinkHealthTimeline data={metrics} onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />
            <LinkPacketLossChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkInterfaceIssuesChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkTrafficChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkLatencyChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkJitterChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
          </div>
        )}
      </div>
    )
  }

  // Wide mode: optimized for full-page view on desktop
  return (
    <div className="space-y-6">
      {/* Endpoints row */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="p-3 bg-muted/30 rounded-lg">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">A-Side</div>
          <div className="text-sm font-medium">
            <Link to={`/dz/devices/${link.sideAPk}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
              {link.sideACode}
            </Link>
            {link.sideAMetro && <span className="text-muted-foreground ml-1">({link.sideAMetro})</span>}
          </div>
          {link.sideAIfaceName && (
            <div className="text-xs text-muted-foreground font-mono mt-1">{link.sideAIfaceName}</div>
          )}
          {link.sideAIP && (
            <div className="text-xs text-muted-foreground font-mono">{link.sideAIP}</div>
          )}
          {hasDirectionalData && (
            <div className="mt-3 pt-3 border-t border-muted/50 grid grid-cols-2 gap-2">
              <div>
                <div className="text-xs text-muted-foreground">RTT</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.latencyAtoZUs)}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Jitter</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.jitterAtoZUs)}</div>
              </div>
            </div>
          )}
        </div>
        <div className="p-3 bg-muted/30 rounded-lg">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">Z-Side</div>
          <div className="text-sm font-medium">
            <Link to={`/dz/devices/${link.sideZPk}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
              {link.sideZCode}
            </Link>
            {link.sideZMetro && <span className="text-muted-foreground ml-1">({link.sideZMetro})</span>}
          </div>
          {link.sideZIfaceName && (
            <div className="text-xs text-muted-foreground font-mono mt-1">{link.sideZIfaceName}</div>
          )}
          {link.sideZIP && (
            <div className="text-xs text-muted-foreground font-mono">{link.sideZIP}</div>
          )}
          {hasDirectionalData && (
            <div className="mt-3 pt-3 border-t border-muted/50 grid grid-cols-2 gap-2">
              <div>
                <div className="text-xs text-muted-foreground">RTT</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.latencyZtoAUs)}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Jitter</div>
                <div className="text-sm font-medium tabular-nums">{formatLatencyUs(link.jitterZtoAUs)}</div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Stats grid - responsive columns */}
      <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-5 gap-2">
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className={`text-base font-medium capitalize ${statusColors[link.status] || ''}`}>{link.status}</div>
          <div className="text-xs text-muted-foreground">Status</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium">{link.linkType}</div>
          <div className="text-xs text-muted-foreground">Type</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium tabular-nums">{formatBps(link.bandwidthBps)}</div>
          <div className="text-xs text-muted-foreground">Bandwidth</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium tabular-nums">{link.utilizationIn.toFixed(1)}%</div>
          <div className="text-xs text-muted-foreground">Util In</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium tabular-nums">{link.utilizationOut.toFixed(1)}%</div>
          <div className="text-xs text-muted-foreground">Util Out</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium tabular-nums">{formatBps(link.inBps)}</div>
          <div className="text-xs text-muted-foreground">Current In</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium tabular-nums">{formatBps(link.outBps)}</div>
          <div className="text-xs text-muted-foreground">Current Out</div>
        </div>
        {link.peakInBps !== undefined && (
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums">{formatBps(link.peakInBps)}</div>
            <div className="text-xs text-muted-foreground">Peak In (1h)</div>
          </div>
        )}
        {link.peakOutBps !== undefined && (
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums">{formatBps(link.peakOutBps)}</div>
            <div className="text-xs text-muted-foreground">Peak Out (1h)</div>
          </div>
        )}
        {link.committedRttNs !== undefined && (
          <div className="text-center p-3 bg-muted/30 rounded-lg">
            <div className="text-base font-medium tabular-nums">
              {link.isisDelayOverrideNs !== undefined && link.isisDelayOverrideNs > 0 ? (
                <div>
                  <div className="line-through text-muted-foreground">{formatLatencyNs(link.committedRttNs)}</div>
                  <div>{formatLatencyNs(link.isisDelayOverrideNs)}</div>
                </div>
              ) : (
                formatLatencyNs(link.committedRttNs)
              )}
            </div>
            <div className="text-xs text-muted-foreground">
              {link.isisDelayOverrideNs !== undefined && link.isisDelayOverrideNs > 0 ? 'Committed Latency (override)' : 'Committed Latency'}
            </div>
          </div>
        )}
        {!hasDirectionalData && (
          <>
            <div className="text-center p-3 bg-muted/30 rounded-lg">
              <div className="text-base font-medium tabular-nums">{formatLatencyUs(link.latencyUs)}</div>
              <div className="text-xs text-muted-foreground">Latency</div>
            </div>
            <div className="text-center p-3 bg-muted/30 rounded-lg">
              <div className="text-base font-medium tabular-nums">{formatLatencyUs(link.jitterUs)}</div>
              <div className="text-xs text-muted-foreground">Jitter</div>
            </div>
          </>
        )}
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium tabular-nums">{formatPercent(link.lossPercent)}</div>
          <div className="text-xs text-muted-foreground">Packet Loss</div>
        </div>
        <div className="text-center p-3 bg-muted/30 rounded-lg">
          <div className="text-base font-medium">
            {link.contributorPk ? (
              <Link to={`/dz/contributors/${link.contributorPk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
                {link.contributorCode}
              </Link>
            ) : (
              link.contributorCode || '—'
            )}
          </div>
          <div className="text-xs text-muted-foreground">Contributor</div>
        </div>
      </div>

      {/* Charts section */}
      {!hideCharts && (
        <>
          {/* Time range and bucket selectors (only shown when not controlled by parent) */}
          {!controlledTimeRange && (
            <div className="flex items-center justify-end gap-2">
              <TrafficFilters
                bucket={bucket}
                onBucketChange={setBucket}
                effectiveBucketLabel={effectiveBucketLabel}
              />
              <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
            </div>
          )}

          {metrics && (
            <div className="space-y-4">
              {!hideStatusRow && <LinkHealthTimeline data={metrics} onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />}
              <LinkPacketLossChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
              <LinkInterfaceIssuesChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
              <LinkTrafficChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
              <LinkLatencyChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
              <LinkJitterChart data={metrics} loading={metricsFetching} className={cardClass} highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            </div>
          )}
        </>
      )}
    </div>
  )
}
