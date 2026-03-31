import { useState, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, RefreshCw, Cable, AlertCircle, ArrowLeft } from 'lucide-react'
import { CopyableText } from '@/components/copyable-text'
import { fetchLink, fetchLinkMetrics, type FetchLinkMetricsParams } from '@/lib/api'
import { LinkInfoContent } from '@/components/shared/LinkInfoContent'
import { linkDetailToInfo } from '@/components/shared/link-info-converters'
import { LinkHealthTimeline } from '@/components/link-charts/LinkHealthTimeline'
import { LinkPacketLossChart } from '@/components/link-charts/LinkPacketLossChart'
import { LinkInterfaceIssuesChart } from '@/components/link-charts/LinkInterfaceIssuesChart'
import { LinkLatencyChart } from '@/components/link-charts/LinkLatencyChart'
import { LinkJitterChart } from '@/components/link-charts/LinkJitterChart'
import { LinkTrafficChart } from '@/components/link-charts/LinkTrafficChart'
import { TimeRangeSelector, TrafficFilters } from '@/components/topology/TimeRangeSelector'
import type { TimeRange, BucketSize } from '@/components/topology/utils'
import { bucketLabels, resolveAutoBucket, type TimeRangePreset } from '@/components/topology/utils'
import { useDocumentTitle } from '@/hooks/use-document-title'

/** Convert a custom time string (yyyy-mm-dd-hh:mm:ss) to unix seconds. */
function parseCustomTime(s: string): number | undefined {
  // Format: 2026-03-28-14:30:00
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})-(\d{2}):(\d{2}):(\d{2})$/)
  if (!m) return undefined
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]))
  return Math.floor(d.getTime() / 1000)
}

/** Convert TimeRange + BucketSize to FetchLinkMetricsParams. */
function toMetricsParams(timeRange: TimeRange, bucket: BucketSize): FetchLinkMetricsParams {
  const params: FetchLinkMetricsParams = {}
  if (timeRange.preset === 'custom' && timeRange.from && timeRange.to) {
    params.startTime = parseCustomTime(timeRange.from)
    params.endTime = parseCustomTime(timeRange.to)
  } else if (timeRange.preset !== 'custom') {
    params.range = timeRange.preset
  }
  if (bucket && bucket !== 'auto') {
    // Convert SQL interval format to short form: "5 MINUTE" → "5m"
    const m = bucket.match(/^(\d+)\s+(SECOND|MINUTE|HOUR|DAY)$/)
    if (m) {
      const unit = { SECOND: 's', MINUTE: 'm', HOUR: 'h', DAY: 'd' }[m[2]] || ''
      params.bucket = `${m[1]}${unit}`
    }
  }
  return params
}

export function LinkDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<TimeRange>({ preset: '24h' })
  const [bucket, setBucket] = useState<BucketSize>('auto')

  const effectiveBucketLabel = bucket === 'auto'
    ? bucketLabels[resolveAutoBucket(timeRange.preset as TimeRangePreset)]
    : undefined

  const { data: link, isLoading, error } = useQuery({
    queryKey: ['link', pk],
    queryFn: () => fetchLink(pk!),
    enabled: !!pk,
  })

  const metricsParams = useMemo(() => toMetricsParams(timeRange, bucket), [timeRange, bucket])

  const { data: metrics, isLoading: metricsLoading, isFetching: metricsFetching } = useQuery({
    queryKey: ['linkMetrics', pk, metricsParams],
    queryFn: () => fetchLinkMetrics(pk!, metricsParams),
    enabled: !!pk,
  })

  useDocumentTitle(link?.code || 'Link')

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !link) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Link not found</div>
          <Link
            to="/dz/links"
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to links
          </Link>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      {/* Header section - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pt-8">
        {/* Back button */}
        <Link
          to="/dz/links"
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to links
        </Link>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <Cable className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium font-mono">
              <CopyableText text={link.code} />
            </h1>
            <div className="text-sm text-muted-foreground font-mono">
              <CopyableText text={link.pk} />
            </div>
          </div>
        </div>
      </div>

      {/* Link stats - constrained width, hide status row and charts */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-6">
        <LinkInfoContent link={linkDetailToInfo(link)} hideStatusRow hideCharts />
      </div>

      {/* Filters + charts */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-8 space-y-6">
        <div className="flex justify-end gap-2 items-center">
          <button
            onClick={() => queryClient.invalidateQueries({ queryKey: ['linkMetrics'] })}
            disabled={metricsFetching}
            className="text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
            title="Refresh"
          >
            {metricsFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          </button>
          <TrafficFilters
            bucket={bucket}
            onBucketChange={setBucket}
            effectiveBucketLabel={effectiveBucketLabel}
          />
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
        </div>

        {metricsLoading && (
          <div className="space-y-4">
            <div className="animate-pulse bg-muted rounded h-6 w-full" />
            {[1, 2, 3, 4, 5].map((i) => (
              <div key={i} className="rounded-lg border border-border p-4 space-y-2">
                <div className="animate-pulse bg-muted rounded h-4 w-32" />
                <div className="animate-pulse bg-muted rounded h-36 w-full" />
              </div>
            ))}
          </div>
        )}
        {metrics && (
          <div className="space-y-4">
            <LinkHealthTimeline data={metrics} />
            <LinkPacketLossChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" />
            <LinkInterfaceIssuesChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" />
            <LinkLatencyChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" />
            <LinkJitterChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" />
            <LinkTrafficChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" />
          </div>
        )}
      </div>
    </div>
  )
}
