import { useState, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, RefreshCw, Cable, AlertCircle, ArrowLeft } from 'lucide-react'
import { CopyableText } from '@/components/copyable-text'
import { fetchLink, fetchLinkMetrics } from '@/lib/api'
import { LinkInfoContent } from '@/components/shared/LinkInfoContent'
import { linkDetailToInfo } from '@/components/shared/link-info-converters'
import { toLinkMetricsParams } from '@/components/shared/metrics-params'
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

export function LinkDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const queryClient = useQueryClient()
  const [timeRange, setTimeRange] = useState<TimeRange>({ preset: '24h' })
  const [bucket, setBucket] = useState<BucketSize>('auto')
  const [hoveredTimeRange, setHoveredTimeRange] = useState<{ start: number; end: number } | null>(null)
  const [chartHoveredTime, setChartHoveredTime] = useState<number | null>(null)

  const effectiveBucketLabel = bucket === 'auto'
    ? bucketLabels[resolveAutoBucket(timeRange.preset as TimeRangePreset)]
    : undefined

  const { data: link, isLoading, error } = useQuery({
    queryKey: ['link', pk],
    queryFn: () => fetchLink(pk!),
    enabled: !!pk,
  })

  const metricsParams = useMemo(() => toLinkMetricsParams(timeRange, bucket), [timeRange, bucket])

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
            <LinkHealthTimeline data={metrics} onBarHover={setHoveredTimeRange} highlightedTime={chartHoveredTime} />
            <LinkPacketLossChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkInterfaceIssuesChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkTrafficChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkLatencyChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
            <LinkJitterChart data={metrics} loading={metricsFetching} className="rounded-lg border border-border p-4" highlightTimeRange={hoveredTimeRange} onCursorTime={setChartHoveredTime} />
          </div>
        )}
      </div>
    </div>
  )
}
