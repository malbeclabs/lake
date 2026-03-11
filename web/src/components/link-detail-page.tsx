import { useState, useMemo } from 'react'
import { useParams, useNavigate, useLocation } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Cable, AlertCircle, ArrowLeft } from 'lucide-react'
import { fetchLink } from '@/lib/api'
import { LinkInfoContent } from '@/components/shared/LinkInfoContent'
import { linkDetailToInfo } from '@/components/shared/link-info-converters'
import { SingleLinkStatusRow } from '@/components/single-link-status-row'
import { InterfaceCharts } from '@/components/topology/InterfaceCharts'
import { LatencyCharts } from '@/components/topology/LatencyCharts'
import { LinkStatusCharts } from '@/components/topology/LinkStatusCharts'
import { TimeRangeSelector, TrafficFilters } from '@/components/topology/TimeRangeSelector'
import type { TimeRange, BucketSize } from '@/components/topology/utils'
import { bucketLabels, resolveAutoBucket, timeRangeToString, type TimeRangePreset } from '@/components/topology/utils'
import { useDocumentTitle } from '@/hooks/use-document-title'

export function LinkDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const location = useLocation()
  const backLabel = (location.state as { backLabel?: string } | null)?.backLabel ?? 'links'
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

  useDocumentTitle(link?.code || 'Link')

  const interfaceLabels = useMemo(() => {
    if (!link) return undefined
    const map = new Map<string, string>()
    if (link.side_a_iface_name) {
      map.set(`A:${link.side_a_iface_name}`, `A: ${link.side_a_code} · ${link.side_a_iface_name}`)
    }
    if (link.side_z_iface_name) {
      map.set(`Z:${link.side_z_iface_name}`, `Z: ${link.side_z_code} · ${link.side_z_iface_name}`)
    }
    return map
  }, [link])

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
          <button
            onClick={() => navigate('/dz/links')}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to links
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      {/* Header section - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pt-8">
        {/* Back button */}
        <button
          onClick={() => window.history.length > 1 ? navigate(-1) : navigate('/dz/links')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to {backLabel}
        </button>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <Cable className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium font-mono">{link.code}</h1>
            <div className="text-sm text-muted-foreground">{link.link_type}</div>
          </div>
        </div>
      </div>

      {/* Link stats - constrained width, hide status row and charts */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-6">
        <LinkInfoContent link={linkDetailToInfo(link)} hideStatusRow hideCharts />
      </div>

      {/* Filters + status row + charts */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-8 space-y-6">
        <div className="flex justify-end gap-2">
          <TrafficFilters
            bucket={bucket}
            onBucketChange={setBucket}
            effectiveBucketLabel={effectiveBucketLabel}
          />
          <TimeRangeSelector value={timeRange} onChange={setTimeRange} />
        </div>

        {/* Status row */}
        <SingleLinkStatusRow linkPk={link.pk} timeRange={timeRangeToString(timeRange)} />

        {/* Link status charts (packet loss, interface issues) */}
        <LinkStatusCharts linkPk={link.pk} timeRange={timeRangeToString(timeRange)} bucket={bucket} className="rounded-lg border border-border p-4" />

        {/* Interface traffic charts */}
        <InterfaceCharts
          entityType="link"
          entityPk={link.pk}
          timeRange={timeRange}
          interfaceLabels={interfaceLabels}
          bucket={bucket}
          onBucketChange={setBucket}
          className="rounded-lg border border-border p-4"
        />

        {/* Latency charts */}
        <LatencyCharts linkPk={link.pk} timeRange={timeRange} bucket={bucket} className="rounded-lg border border-border p-4" />
      </div>
    </div>
  )
}
