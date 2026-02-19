import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Cable, AlertCircle, ArrowLeft } from 'lucide-react'
import { fetchLink } from '@/lib/api'
import { LinkInfoContent } from '@/components/shared/LinkInfoContent'
import { linkDetailToInfo } from '@/components/shared/link-info-converters'
import { SingleLinkStatusRow } from '@/components/single-link-status-row'
import { TrafficCharts } from '@/components/topology/TrafficCharts'
import { LatencyCharts } from '@/components/topology/LatencyCharts'
import { LinkStatusCharts } from '@/components/topology/LinkStatusCharts'
import { useDocumentTitle } from '@/hooks/use-document-title'

export function LinkDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()

  const { data: link, isLoading, error } = useQuery({
    queryKey: ['link', pk],
    queryFn: () => fetchLink(pk!),
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
          onClick={() => navigate('/dz/links')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to links
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

      {/* Status row */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-6">
        <SingleLinkStatusRow linkPk={link.pk} />
      </div>

      {/* Charts - constrained width */}
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 pb-8 space-y-6">
        {/* Charts row - side by side on large screens */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div>
            <TrafficCharts entityType="link" entityPk={link.pk} />
          </div>
          <div>
            <LatencyCharts linkPk={link.pk} />
          </div>
        </div>

        {/* Link status charts (packet loss, interface issues) */}
        <LinkStatusCharts linkPk={link.pk} />
      </div>
    </div>
  )
}
