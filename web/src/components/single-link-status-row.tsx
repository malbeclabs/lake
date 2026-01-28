import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { fetchSingleLinkHistory } from '@/lib/api'
import { StatusTimeline } from './status-timeline'

interface SingleLinkStatusRowProps {
  linkPk: string
  timeRange?: '3h' | '6h' | '12h' | '24h' | '3d' | '7d'
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

export function SingleLinkStatusRow({ linkPk, timeRange = '24h' }: SingleLinkStatusRowProps) {
  const buckets = useBucketCount()

  const { data, isLoading, error } = useQuery({
    queryKey: ['single-link-history', linkPk, timeRange, buckets],
    queryFn: () => fetchSingleLinkHistory(linkPk, timeRange, buckets),
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground mr-2" />
        <span className="text-sm text-muted-foreground">Loading status history...</span>
      </div>
    )
  }

  if (error || !data?.hours || data.hours.length === 0) {
    return null
  }

  // Extract issue reasons from hours data
  const issueReasons: string[] = []
  const hasAnyPacketLoss = data.hours.some(h => h.avg_loss_pct > 0)
  const hasAnyHighLatency = data.committed_rtt_us > 0 && data.hours.some(h => {
    // Only check latency if we have valid data
    if (h.avg_latency_us <= 0) return false
    // Calculate percentage over SLA (must be positive and >= 20%)
    const latencyOveragePct = ((h.avg_latency_us - data.committed_rtt_us) / data.committed_rtt_us) * 100
    return latencyOveragePct >= 20 // LATENCY_WARNING_PCT from backend
  })
  const hasAnyErrors = data.hours.some(h => (h.side_a_in_errors ?? 0) > 0 || (h.side_a_out_errors ?? 0) > 0 || (h.side_z_in_errors ?? 0) > 0 || (h.side_z_out_errors ?? 0) > 0)
  const hasAnyDiscards = data.hours.some(h => (h.side_a_in_discards ?? 0) > 0 || (h.side_a_out_discards ?? 0) > 0 || (h.side_z_in_discards ?? 0) > 0 || (h.side_z_out_discards ?? 0) > 0)
  const hasAnyCarrier = data.hours.some(h => (h.side_a_carrier_transitions ?? 0) > 0 || (h.side_z_carrier_transitions ?? 0) > 0)

  if (hasAnyPacketLoss) issueReasons.push('packet_loss')
  if (hasAnyHighLatency) issueReasons.push('high_latency')
  if (hasAnyErrors) issueReasons.push('interface_errors')
  if (hasAnyDiscards) issueReasons.push('discards')
  if (hasAnyCarrier) issueReasons.push('carrier_transitions')

  return (
    <div className="space-y-3">
      {/* Issue badges */}
      {issueReasons.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {issueReasons.includes('packet_loss') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(168, 85, 247, 0.15)', color: '#9333ea' }}>Loss</span>
          )}
          {issueReasons.includes('high_latency') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(59, 130, 246, 0.15)', color: '#2563eb' }}>High Latency</span>
          )}
          {issueReasons.includes('interface_errors') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(239, 68, 68, 0.15)', color: '#dc2626' }}>Errors</span>
          )}
          {issueReasons.includes('discards') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(20, 184, 166, 0.15)', color: '#0d9488' }}>Discards</span>
          )}
          {issueReasons.includes('carrier_transitions') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(234, 179, 8, 0.15)', color: '#ca8a04' }}>Carrier Transitions</span>
          )}
        </div>
      )}

      {/* Status Timeline */}
      <StatusTimeline
        hours={data.hours}
        committedRttUs={data.committed_rtt_us ?? 0}
        bucketMinutes={data.bucket_minutes ?? 60}
        timeRange={timeRange}
      />
    </div>
  )
}
