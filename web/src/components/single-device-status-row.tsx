import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { Loader2 } from 'lucide-react'
import { fetchSingleDeviceHistory, type DeviceHourStatus } from '@/lib/api'
import { StatusTimeline } from './status-timeline'

interface SingleDeviceStatusRowProps {
  devicePk: string
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

// Adapter to convert DeviceHourStatus to LinkHourStatus format expected by StatusTimeline
function deviceHourToLinkHour(hour: DeviceHourStatus) {
  return {
    hour: hour.hour,
    status: hour.status,
    avg_loss_pct: 0, // Devices don't have packet loss
    avg_latency_us: 0, // Devices don't have latency
    samples: 0,
    side_a_in_errors: hour.in_errors,
    side_a_out_errors: hour.out_errors,
    side_z_in_errors: 0,
    side_z_out_errors: 0,
    side_a_in_discards: hour.in_discards,
    side_a_out_discards: hour.out_discards,
    side_z_in_discards: 0,
    side_z_out_discards: 0,
    side_a_carrier_transitions: hour.carrier_transitions,
    side_z_carrier_transitions: 0,
  }
}

export function SingleDeviceStatusRow({ devicePk, timeRange = '24h' }: SingleDeviceStatusRowProps) {
  const buckets = useBucketCount()

  const { data, isLoading, error } = useQuery({
    queryKey: ['single-device-history', devicePk, timeRange, buckets],
    queryFn: () => fetchSingleDeviceHistory(devicePk, timeRange, buckets),
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
  const hasAnyErrors = data.hours.some(h => h.in_errors > 0 || h.out_errors > 0)
  const hasAnyDiscards = data.hours.some(h => h.in_discards > 0 || h.out_discards > 0)
  const hasAnyCarrier = data.hours.some(h => h.carrier_transitions > 0)
  const hasAnyDrained = data.issue_reasons?.includes('drained') ?? false

  if (hasAnyErrors) issueReasons.push('interface_errors')
  if (hasAnyDiscards) issueReasons.push('discards')
  if (hasAnyCarrier) issueReasons.push('carrier_transitions')
  if (hasAnyDrained) issueReasons.push('drained')

  // Convert device hours to link hour format for StatusTimeline
  const linkHours = data.hours.map(deviceHourToLinkHour)

  return (
    <div className="space-y-3">
      {/* Issue badges */}
      {issueReasons.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {issueReasons.includes('interface_errors') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(239, 68, 68, 0.15)', color: '#dc2626' }}>Errors</span>
          )}
          {issueReasons.includes('discards') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(20, 184, 166, 0.15)', color: '#0d9488' }}>Discards</span>
          )}
          {issueReasons.includes('carrier_transitions') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(234, 179, 8, 0.15)', color: '#ca8a04' }}>Carrier Transitions</span>
          )}
          {issueReasons.includes('drained') && (
            <span className="text-xs px-2 py-1 rounded font-medium" style={{ backgroundColor: 'rgba(156, 163, 175, 0.15)', color: '#6b7280' }}>Drained</span>
          )}
        </div>
      )}

      {/* Status Timeline */}
      <StatusTimeline
        hours={linkHours}
        bucketMinutes={data.bucket_minutes ?? 60}
        timeRange={timeRange}
      />
    </div>
  )
}
