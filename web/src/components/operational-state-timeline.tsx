import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { fetchLinkStateTimeline, fetchDeviceStateTimeline } from '@/lib/api'
import type { StateSegment } from '@/lib/api'

interface OperationalStateTimelineProps {
  entityType: 'link' | 'device'
  entityPk: string
  timeRange?: string
}

const stateColors: Record<string, string> = {
  activated: 'bg-green-500',
  'soft-drained': 'bg-amber-500',
  'hard-drained': 'bg-gray-500 dark:bg-gray-600',
  provisioning: 'bg-blue-500',
  requested: 'bg-blue-400',
  pending: 'bg-blue-400',
  maintenance: 'bg-amber-600',
  offline: 'bg-red-500',
  deleted: 'bg-red-700',
  suspended: 'bg-red-600',
}

const stateLabels: Record<string, string> = {
  activated: 'Activated',
  'soft-drained': 'Soft Drained',
  'hard-drained': 'Hard Drained',
  provisioning: 'Provisioning',
  requested: 'Requested',
  pending: 'Pending',
  maintenance: 'Maintenance',
  offline: 'Offline',
  deleted: 'Deleted',
  suspended: 'Suspended',
}

interface Bucket {
  start: Date
  end: Date
  status: string
}

function segmentsToBuckets(segments: StateSegment[], rangeStart: string, rangeEnd: string, bucketCount: number): Bucket[] {
  const startMs = new Date(rangeStart).getTime()
  const endMs = new Date(rangeEnd).getTime()
  const bucketDuration = (endMs - startMs) / bucketCount

  const buckets: Bucket[] = []
  for (let i = 0; i < bucketCount; i++) {
    const bucketStart = startMs + i * bucketDuration
    const bucketMid = bucketStart + bucketDuration / 2

    // Find the segment that covers the midpoint of this bucket
    let status = segments[0]?.status || ''
    for (const seg of segments) {
      const segStart = new Date(seg.start).getTime()
      const segEnd = new Date(seg.end).getTime()
      if (bucketMid >= segStart && bucketMid < segEnd) {
        status = seg.status
        break
      }
    }

    buckets.push({
      start: new Date(bucketStart),
      end: new Date(bucketStart + bucketDuration),
      status,
    })
  }
  return buckets
}

function formatDate(date: Date): string {
  return date.toLocaleDateString([], { month: 'short', day: 'numeric' })
}

function formatTimeRange(start: Date, end: Date): string {
  const startTime = start.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  const endTime = end.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  if (start.getDate() !== end.getDate()) {
    return `${formatDate(start)} ${startTime} — ${formatDate(end)} ${endTime}`
  }
  return `${formatDate(start)} ${startTime} — ${endTime}`
}

function useBucketCount() {
  const [buckets, setBuckets] = useState(72)
  useEffect(() => {
    const update = () => {
      const w = window.innerWidth
      setBuckets(w < 640 ? 24 : w < 1024 ? 48 : 72)
    }
    update()
    window.addEventListener('resize', update)
    return () => window.removeEventListener('resize', update)
  }, [])
  return buckets
}

const timeLabelsMap: Record<string, string> = {
  '1h': '1h ago',
  '6h': '6h ago',
  '12h': '12h ago',
  '24h': '24h ago',
  '3d': '3d ago',
  '7d': '7d ago',
}

export function OperationalStateTimeline({ entityType, entityPk, timeRange = '24h' }: OperationalStateTimelineProps) {
  const bucketCount = useBucketCount()
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['state-timeline', entityType, entityPk, timeRange],
    queryFn: () =>
      entityType === 'link'
        ? fetchLinkStateTimeline(entityPk, timeRange)
        : fetchDeviceStateTimeline(entityPk, timeRange),
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  if (isLoading) {
    return (
      <div className="rounded-lg border border-border p-4">
        <div className="text-xs font-medium text-muted-foreground mb-3">Status ({timeRange})</div>
        <div className="flex items-center justify-center py-2">
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        </div>
      </div>
    )
  }

  if (error || !data || data.segments.length === 0) {
    return null
  }

  const buckets = segmentsToBuckets(data.segments, data.range_start, data.range_end, bucketCount)

  // Build legend from unique statuses in segment order
  const uniqueStatuses: string[] = []
  for (const seg of data.segments) {
    if (!uniqueStatuses.includes(seg.status)) {
      uniqueStatuses.push(seg.status)
    }
  }

  const timeLabel = timeLabelsMap[timeRange] || `${timeRange} ago`

  return (
    <div className="rounded-lg border border-border p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="text-xs font-medium text-muted-foreground">Status ({timeRange})</div>
        <div className="flex items-center gap-3">
          {uniqueStatuses.map(status => (
            <div key={status} className="flex items-center gap-1.5">
              <div className={`w-2.5 h-2.5 rounded-sm ${stateColors[status] || 'bg-gray-400'}`} />
              <span className="text-[10px] text-muted-foreground">{stateLabels[status] || status}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="relative">
        <div className="flex gap-[2px]">
          {buckets.map((bucket, i) => {
            const color = stateColors[bucket.status] || 'bg-gray-400'
            return (
              <div
                key={i}
                className="relative flex-1 min-w-0"
                onMouseEnter={() => setHoveredIndex(i)}
                onMouseLeave={() => setHoveredIndex(null)}
              >
                <div
                  className={`w-full h-6 rounded-sm ${color} cursor-pointer transition-opacity hover:opacity-80`}
                />
                {hoveredIndex === i && (
                  <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 z-50">
                    <div className="bg-popover border border-border rounded-lg shadow-lg p-3 whitespace-nowrap text-sm">
                      <div className="font-medium mb-1">
                        {formatTimeRange(bucket.start, bucket.end)}
                      </div>
                      <div className={`text-xs ${
                        bucket.status === 'activated' ? 'text-green-600 dark:text-green-400' :
                        bucket.status === 'soft-drained' ? 'text-amber-600 dark:text-amber-400' :
                        bucket.status === 'hard-drained' ? 'text-gray-600 dark:text-gray-300' :
                        bucket.status.includes('drained') ? 'text-amber-600 dark:text-amber-400' :
                        'text-muted-foreground'
                      }`}>
                        {stateLabels[bucket.status] || bucket.status}
                      </div>
                    </div>
                    <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-[1px]">
                      <div className="border-8 border-transparent border-t-border" />
                      <div className="absolute top-0 left-1/2 -translate-x-1/2 border-[7px] border-transparent border-t-popover" />
                    </div>
                  </div>
                )}
              </div>
            )
          })}
        </div>

        <div className="flex justify-between mt-1 text-[10px] text-muted-foreground">
          <span>{timeLabel}</span>
          <span>Now</span>
        </div>
      </div>
    </div>
  )
}
