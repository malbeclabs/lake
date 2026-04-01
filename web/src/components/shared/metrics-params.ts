import type { FetchLinkMetricsParams, FetchDeviceMetricsParams } from '@/lib/api'
import type { TimeRange, BucketSize } from '@/components/topology/utils'

/** Convert a custom time string (yyyy-mm-dd-hh:mm:ss) to unix seconds. */
function parseCustomTime(s: string): number | undefined {
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})-(\d{2}):(\d{2}):(\d{2})$/)
  if (!m) return undefined
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]))
  return Math.floor(d.getTime() / 1000)
}

/** Convert SQL interval bucket to short form: "5 MINUTE" → "5m" */
function bucketToShortForm(bucket: BucketSize): string | undefined {
  if (!bucket || bucket === 'auto') return undefined
  const m = bucket.match(/^(\d+)\s+(SECOND|MINUTE|HOUR|DAY)$/)
  if (!m) return undefined
  const unit = { SECOND: 's', MINUTE: 'm', HOUR: 'h', DAY: 'd' }[m[2]] || ''
  return `${m[1]}${unit}`
}

/** Convert TimeRange + BucketSize to FetchLinkMetricsParams. */
export function toLinkMetricsParams(timeRange: TimeRange, bucket: BucketSize): FetchLinkMetricsParams {
  const params: FetchLinkMetricsParams = {}
  if (timeRange.preset === 'custom' && timeRange.from && timeRange.to) {
    params.startTime = parseCustomTime(timeRange.from)
    params.endTime = parseCustomTime(timeRange.to)
  } else if (timeRange.preset !== 'custom') {
    params.range = timeRange.preset
  }
  const b = bucketToShortForm(bucket)
  if (b) params.bucket = b
  return params
}

/** Convert TimeRange to FetchDeviceMetricsParams. */
export function toDeviceMetricsParams(timeRange: TimeRange): FetchDeviceMetricsParams {
  const params: FetchDeviceMetricsParams = {}
  if (timeRange.preset === 'custom' && timeRange.from && timeRange.to) {
    params.startTime = parseCustomTime(timeRange.from)
    params.endTime = parseCustomTime(timeRange.to)
  } else if (timeRange.preset !== 'custom') {
    params.range = timeRange.preset
  }
  return params
}
