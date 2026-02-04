import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  Tooltip,
} from 'recharts'
import type { HistogramBucket } from '@/lib/api'

function formatBucketTime(timestamp: string): string {
  const date = new Date(timestamp)
  if (date.getMinutes() === 0) {
    return date.toLocaleTimeString([], { hour: 'numeric', hour12: true })
  }
  return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', hour12: true })
}

function formatBucketDate(timestamp: string): string {
  const date = new Date(timestamp)
  return date.toLocaleDateString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })
}

export function EventHistogram({ data, onBucketClick }: { data: HistogramBucket[], onBucketClick?: (bucket: HistogramBucket, nextBucket?: HistogramBucket) => void }) {
  if (!data || data.length < 6) return null

  const maxCount = Math.max(...data.map(d => d.count))
  if (maxCount === 0) return null

  const firstDate = new Date(data[0].timestamp)
  const lastDate = new Date(data[data.length - 1].timestamp)
  const spansDays = lastDate.getTime() - firstDate.getTime() > 24 * 60 * 60 * 1000

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleBarClick = (barData: any) => {
    if (!onBucketClick || !barData?.payload) return
    const clickedBucket = barData.payload as HistogramBucket
    const idx = data.findIndex(b => b.timestamp === clickedBucket.timestamp)
    const nextBucket = idx >= 0 ? data[idx + 1] : undefined
    onBucketClick(clickedBucket, nextBucket)
  }

  return (
    <div className="mb-4">
      <div className="h-20">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="barGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="hsl(220, 70%, 55%)" stopOpacity={0.8} />
                <stop offset="100%" stopColor="hsl(220, 70%, 50%)" stopOpacity={0.3} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="timestamp"
              axisLine={false}
              tickLine={false}
              tick={false}
              hide
            />
            <Tooltip
              cursor={{ fill: 'hsl(var(--foreground))', opacity: 0.1 }}
              content={({ active, payload }) => {
                if (!active || !payload?.[0]) return null
                const bucket = payload[0].payload as HistogramBucket
                return (
                  <div className="bg-popover border border-border rounded px-2 py-1 text-xs shadow-lg">
                    <div className="font-medium">{bucket.count} events</div>
                    <div className="text-muted-foreground">{formatBucketDate(bucket.timestamp)}</div>
                    {onBucketClick && <div className="text-muted-foreground/70 mt-0.5">Click to filter</div>}
                  </div>
                )
              }}
            />
            <Bar
              dataKey="count"
              fill="url(#barGradient)"
              radius={[2, 2, 0, 0]}
              cursor={onBucketClick ? 'pointer' : undefined}
              onClick={handleBarClick}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
        <span>{spansDays ? formatBucketDate(data[0].timestamp) : formatBucketTime(data[0].timestamp)}</span>
        <span>{spansDays ? formatBucketDate(data[data.length - 1].timestamp) : formatBucketTime(data[data.length - 1].timestamp)}</span>
      </div>
    </div>
  )
}
