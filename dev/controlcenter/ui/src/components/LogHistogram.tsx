import { useState, useCallback } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceArea,
  type MouseHandlerDataParam,
} from 'recharts'
import { type HistogramBucket } from '@/lib/api'

const LEVEL_COLORS = {
  error: '#ef4444',
  warn: '#eab308',
  info: '#22c55e',
  debug: '#3b82f6',
  unknown: '#6b7280',
}

const PRESETS = [
  { label: '1h', hours: 1 },
  { label: '3h', hours: 3 },
  { label: '6h', hours: 6 },
  { label: '12h', hours: 12 },
  { label: '24h', hours: 24 },
  { label: '7d', hours: 168 },
]

function getIntervalSeconds(hours: number): number {
  if (hours <= 6) return 5 * 60      // 5 min
  if (hours <= 24) return 30 * 60    // 30 min
  return 2 * 60 * 60                 // 2 hours
}

interface LogHistogramProps {
  data: HistogramBucket[]
  preset: string
  onPresetChange: (preset: string) => void
  onZoom: (from: Date, to: Date) => void
}

export function LogHistogram({ data, preset, onPresetChange, onZoom }: LogHistogramProps) {
  // Store raw activeLabel strings so they exactly match the data keys recharts uses
  const [dragStart, setDragStart] = useState<string | null>(null)
  const [dragEnd, setDragEnd] = useState<string | null>(null)
  const [isDragging, setIsDragging] = useState(false)

  const formatXAxis = (timeStr: string) => {
    const d = new Date(timeStr)
    const hours = d.getHours().toString().padStart(2, '0')
    const mins = d.getMinutes().toString().padStart(2, '0')
    return `${hours}:${mins}`
  }

  const handleMouseDown = useCallback((e: MouseHandlerDataParam) => {
    if (!e.activeLabel) return
    const label = String(e.activeLabel)
    setDragStart(label)
    setDragEnd(label) // initialise end = start so overlay appears immediately
    setIsDragging(true)
  }, [])

  const handleMouseMove = useCallback((e: MouseHandlerDataParam) => {
    if (!isDragging || !e.activeLabel) return
    setDragEnd(String(e.activeLabel))
  }, [isDragging])

  const handleMouseUp = useCallback(() => {
    if (!isDragging) return
    setIsDragging(false)

    if (dragStart !== null && dragEnd !== null) {
      const t1 = new Date(dragStart).getTime()
      const t2 = new Date(dragEnd).getTime()
      const from = new Date(Math.min(t1, t2))
      const to = new Date(Math.max(t1, t2))
      if (to.getTime() - from.getTime() > 0) {
        onZoom(from, to)
      }
    }

    setDragStart(null)
    setDragEnd(null)
  }, [isDragging, dragStart, dragEnd, onZoom])

  // x1/x2 must be in ascending order for ReferenceArea to render correctly
  const t1 = dragStart ? new Date(dragStart).getTime() : null
  const t2 = dragEnd ? new Date(dragEnd).getTime() : null
  const refAreaLeft = t1 !== null && t2 !== null ? (t1 <= t2 ? dragStart : dragEnd) : null
  const refAreaRight = t1 !== null && t2 !== null ? (t1 <= t2 ? dragEnd : dragStart) : null

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="text-sm font-medium text-muted-foreground">Log Activity</span>
        <div className="flex gap-1">
          {PRESETS.map((p) => (
            <button
              key={p.label}
              onClick={() => onPresetChange(p.label)}
              className={`px-2 py-1 text-xs rounded ${
                preset === p.label
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-secondary text-secondary-foreground hover:bg-secondary/80'
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>

      {data.length === 0 ? (
        <div className="flex items-center justify-center h-24 text-muted-foreground text-sm">
          No data for this time range
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={120}>
          <BarChart
            data={data}
            margin={{ top: 0, right: 0, left: -20, bottom: 0 }}
            onMouseDown={handleMouseDown}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            style={{ cursor: isDragging ? 'col-resize' : 'crosshair' }}
          >
            <XAxis
              dataKey="time"
              tickFormatter={formatXAxis}
              tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
              tickLine={false}
              axisLine={false}
              allowDecimals={false}
            />
            <Tooltip
              contentStyle={{
                background: 'var(--card)',
                border: '1px solid var(--border)',
                borderRadius: '6px',
                fontSize: '12px',
              }}
              labelFormatter={(v) => new Date(v as string).toLocaleString()}
              formatter={(value, name) => [value, (name as string).toUpperCase()]}
            />
            <Bar dataKey="error" stackId="a" fill={LEVEL_COLORS.error} isAnimationActive={false} />
            <Bar dataKey="warn" stackId="a" fill={LEVEL_COLORS.warn} isAnimationActive={false} />
            <Bar dataKey="info" stackId="a" fill={LEVEL_COLORS.info} isAnimationActive={false} />
            <Bar dataKey="debug" stackId="a" fill={LEVEL_COLORS.debug} isAnimationActive={false} />
            <Bar dataKey="unknown" stackId="a" fill={LEVEL_COLORS.unknown} isAnimationActive={false} />
            {refAreaLeft !== null && refAreaRight !== null && (
              <ReferenceArea
                x1={refAreaLeft}
                x2={refAreaRight}
                fill="var(--primary)"
                fillOpacity={0.25}
                stroke="var(--primary)"
                strokeOpacity={0.5}
              />
            )}
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}

export { PRESETS, getIntervalSeconds }
