import { useMemo, useRef, useEffect, useCallback } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { useTheme } from '@/hooks/use-theme'
import type { HistogramBucket } from '@/lib/api'

function formatBucketDate(timestamp: string): string {
  const date = new Date(timestamp)
  return date.toLocaleDateString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })
}

function formatBucketTime(timestamp: string): string {
  const date = new Date(timestamp)
  if (date.getMinutes() === 0) {
    return date.toLocaleTimeString([], { hour: 'numeric', hour12: true })
  }
  return date.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', hour12: true })
}

export function EventHistogram({ data, onBucketClick }: { data: HistogramBucket[], onBucketClick?: (bucket: HistogramBucket, nextBucket?: HistogramBucket) => void }) {
  const { resolvedTheme } = useTheme()
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const dataRef = useRef(data)
  dataRef.current = data

  const uplotData = useMemo(() => {
    if (!data || data.length === 0) return [[]] as uPlot.AlignedData
    const timestamps = data.map(d => new Date(d.timestamp).getTime() / 1000)
    const counts = data.map(d => d.count)
    return [timestamps, counts] as uPlot.AlignedData
  }, [data])

  const barPaths = useMemo(() => uPlot.paths.bars?.({ size: [0.8, 20], gap: 1 }), [])

  const series = useMemo((): uPlot.Series[] => [
    {},
    {
      label: 'Events',
      fill: 'rgba(59, 130, 246, 0.5)',
      stroke: 'rgba(59, 130, 246, 0.8)',
      width: 0,
      paths: barPaths,
      points: { show: false },
    },
  ], [barPaths])

  const handleClick = useCallback((_u: uPlot, _seriesIdx: number | null, dataIdx: number | null) => {
    if (!onBucketClick || dataIdx == null) return
    const d = dataRef.current
    if (dataIdx >= 0 && dataIdx < d.length) {
      onBucketClick(d[dataIdx], d[dataIdx + 1])
    }
  }, [onBucketClick])

  useEffect(() => {
    if (!chartRef.current || uplotData[0].length === 0) {
      plotRef.current?.destroy()
      plotRef.current = null
      return
    }

    plotRef.current?.destroy()

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 80,
      series,
      scales: { x: { time: true }, y: { auto: true, range: (_u, _min, max) => [0, max] } },
      axes: [
        { show: false },
        { show: false },
      ],
      cursor: {
        points: { show: false },
        move: (u, left, top) => {
          const idx = u.posToIdx(left)
          if (idx >= 0 && idx < u.data[0].length) {
            const snapped = Math.round(u.valToPos(u.data[0][idx], 'x'))
            return [snapped, top]
          }
          return [left, top]
        },
      },
      legend: { show: false },
    }

    plotRef.current = new uPlot(opts, uplotData, chartRef.current)

    // Click handler
    const overEl = plotRef.current.over
    if (onBucketClick) {
      overEl.style.cursor = 'pointer'
    }
    const clickHandler = () => {
      const u = plotRef.current
      if (!u || !onBucketClick) return
      const idx = u.cursor.idx
      if (idx != null && idx >= 0) {
        handleClick(u, 1, idx)
      }
    }
    overEl.addEventListener('click', clickHandler)

    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        plotRef.current?.setSize({ width: entry.contentRect.width, height: 80 })
      }
    })
    ro.observe(chartRef.current)

    return () => {
      overEl.removeEventListener('click', clickHandler)
      ro.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [uplotData, series, resolvedTheme, onBucketClick, handleClick])

  // Early return after hooks
  if (!data || data.length < 6) return null
  const maxCount = Math.max(...data.map(d => d.count))
  if (maxCount === 0) return null

  const firstDate = new Date(data[0].timestamp)
  const lastDate = new Date(data[data.length - 1].timestamp)
  const spansDays = lastDate.getTime() - firstDate.getTime() > 24 * 60 * 60 * 1000

  return (
    <div className="mb-4">
      <div ref={chartRef} className="h-20" />
      <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
        <span>{spansDays ? formatBucketDate(data[0].timestamp) : formatBucketTime(data[0].timestamp)}</span>
        <span>{spansDays ? formatBucketDate(data[data.length - 1].timestamp) : formatBucketTime(data[data.length - 1].timestamp)}</span>
      </div>
    </div>
  )
}
