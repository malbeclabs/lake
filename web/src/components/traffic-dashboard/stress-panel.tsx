import { useRef, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { fetchDashboardStress } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { Loader2 } from 'lucide-react'

function formatRate(val: number): string {
  if (val >= 1e12) return (val / 1e12).toFixed(1) + ' Tbps'
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gbps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mbps'
  if (val >= 1e3) return (val / 1e3).toFixed(1) + ' Kbps'
  return val.toFixed(0) + ' bps'
}

function formatPercent(val: number): string {
  return (val * 100).toFixed(1) + '%'
}

function formatPps(val: number): string {
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gpps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mpps'
  if (val >= 1e3) return (val / 1e3).toFixed(1) + ' Kpps'
  return val.toFixed(0) + ' pps'
}

export function StressPanel() {
  const state = useDashboard()
  const { setCustomRange } = state
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const params = useMemo(() => ({
    ...dashboardFilterParams(state),
    metric: state.metric,
  }), [state])

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-stress', params],
    queryFn: () => fetchDashboardStress(params),
    staleTime: 30_000,
  })

  const isUtil = state.metric === 'utilization'
  const fmt = isUtil ? formatPercent : state.metric === 'packets' ? formatPps : formatRate

  const uplotData = useMemo(() => {
    if (!data?.timestamps?.length) return null

    const timestamps = data.timestamps.map(t => new Date(t).getTime() / 1000)
    return [
      timestamps,
      data.p50 ?? [],
      data.p95 ?? [],
      data.max ?? [],
    ] as uPlot.AlignedData
  }, [data])

  useEffect(() => {
    if (!chartRef.current || !uplotData) return

    plotRef.current?.destroy()

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 280,
      series: [
        {},
        { label: 'P50', stroke: 'oklch(65% 0.15 250)', width: 2 },
        { label: 'P95', stroke: 'oklch(70% 0.2 45)', width: 2 },
        { label: 'Max', stroke: 'oklch(65% 0.25 25)', width: 2, dash: [4, 4] },
      ],
      scales: {
        x: { time: true },
        y: {
          auto: true,
          range: isUtil ? [0, 1] : undefined,
        },
      },
      axes: [
        { stroke: document.documentElement.classList.contains('dark') ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)', grid: { stroke: 'rgba(128,128,128,0.06)' } },
        {
          values: (_: uPlot, vals: number[]) => vals.map(v => fmt(v)),
          size: 70,
          stroke: document.documentElement.classList.contains('dark') ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)',
          grid: { stroke: 'rgba(128,128,128,0.06)' },
        },
      ],
      cursor: {
        drag: { x: true, y: false },
      },
      hooks: {
        setCursor: [(u: uPlot) => {
          setHoveredIdx(u.cursor.idx ?? null)
        }],
        setSelect: [(u: uPlot) => {
          const left = u.select.left
          const width = u.select.width
          if (width > 0) {
            const startTs = Math.floor(u.posToVal(left, 'x'))
            const endTs = Math.floor(u.posToVal(left + width, 'x'))
            if (endTs > startTs) {
              setCustomRange(startTs, endTs)
            }
            u.setSelect({ left: 0, width: 0, top: 0, height: 0 }, false)
          }
        }],
      },
      legend: { show: false },
    }

    plotRef.current = new uPlot(opts, uplotData, chartRef.current)

    const resizeObserver = new ResizeObserver(entries => {
      const width = entries[0]?.contentRect.width
      if (width && plotRef.current) plotRef.current.setSize({ width, height: 280 })
    })
    resizeObserver.observe(chartRef.current)

    return () => {
      resizeObserver.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [uplotData, isUtil, fmt, setCustomRange])

  // Tooltip values
  const tooltipValues = useMemo(() => {
    if (hoveredIdx === null || !data?.timestamps?.length) return null
    return {
      time: new Date(data.timestamps[hoveredIdx]).toLocaleString(),
      p50: data.p50?.[hoveredIdx] ?? 0,
      p95: data.p95?.[hoveredIdx] ?? 0,
      max: data.max?.[hoveredIdx] ?? 0,
      stressed: data.stressed_count?.[hoveredIdx] ?? 0,
      total: data.total_count?.[hoveredIdx] ?? 0,
    }
  }, [hoveredIdx, data])

  return (
    <div>
      {isLoading ? (
        <div className="h-[280px] flex items-center justify-center">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : !uplotData ? (
        <div className="h-[280px] flex items-center justify-center text-sm text-muted-foreground">
          No data available
        </div>
      ) : (
        <div ref={chartRef} className="w-full" />
      )}
      <div className="flex items-center justify-between mt-2 h-5">
        <div className="flex items-center gap-4 text-xs text-muted-foreground">
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-0.5 bg-[oklch(65%_0.15_250)] inline-block" /> P50
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-0.5 bg-[oklch(70%_0.2_45)] inline-block" /> P95
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-0.5 bg-[oklch(65%_0.25_25)] inline-block border-dashed" /> Max
          </span>
        </div>
        {tooltipValues && (
          <div className="flex items-center gap-4 text-xs text-muted-foreground">
            <span>P50: <span className="font-medium text-foreground">{fmt(tooltipValues.p50)}</span></span>
            <span>P95: <span className="font-medium text-foreground">{fmt(tooltipValues.p95)}</span></span>
            <span>Max: <span className="font-medium text-foreground">{fmt(tooltipValues.max)}</span></span>
            {isUtil && <span>Stressed: <span className="font-medium text-foreground">{tooltipValues.stressed}/{tooltipValues.total}</span></span>}
          </div>
        )}
      </div>
    </div>
  )
}

