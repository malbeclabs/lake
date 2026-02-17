import { useRef, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { fetchDashboardStress } from '@/lib/api'
import { useDashboard, dashboardFilterParams } from './dashboard-context'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'

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

const P50_COLOR = 'oklch(65% 0.15 250)'
const P95_COLOR = 'oklch(70% 0.2 45)'
const MAX_COLOR = 'oklch(65% 0.25 25)'

export function StressPanel() {
  const state = useDashboard()
  const { resolvedTheme } = useTheme()
  const { setCustomRange } = state
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const fmtRef = useRef<(v: number) => string>(formatRate)

  const params = useMemo(() => ({
    ...dashboardFilterParams(state),
    metric: state.metric,
  }), [state])

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-stress', params],
    queryFn: () => fetchDashboardStress(params),
    staleTime: 30_000,
    refetchInterval: state.refetchInterval,
  })

  const isUtil = state.metric === 'utilization'
  const fmt = isUtil ? formatPercent : state.metric === 'packets' ? formatPps : formatRate
  fmtRef.current = fmt

  const uplotData = useMemo(() => {
    if (!data?.timestamps?.length) return null

    const timestamps = data.timestamps.map(t => new Date(t).getTime() / 1000)
    // Rx (in) positive, Tx (out) negative
    const p50Out = (data.p50_out ?? []).map(v => -v)
    const p95Out = (data.p95_out ?? []).map(v => -v)
    const maxOut = (data.max_out ?? []).map(v => -v)
    return [
      timestamps,
      data.p50_in ?? [],   // P50 Rx
      data.p95_in ?? [],   // P95 Rx
      data.max_in ?? [],   // Max Rx
      p50Out,              // P50 Tx (negative)
      p95Out,              // P95 Tx (negative)
      maxOut,              // Max Tx (negative)
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
        { label: 'P50 Rx', stroke: P50_COLOR, width: 2 },
        { label: 'P95 Rx', stroke: P95_COLOR, width: 2 },
        { label: 'Max Rx', stroke: MAX_COLOR, width: 2, dash: [4, 4] },
        { label: 'P50 Tx', stroke: P50_COLOR, width: 2 },
        { label: 'P95 Tx', stroke: P95_COLOR, width: 2 },
        { label: 'Max Tx', stroke: MAX_COLOR, width: 2, dash: [4, 4] },
      ],
      scales: {
        x: { time: true },
        y: { auto: true },
      },
      axes: [
        { stroke: resolvedTheme === 'dark' ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)', grid: { stroke: 'rgba(128,128,128,0.06)' } },
        {
          values: (_: uPlot, vals: number[]) => vals.map(v => fmtRef.current(Math.abs(v))),
          size: 80,
          stroke: resolvedTheme === 'dark' ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)',
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
  }, [uplotData, setCustomRange, resolvedTheme])

  // Display values: hovered index, or latest data point
  const displayValues = useMemo(() => {
    if (!data?.timestamps?.length) return null
    const idx = hoveredIdx ?? data.timestamps.length - 1
    return {
      p50In: data.p50_in?.[idx] ?? 0,
      p95In: data.p95_in?.[idx] ?? 0,
      maxIn: data.max_in?.[idx] ?? 0,
      p50Out: data.p50_out?.[idx] ?? 0,
      p95Out: data.p95_out?.[idx] ?? 0,
      maxOut: data.max_out?.[idx] ?? 0,
      stressed: data.stressed_count?.[idx] ?? 0,
      total: data.total_count?.[idx] ?? 0,
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
        <div className="relative">
          <div className="absolute left-1 top-1 z-10 flex flex-col gap-0.5 text-[10px] text-muted-foreground pointer-events-none">
            <span>&#9650; Rx (in)</span>
          </div>
          <div className="absolute left-1 bottom-1 z-10 flex flex-col gap-0.5 text-[10px] text-muted-foreground pointer-events-none">
            <span>&#9660; Tx (out)</span>
          </div>
          <div ref={chartRef} className="w-full" />
        </div>
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
        {displayValues && (
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <span className="text-muted-foreground/60">Rx:</span>
            <span>P50 <span className="font-medium text-foreground">{fmt(displayValues.p50In)}</span></span>
            <span>P95 <span className="font-medium text-foreground">{fmt(displayValues.p95In)}</span></span>
            <span>Max <span className="font-medium text-foreground">{fmt(displayValues.maxIn)}</span></span>
            <span className="text-muted-foreground/60">Tx:</span>
            <span>P50 <span className="font-medium text-foreground">{fmt(displayValues.p50Out)}</span></span>
            <span>P95 <span className="font-medium text-foreground">{fmt(displayValues.p95Out)}</span></span>
            <span>Max <span className="font-medium text-foreground">{fmt(displayValues.maxOut)}</span></span>
            {isUtil && <span>Stressed: <span className="font-medium text-foreground">{displayValues.stressed}/{displayValues.total}</span></span>}
          </div>
        )}
      </div>
    </div>
  )
}
