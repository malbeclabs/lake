import { useEffect, useRef, type MutableRefObject, type RefObject } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { useTheme } from '@/hooks/use-theme'

export interface UseUPlotChartOptions {
  containerRef: RefObject<HTMLDivElement | null>
  data: uPlot.AlignedData
  series: uPlot.Series[]
  height: number
  axes?: uPlot.Axis[]
  scales?: uPlot.Scales
  onCursorIdx?: (idx: number | null) => void
  onFocusSeries?: (seriesIdx: number | null) => void
  drawHooks?: Array<(u: uPlot) => void>
}

export function useUPlotChart({
  containerRef,
  data,
  series,
  height,
  axes,
  scales,
  onCursorIdx,
  onFocusSeries,
  drawHooks,
}: UseUPlotChartOptions): { plotRef: MutableRefObject<uPlot | null> } {
  const plotRef = useRef<uPlot | null>(null)
  const { resolvedTheme } = useTheme()
  const onCursorIdxRef = useRef(onCursorIdx)
  onCursorIdxRef.current = onCursorIdx
  const onFocusSeriesRef = useRef(onFocusSeries)
  onFocusSeriesRef.current = onFocusSeries
  const drawHooksRef = useRef(drawHooks)
  drawHooksRef.current = drawHooks

  useEffect(() => {
    const container = containerRef.current
    if (!container || data[0].length === 0) return

    const axisStroke = resolvedTheme === 'dark' ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)'
    const gridStroke = 'rgba(128,128,128,0.06)'
    const tickStroke = 'rgba(128,128,128,0.1)'

    const defaultAxes: uPlot.Axis[] = [
      {
        stroke: axisStroke,
        grid: { stroke: gridStroke },
        ticks: { stroke: tickStroke },
      },
      {
        stroke: axisStroke,
        grid: { stroke: gridStroke },
        ticks: { stroke: tickStroke },
        size: (_u: uPlot, values: string[]) => {
          if (!values || values.length === 0) return 80
          const maxLen = Math.max(...values.map(v => (v || '').length))
          return Math.max(50, maxLen * 8 + 16)
        },
      },
    ]

    const mergedAxes = axes
      ? axes.map((a, i) => ({
          ...defaultAxes[i],
          ...a,
        }))
      : defaultAxes

    // Apply spline paths to all data series for smoother lines
    const splinePaths = uPlot.paths.spline?.()
    const smoothedSeries = series.map((s, i) => {
      if (i === 0 || !splinePaths) return s // skip x-axis series
      return { ...s, paths: s.paths ?? splinePaths }
    })

    const opts: uPlot.Options = {
      width: container.offsetWidth,
      height,
      series: smoothedSeries,
      scales: scales ?? { x: { time: true }, y: { auto: true } },
      axes: mergedAxes,
      cursor: {
        focus: { prox: Infinity },
        points: {
          size: 12,
          width: 2,
        },
      },
      hooks: {
        draw: drawHooksRef.current ? [
          (u: uPlot) => {
            if (drawHooksRef.current) {
              for (const hook of drawHooksRef.current) {
                hook(u)
              }
            }
          },
        ] : [],
        setCursor: [
          (u) => {
            const idx = u.cursor.idx
            onCursorIdxRef.current?.(idx ?? null)
            if (onFocusSeriesRef.current) {
              let focusedIdx: number | null = null
              for (let i = 1; i < u.series.length; i++) {
                if ((u.series[i] as uPlot.Series & { _focus?: boolean })._focus) {
                  focusedIdx = i
                  break
                }
              }
              onFocusSeriesRef.current(focusedIdx)
            }
          },
        ],
      },
      legend: { show: false },
    }

    if (plotRef.current) {
      plotRef.current.destroy()
    }

    plotRef.current = new uPlot(opts, data, container)

    const resizeObserver = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect.width
      if (width && plotRef.current) {
        plotRef.current.setSize({ width, height })
      }
    })
    resizeObserver.observe(container)

    return () => {
      resizeObserver.disconnect()
      if (plotRef.current) {
        plotRef.current.destroy()
        plotRef.current = null
      }
    }
  }, [containerRef, data, series, height, axes, scales, resolvedTheme])

  return { plotRef }
}
