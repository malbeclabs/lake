import { useLayoutEffect, type MutableRefObject } from 'react'
import type uPlot from 'uplot'
import type { UseChartLegendReturn } from '@/hooks/use-chart-legend'

/**
 * Syncs useChartLegend opacity state to uPlot series alpha values.
 * Runs on every render to catch both legend state changes and chart recreation.
 */
export function useUPlotLegendSync(
  plotRef: MutableRefObject<uPlot | null>,
  legend: UseChartLegendReturn,
  seriesKeys: string[],
) {
  useLayoutEffect(() => {
    const u = plotRef.current
    if (!u) return

    let changed = false
    for (let i = 0; i < seriesKeys.length; i++) {
      const key = seriesKeys[i]
      const alpha = legend.getOpacity(key)
      const seriesIdx = i + 1 // offset for x-axis series at index 0
      if (seriesIdx < u.series.length && u.series[seriesIdx].alpha !== alpha) {
        u.series[seriesIdx].alpha = alpha
        changed = true
      }
    }
    if (changed) {
      u.redraw()
    }
  })
}
