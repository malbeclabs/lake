// web/src/components/ops/TicketChartBands.tsx
import { useState, useCallback, useEffect, useLayoutEffect, type RefObject, type MutableRefObject } from 'react'
import uPlot from 'uplot'
import type { OpsTicket } from '@/lib/ops-api'

interface BandRect {
  left: number   // CSS px from container left
  width: number  // CSS px
  top: number    // CSS px — top of the plot area (excludes x-axis labels)
  height: number // CSS px — height of the plot area
  type: 'incident' | 'maintenance'
}

interface TicketChartBandsProps {
  containerRef: RefObject<HTMLDivElement | null>
  plotRef: MutableRefObject<uPlot | null>
  plotVersion: number
  tickets: OpsTicket[]
  showIncidents: boolean
  showMaintenance: boolean
}

const COLORS = {
  incident:    { bg: 'rgba(153,27,27,0.28)', border: 'rgba(153,27,27,0.70)' },
  maintenance: { bg: 'rgba(29,78,216,0.22)',  border: 'rgba(29,78,216,0.60)' },
}

/**
 * Renders absolutely-positioned ticket-range bands inside a uPlot chart container.
 * Purely decorative — pointer-events-none so chart cursor interaction is preserved.
 * The parent must have position:relative and overflow-hidden.
 */
export function TicketChartBands({ containerRef, plotRef, plotVersion, tickets, showIncidents, showMaintenance }: TicketChartBandsProps) {
  const [bands, setBands] = useState<BandRect[]>([])

  const computeBands = useCallback(() => {
    const u = plotRef.current
    if (!u || !u.over) return

    // Use u.over's CSS dimensions to get DPR-safe pixel coordinates.
    // u.over is the plot-area element positioned within the uPlot root (= containerRef).
    const plotLeft = u.over.offsetLeft
    const plotWidth = u.over.offsetWidth
    const plotTop = u.over.offsetTop
    const plotHeight = u.over.offsetHeight

    const xMin = u.scales.x.min ?? 0
    const xMax = u.scales.x.max ?? 0
    const range = xMax - xMin
    if (range <= 0) return

    const computed: BandRect[] = []

    for (const t of tickets) {
      if (t.type === 'incident' && !showIncidents) continue
      if (t.type === 'maintenance' && !showMaintenance) continue

      const startSec = t.start_at ? new Date(t.start_at).getTime() / 1000 : null
      const endSec = t.end_at ? new Date(t.end_at).getTime() / 1000 : Date.now() / 1000

      if (!startSec) continue
      if (endSec < xMin || startSec > xMax) continue

      const startPct = (Math.max(startSec, xMin) - xMin) / range
      const endPct = (Math.min(endSec, xMax) - xMin) / range

      const startPos = plotLeft + startPct * plotWidth
      const endPos = plotLeft + endPct * plotWidth

      computed.push({
        left: startPos,
        width: Math.max(endPos - startPos, 2),
        top: plotTop,
        height: plotHeight,
        type: t.type,
      })
    }
    setBands(computed)
  }, [plotRef, tickets, showIncidents, showMaintenance])

  // Recompute on container resize
  useLayoutEffect(() => {
    const container = containerRef.current
    if (!container) return
    const ro = new ResizeObserver(computeBands)
    ro.observe(container)
    computeBands()
    return () => ro.disconnect()
  }, [containerRef, computeBands])

  // Recompute after uPlot initializes (plotVersion increments after uPlot is created)
  useEffect(() => {
    computeBands()
  }, [computeBands, plotVersion])

  if (bands.length === 0) return null

  return (
    <>
      {bands.map((b, i) => {
        const colors = COLORS[b.type]
        return (
          <div
            key={i}
            className="absolute pointer-events-none"
            style={{
              left: b.left,
              width: b.width,
              top: b.top,
              height: b.height,
              background: colors.bg,
              borderLeft: `1px solid ${colors.border}`,
              borderRight: `1px solid ${colors.border}`,
              zIndex: 3,
            }}
          />
        )
      })}
    </>
  )
}
