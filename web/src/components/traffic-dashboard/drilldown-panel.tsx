import { useRef, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { fetchDashboardDrilldown, type DashboardDrilldownPoint } from '@/lib/api'
import { useDashboard, type SelectedEntity } from './dashboard-context'
import { Loader2, Pin, PinOff, X } from 'lucide-react'

function formatRate(val: number): string {
  if (val >= 1e12) return (val / 1e12).toFixed(1) + ' Tbps'
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gbps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mbps'
  if (val >= 1e3) return (val / 1e3).toFixed(1) + ' Kbps'
  return val.toFixed(0) + ' bps'
}

function formatPps(val: number): string {
  if (val >= 1e9) return (val / 1e9).toFixed(1) + ' Gpps'
  if (val >= 1e6) return (val / 1e6).toFixed(1) + ' Mpps'
  if (val >= 1e3) return (val / 1e3).toFixed(1) + ' Kpps'
  return val.toFixed(0) + ' pps'
}

function entityLabel(e: SelectedEntity): string {
  return e.intf ? `${e.deviceCode} ${e.intf}` : e.deviceCode
}

const seriesColors = [
  'oklch(65% 0.15 250)',
  'oklch(65% 0.15 150)',
  'oklch(65% 0.15 350)',
  'oklch(65% 0.15 50)',
  'oklch(65% 0.15 200)',
]

function DrilldownChart({ entity }: { entity: SelectedEntity }) {
  const state = useDashboard()
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const isPinned = state.pinnedEntities.some(
    p => p.devicePk === entity.devicePk && p.intf === entity.intf
  )
  const isPps = state.metric === 'packets'
  const fmt = isPps ? formatPps : formatRate

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-drilldown', entity.devicePk, entity.intf, state.timeRange, state.intfType],
    queryFn: () => fetchDashboardDrilldown({
      device_pk: entity.devicePk,
      intf: entity.intf,
      time_range: state.timeRange,
      intf_type: state.intfType !== 'all' ? state.intfType : undefined,
    }),
    staleTime: 30_000,
  })

  // Group points by interface
  const uplotData = useMemo(() => {
    if (!data?.points?.length) return null

    // Get unique interfaces
    const intfs = [...new Set(data.points.map(p => p.intf))].sort()

    // Collect unique timestamps
    const tsSet = new Set<string>()
    data.points.forEach(p => tsSet.add(p.time))
    const timestamps = [...tsSet].sort().map(t => new Date(t).getTime() / 1000)

    // Build lookup: time -> intf -> point
    const lookup = new Map<string, Map<string, DashboardDrilldownPoint>>()
    data.points.forEach(p => {
      if (!lookup.has(p.time)) lookup.set(p.time, new Map())
      lookup.get(p.time)!.set(p.intf, p)
    })

    // Build series arrays: for each interface, in and out values
    const seriesData: (number | null)[][] = []
    intfs.forEach(intf => {
      const inData: (number | null)[] = []
      const outData: (number | null)[] = [];
      [...tsSet].sort().forEach(t => {
        const point = lookup.get(t)?.get(intf)
        if (isPps) {
          inData.push(point?.in_pps ?? null)
          outData.push(point ? -(point.out_pps) : null)
        } else {
          inData.push(point?.in_bps ?? null)
          outData.push(point ? -(point.out_bps) : null)
        }
      })
      seriesData.push(inData)
      seriesData.push(outData)
    })

    return {
      aligned: [timestamps, ...seriesData] as uPlot.AlignedData,
      intfs,
    }
  }, [data, isPps])

  useEffect(() => {
    if (!chartRef.current || !uplotData) return

    plotRef.current?.destroy()

    const series: uPlot.Series[] = [{}]
    uplotData.intfs.forEach((intf, i) => {
      const color = seriesColors[i % seriesColors.length]
      series.push({
        label: `${intf} Rx`,
        stroke: color,
        width: 1.5,
        fill: color.replace('65%', '65%') + '/10',
      })
      series.push({
        label: `${intf} Tx`,
        stroke: color,
        width: 1.5,
        dash: [4, 2],
        fill: color.replace('65%', '65%') + '/10',
      })
    })

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 240,
      series,
      scales: {
        x: { time: true },
        y: { auto: true },
      },
      axes: [
        {},
        {
          values: (_: uPlot, vals: number[]) => vals.map(v => fmt(Math.abs(v))),
          size: 70,
        },
      ],
      hooks: {
        setCursor: [(u: uPlot) => {
          setHoveredIdx(u.cursor.idx ?? null)
        }],
      },
      legend: { show: false },
    }

    plotRef.current = new uPlot(opts, uplotData.aligned, chartRef.current)

    const resizeObserver = new ResizeObserver(entries => {
      const width = entries[0]?.contentRect.width
      if (width && plotRef.current) plotRef.current.setSize({ width, height: 240 })
    })
    resizeObserver.observe(chartRef.current)

    return () => {
      resizeObserver.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [uplotData, fmt])

  // Build per-interface metadata: bandwidth and link type from series data
  const intfMeta = useMemo(() => {
    if (!data?.series) return new Map<string, { bandwidth: number; linkType: string }>()
    const m = new Map<string, { bandwidth: number; linkType: string }>()
    for (const s of data.series) {
      m.set(s.intf, { bandwidth: s.bandwidth_bps, linkType: s.link_type })
    }
    return m
  }, [data?.series])

  // Find bandwidth for header (single-interface drilldown)
  const bandwidth = data?.series?.find(s => s.intf === entity.intf)?.bandwidth_bps

  // Hover values: for each interface, Rx/Tx at cursor position
  const hoverValues = useMemo(() => {
    if (hoveredIdx === null || !uplotData) return null
    const m = new Map<string, { rx: number; tx: number }>()
    uplotData.intfs.forEach((intf, i) => {
      const rxIdx = 1 + i * 2
      const txIdx = 2 + i * 2
      const rx = uplotData.aligned[rxIdx]?.[hoveredIdx] as number | null
      const tx = uplotData.aligned[txIdx]?.[hoveredIdx] as number | null
      m.set(intf, { rx: rx ?? 0, tx: tx != null ? Math.abs(tx) : 0 })
    })
    return m
  }, [hoveredIdx, uplotData])

  const multiIntf = uplotData && uplotData.intfs.length > 1

  return (
    <div className="border border-border/50 rounded p-3">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <h3 className="text-xs font-semibold font-mono">{entityLabel(entity)}</h3>
          {bandwidth != null && bandwidth > 0 && !multiIntf && (
            <span className="text-xs text-muted-foreground">
              ({formatRate(bandwidth)} capacity)
            </span>
          )}
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={() => isPinned ? state.unpinEntity(entity) : state.pinEntity(entity)}
            className="p-1 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
            title={isPinned ? 'Unpin' : 'Pin for comparison'}
          >
            {isPinned ? <PinOff className="h-3.5 w-3.5" /> : <Pin className="h-3.5 w-3.5" />}
          </button>
          {!isPinned && (
            <button
              onClick={() => state.selectEntity(null)}
              className="p-1 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
              title="Close"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>
      {isLoading ? (
        <div className="h-[240px] flex items-center justify-center">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        </div>
      ) : !uplotData ? (
        <div className="h-[240px] flex items-center justify-center text-sm text-muted-foreground">
          No data
        </div>
      ) : (
        <>
          <div ref={chartRef} className="w-full" />
          {multiIntf ? (
            <table className="w-full mt-2 text-xs table-fixed">
              <colgroup>
                <col />
                <col className="w-24" />
                <col className="w-24" />
                <col className="w-24" />
              </colgroup>
              <thead>
                <tr className="text-muted-foreground">
                  <th className="text-left font-normal pb-1">Interface</th>
                  <th className="text-right font-normal pb-1">Capacity</th>
                  <th className="text-right font-normal pb-1">Rx</th>
                  <th className="text-right font-normal pb-1">Tx</th>
                </tr>
              </thead>
              <tbody>
                {uplotData.intfs.map((intf, i) => {
                  const color = seriesColors[i % seriesColors.length]
                  const meta = intfMeta.get(intf)
                  const hv = hoverValues?.get(intf)
                  return (
                    <tr key={intf} className="border-t border-border/30">
                      <td className="py-1">
                        <span className="flex items-center gap-1.5">
                          <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: color }} />
                          <span className="font-mono text-foreground truncate">{intf}</span>
                        </span>
                      </td>
                      <td className="py-1 text-right text-muted-foreground font-mono tabular-nums">
                        {meta && meta.bandwidth > 0 ? formatRate(meta.bandwidth) : '—'}
                      </td>
                      <td className="py-1 text-right font-mono tabular-nums">
                        {hv ? <span className="text-foreground">{fmt(hv.rx)}</span> : <span className="text-muted-foreground">—</span>}
                      </td>
                      <td className="py-1 text-right font-mono tabular-nums">
                        {hv ? <span className="text-foreground">{fmt(hv.tx)}</span> : <span className="text-muted-foreground">—</span>}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          ) : (
            <div className="flex items-center justify-between mt-1 h-5">
              <div className="flex items-center gap-3 text-xs text-muted-foreground">
                <span className="flex items-center gap-1">
                  <span className="w-3 h-0.5 inline-block" style={{ backgroundColor: seriesColors[0] }} />
                  Rx (solid) / Tx (dashed)
                </span>
              </div>
              {hoverValues && (
                <div className="flex items-center gap-4 text-xs text-muted-foreground">
                  {(() => {
                    const hv = hoverValues.get(uplotData.intfs[0])
                    if (!hv) return null
                    return (
                      <>
                        <span>Rx: <span className="font-medium text-foreground">{fmt(hv.rx)}</span></span>
                        <span>Tx: <span className="font-medium text-foreground">{fmt(hv.tx)}</span></span>
                      </>
                    )
                  })()}
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}

export function DrilldownPanel() {
  const { selectedEntity, pinnedEntities } = useDashboard()

  // Deduplicate: don't show selected entity if it's also pinned
  const entitiesToShow: SelectedEntity[] = [...pinnedEntities]
  if (selectedEntity && !pinnedEntities.some(
    p => p.devicePk === selectedEntity.devicePk && p.intf === selectedEntity.intf
  )) {
    entitiesToShow.push(selectedEntity)
  }

  if (entitiesToShow.length === 0) return null

  return (
    <div className="space-y-3">
      {entitiesToShow.map(e => (
        <DrilldownChart key={`${e.devicePk}-${e.intf || ''}`} entity={e} />
      ))}
    </div>
  )
}
