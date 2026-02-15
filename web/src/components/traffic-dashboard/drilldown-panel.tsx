import { useRef, useEffect, useMemo } from 'react'
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

function entityLabel(e: SelectedEntity): string {
  return e.intf ? `${e.deviceCode} ${e.intf}` : e.deviceCode
}

function DrilldownChart({ entity }: { entity: SelectedEntity }) {
  const state = useDashboard()
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)

  const isPinned = state.pinnedEntities.some(
    p => p.devicePk === entity.devicePk && p.intf === entity.intf
  )

  const { data, isLoading } = useQuery({
    queryKey: ['dashboard-drilldown', entity.devicePk, entity.intf, state.timeRange],
    queryFn: () => fetchDashboardDrilldown({
      device_pk: entity.devicePk,
      intf: entity.intf,
      time_range: state.timeRange,
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

    // Build series arrays: for each interface, in_bps and out_bps
    const seriesData: (number | null)[][] = []
    intfs.forEach(intf => {
      const inData: (number | null)[] = []
      const outData: (number | null)[] = [];
      [...tsSet].sort().forEach(t => {
        const point = lookup.get(t)?.get(intf)
        inData.push(point?.in_bps ?? null)
        outData.push(point ? -(point.out_bps) : null) // negative for out (below axis)
      })
      seriesData.push(inData)
      seriesData.push(outData)
    })

    return {
      aligned: [timestamps, ...seriesData] as uPlot.AlignedData,
      intfs,
    }
  }, [data])

  useEffect(() => {
    if (!chartRef.current || !uplotData) return

    plotRef.current?.destroy()

    const colors = [
      'oklch(65% 0.15 250)',
      'oklch(65% 0.15 150)',
      'oklch(65% 0.15 350)',
      'oklch(65% 0.15 50)',
      'oklch(65% 0.15 200)',
    ]

    const series: uPlot.Series[] = [{}]
    uplotData.intfs.forEach((intf, i) => {
      const color = colors[i % colors.length]
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
          values: (_: uPlot, vals: number[]) => vals.map(v => formatRate(Math.abs(v))),
          size: 70,
        },
      ],
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
  }, [uplotData])

  // Find bandwidth from series metadata
  const bandwidth = data?.series?.find(s => s.intf === entity.intf)?.bandwidth_bps

  return (
    <div className="border border-border/50 rounded p-3">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <h3 className="text-xs font-semibold font-mono">{entityLabel(entity)}</h3>
          {bandwidth != null && bandwidth > 0 && (
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
          <div className="flex items-center gap-3 mt-1 text-xs text-muted-foreground flex-wrap">
            {uplotData.intfs.map((intf, i) => {
              const colors = [
                'oklch(65% 0.15 250)',
                'oklch(65% 0.15 150)',
                'oklch(65% 0.15 350)',
                'oklch(65% 0.15 50)',
                'oklch(65% 0.15 200)',
              ]
              const color = colors[i % colors.length]
              return (
                <span key={intf} className="flex items-center gap-1">
                  <span className="w-3 h-0.5 inline-block" style={{ backgroundColor: color }} />
                  {intf}
                </span>
              )
            })}
          </div>
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
