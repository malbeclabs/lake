import { useRef, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { fetchDashboardDrilldown, type DashboardDrilldownPoint } from '@/lib/api'
import { useDashboard, type SelectedEntity } from './dashboard-context'
import { Loader2, Pin, PinOff, X, Search, ChevronUp, ChevronDown } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'

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
  const { resolvedTheme } = useTheme()
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const [selectedIntfs, setSelectedIntfs] = useState<Set<string>>(new Set())
  const [lastClickedIndex, setLastClickedIndex] = useState<number | null>(null)
  const [searchText, setSearchText] = useState('')
  const [searchExpanded, setSearchExpanded] = useState(false)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [sortBy, setSortBy] = useState<'value' | 'name'>('value')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [listHeight, setListHeight] = useState(160)
  const listContainerRef = useRef<HTMLDivElement>(null)

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
    refetchInterval: state.refetchInterval,
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

    const axisStroke = resolvedTheme === 'dark' ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)'

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 240,
      series,
      scales: {
        x: { time: true },
        y: { auto: true },
      },
      axes: [
        { stroke: axisStroke, grid: { stroke: 'rgba(128,128,128,0.06)' } },
        {
          values: (_: uPlot, vals: number[]) => vals.map(v => fmt(Math.abs(v))),
          size: 70,
          stroke: axisStroke,
          grid: { stroke: 'rgba(128,128,128,0.06)' },
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
  }, [uplotData, fmt, resolvedTheme])

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

  // Latest values: last non-null Rx/Tx per interface
  const latestValues = useMemo(() => {
    if (!uplotData) return new Map<string, { rx: number; tx: number }>()
    const m = new Map<string, { rx: number; tx: number }>()
    uplotData.intfs.forEach((intf, i) => {
      const rxArr = uplotData.aligned[1 + i * 2]
      const txArr = uplotData.aligned[2 + i * 2]
      let rx = 0, tx = 0
      for (let j = (rxArr?.length ?? 0) - 1; j >= 0; j--) {
        const v = rxArr?.[j]
        if (v != null) { rx = v as number; break }
      }
      for (let j = (txArr?.length ?? 0) - 1; j >= 0; j--) {
        const v = txArr?.[j]
        if (v != null) { tx = Math.abs(v as number); break }
      }
      m.set(intf, { rx, tx })
    })
    return m
  }, [uplotData])

  // Visible interfaces (selected or all if none selected)
  const visibleIntfs = useMemo(() => {
    if (selectedIntfs.has('__none__')) return new Set<string>()
    if (selectedIntfs.size > 0) return selectedIntfs
    return new Set(uplotData?.intfs ?? [])
  }, [selectedIntfs, uplotData?.intfs])

  // Filter interfaces by search text
  const filteredIntfs = useMemo(() => {
    if (!uplotData) return [] as string[]
    if (!searchText.trim()) return uplotData.intfs
    const pattern = searchText.toLowerCase()
    if (pattern.includes('*')) {
      try {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'))
        return uplotData.intfs.filter(name => regex.test(name.toLowerCase()))
      } catch {
        return uplotData.intfs.filter(name => name.toLowerCase().includes(pattern))
      }
    }
    return uplotData.intfs.filter(name => name.toLowerCase().includes(pattern))
  }, [uplotData?.intfs, searchText])

  // Sort filtered interfaces
  const sortedFilteredIntfs = useMemo(() => {
    return [...filteredIntfs].sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1
      if (sortBy === 'value') {
        const va = latestValues.get(a)
        const vb = latestValues.get(b)
        return ((va ? va.rx + va.tx : 0) - (vb ? vb.rx + vb.tx : 0)) * dir
      }
      return a.localeCompare(b) * dir
    })
  }, [filteredIntfs, sortBy, sortDir, latestValues])

  // Toggle uPlot series visibility when selection changes
  useEffect(() => {
    if (!plotRef.current || !uplotData) return
    uplotData.intfs.forEach((intf, i) => {
      const show = visibleIntfs.has(intf)
      plotRef.current!.setSeries(1 + i * 2, { show })
      plotRef.current!.setSeries(2 + i * 2, { show })
    })
  }, [visibleIntfs, uplotData])

  // Handlers
  const handleIntfClick = (intf: string, filteredIndex: number, event: React.MouseEvent) => {
    if (event.shiftKey && lastClickedIndex !== null) {
      const start = Math.min(lastClickedIndex, filteredIndex)
      const end = Math.max(lastClickedIndex, filteredIndex)
      const newSelection = new Set(selectedIntfs)
      for (let i = start; i <= end; i++) {
        newSelection.add(sortedFilteredIntfs[i])
      }
      setSelectedIntfs(newSelection)
    } else if (event.ctrlKey || event.metaKey) {
      const newSelection = new Set(selectedIntfs)
      if (newSelection.has(intf)) {
        newSelection.delete(intf)
      } else {
        newSelection.add(intf)
      }
      setSelectedIntfs(newSelection)
    } else {
      if (selectedIntfs.has(intf)) {
        const newSelection = new Set(selectedIntfs)
        newSelection.delete(intf)
        setSelectedIntfs(newSelection)
      } else {
        setSelectedIntfs(new Set([intf]))
      }
    }
    setLastClickedIndex(filteredIndex)
  }

  const handleResizeStart = (e: React.MouseEvent) => {
    e.preventDefault()
    const startY = e.clientY
    const startHeight = listHeight
    const handleMouseMove = (e: MouseEvent) => {
      const newHeight = Math.max(80, Math.min(400, startHeight + (e.clientY - startY)))
      setListHeight(newHeight)
    }
    const handleMouseUp = () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
    document.body.style.cursor = 'ns-resize'
    document.body.style.userSelect = 'none'
  }

  const handleResizeDoubleClick = () => {
    if (listHeight <= 90) {
      setListHeight(160)
    } else {
      setListHeight(80)
    }
  }

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
          <div className="relative w-full">
            <div ref={chartRef} className="w-full" />
            <span className="absolute top-1 left-[72px] text-[10px] text-muted-foreground/60 pointer-events-none">▲ Rx (in)</span>
            <span className="absolute bottom-8 left-[72px] text-[10px] text-muted-foreground/60 pointer-events-none">▼ Tx (out)</span>
          </div>
          {multiIntf ? (
            <div ref={listContainerRef} className="relative mt-2" style={{ height: `${listHeight}px` }}>
              <div className="flex flex-col h-full text-xs">
                {/* Sticky header */}
                <div className="flex-none px-2 pt-2">
                  <div className="flex items-center gap-2 mb-1.5">
                    <div className="text-xs font-medium whitespace-nowrap">
                      Interfaces ({visibleIntfs.size === 0 ? 0 : [...visibleIntfs].filter(i => uplotData.intfs.includes(i)).length}/{sortedFilteredIntfs.length})
                    </div>
                    {searchExpanded ? (
                      <div className="relative flex-1">
                        <input
                          ref={searchInputRef}
                          type="text"
                          value={searchText}
                          onChange={(e) => setSearchText(e.target.value)}
                          onBlur={() => { if (!searchText) setSearchExpanded(false) }}
                          placeholder="Filter"
                          className="w-full px-1.5 py-0.5 pr-6 text-xs bg-transparent border-b border-border focus:outline-none focus:border-foreground placeholder:text-muted-foreground/60"
                        />
                        {searchText && (
                          <button
                            onClick={() => { setSearchText(''); searchInputRef.current?.focus() }}
                            className="absolute right-1 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10"
                            aria-label="Clear search"
                          >
                            <X className="h-3 w-3" />
                          </button>
                        )}
                      </div>
                    ) : (
                      <button
                        onClick={() => { setSearchExpanded(true); setTimeout(() => searchInputRef.current?.focus(), 0) }}
                        className="text-muted-foreground hover:text-foreground"
                        aria-label="Search interfaces"
                      >
                        <Search className="h-3.5 w-3.5" />
                      </button>
                    )}
                    <button
                      onClick={() => {
                        const top10 = [...uplotData.intfs]
                          .sort((a, b) => {
                            const va = latestValues.get(a)
                            const vb = latestValues.get(b)
                            return (vb ? vb.rx + vb.tx : 0) - (va ? va.rx + va.tx : 0)
                          })
                          .slice(0, 10)
                        setSelectedIntfs(new Set(top10))
                      }}
                      className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
                    >
                      Top 10
                    </button>
                    <button
                      onClick={() => setSelectedIntfs(new Set(filteredIntfs))}
                      className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
                    >
                      All
                    </button>
                    <button
                      onClick={() => setSelectedIntfs(new Set(['__none__']))}
                      className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
                    >
                      None
                    </button>
                  </div>
                  {/* Column headers */}
                  <div className="flex items-center justify-between px-1 mb-1">
                    <button
                      onClick={() => { setSortBy('name'); setSortDir(sortBy === 'name' ? (sortDir === 'asc' ? 'desc' : 'asc') : 'asc') }}
                      className="flex items-center gap-0.5 text-xs text-muted-foreground hover:text-foreground"
                    >
                      Name
                      {sortBy === 'name' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
                    </button>
                    <button
                      onClick={() => { setSortBy('value'); setSortDir(sortBy === 'value' ? (sortDir === 'asc' ? 'desc' : 'asc') : 'desc') }}
                      className="flex items-center gap-0.5 text-xs text-muted-foreground hover:text-foreground"
                    >
                      {hoveredIdx !== null ? 'Current (Rx / Tx)' : 'Latest (Rx / Tx)'}
                      {sortBy === 'value' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
                    </button>
                  </div>
                </div>
                {/* Scrollable items */}
                <div className="flex-1 overflow-y-auto px-2 pb-2">
                  <div className="space-y-0.5">
                    {sortedFilteredIntfs.map((intf, filteredIndex) => {
                      const originalIndex = uplotData.intfs.indexOf(intf)
                      const color = seriesColors[originalIndex % seriesColors.length]
                      const isVisible = visibleIntfs.has(intf)
                      const hv = hoverValues?.get(intf)
                      const lv = latestValues.get(intf)
                      const displayVal = hv ?? lv
                      return (
                        <div
                          key={intf}
                          className={`flex items-center justify-between px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${
                            isVisible ? '' : 'opacity-40'
                          }`}
                          onClick={(e) => handleIntfClick(intf, filteredIndex, e)}
                        >
                          <div className="flex items-center gap-1.5 min-w-0">
                            <span className="w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: color }} />
                            <span className="font-mono text-foreground truncate">{intf}</span>
                          </div>
                          <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap ml-2">
                            {displayVal ? `${fmt(displayVal.rx)} / ${fmt(displayVal.tx)}` : '—'}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              </div>
              {/* Resize handle */}
              <div
                onMouseDown={handleResizeStart}
                onDoubleClick={handleResizeDoubleClick}
                className="absolute bottom-0 left-0 right-0 h-3 cursor-ns-resize hover:bg-muted transition-colors flex items-center justify-center"
              >
                <div className="w-12 h-1 bg-border rounded-full" />
              </div>
            </div>
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
