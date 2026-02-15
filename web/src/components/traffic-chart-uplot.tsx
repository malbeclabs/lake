import { useState, useMemo, memo, useRef, useEffect } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { X, Search, ChevronUp, ChevronDown } from 'lucide-react'
import type { TrafficPoint, SeriesInfo } from '@/lib/api'
import type { LinkLookupInfo } from '@/pages/traffic-page'

// Color palette matching the app
const COLORS = [
  '#ff6b35',  // accent orange
  '#5b8fd6',  // blue
  '#4ca89f',  // green
  '#9b59d0',  // purple
  '#e85988',  // pink
  '#f0ad4e',  // yellow
  '#5bc0de',  // cyan
  '#e8603c',  // red-orange
]

interface TrafficChartProps {
  title: string
  data: TrafficPoint[]
  series: SeriesInfo[]
  stacked?: boolean
  linkLookup?: Map<string, LinkLookupInfo>
}

// Format bandwidth for display
function formatBandwidth(bps: number): string {
  if (bps >= 1e9) {
    return `${(bps / 1e9).toFixed(2)} Gb/s`
  } else if (bps >= 1e6) {
    return `${(bps / 1e6).toFixed(2)} Mb/s`
  } else if (bps >= 1e3) {
    return `${(bps / 1e3).toFixed(2)} Kb/s`
  }
  return `${bps.toFixed(2)} b/s`
}

function TrafficChartImpl({ title, data, series, stacked = false, linkLookup }: TrafficChartProps) {
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const linkLookupRef = useRef(linkLookup)
  const seriesMetadataRef = useRef<Map<string, { devicePk: string; device: string; intf: string; direction: string }>>(new Map())
  const [selectedSeries, setSelectedSeries] = useState<Set<string>>(new Set())
  const [lastClickedIndex, setLastClickedIndex] = useState<number | null>(null)
  const [searchText, setSearchText] = useState('')
  const [searchExpanded, setSearchExpanded] = useState(false)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [sortBy, setSortBy] = useState<'value' | 'name'>('value')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [tooltip, setTooltip] = useState<{
    visible: boolean
    x: number
    y: number
    time: string
    label: string
    value: string
    valueBps: number
    devicePk: string
    device: string
    intf: string
    direction: string
    linkInfo?: LinkLookupInfo
  } | null>(null)
  const [isPinned, setIsPinned] = useState(false)
  const isPinnedRef = useRef(false)
  const pinnedSeriesIdxRef = useRef<number>(-1)
  const tooltipRef = useRef<HTMLDivElement>(null)
  const [listHeight, setListHeight] = useState(256) // 16rem = 256px
  const listContainerRef = useRef<HTMLDivElement>(null)

  // Get visible series (selected or all if none selected)
  const visibleSeries = useMemo(() => {
    if (selectedSeries.has('__none__')) {
      return new Set()
    }
    if (selectedSeries.size > 0) {
      return selectedSeries
    }
    return new Set(series.map(s => s.key))
  }, [selectedSeries, series])

  const visibleSeriesList = useMemo(() => {
    return series.filter(s => visibleSeries.has(s.key))
  }, [series, visibleSeries])

  // Filter series list based on search text with wildcard support
  const filteredSeries = useMemo(() => {
    if (!searchText.trim()) {
      return series
    }
    const searchPattern = searchText.toLowerCase()

    // Convert * to regex wildcard
    if (searchPattern.includes('*')) {
      try {
        const regexPattern = searchPattern.replace(/\*/g, '.*')
        const regex = new RegExp(regexPattern)
        return series.filter(s =>
          regex.test(s.key.toLowerCase()) ||
          regex.test(s.device.toLowerCase()) ||
          regex.test(s.intf.toLowerCase())
        )
      } catch {
        // If regex fails, fall back to simple includes
        return series.filter(s =>
          s.key.toLowerCase().includes(searchPattern) ||
          s.device.toLowerCase().includes(searchPattern) ||
          s.intf.toLowerCase().includes(searchPattern)
        )
      }
    }

    // Simple substring search
    return series.filter(s =>
      s.key.toLowerCase().includes(searchPattern) ||
      s.device.toLowerCase().includes(searchPattern) ||
      s.intf.toLowerCase().includes(searchPattern)
    )
  }, [series, searchText])

  // Sort filtered series
  const sortedFilteredSeries = useMemo(() => {
    return [...filteredSeries].sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1
      if (sortBy === 'value') {
        return (a.mean - b.mean) * dir
      }
      return a.key.localeCompare(b.key) * dir
    })
  }, [filteredSeries, sortBy, sortDir])

  // Build series metadata map (device_pk for each series)
  const seriesMetadata = useMemo(() => {
    const map = new Map<string, { devicePk: string; device: string; intf: string; direction: string }>()
    for (const s of series) {
      // Find a data point for this series to get device_pk
      const point = data.find(p => p.device === s.device && p.intf === s.intf)
      if (point) {
        map.set(s.key, {
          devicePk: point.device_pk,
          device: s.device,
          intf: s.intf,
          direction: s.direction,
        })
      }
    }
    return map
  }, [data, series])

  // Keep refs in sync for tooltip hook closure
  useEffect(() => {
    linkLookupRef.current = linkLookup
  }, [linkLookup])

  useEffect(() => {
    seriesMetadataRef.current = seriesMetadata
  }, [seriesMetadata])

  // Transform data for uPlot
  const { uplotData, uplotSeries } = useMemo(() => {
    if (!data.length || visibleSeriesList.length === 0) {
      return { uplotData: [[], []] as uPlot.AlignedData, uplotSeries: [] }
    }

    // Group by timestamp
    const timeMap = new Map<number, Map<string, { in: number; out: number }>>()

    for (const point of data) {
      const timestamp = new Date(point.time).getTime() / 1000 // uPlot uses seconds
      if (!timeMap.has(timestamp)) {
        timeMap.set(timestamp, new Map())
      }
      const entry = timeMap.get(timestamp)!
      const key = `${point.device}-${point.intf}`
      entry.set(key, { in: point.in_bps, out: point.out_bps })
    }

    // Sort timestamps
    const timestamps = Array.from(timeMap.keys()).sort((a, b) => a - b)

    // Build data arrays: [timestamps, series1, series2, ...]
    const dataArrays: (number | null)[][] = [timestamps]

    // Build series configurations
    const seriesConfigs: uPlot.Series[] = [
      {}, // First series is always the x-axis (time)
    ]

    // For stacked charts, add a baseline series (all zeros) to stack from
    if (stacked) {
      dataArrays.push(new Array(timestamps.length).fill(0))
      seriesConfigs.push({
        label: '__baseline__',
        show: false,  // Don't show this series
        stroke: 'transparent',
        width: 0,
      })
    }

    // Collect raw data and compute cumulative for stacking
    const rawSeriesData: (number | null)[][] = []

    // First pass: collect raw values for each series
    for (let i = 0; i < visibleSeriesList.length; i++) {
      const s = visibleSeriesList[i]
      const values: (number | null)[] = []
      const seriesKey = `${s.device}-${s.intf}`

      for (let t = 0; t < timestamps.length; t++) {
        const timestamp = timestamps[t]
        const entry = timeMap.get(timestamp)
        const data = entry?.get(seriesKey)
        const rawValue = data ? (s.direction === 'in' ? data.in : data.out) : null
        values.push(rawValue)
      }

      rawSeriesData.push(values)
    }

    // Second pass: compute cumulative values for stacking
    const cumulativeData: (number | null)[][] = []
    if (stacked) {
      for (let t = 0; t < timestamps.length; t++) {
        let cumulative = 0
        for (let i = 0; i < rawSeriesData.length; i++) {
          if (!cumulativeData[i]) {
            cumulativeData[i] = []
          }
          const val = rawSeriesData[i][t]
          if (val !== null) {
            cumulative += val
          }
          cumulativeData[i][t] = cumulative
        }
      }
    }

    // Add series to data arrays and configure
    // For stacked mode, iterate in reverse order so top bands draw first
    const iterationOrder = stacked
      ? Array.from({ length: visibleSeriesList.length }, (_, i) => visibleSeriesList.length - 1 - i)
      : Array.from({ length: visibleSeriesList.length }, (_, i) => i)

    for (const i of iterationOrder) {
      const s = visibleSeriesList[i]
      const seriesIndex = series.indexOf(s)
      const color = COLORS[seriesIndex % COLORS.length]

      if (stacked) {
        // Add cumulative data
        dataArrays.push(cumulativeData[i])

        // Configure with band to previous series (or baseline if first)
        const currentIndex = dataArrays.length - 1
        const previousIndex = i === visibleSeriesList.length - 1 ? 1 : currentIndex - 1  // 1 is baseline, or previous cumulative

        seriesConfigs.push({
          label: s.key,
          points: { show: false },
          stroke: 'transparent',  // Don't draw lines in stacked mode
          width: 0,
          fill: color + '80',  // Use more opacity for stacked areas
          band: [previousIndex, currentIndex],
          scale: 'y',
        } as uPlot.Series)
      } else {
        // Non-stacked: add raw data
        dataArrays.push(rawSeriesData[i])
        seriesConfigs.push({
          label: s.key,
          points: { show: false },
          stroke: color,
          width: 1.5,
          scale: 'y',
        })
      }
    }

    return {
      uplotData: dataArrays as uPlot.AlignedData,
      uplotSeries: seriesConfigs,
    }
  }, [data, visibleSeriesList, series, stacked])

  // Create/update chart
  useEffect(() => {
    if (!chartRef.current || uplotData[0].length === 0) return

    const axisStroke = document.documentElement.classList.contains('dark') ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)'

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 400,
      series: uplotSeries,
      scales: {
        x: {
          time: true,
        },
        y: {
          auto: true,
        },
      },
      axes: [
        {
          stroke: axisStroke,
          grid: { stroke: 'rgba(128,128,128,0.06)' },
          ticks: { stroke: 'rgba(128,128,128,0.1)' },
        },
        {
          stroke: axisStroke,
          grid: { stroke: 'rgba(128,128,128,0.06)' },
          ticks: { stroke: 'rgba(128,128,128,0.1)' },
          values: (_u, vals) => vals.map(v => formatBandwidth(v)),
          size: 80,
        },
      ],
      cursor: {
        drag: { x: false, y: false },
        focus: {
          prox: stacked ? Infinity : 30,
        },
        points: {
          size: (u: uPlot, seriesIdx: number) => {
            // Only show point on focused series in non-stacked mode
            if (stacked) return 0

            // If pinned, show point on pinned series
            if (isPinnedRef.current) {
              return seriesIdx === pinnedSeriesIdxRef.current ? 8 : 0
            }

            const series = u.series[seriesIdx] as uPlot.Series & { _focus?: boolean }
            return series._focus ? 8 : 0
          },
          width: 1.5,
        },
      },
      hooks: {
        setCursor: [
          (u) => {
            const { left, top, idx } = u.cursor

            // Find focused series in non-stacked mode
            let focusedIdx = -1
            for (let i = 1; i < u.series.length; i++) {
              const series = u.series[i] as uPlot.Series & { _focus?: boolean }
              if (series._focus) {
                focusedIdx = i
                break
              }
            }

            // If pinned, keep the pinned series focused
            if (isPinnedRef.current) {
              if (pinnedSeriesIdxRef.current > 0 && !stacked) {
                // Keep pinned series visually focused
                for (let i = 1; i < u.series.length; i++) {
                  const isPinnedSeries = i === pinnedSeriesIdxRef.current
                  u.series[i].width = isPinnedSeries ? 2.5 : 1.5
                  u.series[i].alpha = isPinnedSeries ? 1.0 : 0.3
                }
              }
              return
            }

            // Update pinned series ref when not pinned
            pinnedSeriesIdxRef.current = focusedIdx

            if (left === undefined || left < 0 || idx === undefined || idx === null) {
              setTooltip(null)
              return
            }

            if (focusedIdx > 0 && !stacked) {
              const timestamp = u.data[0][idx]
              const value = u.data[focusedIdx][idx]
              const label = u.series[focusedIdx].label
              const seriesLabel = typeof label === 'string' ? label : ''

              if (timestamp !== null && value !== null) {
                const date = new Date(timestamp * 1000)
                const timeStr = date.toLocaleString('en-US', {
                  year: 'numeric',
                  month: '2-digit',
                  day: '2-digit',
                  hour: '2-digit',
                  minute: '2-digit',
                  second: '2-digit',
                  hour12: false,
                })

                // Get series metadata
                const metadata = seriesMetadataRef.current.get(seriesLabel)
                let linkInfo: LinkLookupInfo | undefined
                if (metadata && linkLookupRef.current) {
                  const lookupKey = `${metadata.devicePk}:${metadata.intf}`
                  linkInfo = linkLookupRef.current.get(lookupKey)
                }

                const valueBps = value as number

                setTooltip({
                  visible: true,
                  x: left,
                  y: top ?? 0,
                  time: timeStr,
                  label: seriesLabel,
                  value: formatBandwidth(valueBps),
                  valueBps,
                  devicePk: metadata?.devicePk || '',
                  device: metadata?.device || '',
                  intf: metadata?.intf || '',
                  direction: metadata?.direction || '',
                  linkInfo,
                })
                return
              }
            }

            setTooltip(null)
          },
        ],
      },
      legend: {
        show: false, // We use custom legend below
      },
    }

    // Destroy existing plot
    if (plotRef.current) {
      plotRef.current.destroy()
    }

    // Create new plot
    plotRef.current = new uPlot(opts, uplotData, chartRef.current)

    // Handle resize
    const handleResize = () => {
      if (plotRef.current && chartRef.current) {
        plotRef.current.setSize({
          width: chartRef.current.offsetWidth,
          height: 400,
        })
      }
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      if (plotRef.current) {
        plotRef.current.destroy()
        plotRef.current = null
      }
    }
  }, [uplotData, uplotSeries, stacked])

  // Separate effect for handling click to pin/unpin tooltip
  useEffect(() => {
    // Keep ref in sync with state
    isPinnedRef.current = isPinned

    const handleChartClick = (e: MouseEvent) => {
      // Check if click is inside tooltip
      if (tooltipRef.current && tooltipRef.current.contains(e.target as Node)) {
        return // Clicking inside tooltip does nothing
      }

      // If already pinned, unpin on any click outside tooltip
      if (isPinned) {
        setIsPinned(false)
        isPinnedRef.current = false
        // Reset series styles when unpinning
        if (plotRef.current) {
          for (let i = 1; i < plotRef.current.series.length; i++) {
            plotRef.current.series[i].width = 1.5
            plotRef.current.series[i].alpha = 1.0
          }
          plotRef.current.redraw()
        }
        return
      }

      // Pin the tooltip if it's currently visible
      if (tooltip?.visible) {
        setIsPinned(true)
        isPinnedRef.current = true
      }
    }

    const chartElement = chartRef.current
    if (chartElement) {
      chartElement.addEventListener('click', handleChartClick)
    }

    return () => {
      if (chartElement) {
        chartElement.removeEventListener('click', handleChartClick)
      }
    }
  }, [isPinned, tooltip])

  // Handle resize
  const handleResizeStart = (e: React.MouseEvent) => {
    e.preventDefault()
    const startY = e.clientY
    const startHeight = listHeight

    const handleMouseMove = (e: MouseEvent) => {
      const deltaY = e.clientY - startY
      const newHeight = Math.max(128, Math.min(640, startHeight + deltaY)) // min 8rem, max 40rem
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

  // Handle double-click to collapse/restore
  const handleResizeDoubleClick = () => {
    const minHeight = 128
    const defaultHeight = 256
    // If currently at or near minimum, restore to default; otherwise collapse to minimum
    if (listHeight <= minHeight + 10) {
      setListHeight(defaultHeight)
    } else {
      setListHeight(minHeight)
    }
  }

  // Handle series selection
  const handleSeriesClick = (seriesKey: string, filteredIndex: number, event: React.MouseEvent) => {
    if (event.shiftKey && lastClickedIndex !== null) {
      const start = Math.min(lastClickedIndex, filteredIndex)
      const end = Math.max(lastClickedIndex, filteredIndex)
      const newSelection = new Set(selectedSeries)
      for (let i = start; i <= end; i++) {
        newSelection.add(sortedFilteredSeries[i].key)
      }
      setSelectedSeries(newSelection)
    } else if (event.ctrlKey || event.metaKey) {
      const newSelection = new Set(selectedSeries)
      if (newSelection.has(seriesKey)) {
        newSelection.delete(seriesKey)
      } else {
        newSelection.add(seriesKey)
      }
      setSelectedSeries(newSelection)
    } else {
      if (selectedSeries.has(seriesKey)) {
        // If clicking on already selected item, deselect it
        const newSelection = new Set(selectedSeries)
        newSelection.delete(seriesKey)
        setSelectedSeries(newSelection)
      } else {
        // Otherwise, select only this item
        setSelectedSeries(new Set([seriesKey]))
      }
    }
    setLastClickedIndex(filteredIndex)
  }

  if (!data.length || !series.length) {
    return (
      <div className="flex flex-col space-y-2">
        <h3 className="text-lg font-semibold">{title}</h3>
        <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
          <p className="text-muted-foreground">No data available</p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col space-y-2">
      <h3 className="text-lg font-semibold">{title}</h3>

      {/* Chart */}
      <div ref={chartRef} className="w-full relative">
        {/* Tooltip */}
        {tooltip && tooltip.visible && (
          <div
            ref={tooltipRef}
            className={`absolute bg-background border border-border rounded-md px-3 py-2 text-xs shadow-lg z-10 whitespace-nowrap ${
              isPinned ? 'pointer-events-auto cursor-text' : 'pointer-events-none'
            }`}
            style={{
              left: `${tooltip.x + 8}px`,
              bottom: `${400 - tooltip.y + 8}px`,
            }}
          >
            <div className="font-medium mb-1 text-[11px]">{tooltip.time}</div>

            {/* Device link */}
            {tooltip.devicePk && (
              <div className="mb-0.5">
                <a
                  href={`/dz/devices/${tooltip.devicePk}`}
                  className="text-blue-500 hover:text-blue-600 dark:text-blue-400 dark:hover:text-blue-300 font-medium"
                  onClick={(e) => {
                    if (!isPinned) e.preventDefault()
                  }}
                >
                  {tooltip.device}
                </a>
                <span className="text-muted-foreground ml-1">/ {tooltip.intf}</span>
              </div>
            )}

            {/* Link info */}
            {tooltip.linkInfo && (
              <div className="mb-1 text-[10px]">
                <a
                  href={`/dz/links/${tooltip.linkInfo.pk}`}
                  className="text-blue-500 hover:text-blue-600 dark:text-blue-400 dark:hover:text-blue-300"
                  onClick={(e) => {
                    if (!isPinned) e.preventDefault()
                  }}
                >
                  {tooltip.linkInfo.code}
                </a>
              </div>
            )}

            {/* Current value */}
            <div className="font-semibold mt-1 mb-0.5">
              {tooltip.direction === 'in' ? '↓' : '↑'} {tooltip.value}
            </div>

            {/* Link capacity and utilization */}
            {tooltip.linkInfo && (
              <div className="text-[10px] text-muted-foreground space-y-0.5 mt-1 pt-1 border-t border-border">
                <div>Capacity: {formatBandwidth(tooltip.linkInfo.bandwidth_bps)}</div>
                {(() => {
                  const capacity = tooltip.linkInfo.bandwidth_bps
                  let utilizationPct = 0

                  if (capacity > 0) {
                    utilizationPct = (tooltip.valueBps / capacity) * 100
                  }

                  return (
                    <div>
                      Utilization: <span className={utilizationPct > 80 ? 'text-red-500 font-medium' : ''}>{utilizationPct.toFixed(1)}%</span>
                    </div>
                  )
                })()}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Series selection list */}
      <div ref={listContainerRef} className="relative" style={{ height: `${listHeight}px` }}>
        <div className="flex flex-col h-full">
          {/* Sticky header */}
          <div className="flex-none px-3 pt-3">
            <div className="flex items-center gap-2 mb-2">
              <div className="text-sm font-medium whitespace-nowrap">
                Series ({visibleSeriesList.length}/{sortedFilteredSeries.length})
              </div>
              {/* Collapsible search */}
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
                      className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10"
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
                  aria-label="Search series"
                >
                  <Search className="h-3.5 w-3.5" />
                </button>
              )}
              <button
                onClick={() => {
                  const top10 = [...series]
                    .sort((a, b) => b.mean - a.mean)
                    .slice(0, 10)
                    .map(s => s.key)
                  setSelectedSeries(new Set(top10))
                }}
                className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
              >
                Top 10
              </button>
              <button
                onClick={() => setSelectedSeries(new Set(filteredSeries.map(s => s.key)))}
                className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
              >
                All
              </button>
              <button
                onClick={() => setSelectedSeries(new Set(['__none__']))}
                className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
              >
                None
              </button>
            </div>
            {/* Column headers */}
            <div className="flex items-center justify-between px-2 mb-1">
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
                Mean
                {sortBy === 'value' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
              </button>
            </div>
          </div>
          {/* Scrollable items */}
          <div className="flex-1 overflow-y-auto px-3 pb-3">
            <div className="space-y-1">
              {sortedFilteredSeries.map((s, filteredIndex) => {
              const originalIndex = series.indexOf(s)
              const isSelected = visibleSeries.has(s.key)
              const color = COLORS[originalIndex % COLORS.length]
              return (
                <div
                  key={s.key}
                  className={`flex items-center justify-between px-2 py-1 rounded cursor-pointer hover:bg-muted/50 transition-colors ${
                    isSelected ? '' : 'opacity-40'
                  }`}
                  onClick={(e) => handleSeriesClick(s.key, filteredIndex, e)}
                >
                  <div className="flex items-center space-x-2">
                    <div
                      className="w-3 h-3 rounded-sm"
                      style={{ backgroundColor: color }}
                    />
                    <span className="text-sm">{s.key}</span>
                  </div>
                  <span className="text-sm text-muted-foreground">
                    {formatBandwidth(s.mean)}
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
    </div>
  )
}

export const TrafficChart = memo(TrafficChartImpl)
