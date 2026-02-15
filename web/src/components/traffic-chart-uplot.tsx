import { useState, useMemo, memo, useRef, useEffect, useCallback } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { X, Search, ChevronUp, ChevronDown } from 'lucide-react'
import type { TrafficPoint, SeriesInfo } from '@/lib/api'
import type { LinkLookupInfo } from '@/pages/traffic-page'
import { useTheme } from '@/hooks/use-theme'

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
  bidirectional?: boolean
  onTimeRangeSelect?: (startSec: number, endSec: number) => void
  metric?: 'throughput' | 'packets' | 'utilization'
}

// Represents one interface with paired in/out in bidirectional mode
interface InterfaceGroup {
  intfKey: string   // "device-intf"
  device: string
  intf: string
  inSeries: SeriesInfo
  outSeries: SeriesInfo
  colorIndex: number
}

// Format bandwidth for display
function formatBandwidth(bps: number): string {
  if (bps >= 1e12) return (bps / 1e12).toFixed(1) + ' Tbps'
  if (bps >= 1e9) return (bps / 1e9).toFixed(1) + ' Gbps'
  if (bps >= 1e6) return (bps / 1e6).toFixed(1) + ' Mbps'
  if (bps >= 1e3) return (bps / 1e3).toFixed(1) + ' Kbps'
  return bps.toFixed(0) + ' bps'
}

function formatPps(pps: number): string {
  if (pps >= 1e12) return (pps / 1e12).toFixed(1) + ' Tpps'
  if (pps >= 1e9) return (pps / 1e9).toFixed(1) + ' Gpps'
  if (pps >= 1e6) return (pps / 1e6).toFixed(1) + ' Mpps'
  if (pps >= 1e3) return (pps / 1e3).toFixed(1) + ' Kpps'
  return pps.toFixed(0) + ' pps'
}

function TrafficChartImpl({ title, data, series, stacked = false, linkLookup, bidirectional = false, onTimeRangeSelect, metric = 'throughput' }: TrafficChartProps) {
  const { resolvedTheme } = useTheme()
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const linkLookupRef = useRef(linkLookup)
  const onTimeRangeSelectRef = useRef(onTimeRangeSelect)
  onTimeRangeSelectRef.current = onTimeRangeSelect
  const fmtValue = metric === 'packets' ? formatPps : formatBandwidth
  const fmtValueRef = useRef(fmtValue)
  fmtValueRef.current = fmtValue
  const seriesMetadataRef = useRef<Map<string, { devicePk: string; device: string; intf: string; direction: string }>>(new Map())
  const [selectedSeries, setSelectedSeries] = useState<Set<string>>(new Set())
  const [lastClickedIndex, setLastClickedIndex] = useState<number | null>(null)
  const [searchText, setSearchText] = useState('')
  const [searchExpanded, setSearchExpanded] = useState(false)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [sortBy, setSortBy] = useState<'value' | 'name'>('value')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const hoveredIdxRef = useRef<number | null>(null)
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

  // In bidirectional mode, group series by interface (pair in/out)
  const interfaceGroups = useMemo((): InterfaceGroup[] => {
    if (!bidirectional) return []
    const groupMap = new Map<string, { in?: SeriesInfo; out?: SeriesInfo }>()
    for (const s of visibleSeriesList) {
      const intfKey = `${s.device}-${s.intf}`
      if (!groupMap.has(intfKey)) groupMap.set(intfKey, {})
      const g = groupMap.get(intfKey)!
      if (s.direction === 'in') g.in = s
      else g.out = s
    }
    const groups: InterfaceGroup[] = []
    for (const [intfKey, g] of groupMap) {
      if (!g.in || !g.out) continue
      // Use the in series' original index for color assignment
      const colorIndex = series.indexOf(g.in)
      groups.push({
        intfKey,
        device: g.in.device,
        intf: g.in.intf,
        inSeries: g.in,
        outSeries: g.out,
        colorIndex,
      })
    }
    return groups
  }, [bidirectional, visibleSeriesList, series])

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

    // --- Bidirectional mode ---
    if (bidirectional && interfaceGroups.length > 0) {
      const dataArrays: (number | null)[][] = [timestamps]
      const seriesConfigs: uPlot.Series[] = [{}]

      if (stacked) {
        // Stacked bidirectional: stack Rx upward, Tx downward
        // Rx baseline
        dataArrays.push(new Array(timestamps.length).fill(0))
        seriesConfigs.push({ label: '__rx_baseline__', show: false, stroke: 'transparent', width: 0 })

        // Collect raw Rx/Tx data per group
        const rawRx: (number | null)[][] = []
        const rawTx: (number | null)[][] = []
        for (const g of interfaceGroups) {
          const rxVals: (number | null)[] = []
          const txVals: (number | null)[] = []
          for (let t = 0; t < timestamps.length; t++) {
            const entry = timeMap.get(timestamps[t])?.get(g.intfKey)
            rxVals.push(entry ? entry.in : null)
            txVals.push(entry ? -entry.out : null)
          }
          rawRx.push(rxVals)
          rawTx.push(txVals)
        }

        // Compute cumulative Rx (positive direction)
        const cumulativeRx: (number | null)[][] = []
        for (let t = 0; t < timestamps.length; t++) {
          let cum = 0
          for (let i = 0; i < rawRx.length; i++) {
            if (!cumulativeRx[i]) cumulativeRx[i] = []
            const v = rawRx[i][t]
            if (v !== null) cum += v
            cumulativeRx[i][t] = cum
          }
        }

        // Rx series (reversed for band rendering)
        for (let i = interfaceGroups.length - 1; i >= 0; i--) {
          const g = interfaceGroups[i]
          const color = COLORS[g.colorIndex % COLORS.length]
          dataArrays.push(cumulativeRx[i])
          const currentIdx = dataArrays.length - 1
          const prevIdx = i === interfaceGroups.length - 1 ? 1 : currentIdx - 1
          seriesConfigs.push({
            label: `${g.intfKey} Rx`,
            points: { show: false },
            stroke: 'transparent',
            width: 0,
            fill: color + '80',
            band: [prevIdx, currentIdx],
            scale: 'y',
          } as uPlot.Series)
        }

        // Tx baseline
        dataArrays.push(new Array(timestamps.length).fill(0))
        seriesConfigs.push({ label: '__tx_baseline__', show: false, stroke: 'transparent', width: 0 })
        const txBaselineIdx = dataArrays.length - 1

        // Compute cumulative Tx (negative direction)
        const cumulativeTx: (number | null)[][] = []
        for (let t = 0; t < timestamps.length; t++) {
          let cum = 0
          for (let i = 0; i < rawTx.length; i++) {
            if (!cumulativeTx[i]) cumulativeTx[i] = []
            const v = rawTx[i][t]
            if (v !== null) cum += v
            cumulativeTx[i][t] = cum
          }
        }

        // Tx series (reversed for band rendering)
        for (let i = interfaceGroups.length - 1; i >= 0; i--) {
          const g = interfaceGroups[i]
          const color = COLORS[g.colorIndex % COLORS.length]
          dataArrays.push(cumulativeTx[i])
          const currentIdx = dataArrays.length - 1
          const prevIdx = i === interfaceGroups.length - 1 ? txBaselineIdx : currentIdx - 1
          seriesConfigs.push({
            label: `${g.intfKey} Tx`,
            points: { show: false },
            stroke: 'transparent',
            width: 0,
            fill: color + '40',
            band: [prevIdx, currentIdx],
            scale: 'y',
          } as uPlot.Series)
        }
      } else {
        // Non-stacked bidirectional: Rx solid positive, Tx dashed negative
        for (const g of interfaceGroups) {
          const color = COLORS[g.colorIndex % COLORS.length]
          const rxVals: (number | null)[] = []
          const txVals: (number | null)[] = []
          for (let t = 0; t < timestamps.length; t++) {
            const entry = timeMap.get(timestamps[t])?.get(g.intfKey)
            rxVals.push(entry ? entry.in : null)
            txVals.push(entry ? -entry.out : null)
          }
          dataArrays.push(rxVals)
          seriesConfigs.push({
            label: `${g.intfKey} Rx`,
            points: { show: false },
            stroke: color,
            width: 1.5,
            scale: 'y',
          })
          dataArrays.push(txVals)
          seriesConfigs.push({
            label: `${g.intfKey} Tx`,
            points: { show: false },
            stroke: color,
            width: 1.5,
            dash: [4, 2],
            scale: 'y',
          })
        }
      }

      return {
        uplotData: dataArrays as uPlot.AlignedData,
        uplotSeries: seriesConfigs,
      }
    }

    // --- Normal (non-bidirectional) mode ---
    const dataArrays: (number | null)[][] = [timestamps]
    const seriesConfigs: uPlot.Series[] = [{}]

    // For stacked charts, add a baseline series (all zeros) to stack from
    if (stacked) {
      dataArrays.push(new Array(timestamps.length).fill(0))
      seriesConfigs.push({
        label: '__baseline__',
        show: false,
        stroke: 'transparent',
        width: 0,
      })
    }

    // Collect raw data and compute cumulative for stacking
    const rawSeriesData: (number | null)[][] = []

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

    // Compute cumulative values for stacking
    const cumulativeData: (number | null)[][] = []
    if (stacked) {
      for (let t = 0; t < timestamps.length; t++) {
        let cumulative = 0
        for (let i = 0; i < rawSeriesData.length; i++) {
          if (!cumulativeData[i]) cumulativeData[i] = []
          const val = rawSeriesData[i][t]
          if (val !== null) cumulative += val
          cumulativeData[i][t] = cumulative
        }
      }
    }

    // For stacked mode, iterate in reverse order so top bands draw first
    const iterationOrder = stacked
      ? Array.from({ length: visibleSeriesList.length }, (_, i) => visibleSeriesList.length - 1 - i)
      : Array.from({ length: visibleSeriesList.length }, (_, i) => i)

    for (const i of iterationOrder) {
      const s = visibleSeriesList[i]
      const seriesIndex = series.indexOf(s)
      const color = COLORS[seriesIndex % COLORS.length]

      if (stacked) {
        dataArrays.push(cumulativeData[i])
        const currentIndex = dataArrays.length - 1
        const previousIndex = i === visibleSeriesList.length - 1 ? 1 : currentIndex - 1

        seriesConfigs.push({
          label: s.key,
          points: { show: false },
          stroke: 'transparent',
          width: 0,
          fill: color + '80',
          band: [previousIndex, currentIndex],
          scale: 'y',
        } as uPlot.Series)
      } else {
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
  }, [data, visibleSeriesList, series, stacked, bidirectional, interfaceGroups])

  // Create/update chart
  useEffect(() => {
    if (!chartRef.current || uplotData[0].length === 0) return

    const axisStroke = resolvedTheme === 'dark' ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)'

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
          values: (_u, vals) => vals.map(v => fmtValueRef.current(bidirectional ? Math.abs(v) : v)),
          size: 90,
        },
      ],
      cursor: {
        drag: { x: true, y: false, setScale: false },
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

            // Track hover index for legend values
            hoveredIdxRef.current = idx ?? null
            setHoveredIdx(idx ?? null)

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

                // Get series metadata — strip " Rx"/" Tx" suffix for bidirectional labels
                const metadataKey = seriesLabel.replace(/ (Rx|Tx)$/, '')
                const metadata = seriesMetadataRef.current.get(metadataKey) || seriesMetadataRef.current.get(seriesLabel)
                let linkInfo: LinkLookupInfo | undefined
                if (metadata && linkLookupRef.current) {
                  const lookupKey = `${metadata.devicePk}:${metadata.intf}`
                  linkInfo = linkLookupRef.current.get(lookupKey)
                }

                const valueBps = Math.abs(value as number)

                setTooltip({
                  visible: true,
                  x: left,
                  y: top ?? 0,
                  time: timeStr,
                  label: seriesLabel,
                  value: fmtValueRef.current(valueBps),
                  valueBps,
                  devicePk: metadata?.devicePk || '',
                  device: metadata?.device || '',
                  intf: metadata?.intf || '',
                  direction: seriesLabel.endsWith(' Rx') ? 'in' : seriesLabel.endsWith(' Tx') ? 'out' : (metadata?.direction || ''),
                  linkInfo,
                })
                return
              }
            }

            setTooltip(null)
          },
        ],
        setSelect: [
          (u) => {
            const min = u.posToVal(u.select.left, 'x')
            const max = u.posToVal(u.select.left + u.select.width, 'x')
            if (max - min >= 1 && onTimeRangeSelectRef.current) {
              onTimeRangeSelectRef.current(Math.floor(min), Math.floor(max))
            }
            u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false)
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

    // Handle resize via ResizeObserver so charts resize when container changes
    // (e.g. switching between 1-column and 2-column layout)
    const container = chartRef.current
    const resizeObserver = new ResizeObserver(entries => {
      const width = entries[0]?.contentRect.width
      if (width && plotRef.current) {
        plotRef.current.setSize({ width, height: 400 })
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
  }, [uplotData, uplotSeries, stacked, bidirectional, resolvedTheme])

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

  // Compute latest values (last non-null per series/interface)
  const latestValues = useMemo(() => {
    if (bidirectional && interfaceGroups.length > 0) {
      // Bidirectional: compute Rx/Tx per interface group
      const m = new Map<string, { rx: number; tx: number }>()
      for (let gi = 0; gi < interfaceGroups.length; gi++) {
        const g = interfaceGroups[gi]
        // Find the Rx and Tx data array indices in uplotData
        // In non-stacked: pairs at 1+gi*2, 2+gi*2
        // In stacked: more complex layout, search by label
        let rxArr: (number | null | undefined)[] | undefined
        let txArr: (number | null | undefined)[] | undefined
        for (let si = 1; si < uplotSeries.length; si++) {
          if (uplotSeries[si].label === `${g.intfKey} Rx`) rxArr = uplotData[si] as (number | null)[]
          if (uplotSeries[si].label === `${g.intfKey} Tx`) txArr = uplotData[si] as (number | null)[]
        }
        let rx = 0, tx = 0
        if (rxArr) {
          for (let j = rxArr.length - 1; j >= 0; j--) {
            if (rxArr[j] != null) { rx = rxArr[j] as number; break }
          }
        }
        if (txArr) {
          for (let j = txArr.length - 1; j >= 0; j--) {
            if (txArr[j] != null) { tx = Math.abs(txArr[j] as number); break }
          }
        }
        m.set(g.intfKey, { rx, tx })
      }
      return m
    }
    // Non-bidirectional: single value per series
    const m = new Map<string, number>()
    for (let si = 1; si < uplotSeries.length; si++) {
      const label = uplotSeries[si].label
      if (typeof label !== 'string' || label.startsWith('__')) continue
      const arr = uplotData[si] as (number | null)[]
      if (!arr) continue
      for (let j = arr.length - 1; j >= 0; j--) {
        if (arr[j] != null) { m.set(label, arr[j] as number); break }
      }
    }
    return m
  }, [uplotData, uplotSeries, bidirectional, interfaceGroups])

  // Compute hover values (at cursor position)
  const hoverValues = useMemo(() => {
    if (hoveredIdx === null) return null
    if (bidirectional && interfaceGroups.length > 0) {
      const m = new Map<string, { rx: number; tx: number }>()
      for (const g of interfaceGroups) {
        let rx = 0, tx = 0
        for (let si = 1; si < uplotSeries.length; si++) {
          const label = uplotSeries[si].label
          if (typeof label !== 'string') continue
          const arr = uplotData[si] as (number | null)[]
          if (!arr) continue
          const v = arr[hoveredIdx]
          if (label === `${g.intfKey} Rx` && v != null) rx = v as number
          if (label === `${g.intfKey} Tx` && v != null) tx = Math.abs(v as number)
        }
        m.set(g.intfKey, { rx, tx })
      }
      return m
    }
    // Non-bidirectional
    const m = new Map<string, number>()
    for (let si = 1; si < uplotSeries.length; si++) {
      const label = uplotSeries[si].label
      if (typeof label !== 'string' || label.startsWith('__')) continue
      const arr = uplotData[si] as (number | null)[]
      if (!arr) continue
      const v = arr[hoveredIdx]
      if (v != null) m.set(label, v as number)
    }
    return m
  }, [hoveredIdx, uplotData, uplotSeries, bidirectional, interfaceGroups])

  // In bidirectional mode, filter/sort interface groups for legend
  const filteredInterfaceGroups = useMemo(() => {
    if (!bidirectional) return []
    if (!searchText.trim()) return interfaceGroups
    const pattern = searchText.toLowerCase()
    if (pattern.includes('*')) {
      try {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'))
        return interfaceGroups.filter(g =>
          regex.test(g.intfKey.toLowerCase()) ||
          regex.test(g.device.toLowerCase()) ||
          regex.test(g.intf.toLowerCase())
        )
      } catch {
        return interfaceGroups.filter(g =>
          g.intfKey.toLowerCase().includes(pattern) ||
          g.device.toLowerCase().includes(pattern) ||
          g.intf.toLowerCase().includes(pattern)
        )
      }
    }
    return interfaceGroups.filter(g =>
      g.intfKey.toLowerCase().includes(pattern) ||
      g.device.toLowerCase().includes(pattern) ||
      g.intf.toLowerCase().includes(pattern)
    )
  }, [bidirectional, interfaceGroups, searchText])

  const sortedInterfaceGroups = useMemo(() => {
    if (!bidirectional) return []
    const latest = latestValues as Map<string, { rx: number; tx: number }>
    return [...filteredInterfaceGroups].sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1
      if (sortBy === 'value') {
        const va = latest.get(a.intfKey)
        const vb = latest.get(b.intfKey)
        return ((va ? va.rx + va.tx : 0) - (vb ? vb.rx + vb.tx : 0)) * dir
      }
      return a.intfKey.localeCompare(b.intfKey) * dir
    })
  }, [bidirectional, filteredInterfaceGroups, sortBy, sortDir, latestValues])

  // Handle series selection — bidirectional mode uses intfKey
  const handleBidirectionalClick = useCallback((intfKey: string, filteredIndex: number, event: React.MouseEvent) => {
    // In bidirectional mode, selectedSeries stores intfKey (not individual in/out keys)
    // We need to map to the actual series keys for visibility
    const group = interfaceGroups.find(g => g.intfKey === intfKey)
    if (!group) return

    const inKey = group.inSeries.key
    const outKey = group.outSeries.key

    if (event.shiftKey && lastClickedIndex !== null) {
      const start = Math.min(lastClickedIndex, filteredIndex)
      const end = Math.max(lastClickedIndex, filteredIndex)
      const newSelection = new Set(selectedSeries)
      for (let i = start; i <= end; i++) {
        const g = sortedInterfaceGroups[i]
        if (g) {
          newSelection.add(g.inSeries.key)
          newSelection.add(g.outSeries.key)
        }
      }
      setSelectedSeries(newSelection)
    } else if (event.ctrlKey || event.metaKey) {
      const newSelection = new Set(selectedSeries)
      if (newSelection.has(inKey)) {
        newSelection.delete(inKey)
        newSelection.delete(outKey)
      } else {
        newSelection.add(inKey)
        newSelection.add(outKey)
      }
      setSelectedSeries(newSelection)
    } else {
      if (selectedSeries.has(inKey)) {
        const newSelection = new Set(selectedSeries)
        newSelection.delete(inKey)
        newSelection.delete(outKey)
        setSelectedSeries(newSelection)
      } else {
        setSelectedSeries(new Set([inKey, outKey]))
      }
    }
    setLastClickedIndex(filteredIndex)
  }, [interfaceGroups, selectedSeries, lastClickedIndex, sortedInterfaceGroups])

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

  // Helper to get legend display value for non-bidirectional series
  const getLegendValue = (s: SeriesInfo): string => {
    if (hoveredIdx !== null && hoverValues) {
      const hv = (hoverValues as Map<string, number>).get(s.key)
      if (hv != null) return fmtValue(hv)
    }
    const lv = (latestValues as Map<string, number>).get(s.key)
    if (lv != null) return fmtValue(lv)
    return fmtValue(s.mean)
  }

  // Value column header
  const valueColumnHeader = bidirectional
    ? (hoveredIdx !== null ? 'Current (Rx / Tx)' : 'Latest (Rx / Tx)')
    : (hoveredIdx !== null ? 'Current' : 'Latest')

  return (
    <div className="flex flex-col space-y-2">
      <h3 className="text-lg font-semibold">{title}</h3>

      {/* Chart */}
      <div className="relative w-full">
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
        {/* Direction labels for bidirectional mode */}
        {bidirectional && (
          <>
            <span className="absolute top-1 left-[72px] text-[10px] text-muted-foreground/60 pointer-events-none">▲ Rx (in)</span>
            <span className="absolute bottom-8 left-[72px] text-[10px] text-muted-foreground/60 pointer-events-none">▼ Tx (out)</span>
          </>
        )}
      </div>

      {/* Series selection list */}
      <div ref={listContainerRef} className="relative" style={{ height: `${listHeight}px` }}>
        <div className="flex flex-col h-full text-xs">
          {/* Sticky header */}
          <div className="flex-none px-2 pt-2">
            <div className="flex items-center gap-2 mb-1.5">
              <div className="text-xs font-medium whitespace-nowrap">
                {bidirectional
                  ? `Interfaces (${sortedInterfaceGroups.filter(g => visibleSeries.has(g.inSeries.key)).length}/${sortedInterfaceGroups.length})`
                  : `Series (${visibleSeriesList.length}/${sortedFilteredSeries.length})`
                }
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
                  if (bidirectional) {
                    const latest = latestValues as Map<string, { rx: number; tx: number }>
                    const top10 = [...interfaceGroups]
                      .sort((a, b) => {
                        const va = latest.get(a.intfKey)
                        const vb = latest.get(b.intfKey)
                        return (vb ? vb.rx + vb.tx : 0) - (va ? va.rx + va.tx : 0)
                      })
                      .slice(0, 10)
                    const keys = new Set<string>()
                    for (const g of top10) {
                      keys.add(g.inSeries.key)
                      keys.add(g.outSeries.key)
                    }
                    setSelectedSeries(keys)
                  } else {
                    const top10 = [...series]
                      .sort((a, b) => b.mean - a.mean)
                      .slice(0, 10)
                      .map(s => s.key)
                    setSelectedSeries(new Set(top10))
                  }
                }}
                className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
              >
                Top 10
              </button>
              <button
                onClick={() => {
                  if (bidirectional) {
                    const keys = new Set<string>()
                    for (const g of filteredInterfaceGroups) {
                      keys.add(g.inSeries.key)
                      keys.add(g.outSeries.key)
                    }
                    setSelectedSeries(keys)
                  } else {
                    setSelectedSeries(new Set(filteredSeries.map(s => s.key)))
                  }
                }}
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
                {valueColumnHeader}
                {sortBy === 'value' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
              </button>
            </div>
          </div>
          {/* Scrollable items */}
          <div className="flex-1 overflow-y-auto px-2 pb-2">
            <div className="space-y-0.5">
              {bidirectional ? (
                // Bidirectional: one row per interface with Rx/Tx values
                sortedInterfaceGroups.map((g, filteredIndex) => {
                  const color = COLORS[g.colorIndex % COLORS.length]
                  const isVisible = visibleSeries.has(g.inSeries.key)
                  const hv = (hoverValues as Map<string, { rx: number; tx: number }> | null)?.get(g.intfKey)
                  const lv = (latestValues as Map<string, { rx: number; tx: number }>).get(g.intfKey)
                  const displayVal = hv ?? lv
                  return (
                    <div
                      key={g.intfKey}
                      className={`flex items-center justify-between px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${
                        isVisible ? '' : 'opacity-40'
                      }`}
                      onClick={(e) => handleBidirectionalClick(g.intfKey, filteredIndex, e)}
                    >
                      <div className="flex items-center gap-1.5 min-w-0">
                        <div
                          className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                          style={{ backgroundColor: color }}
                        />
                        <span className="text-xs truncate">{g.intfKey}</span>
                      </div>
                      <span className="text-xs text-muted-foreground font-mono tabular-nums whitespace-nowrap ml-2">
                        {displayVal ? `${fmtValue(displayVal.rx)} / ${fmtValue(displayVal.tx)}` : '—'}
                      </span>
                    </div>
                  )
                })
              ) : (
                // Normal: one row per series
                sortedFilteredSeries.map((s, filteredIndex) => {
                  const originalIndex = series.indexOf(s)
                  const isSelected = visibleSeries.has(s.key)
                  const color = COLORS[originalIndex % COLORS.length]
                  return (
                    <div
                      key={s.key}
                      className={`flex items-center justify-between px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${
                        isSelected ? '' : 'opacity-40'
                      }`}
                      onClick={(e) => handleSeriesClick(s.key, filteredIndex, e)}
                    >
                      <div className="flex items-center gap-1.5 min-w-0">
                        <div
                          className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                          style={{ backgroundColor: color }}
                        />
                        <span className="text-xs truncate">{s.key}</span>
                      </div>
                      <span className="text-xs text-muted-foreground font-mono tabular-nums whitespace-nowrap ml-2">
                        {getLegendValue(s)}
                      </span>
                    </div>
                  )
                })
              )}
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
