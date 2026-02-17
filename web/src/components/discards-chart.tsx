import { useState, useMemo, useRef, useEffect } from 'react'
import { BarChart, Bar, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid, Cell } from 'recharts'
import { X, Search, ChevronUp, ChevronDown } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import type { DiscardsPoint, DiscardSeriesInfo } from '@/lib/api'

// Calculate appropriate list height based on item count
// Header is ~48px, each item ~28px, resize handle ~12px
function calculateListHeight(itemCount: number): number {
  const headerHeight = 48
  const itemHeight = 28
  const resizeHandleHeight = 12
  const padding = 8

  const calculatedHeight = headerHeight + (itemCount * itemHeight) + resizeHandleHeight + padding

  // Minimum 96px (shows header + resize handle nicely)
  // Maximum 256px before user needs to scroll
  return Math.max(96, Math.min(256, calculatedHeight))
}

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

interface DiscardsChartProps {
  title: string
  data: DiscardsPoint[]
  series: DiscardSeriesInfo[]
  isLoading?: boolean
}

function formatCount(value: number): string {
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toString()
}

function formatTime(timeStr: string): string {
  const date = new Date(timeStr)
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

interface ChartDataPoint {
  time: string
  timeLabel: string
  [key: string]: number | string // Allow dynamic series keys
}

export function DiscardsChart({ title, data, series, isLoading }: DiscardsChartProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const [selectedSeries, setSelectedSeries] = useState<Set<string>>(new Set())
  const [lastClickedIndex, setLastClickedIndex] = useState<number | null>(null)
  const [searchText, setSearchText] = useState('')
  const [searchExpanded, setSearchExpanded] = useState(false)
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [sortBy, setSortBy] = useState<'value' | 'name'>('value')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [listHeight, setListHeight] = useState(() => calculateListHeight(series.length))
  const listContainerRef = useRef<HTMLDivElement>(null)
  const userHasResizedRef = useRef(false)
  // Capture mount time once for stable time calculations (initialized in useEffect)
  const mountTimeRef = useRef(0)

  // Initialize mount time on first render
  useEffect(() => {
    mountTimeRef.current = Date.now()
  }, [])

  // Auto-size list height based on series count (only if user hasn't manually resized)
  useEffect(() => {
    if (!userHasResizedRef.current && series.length > 0) {
      setListHeight(calculateListHeight(series.length))
    }
  }, [series.length])

  // Get visible series (selected or all if none selected)
  const visibleSeries = useMemo(() => {
    if (selectedSeries.has('__none__')) {
      return new Set<string>()
    }
    if (selectedSeries.size > 0) {
      return selectedSeries
    }
    return new Set(series.map(s => s.key))
  }, [selectedSeries, series])

  // Filter series list based on search text
  const filteredSeries = useMemo(() => {
    if (!searchText.trim()) {
      return series
    }
    const searchPattern = searchText.toLowerCase()

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
        return series.filter(s =>
          s.key.toLowerCase().includes(searchPattern) ||
          s.device.toLowerCase().includes(searchPattern) ||
          s.intf.toLowerCase().includes(searchPattern)
        )
      }
    }

    return series.filter(s =>
      s.key.toLowerCase().includes(searchPattern) ||
      s.device.toLowerCase().includes(searchPattern) ||
      s.intf.toLowerCase().includes(searchPattern)
    )
  }, [series, searchText])

  const visibleSeriesList = useMemo(() => {
    return series.filter(s => visibleSeries.has(s.key))
  }, [series, visibleSeries])

  // Sort series by total discards
  const sortedSeries = useMemo(() => {
    return [...series].sort((a, b) => b.total - a.total)
  }, [series])

  // Sort filtered series
  const sortedFilteredSeries = useMemo(() => {
    return [...filteredSeries].sort((a, b) => {
      const dir = sortDir === 'asc' ? 1 : -1
      if (sortBy === 'value') {
        return (a.total - b.total) * dir
      }
      return a.key.localeCompare(b.key) * dir
    })
  }, [filteredSeries, sortBy, sortDir])

  // Transform data for Recharts - aggregate by time bucket with series as keys
  // Always show at least 24 time columns for consistent display
  const chartData = useMemo(() => {
    if (visibleSeriesList.length === 0) return []

    // Group by timestamp
    const timeMap = new Map<string, ChartDataPoint>()

    for (const point of data) {
      const baseKey = `${point.device}-${point.intf}`
      const inKey = `${baseKey} (In)`
      const outKey = `${baseKey} (Out)`

      // Check if either in or out series is visible
      const inVisible = visibleSeries.has(inKey)
      const outVisible = visibleSeries.has(outKey)
      if (!inVisible && !outVisible) continue

      if (!timeMap.has(point.time)) {
        timeMap.set(point.time, {
          time: point.time,
          timeLabel: formatTime(point.time),
        })
      }
      const entry = timeMap.get(point.time)!

      // Add in discards if visible
      if (inVisible && point.in_discards > 0) {
        entry[inKey] = (entry[inKey] as number || 0) + point.in_discards
      }

      // Add out discards if visible
      if (outVisible && point.out_discards > 0) {
        entry[outKey] = (entry[outKey] as number || 0) + point.out_discards
      }
    }

    // Sort existing data by time
    const sortedData = Array.from(timeMap.values()).sort((a, b) => a.time.localeCompare(b.time))

    // Ensure at least 24 columns by filling in empty time slots
    const minColumns = 24
    if (sortedData.length >= minColumns) {
      return sortedData
    }

    // Default bucket interval: 15 minutes
    let bucketMs = 15 * 60 * 1000

    // If we have 2+ data points, infer bucket interval
    if (sortedData.length >= 2) {
      const firstTime = new Date(sortedData[0].time).getTime()
      const secondTime = new Date(sortedData[1].time).getTime()
      bucketMs = secondTime - firstTime
    }

    // Determine the time range to fill
    // If we have data, center/extend around it; otherwise use mount time
    let endTime: number
    if (sortedData.length > 0) {
      endTime = new Date(sortedData[sortedData.length - 1].time).getTime()
    } else {
      endTime = mountTimeRef.current
    }

    // Generate empty columns, placing actual data at correct positions
    const columnsNeeded = minColumns
    const startTime = endTime - (columnsNeeded - 1) * bucketMs
    const result: ChartDataPoint[] = []

    // Create a map of actual data by rounded time for quick lookup
    const dataByTime = new Map<number, ChartDataPoint>()
    for (const d of sortedData) {
      const t = new Date(d.time).getTime()
      dataByTime.set(t, d)
    }

    for (let i = 0; i < columnsNeeded; i++) {
      const time = startTime + i * bucketMs
      // Check if we have data for this bucket (with some tolerance for rounding)
      let found: ChartDataPoint | undefined
      for (const [t, d] of dataByTime) {
        if (Math.abs(t - time) < bucketMs / 2) {
          found = d
          dataByTime.delete(t) // Remove so we don't match again
          break
        }
      }

      if (found) {
        result.push(found)
      } else {
        const timeStr = new Date(time).toISOString()
        result.push({
          time: timeStr,
          timeLabel: formatTime(timeStr),
        })
      }
    }

    // If there's remaining data that didn't fit in the grid, append it
    for (const d of dataByTime.values()) {
      result.push(d)
    }

    return result.sort((a, b) => a.time.localeCompare(b.time))
  }, [data, visibleSeries, visibleSeriesList])

  // Check if there's any data to show
  const hasAnyDiscards = useMemo(() => {
    return chartData.some(d => {
      for (const s of visibleSeriesList) {
        if ((d[s.key] as number) > 0) return true
      }
      return false
    })
  }, [chartData, visibleSeriesList])

  // Handle resize
  const handleResizeStart = (e: React.MouseEvent) => {
    e.preventDefault()
    const startY = e.clientY
    const startHeight = listHeight

    const handleMouseMove = (e: MouseEvent) => {
      const deltaY = e.clientY - startY
      const newHeight = Math.max(96, Math.min(640, startHeight + deltaY))
      setListHeight(newHeight)
      userHasResizedRef.current = true // Mark that user has manually resized
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
    const minHeight = 96
    const defaultHeight = calculateListHeight(series.length)
    if (listHeight <= minHeight + 10) {
      setListHeight(defaultHeight)
    } else {
      setListHeight(minHeight)
    }
    userHasResizedRef.current = true
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
        const newSelection = new Set(selectedSeries)
        newSelection.delete(seriesKey)
        setSelectedSeries(newSelection)
      } else {
        setSelectedSeries(new Set([seriesKey]))
      }
    }
    setLastClickedIndex(filteredIndex)
  }

  const axisColor = isDark ? '#94a3b8' : '#64748b'
  const gridColor = isDark ? '#334155' : '#e2e8f0'

  // Render the series list (shared between multiple render paths)
  const renderSeriesList = () => (
    <div ref={listContainerRef} className="relative" style={{ height: `${listHeight}px` }}>
      <div className="flex flex-col h-full">
        {/* Sticky header */}
        <div className="flex-none px-3 pt-3">
          <div className="flex items-center gap-2 mb-2">
            <div className="text-sm font-medium whitespace-nowrap">
              Interfaces ({visibleSeriesList.length}/{sortedFilteredSeries.length})
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
                const top10 = sortedSeries.slice(0, 10).map(s => s.key)
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
              Total
              {sortBy === 'value' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
            </button>
          </div>
        </div>
        {/* Scrollable items */}
        <div className="flex-1 overflow-y-auto px-3 pb-3">
          <div className="space-y-1">
            {sortedFilteredSeries.map((s, filteredIndex) => {
              const originalIndex = sortedSeries.indexOf(s)
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
                    {formatCount(s.total)}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      </div>
      <div
        onMouseDown={handleResizeStart}
        onDoubleClick={handleResizeDoubleClick}
        className="absolute bottom-0 left-0 right-0 h-3 cursor-ns-resize hover:bg-accent/50 transition-colors flex items-center justify-center"
      >
        <div className="w-12 h-1 bg-border rounded-full" />
      </div>
    </div>
  )

  // Show loading state
  if (isLoading) {
    return (
      <div className="flex flex-col space-y-2">
        <h3 className="text-lg font-semibold">{title}</h3>
        <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
          <div className="animate-pulse text-muted-foreground">Loading discards data...</div>
        </div>
      </div>
    )
  }

  if (!data.length || !series.length) {
    return (
      <div className="flex flex-col space-y-2">
        <h3 className="text-lg font-semibold">{title}</h3>
        <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
          <p className="text-muted-foreground">No discard data available</p>
        </div>
      </div>
    )
  }

  if (!hasAnyDiscards) {
    return (
      <div className="flex flex-col space-y-2">
        <h3 className="text-lg font-semibold">{title}</h3>
        <div className="border border-border rounded-lg p-8 flex items-center justify-center h-[400px]">
          <p className="text-green-600 dark:text-green-400">No discards in the selected time range</p>
        </div>
        {/* Still show series list for filtering */}
        {renderSeriesList()}
      </div>
    )
  }

  return (
    <div className="flex flex-col space-y-2">
      <h3 className="text-lg font-semibold">{title}</h3>

      {/* Chart */}
      <div className="h-[400px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} barCategoryGap="40%">
            <CartesianGrid strokeDasharray="3 3" stroke={gridColor} opacity={0.5} />
            <XAxis
              dataKey="timeLabel"
              tick={{ fontSize: 9, fill: axisColor }}
              tickLine={false}
              axisLine={false}
              interval={Math.max(0, Math.floor(chartData.length / 12) - 1)}
            />
            <YAxis
              tick={{ fontSize: 10, fill: axisColor }}
              tickLine={false}
              axisLine={false}
              tickFormatter={formatCount}
              width={50}
            />
            <RechartsTooltip
              contentStyle={{
                backgroundColor: 'var(--card)',
                border: '1px solid var(--border)',
                borderRadius: '6px',
                fontSize: '11px',
              }}
              formatter={(value, name) => {
                if (value === 0 || value === undefined) return null
                return [formatCount(value as number), name]
              }}
              labelFormatter={(label: string) => `Time: ${label}`}
            />
            {visibleSeriesList.map((s) => {
              const originalIndex = sortedSeries.indexOf(s)
              const color = COLORS[originalIndex % COLORS.length]
              return (
                <Bar
                  key={s.key}
                  dataKey={s.key}
                  fill={color}
                  radius={[2, 2, 0, 0]}
                  name={s.key}
                  maxBarSize={20}
                >
                  {chartData.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={color} />
                  ))}
                </Bar>
              )
            })}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Series selection list */}
      {renderSeriesList()}
    </div>
  )
}
