import { useMemo, useRef, useState, useCallback, useEffect } from 'react'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { Loader2, RefreshCw, Search, X } from 'lucide-react'
import uPlot from 'uplot'
import { useTheme } from '@/hooks/use-theme'
import { useChartLegend } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { type ChartLegendSeries } from '@/components/topology/ChartLegend'
import { ChartLegendTable } from '@/components/topology/ChartLegendTable'
import { formatHoveredTime } from '@/components/topology/utils'
import { fetchMultiLinkLatencyHistory } from '@/lib/api'
import { ChevronDown } from 'lucide-react'

import { getSeriesColors } from '@/components/chart-colors'

type ChartMode = 'aggregate' | 'per_link'
type Direction = 'combined' | 'a_to_z' | 'z_to_a'

function RefreshButton({ fetching, onClick }: { fetching: boolean; onClick: () => void }) {
  if (fetching) return <Loader2 className="h-3 w-3 animate-spin" />
  return (
    <button onClick={onClick} className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground" title="Refresh">
      <RefreshCw className="h-3 w-3" />
    </button>
  )
}

interface MultiLinkLatencyChartsProps {
  pks?: string[]
  linkNames?: Map<string, string>
  /** Number of explicitly selected links (drives auto aggregate/per-link) */
  selectedCount: number
  timeRange: string
  agg: string
  filters?: Record<string, string>
  className?: string
}

export function MultiLinkLatencyCharts({
  pks,
  linkNames,
  selectedCount,
  timeRange,
  agg,
  filters,
  className,
}: MultiLinkLatencyChartsProps) {
  const queryClient = useQueryClient()
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const colors = getSeriesColors(isDark)

  const PER_LINK_MAX = 10
  const sortedPks = useMemo(() => pks ? [...pks].sort() : [], [pks])

  // Default to per_link when a reasonable number of links are selected, aggregate otherwise
  const [chartMode, setChartMode] = useState<ChartMode>(() =>
    selectedCount > 0 && selectedCount <= PER_LINK_MAX ? 'per_link' : 'aggregate'
  )
  const [userOverride, setUserOverride] = useState(false)

  // Update default when selection count changes (unless user has manually toggled)
  useEffect(() => {
    if (!userOverride) {
      setChartMode(selectedCount > 0 && selectedCount <= PER_LINK_MAX ? 'per_link' : 'aggregate')
    }
  }, [selectedCount, userOverride])

  // Reset override when selection is cleared
  useEffect(() => {
    if (selectedCount === 0) setUserOverride(false)
  }, [selectedCount])

  const handleModeChange = useCallback((mode: ChartMode) => {
    setChartMode(mode)
    setUserOverride(true)
  }, [])

  const [direction, setDirection] = useState<Direction>('combined')

  const { data, isFetching } = useQuery({
    queryKey: ['multi-link-latency', chartMode, sortedPks, timeRange, agg, filters],
    queryFn: () => fetchMultiLinkLatencyHistory({
      mode: chartMode,
      pks: sortedPks.length > 0 ? sortedPks : undefined,
      timeRange,
      agg,
      filters,
    }),
    staleTime: 30000,
    placeholderData: keepPreviousData,
  })

  const rttChartRef = useRef<HTMLDivElement>(null)
  const jitterChartRef = useRef<HTMLDivElement>(null)
  const lossChartRef = useRef<HTMLDivElement>(null)

  const [rttHoveredIdx, setRttHoveredIdx] = useState<number | null>(null)
  const [jitterHoveredIdx, setJitterHoveredIdx] = useState<number | null>(null)
  const [lossHoveredIdx, setLossHoveredIdx] = useState<number | null>(null)
  const handleRttIdx = useCallback((idx: number | null) => setRttHoveredIdx(idx), [])
  const handleJitterIdx = useCallback((idx: number | null) => setJitterHoveredIdx(idx), [])
  const handleLossIdx = useCallback((idx: number | null) => setLossHoveredIdx(idx), [])

  // Organize data by timestamp and link
  const { timestamps, linkPks, rttData, jitterData, lossData, colorMap } = useMemo(() => {
    if (!data?.points?.length) {
      return { timestamps: [] as number[], linkPks: [] as string[], rttData: new Map<string, (number | null)[]>(), jitterData: new Map<string, (number | null)[]>(), lossData: new Map<string, (number | null)[]>(), colorMap: new Map<string, string>() }
    }

    const timeSet = new Set<string>()
    const pkSet = new Set<string>()
    for (const p of data.points) {
      timeSet.add(p.time)
      pkSet.add(p.link_pk)
    }
    const times = [...timeSet].sort()
    // Preserve order from data (aggregate: Avg/P95/P99/Max; per_link: sorted PKs)
    const orderedPks = chartMode === 'aggregate'
      ? [...pkSet]
      : [...pkSet].sort()

    const timestamps = times.map(t => new Date(t).getTime() / 1000)
    const timeIndex = new Map(times.map((t, i) => [t, i]))

    const rttData = new Map<string, (number | null)[]>()
    const jitterData = new Map<string, (number | null)[]>()
    const lossData = new Map<string, (number | null)[]>()

    for (const pk of orderedPks) {
      rttData.set(pk, new Array(timestamps.length).fill(null))
      jitterData.set(pk, new Array(timestamps.length).fill(null))
      lossData.set(pk, new Array(timestamps.length).fill(null))
    }

    for (const p of data.points) {
      const idx = timeIndex.get(p.time)
      if (idx === undefined) continue

      let rttVal: number
      let jitterVal: number
      if (direction === 'a_to_z') {
        rttVal = p.rtt_a_to_z_ms
        jitterVal = p.jitter_a_to_z_ms
      } else if (direction === 'z_to_a') {
        rttVal = p.rtt_z_to_a_ms
        jitterVal = p.jitter_z_to_a_ms
      } else {
        rttVal = (p.rtt_a_to_z_ms + p.rtt_z_to_a_ms) / 2
        jitterVal = (p.jitter_a_to_z_ms + p.jitter_z_to_a_ms) / 2
      }

      if (rttVal > 0) rttData.get(p.link_pk)![idx] = rttVal
      if (jitterVal > 0) jitterData.get(p.link_pk)![idx] = jitterVal
      if (p.loss_pct > 0) lossData.get(p.link_pk)![idx] = p.loss_pct
    }

    const colorMap = new Map<string, string>()
    orderedPks.forEach((pk, i) => colorMap.set(pk, colors[i % colors.length]))

    return { timestamps, linkPks: orderedPks, rttData, jitterData, lossData, colorMap }
  }, [data, colors, chartMode, direction])

  // Resolve display name for a link pk
  const getName = useCallback((pk: string) => {
    if (chartMode === 'aggregate') {
      return data?.points.find(p => p.link_pk === pk)?.link_code ?? pk
    }
    return linkNames?.get(pk) || pk.slice(0, 8)
  }, [chartMode, data?.points, linkNames])

  // Build uPlot data + series for a given metric
  const buildChart = useCallback((metricData: Map<string, (number | null)[]>) => {
    if (timestamps.length === 0) {
      return { data: [[]] as uPlot.AlignedData, series: [] as uPlot.Series[], keys: [] as string[] }
    }
    const series: uPlot.Series[] = [{}]
    const aligned: (number | null)[][] = [timestamps as unknown as (number | null)[]]
    const keys: string[] = []

    for (const pk of linkPks) {
      const arr = metricData.get(pk) || []
      // Skip series with no data (all nulls)
      if (arr.every(v => v === null)) continue
      const name = getName(pk)
      keys.push(pk)
      const seriesOpts: uPlot.Series = {
        label: name,
        stroke: colorMap.get(pk),
        width: 1.5,
        points: { show: false },
      }
      if (chartMode === 'aggregate') {
        if (pk === '_p95') seriesOpts.dash = [6, 3]
        else if (pk === '_p99') seriesOpts.dash = [3, 3]
        else if (pk === '_max') seriesOpts.dash = [2, 2]
      }
      series.push(seriesOpts)
      aligned.push(arr)
    }

    return { data: aligned as uPlot.AlignedData, series, keys }
  }, [timestamps, linkPks, getName, colorMap, chartMode])

  const rttChart = useMemo(() => buildChart(rttData), [buildChart, rttData])
  const jitterChart = useMemo(() => buildChart(jitterData), [buildChart, jitterData])
  const lossChart = useMemo(() => buildChart(lossData), [buildChart, lossData])

  const hasLoss = useMemo(() => {
    for (const arr of lossData.values()) {
      if (arr.some((v: number | null) => v !== null)) return true
    }
    return false
  }, [lossData])

  const msAxes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map(v => `${v.toFixed(1)} ms`) },
  ], [])

  const pctAxes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map(v => `${v.toFixed(1)}%`) },
  ], [])

  const chartScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  const lossScales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true, range: (_u, min, max) => [Math.min(min, 0), Math.min(max, 100)] },
  }), [])

  // Charts
  const { plotRef: rttPlotRef } = useUPlotChart({
    containerRef: rttChartRef,
    data: rttChart.data,
    series: rttChart.series,
    height: 180,
    axes: msAxes,
    scales: chartScales,
    onCursorIdx: handleRttIdx,
  })

  const { plotRef: jitterPlotRef } = useUPlotChart({
    containerRef: jitterChartRef,
    data: jitterChart.data,
    series: jitterChart.series,
    height: 144,
    axes: msAxes,
    scales: chartScales,
    onCursorIdx: handleJitterIdx,
  })

  const { plotRef: lossPlotRef } = useUPlotChart({
    containerRef: lossChartRef,
    data: lossChart.data,
    series: lossChart.series,
    height: 120,
    axes: pctAxes,
    scales: lossScales,
    onCursorIdx: handleLossIdx,
  })

  // Legend state
  const rttLegend = useChartLegend()
  const jitterLegend = useChartLegend()
  const lossLegend = useChartLegend()

  useUPlotLegendSync(rttPlotRef, rttLegend, rttChart.keys)
  useUPlotLegendSync(jitterPlotRef, jitterLegend, jitterChart.keys)
  useUPlotLegendSync(lossPlotRef, lossLegend, lossChart.keys)

  // Click on a chart line to select/highlight it in the legend
  const handleChartClick = useCallback((
    plotRef: React.MutableRefObject<uPlot | null>,
    keys: string[],
    legend: ReturnType<typeof useChartLegend>,
    e: React.MouseEvent,
  ) => {
    const u = plotRef.current
    if (!u) return
    // Find the focused series (nearest to cursor)
    for (let i = 1; i < u.series.length; i++) {
      if ((u.series[i] as uPlot.Series & { _focus?: boolean })._focus) {
        const key = keys[i - 1] // keys array is offset by 1 (no x-axis entry)
        if (key) legend.handleClick(key, e)
        return
      }
    }
  }, [])

  // Legend series definitions
  const buildLegendSeries = useCallback((keys: string[]): ChartLegendSeries[] =>
    keys.map(pk => ({
      key: pk,
      color: colorMap.get(pk) || '#888',
      label: getName(pk),
      dashed: chartMode === 'aggregate' && pk !== '_avg' && pk.startsWith('_'),
    })),
    [colorMap, getName, chartMode]
  )

  const rttLegendSeries = useMemo(() => buildLegendSeries(rttChart.keys), [buildLegendSeries, rttChart.keys])
  const jitterLegendSeries = useMemo(() => buildLegendSeries(jitterChart.keys), [buildLegendSeries, jitterChart.keys])
  const lossLegendSeries = useMemo(() => buildLegendSeries(lossChart.keys), [buildLegendSeries, lossChart.keys])

  // Display values (hovered or latest)
  const buildDisplayValues = useCallback((
    uplotData: uPlot.AlignedData,
    keys: string[],
    hoveredIdx: number | null,
    unit: string,
    decimals = 2,
  ) => {
    const map = new Map<string, string>()
    if (uplotData[0].length === 0) return map
    const idx = hoveredIdx != null && hoveredIdx < uplotData[0].length ? hoveredIdx : uplotData[0].length - 1
    for (let i = 0; i < keys.length; i++) {
      const val = (uplotData[i + 1] as (number | null)[])?.[idx]
      map.set(keys[i], val != null ? `${val.toFixed(decimals)} ${unit}` : '—')
    }
    return map
  }, [])

  const buildMaxValues = useCallback((
    uplotData: uPlot.AlignedData,
    keys: string[],
    unit: string,
    decimals = 2,
  ) => {
    const map = new Map<string, string>()
    if (uplotData[0].length === 0) return map
    for (let i = 0; i < keys.length; i++) {
      const series = uplotData[i + 1] as (number | null)[]
      let max = 0
      if (series) for (const v of series) if (v != null && v > max) max = v
      map.set(keys[i], `${max.toFixed(decimals)} ${unit}`)
    }
    return map
  }, [])

  const rttDisplayValues = useMemo(() => buildDisplayValues(rttChart.data, rttChart.keys, rttHoveredIdx, 'ms'), [buildDisplayValues, rttChart.data, rttChart.keys, rttHoveredIdx])
  const jitterDisplayValues = useMemo(() => buildDisplayValues(jitterChart.data, jitterChart.keys, jitterHoveredIdx, 'ms'), [buildDisplayValues, jitterChart.data, jitterChart.keys, jitterHoveredIdx])
  const lossDisplayValues = useMemo(() => buildDisplayValues(lossChart.data, lossChart.keys, lossHoveredIdx, '%', 1), [buildDisplayValues, lossChart.data, lossChart.keys, lossHoveredIdx])

  const rttMaxValues = useMemo(() => buildMaxValues(rttChart.data, rttChart.keys, 'ms'), [buildMaxValues, rttChart.data, rttChart.keys])
  const jitterMaxValues = useMemo(() => buildMaxValues(jitterChart.data, jitterChart.keys, 'ms'), [buildMaxValues, jitterChart.data, jitterChart.keys])
  const lossMaxValues = useMemo(() => buildMaxValues(lossChart.data, lossChart.keys, '%', 1), [buildMaxValues, lossChart.data, lossChart.keys])

  const rttHoveredTime = useMemo(() => formatHoveredTime(rttChart.data[0] as ArrayLike<number>, rttHoveredIdx), [rttChart.data, rttHoveredIdx])
  const jitterHoveredTime = useMemo(() => formatHoveredTime(jitterChart.data[0] as ArrayLike<number>, jitterHoveredIdx), [jitterChart.data, jitterHoveredIdx])
  const lossHoveredTime = useMemo(() => formatHoveredTime(lossChart.data[0] as ArrayLike<number>, lossHoveredIdx), [lossChart.data, lossHoveredIdx])

  const refresh = () => queryClient.invalidateQueries({ queryKey: ['multi-link-latency'] })

  const directionToggle = (
    <div className="flex items-center gap-0.5" onClick={e => e.stopPropagation()}>
      {([['combined', 'A+Z'], ['a_to_z', 'A→Z'], ['z_to_a', 'Z→A']] as [Direction, string][]).map(([d, label]) => (
        <button
          key={d}
          onClick={() => setDirection(d)}
          className={`px-2 py-1 text-xs rounded-md border transition-colors ${
            direction === d
              ? 'border-foreground/30 text-foreground bg-muted'
              : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  )

  const modeToggle = (
    <button
      onClick={(e) => { e.stopPropagation(); handleModeChange(chartMode === 'aggregate' ? 'per_link' : 'aggregate') }}
      className={`px-2.5 py-1 text-xs rounded-md border transition-colors ${
        chartMode === 'aggregate'
          ? 'border-foreground/30 text-foreground bg-muted'
          : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
      }`}
    >
      Aggregate
    </button>
  )

  // Legend heights: null means auto-size to content (capped at 128px), set explicitly on drag
  const [rttLegendHeight, setRttLegendHeight] = useState<number | null>(null)
  const [jitterLegendHeight, setJitterLegendHeight] = useState<number | null>(null)
  const [lossLegendHeight, setLossLegendHeight] = useState<number | null>(null)

  // Reset to auto when chart mode changes (series count changes)
  useEffect(() => {
    setRttLegendHeight(null)
    setJitterLegendHeight(null)
    setLossLegendHeight(null)
  }, [chartMode])

  const [rttOpen, setRttOpen] = useState(true)
  const [jitterOpen, setJitterOpen] = useState(true)
  const [lossOpen, setLossOpen] = useState(true)

  return (
    <div className={`space-y-4 ${className || ''}`}>
      {/* RTT */}
      <div className="border border-border rounded-lg bg-card overflow-hidden group/chart">
        <div className="flex items-center gap-3 px-4 py-3">
          <button onClick={() => setRttOpen(o => !o)} className="text-muted-foreground hover:text-foreground transition-colors">
            <ChevronDown className={`h-4 w-4 transition-transform ${!rttOpen ? '-rotate-90' : ''}`} />
          </button>
          <span className="text-xs text-muted-foreground uppercase tracking-wider">Round-Trip Time</span>
          <RefreshButton fetching={isFetching} onClick={refresh} />
          <div className="flex-1" />
          {directionToggle}
          {modeToggle}
        </div>
        <div className="h-0.5 w-full overflow-hidden">
          {isFetching && <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />}
        </div>
        {rttOpen && (
          <div className="px-4 pb-4">
            <div ref={rttChartRef} className="h-[180px]" onClick={(e) => handleChartClick(rttPlotRef, rttChart.keys, rttLegend, e)} />
            <LegendPanel series={rttLegendSeries} legend={rttLegend} values={rttDisplayValues} maxValues={rttMaxValues} hoveredTime={rttHoveredTime} height={rttLegendHeight} onResize={setRttLegendHeight} />
          </div>
        )}
      </div>

      {/* Jitter */}
      <div className="border border-border rounded-lg bg-card overflow-hidden group/chart">
        <div className="flex items-center gap-3 px-4 py-3">
          <button onClick={() => setJitterOpen(o => !o)} className="text-muted-foreground hover:text-foreground transition-colors">
            <ChevronDown className={`h-4 w-4 transition-transform ${!jitterOpen ? '-rotate-90' : ''}`} />
          </button>
          <span className="text-xs text-muted-foreground uppercase tracking-wider">Jitter</span>
          <RefreshButton fetching={isFetching} onClick={refresh} />
          <div className="flex-1" />
        </div>
        <div className="h-0.5 w-full overflow-hidden">
          {isFetching && <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />}
        </div>
        {jitterOpen && (
          <div className="px-4 pb-4">
            <div ref={jitterChartRef} className="h-36" onClick={(e) => handleChartClick(jitterPlotRef, jitterChart.keys, jitterLegend, e)} />
            <LegendPanel series={jitterLegendSeries} legend={jitterLegend} values={jitterDisplayValues} maxValues={jitterMaxValues} hoveredTime={jitterHoveredTime} height={jitterLegendHeight} onResize={setJitterLegendHeight} />
          </div>
        )}
      </div>

      {/* Loss */}
      {hasLoss && (
        <div className="border border-border rounded-lg bg-card overflow-hidden group/chart">
          <div className="flex items-center gap-3 px-4 py-3">
            <button onClick={() => setLossOpen(o => !o)} className="text-muted-foreground hover:text-foreground transition-colors">
              <ChevronDown className={`h-4 w-4 transition-transform ${!lossOpen ? '-rotate-90' : ''}`} />
            </button>
            <span className="text-xs text-muted-foreground uppercase tracking-wider">Packet Loss</span>
            <RefreshButton fetching={isFetching} onClick={refresh} />
            <div className="flex-1" />
          </div>
          <div className="h-0.5 w-full overflow-hidden">
            {isFetching && <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />}
          </div>
          {lossOpen && (
            <div className="px-4 pb-4">
              <div ref={lossChartRef} className="h-[120px]" onClick={(e) => handleChartClick(lossPlotRef, lossChart.keys, lossLegend, e)} />
              <LegendPanel series={lossLegendSeries} legend={lossLegend} values={lossDisplayValues} maxValues={lossMaxValues} hoveredTime={lossHoveredTime} height={lossLegendHeight} onResize={setLossLegendHeight} />
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function LegendPanel({
  series,
  legend,
  values,
  maxValues,
  hoveredTime,
  height,
  onResize,
}: {
  series: ChartLegendSeries[]
  legend: ReturnType<typeof useChartLegend>
  values: Map<string, string>
  maxValues: Map<string, string>
  hoveredTime?: string
  height: number | null
  onResize: (h: number) => void
}) {
  const [searchText, setSearchText] = useState('')
  const [searchExpanded, setSearchExpanded] = useState(false)
  const searchInputRef = useRef<HTMLInputElement>(null)

  const filteredSeries = useMemo(() => {
    if (!searchText.trim()) return series
    const q = searchText.toLowerCase()
    return series.filter(s => s.label.toLowerCase().includes(q))
  }, [series, searchText])

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    const startY = e.clientY
    const container = (e.currentTarget as HTMLElement).parentElement
    const startHeight = container?.offsetHeight ?? 128
    const handleMove = (ev: MouseEvent) => {
      onResize(Math.max(48, Math.min(400, startHeight + (ev.clientY - startY))))
    }
    const handleUp = () => {
      document.removeEventListener('mousemove', handleMove)
      document.removeEventListener('mouseup', handleUp)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
    document.addEventListener('mousemove', handleMove)
    document.addEventListener('mouseup', handleUp)
    document.body.style.cursor = 'ns-resize'
    document.body.style.userSelect = 'none'
  }, [onResize])

  return (
    <div className="relative flex flex-col overflow-hidden" style={height != null ? { height } : { maxHeight: 128 }}>
        {/* Legend header */}
        <div className="flex-none flex items-center gap-2 px-2 pt-1 pb-1 text-xs">
          <span className="text-muted-foreground">
            {filteredSeries.length}/{series.length}
          </span>
          {hoveredTime && (
            <span className="text-[10px] text-muted-foreground">{hoveredTime}</span>
          )}
          <div className="flex-1" />
          {searchExpanded ? (
            <div className="relative flex-1 max-w-[200px]">
              <input
                ref={searchInputRef}
                type="text"
                value={searchText}
                onChange={e => setSearchText(e.target.value)}
                onBlur={() => { if (!searchText) setSearchExpanded(false) }}
                placeholder="Filter"
                className="w-full px-1.5 py-0.5 pr-6 text-xs bg-transparent border-b border-border focus:outline-none focus:border-foreground placeholder:text-muted-foreground/60"
              />
              {searchText && (
                <button
                  onClick={() => { setSearchText(''); searchInputRef.current?.focus() }}
                  className="absolute right-1 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  <X className="h-3 w-3" />
                </button>
              )}
            </div>
          ) : (
            <button
              onClick={() => { setSearchExpanded(true); setTimeout(() => searchInputRef.current?.focus(), 0) }}
              className="text-muted-foreground hover:text-foreground"
            >
              <Search className="h-3 w-3" />
            </button>
          )}
          <button
            onClick={() => legend.setSelectedSeries(new Set())}
            className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
          >
            All
          </button>
          <button
            onClick={() => legend.setSelectedSeries(new Set(['__none__']))}
            className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
          >
            None
          </button>
        </div>
        {/* Scrollable legend rows */}
        <div className="flex-1 overflow-auto min-h-0">
          <ChartLegendTable series={filteredSeries} legend={legend} values={values} maxValues={maxValues} />
        </div>
      {/* Resize handle */}
      <div
        onMouseDown={handleResizeStart}
        className="absolute bottom-0 left-0 right-0 h-3 cursor-ns-resize hover:bg-muted transition-colors flex items-center justify-center"
      >
        <div className="w-12 h-1 bg-border rounded-full" />
      </div>
    </div>
  )
}
