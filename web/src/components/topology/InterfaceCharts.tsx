import { useMemo, useCallback, useRef, useState } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import uPlot from 'uplot'
import { useChartLegend, type UseChartLegendReturn } from '@/hooks/use-chart-legend'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useUPlotLegendSync } from '@/hooks/use-uplot-legend-sync'
import { InterfaceLegendTable, type InterfaceValues } from './InterfaceLegendTable'
import { fetchTrafficHistoryByInterface, formatChartAxisRate, formatChartTooltipRate, formatHoveredTime, resolveAutoBucket, bucketLabels, type TimeRange, type TimeRangePreset, type InterfaceTrafficPoint, type BucketSize, type TrafficMetric } from './utils'
import { fetchDeviceInterfaceHistory } from '@/lib/api'
import { TrafficFilters } from './TimeRangeSelector'

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316']

function formatCount(value: number): string {
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toString()
}

function formatPpsAxis(pps: number): string {
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(1)}G`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)}M`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(1)}K`
  return `${pps.toFixed(0)}`
}

function formatPpsTooltip(pps: number): string {
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(2)} Gpps`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(2)} Mpps`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(2)} Kpps`
  return `${pps.toFixed(0)} pps`
}

/** Compact legend for health charts — shows interface name + single value, connected to shared legend */
function HealthLegendTable({
  interfaces,
  colors,
  data,
  hoveredIdx,
  legend,
  visibleSeries,
  interfaceLabels,
  bidirectional,
  hoveredTime,
}: {
  interfaces: string[]
  colors: string[]
  data: uPlot.AlignedData
  hoveredIdx: number | null
  legend: UseChartLegendReturn
  visibleSeries: Set<string>
  interfaceLabels?: Map<string, string>
  bidirectional?: boolean
  hoveredTime?: string
}) {
  // Max values across all timestamps
  const maxValues = useMemo(() => {
    const map = new Map<string, number>()
    if (data[0].length === 0) return map
    if (bidirectional) {
      for (let i = 0; i < interfaces.length; i++) {
        const inSeries = data[i * 2 + 1] as (number | null)[]
        const outSeries = data[i * 2 + 2] as (number | null)[]
        // For bidirectional, we'll store max per direction using separate keys
        let maxIn = 0, maxOut = 0
        if (inSeries) for (const v of inSeries) if (v != null && v > maxIn) maxIn = v
        if (outSeries) for (const v of outSeries) if (v != null && v > maxOut) maxOut = v
        map.set(`${interfaces[i]}:in`, maxIn)
        map.set(`${interfaces[i]}:out`, maxOut)
      }
    } else {
      for (let i = 0; i < interfaces.length; i++) {
        const series = data[i + 1] as (number | null)[]
        let max = 0
        if (series) for (const v of series) if (v != null && v > max) max = v
        map.set(interfaces[i], max)
      }
    }
    return map
  }, [data, interfaces, bidirectional])

  // Show hovered or latest values (non-bidirectional)
  const values = useMemo(() => {
    const map = new Map<string, number>()
    if (bidirectional || data[0].length === 0) return map
    const idx = hoveredIdx != null && hoveredIdx < data[0].length ? hoveredIdx : data[0].length - 1
    for (let i = 0; i < interfaces.length; i++) {
      const val = (data[i + 1] as (number | null)[])?.[idx]
      map.set(interfaces[i], val ?? 0)
    }
    return map
  }, [data, interfaces, hoveredIdx, bidirectional])

  // Show hovered or latest values (bidirectional: in/out per interface)
  const biValues = useMemo(() => {
    const map = new Map<string, { in: number; out: number }>()
    if (!bidirectional || data[0].length === 0) return map
    const idx = hoveredIdx != null && hoveredIdx < data[0].length ? hoveredIdx : data[0].length - 1
    for (let i = 0; i < interfaces.length; i++) {
      const inVal = (data[i * 2 + 1] as (number | null)[])?.[idx]
      const outVal = (data[i * 2 + 2] as (number | null)[])?.[idx]
      map.set(interfaces[i], { in: inVal ?? 0, out: outVal ?? 0 })
    }
    return map
  }, [data, interfaces, hoveredIdx, bidirectional])

  // Track which directions have data per interface (bidirectional only)
  const directionActivity = useMemo(() => {
    if (!bidirectional) return new Map<string, { hasIn: boolean; hasOut: boolean }>()
    const map = new Map<string, { hasIn: boolean; hasOut: boolean }>()
    for (let i = 0; i < interfaces.length; i++) {
      const inSeries = data[i * 2 + 1] as (number | null)[]
      const outSeries = data[i * 2 + 2] as (number | null)[]
      map.set(interfaces[i], {
        hasIn: inSeries?.some((v) => v != null && v > 0) ?? false,
        hasOut: outSeries?.some((v) => v != null && v > 0) ?? false,
      })
    }
    return map
  }, [interfaces, data, bidirectional])

  // Only show interfaces that have any non-zero value across the entire dataset
  const activeInterfaces = useMemo(() => {
    if (bidirectional) {
      return interfaces.filter((intf) => {
        const activity = directionActivity.get(intf)
        return activity ? (activity.hasIn || activity.hasOut) : false
      })
    }
    return interfaces.filter((_, i) => {
      const series = data[i + 1] as (number | null)[]
      if (!series) return false
      return series.some((v) => v != null && v > 0)
    })
  }, [interfaces, data, bidirectional, directionActivity])

  // Build legend rows: for bidirectional, one row per active direction per interface
  const legendRows = useMemo(() => {
    if (!bidirectional) {
      return activeInterfaces.map((intf) => ({
        key: intf,
        intf,
        label: interfaceLabels?.get(intf) ?? intf,
        direction: null as null,
      }))
    }
    const rows: { key: string; intf: string; label: string; direction: 'in' | 'out' }[] = []
    for (const intf of activeInterfaces) {
      const activity = directionActivity.get(intf)
      const label = interfaceLabels?.get(intf) ?? intf
      if (activity?.hasIn) {
        rows.push({ key: `${intf}:in`, intf, label: `${label}:In`, direction: 'in' })
      }
      if (activity?.hasOut) {
        rows.push({ key: `${intf}:out`, intf, label: `${label}:Out`, direction: 'out' })
      }
    }
    return rows
  }, [activeInterfaces, bidirectional, directionActivity, interfaceLabels])

  if (activeInterfaces.length === 0) return null

  return (
    <div className="flex flex-col text-xs px-2 pt-1 pb-2">
      <div className="flex items-center px-1 mb-1">
        <span className="text-xs text-muted-foreground flex-1 min-w-0">Name</span>
        <span className="text-xs text-muted-foreground w-24 text-right whitespace-nowrap">Max</span>
        <span className="text-xs text-muted-foreground w-24 text-right whitespace-nowrap">{hoveredTime ?? 'Value'}</span>
      </div>
      <div className="max-h-32 overflow-y-auto space-y-0.5">
        {legendRows.map((row) => {
          const colorIndex = interfaces.indexOf(row.intf)
          const color = colors[colorIndex % colors.length]
          const isVisible = visibleSeries.has(row.intf)
          const isDashed = row.direction === 'out'

          let displayValue: number
          if (bidirectional) {
            const vals = biValues.get(row.intf) ?? { in: 0, out: 0 }
            displayValue = row.direction === 'out' ? vals.out : vals.in
          } else {
            displayValue = values.get(row.intf) ?? 0
          }

          const maxValue = maxValues.get(row.key) ?? 0

          return (
            <div
              key={row.key}
              className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${
                isVisible ? '' : 'opacity-40'
              }`}
              onClick={(e) => legend.handleClick(row.intf, e)}
              onMouseEnter={() => legend.handleMouseEnter(row.intf)}
              onMouseLeave={legend.handleMouseLeave}
            >
              <div className="flex items-center gap-1.5 min-w-0 flex-1">
                {isDashed ? (
                  <svg className="w-2.5 h-2.5 flex-shrink-0" viewBox="0 0 10 10">
                    <line x1="0" y1="5" x2="10" y2="5" stroke={color} strokeWidth="3" strokeDasharray="3 2" />
                  </svg>
                ) : (
                  <span
                    className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                    style={{ backgroundColor: color }}
                  />
                )}
                <span className="font-mono text-foreground truncate">{row.label}</span>
              </div>
              <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
                {formatCount(maxValue)}
              </span>
              <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
                {formatCount(displayValue)}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

interface InterfaceChartsProps {
  entityType: 'link' | 'device'
  entityPk: string
  timeRange?: TimeRange
  interfaceLabels?: Map<string, string>
  /** Controlled traffic filter state (when managed by parent) */
  bucket?: BucketSize
  onBucketChange?: (bucket: BucketSize) => void
  metric?: TrafficMetric
  onMetricChange?: (metric: TrafficMetric) => void
  trafficView?: 'avg' | 'peak'
  onTrafficViewChange?: (view: 'avg' | 'peak') => void
  /** Additional CSS classes for the outer wrapper */
  className?: string
}

/** Convert flat per-interface points into columnar uPlot AlignedData with Unix timestamp x-axis */
function toUPlotData(
  points: InterfaceTrafficPoint[],
  interfaces: string[],
  metric: 'avg' | 'peak'
): uPlot.AlignedData {
  const inKey = metric === 'avg' ? 'avgIn' : 'peakIn'
  const outKey = metric === 'avg' ? 'avgOut' : 'peakOut'

  // Collect sorted unique timestamps
  const timeSet = new Set<string>()
  for (const p of points) timeSet.add(p.time)
  const sortedTimes = Array.from(timeSet).sort()

  // Build time → intf → point lookup
  const lookup = new Map<string, Map<string, InterfaceTrafficPoint>>()
  for (const p of points) {
    let byIntf = lookup.get(p.time)
    if (!byIntf) {
      byIntf = new Map()
      lookup.set(p.time, byIntf)
    }
    byIntf.set(p.intf, p)
  }

  // Use real Unix timestamps (seconds) for x-axis — matches traffic page charts
  const timestamps = sortedTimes.map((t) => new Date(t).getTime() / 1000)
  const arrays: (number | null)[][] = [timestamps]

  for (const intf of interfaces) {
    const inVals: (number | null)[] = []
    const outVals: (number | null)[] = []
    for (const t of sortedTimes) {
      const p = lookup.get(t)?.get(intf)
      inVals.push(p ? (p[inKey] as number) : null)
      outVals.push(p ? -(p[outKey] as number) : null)
    }
    arrays.push(inVals)
    arrays.push(outVals)
  }

  return arrays as uPlot.AlignedData
}

interface HealthColumnar {
  data: uPlot.AlignedData
  hasData: boolean
}

function buildHealthColumnar(
  historyData: Awaited<ReturnType<typeof fetchDeviceInterfaceHistory>>,
  interfaces: string[],
  field: 'errors' | 'fcs_errors' | 'discards' | 'transitions',
  bidirectional?: boolean
): HealthColumnar {
  const allTimes = new Set<string>()
  for (const iface of historyData.interfaces) {
    for (const h of iface.hours) allTimes.add(h.hour)
  }
  const sortedTimes = Array.from(allTimes).sort()
  // Use real Unix timestamps (seconds) for x-axis
  const timestamps = sortedTimes.map((t) => new Date(t).getTime() / 1000)

  let hasData = false

  // Bidirectional mode: produce two columns per interface (in + out)
  if (bidirectional && (field === 'errors' || field === 'discards')) {
    const lookup = new Map<string, Map<string, { in: number; out: number }>>()
    const activeIn = new Set<string>()
    const activeOut = new Set<string>()

    for (const iface of historyData.interfaces) {
      const name = iface.interface_name
      if (!interfaces.includes(name)) continue
      for (const h of iface.hours) {
        const inVal = field === 'errors' ? (h.in_errors || 0) : (h.in_discards || 0)
        const outVal = field === 'errors' ? (h.out_errors || 0) : (h.out_discards || 0)
        if (inVal > 0 || outVal > 0) {
          hasData = true
          if (inVal > 0) activeIn.add(name)
          if (outVal > 0) activeOut.add(name)
          let byIntf = lookup.get(h.hour)
          if (!byIntf) {
            byIntf = new Map()
            lookup.set(h.hour, byIntf)
          }
          byIntf.set(name, { in: inVal, out: outVal })
        }
      }
    }

    const arrays: (number | null)[][] = [timestamps]
    for (const intf of interfaces) {
      const inVals: (number | null)[] = []
      const outVals: (number | null)[] = []
      for (const t of sortedTimes) {
        const v = lookup.get(t)?.get(intf)
        inVals.push(v ? v.in : (activeIn.has(intf) ? 0 : null))
        outVals.push(v ? v.out : (activeOut.has(intf) ? 0 : null))
      }
      arrays.push(inVals)
      arrays.push(outVals)
    }

    return { data: arrays as uPlot.AlignedData, hasData }
  }

  // Non-bidirectional: single column per interface (summed or transitions)
  const lookup = new Map<string, Map<string, number>>()

  for (const iface of historyData.interfaces) {
    const name = iface.interface_name
    if (!interfaces.includes(name)) continue
    for (const h of iface.hours) {
      let value: number
      if (field === 'errors') {
        value = (h.in_errors || 0) + (h.out_errors || 0)
      } else if (field === 'fcs_errors') {
        value = h.in_fcs_errors || 0
      } else if (field === 'discards') {
        value = (h.in_discards || 0) + (h.out_discards || 0)
      } else {
        value = h.carrier_transitions || 0
      }
      if (value > 0) {
        hasData = true
        let byIntf = lookup.get(h.hour)
        if (!byIntf) {
          byIntf = new Map()
          lookup.set(h.hour, byIntf)
        }
        byIntf.set(name, value)
      }
    }
  }

  // Determine which interfaces have any non-zero data for this field
  const activeInterfaces = new Set<string>()
  for (const [, byIntf] of lookup) {
    for (const [name] of byIntf) activeInterfaces.add(name)
  }

  const arrays: (number | null)[][] = [timestamps]
  for (const intf of interfaces) {
    const isActive = activeInterfaces.has(intf)
    const vals: (number | null)[] = []
    for (const t of sortedTimes) {
      const v = lookup.get(t)?.get(intf)
      // Fill gaps with 0 for active series so the line stays connected at baseline
      vals.push(v ?? (isActive ? 0 : null))
    }
    arrays.push(vals)
  }

  return { data: arrays as uPlot.AlignedData, hasData }
}

export function InterfaceCharts({ entityType, entityPk, timeRange, interfaceLabels, bucket: controlledBucket, onBucketChange, metric: controlledMetric, onMetricChange, trafficView: controlledTrafficView, onTrafficViewChange, className }: InterfaceChartsProps) {
  const effectiveRange = timeRange ?? { preset: '24h' as const }

  const timeRangeStr = effectiveRange.preset === 'custom' ? 'custom' : effectiveRange.preset

  const [internalBucket, setInternalBucket] = useState<BucketSize>('auto')
  const [internalMetric, setInternalMetric] = useState<TrafficMetric>('throughput')
  const [internalTrafficView, setInternalTrafficView] = useState<'avg' | 'peak'>('avg')

  const bucket = controlledBucket ?? internalBucket
  const setBucket = onBucketChange ?? setInternalBucket
  const metric = controlledMetric ?? internalMetric
  const setMetric = onMetricChange ?? setInternalMetric
  const trafficView = controlledTrafficView ?? internalTrafficView
  const setTrafficView = onTrafficViewChange ?? setInternalTrafficView

  const effectiveBucketLabel = bucket === 'auto'
    ? bucketLabels[resolveAutoBucket(effectiveRange.preset as TimeRangePreset)]
    : undefined

  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null)
  const [errorHoveredIdx, setErrorHoveredIdx] = useState<number | null>(null)
  const [fcsErrorHoveredIdx, setFcsErrorHoveredIdx] = useState<number | null>(null)
  const [discardHoveredIdx, setDiscardHoveredIdx] = useState<number | null>(null)
  const [transitionHoveredIdx, setTransitionHoveredIdx] = useState<number | null>(null)

  const trafficChartRef = useRef<HTMLDivElement>(null)
  const errorChartRef = useRef<HTMLDivElement>(null)
  const fcsErrorChartRef = useRef<HTMLDivElement>(null)
  const discardChartRef = useRef<HTMLDivElement>(null)
  const transitionChartRef = useRef<HTMLDivElement>(null)

  // Traffic data
  const { data: rawPoints, isLoading: trafficLoading, error: trafficError } = useQuery({
    queryKey: ['topology-traffic-interface', entityType, entityPk, effectiveRange, bucket, metric],
    queryFn: () => fetchTrafficHistoryByInterface(entityType, entityPk, effectiveRange, bucket, metric),
    refetchInterval: 60000,
    retry: 2,
    placeholderData: keepPreviousData,
  })

  // Health data (only for devices)
  const { data: historyData, isLoading: healthLoading } = useQuery({
    queryKey: ['device-interface-health', entityPk, timeRangeStr],
    queryFn: () => fetchDeviceInterfaceHistory(entityPk, timeRangeStr),
    refetchInterval: 60000,
    retry: false,
    enabled: entityType === 'device',
    placeholderData: keepPreviousData,
  })

  const interfaces = useMemo(() => {
    if (!rawPoints || rawPoints.length === 0) return []
    const intfSet = new Set<string>()
    for (const p of rawPoints) intfSet.add(p.intf)
    return Array.from(intfSet).sort()
  }, [rawPoints])

  const avgData = useMemo(() => {
    if (!rawPoints || rawPoints.length === 0 || interfaces.length === 0)
      return [[]] as uPlot.AlignedData
    return toUPlotData(rawPoints, interfaces, 'avg')
  }, [rawPoints, interfaces])

  const peakData = useMemo(() => {
    if (!rawPoints || rawPoints.length === 0 || interfaces.length === 0)
      return [[]] as uPlot.AlignedData
    return toUPlotData(rawPoints, interfaces, 'peak')
  }, [rawPoints, interfaces])

  // Build uPlot series configs for traffic charts: per interface, in (solid) + out (dashed)
  const trafficSeries = useMemo((): uPlot.Series[] => {
    const s: uPlot.Series[] = [{}] // x-axis
    for (let i = 0; i < interfaces.length; i++) {
      const color = COLORS[i % COLORS.length]
      s.push({
        label: `${interfaces[i]}:in`,
        stroke: color,
        width: 1.5,
        points: { show: false },
      })
      s.push({
        label: `${interfaces[i]}:out`,
        stroke: color,
        width: 1.5,
        dash: [4, 2],
        points: { show: false },
      })
    }
    return s
  }, [interfaces])

  // Series keys for traffic legend sync: alternating in/out per interface
  const trafficSeriesKeys = useMemo(() => {
    const keys: string[] = []
    for (const intf of interfaces) {
      keys.push(intf) // in
      keys.push(intf) // out (same legend key)
    }
    return keys
  }, [interfaces])

  const trafficAxes = useMemo((): uPlot.Axis[] => [
    {},
    {
      values: metric === 'packets'
        ? (_u: uPlot, vals: number[]) => vals.map((v) => formatPpsAxis(Math.abs(v)))
        : (_u: uPlot, vals: number[]) => vals.map((v) => formatChartAxisRate(Math.abs(v))),
    },
  ], [metric])

  // Health data
  const errorHealth = useMemo(() => {
    if (!historyData?.interfaces || interfaces.length === 0) return null
    return buildHealthColumnar(historyData, interfaces, 'errors', true)
  }, [historyData, interfaces])

  const fcsErrorHealth = useMemo(() => {
    if (!historyData?.interfaces || interfaces.length === 0) return null
    return buildHealthColumnar(historyData, interfaces, 'fcs_errors')
  }, [historyData, interfaces])

  const discardHealth = useMemo(() => {
    if (!historyData?.interfaces || interfaces.length === 0) return null
    return buildHealthColumnar(historyData, interfaces, 'discards', true)
  }, [historyData, interfaces])

  const transitionHealth = useMemo(() => {
    if (!historyData?.interfaces || interfaces.length === 0) return null
    return buildHealthColumnar(historyData, interfaces, 'transitions')
  }, [historyData, interfaces])

  // Health series: one per interface (not bidirectional)
  const healthSeries = useMemo((): uPlot.Series[] => {
    const s: uPlot.Series[] = [{}]
    for (let i = 0; i < interfaces.length; i++) {
      s.push({
        label: interfaces[i],
        stroke: COLORS[i % COLORS.length],
        width: 1.5,
        points: { show: false },
      })
    }
    return s
  }, [interfaces])

  // Bidirectional health series: two series per interface (in solid, out dashed)
  const healthBidirectionalSeries = useMemo((): uPlot.Series[] => {
    const s: uPlot.Series[] = [{}] // x-axis
    for (let i = 0; i < interfaces.length; i++) {
      const color = COLORS[i % COLORS.length]
      s.push({
        label: `${interfaces[i]}:in`,
        stroke: color,
        width: 1.5,
        points: { show: false },
      })
      s.push({
        label: `${interfaces[i]}:out`,
        stroke: color,
        width: 1.5,
        dash: [4, 2],
        points: { show: false },
      })
    }
    return s
  }, [interfaces])

  // Series keys for bidirectional health legend sync: both in/out map to same interface
  const healthBidirectionalSeriesKeys = useMemo(() => {
    const keys: string[] = []
    for (const intf of interfaces) {
      keys.push(intf) // in
      keys.push(intf) // out (same legend key)
    }
    return keys
  }, [interfaces])

  const healthAxes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map((v) => formatCount(v)) },
  ], [])

  const legend = useChartLegend()

  const visibleSeries = useMemo(() => {
    if (legend.selectedSeries.has('__none__')) return new Set<string>()
    if (legend.selectedSeries.size > 0) return legend.selectedSeries
    return new Set(interfaces)
  }, [legend.selectedSeries, interfaces])

  const colors = useMemo(
    () => interfaces.map((_, i) => COLORS[i % COLORS.length]),
    [interfaces]
  )

  // Cursor handlers
  const handleCursorIdx = useCallback((idx: number | null) => {
    setHoveredIndex(idx)
  }, [])
  const handleErrorCursorIdx = useCallback((idx: number | null) => {
    setErrorHoveredIdx(idx)
  }, [])
  const handleFcsErrorCursorIdx = useCallback((idx: number | null) => {
    setFcsErrorHoveredIdx(idx)
  }, [])
  const handleDiscardCursorIdx = useCallback((idx: number | null) => {
    setDiscardHoveredIdx(idx)
  }, [])
  const handleTransitionCursorIdx = useCallback((idx: number | null) => {
    setTransitionHoveredIdx(idx)
  }, [])

  const trafficData = trafficView === 'avg' ? avgData : peakData

  // Charts
  const { plotRef: trafficPlotRef} = useUPlotChart({
    containerRef: trafficChartRef,
    data: trafficData,
    series: trafficSeries,
    height: 144,
    axes: trafficAxes,
    onCursorIdx: handleCursorIdx,
  })

  const { plotRef: errorPlotRef} = useUPlotChart({
    containerRef: errorChartRef,
    data: errorHealth?.data ?? ([[]] as uPlot.AlignedData),
    series: healthBidirectionalSeries,
    height: 144,
    axes: healthAxes,
    onCursorIdx: handleErrorCursorIdx,
  })

  const { plotRef: fcsErrorPlotRef} = useUPlotChart({
    containerRef: fcsErrorChartRef,
    data: fcsErrorHealth?.data ?? ([[]] as uPlot.AlignedData),
    series: healthSeries,
    height: 144,
    axes: healthAxes,
    onCursorIdx: handleFcsErrorCursorIdx,
  })

  const { plotRef: discardPlotRef} = useUPlotChart({
    containerRef: discardChartRef,
    data: discardHealth?.data ?? ([[]] as uPlot.AlignedData),
    series: healthBidirectionalSeries,
    height: 144,
    axes: healthAxes,
    onCursorIdx: handleDiscardCursorIdx,
  })

  const { plotRef: transitionPlotRef} = useUPlotChart({
    containerRef: transitionChartRef,
    data: transitionHealth?.data ?? ([[]] as uPlot.AlignedData),
    series: healthSeries,
    height: 144,
    axes: healthAxes,
    onCursorIdx: handleTransitionCursorIdx,
  })

  // Sync legend visibility to traffic chart
  useUPlotLegendSync(trafficPlotRef, legend, trafficSeriesKeys)

  // Sync legend visibility to health charts
  const healthSeriesKeys = useMemo(() => [...interfaces], [interfaces])
  useUPlotLegendSync(errorPlotRef, legend, healthBidirectionalSeriesKeys)
  useUPlotLegendSync(fcsErrorPlotRef, legend, healthSeriesKeys)
  useUPlotLegendSync(discardPlotRef, legend, healthBidirectionalSeriesKeys)
  useUPlotLegendSync(transitionPlotRef, legend, healthSeriesKeys)

  // Values shown in legend: hovered time point or latest
  const displayValues = useMemo(() => {
    const map = new Map<string, InterfaceValues>()
    if (avgData[0].length === 0 || peakData[0].length === 0) return map

    const idx = hoveredIndex != null && hoveredIndex < avgData[0].length ? hoveredIndex : avgData[0].length - 1

    for (let i = 0; i < interfaces.length; i++) {
      const avgInIdx = i * 2 + 1
      const avgOutIdx = i * 2 + 2
      const peakInIdx = i * 2 + 1
      const peakOutIdx = i * 2 + 2

      map.set(interfaces[i], {
        avgIn: (avgData[avgInIdx] as number[])?.[idx] ?? 0,
        avgOut: Math.abs((avgData[avgOutIdx] as number[])?.[idx] ?? 0),
        peakIn: (peakData[peakInIdx] as number[])?.[idx] ?? 0,
        peakOut: Math.abs((peakData[peakOutIdx] as number[])?.[idx] ?? 0),
      })
    }
    return map
  }, [avgData, peakData, interfaces, hoveredIndex])

  // Max values across the entire time range for traffic legend
  const trafficMaxValues = useMemo(() => {
    const map = new Map<string, InterfaceValues>()
    if (avgData[0].length === 0 || peakData[0].length === 0) return map

    for (let i = 0; i < interfaces.length; i++) {
      const avgInSeries = avgData[i * 2 + 1] as (number | null)[]
      const avgOutSeries = avgData[i * 2 + 2] as (number | null)[]
      const peakInSeries = peakData[i * 2 + 1] as (number | null)[]
      const peakOutSeries = peakData[i * 2 + 2] as (number | null)[]

      let maxAvgIn = 0, maxAvgOut = 0, maxPeakIn = 0, maxPeakOut = 0
      if (avgInSeries) for (const v of avgInSeries) if (v != null && v > maxAvgIn) maxAvgIn = v
      if (avgOutSeries) for (const v of avgOutSeries) { const a = Math.abs(v ?? 0); if (a > maxAvgOut) maxAvgOut = a }
      if (peakInSeries) for (const v of peakInSeries) if (v != null && v > maxPeakIn) maxPeakIn = v
      if (peakOutSeries) for (const v of peakOutSeries) { const a = Math.abs(v ?? 0); if (a > maxPeakOut) maxPeakOut = a }

      map.set(interfaces[i], { avgIn: maxAvgIn, avgOut: maxAvgOut, peakIn: maxPeakIn, peakOut: maxPeakOut })
    }
    return map
  }, [avgData, peakData, interfaces])

  // Hovered time labels for each chart
  const trafficHoveredTime = useMemo(() =>
    formatHoveredTime(trafficData[0] as ArrayLike<number>, hoveredIndex),
    [trafficData, hoveredIndex])
  const errorHoveredTime = useMemo(() =>
    formatHoveredTime((errorHealth?.data[0] ?? []) as ArrayLike<number>, errorHoveredIdx),
    [errorHealth, errorHoveredIdx])
  const fcsErrorHoveredTime = useMemo(() =>
    formatHoveredTime((fcsErrorHealth?.data[0] ?? []) as ArrayLike<number>, fcsErrorHoveredIdx),
    [fcsErrorHealth, fcsErrorHoveredIdx])
  const discardHoveredTime = useMemo(() =>
    formatHoveredTime((discardHealth?.data[0] ?? []) as ArrayLike<number>, discardHoveredIdx),
    [discardHealth, discardHoveredIdx])
  const transitionHoveredTime = useMemo(() =>
    formatHoveredTime((transitionHealth?.data[0] ?? []) as ArrayLike<number>, transitionHoveredIdx),
    [transitionHealth, transitionHoveredIdx])

  if ((trafficLoading || (entityType === 'device' && healthLoading)) && !rawPoints) {
    return <div className="h-36 animate-pulse bg-muted/50 rounded" />
  }

  if (trafficError) {
    return (
      <div className="text-sm text-muted-foreground text-center py-4">
        Unable to load traffic data — the request may have timed out
      </div>
    )
  }

  if (interfaces.length === 0) return null

  return (
    <div className="space-y-6">
      {errorHealth?.hasData && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Errors</div>
          <div ref={errorChartRef} className="h-36" />
          <HealthLegendTable
            interfaces={interfaces}
            colors={colors}
            data={errorHealth.data}
            hoveredIdx={errorHoveredIdx}
            legend={legend}
            visibleSeries={visibleSeries}
            interfaceLabels={interfaceLabels}
            bidirectional
            hoveredTime={errorHoveredTime}
          />
        </div>
      )}

      {fcsErrorHealth?.hasData && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            FCS Errors</div>
          <div ref={fcsErrorChartRef} className="h-36" />
          <HealthLegendTable
            interfaces={interfaces}
            colors={colors}
            data={fcsErrorHealth.data}
            hoveredIdx={fcsErrorHoveredIdx}
            legend={legend}
            visibleSeries={visibleSeries}
            interfaceLabels={interfaceLabels}
            hoveredTime={fcsErrorHoveredTime}
          />
        </div>
      )}

      {discardHealth?.hasData && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Discards</div>
          <div ref={discardChartRef} className="h-36" />
          <HealthLegendTable
            interfaces={interfaces}
            colors={colors}
            data={discardHealth.data}
            hoveredIdx={discardHoveredIdx}
            legend={legend}
            visibleSeries={visibleSeries}
            interfaceLabels={interfaceLabels}
            bidirectional
            hoveredTime={discardHoveredTime}
          />
        </div>
      )}

      {transitionHealth?.hasData && (
        <div className={className}>
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">
            Carrier Transitions</div>
          <div ref={transitionChartRef} className="h-36" />
          <HealthLegendTable
            interfaces={interfaces}
            colors={colors}
            data={transitionHealth.data}
            hoveredIdx={transitionHoveredIdx}
            legend={legend}
            visibleSeries={visibleSeries}
            interfaceLabels={interfaceLabels}
            hoveredTime={transitionHoveredTime}
          />
        </div>
      )}

      <div className={className}>
        <div className="flex items-center justify-between mb-2">
          <div className="text-xs text-muted-foreground uppercase tracking-wider">
            {trafficView === 'avg' ? 'Avg' : 'Peak'} Traffic</div>
          <TrafficFilters
            bucket={!controlledBucket ? bucket : undefined}
            onBucketChange={!controlledBucket ? setBucket : undefined}
            metric={metric}
            onMetricChange={setMetric}
            effectiveBucketLabel={effectiveBucketLabel}
            trafficView={trafficView}
            onTrafficViewChange={setTrafficView}
          />
        </div>
        <div ref={trafficChartRef} className="h-36" />

        <InterfaceLegendTable
          interfaces={interfaces}
          colors={colors}
          legend={legend}
          visibleSeries={visibleSeries}
          latestValues={displayValues}
          maxValues={trafficMaxValues}
          formatValue={metric === 'packets' ? formatPpsTooltip : formatChartTooltipRate}
          interfaceLabels={interfaceLabels}
          trafficView={trafficView}
          hoveredTime={trafficHoveredTime}
        />
      </div>
    </div>
  )
}
