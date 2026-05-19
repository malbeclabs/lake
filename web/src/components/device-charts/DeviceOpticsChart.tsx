import { useCallback, useMemo, useRef, useState } from 'react'
import { useQuery, keepPreviousData, useQueryClient } from '@tanstack/react-query'
import uPlot from 'uplot'
import { Loader2, RefreshCw } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { fetchDeviceOpticsHistory } from '@/lib/api'
import type { TimeRange } from '@/components/topology/utils'
import { formatHoveredTime } from '@/components/topology/utils'
import { toDeviceMetricsParams } from '@/components/shared/metrics-params'

interface DeviceOpticsChartProps {
  devicePk: string
  interfaceName: string
  channelIndex: number
  timeRange: TimeRange
  className?: string
}

function formatDbm(v: number | null | undefined): string {
  return v == null ? '—' : `${v.toFixed(2)} dBm`
}

export function DeviceOpticsChart({
  devicePk,
  interfaceName,
  channelIndex,
  timeRange,
  className,
}: DeviceOpticsChartProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const chartRef = useRef<HTMLDivElement>(null)
  const queryClient = useQueryClient()
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const fetchParams = useMemo(() => {
    const { range, startTime, endTime } = toDeviceMetricsParams(timeRange)
    return {
      interface: interfaceName,
      channel: channelIndex,
      range,
      startTime,
      endTime,
    }
  }, [timeRange, interfaceName, channelIndex])

  const queryKey = ['deviceOpticsHistory', devicePk, fetchParams] as const

  const { data, isFetching } = useQuery({
    queryKey,
    queryFn: () => fetchDeviceOpticsHistory(devicePk, fetchParams),
    placeholderData: keepPreviousData,
  })

  const inputColor = isDark ? '#60a5fa' : '#2563eb'
  const outputColor = isDark ? '#34d399' : '#059669'

  const { uPlotData, uPlotSeries, inputValues, outputValues } = useMemo(() => {
    const buckets = data?.buckets ?? []
    if (buckets.length === 0) {
      return {
        uPlotData: [[]] as uPlot.AlignedData,
        uPlotSeries: [] as uPlot.Series[],
        inputValues: [] as (number | null)[],
        outputValues: [] as (number | null)[],
      }
    }
    const ts = buckets.map(b => new Date(b.ts).getTime() / 1000)
    const inP = buckets.map(b => b.avg_input_power)
    const outP = buckets.map(b => b.avg_output_power)
    return {
      uPlotData: [ts, inP, outP] as uPlot.AlignedData,
      uPlotSeries: [
        {},
        { label: 'input', stroke: inputColor, width: 1.5, points: { show: false } },
        { label: 'output', stroke: outputColor, width: 1.5, points: { show: false } },
      ] as uPlot.Series[],
      inputValues: inP as (number | null)[],
      outputValues: outP as (number | null)[],
    }
  }, [data, inputColor, outputColor])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map(v => v.toFixed(1)) },
  ], [])

  const scales = useMemo((): uPlot.Scales => {
    const fromMs = data?.from ? new Date(data.from).getTime() : undefined
    const toMs = data?.to ? new Date(data.to).getTime() : undefined
    if (fromMs != null && toMs != null) {
      return {
        x: { time: true, range: () => [fromMs / 1000, toMs / 1000] },
        y: { auto: true },
      }
    }
    return { x: { time: true }, y: { auto: true } }
  }, [data?.from, data?.to])

  const handleCursorIdx = useCallback((idx: number | null) => {
    setHoveredIdx(idx)
  }, [])

  useUPlotChart({
    containerRef: chartRef,
    data: uPlotData,
    series: uPlotSeries,
    height: 144,
    axes,
    scales,
    onCursorIdx: handleCursorIdx,
  })

  const hasData = uPlotData[0].length > 0
  const bucketSeconds = data?.bucket_seconds ?? 0

  const timestamps = uPlotData[0] as ArrayLike<number>
  const hoveredTime = useMemo(
    () => formatHoveredTime(timestamps, hoveredIdx, bucketSeconds > 0 && bucketSeconds < 60),
    [timestamps, hoveredIdx, bucketSeconds],
  )

  const displayIdx =
    hoveredIdx != null && hoveredIdx < inputValues.length ? hoveredIdx : inputValues.length - 1
  const currentInput = displayIdx >= 0 ? inputValues[displayIdx] : null
  const currentOutput = displayIdx >= 0 ? outputValues[displayIdx] : null

  const { maxInput, maxOutput } = useMemo(() => {
    let mi: number | null = null
    let mo: number | null = null
    for (const v of inputValues) if (v != null && (mi == null || v > mi)) mi = v
    for (const v of outputValues) if (v != null && (mo == null || v > mo)) mo = v
    return { maxInput: mi, maxOutput: mo }
  }, [inputValues, outputValues])

  return (
    <div className={`${className ?? ''} group/chart`}>
      <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider mb-1">
        <span>
          Optical Power — {interfaceName} / channel {channelIndex}
        </span>
        {isFetching ? (
          <Loader2 className="h-3 w-3 animate-spin" />
        ) : (
          <button
            type="button"
            onClick={() => queryClient.invalidateQueries({ queryKey })}
            className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground"
            title="Refresh"
          >
            <RefreshCw className="h-3 w-3" />
          </button>
        )}
      </div>
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
        {isFetching && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>
      <div ref={chartRef} className="h-36" />
      {hasData ? (
        <div className="flex flex-col text-xs px-2 pt-2 pb-1">
          <div className="flex items-center px-1 mb-1">
            <span className="text-xs text-muted-foreground flex-1 min-w-0">Series</span>
            <span className="text-xs text-muted-foreground w-24 text-right whitespace-nowrap">Max</span>
            <span className="text-xs text-muted-foreground w-32 text-right whitespace-nowrap">
              {hoveredTime ?? 'Value'}
            </span>
          </div>
          <div className="flex items-center px-1 py-0.5">
            <div className="flex items-center gap-1.5 min-w-0 flex-1">
              <span
                className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                style={{ backgroundColor: inputColor }}
              />
              <span className="font-mono text-foreground truncate">Input</span>
            </div>
            <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
              {formatDbm(maxInput)}
            </span>
            <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-32 text-right">
              {formatDbm(currentInput)}
            </span>
          </div>
          <div className="flex items-center px-1 py-0.5">
            <div className="flex items-center gap-1.5 min-w-0 flex-1">
              <span
                className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                style={{ backgroundColor: outputColor }}
              />
              <span className="font-mono text-foreground truncate">Output</span>
            </div>
            <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-24 text-right">
              {formatDbm(maxOutput)}
            </span>
            <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-32 text-right">
              {formatDbm(currentOutput)}
            </span>
          </div>
        </div>
      ) : (
        !isFetching && (
          <div className="text-xs text-muted-foreground/60 py-2 text-center">
            No samples in the selected time range
          </div>
        )
      )}
    </div>
  )
}
