import { useMemo, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import uPlot from 'uplot'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { fetchDeviceOpticsHistory } from '@/lib/api'

interface DeviceOpticsChartProps {
  devicePk: string
  interfaceName: string
  channelIndex: number
  hours?: number
  className?: string
}

export function DeviceOpticsChart({
  devicePk,
  interfaceName,
  channelIndex,
  hours = 24,
  className,
}: DeviceOpticsChartProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const chartRef = useRef<HTMLDivElement>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['deviceOpticsHistory', devicePk, interfaceName, channelIndex, hours],
    queryFn: () => fetchDeviceOpticsHistory(devicePk, { interface: interfaceName, channel: channelIndex, hours }),
  })

  const inputColor = isDark ? '#60a5fa' : '#2563eb'
  const outputColor = isDark ? '#34d399' : '#059669'

  const { uPlotData, uPlotSeries } = useMemo(() => {
    const buckets = data?.buckets ?? []
    if (buckets.length === 0) {
      return { uPlotData: [[]] as uPlot.AlignedData, uPlotSeries: [] as uPlot.Series[] }
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
    }
  }, [data, inputColor, outputColor])

  const axes = useMemo((): uPlot.Axis[] => [
    {},
    { values: (_u: uPlot, vals: number[]) => vals.map(v => `${v.toFixed(1)}`) },
  ], [])

  const scales = useMemo((): uPlot.Scales => ({
    x: { time: true },
    y: { auto: true },
  }), [])

  useUPlotChart({
    containerRef: chartRef,
    data: uPlotData,
    series: uPlotSeries,
    height: 144,
    axes,
    scales,
  })

  const hasData = uPlotData[0].length > 0

  return (
    <div className={className}>
      <div className="flex items-center justify-between text-xs text-muted-foreground uppercase tracking-wider mb-1">
        <span>
          Optical Power — {interfaceName} / channel {channelIndex}
        </span>
        {isLoading && <Loader2 className="h-3 w-3 animate-spin" />}
      </div>
      {hasData ? (
        <>
          <div ref={chartRef} className="h-36" />
          <div className="mt-2 flex items-center gap-4 text-[11px] text-muted-foreground">
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-2 w-2 rounded-full" style={{ background: inputColor }} />
              Input (dBm)
            </span>
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-2 w-2 rounded-full" style={{ background: outputColor }} />
              Output (dBm)
            </span>
          </div>
        </>
      ) : (
        <div className="text-xs text-muted-foreground/60 py-6 text-center">
          {isLoading ? 'Loading…' : 'No samples in the selected window'}
        </div>
      )}
    </div>
  )
}
