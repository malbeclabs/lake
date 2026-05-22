import { Fragment, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { fetchDeviceOptics, type OpticsLane, type OpticsSeverity } from '@/lib/api'
import type { TimeRange } from '@/components/topology/utils'
import { DeviceOpticsChart } from './DeviceOpticsChart'

interface DeviceOpticsPanelProps {
  devicePk: string
  timeRange: TimeRange
  className?: string
}

const severityBadge: Record<OpticsSeverity, string> = {
  ok: 'bg-green-500/10 text-green-600 dark:text-green-400',
  warning: 'bg-amber-500/10 text-amber-600 dark:text-amber-400',
  critical: 'bg-red-500/10 text-red-600 dark:text-red-400',
  unknown: 'bg-muted/60 text-muted-foreground',
}

const severityDot: Record<OpticsSeverity, string> = {
  ok: 'bg-green-500',
  warning: 'bg-amber-500',
  critical: 'bg-red-500',
  unknown: 'bg-muted-foreground/40',
}

function formatDbm(v: number): string {
  return `${v.toFixed(2)} dBm`
}

function formatMa(v: number): string {
  return `${v.toFixed(2)} mA`
}

function laneKey(lane: OpticsLane): string {
  return `${lane.interface_name}:${lane.channel_index}`
}

export function DeviceOpticsPanel({ devicePk, timeRange, className }: DeviceOpticsPanelProps) {
  const [expanded, setExpanded] = useState<string | null>(null)

  const { data } = useQuery({
    queryKey: ['deviceOptics', devicePk],
    queryFn: () => fetchDeviceOptics(devicePk),
  })

  if (!data || !data.lanes || data.lanes.length === 0) {
    return null
  }

  const { lanes, summary } = data

  return (
    <div className={className}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2 text-xs text-muted-foreground uppercase tracking-wider">
          <span>Optics</span>
          <span className="text-muted-foreground/60 normal-case tracking-normal">
            {summary.total} {summary.total === 1 ? 'lane' : 'lanes'}
          </span>
        </div>
        <div className="flex items-center gap-2 text-[11px]">
          {summary.critical > 0 && (
            <span className={`px-2 py-0.5 rounded-full ${severityBadge.critical}`}>
              {summary.critical} critical
            </span>
          )}
          {summary.warning > 0 && (
            <span className={`px-2 py-0.5 rounded-full ${severityBadge.warning}`}>
              {summary.warning} warning
            </span>
          )}
          {summary.ok > 0 && (
            <span className={`px-2 py-0.5 rounded-full ${severityBadge.ok}`}>
              {summary.ok} ok
            </span>
          )}
          {summary.unknown > 0 && (
            <span className={`px-2 py-0.5 rounded-full ${severityBadge.unknown}`}>
              {summary.unknown} unknown
            </span>
          )}
        </div>
      </div>

      <div className="rounded-lg border border-border overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-muted/40 text-xs text-muted-foreground uppercase tracking-wider">
            <tr>
              <th className="text-left font-normal px-3 py-2 w-6"></th>
              <th className="text-left font-normal px-3 py-2">Interface</th>
              <th className="text-left font-normal px-3 py-2">Ch</th>
              <th className="text-right font-normal px-3 py-2">Input</th>
              <th className="text-right font-normal px-3 py-2">Output</th>
              <th className="text-right font-normal px-3 py-2">Bias</th>
              <th className="text-left font-normal px-3 py-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {lanes.map(lane => {
              const key = laneKey(lane)
              const isOpen = expanded === key
              return (
                <Fragment key={key}>
                  <tr
                    className="border-t border-border cursor-pointer hover:bg-muted/30"
                    onClick={() => setExpanded(isOpen ? null : key)}
                  >
                    <td className="px-3 py-2 text-muted-foreground">
                      {isOpen ? (
                        <ChevronDown className="h-3.5 w-3.5" />
                      ) : (
                        <ChevronRight className="h-3.5 w-3.5" />
                      )}
                    </td>
                    <td className="px-3 py-2 font-mono">{lane.interface_name}</td>
                    <td className="px-3 py-2 font-mono text-muted-foreground">{lane.channel_index}</td>
                    <td className={`px-3 py-2 text-right font-mono ${lane.input_severity === 'critical' ? 'text-red-600 dark:text-red-400' : lane.input_severity === 'warning' ? 'text-amber-600 dark:text-amber-400' : ''}`}>
                      {formatDbm(lane.input_power)}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono ${lane.output_severity === 'critical' ? 'text-red-600 dark:text-red-400' : lane.output_severity === 'warning' ? 'text-amber-600 dark:text-amber-400' : ''}`}>
                      {formatDbm(lane.output_power)}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono ${lane.bias_severity === 'critical' ? 'text-red-600 dark:text-red-400' : lane.bias_severity === 'warning' ? 'text-amber-600 dark:text-amber-400' : ''}`}>
                      {formatMa(lane.laser_bias_current)}
                    </td>
                    <td className="px-3 py-2">
                      <span className="inline-flex items-center gap-1.5">
                        <span className={`inline-block h-2 w-2 rounded-full ${severityDot[lane.overall_severity]}`} />
                        <span className="capitalize text-xs">{lane.overall_severity}</span>
                      </span>
                    </td>
                  </tr>
                  {isOpen && (
                    <tr className="border-border bg-muted/10">
                      <td colSpan={7} className="px-3 py-3">
                        <DeviceOpticsChart
                          devicePk={devicePk}
                          interfaceName={lane.interface_name}
                          channelIndex={lane.channel_index}
                          timeRange={timeRange}
                        />
                        {lane.thresholds && (
                          <div className="mt-3 grid grid-cols-3 gap-3 text-[11px] text-muted-foreground">
                            <ThresholdSummary
                              label="Input"
                              unit="dBm"
                              warnLo={lane.thresholds.input_warning_lower}
                              warnHi={lane.thresholds.input_warning_upper}
                              critLo={lane.thresholds.input_critical_lower}
                              critHi={lane.thresholds.input_critical_upper}
                            />
                            <ThresholdSummary
                              label="Output"
                              unit="dBm"
                              warnLo={lane.thresholds.output_warning_lower}
                              warnHi={lane.thresholds.output_warning_upper}
                              critLo={lane.thresholds.output_critical_lower}
                              critHi={lane.thresholds.output_critical_upper}
                            />
                            <ThresholdSummary
                              label="Bias"
                              unit="mA"
                              warnLo={lane.thresholds.bias_warning_lower}
                              warnHi={lane.thresholds.bias_warning_upper}
                              critLo={lane.thresholds.bias_critical_lower}
                              critHi={lane.thresholds.bias_critical_upper}
                            />
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

interface ThresholdSummaryProps {
  label: string
  unit: string
  warnLo?: number
  warnHi?: number
  critLo?: number
  critHi?: number
}

function ThresholdSummary({ label, unit, warnLo, warnHi, critLo, critHi }: ThresholdSummaryProps) {
  const fmt = (v: number | undefined) => (v == null ? '—' : v.toFixed(2))
  return (
    <div>
      <div className="font-medium text-foreground/80 mb-0.5">{label}</div>
      <div>
        Warn: {fmt(warnLo)} to {fmt(warnHi)} {unit}
      </div>
      <div>
        Crit: {fmt(critLo)} to {fmt(critHi)} {unit}
      </div>
    </div>
  )
}
