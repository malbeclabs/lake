import type { UseChartLegendReturn } from '@/hooks/use-chart-legend'

export interface ChartLegendSeries {
  key: string
  color: string
  label: string
  dashed?: boolean
}

interface ChartLegendProps {
  series: ChartLegendSeries[]
  legend: UseChartLegendReturn
}

function ChartLegendItem({
  color,
  label,
  dashed = false,
  opacity,
  onMouseEnter,
  onMouseLeave,
  onClick,
}: {
  color: string
  label: string
  dashed?: boolean
  opacity: number
  onMouseEnter: () => void
  onMouseLeave: () => void
  onClick: (e: React.MouseEvent) => void
}) {
  return (
    <button
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      onClick={onClick}
      className="flex items-center gap-1 px-1.5 py-0.5 rounded text-xs hover:bg-[var(--muted)]/50 transition-opacity"
      style={{ opacity }}
    >
      {dashed ? (
        <span
          className="w-3 h-0.5"
          style={{
            backgroundImage: `repeating-linear-gradient(90deg, ${color} 0, ${color} 2px, transparent 2px, transparent 4px)`,
            backgroundSize: '4px 1px',
          }}
        />
      ) : (
        <span className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: color }} />
      )}
      {label}
    </button>
  )
}

export function ChartLegend({ series, legend }: ChartLegendProps) {
  return (
    <div className="flex justify-center gap-1 text-xs mt-1 flex-wrap">
      {series.map((s) => (
        <ChartLegendItem
          key={s.key}
          color={s.color}
          label={s.label}
          dashed={s.dashed}
          opacity={legend.getOpacity(s.key)}
          onMouseEnter={() => legend.handleMouseEnter(s.key)}
          onMouseLeave={legend.handleMouseLeave}
          onClick={(e) => legend.handleClick(s.key, e)}
        />
      ))}
    </div>
  )
}
