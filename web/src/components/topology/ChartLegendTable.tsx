import { useState, useMemo, useRef, useEffect } from 'react'
import { ArrowUp, ArrowDown } from 'lucide-react'
import type { UseChartLegendReturn } from '@/hooks/use-chart-legend'
import type { ChartLegendSeries } from './ChartLegend'

type SortField = 'name' | 'value' | 'max'
type SortDir = 'asc' | 'desc'

/** Extract a numeric value from a formatted string like "1.23 ms" or "—" */
function parseNumeric(s: string | undefined): number | null {
  if (!s || s === '—') return null
  const n = parseFloat(s)
  return isNaN(n) ? null : n
}

interface ChartLegendTableProps {
  series: ChartLegendSeries[]
  legend: UseChartLegendReturn
  /** Map from series key to display value (formatted string) */
  values?: Map<string, string>
  /** Map from series key to max value (formatted string) */
  maxValues?: Map<string, string>
  /** Formatted timestamp to show in the header (hovered or latest) */
  hoveredTime?: string
}

export function ChartLegendTable({ series, legend, values, maxValues, hoveredTime }: ChartLegendTableProps) {
  const [sortField, setSortField] = useState<SortField | null>(null)
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      if (sortDir === 'desc') setSortDir('asc')
      else { setSortField(null); setSortDir('desc') }
    } else {
      setSortField(field)
      setSortDir(field === 'name' ? 'asc' : 'desc')
    }
  }

  const sortedSeries = useMemo(() => {
    if (!sortField) return series
    return [...series].sort((a, b) => {
      let cmp: number
      if (sortField === 'name') {
        cmp = a.label.localeCompare(b.label)
      } else {
        const map = sortField === 'max' ? maxValues : values
        const aVal = parseNumeric(map?.get(a.key))
        const bVal = parseNumeric(map?.get(b.key))
        if (aVal === null && bVal === null) cmp = 0
        else if (aVal === null) cmp = 1
        else if (bVal === null) cmp = -1
        else cmp = aVal - bVal
      }
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [series, sortField, sortDir, values, maxValues])

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return null
    return sortDir === 'asc'
      ? <ArrowUp className="h-2.5 w-2.5 inline-block ml-0.5" />
      : <ArrowDown className="h-2.5 w-2.5 inline-block ml-0.5" />
  }

  return (
    <div className="flex flex-col text-xs px-2 pt-1 pb-2">
      <div className="flex items-center px-1 mb-1">
        <span
          className="text-xs text-muted-foreground flex-1 min-w-0 cursor-pointer select-none hover:text-foreground"
          onClick={() => handleSort('name')}
        >
          Name<SortIcon field="name" />
        </span>
        {maxValues && (
          <span
            className="text-xs text-muted-foreground w-28 text-right whitespace-nowrap cursor-pointer select-none hover:text-foreground"
            onClick={() => handleSort('max')}
          >
            Peak<SortIcon field="max" />
          </span>
        )}
        <span
          className="text-xs text-muted-foreground w-28 text-right whitespace-nowrap cursor-pointer select-none hover:text-foreground"
          onClick={() => handleSort('value')}
        >
          {hoveredTime ?? 'Value'}<SortIcon field="value" />
        </span>
      </div>
      <div className="space-y-0.5">
        {sortedSeries.map((s) => {
          const opacity = legend.getOpacity(s.key)
          const isSelected = legend.selectedSeries.size > 0 && legend.selectedSeries.has(s.key)
          const value = values?.get(s.key)
          const maxValue = maxValues?.get(s.key)
          return (
            <LegendRow
              key={s.key}
              series={s}
              opacity={opacity}
              isSelected={isSelected}
              value={value}
              maxValue={maxValue}
              hasMaxColumn={!!maxValues}
              onClick={(e) => legend.handleClick(s.key, e)}
              onMouseEnter={() => legend.handleMouseEnter(s.key)}
              onMouseLeave={legend.handleMouseLeave}
            />
          )
        })}
      </div>
    </div>
  )
}

function LegendRow({
  series: s,
  opacity,
  isSelected,
  value,
  maxValue,
  hasMaxColumn,
  onClick,
  onMouseEnter,
  onMouseLeave,
}: {
  series: ChartLegendSeries
  opacity: number
  isSelected: boolean
  value?: string
  maxValue?: string
  hasMaxColumn: boolean
  onClick: (e: React.MouseEvent) => void
  onMouseEnter: () => void
  onMouseLeave: () => void
}) {
  const ref = useRef<HTMLDivElement>(null)

  // Scroll into view when selected (e.g. by clicking chart line)
  useEffect(() => {
    if (isSelected && ref.current) {
      ref.current.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
    }
  }, [isSelected])

  return (
    <div
      ref={ref}
      className={`flex items-center px-1 py-0.5 rounded cursor-pointer transition-colors ${
        isSelected
          ? 'bg-foreground/10 ring-1 ring-foreground/20'
          : 'hover:bg-muted/50'
      }`}
      style={{ opacity }}
      onClick={onClick}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="flex items-center gap-1.5 min-w-0 flex-1">
        {s.dashed ? (
          <span
            className="w-3 h-0.5 flex-shrink-0"
            style={{
              backgroundImage: `repeating-linear-gradient(90deg, ${s.color} 0, ${s.color} 2px, transparent 2px, transparent 4px)`,
              backgroundSize: '4px 1px',
            }}
          />
        ) : (
          <span
            className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
            style={{ backgroundColor: s.color }}
          />
        )}
        <span className={`font-mono truncate ${isSelected ? 'text-foreground font-medium' : 'text-foreground'}`}>{s.label}</span>
      </div>
      {hasMaxColumn && (
        <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-28 text-right">
          {maxValue ?? '—'}
        </span>
      )}
      <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-28 text-right">
        {value ?? '—'}
      </span>
    </div>
  )
}
