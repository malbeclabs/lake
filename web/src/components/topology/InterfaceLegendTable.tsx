import { useState, useRef, useMemo } from 'react'
import { Search, X, ChevronUp, ChevronDown } from 'lucide-react'
import type { UseChartLegendReturn } from '@/hooks/use-chart-legend'

export interface InterfaceValues {
  avgIn: number
  avgOut: number
  peakIn: number
  peakOut: number
}

interface InterfaceLegendTableProps {
  interfaces: string[]
  colors: string[]
  legend: UseChartLegendReturn
  visibleSeries: Set<string>
  latestValues?: Map<string, InterfaceValues>
  formatValue?: (v: number) => string
  /** Optional display labels for interfaces */
  interfaceLabels?: Map<string, string>
  /** Which view is active — controls which columns are shown */
  trafficView?: 'avg' | 'peak'
}

export function InterfaceLegendTable({
  interfaces,
  colors,
  legend,
  visibleSeries,
  latestValues,
  formatValue = (v) => v.toLocaleString(),
  interfaceLabels,
  trafficView = 'avg',
}: InterfaceLegendTableProps) {
  const [searchExpanded, setSearchExpanded] = useState(false)
  const [searchText, setSearchText] = useState('')
  const searchInputRef = useRef<HTMLInputElement>(null)
  const [sortBy, setSortBy] = useState<'name' | 'value'>('name')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')

  const filteredInterfaces = useMemo(() => {
    if (!searchText) return interfaces
    // Convert wildcard pattern to regex
    const pattern = searchText
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      .replace(/\\\*/g, '.*')
    const matchText = (intf: string) => {
      const label = interfaceLabels?.get(intf) ?? intf
      return `${intf} ${label}`
    }
    try {
      const re = new RegExp(pattern, 'i')
      return interfaces.filter((intf) => re.test(matchText(intf)))
    } catch {
      return interfaces.filter((intf) =>
        matchText(intf).toLowerCase().includes(searchText.toLowerCase())
      )
    }
  }, [interfaces, searchText, interfaceLabels])

  const sortedInterfaces = useMemo(() => {
    const sorted = [...filteredInterfaces]
    if (sortBy === 'name') {
      sorted.sort((a, b) => {
        const cmp = a.localeCompare(b)
        return sortDir === 'asc' ? cmp : -cmp
      })
    } else {
      sorted.sort((a, b) => {
        const va = latestValues?.get(a)
        const vb = latestValues?.get(b)
        const sumA = va ? va.avgIn + va.avgOut : 0
        const sumB = vb ? vb.avgIn + vb.avgOut : 0
        return sortDir === 'asc' ? sumA - sumB : sumB - sumA
      })
    }
    return sorted
  }, [filteredInterfaces, sortBy, sortDir, latestValues])

  const visibleCount = [...visibleSeries].filter((k) => interfaces.includes(k)).length

  return (
    <div className="flex flex-col text-xs">
      {/* Header */}
      <div className="px-2 pt-2">
        <div className="flex items-center gap-2 mb-1.5">
          <div className="text-xs font-medium whitespace-nowrap">
            Interfaces ({visibleCount}/{interfaces.length})
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
                  className="absolute right-1 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground z-10"
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
              aria-label="Search interfaces"
            >
              <Search className="h-3.5 w-3.5" />
            </button>
          )}
          <button
            onClick={() => {
              const top10 = [...interfaces]
                .sort((a, b) => {
                  const va = latestValues?.get(a)
                  const vb = latestValues?.get(b)
                  return (vb ? vb.avgIn + vb.avgOut : 0) - (va ? va.avgIn + va.avgOut : 0)
                })
                .slice(0, 10)
              legend.setSelectedSeries(new Set(top10))
            }}
            className="text-xs text-muted-foreground hover:text-foreground whitespace-nowrap"
          >
            Top 10
          </button>
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
        {/* Column headers */}
        <div className="flex items-center px-1 mb-1">
          <button
            onClick={() => { setSortBy('name'); setSortDir(sortBy === 'name' ? (sortDir === 'asc' ? 'desc' : 'asc') : 'asc') }}
            className="flex items-center gap-0.5 text-xs text-muted-foreground hover:text-foreground flex-1 min-w-0"
          >
            Name
            {sortBy === 'name' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
          </button>
          <button
            onClick={() => { setSortBy('value'); setSortDir(sortBy === 'value' ? (sortDir === 'asc' ? 'desc' : 'asc') : 'desc') }}
            className="flex items-center justify-end gap-0.5 text-xs text-muted-foreground hover:text-foreground whitespace-nowrap w-48"
          >
            In / Out
            {sortBy === 'value' && (sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />)}
          </button>
        </div>
      </div>
      {/* Scrollable items */}
      <div className="max-h-48 overflow-y-auto px-2 pb-2">
        <div className="space-y-0.5">
          {sortedInterfaces.map((intf) => {
            const colorIndex = interfaces.indexOf(intf)
            const color = colors[colorIndex % colors.length]
            const isVisible = visibleSeries.has(intf)
            const lv = latestValues?.get(intf)
            return (
              <div
                key={intf}
                className={`flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors ${
                  isVisible ? '' : 'opacity-40'
                }`}
                onClick={(e) => legend.handleClick(intf, e)}
                onMouseEnter={() => legend.handleMouseEnter(intf)}
                onMouseLeave={legend.handleMouseLeave}
              >
                <div className="flex items-center gap-1.5 min-w-0 flex-1">
                  <span
                    className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                    style={{ backgroundColor: color }}
                  />
                  <span className="font-mono text-foreground truncate">{interfaceLabels?.get(intf) ?? intf}</span>
                </div>
                <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-48 text-right">
                  {lv ? `${formatValue(trafficView === 'avg' ? lv.avgIn : lv.peakIn)} / ${formatValue(trafficView === 'avg' ? lv.avgOut : lv.peakOut)}` : '—'}
                </span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
