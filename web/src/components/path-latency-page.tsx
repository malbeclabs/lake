import { useMemo, useCallback, useState, useRef, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Loader2, Route, Download, ArrowRight, ChevronDown, X, Search, MapPin, Filter } from 'lucide-react'
import { fetchMetroConnectivity, fetchMetroPathLatency, fetchMetroPathDetail, fetchFieldValues } from '@/lib/api'
import type { MetroPathLatency, MetroPathDetailResponse, PathOptimizeMode } from '@/lib/api'
import { ErrorState } from '@/components/ui/error-state'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'
import { PageHeader } from '@/components/page-header'
import { cn } from '@/lib/utils'

const DEBOUNCE_MS = 150

// Parse metro filters from URL
function parseMetroFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

function MetroInlineFilter({ onCommit }: { onCommit: (metro: string) => void }) {
  const [query, setQuery] = useState('')
  const [debouncedQuery, setDebouncedQuery] = useState('')
  const [isFocused, setIsFocused] = useState(false)
  const [selectedIndex, setSelectedIndex] = useState(-1)
  const inputRef = useRef<HTMLInputElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(query), DEBOUNCE_MS)
    return () => clearTimeout(timer)
  }, [query])

  const { data: metroValues, isLoading } = useQuery({
    queryKey: ['field-values', 'devices', 'metro'],
    queryFn: () => fetchFieldValues('devices', 'metro'),
    staleTime: 60000,
  })

  const filteredValues = useMemo(() => {
    if (!metroValues) return []
    if (!query) return metroValues
    return metroValues.filter(v => v.toLowerCase().includes(query.toLowerCase()))
  }, [metroValues, query])

  const items = isFocused ? filteredValues : []

  useEffect(() => {
    setSelectedIndex(-1)
  }, [debouncedQuery])

  const commit = useCallback((value: string) => {
    onCommit(value)
    setQuery('')
    inputRef.current?.focus()
  }, [onCommit])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    const isDropdownOpen = isFocused && items.length > 0
    switch (e.key) {
      case 'ArrowDown':
        if (isDropdownOpen) { e.preventDefault(); setSelectedIndex(prev => Math.min(prev + 1, items.length - 1)) }
        break
      case 'ArrowUp':
        if (isDropdownOpen) { e.preventDefault(); setSelectedIndex(prev => Math.max(prev - 1, -1)) }
        break
      case 'Enter': {
        e.preventDefault()
        const idx = selectedIndex >= 0 ? selectedIndex : 0
        if (idx < items.length) commit(items[idx])
        break
      }
      case 'Escape':
        e.preventDefault()
        setQuery('')
        inputRef.current?.blur()
        break
    }
  }, [items, selectedIndex, commit, isFocused])

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) setIsFocused(false)
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const showDropdown = isFocused && items.length > 0

  return (
    <div ref={containerRef} className="relative">
      <div className="flex items-center gap-1.5 px-2 py-1 text-xs border border-border rounded-md bg-background hover:bg-muted/50 focus-within:ring-1 focus-within:ring-ring transition-colors">
        <Search className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => setIsFocused(true)}
          placeholder="Filter by metro..."
          className="w-28 bg-transparent border-0 focus:outline-none placeholder:text-muted-foreground text-xs"
        />
        {isLoading && <Loader2 className="h-3 w-3 text-muted-foreground animate-spin" />}
      </div>

      {showDropdown && (
        <div className="absolute top-full left-0 mt-1 w-48 max-h-64 overflow-y-auto bg-card border border-border rounded-lg shadow-lg z-40">
          {!query && (
            <div className="px-3 py-1.5 text-xs text-muted-foreground border-b border-border flex items-center gap-1">
              <Filter className="h-3 w-3" />
              Select metro
            </div>
          )}
          {items.map((value, index) => (
            <button
              key={value}
              onClick={() => commit(value)}
              className={cn(
                'w-full flex items-center gap-2 px-3 py-2 text-left text-xs hover:bg-muted transition-colors',
                index === selectedIndex && 'bg-muted'
              )}
            >
              <MapPin className="h-3 w-3 text-muted-foreground flex-shrink-0" />
              <span className="truncate">{value}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// Color classes for improvement
const STRENGTH_COLORS = {
  strong: {
    bg: 'bg-green-100 dark:bg-green-900/40',
    text: 'text-green-700 dark:text-green-400',
    hover: 'hover:bg-green-200 dark:hover:bg-green-900/60',
  },
  medium: {
    bg: 'bg-yellow-100 dark:bg-yellow-900/40',
    text: 'text-yellow-700 dark:text-yellow-400',
    hover: 'hover:bg-yellow-200 dark:hover:bg-yellow-900/60',
  },
  weak: {
    bg: 'bg-red-100 dark:bg-red-900/40',
    text: 'text-red-700 dark:text-red-400',
    hover: 'hover:bg-red-200 dark:hover:bg-red-900/60',
  },
  none: {
    bg: 'bg-muted/50',
    text: 'text-muted-foreground',
    hover: 'hover:bg-muted',
  },
}

// Get improvement color class based on percentage
// Positive = green (DZ is faster), slightly negative = yellow, very negative = red
function getImprovementColor(pct: number | null): { bg: string; text: string; hover: string } {
  if (pct === null) return STRENGTH_COLORS.none
  if (pct > 0) return STRENGTH_COLORS.strong    // Any positive = green
  if (pct >= -10) return STRENGTH_COLORS.medium // 0% to -10% = yellow
  return STRENGTH_COLORS.weak                    // < -10% = red
}

// Path latency cell component for the matrix
function PathLatencyCell({
  pathLatency,
  onClick,
  isSelected,
}: {
  pathLatency: MetroPathLatency | null
  onClick: () => void
  isSelected: boolean
}) {
  if (!pathLatency) {
    // Diagonal cell or no data
    return (
      <div className="w-full h-full flex items-center justify-center bg-muted/30">
        <span className="text-muted-foreground text-xs">-</span>
      </div>
    )
  }

  const colors = getImprovementColor(pathLatency.improvementPct)
  const hasInternet = pathLatency.internetLatencyMs > 0

  return (
    <button
      onClick={onClick}
      className={`w-full h-full flex flex-col items-center justify-center p-1 transition-colors cursor-pointer ${colors.bg} ${colors.hover} ${isSelected ? 'ring-2 ring-accent ring-inset' : ''}`}
      title={`${pathLatency.fromMetroCode} → ${pathLatency.toMetroCode}: ${pathLatency.pathLatencyMs.toFixed(1)}ms (${pathLatency.hopCount} hops)${hasInternet ? ` vs Internet ${pathLatency.internetLatencyMs.toFixed(1)}ms` : ''}`}
    >
      <span className={`text-sm font-medium ${colors.text}`}>
        {pathLatency.pathLatencyMs.toFixed(1)}
      </span>
      <span className="text-[10px] text-muted-foreground">
        {pathLatency.hopCount}h
      </span>
    </button>
  )
}

// Detail panel for path latency with breakdown
function PathLatencyDetail({
  fromCode,
  toCode,
  pathLatency,
  pathDetail,
  isLoadingDetail,
  onClose,
}: {
  fromCode: string
  toCode: string
  pathLatency: MetroPathLatency
  pathDetail: MetroPathDetailResponse | null
  isLoadingDetail: boolean
  onClose: () => void
}) {
  const colors = getImprovementColor(pathLatency.improvementPct)
  const hasInternet = pathLatency.internetLatencyMs > 0

  return (
    <div className="p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-medium flex items-center gap-2">
          <span>{fromCode}</span>
          <ArrowRight className="h-4 w-4 text-muted-foreground" />
          <span>{toCode}</span>
        </h3>
        <button
          onClick={onClose}
          className="text-muted-foreground hover:text-foreground"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-2 mb-4">
        <div className="rounded-lg p-2 bg-muted">
          <div className="text-[10px] text-muted-foreground mb-0.5">DZ Latency</div>
          <div className="text-lg font-bold">{pathLatency.pathLatencyMs.toFixed(1)}ms</div>
        </div>
        <div className="rounded-lg p-2 bg-muted">
          <div className="text-[10px] text-muted-foreground mb-0.5">Hops</div>
          <div className="text-lg font-bold">{pathLatency.hopCount}</div>
        </div>
        {pathLatency.bottleneckBwGbps > 0 && (
          <div className="rounded-lg p-2 bg-muted">
            <div className="text-[10px] text-muted-foreground mb-0.5">Bottleneck</div>
            <div className="text-lg font-bold">{pathLatency.bottleneckBwGbps.toFixed(0)} Gbps</div>
          </div>
        )}
      </div>

      {/* Internet comparison */}
      {hasInternet && pathLatency.improvementPct !== null && (
        <div className={`rounded-lg p-3 ${colors.bg} mb-4`}>
          <div className="text-xs text-muted-foreground mb-1">vs Internet ({pathLatency.internetLatencyMs.toFixed(1)}ms)</div>
          <div className={`text-xl font-bold ${colors.text}`}>
            {pathLatency.improvementPct > 0 ? '+' : ''}{pathLatency.improvementPct.toFixed(1)}% {pathLatency.improvementPct > 0 ? 'faster' : 'slower'}
          </div>
        </div>
      )}

      {/* Path breakdown */}
      <div className="text-xs text-muted-foreground uppercase tracking-wider mb-2">Path Breakdown</div>
      {isLoadingDetail ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading path details...
        </div>
      ) : pathDetail && pathDetail.hops.length > 0 ? (
        <div className="space-y-1">
          {pathDetail.hops.map((hop, idx) => (
            <div key={idx} className="flex items-center gap-2 text-sm">
              <span className="font-mono text-xs text-muted-foreground w-8">{hop.metroCode}</span>
              <span className="font-medium">{hop.deviceCode}</span>
              {idx < pathDetail.hops.length - 1 && (
                <span className="text-muted-foreground ml-auto">
                  → {hop.linkLatency.toFixed(2)}ms
                </span>
              )}
            </div>
          ))}
        </div>
      ) : (
        <div className="text-sm text-muted-foreground">No path details available</div>
      )}
    </div>
  )
}

export function PathLatencyPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const optimizeParam = searchParams.get('optimize') as PathOptimizeMode | null
  const optimizeMode: PathOptimizeMode = optimizeParam || 'latency'

  // Read selection from URL params (metro codes for readability)
  const fromCodeParam = searchParams.get('from')
  const toCodeParam = searchParams.get('to')

  // Get metro filters from URL
  const metroFilters = useMemo(() => {
    return parseMetroFilters(searchParams.get('metros') || '')
  }, [searchParams])

  // Add a metro filter
  const addMetroFilter = useCallback((metro: string) => {
    setSearchParams(prev => {
      const current = parseMetroFilters(prev.get('metros') || '')
      if (!current.includes(metro)) {
        prev.set('metros', [...current, metro].join(','))
      }
      return prev
    })
  }, [setSearchParams])

  // Remove a metro filter
  const removeMetroFilter = useCallback((metro: string) => {
    setSearchParams(prev => {
      const current = parseMetroFilters(prev.get('metros') || '')
      const newFilters = current.filter(m => m !== metro)
      if (newFilters.length === 0) {
        prev.delete('metros')
      } else {
        prev.set('metros', newFilters.join(','))
      }
      return prev
    })
  }, [setSearchParams])

  // Clear all metro filters
  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      prev.delete('metros')
      return prev
    })
  }, [setSearchParams])

  const queryClient = useQueryClient()

  // Fetch metro connectivity for the matrix structure (metros list)
  const { data: connectivityData, isLoading: connectivityLoading, error: connectivityError, isFetching: connectivityFetching } = useQuery({
    queryKey: ['metro-connectivity'],
    queryFn: fetchMetroConnectivity,
    staleTime: 60000,
    retry: 2,
  })

  // Derive selectedCell from URL params by looking up metro PKs (case-insensitive)
  const selectedCell = useMemo(() => {
    if (!fromCodeParam || !toCodeParam || !connectivityData) return null
    const fromUpper = fromCodeParam.toUpperCase()
    const toUpper = toCodeParam.toUpperCase()
    const fromMetro = connectivityData.metros.find(m => m.code.toUpperCase() === fromUpper)
    const toMetro = connectivityData.metros.find(m => m.code.toUpperCase() === toUpper)
    if (!fromMetro || !toMetro) return null
    return { from: fromMetro.pk, to: toMetro.pk }
  }, [fromCodeParam, toCodeParam, connectivityData])

  // Update selection in URL params (uppercase for consistency)
  const setSelectedCell = useCallback((cell: { from: string; to: string } | null, fromCode?: string, toCode?: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (cell && fromCode && toCode) {
        next.set('from', fromCode.toUpperCase())
        next.set('to', toCode.toUpperCase())
      } else {
        next.delete('from')
        next.delete('to')
      }
      return next
    })
  }, [setSearchParams])

  // Delay showing loading spinner to avoid flash on fast loads
  const showLoading = useDelayedLoading(connectivityLoading)

  // Fetch path latency data
  const { data: pathLatencyData, isLoading: pathLatencyLoading } = useQuery({
    queryKey: ['metro-path-latency', optimizeMode],
    queryFn: () => fetchMetroPathLatency(optimizeMode),
    staleTime: 60000,
  })

  // Build path latency lookup map (by metro PKs)
  const pathLatencyMap = useMemo(() => {
    if (!pathLatencyData) return new Map<string, MetroPathLatency>()
    const map = new Map<string, MetroPathLatency>()
    for (const pl of pathLatencyData.paths) {
      map.set(`${pl.fromMetroPK}:${pl.toMetroPK}`, pl)
    }
    return map
  }, [pathLatencyData])

  // Get selected path latency
  const selectedPathLatency = useMemo(() => {
    if (!selectedCell) return null
    return pathLatencyMap.get(`${selectedCell.from}:${selectedCell.to}`) ?? null
  }, [selectedCell, pathLatencyMap])

  // Fetch path detail when a cell is selected
  const { data: pathDetailData, isLoading: pathDetailLoading } = useQuery({
    queryKey: ['metro-path-detail', selectedPathLatency?.fromMetroCode, selectedPathLatency?.toMetroCode, optimizeMode],
    queryFn: () => {
      if (!selectedPathLatency) return Promise.resolve(null)
      return fetchMetroPathDetail(selectedPathLatency.fromMetroCode, selectedPathLatency.toMetroCode, optimizeMode)
    },
    staleTime: 60000,
    enabled: selectedPathLatency !== null,
  })

  // Filter metros based on metro filters (must be before early returns to preserve hook order)
  const metros = useMemo(() => {
    if (!connectivityData) return []
    if (metroFilters.length === 0) return connectivityData.metros
    return connectivityData.metros.filter(m =>
      metroFilters.some(f => m.code.toLowerCase() === f.toLowerCase())
    )
  }, [connectivityData, metroFilters])

  // Export to CSV
  const handleExport = () => {
    if (!pathLatencyData) return

    const headers = ['From Metro', 'To Metro', 'Path Latency (ms)', 'Hop Count', 'Internet Latency (ms)', 'Improvement (%)', 'Bottleneck BW (Gbps)']
    const rows = pathLatencyData.paths.map(pl => [
      pl.fromMetroCode,
      pl.toMetroCode,
      pl.pathLatencyMs.toFixed(1),
      pl.hopCount.toString(),
      pl.internetLatencyMs > 0 ? pl.internetLatencyMs.toFixed(1) : '-',
      pl.improvementPct !== null ? pl.improvementPct.toFixed(1) : '-',
      pl.bottleneckBwGbps > 0 ? pl.bottleneckBwGbps.toFixed(1) : '-',
    ])

    const csv = [headers.join(','), ...rows.map(row => row.join(','))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'path-latency.csv'
    a.click()
    URL.revokeObjectURL(url)
  }

  if (showLoading) {
    return (
      <div className="flex-1 flex items-center justify-center bg-background">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (connectivityError || connectivityData?.error) {
    const errorMessage = connectivityData?.error || (connectivityError instanceof Error ? connectivityError.message : 'Unknown error')
    return (
      <div className="flex-1 flex items-center justify-center bg-background">
        <ErrorState
          title="Failed to load metro data"
          message={errorMessage}
          onRetry={() => queryClient.invalidateQueries({ queryKey: ['metro-connectivity'] })}
          retrying={connectivityFetching}
        />
      </div>
    )
  }

  if (!connectivityData || connectivityData.metros.length === 0) {
    // Don't show "no data" message while still loading (before delay threshold)
    if (connectivityLoading) {
      return <div className="flex-1 bg-background" />
    }
    return (
      <div className="flex-1 flex items-center justify-center bg-background">
        <div className="text-muted-foreground">No metros found</div>
      </div>
    )
  }

  return (
    <div className="flex-1 flex flex-col bg-background overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4">
        <PageHeader
          icon={Route}
          title="Path Latency"
          actions={
            <div className="flex items-center gap-3">
              <div className="relative">
                <select
                  value={optimizeMode}
                  onChange={(e) => {
                    const newMode = e.target.value as PathOptimizeMode
                    setSearchParams({ optimize: newMode })
                  }}
                  className="appearance-none border border-border bg-background hover:bg-muted/50 rounded-md px-3 py-1.5 pr-8 text-sm cursor-pointer transition-colors"
                >
                  <option value="latency">Optimize: Latency</option>
                  <option value="hops">Optimize: Hops</option>
                  <option value="bandwidth">Optimize: Bandwidth</option>
                </select>
                <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
              </div>
              <button
                onClick={handleExport}
                className="flex items-center gap-2 px-3 py-1.5 text-sm border border-border bg-background hover:bg-muted/50 rounded-md transition-colors"
              >
                <Download className="h-4 w-4" />
                Export CSV
              </button>
            </div>
          }
        />

        <p className="mt-3 text-sm text-muted-foreground">
          Compares end-to-end path latency across the DZ network against public internet latency.
        </p>

        {/* Filter bar */}
        <div className="flex items-center gap-2 flex-wrap mt-4">
          <MetroInlineFilter onCommit={addMetroFilter} />

          {metroFilters.map((metro) => (
            <button
              key={metro}
              onClick={() => removeMetroFilter(metro)}
              className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
            >
              <MapPin className="h-3 w-3" />
              {metro}
              <X className="h-3 w-3" />
            </button>
          ))}

          {metroFilters.length > 1 && (
            <button
              onClick={clearAllFilters}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              Clear all
            </button>
          )}
        </div>

        {/* Summary stats */}
        {pathLatencyData && (
          <div className="flex gap-6 mt-4 text-sm">
            <div className="flex items-center gap-2">
              <span className="text-muted-foreground">Metro Pairs:</span>
              <span className="font-medium">{pathLatencyData.summary.totalPairs}</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-muted-foreground">With Internet Data:</span>
              <span className="font-medium">{pathLatencyData.summary.pairsWithInternet}</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-muted-foreground">Avg Improvement:</span>
              <span className="font-medium text-green-600 dark:text-green-400">
                {pathLatencyData.summary.avgImprovementPct.toFixed(1)}%
              </span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-muted-foreground">Max Improvement:</span>
              <span className="font-medium text-green-600 dark:text-green-400">
                {pathLatencyData.summary.maxImprovementPct.toFixed(1)}%
              </span>
            </div>
          </div>
        )}

        {/* Loading indicator */}
        {pathLatencyLoading && (
          <div className="flex items-center gap-2 mt-4 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading path latency data...
          </div>
        )}
      </div>

      {/* Matrix grid */}
      <div className="flex-1 flex gap-6 px-6 pb-6 min-h-0">
        {/* Scrollable table area */}
        <div className="flex-1 overflow-auto min-w-0">
          <table className="border-separate border-spacing-0">
            <thead>
              <tr>
                {/* Top-left corner (empty) */}
                <th className="relative bg-muted sticky top-0 left-0 z-30 min-w-[48px] shadow-[inset_0_0_0_1px_hsl(var(--border))] before:absolute before:-top-1 before:left-0 before:right-0 before:h-1 before:bg-muted [backface-visibility:hidden] [transform:translateZ(0)]" />

                {/* Column headers */}
                {metros.map(metro => (
                  <th
                    key={`col-${metro.pk}`}
                    className="relative bg-muted px-1 py-2 text-xs font-medium text-center sticky top-0 z-20 min-w-[48px] max-w-[60px] shadow-[inset_0_0_0_1px_hsl(var(--border))] before:absolute before:-top-1 before:left-0 before:right-0 before:h-1 before:bg-muted [backface-visibility:hidden] [transform:translateZ(0)]"
                    title={metro.name}
                  >
                    <span className="writing-mode-vertical transform -rotate-45 origin-center whitespace-nowrap inline-block">
                      {metro.code}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* Rows */}
              {metros.map(fromMetro => (
                <tr key={`row-${fromMetro.pk}`}>
                  {/* Row header */}
                  <th
                    className="bg-muted px-2 py-1 text-xs font-medium text-right sticky left-0 z-10 whitespace-nowrap shadow-[inset_0_0_0_1px_hsl(var(--border))] [backface-visibility:hidden] [transform:translateZ(0)]"
                    title={fromMetro.name}
                  >
                    {fromMetro.code}
                  </th>

                  {/* Cells */}
                  {metros.map(toMetro => {
                    const isSame = fromMetro.pk === toMetro.pk
                    const pathLatency = isSame ? null : pathLatencyMap.get(`${fromMetro.pk}:${toMetro.pk}`) ?? null
                    const isSelected = selectedCell?.from === fromMetro.pk && selectedCell?.to === toMetro.pk

                    return (
                      <td
                        key={`cell-${fromMetro.pk}-${toMetro.pk}`}
                        className="border border-border p-0 min-w-[48px] max-w-[60px] h-[40px]"
                      >
                        <PathLatencyCell
                          pathLatency={pathLatency}
                          onClick={() => {
                            if (!isSame && pathLatency) {
                              if (isSelected) {
                                setSelectedCell(null)
                              } else {
                                setSelectedCell({ from: fromMetro.pk, to: toMetro.pk }, fromMetro.code, toMetro.code)
                              }
                            }
                          }}
                          isSelected={isSelected}
                        />
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Detail panel */}
        {selectedPathLatency && selectedCell && (
          <div className="w-96 flex-shrink-0 border-l border-border bg-card overflow-y-auto">
            <PathLatencyDetail
              fromCode={selectedPathLatency.fromMetroCode}
              toCode={selectedPathLatency.toMetroCode}
              pathLatency={selectedPathLatency}
              pathDetail={pathDetailData ?? null}
              isLoadingDetail={pathDetailLoading}
              onClose={() => setSelectedCell(null)}
            />
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="px-6 pb-6">
        <div className="flex items-center gap-6 text-xs text-muted-foreground">
          <span className="font-medium">Legend (DZ Path vs Internet):</span>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-green-100 dark:bg-green-900/40 border border-green-200 dark:border-green-800" />
            <span>DZ faster</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-yellow-100 dark:bg-yellow-900/40 border border-yellow-200 dark:border-yellow-800" />
            <span>Similar (0 to -10%)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-red-100 dark:bg-red-900/40 border border-red-200 dark:border-red-800" />
            <span>Internet faster (&lt;-10%)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-muted/50 border border-border" />
            <span>No internet data</span>
          </div>
        </div>
      </div>
    </div>
  )
}
