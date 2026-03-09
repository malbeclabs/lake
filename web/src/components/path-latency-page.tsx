import { useMemo, useCallback, useState, useRef, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Loader2, Route, Download, ArrowRight, ChevronDown, X, Search, MapPin, Filter, Zap, Globe, TrendingUp, Activity } from 'lucide-react'
import { fetchMetroConnectivity, fetchMetroPathLatency, fetchMetroPathDetail, fetchFieldValues } from '@/lib/api'
import type { MetroPathLatency, MetroPathDetailResponse, MetroPathDetailHop, PathOptimizeMode } from '@/lib/api'
import { ErrorState } from '@/components/ui/error-state'
import { useDelayedLoading } from '@/hooks/use-delayed-loading'
import { PageHeader } from '@/components/page-header'
import { cn } from '@/lib/utils'

const DEBOUNCE_MS = 150

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

// 5-tier improvement classification
type ImprovementTier = 'great' | 'good' | 'neutral' | 'bad' | 'none'

function getImprovementTier(pct: number | null): ImprovementTier {
  if (pct === null) return 'none'
  if (pct > 20) return 'great'
  if (pct > 0) return 'good'
  if (pct >= -10) return 'neutral'
  return 'bad'
}

const TIER_CELL = {
  great: 'bg-green-100 dark:bg-green-950/60 hover:bg-green-200 dark:hover:bg-green-950/80',
  good: 'bg-emerald-50 dark:bg-emerald-950/40 hover:bg-emerald-100 dark:hover:bg-emerald-950/60',
  neutral: 'bg-amber-50 dark:bg-amber-950/30 hover:bg-amber-100 dark:hover:bg-amber-950/50',
  bad: 'bg-red-50 dark:bg-red-950/30 hover:bg-red-100 dark:hover:bg-red-950/50',
  none: 'bg-muted/20 hover:bg-muted/40',
} as const

const TIER_PCT_TEXT = {
  great: 'text-green-700 dark:text-green-400',
  good: 'text-emerald-700 dark:text-emerald-400',
  neutral: 'text-amber-700 dark:text-amber-400',
  bad: 'text-red-700 dark:text-red-400',
  none: 'text-muted-foreground',
} as const

const TIER_DETAIL_BG = {
  great: 'bg-green-50 dark:bg-green-950/40 border border-green-200 dark:border-green-900',
  good: 'bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-900',
  neutral: 'bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-900',
  bad: 'bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900',
  none: 'bg-muted border border-border',
} as const

// Matrix cell
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
    return (
      <div className="w-full h-full flex items-center justify-center bg-muted/20">
        <div className="w-1.5 h-1.5 rounded-full bg-border" />
      </div>
    )
  }

  const tier = getImprovementTier(pathLatency.improvementPct)
  const hasInternet = pathLatency.internetLatencyMs > 0 && pathLatency.improvementPct !== null
  const pct = pathLatency.improvementPct

  return (
    <button
      onClick={onClick}
      className={cn(
        'w-full h-full flex flex-col items-center justify-center gap-0.5 px-1 transition-colors cursor-pointer',
        TIER_CELL[tier],
        isSelected && 'ring-2 ring-primary ring-inset'
      )}
      title={`${pathLatency.fromMetroCode} → ${pathLatency.toMetroCode}: ${pathLatency.pathLatencyMs.toFixed(1)}ms (${pathLatency.hopCount} hops)${hasInternet ? ` vs Internet ${pathLatency.internetLatencyMs.toFixed(1)}ms` : ''}`}
    >
      {hasInternet && pct !== null ? (
        <>
          <span className={cn('text-[11px] font-semibold leading-none tabular-nums', TIER_PCT_TEXT[tier])}>
            {pct > 0 ? '+' : ''}{pct.toFixed(0)}%
          </span>
          <span className="text-[10px] text-muted-foreground leading-none tabular-nums">
            {pathLatency.pathLatencyMs.toFixed(0)}ms
          </span>
        </>
      ) : (
        <>
          <span className="text-[12px] font-medium leading-none tabular-nums text-foreground">
            {pathLatency.pathLatencyMs.toFixed(0)}
          </span>
          <span className="text-[9px] text-muted-foreground leading-none">
            {pathLatency.hopCount}h
          </span>
        </>
      )}
    </button>
  )
}

// Visual stepper for path hops
function PathStepper({ hops }: { hops: MetroPathDetailHop[] }) {
  return (
    <div className="space-y-0">
      {hops.map((hop, idx) => {
        const isLast = idx === hops.length - 1
        const hasLink = !isLast && hop.linkLatency > 0

        return (
          <div key={idx} className="flex items-stretch gap-3">
            {/* Timeline rail */}
            <div className="flex flex-col items-center w-5 flex-shrink-0">
              <div className={cn(
                'w-2.5 h-2.5 rounded-full flex-shrink-0 ring-2 ring-background mt-1',
                idx === 0 ? 'bg-primary' : isLast ? 'bg-primary' : 'bg-muted-foreground/60'
              )} />
              {!isLast && (
                <div className="w-px flex-1 bg-border min-h-[32px]" />
              )}
            </div>

            {/* Content */}
            <div className={cn('flex-1 flex items-start justify-between', !isLast && 'pb-3')}>
              <div className="flex items-center gap-2 pt-0.5">
                <span className="font-mono text-sm font-medium leading-none">{hop.deviceCode}</span>
                <span className="text-[10px] text-muted-foreground bg-muted px-1.5 py-0.5 rounded">
                  {hop.metroCode}
                </span>
              </div>
              {hasLink && (
                <div className="text-right pt-0.5">
                  <div className="text-xs font-medium text-primary tabular-nums">
                    {hop.linkLatency.toFixed(2)}ms
                  </div>
                  {hop.linkBwGbps > 0 && (
                    <div className="text-[10px] text-muted-foreground">
                      {hop.linkBwGbps.toFixed(0)} Gbps
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}

// Latency comparison bar
function LatencyBar({ label, ms, maxMs, color }: { label: string; ms: number; maxMs: number; color: string }) {
  const pct = Math.max(5, (ms / maxMs) * 100)
  return (
    <div className="space-y-1">
      <div className="flex justify-between items-baseline">
        <span className="text-xs text-muted-foreground">{label}</span>
        <span className="text-sm font-semibold tabular-nums">{ms.toFixed(1)}ms</span>
      </div>
      <div className="h-2 bg-muted rounded-full overflow-hidden">
        <div
          className={cn('h-full rounded-full transition-all duration-700', color)}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}

// Detail panel
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
  const tier = getImprovementTier(pathLatency.improvementPct)
  const hasInternet = pathLatency.internetLatencyMs > 0
  const maxMs = hasInternet
    ? Math.max(pathLatency.pathLatencyMs, pathLatency.internetLatencyMs)
    : pathLatency.pathLatencyMs
  const pct = pathLatency.improvementPct

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-4 pt-4 pb-3 border-b border-border flex items-center justify-between flex-shrink-0">
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1.5 font-semibold text-base">
            <span>{fromCode}</span>
            <ArrowRight className="h-4 w-4 text-muted-foreground" />
            <span>{toCode}</span>
          </div>
        </div>
        <button
          onClick={onClose}
          className="p-1 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto">
        {/* Improvement badge */}
        {hasInternet && pct !== null && (
          <div className={cn('mx-4 mt-4 rounded-lg p-3', TIER_DETAIL_BG[tier])}>
            <div className="flex items-center justify-between">
              <span className="text-xs text-muted-foreground">DZ vs Internet</span>
              <span className={cn('text-xl font-bold tabular-nums', TIER_PCT_TEXT[tier])}>
                {pct > 0 ? '+' : ''}{pct.toFixed(1)}%
              </span>
            </div>
            <div className={cn('text-xs mt-0.5', TIER_PCT_TEXT[tier])}>
              {pct > 0 ? `${pct.toFixed(1)}% faster than internet` : `${Math.abs(pct).toFixed(1)}% slower than internet`}
            </div>
          </div>
        )}

        {/* Stats grid */}
        <div className="grid grid-cols-3 gap-2 px-4 mt-3">
          <div className="bg-muted/50 rounded-lg p-2.5">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">DZ Latency</div>
            <div className="text-lg font-bold tabular-nums">{pathLatency.pathLatencyMs.toFixed(1)}<span className="text-xs font-normal ml-0.5">ms</span></div>
          </div>
          <div className="bg-muted/50 rounded-lg p-2.5">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">Hops</div>
            <div className="text-lg font-bold tabular-nums">{pathLatency.hopCount}</div>
          </div>
          {pathLatency.bottleneckBwGbps > 0 ? (
            <div className="bg-muted/50 rounded-lg p-2.5">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">Bottleneck</div>
              <div className="text-lg font-bold tabular-nums">{pathLatency.bottleneckBwGbps.toFixed(0)}<span className="text-xs font-normal ml-0.5">Gbps</span></div>
            </div>
          ) : hasInternet ? (
            <div className="bg-muted/50 rounded-lg p-2.5">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">Internet</div>
              <div className="text-lg font-bold tabular-nums">{pathLatency.internetLatencyMs.toFixed(1)}<span className="text-xs font-normal ml-0.5">ms</span></div>
            </div>
          ) : (
            <div className="bg-muted/50 rounded-lg p-2.5 opacity-40">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">Internet</div>
              <div className="text-sm text-muted-foreground">—</div>
            </div>
          )}
        </div>

        {/* Latency comparison bars */}
        {hasInternet && (
          <div className="px-4 mt-4 space-y-2.5">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium">Latency Comparison</div>
            <LatencyBar label="DZ Network" ms={pathLatency.pathLatencyMs} maxMs={maxMs} color="bg-blue-500" />
            <LatencyBar label="Internet" ms={pathLatency.internetLatencyMs} maxMs={maxMs} color="bg-muted-foreground/40" />
          </div>
        )}

        {/* Path breakdown */}
        <div className="px-4 mt-4 mb-4">
          <div className="text-[10px] text-muted-foreground uppercase tracking-wider font-medium mb-3">Path Breakdown</div>
          {isLoadingDetail ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading path details...
            </div>
          ) : pathDetail && pathDetail.hops.length > 0 ? (
            <PathStepper hops={pathDetail.hops} />
          ) : (
            <div className="text-sm text-muted-foreground">No path details available</div>
          )}
        </div>
      </div>
    </div>
  )
}

// Summary stat card
function SummaryCard({
  icon: Icon,
  label,
  value,
  className,
}: {
  icon: React.ElementType
  label: string
  value: string
  className?: string
}) {
  return (
    <div className="bg-card border border-border rounded-lg px-3 py-2.5 flex items-center gap-3">
      <div className="p-1.5 rounded-md bg-muted flex-shrink-0">
        <Icon className="h-3.5 w-3.5 text-muted-foreground" />
      </div>
      <div className="min-w-0">
        <div className="text-[10px] text-muted-foreground uppercase tracking-wider leading-none mb-1">{label}</div>
        <div className={cn('text-base font-semibold tabular-nums leading-none', className)}>{value}</div>
      </div>
    </div>
  )
}

const PANEL_MIN_WIDTH = 240
const PANEL_MAX_WIDTH = 640
const PANEL_DEFAULT_WIDTH = 320

export function PathLatencyPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const optimizeParam = searchParams.get('optimize') as PathOptimizeMode | null
  const optimizeMode: PathOptimizeMode = optimizeParam || 'latency'

  const fromCodeParam = searchParams.get('from')
  const toCodeParam = searchParams.get('to')

  const metroFilters = useMemo(() => {
    return parseMetroFilters(searchParams.get('metros') || '')
  }, [searchParams])

  const addMetroFilter = useCallback((metro: string) => {
    setSearchParams(prev => {
      const current = parseMetroFilters(prev.get('metros') || '')
      if (!current.includes(metro)) {
        prev.set('metros', [...current, metro].join(','))
      }
      return prev
    })
  }, [setSearchParams])

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

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      prev.delete('metros')
      return prev
    })
  }, [setSearchParams])

  const queryClient = useQueryClient()

  const { data: connectivityData, isLoading: connectivityLoading, error: connectivityError, isFetching: connectivityFetching } = useQuery({
    queryKey: ['metro-connectivity'],
    queryFn: fetchMetroConnectivity,
    staleTime: 60000,
    retry: 2,
  })

  const selectedCell = useMemo(() => {
    if (!fromCodeParam || !toCodeParam || !connectivityData) return null
    const fromUpper = fromCodeParam.toUpperCase()
    const toUpper = toCodeParam.toUpperCase()
    const fromMetro = connectivityData.metros.find(m => m.code.toUpperCase() === fromUpper)
    const toMetro = connectivityData.metros.find(m => m.code.toUpperCase() === toUpper)
    if (!fromMetro || !toMetro) return null
    return { from: fromMetro.pk, to: toMetro.pk }
  }, [fromCodeParam, toCodeParam, connectivityData])

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

  const showLoading = useDelayedLoading(connectivityLoading)

  const [panelWidth, setPanelWidth] = useState(PANEL_DEFAULT_WIDTH)
  const isDragging = useRef(false)
  const dragStartX = useRef(0)
  const dragStartWidth = useRef(0)

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    isDragging.current = true
    dragStartX.current = e.clientX
    dragStartWidth.current = panelWidth

    const onMove = (ev: MouseEvent) => {
      if (!isDragging.current) return
      const delta = dragStartX.current - ev.clientX
      const newWidth = Math.min(PANEL_MAX_WIDTH, Math.max(PANEL_MIN_WIDTH, dragStartWidth.current + delta))
      setPanelWidth(newWidth)
    }

    const onUp = () => {
      isDragging.current = false
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }

    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
  }, [panelWidth])

  const { data: pathLatencyData, isLoading: pathLatencyLoading } = useQuery({
    queryKey: ['metro-path-latency', optimizeMode],
    queryFn: () => fetchMetroPathLatency(optimizeMode),
    staleTime: 60000,
  })

  const pathLatencyMap = useMemo(() => {
    if (!pathLatencyData) return new Map<string, MetroPathLatency>()
    const map = new Map<string, MetroPathLatency>()
    for (const pl of pathLatencyData.paths) {
      map.set(`${pl.fromMetroPK}:${pl.toMetroPK}`, pl)
    }
    return map
  }, [pathLatencyData])

  const selectedPathLatency = useMemo(() => {
    if (!selectedCell) return null
    return pathLatencyMap.get(`${selectedCell.from}:${selectedCell.to}`) ?? null
  }, [selectedCell, pathLatencyMap])

  const { data: pathDetailData, isLoading: pathDetailLoading } = useQuery({
    queryKey: ['metro-path-detail', selectedPathLatency?.fromMetroCode, selectedPathLatency?.toMetroCode, optimizeMode],
    queryFn: () => {
      if (!selectedPathLatency) return Promise.resolve(null)
      return fetchMetroPathDetail(selectedPathLatency.fromMetroCode, selectedPathLatency.toMetroCode, optimizeMode)
    },
    staleTime: 60000,
    enabled: selectedPathLatency !== null,
  })

  const metros = useMemo(() => {
    if (!connectivityData) return []
    if (metroFilters.length === 0) return connectivityData.metros
    return connectivityData.metros.filter(m =>
      metroFilters.some(f => m.code.toLowerCase() === f.toLowerCase())
    )
  }, [connectivityData, metroFilters])

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
    if (connectivityLoading) {
      return <div className="flex-1 bg-background" />
    }
    return (
      <div className="flex-1 flex items-center justify-center bg-background">
        <div className="text-muted-foreground">No metros found</div>
      </div>
    )
  }

  const summary = pathLatencyData?.summary

  return (
    <div className="flex-1 flex flex-col bg-background overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 flex-shrink-0">
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

        <p className="mt-2 text-sm text-muted-foreground">
          End-to-end path latency across the DZ network vs public internet.
        </p>

        {/* Summary cards */}
        {summary ? (
          <div className="grid grid-cols-4 gap-2 mt-4">
            <SummaryCard icon={Activity} label="Metro Pairs" value={summary.totalPairs.toString()} />
            <SummaryCard icon={Globe} label="With Internet" value={summary.pairsWithInternet.toString()} />
            <SummaryCard
              icon={TrendingUp}
              label="Avg Improvement"
              value={`+${summary.avgImprovementPct.toFixed(1)}%`}
              className="text-green-600 dark:text-green-400"
            />
            <SummaryCard
              icon={Zap}
              label="Best Improvement"
              value={`+${summary.maxImprovementPct.toFixed(1)}%`}
              className="text-green-600 dark:text-green-400"
            />
          </div>
        ) : pathLatencyLoading ? (
          <div className="grid grid-cols-4 gap-2 mt-4">
            {[0, 1, 2, 3].map(i => (
              <div key={i} className="bg-card border border-border rounded-lg px-3 py-2.5 h-[56px] animate-pulse" />
            ))}
          </div>
        ) : null}

        {/* Filter bar */}
        <div className="flex items-center gap-2 flex-wrap mt-3">
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
      </div>

      {/* Matrix + detail panel */}
      <div className="flex-1 flex gap-0 min-h-0 border-t border-border">
        {/* Scrollable matrix */}
        <div className="flex-1 overflow-auto min-w-0 p-4">
          <table className="border-separate border-spacing-0">
            <thead>
              <tr>
                {/* Top-left corner */}
                <th className="relative bg-muted sticky top-0 left-0 z-30 min-w-[52px] shadow-[inset_0_0_0_1px_hsl(var(--border))] before:absolute before:-top-1 before:left-0 before:right-0 before:h-1 before:bg-muted [backface-visibility:hidden] [transform:translateZ(0)]" />

                {metros.map(metro => (
                  <th
                    key={`col-${metro.pk}`}
                    className="relative bg-muted px-1 py-2 text-xs font-medium text-center sticky top-0 z-20 min-w-[52px] max-w-[64px] shadow-[inset_0_0_0_1px_hsl(var(--border))] before:absolute before:-top-1 before:left-0 before:right-0 before:h-1 before:bg-muted [backface-visibility:hidden] [transform:translateZ(0)]"
                    title={metro.name}
                  >
                    <span className="writing-mode-vertical transform -rotate-45 origin-center whitespace-nowrap inline-block text-[11px]">
                      {metro.code}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {metros.map(fromMetro => (
                <tr key={`row-${fromMetro.pk}`}>
                  <th
                    className="bg-muted px-2 py-1 text-[11px] font-medium text-right sticky left-0 z-10 whitespace-nowrap shadow-[inset_0_0_0_1px_hsl(var(--border))] [backface-visibility:hidden] [transform:translateZ(0)]"
                    title={fromMetro.name}
                  >
                    {fromMetro.code}
                  </th>

                  {metros.map(toMetro => {
                    const isSame = fromMetro.pk === toMetro.pk
                    const pathLatency = isSame ? null : pathLatencyMap.get(`${fromMetro.pk}:${toMetro.pk}`) ?? null
                    const isSelected = selectedCell?.from === fromMetro.pk && selectedCell?.to === toMetro.pk

                    return (
                      <td
                        key={`cell-${fromMetro.pk}-${toMetro.pk}`}
                        className="border border-border/60 p-0 min-w-[52px] max-w-[64px] h-[44px]"
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
          <>
            {/* Drag handle */}
            <div
              onMouseDown={handleResizeStart}
              className="w-1 flex-shrink-0 cursor-col-resize bg-border hover:bg-primary/40 transition-colors group relative"
              title="Drag to resize"
            >
              <div className="absolute inset-y-0 -left-1 -right-1" />
            </div>
            <div
              className="flex-shrink-0 border-l border-border bg-card flex flex-col overflow-hidden"
              style={{ width: panelWidth }}
            >
              <PathLatencyDetail
                fromCode={selectedPathLatency.fromMetroCode}
                toCode={selectedPathLatency.toMetroCode}
                pathLatency={selectedPathLatency}
                pathDetail={pathDetailData ?? null}
                isLoadingDetail={pathDetailLoading}
                onClose={() => setSelectedCell(null)}
              />
            </div>
          </>
        )}
      </div>

      {/* Legend */}
      <div className="px-6 py-3 border-t border-border bg-muted/20 flex-shrink-0">
        <div className="flex items-center gap-6 text-xs text-muted-foreground">
          <span className="font-medium text-foreground">DZ vs Internet:</span>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-sm bg-red-100 dark:bg-red-950/60 border border-red-200 dark:border-red-900" />
            <span>Slower (&lt;-10%)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-sm bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-900" />
            <span>Similar (0 to -10%)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-sm bg-emerald-50 dark:bg-emerald-950/40 border border-emerald-200 dark:border-emerald-900" />
            <span>Faster (0 to +20%)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-sm bg-green-100 dark:bg-green-950/60 border border-green-200 dark:border-green-900" />
            <span>Much faster (&gt;+20%)</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-sm bg-muted/40 border border-border" />
            <span>No internet data</span>
          </div>
          <div className="ml-auto text-[10px]">
            Primary: improvement % — Secondary: latency ms
          </div>
        </div>
      </div>
    </div>
  )
}
