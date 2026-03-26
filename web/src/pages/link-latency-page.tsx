import { useState, useMemo, useCallback, useRef, useTransition } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { ChevronDown, ChevronUp, ArrowRightLeft, X, ArrowUpToLine, Loader2 } from 'lucide-react'
import { fetchLinkLatencySummary, type LinkLatencySummary } from '@/lib/api'
import { DashboardProvider, useDashboard, dashboardFilterParams } from '@/components/traffic-dashboard/dashboard-context'
import { DashboardFilters, DashboardFilterBadges } from '@/components/traffic-dashboard/dashboard-filters'
import { PageHeader } from '@/components/page-header'
import { InlineFilter } from '@/components/inline-filter'
import { MultiLinkLatencyCharts } from '@/components/multi-link-latency-charts'
import { getSeriesColors } from '@/components/chart-colors'
import { useTheme } from '@/hooks/use-theme'

type AggMethod = 'max' | 'avg' | 'min' | 'p50' | 'p90' | 'p95' | 'p99'

const aggLabels: Record<AggMethod, string> = {
  'max': 'Max',
  'p99': 'P99',
  'p95': 'P95',
  'p90': 'P90',
  'p50': 'P50',
  'avg': 'Average',
  'min': 'Min',
}

const COMMITTED_RTT_PROVISIONING_MS = 1000

function AggSelector({
  value,
  onChange,
}: {
  value: AggMethod
  onChange: (value: AggMethod) => void
}) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="px-3 py-1.5 text-sm border border-border rounded-md hover:bg-muted transition-colors inline-flex items-center gap-1.5"
      >
        Agg: {aggLabels[value]}
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[140px]">
            {(['max', 'p99', 'p95', 'p90', 'p50', 'avg', 'min'] as AggMethod[]).map((agg) => (
              <button
                key={agg}
                onClick={() => {
                  onChange(agg)
                  setIsOpen(false)
                }}
                className={`w-full px-3 py-1.5 text-left text-sm transition-colors ${
                  value === agg
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                }`}
              >
                {aggLabels[agg]}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// Color based on ratio to committed value. No committed = no color.
function ratioColor(measured: number, committed: number | undefined): string {
  if (measured <= 0) return 'text-muted-foreground'
  if (!committed || committed <= 0 || committed >= COMMITTED_RTT_PROVISIONING_MS) return 'text-foreground'
  const ratio = measured / committed
  if (ratio <= 1.0) return 'text-emerald-600 dark:text-emerald-400/80'
  if (ratio <= 1.2) return 'text-amber-600 dark:text-amber-400/80'
  if (ratio <= 1.5) return 'text-orange-600 dark:text-orange-400/80'
  return 'text-rose-600 dark:text-rose-400/80'
}

function lossColor(pct: number): string {
  if (pct <= 0) return 'text-muted-foreground'
  if (pct <= 0.5) return 'text-amber-600 dark:text-amber-400/80'
  if (pct <= 2) return 'text-orange-600 dark:text-orange-400/80'
  return 'text-rose-600 dark:text-rose-400/80'
}

function fmt(v: number | undefined, decimals = 2): string {
  if (v == null || v <= 0) return '—'
  return v.toFixed(decimals)
}

type SortField = 'link_code' | 'link_type' | 'contributor_code' | 'side_a_code' | 'side_z_code' | 'committed_rtt_ms' | 'committed_jitter_ms' | 'rtt_a_to_z_ms' | 'rtt_z_to_a_ms' | 'jitter_a_to_z_ms' | 'jitter_z_to_a_ms' | 'loss_a_pct' | 'loss_z_pct'
type SortDir = 'asc' | 'desc'

const textFields: SortField[] = ['link_code', 'link_type', 'contributor_code', 'side_a_code', 'side_z_code']

// InlineFilter config
const filterFieldPrefixes = [
  { prefix: 'code:', description: 'Filter by link code' },
  { prefix: 'type:', description: 'Filter by link type' },
  { prefix: 'contributor:', description: 'Filter by contributor' },
  { prefix: 'sideA:', description: 'Filter by side A device' },
  { prefix: 'sideZ:', description: 'Filter by side Z device' },
  { prefix: 'status:', description: 'Filter by link status' },
]
const filterAutocompleteFields = ['type', 'contributor', 'status']
const validFilterFieldNames = ['code', 'type', 'contributor', 'sideA', 'sideZ', 'status']

function parseFilter(filter: string): { field: string; value: string } {
  const colonIndex = filter.indexOf(':')
  if (colonIndex > 0) {
    const field = filter.slice(0, colonIndex).toLowerCase()
    const normalized = field === 'sidea' ? 'sideA' : field === 'sidez' ? 'sideZ' : field
    const value = filter.slice(colonIndex + 1)
    if (validFilterFieldNames.includes(normalized) && value) {
      return { field: normalized, value }
    }
  }
  return { field: 'all', value: filter }
}

function parseSearchFilters(param: string): string[] {
  if (!param) return []
  return param.split(',').filter(Boolean)
}

function matchesFilter(link: LinkLatencySummary, filterRaw: string): boolean {
  const { field, value } = parseFilter(filterRaw)
  const needle = value.trim().toLowerCase()
  if (!needle) return true

  const getField = (f: string) => {
    switch (f) {
      case 'code': return link.link_code
      case 'type': return link.link_type
      case 'contributor': return link.contributor_code
      case 'sideA': return link.side_a_code
      case 'sideZ': return link.side_z_code
      case 'status': return link.link_status
      default: return ''
    }
  }

  if (field === 'all') {
    return ['code', 'type', 'contributor', 'sideA', 'sideZ'].some(f =>
      getField(f).toLowerCase().includes(needle)
    )
  }
  return getField(field).toLowerCase().includes(needle)
}

const columns: { field: SortField; label: string; align?: 'right' }[] = [
  { field: 'link_code', label: 'Code' },
  { field: 'link_type', label: 'Type' },
  { field: 'contributor_code', label: 'Contributor' },
  { field: 'side_a_code', label: 'Side A' },
  { field: 'side_z_code', label: 'Side Z' },
  { field: 'committed_rtt_ms', label: 'SLO RTT', align: 'right' },
  { field: 'rtt_a_to_z_ms', label: 'RTT A→Z', align: 'right' },
  { field: 'rtt_z_to_a_ms', label: 'RTT Z→A', align: 'right' },
  { field: 'committed_jitter_ms', label: 'SLO Jitter', align: 'right' },
  { field: 'jitter_a_to_z_ms', label: 'Jitter A→Z', align: 'right' },
  { field: 'jitter_z_to_a_ms', label: 'Jitter Z→A', align: 'right' },
  { field: 'loss_a_pct', label: 'Loss A%', align: 'right' },
  { field: 'loss_z_pct', label: 'Loss Z%', align: 'right' },
]


const EXCLUDED_CATEGORIES = [
  { key: 'isis-down', label: 'ISIS Down', color: 'bg-red-500/10 text-red-600 dark:text-red-400' },
  { key: 'soft-drained', label: 'Soft Drained', color: 'bg-amber-500/10 text-amber-600 dark:text-amber-400' },
  { key: 'hard-drained', label: 'Hard Drained', color: 'bg-amber-500/10 text-amber-600 dark:text-amber-400' },
  { key: 'provisioning', label: 'Provisioning', color: 'bg-blue-500/10 text-blue-600 dark:text-blue-400' },
]

function StatusFilterDropdown({
  showCategories,
  onToggle,
}: {
  showCategories: Set<string>
  onToggle: (category: string) => void
}) {
  const [isOpen, setIsOpen] = useState(false)
  const anyActive = showCategories.size > 0

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className={`px-3 py-1.5 text-sm border rounded-md transition-colors inline-flex items-center gap-1.5 ${
          anyActive
            ? 'border-foreground/30 text-foreground bg-muted'
            : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
        }`}
      >
        Include
        {anyActive && <span className="text-xs opacity-60">({showCategories.size})</span>}
        <ChevronDown className="h-4 w-4" />
      </button>
      {isOpen && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-50 bg-popover border border-border rounded-md shadow-lg py-1 min-w-[180px]">
            {EXCLUDED_CATEGORIES.map(cat => (
              <button
                key={cat.key}
                onClick={() => onToggle(cat.key)}
                className="w-full px-3 py-1.5 text-left text-sm flex items-center gap-2 hover:bg-muted/50 transition-colors"
              >
                <span className={`w-3.5 h-3.5 rounded border flex items-center justify-center ${
                  showCategories.has(cat.key) ? 'border-foreground bg-foreground' : 'border-muted-foreground'
                }`}>
                  {showCategories.has(cat.key) && <span className="text-[10px] text-background">&#10003;</span>}
                </span>
                <span className={showCategories.has(cat.key) ? 'text-foreground' : 'text-muted-foreground'}>
                  {cat.label}
                </span>
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

function SortIcon({ field, sortField, sortDir }: { field: SortField; sortField: SortField; sortDir: SortDir }) {
  if (field !== sortField) return <ChevronDown className="h-3 w-3 opacity-0 group-hover/th:opacity-40" />
  return sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
}

function LinkLatencyPageContent() {
  const dashboardState = useDashboard()
  const { timeRange, customStart, customEnd } = dashboardState
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const seriesColors = getSeriesColors(isDark)

  const [searchParams, setSearchParamsRaw] = useSearchParams()
  const [, startTransition] = useTransition()
  const setSearchParams = useCallback(
    (updater: (prev: URLSearchParams) => URLSearchParams) => {
      startTransition(() => { setSearchParamsRaw(updater) })
    },
    [setSearchParamsRaw, startTransition]
  )

  // URL-backed state helpers
  const setUrlParam = useCallback((key: string, value: string, defaultValue: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (value === defaultValue) next.delete(key)
      else next.set(key, value)
      return next
    })
  }, [setSearchParams])

  // Read state from URL with defaults
  const aggMethod = (searchParams.get('agg') || 'avg') as AggMethod
  const setAggMethod = useCallback((v: AggMethod) => setUrlParam('agg', v, 'avg'), [setUrlParam])

  const sortField = (searchParams.get('sort') || 'rtt_a_to_z_ms') as SortField
  const sortDir = (searchParams.get('sort_dir') || 'desc') as SortDir

  // Filter state: committed filters in URL, live filter in local state
  const [liveFilter, setLiveFilter] = useState('')
  const searchFilters = useMemo(() => parseSearchFilters(searchParams.get('search') || ''), [searchParams])
  const allFilters = useMemo(() => liveFilter ? [...searchFilters, liveFilter] : searchFilters, [searchFilters, liveFilter])

  const removeFilter = useCallback((filterToRemove: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      const filters = parseSearchFilters(prev.get('search') || '').filter(f => f !== filterToRemove)
      if (filters.length === 0) next.delete('search')
      else next.set('search', filters.join(','))
      return next
    })
  }, [setSearchParams])

  const clearAllFilters = useCallback(() => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.delete('search')
      return next
    })
  }, [setSearchParams])

  const tableCollapsed = searchParams.get('table') === 'collapsed'
  const setTableCollapsed = useCallback((fn: (prev: boolean) => boolean) => {
    const next = fn(searchParams.get('table') === 'collapsed')
    setUrlParam('table', next ? 'collapsed' : '', '')
  }, [setUrlParam, searchParams])

  const selectedToTop = searchParams.get('top') === '1'
  const setSelectedToTop = useCallback((fn: (prev: boolean) => boolean) => {
    const next = fn(searchParams.get('top') === '1')
    setUrlParam('top', next ? '1' : '', '')
  }, [setUrlParam, searchParams])

  // Which excluded categories to show (empty = hide all excluded)
  const showCategories = useMemo(() => {
    const raw = searchParams.get('show')
    if (!raw) return new Set<string>()
    return new Set(raw.split(',').filter(Boolean))
  }, [searchParams])

  const toggleShowCategory = useCallback((category: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      const current = new Set((prev.get('show') || '').split(',').filter(Boolean))
      if (current.has(category)) current.delete(category)
      else current.add(category)
      if (current.size === 0) next.delete('show')
      else next.set('show', [...current].join(','))
      return next
    })
  }, [setSearchParams])

  // Fetch all links when any excluded category is shown
  const showExcluded = showCategories.size > 0

  // Selected links from URL (comma-separated PKs)
  const selectedLinkPks = useMemo(() => {
    const raw = searchParams.get('sel')
    if (!raw) return new Set<string>()
    return new Set(raw.split(',').filter(Boolean))
  }, [searchParams])

  const setSelectedLinkPks = useCallback((next: Set<string>) => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (next.size === 0) p.delete('sel')
      else p.set('sel', [...next].join(','))
      return p
    })
  }, [setSearchParams])

  const filterParams = useMemo(() => dashboardFilterParams(dashboardState), [dashboardState])

  const { data, isFetching, error } = useQuery({
    queryKey: ['link-latency-summary', timeRange, aggMethod, filterParams, showExcluded],
    queryFn: () => fetchLinkLatencySummary(timeRange, aggMethod, filterParams, showExcluded),
    staleTime: 30000,
    refetchInterval: dashboardState.refetchInterval,
  })

  const links = useMemo(() => data?.links ?? [], [data])

  // Build name map for selected links
  const linkNameMap = useMemo(() => {
    const map = new Map<string, string>()
    for (const l of links) map.set(l.link_pk, l.link_code)
    return map
  }, [links])

  // Stable sorted selected PKs for consistent color assignment
  const selectedPks = useMemo(() => [...selectedLinkPks].sort(), [selectedLinkPks])

  // Color map for pinned links (must match MultiLinkLatencyCharts)
  const selectedColorMap = useMemo(() => {
    const map = new Map<string, string>()
    selectedPks.forEach((pk, i) => map.set(pk, seriesColors[i % seriesColors.length]))
    return map
  }, [selectedPks, seriesColors])

  // Filtered + sorted rows
  const filteredLinks = useMemo(() => {
    let result = links

    // Filter out excluded categories unless toggled on
    if (showExcluded) {
      result = result.filter(l => {
        if (l.isis_down) return showCategories.has('isis-down')
        if (l.provisioning) return showCategories.has('provisioning')
        if (l.link_status === 'soft-drained') return showCategories.has('soft-drained')
        if (l.link_status === 'hard-drained') return showCategories.has('hard-drained')
        return true
      })
    }

    // Apply inline filters (committed + live)
    if (allFilters.length > 0) {
      // Group by field: OR within same field, AND across different fields
      const grouped = new Map<string, string[]>()
      for (const f of allFilters) {
        const { field } = parseFilter(f)
        const existing = grouped.get(field) ?? []
        existing.push(f)
        grouped.set(field, existing)
      }
      result = result.filter(l =>
        [...grouped.values()].every(group =>
          group.some(f => matchesFilter(l, f))
        )
      )
    }

    return [...result].sort((a, b) => {
      if (selectedToTop && selectedLinkPks.size > 0) {
        const aPin = selectedLinkPks.has(a.link_pk) ? 0 : 1
        const bPin = selectedLinkPks.has(b.link_pk) ? 0 : 1
        if (aPin !== bPin) return aPin - bPin
      }
      const av = a[sortField] as string | number
      const bv = b[sortField] as string | number
      const cmp = typeof av === 'string' ? av.localeCompare(bv as string) : (av as number) - (bv as number)
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [links, allFilters, sortField, sortDir, selectedToTop, selectedLinkPks, showExcluded, showCategories])

  const handleSort = useCallback((field: SortField) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (prev.get('sort') === field || (!prev.get('sort') && field === 'rtt_a_to_z_ms')) {
        // Toggle direction
        const curDir = prev.get('sort_dir') || 'desc'
        const newDir = curDir === 'asc' ? 'desc' : 'asc'
        if (newDir === 'desc') next.delete('sort_dir')
        else next.set('sort_dir', 'asc')
      } else {
        // New field
        if (field === 'rtt_a_to_z_ms') next.delete('sort')
        else next.set('sort', field)
        const defaultDir = textFields.includes(field) ? 'asc' : 'desc'
        if (defaultDir === 'desc') next.delete('sort_dir')
        else next.set('sort_dir', defaultDir)
      }
      return next
    })
  }, [setSearchParams])

  const toggleSelect = useCallback((pk: string) => {
    const next = new Set(selectedLinkPks)
    if (next.has(pk)) next.delete(pk)
    else next.add(pk)
    setSelectedLinkPks(next)
  }, [selectedLinkPks, setSelectedLinkPks])

  // Build chart filter params — same filters as the table query so aggregate matches
  const chartFilters = useMemo(() => {
    const params: Record<string, string> = {}
    if (customStart && customEnd) {
      params.start_time = String(customStart)
      params.end_time = String(customEnd)
    }
    // Pass through dashboard dimension filters
    for (const [k, v] of Object.entries(filterParams)) {
      if (v && k !== 'time_range' && k !== 'threshold' && k !== 'bucket') {
        params[k] = v
      }
    }
    return Object.keys(params).length > 0 ? params : undefined
  }, [customStart, customEnd, filterParams])

  const deltaBadge = (measured: number, committed: number | undefined) => {
    if (!committed || committed <= 0 || committed >= COMMITTED_RTT_PROVISIONING_MS || measured <= 0) return null
    const pct = ((measured - committed) / committed) * 100
    const rounded = Math.round(pct)
    if (rounded === 0) return <span className="ml-1.5 text-[10px] text-muted-foreground">0%</span>
    const sign = rounded > 0 ? '+' : ''
    const color = ratioColor(measured, committed)
    return <span className={`ml-1.5 text-[10px] ${color}`}>{sign}{rounded}%</span>
  }

  const statusBadge = (link: LinkLatencySummary) => {
    if (link.isis_down) return <span className="ml-1.5 text-[10px] px-1 py-0.5 rounded bg-red-500/10 text-red-600 dark:text-red-400">isis down</span>
    if (link.provisioning) return <span className="ml-1.5 text-[10px] px-1 py-0.5 rounded bg-blue-500/10 text-blue-600 dark:text-blue-400">provisioning</span>
    if (link.link_status === 'soft-drained') return <span className="ml-1.5 text-[10px] px-1 py-0.5 rounded bg-amber-500/10 text-amber-600 dark:text-amber-400">soft-drained</span>
    if (link.link_status === 'hard-drained') return <span className="ml-1.5 text-[10px] px-1 py-0.5 rounded bg-amber-500/10 text-amber-600 dark:text-amber-400">hard-drained</span>
    if (link.link_status === 'suspended') return <span className="ml-1.5 text-[10px] px-1 py-0.5 rounded bg-red-500/10 text-red-600 dark:text-red-400">suspended</span>
    return null
  }

  const cellValue = (link: LinkLatencySummary, field: SortField) => {
    const v = link[field]
    if (typeof v === 'string') {
      if (field === 'link_code') {
        return <span className="text-foreground">{v || '—'}{statusBadge(link)}</span>
      }
      return <span className="text-foreground">{v || '—'}</span>
    }
    const n = v ?? 0
    if (field === 'committed_rtt_ms' || field === 'committed_jitter_ms') {
      if (n <= 0 || n >= COMMITTED_RTT_PROVISIONING_MS) return <span className="text-muted-foreground">—</span>
      return <span className="text-muted-foreground">{fmt(n)} ms</span>
    }
    if (field.startsWith('rtt_')) {
      return (
        <span className={ratioColor(n, link.committed_rtt_ms)}>
          {fmt(n)} ms{deltaBadge(n, link.committed_rtt_ms)}
        </span>
      )
    }
    if (field.startsWith('jitter_')) {
      return (
        <span className={ratioColor(n, link.committed_jitter_ms)}>
          {fmt(n)} ms{deltaBadge(n, link.committed_jitter_ms)}
        </span>
      )
    }
    if (field.startsWith('loss_')) return <span className={lossColor(n)}>{fmt(n, 1)}%</span>
    return <span>{String(n)}</span>
  }

  // Resizable table: maxHeight is user's preferred cap, content shrinks below it naturally
  const [maxTableHeight, setMaxTableHeight] = useState(320)
  const dragRef = useRef<{ startY: number; startHeight: number } | null>(null)

  const handleDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    dragRef.current = { startY: e.clientY, startHeight: maxTableHeight }

    const handleDragMove = (ev: MouseEvent) => {
      if (!dragRef.current) return
      const delta = ev.clientY - dragRef.current.startY
      setMaxTableHeight(Math.max(120, Math.min(800, dragRef.current.startHeight + delta)))
    }
    const handleDragEnd = () => {
      dragRef.current = null
      document.removeEventListener('mousemove', handleDragMove)
      document.removeEventListener('mouseup', handleDragEnd)
    }
    document.addEventListener('mousemove', handleDragMove)
    document.addEventListener('mouseup', handleDragEnd)
  }, [maxTableHeight])

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Sticky header */}
      <div className="flex-none bg-background border-b border-border px-4 sm:px-8 pt-6 pb-4 z-10">
        <div className="[&>div]:mb-0">
          <PageHeader
            icon={ArrowRightLeft}
            title="Link Latency"
            actions={<DashboardFilters hideMetric hideIntfType hideSearch />}
          />
        </div>
        <div className="flex items-center gap-2 mt-3 flex-wrap">
          <div className="flex items-center gap-2 ml-auto">
            <DashboardFilterBadges />
            {searchFilters.map((filter, idx) => (
              <button
                key={`${filter}-${idx}`}
                onClick={() => removeFilter(filter)}
                className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
              >
                {filter}
                <X className="h-3 w-3" />
              </button>
            ))}
            {searchFilters.length > 1 && (
              <button onClick={clearAllFilters} className="text-xs text-muted-foreground hover:text-foreground transition-colors">
                Clear all
              </button>
            )}
            <InlineFilter
              fieldPrefixes={filterFieldPrefixes}
              entity="links"
              autocompleteFields={filterAutocompleteFields}
              placeholder="Filter links..."
              onLiveFilterChange={setLiveFilter}
            />
            <StatusFilterDropdown showCategories={showCategories} onToggle={toggleShowCategory} />
            <AggSelector value={aggMethod} onChange={setAggMethod} />
          </div>
        </div>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-auto px-4 sm:px-8 py-6 space-y-6">
        {/* Link Table — collapsible, resizable with internal scroll */}
        <div className="border border-border rounded-lg overflow-hidden flex flex-col" style={tableCollapsed ? undefined : { maxHeight: maxTableHeight }}>
          {/* Table header */}
          <div className="flex-none px-3 py-2 flex items-center gap-2 border-b border-border">
            <button
              onClick={() => setTableCollapsed(c => !c)}
              className="text-muted-foreground hover:text-foreground transition-colors"
              title={tableCollapsed ? 'Show table' : 'Hide table'}
            >
              <ChevronDown className={`h-4 w-4 transition-transform ${tableCollapsed ? '-rotate-90' : ''}`} />
            </button>
            <span className="text-xs text-muted-foreground">{filteredLinks.length} links</span>
            {isFetching && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
            <div className="flex-1" />
            {selectedLinkPks.size > 0 && (
              <>
                <button
                  onClick={() => setSelectedToTop(p => !p)}
                  className={`px-1.5 py-0.5 rounded transition-colors ${selectedToTop ? 'text-foreground bg-muted' : 'text-muted-foreground hover:text-foreground'}`}
                  title={selectedToTop ? 'Stop sorting selected to top' : 'Sort selected to top'}
                >
                  <ArrowUpToLine className="h-3.5 w-3.5" />
                </button>
                <button
                  onClick={() => setSelectedLinkPks(new Set())}
                  className="text-xs text-muted-foreground hover:text-foreground transition-colors"
                >
                  Clear {selectedLinkPks.size}
                </button>
              </>
            )}
          </div>
          {/* Shimmer loading bar - overlays below header, no layout shift */}
          <div className="flex-none h-0 w-full relative z-[2]">
            {isFetching && <div className="absolute top-0 left-0 right-0 h-0.5 overflow-hidden"><div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" /></div>}
          </div>
          {!tableCollapsed && (
            <>
              <div className="flex-1 overflow-auto">
                {error ? (
                  <div className="p-8 text-center text-muted-foreground">
                    Error: {(error as Error).message || String(error)}
                  </div>
                ) : isFetching && links.length === 0 ? (
                  <div className="p-8 text-center text-muted-foreground">Loading...</div>
                ) : filteredLinks.length === 0 ? (
                  <div className="p-8 text-center text-muted-foreground">No links found</div>
                ) : (
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 z-[1] bg-muted/80 backdrop-blur-sm">
                      <tr className="border-b border-border">
                        <th className="w-1 px-0" />
                        {columns.map(col => (
                          <th
                            key={col.field}
                            className={`px-3 py-2 font-medium text-muted-foreground cursor-pointer select-none group/th whitespace-nowrap ${
                              col.align === 'right' ? 'text-right' : 'text-left'
                            }`}
                            onClick={() => handleSort(col.field)}
                          >
                            <span className="inline-flex items-center gap-1">
                              {col.label}
                              <SortIcon field={col.field} sortField={sortField} sortDir={sortDir} />
                            </span>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {filteredLinks.map(link => {
                        const isSelected = selectedLinkPks.has(link.link_pk)
                        const selectColor = isSelected ? selectedColorMap.get(link.link_pk) : undefined
                        return (
                          <tr
                            key={link.link_pk}
                            onClick={() => toggleSelect(link.link_pk)}
                            className="border-b border-border cursor-pointer transition-colors hover:bg-muted/50"
                          >
                            <td className="w-1 px-0">
                              {selectColor && (
                                <div className="w-1 h-full min-h-[32px] rounded-r-sm" style={{ backgroundColor: selectColor }} />
                              )}
                            </td>
                            {columns.map(col => (
                              <td
                                key={col.field}
                                className={`px-3 py-1.5 whitespace-nowrap ${col.align === 'right' ? 'text-right tabular-nums' : ''}`}
                              >
                                {cellValue(link, col.field)}
                              </td>
                            ))}
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                )}
              </div>

              {/* Drag handle to resize */}
              <div
                onMouseDown={handleDragStart}
                className="flex-none h-1.5 cursor-row-resize bg-transparent hover:bg-muted-foreground/20 transition-colors flex items-center justify-center border-t border-border"
              >
                <div className="w-8 h-0.5 rounded-full bg-muted-foreground/30" />
              </div>
            </>
          )}
        </div>

        {/* Charts — always visible */}
        <MultiLinkLatencyCharts
          pks={selectedPks.length > 0 ? selectedPks : filteredLinks.map(l => l.link_pk)}
          selectedCount={selectedLinkPks.size}
          linkNames={linkNameMap}
          timeRange={timeRange}
          agg={aggMethod}
          filters={chartFilters}
        />
      </div>
    </div>
  )
}

export function LinkLatencyPage() {
  return (
    <DashboardProvider defaultTimeRange="24h">
      <LinkLatencyPageContent />
    </DashboardProvider>
  )
}
