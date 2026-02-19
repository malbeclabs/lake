import { useState, useEffect, useRef, useMemo } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useInfiniteQuery } from '@tanstack/react-query'
import {
  Clock,
  AlertCircle,
  RefreshCw,
  Search,
  X,
} from 'lucide-react'
import {
  fetchTimeline,
  fetchTimelineBounds,
  type TimelineEvent,
  type TimeRange,
  type ActionFilter,
  type HistogramBucket,
} from '@/lib/api'
import {
  parseSetParam,
  serializeSetParam,
  presets,
  ALL_CATEGORIES,
  ALL_ACTIONS,
  ALL_ENTITY_TYPES,
  ALL_SOLANA_ENTITIES,
  DEFAULT_ENTITY_TYPES,
  getDateKey,
  type Category,
  type EntityType,
  type DZFilter,
  type MinStakeOption,
} from './timeline-constants'
import { TimelineFilters } from './timeline-filters'
import { TimelineEventCard, DateSeparator } from './timeline-event-card'
import { EventHistogram } from './timeline-histogram'
import { PageHeader } from './page-header'

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

export function TimelinePage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const searchParam = searchParams.get('search') || ''
  const searchFilters = useMemo(() => searchParam ? searchParam.split(',').map(f => f.trim()).filter(Boolean) : [], [searchParam])

  const initialTimeRange = (searchParams.get('range') || '24h') as TimeRange | 'custom'
  const initialCategories = parseSetParam(searchParams.get('categories'), ALL_CATEGORIES, ALL_CATEGORIES)
  const initialEntityTypes = parseSetParam(searchParams.get('entities'), ALL_ENTITY_TYPES, DEFAULT_ENTITY_TYPES)
  const initialActions = parseSetParam(searchParams.get('actions'), ALL_ACTIONS, ALL_ACTIONS)
  const initialDzFilter = (searchParams.get('dz') || 'on_dz') as DZFilter
  const initialMinStake = (searchParams.get('min_stake') || '0') as MinStakeOption
  const initialIncludeInternal = searchParams.get('internal') === 'true'
  const initialCustomStart = searchParams.get('start') || ''
  const initialCustomEnd = searchParams.get('end') || ''

  const [timeRange, setTimeRange] = useState<TimeRange | 'custom'>(initialTimeRange)
  const [selectedCategories, setSelectedCategories] = useState<Set<Category>>(initialCategories)
  const [selectedEntityTypes, setSelectedEntityTypes] = useState<Set<EntityType>>(initialEntityTypes)
  const [selectedActions, setSelectedActions] = useState<Set<ActionFilter>>(initialActions)
  const [dzFilter, setDzFilter] = useState<DZFilter>(initialDzFilter)
  const [minStake, setMinStake] = useState<MinStakeOption>(initialMinStake)
  const [includeInternal, setIncludeInternal] = useState(initialIncludeInternal)
  const [customStart, setCustomStart] = useState<string>(initialCustomStart)
  const [customEnd, setCustomEnd] = useState<string>(initialCustomEnd)
  const limit = 50

  const loadMoreRef = useRef<HTMLDivElement>(null)

  const searchParamsString = searchParams.toString()
  useEffect(() => {
    setTimeRange((searchParams.get('range') || '24h') as TimeRange | 'custom')
    setSelectedCategories(parseSetParam(searchParams.get('categories'), ALL_CATEGORIES, ALL_CATEGORIES))
    setSelectedEntityTypes(parseSetParam(searchParams.get('entities'), ALL_ENTITY_TYPES, DEFAULT_ENTITY_TYPES))
    setSelectedActions(parseSetParam(searchParams.get('actions'), ALL_ACTIONS, ALL_ACTIONS))
    setDzFilter((searchParams.get('dz') || 'on_dz') as DZFilter)
    setMinStake((searchParams.get('min_stake') || '0') as MinStakeOption)
    setIncludeInternal(searchParams.get('internal') === 'true')
    setCustomStart(searchParams.get('start') || '')
    setCustomEnd(searchParams.get('end') || '')
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParamsString])

  const updateUrlParams = (updates: {
    range?: TimeRange | 'custom'
    categories?: Set<Category>
    entities?: Set<EntityType>
    actions?: Set<ActionFilter>
    dz?: DZFilter
    min_stake?: MinStakeOption
    internal?: boolean
    start?: string
    end?: string
    search?: string | null
  }) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      if (updates.range !== undefined) {
        if (updates.range === '24h') next.delete('range'); else next.set('range', updates.range)
      }
      if (updates.categories !== undefined) {
        const s = serializeSetParam(updates.categories, ALL_CATEGORIES)
        if (s) next.set('categories', s); else next.delete('categories')
      }
      if (updates.entities !== undefined) {
        const s = serializeSetParam(updates.entities, DEFAULT_ENTITY_TYPES)
        if (s) next.set('entities', s); else next.delete('entities')
      }
      if (updates.actions !== undefined) {
        const s = serializeSetParam(updates.actions, ALL_ACTIONS)
        if (s) next.set('actions', s); else next.delete('actions')
      }
      if (updates.dz !== undefined) {
        if (updates.dz === 'on_dz') next.delete('dz'); else next.set('dz', updates.dz)
      }
      if (updates.min_stake !== undefined) {
        if (updates.min_stake !== '0') next.set('min_stake', updates.min_stake); else next.delete('min_stake')
      }
      if (updates.internal !== undefined) {
        if (updates.internal) next.set('internal', 'true'); else next.delete('internal')
      }
      if (updates.start !== undefined) {
        if (updates.start) next.set('start', updates.start); else next.delete('start')
      }
      if (updates.end !== undefined) {
        if (updates.end) next.set('end', updates.end); else next.delete('end')
      }
      if (updates.search !== undefined) {
        if (updates.search) next.set('search', updates.search); else next.delete('search')
      }
      return next
    })
  }

  const clearSearchFilter = () => updateUrlParams({ search: null })
  const removeSearchFilter = (filterToRemove: string) => {
    const newFilters = searchFilters.filter(f => f !== filterToRemove)
    updateUrlParams({ search: newFilters.length > 0 ? newFilters.join(',') : null })
  }

  const { data: bounds } = useQuery({
    queryKey: ['timeline-bounds'],
    queryFn: fetchTimelineBounds,
    staleTime: 60_000,
  })

  const lastSeenTimestamp = useRef<string>('')
  const [newEventIds, setNewEventIds] = useState<Set<string>>(new Set())

  const categoryFilter = selectedCategories.size === ALL_CATEGORIES.length ? undefined : Array.from(selectedCategories).join(',')
  const entityTypeFilter = selectedEntityTypes.size === ALL_ENTITY_TYPES.length ? undefined : Array.from(selectedEntityTypes).join(',')
  const actionFilter = selectedActions.size === ALL_ACTIONS.length ? undefined : Array.from(selectedActions).join(',')
  const hasSolanaEntities = ALL_SOLANA_ENTITIES.some(e => selectedEntityTypes.has(e))
  const dzFilterParam = hasSolanaEntities && dzFilter !== 'all' ? dzFilter : undefined
  const minStakePctParam = hasSolanaEntities && minStake !== '0' ? parseFloat(minStake) : undefined

  const {
    data,
    isLoading,
    error,
    refetch,
    isFetching,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ['timeline', timeRange, customStart, customEnd, categoryFilter, entityTypeFilter, actionFilter, dzFilterParam, minStakePctParam, includeInternal, searchParam],
    queryFn: ({ pageParam = 0 }) => fetchTimeline({
      range: timeRange !== 'custom' ? timeRange : undefined,
      start: timeRange === 'custom' && customStart ? customStart : undefined,
      end: timeRange === 'custom' && customEnd ? customEnd : undefined,
      category: categoryFilter,
      entity_type: entityTypeFilter,
      action: actionFilter,
      dz_filter: dzFilterParam,
      min_stake_pct: minStakePctParam,
      search: searchParam || undefined,
      include_internal: includeInternal,
      limit,
      offset: pageParam,
    }),
    getNextPageParam: (lastPage, allPages) => {
      const totalLoaded = allPages.reduce((acc, page) => acc + page.events.length, 0)
      return totalLoaded < lastPage.total ? totalLoaded : undefined
    },
    initialPageParam: 0,
    refetchInterval: timeRange !== 'custom' ? 15_000 : undefined,
    staleTime: timeRange === '24h' ? 10_000 : 0,
  })

  const allEvents = useMemo(() => data?.pages.flatMap(page => page.events) ?? [], [data])
  const totalCount = data?.pages[0]?.total ?? 0
  const histogram = data?.pages[0]?.histogram

  useEffect(() => {
    const sentinel = loadMoreRef.current
    if (!sentinel) return
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasNextPage && !isFetchingNextPage) fetchNextPage()
      },
      { threshold: 0.1 }
    )
    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [hasNextPage, isFetchingNextPage, fetchNextPage])

  useEffect(() => {
    if (allEvents.length === 0) return
    const newIds = new Set<string>()
    const mostRecentTimestamp = allEvents[0]?.timestamp || ''
    if (lastSeenTimestamp.current) {
      for (const event of allEvents) {
        if (event.timestamp > lastSeenTimestamp.current) newIds.add(event.id)
      }
    }
    if (mostRecentTimestamp && mostRecentTimestamp > lastSeenTimestamp.current) {
      lastSeenTimestamp.current = mostRecentTimestamp
    }
    if (newIds.size > 0) {
      setNewEventIds(newIds)
      setTimeout(() => setNewEventIds(new Set()), 5000)
    }
  }, [allEvents])

  const resetSeenEvents = () => {
    lastSeenTimestamp.current = ''
    setNewEventIds(new Set())
  }

  const resetAllFilters = () => {
    setTimeRange('24h')
    setSelectedCategories(new Set(ALL_CATEGORIES))
    setSelectedEntityTypes(new Set(DEFAULT_ENTITY_TYPES))
    setSelectedActions(new Set(ALL_ACTIONS))
    setDzFilter('on_dz')
    setMinStake('0')
    setIncludeInternal(false)
    setCustomStart('')
    setCustomEnd('')
    resetSeenEvents()
    setSearchParams(new URLSearchParams())
  }

  const filteredEvents = useMemo(() => {
    if (allEvents.length === 0 || searchFilters.length === 0) return allEvents
    const matchesSearch = (value: unknown, lowerSearch: string): boolean => {
      if (!value) return false
      if (typeof value === 'string') return value.toLowerCase().includes(lowerSearch)
      if (typeof value === 'number') return String(value).includes(lowerSearch)
      return false
    }
    const eventMatchesFilter = (event: TimelineEvent, filter: string): boolean => {
      const lowerSearch = filter.toLowerCase()
      if (matchesSearch(event.entity_code, lowerSearch)) return true
      if (matchesSearch(event.title, lowerSearch)) return true
      if (matchesSearch(event.description, lowerSearch)) return true
      const details = event.details
      if (!details) return false
      if ('entity' in details && details.entity) {
        const entity = details.entity as unknown as Record<string, unknown>
        if (matchesSearch(entity.device_code, lowerSearch)) return true
        if (matchesSearch(entity.metro_code, lowerSearch)) return true
        if (matchesSearch(entity.contributor_code, lowerSearch)) return true
        if (matchesSearch(entity.code, lowerSearch)) return true
        if (matchesSearch(entity.name, lowerSearch)) return true
        if (matchesSearch(entity.public_ip, lowerSearch)) return true
        if (matchesSearch(entity.client_ip, lowerSearch)) return true
        if (matchesSearch(entity.dz_ip, lowerSearch)) return true
        if (matchesSearch(entity.owner_pubkey, lowerSearch)) return true
        if (matchesSearch(entity.side_a_code, lowerSearch)) return true
        if (matchesSearch(entity.side_z_code, lowerSearch)) return true
        if (matchesSearch(entity.side_a_metro_code, lowerSearch)) return true
        if (matchesSearch(entity.side_z_metro_code, lowerSearch)) return true
      }
      if ('device_code' in details && matchesSearch(details.device_code, lowerSearch)) return true
      if ('metro_code' in details && matchesSearch(details.metro_code, lowerSearch)) return true
      if ('owner_pubkey' in details && matchesSearch(details.owner_pubkey, lowerSearch)) return true
      if ('vote_pubkey' in details && matchesSearch(details.vote_pubkey, lowerSearch)) return true
      if ('node_pubkey' in details && matchesSearch(details.node_pubkey, lowerSearch)) return true
      if ('dz_ip' in details && matchesSearch(details.dz_ip, lowerSearch)) return true
      if ('interface_name' in details && matchesSearch(details.interface_name, lowerSearch)) return true
      if ('link_code' in details && matchesSearch(details.link_code, lowerSearch)) return true
      if ('interfaces' in details && Array.isArray(details.interfaces)) {
        for (const intf of details.interfaces) {
          if (matchesSearch(intf.interface_name, lowerSearch)) return true
          if (matchesSearch(intf.link_code, lowerSearch)) return true
        }
      }
      if ('side_a_metro' in details && matchesSearch(details.side_a_metro, lowerSearch)) return true
      if ('side_z_metro' in details && matchesSearch(details.side_z_metro, lowerSearch)) return true
      return false
    }
    return allEvents.filter(event =>
      searchFilters.some(filter => eventMatchesFilter(event, filter))
    )
  }, [allEvents, searchFilters])

  const hasActiveFilters = timeRange !== '24h' ||
    selectedCategories.size !== ALL_CATEGORIES.length ||
    selectedEntityTypes.size !== DEFAULT_ENTITY_TYPES.length ||
    !DEFAULT_ENTITY_TYPES.every(e => selectedEntityTypes.has(e)) ||
    selectedActions.size !== ALL_ACTIONS.length ||
    dzFilter !== 'on_dz' ||
    minStake !== '0' ||
    includeInternal ||
    searchFilters.length > 0

  const handleBucketClick = (bucket: HistogramBucket, nextBucket?: HistogramBucket) => {
    const start = new Date(bucket.timestamp)
    let end: Date
    if (nextBucket) {
      end = new Date(nextBucket.timestamp)
    } else if (histogram && histogram.length > 1) {
      const bucketDuration = new Date(histogram[1].timestamp).getTime() - new Date(histogram[0].timestamp).getTime()
      end = new Date(start.getTime() + bucketDuration)
    } else {
      end = new Date(start.getTime() + 30 * 60 * 1000)
    }
    const startStr = start.toISOString().slice(0, 16)
    const endStr = end.toISOString().slice(0, 16)
    setTimeRange('custom')
    setCustomStart(startStr)
    setCustomEnd(endStr)
    updateUrlParams({ range: 'custom', start: startStr, end: endStr })
    resetSeenEvents()
  }

  const handleTimeRangeChange = (range: TimeRange | 'custom') => {
    setTimeRange(range)
    if (range === 'custom' && bounds) {
      const end = new Date()
      const start = new Date(end.getTime() - 24 * 60 * 60 * 1000)
      const earliest = new Date(bounds.earliest_data)
      if (start < earliest) start.setTime(earliest.getTime())
      const startStr = start.toISOString().slice(0, 16)
      const endStr = end.toISOString().slice(0, 16)
      setCustomStart(startStr)
      setCustomEnd(endStr)
      updateUrlParams({ range, start: startStr, end: endStr })
    } else {
      updateUrlParams({ range, start: '', end: '' })
    }
    resetSeenEvents()
  }

  const handleApplyPreset = (preset: typeof presets[0]) => {
    const next = new URLSearchParams(preset.params)
    setSearchParams(next)
    setTimeRange((preset.params.range || '24h') as TimeRange | 'custom')
    setSelectedCategories(parseSetParam(preset.params.categories || null, ALL_CATEGORIES, ALL_CATEGORIES))
    setSelectedEntityTypes(parseSetParam(preset.params.entities || null, ALL_ENTITY_TYPES, DEFAULT_ENTITY_TYPES))
    setSelectedActions(parseSetParam(preset.params.actions || null, ALL_ACTIONS, ALL_ACTIONS))
    setDzFilter((preset.params.dz || 'on_dz') as DZFilter)
    setMinStake((preset.params.min_stake || '0') as MinStakeOption)
    setIncludeInternal(false)
    setCustomStart('')
    setCustomEnd('')
    resetSeenEvents()
  }

  // Build event list with date separators
  const eventsWithSeparators = useMemo(() => {
    const items: { type: 'separator'; date: string; key: string }[] | { type: 'event'; event: TimelineEvent; key: string }[] = []
    let lastDateKey = ''
    for (const event of filteredEvents) {
      const dateKey = getDateKey(event.timestamp)
      if (dateKey !== lastDateKey) {
        (items as { type: 'separator'; date: string; key: string }[]).push({ type: 'separator', date: event.timestamp, key: `sep-${dateKey}` })
        lastDateKey = dateKey
      }
      (items as { type: 'event'; event: TimelineEvent; key: string }[]).push({ type: 'event', event, key: event.id })
    }
    return items as ({ type: 'separator'; date: string; key: string } | { type: 'event'; event: TimelineEvent; key: string })[]
  }, [filteredEvents])

  return (
    <div className="flex-1 overflow-auto scroll-smooth">
      <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Clock}
          title="Timeline"
          subtitle={
            <>
              {totalCount > 0 && (
                <span className="text-sm text-muted-foreground bg-muted px-2 py-0.5 rounded">
                  {searchFilters.length > 0 ? `${filteredEvents.length} of ${totalCount.toLocaleString()}` : totalCount.toLocaleString()} events
                </span>
              )}
              {newEventIds.size > 0 && (
                <span className="text-xs px-2 py-0.5 rounded-full bg-primary text-primary-foreground animate-pulse">
                  +{newEventIds.size} new
                </span>
              )}
              {isFetching && !isLoading && (
                <span className="text-xs text-muted-foreground">Updating...</span>
              )}
            </>
          }
          actions={searchFilters.length > 0 ? (
            <>
              {searchFilters.map((filter, idx) => (
                <button
                  key={idx}
                  onClick={() => removeSearchFilter(filter)}
                  className="inline-flex items-center gap-1 text-sm px-2 py-0.5 rounded bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
                >
                  <Search className="h-3 w-3" />
                  {filter}
                  <X className="h-3 w-3" />
                </button>
              ))}
              {searchFilters.length > 1 && (
                <button
                  onClick={clearSearchFilter}
                  className="text-xs text-muted-foreground hover:text-foreground transition-colors"
                >
                  Clear all
                </button>
              )}
            </>
          ) : undefined}
        />

        {/* Filters */}
        <TimelineFilters
          searchParams={searchParams}
          timeRange={timeRange}
          selectedCategories={selectedCategories}
          selectedEntityTypes={selectedEntityTypes}
          selectedActions={selectedActions}
          dzFilter={dzFilter}
          minStake={minStake}
          includeInternal={includeInternal}
          customStart={customStart}
          customEnd={customEnd}
          isFetching={isFetching}
          hasActiveFilters={hasActiveFilters}
          onTimeRangeChange={handleTimeRangeChange}
          onToggleCategory={(category) => {
            const next = new Set(selectedCategories)
            if (next.has(category)) next.delete(category); else next.add(category)
            setSelectedCategories(next)
            updateUrlParams({ categories: next })
            resetSeenEvents()
          }}
          onToggleEntityType={(entityType) => {
            const next = new Set(selectedEntityTypes)
            if (next.has(entityType)) next.delete(entityType); else next.add(entityType)
            setSelectedEntityTypes(next)
            updateUrlParams({ entities: next })
            resetSeenEvents()
          }}
          onToggleAction={(action) => {
            const next = new Set(selectedActions)
            if (next.has(action)) next.delete(action); else next.add(action)
            setSelectedActions(next)
            updateUrlParams({ actions: next })
            resetSeenEvents()
          }}
          onDzFilterChange={(dz) => {
            setDzFilter(dz)
            updateUrlParams({ dz })
            resetSeenEvents()
          }}
          onMinStakeChange={(value) => {
            setMinStake(value)
            updateUrlParams({ min_stake: value })
            resetSeenEvents()
          }}
          onIncludeInternalChange={(value) => {
            setIncludeInternal(value)
            updateUrlParams({ internal: value })
            resetSeenEvents()
          }}
          onCustomStartChange={(value) => {
            setCustomStart(value)
            updateUrlParams({ start: value })
            resetSeenEvents()
          }}
          onCustomEndChange={(value) => {
            setCustomEnd(value)
            updateUrlParams({ end: value })
            resetSeenEvents()
          }}
          onRefetch={() => refetch()}
          onResetAll={resetAllFilters}
          onApplyPreset={handleApplyPreset}
        />

        {/* Histogram */}
        {histogram && histogram.length > 0 && (
          <EventHistogram data={histogram} onBucketClick={handleBucketClick} />
        )}

        {/* Loading state */}
        {isLoading && (
          <div className="space-y-1">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="flex gap-3">
                <div className="flex flex-col items-center pt-1">
                  <Skeleton className="h-2.5 w-2.5 rounded-full" />
                </div>
                <div className="flex-1 border border-border/50 rounded-md p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <Skeleton className="h-4 w-24" />
                    <Skeleton className="h-5 w-16 rounded" />
                    <Skeleton className="h-4 w-16" />
                  </div>
                  <Skeleton className="h-4 w-3/4" />
                  <Skeleton className="h-3 w-1/2 mt-2" />
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Error state */}
        {error && (
          <div className="text-center py-12">
            <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-3" />
            <div className="text-sm text-muted-foreground">
              {error instanceof Error ? error.message : 'Failed to load timeline'}
            </div>
          </div>
        )}

        {/* Empty state */}
        {data && filteredEvents.length === 0 && (
          <div className="text-center py-12 border border-dashed border-border rounded-lg">
            <Clock className="h-12 w-12 text-muted-foreground/50 mx-auto mb-3" />
            <div className="text-sm text-muted-foreground">
              {searchFilters.length > 0
                ? `No events matching filters: ${searchFilters.join(', ')}`
                : 'No events found in the selected time range'}
            </div>
            {searchFilters.length > 0 && (
              <button
                onClick={clearSearchFilter}
                className="mt-2 text-sm text-blue-500 hover:underline"
              >
                Clear search filters
              </button>
            )}
          </div>
        )}

        {/* Timeline event list with vertical line */}
        {data && filteredEvents.length > 0 && (
          <>
            <div className="relative">
              {/* Vertical timeline line */}
              <div className="absolute left-[4px] top-0 bottom-0 w-0.5 bg-border" />

              <div className="space-y-1">
                {eventsWithSeparators.map(item => {
                  if (item.type === 'separator') {
                    return <DateSeparator key={item.key} date={item.date} />
                  }
                  return (
                    <TimelineEventCard
                      key={item.key}
                      event={item.event}
                      isNew={newEventIds.has(item.event.id)}
                    />
                  )
                })}
              </div>
            </div>

            {/* Infinite scroll sentinel */}
            {searchFilters.length === 0 && (
              <div ref={loadMoreRef} className="py-8 flex justify-center">
                {isFetchingNextPage ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    Loading more...
                  </div>
                ) : hasNextPage ? (
                  <div className="text-sm text-muted-foreground">Scroll for more</div>
                ) : allEvents.length > limit ? (
                  <div className="text-sm text-muted-foreground">All {totalCount.toLocaleString()} events loaded</div>
                ) : null}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
