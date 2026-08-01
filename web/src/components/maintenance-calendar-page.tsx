import { useMemo, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import { useMaintenanceEvents } from './maintenance-calendar/use-maintenance-events'
import { MaintenanceFilters } from './maintenance-calendar/filters'
import { GanttView } from './maintenance-calendar/gantt-view'
import { DayView } from './maintenance-calendar/day-view'
import {
  startOfDay,
  getDays,
  formatDateRange,
  formatISODate,
  parseISODate,
  isSameDay,
} from './maintenance-calendar/date-utils'
import type { CalendarView } from './maintenance-calendar/date-utils'

const VALID_VIEWS: CalendarView[] = ['day', 'week', '2week', 'month']

function navigate(anchor: Date, view: CalendarView, dir: -1 | 1): Date {
  const next = new Date(anchor)
  if (view === 'day') next.setDate(next.getDate() + dir)
  else if (view === 'week') next.setDate(next.getDate() + dir * 7)
  else if (view === '2week') next.setDate(next.getDate() + dir * 14)
  else next.setMonth(next.getMonth() + dir)
  return next
}

export function MaintenanceCalendarPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const view = useMemo<CalendarView>(() => {
    const v = searchParams.get('view') as CalendarView | null
    return v && VALID_VIEWS.includes(v) ? v : '2week'
  }, [searchParams])
  const anchor = useMemo(
    () => parseISODate(searchParams.get('date')) ?? startOfDay(new Date()),
    [searchParams]
  )

  const {
    events,
    allContributors,
    allMetros,
    allDevices,
    allLinks,
    rawCount,
    filters,
    setFilters,
    hasActiveFilters,
    isLoading,
    error,
  } = useMaintenanceEvents()

  const days = view === 'day' ? [anchor] : getDays(view, anchor)

  const setParam = useCallback(
    (key: string, value: string | null) =>
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          if (value === null) next.delete(key)
          else next.set(key, value)
          return next
        },
        { replace: true }
      ),
    [setSearchParams]
  )

  const setAnchorParam = useCallback(
    (d: Date) =>
      setParam('date', isSameDay(d, startOfDay(new Date())) ? null : formatISODate(d)),
    [setParam]
  )

  const handleNavigate = useCallback(
    (dir: -1 | 1) => setAnchorParam(navigate(anchor, view, dir)),
    [anchor, view, setAnchorParam]
  )

  const handleViewChange = useCallback(
    (v: CalendarView) => setParam('view', v === '2week' ? null : v),
    [setParam]
  )

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Page header */}
      <div className="flex items-center justify-between gap-4 px-6 py-3 border-b border-border flex-shrink-0">
        <h1 className="text-xl font-medium">Maintenance Calendar</h1>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => setAnchorParam(startOfDay(new Date()))}
            className="text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            Go to today
          </button>
          <div className="flex items-center border border-border">
            <button
              type="button"
              onClick={() => handleNavigate(-1)}
              className="h-[30px] w-[30px] flex items-center justify-center text-muted-foreground hover:text-foreground hover:bg-muted/30 border-r border-border transition-colors"
              aria-label="Previous period"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            <span className="h-[30px] flex items-center px-4 text-sm text-muted-foreground bg-[var(--input)] whitespace-nowrap min-w-[200px] justify-center">
              {formatDateRange(view, anchor, days)}
            </span>
            <button
              type="button"
              onClick={() => handleNavigate(1)}
              className="h-[30px] w-[30px] flex items-center justify-center text-muted-foreground hover:text-foreground hover:bg-muted/30 border-l border-border transition-colors"
              aria-label="Next period"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Filters toolbar */}
      <MaintenanceFilters
        filters={filters}
        onFiltersChange={setFilters}
        hasActiveFilters={hasActiveFilters}
        allContributors={allContributors}
        allMetros={allMetros}
        allDevices={allDevices}
        allLinks={allLinks}
        view={view}
        onViewChange={handleViewChange}
      />

      {/* Calendar body */}
      {isLoading ? (
        <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
          Loading maintenance events…
        </div>
      ) : error ? (
        <div className="flex-1 flex items-center justify-center text-sm text-destructive">
          Failed to load maintenance events: {String(error)}
        </div>
      ) : rawCount === 0 ? (
        <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
          No maintenance tickets found in the system.
        </div>
      ) : view === 'day' ? (
        <DayView events={events} anchor={anchor} />
      ) : (
        <GanttView events={events} view={view} anchor={anchor} />
      )}

      {/* Legend */}
      <div className="flex items-center gap-5 px-6 py-2 border-t border-border bg-card text-xs text-muted-foreground flex-shrink-0">
        <div className="flex items-center gap-2">
          <div className="w-5 h-2.5 border-l-[3px] bg-[oklch(0.5_0.1_250/30%)] border-l-[oklch(0.65_0.12_250/80%)]" />
          Planned
        </div>
        <div className="flex items-center gap-2">
          <div className="w-5 h-2.5 border-l-[3px] border-dashed bg-[oklch(0.5_0.1_140/30%)] border-l-[oklch(0.65_0.12_140/80%)]" />
          In progress
        </div>
        <div className="flex items-center gap-2">
          <div className="h-0.5 w-5 bg-accent" />
          Current time
        </div>
        <span className="ml-auto">
          {events.length === rawCount
            ? `${events.length} event${events.length !== 1 ? 's' : ''}`
            : `${events.length} of ${rawCount} event${rawCount !== 1 ? 's' : ''} (filtered)`}
        </span>
      </div>
    </div>
  )
}
