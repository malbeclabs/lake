import { Fragment, useState, useRef, useCallback } from 'react'
import { cn } from '@/lib/utils'
import { evBg, evBorderColor, evText, dotColor } from './colors'
import {
  startOfDay,
  isSameDay,
  daysBetween,
  isWeekend,
  getDays,
} from './date-utils'
import { EventTooltipPanel } from './event-tooltip'
import type { TooltipState } from './event-tooltip'
import type { MaintenanceEvent } from './use-maintenance-events'

const LBL_W = 220
const DAY_W_NORMAL = 54
const DAY_W_MONTH = 34
const MIN_BAR_TEXT_PX = 18  // hide only sub-icon-width bars
const DAY_ABBR = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

interface ContributorGroup {
  contributorName: string
  hue: number
  events: MaintenanceEvent[]
}

function groupByContributor(events: MaintenanceEvent[]): ContributorGroup[] {
  const map = new Map<string, ContributorGroup>()
  for (const ev of events) {
    if (!map.has(ev.contributorName)) {
      map.set(ev.contributorName, { contributorName: ev.contributorName, hue: ev.hue, events: [] })
    }
    map.get(ev.contributorName)!.events.push(ev)
  }
  return [...map.values()].sort((a, b) => a.contributorName.localeCompare(b.contributorName))
}

interface GanttViewProps {
  events: MaintenanceEvent[]
  view: 'week' | '2week' | 'month'
  anchor: Date
}

export function GanttView({ events, view, anchor }: GanttViewProps) {
  const today = startOfDay(new Date())
  const days = getDays(view, anchor)
  const DAY_W = view === 'month' ? DAY_W_MONTH : DAY_W_NORMAL
  const rangeStart = startOfDay(days[0])
  const rangeEnd = startOfDay(days[days.length - 1])

  const rangeEndInclusive = new Date(rangeEnd)
  rangeEndInclusive.setHours(23, 59, 59, 999)

  const visibleEvents = events.filter(
    (ev) => ev.startAt <= rangeEndInclusive && ev.endAt >= rangeStart
  )
  const groups = groupByContributor(visibleEvents)

  // Tooltip state with grace-period hide so links are clickable
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  const hideTimer = useRef<ReturnType<typeof setTimeout>>(undefined)

  const showTooltip = useCallback((ev: MaintenanceEvent, e: React.MouseEvent) => {
    clearTimeout(hideTimer.current)
    setTooltip({ ev, x: e.clientX, y: e.clientY })
  }, [])

  const startHide = useCallback(() => {
    hideTimer.current = setTimeout(() => setTooltip(null), 200)
  }, [])

  const cancelHide = useCallback(() => {
    clearTimeout(hideTimer.current)
  }, [])

  return (
    <>
      <div className="flex-1 overflow-auto">
        <table
          style={{
            tableLayout: 'fixed',
            borderCollapse: 'collapse',
            width: LBL_W + days.length * DAY_W,
          }}
        >
          <colgroup>
            <col style={{ width: LBL_W }} />
            {days.map((_, i) => (
              <col key={i} style={{ width: DAY_W }} />
            ))}
          </colgroup>

          <thead>
            <tr>
              <th className="sticky left-0 z-20 text-left text-[10px] font-normal text-muted-foreground/70 uppercase tracking-widest px-4 py-2 border-b border-r border-border bg-[var(--sidebar)] h-10">
                Contributor / Event
              </th>
              {days.map((day, i) => {
                const isToday = isSameDay(day, today)
                const weekend = isWeekend(day)
                return (
                  <th
                    key={i}
                    className={cn(
                      'text-center text-[10px] font-normal border-b border-r border-border h-10',
                      isToday
                        ? 'bg-[oklch(0.55_0.06_250/6%)] text-[oklch(0.7_0.12_250)]'
                        : weekend
                        ? 'bg-[oklch(0_0_0/40%)] text-muted-foreground/60'
                        : 'text-muted-foreground'
                    )}
                  >
                    <div className="leading-none">{DAY_ABBR[day.getDay()]}</div>
                    <div className={cn('text-sm mt-0.5', isToday && 'font-semibold')}>
                      {day.getDate()}
                    </div>
                  </th>
                )
              })}
            </tr>
          </thead>

          <tbody>
            {groups.length === 0 ? (
              <tr>
                <td
                  colSpan={days.length + 1}
                  className="text-center text-sm text-muted-foreground py-12"
                >
                  No maintenance events in this period.
                </td>
              </tr>
            ) : (
              groups.map(({ contributorName, hue, events: groupEvents }) => (
                <Fragment key={contributorName}>
                  {/* Contributor header row */}
                  <tr className="bg-[oklch(0.17_0.003_285.823)]">
                    <td className="sticky left-0 z-10 border-b border-r border-border bg-[oklch(0.17_0.003_285.823)] h-8">
                      <div className="flex items-center gap-2 px-4 h-full text-sm font-medium text-foreground">
                        <span
                          className="inline-block w-2 h-2 rounded-full flex-shrink-0"
                          style={{ background: dotColor(hue) }}
                        />
                        <span className="truncate">{contributorName}</span>
                        <span className="ml-auto text-[10px] text-muted-foreground bg-muted px-1.5 py-0.5 flex-shrink-0">
                          {groupEvents.length}
                        </span>
                      </div>
                    </td>
                    {days.map((day, i) => (
                      <td
                        key={i}
                        className={cn(
                          'border-b border-r border-border h-8',
                          isSameDay(day, today)
                            ? 'bg-[oklch(0.55_0.06_250/8%)]'
                            : isWeekend(day)
                            ? 'bg-[oklch(0_0_0/40%)]'
                            : ''
                        )}
                      />
                    ))}
                  </tr>

                  {/* One row per event */}
                  {groupEvents.map((ev) => {
                    const clampedStart = ev.startAt < rangeStart ? rangeStart : startOfDay(ev.startAt)
                    const clampedEnd = ev.endAt > rangeEndInclusive ? rangeEnd : startOfDay(ev.endAt)
                    const startDayIdx = daysBetween(rangeStart, clampedStart)
                    const spanDays = daysBetween(clampedStart, clampedEnd) + 1
                    // Cap width so bar never overflows the right grid boundary
                    const maxSpan = days.length - startDayIdx
                    const barWidth = Math.min(spanDays, maxSpan) * DAY_W - 1

                    return (
                      <tr key={ev.id}>
                        <td className="sticky left-0 z-10 px-4 py-0 text-xs text-muted-foreground border-b border-r border-border bg-card h-[30px] truncate">
                          {ev.title}
                        </td>
                        {days.map((day, dayIdx) => {
                          const isBarStart = dayIdx === startDayIdx
                          return (
                            <td
                              key={dayIdx}
                              className={cn(
                                'relative border-b border-r border-border h-[30px] p-0',
                                isSameDay(day, today)
                                  ? 'bg-[oklch(0.55_0.06_250/6%)]'
                                  : isWeekend(day)
                                  ? 'bg-[oklch(0_0_0/40%)]'
                                  : ''
                              )}
                            >
                              {isBarStart && (
                                <div
                                  className={cn(
                                    'absolute top-[4px] bottom-[4px] z-[4] flex items-center px-2 text-[11px] whitespace-nowrap overflow-hidden cursor-pointer',
                                    ev.status === 'in-progress'
                                      ? 'border-l-[3px] border-dashed'
                                      : 'border-l-[3px]'
                                  )}
                                  style={{
                                    left: 0,
                                    width: barWidth,
                                    background: evBg(hue),
                                    borderLeftColor: evBorderColor(hue),
                                    color: evText(hue),
                                  }}
                                  onMouseEnter={(e) => showTooltip(ev, e)}
                                  onMouseLeave={startHide}
                                >
                                  {barWidth >= MIN_BAR_TEXT_PX && ev.title}
                                </div>
                              )}
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                </Fragment>
              ))
            )}
          </tbody>
        </table>
      </div>

      {tooltip && (
        <EventTooltipPanel
          state={tooltip}
          onMouseEnter={cancelHide}
          onMouseLeave={startHide}
        />
      )}
    </>
  )
}
