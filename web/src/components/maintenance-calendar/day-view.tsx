import { useRef, useEffect, useState, useCallback } from 'react'
import { cn } from '@/lib/utils'
import { evBg, evBorderColor, evText } from './colors'
import { isSameDay, startOfDay, formatHour } from './date-utils'
import { EventTooltipPanel } from './event-tooltip'
import type { TooltipState } from './event-tooltip'
import type { MaintenanceEvent } from './use-maintenance-events'

const HOUR_H = 64  // px per hour

interface DayViewProps {
  events: MaintenanceEvent[]
  anchor: Date
}

interface TimedEvent {
  ev: MaintenanceEvent
  sh: number
  eh: number
  colIdx: number
}

function assignColumns(items: Omit<TimedEvent, 'colIdx'>[]): TimedEvent[] {
  const cols: Array<Array<{ sh: number; eh: number }>> = []
  return items.map((item) => {
    let colIdx = -1
    for (let c = 0; c < cols.length; c++) {
      if (!cols[c].some((x) => item.sh < x.eh && item.eh > x.sh)) {
        cols[c].push(item)
        colIdx = c
        break
      }
    }
    if (colIdx === -1) {
      colIdx = cols.length
      cols.push([item])
    }
    return { ...item, colIdx }
  })
}

function fmtTime(d: Date): string {
  const h = d.getHours()
  const m = d.getMinutes()
  const ampm = h < 12 ? 'am' : 'pm'
  const h12 = h % 12 || 12
  return m === 0 ? `${h12} ${ampm}` : `${h12}:${String(m).padStart(2, '0')} ${ampm}`
}

export function DayView({ events, anchor }: DayViewProps) {
  const anchorDay = startOfDay(anchor)
  const today = startOfDay(new Date())
  const isToday = isSameDay(anchor, today)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    if (isToday) {
      const now = new Date()
      const nowY = (now.getHours() + now.getMinutes() / 60) * HOUR_H
      el.scrollTop = Math.max(0, nowY - 80)
    } else {
      el.scrollTop = 6 * HOUR_H
    }
  }, [anchor, isToday])

  // Tooltip with grace-period hide so links are clickable
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

  // Only events overlapping this day
  const dayEnd = new Date(anchorDay.getTime() + 86_400_000)
  const dayEvents = events.filter((ev) => ev.startAt < dayEnd && ev.endAt > anchorDay)

  // All-day banner: events whose total duration exceeds 24 hours.
  // Overnight events (e.g. 9 pm → 4 am, 7 h) stay in the hour grid.
  const MS_24H = 24 * 60 * 60 * 1000
  const allDayEvents = dayEvents.filter(
    (ev) => ev.endAt.getTime() - ev.startAt.getTime() > MS_24H
  )

  // Hour grid: events ≤ 24 h duration (including overnight cross-midnight events)
  const rawItems = dayEvents
    .filter((ev) => ev.endAt.getTime() - ev.startAt.getTime() <= MS_24H)
    .map((ev) => {
      // Clamp to this day's bounds so cross-midnight events don't overflow the grid
      const sh = isSameDay(ev.startAt, anchorDay)
        ? ev.startAt.getHours() + ev.startAt.getMinutes() / 60
        : 0
      const eh = isSameDay(ev.endAt, anchorDay)
        ? ev.endAt.getHours() + ev.endAt.getMinutes() / 60
        : 24
      return { ev, sh, eh: Math.max(eh, sh + 0.5) }
    })
    .sort((a, b) => a.sh - b.sh)

  const timedItems = assignColumns(rawItems)
  const numCols = timedItems.length > 0
    ? Math.max(...timedItems.map((x) => x.colIdx)) + 1
    : 1

  const nowDate = new Date()
  const nowFrac = isToday
    ? nowDate.getHours() + nowDate.getMinutes() / 60
    : null

  return (
    <>
      <div className="flex flex-col flex-1 min-h-0">
        {/* All-day banner for multi-day maintenance events */}
        {allDayEvents.length > 0 && (
          <div className="flex-shrink-0 border-b border-border bg-card">
            <div className="flex">
              {/* Align with hour label column */}
              <div className="flex-shrink-0 w-14 text-[10px] text-muted-foreground text-right pr-2.5 pt-1.5">
                all-day
              </div>
              <div className="flex-1 border-l border-border py-1 space-y-0.5">
                {allDayEvents.map((ev) => (
                  <div
                    key={ev.id}
                    className={cn(
                      'mx-2 h-[22px] flex items-center px-2 text-[11px] truncate border-l-[3px] cursor-pointer',
                      ev.status === 'in-progress' ? 'border-dashed' : ''
                    )}
                    style={{
                      background: evBg(ev.hue, 30),
                      borderLeftColor: evBorderColor(ev.hue, 90),
                      color: evText(ev.hue),
                    }}
                    onMouseEnter={(e) => showTooltip(ev, e)}
                    onMouseLeave={startHide}
                  >
                    {ev.title}
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Hour grid (same-day events only) */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto">
          <div className="flex" style={{ minHeight: 24 * HOUR_H }}>
            {/* Hour labels — each label is centered on its grid line */}
            <div className="flex-shrink-0 w-14 relative" style={{ minHeight: 24 * HOUR_H }}>
              {Array.from({ length: 24 }, (_, h) =>
                h === 0 ? null : (
                  <div
                    key={h}
                    className="absolute right-0 pr-2.5 text-[10px] leading-none text-muted-foreground select-none whitespace-nowrap"
                    style={{ top: h * HOUR_H - 5 }}
                  >
                    {formatHour(h)}
                  </div>
                )
              )}
            </div>

            {/* Events area */}
            <div className="flex-1 relative border-l border-border" style={{ minHeight: 24 * HOUR_H }}>
              {/* Hour grid lines */}
              {Array.from({ length: 24 }, (_, h) => (
                <div
                  key={h}
                  className="absolute left-0 right-0 border-t border-border/40"
                  style={{ top: h * HOUR_H }}
                />
              ))}
              {/* Half-hour dotted lines */}
              {Array.from({ length: 24 }, (_, h) => (
                <div
                  key={`h-${h}`}
                  className="absolute left-0 right-0 border-t border-dashed border-border/20"
                  style={{ top: h * HOUR_H + HOUR_H / 2 }}
                />
              ))}

              {/* Current time indicator */}
              {nowFrac !== null && (
                <div
                  className="absolute left-0 right-0 z-10"
                  style={{ top: nowFrac * HOUR_H }}
                >
                  <div className="absolute left-[-4px] w-2 h-2 rounded-full bg-accent" style={{ top: -4 }} />
                  <div className="h-0.5 bg-accent w-full" />
                </div>
              )}

              {/* Empty state — only when no events at all today */}
              {timedItems.length === 0 && allDayEvents.length === 0 && (
                <div className="absolute inset-0 flex items-center justify-center text-sm text-muted-foreground">
                  No maintenance scheduled.
                </div>
              )}
              {timedItems.length === 0 && allDayEvents.length > 0 && (
                <div className="absolute inset-0 flex items-center justify-center text-sm text-muted-foreground">
                  No same-day maintenance events.
                </div>
              )}

              {/* Event blocks */}
              {timedItems.map(({ ev, sh, eh, colIdx }) => {
                const topPx = sh * HOUR_H
                const heightPx = Math.max((eh - sh) * HOUR_H - 2, 28)
                const leftPct = (colIdx / numCols) * 100
                const widthPct = 100 / numCols

                return (
                  <div
                    key={ev.id}
                    className={cn(
                      'absolute border-l-[3px] overflow-hidden px-2 py-1 cursor-pointer',
                      ev.status === 'in-progress' && 'border-dashed'
                    )}
                    style={{
                      top: topPx,
                      height: heightPx,
                      left: `calc(${leftPct}% + 8px)`,
                      width: `calc(${widthPct}% - 12px)`,
                      background: evBg(ev.hue, 30),
                      borderLeftColor: evBorderColor(ev.hue, 90),
                      color: evText(ev.hue),
                    }}
                    onMouseEnter={(e) => showTooltip(ev, e)}
                    onMouseLeave={startHide}
                  >
                    <div className="text-xs font-medium leading-tight truncate">{ev.title}</div>
                    {heightPx > 42 && (
                      <div className="text-[10px] mt-0.5 opacity-70 truncate">
                        {ev.contributorName} · {fmtTime(ev.startAt)}–{fmtTime(ev.endAt)}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        </div>
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
