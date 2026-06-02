export type CalendarView = 'day' | 'week' | '2week' | 'month'

export function startOfDay(d: Date): Date {
  const r = new Date(d)
  r.setHours(0, 0, 0, 0)
  return r
}

export function isSameDay(a: Date, b: Date): boolean {
  return (
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()
  )
}

// Calendar days from a to b (positive when b is later)
export function daysBetween(a: Date, b: Date): number {
  return Math.round(
    (startOfDay(b).getTime() - startOfDay(a).getTime()) / 86_400_000
  )
}

export function addDays(d: Date, n: number): Date {
  const r = new Date(d)
  r.setDate(r.getDate() + n)
  return r
}

// Monday of the ISO week containing d
export function getMondayOf(d: Date): Date {
  const r = startOfDay(d)
  const dow = r.getDay()
  r.setDate(r.getDate() - (dow === 0 ? 6 : dow - 1))
  return r
}

export function isWeekend(d: Date): boolean {
  const dow = d.getDay()
  return dow === 0 || dow === 6
}

const DAY_ABBR = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const MONTH_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const MONTH_FULL = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

export function getDays(view: CalendarView, anchor: Date): Date[] {
  if (view === 'day') return [startOfDay(anchor)]
  if (view === 'week') {
    const start = getMondayOf(anchor)
    return Array.from({ length: 7 }, (_, i) => addDays(start, i))
  }
  if (view === '2week') {
    const start = getMondayOf(anchor)
    return Array.from({ length: 14 }, (_, i) => addDays(start, i))
  }
  // month — exactly the days in the current month, no week-padding
  const y = anchor.getFullYear()
  const m = anchor.getMonth()
  const daysInMonth = new Date(y, m + 1, 0).getDate()
  return Array.from({ length: daysInMonth }, (_, i) => new Date(y, m, i + 1))
}

export function formatDateRange(view: CalendarView, anchor: Date, days: Date[]): string {
  if (view === 'day') {
    const d = anchor
    return `${DAY_ABBR[d.getDay()]}, ${MONTH_FULL[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`
  }
  if (view === 'month') {
    return `${MONTH_FULL[anchor.getMonth()]} ${anchor.getFullYear()}`
  }
  const s = days[0], e = days[days.length - 1]
  if (s.getMonth() === e.getMonth()) {
    return `${MONTH_FULL[s.getMonth()]} ${s.getDate()}–${e.getDate()}, ${s.getFullYear()}`
  }
  return `${MONTH_SHORT[s.getMonth()]} ${s.getDate()} – ${MONTH_SHORT[e.getMonth()]} ${e.getDate()}, ${s.getFullYear()}`
}

// "1:00 am", "12:00 pm", etc.
export function formatHour(h: number): string {
  if (h === 0 || h === 24) return '12:00 am'
  if (h === 12) return '12:00 pm'
  const ampm = h < 12 ? 'am' : 'pm'
  return `${h < 12 ? h : h - 12}:00 ${ampm}`
}

// "Mon, Jun 15 · 1 am" / "Tue, Jun 16 · 1:30 pm"
export function fmtDT(d: Date): string {
  const h = d.getHours(), m = d.getMinutes()
  const ampm = h < 12 ? 'am' : 'pm'
  const h12 = h % 12 || 12
  const time = m === 0 ? `${h12} ${ampm}` : `${h12}:${String(m).padStart(2, '0')} ${ampm}`
  return `${DAY_ABBR[d.getDay()]}, ${MONTH_SHORT[d.getMonth()]} ${d.getDate()} · ${time}`
}
