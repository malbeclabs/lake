// Axis and label formatting for the Win Rate vs Competitors panel.
export function formatDay(day: string): string {
  const d = new Date(`${day}T00:00:00Z`)
  if (Number.isNaN(d.getTime())) return day
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })
}

// dayToTs converts a UTC bucket date to epoch milliseconds for the time axis.
// Returns NaN for anything unparseable so the caller can drop the point rather
// than poison the axis domain. Parsing at UTC midnight is what keeps a point on
// its own day for a reader west of Greenwich.
export function dayToTs(day: string): number {
  return new Date(`${day}T00:00:00Z`).getTime()
}

// tsToDay is the inverse, for formatting a numeric axis tick back to its bucket.
export function tsToDay(ts: number): string {
  if (!Number.isFinite(ts)) return ''
  return new Date(ts).toISOString().slice(0, 10)
}

// formatLeadMs renders a lead in milliseconds, signed.
export function formatLeadMs(ms: number): string {
  return `${ms >= 0 ? '+' : ''}${ms.toFixed(2)} ms`
}
