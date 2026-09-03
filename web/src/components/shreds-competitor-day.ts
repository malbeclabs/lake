// Axis and label formatting for the Win Rate vs Competitors panel.
export function formatDay(day: string): string {
  const d = new Date(`${day}T00:00:00Z`)
  if (Number.isNaN(d.getTime())) return day
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })
}

// formatLeadMs renders a lead in milliseconds, signed.
export function formatLeadMs(ms: number): string {
  return `${ms >= 0 ? '+' : ''}${ms.toFixed(2)} ms`
}
