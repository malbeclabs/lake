import type { ShredsEconomicsMonth } from '@/lib/api'

// "2026-08-26" -> 26, and 0 for anything unparseable. Hand-parsed like the rest
// of the page's date handling so the viewer's timezone can never move the day.
function dayOfMonth(iso: string): number {
  return Number(iso?.split('-')[2]) || 0
}

// Days of the open month the seat projection still has to cover.
//
// month.days is accrual coverage, not elapsed days, so days_in_month - days is
// not "the days still to run": it also counts every elapsed day that booked no
// charge, and those are two different things. Days after the as-of date have not
// happened and are projected. An elapsed day that booked nothing is only pending
// while the epoch covering it is in flight; past that it earned nothing and
// never will, so projecting it at the live rate would invent revenue that was
// never charged. The catch-up is capped at one epoch's span for that reason.
//
// A missing as-of falls back to the booked days, which zeroes the catch-up and
// leaves the projection covering the unbooked days alone.
export function projectedDays(
  month: Pick<ShredsEconomicsMonth, 'days' | 'days_in_month'>,
  asOf: string,
  epochDays: number,
): number {
  const elapsed = Math.min(dayOfMonth(asOf) || month.days, month.days_in_month)
  const ahead = Math.max(0, month.days_in_month - elapsed)
  const pending = Math.min(Math.max(0, elapsed - month.days), Math.max(0, epochDays))
  return ahead + pending
}
