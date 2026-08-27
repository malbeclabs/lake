import { describe, expect, it } from 'vitest'
import { projectedDays } from './shreds-economics'

// The seat side of Run-rate MRR is the open month's accrual plus the days it
// has left at the live-seat rate. month.days is accrual coverage, not elapsed
// days, so the count of days left cannot be read off it alone.
describe('projectedDays', () => {
  const august = { days_in_month: 31 }
  const epochDays = 2.01

  it('counts the days the month has left', () => {
    // Coverage runs right up to the as-of day, so nothing is pending and only
    // the four days after 27 August are projected.
    expect(projectedDays({ ...august, days: 27 }, '2026-08-27', epochDays)).toBe(4)
  })

  it('adds the epoch in flight, whose charge has not landed', () => {
    // 27 elapsed, 26 booked. The 27th is covered by an epoch still running, so
    // it is genuinely unbilled rather than unearned, and is projected.
    expect(projectedDays({ ...august, days: 26 }, '2026-08-27', epochDays)).toBe(5)
  })

  it('does not project an elapsed gap that earned nothing', () => {
    // Seats live 1-3 August, nothing until they came back on the 25th. The 21
    // days in between were never charged and never will be. Projecting them at
    // the live rate would invent revenue; only one epoch of catch-up is real.
    const gapped = projectedDays({ ...august, days: 5 }, '2026-08-26', epochDays)
    expect(gapped).toBeCloseTo(5 + 2.01, 5)
    // The formula this replaced billed every unbooked day of the month.
    expect(gapped).toBeLessThan(31 - 5)
  })

  it('caps the catch-up at one epoch, not at the gap', () => {
    // A 19-day gap and a 10-day gap both clear the cap, so both contribute the
    // same one epoch of catch-up. How wide the gap is cannot move the figure.
    const wide = projectedDays({ ...august, days: 1 }, '2026-08-20', epochDays)
    const narrow = projectedDays({ ...august, days: 10 }, '2026-08-20', epochDays)
    expect(wide).toBeCloseTo(narrow, 5)
    expect(wide).toBeCloseTo(11 + epochDays, 5)
  })

  it('projects nothing once the month is fully booked', () => {
    expect(projectedDays({ ...august, days: 31 }, '2026-08-31', epochDays)).toBe(0)
  })

  it('never returns a negative count when coverage runs past the as-of day', () => {
    // An epoch spread can book a day the as-of cut has not reached.
    expect(projectedDays({ ...august, days: 28 }, '2026-08-27', epochDays)).toBe(4)
  })

  it('falls back to the booked days when the as-of is missing', () => {
    // No as-of means no way to tell elapsed from unbooked, so the catch-up term
    // drops out and the projection covers the unbooked days alone.
    expect(projectedDays({ ...august, days: 26 }, '', epochDays)).toBe(5)
  })

  it('survives an epoch length of zero', () => {
    expect(projectedDays({ ...august, days: 26 }, '2026-08-27', 0)).toBe(4)
  })
})
