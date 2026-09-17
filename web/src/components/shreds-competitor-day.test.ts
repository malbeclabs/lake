import { describe, expect, it } from 'vitest'

import { dayToTs, formatDay, formatLeadMs, tsToDay } from './shreds-competitor-day'

describe('formatDay', () => {
  // The whole reason this helper exists. A UTC bucket must render as itself, not
  // as the previous day, for a reader west of Greenwich.
  it('renders the UTC day regardless of the runtime timezone', () => {
    expect(formatDay('2026-09-03')).toBe('Sep 3')
    expect(formatDay('2026-01-01')).toBe('Jan 1')
    expect(formatDay('2026-12-31')).toBe('Dec 31')
  })

  it('handles a leap day', () => {
    expect(formatDay('2028-02-29')).toBe('Feb 29')
  })

  // The series comes from ClickHouse's toString(Date), so a malformed value means
  // something upstream changed shape. Showing it raw beats rendering the string
  // "Invalid Date" across the axis.
  it('falls back to the raw value when unparseable', () => {
    expect(formatDay('not-a-date')).toBe('not-a-date')
    expect(formatDay('')).toBe('')
  })
})

describe('formatLeadMs', () => {
  it('always shows the sign, so the direction is never inferred', () => {
    expect(formatLeadMs(0.26)).toBe('+0.26 ms')
    expect(formatLeadMs(0)).toBe('+0.00 ms')
    expect(formatLeadMs(-0.31)).toBe('-0.31 ms')
  })

  it('fixes two decimals', () => {
    expect(formatLeadMs(5.6118)).toBe('+5.61 ms')
    expect(formatLeadMs(12)).toBe('+12.00 ms')
  })
})

describe('dayToTs', () => {
  // The axis is numeric so that a day the rollup skipped leaves a real gap
  // instead of being spaced like any other neighbour. That only works if the
  // conversion is anchored at UTC midnight.
  it('anchors a bucket at UTC midnight', () => {
    expect(dayToTs('2026-09-03')).toBe(Date.UTC(2026, 8, 3))
    expect(dayToTs('2026-01-01')).toBe(Date.UTC(2026, 0, 1))
  })

  it('spaces consecutive days one day apart and a skipped day two', () => {
    const dayMs = 86_400_000
    expect(dayToTs('2026-09-03') - dayToTs('2026-09-02')).toBe(dayMs)
    expect(dayToTs('2026-09-04') - dayToTs('2026-09-02')).toBe(2 * dayMs)
  })

  it('returns NaN for an unparseable value so the caller can drop the point', () => {
    expect(Number.isNaN(dayToTs('not-a-date'))).toBe(true)
  })
})

describe('tsToDay', () => {
  it('round-trips a bucket', () => {
    expect(tsToDay(dayToTs('2026-09-03'))).toBe('2026-09-03')
    expect(tsToDay(dayToTs('2028-02-29'))).toBe('2028-02-29')
  })

  it('is empty for a non-finite timestamp', () => {
    expect(tsToDay(NaN)).toBe('')
  })
})
