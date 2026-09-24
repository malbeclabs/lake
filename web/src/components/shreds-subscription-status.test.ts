import { describe, expect, it } from 'vitest'
import { getSubscriptionStatus } from './shreds-subscription-status'

// The badge on a subscription row and the status chips above the table must
// agree about every row, and they are computed in different places: the chips
// filter in SQL (shredSubscriptionStatusClauses), the badge here. These cases
// are the SQL's three clauses, restated.
describe('getSubscriptionStatus', () => {
  const now = Date.parse('2026-09-03T12:00:00Z')
  const iso = (s: string) => new Date(s).toISOString()
  const seat = (windowEnd: string, currentUsers = 1) => ({
    window_end: windowEnd === '' ? '' : iso(windowEnd),
    current_users: currentUsers,
  })

  it('is active inside the billed window with someone connected', () => {
    expect(getSubscriptionStatus(seat('2026-10-03T00:00:00Z'), now)).toBe('active')
  })

  it('is pending inside the window when nobody has connected', () => {
    expect(getSubscriptionStatus(seat('2026-10-03T00:00:00Z', 0), now)).toBe('pending')
  })

  it('is expired once the term date has passed', () => {
    expect(getSubscriptionStatus(seat('2026-09-01T00:00:00Z'), now)).toBe('expired')
  })

  it('does not let an unconnected seat mask an ended term', () => {
    expect(getSubscriptionStatus(seat('2026-09-01T00:00:00Z', 0), now)).toBe('expired')
  })

  it('flips exactly on the term date', () => {
    const term = '2026-09-03T12:00:00Z'
    expect(getSubscriptionStatus(seat(term), now - 1)).toBe('active')
    expect(getSubscriptionStatus(seat(term), now)).toBe('expired')
  })

  it('treats a missing timestamp as long past, the way the query does', () => {
    expect(getSubscriptionStatus(seat(''), now)).toBe('expired')
  })
})
