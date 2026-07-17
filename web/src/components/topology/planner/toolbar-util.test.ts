import { describe, it, expect } from 'vitest'
import { statusBadgeClass, canCreateIssues } from './toolbar-util'

describe('canCreateIssues', () => {
  it('is true only once approved', () => {
    expect(canCreateIssues('approved')).toBe(true)
    expect(canCreateIssues('draft')).toBe(false)
    expect(canCreateIssues('done')).toBe(false)
  })
})

describe('statusBadgeClass', () => {
  it('returns a class string for every status', () => {
    for (const s of ['draft', 'approved', 'done', 'archived'] as const) {
      expect(statusBadgeClass(s)).toContain('rounded')
    }
  })
})
