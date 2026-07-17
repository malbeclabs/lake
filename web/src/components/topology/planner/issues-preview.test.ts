import { describe, it, expect } from 'vitest'
import { summarizeIssuePreview } from './issues-preview'
import type { IssuePreviewItem } from '@/lib/api'

const mk = (action: 'create' | 'update', overrides: Partial<IssuePreviewItem> = {}): IssuePreviewItem => ({
  kind: 'contributor',
  contributor_pk: 'pk',
  contributor_code: 'code',
  is_parent: false,
  action,
  title: 't',
  body: 'b',
  repo: 'malbeclabs/infra',
  ...overrides,
})

describe('summarizeIssuePreview', () => {
  it('counts creates and updates', () => {
    const s = summarizeIssuePreview([mk('create'), mk('update'), mk('create')])
    expect(s).toEqual({ creates: 2, updates: 1, total: 3 })
  })

  it('handles empty input', () => {
    expect(summarizeIssuePreview([])).toEqual({ creates: 0, updates: 0, total: 0 })
  })
})
