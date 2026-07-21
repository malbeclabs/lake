import { describe, it, expect } from 'vitest'
import { summarizeIssuePreview } from './issues-preview'
import type { IssuePreviewItem } from '@/lib/api'

const mk = (overrides: Partial<IssuePreviewItem> = {}): IssuePreviewItem => ({
  kind: 'contributor',
  contributor_pk: 'pk',
  contributor_code: 'code',
  is_parent: false,
  action: 'create',
  title: 't',
  body: 'b',
  labels: [],
  repo: 'malbeclabs/infra',
  ...overrides,
})

describe('summarizeIssuePreview', () => {
  it('counts the previewed issues', () => {
    const s = summarizeIssuePreview([mk(), mk(), mk()])
    expect(s).toEqual({ total: 3 })
  })

  it('handles empty input', () => {
    expect(summarizeIssuePreview([])).toEqual({ total: 0 })
  })
})
