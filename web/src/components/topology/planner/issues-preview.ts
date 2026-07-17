import type { IssuePreviewItem } from '@/lib/api'

export interface IssuePreviewSummary {
  creates: number
  updates: number
  total: number
}

/** Counts how many previewed issues would be created vs updated. */
export function summarizeIssuePreview(previews: IssuePreviewItem[]): IssuePreviewSummary {
  let creates = 0
  let updates = 0
  for (const p of previews) {
    if (p.action === 'update') {
      updates++
    } else {
      creates++
    }
  }
  return { creates, updates, total: previews.length }
}
