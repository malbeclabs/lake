import type { IssuePreviewItem } from '@/lib/api'

export interface IssuePreviewSummary {
  total: number
}

/** Counts how many issues a manual-creation dialog would list. */
export function summarizeIssuePreview(previews: IssuePreviewItem[]): IssuePreviewSummary {
  return { total: previews.length }
}
