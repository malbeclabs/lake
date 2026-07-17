// Pure counting/formatting for the plan landing cards' "what this plan will do"
// summary, e.g. "3 devices added, 1 device removed, 2 links added".
import type { PlanChange } from '@/lib/api'

export interface PlanChangeCounts {
  devicesAdded: number
  devicesRemoved: number
  linksAdded: number
  linksRemoved: number
  linksMoved: number
}

// Counts only PENDING changes -- the same convention buildDraft uses (SC-8):
// 'done' is already reflected in the live baseline, 'skipped'/'superseded' never
// take effect, so neither belongs in "what this plan will do".
export function countPendingChanges(changes: PlanChange[]): PlanChangeCounts {
  const counts: PlanChangeCounts = {
    devicesAdded: 0,
    devicesRemoved: 0,
    linksAdded: 0,
    linksRemoved: 0,
    linksMoved: 0,
  }
  for (const c of changes) {
    if (c.state !== 'pending') continue
    switch (c.op_type) {
      case 'add_device':
        counts.devicesAdded++
        break
      case 'remove_device':
        counts.devicesRemoved++
        break
      case 'add_link':
        counts.linksAdded++
        break
      case 'remove_link':
        counts.linksRemoved++
        break
      case 'move_link_end':
        counts.linksMoved++
        break
    }
  }
  return counts
}

export function totalPendingChanges(counts: PlanChangeCounts): number {
  return (
    counts.devicesAdded +
    counts.devicesRemoved +
    counts.linksAdded +
    counts.linksRemoved +
    counts.linksMoved
  )
}

function pluralize(n: number, noun: string): string {
  return `${n} ${noun}${n === 1 ? '' : 's'}`
}

// Human summary like "3 devices added, 1 device removed, 2 links added, 4 links
// removed, 1 link moved". Empty string when there are no pending changes.
export function formatChangeSummary(counts: PlanChangeCounts): string {
  const parts: string[] = []
  if (counts.devicesAdded > 0) parts.push(`${pluralize(counts.devicesAdded, 'device')} added`)
  if (counts.devicesRemoved > 0) parts.push(`${pluralize(counts.devicesRemoved, 'device')} removed`)
  if (counts.linksAdded > 0) parts.push(`${pluralize(counts.linksAdded, 'link')} added`)
  if (counts.linksRemoved > 0) parts.push(`${pluralize(counts.linksRemoved, 'link')} removed`)
  if (counts.linksMoved > 0) parts.push(`${pluralize(counts.linksMoved, 'link')} moved`)
  return parts.join(', ')
}
