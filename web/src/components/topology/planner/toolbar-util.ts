import type { PlanStatus } from '@/lib/api'

export function canCreateIssues(status: PlanStatus): boolean {
  return status === 'approved'
}

export function statusBadgeClass(status: PlanStatus): string {
  const base = 'px-2 py-0.5 text-xs font-medium rounded'
  switch (status) {
    case 'approved':
      return `${base} bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400`
    case 'done':
      return `${base} bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-400`
    case 'archived':
      return `${base} bg-muted text-muted-foreground`
    default:
      return `${base} bg-yellow-100 dark:bg-yellow-900/40 text-yellow-700 dark:text-yellow-400`
  }
}
