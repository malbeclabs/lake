import { driftLabel } from './drift'
import type { DriftStatus } from '@/lib/api'

export function DriftBadge({ drift }: { drift: DriftStatus }) {
  if (drift === 'pending') return null
  const cls =
    drift === 'broken'
      ? 'bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-400'
      : 'bg-muted text-muted-foreground'
  return (
    <span className={`px-1.5 py-0.5 text-[10px] font-medium rounded ${cls}`}>
      {driftLabel(drift)}
    </span>
  )
}
