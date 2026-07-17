import { AlertTriangle } from 'lucide-react'
import { usePlanner } from './PlannerContext'

export function ConflictBanner() {
  const { conflict, reload, dismissConflict } = usePlanner()
  if (!conflict) return null
  return (
    <div className="flex items-center gap-2 px-4 py-2 bg-red-100 dark:bg-red-900/30 border-b border-red-200 dark:border-red-800 text-sm text-red-700 dark:text-red-400">
      <AlertTriangle className="h-4 w-4 shrink-0" />
      <span className="flex-1">Someone else changed this plan. Reload to get the latest.</span>
      <button
        onClick={reload}
        className="px-2 py-1 text-xs rounded bg-red-600 text-white hover:bg-red-700"
      >
        Reload
      </button>
      <button
        onClick={dismissConflict}
        className="px-2 py-1 text-xs rounded bg-muted hover:bg-muted/80 text-foreground"
      >
        Dismiss
      </button>
    </div>
  )
}
