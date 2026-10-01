import type { ReactNode } from 'react'

// Temporary. Delete this file and its uses when device telemetry collection resumes.
export function DeviceTelemetryPausedNotice({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-100">
      Device telemetry collection has been paused for maintenance since 1 Oct 2026, 17:27 UTC. {children}
    </div>
  )
}
