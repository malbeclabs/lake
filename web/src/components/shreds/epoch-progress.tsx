import { AlertTriangle } from 'lucide-react'

function formatEta(ms: number): string {
  const totalSeconds = Math.round(ms / 1000)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

interface EpochProgressProps {
  epoch: number
  progressPct: number
  remainingMs: number
}

export function EpochProgress({ epoch, progressPct, remainingMs }: EpochProgressProps) {
  const nearEnd = progressPct >= 90
  return (
    <div className={`flex items-center gap-3 px-3 py-2 rounded-lg border text-sm ${
      nearEnd
        ? 'bg-amber-500/10 border-amber-500/20 text-amber-600 dark:text-amber-400'
        : 'bg-muted/50 border-border text-muted-foreground'
    }`}>
      <span className="font-medium text-foreground shrink-0">Epoch {epoch}</span>
      <div className="flex-1 relative h-1.5 bg-border rounded-full overflow-hidden min-w-0">
        <div
          className={`absolute inset-y-0 left-0 rounded-full ${nearEnd ? 'bg-amber-500' : 'bg-primary'}`}
          style={{ width: `${Math.min(progressPct, 100).toFixed(2)}%` }}
        />
      </div>
      <span className="tabular-nums text-xs shrink-0">{progressPct.toFixed(0)}%</span>
      <span className="text-xs shrink-0">ETA {formatEta(remainingMs)}</span>
      <span className="text-xs text-muted-foreground shrink-0">→ {epoch + 1}</span>
    </div>
  )
}

// Stub — we don't have slot-level data from the API yet, so we show a generic note.
export function EpochWarning({ currentEpoch }: { currentEpoch: number }) {
  // In the CLI, this warns when <10% of the epoch remains.
  // We'd need slot-level data (slot_index, slots_in_epoch) to replicate exactly.
  void currentEpoch
  return (
    <div className="flex items-start gap-2 text-sm px-3 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-600 dark:text-amber-400">
      <AlertTriangle className="h-4 w-4 flex-shrink-0 mt-0.5" />
      <span>
        New subscriptions are activated for the current epoch. If the epoch is almost over,
        your first funded epoch may be shorter than a full epoch.
      </span>
    </div>
  )
}
