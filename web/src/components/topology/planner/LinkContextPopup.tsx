import { Trash2, Pencil } from 'lucide-react'
import type { DraftLink } from './draft'

export function LinkContextPopup({
  link,
  onDelete,
  onEdit,
}: {
  link: DraftLink
  onDelete: () => void
  onEdit: () => void
}) {
  const removed = link.changeState === 'removed'
  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-2 w-52 space-y-1">
      <div className="px-1 pb-1 text-xs font-medium truncate">{link.code}</div>
      <div className="px-1 pb-1 text-[11px] text-muted-foreground">
        {(link.latency_us / 1000).toFixed(2)} ms · {(link.bandwidth_bps / 1e9).toFixed(0)} Gbps
      </div>
      <button
        onClick={onEdit}
        className="w-full flex items-center gap-2 px-2 py-1.5 text-xs rounded hover:bg-muted"
      >
        <Pencil className="h-3.5 w-3.5" />
        Edit latency / bandwidth
      </button>
      <button
        onClick={onDelete}
        disabled={removed}
        className="w-full flex items-center gap-2 px-2 py-1.5 text-xs rounded hover:bg-muted text-red-600 dark:text-red-400 disabled:opacity-50"
      >
        <Trash2 className="h-3.5 w-3.5" />
        {removed ? 'Already removed' : 'Delete link'}
      </button>
      <div className="px-2 pt-1 text-[11px] text-muted-foreground">
        Drag an endpoint handle to move it.
      </div>
    </div>
  )
}
