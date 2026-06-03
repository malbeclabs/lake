import { Coins, Trash2, Download, Tag } from 'lucide-react'
import type { ShredClientSeat } from '@/lib/api'

interface ConsoleBulkBarProps {
  selected: ShredClientSeat[]
  totalRows: number
  onClear: () => void
  onDeposit: () => void
  onExport: () => void
  onWithdraw: () => void
}

export function ConsoleBulkBar({
  selected, totalRows, onClear, onDeposit, onExport, onWithdraw,
}: ConsoleBulkBarProps) {
  const combinedRunRate = selected.reduce((sum, s) => sum + s.price_per_epoch_dollars, 0)

  return (
    <div className="sticky top-2 z-10 mb-3 flex flex-wrap items-center gap-3 rounded-xl border border-primary bg-card p-3 shadow-[0_0_0_4px_var(--ring-primary-bg,rgba(56,189,248,0.10))]">
      <span className="text-[13px] font-semibold">
        <span className="text-primary tabular-nums">{selected.length}</span> of {totalRows} selected
        <span className="ml-3 font-normal text-muted-foreground">
          · ${combinedRunRate.toLocaleString()}/epoch combined run rate
        </span>
      </span>
      <span className="h-4 w-px bg-border" />
      <button
        type="button"
        onClick={onDeposit}
        disabled={selected.length === 0}
        className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3 py-1 text-[12.5px] hover:bg-card disabled:opacity-50"
      >
        <Coins className="h-3.5 w-3.5" /> Deposit USDC…
      </button>
      <button
        type="button"
        disabled
        title="Tags are coming soon"
        className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3 py-1 text-[12.5px] opacity-50"
      >
        <Tag className="h-3.5 w-3.5" /> Tag
      </button>
      <button
        type="button"
        onClick={onExport}
        className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3 py-1 text-[12.5px] hover:bg-card"
      >
        <Download className="h-3.5 w-3.5" /> Export {selected.length}
      </button>
      <button
        type="button"
        onClick={onWithdraw}
        disabled={selected.length === 0}
        className="inline-flex items-center gap-1.5 rounded-md border border-red-500/40 bg-red-500/5 px-3 py-1 text-[12.5px] text-red-400 hover:bg-red-500/10 disabled:opacity-50"
      >
        <Trash2 className="h-3.5 w-3.5" /> Withdraw escrow &amp; cancel
      </button>
      <div className="ml-auto">
        <button
          type="button"
          onClick={onClear}
          className="rounded-md px-3 py-1 text-[12.5px] text-muted-foreground hover:bg-background hover:text-foreground"
        >
          Clear selection
        </button>
      </div>
    </div>
  )
}
