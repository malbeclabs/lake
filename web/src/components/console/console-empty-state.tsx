import { Layers, Plus, ArrowRight } from 'lucide-react'
import { Link } from 'react-router-dom'

interface ConsoleEmptyStateProps {
  isInternal?: boolean
  onEnterPreview?: () => void
}

export function ConsoleEmptyState({ isInternal = false, onEnterPreview }: ConsoleEmptyStateProps = {}) {
  return (
    <div className="rounded-xl border border-border bg-card px-8 py-16 text-center">
      <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-xl border border-border bg-background text-primary">
        <Layers className="h-5 w-5" />
      </div>
      <h3 className="mb-1.5 text-[16px] font-semibold">No subscriptions yet</h3>
      <p className="mx-auto mb-5 max-w-md text-[13px] text-muted-foreground">
        Subscribe a server to a doublezero edge device to start receiving raw Solana shreds. Fund the
        subscription&rsquo;s USDC escrow once &mdash; it drains per epoch until you withdraw the remainder.
      </p>
      <div className="flex flex-wrap items-center justify-center gap-2">
        <Link
          to="/dz/shreds/pay"
          className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3.5 py-1.5 text-[13px] font-medium text-primary-foreground hover:bg-primary/90"
        >
          <Plus className="h-3.5 w-3.5" /> New subscription
        </Link>
        <button
          type="button"
          disabled
          title="CLI token import coming soon"
          className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3.5 py-1.5 text-[13px] opacity-50"
        >
          Import existing (CLI token)
        </button>
        <a
          href="https://docs.doublezero.xyz"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-3.5 py-1.5 text-[13px] hover:bg-card"
        >
          Read the docs
        </a>
      </div>
      {isInternal && onEnterPreview && (
        <div className="mt-5">
          <button
            type="button"
            onClick={onEnterPreview}
            className="inline-flex items-center gap-1.5 text-[12.5px] text-muted-foreground transition-colors hover:text-foreground"
          >
            See sample workflow <ArrowRight className="h-3 w-3" />
          </button>
        </div>
      )}
    </div>
  )
}
