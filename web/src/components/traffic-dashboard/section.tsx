import { useState } from 'react'
import { ChevronDown, Loader2 } from 'lucide-react'
import { cn } from '@/lib/utils'

export function Section({
  title,
  description,
  defaultOpen = true,
  loading = false,
  progress,
  children,
}: {
  title: string
  description?: string
  defaultOpen?: boolean
  loading?: boolean
  progress?: number
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <div className="border border-border rounded-lg bg-card overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-start gap-3 px-4 py-3 text-left hover:bg-muted/30 transition-colors"
      >
        <ChevronDown
          className={cn(
            'h-4 w-4 mt-0.5 shrink-0 text-muted-foreground transition-transform',
            !open && '-rotate-90'
          )}
        />
        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-semibold">{title}</h2>
              {loading && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
            </div>
            {progress !== undefined && (
              <span className="text-sm tabular-nums text-muted-foreground">{progress.toFixed(1)}%</span>
            )}
          </div>
          {description && (
            <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
          )}
          {progress !== undefined && (
            <div className="h-1.5 rounded-full bg-muted-foreground/25 overflow-hidden mt-2">
              <div className="h-full rounded-full bg-emerald-500 transition-all duration-500" style={{ width: `${Math.min(100, progress)}%` }} />
            </div>
          )}
        </div>
      </button>
      <div className="h-0.5 w-full overflow-hidden">
        {loading && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>
      {open && (
        <div className="px-4 pb-4">
          {children}
        </div>
      )}
    </div>
  )
}
