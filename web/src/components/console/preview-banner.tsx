import { AlertTriangle } from 'lucide-react'

interface PreviewBannerProps {
  onExit: () => void
}

export function PreviewBanner({ onExit }: PreviewBannerProps) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-amber-700 dark:text-amber-300">
      <div className="flex items-center gap-2 text-sm">
        <AlertTriangle className="h-4 w-4 shrink-0" />
        <span>
          <strong className="font-semibold">Preview mode</strong> — sample
          subscriptions. No wallet calls, no funds moved.
        </span>
      </div>
      <button
        onClick={onExit}
        className="text-xs underline underline-offset-2 hover:opacity-80"
      >
        Exit preview
      </button>
    </div>
  )
}
