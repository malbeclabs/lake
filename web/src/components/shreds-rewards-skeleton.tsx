// Loading affordances shared by the two Edge Rewards pages: a shimmer bar over
// content that stays on screen while it is replaced (a page turn), skeletons in
// place of content that isn't there yet (a first load). Both are held back by
// useDelayedLoading at their call sites so a fast response doesn't flash them.
import { cn } from '@/lib/utils'

// RewardsShimmerBar is an indeterminate progress bar, matching the one the
// scoreboard and latency pages already use. The track is always rendered so
// that showing the bar cannot shift the layout under it.
export function RewardsShimmerBar({ show }: { show: boolean }) {
  return (
    <div
      className="h-0.5 w-full overflow-hidden"
      role="progressbar"
      aria-hidden={!show}
      aria-label={show ? 'Loading' : undefined}
    >
      {show && (
        <div className="h-full w-1/3 rounded-full bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite]" />
      )}
    </div>
  )
}

export function SkeletonCell({ className }: { className?: string }) {
  return <span className={cn('block h-4 rounded bg-muted animate-pulse', className)} />
}

// SkeletonRows fills a tbody with placeholder rows. `widths` sets the column
// count and each cell's width, so it must match the header above it.
export function SkeletonRows({
  rows,
  widths,
  align,
}: {
  rows: number
  widths: string[]
  align?: ('left' | 'right')[]
}) {
  return (
    <>
      {Array.from({ length: rows }, (_, r) => (
        <tr key={r} className="border-b border-border last:border-b-0">
          {widths.map((w, c) => (
            <td key={c} className="px-4 py-3">
              <SkeletonCell
                className={cn(w, align?.[c] === 'right' && 'ml-auto')}
              />
            </td>
          ))}
        </tr>
      ))}
    </>
  )
}
