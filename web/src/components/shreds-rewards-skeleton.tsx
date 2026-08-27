// Loading affordances shared by the two Edge Rewards pages.
//
// There are two of them because they answer two different questions. The
// shimmer bar says "the thing you are already looking at is being replaced" and
// rides above content that stays on screen; the skeletons say "the page exists
// and its contents are on the way" and stand in for content that isn't there
// yet. Using the bar for a first load would leave a blank page under it, and
// using skeletons for a page turn would throw away a readable table to show
// grey boxes.
//
// Both are held back by useDelayedLoading at their call sites: an unfiltered
// page turn is served from the page cache and usually lands inside a frame or
// two, and a loading animation that appears and vanishes that fast reads as a
// glitch rather than as progress.
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

// SkeletonCell is one grey placeholder bar. Widths are passed per column so a
// skeleton table keeps the shape of the real one — a name column and a numeric
// column that placeholder to the same width read as a loading bar, not as a
// table about to appear.
export function SkeletonCell({ className }: { className?: string }) {
  return <span className={cn('block h-4 rounded bg-muted animate-pulse', className)} />
}

// SkeletonRows fills a tbody with placeholder rows. widths drives both the
// column count and each cell's placeholder width, so it has to line up with the
// real header the table renders above it.
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
