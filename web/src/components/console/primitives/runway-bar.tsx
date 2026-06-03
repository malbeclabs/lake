interface RunwayBarProps {
  /** 0–100 */
  pct: number
  /** Color state — 'default' uses primary, 'low' uses amber, 'crit' uses red. */
  state?: 'default' | 'low' | 'crit'
  /** Right-side text like "$1,800 · 12 ep". */
  caption?: string
  className?: string
}

export function RunwayBar({ pct, state = 'default', caption, className = '' }: RunwayBarProps) {
  const clamped = Math.max(0, Math.min(100, pct))
  const fill =
    state === 'crit' ? 'bg-red-500'
    : state === 'low' ? 'bg-amber-500'
    : 'bg-primary'
  return (
    <div className={`flex min-w-[140px] items-center gap-2 ${className}`}>
      <div className="relative h-[5px] flex-1 overflow-hidden rounded-sm bg-muted">
        <div className={`h-full rounded-sm ${fill}`} style={{ width: `${clamped}%` }} />
      </div>
      {caption ? (
        <span className="whitespace-nowrap font-mono text-[11.5px] text-muted-foreground">{caption}</span>
      ) : null}
    </div>
  )
}
