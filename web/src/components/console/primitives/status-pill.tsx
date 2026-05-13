import type { ReactNode } from 'react'

export type PillTone = 'green' | 'amber' | 'red' | 'blue' | 'gray' | 'purple'

const TONE_STYLES: Record<PillTone, { wrap: string; dot: string }> = {
  green: { wrap: 'bg-green-500/10 text-green-400', dot: 'bg-green-500 shadow-[0_0_6px_var(--color-green-500)]' },
  amber: { wrap: 'bg-amber-500/10 text-amber-300', dot: 'bg-amber-500' },
  red:   { wrap: 'bg-red-500/10 text-red-300', dot: 'bg-red-500' },
  blue:  { wrap: 'bg-primary/10 text-primary', dot: 'bg-primary' },
  gray:  { wrap: 'bg-muted text-muted-foreground border border-border', dot: 'bg-muted-foreground/60' },
  purple:{ wrap: 'bg-purple-500/10 text-purple-300', dot: 'bg-purple-500' },
}

interface StatusPillProps {
  tone: PillTone
  children: ReactNode
  className?: string
}

export function StatusPill({ tone, children, className = '' }: StatusPillProps) {
  const s = TONE_STYLES[tone]
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11.5px] font-medium ${s.wrap} ${className}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${s.dot}`} />
      {children}
    </span>
  )
}
