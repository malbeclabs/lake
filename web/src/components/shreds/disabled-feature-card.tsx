import type { ReactNode } from 'react'
import { AlertCircle } from 'lucide-react'

export type DisabledReason = 'Backend not yet supported' | 'Coming with Login flow'

interface DisabledFeatureCardProps {
  reason: DisabledReason
  children: ReactNode
  className?: string
}

export function DisabledFeatureCard({ reason, children, className = '' }: DisabledFeatureCardProps) {
  return (
    <div
      className={`relative border border-dashed border-border rounded-lg p-3 opacity-60 select-none ${className}`}
      aria-disabled="true"
    >
      <div className="pointer-events-none">{children}</div>
      <div className="mt-2 flex items-center gap-1.5 text-xs italic text-muted-foreground">
        <AlertCircle className="h-3 w-3 shrink-0" />
        <span>{reason}.</span>
      </div>
    </div>
  )
}
