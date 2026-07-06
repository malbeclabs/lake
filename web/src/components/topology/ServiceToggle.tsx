import type { PathService } from '@/lib/api'
import { cn } from '@/lib/utils'

const DESCRIPTIONS: Record<PathService, string> = {
  unicast: 'Flex-algo 128 — topology-tagged links only',
  multicast: 'Algo 0 — all links',
}

interface ServiceToggleProps {
  value: PathService
  onChange: (service: PathService) => void
  /** 'sm' for the compact panel toggle, 'md' for the standalone page toggle */
  size?: 'sm' | 'md'
  label?: string
  showDescription?: boolean
  className?: string
}

// Unicast/Multicast segmented toggle shared by the path calculator page and the
// device-path / metro-path panels. Selecting a service changes which IS-IS
// topology paths are resolved through (see PathService).
export function ServiceToggle({
  value,
  onChange,
  size = 'sm',
  label = 'Traffic',
  showDescription = false,
  className,
}: ServiceToggleProps) {
  const sm = size === 'sm'
  return (
    <div className={cn('flex items-center', sm ? 'gap-2' : 'gap-3', className)}>
      <span className={cn('text-muted-foreground', sm ? 'text-[10px]' : 'text-sm')}>{label}</span>
      <div
        className={cn(
          'inline-flex border border-border bg-muted/40',
          sm ? 'rounded p-px' : 'rounded-md p-0.5',
        )}
      >
        {(['unicast', 'multicast'] as const).map((s) => (
          <button
            key={s}
            onClick={() => onChange(s)}
            className={cn(
              'rounded-sm transition-colors',
              sm ? 'px-2 py-0.5 text-[10px]' : 'px-3 py-1 text-sm',
              value === s
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {s === 'unicast' ? 'Unicast' : 'Multicast'}
          </button>
        ))}
      </div>
      {showDescription && (
        <span className="text-xs text-muted-foreground">{DESCRIPTIONS[value]}</span>
      )}
    </div>
  )
}
