import { cn } from '@/lib/utils'

interface StatusDotProps {
  ok: boolean
  label?: string
}

export function StatusDot({ ok, label }: StatusDotProps) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className={cn(
          'inline-block h-2.5 w-2.5 rounded-full',
          ok ? 'bg-green-500' : 'bg-red-500'
        )}
      />
      {label && <span className="text-sm">{label}</span>}
    </span>
  )
}
