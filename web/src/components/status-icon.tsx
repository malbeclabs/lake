import { Check, X } from 'lucide-react'

interface StatusIconProps {
  ok: boolean
}

export function StatusIcon({ ok }: StatusIconProps) {
  return ok
    ? <Check className="h-4 w-4 text-green-500 mx-auto" />
    : <X className="h-4 w-4 text-red-500 mx-auto" />
}
