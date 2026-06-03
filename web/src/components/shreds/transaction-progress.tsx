import { Loader2, Check, ChevronRight, ExternalLink } from 'lucide-react'
import type { TransactionStatus } from '@/hooks/use-shred-transaction'

export function StatusStep({ label, done, active }: { label: string; done: boolean; active: boolean }) {
  return (
    <div className="flex items-center gap-2">
      {done ? (
        <div className="h-5 w-5 rounded-full bg-green-500 flex items-center justify-center">
          <Check className="h-3 w-3 text-white" />
        </div>
      ) : active ? (
        <Loader2 className="h-5 w-5 text-primary animate-spin" />
      ) : (
        <div className="h-5 w-5 rounded-full border-2 border-border" />
      )}
      <span className={`text-sm ${done ? 'text-foreground' : active ? 'text-foreground' : 'text-muted-foreground'}`}>
        {label}
      </span>
    </div>
  )
}

export function TransactionProgress({ status, txSignature }: { status: TransactionStatus; txSignature: string | null }) {
  const steps: { key: TransactionStatus[]; label: string }[] = [
    { key: ['signing'], label: 'Signing transaction' },
    { key: ['sending'], label: 'Sending to network' },
    { key: ['confirming'], label: 'Confirming on-chain' },
  ]

  return (
    <div className="flex items-center gap-4">
      {steps.map((step, i) => {
        const done = steps.slice(i + 1).some(s => s.key.some(k => status === k)) || status === 'confirmed'
        const active = step.key.includes(status)
        return (
          <div key={step.label} className="flex items-center gap-2">
            {i > 0 && <ChevronRight className="h-3 w-3 text-muted-foreground" />}
            <StatusStep label={step.label} done={done} active={active} />
          </div>
        )
      })}
      {status === 'confirmed' && txSignature && (
        <>
          <ChevronRight className="h-3 w-3 text-muted-foreground" />
          <a
            href={`https://solscan.io/tx/${txSignature}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-sm text-blue-500 hover:underline"
          >
            View on Solscan <ExternalLink className="h-3 w-3" />
          </a>
        </>
      )}
    </div>
  )
}
