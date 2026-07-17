import { useState } from 'react'
import { X, Check, AlertCircle, Loader2, Send } from 'lucide-react'
import type { ShredClientSeat } from '../lib/api'
import { useCreateSeatAlert, useSendTestAlert } from '../hooks/use-seat-alerts'

interface SeatAlertModalProps {
  seat: ShredClientSeat
  onClose: () => void
}

type Trigger = 'epochs_left' | 'balance_below_usdc'

export function SeatAlertModal({ seat, onClose }: SeatAlertModalProps) {
  const [trigger, setTrigger] = useState<Trigger>('epochs_left')
  const [threshold, setThreshold] = useState('2')
  const [announce, setAnnounce] = useState(true)
  const create = useCreateSeatAlert()
  const test = useSendTestAlert()

  const thresholdNum = parseFloat(threshold)
  const canSubmit = !isNaN(thresholdNum) && thresholdNum >= 0 && create.status === 'idle'

  const submit = () => {
    create.mutate({
      seat_pk: seat.pk,
      trigger_type: trigger,
      threshold_value: thresholdNum,
      announcements_opt_in: announce,
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className="relative bg-card border border-border rounded-lg shadow-lg max-w-md w-full mx-4 p-6">
        <button onClick={onClose} className="absolute top-3 right-3 p-1 text-muted-foreground hover:text-foreground">
          <X className="h-4 w-4" />
        </button>
        <h2 className="text-lg font-medium mb-1">Set up a Telegram alert</h2>
        <p className="text-xs text-muted-foreground font-mono">{seat.pk}</p>
        <p className="text-xs text-muted-foreground mb-4">
          You'll get a Telegram message when this seat runs low. Telegram is the only channel for now — close this if you don't use it.
        </p>

        {create.isSuccess ? (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
              <Check className="h-5 w-5" /><span className="font-medium">Alert created</span>
            </div>
            <p className="text-sm text-muted-foreground">
              Tap below to open Telegram, then press <b>Start</b> to activate. You'll get a confirmation message.
            </p>
            <a
              href={create.data!.telegram_deep_link}
              target="_blank" rel="noreferrer"
              className="inline-flex items-center gap-2 px-3 py-2 rounded-md bg-primary text-primary-foreground text-sm"
            >
              <Send className="h-4 w-4" /> Activate in Telegram
            </a>
            <div>
              <button
                onClick={() => test.mutate(create.data!.id)}
                className="text-sm text-primary hover:underline"
                disabled={test.isPending}
              >
                {test.isPending ? 'Sending…' : 'Send a test message'}
              </button>
              {test.isSuccess && <span className="ml-2 text-xs text-green-600">sent</span>}
              {test.isError && <span className="ml-2 text-xs text-red-500">failed (activate first)</span>}
            </div>
          </div>
        ) : create.isError ? (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-red-500">
              <AlertCircle className="h-5 w-5" /><span className="text-sm">{String(create.error)}</span>
            </div>
            <button onClick={() => create.reset()} className="text-sm text-primary hover:underline">Try again</button>
          </div>
        ) : create.isPending ? (
          <div className="flex items-center justify-center gap-2 py-4">
            <Loader2 className="h-5 w-5 animate-spin text-primary" /><span className="text-sm">Creating…</span>
          </div>
        ) : (
          <div className="space-y-4">
            <div>
              <label className="block text-sm mb-1">Warn me when</label>
              <div className="flex gap-2">
                <button
                  onClick={() => { setTrigger('epochs_left'); setThreshold('2') }}
                  className={`text-xs px-2.5 py-1 rounded-full border ${trigger === 'epochs_left' ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}`}
                >epochs left ≤</button>
                <button
                  onClick={() => { setTrigger('balance_below_usdc'); setThreshold('') }}
                  className={`text-xs px-2.5 py-1 rounded-full border ${trigger === 'balance_below_usdc' ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground'}`}
                >balance below (USDC)</button>
              </div>
            </div>
            <input
              type="number" min="0" value={threshold}
              onChange={(e) => setThreshold(e.target.value)}
              className="w-full bg-background border border-border rounded-md px-3 py-2 text-sm"
            />
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={announce} onChange={(e) => setAnnounce(e.target.checked)} />
              Occasional DoubleZero product updates
            </label>
            <button
              onClick={submit} disabled={!canSubmit}
              className="w-full px-3 py-2 rounded-md bg-primary text-primary-foreground text-sm disabled:opacity-50"
            >Create alert</button>
          </div>
        )}
      </div>
    </div>
  )
}
