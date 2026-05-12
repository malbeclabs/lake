import { Coins, Loader2 } from 'lucide-react'
import type { ShredDevice } from '@/lib/api'
import type { TransactionStatus } from '@/hooks/use-shred-transaction'
import { DisabledFeatureCard } from './disabled-feature-card'

interface ReceiptPanelProps {
  selectedDevice: ShredDevice
  clientIp: string
  amountStr: string
  amount: number
  amountValid: boolean
  pricePerEpoch: number
  prepaidEpochs: number
  usdcBalance: bigint
  connected: boolean
  isAuthenticated: boolean
  userEmail: string | null
  canSubmit: boolean
  txStatus: TransactionStatus
  simulateMode: boolean
  onPay: () => void
}

export function ReceiptPanel({
  selectedDevice,
  clientIp,
  amountStr,
  amount,
  amountValid,
  pricePerEpoch,
  prepaidEpochs,
  usdcBalance,
  connected,
  isAuthenticated,
  userEmail,
  canSubmit,
  txStatus,
  simulateMode,
  onPay,
}: ReceiptPanelProps) {
  const txInFlight = txStatus !== 'idle' && txStatus !== 'error' && txStatus !== 'confirmed' && txStatus !== 'simulated'
  const payDisabled = !canSubmit || txInFlight

  const totalDisplay = amountValid ? `$${amount.toFixed(2)}` : `$${amountStr || '0.00'}`
  const balanceDisplay = connected ? `$${(Number(usdcBalance) / 1e6).toFixed(2)}` : '—'

  return (
    <aside className="sticky top-4 border border-border rounded-lg bg-card p-5 space-y-4">
      {/* Lane header */}
      <div>
        {isAuthenticated ? (
          <div className="text-xs">
            <span className="text-muted-foreground">Signed in as </span>
            <span className="font-medium text-foreground truncate">{userEmail ?? 'user'}</span>
          </div>
        ) : (
          <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium rounded bg-muted text-muted-foreground border border-border">
            Guest checkout
          </span>
        )}
      </div>

      {/* Line items */}
      <div className="text-sm space-y-1.5">
        <Row label="Device" value={<span className="font-mono">{selectedDevice.device_code}</span>} />
        <Row label="Metro" value={selectedDevice.metro_code} />
        <Row label="IP" value={clientIp ? <span className="font-mono text-xs">{clientIp}</span> : <span className="text-muted-foreground">—</span>} />
        <Row label="$ / epoch" value={`$${pricePerEpoch}`} />
        <Row
          label="Epochs"
          value={amountValid && prepaidEpochs > 0 ? `~${prepaidEpochs}` : <span className="text-muted-foreground">—</span>}
        />
      </div>

      <div className="border-t border-border" />

      <div className="text-sm space-y-1.5">
        <Row label={<span className="font-medium">Total</span>} value={<span className="font-medium tabular-nums">{totalDisplay} USDC</span>} />
        <Row label="Balance" value={<span className="tabular-nums text-muted-foreground">{balanceDisplay}</span>} />
      </div>

      {/* Pay button */}
      {simulateMode ? (
        <button
          onClick={onPay}
          disabled={payDisabled}
          className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg bg-amber-600 text-white font-medium text-sm hover:bg-amber-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {txInFlight ? <Loader2 className="h-4 w-4 animate-spin" /> : <Coins className="h-4 w-4" />}
          {amountValid ? `Simulate ${totalDisplay} USDC` : 'Simulate'}
        </button>
      ) : (
        <button
          onClick={onPay}
          disabled={payDisabled}
          className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {txInFlight ? <Loader2 className="h-4 w-4 animate-spin" /> : <Coins className="h-4 w-4" />}
          {!connected
            ? 'Connect wallet to pay'
            : amountValid ? `Pay ${totalDisplay} USDC` : 'Pay'}
        </button>
      )}

      {/* Subscription history stub — only when signed in */}
      {isAuthenticated && (
        <DisabledFeatureCard reason="Coming with Login flow">
          <div className="text-xs text-muted-foreground">Subscription history will appear here.</div>
        </DisabledFeatureCard>
      )}
    </aside>
  )
}

function Row({ label, value }: { label: React.ReactNode; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <span className="text-muted-foreground">{label}</span>
      <span className="text-foreground text-right truncate min-w-0">{value}</span>
    </div>
  )
}
