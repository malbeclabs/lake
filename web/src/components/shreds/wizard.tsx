import { Link } from 'react-router-dom'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import type { PublicKey } from '@solana/web3.js'
import {
  Coins,
  Loader2,
  AlertCircle,
  Check,
  AlertTriangle,
  ArrowRight,
  ArrowLeft,
} from 'lucide-react'
import type { ShredDevice, ShredsOverview } from '@/lib/api'
import type { TransactionStatus } from '@/hooks/use-shred-transaction'
import type { EpochInfo } from '@/hooks/use-epoch-info'
import { TransactionProgress } from './transaction-progress'
import { EpochProgress, EpochWarning } from './epoch-progress'
import { DisabledFeatureCard } from './disabled-feature-card'
import { ReceiptPanel } from './receipt-panel'

const EPOCH_PRESETS = [1, 4, 15, 90]
const DEFAULT_PRESET = 4

type ShredAccountState = {
  seatExists: boolean
  escrowExists: boolean
  seatActive: boolean
}

interface WizardProps {
  selectedDevice: ShredDevice
  clientIp: string
  setClientIp: (v: string) => void
  ipValid: boolean
  amountStr: string
  setAmountStr: (v: string) => void
  amount: number
  amountValid: boolean
  amountBelowMin: boolean
  pricePerEpoch: number
  prepaidEpochs: number
  minAmount: number
  usdcBalance: bigint
  insufficientBalance: boolean
  shredState: ShredAccountState
  epochInfo: EpochInfo | null | undefined
  overview: ShredsOverview | undefined
  connected: boolean
  walletPublicKey: PublicKey | null
  txStatus: TransactionStatus
  txSignature: string | null
  txError: string | null
  simulateMode: boolean
  canSubmit: boolean
  onSubscribe: () => void
  onSimulate: () => void
  onReset: () => void
  onChangeDevice: () => void
  onStartOver: () => void
  isAuthenticated: boolean
  userEmail: string | null
}

export function Wizard(props: WizardProps) {
  const {
    selectedDevice,
    clientIp,
    setClientIp,
    ipValid,
    amountStr,
    setAmountStr,
    amount,
    amountValid,
    amountBelowMin,
    pricePerEpoch,
    prepaidEpochs,
    minAmount,
    usdcBalance,
    insufficientBalance,
    shredState,
    epochInfo,
    overview,
    connected,
    walletPublicKey,
    txStatus,
    txSignature,
    txError,
    simulateMode,
    canSubmit,
    onSubscribe,
    onSimulate,
    onReset,
    onChangeDevice,
    onStartOver,
    isAuthenticated,
    userEmail,
  } = props

  const ipDone = ipValid && clientIp !== ''
  const fundDone = amountValid && !amountBelowMin
  const confirmDone = txStatus === 'confirmed'

  // Step state for the top bar.
  const steps: { label: string; status: 'done' | 'current' | 'pending' }[] = [
    { label: 'Device', status: 'done' }, // always done — we only enter the wizard with a device
    {
      label: 'Server IP',
      status: ipDone ? 'done' : 'current',
    },
    {
      label: 'Fund',
      status: confirmDone || fundDone ? 'done' : ipDone ? 'current' : 'pending',
    },
    {
      label: 'Confirm',
      status: confirmDone ? 'done' : fundDone && ipDone ? 'current' : 'pending',
    },
  ]

  // The amount field is the source of truth for the tx; presets just set its value.
  const handleEpochPreset = (n: number) => {
    if (pricePerEpoch <= 0) return
    setAmountStr(String(n * pricePerEpoch))
  }

  // Which preset (if any) is currently selected — derived purely from amountStr.
  const selectedPreset = (() => {
    if (pricePerEpoch <= 0 || !amountValid) return null
    const exact = amount / pricePerEpoch
    if (Math.abs(exact - Math.round(exact)) > 1e-9) return null
    return EPOCH_PRESETS.includes(Math.round(exact)) ? Math.round(exact) : null
  })()

  return (
    <div className="space-y-6">
      {/* Step bar */}
      <StepBar steps={steps} />

      <div className="grid grid-cols-1 lg:grid-cols-[1fr_320px] gap-6 items-start">
        <div className="space-y-6 min-w-0">
          {/* Step 1: Device */}
          <Section title="1 · Device">
            <div className="border border-border rounded-lg bg-card p-5 flex flex-wrap items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="text-sm font-mono font-medium truncate">{selectedDevice.device_code}</div>
                <div className="text-xs text-muted-foreground mt-0.5">
                  {selectedDevice.metro_code} · ${selectedDevice.total_price_dollars} / epoch · {selectedDevice.available_seats} seats free
                </div>
              </div>
              <button
                onClick={onChangeDevice}
                className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
              >
                <ArrowLeft className="h-3.5 w-3.5" />
                Change device
              </button>
            </div>
          </Section>

          {/* Step 2: Server IP */}
          <Section title="2 · Server IP">
            <div className="border border-border rounded-lg bg-card p-5 space-y-3">
              <div>
                <label className="block text-sm font-medium mb-1.5">Client IP Address</label>
                <input
                  type="text"
                  value={clientIp}
                  onChange={e => { setClientIp(e.target.value); onReset() }}
                  placeholder="e.g. 192.168.1.100"
                  className={`w-full max-w-xs px-3 py-2 text-sm border rounded-lg bg-background focus:outline-none focus:ring-2 focus:ring-primary/50 font-mono ${
                    clientIp && !ipValid ? 'border-red-500' : 'border-border'
                  }`}
                />
                {clientIp && !ipValid && (
                  <p className="text-xs text-red-500 mt-1">Enter a valid IPv4 address</p>
                )}
                <p className="text-xs text-muted-foreground mt-1">
                  The IPv4 address of the server that will receive shreds. UDP port 9090.
                </p>
              </div>

              {/* Multi-seat / multi-IP placeholder */}
              <DisabledFeatureCard reason="Backend not yet supported">
                <div className="space-y-2">
                  <div className="text-xs text-muted-foreground uppercase tracking-wider">Additional seats</div>
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="e.g. 192.168.1.101"
                      className="flex-1 max-w-xs px-3 py-1.5 text-sm border border-border rounded bg-background font-mono"
                      tabIndex={-1}
                    />
                    <button className="text-xs px-3 py-1.5 border border-border rounded bg-muted text-muted-foreground" tabIndex={-1}>
                      + Add seat
                    </button>
                  </div>
                  <button className="text-xs px-3 py-1 border border-dashed border-border rounded text-muted-foreground" tabIndex={-1}>
                    Paste CSV of IPs
                  </button>
                </div>
              </DisabledFeatureCard>
            </div>
          </Section>

          {/* Step 3: Fund */}
          <Section title="3 · Fund">
            <div className="border border-border rounded-lg bg-card p-5 space-y-4">
              {/* Epoch presets */}
              <div>
                <label className="block text-sm font-medium mb-1.5">Epochs</label>
                <div className="flex flex-wrap gap-2">
                  {EPOCH_PRESETS.map(n => {
                    const isSelected = selectedPreset === n
                    return (
                      <button
                        key={n}
                        onClick={() => handleEpochPreset(n)}
                        className={`px-3 py-1.5 text-sm rounded-lg border transition-colors ${
                          isSelected
                            ? 'border-primary bg-primary/10 text-foreground'
                            : 'border-border bg-background text-muted-foreground hover:text-foreground hover:bg-muted'
                        }`}
                      >
                        {n}
                        {n === DEFAULT_PRESET && !isSelected && (
                          <span className="ml-1.5 text-[10px] uppercase tracking-wider text-muted-foreground">default</span>
                        )}
                        <span className="ml-1.5 text-xs text-muted-foreground tabular-nums">
                          ${n * pricePerEpoch}
                        </span>
                      </button>
                    )
                  })}
                </div>
                <p className="text-xs text-muted-foreground mt-1.5">
                  One epoch is ~2 days. Top up any time; refund unused funds with the unsubscribe flow.
                </p>
              </div>

              {/* Custom amount */}
              <div>
                <label className="block text-sm font-medium mb-1.5">Amount (USDC)</label>
                <div className="flex items-center gap-3">
                  <div className="relative max-w-xs">
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground text-sm">$</span>
                    <input
                      type="number"
                      value={amountStr}
                      onChange={e => { setAmountStr(e.target.value); onReset() }}
                      placeholder="0.00"
                      min="0"
                      step="0.01"
                      className={`w-full pl-7 pr-3 py-2 text-sm border rounded-lg bg-background focus:outline-none focus:ring-2 focus:ring-primary/50 tabular-nums ${
                        amountBelowMin ? 'border-red-500' : 'border-border'
                      }`}
                    />
                  </div>
                  {connected && (
                    <span className="text-xs text-muted-foreground">
                      Balance: ${(Number(usdcBalance) / 1e6).toFixed(2)}
                    </span>
                  )}
                </div>
                {amountBelowMin && (
                  <p className="text-xs text-red-500 mt-1">
                    Minimum amount is ${minAmount} (1 epoch)
                  </p>
                )}
                {insufficientBalance && !amountBelowMin && (
                  <p className="text-xs text-red-500 mt-1">
                    Insufficient USDC balance
                  </p>
                )}
                {amountValid && !amountBelowMin && prepaidEpochs > 0 && (
                  <p className="text-xs text-muted-foreground mt-1">
                    Covers ~{prepaidEpochs} epoch{prepaidEpochs !== 1 ? 's' : ''} at ${pricePerEpoch}/epoch
                  </p>
                )}
              </div>

              {/* On-chain state info */}
              {shredState.seatExists && (
                <div className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg bg-blue-500/10 border border-blue-500/20 text-blue-600 dark:text-blue-400">
                  <AlertCircle className="h-4 w-4 flex-shrink-0" />
                  <span>
                    A seat already exists for this device + IP.
                    {shredState.seatActive
                      ? ' This will add funds to the existing subscription.'
                      : ' This will re-activate the seat and add funds.'}
                  </span>
                </div>
              )}

              {/* Epoch progress counter */}
              {epochInfo && (
                <EpochProgress
                  epoch={epochInfo.epoch}
                  progressPct={epochInfo.progressPct}
                  remainingMs={epochInfo.remainingMs}
                />
              )}

              {/* Epoch warning */}
              {overview && overview.current_solana_epoch > 0 && !shredState.seatActive && (
                <EpochWarning currentEpoch={overview.current_solana_epoch} />
              )}

              {/* Auto-renew placeholder */}
              <DisabledFeatureCard reason="Coming with Login flow">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-sm font-medium">Auto-renew</div>
                    <div className="text-xs text-muted-foreground">Top up when escrow runs low.</div>
                  </div>
                  <div
                    role="switch"
                    aria-checked={false}
                    className="h-5 w-9 rounded-full bg-muted border border-border relative"
                    tabIndex={-1}
                  >
                    <div className="absolute top-0.5 left-0.5 h-3.5 w-3.5 rounded-full bg-background border border-border" />
                  </div>
                </div>
              </DisabledFeatureCard>

              {/* Payment method placeholder */}
              <div>
                <label className="block text-sm font-medium mb-1.5">Pay with</label>
                <div className="flex flex-wrap gap-2 items-center">
                  <span className="inline-flex items-center px-3 py-1.5 text-sm rounded-lg border border-primary bg-primary/10 text-foreground">
                    <Check className="h-3.5 w-3.5 mr-1.5" />
                    USDC · wallet
                  </span>
                  <DisabledFeatureCard reason="Coming with Login flow" className="!p-2 inline-block">
                    <div className="flex gap-2">
                      <span className="inline-flex items-center px-3 py-1 text-sm rounded border border-border bg-background text-muted-foreground">Card</span>
                      <span className="inline-flex items-center px-3 py-1 text-sm rounded border border-border bg-background text-muted-foreground">Invoice (Net-15)</span>
                    </div>
                  </DisabledFeatureCard>
                </div>
              </div>
            </div>
          </Section>

          {/* Step 4: Confirm */}
          <Section title="4 · Confirm">
            {simulateMode && (
              <div className="flex items-center gap-2 text-xs text-amber-600 dark:text-amber-400 mb-3 px-1">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                Simulate mode — transactions will not be submitted
              </div>
            )}

            <div className="border border-border rounded-lg bg-card p-5">
              {!connected ? (
                <div className="flex flex-col items-center gap-4 py-4">
                  <p className="text-sm text-muted-foreground">Connect your wallet to subscribe</p>
                  <WalletMultiButton />
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div className="text-sm">
                      <span className="text-muted-foreground">Connected:</span>{' '}
                      <span className="font-mono text-xs">
                        {walletPublicKey?.toBase58().slice(0, 6)}...{walletPublicKey?.toBase58().slice(-4)}
                      </span>
                    </div>
                    <WalletMultiButton />
                  </div>

                  {txStatus === 'simulated' ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                        <Check className="h-5 w-5" />
                        <span className="font-medium">Simulation passed — transaction is valid. No funds spent.</span>
                      </div>
                      <button
                        onClick={onReset}
                        className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                      >
                        Try again
                      </button>
                    </div>
                  ) : txStatus === 'confirmed' ? (
                    <div className="space-y-4">
                      <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                        <Check className="h-5 w-5" />
                        <span className="font-medium">Subscription successful!</span>
                      </div>
                      <TransactionProgress status={txStatus} txSignature={txSignature} />
                      <div className="flex flex-wrap items-center gap-3 pt-2">
                        <Link
                          to="/dz/shreds/subscribers"
                          className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
                        >
                          View your seats <ArrowRight className="h-3.5 w-3.5" />
                        </Link>
                        <button
                          onClick={onStartOver}
                          className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                        >
                          Subscribe to another device
                        </button>
                        <DisabledFeatureCard reason="Coming with Login flow" className="!p-2 inline-block">
                          <button className="text-sm text-muted-foreground" tabIndex={-1}>Email receipt</button>
                        </DisabledFeatureCard>
                      </div>
                    </div>
                  ) : txStatus === 'error' ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-red-500">
                        <AlertCircle className="h-5 w-5" />
                        <span className="text-sm">
                          {simulateMode ? 'Simulation error: ' : ''}{txError}
                        </span>
                      </div>
                      {txSignature && (
                        <TransactionProgress status={txStatus} txSignature={txSignature} />
                      )}
                      <button
                        onClick={onReset}
                        className="text-sm text-primary hover:underline"
                      >
                        Try again
                      </button>
                    </div>
                  ) : txStatus === 'simulating' ? (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Simulating on-chain...
                    </div>
                  ) : txStatus !== 'idle' ? (
                    <TransactionProgress status={txStatus} txSignature={txSignature} />
                  ) : simulateMode ? (
                    <button
                      onClick={onSimulate}
                      disabled={!canSubmit}
                      className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-amber-600 text-white font-medium text-sm hover:bg-amber-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <Coins className="h-4 w-4" />
                      {amountValid
                        ? `Simulate — $${amount.toFixed(2)} USDC (no funds sent)`
                        : 'Simulate'}
                    </button>
                  ) : (
                    <button
                      onClick={onSubscribe}
                      disabled={!canSubmit}
                      className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <Coins className="h-4 w-4" />
                      {amountValid
                        ? `Subscribe — $${amount.toFixed(2)} USDC`
                        : 'Subscribe'}
                    </button>
                  )}
                </div>
              )}
            </div>
          </Section>
        </div>

        {/* Right column: sticky receipt */}
        <div>
          <ReceiptPanel
            selectedDevice={selectedDevice}
            clientIp={clientIp}
            amountStr={amountStr}
            amount={amount}
            amountValid={amountValid}
            pricePerEpoch={pricePerEpoch}
            prepaidEpochs={prepaidEpochs}
            usdcBalance={usdcBalance}
            connected={connected}
            isAuthenticated={isAuthenticated}
            userEmail={userEmail}
            canSubmit={canSubmit}
            txStatus={txStatus}
            simulateMode={simulateMode}
            onPay={simulateMode ? onSimulate : onSubscribe}
          />
        </div>
      </div>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">{title}</h2>
      {children}
    </section>
  )
}

function StepBar({ steps }: { steps: { label: string; status: 'done' | 'current' | 'pending' }[] }) {
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      {steps.map((step, i) => (
        <div key={step.label} className="flex items-center gap-2">
          <div
            className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full border transition-colors ${
              step.status === 'done'
                ? 'bg-primary text-primary-foreground border-primary'
                : step.status === 'current'
                  ? 'bg-primary/10 text-foreground border-primary border-2 py-[5px]'
                  : 'bg-background text-muted-foreground border-border'
            }`}
          >
            <span
              className={`inline-flex items-center justify-center h-5 w-5 rounded-full text-xs tabular-nums ${
                step.status === 'done'
                  ? 'bg-primary-foreground/20'
                  : step.status === 'current'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-muted'
              }`}
            >
              {step.status === 'done' ? <Check className="h-3 w-3" /> : i + 1}
            </span>
            <span className="font-medium text-xs uppercase tracking-wider">{step.label}</span>
          </div>
          {i < steps.length - 1 && <div className="h-px w-4 bg-border" />}
        </div>
      ))}
    </div>
  )
}
