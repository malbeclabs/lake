import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useWallet } from '@solana/wallet-adapter-react'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import { PublicKey } from '@solana/web3.js'
import {
  AlertCircle,
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  Check,
  ExternalLink,
  Loader2,
  LogOut,
} from 'lucide-react'
import { fetchShredClientSeats, type ShredClientSeat } from '@/lib/api'
import { ipv4ToU32 } from '@/lib/shred-program'
import {
  buildUnsubscribeInstructions,
  deriveShredAccounts,
} from '@/lib/shred-transactions'
import { useShredAccounts } from '@/hooks/use-shred-accounts'
import {
  useShredTransaction,
  type TransactionStatus,
  type UseShredTransactionResult,
} from '@/hooks/use-shred-transaction'
import { useAuth } from '@/contexts/AuthContext'
import { Section, StepBar } from './wizard-shared'
import { TransactionProgress } from './transaction-progress'

const STATUSES = 'active,expiring,pending,inactive'

// Sample seats shown only when an internal user enters preview mode
// (?preview=true). RFC 5737 TEST-NET-2 IPs guarantee no collision with real ones.
const PREVIEW_SEATS: ShredClientSeat[] = [
  {
    pk: 'preview-seat-1',
    device_key: 'preview-device-1',
    device_code: 'dz-demo-01',
    metro_pk: 'preview-metro-1',
    metro_code: 'NYC',
    client_ip: '198.51.100.42',
    tenure_epochs: 5,
    funded_epoch: 0,
    active_epoch: 0,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 1,
    total_usdc_balance: 24_000_000,
    price_per_epoch_dollars: 4,
    funding_authority_key: 'preview-funder',
    user_pk: 'preview-user-1',
    user_owner_pubkey: 'preview-owner',
    user_status: 'active',
    last_activity: new Date().toISOString(),
  },
  {
    pk: 'preview-seat-2',
    device_key: 'preview-device-2',
    device_code: 'dz-demo-02',
    metro_pk: 'preview-metro-2',
    metro_code: 'LAX',
    client_ip: '198.51.100.43',
    tenure_epochs: 1,
    funded_epoch: 0,
    active_epoch: 0,
    has_price_override: 0,
    override_usdc_price_dollars: 0,
    escrow_count: 1,
    total_usdc_balance: 4_000_000,
    price_per_epoch_dollars: 4,
    funding_authority_key: 'preview-funder',
    user_pk: 'preview-user-2',
    user_owner_pubkey: 'preview-owner',
    user_status: 'expiring',
    last_activity: new Date().toISOString(),
  },
]

// Drives the same status sequence as a real withdraw, with no wallet or RPC calls.
function useMockedWithdrawTx(): UseShredTransactionResult {
  const [status, setStatus] = useState<TransactionStatus>('idle')
  const [txSignature, setTxSignature] = useState<string | null>(null)

  const reset = useCallback(() => {
    setStatus('idle')
    setTxSignature(null)
  }, [])

  const execute = useCallback(async (): Promise<string | null> => {
    const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))
    setTxSignature(null)
    setStatus('building');   await sleep(300)
    setStatus('signing');    await sleep(600)
    setStatus('sending');    await sleep(500)
    const sig = 'PreviewTx_NotReal_' + Date.now()
    setTxSignature(sig)
    setStatus('confirming'); await sleep(800)
    setStatus('confirmed')
    return sig
  }, [])

  // simulate is unused in preview but kept to match the result shape.
  const simulate = useCallback(async () => {
    setStatus('simulated')
  }, [])

  return { status, txSignature, error: null, execute, simulate, reset }
}

export function WithdrawWizard() {
  const { publicKey: wallet, connected } = useWallet()
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const simulateMode = import.meta.env.DEV && searchParams.get('simulate') === 'true'

  const { user } = useAuth()
  const isInternal = !!user?.is_internal_user
  const preview = isInternal && searchParams.get('preview') === 'true'

  const [selectedSeatPk, setSelectedSeatPk] = useState<string | null>(null)

  const walletStr = wallet?.toBase58() ?? null

  const {
    data: seatsData,
    isLoading: rawSeatsLoading,
    error: rawSeatsError,
  } = useQuery({
    queryKey: ['my-shred-seats', walletStr],
    queryFn: () =>
      fetchShredClientSeats({
        filters: [`funder:${walletStr!}`],
        status: STATUSES,
        limit: 100,
        sortBy: 'last_activity',
        sortDir: 'desc',
      }),
    enabled: !!walletStr,
    refetchInterval: 30_000,
  })

  const seats = useMemo<ShredClientSeat[]>(
    () => (preview ? PREVIEW_SEATS : (seatsData?.items ?? [])),
    [preview, seatsData],
  )
  const seatsLoading = preview ? false : rawSeatsLoading
  const seatsError = preview ? null : rawSeatsError

  // Auto-select when exactly one seat exists and nothing else is picked yet.
  useEffect(() => {
    if (!selectedSeatPk && seats.length === 1) {
      setSelectedSeatPk(seats[0].pk)
    }
  }, [seats, selectedSeatPk])

  const selectedSeat = useMemo(
    () => seats.find(s => s.pk === selectedSeatPk) ?? null,
    [seats, selectedSeatPk],
  )

  const devicePubkey = useMemo(() => {
    if (!selectedSeat) return null
    try { return new PublicKey(selectedSeat.device_key) } catch { return null }
  }, [selectedSeat])

  const metroPubkey = useMemo(() => {
    if (!selectedSeat) return null
    try { return new PublicKey(selectedSeat.metro_pk) } catch { return null }
  }, [selectedSeat])

  const shredState = useShredAccounts(devicePubkey, selectedSeat?.client_ip ?? null)
  // Call both hooks unconditionally to satisfy the rules of hooks; pick the
  // result we render from based on `preview`.
  const realTx = useShredTransaction()
  const mockTx = useMockedWithdrawTx()
  const { status: txStatus, txSignature, error: txError, execute, simulate, reset: resetTx } = preview ? mockTx : realTx

  const canSubmit = Boolean(
    selectedSeat && txStatus === 'idle' &&
    (preview || (connected && devicePubkey && metroPubkey)),
  )

  const handleWithdraw = useCallback(async () => {
    if (!canSubmit || !selectedSeat) return
    if (preview) {
      await execute([])
      return
    }
    if (!wallet || !devicePubkey || !metroPubkey) return
    const clientIpBits = ipv4ToU32(selectedSeat.client_ip)
    const accounts = deriveShredAccounts({ device: devicePubkey, metroExchange: metroPubkey, clientIpBits, wallet })
    const ixs = buildUnsubscribeInstructions({ accounts, wallet, escrowExists: shredState.escrowExists })
    if (simulateMode) {
      await simulate(ixs)
      return
    }
    const sig = await execute(ixs)
    if (sig) {
      queryClient.invalidateQueries({ queryKey: ['my-shred-seats'] })
      queryClient.invalidateQueries({ queryKey: ['shred-client-seats'] })
    }
  }, [canSubmit, preview, wallet, selectedSeat, devicePubkey, metroPubkey, shredState.escrowExists, simulateMode, simulate, execute, queryClient])

  const handleChangeSeat = useCallback(() => {
    setSelectedSeatPk(null)
    resetTx()
  }, [resetTx])

  const handleWithdrawAnother = useCallback(() => {
    setSelectedSeatPk(null)
    resetTx()
    queryClient.invalidateQueries({ queryKey: ['my-shred-seats'] })
  }, [resetTx, queryClient])

  const handleSwitchToSubscribe = useCallback(() => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set('mode', 'subscribe')
      return next
    })
  }, [setSearchParams])

  const handleEnterPreview = useCallback(() => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set('preview', 'true')
      return next
    })
  }, [setSearchParams])

  const handleExitPreview = useCallback(() => {
    setSelectedSeatPk(null)
    mockTx.reset()
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.delete('preview')
      return next
    })
  }, [setSearchParams, mockTx])

  const seatPicked = !!selectedSeat
  const confirmed = txStatus === 'confirmed' || txStatus === 'simulated'

  const steps = [
    { label: 'Seat', status: seatPicked ? ('done' as const) : ('current' as const) },
    {
      label: 'Confirm',
      status: confirmed ? ('done' as const) : seatPicked ? ('current' as const) : ('pending' as const),
    },
  ]

  // Connect-wallet gate. Skipped in preview so internal users can demo without a wallet.
  if (!connected && !preview) {
    return (
      <div className="space-y-6">
        <StepBar steps={steps} />
        <Section title="1 · Seat">
          <div className="border border-border rounded-lg bg-card p-8 flex flex-col items-center gap-4">
            <LogOut className="h-8 w-8 text-muted-foreground" />
            <p className="text-sm text-muted-foreground text-center max-w-sm">
              Connect the wallet that originally funded your seat to view and withdraw it.
            </p>
            <WalletMultiButton />
            {isInternal && (
              <button
                onClick={handleEnterPreview}
                className="text-xs text-muted-foreground hover:text-foreground transition-colors inline-flex items-center gap-1.5"
              >
                Or see a sample workflow <ArrowRight className="h-3 w-3" />
              </button>
            )}
          </div>
        </Section>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {preview && <PreviewBanner onExit={handleExitPreview} />}
      <StepBar steps={steps} />

      {/* Step 1: Seat */}
      <Section title="1 · Seat">
        {selectedSeat ? (
          <SelectedSeatCard seat={selectedSeat} onChange={handleChangeSeat} />
        ) : seatsLoading ? (
          <div className="border border-border rounded-lg bg-card p-8 flex justify-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : seatsError ? (
          <div className="border border-border rounded-lg bg-card p-5 text-sm text-red-500 flex items-center gap-2">
            <AlertCircle className="h-4 w-4" />
            Failed to load your seats. Try refreshing.
          </div>
        ) : seats.length === 0 ? (
          <div className="border border-border rounded-lg bg-card p-8 flex flex-col items-center gap-3">
            <p className="text-sm text-muted-foreground">
              No shred seats found for this wallet.
            </p>
            <div className="flex flex-wrap items-center gap-x-4 gap-y-2 justify-center">
              <button
                onClick={handleSwitchToSubscribe}
                className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
              >
                Subscribe to a device <ArrowRight className="h-3.5 w-3.5" />
              </button>
              {isInternal && !preview && (
                <button
                  onClick={handleEnterPreview}
                  className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
                >
                  See sample workflow <ArrowRight className="h-3.5 w-3.5" />
                </button>
              )}
            </div>
          </div>
        ) : (
          <SeatList seats={seats} onSelect={setSelectedSeatPk} />
        )}
      </Section>

      {/* Step 2: Confirm */}
      {selectedSeat && (
        <Section title="2 · Confirm">
          <div className="border border-border rounded-lg bg-card p-5 space-y-4">
            {simulateMode && (
              <div className="flex items-center gap-2 text-xs text-amber-600 dark:text-amber-400">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                Simulate mode — transaction will not be submitted
              </div>
            )}

            {txStatus === 'simulated' ? (
              <div className="space-y-3">
                <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                  <Check className="h-5 w-5" />
                  <span className="font-medium">Simulation passed — transaction is valid. No funds withdrawn.</span>
                </div>
                <button onClick={resetTx} className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                  Try again
                </button>
              </div>
            ) : txStatus === 'confirmed' ? (
              <ConfirmedPanel
                seat={selectedSeat}
                txSignature={txSignature}
                preview={preview}
                onWithdrawAnother={handleWithdrawAnother}
                onSwitchToSubscribe={handleSwitchToSubscribe}
              />
            ) : txStatus === 'error' ? (
              <div className="space-y-3">
                <div className="flex items-center gap-2 text-red-500">
                  <AlertCircle className="h-5 w-5" />
                  <span className="text-sm">
                    {simulateMode ? 'Simulation error: ' : ''}{txError}
                  </span>
                </div>
                {txSignature && <TransactionProgress status={txStatus} txSignature={txSignature} />}
                <button onClick={resetTx} className="text-sm text-primary hover:underline">
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
            ) : (
              <ConfirmPrompt
                seat={selectedSeat}
                canSubmit={canSubmit}
                simulateMode={simulateMode}
                onWithdraw={handleWithdraw}
              />
            )}
          </div>
        </Section>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Subcomponents
// ---------------------------------------------------------------------------

function SeatList({ seats, onSelect }: { seats: ShredClientSeat[]; onSelect: (pk: string) => void }) {
  return (
    <div className="space-y-2">
      {seats.map(seat => {
        const balance = seat.total_usdc_balance / 1e6
        return (
          <button
            key={seat.pk}
            type="button"
            onClick={() => onSelect(seat.pk)}
            className="w-full text-left border border-border rounded-lg bg-card hover:bg-muted/50 transition-colors p-4 flex flex-wrap items-center justify-between gap-3"
          >
            <div className="min-w-0">
              <div className="text-sm font-mono font-medium truncate">{seat.device_code || seat.device_key.slice(0, 12)}</div>
              <div className="text-xs text-muted-foreground mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-0.5">
                <span>{seat.metro_code || '—'}</span>
                <span>·</span>
                <span className="font-mono">{seat.client_ip}</span>
                <span>·</span>
                <span>tenure {seat.tenure_epochs}</span>
                <SeatStatusBadge status={seat.user_status} />
              </div>
            </div>
            <div className="flex items-center gap-3 shrink-0">
              <div className="text-right">
                <div className="text-sm tabular-nums">${balance.toFixed(2)}</div>
                <div className="text-[10px] text-muted-foreground uppercase tracking-wider">balance</div>
              </div>
              <ArrowRight className="h-4 w-4 text-muted-foreground" />
            </div>
          </button>
        )
      })}
    </div>
  )
}

function SelectedSeatCard({ seat, onChange }: { seat: ShredClientSeat; onChange: () => void }) {
  const balance = seat.total_usdc_balance / 1e6
  return (
    <div className="border border-border rounded-lg bg-card p-5 flex flex-wrap items-center justify-between gap-3">
      <div className="min-w-0">
        <div className="text-sm font-mono font-medium truncate">{seat.device_code || seat.device_key.slice(0, 12)}</div>
        <div className="text-xs text-muted-foreground mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-0.5">
          <span>{seat.metro_code || '—'}</span>
          <span>·</span>
          <span className="font-mono">{seat.client_ip}</span>
          <span>·</span>
          <span className="tabular-nums">${balance.toFixed(2)} balance</span>
          <span>·</span>
          <span>tenure {seat.tenure_epochs}</span>
        </div>
      </div>
      <button
        onClick={onChange}
        className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
      >
        <ArrowLeft className="h-3.5 w-3.5" />
        Change seat
      </button>
    </div>
  )
}

function ConfirmPrompt({
  seat,
  canSubmit,
  simulateMode,
  onWithdraw,
}: {
  seat: ShredClientSeat
  canSubmit: boolean
  simulateMode: boolean
  onWithdraw: () => void
}) {
  const balance = seat.total_usdc_balance / 1e6
  return (
    <div className="space-y-4">
      <div className="text-sm space-y-1.5 p-3 rounded-lg bg-muted/50">
        <Row label="Device" value={<span className="font-mono text-xs">{seat.device_code || seat.device_key.slice(0, 12)}</span>} />
        <Row label="Metro" value={seat.metro_code || '—'} />
        <Row label="Client IP" value={<span className="font-mono">{seat.client_ip}</span>} />
        <Row label="Tenure" value={`${seat.tenure_epochs} epoch${seat.tenure_epochs !== 1 ? 's' : ''}`} />
        <Row label="Refund" value={<span className="tabular-nums font-medium">${balance.toFixed(2)} USDC</span>} />
      </div>

      <div className="flex items-start gap-2 text-sm px-3 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-600 dark:text-amber-400">
        <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
        <span>
          Withdrawing closes this seat and your payment escrow.{' '}
          <strong className="font-semibold">You'll lose your accumulated tenure</strong>, so any future
          subscription starts from zero priority.
          {balance > 0
            ? ` $${balance.toFixed(2)} USDC will be refunded to your wallet (USDC ATA).`
            : ''}
        </span>
      </div>

      <button
        onClick={onWithdraw}
        disabled={!canSubmit}
        className={`inline-flex items-center gap-2 px-5 py-2.5 rounded-lg font-medium text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${
          simulateMode
            ? 'bg-amber-600 text-white hover:bg-amber-500'
            : 'bg-red-600 text-white hover:bg-red-700'
        }`}
      >
        <LogOut className="h-4 w-4" />
        {simulateMode
          ? `Simulate withdraw${balance > 0 ? ` — $${balance.toFixed(2)} USDC (no funds sent)` : ''}`
          : `Unsubscribe${balance > 0 ? ` & Withdraw $${balance.toFixed(2)}` : ''}`}
      </button>
    </div>
  )
}

function ConfirmedPanel({
  seat,
  txSignature,
  preview,
  onWithdrawAnother,
  onSwitchToSubscribe,
}: {
  seat: ShredClientSeat
  txSignature: string | null
  preview: boolean
  onWithdrawAnother: () => void
  onSwitchToSubscribe: () => void
}) {
  const balance = seat.total_usdc_balance / 1e6
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
        <Check className="h-5 w-5" />
        <span className="font-medium">Unsubscribed successfully.</span>
      </div>
      {balance > 0 && (
        <p className="text-sm text-muted-foreground">
          ${balance.toFixed(2)} USDC has been returned to your wallet.
        </p>
      )}
      {preview ? (
        <p className="text-xs text-muted-foreground italic">Sample tx — not on-chain.</p>
      ) : txSignature && (
        <a
          href={`https://solscan.io/tx/${txSignature}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-sm text-blue-500 hover:underline"
        >
          View on Solscan <ExternalLink className="h-3 w-3" />
        </a>
      )}
      <div className="flex flex-wrap items-center gap-3 pt-2">
        <Link
          to="/dz/shreds/subscribers"
          className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
        >
          View seats <ArrowRight className="h-3.5 w-3.5" />
        </Link>
        <button
          onClick={onWithdrawAnother}
          className="text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          Withdraw another seat
        </button>
        <button
          onClick={onSwitchToSubscribe}
          className="text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          Subscribe to a device
        </button>
      </div>
    </div>
  )
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex justify-between gap-3">
      <span className="text-muted-foreground">{label}</span>
      <span>{value}</span>
    </div>
  )
}

function SeatStatusBadge({ status }: { status: string }) {
  const s = (status || '').toLowerCase()
  const tone =
    s === 'active' ? 'text-green-600 dark:text-green-400 bg-green-500/10 border-green-500/20'
    : s === 'expiring' ? 'text-amber-600 dark:text-amber-400 bg-amber-500/10 border-amber-500/20'
    : s === 'pending' ? 'text-blue-600 dark:text-blue-400 bg-blue-500/10 border-blue-500/20'
    : 'text-muted-foreground bg-muted border-border'
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] uppercase tracking-wider border ${tone}`}>
      {s || 'unknown'}
    </span>
  )
}

function PreviewBanner({ onExit }: { onExit: () => void }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 px-3 py-2 rounded-lg border border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-300">
      <div className="flex items-center gap-2 text-sm">
        <AlertTriangle className="h-4 w-4 shrink-0" />
        <span>
          <strong className="font-semibold">Preview mode</strong> — sample seats. No wallet calls, no funds moved.
        </span>
      </div>
      <button
        onClick={onExit}
        className="text-xs underline underline-offset-2 hover:opacity-80"
      >
        Exit preview
      </button>
    </div>
  )
}
