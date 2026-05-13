import { useState, useCallback, useMemo } from 'react'
import { useWallet } from '@solana/wallet-adapter-react'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import { PublicKey } from '@solana/web3.js'
import { X, Loader2, Check, AlertCircle, ExternalLink, Coins } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import type { ShredClientSeat } from '@/lib/api'
import { ipv4ToU32 } from '@/lib/shred-program'
import { deriveShredAccounts, buildFundInstructions } from '@/lib/shred-transactions'
import { useUsdcBalance } from '@/hooks/use-shred-accounts'
import { useShredTransaction } from '@/hooks/use-shred-transaction'
import { useMockedShredTransaction } from '@/lib/mocked-shred-transaction'

interface BulkDepositModalProps {
  seats: ShredClientSeat[]
  onClose: () => void
  preview?: boolean
}

/**
 * Bulk-deposit USDC across multiple seats. The user enters a total dollar amount;
 * we split it proportional to each seat's per-epoch burn rate so each seat gains the
 * same number of additional epochs of runway.
 *
 * Solana legacy tx size limits force us to chunk into ~3 seats per transaction.
 */
const SEATS_PER_TX = 3

export function BulkDepositModal({ seats, onClose, preview = false }: BulkDepositModalProps) {
  const { publicKey: wallet, connected } = useWallet()
  const { balance: usdcBalance } = useUsdcBalance()
  const realTx = useShredTransaction()
  const mockTx = useMockedShredTransaction()
  const { status, error, execute, reset } = preview ? mockTx : realTx
  const queryClient = useQueryClient()

  const [amountStr, setAmountStr] = useState('')
  const amount = parseFloat(amountStr)
  const amountValid = !isNaN(amount) && amount > 0
  const amountMicro = amountValid ? BigInt(Math.floor(amount * 1_000_000)) : 0n
  const insufficientBalance = !preview && amountValid && amountMicro > usdcBalance
  const [batchIdx, setBatchIdx] = useState(0)
  const [allSigs, setAllSigs] = useState<string[]>([])

  const totalBurnRate = useMemo(
    () => seats.reduce((sum, s) => sum + s.price_per_epoch_dollars, 0),
    [seats],
  )

  const epochsAdded = totalBurnRate > 0 && amountValid ? amount / totalBurnRate : 0

  /** Per-seat allocation in micro-USDC, proportional to burn rate. */
  const allocations = useMemo(() => {
    if (!amountValid || totalBurnRate <= 0) return new Map<string, bigint>()
    const allocs = new Map<string, bigint>()
    let assigned = 0n
    for (let i = 0; i < seats.length; i++) {
      const s = seats[i]
      const share = s.price_per_epoch_dollars / totalBurnRate
      const seatMicro =
        i === seats.length - 1
          ? amountMicro - assigned
          : BigInt(Math.floor(Number(amountMicro) * share))
      allocs.set(s.pk, seatMicro)
      assigned += seatMicro
    }
    return allocs
  }, [seats, totalBurnRate, amountValid, amountMicro])

  const chunks = useMemo(() => {
    const out: ShredClientSeat[][] = []
    for (let i = 0; i < seats.length; i += SEATS_PER_TX) {
      out.push(seats.slice(i, i + SEATS_PER_TX))
    }
    return out
  }, [seats])

  const canSubmit = (preview || connected) && amountValid && !insufficientBalance && status === 'idle'

  const handleDeposit = useCallback(async () => {
    if (!canSubmit) return

    if (preview) {
      for (let ci = 0; ci < chunks.length; ci++) {
        setBatchIdx(ci)
        const sig = await execute([])
        if (!sig) return
        setAllSigs((prev) => [...prev, sig])
      }
      return
    }

    if (!wallet) return
    for (let ci = 0; ci < chunks.length; ci++) {
      setBatchIdx(ci)
      const chunk = chunks[ci]
      const ixs = []
      for (const seat of chunk) {
        const seatMicro = allocations.get(seat.pk) ?? 0n
        if (seatMicro <= 0n) continue
        let devicePk: PublicKey
        let metroPk: PublicKey
        try {
          devicePk = new PublicKey(seat.device_key)
          metroPk = new PublicKey(seat.metro_pk)
        } catch { continue }
        const accounts = deriveShredAccounts({
          device: devicePk,
          metroExchange: metroPk,
          clientIpBits: ipv4ToU32(seat.client_ip),
          wallet,
        })
        ixs.push(...buildFundInstructions({ accounts, wallet, amountMicro: seatMicro }))
      }
      if (ixs.length === 0) continue
      const sig = await execute(ixs)
      if (!sig) return
      setAllSigs((prev) => [...prev, sig])
    }
    queryClient.invalidateQueries({ queryKey: ['shred-client-seats'] })
  }, [canSubmit, preview, wallet, chunks, allocations, execute, queryClient])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={status === 'idle' || status === 'confirmed' || status === 'error' ? onClose : undefined} />
      <div className="relative mx-4 w-full max-w-lg rounded-xl border border-border bg-card shadow-2xl">
        <div className="flex items-center gap-2 border-b border-border p-4">
          <Coins className="h-4 w-4 text-primary" />
          <h3 className="m-0 text-[15px] font-semibold">
            Deposit USDC across {seats.length} subscription{seats.length === 1 ? '' : 's'}
          </h3>
          <button
            type="button"
            onClick={onClose}
            className="ml-auto flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground hover:bg-background hover:text-foreground"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>

        <div className="p-4">
          {status === 'confirmed' && batchIdx + 1 >= chunks.length ? (
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-green-400">
                <Check className="h-5 w-5" />
                <span className="font-medium">Deposited across {seats.length} seats.</span>
              </div>
              <div className="space-y-1 text-xs">
                {allSigs.map((s) => (
                  <a
                    key={s}
                    href={`https://solscan.io/tx/${s}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-1 text-blue-400 hover:underline"
                  >
                    {s.slice(0, 10)}…{s.slice(-6)} <ExternalLink className="h-3 w-3" />
                  </a>
                ))}
              </div>
              <div className="flex justify-end">
                <button onClick={onClose} className="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90">
                  Done
                </button>
              </div>
            </div>
          ) : status === 'error' ? (
            <div className="space-y-3">
              <div className="flex items-center gap-2 text-red-400">
                <AlertCircle className="h-5 w-5" />
                <span className="text-sm">{error}</span>
              </div>
              <button onClick={reset} className="text-sm text-primary hover:underline">
                Try again
              </button>
            </div>
          ) : status !== 'idle' ? (
            <div className="flex flex-col items-center justify-center gap-2 py-4">
              <Loader2 className="h-5 w-5 animate-spin text-primary" />
              <span className="text-sm text-muted-foreground">
                Batch {batchIdx + 1} of {chunks.length} —{' '}
                {status === 'building' && 'Building transaction…'}
                {status === 'signing' && 'Waiting for wallet…'}
                {status === 'sending' && 'Sending transaction…'}
                {status === 'confirming' && 'Confirming on-chain…'}
              </span>
            </div>
          ) : !connected && !preview ? (
            <div className="flex flex-col items-center gap-3 py-2">
              <p className="text-sm text-muted-foreground">Connect your wallet to deposit USDC</p>
              <WalletMultiButton />
            </div>
          ) : (
            <div className="space-y-4">
              <div className="rounded-md border border-border bg-background px-3 py-2.5 text-[12.5px]">
                <div className="flex justify-between"><span className="text-muted-foreground">Selected</span><span>{seats.length} seats</span></div>
                <div className="flex justify-between"><span className="text-muted-foreground">Combined burn rate</span><span className="font-mono">${totalBurnRate.toLocaleString()} / ep</span></div>
                <div className="flex justify-between"><span className="text-muted-foreground">Wallet balance</span><span className="font-mono">${(Number(usdcBalance) / 1e6).toFixed(2)} USDC</span></div>
              </div>

              <div>
                <label className="mb-1 block text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Total amount (USDC)
                </label>
                <div className="flex items-center gap-2">
                  <div className="relative flex-1">
                    <span className="absolute left-3 top-1/2 -translate-y-1/2 text-sm text-muted-foreground">$</span>
                    <input
                      type="number"
                      value={amountStr}
                      onChange={(e) => setAmountStr(e.target.value)}
                      placeholder="0.00"
                      min="0"
                      step="0.01"
                      className="w-full rounded-md border border-border bg-background py-2 pl-7 pr-3 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-primary/50"
                    />
                  </div>
                  {amountValid && (
                    <span className="whitespace-nowrap text-xs text-muted-foreground">
                      ≈ {epochsAdded.toFixed(1)} epochs each
                    </span>
                  )}
                </div>
                {totalBurnRate > 0 && (
                  <div className="mt-2 flex flex-wrap gap-1.5">
                    {[1, 4, 15, 90].map((ep) => {
                      const total = totalBurnRate * ep
                      const isOn = amountValid && Math.abs(amount - total) < 0.001
                      return (
                        <button
                          key={ep}
                          type="button"
                          onClick={() => setAmountStr(total.toFixed(2))}
                          className={`rounded-md border px-2.5 py-1 text-xs transition-colors ${
                            isOn
                              ? 'border-primary bg-primary/10 text-primary'
                              : 'border-border bg-background text-muted-foreground hover:border-muted-foreground/40 hover:text-foreground'
                          }`}
                        >
                          ${total.toLocaleString()} <span className="opacity-70">{ep} ep each</span>
                        </button>
                      )
                    })}
                  </div>
                )}
                {insufficientBalance && (
                  <p className="mt-1 text-xs text-red-400">Insufficient USDC balance</p>
                )}
              </div>

              {amountValid && (
                <div className="rounded-md border border-border bg-background p-3 text-[12.5px]">
                  <div className="mb-1 text-muted-foreground">Per-seat allocation</div>
                  <ul className="space-y-1">
                    {seats.map((s) => {
                      const a = allocations.get(s.pk) ?? 0n
                      return (
                        <li key={s.pk} className="flex justify-between">
                          <span className="font-mono">{s.device_code || s.device_key.slice(0, 10)}</span>
                          <span className="font-mono tabular-nums">${(Number(a) / 1e6).toFixed(2)}</span>
                        </li>
                      )
                    })}
                  </ul>
                  {chunks.length > 1 && (
                    <p className="mt-2 text-[11.5px] text-muted-foreground">
                      Will sign {chunks.length} transactions ({SEATS_PER_TX} seats per tx).
                    </p>
                  )}
                </div>
              )}

              <div className="flex justify-end gap-2 pt-1">
                <button
                  onClick={onClose}
                  className="rounded-md px-3 py-2 text-sm text-muted-foreground hover:bg-background hover:text-foreground"
                >
                  Cancel
                </button>
                <button
                  onClick={handleDeposit}
                  disabled={!canSubmit}
                  className="inline-flex items-center gap-1.5 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
                >
                  <Coins className="h-3.5 w-3.5" />
                  {amountValid ? `Sign & deposit $${amount.toFixed(2)}` : 'Sign & deposit'}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
