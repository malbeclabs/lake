import { useState, useCallback, useMemo } from 'react'
import { useWallet } from '@solana/wallet-adapter-react'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import { PublicKey } from '@solana/web3.js'
import { X, Loader2, Check, AlertCircle, ExternalLink, DollarSign } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import type { ShredClientSeat } from '@/lib/api'
import { ipv4ToU32 } from '@/lib/shred-program'
import { deriveShredAccounts, buildFundInstructions } from '@/lib/shred-transactions'
import { useUsdcBalance } from '@/hooks/use-shred-accounts'
import { useShredTransaction } from '@/hooks/use-shred-transaction'

interface ShredFundModalProps {
  seat: ShredClientSeat
  onClose: () => void
}

export function ShredFundModal({ seat, onClose }: ShredFundModalProps) {
  const { publicKey: wallet, connected } = useWallet()
  const { balance: usdcBalance } = useUsdcBalance()
  const { status, txSignature, error, execute, reset } = useShredTransaction()
  const queryClient = useQueryClient()

  const [amountStr, setAmountStr] = useState('')
  const amount = parseFloat(amountStr)
  const amountValid = !isNaN(amount) && amount > 0
  const amountMicro = amountValid ? BigInt(Math.floor(amount * 1_000_000)) : 0n
  const insufficientBalance = amountValid && amountMicro > usdcBalance

  const pricePerEpoch = seat.price_per_epoch_dollars
  const prepaidEpochs = pricePerEpoch > 0 && amountValid ? Math.floor(amount / pricePerEpoch) : 0
  const currentBalance = seat.total_usdc_balance / 1e6

  const canSubmit = connected && amountValid && !insufficientBalance && status === 'idle'

  const devicePubkey = useMemo(() => {
    try { return new PublicKey(seat.device_key) } catch { return null }
  }, [seat.device_key])

  const metroPubkey = useMemo(() => {
    try { return new PublicKey(seat.metro_pk) } catch { return null }
  }, [seat.metro_pk])

  const handleFund = useCallback(async () => {
    if (!canSubmit || !wallet || !devicePubkey || !metroPubkey) return

    const clientIpBits = ipv4ToU32(seat.client_ip)
    const accounts = deriveShredAccounts({
      device: devicePubkey,
      metroExchange: metroPubkey,
      clientIpBits,
      wallet,
    })

    const instructions = buildFundInstructions({
      accounts,
      wallet,
      amountMicro,
    })

    const sig = await execute(instructions)
    if (sig) {
      // Refresh seats data
      queryClient.invalidateQueries({ queryKey: ['shred-client-seats'] })
    }
  }, [canSubmit, wallet, devicePubkey, metroPubkey, seat.client_ip, amountMicro, execute, queryClient])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={status === 'idle' || status === 'confirmed' || status === 'error' ? onClose : undefined} />
      <div className="relative bg-card border border-border rounded-lg shadow-lg max-w-md w-full mx-4 p-6">
        <button
          onClick={onClose}
          className="absolute top-3 right-3 p-1 text-muted-foreground hover:text-foreground transition-colors"
        >
          <X className="h-4 w-4" />
        </button>

        <div className="flex items-center gap-2 mb-4">
          <DollarSign className="h-5 w-5 text-primary" />
          <h3 className="text-lg font-medium">Fund Subscription</h3>
        </div>

        {/* Seat info */}
        <div className="text-sm space-y-1.5 mb-5 p-3 rounded-lg bg-muted/50">
          <div className="flex justify-between">
            <span className="text-muted-foreground">Device</span>
            <span className="font-mono text-xs">{seat.device_code || seat.device_key.slice(0, 12)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Metro</span>
            <span>{seat.metro_code || '\u2014'}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Client IP</span>
            <span className="font-mono">{seat.client_ip}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Current Balance</span>
            <span className="tabular-nums">${currentBalance.toFixed(2)}</span>
          </div>
          {pricePerEpoch > 0 && (
            <div className="flex justify-between">
              <span className="text-muted-foreground">Price / Epoch</span>
              <span className="tabular-nums">${pricePerEpoch}</span>
            </div>
          )}
        </div>

        {status === 'confirmed' ? (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
              <Check className="h-5 w-5" />
              <span className="font-medium">Funded successfully!</span>
            </div>
            {txSignature && (
              <a
                href={`https://solscan.io/tx/${txSignature}`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-sm text-blue-500 hover:underline"
              >
                View on Solscan <ExternalLink className="h-3 w-3" />
              </a>
            )}
            <div className="flex justify-end pt-2">
              <button onClick={onClose} className="px-4 py-2 text-sm rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 transition-colors">
                Done
              </button>
            </div>
          </div>
        ) : status === 'error' ? (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-red-500">
              <AlertCircle className="h-5 w-5" />
              <span className="text-sm">{error}</span>
            </div>
            <button onClick={reset} className="text-sm text-primary hover:underline">
              Try again
            </button>
          </div>
        ) : status !== 'idle' ? (
          <div className="flex items-center justify-center gap-2 py-4">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
            <span className="text-sm text-muted-foreground">
              {status === 'building' && 'Building transaction...'}
              {status === 'signing' && 'Waiting for wallet...'}
              {status === 'sending' && 'Sending transaction...'}
              {status === 'confirming' && 'Confirming on-chain...'}
            </span>
          </div>
        ) : !connected ? (
          <div className="flex flex-col items-center gap-3 py-2">
            <p className="text-sm text-muted-foreground">Connect your wallet to fund this seat</p>
            <WalletMultiButton />
          </div>
        ) : (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1.5">Amount (USDC)</label>
              <div className="relative max-w-xs">
                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground text-sm">$</span>
                <input
                  type="number"
                  value={amountStr}
                  onChange={e => { setAmountStr(e.target.value); reset() }}
                  placeholder="0.00"
                  min="0"
                  step="0.01"
                  className="w-full pl-7 pr-3 py-2 text-sm border border-border rounded-lg bg-background focus:outline-none focus:ring-2 focus:ring-primary/50 tabular-nums"
                />
              </div>
              {connected && (
                <p className="text-xs text-muted-foreground mt-1">
                  Wallet balance: ${(Number(usdcBalance) / 1e6).toFixed(2)}
                </p>
              )}
              {insufficientBalance && (
                <p className="text-xs text-red-500 mt-1">Insufficient USDC balance</p>
              )}
              {amountValid && prepaidEpochs > 0 && (
                <p className="text-xs text-muted-foreground mt-1">
                  Adds ~{prepaidEpochs} epoch{prepaidEpochs !== 1 ? 's' : ''} at ${pricePerEpoch}/epoch
                </p>
              )}
            </div>

            <div className="flex justify-end gap-3">
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-muted rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleFund}
                disabled={!canSubmit}
                className="px-4 py-2 text-sm rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {amountValid ? `Fund $${amount.toFixed(2)} USDC` : 'Fund'}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
