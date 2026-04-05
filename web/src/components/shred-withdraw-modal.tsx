import { useCallback, useMemo } from 'react'
import { useWallet } from '@solana/wallet-adapter-react'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import { PublicKey } from '@solana/web3.js'
import { X, Loader2, Check, AlertCircle, ExternalLink, LogOut } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import type { ShredClientSeat } from '@/lib/api'
import { ipv4ToU32 } from '@/lib/shred-program'
import { deriveShredAccounts, buildUnsubscribeInstructions } from '@/lib/shred-transactions'
import { useShredAccounts } from '@/hooks/use-shred-accounts'
import { useShredTransaction } from '@/hooks/use-shred-transaction'

interface ShredWithdrawModalProps {
  seat: ShredClientSeat
  onClose: () => void
}

export function ShredWithdrawModal({ seat, onClose }: ShredWithdrawModalProps) {
  const { publicKey: wallet, connected } = useWallet()
  const { status, txSignature, error, execute, reset } = useShredTransaction()
  const queryClient = useQueryClient()

  const currentBalance = seat.total_usdc_balance / 1e6

  const devicePubkey = useMemo(() => {
    try { return new PublicKey(seat.device_key) } catch { return null }
  }, [seat.device_key])

  const metroPubkey = useMemo(() => {
    try { return new PublicKey(seat.metro_pk) } catch { return null }
  }, [seat.metro_pk])

  // Fetch on-chain state to know if escrow exists
  const shredState = useShredAccounts(devicePubkey, seat.client_ip)

  const canSubmit = connected && status === 'idle'

  const handleWithdraw = useCallback(async () => {
    if (!canSubmit || !wallet || !devicePubkey || !metroPubkey) return

    const clientIpBits = ipv4ToU32(seat.client_ip)
    const accounts = deriveShredAccounts({
      device: devicePubkey,
      metroExchange: metroPubkey,
      clientIpBits,
      wallet,
    })

    const instructions = buildUnsubscribeInstructions({
      accounts,
      wallet,
      escrowExists: shredState.escrowExists,
    })

    const sig = await execute(instructions)
    if (sig) {
      queryClient.invalidateQueries({ queryKey: ['shred-client-seats'] })
    }
  }, [canSubmit, wallet, devicePubkey, metroPubkey, seat.client_ip, shredState.escrowExists, execute, queryClient])

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
          <LogOut className="h-5 w-5 text-red-500" />
          <h3 className="text-lg font-medium">Unsubscribe & Withdraw</h3>
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
            <span className="text-muted-foreground">Balance to withdraw</span>
            <span className="tabular-nums font-medium">${currentBalance.toFixed(2)} USDC</span>
          </div>
        </div>

        {status === 'confirmed' ? (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
              <Check className="h-5 w-5" />
              <span className="font-medium">Unsubscribed successfully!</span>
            </div>
            {currentBalance > 0 && (
              <p className="text-sm text-muted-foreground">
                ${currentBalance.toFixed(2)} USDC has been returned to your wallet.
              </p>
            )}
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
            <p className="text-sm text-muted-foreground">Connect your wallet to unsubscribe</p>
            <WalletMultiButton />
          </div>
        ) : (
          <div className="space-y-4">
            <div className="flex items-start gap-2 text-sm px-3 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-600 dark:text-amber-400">
              <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
              <span>
                This will end your shred delivery for this device + IP and close the payment escrow.
                {currentBalance > 0
                  ? ` $${currentBalance.toFixed(2)} USDC will be refunded to your wallet.`
                  : ''}
              </span>
            </div>

            <div className="flex justify-end gap-3">
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-muted rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleWithdraw}
                disabled={!canSubmit}
                className="px-4 py-2 text-sm rounded-lg bg-red-600 text-white hover:bg-red-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Unsubscribe{currentBalance > 0 ? ` & Withdraw $${currentBalance.toFixed(2)}` : ''}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
