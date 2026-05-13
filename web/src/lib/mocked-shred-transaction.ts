import { useState, useCallback } from 'react'
import type { UseShredTransactionResult, TransactionStatus } from '@/hooks/use-shred-transaction'

/**
 * Mocked replacement for `useShredTransaction()` used in preview mode for internal
 * (DoubleZero) users. Drives the same status sequence — building → signing →
 * sending → confirming → confirmed — without touching the wallet or RPC, so the
 * UI can be demoed end-to-end with sample data.
 *
 * Matches the result shape of the real hook so consumers can swap at render time:
 *
 *     const real = useShredTransaction()
 *     const mock = useMockedShredTransaction()
 *     const tx = preview ? mock : real
 */
export function useMockedShredTransaction(): UseShredTransactionResult {
  const [status, setStatus] = useState<TransactionStatus>('idle')
  const [txSignature, setTxSignature] = useState<string | null>(null)

  const reset = useCallback(() => {
    setStatus('idle')
    setTxSignature(null)
  }, [])

  const execute = useCallback(async (): Promise<string | null> => {
    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
    setTxSignature(null)
    setStatus('building');   await sleep(250)
    setStatus('signing');    await sleep(450)
    setStatus('sending');    await sleep(400)
    const sig = 'PreviewTx_NotReal_' + Date.now()
    setTxSignature(sig)
    setStatus('confirming'); await sleep(600)
    setStatus('confirmed')
    return sig
  }, [])

  const simulate = useCallback(async () => {
    setStatus('simulated')
  }, [])

  return { status, txSignature, error: null, execute, simulate, reset }
}
