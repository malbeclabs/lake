import { useCallback, useState } from 'react'
import { useConnection, useWallet } from '@solana/wallet-adapter-react'
import { Transaction, type TransactionInstruction } from '@solana/web3.js'

export type TransactionStatus =
  | 'idle'
  | 'building'
  | 'signing'
  | 'sending'
  | 'confirming'
  | 'confirmed'
  | 'simulating'
  | 'simulated'
  | 'error'

export interface UseShredTransactionResult {
  status: TransactionStatus
  txSignature: string | null
  error: string | null
  execute: (instructions: TransactionInstruction[]) => Promise<string | null>
  simulate: (instructions: TransactionInstruction[]) => Promise<void>
  reset: () => void
}

/**
 * Hook that manages the sign → send → confirm lifecycle for a Solana transaction.
 */
export function useShredTransaction(): UseShredTransactionResult {
  const { connection } = useConnection()
  const { publicKey: wallet, signTransaction } = useWallet()

  const [status, setStatus] = useState<TransactionStatus>('idle')
  const [txSignature, setTxSignature] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const reset = useCallback(() => {
    setStatus('idle')
    setTxSignature(null)
    setError(null)
  }, [])

  const execute = useCallback(
    async (instructions: TransactionInstruction[]): Promise<string | null> => {
      if (!wallet || !signTransaction) {
        setError('Wallet not connected')
        setStatus('error')
        return null
      }

      try {
        setStatus('building')
        setError(null)
        setTxSignature(null)

        const tx = new Transaction()
        tx.add(...instructions)

        const { blockhash, lastValidBlockHeight } =
          await connection.getLatestBlockhash('confirmed')
        tx.recentBlockhash = blockhash
        tx.feePayer = wallet
        tx.lastValidBlockHeight = lastValidBlockHeight

        setStatus('signing')
        const signed = await signTransaction(tx)

        setStatus('sending')
        const signature = await connection.sendRawTransaction(signed.serialize(), {
          skipPreflight: false,
          preflightCommitment: 'confirmed',
        })
        setTxSignature(signature)

        setStatus('confirming')
        const result = await connection.confirmTransaction(
          { signature, blockhash, lastValidBlockHeight },
          'confirmed',
        )

        if (result.value.err) {
          const errMsg =
            typeof result.value.err === 'string'
              ? result.value.err
              : JSON.stringify(result.value.err)
          setError(`Transaction failed: ${errMsg}`)
          setStatus('error')
          return null
        }

        setStatus('confirmed')
        return signature
      } catch (err: unknown) {
        const message =
          err instanceof Error ? err.message : 'Unknown error'

        // Wallet rejected
        if (
          message.includes('User rejected') ||
          message.includes('rejected the request')
        ) {
          setError('Transaction cancelled by user')
        } else {
          setError(message)
        }

        setStatus('error')
        return null
      }
    },
    [wallet, signTransaction, connection],
  )

  const simulate = useCallback(
    async (instructions: TransactionInstruction[]): Promise<void> => {
      if (!wallet) {
        setError('Wallet not connected')
        setStatus('error')
        return
      }

      try {
        setStatus('simulating')
        setError(null)
        setTxSignature(null)

        const tx = new Transaction()
        tx.add(...instructions)
        tx.feePayer = wallet
        // recentBlockhash intentionally omitted — simulateTransaction fetches it automatically
        // when no signers are passed, which also sets sigVerify: false on the RPC call

        const result = await connection.simulateTransaction(tx)

        if (result.value.err) {
          // Surface the most useful log line (last "Program log: Error" or the raw error)
          const logs = result.value.logs ?? []
          console.error('[simulate] err:', result.value.err)
          console.error('[simulate] logs:', logs)
          // Prefer "Program log: <msg>" lines — they contain the actual program error text.
          // Fall back to any line with Error/failed, then the raw RPC error object.
          const programLogErr = logs
            .filter((l: string) => l.startsWith('Program log:'))
            .reverse()
            .find((l: string) => /error|invalid|failed|closed/i.test(l))
          const fallbackErr = [...logs].reverse().find((l: string) => /Error|failed/.test(l))
          const errMsg = programLogErr
            ? programLogErr.replace(/^Program log:\s*/, '')
            : (fallbackErr ?? JSON.stringify(result.value.err))
          setError(errMsg)
          setStatus('error')
          return
        }

        setStatus('simulated')
      } catch (err: unknown) {
        setError(err instanceof Error ? err.message : 'Simulation error')
        setStatus('error')
      }
    },
    [wallet, connection],
  )

  return { status, txSignature, error, execute, simulate, reset }
}
