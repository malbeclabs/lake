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
  | 'error'

export interface UseShredTransactionResult {
  status: TransactionStatus
  txSignature: string | null
  error: string | null
  execute: (instructions: TransactionInstruction[]) => Promise<string | null>
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

  return { status, txSignature, error, execute, reset }
}
