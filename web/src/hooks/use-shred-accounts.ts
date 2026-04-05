import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useConnection, useWallet } from '@solana/wallet-adapter-react'
import { PublicKey } from '@solana/web3.js'
import {
  findClientSeatAddress,
  findPaymentEscrowAddress,
  ipv4ToU32,
  parseClientSeat,
  parsePaymentEscrow,
  USDC_MINT_MAINNET,
  SPL_TOKEN_PROGRAM_ID,
} from '@/lib/shred-program'

const ASSOCIATED_TOKEN_PROGRAM_ID = new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL')

function getAssociatedTokenAddressSync(mint: PublicKey, owner: PublicKey): PublicKey {
  const [ata] = PublicKey.findProgramAddressSync(
    [owner.toBytes(), SPL_TOKEN_PROGRAM_ID.toBytes(), mint.toBytes()],
    ASSOCIATED_TOKEN_PROGRAM_ID,
  )
  return ata
}

export interface ShredAccountState {
  seatExists: boolean
  escrowExists: boolean
  tenureEpochs: number
  activeEpoch: bigint
  escrowCount: number
  escrowBalance: bigint
  seatActive: boolean
  clientSeatKey: PublicKey | null
  paymentEscrowKey: PublicKey | null
}

const EMPTY_STATE: ShredAccountState = {
  seatExists: false,
  escrowExists: false,
  tenureEpochs: 0,
  activeEpoch: 0n,
  escrowCount: 0,
  escrowBalance: 0n,
  seatActive: false,
  clientSeatKey: null,
  paymentEscrowKey: null,
}

/**
 * Fetches on-chain state for a shred client seat and payment escrow.
 * Returns whether they exist, tenure, balance, etc.
 */
export function useShredAccounts(
  device: PublicKey | null,
  clientIp: string | null,
) {
  const { connection } = useConnection()
  const { publicKey: wallet } = useWallet()

  const derived = useMemo(() => {
    if (!device || !clientIp || !wallet) return null
    try {
      const ipBits = ipv4ToU32(clientIp)
      const [seatKey] = findClientSeatAddress(device, ipBits)
      const [escrowKey] = findPaymentEscrowAddress(seatKey, wallet)
      return { seatKey, escrowKey }
    } catch {
      return null
    }
  }, [device, clientIp, wallet])

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['shred-accounts', derived?.seatKey?.toBase58(), derived?.escrowKey?.toBase58()],
    queryFn: async (): Promise<ShredAccountState> => {
      if (!derived) return EMPTY_STATE
      const accounts = await connection.getMultipleAccountsInfo([
        derived.seatKey,
        derived.escrowKey,
      ])

      const seatData = accounts[0]?.data
        ? new Uint8Array(accounts[0].data)
        : null
      const escrowData = accounts[1]?.data
        ? new Uint8Array(accounts[1].data)
        : null

      const seat = seatData ? parseClientSeat(seatData) : null
      const escrow = escrowData ? parsePaymentEscrow(escrowData) : null

      return {
        seatExists: seat !== null,
        escrowExists: escrow !== null,
        tenureEpochs: seat?.tenureEpochs ?? 0,
        activeEpoch: seat?.activeEpoch ?? 0n,
        escrowCount: seat?.escrowCount ?? 0,
        escrowBalance: escrow?.usdcBalance ?? 0n,
        seatActive: (seat?.tenureEpochs ?? 0) > 0,
        clientSeatKey: derived.seatKey,
        paymentEscrowKey: derived.escrowKey,
      }
    },
    enabled: derived !== null,
    refetchInterval: 10_000,
    staleTime: 5_000,
  })

  return {
    ...(data ?? EMPTY_STATE),
    isLoading: derived !== null && isLoading,
    refetch,
  }
}

/**
 * Fetches the user's USDC token balance.
 */
export function useUsdcBalance() {
  const { connection } = useConnection()
  const { publicKey: wallet } = useWallet()

  const { data: balance, isLoading } = useQuery({
    queryKey: ['usdc-balance', wallet?.toBase58()],
    queryFn: async (): Promise<bigint> => {
      if (!wallet) return 0n
      try {
        const ata = getAssociatedTokenAddressSync(USDC_MINT_MAINNET, wallet)
        const account = await connection.getTokenAccountBalance(ata)
        return BigInt(account.value.amount)
      } catch {
        return 0n
      }
    },
    enabled: wallet !== null,
    refetchInterval: 15_000,
    staleTime: 10_000,
  })

  return { balance: balance ?? 0n, isLoading }
}
