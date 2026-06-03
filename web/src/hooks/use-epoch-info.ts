import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/lib/api'

export interface EpochInfo {
  epoch: number
  slotIndex: number
  slotsInEpoch: number
  progressPct: number
  remainingMs: number
}

async function fetchEpochInfo(): Promise<EpochInfo> {
  const res = await apiFetch('/api/solana/ledger')
  if (!res.ok) throw new Error(`ledger API error: ${res.status}`)
  const data = await res.json()
  return {
    epoch: data.epoch,
    slotIndex: data.slot_index,
    slotsInEpoch: data.slots_in_epoch,
    progressPct: data.epoch_pct,
    remainingMs: data.epoch_eta_sec * 1000,
  }
}

/**
 * Fetches current Solana epoch progress via the API (which proxies to mainnet RPC server-side).
 * Avoids CORS issues with direct browser-to-RPC calls.
 */
export function useEpochInfo(): { data: EpochInfo | null; isLoading: boolean } {
  const { data, isLoading } = useQuery({
    queryKey: ['epoch-info'],
    queryFn: fetchEpochInfo,
    refetchInterval: 30_000,
    staleTime: 20_000,
  })

  return { data: data ?? null, isLoading }
}
