import type { ShredClientSeat } from '@/lib/api'

export type SeatStatus = 'active' | 'low' | 'pending' | 'expired'

export function balanceDollars(seat: ShredClientSeat): number {
  return seat.total_usdc_balance / 1e6
}

export function deriveStatus(seat: ShredClientSeat, currentEpoch: number): SeatStatus {
  const bal = balanceDollars(seat)
  const price = seat.price_per_epoch_dollars
  if (bal <= 0) return 'expired'
  if (seat.active_epoch > currentEpoch) return 'pending'
  if (price > 0 && bal < price) return 'low'
  return 'active'
}

/** Epochs of runway given current balance and per-epoch price. Infinity if free. */
export function runwayEpochs(seat: ShredClientSeat): number {
  if (seat.price_per_epoch_dollars <= 0) return Infinity
  return balanceDollars(seat) / seat.price_per_epoch_dollars
}

/** 0–100 progress for the runway bar, saturating at `capEpochs` epochs. */
export function runwayBarPct(seat: ShredClientSeat, capEpochs = 30): number {
  const epochs = runwayEpochs(seat)
  if (!Number.isFinite(epochs)) return 100
  return Math.min(100, (epochs / capEpochs) * 100)
}

export function barState(seat: ShredClientSeat): 'default' | 'low' | 'crit' {
  const e = runwayEpochs(seat)
  if (e < 1) return 'crit'
  if (e < 4) return 'low'
  return 'default'
}

export function statusPillFor(status: SeatStatus): { tone: 'green' | 'amber' | 'red' | 'blue'; label: string } {
  switch (status) {
    case 'active':  return { tone: 'green', label: 'Active' }
    case 'low':     return { tone: 'amber', label: 'Low escrow' }
    case 'pending': return { tone: 'blue',  label: 'Pending' }
    case 'expired': return { tone: 'red',   label: 'Expired' }
  }
}

/** Approx "May 28 · ep 444" funded-through label. */
export function formatFundedThrough(seat: ShredClientSeat, currentEpoch: number, msPerEpoch = MS_PER_EPOCH): string {
  const epochs = runwayEpochs(seat)
  if (!Number.isFinite(epochs)) return '—'
  const targetEpoch = seat.active_epoch + Math.floor(epochs)
  const epochsFromNow = targetEpoch - currentEpoch
  const date = new Date(Date.now() + epochsFromNow * msPerEpoch)
  const month = date.toLocaleDateString(undefined, { month: 'short' })
  const day = date.getDate()
  return `${month} ${day} · ep ${targetEpoch}`
}

/** Solana mainnet ~2 days per epoch. Used as a rough display estimate only. */
export const MS_PER_EPOCH = 2 * 24 * 60 * 60 * 1000

export function formatBurnRate(seat: ShredClientSeat): string {
  return `$${seat.price_per_epoch_dollars}`
}

/** Format a runway like "12 ep" or "0.5 ep" or "—". */
export function formatRunway(seat: ShredClientSeat): string {
  const e = runwayEpochs(seat)
  if (!Number.isFinite(e)) return '∞'
  if (e === 0) return '0 ep'
  if (e < 1) return `${e.toFixed(1)} ep`
  return `${Math.floor(e)} ep`
}

/** Format the inline progress caption: "$1,800 · 12 ep" or "$240 · pending" or "$0 · empty". */
export function formatRunwayCaption(seat: ShredClientSeat, status: SeatStatus): string {
  const bal = balanceDollars(seat)
  const balStr = `$${Math.round(bal).toLocaleString()}`
  if (status === 'pending') return `${balStr} · pending`
  if (status === 'expired') return `${balStr} · empty`
  return `${balStr} · ${formatRunway(seat)}`
}
