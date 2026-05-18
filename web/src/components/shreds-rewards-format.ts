// Shared formatting helpers for the Edge Rewards pages.

export function format2Z(amount: number): string {
  if (!Number.isFinite(amount) || amount <= 0) return '—'
  if (amount >= 1_000_000)
    return `${(amount / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M 2Z`
  if (amount >= 10_000)
    return `${(amount / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}K 2Z`
  return `${amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 2Z`
}
