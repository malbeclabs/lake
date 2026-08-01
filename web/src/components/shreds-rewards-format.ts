// Shared formatting helpers for the Edge Rewards pages.

// formatTokenAmount renders a reward amount already scaled to whole tokens (the
// API divides on-chain base units by the token's decimals) and appends the
// token symbol. From epoch 968 validators may be rewarded in 2Z, USDC, or wSOL,
// so the symbol must travel with the amount rather than being assumed to be 2Z.
export function formatTokenAmount(amount: number, symbol: string): string {
  const sym = symbol || '2Z'
  if (!Number.isFinite(amount) || amount <= 0) return '—'
  if (amount >= 1_000_000)
    return `${(amount / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M ${sym}`
  if (amount >= 10_000)
    return `${(amount / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}K ${sym}`
  return `${amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${sym}`
}

// format2Z formats a whole-2Z amount. Thin wrapper for the 2Z-only call sites
// (e.g. the list page's headline 2Z totals).
export function format2Z(amount: number): string {
  return formatTokenAmount(amount, '2Z')
}
