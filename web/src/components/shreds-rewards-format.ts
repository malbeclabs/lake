// Shared formatting helpers for the Edge Rewards pages.

// The 2Z mint has 8 decimals (DOUBLEZERO_MINT_DECIMALS), so on-chain amounts
// arrive in base units where 1 whole 2Z = 10^8 base units. Scale to whole 2Z
// before display.
const TWO_Z_UNITS_PER_TOKEN = 100_000_000

export function format2Z(baseUnits: number): string {
  if (!Number.isFinite(baseUnits) || baseUnits <= 0) return '—'
  const amount = baseUnits / TWO_Z_UNITS_PER_TOKEN
  if (amount >= 1_000_000)
    return `${(amount / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M 2Z`
  if (amount >= 10_000)
    return `${(amount / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}K 2Z`
  return `${amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 2Z`
}
