// Pure derivation of a link's type (WAN/DZX) from its two endpoints. See the
// DoubleZero WAN/DZX rule: WAN = same contributor + different metros; DZX =
// different contributors + same metro (the exchange). Same contributor + same
// metro is allowed but ambiguous (operator picks). Different contributor +
// different metro is invalid.

export type LinkTypeChoice = 'WAN' | 'DZX'

export interface LinkTypeDerivation {
  // 'WAN' or 'DZX' when the pair unambiguously determines the type; null when the
  // operator must choose (same contributor + same metro).
  type: LinkTypeChoice | null
  valid: boolean
  // Present when !valid: a short, plain reason to show the operator.
  reason?: string
  // Present when type === null: the two allowed choices + a default.
  ambiguous?: boolean
}

// endpoint = one link end resolved from the draft: its owning contributor and its metro.
// Use the draft device's contributor_pk (fallback contributor_code) and metro_pk. A
// blank contributor or metro means "unknown"; treat unknown as NOT matching (so a pair
// with an unknown side is invalid rather than silently allowed).
export interface LinkEndpoint {
  contributorKey: string
  metroKey: string
}

export function deriveLinkType(a: LinkEndpoint, z: LinkEndpoint): LinkTypeDerivation {
  if (!a.contributorKey || !a.metroKey || !z.contributorKey || !z.metroKey) {
    return {
      valid: false,
      type: null,
      reason: 'Cannot determine link type: an endpoint is missing its contributor or metro.',
    }
  }

  const sameContrib = a.contributorKey === z.contributorKey
  const sameMetro = a.metroKey === z.metroKey

  if (sameContrib && !sameMetro) {
    return { valid: true, type: 'WAN' }
  }
  if (!sameContrib && sameMetro) {
    return { valid: true, type: 'DZX' }
  }
  if (sameContrib && sameMetro) {
    return { valid: true, type: null, ambiguous: true }
  }
  return {
    valid: false,
    type: null,
    reason:
      'A cross-contributor link must be within one metro (DZX); a cross-metro link must be owned by one contributor (WAN). This pair is neither.',
  }
}
