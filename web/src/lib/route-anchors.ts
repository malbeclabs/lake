// ponytail: throwaway scaffolding for metros DoubleZero does not yet serve.
// Delete an entry the moment its metro appears in dz_metros_current — the real
// metro then flows through the normal path and the anchor picker disappears
// with it. Nothing else in the page is coupled to this list.
//
// Rule: anchor where coverage is committed, N/A where it is not.

export type OffNetEndpoint = {
  id: string
  label: string
  /** Axis label in the matrix, where a metro contributes its three-letter code. */
  short: string
  note: string
  /** null means N/A — no substitution is offered. */
  defaultAnchor: string | null
  candidateAnchors: string[]
}

export const OFF_NET_ENDPOINTS: OffNetEndpoint[] = [
  {
    id: 'ohio',
    label: 'Ohio (AWS us-east-2)',
    short: 'OHIO',
    note:
      'DoubleZero coverage in Ohio is in progress. Figures are measured at the on-ramp shown. ' +
      'Your access leg from us-east-2 to the on-ramp applies equally to both the DoubleZero and ' +
      'public-internet path, so the difference between them is unaffected.',
    defaultAnchor: 'chi',
    candidateAnchors: ['chi', 'pit', 'nyc', 'was'],
  },
  {
    id: 'zurich',
    label: 'Zurich (ZH4)',
    short: 'ZRH',
    note: 'DoubleZero has no presence in Zurich, so there is no figure to report.',
    defaultAnchor: null,
    candidateAnchors: [],
  },
]

export function resolveEndpoint(
  id: string,
  anchor?: string
): { metroCode: string | null; offNet: OffNetEndpoint | null; anchor: string | null } {
  const offNet = OFF_NET_ENDPOINTS.find((e) => e.id === id)
  if (!offNet) {
    return { metroCode: id, offNet: null, anchor: null }
  }
  const chosen =
    anchor && offNet.candidateAnchors.includes(anchor) ? anchor : offNet.defaultAnchor
  return { metroCode: chosen, offNet, anchor: chosen }
}

/**
 * Parses `tyo-lon` or `ohio@pit-lon` into its endpoints and optional anchors.
 *
 * This backs a shareable URL, so a corrupted or hand-edited token must fail
 * visibly rather than parse into a plausible-but-wrong route. Returns `null`
 * for anything malformed — callers must drop `null` results, not coerce them
 * (e.g. with `?? ''`).
 */
export function parseRouteToken(token: string): {
  from: string
  to: string
  fromAnchor?: string
  toAnchor?: string
} | null {
  if (!token.trim()) return null

  const segments = token.split('-')
  if (segments.length !== 2) return null

  const left = parseCityToken(segments[0])
  const right = parseCityToken(segments[1])
  if (!left || !right) return null

  return {
    from: left.id,
    to: right.id,
    ...(left.anchor ? { fromAnchor: left.anchor } : {}),
    ...(right.anchor ? { toAnchor: right.anchor } : {}),
  }
}

/**
 * Parses one location token — `lon`, or `ohio@chi` carrying its anchor.
 *
 * Also one side of a route token, so the `?cities=` list and the `?routes=`
 * encoding share a single rule: anything malformed returns `null` and must be
 * dropped by the caller, never coerced into a plausible-but-wrong location.
 *
 * Case-folded, because every id lookup downstream is case-sensitive — the
 * off-net table here, the metro table on the page. A link an inbox uppercased to
 * `?cities=ZURICH` would otherwise pass for an unknown metro code and print a
 * whole row of "no path", asserting DoubleZero cannot reach somewhere it has
 * only ever said it does not serve. That is the same coercion this function's
 * strictness exists to prevent, arriving through casing instead of syntax.
 */
export function parseCityToken(raw: string): { id: string; anchor?: string } | null {
  const parts = raw.trim().toLowerCase().split('@')
  if (parts.length > 2) return null
  const [id, anchor] = parts
  if (!id) return null
  if (parts.length === 2 && !anchor) return null
  return { id, ...(anchor ? { anchor } : {}) }
}

export function formatCityToken(id: string, anchor?: string): string {
  return anchor ? `${id}@${anchor}` : id
}

export function formatRouteToken(
  from: string,
  to: string,
  fromAnchor?: string,
  toAnchor?: string
): string {
  return `${formatCityToken(from, fromAnchor)}-${formatCityToken(to, toAnchor)}`
}
