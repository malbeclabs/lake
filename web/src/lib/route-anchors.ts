// ponytail: throwaway scaffolding for metros DoubleZero does not yet serve.
// Delete an entry the moment its metro appears in dz_metros_current — the real
// metro then flows through the normal path and the anchor picker disappears
// with it. Nothing else in the page is coupled to this list.
//
// Rule: anchor where coverage is committed, N/A where it is not.

export type OffNetEndpoint = {
  id: string
  label: string
  note: string
  /** null means N/A — no substitution is offered. */
  defaultAnchor: string | null
  candidateAnchors: string[]
}

export const OFF_NET_ENDPOINTS: OffNetEndpoint[] = [
  {
    id: 'ohio',
    label: 'Ohio (AWS us-east-2)',
    note:
      'DoubleZero coverage in Ohio is in progress. Figures are measured at the on-ramp below. ' +
      'Your access leg from us-east-2 to the on-ramp applies equally to both the DoubleZero and ' +
      'public-internet path, so the difference between them is unaffected.',
    defaultAnchor: 'chi',
    candidateAnchors: ['chi', 'pit', 'nyc', 'was'],
  },
  {
    id: 'zurich',
    label: 'Zurich (ZH4)',
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

  const left = parseSide(segments[0])
  const right = parseSide(segments[1])
  if (!left || !right) return null

  return {
    from: left.id,
    to: right.id,
    ...(left.anchor ? { fromAnchor: left.anchor } : {}),
    ...(right.anchor ? { toAnchor: right.anchor } : {}),
  }
}

/** Splits one side of a route token (`id` or `id@anchor`); null if malformed. */
function parseSide(raw: string): { id: string; anchor?: string } | null {
  const parts = raw.split('@')
  if (parts.length > 2) return null
  const [id, anchor] = parts
  if (!id) return null
  if (parts.length === 2 && !anchor) return null
  return { id, ...(anchor ? { anchor } : {}) }
}

export function formatRouteToken(
  from: string,
  to: string,
  fromAnchor?: string,
  toAnchor?: string
): string {
  const left = fromAnchor ? `${from}@${fromAnchor}` : from
  const right = toAnchor ? `${to}@${toAnchor}` : to
  return `${left}-${right}`
}
