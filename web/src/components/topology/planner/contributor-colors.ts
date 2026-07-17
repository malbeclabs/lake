import type { DraftDevice, DraftLink } from './draft'

// Same 12-color palette as the main topology map (components/topology-map.tsx CONTRIBUTOR_COLORS).
export const CONTRIBUTOR_COLORS = [
  '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f59e0b', '#6366f1',
  '#14b8a6', '#f97316', '#a855f7', '#10b981', '#ef4444', '#0ea5e9',
]

export interface ContributorColorInfo {
  pk: string
  code: string
  color: string
  deviceCount: number
  linkCount: number
}

export interface ContributorColors {
  colorByPk: Map<string, string>
  // Legend rows, sorted by deviceCount desc then code asc.
  contributors: ContributorColorInfo[]
}

// A link's owning contributor: contributor_pk, falling back to side_a_contributor_pk
// when contributor_pk is empty.
function linkOwnerPk(link: DraftLink): string {
  return link.contributor_pk || link.side_a_contributor_pk
}

// Build a stable contributor->color assignment over a draft's devices + links.
// Color index is assigned by contributor code sorted ascending (so color is stable
// regardless of device/link counts); the returned legend list is sorted by device
// count desc (then code) for display. A device's contributor is device.contributor_pk;
// a link's owning contributor is link.contributor_pk, falling back to
// link.side_a_contributor_pk when contributor_pk is empty.
export function buildContributorColors(devices: DraftDevice[], links: DraftLink[]): ContributorColors {
  const codeByPk = new Map<string, string>()
  const deviceCountByPk = new Map<string, number>()
  const linkCountByPk = new Map<string, number>()

  const noteCode = (pk: string, code: string) => {
    if (!pk) return
    // Keep the first non-empty code seen for this pk; never overwrite one already known.
    if (!codeByPk.get(pk)) codeByPk.set(pk, code)
  }

  for (const d of devices) {
    noteCode(d.contributor_pk, d.contributor_code)
    if (d.contributor_pk) {
      deviceCountByPk.set(d.contributor_pk, (deviceCountByPk.get(d.contributor_pk) ?? 0) + 1)
    }
  }

  for (const l of links) {
    noteCode(l.contributor_pk, l.contributor_code)
    noteCode(l.side_a_contributor_pk, l.side_a_contributor_code)
    noteCode(l.side_z_contributor_pk, l.side_z_contributor_code)
    const ownerPk = linkOwnerPk(l)
    if (ownerPk) {
      linkCountByPk.set(ownerPk, (linkCountByPk.get(ownerPk) ?? 0) + 1)
    }
  }

  const pks = [...codeByPk.keys()].sort((a, b) => {
    const codeCmp = (codeByPk.get(a) ?? '').localeCompare(codeByPk.get(b) ?? '')
    return codeCmp !== 0 ? codeCmp : a.localeCompare(b)
  })

  const colorByPk = new Map<string, string>()
  pks.forEach((pk, i) => {
    colorByPk.set(pk, CONTRIBUTOR_COLORS[i % CONTRIBUTOR_COLORS.length])
  })

  const contributors: ContributorColorInfo[] = pks
    .map((pk) => ({
      pk,
      code: codeByPk.get(pk) ?? '',
      color: colorByPk.get(pk) ?? CONTRIBUTOR_COLORS[0],
      deviceCount: deviceCountByPk.get(pk) ?? 0,
      linkCount: linkCountByPk.get(pk) ?? 0,
    }))
    .sort((a, b) => {
      const countCmp = b.deviceCount - a.deviceCount
      return countCmp !== 0 ? countCmp : a.code.localeCompare(b.code)
    })

  return { colorByPk, contributors }
}
