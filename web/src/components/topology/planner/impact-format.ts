import type {
  ImpactSeverity,
  PlanImpactReport,
  MetroLatencyDelta,
  RedundancyChange,
  CapacityRisk,
  PlanOpType,
  ChangeRef,
} from '@/lib/api'

const SEVERITY_ORDER: Record<ImpactSeverity, number> = { high: 0, medium: 1, low: 2 }

export function severityRank(s: ImpactSeverity): number {
  return SEVERITY_ORDER[s] ?? 3
}

/** Worst-first: unreachable pairs, then largest slowdown, then severity. */
export function sortLatencyDeltas(items: MetroLatencyDelta[]): MetroLatencyDelta[] {
  return [...items].sort((a, b) => {
    const aUnreachable = a.after_us < 0
    const bUnreachable = b.after_us < 0
    if (aUnreachable !== bUnreachable) return aUnreachable ? -1 : 1
    if (b.delta_us !== a.delta_us) return b.delta_us - a.delta_us
    return severityRank(a.severity) - severityRank(b.severity)
  })
}

/** A row in the grouped latency-delta list: either one metro pair, or a
 *  collapsed summary of several pairs that share a common endpoint metro and
 *  the same added latency (e.g. "13 metros +10.0ms to fra"). */
export interface LatencyDeltaGroup {
  /** Stable key for React lists. */
  key: string
  /** The endpoint metro shared by every member. For a lone pair (no grouping
   *  applied) this is just one side of that pair, picked so the panel can
   *  render `otherMetros[0] -> commonMetro` in the original metro_a/metro_z
   *  order. */
  commonMetro: string
  /** The "other" endpoint metro for each member, aligned 1:1 with `members`. */
  otherMetros: string[]
  /** Canonical bucketed delta_us for the group (rounded to the same 0.1ms
   *  granularity it displays at), null when `unreachable`. Grouping and the
   *  shown ms figure both derive from this one value, so two pairs display
   *  the same figure iff they share a bucket -- no boundary disagreement. */
  deltaUs: number | null
  /** True when every member has no path (after_us < 0). Unreachable pairs
   *  are always their own singleton group -- they're a different category
   *  from a latency slowdown and are never merged with each other. */
  unreachable: boolean
  /** Worst (most severe) severity across all members. */
  severity: ImpactSeverity
  /** Union of every member's caused_by refs, de-duplicated by seq. */
  causedBy: ChangeRef[] | null
  /** The original pairs that make up this group, unmodified. */
  members: MetroLatencyDelta[]
}

// Rounds to the nearest 0.1ms (100us), the same precision formatDeltaMs
// displays at (toFixed(1)). The group's shown ms figure is ALWAYS
// formatDeltaMs(bucketUs) -- never a raw member's delta_us -- so the display
// can't disagree with the bucketing at an x.x5ms boundary, and two pairs
// display the same figure iff they land in the same bucket.
const DELTA_BUCKET_US = 100

function bucketDelta(deltaUs: number): number {
  return Math.round(deltaUs / DELTA_BUCKET_US) * DELTA_BUCKET_US
}

function worstSeverity(items: { severity: ImpactSeverity }[]): ImpactSeverity {
  let worst: ImpactSeverity = items[0]?.severity ?? 'low'
  for (const item of items) {
    if (severityRank(item.severity) < severityRank(worst)) worst = item.severity
  }
  return worst
}

function mergeCausedBy(items: { caused_by: ChangeRef[] | null }[]): ChangeRef[] | null {
  const seenSeqs = new Set<number>()
  const merged: ChangeRef[] = []
  for (const item of items) {
    if (!item.caused_by) continue
    for (const ref of item.caused_by) {
      if (seenSeqs.has(ref.seq)) continue
      seenSeqs.add(ref.seq)
      merged.push(ref)
    }
  }
  return merged.length > 0 ? merged : null
}

function singletonGroup(d: MetroLatencyDelta, keyPrefix: string): LatencyDeltaGroup {
  const unreachable = d.after_us < 0
  return {
    key: `${keyPrefix}-${d.metro_a}-${d.metro_z}-${d.delta_us}`,
    // Pick metro_z as the "common" side so the panel's fixed render order
    // (otherMetros[0] -> commonMetro) reproduces the original metro_a ->
    // metro_z pair order for an ungrouped row.
    commonMetro: d.metro_z,
    otherMetros: [d.metro_a],
    // Bucketed (not raw) so a lone row's shown figure is consistent with any
    // grouped row it would visually match.
    deltaUs: unreachable ? null : bucketDelta(d.delta_us),
    unreachable,
    severity: d.severity,
    causedBy: mergeCausedBy([d]),
    members: [d],
  }
}

function sortGroups(groups: LatencyDeltaGroup[]): LatencyDeltaGroup[] {
  return [...groups].sort((a, b) => {
    if (a.unreachable !== b.unreachable) return a.unreachable ? -1 : 1
    const aDelta = a.deltaUs ?? 0
    const bDelta = b.deltaUs ?? 0
    if (bDelta !== aDelta) return bDelta - aDelta
    return severityRank(a.severity) - severityRank(b.severity)
  })
}

/** One (common metro, delta bucket) grouping candidate a reachable pair
 *  could join, from one endpoint's perspective. */
interface Candidate {
  common: string
  bucket: number
  items: MetroLatencyDelta[]
}

/**
 * Group metro-pair latency deltas so pairs that share a common endpoint
 * metro AND have the same (rounded) added latency collapse into one summary
 * row, e.g. "13 metros +10.0ms to fra", instead of 13 near-identical rows.
 *
 * Unreachable pairs (after_us < 0, "no path") are a different category and
 * are never bucketed into a latency-delta group -- each stays its own
 * singleton group, listed as-is.
 *
 * A latency pair is undirected, so for each reachable pair either endpoint
 * could be the "common" metro of a group. We build every candidate group
 * (one per endpoint per pair) and greedily keep the largest candidates
 * first, so the collapse hides as many rows as possible. Anything left
 * over (including every candidate that never reached 2 members) renders as
 * its own single-row group, exactly like before grouping existed.
 */
export function groupLatencyDeltas(deltas: MetroLatencyDelta[]): LatencyDeltaGroup[] {
  const groups: LatencyDeltaGroup[] = []
  const reachable: MetroLatencyDelta[] = []

  for (const d of deltas) {
    if (d.after_us < 0) {
      groups.push(singletonGroup(d, 'unreachable'))
    } else {
      reachable.push(d)
    }
  }

  // Every reachable pair contributes two candidates: one with metro_a as
  // the common endpoint, one with metro_z. Keyed by object identity (not a
  // joined string) so a metro code can never collide with the bucket value.
  const candidates = new Map<string, Candidate>()
  for (const d of reachable) {
    const bucket = bucketDelta(d.delta_us)
    for (const common of [d.metro_a, d.metro_z]) {
      const mapKey = JSON.stringify([common, bucket])
      const existing = candidates.get(mapKey)
      if (existing) existing.items.push(d)
      else candidates.set(mapKey, { common, bucket, items: [d] })
    }
  }

  const orderedCandidates = [...candidates.entries()].sort((a, b) => {
    if (b[1].items.length !== a[1].items.length) return b[1].items.length - a[1].items.length
    return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0
  })

  const used = new Set<MetroLatencyDelta>()
  for (const [mapKey, candidate] of orderedCandidates) {
    const remaining = candidate.items.filter((d) => !used.has(d))
    if (remaining.length < 2) continue
    const common = candidate.common
    for (const d of remaining) used.add(d)
    groups.push({
      key: `delta-${mapKey}`,
      commonMetro: common,
      otherMetros: remaining.map((d) => (d.metro_a === common ? d.metro_z : d.metro_a)),
      // The canonical bucket the members were grouped by -- the shown ms
      // figure derives from this, never a raw member's delta_us.
      deltaUs: candidate.bucket,
      unreachable: false,
      severity: worstSeverity(remaining),
      causedBy: mergeCausedBy(remaining),
      members: remaining,
    })
  }

  for (const d of reachable) {
    if (used.has(d)) continue
    groups.push(singletonGroup(d, 'single'))
  }

  return sortGroups(groups)
}

/** Splits latency improvements into the two visual kinds: a faster path
 *  (`before_us >= 0`, rendered via `groupLatencyDeltas` alongside its negative
 *  delta) and a newly-reachable pair (`before_us < 0`, no prior path so there
 *  is no meaningful delta to show). */
export function splitLatencyImprovements(items: MetroLatencyDelta[]): {
  reductions: MetroLatencyDelta[]
  newlyReachable: MetroLatencyDelta[]
} {
  const reductions: MetroLatencyDelta[] = []
  const newlyReachable: MetroLatencyDelta[] = []
  for (const item of items) {
    if (item.before_us < 0) newlyReachable.push(item)
    else reductions.push(item)
  }
  return { reductions, newlyReachable }
}

/** Biggest drop in independent paths first, then lowest remaining count. */
export function sortRedundancy(items: RedundancyChange[]): RedundancyChange[] {
  return [...items].sort((a, b) => {
    const dropA = a.before_paths - a.after_paths
    const dropB = b.before_paths - b.after_paths
    if (dropB !== dropA) return dropB - dropA
    return a.after_paths - b.after_paths
  })
}

/** Biggest gain in independent paths first, then metro codes. */
export function sortRedundancyImprovements(items: RedundancyChange[]): RedundancyChange[] {
  return [...items].sort((a, b) => {
    const gainA = a.after_paths - a.before_paths
    const gainB = b.after_paths - b.before_paths
    if (gainB !== gainA) return gainB - gainA
    if (a.metro_a !== b.metro_a) return a.metro_a < b.metro_a ? -1 : 1
    return a.metro_z < b.metro_z ? -1 : a.metro_z > b.metro_z ? 1 : 0
  })
}

/** Most severe first (v1 capacity risk carries no numeric load). */
export function sortCapacityRisks(items: CapacityRisk[]): CapacityRisk[] {
  return [...items].sort((a, b) => severityRank(a.severity) - severityRank(b.severity))
}

export interface ImpactSeverityCounts {
  high: number
  medium: number
  low: number
  total: number
}

export function countBySeverity(report: PlanImpactReport): ImpactSeverityCounts {
  const counts: ImpactSeverityCounts = { high: 0, medium: 0, low: 0, total: 0 }
  const all: { severity: ImpactSeverity }[] = [
    ...report.partition_issues,
    ...report.latency_deltas,
    ...report.redundancy_changes,
    ...report.capacity_risks,
    ...report.overlap_warnings,
  ]
  for (const f of all) {
    counts.total++
    if (f.severity === 'high') counts.high++
    else if (f.severity === 'medium') counts.medium++
    else counts.low++
  }
  return counts
}

export function hasAnyImpact(report: PlanImpactReport): boolean {
  return countBySeverity(report).total > 0
}

/** Plain (unsigned) microsecond -> millisecond display, e.g. `4100` -> `"4.1ms"`.
 *  Internal values stay in µs/ns everywhere else; this is a DISPLAY-only
 *  conversion. `formatDeltaMs` below reuses this for the signed delta case. */
export function formatMs(us: number): string {
  return `${(us / 1000).toFixed(1)}ms`
}

export function formatDeltaMs(us: number): string {
  const ms = us / 1000
  const sign = ms > 0 ? '+' : ''
  return `${sign}${formatMs(us)}`
}

const OP_LABEL: Record<PlanOpType, string> = {
  add_device: 'Add device',
  remove_device: 'Remove device',
  add_link: 'Add link',
  remove_link: 'Remove link',
  move_link_end: 'Move link end',
}

// Op-aware label keys: link ops read the LINK identity, device ops the DEVICE
// identity. This mirrors changeSummary in change-label.ts so a move_link_end
// change shows the link being moved, not just its new device.
const LINK_LABEL_KEYS = ['link_code', 'code', 'link_label', 'label']
const DEVICE_LABEL_KEYS = ['device_code', 'code', 'label']

export interface ChangeLabelInput {
  seq: number
  op_type: PlanOpType
  ref_snapshot?: Record<string, unknown> | null
}

function snapshotLabel(
  snap: Record<string, unknown> | null | undefined,
  keys: string[],
): string {
  if (!snap) return ''
  for (const key of keys) {
    const v = snap[key]
    if (typeof v === 'string' && v.length > 0) return v
  }
  return ''
}

/**
 * A compact, human label for a change, e.g. "#20 Remove link chi-nyc-1".
 * Op-aware: link ops show the link identity (not the device), matching the
 * changeSummary convention in change-label.ts; move_link_end shows both the
 * link being moved and its new target device.
 */
export function changeShortLabel(c: ChangeLabelInput): string {
  const prefix = `#${c.seq} `
  const snap = c.ref_snapshot
  switch (c.op_type) {
    case 'move_link_end': {
      const link = snapshotLabel(snap, LINK_LABEL_KEYS)
      const device = snapshotLabel(snap, ['device_code'])
      if (link && device) return `${prefix}Move link ${link} → ${device}`
      if (link) return `${prefix}Move link ${link}`
      if (device) return `${prefix}Move link end → ${device}`
      return `${prefix}Move link end`
    }
    case 'add_link':
    case 'remove_link': {
      const link = snapshotLabel(snap, LINK_LABEL_KEYS)
      return `${prefix}${OP_LABEL[c.op_type]}${link ? ` ${link}` : ''}`
    }
    case 'add_device':
    case 'remove_device': {
      const device = snapshotLabel(snap, DEVICE_LABEL_KEYS)
      return `${prefix}${OP_LABEL[c.op_type]}${device ? ` ${device}` : ''}`
    }
    default:
      return `${prefix}${OP_LABEL[c.op_type] ?? c.op_type}`
  }
}
