import type {
  ImpactSeverity,
  PlanImpactReport,
  MetroLatencyDelta,
  RedundancyChange,
  CapacityRisk,
  PlanOpType,
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

/** Biggest drop in independent paths first, then lowest remaining count. */
export function sortRedundancy(items: RedundancyChange[]): RedundancyChange[] {
  return [...items].sort((a, b) => {
    const dropA = a.before_paths - a.after_paths
    const dropB = b.before_paths - b.after_paths
    if (dropB !== dropA) return dropB - dropA
    return a.after_paths - b.after_paths
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

export function formatDeltaMs(us: number): string {
  const ms = us / 1000
  const sign = ms > 0 ? '+' : ''
  return `${sign}${ms.toFixed(1)}ms`
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
