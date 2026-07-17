import { describe, it, expect } from 'vitest'
import type {
  PlanImpactReport,
  MetroLatencyDelta,
  RedundancyChange,
  CapacityRisk,
} from '@/lib/api'
import {
  severityRank,
  sortLatencyDeltas,
  sortRedundancy,
  sortCapacityRisks,
  countBySeverity,
  hasAnyImpact,
  formatDeltaMs,
  changeShortLabel,
} from './impact-format'

const emptyReport: PlanImpactReport = {
  partition_issues: [],
  latency_deltas: [],
  redundancy_changes: [],
  capacity_risks: [],
  overlap_warnings: [],
  data_issues: [],
  estimated: false,
  generated_at: 'x',
}

const lat = (o: Partial<MetroLatencyDelta>): MetroLatencyDelta => ({
  severity: 'medium',
  metro_a: 'a',
  metro_z: 'b',
  before_us: 0,
  after_us: 0,
  delta_us: 0,
  caused_by: [],
  ...o,
})

const red = (o: Partial<RedundancyChange>): RedundancyChange => ({
  severity: 'medium',
  metro_a: 'a',
  metro_z: 'b',
  before_paths: 2,
  after_paths: 2,
  caused_by: [],
  ...o,
})

const cap = (o: Partial<CapacityRisk>): CapacityRisk => ({
  severity: 'medium',
  link_pk: 'l',
  description: 'l',
  estimated: true,
  reroute_from_link_pk: '',
  current_bps: 0,
  displaced_bps: 0,
  projected_bps: 0,
  bandwidth_bps: 0,
  utilization_pct: 0,
  caused_by: [],
  note: '',
  ...o,
})

describe('severityRank', () => {
  it('orders high before medium before low', () => {
    expect(severityRank('high')).toBeLessThan(severityRank('medium'))
    expect(severityRank('medium')).toBeLessThan(severityRank('low'))
  })
})

describe('sortLatencyDeltas', () => {
  it('puts unreachable pairs first, then largest slowdown', () => {
    const items = [
      lat({ metro_a: 'nyc', delta_us: 3000 }),
      lat({ metro_a: 'sea', after_us: -1, delta_us: 0 }),
      lat({ metro_a: 'lax', delta_us: 9000 }),
    ]
    const out = sortLatencyDeltas(items)
    expect(out.map((d) => d.metro_a)).toEqual(['sea', 'lax', 'nyc'])
  })

  it('does not mutate the input', () => {
    const items = [lat({ delta_us: 1 }), lat({ delta_us: 2 })]
    const copy = [...items]
    sortLatencyDeltas(items)
    expect(items).toEqual(copy)
  })
})

describe('sortRedundancy', () => {
  it('ranks the biggest path-count drop first', () => {
    const items = [
      red({ metro_a: 'a', before_paths: 3, after_paths: 2 }),
      red({ metro_a: 'b', before_paths: 3, after_paths: 1 }),
    ]
    expect(sortRedundancy(items).map((r) => r.metro_a)).toEqual(['b', 'a'])
  })
})

describe('sortCapacityRisks', () => {
  it('ranks the most severe first', () => {
    const items = [cap({ link_pk: 'x', severity: 'medium' }), cap({ link_pk: 'y', severity: 'high' })]
    expect(sortCapacityRisks(items).map((c) => c.link_pk)).toEqual(['y', 'x'])
  })
})

describe('countBySeverity', () => {
  it('tallies every finding across all four checks plus overlaps', () => {
    const report: PlanImpactReport = {
      ...emptyReport,
      partition_issues: [
        {
          severity: 'high',
          entity_type: 'device',
          entity_pk: 'd',
          entity_code: 'd',
          description: '',
          caused_by: [],
          type: 'device_isolated',
        },
      ],
      latency_deltas: [lat({ severity: 'medium' })],
      capacity_risks: [cap({ severity: 'medium' })],
      overlap_warnings: [
        {
          severity: 'low',
          other_plan_id: 'p',
          other_plan_name: 'p',
          other_plan_status: 'draft',
          entity_type: 'link',
          entity_pk: 'l',
          entity_code: 'l',
          description: '',
        },
      ],
    }
    expect(countBySeverity(report)).toEqual({ high: 1, medium: 2, low: 1, total: 4 })
  })
})

describe('hasAnyImpact', () => {
  it('is false for an empty report', () => {
    expect(hasAnyImpact(emptyReport)).toBe(false)
  })
  it('is true when any finding exists', () => {
    expect(hasAnyImpact({ ...emptyReport, latency_deltas: [lat({})] })).toBe(true)
  })
})

describe('formatDeltaMs', () => {
  it('shows a signed millisecond value', () => {
    expect(formatDeltaMs(3200)).toBe('+3.2ms')
    expect(formatDeltaMs(-1500)).toBe('-1.5ms')
    expect(formatDeltaMs(0)).toBe('0.0ms')
  })
})

describe('changeShortLabel', () => {
  it('builds a #seq + human op + entity label', () => {
    expect(
      changeShortLabel({ seq: 20, op_type: 'remove_link', ref_snapshot: { code: 'chi-nyc-1' } }),
    ).toBe('#20 Remove link chi-nyc-1')
  })
  it('falls back gracefully when no snapshot label is present', () => {
    expect(changeShortLabel({ seq: 10, op_type: 'add_device', ref_snapshot: null })).toBe('#10 Add device')
  })

  it('shows the LINK identity (not just the device) for a move_link_end change', () => {
    expect(
      changeShortLabel({
        seq: 5,
        op_type: 'move_link_end',
        ref_snapshot: { link_code: 'chi-nyc-1', device_code: 'nyc-dz-2' },
      }),
    ).toBe('#5 Move link chi-nyc-1 → nyc-dz-2')
  })

  it('shows the link identity for a remove_link change', () => {
    expect(
      changeShortLabel({
        seq: 7,
        op_type: 'remove_link',
        ref_snapshot: { link_code: 'lax-sea-1' },
      }),
    ).toBe('#7 Remove link lax-sea-1')
  })

  it('shows the device code for a device op', () => {
    expect(
      changeShortLabel({
        seq: 3,
        op_type: 'remove_device',
        ref_snapshot: { device_code: 'fra-dz-1' },
      }),
    ).toBe('#3 Remove device fra-dz-1')
  })
})
