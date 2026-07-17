import { describe, it, expect } from 'vitest'
import {
  collectChangedEntities,
  changedEntitiesBBox,
  projectPoint,
  buildPlanPreview,
  collectChangedMetros,
  type BBox,
} from './plan-preview'
import { buildDraft } from './draft'
import type { PlanChange, TopologyResponse } from '@/lib/api'

function baseline(): TopologyResponse {
  return {
    metros: [
      { pk: 'mA', code: 'nyc', name: 'NYC', latitude: 40.7, longitude: -74 },
      { pk: 'mB', code: 'lon', name: 'LON', latitude: 51.5, longitude: -0.1 },
      { pk: 'mC', code: 'fra', name: 'FRA', latitude: 50.1, longitude: 8.7 },
    ],
    devices: [
      { pk: 'dA', code: 'nyc-x1', metro_pk: 'mA', contributor_pk: 'c1' },
      { pk: 'dB', code: 'lon-x1', metro_pk: 'mB', contributor_pk: 'c2' },
      { pk: 'dC', code: 'fra-x1', metro_pk: 'mC', contributor_pk: 'c3' },
    ],
    links: [
      {
        pk: 'L1', code: 'nyc-lon1', link_type: 'WAN',
        side_a_pk: 'dA', side_z_pk: 'dB',
        latency_us: 70_000, committed_rtt_ns: 70_000_000, bandwidth_bps: 1e10,
      },
    ],
    validators: [],
  } as unknown as TopologyResponse
}

function change(over: Partial<PlanChange>): PlanChange {
  return {
    id: 'c', plan_id: 'p', seq: 10, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    created_at: '', updated_at: '', ...over,
  } as PlanChange
}

describe('collectChangedEntities', () => {
  it('excludes unchanged devices and links entirely', () => {
    const draft = buildDraft(baseline(), [])
    const entities = collectChangedEntities(draft)
    expect(entities.devices).toHaveLength(0)
    expect(entities.links).toHaveLength(0)
  })

  it('includes a removed device', () => {
    const draft = buildDraft(baseline(), [
      change({ op_type: 'remove_device', ref_device_pk: 'dC' }),
    ])
    const entities = collectChangedEntities(draft)
    expect(entities.devices).toHaveLength(1)
    expect(entities.devices[0]).toMatchObject({ key: 'dC', state: 'removed' })
  })

  it('includes both endpoints of a removed link even though neither device changed', () => {
    const draft = buildDraft(baseline(), [
      change({ op_type: 'remove_link', ref_link_pk: 'L1' }),
    ])
    const entities = collectChangedEntities(draft)
    expect(entities.devices).toHaveLength(0) // endpoints stay unchanged themselves
    expect(entities.links).toHaveLength(1)
    expect(entities.links[0].state).toBe('removed')
    // Endpoints resolve to real coordinates (NYC / LON), not (0,0).
    expect(entities.links[0].a.lat).toBeCloseTo(40.7, 1)
    expect(entities.links[0].z.lat).toBeCloseTo(51.5, 1)
  })

  it('includes an added device and an added link referencing it, plus the existing endpoint', () => {
    const draft = buildDraft(baseline(), [
      change({
        op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', metro_pk: 'mC', code: 'fra-x2', device_type: 'switch' },
      }),
      change({
        seq: 20, op_type: 'add_link', local_ref: 'tmp_link_1',
        payload: { side_a_device_pk: 'dA', side_z_ref: 'tmp_dev_1', latency_ns: 42_000_000, bandwidth_bps: 1e10 },
      }),
    ])
    const entities = collectChangedEntities(draft)
    expect(entities.devices.map((d) => d.key)).toEqual(['tmp_dev_1'])
    expect(entities.devices[0].state).toBe('added')
    expect(entities.links).toHaveLength(1)
    expect(entities.links[0].state).toBe('added')
  })

  it('skips a changed device with no resolvable metro position', () => {
    const draft = buildDraft(baseline(), [
      change({
        op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', code: 'orphan-x1', device_type: 'switch' }, // no metro_pk
      }),
    ])
    const entities = collectChangedEntities(draft)
    expect(entities.devices).toHaveLength(0)
  })
})

describe('changedEntitiesBBox', () => {
  it('returns null when there are no changed entities', () => {
    expect(changedEntitiesBBox({ devices: [], links: [] })).toBeNull()
  })

  it('bounds a single device with padding on every side', () => {
    const bbox = changedEntitiesBBox(
      { devices: [{ key: 'd1', lat: 10, lng: 20, state: 'added' }], links: [] },
      2
    )
    expect(bbox).toEqual({ minLat: 8, maxLat: 12, minLng: 18, maxLng: 22 })
  })

  it('expands the box to cover link endpoints, not just device points', () => {
    const bbox = changedEntitiesBBox(
      {
        devices: [{ key: 'd1', lat: 0, lng: 0, state: 'added' }],
        links: [
          {
            key: 'l1',
            state: 'removed',
            a: { lat: 0, lng: 0 },
            z: { lat: 30, lng: 40 },
          },
        ],
      },
      0
    )
    expect(bbox).toEqual({ minLat: 0, maxLat: 30, minLng: 0, maxLng: 40 })
  })
})

describe('projectPoint', () => {
  it('maps a square bbox onto a square viewbox with no letterboxing', () => {
    const bbox: BBox = { minLat: 0, maxLat: 10, minLng: 0, maxLng: 10 }
    expect(projectPoint(0, 0, bbox, 100, 100)).toEqual([0, 100])
    expect(projectPoint(10, 10, bbox, 100, 100)).toEqual([100, 0])
    expect(projectPoint(5, 5, bbox, 100, 100)).toEqual([50, 50])
  })

  it('preserves aspect ratio for a non-square bbox by centering with letterbox offsets', () => {
    // latSpan=20, lngSpan=10 -> scale = min(100/10, 100/20) = 5, contentW=50, contentH=100.
    const bbox: BBox = { minLat: 0, maxLat: 20, minLng: 0, maxLng: 10 }
    expect(projectPoint(0, 0, bbox, 100, 100)).toEqual([25, 100])
    expect(projectPoint(20, 10, bbox, 100, 100)).toEqual([75, 0])
    expect(projectPoint(10, 5, bbox, 100, 100)).toEqual([50, 50])
  })

  it('does not divide by zero when every point coincides', () => {
    const bbox: BBox = { minLat: 5, maxLat: 5, minLng: 5, maxLng: 5 }
    const [x, y] = projectPoint(5, 5, bbox, 100, 100)
    expect(Number.isFinite(x)).toBe(true)
    expect(Number.isFinite(y)).toBe(true)
  })
})

describe('buildPlanPreview', () => {
  it('returns null for a plan with no changes', () => {
    expect(buildPlanPreview(baseline(), [], 160, 100)).toBeNull()
  })

  it('returns null when every change is done, skipped or superseded (nothing pending)', () => {
    const preview = buildPlanPreview(
      baseline(),
      [
        change({ op_type: 'remove_link', ref_link_pk: 'L1', state: 'done' }),
        change({ id: 'c2', op_type: 'remove_device', ref_device_pk: 'dC', state: 'skipped' }),
      ],
      160,
      100
    )
    expect(preview).toBeNull()
  })

  it('produces device + link geometry within the viewbox for a plan with pending changes', () => {
    const preview = buildPlanPreview(
      baseline(),
      [change({ op_type: 'remove_link', ref_link_pk: 'L1' })],
      160,
      100
    )
    expect(preview).not.toBeNull()
    expect(preview!.links).toHaveLength(1)
    expect(preview!.links[0].state).toBe('removed')
    for (const p of [...preview!.devices, ...preview!.links.flatMap((l) => [{ x: l.x1, y: l.y1 }, { x: l.x2, y: l.y2 }])]) {
      expect(p.x).toBeGreaterThanOrEqual(0)
      expect(p.x).toBeLessThanOrEqual(160)
      expect(p.y).toBeGreaterThanOrEqual(0)
      expect(p.y).toBeLessThanOrEqual(100)
    }
  })

  it('reflects a move_link_end as a modified line', () => {
    const preview = buildPlanPreview(
      baseline(),
      [
        change({
          op_type: 'move_link_end', ref_link_pk: 'L1', new_device_pk: 'dC',
          payload: { side: 'z', latency_ns: 60_000_000, bandwidth_bps: 1e10 },
        }),
      ],
      160,
      100
    )
    expect(preview).not.toBeNull()
    expect(preview!.links[0].state).toBe('modified')
  })

  it('returns non-empty metros and includes context geometry for unchanged entities inside the bbox', () => {
    // Removing L1 (NYC <-> LON) leaves dA/dB unchanged themselves but touched by
    // the change, so they should show up both as changed-link endpoints AND as
    // context devices (their own changeState stays 'unchanged'). FRA sits well
    // outside the padded NYC/LON bbox, so it contributes no context or metro.
    const preview = buildPlanPreview(
      baseline(),
      [change({ op_type: 'remove_link', ref_link_pk: 'L1' })],
      160,
      100
    )
    expect(preview).not.toBeNull()
    expect(preview!.metros.length).toBeGreaterThan(0)
    expect(preview!.metros.map((m) => m.code).sort()).toEqual(['lon', 'nyc'])
    expect(preview!.context.devices.length).toBeGreaterThan(0)
  })
})

describe('collectChangedMetros', () => {
  it('returns the deduped touched metros with correct codes for a changed device and a changed link across two metros', () => {
    const draft = buildDraft(baseline(), [
      change({
        op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', metro_pk: 'mC', code: 'fra-x2', device_type: 'switch' },
      }),
      change({
        seq: 20, op_type: 'add_link', local_ref: 'tmp_link_1',
        payload: { side_a_device_pk: 'dA', side_z_ref: 'tmp_dev_1', latency_ns: 42_000_000, bandwidth_bps: 1e10 },
      }),
    ])
    const metros = collectChangedMetros(draft)
    expect(metros.map((m) => m.code).sort()).toEqual(['fra', 'nyc'])
    // Deduped: FRA is touched twice (the added device + the added link's z-side)
    // but appears only once.
    expect(metros).toHaveLength(2)
  })

  it('returns an empty list when there are no changed entities', () => {
    const draft = buildDraft(baseline(), [])
    expect(collectChangedMetros(draft)).toEqual([])
  })
})
