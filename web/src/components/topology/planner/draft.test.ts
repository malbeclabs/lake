import { describe, it, expect } from 'vitest'
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

describe('buildDraft', () => {
  it('marks unchanged entities', () => {
    const d = buildDraft(baseline(), [])
    expect(d.linkByKey.get('L1')?.changeState).toBe('unchanged')
    expect(d.deviceByKey.get('dA')?.changeState).toBe('unchanged')
  })

  it('remove_link marks the link removed but keeps it in the draft', () => {
    const d = buildDraft(baseline(), [change({ op_type: 'remove_link', ref_link_pk: 'L1' })])
    expect(d.linkByKey.get('L1')?.changeState).toBe('removed')
  })

  it('remove_device marks the device removed', () => {
    const d = buildDraft(baseline(), [change({ op_type: 'remove_device', ref_device_pk: 'dC' })])
    expect(d.deviceByKey.get('dC')?.changeState).toBe('removed')
  })

  it('add_device inserts a device keyed by local_ref', () => {
    const d = buildDraft(baseline(), [
      change({
        op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', metro_pk: 'mC', code: 'fra-x2', device_type: 'switch' },
      }),
    ])
    const dev = d.deviceByKey.get('tmp_dev_1')
    expect(dev?.changeState).toBe('added')
    expect(dev?.metro_pk).toBe('mC')
    expect(dev?.code).toBe('fra-x2')
  })

  // Canonical add_device shape: a brand-new metro (no baseline metro_pk) carries
  // its own code + coordinates inline. The draft must add it to its own metro
  // list (without mutating the baseline) so the device renders at the right place.
  it('add_device with new_metro adds a synthetic metro to the draft without mutating the baseline', () => {
    const b = baseline()
    const d = buildDraft(b, [
      change({
        op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: {
          contributor_code: 'newco', code: 'zzz-x1',
          new_metro: { code: 'zzz', latitude: 10, longitude: 20 },
        },
      }),
    ])
    const dev = d.deviceByKey.get('tmp_dev_1')
    expect(dev?.changeState).toBe('added')
    expect(dev?.contributor_code).toBe('newco')

    const metro = d.metros.find((m) => m.code === 'zzz')
    expect(metro).toBeDefined()
    expect(metro?.latitude).toBe(10)
    expect(metro?.longitude).toBe(20)
    expect(dev?.metro_pk).toBe(metro?.pk)

    // The baseline's own metros array must stay untouched.
    expect(b.metros).toHaveLength(3)
  })

  it('add_link resolves endpoints (pk and local_ref) and converts latency to us', () => {
    const d = buildDraft(baseline(), [
      change({
        op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', metro_pk: 'mC', code: 'fra-x2', device_type: 'switch' },
      }),
      change({
        seq: 20, op_type: 'add_link', local_ref: 'tmp_link_1',
        payload: {
          side_a_device_pk: 'dA', side_z_ref: 'tmp_dev_1',
          latency_ns: 42_000_000, bandwidth_bps: 1e10, link_type: 'WAN',
        },
      }),
    ])
    const link = d.linkByKey.get('tmp_link_1')
    expect(link?.changeState).toBe('added')
    expect(link?.side_a_pk).toBe('dA')
    expect(link?.side_z_pk).toBe('tmp_dev_1')
    expect(link?.latency_us).toBe(42_000)
  })

  it('move_link_end reassigns an endpoint and records modified fields', () => {
    const d = buildDraft(baseline(), [
      change({
        op_type: 'move_link_end', ref_link_pk: 'L1',
        new_device_pk: 'dC',
        payload: { side: 'z', new_iface_name: 'Ethernet1', latency_ns: 60_000_000, bandwidth_bps: 1e10 },
      }),
    ])
    const link = d.linkByKey.get('L1')
    expect(link?.changeState).toBe('modified')
    expect(link?.side_z_pk).toBe('dC')
    expect(link?.latency_us).toBe(60_000)
    expect(link?.modifiedFields).toContain('side_z_pk')
  })

  it('move_link_end via new_device_ref moves the endpoint to a newly added device and reflects its attribution', () => {
    const d = buildDraft(baseline(), [
      change({
        seq: 10, op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', metro_pk: 'mC', code: 'fra-x2', device_type: 'switch' },
      }),
      change({
        seq: 20, op_type: 'move_link_end', ref_link_pk: 'L1',
        payload: { side: 'z', new_device_ref: 'tmp_dev_1', latency_ns: 55_000_000, bandwidth_bps: 1e10 },
      }),
    ])
    const link = d.linkByKey.get('L1')
    expect(link?.changeState).toBe('modified')
    expect(link?.side_z_pk).toBe('tmp_dev_1')
    expect(link?.side_z_code).toBe('fra-x2')
    expect(link?.side_z_contributor_pk).toBe('c9')
    expect(link?.modifiedFields).toContain('side_z_pk')
  })

  it('add_link reflects endpoint ownership from resolved devices', () => {
    const d = buildDraft(baseline(), [
      change({
        seq: 10, op_type: 'add_device', local_ref: 'tmp_dev_1',
        payload: { contributor_pk: 'c9', metro_pk: 'mC', code: 'fra-x2', device_type: 'switch' },
      }),
      change({
        seq: 20, op_type: 'add_link', local_ref: 'tmp_link_1',
        payload: {
          side_a_device_pk: 'dA', side_z_ref: 'tmp_dev_1',
          latency_ns: 42_000_000, metric_override_ns: 50_000_000, bandwidth_bps: 1e10, link_type: 'WAN',
        },
      }),
    ])
    const link = d.linkByKey.get('tmp_link_1')
    expect(link?.side_a_code).toBe('nyc-x1')
    expect(link?.side_a_contributor_pk).toBe('c1')
    expect(link?.side_z_code).toBe('fra-x2')
    expect(link?.side_z_contributor_pk).toBe('c9')
    // display latency stays latency_ns; routing override rides isis_delay_override_ns
    expect(link?.latency_us).toBe(42_000)
    expect(link?.isis_delay_override_ns).toBe(50_000_000)
  })

  it('skips changes that are skipped or superseded', () => {
    const d = buildDraft(baseline(), [
      change({ op_type: 'remove_link', ref_link_pk: 'L1', state: 'skipped' }),
    ])
    expect(d.linkByKey.get('L1')?.changeState).toBe('unchanged')
  })

  it('excludes a superseded change', () => {
    const d = buildDraft(baseline(), [
      change({ op_type: 'remove_link', ref_link_pk: 'L1', state: 'superseded' }),
    ])
    expect(d.linkByKey.get('L1')?.changeState).toBe('unchanged')
  })

  // SC-8: the live baseline already reflects a 'done' change, so re-applying it
  // would double-count the edge/device. buildDraft must NOT re-apply 'done'.
  it('does not re-apply a done change (already reflected in the live baseline)', () => {
    const d = buildDraft(baseline(), [
      change({ op_type: 'remove_link', ref_link_pk: 'L1', state: 'done' }),
    ])
    expect(d.linkByKey.get('L1')?.changeState).toBe('unchanged')
  })

  it('applies a pending change', () => {
    const d = buildDraft(baseline(), [
      change({ op_type: 'remove_link', ref_link_pk: 'L1', state: 'pending' }),
    ])
    expect(d.linkByKey.get('L1')?.changeState).toBe('removed')
  })
})
