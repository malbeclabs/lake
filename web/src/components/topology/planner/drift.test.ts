import { describe, it, expect } from 'vitest'
import { computeChangeDrift, annotateDrift } from './drift'
import type { PlanChange, TopologyResponse } from '@/lib/api'

function baseline(): TopologyResponse {
  return {
    metros: [],
    devices: [
      { pk: 'dA', code: 'nyc-x1', metro_pk: 'mA' },
      { pk: 'dB', code: 'lon-x1', metro_pk: 'mB' },
    ],
    links: [{ pk: 'L1', code: 'nyc-lon1', side_a_pk: 'dA', side_z_pk: 'dB' }],
    validators: [],
  } as unknown as TopologyResponse
}

function ch(over: Partial<PlanChange>): PlanChange {
  return {
    id: 'c', plan_id: 'p', seq: 10, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    created_at: '', updated_at: '', ...over,
  } as PlanChange
}

describe('computeChangeDrift', () => {
  it('remove_link is pending while the link still exists', () => {
    expect(computeChangeDrift(baseline(), ch({ op_type: 'remove_link', ref_link_pk: 'L1' }))).toBe('pending')
  })
  it('remove_link is already_done once the link is gone', () => {
    expect(computeChangeDrift(baseline(), ch({ op_type: 'remove_link', ref_link_pk: 'GONE' }))).toBe('already_done')
  })
  it('remove_device is already_done once the device is gone', () => {
    expect(computeChangeDrift(baseline(), ch({ op_type: 'remove_device', ref_device_pk: 'GONE' }))).toBe('already_done')
  })
  it('move_link_end is broken when the target link vanished', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'move_link_end', ref_link_pk: 'GONE', new_device_pk: 'dB', payload: { side: 'z' } }))
    ).toBe('broken')
  })
  it('move_link_end is already_done when the endpoint already points at the target', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'move_link_end', ref_link_pk: 'L1', new_device_pk: 'dB', payload: { side: 'z' } }))
    ).toBe('already_done')
  })
  it('move_link_end is broken when the target device was removed from live topology', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'move_link_end', ref_link_pk: 'L1', new_device_pk: 'GONE', payload: { side: 'z' } }))
    ).toBe('broken')
  })
  it('move_link_end is pending when the link and target device both still exist', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'move_link_end', ref_link_pk: 'L1', new_device_pk: 'dA', payload: { side: 'z' } }))
    ).toBe('pending')
  })
  it('move_link_end target that is a same-plan local_ref is not marked broken on live-topology absence', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'move_link_end', ref_link_pk: 'L1', payload: { side: 'z', new_device_ref: 't1' } }))
    ).toBe('pending')
  })
  it('add_device is already_done when a device with the same code exists', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'add_device', local_ref: 't1', payload: { code: 'nyc-x1', metro_pk: 'mA' } }))
    ).toBe('already_done')
  })
  it('add_link is broken when an endpoint device is missing', () => {
    expect(
      computeChangeDrift(baseline(), ch({ op_type: 'add_link', local_ref: 't1', payload: { side_a_device_pk: 'dA', side_z_device_pk: 'GONE' } }))
    ).toBe('broken')
  })
})

describe('annotateDrift', () => {
  it('maps every change id to a drift label', () => {
    const changes = [ch({ id: 'c1', op_type: 'remove_link', ref_link_pk: 'L1' })]
    const m = annotateDrift(baseline(), changes)
    expect(m.get('c1')).toBe('pending')
  })
})
