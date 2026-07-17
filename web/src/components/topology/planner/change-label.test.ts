import { describe, it, expect } from 'vitest'
import { changeSummary } from './change-label'
import type { PlanChange } from '@/lib/api'

function ch(over: Partial<PlanChange>): PlanChange {
  return {
    id: 'c', plan_id: 'p', seq: 10, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    created_at: '', updated_at: '', ...over,
  } as PlanChange
}

describe('changeSummary', () => {
  it('describes remove_link with the link code', () => {
    expect(changeSummary(ch({ op_type: 'remove_link', ref_snapshot: { link_code: 'nyc-lon1' } }))).toBe(
      'Remove link nyc-lon1'
    )
  })
  it('describes remove_device with the device code', () => {
    expect(changeSummary(ch({ op_type: 'remove_device', ref_snapshot: { device_code: 'nyc-x1' } }))).toBe(
      'Remove device nyc-x1'
    )
  })
  it('describes add_device with the new code', () => {
    expect(changeSummary(ch({ op_type: 'add_device', payload: { code: 'fra-x2' } }))).toBe(
      'Add device fra-x2'
    )
  })
  it('describes add_link with the snapshot code', () => {
    expect(changeSummary(ch({ op_type: 'add_link', ref_snapshot: { link_code: 'a-b' } }))).toBe(
      'Add link a-b'
    )
  })
  it('describes move_link_end with link and target device', () => {
    expect(
      changeSummary(
        ch({ op_type: 'move_link_end', ref_snapshot: { link_code: 'nyc-lon1', device_code: 'fra-x1' } })
      )
    ).toBe('Move link nyc-lon1 → fra-x1')
  })
})
