import { describe, it, expect } from 'vitest'
import { attachedLinks } from './attached-links'
import { buildDraft } from './draft'
import type { PlanChange, TopologyResponse } from '@/lib/api'

function baseline(): TopologyResponse {
  return {
    metros: [],
    devices: [
      { pk: 'dA', code: 'a', metro_pk: 'm' },
      { pk: 'dB', code: 'b', metro_pk: 'm' },
      { pk: 'dC', code: 'c', metro_pk: 'm' },
    ],
    links: [
      { pk: 'L1', code: 'ab', side_a_pk: 'dA', side_z_pk: 'dB' },
      { pk: 'L2', code: 'bc', side_a_pk: 'dB', side_z_pk: 'dC' },
    ],
    validators: [],
  } as unknown as TopologyResponse
}

describe('attachedLinks', () => {
  it('returns links touching the device', () => {
    const draft = buildDraft(baseline(), [])
    expect(attachedLinks(draft, 'dB').map((l) => l.pk).sort()).toEqual(['L1', 'L2'])
  })
  it('excludes links already staged for removal', () => {
    const removeL1: PlanChange = {
      id: 'c1', plan_id: 'p', seq: 10, op_type: 'remove_link', ref_link_pk: 'L1',
      payload: {}, ref_snapshot: {}, state: 'pending', version: 1, created_at: '', updated_at: '',
    } as PlanChange
    const draft = buildDraft(baseline(), [removeL1])
    expect(attachedLinks(draft, 'dB').map((l) => l.pk)).toEqual(['L2'])
  })
})
