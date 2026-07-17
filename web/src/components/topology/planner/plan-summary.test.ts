import { describe, it, expect } from 'vitest'
import { countPendingChanges, totalPendingChanges, formatChangeSummary } from './plan-summary'
import type { PlanChange } from '@/lib/api'

function ch(over: Partial<PlanChange>): PlanChange {
  return {
    id: 'c', plan_id: 'p', seq: 10, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    created_at: '', updated_at: '', ...over,
  } as PlanChange
}

describe('countPendingChanges', () => {
  it('counts each op type independently', () => {
    const counts = countPendingChanges([
      ch({ id: 'c1', op_type: 'add_device' }),
      ch({ id: 'c2', op_type: 'add_device' }),
      ch({ id: 'c3', op_type: 'add_device' }),
      ch({ id: 'c4', op_type: 'remove_device' }),
      ch({ id: 'c5', op_type: 'add_link' }),
      ch({ id: 'c6', op_type: 'add_link' }),
      ch({ id: 'c7', op_type: 'remove_link' }),
      ch({ id: 'c8', op_type: 'remove_link' }),
      ch({ id: 'c9', op_type: 'remove_link' }),
      ch({ id: 'c10', op_type: 'remove_link' }),
      ch({ id: 'c11', op_type: 'move_link_end' }),
    ])
    expect(counts).toEqual({
      devicesAdded: 3,
      devicesRemoved: 1,
      linksAdded: 2,
      linksRemoved: 4,
      linksMoved: 1,
    })
  })

  it('ignores done, skipped and superseded changes', () => {
    const counts = countPendingChanges([
      ch({ id: 'c1', op_type: 'add_device', state: 'done' }),
      ch({ id: 'c2', op_type: 'add_device', state: 'skipped' }),
      ch({ id: 'c3', op_type: 'add_device', state: 'superseded' }),
      ch({ id: 'c4', op_type: 'add_device', state: 'pending' }),
    ])
    expect(counts.devicesAdded).toBe(1)
  })

  it('returns all zeros for an empty change list', () => {
    expect(countPendingChanges([])).toEqual({
      devicesAdded: 0,
      devicesRemoved: 0,
      linksAdded: 0,
      linksRemoved: 0,
      linksMoved: 0,
    })
  })
})

describe('totalPendingChanges', () => {
  it('sums every bucket', () => {
    const counts = countPendingChanges([
      ch({ id: 'c1', op_type: 'add_device' }),
      ch({ id: 'c2', op_type: 'remove_link' }),
    ])
    expect(totalPendingChanges(counts)).toBe(2)
  })
})

describe('formatChangeSummary', () => {
  it('matches the canonical phrasing across all five buckets', () => {
    const counts = countPendingChanges([
      ch({ id: 'c1', op_type: 'add_device' }),
      ch({ id: 'c2', op_type: 'add_device' }),
      ch({ id: 'c3', op_type: 'add_device' }),
      ch({ id: 'c4', op_type: 'remove_device' }),
      ch({ id: 'c5', op_type: 'add_link' }),
      ch({ id: 'c6', op_type: 'add_link' }),
      ch({ id: 'c7', op_type: 'remove_link' }),
      ch({ id: 'c8', op_type: 'remove_link' }),
      ch({ id: 'c9', op_type: 'remove_link' }),
      ch({ id: 'c10', op_type: 'remove_link' }),
      ch({ id: 'c11', op_type: 'move_link_end' }),
    ])
    expect(formatChangeSummary(counts)).toBe(
      '3 devices added, 1 device removed, 2 links added, 4 links removed, 1 link moved'
    )
  })

  it('singularizes a count of one', () => {
    const counts = countPendingChanges([ch({ id: 'c1', op_type: 'add_device' })])
    expect(formatChangeSummary(counts)).toBe('1 device added')
  })

  it('is empty when there are no pending changes', () => {
    expect(formatChangeSummary(countPendingChanges([]))).toBe('')
  })
})
