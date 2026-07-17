import { describe, it, expect } from 'vitest'
import {
  plannerReducer,
  initialPlannerState,
  nextSeq,
  reorderedSeq,
} from './planner-reducer'
import type { Plan, PlanChange } from '@/lib/api'

function change(id: string, seq: number): PlanChange {
  return {
    id, plan_id: 'p', seq, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    created_at: '', updated_at: '',
  } as PlanChange
}

const plan: Plan = {
  id: 'p', name: 'Plan', status: 'draft', environment: 'testnet',
  baseline_as_of: '', version: 1, created_at: '', updated_at: '', change_count: 1,
  changes: [change('c1', 10)],
}

describe('nextSeq', () => {
  it('returns 10 for an empty list', () => {
    expect(nextSeq([])).toBe(10)
  })
  it('returns max + 10 otherwise', () => {
    expect(nextSeq([change('a', 10), change('b', 30)])).toBe(40)
  })
})

describe('reorderedSeq', () => {
  it('renumbers to 10,20,30 in the given order', () => {
    const out = reorderedSeq(['c2', 'c1'], [change('c1', 10), change('c2', 20)])
    expect(out.map((c) => [c.id, c.seq])).toEqual([['c2', 10], ['c1', 20]])
  })
})

describe('plannerReducer', () => {
  it('sets the plan', () => {
    const s = plannerReducer(initialPlannerState, { type: 'setPlan', plan })
    expect(s.plan?.id).toBe('p')
  })
  it('switches tool and clears selection', () => {
    let s = plannerReducer(initialPlannerState, { type: 'selectLink', key: 'L1' })
    s = plannerReducer(s, { type: 'setTool', tool: 'add-link' })
    expect(s.tool).toBe('add-link')
    expect(s.selectedLinkKey).toBeNull()
  })
  it('upserts a change into the plan (optimistic add)', () => {
    let s = plannerReducer(initialPlannerState, { type: 'setPlan', plan })
    s = plannerReducer(s, { type: 'upsertChange', change: change('c2', 20) })
    expect(s.plan?.changes.map((c) => c.id)).toEqual(['c1', 'c2'])
  })
  it('replaces an optimistic change by id', () => {
    let s = plannerReducer(initialPlannerState, { type: 'setPlan', plan })
    s = plannerReducer(s, { type: 'upsertChange', change: change('tmp', 20) })
    s = plannerReducer(s, { type: 'replaceChange', tempId: 'tmp', change: change('real', 20) })
    expect(s.plan?.changes.map((c) => c.id)).toEqual(['c1', 'real'])
  })
  it('appends the server change when the temp id is gone', () => {
    let s = plannerReducer(initialPlannerState, { type: 'setPlan', plan })
    // A reload replaced changes before the server response arrived; 'tmp' is gone.
    s = plannerReducer(s, { type: 'replaceChange', tempId: 'tmp', change: change('real', 20) })
    expect(s.plan?.changes.map((c) => c.id)).toEqual(['c1', 'real'])
  })
  it('does not duplicate when the server change is already present', () => {
    let s = plannerReducer(initialPlannerState, { type: 'setPlan', plan })
    // The reload already contained the server-persisted change ('c1').
    s = plannerReducer(s, { type: 'replaceChange', tempId: 'tmp', change: change('c1', 10) })
    expect(s.plan?.changes.map((c) => c.id)).toEqual(['c1'])
  })
  it('removes a change', () => {
    let s = plannerReducer(initialPlannerState, { type: 'setPlan', plan })
    s = plannerReducer(s, { type: 'removeChange', id: 'c1' })
    expect(s.plan?.changes).toHaveLength(0)
  })
  it('sets and clears the conflict flag', () => {
    let s = plannerReducer(initialPlannerState, { type: 'setConflict', conflict: true })
    expect(s.conflict).toBe(true)
    s = plannerReducer(s, { type: 'setConflict', conflict: false })
    expect(s.conflict).toBe(false)
  })
})
