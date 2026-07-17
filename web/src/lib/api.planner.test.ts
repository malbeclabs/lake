import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  fetchPlans,
  fetchPlan,
  createPlan,
  updatePlan,
  deletePlan,
  addPlanChange,
  updatePlanChange,
  duplicatePlan,
  deletePlanChange,
  reorderPlanChanges,
  PlanConflictError,
} from './api'

type FetchArgs = { url: string; init: RequestInit | undefined }

function mockFetch(status: number, body: unknown) {
  const calls: FetchArgs[] = []
  const fn = vi.fn(async (url: string, init?: RequestInit) => {
    calls.push({ url, init })
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => body,
    } as unknown as Response
  })
  vi.stubGlobal('fetch', fn)
  return calls
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('plan api client', () => {
  beforeEach(() => vi.clearAllMocks())

  it('fetchPlans GETs the list endpoint and unwraps the { plans } envelope', async () => {
    // Backend ListPlans responds with an envelope, not a bare array.
    const calls = mockFetch(200, { plans: [{ id: 'p1', name: 'Plan A' }] })
    const plans = await fetchPlans()
    expect(calls[0].url).toContain('/api/topology/plans')
    expect(Array.isArray(plans)).toBe(true)
    expect(plans).toHaveLength(1)
    expect(plans[0].id).toBe('p1')
  })

  it('fetchPlans returns [] when the envelope has no plans key', async () => {
    mockFetch(200, {})
    const plans = await fetchPlans()
    expect(plans).toEqual([])
  })

  it('fetchPlan GETs a single plan by id', async () => {
    const calls = mockFetch(200, { id: 'p1', name: 'A', changes: [] })
    const plan = await fetchPlan('p1')
    expect(calls[0].url).toContain('/api/topology/plans/p1')
    expect(plan.changes).toEqual([])
  })

  it('createPlan POSTs name + description', async () => {
    const calls = mockFetch(200, { id: 'p2', name: 'New' })
    await createPlan({ name: 'New', description: 'ctx' })
    expect(calls[0].init?.method).toBe('POST')
    expect(JSON.parse(String(calls[0].init?.body))).toEqual({ name: 'New', description: 'ctx' })
  })

  it('updatePlan PATCHes with the version for optimistic concurrency', async () => {
    const calls = mockFetch(200, { id: 'p1', name: 'Renamed', version: 3 })
    await updatePlan('p1', { name: 'Renamed' }, 2)
    expect(calls[0].init?.method).toBe('PATCH')
    expect(JSON.parse(String(calls[0].init?.body))).toEqual({ name: 'Renamed', version: 2 })
  })

  it('addPlanChange POSTs to the changes sub-collection', async () => {
    const calls = mockFetch(200, { id: 'c1', op_type: 'remove_link' })
    await addPlanChange('p1', {
      op_type: 'remove_link',
      ref_link_pk: 'L1',
      payload: {},
      ref_snapshot: { link_code: 'nyc-lon1' },
    })
    expect(calls[0].url).toContain('/api/topology/plans/p1/changes')
    expect(calls[0].init?.method).toBe('POST')
    expect(JSON.parse(String(calls[0].init?.body)).op_type).toBe('remove_link')
  })

  it('duplicatePlan POSTs to the duplicate endpoint', async () => {
    const calls = mockFetch(200, { id: 'p3' })
    await duplicatePlan('p1')
    expect(calls[0].url).toContain('/api/topology/plans/p1/duplicate')
    expect(calls[0].init?.method).toBe('POST')
  })

  it('deletePlan DELETEs the plan endpoint', async () => {
    const calls = mockFetch(204, {})
    await deletePlan('p1')
    expect(calls[0].url).toContain('/api/topology/plans/p1')
    expect(calls[0].init?.method).toBe('DELETE')
  })

  it('deletePlan resolves on 404 (idempotent delete)', async () => {
    mockFetch(404, {})
    await expect(deletePlan('gone')).resolves.toBeUndefined()
  })

  it('updatePlanChange PATCHes the change endpoint with version', async () => {
    const calls = mockFetch(200, { id: 'c1', state: 'done', version: 4 })
    await updatePlanChange('p1', 'c1', { state: 'done' }, 3)
    expect(calls[0].url).toContain('/api/topology/plans/p1/changes/c1')
    expect(calls[0].init?.method).toBe('PATCH')
    expect(JSON.parse(String(calls[0].init?.body))).toEqual({ state: 'done', version: 3 })
  })

  it('updatePlanChange throws PlanConflictError on HTTP 409', async () => {
    mockFetch(409, { error: 'stale' })
    await expect(updatePlanChange('p1', 'c1', { state: 'done' }, 1)).rejects.toBeInstanceOf(
      PlanConflictError
    )
  })

  it('deletePlanChange DELETEs and resolves on 404', async () => {
    const calls = mockFetch(404, {})
    await expect(deletePlanChange('p1', 'gone')).resolves.toBeUndefined()
    expect(calls[0].url).toContain('/api/topology/plans/p1/changes/gone')
    expect(calls[0].init?.method).toBe('DELETE')
  })

  it('throws PlanConflictError on HTTP 409', async () => {
    mockFetch(409, { error: 'stale' })
    await expect(updatePlan('p1', { name: 'x' }, 1)).rejects.toBeInstanceOf(PlanConflictError)
  })

  it('reorderPlanChanges POSTs the bulk reorder endpoint with ordered_ids and returns the updated plan', async () => {
    const calls = mockFetch(200, { id: 'p1', version: 4, changes: [{ id: 'c2' }, { id: 'c1' }] })
    const plan = await reorderPlanChanges('p1', ['c2', 'c1'])
    expect(calls[0].url).toContain('/api/topology/plans/p1/changes/reorder')
    expect(calls[0].init?.method).toBe('POST')
    expect(JSON.parse(String(calls[0].init?.body))).toEqual({ ordered_ids: ['c2', 'c1'] })
    expect(plan.version).toBe(4)
    expect(plan.changes.map((c) => c.id)).toEqual(['c2', 'c1'])
  })

  it('reorderPlanChanges throws PlanConflictError on HTTP 409', async () => {
    mockFetch(409, { error: 'stale' })
    await expect(reorderPlanChanges('p1', ['c1'])).rejects.toBeInstanceOf(PlanConflictError)
  })
})
