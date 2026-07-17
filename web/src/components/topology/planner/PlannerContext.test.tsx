import { renderHook, act, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { PlannerProvider, usePlanner } from './PlannerContext'
import {
  fetchTopology,
  fetchPlan,
  updatePlan,
  addPlanChange,
  updatePlanChange,
  reorderPlanChanges,
  PlanConflictError,
  type Plan,
  type PlanChange,
  type TopologyResponse,
} from '@/lib/api'

// Mock only the network fns; keep the real PlanConflictError class + types so
// `instanceof PlanConflictError` inside the provider matches what we throw here.
vi.mock('@/lib/api', async (importActual) => {
  const actual = await importActual<typeof import('@/lib/api')>()
  return {
    ...actual,
    fetchTopology: vi.fn(),
    fetchPlan: vi.fn(),
    createPlan: vi.fn(),
    updatePlan: vi.fn(),
    addPlanChange: vi.fn(),
    updatePlanChange: vi.fn(),
    deletePlanChange: vi.fn(),
    reorderPlanChanges: vi.fn(),
  }
})

const EMPTY_BASELINE: TopologyResponse = {
  metros: [],
  devices: [],
  links: [],
  validators: [],
}

function makePlan(id: string, over: Partial<Plan> = {}): Plan {
  const changes = over.changes ?? []
  return {
    id,
    name: `Plan ${id}`,
    description: '',
    status: 'draft',
    environment: 'testnet',
    baseline_as_of: '2026-07-16T00:00:00Z',
    version: 1,
    created_by_email: null,
    updated_by_email: null,
    forked_from_plan_id: null,
    created_at: '2026-07-16T00:00:00Z',
    updated_at: '2026-07-16T00:00:00Z',
    change_count: changes.length,
    ...over,
    changes,
  }
}

function makeChange(id: string, planId: string, over: Partial<PlanChange> = {}): PlanChange {
  return {
    id,
    plan_id: planId,
    seq: 10,
    op_type: 'add_device',
    ref_device_pk: null,
    ref_link_pk: null,
    new_device_pk: null,
    local_ref: null,
    payload: {},
    ref_snapshot: {},
    target_date: null,
    assignee_note: null,
    state: 'pending',
    version: 1,
    created_by_email: null,
    created_at: '2026-07-16T00:00:00Z',
    updated_at: '2026-07-16T00:00:00Z',
    ...over,
  }
}

// Fresh QueryClient per test so cache/state never leaks between cases. Returns the
// client too, so tests can drive a background refetch via qc.invalidateQueries.
function makeWrapper(initialEntry = '/?plan=P1') {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <PlannerProvider>{children}</PlannerProvider>
      </MemoryRouter>
    </QueryClientProvider>
  )
  return { wrapper, qc }
}

beforeEach(() => {
  // resetAllMocks (not clearAllMocks) also drains any leftover mock*Once queues,
  // so a queued once-value can't leak into a later test.
  vi.resetAllMocks()
  vi.mocked(fetchTopology).mockResolvedValue(EMPTY_BASELINE)
})

describe('PlannerContext', () => {
  // Finding 1 (CRITICAL): reload/conflict recovery must re-sync state.plan to the
  // fresh server version so the retried save uses the new version, not a stale one
  // that would 409 forever.
  it('re-syncs to the fresh server version after a 409, so the next save succeeds', async () => {
    const store: Record<string, Plan> = { P1: makePlan('P1', { version: 1 }) }
    vi.mocked(fetchPlan).mockImplementation(async (id: string) => makePlan(id, store[id]))

    const { wrapper } = makeWrapper()
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.version).toBe(1))

    // Someone else advanced the server to v2; our next PATCH (with v1) will 409.
    store.P1 = makePlan('P1', { version: 2 })
    vi.mocked(updatePlan).mockRejectedValueOnce(new PlanConflictError())

    await act(async () => {
      await result.current.savePlanMeta({ name: 'renamed' })
    })

    // The 409 raised the banner AND reloaded to server truth (v2).
    await waitFor(() => {
      expect(result.current.conflict).toBe(true)
      expect(result.current.plan?.version).toBe(2)
    })
    // First attempt used the stale version 1.
    expect(vi.mocked(updatePlan)).toHaveBeenNthCalledWith(1, 'P1', { name: 'renamed' }, 1)

    // Retry: now succeeds because state.plan holds the fresh v2 (no infinite 409).
    vi.mocked(updatePlan).mockResolvedValueOnce(makePlan('P1', { version: 3, name: 'renamed' }))
    await act(async () => {
      await result.current.savePlanMeta({ name: 'renamed' })
    })

    expect(vi.mocked(updatePlan)).toHaveBeenLastCalledWith('P1', { name: 'renamed' }, 2)
    await waitFor(() => expect(result.current.plan?.version).toBe(3))
  })

  // Finding 2 (IMPORTANT): a failed patchChange must roll back the optimistic edit,
  // not leave it shown as if saved.
  it('rolls back the optimistic patch when patchChange fails', async () => {
    const c1 = makeChange('c1', 'P1', { assignee_note: 'orig', version: 1 })
    vi.mocked(fetchPlan).mockImplementation(async (id: string) =>
      makePlan(id, { version: 1, changes: [c1] })
    )

    const { wrapper } = makeWrapper()
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.changes[0]?.assignee_note).toBe('orig'))

    vi.mocked(updatePlanChange).mockRejectedValueOnce(new Error('boom'))

    await act(async () => {
      await expect(
        result.current.patchChange('c1', { assignee_note: 'edited' })
      ).rejects.toThrow('boom')
    })

    // Optimistic 'edited' was reverted to the pre-mutation value.
    expect(result.current.plan?.changes[0]?.assignee_note).toBe('orig')
    expect(result.current.saving).toBe(false)
  })

  // Finding 3 (IMPORTANT): a response for the plan we started on must not land after
  // the user has switched to a different plan.
  it('drops a mid-save response when the user switched plans (captured-id guard)', async () => {
    const store: Record<string, Plan> = {
      P1: makePlan('P1', { version: 1, changes: [] }),
      P2: makePlan('P2', { version: 1, changes: [] }),
    }
    vi.mocked(fetchPlan).mockImplementation(async (id: string) => makePlan(id, store[id]))

    const { wrapper } = makeWrapper('/?plan=P1')
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.id).toBe('P1'))

    // addPlanChange stays pending until we resolve it by hand.
    let resolveAdd: (c: PlanChange) => void = () => {}
    vi.mocked(addPlanChange).mockReturnValueOnce(
      new Promise<PlanChange>((res) => {
        resolveAdd = res
      })
    )

    let addPromise: Promise<void>
    act(() => {
      addPromise = result.current.addChange({
        op_type: 'add_device',
        payload: {},
        ref_snapshot: {},
      })
    })
    // Optimistic temp change is shown on P1.
    expect(result.current.plan?.changes.length).toBe(1)

    // Switch to P2 before the save resolves.
    act(() => {
      result.current.openPlan('P2')
    })
    await waitFor(() => expect(result.current.plan?.id).toBe('P2'))
    expect(result.current.plan?.changes.length).toBe(0)

    // Resolve the P1 save now; the guard must drop it (no contamination of P2).
    await act(async () => {
      resolveAdd(makeChange('cServer', 'P1'))
      await addPromise
    })

    expect(result.current.plan?.id).toBe('P2')
    expect(result.current.plan?.changes.length).toBe(0)
  })

  // Round-2 defect: a background refetch (window focus / staleTime / touchPlan
  // bumping updated_at) that lands mid-mutation must NOT wholesale-replace state.plan
  // and revert the in-flight optimistic edit. Once the mutation settles, the deferred
  // server truth is then adopted.
  it('does not revert an in-flight optimistic edit when a background refetch lands, then adopts fresh truth after settle', async () => {
    let serverPlan: Plan = makePlan('P1', {
      version: 1,
      changes: [makeChange('c1', 'P1', { assignee_note: 'orig', version: 1 })],
    })
    // fetchPlan is used for the initial load and for endMutation's reload after settle.
    vi.mocked(fetchPlan).mockImplementation(async () => serverPlan)

    const { wrapper, qc } = makeWrapper()
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.changes[0]?.assignee_note).toBe('orig'))

    // Hold the PATCH open so the mutation stays in flight while the refetch lands.
    let resolvePatch: (c: PlanChange) => void = () => {}
    vi.mocked(updatePlanChange).mockImplementationOnce(
      () =>
        new Promise<PlanChange>((res) => {
          resolvePatch = res
        })
    )

    let patchPromise: Promise<void>
    act(() => {
      patchPromise = result.current.patchChange('c1', { assignee_note: 'edited' })
    })
    // Optimistic edit is shown.
    expect(result.current.plan?.changes[0]?.assignee_note).toBe('edited')

    // A background refresh of the SAME plan lands mid-flight (new signature: v2).
    // setQueryData updates the cache; the setTimeout(0) inside the async act lets
    // React Query's re-render land NOW, while the patch is provably still in flight
    // (resolvePatch has not been called yet).
    await act(async () => {
      qc.setQueryData(
        ['plan', 'P1'],
        makePlan('P1', {
          version: 2,
          updated_at: '2026-07-16T01:00:00Z',
          changes: [makeChange('c1', 'P1', { assignee_note: 'orig', version: 1 })],
        })
      )
      await new Promise((r) => setTimeout(r, 0))
    })

    // CRITICAL: the background refresh did NOT wholesale-replace state.plan; the
    // in-flight optimistic edit survives.
    expect(result.current.plan?.changes[0]?.assignee_note).toBe('edited')

    // Server now holds the edit at a fresh version (v3); endMutation's reload fetches it.
    serverPlan = makePlan('P1', {
      version: 3,
      updated_at: '2026-07-16T02:00:00Z',
      changes: [makeChange('c1', 'P1', { assignee_note: 'edited', version: 2 })],
    })
    await act(async () => {
      resolvePatch(makeChange('c1', 'P1', { assignee_note: 'edited', version: 2 }))
      await patchPromise
    })

    // After settle, the deferred resync adopts fresh server truth (v3, edit intact).
    await waitFor(() => expect(result.current.plan?.version).toBe(3))
    expect(result.current.plan?.changes[0]?.assignee_note).toBe('edited')
  })

  // FIX B: reorderChanges must call the new bulk reorder endpoint exactly once
  // (not one PATCH per change), and adopt the returned plan (esp. its bumped
  // version) so a subsequent save doesn't 409.
  it('reorderChanges calls reorderPlanChanges once and adopts the returned plan', async () => {
    const c1 = makeChange('c1', 'P1', { seq: 10, version: 1 })
    const c2 = makeChange('c2', 'P1', { seq: 20, version: 1 })
    vi.mocked(fetchPlan).mockImplementation(async (id: string) =>
      makePlan(id, { version: 1, changes: [c1, c2] })
    )

    const { wrapper } = makeWrapper()
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.changes.length).toBe(2))

    const updatedPlan = makePlan('P1', {
      version: 2,
      changes: [
        makeChange('c2', 'P1', { seq: 10, version: 2 }),
        makeChange('c1', 'P1', { seq: 20, version: 2 }),
      ],
    })
    vi.mocked(reorderPlanChanges).mockResolvedValueOnce(updatedPlan)

    await act(async () => {
      await result.current.reorderChanges(['c2', 'c1'])
    })

    expect(vi.mocked(reorderPlanChanges)).toHaveBeenCalledTimes(1)
    expect(vi.mocked(reorderPlanChanges)).toHaveBeenCalledWith('P1', ['c2', 'c1'])
    expect(vi.mocked(updatePlanChange)).not.toHaveBeenCalled()
    expect(result.current.plan?.changes.map((c) => c.id)).toEqual(['c2', 'c1'])
    expect(result.current.plan?.version).toBe(2)
  })

  it('reorderChanges rolls back to the pre-reorder order on failure', async () => {
    const c1 = makeChange('c1', 'P1', { seq: 10, version: 1 })
    const c2 = makeChange('c2', 'P1', { seq: 20, version: 1 })
    vi.mocked(fetchPlan).mockImplementation(async (id: string) =>
      makePlan(id, { version: 1, changes: [c1, c2] })
    )

    const { wrapper } = makeWrapper()
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.changes.length).toBe(2))

    vi.mocked(reorderPlanChanges).mockRejectedValueOnce(new Error('boom'))

    await act(async () => {
      await expect(result.current.reorderChanges(['c2', 'c1'])).rejects.toThrow('boom')
    })

    expect(result.current.plan?.changes.map((c) => c.id)).toEqual(['c1', 'c2'])
    expect(result.current.saving).toBe(false)
  })

  // FIX C: the Action List tab's query must be invalidated after a successful
  // change mutation so it stops disagreeing with the live-recomputing Impact tab.
  it('invalidates the plan-action-list query after a successful change mutation', async () => {
    const c1 = makeChange('c1', 'P1', { assignee_note: 'orig', version: 1 })
    vi.mocked(fetchPlan).mockImplementation(async (id: string) =>
      makePlan(id, { version: 1, changes: [c1] })
    )
    vi.mocked(updatePlanChange).mockResolvedValueOnce(
      makeChange('c1', 'P1', { assignee_note: 'edited', version: 2 })
    )

    const { wrapper, qc } = makeWrapper()
    const invalidateSpy = vi.spyOn(qc, 'invalidateQueries')
    const { result } = renderHook(() => usePlanner(), { wrapper })
    await waitFor(() => expect(result.current.plan?.changes[0]?.assignee_note).toBe('orig'))

    await act(async () => {
      await result.current.patchChange('c1', { assignee_note: 'edited' })
    })

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['plan-action-list', 'P1'] })
  })
})
