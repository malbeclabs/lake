import { describe, it, expect, vi, beforeEach, afterEach, type Mock } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import type { PlanImpactReport, PlanChange } from '@/lib/api'

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return { ...actual, fetchPlanImpact: vi.fn() }
})

import { fetchPlanImpact } from '@/lib/api'
import { usePlanImpact } from './use-plan-impact'

const mockImpact = fetchPlanImpact as unknown as Mock

const sampleReport: PlanImpactReport = {
  partition_issues: [],
  latency_deltas: [],
  redundancy_changes: [],
  capacity_risks: [],
  overlap_warnings: [],
  data_issues: [],
  estimated: false,
  generated_at: 'x',
}

const ch = (id: string) => [{ id }] as unknown as PlanChange[]

interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (reason?: unknown) => void
}

// A promise whose resolution we control manually, so a test can decide the
// order in which overlapping in-flight requests settle.
function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

// Flush the microtask queue enough times to drain the hook's
// .then().catch().finally() chain (each link is a separate tick). Works with
// fake timers because vitest does not fake native promise microtasks.
async function flushMicrotasks() {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve()
  }
}

beforeEach(() => {
  vi.useFakeTimers()
  mockImpact.mockReset()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('usePlanImpact', () => {
  it('does not fetch when planId is null', async () => {
    const { result } = renderHook(() => usePlanImpact(null, [], 300))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })
    expect(mockImpact).not.toHaveBeenCalled()
    expect(result.current.report).toBeNull()
    expect(result.current.isLoading).toBe(false)
  })

  it('computes impact after the debounce window', async () => {
    mockImpact.mockResolvedValue(sampleReport)
    const { result } = renderHook(() => usePlanImpact('p1', [], 300))

    expect(result.current.isLoading).toBe(true)
    expect(mockImpact).not.toHaveBeenCalled()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })

    expect(mockImpact).toHaveBeenCalledTimes(1)
    expect(mockImpact).toHaveBeenCalledWith('p1', [])
    expect(result.current.report).toEqual(sampleReport)
    expect(result.current.isLoading).toBe(false)
  })

  it('debounces rapid draft edits into a single request with the latest changes', async () => {
    mockImpact.mockResolvedValue(sampleReport)
    const { rerender } = renderHook(({ changes }) => usePlanImpact('p1', changes, 300), {
      initialProps: { changes: ch('a') },
    })

    rerender({ changes: ch('b') })
    rerender({ changes: ch('c') })

    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })

    expect(mockImpact).toHaveBeenCalledTimes(1)
    expect(mockImpact).toHaveBeenLastCalledWith('p1', ch('c'))
  })

  it('surfaces fetch errors', async () => {
    mockImpact.mockRejectedValue(new Error('kaboom'))
    const { result } = renderHook(() => usePlanImpact('p1', [], 300))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })

    expect(result.current.error).toBe('kaboom')
    expect(result.current.isLoading).toBe(false)
  })

  it('ignores a stale response that resolves out of order', async () => {
    const deferredA = createDeferred<PlanImpactReport>()
    const deferredB = createDeferred<PlanImpactReport>()
    const reportA: PlanImpactReport = { ...sampleReport, generated_at: 'A' }
    const reportB: PlanImpactReport = { ...sampleReport, generated_at: 'B' }
    mockImpact
      .mockReturnValueOnce(deferredA.promise)
      .mockReturnValueOnce(deferredB.promise)

    const { result, rerender } = renderHook(
      ({ changes }) => usePlanImpact('p1', changes, 300),
      { initialProps: { changes: ch('a') } },
    )

    // Fire request A (debounce elapses) but leave it unresolved.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })
    expect(mockImpact).toHaveBeenNthCalledWith(1, 'p1', ch('a'))

    // Edit the draft -> fire request B while A is still in flight.
    rerender({ changes: ch('b') })
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })
    expect(mockImpact).toHaveBeenNthCalledWith(2, 'p1', ch('b'))

    // Settle B first (the current request), then A LAST (out of order).
    await act(async () => {
      deferredB.resolve(reportB)
      await flushMicrotasks()
    })
    expect(result.current.report).toEqual(reportB)
    expect(result.current.isLoading).toBe(false)

    await act(async () => {
      deferredA.resolve(reportA)
      await flushMicrotasks()
    })

    // A's late response is stale and must be discarded; B's result stands.
    expect(result.current.report).toEqual(reportB)
    expect(result.current.isLoading).toBe(false)
  })

  it('discards a late response after planId is cleared', async () => {
    const deferredA = createDeferred<PlanImpactReport>()
    const reportA: PlanImpactReport = { ...sampleReport, generated_at: 'A' }
    mockImpact.mockReturnValueOnce(deferredA.promise)

    const { result, rerender } = renderHook(
      ({ planId }) => usePlanImpact(planId, [], 300),
      { initialProps: { planId: 'p1' as string | null } },
    )

    // Fire request A but leave it unresolved.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(300)
    })
    expect(mockImpact).toHaveBeenCalledTimes(1)
    expect(result.current.isLoading).toBe(true)

    // Clear planId while A is still in flight: the hook resets to idle.
    rerender({ planId: null })
    expect(result.current.report).toBeNull()
    expect(result.current.isLoading).toBe(false)

    // A resolves late. The request-id ref (bumped by the null branch) makes the
    // hook ignore it, so no stale report and no error appear.
    await act(async () => {
      deferredA.resolve(reportA)
      await flushMicrotasks()
    })
    expect(result.current.report).toBeNull()
    expect(result.current.error).toBeNull()
    expect(result.current.isLoading).toBe(false)
  })
})
