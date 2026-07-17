import { describe, it, expect, vi, beforeEach, afterEach, type Mock } from 'vitest'
import { render, screen, act } from '@testing-library/react'

vi.mock('./PlannerContext', () => ({ usePlanner: vi.fn() }))
vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return { ...actual, fetchPlanImpact: vi.fn() }
})

import { usePlanner } from './PlannerContext'
import { fetchPlanImpact } from '@/lib/api'
import { PlannerImpactTab } from './PlannerImpactTab'

const mockPlanner = usePlanner as unknown as Mock
const mockImpact = fetchPlanImpact as unknown as Mock

beforeEach(() => {
  vi.useFakeTimers()
  mockPlanner.mockReset()
  mockImpact.mockReset()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('PlannerImpactTab', () => {
  it('prompts to save when the plan is unsaved', () => {
    mockPlanner.mockReturnValue({ plan: null })
    render(<PlannerImpactTab />)
    expect(screen.getByText(/save the plan/i)).toBeInTheDocument()
    expect(mockImpact).not.toHaveBeenCalled()
  })

  it('recomputes impact for the saved plan against the current draft', async () => {
    const changes = [
      { id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { code: 'chi-nyc-1' } },
    ]
    // usePlanner's established shape (PlannerContext.tsx) nests changes under
    // plan.changes -- there is no separate top-level `changes` field.
    mockPlanner.mockReturnValue({ plan: { id: 'p1', changes } })
    mockImpact.mockResolvedValue({
      partition_issues: [],
      latency_deltas: [],
      redundancy_changes: [],
      capacity_risks: [],
      overlap_warnings: [],
      data_issues: [],
      estimated: false,
      generated_at: 'x',
    })

    render(<PlannerImpactTab />)

    await act(async () => {
      await vi.advanceTimersByTimeAsync(700)
    })

    expect(mockImpact).toHaveBeenCalledWith('p1', changes)
    expect(screen.getByText(/no impact detected/i)).toBeInTheDocument()
  })
})
