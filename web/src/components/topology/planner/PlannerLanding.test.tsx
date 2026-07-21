import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { Plan, PlanChange, PlanSummary, TopologyResponse } from '@/lib/api'

vi.mock('./PlannerContext', () => ({ usePlanner: vi.fn() }))
vi.mock('@/hooks/use-theme', () => ({
  useTheme: () => ({ resolvedTheme: 'light', theme: 'light', setTheme: () => {} }),
}))
vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return { ...actual, fetchPlans: vi.fn(), fetchPlan: vi.fn(), deletePlan: vi.fn() }
})

import { usePlanner } from './PlannerContext'
import { fetchPlans, fetchPlan, deletePlan } from '@/lib/api'
import { PlannerLanding } from './PlannerLanding'

const mockPlanner = usePlanner as unknown as Mock
const mockFetchPlans = fetchPlans as unknown as Mock
const mockFetchPlan = fetchPlan as unknown as Mock
const mockDeletePlan = deletePlan as unknown as Mock

const BASELINE: TopologyResponse = {
  metros: [
    { pk: 'mA', code: 'nyc', name: 'NYC', latitude: 40.7, longitude: -74 },
    { pk: 'mB', code: 'lon', name: 'LON', latitude: 51.5, longitude: -0.1 },
  ],
  devices: [
    { pk: 'dA', code: 'nyc-x1', metro_pk: 'mA', contributor_pk: 'c1' },
    { pk: 'dB', code: 'lon-x1', metro_pk: 'mB', contributor_pk: 'c2' },
  ],
  links: [],
  validators: [],
} as unknown as TopologyResponse

function ch(over: Partial<PlanChange>): PlanChange {
  return {
    id: 'c', plan_id: 'p', seq: 10, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    created_at: '', updated_at: '', ...over,
  } as PlanChange
}

function summary(over: Partial<PlanSummary>): PlanSummary {
  return {
    id: 'p1', name: 'Warsaw expansion', description: '', status: 'draft',
    environment: 'testnet', baseline_as_of: '', version: 1,
    created_by_email: null, updated_by_email: null, forked_from_plan_id: null,
    created_at: '2026-07-01T00:00:00Z', updated_at: '2026-07-01T00:00:00Z',
    change_count: 0, ...over,
  }
}

function planFor(s: PlanSummary, changes: PlanChange[]): Plan {
  return { ...s, changes, change_count: changes.length }
}

function renderLanding() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <PlannerLanding />
    </QueryClientProvider>
  )
}

describe('PlannerLanding', () => {
  const newPlan = vi.fn()
  const openPlan = vi.fn()

  beforeEach(() => {
    mockFetchPlans.mockReset()
    mockFetchPlan.mockReset()
    mockDeletePlan.mockReset()
    newPlan.mockReset()
    openPlan.mockReset()
    mockPlanner.mockReset()
    mockPlanner.mockReturnValue({ baseline: BASELINE, newPlan, openPlan })
  })

  it('shows the create-new CTA and creates a plan from a prompt', async () => {
    mockFetchPlans.mockResolvedValue([])
    const promptSpy = vi.spyOn(window, 'prompt').mockReturnValue('New Tokyo plan')
    renderLanding()

    const cta = await screen.findByRole('button', { name: /create new plan/i })
    fireEvent.click(cta)

    await waitFor(() => expect(newPlan).toHaveBeenCalledWith('New Tokyo plan'))
    promptSpy.mockRestore()
  })

  it('shows an empty state when there are no plans yet', async () => {
    mockFetchPlans.mockResolvedValue([])
    renderLanding()
    expect(await screen.findByText(/no plans yet/i)).toBeInTheDocument()
  })

  it('lists existing plans with name, status and updated time', async () => {
    mockFetchPlans.mockResolvedValue([
      summary({ id: 'p1', name: 'Warsaw expansion', status: 'draft' }),
      summary({ id: 'p2', name: 'Tokyo decom', status: 'approved' }),
    ])
    mockFetchPlan.mockResolvedValue(planFor(summary({ id: 'p1' }), []))
    renderLanding()

    expect(await screen.findByText('Warsaw expansion')).toBeInTheDocument()
    expect(screen.getByText('Tokyo decom')).toBeInTheDocument()
    expect(screen.getByText('draft')).toBeInTheDocument()
    expect(screen.getByText('approved')).toBeInTheDocument()
  })

  it('opens a plan when its card is clicked', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    mockFetchPlan.mockResolvedValue(planFor(summary({ id: 'p1' }), []))
    renderLanding()

    const card = await screen.findByText('Warsaw expansion')
    fireEvent.click(card)
    expect(openPlan).toHaveBeenCalledWith('p1')
  })

  it('renders the pending-change summary counts for a plan', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    mockFetchPlan.mockResolvedValue(
      planFor(summary({ id: 'p1' }), [
        ch({ id: 'c1', op_type: 'add_device' }),
        ch({ id: 'c2', op_type: 'add_device' }),
        ch({ id: 'c3', op_type: 'remove_link', ref_link_pk: 'L1' }),
        // Not pending -> excluded from the summary.
        ch({ id: 'c4', op_type: 'remove_device', state: 'done' }),
      ])
    )
    renderLanding()

    expect(await screen.findByText('2 devices added, 1 link removed')).toBeInTheDocument()
  })

  it('shows a neutral placeholder for a plan with no pending changes', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Empty plan example' })])
    mockFetchPlan.mockResolvedValue(planFor(summary({ id: 'p1' }), []))
    renderLanding()

    expect(await screen.findByText('No pending changes')).toBeInTheDocument()
    expect(await screen.findByText('Empty plan')).toBeInTheDocument()
  })

  it('deletes a plan from its trash button without opening it', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    mockFetchPlan.mockResolvedValue(planFor(summary({ id: 'p1' }), []))
    mockDeletePlan.mockResolvedValue(undefined)
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true)
    renderLanding()

    const trash = await screen.findByTitle('Delete plan')
    fireEvent.click(trash)

    expect(confirmSpy).toHaveBeenCalledWith(
      'Delete plan "Warsaw expansion"? It will be removed from your list.'
    )
    await waitFor(() => expect(mockDeletePlan).toHaveBeenCalledWith('p1'))
    expect(openPlan).not.toHaveBeenCalled()
    confirmSpy.mockRestore()
  })

  it('does not delete a plan when the confirm dialog is dismissed', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    mockFetchPlan.mockResolvedValue(planFor(summary({ id: 'p1' }), []))
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false)
    renderLanding()

    const trash = await screen.findByTitle('Delete plan')
    fireEvent.click(trash)

    await waitFor(() => expect(confirmSpy).toHaveBeenCalled())
    expect(mockDeletePlan).not.toHaveBeenCalled()
    confirmSpy.mockRestore()
  })
})
