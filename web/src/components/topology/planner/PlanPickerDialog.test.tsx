import { describe, it, expect, vi, beforeEach, type Mock } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { PlanSummary } from '@/lib/api'

vi.mock('@/lib/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/api')>()
  return { ...actual, fetchPlans: vi.fn(), deletePlan: vi.fn() }
})

import { fetchPlans, deletePlan } from '@/lib/api'
import { PlanPickerDialog } from './PlanPickerDialog'

const mockFetchPlans = fetchPlans as unknown as Mock
const mockDeletePlan = deletePlan as unknown as Mock

function summary(over: Partial<PlanSummary>): PlanSummary {
  return {
    id: 'p1', name: 'Warsaw expansion', description: '', status: 'draft',
    environment: 'testnet', baseline_as_of: '', version: 1,
    created_by_email: null, updated_by_email: null, forked_from_plan_id: null,
    created_at: '2026-07-01T00:00:00Z', updated_at: '2026-07-01T00:00:00Z',
    change_count: 0, ...over,
  }
}

function renderDialog(onPick = vi.fn(), onClose = vi.fn()) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <PlanPickerDialog onPick={onPick} onClose={onClose} />
    </QueryClientProvider>
  )
  return { onPick, onClose }
}

describe('PlanPickerDialog', () => {
  beforeEach(() => {
    mockFetchPlans.mockReset()
    mockDeletePlan.mockReset()
  })

  it('picks a plan when its row is clicked', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    const { onPick, onClose } = renderDialog()

    const row = await screen.findByText('Warsaw expansion')
    fireEvent.click(row)

    expect(onPick).toHaveBeenCalledWith('p1')
    expect(onClose).toHaveBeenCalled()
  })

  it('deletes a plan from its trash button without picking it', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    mockDeletePlan.mockResolvedValue(undefined)
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true)
    const { onPick, onClose } = renderDialog()

    const trash = await screen.findByTitle('Delete plan')
    fireEvent.click(trash)

    expect(confirmSpy).toHaveBeenCalledWith(
      'Delete plan "Warsaw expansion"? It will be removed from your list.'
    )
    await waitFor(() => expect(mockDeletePlan).toHaveBeenCalledWith('p1'))
    expect(onPick).not.toHaveBeenCalled()
    expect(onClose).not.toHaveBeenCalled()
    confirmSpy.mockRestore()
  })

  it('does not delete a plan when the confirm dialog is dismissed', async () => {
    mockFetchPlans.mockResolvedValue([summary({ id: 'p1', name: 'Warsaw expansion' })])
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(false)
    renderDialog()

    const trash = await screen.findByTitle('Delete plan')
    fireEvent.click(trash)

    await waitFor(() => expect(confirmSpy).toHaveBeenCalled())
    expect(mockDeletePlan).not.toHaveBeenCalled()
    confirmSpy.mockRestore()
  })
})
