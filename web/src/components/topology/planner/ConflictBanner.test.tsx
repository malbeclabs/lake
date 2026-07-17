import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { ConflictBanner } from './ConflictBanner'

// Shared, hoisted ref so the mocked PlannerContext can read the value each test sets.
const { plannerRef } = vi.hoisted(() => ({
  plannerRef: { current: null as unknown as ReturnType<typeof makePlanner> },
}))

vi.mock('./PlannerContext', () => ({
  usePlanner: () => plannerRef.current,
}))

function makePlanner(conflict: boolean) {
  return {
    conflict,
    reload: vi.fn(),
    dismissConflict: vi.fn(),
  }
}

describe('ConflictBanner', () => {
  it('renders nothing when there is no conflict', () => {
    plannerRef.current = makePlanner(false)
    const { container } = render(<ConflictBanner />)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the reload prompt when a conflict is present', () => {
    plannerRef.current = makePlanner(true)
    render(<ConflictBanner />)
    expect(
      screen.getByText('Someone else changed this plan. Reload to get the latest.')
    ).toBeInTheDocument()
  })

  it('calls reload when "Reload" is clicked', () => {
    const planner = makePlanner(true)
    plannerRef.current = planner
    render(<ConflictBanner />)
    fireEvent.click(screen.getByText('Reload'))
    expect(planner.reload).toHaveBeenCalledTimes(1)
  })

  it('calls dismissConflict when "Dismiss" is clicked', () => {
    const planner = makePlanner(true)
    plannerRef.current = planner
    render(<ConflictBanner />)
    fireEvent.click(screen.getByText('Dismiss'))
    expect(planner.dismissConflict).toHaveBeenCalledTimes(1)
  })
})
