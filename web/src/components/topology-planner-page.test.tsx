import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'

vi.mock('@/hooks/use-is-ops-user', () => ({ useIsOpsUser: () => true }))

vi.mock('@/components/topology/planner/PlannerContext', () => ({
  PlannerProvider: ({ children }: { children: React.ReactNode }) => children,
  usePlanner: vi.fn(),
}))
vi.mock('@/components/topology/planner/PlannerToolbar', () => ({
  PlannerToolbar: () => <div>toolbar-stub</div>,
}))
vi.mock('@/components/topology/planner/PlannerMap', () => ({
  PlannerMap: () => <div>map-stub</div>,
}))
vi.mock('@/components/topology/planner/PlannerLanding', () => ({
  PlannerLanding: () => <div>landing-stub</div>,
}))
vi.mock('@/components/topology/planner/ConflictBanner', () => ({
  ConflictBanner: () => null,
}))
vi.mock('@/components/topology/planner/ChangesPanel', () => ({
  ChangesPanel: () => <div>changes-panel-content</div>,
}))
vi.mock('@/components/topology/planner/ActionListPanel', () => ({
  ActionListPanel: ({ planId }: { planId: string }) => (
    <div>action-list-content-for-{planId}</div>
  ),
}))
vi.mock('@/components/topology/planner/PlannerImpactTab', () => ({
  PlannerImpactTab: () => <div>impact-tab-content</div>,
}))

import { usePlanner } from '@/components/topology/planner/PlannerContext'
import { TopologyPlannerPage } from './topology-planner-page'

const mockPlanner = usePlanner as unknown as ReturnType<typeof vi.fn>

describe('TopologyPlannerPage right-panel tabs', () => {
  beforeEach(() => {
    mockPlanner.mockReset()
    mockPlanner.mockReturnValue({ plan: { id: 'p1', changes: [] }, loading: false })
  })

  it('defaults to the Changes tab and hides the other two panels', () => {
    render(<TopologyPlannerPage />)
    expect(screen.getByText('changes-panel-content')).toBeInTheDocument()
    expect(screen.queryByText(/action-list-content/)).not.toBeInTheDocument()
    expect(screen.queryByText('impact-tab-content')).not.toBeInTheDocument()
  })

  it('switches to the Action List tab and passes the plan id', () => {
    render(<TopologyPlannerPage />)
    fireEvent.click(screen.getByRole('button', { name: /action list/i }))
    expect(screen.getByText('action-list-content-for-p1')).toBeInTheDocument()
    expect(screen.queryByText('changes-panel-content')).not.toBeInTheDocument()
    expect(screen.queryByText('impact-tab-content')).not.toBeInTheDocument()
  })

  it('switches to the Impact tab', () => {
    render(<TopologyPlannerPage />)
    fireEvent.click(screen.getByRole('button', { name: /^impact$/i }))
    expect(screen.getByText('impact-tab-content')).toBeInTheDocument()
    expect(screen.queryByText('changes-panel-content')).not.toBeInTheDocument()
    expect(screen.queryByText(/action-list-content/)).not.toBeInTheDocument()
  })

  it('does not render the tab panels when no plan is loaded, and shows the landing view instead', () => {
    mockPlanner.mockReturnValue({ plan: null, loading: false })
    render(<TopologyPlannerPage />)
    expect(screen.queryByText('changes-panel-content')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /action list/i })).not.toBeInTheDocument()
    expect(screen.getByText('landing-stub')).toBeInTheDocument()
  })
})
