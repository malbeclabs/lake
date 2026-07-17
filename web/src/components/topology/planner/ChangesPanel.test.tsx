import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { ChangesPanel } from './ChangesPanel'
import type { DriftStatus, Plan, PlanChange } from '@/lib/api'

// Shared, hoisted ref so the mocked PlannerContext can read the value each test sets.
const { plannerRef } = vi.hoisted(() => ({
  plannerRef: { current: null as unknown as ReturnType<typeof makePlanner> },
}))

vi.mock('./PlannerContext', () => ({
  usePlanner: () => plannerRef.current,
}))

function ch(over: Partial<PlanChange>): PlanChange {
  return {
    id: 'c', plan_id: 'p', seq: 10, op_type: 'remove_link',
    payload: {}, ref_snapshot: {}, state: 'pending', version: 1,
    target_date: null, assignee_note: null,
    created_at: '', updated_at: '', ...over,
  } as PlanChange
}

function makePlan(changes: PlanChange[]): Plan {
  return {
    id: 'p', name: 'Plan', description: '', status: 'draft', environment: 'testnet',
    baseline_as_of: '', version: 1, created_by_email: null, updated_by_email: null,
    forked_from_plan_id: null, created_at: '', updated_at: '',
    change_count: changes.length, changes,
  }
}

function makePlanner(changes: PlanChange[], drift: Map<string, DriftStatus> = new Map()) {
  return {
    plan: makePlan(changes),
    drift,
    patchChange: vi.fn(),
    removeChange: vi.fn(),
    reorderChanges: vi.fn(),
  }
}

describe('ChangesPanel note editing', () => {
  it('commits the note as a single patch on blur, with the final value', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'nyc-lon1' } }),
    ])
    plannerRef.current = planner
    render(<ChangesPanel />)

    const note = screen.getByPlaceholderText(/Note/)
    // Simulate typing character by character.
    fireEvent.change(note, { target: { value: 'O' } })
    fireEvent.change(note, { target: { value: 'OP' } })
    fireEvent.change(note, { target: { value: 'OPS-42' } })

    // No PATCH fires while typing.
    expect(planner.patchChange).not.toHaveBeenCalled()

    fireEvent.blur(note)

    // Exactly one PATCH, carrying the final value.
    expect(planner.patchChange).toHaveBeenCalledTimes(1)
    expect(planner.patchChange).toHaveBeenCalledWith('c1', { assignee_note: 'OPS-42' })
  })

  it('does not patch on blur when the note is unchanged', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, assignee_note: 'kept' }),
    ])
    plannerRef.current = planner
    render(<ChangesPanel />)

    const note = screen.getByDisplayValue('kept')
    fireEvent.blur(note)
    expect(planner.patchChange).not.toHaveBeenCalled()
  })

  it('keeps each row note isolated and commits under the right change id', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'aaa' }, assignee_note: 'note-a' }),
      ch({ id: 'c2', seq: 20, op_type: 'remove_link', ref_snapshot: { link_code: 'bbb' }, assignee_note: 'note-b' }),
    ])
    plannerRef.current = planner
    render(<ChangesPanel />)

    const rowA = screen.getByDisplayValue('note-a')
    const rowB = screen.getByDisplayValue('note-b')

    // Typing into row A must not leak into row B.
    fireEvent.change(rowA, { target: { value: 'note-a-edited' } })
    expect(rowB).toHaveValue('note-b')

    fireEvent.blur(rowA)
    expect(planner.patchChange).toHaveBeenCalledTimes(1)
    expect(planner.patchChange).toHaveBeenCalledWith('c1', { assignee_note: 'note-a-edited' })
  })
})

describe('ChangesPanel drift', () => {
  it('shows no summary banner and no badge when every change is pending', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'aaa' } }),
    ], new Map([['c1', 'pending']]))
    plannerRef.current = planner
    render(<ChangesPanel />)

    expect(screen.queryByText(/broken/)).not.toBeInTheDocument()
    expect(screen.queryByText(/already done/i)).not.toBeInTheDocument()
  })

  it('shows a "Broken" badge on a change whose reference vanished', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'aaa' } }),
    ], new Map([['c1', 'broken']]))
    plannerRef.current = planner
    render(<ChangesPanel />)

    expect(screen.getByText('Broken')).toBeInTheDocument()
  })

  it('shows an "Already done" badge on a change already reflected in live topology', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'aaa' } }),
    ], new Map([['c1', 'already_done']]))
    plannerRef.current = planner
    render(<ChangesPanel />)

    expect(screen.getByText('Already done')).toBeInTheDocument()
  })

  it('summarizes broken and already-done counts in a header banner', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'aaa' } }),
      ch({ id: 'c2', seq: 20, op_type: 'remove_link', ref_snapshot: { link_code: 'bbb' } }),
      ch({ id: 'c3', seq: 30, op_type: 'remove_link', ref_snapshot: { link_code: 'ccc' } }),
    ], new Map([
      ['c1', 'broken'],
      ['c2', 'already_done'],
      ['c3', 'pending'],
    ]))
    plannerRef.current = planner
    render(<ChangesPanel />)

    expect(screen.getByText('1 broken')).toBeInTheDocument()
    expect(screen.getByText('1 already done vs live topology')).toBeInTheDocument()
  })

  it('treats a change absent from the drift map as pending (no badge)', () => {
    const planner = makePlanner([
      ch({ id: 'c1', seq: 10, op_type: 'remove_link', ref_snapshot: { link_code: 'aaa' } }),
    ], new Map())
    plannerRef.current = planner
    render(<ChangesPanel />)

    expect(screen.queryByText('Broken')).not.toBeInTheDocument()
    expect(screen.queryByText('Already done')).not.toBeInTheDocument()
  })
})
