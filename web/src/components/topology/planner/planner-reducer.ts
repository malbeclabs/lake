import type { Plan, PlanChange } from '@/lib/api'

export type PlannerTool = 'select' | 'add-device' | 'remove-device' | 'add-link'

export interface PlannerState {
  plan: Plan | null
  tool: PlannerTool
  selectedLinkKey: string | null
  saving: boolean
  conflict: boolean
}

export const initialPlannerState: PlannerState = {
  plan: null,
  tool: 'select',
  selectedLinkKey: null,
  saving: false,
  conflict: false,
}

export type PlannerAction =
  | { type: 'setPlan'; plan: Plan | null }
  | { type: 'setTool'; tool: PlannerTool }
  | { type: 'selectLink'; key: string | null }
  | { type: 'upsertChange'; change: PlanChange }
  | { type: 'replaceChange'; tempId: string; change: PlanChange }
  | { type: 'patchChange'; id: string; patch: Partial<PlanChange> }
  | { type: 'removeChange'; id: string }
  | { type: 'setChanges'; changes: PlanChange[] }
  | { type: 'setSaving'; saving: boolean }
  | { type: 'setConflict'; conflict: boolean }

export function nextSeq(changes: PlanChange[]): number {
  if (changes.length === 0) return 10
  return Math.max(...changes.map((c) => c.seq)) + 10
}

// Reorder to the given id order and renumber seq to a fresh 10,20,30 sequence.
export function reorderedSeq(orderedIds: string[], changes: PlanChange[]): PlanChange[] {
  const byId = new Map(changes.map((c) => [c.id, c]))
  return orderedIds
    .map((id) => byId.get(id))
    .filter((c): c is PlanChange => !!c)
    .map((c, i) => ({ ...c, seq: (i + 1) * 10 }))
}

function withChanges(plan: Plan | null, changes: PlanChange[]): Plan | null {
  if (!plan) return plan
  return { ...plan, changes, change_count: changes.length }
}

export function plannerReducer(state: PlannerState, action: PlannerAction): PlannerState {
  switch (action.type) {
    case 'setPlan':
      return { ...state, plan: action.plan, selectedLinkKey: null }

    case 'setTool':
      return { ...state, tool: action.tool, selectedLinkKey: null }

    case 'selectLink':
      return { ...state, selectedLinkKey: action.key }

    case 'upsertChange': {
      if (!state.plan) return state
      const existing = state.plan.changes.some((c) => c.id === action.change.id)
      const changes = existing
        ? state.plan.changes.map((c) => (c.id === action.change.id ? action.change : c))
        : [...state.plan.changes, action.change]
      return { ...state, plan: withChanges(state.plan, changes) }
    }

    case 'replaceChange': {
      if (!state.plan) return state
      const hasTemp = state.plan.changes.some((c) => c.id === action.tempId)
      if (hasTemp) {
        const changes = state.plan.changes.map((c) =>
          c.id === action.tempId ? action.change : c
        )
        return { ...state, plan: withChanges(state.plan, changes) }
      }
      // The temp change is gone (e.g. a reload landed between the optimistic
      // add and the server response). Don't drop the server-persisted change:
      // append it, unless one with the same server id is already present.
      const hasServer = state.plan.changes.some((c) => c.id === action.change.id)
      if (hasServer) return state
      const changes = [...state.plan.changes, action.change]
      return { ...state, plan: withChanges(state.plan, changes) }
    }

    case 'patchChange': {
      if (!state.plan) return state
      const changes = state.plan.changes.map((c) =>
        c.id === action.id ? { ...c, ...action.patch } : c
      )
      return { ...state, plan: withChanges(state.plan, changes) }
    }

    case 'removeChange': {
      if (!state.plan) return state
      const changes = state.plan.changes.filter((c) => c.id !== action.id)
      return { ...state, plan: withChanges(state.plan, changes) }
    }

    case 'setChanges':
      return { ...state, plan: withChanges(state.plan, action.changes) }

    case 'setSaving':
      return { ...state, saving: action.saving }

    case 'setConflict':
      return { ...state, conflict: action.conflict }

    default:
      return state
  }
}
