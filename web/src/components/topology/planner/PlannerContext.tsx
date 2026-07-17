/* eslint-disable react-refresh/only-export-components */
import {
  createContext,
  useContext,
  useReducer,
  useCallback,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import { useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  fetchTopology,
  fetchPlan,
  createPlan,
  updatePlan,
  addPlanChange,
  updatePlanChange,
  deletePlanChange,
  reorderPlanChanges,
  PlanConflictError,
  type Plan,
  type PlanChange,
  type PlanStatus,
  type NewChangeInput,
  type DriftStatus,
  type TopologyResponse,
} from '@/lib/api'
import {
  plannerReducer,
  initialPlannerState,
  nextSeq,
  reorderedSeq,
  type PlannerTool,
} from './planner-reducer'
import { buildDraft, type DraftTopology } from './draft'
import { annotateDrift } from './drift'

export interface PlannerContextValue {
  loading: boolean
  baseline: TopologyResponse | null
  plan: Plan | null
  draft: DraftTopology | null
  drift: Map<string, DriftStatus>
  tool: PlannerTool
  setTool: (t: PlannerTool) => void
  selectedLinkKey: string | null
  selectLink: (key: string | null) => void
  saving: boolean
  conflict: boolean
  dismissConflict: () => void
  newPlan: (name: string) => Promise<void>
  openPlan: (id: string) => void
  closePlan: () => void
  addChange: (input: NewChangeInput) => Promise<void>
  patchChange: (
    id: string,
    patch: Partial<
      Pick<
        PlanChange,
        'payload' | 'ref_snapshot' | 'target_date' | 'assignee_note' | 'state' | 'seq'
      >
    >
  ) => Promise<void>
  removeChange: (id: string) => Promise<void>
  reorderChanges: (orderedIds: string[]) => Promise<void>
  savePlanMeta: (patch: {
    name?: string
    description?: string
    status?: PlanStatus
  }) => Promise<void>
  reload: () => Promise<void>
  focusChange: (changeId: string) => void
  focusRequest: { changeId: string; nonce: number } | null
}

const PlannerContext = createContext<PlannerContextValue | null>(null)

export function PlannerProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const planId = searchParams.get('plan')
  const qc = useQueryClient()
  const [state, dispatch] = useReducer(plannerReducer, initialPlannerState)

  // Live topology baseline (shared cache key with the rest of the app).
  const { data: baseline, isLoading: baselineLoading } = useQuery({
    queryKey: ['topology'],
    queryFn: fetchTopology,
    staleTime: 60_000,
  })

  // Plan load. Kept in React Query so reload() is a simple invalidate.
  const { data: loadedPlan, isLoading: planLoading } = useQuery({
    queryKey: ['plan', planId],
    queryFn: () => fetchPlan(planId as string),
    enabled: !!planId,
    staleTime: 10_000,
  })

  // The id of the plan currently intended by the URL. Async mutators capture the
  // id they started against and compare it to this ref at resolution time, so a
  // response for plan A never lands after the user has switched to plan B.
  const planIdRef = useRef(planId)
  planIdRef.current = planId

  // Count of mutations awaiting a server response, and a flag remembering that a
  // background refetch of the SAME plan arrived while one was in flight. Both are
  // refs (not state) because they gate a render-body decision, not the UI.
  const inFlightRef = useRef(0)
  const pendingResyncRef = useRef(false)

  // Sync the reducer whenever React Query hands us a *different* server plan.
  // Keyed on id + version + updated_at (not id alone), so a reload() of the SAME
  // id after a 409 re-syncs state.plan to the fresh server version — otherwise the
  // stale version would 409 forever. Optimistic dispatches never change loadedPlan
  // (we don't write the query cache), so they are not clobbered by this guard.
  const lastSyncedRef = useRef<string | null>(null)
  const loadedSig = loadedPlan
    ? `${loadedPlan.id}:${loadedPlan.version}:${loadedPlan.updated_at}`
    : null
  if (loadedPlan && loadedSig !== lastSyncedRef.current) {
    // A plan *switch* (different id) is a deliberate navigation and always wins;
    // the mutator plan-switch guard keeps the old save from contaminating it.
    const sameId = state.plan?.id === loadedPlan.id
    if (sameId && inFlightRef.current > 0) {
      // Same plan, background refetch mid-mutation (window focus / staleTime /
      // touchPlan bumping updated_at). Do NOT wholesale-replace state.plan — that
      // would revert the in-flight optimistic edit. Record this stale signature so
      // it isn't adopted, and remember to reload to fresh truth once the mutation
      // settles (endMutation), which then picks up our own write + any collaborator's.
      lastSyncedRef.current = loadedSig
      pendingResyncRef.current = true
    } else {
      lastSyncedRef.current = loadedSig
      dispatch({ type: 'setPlan', plan: loadedPlan })
    }
  }
  const activePlan = state.plan && state.plan.id === planId ? state.plan : loadedPlan ?? null

  const draft = useMemo<DraftTopology | null>(() => {
    if (!baseline) return null
    return buildDraft(baseline, activePlan?.changes ?? [])
  }, [baseline, activePlan])

  const drift = useMemo<Map<string, DriftStatus>>(() => {
    if (!baseline || !activePlan) return new Map()
    return annotateDrift(baseline, activePlan.changes)
  }, [baseline, activePlan])

  // Focus bus: clicking a change in the Changes panel asks the map to fly there.
  // The nonce guarantees a NEW request object on every call, even a repeat click on
  // the same change, so the map's effect (keyed on this object) always re-fires.
  const [focusRequest, setFocusRequest] = useState<{ changeId: string; nonce: number } | null>(
    null
  )
  const focusChange = useCallback((changeId: string) => {
    setFocusRequest((prev) => ({ changeId, nonce: (prev?.nonce ?? 0) + 1 }))
  }, [])

  const setTool = useCallback((t: PlannerTool) => dispatch({ type: 'setTool', tool: t }), [])
  const selectLink = useCallback(
    (key: string | null) => dispatch({ type: 'selectLink', key }),
    []
  )
  const dismissConflict = useCallback(
    () => dispatch({ type: 'setConflict', conflict: false }),
    []
  )

  const reload = useCallback(async () => {
    dispatch({ type: 'setConflict', conflict: false })
    // Refetch forces loadedPlan to change, which re-syncs the reducer to server
    // truth (fresh version) via the signature guard above.
    await qc.invalidateQueries({ queryKey: ['plan', planIdRef.current] })
  }, [qc])

  const openPlan = useCallback(
    (id: string) => {
      setSearchParams((prev) => {
        prev.set('plan', id)
        return prev
      })
    },
    [setSearchParams]
  )

  const closePlan = useCallback(() => {
    setSearchParams((prev) => {
      prev.delete('plan')
      return prev
    })
  }, [setSearchParams])

  const newPlan = useCallback(
    async (name: string) => {
      const created = await createPlan({ name })
      // Record the synced signature so the follow-up GET (the query enables once
      // the URL carries ?plan=) doesn't re-dispatch over the just-created plan.
      lastSyncedRef.current = `${created.id}:${created.version}:${created.updated_at}`
      dispatch({ type: 'setPlan', plan: created })
      setSearchParams((prev) => {
        prev.set('plan', created.id)
        return prev
      })
    },
    [setSearchParams]
  )

  const handleConflict = useCallback(
    (e: unknown) => {
      if (e instanceof PlanConflictError) {
        // Surface the conflict banner AND reload to server truth. The refetch
        // re-syncs state.plan (fresh version) so the retried save no longer 409s.
        // The banner stays up (setPlan does not clear it) until dismissConflict.
        // This intentionally adopts server truth: the mutation FAILED, so there is
        // no competing successful optimistic edit to clobber. endMutation's finally
        // decrements inFlight synchronously before this async refetch re-renders,
        // so the resulting sync is not deferred.
        dispatch({ type: 'setConflict', conflict: true })
        void qc.invalidateQueries({ queryKey: ['plan', planIdRef.current] })
        return true
      }
      return false
    },
    [qc]
  )

  // Bracket every mutation so the render-body knows when an optimistic edit is in
  // flight. beginMutation is called before the first await; endMutation runs in a
  // finally on both resolve and error. When the last mutation settles and a
  // background refetch was deferred, reload to fresh server truth.
  const beginMutation = useCallback(() => {
    inFlightRef.current += 1
  }, [])

  const endMutation = useCallback(() => {
    inFlightRef.current = Math.max(0, inFlightRef.current - 1)
    if (inFlightRef.current === 0 && pendingResyncRef.current) {
      pendingResyncRef.current = false
      void qc.invalidateQueries({ queryKey: ['plan', planIdRef.current] })
    }
  }, [qc])

  // The Action List tab derives from plan.changes server-side; without this it
  // keeps showing pre-mutation data until an unrelated remount refetches it.
  const invalidateActionList = useCallback(
    (planId: string) => {
      void qc.invalidateQueries({ queryKey: ['plan-action-list', planId] })
    },
    [qc]
  )

  const addChange = useCallback(
    async (input: NewChangeInput) => {
      const plan = state.plan
      if (!plan) return
      const actingId = plan.id
      const tempId = `tmp_${crypto.randomUUID()}`
      const optimistic: PlanChange = {
        id: tempId,
        plan_id: plan.id,
        seq: nextSeq(plan.changes),
        op_type: input.op_type,
        ref_device_pk: input.ref_device_pk ?? null,
        ref_link_pk: input.ref_link_pk ?? null,
        new_device_pk: input.new_device_pk ?? null,
        local_ref: input.local_ref ?? null,
        payload: input.payload,
        ref_snapshot: input.ref_snapshot,
        target_date: input.target_date ?? null,
        assignee_note: input.assignee_note ?? null,
        state: 'pending',
        version: 1,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }
      dispatch({ type: 'upsertChange', change: optimistic })
      dispatch({ type: 'setSaving', saving: true })
      beginMutation()
      try {
        const saved = await addPlanChange(plan.id, input)
        // Dropped if the user switched plans mid-save (would contaminate the new plan).
        if (planIdRef.current !== actingId) return
        dispatch({ type: 'replaceChange', tempId, change: saved })
        invalidateActionList(actingId)
      } catch (e) {
        if (planIdRef.current !== actingId) return
        dispatch({ type: 'removeChange', id: tempId })
        if (!handleConflict(e)) throw e
      } finally {
        dispatch({ type: 'setSaving', saving: false })
        endMutation()
      }
    },
    [state.plan, handleConflict, beginMutation, endMutation, invalidateActionList]
  )

  const patchChange = useCallback(
    async (
      id: string,
      patch: Partial<
        Pick<
          PlanChange,
          'payload' | 'ref_snapshot' | 'target_date' | 'assignee_note' | 'state' | 'seq'
        >
      >
    ) => {
      const plan = state.plan
      if (!plan) return
      const actingId = plan.id
      const before = plan.changes
      const current = plan.changes.find((c) => c.id === id)
      if (!current) return
      dispatch({ type: 'patchChange', id, patch })
      dispatch({ type: 'setSaving', saving: true })
      beginMutation()
      try {
        const saved = await updatePlanChange(plan.id, id, patch, current.version)
        if (planIdRef.current !== actingId) return
        dispatch({ type: 'patchChange', id, patch: saved })
        invalidateActionList(actingId)
      } catch (e) {
        if (planIdRef.current !== actingId) return
        // Roll back the optimistic patch so a failed edit isn't left shown as saved.
        dispatch({ type: 'setChanges', changes: before })
        if (!handleConflict(e)) throw e
      } finally {
        dispatch({ type: 'setSaving', saving: false })
        endMutation()
      }
    },
    [state.plan, handleConflict, beginMutation, endMutation, invalidateActionList]
  )

  const removeChange = useCallback(
    async (id: string) => {
      const plan = state.plan
      if (!plan) return
      const actingId = plan.id
      const prev = plan.changes
      dispatch({ type: 'removeChange', id })
      beginMutation()
      try {
        await deletePlanChange(plan.id, id)
        invalidateActionList(actingId)
      } catch (e) {
        if (planIdRef.current !== actingId) return
        dispatch({ type: 'setChanges', changes: prev })
        throw e
      } finally {
        endMutation()
      }
    },
    [state.plan, beginMutation, endMutation, invalidateActionList]
  )

  const reorderChanges = useCallback(
    async (orderedIds: string[]) => {
      const plan = state.plan
      if (!plan) return
      const actingId = plan.id
      const before = plan.changes
      const reordered = reorderedSeq(orderedIds, plan.changes)
      dispatch({ type: 'setChanges', changes: reordered })
      dispatch({ type: 'setSaving', saving: true })
      beginMutation()
      try {
        // Single bulk call: rewrites all seqs server-side in one transaction and
        // returns the updated plan (version bumped), replacing the old
        // one-PATCH-per-change loop.
        const updated = await reorderPlanChanges(plan.id, orderedIds)
        if (planIdRef.current !== actingId) return
        // Adopt the server's canonical order/seq + bumped version so a subsequent
        // save doesn't 409 against the now-stale pre-reorder version.
        dispatch({ type: 'setPlan', plan: updated })
        invalidateActionList(actingId)
      } catch (e) {
        if (planIdRef.current !== actingId) return
        // Roll back the optimistic reorder so a failed reorder isn't shown as saved.
        dispatch({ type: 'setChanges', changes: before })
        if (!handleConflict(e)) throw e
      } finally {
        dispatch({ type: 'setSaving', saving: false })
        endMutation()
      }
    },
    [state.plan, handleConflict, beginMutation, endMutation, invalidateActionList]
  )

  const savePlanMeta = useCallback(
    async (patch: { name?: string; description?: string; status?: PlanStatus }) => {
      const plan = state.plan
      if (!plan) return
      const actingId = plan.id
      dispatch({ type: 'setSaving', saving: true })
      beginMutation()
      try {
        const saved = await updatePlan(plan.id, patch, plan.version)
        if (planIdRef.current !== actingId) return
        dispatch({ type: 'setPlan', plan: { ...saved, changes: plan.changes } })
      } catch (e) {
        if (planIdRef.current !== actingId) return
        if (!handleConflict(e)) throw e
      } finally {
        dispatch({ type: 'setSaving', saving: false })
        endMutation()
      }
    },
    [state.plan, handleConflict, beginMutation, endMutation]
  )

  const value: PlannerContextValue = {
    loading: baselineLoading || (!!planId && planLoading),
    baseline: baseline ?? null,
    plan: activePlan,
    draft,
    drift,
    tool: state.tool,
    setTool,
    selectedLinkKey: state.selectedLinkKey,
    selectLink,
    saving: state.saving,
    conflict: state.conflict,
    dismissConflict,
    newPlan,
    openPlan,
    closePlan,
    addChange,
    patchChange,
    removeChange,
    reorderChanges,
    savePlanMeta,
    reload,
    focusChange,
    focusRequest,
  }

  return <PlannerContext.Provider value={value}>{children}</PlannerContext.Provider>
}

export function usePlanner(): PlannerContextValue {
  const ctx = useContext(PlannerContext)
  if (!ctx) throw new Error('usePlanner must be used within a PlannerProvider')
  return ctx
}
