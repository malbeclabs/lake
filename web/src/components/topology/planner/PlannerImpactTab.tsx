import { useMemo } from 'react'
import { usePlanner } from './PlannerContext'
import { usePlanImpact } from './use-plan-impact'
import { PlannerImpactPanel } from './PlannerImpactPanel'
import { changeShortLabel } from './impact-format'

/**
 * Connected Impact tab: reads the draft from PlannerContext, recomputes impact
 * (debounced) against the current draft, and renders the impact panel.
 */
export function PlannerImpactTab() {
  const { plan } = usePlanner()
  const changes = plan?.changes ?? []
  const planId = plan?.id ?? null
  const { report, isLoading, error } = usePlanImpact(planId, changes)

  const changeLabels = useMemo(() => {
    const m = new Map<number, string>()
    for (const c of changes) {
      m.set(c.seq, changeShortLabel({ ...c, ref_snapshot: c.ref_snapshot as Record<string, unknown> }))
    }
    return m
  }, [changes])

  if (!planId) {
    return <div className="p-3 text-xs text-muted-foreground">Save the plan to compute impact.</div>
  }

  return (
    <PlannerImpactPanel report={report} isLoading={isLoading} error={error} changeLabels={changeLabels} />
  )
}
