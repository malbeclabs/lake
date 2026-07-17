import { useEffect, useRef, useState } from 'react'
import { fetchPlanImpact } from '@/lib/api'
import type { PlanImpactReport, PlanChange } from '@/lib/api'

const DEFAULT_DEBOUNCE_MS = 600

export interface UsePlanImpactResult {
  report: PlanImpactReport | null
  isLoading: boolean
  error: string | null
}

/**
 * Recompute plan impact whenever the draft changes, debounced. Posts the
 * current draft (including unsaved edits) for live preview and ignores stale
 * responses so fast edits never render an out-of-order report.
 */
export function usePlanImpact(
  planId: string | null,
  changes: PlanChange[],
  debounceMs: number = DEFAULT_DEBOUNCE_MS,
): UsePlanImpactResult {
  const [report, setReport] = useState<PlanImpactReport | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const reqId = useRef(0)

  // Re-run only when the draft's content actually changes.
  const changesKey = JSON.stringify(changes)

  useEffect(() => {
    if (!planId) {
      reqId.current++
      setReport(null)
      setError(null)
      setIsLoading(false)
      return
    }

    setIsLoading(true)
    setError(null)
    const myReq = ++reqId.current

    const timer = setTimeout(() => {
      fetchPlanImpact(planId, changes)
        .then((r) => {
          if (myReq !== reqId.current) return
          setReport(r)
          setError(null)
        })
        .catch((e) => {
          if (myReq !== reqId.current) return
          setError(e instanceof Error ? e.message : 'Failed to compute impact')
        })
        .finally(() => {
          if (myReq !== reqId.current) return
          setIsLoading(false)
        })
    }, debounceMs)

    return () => clearTimeout(timer)
    // changesKey stands in for the changes array identity.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [planId, changesKey, debounceMs])

  return { report, isLoading, error }
}
