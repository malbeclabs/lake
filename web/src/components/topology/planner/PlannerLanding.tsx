// Landing view shown when the planner has no plan open. Replaces the old bare
// "Create a new plan or open an existing one" hint with a real CTA plus a
// browsable list of existing plans, each previewed via a small SVG thumbnail
// (see PlanPreviewThumb) so the user can recognize a plan at a glance.
import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Plus, FolderOpen, Loader2, MapIcon } from 'lucide-react'
import { fetchPlans, fetchPlan, type PlanSummary, type TopologyResponse } from '@/lib/api'
import { useTheme } from '@/hooks/use-theme'
import { usePlanner } from './PlannerContext'
import { PlanPickerDialog } from './PlanPickerDialog'
import { statusBadgeClass } from './toolbar-util'
import { countPendingChanges, formatChangeSummary, totalPendingChanges } from './plan-summary'
import { buildPlanPreview } from './plan-preview'
import { PlanPreviewThumb, PREVIEW_VIEW_W, PREVIEW_VIEW_H } from './PlanPreviewThumb'

// Beyond this many plans, later cards skip the per-plan changes fetch (summary +
// preview thumbnail) so a huge plan list doesn't fire dozens of requests at
// once. Every plan still appears in the list and stays clickable -- only the
// heavier per-card detail is capped.
const PREVIEW_FETCH_LIMIT = 24

function formatTimeAgo(isoString: string): string {
  const date = new Date(isoString)
  if (isNaN(date.getTime())) return 'unknown'
  const diffSecs = Math.floor((Date.now() - date.getTime()) / 1000)
  if (diffSecs < 60) return 'just now'
  if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`
  if (diffSecs < 86400) return `${Math.floor(diffSecs / 3600)}h ago`
  const days = Math.floor(diffSecs / 86400)
  if (days < 30) return `${days}d ago`
  return `${Math.floor(days / 30)}mo ago`
}

function PlanCard({
  plan,
  baseline,
  isDark,
  fetchDetail,
  onOpen,
}: {
  plan: PlanSummary
  baseline: TopologyResponse | null
  isDark: boolean
  fetchDetail: boolean
  onOpen: () => void
}) {
  // Same query key PlannerContext uses when a plan is actually opened, so
  // opening this card from cache-warm state is instant.
  const { data: full, isLoading } = useQuery({
    queryKey: ['plan', plan.id],
    queryFn: () => fetchPlan(plan.id),
    enabled: fetchDetail,
    staleTime: 10_000,
  })

  const counts = useMemo(() => (full ? countPendingChanges(full.changes) : null), [full])
  const geometry = useMemo(() => {
    if (!baseline || !full) return null
    return buildPlanPreview(baseline, full.changes, PREVIEW_VIEW_W, PREVIEW_VIEW_H)
  }, [baseline, full])

  const summaryText = counts
    ? totalPendingChanges(counts) > 0
      ? formatChangeSummary(counts)
      : 'No pending changes'
    : fetchDetail
      ? isLoading
        ? 'Loading changes…'
        : 'Changes unavailable'
      : 'Open to see changes'

  return (
    <button
      type="button"
      onClick={onOpen}
      className="flex flex-col text-left rounded-lg border border-border bg-card overflow-hidden hover:border-purple-400 hover:shadow-md transition-all"
    >
      <div className="h-28 w-full">
        {fetchDetail ? (
          <PlanPreviewThumb geometry={geometry} isDark={isDark} />
        ) : (
          <div className="flex h-full w-full items-center justify-center bg-muted/40 text-[11px] text-muted-foreground">
            Preview not loaded
          </div>
        )}
      </div>
      <div className="flex min-w-0 flex-col gap-1.5 p-3">
        <div className="flex items-center justify-between gap-2">
          <span className="truncate text-sm font-semibold">{plan.name}</span>
          <span className={statusBadgeClass(plan.status)}>{plan.status}</span>
        </div>
        <p className="truncate text-xs text-muted-foreground">{summaryText}</p>
        <p className="text-[11px] text-muted-foreground">Updated {formatTimeAgo(plan.updated_at)}</p>
      </div>
    </button>
  )
}

export function PlannerLanding() {
  const { baseline, newPlan, openPlan } = usePlanner()
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const [picking, setPicking] = useState(false)

  const { data: plans, isLoading } = useQuery({
    queryKey: ['plans'],
    queryFn: fetchPlans,
    staleTime: 10_000,
  })

  const handleCreate = async () => {
    const name = window.prompt('New plan name')
    if (name) await newPlan(name)
  }

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="flex flex-col items-center gap-3 px-4 pb-8 pt-12 text-center">
        <MapIcon className="h-8 w-8 text-muted-foreground" />
        <h1 className="text-lg font-semibold">No plan open</h1>
        <p className="max-w-md text-sm text-muted-foreground">
          Create a new topology plan, or open one of the plans below.
        </p>
        <div className="mt-2 flex items-center gap-2">
          <button
            type="button"
            onClick={handleCreate}
            className="flex items-center gap-1.5 rounded-md bg-purple-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-purple-500"
          >
            <Plus className="h-4 w-4" />
            Create new plan
          </button>
          <button
            type="button"
            onClick={() => setPicking(true)}
            className="flex items-center gap-1.5 rounded-md border border-border px-4 py-2 text-sm text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          >
            <FolderOpen className="h-4 w-4" />
            Open
          </button>
        </div>
      </div>

      <div className="mx-auto max-w-5xl px-6 pb-10">
        <h2 className="mb-3 text-sm font-semibold text-muted-foreground">
          Existing plans{plans && plans.length > 0 ? ` (${plans.length})` : ''}
        </h2>
        {isLoading ? (
          <div className="flex items-center justify-center py-10 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin" />
          </div>
        ) : !plans || plans.length === 0 ? (
          <p className="py-6 text-sm text-muted-foreground">
            No plans yet. Create your first plan above.
          </p>
        ) : (
          <>
            {plans.length > PREVIEW_FETCH_LIMIT && (
              <p className="mb-2 text-[11px] text-muted-foreground">
                Showing full previews for the first {PREVIEW_FETCH_LIMIT} plans. Open any plan to
                see its full change list.
              </p>
            )}
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {plans.map((p, i) => (
                <PlanCard
                  key={p.id}
                  plan={p}
                  baseline={baseline}
                  isDark={isDark}
                  fetchDetail={i < PREVIEW_FETCH_LIMIT}
                  onOpen={() => openPlan(p.id)}
                />
              ))}
            </div>
          </>
        )}
      </div>

      {picking && <PlanPickerDialog onPick={openPlan} onClose={() => setPicking(false)} />}
    </div>
  )
}
