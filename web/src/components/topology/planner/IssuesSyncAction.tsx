import { useState, useEffect } from 'react'
import { useMutation } from '@tanstack/react-query'
import { CircleDot, Loader2, X, ExternalLink, AlertCircle, RefreshCw } from 'lucide-react'
import {
  previewPlanIssues,
  syncPlanIssues,
  type IssuePreviewItem,
  type SyncedIssue,
  type PlanStatus,
} from '@/lib/api'
import { summarizeIssuePreview } from './issues-preview'
import { canCreateIssues } from './toolbar-util'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

interface IssuesSyncActionProps {
  planId: string
  planStatus: string
}

// Shared row-labeling helpers for both the preview and synced issue lists: a
// decom issue (device_decom / link_decom) has no contributor code, so it is
// keyed and labeled by its own kind + entity rather than by contributor.
type IssueRow = Pick<IssuePreviewItem, 'is_parent' | 'kind' | 'entity_pk' | 'contributor_pk' | 'contributor_code' | 'title'>

function isDecomKind(kind: string): boolean {
  return kind === 'device_decom' || kind === 'link_decom'
}

function issueRowKey(row: IssueRow): string {
  if (row.is_parent) return '__parent__'
  if (row.entity_pk) return `${row.kind}:${row.entity_pk}`
  return row.contributor_pk
}

function issueRowLabel(row: IssueRow): string {
  if (row.is_parent) return 'Parent tracking issue'
  if (isDecomKind(row.kind)) return row.title
  return row.contributor_code
}

export function IssuesSyncAction({ planId, planStatus }: IssuesSyncActionProps) {
  const [open, setOpen] = useState(false)
  const isApproved = canCreateIssues(planStatus as PlanStatus)

  return (
    <>
      <Button
        variant="outline"
        size="sm"
        disabled={!isApproved}
        title={isApproved ? 'Create or sync GitHub issues' : 'Approve the plan to create issues'}
        onClick={() => setOpen(true)}
      >
        <CircleDot className="mr-2 h-4 w-4" />
        Create / Sync issues
      </Button>
      {open && <IssuesSyncDialog planId={planId} onClose={() => setOpen(false)} />}
    </>
  )
}

function IssuesSyncDialog({ planId, onClose }: { planId: string; onClose: () => void }) {
  const preview = useMutation({ mutationFn: () => previewPlanIssues(planId) })
  const sync = useMutation({ mutationFn: () => syncPlanIssues(planId) })

  // Load the preview once when the dialog mounts. useEffect (not a useState lazy
  // initializer) so the mutation runs after commit and behaves correctly under
  // React StrictMode's mount/unmount/mount cycle. preview.mutate is stable.
  const previewMutate = preview.mutate
  useEffect(() => {
    previewMutate()
  }, [previewMutate])

  const previews: IssuePreviewItem[] = preview.data?.issues ?? []
  const summary = summarizeIssuePreview(previews)
  const synced: SyncedIssue[] = sync.data?.issues ?? []

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="w-full max-w-2xl overflow-hidden rounded-lg border border-border bg-background shadow-xl">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="flex items-center gap-2 font-medium">
            <CircleDot className="h-4 w-4" />
            Create / Sync GitHub issues
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="max-h-[60vh] overflow-y-auto px-4 py-3">
          {preview.isPending && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Loading preview...
            </div>
          )}
          {preview.isError && (
            <div className="flex items-center gap-2 text-sm text-red-500">
              <AlertCircle className="h-4 w-4" /> {(preview.error as Error).message}
            </div>
          )}

          {synced.length === 0 && previews.length > 0 && (
            <>
              <p className="mb-3 text-sm text-muted-foreground">
                Repo <code>{preview.data?.repo}</code>: {summary.creates} to create, {summary.updates} to update.
              </p>
              <ul className="space-y-2">
                {previews.map((p) => (
                  <li
                    key={issueRowKey(p)}
                    className="flex items-center justify-between rounded border border-border px-3 py-2 text-sm"
                  >
                    <span className="flex items-center gap-2">
                      {isDecomKind(p.kind) && (
                        <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
                          decom
                        </span>
                      )}
                      {issueRowLabel(p)}
                    </span>
                    <span
                      className={cn(
                        'rounded px-2 py-0.5 text-xs',
                        p.action === 'update'
                          ? 'bg-amber-500/15 text-amber-600'
                          : 'bg-emerald-500/15 text-emerald-600'
                      )}
                    >
                      {p.action}
                    </span>
                  </li>
                ))}
              </ul>
            </>
          )}

          {synced.length > 0 && (
            <ul className="space-y-2">
              {synced.map((s) => (
                <li
                  key={issueRowKey(s)}
                  className="flex items-center justify-between rounded border border-border px-3 py-2 text-sm"
                >
                  <span className="flex items-center gap-2">
                    {isDecomKind(s.kind) && (
                      <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
                        decom
                      </span>
                    )}
                    {issueRowLabel(s)}{' '}
                    <span className="text-xs text-muted-foreground">({s.action})</span>
                  </span>
                  <a
                    href={s.issue_url}
                    target="_blank"
                    rel="noreferrer"
                    className="flex items-center gap-1 text-blue-500 hover:underline"
                  >
                    #{s.issue_number} <ExternalLink className="h-3 w-3" />
                  </a>
                </li>
              ))}
            </ul>
          )}
          {sync.isError && (
            <div className="mt-2 flex items-center gap-2 text-sm text-red-500">
              <AlertCircle className="h-4 w-4" /> {(sync.error as Error).message}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 border-t border-border px-4 py-3">
          <Button variant="ghost" size="sm" onClick={onClose}>
            {synced.length > 0 ? 'Close' : 'Cancel'}
          </Button>
          {synced.length === 0 && (
            <Button
              size="sm"
              disabled={previews.length === 0 || sync.isPending || !!sync.data}
              onClick={() => {
                // Guard against a double confirm: only sync once.
                if (sync.isPending || sync.data) return
                sync.mutate()
              }}
            >
              {sync.isPending ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="mr-2 h-4 w-4" />
              )}
              Confirm: {summary.creates} create, {summary.updates} update
            </Button>
          )}
        </div>
      </div>
    </div>
  )
}
