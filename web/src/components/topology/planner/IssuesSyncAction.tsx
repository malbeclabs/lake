import { useState, useEffect } from 'react'
import { useMutation } from '@tanstack/react-query'
import { FileText, Loader2, X, ExternalLink, AlertCircle, Copy, Check } from 'lucide-react'
import { previewPlanIssues, type IssuePreviewItem } from '@/lib/api'
import { Button } from '@/components/ui/button'

interface IssuesSyncActionProps {
  planId: string
  changeCount: number
}

// A decom issue (device_decom / link_decom) has no contributor code, so it is
// chipped differently from a per-contributor issue.
function isDecomKind(kind: string): boolean {
  return kind === 'device_decom' || kind === 'link_decom'
}

function issueRowKey(item: IssuePreviewItem, index: number): string {
  if (item.entity_pk) return `${item.kind}:${item.entity_pk}`
  return item.contributor_pk || `${item.kind}:${index}`
}

// Builds a pre-filled "new issue" GitHub URL. GitHub caps URL length, so a
// long decom body may be truncated by the browser/server before it reaches
// the form; the Copy button is the reliable path regardless.
function githubNewIssueURL(repo: string, item: IssuePreviewItem): string {
  return `https://github.com/${repo}/issues/new?title=${encodeURIComponent(item.title)}&body=${encodeURIComponent(item.body)}&labels=${encodeURIComponent(item.labels.join(','))}`
}

export function IssuesSyncAction({ planId, changeCount }: IssuesSyncActionProps) {
  const [open, setOpen] = useState(false)
  const hasChanges = changeCount > 0

  return (
    <>
      <Button
        variant="outline"
        size="sm"
        disabled={!hasChanges}
        title={hasChanges ? 'Create GitHub issues for this plan' : 'Add a change to create issues'}
        onClick={() => setOpen(true)}
      >
        <FileText className="mr-2 h-4 w-4" />
        Create issues
      </Button>
      {open && <IssuesDialog planId={planId} onClose={() => setOpen(false)} />}
    </>
  )
}

function IssueRow({ item, repo }: { item: IssuePreviewItem; repo: string }) {
  const [expanded, setExpanded] = useState(false)
  const [copied, setCopied] = useState(false)

  const copy = () => {
    void navigator.clipboard.writeText(`${item.title}\n\n${item.body}`)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <li className="rounded border border-border">
      <div className="flex items-center justify-between gap-2 px-3 py-2">
        <span className="flex min-w-0 items-center gap-2 text-sm">
          <span className="shrink-0 rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
            {isDecomKind(item.kind) ? 'decom' : 'contributor'}
          </span>
          <span className="truncate font-medium">{item.title}</span>
        </span>
        <div className="flex shrink-0 items-center gap-1">
          <button
            onClick={copy}
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs text-muted-foreground hover:text-foreground"
          >
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
            {copied ? 'Copied' : 'Copy'}
          </button>
          <a
            href={githubNewIssueURL(repo, item)}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs text-blue-500 hover:underline"
          >
            <ExternalLink className="h-3 w-3" />
            Open in GitHub
          </a>
        </div>
      </div>
      <div className="border-t border-border px-3 py-1.5">
        <button
          onClick={() => setExpanded((e) => !e)}
          className="text-xs text-muted-foreground underline hover:text-foreground"
        >
          {expanded ? 'Hide body' : 'Show body'}
        </button>
        {expanded && (
          <pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded bg-muted p-2 font-mono text-xs">
            {item.body}
          </pre>
        )}
      </div>
    </li>
  )
}

function IssuesDialog({ planId, onClose }: { planId: string; onClose: () => void }) {
  const preview = useMutation({ mutationFn: () => previewPlanIssues(planId) })

  // Load the preview once when the dialog mounts. useEffect (not a useState lazy
  // initializer) so the mutation runs after commit and behaves correctly under
  // React StrictMode's mount/unmount/mount cycle. preview.mutate is stable.
  const previewMutate = preview.mutate
  useEffect(() => {
    previewMutate()
  }, [previewMutate])

  const previews: IssuePreviewItem[] = preview.data?.issues ?? []
  const repo = preview.data?.repo ?? ''
  const [copiedAll, setCopiedAll] = useState(false)

  const copyAll = () => {
    const text = previews.map((p) => `${p.title}\n\n${p.body}`).join('\n\n---\n\n')
    void navigator.clipboard.writeText(text)
    setCopiedAll(true)
    setTimeout(() => setCopiedAll(false), 2000)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="w-full max-w-2xl overflow-hidden rounded-lg border border-border bg-background shadow-xl">
        <div className="flex items-center justify-between border-b border-border px-4 py-3">
          <div className="flex items-center gap-2 font-medium">
            <FileText className="h-4 w-4" />
            Create GitHub issues
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

          {!preview.isPending && !preview.isError && previews.length === 0 && (
            <p className="text-sm text-muted-foreground">No issues to create for this plan.</p>
          )}

          {previews.length > 0 && (
            <>
              <p className="mb-3 text-xs text-muted-foreground">
                Repo <code>{repo}</code>. Open in GitHub pre-fills the form; if a body is truncated, use Copy
                instead.
              </p>
              <ul className="space-y-2">
                {previews.map((p, i) => (
                  <IssueRow key={issueRowKey(p, i)} item={p} repo={repo} />
                ))}
              </ul>
            </>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 border-t border-border px-4 py-3">
          {previews.length > 0 && (
            <Button variant="ghost" size="sm" onClick={copyAll}>
              {copiedAll ? <Check className="mr-2 h-4 w-4" /> : <Copy className="mr-2 h-4 w-4" />}
              {copiedAll ? 'Copied all' : 'Copy all'}
            </Button>
          )}
          <Button variant="ghost" size="sm" onClick={onClose}>
            Close
          </Button>
        </div>
      </div>
    </div>
  )
}
