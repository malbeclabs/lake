import { useQuery } from '@tanstack/react-query'
import { Loader2, ShieldCheck, AlertCircle } from 'lucide-react'
import { fetchPermissionAudit, type PermissionAuditEvent } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { CopyableText } from '@/components/copyable-text'

function shortPubkey(pk: string): string {
  if (!pk) return '—'
  return `${pk.slice(0, 6)}…${pk.slice(-4)}`
}

function eventTypeClasses(eventType: string): string {
  switch (eventType) {
    case 'Create':
      return 'bg-green-500/15 text-green-600 dark:text-green-400'
    case 'Update':
      return 'bg-blue-500/15 text-blue-600 dark:text-blue-400'
    case 'Suspend':
      return 'bg-amber-500/15 text-amber-600 dark:text-amber-400'
    case 'Resume':
      return 'bg-teal-500/15 text-teal-600 dark:text-teal-400'
    case 'Delete':
      return 'bg-red-500/15 text-red-600 dark:text-red-400'
    default:
      return 'bg-muted text-muted-foreground'
  }
}

function formatTs(ts: string): string {
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ts
  return d.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function FlagList({ names, tone }: { names: string; tone: 'add' | 'remove' }) {
  if (!names) return <span className="text-muted-foreground">—</span>
  const cls =
    tone === 'add'
      ? 'bg-green-500/10 text-green-600 dark:text-green-400'
      : 'bg-red-500/10 text-red-600 dark:text-red-400'
  return (
    <div className="flex flex-wrap gap-1">
      {names.split(', ').map((n) => (
        <span key={n} className={`inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium ${cls}`}>
          {tone === 'remove' ? `−${n}` : n}
        </span>
      ))}
    </div>
  )
}

export function PermissionAuditPage() {
  useDocumentTitle('Permission Audit')

  const { data, isLoading, error } = useQuery({
    queryKey: ['permission-audit'],
    queryFn: () => fetchPermissionAudit(500),
    staleTime: 30_000,
  })

  const events: PermissionAuditEvent[] = data?.events ?? []

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium">Failed to load permission audit</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1400px] mx-auto px-4 sm:px-8 py-8">
        <div className="flex items-center gap-3 mb-2">
          <ShieldCheck className="h-6 w-6 text-purple-500" />
          <h1 className="text-2xl font-bold">Permission Audit</h1>
        </div>
        <p className="text-sm text-muted-foreground mb-8">
          Onchain history of serviceability permission grants, changes, suspensions and revocations —
          who did what, to whom, and when.
        </p>

        <div className="border border-border rounded-lg overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-left text-sm text-muted-foreground border-b border-border bg-muted/30">
                <th className="px-4 py-2.5 font-medium">Time (UTC local)</th>
                <th className="px-4 py-2.5 font-medium">Event</th>
                <th className="px-4 py-2.5 font-medium">Admin (signer)</th>
                <th className="px-4 py-2.5 font-medium">Grantee</th>
                <th className="px-4 py-2.5 font-medium">Added</th>
                <th className="px-4 py-2.5 font-medium">Removed</th>
                <th className="px-4 py-2.5 font-medium">Tx</th>
              </tr>
            </thead>
            <tbody>
              {events.map((e) => (
                <tr
                  key={`${e.txSignature}-${e.permissionPk}-${e.eventType}-${e.slot}`}
                  className="border-b border-border last:border-b-0 hover:bg-muted/50 transition-colors"
                >
                  <td className="px-4 py-2.5 text-sm whitespace-nowrap tabular-nums">{formatTs(e.eventTs)}</td>
                  <td className="px-4 py-2.5">
                    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium ${eventTypeClasses(e.eventType)}`}>
                      {e.eventType}
                    </span>
                    {!e.success && (
                      <span className="ml-1 inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-red-500/15 text-red-600 dark:text-red-400">
                        failed
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-sm font-mono">
                    {e.signer ? <CopyableText text={e.signer}>{shortPubkey(e.signer)}</CopyableText> : '—'}
                  </td>
                  <td className="px-4 py-2.5 text-sm font-mono">
                    {e.targetPubkey ? <CopyableText text={e.targetPubkey}>{shortPubkey(e.targetPubkey)}</CopyableText> : '—'}
                  </td>
                  <td className="px-4 py-2.5"><FlagList names={e.permissionsAdded} tone="add" /></td>
                  <td className="px-4 py-2.5"><FlagList names={e.permissionsRemoved} tone="remove" /></td>
                  <td className="px-4 py-2.5 text-sm font-mono">
                    <a
                      href={`https://solscan.io/tx/${e.txSignature}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-purple-500 hover:underline"
                    >
                      {shortPubkey(e.txSignature)}
                    </a>
                  </td>
                </tr>
              ))}
              {events.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
                    No permission events found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
