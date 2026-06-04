import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { Loader2, AlertCircle, Info, ChevronRight } from 'lucide-react'
import { Tooltip } from '@/components/ui/tooltip'
import {
  fetchMulticastGroupHealth,
  fetchMulticastGroupHealthUsers,
  fetchMulticastGroupHealthPaths,
  type MulticastHealthStatus,
  type MulticastHealthStatusCounts,
} from '@/lib/api'

const STATUS_BADGE: Record<MulticastHealthStatus, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  degraded: 'bg-amber-500/15 text-amber-500',
  unhealthy: 'bg-red-500/15 text-red-500',
  unknown: 'bg-muted text-muted-foreground',
}

const STATUS_DOT: Record<MulticastHealthStatus, string> = {
  healthy: 'bg-emerald-500',
  degraded: 'bg-amber-500',
  unhealthy: 'bg-red-500',
  unknown: 'bg-muted-foreground',
}

const STATUS_DEFINITIONS: Array<{ status: MulticastHealthStatus; short: string }> = [
  { status: 'healthy', short: '(S,G) present, SPT bit set, expected tunnel in IIF/OIF' },
  { status: 'degraded', short: '(S,G) present but flags or tunnel position wrong' },
  { status: 'unhealthy', short: 'no (S,G) — only (*,G), or expected tunnel missing' },
  { status: 'unknown', short: 'state-collect has not reported for this device yet' },
]

const SECTION_HELP = {
  summary:
    'Per-status totals across three view granularities. A row is rolled up into "healthy" only if reconciliation passes on every dimension.',
  users:
    'Each row pairs one onchain user with the mroute observed at their device. ' +
    'Publishers expect their Tunnel<N> as the IIF of (S,G); subscribers expect their Tunnel<N> in the OIF list. ' +
    'A user in P+S mode contributes two rows.',
  paths:
    'Each row is a (publisher, subscriber) pair belonging to the group. ' +
    'Endpoints-only verification: both endpoints must be reconciled. ' +
    'A recursive OIF chain walk (full-path) is a follow-up that will replace "endpoints only" with "full path".',
}

function HealthBadge({ status }: { status: MulticastHealthStatus }) {
  const cls = STATUS_BADGE[status] ?? STATUS_BADGE.unknown
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${cls}`}>
      {status}
    </span>
  )
}

function HelpIcon({ content }: { content: string }) {
  return (
    <Tooltip content={content}>
      <button
        type="button"
        className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors"
        aria-label="What does this mean?"
      >
        <Info className="h-3.5 w-3.5" />
      </button>
    </Tooltip>
  )
}

function StatusLegend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground border-t border-border pt-3 mt-1">
      <span className="font-medium text-foreground">Status:</span>
      {STATUS_DEFINITIONS.map(({ status, short }) => (
        <span key={status} className="inline-flex items-center gap-1.5">
          <span className={`inline-block h-2 w-2 rounded-full ${STATUS_DOT[status]}`} />
          <span className="font-medium text-foreground">{status}</span>
          <span>— {short}</span>
        </span>
      ))}
    </div>
  )
}

function CountsRow({
  label,
  counts,
}: {
  label: string
  counts: MulticastHealthStatusCounts
}) {
  return (
    <div className="flex items-center justify-between py-2 text-sm">
      <div className="text-muted-foreground">{label}</div>
      <div className="flex items-center gap-3 tabular-nums">
        <span className="text-emerald-500">{counts.healthy} healthy</span>
        <span className="text-amber-500">{counts.degraded} degraded</span>
        <span className="text-red-500">{counts.unhealthy} unhealthy</span>
        <span className="text-muted-foreground">/ {counts.total}</span>
      </div>
    </div>
  )
}

function NavLinkArrow() {
  return <ChevronRight className="inline h-3 w-3 ml-0.5 opacity-60 group-hover:opacity-100 transition-opacity" />
}

export function MulticastGroupHealthTab({ groupPkOrCode }: { groupPkOrCode: string }) {
  const summaryQuery = useQuery({
    queryKey: ['multicast-group-health-summary', groupPkOrCode],
    queryFn: () => fetchMulticastGroupHealth(groupPkOrCode),
    enabled: !!groupPkOrCode,
    refetchInterval: 30_000,
  })

  const usersQuery = useQuery({
    queryKey: ['multicast-group-health-users', groupPkOrCode],
    queryFn: () => fetchMulticastGroupHealthUsers(groupPkOrCode),
    enabled: !!groupPkOrCode,
    refetchInterval: 30_000,
  })

  const pathsQuery = useQuery({
    queryKey: ['multicast-group-health-paths', groupPkOrCode],
    queryFn: () => fetchMulticastGroupHealthPaths(groupPkOrCode),
    enabled: !!groupPkOrCode,
    refetchInterval: 30_000,
  })

  if (summaryQuery.isLoading) {
    return (
      <div className="px-4 py-8 text-center text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin mx-auto" />
      </div>
    )
  }

  if (summaryQuery.error) {
    return (
      <div className="px-4 py-6 text-sm text-red-500 flex items-center gap-2">
        <AlertCircle className="h-4 w-4" />
        Failed to load health data: {(summaryQuery.error as Error).message}
      </div>
    )
  }

  const summary = summaryQuery.data
  if (!summary) return null

  return (
    <div className="p-4 space-y-6">
      {/* Summary counts */}
      <div className="border border-border rounded-lg bg-card p-4">
        <div className="flex items-baseline justify-between mb-2">
          <div className="flex items-center gap-1.5">
            <h3 className="text-sm font-medium">Reconciliation summary</h3>
            <HelpIcon content={SECTION_HELP.summary} />
          </div>
          {!summary.source_available && (
            <span className="text-xs text-muted-foreground">no state-collect data yet</span>
          )}
        </div>
        <CountsRow label="Users (onchain ↔ mroute IIF/OIF)" counts={summary.counts.users} />
        <CountsRow label="Paths (publisher → subscriber endpoints)" counts={summary.counts.paths} />
        <CountsRow label="Mroutes (per-row dataplane state)" counts={summary.counts.mroutes} />
        <StatusLegend />
      </div>

      {/* Per-user reconciliation */}
      <div className="border border-border rounded-lg bg-card">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            <h3 className="text-sm font-medium">Per-user reconciliation</h3>
            <HelpIcon content={SECTION_HELP.users} />
          </div>
          {usersQuery.isFetching && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-sm text-left text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">User</th>
                <th className="px-4 py-3 font-medium">Mode</th>
                <th className="px-4 py-3 font-medium">Device</th>
                <th className="px-4 py-3 font-medium text-right">Tunnel</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Reason</th>
              </tr>
            </thead>
            <tbody>
              {usersQuery.data?.items.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                    No multicast users
                  </td>
                </tr>
              )}
              {usersQuery.data?.items.map((u) => (
                <tr key={`${u.user_pk}-${u.mode}`} className="border-b border-border last:border-b-0 hover:bg-muted">
                  <td className="px-4 py-3 text-sm">
                    <Link
                      to={`/dz/users/${u.user_pk}`}
                      className="group text-blue-600 dark:text-blue-400 hover:underline font-mono inline-flex items-center"
                    >
                      {u.user_owner_pubkey ? u.user_owner_pubkey.slice(0, 8) : u.user_pk.slice(0, 8)}
                      <NavLinkArrow />
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <span className="text-xs text-muted-foreground">{u.mode}</span>
                  </td>
                  <td className="px-4 py-3 text-sm">
                    {u.user_device_pk ? (
                      <Link
                        to={`/dz/devices/${u.user_device_pk}`}
                        className="group text-blue-600 dark:text-blue-400 hover:underline font-mono inline-flex items-center"
                      >
                        {u.user_device_code || u.user_device_pk.slice(0, 8)}
                        <NavLinkArrow />
                      </Link>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm tabular-nums text-right font-mono text-muted-foreground">
                    {u.user_tunnel_id > 0 ? u.user_tunnel_id : '—'}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <HealthBadge status={u.health_status} />
                  </td>
                  <td className="px-4 py-3 text-sm text-muted-foreground">{u.mismatch_reason || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Per-path reconciliation */}
      <div className="border border-border rounded-lg bg-card">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            <h3 className="text-sm font-medium">Per-path reconciliation (publisher → subscriber)</h3>
            <HelpIcon content={SECTION_HELP.paths} />
          </div>
          {pathsQuery.isFetching && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-sm text-left text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">Publisher</th>
                <th className="px-4 py-3 font-medium">FHR device</th>
                <th className="px-4 py-3 font-medium">Subscriber</th>
                <th className="px-4 py-3 font-medium">LHR device</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Verified</th>
              </tr>
            </thead>
            <tbody>
              {pathsQuery.data?.items.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                    No (publisher, subscriber) pairs
                  </td>
                </tr>
              )}
              {pathsQuery.data?.items.map((p) => (
                <tr
                  key={`${p.publisher_user_pk}-${p.subscriber_user_pk}`}
                  className="border-b border-border last:border-b-0 hover:bg-muted"
                >
                  <td className="px-4 py-3 text-sm font-mono">
                    <Link
                      to={`/dz/users/${p.publisher_user_pk}`}
                      className="group text-blue-600 dark:text-blue-400 hover:underline inline-flex items-center"
                    >
                      {p.publisher_owner_pubkey?.slice(0, 8) || p.publisher_user_pk.slice(0, 8)}
                      <NavLinkArrow />
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm font-mono">
                    {p.publisher_device_pk ? (
                      <Link
                        to={`/dz/devices/${p.publisher_device_pk}`}
                        className="group text-blue-600 dark:text-blue-400 hover:underline inline-flex items-center"
                      >
                        {p.publisher_device_code || p.publisher_device_pk.slice(0, 8)}
                        <NavLinkArrow />
                      </Link>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm font-mono">
                    <Link
                      to={`/dz/users/${p.subscriber_user_pk}`}
                      className="group text-blue-600 dark:text-blue-400 hover:underline inline-flex items-center"
                    >
                      {p.subscriber_owner_pubkey?.slice(0, 8) || p.subscriber_user_pk.slice(0, 8)}
                      <NavLinkArrow />
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm font-mono">
                    {p.subscriber_device_pk ? (
                      <Link
                        to={`/dz/devices/${p.subscriber_device_pk}`}
                        className="group text-blue-600 dark:text-blue-400 hover:underline inline-flex items-center"
                      >
                        {p.subscriber_device_code || p.subscriber_device_pk.slice(0, 8)}
                        <NavLinkArrow />
                      </Link>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <HealthBadge status={p.health_status} />
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">
                    {p.verification_method === 'endpoints_only' ? 'endpoints only' : p.verification_method}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
