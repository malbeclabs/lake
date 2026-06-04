import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { Loader2, AlertCircle } from 'lucide-react'
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

function HealthBadge({ status }: { status: MulticastHealthStatus }) {
  const cls = STATUS_BADGE[status] ?? STATUS_BADGE.unknown
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${cls}`}>
      {status}
    </span>
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
          <h3 className="text-sm font-medium">Reconciliation summary</h3>
          {!summary.source_available && (
            <span className="text-xs text-muted-foreground">no state-collect data yet</span>
          )}
        </div>
        <CountsRow label="Users (onchain ↔ mroute IIF/OIF)" counts={summary.counts.users} />
        <CountsRow label="Paths (publisher → subscriber endpoints)" counts={summary.counts.paths} />
        <CountsRow label="Mroutes (per-row dataplane state)" counts={summary.counts.mroutes} />
      </div>

      {/* Per-user reconciliation */}
      <div className="border border-border rounded-lg bg-card">
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <h3 className="text-sm font-medium">Per-user reconciliation</h3>
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
                      className="text-blue-600 dark:text-blue-400 hover:underline font-mono"
                    >
                      {u.user_owner_pubkey ? u.user_owner_pubkey.slice(0, 8) : u.user_pk.slice(0, 8)}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <span className="text-xs text-muted-foreground">{u.mode}</span>
                  </td>
                  <td className="px-4 py-3 text-sm">
                    {u.user_device_pk ? (
                      <Link
                        to={`/dz/devices/${u.user_device_pk}`}
                        className="text-blue-600 dark:text-blue-400 hover:underline font-mono"
                      >
                        {u.user_device_code || u.user_device_pk.slice(0, 8)}
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
          <h3 className="text-sm font-medium">Per-path reconciliation (publisher → subscriber)</h3>
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
                      className="text-blue-600 dark:text-blue-400 hover:underline"
                    >
                      {p.publisher_owner_pubkey?.slice(0, 8) || p.publisher_user_pk.slice(0, 8)}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm font-mono">
                    {p.publisher_device_pk ? (
                      <Link
                        to={`/dz/devices/${p.publisher_device_pk}`}
                        className="text-blue-600 dark:text-blue-400 hover:underline"
                      >
                        {p.publisher_device_code || p.publisher_device_pk.slice(0, 8)}
                      </Link>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm font-mono">
                    <Link
                      to={`/dz/users/${p.subscriber_user_pk}`}
                      className="text-blue-600 dark:text-blue-400 hover:underline"
                    >
                      {p.subscriber_owner_pubkey?.slice(0, 8) || p.subscriber_user_pk.slice(0, 8)}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm font-mono">
                    {p.subscriber_device_pk ? (
                      <Link
                        to={`/dz/devices/${p.subscriber_device_pk}`}
                        className="text-blue-600 dark:text-blue-400 hover:underline"
                      >
                        {p.subscriber_device_code || p.subscriber_device_pk.slice(0, 8)}
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
