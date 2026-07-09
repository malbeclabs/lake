import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { Loader2, AlertCircle, Info, ChevronRight, X } from 'lucide-react'
import { Tooltip } from '@/components/ui/tooltip'
import { Pagination } from '@/components/pagination'
import { InlineFilter } from '@/components/inline-filter'
import {
  fetchMulticastGroupHealth,
  fetchMulticastGroupHealthUsers,
  fetchMulticastGroupHealthPaths,
  type MulticastHealthStatus,
  type MulticastHealthStatusCounts,
  type MulticastHealthUserItem,
  type MulticastRateStatus,
  type MulticastRateStatusReason,
} from '@/lib/api'

// URL query-string slots for committed Health-tab filter chips. Distinct
// from the parent page's '?search=' (members filter) so they don't
// trample each other when switching tabs. Per-user and per-path each
// have their own slot so the two tables filter independently.
const HEALTH_USERS_SEARCH_PARAM = 'husers'
const HEALTH_PATHS_SEARCH_PARAM = 'hpaths'

// Field prefixes for the per-user table filter.
const healthUserFieldPrefixes = [
  { prefix: 'device:', description: 'Filter by device code (e.g. nyc001)' },
  { prefix: 'status:', description: 'Filter by health status (healthy, degraded, unhealthy, disconnected, unknown)' },
  { prefix: 'mode:', description: 'Mode: P, S, or P+S' },
  { prefix: 'tunnel:', description: 'Tunnel id (exact match)' },
  { prefix: 'user:', description: "Match user account or owner pubkey" },
  { prefix: 'owner:', description: 'Match owner pubkey' },
  { prefix: 'ip:', description: "Match user's dz_ip" },
]

// Field prefixes for the per-path table filter.
const healthPathFieldPrefixes = [
  { prefix: 'publisher:', description: 'Match the publisher side (pk/owner/ip/device)' },
  { prefix: 'subscriber:', description: 'Match the subscriber side (pk/owner/ip/device)' },
  { prefix: 'device:', description: 'Match either publisher or subscriber device' },
  { prefix: 'status:', description: 'Filter by health status (healthy, degraded, unhealthy, disconnected, unknown)' },
  { prefix: 'user:', description: 'Match either side\'s account or owner pubkey' },
  { prefix: 'owner:', description: 'Match either side\'s owner pubkey' },
  { prefix: 'ip:', description: "Match either side's dz_ip" },
]

function parseHealthSearchFilters(searchParam: string): string[] {
  if (!searchParam) return []
  return searchParam.split(',').map(f => f.trim()).filter(Boolean)
}

function formatBps(bps?: number): string {
  if (bps === undefined || bps === null) return '—'
  if (bps === 0) return '0'
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(2)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(2)} Kbps`
  return `${bps.toFixed(0)} bps`
}

const RATE_STATUS_BADGE: Record<MulticastRateStatus, string> = {
  reconciled: 'bg-emerald-500/15 text-emerald-500',
  mismatch: 'bg-red-500/15 text-red-500',
  unknown: 'bg-muted text-muted-foreground',
}

const RATE_REASON_HUMAN: Record<MulticastRateStatusReason, string> = {
  active: 'transmitting',
  idle: 'idle (registered, transmitting 0)',
  no_data: 'no counter data in 15 min',
  reconciled: 'TX matches sum of publishers',
  mismatch: 'TX deviates from sum of publishers',
  monitoring_gap: 'a publisher in this group has no counter data',
  group_idle: 'all publishers transmitting 0 — nothing to verify against',
}

// 25 keeps the per-page Radix Tooltip.Root count (~2 per row × N rows) below
// the threshold where mounting blocks the main thread long enough to trigger
// Chrome's "Page Unresponsive" prompt on large groups (edge-solana-shreds has
// 857 users). It also matches the server-side cached page size for the hot
// group so the first paint hits the worker cache.
const HEALTH_PAGE_SIZE = 25

// HealthFilterBar renders the InlineFilter chip UX seen elsewhere on the
// site (Devices, Facilities, etc.). Committed filters live in the URL
// slot named by `paramName` so reloads / shareable URLs preserve them.
// The bar tracks its own live (in-progress) filter via a callback so the
// parent can pass it through to the underlying API call.
function HealthFilterBar({
  paramName,
  entity,
  placeholder,
  fieldPrefixes,
  liveFilter,
  setLiveFilter,
}: {
  paramName: string
  entity: string
  placeholder: string
  fieldPrefixes: { prefix: string; description: string }[]
  liveFilter: string
  setLiveFilter: (v: string) => void
}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const committed = parseHealthSearchFilters(searchParams.get(paramName) || '')

  const removeFilter = useCallback((toRemove: string) => {
    const next = committed.filter(f => f !== toRemove)
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      if (next.length === 0) p.delete(paramName)
      else p.set(paramName, next.join(','))
      return p
    })
  }, [committed, setSearchParams, paramName])

  const clearAll = useCallback(() => {
    setSearchParams(prev => {
      const p = new URLSearchParams(prev)
      p.delete(paramName)
      return p
    })
  }, [setSearchParams, paramName])

  return (
    <div className="flex flex-wrap items-center gap-2">
      <InlineFilter
        fieldPrefixes={fieldPrefixes}
        entity={entity}
        autocompleteFields={[]}
        placeholder={placeholder}
        paramName={paramName}
        onLiveFilterChange={setLiveFilter}
      />
      {committed.map((filter, idx) => (
        <button
          key={`${filter}-${idx}`}
          onClick={() => removeFilter(filter)}
          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
        >
          {filter}
          <X className="h-3 w-3" />
        </button>
      ))}
      {committed.length > 1 && (
        <button
          onClick={clearAll}
          className="text-xs text-muted-foreground hover:text-foreground transition-colors"
        >
          Clear all
        </button>
      )}
      {liveFilter && (
        <span className="text-xs text-muted-foreground italic">
          previewing: {liveFilter}
        </span>
      )}
    </div>
  )
}

const STATUS_BADGE: Record<MulticastHealthStatus, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  degraded: 'bg-amber-500/15 text-amber-500',
  unhealthy: 'bg-red-500/15 text-red-500',
  disconnected: 'bg-sky-500/15 text-sky-500',
  unknown: 'bg-muted text-muted-foreground',
}

const STATUS_DOT: Record<MulticastHealthStatus, string> = {
  healthy: 'bg-emerald-500',
  degraded: 'bg-amber-500',
  unhealthy: 'bg-red-500',
  disconnected: 'bg-sky-500',
  unknown: 'bg-muted-foreground',
}

const STATUS_DEFINITIONS: Array<{ status: MulticastHealthStatus; short: string }> = [
  { status: 'healthy', short: 'control plane reconciles AND subscriber TX matches sum of publishers (±5% / 1 Mbps)' },
  { status: 'degraded', short: 'control plane reconciles but rates diverge, or partial control plane reconciliation' },
  { status: 'unhealthy', short: 'no (S,G) mroute, or rates diverge under a degraded control plane' },
  { status: 'disconnected', short: "user's onchain BGP session is down — not connected, so no (S,G)/OIF is expected (not a forwarding fault)" },
  { status: 'unknown', short: 'no counter data, or no traffic flowing in the 5-min window' },
]

const SECTION_HELP = {
  summary:
    'Per-status totals across three view granularities. The combined verdict requires control-plane reconciliation (mroute matches onchain) AND data-plane reconciliation (subscriber TX matches sum of publishers within tolerance).',
  users:
    'Each row pairs one onchain user with the mroute and 5-min rate observed at their device. ' +
    'Publishers expect their Tunnel<N> as the IIF of (S,G) and to be transmitting; ' +
    'subscribers expect their Tunnel<N> in the OIF list and to receive at the same rate publishers send. ' +
    'A user in P+S mode contributes one row (mode = "P+S") that reconciles subscriber-side ' +
    'against (sum of publishers − self).',
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

function RateCell({ item }: { item: MulticastHealthUserItem }) {
  const cls = RATE_STATUS_BADGE[item.rate_status] ?? RATE_STATUS_BADGE.unknown
  const tooltip = (
    <div className="space-y-1">
      <div>
        <span className="font-medium">{item.rate_status_reason}</span> — {RATE_REASON_HUMAN[item.rate_status_reason] ?? ''}
      </div>
      <div className="text-muted-foreground">Observed: {formatBps(item.observed_bps_5m)}</div>
      {item.expected_bps_5m !== undefined && item.expected_bps_5m !== null && (
        <div className="text-muted-foreground">Expected: {formatBps(item.expected_bps_5m)} (sum of publishers' RX)</div>
      )}
      {item.rate_bucket_ts && (
        <div className="text-muted-foreground">5-min bucket: {new Date(item.rate_bucket_ts).toLocaleTimeString()}</div>
      )}
    </div>
  )
  return (
    <Tooltip content={tooltip}>
      <span
        tabIndex={0}
        aria-label={`Rate ${item.rate_status}: ${item.rate_status_reason}`}
        className="inline-flex items-center gap-1.5 cursor-help focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring focus-visible:ring-offset-1 rounded-full"
      >
        <span className="tabular-nums font-mono text-xs">{formatBps(item.observed_bps_5m)}</span>
        <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${cls}`}>
          {item.rate_status}
        </span>
      </span>
    </Tooltip>
  )
}

function rowReason(item: MulticastHealthUserItem): string {
  if (item.mismatch_reason) return item.mismatch_reason
  // No CP reason → surface the rate reason (or nothing if rate is healthy/active/reconciled).
  if (['active', 'reconciled'].includes(item.rate_status_reason)) return '—'
  return RATE_REASON_HUMAN[item.rate_status_reason] ?? '—'
}

// DIM_BADGE_CLASS covers both health-status and rate-status values used in
// the combined-verdict hover-card on the Status column.
const DIM_BADGE_CLASS: Record<string, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  reconciled: 'bg-emerald-500/15 text-emerald-500',
  active: 'bg-emerald-500/15 text-emerald-500',
  degraded: 'bg-amber-500/15 text-amber-500',
  mismatch: 'bg-red-500/15 text-red-500',
  unhealthy: 'bg-red-500/15 text-red-500',
  disconnected: 'bg-sky-500/15 text-sky-500',
  unknown: 'bg-muted text-muted-foreground',
  idle: 'bg-muted text-muted-foreground',
  no_data: 'bg-muted text-muted-foreground',
  monitoring_gap: 'bg-muted text-muted-foreground',
  group_idle: 'bg-muted text-muted-foreground',
}

function DimBadge({ value }: { value: string }) {
  const cls = DIM_BADGE_CLASS[value] ?? DIM_BADGE_CLASS.unknown
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${cls}`}>
      {value}
    </span>
  )
}

function UserCombinedHealthBadge({ item }: { item: MulticastHealthUserItem }) {
  const cls = STATUS_BADGE[item.health_status] ?? STATUS_BADGE.unknown
  const tooltip = (
    <div className="space-y-2 min-w-[260px]">
      <div className="text-xs font-medium">
        {item.mode === 'P' ? 'Publisher' : item.mode === 'S' ? 'Subscriber' : 'Publisher + Subscriber'} —{' '}
        <span className="capitalize">{item.health_status}</span>
      </div>
      <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5 text-xs">
        <span className="text-muted-foreground">Control plane</span>
        <span className="flex items-center gap-2">
          <DimBadge value={item.control_plane_status} />
          {item.mismatch_reason && (
            <span className="text-muted-foreground text-[11px]">{item.mismatch_reason}</span>
          )}
        </span>
        <span className="text-muted-foreground">Rate</span>
        <span className="flex items-center gap-2">
          <DimBadge value={item.rate_status} />
          <span className="text-muted-foreground text-[11px]">{item.rate_status_reason}</span>
        </span>
      </div>
    </div>
  )
  return (
    <Tooltip content={tooltip} delayDuration={120}>
      <span
        tabIndex={0}
        aria-label={`Combined health ${item.health_status}: CP ${item.control_plane_status}, Rate ${item.rate_status}`}
        className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium cursor-help focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring focus-visible:ring-offset-1 ${cls}`}
      >
        {item.health_status}
      </span>
    </Tooltip>
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
        <span className="text-sky-500">{counts.disconnected} disconnected</span>
        <span className="text-muted-foreground">{counts.unknown} unknown</span>
        <span className="text-muted-foreground">/ {counts.total}</span>
      </div>
    </div>
  )
}

function NavLinkArrow() {
  return <ChevronRight className="inline h-3 w-3 ml-0.5 opacity-60 group-hover:opacity-100 transition-opacity" />
}

// TableStateRow renders a single placeholder row covering loading / error /
// empty states so the surrounding table doesn't fall into a misleading
// "empty" state while the query is still in flight or has failed.
function TableStateRow({
  isLoading,
  error,
  isEmpty,
  emptyText,
  colSpan,
}: {
  isLoading: boolean
  error: unknown
  isEmpty: boolean
  emptyText: string
  colSpan: number
}) {
  if (isLoading) {
    return (
      <tr>
        <td colSpan={colSpan} className="px-4 py-8 text-center text-muted-foreground">
          <Loader2 className="inline h-3 w-3 animate-spin mr-2" />
          Loading…
        </td>
      </tr>
    )
  }
  if (error) {
    return (
      <tr>
        <td colSpan={colSpan} className="px-4 py-8 text-center text-red-500">
          Failed to load: {(error as Error).message}
        </td>
      </tr>
    )
  }
  if (isEmpty) {
    return (
      <tr>
        <td colSpan={colSpan} className="px-4 py-8 text-center text-muted-foreground">
          {emptyText}
        </td>
      </tr>
    )
  }
  return null
}

export function MulticastGroupHealthTab({ groupPkOrCode }: { groupPkOrCode: string }) {
  const [searchParams] = useSearchParams()
  const [usersOffset, setUsersOffset] = useState(0)
  const [pathsOffset, setPathsOffset] = useState(0)
  const [showPathDetails, setShowPathDetails] = useState(false)
  // Live (in-progress, not yet committed) filter for each table. Combined
  // with that table's URL-persisted chips before being sent to the API so
  // the table updates as the user types.
  const [usersLiveFilter, setUsersLiveFilter] = useState('')
  const [pathsLiveFilter, setPathsLiveFilter] = useState('')

  const usersCommitted = useMemo(
    () => parseHealthSearchFilters(searchParams.get(HEALTH_USERS_SEARCH_PARAM) || ''),
    [searchParams],
  )
  const pathsCommitted = useMemo(
    () => parseHealthSearchFilters(searchParams.get(HEALTH_PATHS_SEARCH_PARAM) || ''),
    [searchParams],
  )
  const usersSearch = useMemo(() => {
    const all = usersLiveFilter ? [...usersCommitted, usersLiveFilter] : usersCommitted
    return all.join(',')
  }, [usersCommitted, usersLiveFilter])
  const pathsSearch = useMemo(() => {
    const all = pathsLiveFilter ? [...pathsCommitted, pathsLiveFilter] : pathsCommitted
    return all.join(',')
  }, [pathsCommitted, pathsLiveFilter])

  useEffect(() => {
    setUsersOffset(0)
    setPathsOffset(0)
    setShowPathDetails(false)
  }, [groupPkOrCode])

  // Reset to page 1 whenever that table's filter set changes so the user
  // sees the first page of matches rather than an empty page-N.
  useEffect(() => {
    setUsersOffset(0)
  }, [usersSearch])
  useEffect(() => {
    setPathsOffset(0)
  }, [pathsSearch])

  const summaryQuery = useQuery({
    queryKey: ['multicast-group-health-summary', groupPkOrCode],
    queryFn: () => fetchMulticastGroupHealth(groupPkOrCode),
    enabled: !!groupPkOrCode,
    refetchInterval: 60_000,
  })

  const usersQuery = useQuery({
    queryKey: ['multicast-group-health-users', groupPkOrCode, HEALTH_PAGE_SIZE, usersOffset, usersSearch],
    queryFn: () => fetchMulticastGroupHealthUsers(groupPkOrCode, HEALTH_PAGE_SIZE, usersOffset, usersSearch || undefined),
    enabled: !!groupPkOrCode,
    refetchInterval: 60_000,
    placeholderData: keepPreviousData,
  })

  const pathsQuery = useQuery({
    queryKey: ['multicast-group-health-paths', groupPkOrCode, HEALTH_PAGE_SIZE, pathsOffset, pathsSearch],
    queryFn: () => fetchMulticastGroupHealthPaths(groupPkOrCode, HEALTH_PAGE_SIZE, pathsOffset, pathsSearch || undefined),
    enabled: !!groupPkOrCode && showPathDetails,
    refetchInterval: 60_000,
    placeholderData: keepPreviousData,
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

  const pathCount = summary.counts.paths.total

  return (
    <div className="p-4 space-y-6">
      {/* Under-development notice. The reconciliation logic and rate dimension
          here surface real data, but the verdicts assume state-collect covers
          every device — today only jump devices are collected, so non-jump
          publishers/subscribers can falsely render as unhealthy. Wording is
          deliberately concrete so operators read it as caveat, not "do not
          trust this page". */}
      <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-100">
        <div className="font-medium">This view is under development.</div>
        <div className="mt-1 text-amber-700 dark:text-amber-200/90">
          Health verdicts and the rate dimension are work in progress. State-collect runs only on jump devices today, so any user, publisher, or subscriber on a non-jump device will appear as <span className="font-mono">unhealthy</span> (or <span className="font-mono">disconnected</span> when its onchain BGP session is down) even when it is functioning normally. Treat verdicts as a starting point, not ground truth.
        </div>
      </div>

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
        <div className="px-4 py-3 border-b border-border flex flex-col gap-2">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-1.5">
              <h3 className="text-sm font-medium">Per-user reconciliation</h3>
              <HelpIcon content={SECTION_HELP.users} />
            </div>
            {usersQuery.isFetching && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
          </div>
          <HealthFilterBar
            paramName={HEALTH_USERS_SEARCH_PARAM}
            entity="multicast-health-users"
            placeholder="Filter users…"
            fieldPrefixes={healthUserFieldPrefixes}
            liveFilter={usersLiveFilter}
            setLiveFilter={setUsersLiveFilter}
          />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="text-sm text-left text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">User</th>
                <th className="px-4 py-3 font-medium">Mode</th>
                <th className="px-4 py-3 font-medium">Device</th>
                <th className="px-4 py-3 font-medium text-right">Tunnel</th>
                <th className="px-4 py-3 font-medium">Rate (5m)</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Reason</th>
              </tr>
            </thead>
            <tbody>
              <TableStateRow
                isLoading={usersQuery.isLoading}
                error={usersQuery.error}
                isEmpty={(usersQuery.data?.items ?? []).length === 0}
                emptyText="No multicast users"
                colSpan={7}
              />
              {(usersQuery.data?.items ?? []).map((u) => (
                <tr key={`${u.user_pk}-${u.mode}`} className="border-b border-border last:border-b-0 hover:bg-muted">
                  <td className="px-4 py-3 text-sm">
                    <Link
                      to={`/dz/users/${u.user_pk}`}
                      className="group text-blue-600 dark:text-blue-400 hover:underline font-mono inline-flex items-center"
                      title={u.user_owner_pubkey ? `account ${u.user_pk}\nowner ${u.user_owner_pubkey}` : u.user_pk}
                    >
                      {u.user_pk.slice(0, 8)}
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
                    <RateCell item={u} />
                  </td>
                  <td className="px-4 py-3 text-sm">
                    <div className="flex flex-col gap-1">
                      <UserCombinedHealthBadge item={u} />
                      {/* Inline dimension breakdown so operators can scan
                          CP × Rate without needing to hover every row. */}
                      <div className="flex items-center gap-1 text-[10px] text-muted-foreground">
                        <span>CP</span>
                        <DimBadge value={u.control_plane_status} />
                        <span className="ml-1">Rate</span>
                        <DimBadge value={u.rate_status} />
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-sm text-muted-foreground">{rowReason(u)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {usersQuery.data && (
          <Pagination
            total={usersQuery.data.total}
            limit={HEALTH_PAGE_SIZE}
            offset={usersOffset}
            onOffsetChange={setUsersOffset}
          />
        )}
      </div>

      {/* Per-path reconciliation */}
      <div className="border border-border rounded-lg bg-card">
        <div className="px-4 py-3 border-b border-border flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <h3 className="text-sm font-medium">Per-path reconciliation (publisher → subscriber)</h3>
              <HelpIcon content={SECTION_HELP.paths} />
            </div>
            <div className="flex items-center gap-2">
              {showPathDetails && pathsQuery.isFetching && (
                <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
              )}
              <button
                type="button"
                onClick={() => setShowPathDetails((v) => !v)}
                className="text-xs text-blue-600 dark:text-blue-400 hover:underline"
              >
                {showPathDetails ? 'Hide details' : 'Show details'}
              </button>
            </div>
          </div>
          {showPathDetails && (
            <HealthFilterBar
              paramName={HEALTH_PATHS_SEARCH_PARAM}
              entity="multicast-health-paths"
              placeholder="Filter paths…"
              fieldPrefixes={healthPathFieldPrefixes}
              liveFilter={pathsLiveFilter}
              setLiveFilter={setPathsLiveFilter}
            />
          )}
        </div>
        {!showPathDetails ? (
          <div className="px-4 py-6 text-sm text-muted-foreground flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <div>
              <div className="text-foreground font-medium">Path details are collapsed by default.</div>
              <div>
                This group has {pathCount.toLocaleString()} publisher → subscriber pairs. Load details only when
                you need row-level endpoint reconciliation.
              </div>
            </div>
            <button
              type="button"
              onClick={() => setShowPathDetails(true)}
              className="self-start sm:self-auto px-3 py-1.5 rounded-md border border-border text-xs text-foreground hover:bg-muted transition-colors"
            >
              Load path details
            </button>
          </div>
        ) : (
          <>
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
                  <TableStateRow
                    isLoading={pathsQuery.isLoading}
                    error={pathsQuery.error}
                    isEmpty={(pathsQuery.data?.items ?? []).length === 0}
                    emptyText="No (publisher, subscriber) pairs"
                    colSpan={6}
                  />
                  {(pathsQuery.data?.items ?? []).map((p) => (
                    <tr
                      key={`${p.publisher_user_pk}-${p.subscriber_user_pk}`}
                      className="border-b border-border last:border-b-0 hover:bg-muted"
                    >
                      <td className="px-4 py-3 text-sm font-mono">
                        <Link
                          to={`/dz/users/${p.publisher_user_pk}`}
                          className="group text-blue-600 dark:text-blue-400 hover:underline inline-flex items-center"
                          // Show the account (user_pk) — owner_pubkey collides
                          // across an operator's many accounts. Owner is in title.
                          title={p.publisher_owner_pubkey ? `account ${p.publisher_user_pk}\nowner ${p.publisher_owner_pubkey}` : p.publisher_user_pk}
                        >
                          {p.publisher_user_pk.slice(0, 8)}
                          {p.publisher_tunnel_id > 0 && (
                            <span className="ml-1 text-muted-foreground">·T{p.publisher_tunnel_id}</span>
                          )}
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
                          title={p.subscriber_owner_pubkey ? `account ${p.subscriber_user_pk}\nowner ${p.subscriber_owner_pubkey}` : p.subscriber_user_pk}
                        >
                          {p.subscriber_user_pk.slice(0, 8)}
                          {p.subscriber_tunnel_id > 0 && (
                            <span className="ml-1 text-muted-foreground">·T{p.subscriber_tunnel_id}</span>
                          )}
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
                        {p.missing_endpoint_reasons && p.missing_endpoint_reasons.length > 0 ? (
                          <Tooltip
                            content={
                              <div className="space-y-1 min-w-[220px]">
                                <div className="text-xs font-medium">Missing endpoints</div>
                                <ul className="space-y-0.5 text-xs">
                                  {p.missing_endpoint_reasons.map((r) => (
                                    <li key={r} className="text-muted-foreground">
                                      • {r}
                                    </li>
                                  ))}
                                </ul>
                              </div>
                            }
                            delayDuration={120}
                          >
                            <span
                              tabIndex={0}
                              aria-label={`Status ${p.health_status}: ${p.missing_endpoint_reasons.join(', ')}`}
                              className="inline-flex items-center cursor-help focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring focus-visible:ring-offset-1 rounded-full"
                            >
                              <HealthBadge status={p.health_status} />
                            </span>
                          </Tooltip>
                        ) : (
                          <HealthBadge status={p.health_status} />
                        )}
                      </td>
                      <td className="px-4 py-3 text-xs text-muted-foreground">
                        {p.verification_method === 'endpoints_only' ? 'endpoints only' : p.verification_method}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {pathsQuery.data && (
              <Pagination
                total={pathsQuery.data.total}
                limit={HEALTH_PAGE_SIZE}
                offset={pathsOffset}
                onOffsetChange={setPathsOffset}
              />
            )}
          </>
        )}
      </div>
    </div>
  )
}
