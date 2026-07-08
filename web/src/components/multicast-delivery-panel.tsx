import { useState, type ReactNode } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { AlertCircle, ChevronDown, Info, Loader2, Radio, RefreshCw } from 'lucide-react'
import { Pagination } from '@/components/pagination'
import { Tooltip } from '@/components/ui/tooltip'
import {
  fetchDeviceMulticastDelivery,
  fetchLinkMulticastDelivery,
  type DeviceMulticastDeliveryResponse,
  type LinkMulticastDeliveryResponse,
  type MulticastDeliveryEntityGroup,
  type MulticastDeliveryFreshness,
  type MulticastDeliveryLinkBranch,
  type MulticastDeliveryMroute,
  type MulticastDeliveryOIF,
  type MulticastDeliveryRole,
  type MulticastEntityHealthStatusCounts,
  type MulticastHealthPathItem,
  type MulticastHealthStatus,
  type MulticastHealthUserItem,
  type MulticastRateStatus,
  type MulticastRateStatusReason,
} from '@/lib/api'

const PAGE_SIZE = 50
const ENDPOINT_PAGE_SIZE = 25
const EMPTY = 'No data'

const STATUS_BADGE: Record<MulticastHealthStatus, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  degraded: 'bg-amber-500/15 text-amber-500',
  unhealthy: 'bg-red-500/15 text-red-500',
  disconnected: 'bg-sky-500/15 text-sky-500',
  unknown: 'bg-muted text-muted-foreground',
}

const RATE_STATUS_BADGE: Record<MulticastRateStatus, string> = {
  reconciled: 'bg-emerald-500/15 text-emerald-500',
  mismatch: 'bg-red-500/15 text-red-500',
  unknown: 'bg-muted text-muted-foreground',
}

const RATE_REASON_HUMAN: Record<MulticastRateStatusReason, string> = {
  active: 'transmitting',
  idle: 'idle, registered but sending zero',
  no_data: 'no counter data in 15 min',
  reconciled: 'TX matches sum of publishers',
  mismatch: 'TX deviates from sum of publishers',
  monitoring_gap: 'a publisher in this group has no counter data',
  group_idle: 'all publishers are idle, nothing to verify',
}

function compactNumber(value: number): string {
  return value.toLocaleString()
}

function valueOrEmpty(value?: string | number | null): string {
  if (value === undefined || value === null || value === '') return EMPTY
  return String(value)
}

function listOrEmpty<T>(items?: T[] | null): T[] {
  return Array.isArray(items) ? items : []
}

function formatBps(bps?: number | null): string {
  if (bps === undefined || bps === null) return EMPTY
  if (bps === 0) return '0 bps'
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(2)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(2)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function formatAge(seconds?: number): string {
  if (seconds === undefined) return EMPTY
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h ago`
  return `${(seconds / 86400).toFixed(1)}d ago`
}

function freshnessClass(status: string): string {
  switch (status) {
    case 'fresh':
      return 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
    case 'stale':
      return 'bg-amber-500/10 text-amber-700 dark:text-amber-300'
    case 'missing':
      return 'bg-muted text-muted-foreground'
    default:
      return 'bg-muted/60 text-muted-foreground'
  }
}

function worstHealth(counts?: MulticastEntityHealthStatusCounts): MulticastHealthStatus | null {
  if (!counts || counts.total === 0) return null
  if (counts.unhealthy > 0) return 'unhealthy'
  if (counts.degraded > 0) return 'degraded'
  if (counts.unknown > 0) return 'unknown'
  if (counts.disconnected > 0) return 'disconnected'
  return 'healthy'
}

function HealthBadge({ status }: { status: MulticastHealthStatus }) {
  return <span className={`inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium ${STATUS_BADGE[status]}`}>{status}</span>
}

function FocusHealthBadge({ status, label }: { status: MulticastHealthStatus; label: ReactNode }) {
  return (
    <Tooltip content={label}>
      <button type="button" className="inline-flex rounded-full focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1">
        <HealthBadge status={status} />
      </button>
    </Tooltip>
  )
}

function FreshnessBadge({ freshness }: { freshness?: MulticastDeliveryFreshness }) {
  const state = freshness?.mroute
  const status = state?.available ? state.status : 'unavailable'
  return <span className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${freshnessClass(status)}`}>{status} {state?.age_seconds !== undefined ? `· ${formatAge(state.age_seconds)}` : ''}</span>
}

function HealthCountsStrip({ counts, compact = false }: { counts?: MulticastEntityHealthStatusCounts; compact?: boolean }) {
  if (!counts || counts.total === 0) return <span className="text-xs text-muted-foreground">No health rows</span>
  const parts: Array<[MulticastHealthStatus, number]> = [
    ['unhealthy', counts.unhealthy],
    ['degraded', counts.degraded],
    ['unknown', counts.unknown],
    ['disconnected', counts.disconnected],
    ['healthy', counts.healthy],
  ]
  return (
    <span className={`flex flex-wrap items-center ${compact ? 'gap-1.5' : 'gap-2'}`}>
      {parts.map(([status, value]) => value > 0 && (
        <span key={status} className={`rounded-full px-1.5 py-0.5 text-[10px] font-medium ${STATUS_BADGE[status]}`}>
          {compactNumber(value)} {status}
        </span>
      ))}
      {!compact && <span className="text-xs text-muted-foreground">{compactNumber(counts.total)} total</span>}
    </span>
  )
}

function PanelSkeleton() {
  return (
    <div className="space-y-4 rounded-lg border border-border bg-card p-4">
      <div className="flex items-center justify-between">
        <div className="h-5 w-44 animate-pulse rounded bg-muted" />
        <div className="h-5 w-20 animate-pulse rounded bg-muted" />
      </div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-6">
        {[1, 2, 3, 4, 5, 6].map(i => <div key={i} className="h-14 animate-pulse rounded-lg bg-muted" />)}
      </div>
      <div className="h-28 animate-pulse rounded bg-muted" />
    </div>
  )
}

function PanelFrame({ title, subtitle, generatedAt, freshness, isFetching, onRefresh, children }: { title: string; subtitle: string; generatedAt?: string; freshness?: MulticastDeliveryFreshness; isFetching?: boolean; onRefresh?: () => void; children: ReactNode }) {
  return (
    <div className="space-y-4 rounded-lg border border-border bg-card p-4">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex items-start gap-2">
          <Radio className="mt-0.5 h-4 w-4 text-muted-foreground" />
          <div>
            <div className="text-sm font-medium">{title}</div>
            <div className="text-xs text-muted-foreground">{subtitle}{generatedAt ? ` · generated ${new Date(generatedAt).toLocaleTimeString()}` : ''}</div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <FreshnessBadge freshness={freshness} />
          {onRefresh && (
            <button type="button" onClick={onRefresh} disabled={isFetching} className="text-muted-foreground transition-colors hover:text-foreground disabled:opacity-50" aria-label="Refresh multicast delivery state">
              {isFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            </button>
          )}
        </div>
      </div>
      {children}
    </div>
  )
}

function CalmMessage({ icon, title, body }: { icon?: boolean; title: string; body?: string }) {
  return (
    <div className="rounded-lg bg-muted/25 px-4 py-6 text-center">
      {icon && <AlertCircle className="mx-auto mb-2 h-5 w-5 text-muted-foreground" />}
      <div className="text-sm font-medium">{title}</div>
      {body && <div className="mx-auto mt-1 max-w-2xl text-xs text-muted-foreground">{body}</div>}
    </div>
  )
}

function SummaryGrid({ items }: { items: Array<{ label: string; value: number; health?: MulticastEntityHealthStatusCounts }> }) {
  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-6">
      {items.map(item => (
        <div key={item.label} className="rounded-lg bg-muted/30 px-3 py-2">
          <div className="flex items-center gap-2">
            <div className="text-base font-medium tabular-nums">{compactNumber(item.value)}</div>
            {worstHealth(item.health) && <HealthBadge status={worstHealth(item.health)!} />}
          </div>
          <div className="text-[11px] text-muted-foreground">{item.label}</div>
        </div>
      ))}
    </div>
  )
}

function groupTarget(group: MulticastDeliveryEntityGroup): string | null {
  const key = group.group_pk || group.group_code
  return key ? `/dz/multicast-groups/${encodeURIComponent(key)}?tab=health` : null
}

function groupLabel(group: MulticastDeliveryEntityGroup): string {
  return group.group_code || group.group_address || group.group_pk || EMPTY
}

function GroupChips({ groups, showHealth = false }: { groups: MulticastDeliveryEntityGroup[]; showHealth?: boolean }) {
  if (groups.length === 0) return null
  return (
    <div className="flex flex-wrap gap-2">
      {groups.slice(0, 8).map(group => {
        const target = groupTarget(group)
        const body = (
          <>
            <span className="font-mono">{groupLabel(group)}</span>
            <span className="text-muted-foreground">{compactNumber(group.source_count)} src · {compactNumber(group.oif_count)} OIF</span>
            {showHealth && <HealthCountsStrip counts={group.health_counts} compact />}
          </>
        )
        return target ? (
          <RouterLink key={`${groupLabel(group)}-${group.group_address}`} to={target} className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-2 py-1 text-xs transition-colors hover:bg-muted/40">
            {body}
          </RouterLink>
        ) : <span key={`${groupLabel(group)}-${group.group_address}`} className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-2 py-1 text-xs">{body}</span>
      })}
      {groups.length > 8 && <span className="inline-flex items-center rounded-md border border-border px-2 py-1 text-xs text-muted-foreground">+{groups.length - 8} more</span>}
    </div>
  )
}

function RoleStrip({ roles }: { roles: MulticastDeliveryRole[] }) {
  if (roles.length === 0) return null
  return (
    <div className="flex flex-wrap gap-2">
      {roles.map(role => (
        <Tooltip key={role.role} content={role.description}>
          <button type="button" className="inline-flex items-center gap-2 rounded-md bg-muted/40 px-2 py-1 text-xs text-left focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1">
            <span className="font-medium">{role.label}</span>
            <span className="text-muted-foreground">{compactNumber(role.source_count)} src · {compactNumber(role.oif_count)} OIF</span>
          </button>
        </Tooltip>
      ))}
    </div>
  )
}

function UserHealthTable({ items }: { items: MulticastHealthUserItem[] }) {
  if (items.length === 0) return <CalmMessage title="No multicast users on this device have health rows." />
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full text-xs">
        <thead className="bg-muted/40 text-muted-foreground">
          <tr>
            {['Group', 'User', 'Mode', 'Tunnel', 'Control plane', 'Rate', 'Health', 'Reason'].map(h => <th key={h} className="px-3 py-2 text-left font-medium">{h}</th>)}
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {items.map(item => (
            <tr key={`${item.multicast_group_pk}-${item.user_pk}-${item.mode}`}>
              <td className="px-3 py-2 font-mono whitespace-nowrap"><RouterLink to={`/dz/multicast-groups/${encodeURIComponent(item.multicast_group_pk || item.multicast_group_code)}?tab=health`} className="hover:underline">{valueOrEmpty(item.multicast_group_code || item.group_address)}</RouterLink></td>
              <td className="px-3 py-2 font-mono whitespace-nowrap">{valueOrEmpty(item.user_dz_ip || item.user_pk)}</td>
              <td className="px-3 py-2 whitespace-nowrap">{item.mode}</td>
              <td className="px-3 py-2 font-mono whitespace-nowrap">Tunnel{item.user_tunnel_id}</td>
              <td className="px-3 py-2"><HealthBadge status={item.control_plane_status} /></td>
              <td className="px-3 py-2 whitespace-nowrap">
                <span className="font-mono tabular-nums">{formatBps(item.observed_bps_5m)}</span>
                <span className={`ml-1.5 rounded-full px-1.5 py-0.5 text-[10px] font-medium ${RATE_STATUS_BADGE[item.rate_status]}`}>{item.rate_status}</span>
              </td>
              <td className="px-3 py-2"><FocusHealthBadge status={item.health_status} label={<HealthTooltip item={item} />} /></td>
              <td className="px-3 py-2 text-muted-foreground">{rowReason(item)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function HealthTooltip({ item }: { item: MulticastHealthUserItem }) {
  return (
    <div className="space-y-1">
      <div><span className="font-medium">Combined:</span> {item.health_status}</div>
      <div><span className="font-medium">Control plane:</span> {item.control_plane_status}</div>
      <div><span className="font-medium">Rate:</span> {item.rate_status}, {RATE_REASON_HUMAN[item.rate_status_reason] ?? item.rate_status_reason}</div>
      <div className="text-muted-foreground">Observed: {formatBps(item.observed_bps_5m)}</div>
      {item.expected_bps_5m !== undefined && <div className="text-muted-foreground">Expected: {formatBps(item.expected_bps_5m)}</div>}
    </div>
  )
}

function rowReason(item: MulticastHealthUserItem): string {
  if (item.mismatch_reason) return item.mismatch_reason
  if (item.rate_status_reason === 'active' || item.rate_status_reason === 'reconciled') return EMPTY
  return RATE_REASON_HUMAN[item.rate_status_reason] ?? item.rate_status_reason
}

function EndpointHealthDisclosure({ data }: { data: DeviceMulticastDeliveryResponse }) {
  const [open, setOpen] = useState(false)
  const endpointItems = listOrEmpty(data.endpoint_health_items)
  const affected = data.summary.endpoint_health_counts.unhealthy + data.summary.endpoint_health_counts.degraded
  return (
    <div className="rounded-lg border border-border p-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex items-center gap-2 text-sm font-medium">Endpoint health <Tooltip content={data.health_context_note}><button type="button" className="text-muted-foreground hover:text-foreground" aria-label="Endpoint health note"><Info className="h-3.5 w-3.5" /></button></Tooltip></div>
          <div className="text-xs text-muted-foreground">{compactNumber(data.endpoint_health_total)} endpoint pairs checked. Endpoint checks do not prove every transit hop.</div>
        </div>
        <div className="flex items-center gap-3">
          <HealthCountsStrip counts={data.summary.endpoint_health_counts} />
          <button type="button" onClick={() => setOpen(v => !v)} aria-expanded={open} className="inline-flex items-center gap-1 rounded-md border border-border px-2 py-1 text-xs hover:bg-muted/40">
            {affected > 0 ? 'Show affected endpoints' : 'Show endpoints'}
            <ChevronDown className={`h-3.5 w-3.5 transition-transform ${open ? 'rotate-180' : ''}`} />
          </button>
        </div>
      </div>
      {open && <EndpointHealthTable items={endpointItems} total={data.endpoint_health_total} limit={data.endpoint_limit} />}
    </div>
  )
}

function EndpointHealthTable({ items, total, limit }: { items: MulticastHealthPathItem[]; total: number; limit: number }) {
  if (items.length === 0) return <div className="pt-3"><CalmMessage title="No endpoint pair rows match the current filters." /></div>
  return (
    <div className="mt-3 overflow-x-auto rounded-lg border border-border">
      <table className="w-full text-xs">
        <thead className="bg-muted/40 text-muted-foreground"><tr>{['Group', 'Publisher', 'Subscriber', 'Publisher endpoint', 'Subscriber endpoint', 'Health', 'Reason'].map(h => <th key={h} className="px-3 py-2 text-left font-medium">{h}</th>)}</tr></thead>
        <tbody className="divide-y divide-border">
          {items.map(item => (
            <tr key={`${item.multicast_group_pk}-${item.publisher_user_pk}-${item.subscriber_user_pk}`}>
              <td className="px-3 py-2 font-mono whitespace-nowrap">{valueOrEmpty(item.multicast_group_code || item.group_address)}</td>
              <td className="px-3 py-2 font-mono whitespace-nowrap">{valueOrEmpty(item.publisher_device_code || item.publisher_dz_ip)}</td>
              <td className="px-3 py-2 font-mono whitespace-nowrap">{valueOrEmpty(item.subscriber_device_code || item.subscriber_dz_ip)}</td>
              <td className="px-3 py-2">{item.publisher_endpoint_observed ? 'observed' : 'missing'}</td>
              <td className="px-3 py-2">{item.subscriber_endpoint_observed ? 'observed' : 'missing'}</td>
              <td className="px-3 py-2"><HealthBadge status={item.health_status} /></td>
              <td className="px-3 py-2 text-muted-foreground">{item.missing_endpoint_reasons?.join('; ') || EMPTY}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {total > limit && <div className="border-t border-border px-3 py-2 text-xs text-muted-foreground">Showing first {compactNumber(limit)} of {compactNumber(total)} endpoint rows.</div>}
    </div>
  )
}

function DeviceRouteTable({ routes }: { routes: MulticastDeliveryMroute[] }) {
  if (routes.length === 0) return null
  return <EntityTable headers={['Group', 'Source', 'Reporting device', 'Publisher', 'RPF', 'OIFs', 'Freshness']} rows={routes.map(route => [groupCell(route.multicast_group_pk, route.multicast_group_code || route.group_address), valueOrEmpty(route.source_address), deviceCell(route.device_pk, route.device_code), valueOrEmpty(route.publisher_device_code || route.publisher_device_pk), `${valueOrEmpty(route.rpf_interface)} · ${valueOrEmpty(route.rpf_neighbor)}`, compactNumber(route.oif_count), <span className={`rounded-full px-2 py-0.5 ${freshnessClass(route.freshness_status)}`}>{route.freshness_status}</span>])} />
}

function DeviceOIFTable({ oifs }: { oifs: MulticastDeliveryOIF[] }) {
  if (oifs.length === 0) return null
  return <EntityTable headers={['Group', 'Source', 'OIF', 'Kind', 'Link or peer', 'Role']} rows={oifs.map(oif => [groupCell(oif.multicast_group_pk, oif.multicast_group_code || oif.group_address), valueOrEmpty(oif.source_address), valueOrEmpty(oif.oif_name), valueOrEmpty(oif.oif_kind), oif.link_pk ? linkCell(oif.link_pk, oif.link_code) : valueOrEmpty(oif.peer_device_code || oif.subscriber_device_code), valueOrEmpty(oif.observed_delivery_role)])} />
}

function LinkBranchTable({ branches }: { branches: MulticastDeliveryLinkBranch[] }) {
  if (branches.length === 0) return null
  return <EntityTable headers={['Group', 'Source', 'Direction', 'Reporting device', 'OIF', 'Peer', 'Role', 'Freshness']} rows={branches.map(branch => [groupCell(branch.multicast_group_pk, branch.multicast_group_code || branch.group_address), valueOrEmpty(branch.source_address), branch.direction.replaceAll('_', ' '), deviceCell(branch.device_pk, branch.device_code), valueOrEmpty(branch.oif_name), valueOrEmpty(branch.peer_device_code || branch.peer_device_pk), valueOrEmpty(branch.observed_delivery_role), <span className={`rounded-full px-2 py-0.5 ${freshnessClass(branch.freshness_status)}`}>{branch.freshness_status}</span>])} />
}

function EntityTable({ headers, rows }: { headers: string[]; rows: ReactNode[][] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full text-xs">
        <thead className="bg-muted/40 text-muted-foreground"><tr>{headers.map(header => <th key={header} className="px-3 py-2 text-left font-medium">{header}</th>)}</tr></thead>
        <tbody className="divide-y divide-border">{rows.map((row, i) => <tr key={i}>{row.map((cell, j) => <td key={j} className="px-3 py-2 whitespace-nowrap first:font-mono">{cell}</td>)}</tr>)}</tbody>
      </table>
    </div>
  )
}

function groupCell(pk?: string, label?: string): ReactNode {
  return pk ? <RouterLink to={`/dz/multicast-groups/${encodeURIComponent(pk)}?tab=health`} className="hover:underline">{valueOrEmpty(label)}</RouterLink> : valueOrEmpty(label)
}

function deviceCell(pk?: string, label?: string): ReactNode {
  return pk ? <RouterLink to={`/dz/devices/${encodeURIComponent(pk)}`} className="font-mono hover:underline">{valueOrEmpty(label || pk)}</RouterLink> : valueOrEmpty(label)
}

function linkCell(pk?: string, label?: string): ReactNode {
  return pk ? <RouterLink to={`/dz/links/${encodeURIComponent(pk)}`} className="font-mono hover:underline">{valueOrEmpty(label || pk)}</RouterLink> : valueOrEmpty(label)
}

function DevicePanelContent({ data, offset, onOffsetChange }: { data: DeviceMulticastDeliveryResponse; offset: number; onOffsetChange: (offset: number) => void }) {
  const routes = listOrEmpty(data.routes)
  const oifs = listOrEmpty(data.oifs)
  const total = Math.max(data.route_total, data.oif_total)
  return (
    <>
      <SummaryGrid items={[{ label: 'groups', value: data.summary.group_count }, { label: 'sources', value: data.summary.source_count }, { label: 'routes', value: data.summary.mroute_count }, { label: 'OIF branches', value: data.summary.oif_count }, { label: 'user health', value: data.summary.user_health_counts.total, health: data.summary.user_health_counts }, { label: 'endpoint pairs', value: data.summary.endpoint_health_counts.total, health: data.summary.endpoint_health_counts }]} />
      <RoleStrip roles={listOrEmpty(data.roles)} />
      <GroupChips groups={listOrEmpty(data.groups)} />
      <div className="text-xs text-muted-foreground">{data.coverage_note}</div>
      <section className="space-y-3">
        <div className="flex items-center justify-between"><h3 className="text-sm font-medium">Health context</h3><HealthCountsStrip counts={data.summary.user_health_counts} /></div>
        <UserHealthTable items={listOrEmpty(data.health_users)} />
        <EndpointHealthDisclosure data={data} />
      </section>
      {!data.source_available ? <CalmMessage title="Multicast forwarding telemetry is unavailable for this environment." /> : total === 0 ? <CalmMessage title="No current multicast forwarding state observed for this device." body={data.coverage_note} /> : <div className="space-y-4"><h3 className="text-sm font-medium">Observed forwarding state</h3><DeviceRouteTable routes={routes} /><DeviceOIFTable oifs={oifs} /><Pagination total={total} limit={data.limit || PAGE_SIZE} offset={offset} onOffsetChange={onOffsetChange} /></div>}
    </>
  )
}

function LinkPanelContent({ data, offset, onOffsetChange }: { data: LinkMulticastDeliveryResponse; offset: number; onOffsetChange: (offset: number) => void }) {
  const branches = listOrEmpty(data.branches)
  return (
    <>
      <SummaryGrid items={[{ label: 'groups', value: data.summary.group_count }, { label: 'sources', value: data.summary.source_count }, { label: 'branches', value: data.summary.branch_count }, { label: 'A to Z', value: data.summary.a_to_z_count }, { label: 'Z to A', value: data.summary.z_to_a_count }, { label: 'related health', value: data.summary.related_group_health_counts.total, health: data.summary.related_group_health_counts }]} />
      <GroupChips groups={listOrEmpty(data.groups)} showHealth />
      <div className="rounded-lg bg-muted/25 px-3 py-2 text-xs text-muted-foreground">{data.coverage_note} {data.health_context_note}</div>
      {!data.source_available ? <CalmMessage title="Multicast branch telemetry is unavailable for this environment." /> : data.branch_total === 0 ? <CalmMessage title="No current multicast branches observed on this link." body={data.coverage_note} /> : <div className="space-y-3"><h3 className="text-sm font-medium">Directional observed branches</h3><LinkBranchTable branches={branches} /><Pagination total={data.branch_total} limit={data.limit || PAGE_SIZE} offset={offset} onOffsetChange={onOffsetChange} /></div>}
    </>
  )
}

export function DeviceMulticastDeliveryPanel({ devicePk }: { devicePk: string }) {
  const [offset, setOffset] = useState(0)
  const query = useQuery({ queryKey: ['deviceMulticastDelivery', devicePk, offset], queryFn: () => fetchDeviceMulticastDelivery(devicePk, { limit: PAGE_SIZE, offset, endpointLimit: ENDPOINT_PAGE_SIZE }), enabled: !!devicePk })
  if (query.isLoading) return <PanelSkeleton />
  return <PanelFrame title="Multicast delivery" subtitle="Observed forwarding plus device health context" generatedAt={query.data?.generated_at} freshness={query.data?.freshness} isFetching={query.isFetching} onRefresh={() => query.refetch()}>{query.error || !query.data ? <CalmMessage icon title="Unable to load multicast delivery state." body="The rest of this device page is unaffected." /> : <DevicePanelContent data={query.data} offset={offset} onOffsetChange={setOffset} />}</PanelFrame>
}

export function LinkMulticastDeliveryPanel({ linkPk }: { linkPk: string }) {
  const [offset, setOffset] = useState(0)
  const query = useQuery({ queryKey: ['linkMulticastDelivery', linkPk, offset], queryFn: () => fetchLinkMulticastDelivery(linkPk, { limit: PAGE_SIZE, offset }), enabled: !!linkPk })
  if (query.isLoading) return <PanelSkeleton />
  return <PanelFrame title="Multicast carriage" subtitle="Observed branch state with related group health context" generatedAt={query.data?.generated_at} freshness={query.data?.freshness} isFetching={query.isFetching} onRefresh={() => query.refetch()}>{query.error || !query.data ? <CalmMessage icon title="Unable to load multicast branch state." body="The rest of this link page is unaffected." /> : <LinkPanelContent data={query.data} offset={offset} onOffsetChange={setOffset} />}</PanelFrame>
}
