import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useNavigate } from 'react-router-dom'
import { AlertCircle, ChevronDown, ChevronRight, Loader2, Radio } from 'lucide-react'
import { Tooltip } from '@/components/ui/tooltip'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'
import { handleRowClick } from '@/lib/utils'
import {
  fetchEdgeMulticast,
  type EdgeMulticastGroup,
  type EdgeMulticastPublisher,
  type EdgeMulticastService,
} from '@/lib/api'

// One screen for every multicast group in the Edge product — the groups whose ledger code carries
// the `edge-` prefix — showing who publishes, who receives, and whether anything is flowing. The per-group pages already answer this one group at a time; the
// question that needs a fleet view is "is any feed silent right now".
//
// Everything about freshness on this page is deliberate. Rates come from five-minute counter
// rollups that land several minutes late, so a green dot here means "traffic in the last bucket
// we can see", never "traffic this second". Every rate is rendered next to the age of the bucket
// it came from, and a bucket older than STALE_AFTER_SECS greys the row's numbers out instead of
// showing a confident colour over data nobody should trust.

// Past this age the rate columns stop asserting anything. Two rollup grains plus the observed
// pipeline lag: the normal steady state is 5-10 minutes behind, so a threshold much tighter than
// this would mark healthy feeds stale for most of every cycle.
const STALE_AFTER_SECS = 15 * 60

// Ages are computed here, from the payload's timestamps, and against two different clocks on
// purpose. The endpoint is served cache-first and rewritten once per refresh interval, so an age
// computed server-side at fetch time would be served unchanged for the rest of that interval.
//
//   - The counter columns are read against the payload's own generated_at. What they measure is
//     how far behind the rollup pipeline was when the numbers were computed — a property of the
//     data. Reading them against wall clock would add the cache's age to the pipeline's lag and
//     mark healthy feeds stale for most of every cycle, which is the alarm-always-on failure
//     mode kalshi-l2-page.tsx documents for the same reason.
//   - "Heard" is read against wall clock, ticking. That column exists to answer "is this feed
//     alive right now", and a recorder that stopped just after a refresh must not keep reading
//     "1s ago" until the next one. It ages in front of the reader, as it should.
//
// The payload's own age is in the header, so the two can always be reconciled.

// Keyed by feed family — the feed code without its plane suffix — because the API groups the
// planes of one product into a single section and gives each row a Plane instead.
const SERVICE_LABELS: Record<string, string> = {
  'solana-shreds-full': 'Solana Shreds',
  'kalshi-sports': 'Kalshi Sports',
  'kalshi-perps': 'Kalshi Perps',
  'edge-unclaimed': 'Edge groups with no feed row',
}

// An unlisted family renders under its own code rather than being dropped, so a product added to
// the ledger shows up here without a deploy.
function serviceLabel(code: string): string {
  return SERVICE_LABELS[code] ?? code
}

// The plane a group carries. Monospaced and dimmed rather than a coloured badge: it identifies the
// row, it does not signal anything, and a badge here would compete with the health column.
function PlaneCell({ plane }: { plane?: string }) {
  if (!plane) return <span className="text-muted-foreground">—</span>
  return <span className="font-mono text-xs text-muted-foreground">{plane}</span>
}

// The five states `health` can carry. Silent is the only red: publishers were measured and none
// of them sent anything. 'thin' (a publisher under the floor) and 'skewed' (a recorder far behind
// its peers) are amber — a real fault in one member of a feed that is otherwise flowing, which is
// a different call to action than a dead feed.
const HEALTH_BADGE: Record<string, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  thin: 'bg-amber-500/15 text-amber-500',
  skewed: 'bg-amber-500/15 text-amber-500',
  silent: 'bg-red-500/15 text-red-500',
  unknown: 'bg-muted text-muted-foreground',
}

// Per-publisher line states, dot colour. 'thin' and 'idle' are what the group-level dot used to
// hide: both are publishers failing the floor, and one of them next to a healthy peer is exactly
// the case this page could not previously show.
const PUBLISHER_DOT: Record<string, string> = {
  publishing: 'bg-emerald-500',
  thin: 'bg-amber-500',
  idle: 'bg-red-500',
  unknown: 'bg-muted-foreground',
}

// How a member's classification reads on a publisher line. Only the DoubleZero ones are labelled:
// 'customer' is the default nothing has asserted, and printing it on every line would present a
// guess as a fact — the same reason the subscriber tooltip says how much is actually classified.
const CLASS_LABEL: Record<string, string> = {
  recorder: 'DZ recorder',
  internal_probe: 'DZ probe',
}

function formatBps(bps: number): string {
  if (!bps) return '0'
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(2)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function formatAge(secs: number): string {
  if (secs < 60) return `${Math.round(secs)}s ago`
  if (secs < 3600) return `${Math.round(secs / 60)}m ago`
  return `${Math.round(secs / 3600)}h ago`
}

// Seconds between an ISO timestamp and the given clock. Negative ages (a bucket stamped slightly
// ahead of the payload, or a clock skewed browser) clamp to 0 rather than rendering "-3s ago".
function ageSecs(iso: string | undefined, nowMs: number): number | undefined {
  if (!iso) return undefined
  const t = new Date(iso).getTime()
  if (Number.isNaN(t)) return undefined
  return Math.max(0, (nowMs - t) / 1000)
}

// The badge carries the verdict; the tooltip says which of the two checks produced it and then
// the control-plane reconciliation breakdown, which is per-member and never rolls up to a group
// verdict — a customer with BGP down and one publisher missing from a device snapshot both live
// in there.
function HealthBadge({ group, floorBps }: { group: EdgeMulticastGroup; floorBps: number }) {
  if (!group.health) return <span className="text-muted-foreground">—</span>

  const lagging = (group.capture_nodes ?? []).filter((n) => n.lagging)
  const verdict = {
    healthy: `every publisher above ${formatBps(floorBps)}${
      (group.capture_nodes?.length ?? 0) > 1 ? '; recorders all within range of each other' : ''
    }`,
    thin: `${group.publishers_below_floor} of ${group.publisher_lines_total} publisher(s) below ${formatBps(floorBps)}`,
    skewed: lagging.length
      ? `publishers fine; ${lagging.map((n) => `${n.node} at ${Math.round(n.share_of_median * 100)}% of the group median`).join(', ')}`
      : 'a recording node is behind its peers',
    silent: 'publishers were measured and none of them moved a byte',
    unknown: 'no counter row for any publisher: a monitoring gap, not an outage',
  }[group.health]

  const c = group.health_counts
  const controlPlane = [
    c.unhealthy > 0 ? `${c.unhealthy} unhealthy` : '',
    c.degraded > 0 ? `${c.degraded} degraded` : '',
    c.disconnected > 0 ? `${c.disconnected} with BGP down` : '',
    c.healthy > 0 ? `${c.healthy} reconciled` : '',
  ]
    .filter(Boolean)
    .join(' · ')

  const detail = [verdict, controlPlane ? `Control plane, per member: ${controlPlane}` : '']
    .filter(Boolean)
    .join(' — ')

  const badge = (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${HEALTH_BADGE[group.health] ?? HEALTH_BADGE.unknown}`}>
      {group.health}
    </span>
  )
  if (!detail) return badge
  return <Tooltip content={detail}>{badge}</Tooltip>
}

// PublisherCell is the point of the page, and it counts publishers CLEARING THE FLOOR rather
// than publishers with a non-zero counter. The distinction is the whole reason the per-publisher
// lines exist: a tunnel carrying protocol overhead and no product used to count as active here.
//
//   green — every publisher is above the floor
//   amber — some are, some are not: the feed is flowing and one of its publishers is not
//   red   — publishers are registered and every counter reads zero
//   grey  — nothing measured them, a monitoring gap rather than an outage
//
// Clicking the cell expands the group's publisher lines, which is where "which one" is answered.
function PublisherCell({
  group,
  stale,
  floorBps,
  expanded,
  onToggle,
}: {
  group: EdgeMulticastGroup
  stale: boolean
  floorBps: number
  expanded: boolean
  onToggle: () => void
}) {
  const { publishers } = group
  if (publishers.total === 0) {
    return <span className="text-muted-foreground">—</span>
  }

  const below = group.publishers_below_floor
  const dot = group.silent
    ? 'bg-red-500'
    : stale || group.publishers_publishing === 0
      ? 'bg-muted-foreground'
      : below > 0
        ? 'bg-amber-500'
        : 'bg-emerald-500'

  const detail = [
    `${group.publishers_publishing} above ${formatBps(floorBps)}`,
    below > 0 ? `${below} below it` : '',
    publishers.unknown > 0 ? `${publishers.unknown} with no counter data` : '',
    'click to list them',
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <Tooltip content={detail}>
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation()
          onToggle()
        }}
        className="inline-flex items-center gap-1.5 tabular-nums hover:text-foreground"
      >
        {expanded ? (
          <ChevronDown className="h-3 w-3 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-3 w-3 text-muted-foreground" />
        )}
        <span className={`inline-block h-2 w-2 rounded-full ${dot}`} />
        <span>
          {group.publishers_publishing}
          <span className="text-muted-foreground">/{publishers.total}</span>
        </span>
      </button>
    </Tooltip>
  )
}

// One publisher, one line. Everything on it is per member: its own rate, its own bucket age, its
// own verdict against the floor — none of which survives the group roll-up above.
function PublisherLineRow({
  line,
  asOf,
  floorBps,
  columns,
}: {
  line: EdgeMulticastPublisher
  asOf: number
  floorBps: number
  columns: number
}) {
  const age = ageSecs(line.observed_at, asOf)
  const stale = age === undefined || age > STALE_AFTER_SECS
  const label = CLASS_LABEL[line.class]

  const statusDetail =
    line.status === 'thin'
      ? `below the ${formatBps(floorBps)} floor: the tunnel is moving something, not the product`
      : line.status === 'idle'
        ? 'counter read zero in the last visible bucket'
        : line.status === 'unknown'
          ? 'no counter row for this publisher: nothing measured it'
          : `at or above the ${formatBps(floorBps)} floor`

  return (
    <tr className="border-b border-border/50 last:border-b-0 bg-muted/20 text-xs">
      <td className="pl-10 pr-4 py-1.5 whitespace-nowrap">
        <span className="inline-flex items-center gap-2">
          <CopyableText text={line.client_ip} className="font-mono text-xs" />
          {/* The classification rides with the identity rather than in the Subscribers column:
              a publisher line has nothing to say about that side of the group. */}
          {label && <span className="text-muted-foreground">{label}</span>}
        </span>
      </td>
      <td className="px-4 py-1.5 whitespace-nowrap font-mono text-muted-foreground">
        {line.device_code || '—'}
      </td>
      <td className="px-4 py-1.5 whitespace-nowrap text-muted-foreground">
        {line.tunnel_id ? `tun ${line.tunnel_id}` : '—'}
      </td>
      <td className="px-4 py-1.5 whitespace-nowrap">
        <Tooltip content={statusDetail}>
          <span className="inline-flex items-center gap-1.5">
            <span
              className={`inline-block h-1.5 w-1.5 rounded-full ${PUBLISHER_DOT[line.status] ?? PUBLISHER_DOT.unknown}`}
            />
            <span className={line.status === 'publishing' ? 'text-muted-foreground' : ''}>
              {line.status}
            </span>
          </span>
        </Tooltip>
      </td>
      <td className="px-4 py-1.5" />
      <td className="px-4 py-1.5 text-right whitespace-nowrap">
        {line.bps === null ? (
          <span className="text-muted-foreground">no data</span>
        ) : (
          <RateCell bps={line.bps} ambiguous={line.multi_group} stale={stale} />
        )}
      </td>
      <td className="px-4 py-1.5" />
      <td className="px-4 py-1.5 text-right whitespace-nowrap">
        {age === undefined ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <span className={stale ? 'text-amber-500' : 'text-muted-foreground'}>{formatAge(age)}</span>
        )}
      </td>
      {/* The remaining columns belong to the group, not to one of its publishers. */}
      <td className="px-4 py-1.5" colSpan={columns - 8} />
    </tr>
  )
}

// Subscribers are split by whose box it is. The counts are only as good as the classification
// behind them, so the tooltip says how many are actually known — asserted by an operator or
// derived from the capture-host list — versus merely defaulted to customer.
function SubscriberCell({ group }: { group: EdgeMulticastGroup }) {
  const { subscribers } = group
  if (subscribers.total === 0) {
    return <span className="text-muted-foreground">—</span>
  }

  const dz = subscribers.recorders + subscribers.internal_probes
  const known = subscribers.class_asserted + subscribers.class_derived
  const detail = [
    `${subscribers.customers} customer(s)`,
    `${subscribers.recorders} recorder(s)`,
    subscribers.internal_probes > 0 ? `${subscribers.internal_probes} internal probe(s)` : '',
    `${subscribers.active} receiving traffic`,
    known === 0
      ? 'nothing is classified: every member defaults to customer'
      : `${known} of ${subscribers.total} classified (${subscribers.class_asserted} asserted, ${subscribers.class_derived} derived)`,
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <Tooltip content={detail}>
      <span className="tabular-nums">
        {subscribers.total}
        <span className="text-muted-foreground text-xs">
          {' '}
          ({subscribers.customers} cust{dz > 0 ? ` / ${dz} DZ` : ''})
        </span>
      </span>
    </Tooltip>
  )
}

// The application-plane column: when a recorder last actually received something on this group.
// It is the only number on the page that can be seconds old — but it is receive-side, so a blank
// is "no capture covers this group", never "the feed is dead".
function LastHeardCell({ group, now }: { group: EdgeMulticastGroup; now: number }) {
  const age = ageSecs(group.last_heard, now)
  if (age === undefined) {
    return <span className="text-muted-foreground">—</span>
  }
  const captureSources = group.last_heard_capture_sources ?? 1
  const detail = [
    `from ${group.last_heard_table}`,
    captureSources > 1
      ? `max over ${captureSources} capture sources — one dead capture source does not move this; see the Kalshi L2 page for the per-capture-source detail`
      : '',
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <Tooltip content={detail}>
      <span className="tabular-nums">
        {formatAge(age)}
        {captureSources > 1 && (
          <span className="text-muted-foreground text-xs"> ×{captureSources}</span>
        )}
      </span>
    </Tooltip>
  )
}

// Rates carry a tilde when the group's publishers also publish elsewhere from the same tunnel:
// counters are per interface, so the figure is an upper bound for this group. Hiding that would
// present a shared measurement as a per-group one.
function RateCell({ bps, ambiguous, stale }: { bps: number; ambiguous: boolean; stale: boolean }) {
  const body = (
    <span className={`tabular-nums ${stale ? 'text-muted-foreground' : ''}`}>
      {ambiguous && bps > 0 ? '~' : ''}
      {formatBps(bps)}
    </span>
  )
  if (!ambiguous) return body
  return (
    <Tooltip content="Upper bound: at least one publisher feeds several groups from one tunnel, and interface counters cannot be split between them.">
      {body}
    </Tooltip>
  )
}

// A group's publisher lines are open by default while the whole published set fits on screen —
// the market-data feeds have two publishers and that IS the view someone opens this page for. The
// shreds groups have hundreds, so they start collapsed rather than burying every other row.
const PUBLISHER_LINES_OPEN_BELOW = 5

function GroupRow({
  group,
  asOf,
  now,
  showLastHeard,
  floorBps,
  columns,
  onOpen,
}: {
  group: EdgeMulticastGroup
  asOf: number
  now: number
  showLastHeard: boolean
  floorBps: number
  columns: number
  onOpen: (e: React.MouseEvent, pk: string) => void
}) {
  const age = ageSecs(group.observed_at, asOf)
  const stale = age === undefined || age > STALE_AFTER_SECS
  const lines = group.publisher_lines ?? []
  const [expanded, setExpanded] = useState(lines.length > 0 && lines.length < PUBLISHER_LINES_OPEN_BELOW)
  const hidden = group.publisher_lines_total - lines.length

  return (
    <>
    <tr
      className="border-b border-border last:border-b-0 hover:bg-muted cursor-pointer transition-colors"
      onClick={(e) => onOpen(e, group.pk)}
    >
      <td className="px-4 py-3 whitespace-nowrap">
        <CopyableText text={group.code} className="font-mono text-sm" />
      </td>
      <td className="px-4 py-3 whitespace-nowrap">
        <CopyableText text={group.multicast_ip} className="font-mono text-sm text-muted-foreground" />
      </td>
      <td className="px-4 py-3 text-sm whitespace-nowrap">
        <PlaneCell plane={group.plane} />
      </td>
      <td className="px-4 py-3 text-sm">
        <PublisherCell
          group={group}
          stale={stale}
          floorBps={floorBps}
          expanded={expanded}
          onToggle={() => setExpanded((v) => !v)}
        />
      </td>
      <td className="px-4 py-3 text-sm">
        <SubscriberCell group={group} />
      </td>
      <td className="px-4 py-3 text-sm text-right">
        <RateCell bps={group.ingress_bps} ambiguous={group.traffic_ambiguous} stale={stale} />
      </td>
      <td className="px-4 py-3 text-sm text-right">
        <RateCell bps={group.egress_bps} ambiguous={group.traffic_ambiguous} stale={stale} />
      </td>
      <td className="px-4 py-3 text-sm text-right whitespace-nowrap">
        {age === undefined ? (
          <span className="text-muted-foreground">no data</span>
        ) : (
          <span className={stale ? 'text-amber-500' : 'text-muted-foreground'}>{formatAge(age)}</span>
        )}
      </td>
      {showLastHeard && (
        <td className="px-4 py-3 text-sm text-right whitespace-nowrap">
          <LastHeardCell group={group} now={now} />
        </td>
      )}
      <td className="px-4 py-3 text-sm">
        {/* Straight to the reconciliation view for this group — the row itself opens the
            publisher list, which is the more common next step. */}
        <Link
          to={`/dz/multicast-groups/${group.pk}?tab=health`}
          onClick={(e) => e.stopPropagation()}
          className="inline-flex"
        >
          <HealthBadge group={group} floorBps={floorBps} />
        </Link>
      </td>
    </tr>
    {expanded &&
      lines.map((line) => (
        <PublisherLineRow
          key={`${group.pk}-${line.user_pk}`}
          line={line}
          asOf={asOf}
          floorBps={floorBps}
          columns={columns}
        />
      ))}
    {expanded && hidden > 0 && (
      <tr className="border-b border-border/50 bg-muted/20 text-xs">
        {/* Worst-first ordering is what makes this safe to truncate: everything failing the
            floor is above the cut. */}
        <td className="pl-10 pr-4 py-1.5 text-muted-foreground" colSpan={columns}>
          +{hidden} more publisher{hidden === 1 ? '' : 's'}, all above the floor
        </td>
      </tr>
    )}
    </>
  )
}

function ServiceSection({
  service,
  asOf,
  now,
  showLastHeard,
  floorBps,
  onOpen,
}: {
  service: EdgeMulticastService
  asOf: number
  now: number
  showLastHeard: boolean
  floorBps: number
  onOpen: (e: React.MouseEvent, pk: string) => void
}) {
  // The publisher lines fill the first eight columns and blank the rest, so the count has to
  // match the header exactly — a mismatch silently shifts every group column one to the left.
  const columns = showLastHeard ? 10 : 9
  const silent = service.groups.filter((g) => g.silent).length

  return (
    <div className="border border-border rounded-lg overflow-hidden bg-card">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-4 py-3 border-b border-border">
        <h2 className="text-sm font-medium">{serviceLabel(service.code)}</h2>
        <span className="text-xs text-muted-foreground">
          {service.groups.length} group{service.groups.length === 1 ? '' : 's'}
          {service.managed && service.metro_count > 0 && ` · sold in ${service.metro_count} metros`}
          {!service.managed && ' · no feed row in the ledger'}
        </span>
        {silent > 0 && (
          <span className="text-[10px] font-medium uppercase px-1.5 py-0.5 rounded-full bg-red-500/15 text-red-500">
            {silent} silent
          </span>
        )}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="text-sm text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-2 font-medium">Group</th>
              <th className="px-4 py-2 font-medium">Multicast IP</th>
              <th className="px-4 py-2 font-medium">Plane</th>
              <th className="px-4 py-2 font-medium">Publishers</th>
              <th className="px-4 py-2 font-medium">Subscribers</th>
              <th className="px-4 py-2 font-medium text-right">Ingress</th>
              <th className="px-4 py-2 font-medium text-right">Egress</th>
              <th className="px-4 py-2 font-medium text-right">Measured</th>
              {showLastHeard && <th className="px-4 py-2 font-medium text-right">Heard</th>}
              <th className="px-4 py-2 font-medium">Health</th>
            </tr>
          </thead>
          <tbody>
            {service.groups.map((g) => (
              <GroupRow
                key={`${service.code}-${g.pk}`}
                group={g}
                asOf={asOf}
                now={now}
                showLastHeard={showLastHeard}
                floorBps={floorBps}
                columns={columns}
                onOpen={onOpen}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function EdgeMulticastPage() {
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['edge-multicast'],
    queryFn: fetchEdgeMulticast,
    refetchInterval: 30000,
  })

  // A ticking clock rather than Date.now() during render, so the "Heard" column keeps ageing
  // between refetches instead of freezing at whatever the last render saw.
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 5000)
    return () => clearInterval(tick)
  }, [])

  // When the payload was computed. Judgements about the counter data are made against this and
  // not against wall clock — see the note at the top of the file. With no payload clock yet, fall
  // back to the ticking one.
  const asOf = useMemo(
    () => (data?.generated_at ? new Date(data.generated_at).getTime() : now),
    [data?.generated_at, now],
  )

  const { groupCount, silentCount } = useMemo(() => {
    const services = data?.services ?? []
    // A group claimed by two feeds appears in both sections; count it once here so the header
    // reports groups, not rows.
    const seen = new Set<string>()
    let silent = 0
    for (const s of services) {
      for (const g of s.groups) {
        if (seen.has(g.pk)) continue
        seen.add(g.pk)
        if (g.silent) silent++
      }
    }
    return { groupCount: seen.size, silentCount: silent }
  }, [data])

  const onOpen = (e: React.MouseEvent, pk: string) => {
    // The group page's publishers tab carries the per-source rows and the per-member traffic
    // chart — the drill-down this table exists to hand off to.
    handleRowClick(e, `/dz/multicast-groups/${pk}?tab=publishers`, navigate)
  }

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
          <div className="text-lg font-medium mb-2">Unable to load multicast overview</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Radio}
          title="Edge Multicast"
          count={groupCount}
          subtitle={
            <span className="flex items-center gap-2 text-sm">
              {silentCount > 0 && (
                <span className="text-red-500">
                  {silentCount} silent group{silentCount === 1 ? '' : 's'}
                </span>
              )}
              {data?.generated_at && (
                <span className="text-xs text-muted-foreground/50">computed {formatAge(ageSecs(data.generated_at, now) ?? 0)}</span>
              )}
            </span>
          }
        />

        <p className="text-xs text-muted-foreground mb-6 max-w-3xl">
          Rates are {data?.rate_grain_minutes ?? 5}-minute counter rollups measured at each member's tunnel and
          land a few minutes behind wall clock — the “Measured” column is the age of the newest bucket behind
          the row. A group is <span className="text-red-500 font-medium">silent</span> when its publishers are
          registered and their counters read zero; a group with no counter data is left blank rather than
          called down.
          {data?.last_heard_available && (
            <>
              {' '}
              “Heard” is a different measurement altogether: when a recording node last received a
              message on the group, seconds rather than minutes old. It covers only the groups with a
              capture behind them, it is receive-side — a silent recorder looks the same as a silent
              publisher — and it never sets the silent flag for that reason.
            </>
          )}
        </p>

        <div className="space-y-6">
          {(data?.services ?? []).map((s) => (
            <ServiceSection
              key={s.code}
              service={s}
              asOf={asOf}
              now={now}
              showLastHeard={data?.last_heard_available ?? false}
              floorBps={data?.publisher_floor_bps ?? 0}
              onOpen={onOpen}
            />
          ))}
          {(data?.services ?? []).length === 0 && (
            <div className="border border-border rounded-lg bg-card px-4 py-8 text-center text-muted-foreground">
              No multicast groups found
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
