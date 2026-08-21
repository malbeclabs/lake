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
  type EdgeMulticastChannelInstance,
  type EdgeMulticastGroup,
  type EdgeMulticastPublisher,
  type EdgeMulticastSequenceHealth,
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

// Where a drill-down out of this page comes back to. Every detail page in the app reads
// `state.back` (use-back-link.ts) and otherwise falls back to its own list page — so without this,
// a reader who opened a publisher from here would be sent to /dz/users on the way out, a page they
// were never on.
const EDGE_MULTICAST_BACK = { back: { to: '/dz/edge/multicast', label: 'Edge Multicast' } }

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

// The five states `health` can carry. Silent is the only red here: publishers were measured and none
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

// Sequence-counter states. A gap is recorded data loss on the wire protocol — the one thing on
// this page that is a fault in the FEED itself rather than in one member of it — so it gets the
// red. 'stalled' is amber: the series stopped advancing, which is either a dead publisher path or
// a dead recorder, and this column cannot tell those apart.
const SEQUENCE_BADGE: Record<string, string> = {
  ok: 'bg-emerald-500/15 text-emerald-500',
  gapped: 'bg-red-500/15 text-red-500',
  stalled: 'bg-amber-500/15 text-amber-500',
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
// own recorded sequence series, its own verdict against the floor — none of which survives the
// group roll-up above.
function PublisherLineRow({
  line,
  asOf,
  showLastHeard,
  showSequence,
  sequenceAsOfAge,
  floorBps,
}: {
  line: EdgeMulticastPublisher
  asOf: number
  showLastHeard: boolean
  showSequence: boolean
  sequenceAsOfAge?: number
  floorBps: number
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
      {/* The DoubleZero address first, because that is the source IP address the datagrams
          carry and the one the recorders and the allow-lists talk about. The client IP is the
          box's own public address — the key the operator override table uses — and both are
          worth having on the line. */}
      <td className="pl-8 pr-3 py-1.5 whitespace-nowrap">
        <span className="inline-flex items-center gap-2">
          {/* The address links to the ledger User behind it: a source that is gapping is a
              question about that account — its tunnel, its device, its access pass — and this
              page already holds the pk. stopPropagation because the row click opens the group. */}
          <Link
            to={`/dz/users/${line.user_pk}`}
            state={EDGE_MULTICAST_BACK}
            onClick={(e) => e.stopPropagation()}
            className="font-mono text-xs hover:underline"
          >
            {line.dz_ip || line.client_ip}
          </Link>
          {/* The classification rides with the identity rather than in the Subscribers column:
              a publisher line has nothing to say about that side of the group. */}
          {label && <span className="text-muted-foreground">{label}</span>}
        </span>
      </td>
      <td className="px-3 py-1.5 whitespace-nowrap">
        <CopyableText text={line.client_ip} className="font-mono text-xs text-muted-foreground" />
      </td>
      <td className="px-3 py-1.5 whitespace-nowrap text-muted-foreground">
        <span className="font-mono">{line.device_code || '—'}</span>
        {line.tunnel_id ? <span className="text-xs"> tun {line.tunnel_id}</span> : null}
      </td>
      <td className="px-3 py-1.5 whitespace-nowrap">
        <span className="inline-flex items-center gap-2">
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
          {/* A publisher with no BGP session cannot be sending the feed it is registered to send.
              Rendered next to the counter verdict rather than replacing it, because the two can
              legitimately disagree: the ledger snapshot and the rate bucket are minutes apart, so
              a publisher can read 'down' here while its tunnel still moved bytes. */}
          {line.bgp_status === 'down' && (
            <Tooltip content="BGP session down in the ledger: this publisher has no session to send the feed over. The counter verdict beside it is measured minutes apart, so the two can disagree.">
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full bg-red-500/15 text-red-500 text-[10px] font-medium">
                <AlertCircle className="h-3 w-3" />
                BGP down
              </span>
            </Tooltip>
          )}
        </span>
      </td>
      <td className="px-3 py-1.5" />
      <td className="px-3 py-1.5 text-right whitespace-nowrap">
        {line.bps === null ? (
          <span className="text-muted-foreground">no data</span>
        ) : (
          <RateCell bps={line.bps} ambiguous={line.multi_group} stale={stale} />
        )}
      </td>
      <td className="px-3 py-1.5 text-right whitespace-nowrap">
        {age === undefined ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <span className={stale ? 'text-amber-500' : 'text-muted-foreground'}>{formatAge(age)}</span>
        )}
      </td>
      {/* "Heard" is per group — the recorders' own plane, with nothing to say about one
          publisher — but a sequence series is per publisher, so that column is filled in. */}
      {showLastHeard && <td className="px-3 py-1.5" />}
      {showSequence && (
        <td className="px-3 py-1.5 whitespace-nowrap">
          <PublisherSequenceCell sequence={line.sequence} asOfAge={sequenceAsOfAge} />
        </td>
      )}
      <td className="px-3 py-1.5" />
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

// One channel instance as a tooltip line. Gap counts are BOOKS, never gap-marked messages — see
// KalshiL2Lane, which documents why the message count is a duration rather than a fault count.
function sequenceInstanceLine(i: EdgeMulticastChannelInstance): string {
  const from = i.publisher_source_ip ? `${i.publisher_source_ip} ` : ''
  return (
    `${from}ch${i.channel_id} @${i.node} (${i.capture_source}): ${i.messages.toLocaleString()} msgs, ` +
    `${i.gap_books.toLocaleString()} book(s) gapped, ${i.resets.toLocaleString()} resets, ` +
    `${i.snapshot_cycles.toLocaleString()} snapshot cycles`
  )
}

// The badge, shared by the group roll-up and the publisher lines. One instance per tooltip line:
// these are the only tooltips on the page with a list in them, and run together as a paragraph
// they are unreadable.
function SequenceBadge({ status, count, detail }: { status: string; count: string; detail: string }) {
  return (
    <Tooltip content={detail} className="whitespace-pre-line">
      <span className="inline-flex items-center gap-1.5">
        <span
          className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${
            SEQUENCE_BADGE[status] ?? 'bg-muted text-muted-foreground'
          }`}
        >
          {status}
        </span>
        {count && <span className="text-xs text-muted-foreground tabular-nums">{count}</span>}
      </span>
    </Tooltip>
  )
}

// The sequence verdict for ONE publisher: the recorded wire protocol's own counters for the series
// it emitted, and the only thing on this page that can say "this path lost data" as opposed to
// "this member is quiet". A series is owned by one path — two paths carrying one channel cannot
// share a counter — so this, not the group cell, is where the verdict belongs.
function PublisherSequenceCell({
  sequence,
  asOfAge,
}: {
  sequence?: EdgeMulticastSequenceHealth
  asOfAge?: number
}) {
  // Nothing was recorded from this publisher's address. Not a verdict of any kind, so no badge:
  // the group cell says how many series were attributed and how many were not.
  if (!sequence || sequence.instances.length === 0) {
    return <span className="text-muted-foreground">—</span>
  }

  const total = sequence.instances.length
  const bad = sequence.gapped + sequence.stalled
  const detail = [
    ...sequence.instances.map(sequenceInstanceLine),
    asOfAge === undefined
      ? ''
      : `folded from the L2 coverage refresher, computed ${formatAge(asOfAge)}`,
  ]
    .filter(Boolean)
    .join('\n')

  return (
    <SequenceBadge
      status={sequence.status}
      count={bad > 0 ? `${bad}/${total}` : total > 1 ? String(total) : ''}
      detail={detail}
    />
  )
}

// The group's sequence cell, which is a ROLL-UP and says so: the count is publishers, because that
// is the grain a series has, and the verdict per publisher is on the publisher lines the row
// expands into. It stays on the group row for two reasons — the lines are collapsed by default on
// anything but the smallest groups, and a series recorded from an address no publisher of this
// group carries has no line to sit on and would otherwise go unreported.
function SequenceCell({
  sequence,
  asOfAge,
}: {
  sequence?: EdgeMulticastSequenceHealth
  asOfAge?: number
}) {
  if (!sequence || sequence.instances.length === 0) {
    return <span className="text-muted-foreground">—</span>
  }

  const instances = sequence.instances.length
  const publishers = sequence.publishers ?? 0
  const badPublishers = (sequence.publishers_gapped ?? 0) + (sequence.publishers_stalled ?? 0)
  const unattributed = sequence.unattributed ?? 0
  const detail = [
    publishers > 0
      ? `${publishers} publisher${publishers === 1 ? '' : 's'}, ${instances} channel instance${instances === 1 ? '' : 's'} — expand the row for the verdict per publisher`
      : `${instances} channel instance${instances === 1 ? '' : 's'}`,
    unattributed > 0
      ? `${unattributed} recorded from an address no publisher of this group carries, so ${unattributed === 1 ? 'it has' : 'they have'} no line of ${unattributed === 1 ? 'its' : 'their'} own`
      : '',
    ...sequence.instances.map(sequenceInstanceLine),
    asOfAge === undefined
      ? ''
      : `folded from the L2 coverage refresher, computed ${formatAge(asOfAge)}`,
  ]
    .filter(Boolean)
    .join('\n')

  // Publishers where they are known, instances where nothing was attributed: a count of series is
  // still better than no count, and it is what the tooltip's first line says it is.
  const count =
    publishers > 0
      ? badPublishers > 0
        ? `${badPublishers}/${publishers}`
        : String(publishers)
      : `${sequence.gapped + sequence.stalled > 0 ? `${sequence.gapped + sequence.stalled}/` : ''}${instances}`

  return <SequenceBadge status={sequence.status} count={count} detail={detail} />
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
  showSequence,
  sequenceAsOfAge,
  floorBps,
  columns,
  onOpen,
}: {
  group: EdgeMulticastGroup
  asOf: number
  now: number
  showLastHeard: boolean
  showSequence: boolean
  sequenceAsOfAge?: number
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
      <td className="px-3 py-3 whitespace-nowrap">
        <CopyableText text={group.code} className="font-mono text-sm" />
      </td>
      <td className="px-3 py-3 whitespace-nowrap">
        <CopyableText text={group.multicast_ip} className="font-mono text-sm text-muted-foreground" />
      </td>
      <td className="px-3 py-3 text-sm whitespace-nowrap">
        <PlaneCell plane={group.plane} />
      </td>
      <td className="px-3 py-3 text-sm">
        <PublisherCell
          group={group}
          stale={stale}
          floorBps={floorBps}
          expanded={expanded}
          onToggle={() => setExpanded((v) => !v)}
        />
      </td>
      <td className="px-3 py-3 text-sm">
        <SubscriberCell group={group} />
      </td>
      <td className="px-3 py-3 text-sm text-right">
        <RateCell bps={group.ingress_bps} ambiguous={group.traffic_ambiguous} stale={stale} />
      </td>
      <td className="px-3 py-3 text-sm text-right whitespace-nowrap">
        {age === undefined ? (
          <span className="text-muted-foreground">no data</span>
        ) : (
          <span className={stale ? 'text-amber-500' : 'text-muted-foreground'}>{formatAge(age)}</span>
        )}
      </td>
      {showLastHeard && (
        <td className="px-3 py-3 text-sm text-right whitespace-nowrap">
          <LastHeardCell group={group} now={now} />
        </td>
      )}
      {showSequence && (
        <td className="px-3 py-3 text-sm whitespace-nowrap">
          <SequenceCell sequence={group.sequence} asOfAge={sequenceAsOfAge} />
        </td>
      )}
      <td className="px-3 py-3 text-sm">
        {/* Straight to the reconciliation view for this group — the row itself opens the
            publisher list, which is the more common next step. */}
        <Link
          to={`/dz/multicast-groups/${group.pk}?tab=health`}
          state={EDGE_MULTICAST_BACK}
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
          showLastHeard={showLastHeard}
          showSequence={showSequence}
          sequenceAsOfAge={sequenceAsOfAge}
          floorBps={floorBps}
        />
      ))}
    {expanded && hidden > 0 && (
      <tr className="border-b border-border/50 bg-muted/20 text-xs">
        {/* Worst-first ordering is what makes this safe to truncate: everything failing the
            floor is above the cut. */}
        <td className="pl-8 pr-3 py-1.5 text-muted-foreground" colSpan={columns}>
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
  showSequence,
  sequenceAsOfAge,
  floorBps,
  onOpen,
}: {
  service: EdgeMulticastService
  asOf: number
  now: number
  showLastHeard: boolean
  showSequence: boolean
  sequenceAsOfAge?: number
  floorBps: number
  onOpen: (e: React.MouseEvent, pk: string) => void
}) {
  // Only the truncation notice spans the table now; the publisher lines carry a cell per column,
  // so the count still has to match the header exactly — a mismatch silently shifts every group
  // column one to the left.
  const columns = 8 + (showLastHeard ? 1 : 0) + (showSequence ? 1 : 0)
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
              <th className="px-3 py-2 font-medium">Group</th>
              <th className="px-3 py-2 font-medium">Multicast IP</th>
              <th className="px-3 py-2 font-medium">Plane</th>
              <th className="px-3 py-2 font-medium">Publishers</th>
              <th className="px-3 py-2 font-medium">Subscribers</th>
              <th className="px-3 py-2 font-medium text-right">Ingress</th>
              <th className="px-3 py-2 font-medium text-right">Measured</th>
              {showLastHeard && <th className="px-3 py-2 font-medium text-right">Heard</th>}
              {showSequence && <th className="px-3 py-2 font-medium">Sequence</th>}
              <th className="px-3 py-2 font-medium">Health</th>
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
                showSequence={showSequence}
                sequenceAsOfAge={sequenceAsOfAge}
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

  // The sequence column exists only where a recorder runs the Edge wire protocol, which today is
  // the market-by-price groups alone. Dropped entirely rather than rendered as a screenful of
  // dashes, the same rule the Heard column follows.
  const showSequence = useMemo(
    () => (data?.services ?? []).some((s) => s.groups.some((g) => (g.sequence?.instances.length ?? 0) > 0)),
    [data],
  )

  // Read against wall clock, ticking: this measures how stale the refresher's cached numbers are,
  // which keeps ageing whether or not this page refetches.
  const sequenceAsOfAge = ageSecs(data?.sequence_as_of, now)

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
    handleRowClick(e, `/dz/multicast-groups/${pk}?tab=publishers`, navigate, EDGE_MULTICAST_BACK)
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
          the row. Health is taken per member, not per group: a feed is healthy when every publisher clears{' '}
          {formatBps(data?.publisher_floor_bps ?? 0)} and every recording node hears a share of the feed
          comparable with its peers. It reads{' '}
          <span className="text-amber-500 font-medium">thin</span> when a publisher is below that floor,{' '}
          <span className="text-amber-500 font-medium">skewed</span> when a recorder is far behind the others,
          and <span className="text-red-500 font-medium">silent</span> when publishers are registered and not
          one of them moved a byte. A group with no counter data is left blank rather than called down. Click a
          Publishers cell to list that group's publishers one line each.
          {data?.last_heard_available && (
            <>
              {' '}
              “Heard” is a different measurement altogether: when a recording node last received a
              message on the group, seconds rather than minutes old. It covers only the groups with a
              capture behind them, it is receive-side — a silent recorder looks the same as a silent
              publisher — and it never sets the silent flag for that reason.
            </>
          )}
          {showSequence && (
            <>
              {' '}
              “Sequence” is the recorded wire protocol's own counters, and the only column here that
              can say the feed lost data rather than that a member went quiet. A series is owned by one
              channel instance — (source IP, channel, recording node) — so the verdict sits on the
              publisher that emitted it, and the group cell is a roll-up over its publishers. Gap counts
              are books, never gap-marked messages. It is folded from the L2 coverage refresher, so it is
              minutes older than the rest of the row.
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
              showSequence={showSequence}
              sequenceAsOfAge={sequenceAsOfAge}
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
