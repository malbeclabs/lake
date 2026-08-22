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
  type EdgeMulticastPathParity,
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
  // Matched by owner wallet, which establishes whose box it is and stops there — one wallet holds
  // recorders, probes and lab boxes at once. The label has to stop there too.
  doublezero: 'DZ',
}

// Per-publisher verdicts. Same palette as the group badge used to use, minus 'skewed', which is a
// statement about a group's recorders and belongs to no single publisher.
const PUBLISHER_HEALTH_BADGE: Record<string, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  thin: 'bg-amber-500/15 text-amber-500',
  stalled: 'bg-amber-500/15 text-amber-500',
  behind: 'bg-amber-500/15 text-amber-500',
  gapped: 'bg-red-500/15 text-red-500',
  silent: 'bg-red-500/15 text-red-500',
  unknown: 'bg-muted text-muted-foreground',
}

// What each verdict means, spelled out: the badge is one word and the difference between "nothing
// measured it" and "it measured zero" is the difference between a monitoring gap and an outage.
const PUBLISHER_HEALTH_DETAIL: Record<string, string> = {
  healthy: 'above the floor, and its recorded series is intact',
  thin: 'the tunnel is moving something, not the product',
  stalled: 'its recorded series stopped advancing: a dead path or a dead recorder',
  behind: 'this path is delivering less of the feed than its redundant peer, at the same recorder',
  gapped: 'its recorded series lost data on the wire',
  silent: 'its counter read zero in the last visible bucket',
  unknown: 'no counter row for this publisher: nothing measured it',
}

// Sequence-counter states. A gap is recorded data loss on the wire protocol — the one thing on
// this page that is a fault in the FEED itself rather than in one member of it — so it gets the
// red. 'stalled' is amber: the series stopped advancing, which is either a dead publisher path or
// a dead recorder, and this column cannot tell those apart.
const SEQUENCE_BADGE: Record<string, string> = {
  ok: 'bg-emerald-500/15 text-emerald-500',
  gapped: 'bg-red-500/15 text-red-500',
  stalled: 'bg-amber-500/15 text-amber-500',
  // Top-of-book: the series is advancing and nothing checked it for loss, because that plane
  // records no gap marker. Outlined rather than filled, so a glance down the column cannot read
  // it as the same clean bill of health the market-by-price rows carry.
  advancing: 'border border-emerald-500/40 text-emerald-500',
}

// What to call an 'ok' whose gap count was never taken. Every instance behind the badge has to be
// unmeasured for this: a mixed set is still reporting a real zero for the half that was checked.
function sequenceLabel(status: string, total: number, gapsUnmeasured: number): string {
  if (status === 'ok' && total > 0 && gapsUnmeasured === total) return 'advancing'
  return status
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


// PublisherCell is the point of the page, and it counts publishers CLEARING THE FLOOR rather
// than publishers with a non-zero counter. The distinction is the whole reason the per-publisher
// lines exist: a tunnel carrying protocol overhead and no product used to count as active here.
//
//   green — every publisher is above the floor
//   amber — at least one line is faulted: the feed is flowing and one of its publishers is not
//   red   — every line is silent: publishers are registered and no counter moved
//   grey  — nothing measured them, a monitoring gap rather than an outage
//
// The colour reads the per-LINE verdicts, not the floor tally alone, and that is the whole point:
// the health badge moved to the lines, so a group nobody expanded would otherwise summarise itself
// on the counter plane and read clean while one of its series was gapping. It stays a count of
// lines with a colour, never a verdict word — the verdict belongs to the publisher.
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
  const v = group.publisher_verdicts
  const faulted = v.thin + v.gapped + v.stalled + v.behind + v.silent
  // Measured is healthy plus faulted: a line nothing measured cannot colour the cell either way.
  const measured = v.healthy + faulted
  const dot =
    measured === 0 || stale
      ? 'bg-muted-foreground'
      : v.silent === measured
        ? 'bg-red-500'
        : faulted > 0
          ? 'bg-amber-500'
          : 'bg-emerald-500'

  const detail = [
    `${v.healthy} of ${publishers.total} publisher(s) healthy`,
    v.silent > 0 ? `${v.silent} silent` : '',
    v.thin > 0 ? `${v.thin} below ${formatBps(floorBps)}` : '',
    v.gapped > 0 ? `${v.gapped} with a gapped series` : '',
    v.stalled > 0 ? `${v.stalled} with a stalled series` : '',
    v.behind > 0 ? `${v.behind} behind their redundant peer` : '',
    v.unknown > 0 ? `${v.unknown} with no counter data — not a fault, nothing measured them` : '',
    below > 0 && below !== v.thin + v.silent
      ? `${below} below the floor on the counter plane`
      : '',
    'click to list them, one verdict each',
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
          {v.healthy}
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
  showObservations,
  sequenceAsOfAge,
  floorBps,
}: {
  line: EdgeMulticastPublisher
  asOf: number
  showLastHeard: boolean
  showSequence: boolean
  showObservations: boolean
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
      {/* The client IP first, because that is the box — its own public address, the key the
          operator override table uses, and what an operator recognises. The DoubleZero address
          goes under Multicast IP, which is the column's role on this table: the group row holds
          the destination address, the publisher line holds the source IP address its datagrams
          carry, which is what the recorders and the allow-lists talk about. */}
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
            {line.client_ip}
          </Link>
          {/* The classification rides with the identity rather than in the Subscribers column:
              a publisher line has nothing to say about that side of the group. */}
          {label && <span className="text-muted-foreground">{label}</span>}
        </span>
      </td>
      <td className="px-3 py-1.5 whitespace-nowrap">
        {line.dz_ip ? (
          <CopyableText text={line.dz_ip} className="font-mono text-xs text-muted-foreground" />
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
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
      {showObservations && (
        <td className="px-3 py-1.5 text-right whitespace-nowrap">
          <MsgRateCell msgPerSec={line.msg_per_sec} />
        </td>
      )}
      {showObservations && (
        <td className="px-3 py-1.5 text-right whitespace-nowrap">
          <PeerParityCell parity={line.path_parity} />
        </td>
      )}
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
      <td className="px-3 py-1.5">
        <PublisherHealthBadge health={line.health} parity={line.path_parity} />
      </td>
    </tr>
  )
}

// What the recorders actually received from this path, per second.
//
// It sits next to the counter rate rather than replacing it because the two answer different
// questions and fail differently. The counter is per tunnel and minutes late — an upper bound that
// a multi-group publisher shares across its groups. This is per group and comes from the far end,
// so it is what arrived rather than what was sent, and it is blank for any feed with no recorder
// behind it. Neither is redundant with the other.
function MsgRateCell({ msgPerSec }: { msgPerSec?: number }) {
  if (msgPerSec === undefined) {
    return <span className="text-muted-foreground">—</span>
  }
  const text = msgPerSec >= 100 ? msgPerSec.toFixed(0) : msgPerSec.toFixed(1)
  return (
    <Tooltip content="messages the recorders received from this path, per second over the observation window — per group, unlike the counter rate beside it">
      <span className="tabular-nums text-muted-foreground">{text}</span>
    </Tooltip>
  )
}

// This path's delivery against its redundant peer, at the recorder that saw both.
//
// The number is the point, not a badge: redundant paths carry the same feed, so anything below 1
// is this path losing something its peer did not, and the eye should be able to scan the column
// for a digit that is not a 1. An em dash means there was no peer to compare against, which is not
// a pass.
function PeerParityCell({ parity }: { parity?: EdgeMulticastPathParity }) {
  if (!parity || parity.compared === 0) {
    return <span className="text-muted-foreground">—</span>
  }
  const behind = parity.behind > 0
  return (
    <Tooltip
      content={
        behind
          ? `behind its peer on ${parity.behind} of ${parity.compared} compared, worst ${parity.worst_source ?? ''}${parity.worst_node ? ` at ${parity.worst_node}` : ''}`
          : `level with its peer across ${parity.compared} compared`
      }
    >
      <span className={`tabular-nums ${behind ? 'text-amber-500' : 'text-muted-foreground'}`}>
        {parity.worst_ratio.toFixed(3)}
      </span>
    </Tooltip>
  )
}

// The verdict for ONE publisher, which is where it belongs: a group badge over a feed with one
// dead publisher and one live one describes neither of them.
//
// It folds the counter status and the publisher's own sequence series. It does NOT fold BGP, which
// keeps its own marker in the Publishers cell — the ledger snapshot and the rate bucket are
// minutes apart, so a publisher can read 'down' there while its tunnel still moved bytes, and both
// statements are worth having separately.
function PublisherHealthBadge({
  health,
  parity,
}: {
  health?: string
  parity?: EdgeMulticastPathParity
}) {
  if (!health) return <span className="text-muted-foreground">—</span>
  // The ratio earns its place only on the verdict it produced: everywhere else it is a passing
  // number nobody is asking about, and this column has one job.
  const detail =
    health === 'behind' && parity
      ? `${PUBLISHER_HEALTH_DETAIL.behind} — ${(parity.worst_ratio * 100).toFixed(1)}% of its peer on ${parity.worst_source ?? 'that feed'}${parity.worst_node ? ` at ${parity.worst_node}` : ''}, ${parity.behind} of ${parity.compared} compared`
      : (PUBLISHER_HEALTH_DETAIL[health] ?? health)
  return (
    <Tooltip content={detail}>
      <span
        className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${
          PUBLISHER_HEALTH_BADGE[health] ?? 'bg-muted text-muted-foreground'
        }`}
      >
        {health}
      </span>
    </Tooltip>
  )
}

// This page is about the feed and the publishers that fill it, not about who buys it. What the
// subscriber side still owes it is the measurement apparatus: how many of a group's receivers are
// DoubleZero's own boxes, because those are the recorders every application-plane signal on the
// row — Heard, Sequence, Msg/s, Peer — is measured at. The customer split stays in the payload for
// the drill-down and is deliberately not on screen.
function RecorderCell({ group }: { group: EdgeMulticastGroup }) {
  const { subscribers } = group
  if (subscribers.total === 0) {
    return <span className="text-muted-foreground">—</span>
  }

  // 'doublezero' is the wallet tier: ours, kind not asserted. Counted here because a box we run
  // that receives the feed is apparatus either way; the tooltip keeps the distinction visible.
  const ours = subscribers.recorders + subscribers.internal_probes + subscribers.doublezero
  const known = subscribers.class_asserted + subscribers.class_derived
  const detail = [
    `${subscribers.recorders} asserted recorder(s)`,
    subscribers.internal_probes > 0 ? `${subscribers.internal_probes} internal probe(s)` : '',
    subscribers.doublezero > 0
      ? `${subscribers.doublezero} DoubleZero-operated, kind not asserted`
      : '',
    `${subscribers.total - ours} other receiver(s)`,
    known === 0
      ? 'nothing is classified: every member defaults to customer'
      : `${known} of ${subscribers.total} classified (${subscribers.class_asserted} asserted, ${subscribers.class_derived} derived)`,
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <Tooltip content={detail}>
      <span className="tabular-nums">
        {ours === 0 ? <span className="text-muted-foreground">none</span> : ours}
        <span className="text-muted-foreground text-xs"> /{subscribers.total}</span>
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
  const head = `${from}ch${i.channel_id} @${i.node} (${i.capture_source}): ${i.messages.toLocaleString()} msgs`
  // Without a gap marker there is no book-level fault count and no snapshot cycle to report, so
  // the line says what was NOT measured instead of printing zeros that would read as findings.
  if (!i.gaps_measured) {
    return `${head}, ${i.resets.toLocaleString()} resets — gap counting not available on this plane`
  }
  return (
    `${head}, ${i.gap_books.toLocaleString()} book(s) gapped, ${i.resets.toLocaleString()} resets, ` +
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
    asOfAge === undefined ? '' : `computed ${formatAge(asOfAge)}`,
  ]
    .filter(Boolean)
    .join('\n')

  return (
    <SequenceBadge
      status={sequenceLabel(sequence.status, total, sequence.gaps_unmeasured ?? 0)}
      count={bad > 0 ? `${bad}/${total}` : total > 1 ? String(total) : ''}
      detail={detail}
    />
  )
}

// What is left on the group row once the verdict moves to the publisher lines: the series that
// have no line to sit on. A series recorded from an address no publisher of this group carries is
// the one thing the per-publisher view structurally cannot show, and dropping it silently is the
// outcome this column exists to prevent.
function UnattributedSequenceCell({ sequence }: { sequence?: EdgeMulticastSequenceHealth }) {
  const unattributed = sequence?.unattributed ?? 0
  if (unattributed === 0) {
    return <span className="text-muted-foreground">—</span>
  }
  return (
    <Tooltip
      content={`${unattributed} recorded series from ${unattributed === 1 ? 'an address' : 'addresses'} no publisher of this group carries, so ${unattributed === 1 ? 'it has' : 'they have'} no line of ${unattributed === 1 ? 'its' : 'their'} own`}
    >
      <span className="inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium bg-amber-500/15 text-amber-500">
        {unattributed} unattributed
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
  showSequence,
  showObservations,
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
  showObservations: boolean
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
        <RecorderCell group={group} />
      </td>
      <td className="px-3 py-3 text-sm text-right">
        <RateCell bps={group.ingress_bps} ambiguous={group.traffic_ambiguous} stale={stale} />
      </td>
      {/* Msg/s and Peer are per PATH and the group row carries neither. Summing recorded message
          rates over a group's paths would double the feed — redundant paths carry the same
          traffic — and a parity ratio has no meaning until you name which path it is about. */}
      {showObservations && <td className="px-3 py-3" />}
      {showObservations && <td className="px-3 py-3" />}
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
      {/* Sequence and Health are per PUBLISHER and the group row carries neither. A series is
          owned by one publisher and a floor is cleared by one publisher, so a badge here is a
          worst-of that names nobody — on a two-publisher feed with one path dead it reads the
          same as a feed with one path merely quiet. The group row identifies and counts; the
          lines it expands into carry the verdicts. What the group cell still has to report is
          what no line can: series recorded from an address no publisher of this group carries. */}
      {showSequence && (
        <td className="px-3 py-3 text-sm whitespace-nowrap">
          <UnattributedSequenceCell sequence={group.sequence} />
        </td>
      )}
      <td className="px-3 py-3 text-sm">
        {/* The row click opens the group; this keeps the direct route to its reconciliation
            view, which is the drill-down the verdict used to hang off. */}
        <Link
          to={`/dz/multicast-groups/${group.pk}?tab=health`}
          state={EDGE_MULTICAST_BACK}
          onClick={(e) => e.stopPropagation()}
          className="text-xs text-muted-foreground hover:text-foreground hover:underline"
        >
          reconcile
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
          showObservations={showObservations}
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
  showObservations,
  sequenceAsOfAge,
  floorBps,
  onOpen,
}: {
  service: EdgeMulticastService
  asOf: number
  now: number
  showLastHeard: boolean
  showSequence: boolean
  showObservations: boolean
  sequenceAsOfAge?: number
  floorBps: number
  onOpen: (e: React.MouseEvent, pk: string) => void
}) {
  // Only the truncation notice spans the table now; the publisher lines carry a cell per column,
  // so the count still has to match the header exactly — a mismatch silently shifts every group
  // column one to the left.
  const columns = 8 + (showObservations ? 2 : 0) + (showLastHeard ? 1 : 0) + (showSequence ? 1 : 0)
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
              <th className="px-3 py-2 font-medium">Recorders</th>
              <th className="px-3 py-2 font-medium text-right">Ingress</th>
              {showObservations && <th className="px-3 py-2 font-medium text-right">Msg/s</th>}
              {showObservations && <th className="px-3 py-2 font-medium text-right">Peer</th>}
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
                showObservations={showObservations}
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

  // Msg/s and Peer come from the observations refresher, and its payload can be absent — a new
  // cache key is empty until the first cycle after a deploy, and a feed with no recorder behind it
  // never gets one at all. Dropped entirely rather than rendered as two columns of dashes, the
  // same rule Heard and Sequence follow. Keyed on msg_per_sec because every path the refresher saw
  // has one, where path_parity needs a peer to exist.
  const showObservations = useMemo(
    () =>
      (data?.services ?? []).some((s) =>
        s.groups.some((g) => (g.publisher_lines ?? []).some((l) => l.msg_per_sec !== undefined)),
      ),
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
          the row. “Health” and “Sequence” are per PUBLISHER and the group row carries neither: a badge over a
          feed with one dead path and one live one describes neither of them. Click a Publishers cell to list a
          group's publishers, one line each, and read the verdicts there. A publisher is{' '}
          <span className="text-emerald-500 font-medium">healthy</span> when it clears{' '}
          {formatBps(data?.publisher_floor_bps ?? 0)} and its recorded series is intact,{' '}
          <span className="text-amber-500 font-medium">thin</span> when it is below that floor,{' '}
          <span className="text-red-500 font-medium">silent</span> when its counter read zero, and{' '}
          <span className="text-muted-foreground font-medium">unknown</span> when nothing measured it — which is
          a monitoring gap, never an outage. The group's own cell reports only what no line can: series recorded
          from an address no publisher of the group carries.
          {' '}This page is about the feed and the publishers that fill it: “Recorders” counts the
          DoubleZero boxes receiving the group, which is the apparatus every application-plane
          column here is measured at, and the customer split is left to the group's own page.
          “Msg/s” and “Peer” come from those recorders and are per PATH — a recorded message rate
          per group rather than the per-tunnel counter beside it, and each path's delivery against
          its redundant peer at the recorder that saw both. Anything below 1 in “Peer” is this path
          losing something its peer did not.
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
              publisher that emitted it. Gap counts are books, never gap-marked messages. It is folded from background refreshers, so it is
              minutes older than the rest of the row. On the top-of-book plane the recorder writes no gap
              marker, so those series read “advancing” — the counters move and nothing checked them for
              loss — rather than the market-by-price rows' gap-checked “ok”.
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
              showObservations={showObservations}
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
