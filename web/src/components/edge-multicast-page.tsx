import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useNavigate } from 'react-router-dom'
import { AlertCircle, Loader2, Radio } from 'lucide-react'
import { Tooltip } from '@/components/ui/tooltip'
import { PageHeader } from './page-header'
import { CopyableText } from './copyable-text'
import { handleRowClick } from '@/lib/utils'
import { fetchEdgeMulticast, type EdgeMulticastGroup, type EdgeMulticastService } from '@/lib/api'

// One screen for every multicast group in the Edge product — the groups whose ledger code carries
// the `edge-` prefix — showing who publishes, who receives, and whether anything is flowing. The per-group pages already answer this one group at a time; the
// question that needs a fleet view is "is any lane silent right now".
//
// Everything about freshness on this page is deliberate. Rates come from five-minute counter
// rollups that land several minutes late, so a green dot here means "traffic in the last bucket
// we can see", never "traffic this second". Every rate is rendered next to the age of the bucket
// it came from, and a bucket older than STALE_AFTER_SECS greys the row's numbers out instead of
// showing a confident colour over data nobody should trust.

// Past this age the rate columns stop asserting anything. Two rollup grains plus the observed
// pipeline lag: the normal steady state is 5-10 minutes behind, so a threshold much tighter than
// this would mark healthy lanes stale for most of every cycle.
const STALE_AFTER_SECS = 15 * 60

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

// The plane a lane carries. Monospaced and dimmed rather than a coloured badge: it identifies the
// row, it does not signal anything, and a badge here would compete with the health column.
function PlaneCell({ plane }: { plane?: string }) {
  if (!plane) return <span className="text-muted-foreground">—</span>
  return <span className="font-mono text-xs text-muted-foreground">{plane}</span>
}

const HEALTH_BADGE: Record<string, string> = {
  healthy: 'bg-emerald-500/15 text-emerald-500',
  degraded: 'bg-amber-500/15 text-amber-500',
  unhealthy: 'bg-red-500/15 text-red-500',
  disconnected: 'bg-sky-500/15 text-sky-500',
  unknown: 'bg-muted text-muted-foreground',
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

function HealthBadge({ status }: { status: string }) {
  if (!status) return <span className="text-muted-foreground">—</span>
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium ${HEALTH_BADGE[status] ?? HEALTH_BADGE.unknown}`}>
      {status}
    </span>
  )
}

// PublisherCell is the point of the page. Three states, and the third is not a failure:
//   active  — at least one publisher moved traffic in the last visible bucket
//   silent  — publishers are registered, their counters read zero
//   unknown — nothing measured them, which is a monitoring gap, not an outage
function PublisherCell({ group, stale }: { group: EdgeMulticastGroup; stale: boolean }) {
  const { publishers } = group
  if (publishers.total === 0) {
    return <span className="text-muted-foreground">—</span>
  }

  const dot = group.silent
    ? 'bg-red-500'
    : publishers.active > 0
      ? stale
        ? 'bg-muted-foreground'
        : 'bg-emerald-500'
      : 'bg-muted-foreground'

  const detail = [
    `${publishers.active} publishing`,
    publishers.idle > 0 ? `${publishers.idle} registered but idle` : '',
    publishers.unknown > 0 ? `${publishers.unknown} with no counter data` : '',
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <Tooltip content={detail}>
      <span className="inline-flex items-center gap-1.5 tabular-nums">
        <span className={`inline-block h-2 w-2 rounded-full ${dot}`} />
        <span>
          {publishers.active}
          <span className="text-muted-foreground">/{publishers.total}</span>
        </span>
        {group.silent && <span className="text-[10px] font-medium text-red-500 uppercase">silent</span>}
      </span>
    </Tooltip>
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
// is "no capture covers this group", never "the lane is dead".
function LastHeardCell({ group }: { group: EdgeMulticastGroup }) {
  if (group.last_heard_age_seconds === undefined) {
    return <span className="text-muted-foreground">—</span>
  }
  const lanes = group.last_heard_lanes ?? 1
  const detail = [
    `from ${group.last_heard_source}`,
    lanes > 1
      ? `max over ${lanes} capture lanes — one dead lane does not move this; see the Kalshi L2 page for per-lane detail`
      : '',
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <Tooltip content={detail}>
      <span className="tabular-nums">
        {formatAge(group.last_heard_age_seconds)}
        {lanes > 1 && <span className="text-muted-foreground text-xs"> ×{lanes}</span>}
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

function GroupRow({
  group,
  showLastHeard,
  onOpen,
}: {
  group: EdgeMulticastGroup
  showLastHeard: boolean
  onOpen: (e: React.MouseEvent, pk: string) => void
}) {
  const age = group.observed_age_seconds
  const stale = age === undefined || age > STALE_AFTER_SECS

  return (
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
        <PublisherCell group={group} stale={stale} />
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
          <LastHeardCell group={group} />
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
          <HealthBadge status={group.health} />
        </Link>
      </td>
    </tr>
  )
}

function ServiceSection({
  service,
  showLastHeard,
  onOpen,
}: {
  service: EdgeMulticastService
  showLastHeard: boolean
  onOpen: (e: React.MouseEvent, pk: string) => void
}) {
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
              <GroupRow key={`${service.code}-${g.pk}`} group={g} showLastHeard={showLastHeard} onOpen={onOpen} />
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
            silentCount > 0 ? (
              <span className="text-sm text-red-500">
                {silentCount} silent group{silentCount === 1 ? '' : 's'}
              </span>
            ) : undefined
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
            <ServiceSection key={s.code} service={s} showLastHeard={data?.last_heard_available ?? false} onOpen={onOpen} />
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
