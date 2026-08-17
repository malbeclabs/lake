/* eslint-disable react-refresh/only-export-components */
import { useEffect, useMemo, useState } from 'react'
import { Link, useSearchParams, useNavigate } from 'react-router-dom'
import { useQuery, keepPreviousData, type UseQueryResult } from '@tanstack/react-query'
import { BarChart3, Loader2, AlertCircle, ChevronDown, ArrowLeft, Info } from 'lucide-react'
import {
  BarChart,
  Bar,
  LabelList,
  AreaChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ReferenceLine,
  ReferenceArea,
  Tooltip as ReTooltip,
  ResponsiveContainer,
} from 'recharts'
import {
  fetchNetworkHealthDeferred,
  fetchNHOverview,
  fetchNHAvailability,
  fetchNHLatency,
  fetchNHCapacity,
  fetchNHOutages,
  fetchNHDrain,
  fetchNHTickets,
  fetchNHImpactful,
  type NetworkHealthDeferred,
  type NetworkHealthParams,
  type NetworkHealthDeltas,
  type NetworkHealthReliability,
  type NetworkHealthReliabilityPrev,
  type NetworkHealthDrainTiming,
  type NetworkHealthTrafficLink,
  type NetworkHealthCapacityLink,
  type NetworkHealthDeviceSlots,
  type NetworkHealthDiaInterface,
  type NetworkHealthPerfLink,
  type NetworkHealthErrorHotspot,
  type NetworkHealthTickets,
  type NetworkHealthRootCause,
  type NetworkHealthAvailability,
  type NetworkHealthOutageSummary,
  type NetworkHealthDowntimeRow,
  type NetworkHealthTsPoint,
  type NetworkHealthCountPoint,
  type NetworkHealthWindow,
  type NHOverview,
  type NHLatencyGroup,
  type NHCapacityGroup,
  type NHOutagesGroup,
  type NHImpactful,
} from '@/lib/api'
import { PageHeader } from '@/components/page-header'
import { StatCard } from '@/components/stat-card'
import { deltaColorClass, toneColorClass, formatDelta } from '@/components/stat-card-utils'
import { Tooltip } from '@/components/ui/tooltip'
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from '@/components/ui/table'
import {
  getDays,
  formatDateRange,
  formatISODate,
  parseISODate,
  isSameDay,
  startOfDay,
} from '@/components/maintenance-calendar/date-utils'

// Network Health Reporting. FACTS ONLY (see plans/network-health-dashboard/SPEC.md
// section 1.1): the page never ranks, scores, or grades contributors against each
// other. Per-contributor data is shown in neutral, sortable tables with no best/worst
// labels. Directional (green/red) deltas are direction-aware per metric: green marks
// movement in the good direction for that metric (fewer outages is green, higher
// throughput is green), and a metric with no inherent good direction stays neutral.
// One deliberate exception to facts-only: on a contributor's own view the share of
// incidents they self-reported is graded green/yellow/red. That grades the contributor
// against their own reporting on their own view, not against other contributors (there
// is no cross-contributor leaderboard). Every metric carries a plain-language
// explanation via a hover info icon (DEFS below).

type Sort = { key: string; dir: 'asc' | 'desc' }

// Canonical bar fills reused by every ranked-bar panel (traffic, capacity, etc).
const BAR = { blue: '#3b82f6', amber: '#f59e0b', slate: '#64748b' } as const

// Plain-language explanation for every metric on the page.
const DEFS = {
  peak: 'The highest total user throughput seen in any 5-minute window over the period. This is the top of the peak line on the throughput chart.',
  jitter:
    'How much steadier DoubleZero latency is than the public internet. Jitter is the wobble in latency, not the latency itself, so this is not a claim about speed. For each metro pair we take internet jitter minus DoubleZero jitter, divided by internet jitter, then average across pairs. Higher is better. A route where DoubleZero wobbled more than the internet would show a negative number.',
  loss: 'Share of DoubleZero probe packets marked lost, averaged across measured metro pairs. This is DoubleZero\'s own loss, not a comparison with the internet. Lower is better.',
  outages: 'Link failure episodes (a link with IS-IS down or 10% or more packet loss on 5-minute buckets) in this window. Counts sustained failures lasting at least 10 minutes. Brief flaps under 10 minutes are not counted here; they appear in "Link failures by duration". In this routed mesh a link failure usually means traffic rerouted with added latency, not lost service.',
  impactfulDowntime:
    'Failure hours weighted toward the links that mattered: counted only for links that were carrying traffic (at least 1 Mbps just before they failed) or were the primary lowest-latency path for their metro pair. This traffic almost always rerouted onto another path (added latency) rather than being dropped. Failures on idle, non-primary links are excluded.',
  activeLinks: 'Links in the "activated" state at the end of the window.',
  activeDevices: 'Devices in the "activated" state at the end of the window.',
  activeMetros: 'Metros with at least one active device at the end of the window.',
  drainTiming:
    'How quickly a failed link was taken out of service (drained) and how long it stayed drained. Measured from telemetry and status changes.',
  timeToDrain: 'Minutes from a link failing to it being drained (soft or hard), meaning taken out of service.',
  timeDrained:
    'Minutes a link stayed drained (soft or hard), from the drain until it was returned to service.',
  timeToUndrain:
    'Minutes from when a drained link came back healthy to when it was actually returned to service. Long times mean healthy capacity sat out of service.',
  drainWithin30m: 'Share of link failures where the link was drained within 30 minutes of failing.',
  capacityLinks:
    'Links closest to their provisioned bandwidth: typical (P50) and peak (P99) throughput as a share of link capacity, side by side. P99 (not raw 5-minute max) so a single outlier bucket does not overstate utilization. A capacity-planning signal for which links to upgrade first.',
  durationHist:
    'The distribution of individual failure lengths, from brief flaps to sustained multi-hour events. This includes every episode, even brief flaps under 5 minutes, so it shows more episodes than the headline link-failure count, which counts only sustained failures of 10 minutes or more. Where "Most failure time" totals hours per link or device, this counts failures by how long each one lasted.',
  degraded: 'Links that stayed up but saw more than 1% packet loss at some point in the window.',
  trend: 'Change versus the previous window of the same length (the period immediately before this one).',
  availability:
    'Network-wide availability from traffic-weighted failure hours: 1 minus (traffic-weighted failure hours / (active links x window hours)). Uses the same figure shown in the headline, so it excludes idle, non-primary links and intentional drain (maintenance). The per-link and per-device availability panels further down the page take a stricter cut and count maintenance drain as unavailable.',
  sla: 'Of links that have an onchain committed round-trip time, the share whose average measured RTT stayed within that committed value over the window.',
  perfLinks:
    'Links that carry an onchain committed round-trip time, comparing the committed value to the measured RTT over the window (average and worst 5-minute bucket). A committed value tagged "override" reflects an active IS-IS delay override rather than the raw onchain commitment.',
  linkCommitted:
    'The link\'s onchain committed round-trip time: the latency it promises. When a contributor sets an IS-IS delay override, this shows the override (the value the network actually routes on) and the row is tagged; the raw onchain value is in the tag\'s tooltip.',
  linkMeasured: 'The link\'s measured round-trip time over the window: the average across 5-minute buckets, and the worst single bucket.',
  linkOverCommitted: 'Share of 5-minute buckets where the measured RTT was above the committed value.',
  linkDrift:
    'Measured average RTT minus the committed value, in ms and as a percent of committed. Positive means the link is measuring slower than its onchain commitment.',
  capacity: 'User seats in use versus configured seat capacity across activated devices. Low utilization means headroom to grow.',
  deviceSlots:
    'Devices with the most user slots in use: unicast, multicast-subscriber, and multicast-publisher seats, against the device\'s configured max_users cap.',
  dia: 'Direct Internet Access (DIA) interfaces: typical (P50) and peak (P99) outbound throughput as a share of interface bandwidth (the physical port speed, e.g. a 10G port = 10G of capacity). The committed rate (CIR) is shown alongside for reference but is not the denominator. Port speed comes from a snapshot refreshed roughly once a month, so a recently changed interface may not yet be reflected.',
  isis: 'IS-IS control-plane health: devices asking to be routed around (overload bit) or unreachable, and the count of established adjacencies.',
  freshness: 'How fresh the telemetry is: seconds since the interface-counters feed last advanced, and how many active devices reported within the last hour of that feed.',
  throughputChart: 'User traffic throughput over the window, averaged per bucket, with the peak per bucket. From the 5-minute device interface rollups.',
  outagesChart:
    'Link-down incidents (from the 5-minute link rollups) that started in each time bucket, across the full selected window.',
  topLinks: 'Links carrying the most user traffic in the window (average and peak).',
  hotspots: 'Devices with the most interface errors, discards, and FCS errors, plus carrier transitions. An early warning of degrading optics or fiber before it becomes a failure.',
  opsMgmt:
    'Tickets logged in the Ops Management portal over the window: incidents vs maintenance, severity, how quickly they were filed and resolved, and how many link failures had a matching ticket. Aggregate numbers only, no ticket details. On the whole-network view this reflects the cached 30-day view; on a contributor view it is computed live and filtered to that contributor.',
  opsResponse:
    'Median of (ticket created time minus event start), across incidents that have both timestamps. Incidents only: scheduled maintenance is filed ahead of the event and is excluded so it does not pull this negative. A single high-volume filer can dominate this median; use the contributor filter to narrow it.',
  opsResolution: 'Median minutes from event start to ticket close, across closed incidents.',
  selfReported:
    'Share of incidents in this window that the contributor filed themselves, rather than DoubleZero filing on their behalf. Each incident is credited to whoever created its ticket. Higher means the contributor is surfacing its own issues. Shown as unavailable when the ticket-creator registry could not be read.',
  maintenance:
    'Scheduled maintenance tickets in this window, split into completed and still planned. Timing and response are not tracked for scheduled maintenance because it is filed ahead of the work.',
  rootCauses:
    'Recorded root cause for incident tickets in this window (maintenance excluded), with each cause as a share of incidents that have a cause. Self-resolved incidents cleared on their own with no operator action and are shown separately below the charted causes.',
  opsNoTicket:
    'Link failures detected from telemetry that had no matching ticket in the window, by affected link and overlapping time. These are the clearest candidates to file a ticket for.',
  availabilityLink:
    "Each link's time in the window split three ways, as a percent of the window: available (up), maintenance (intentionally drained, taken out of service e.g. for maintenance), and down (a fault: link down or high loss). Intentional maintenance drain counts as down here, and is shown as its own segment so planned maintenance is not mistaken for a fault. Currently soft- or hard-drained links are included, so a link fully out for maintenance shows near the top as mostly drained. Least available first. For total fault-hours with maintenance excluded, see \"Most failure time\" in Operations; a link parked in maintenance ranks low here but does not appear there.",
  availabilityDevice:
    "Device reachability over the window, as a percent of time. This is the page's true-outage signal: a device is counted unreachable only when it has no working path at all. Available: at least one of its links is working. Unreachable: a fault leaves every activated link down (IS-IS down or 10%+ loss). Maintenance: reachable-blocked only because every link was intentionally drained (soft- or hard-drained) with no fault on any link, so this is planned, not an outage. One link down while others are up does not lower the device. Ranked by unreachable (fault) time, so a device out only for maintenance is not counted as least available.",
  outageSummary:
    'Link and device failures in this window: how many, total failure-hours, and how many distinct links/devices were affected. The count and the failure-hours are measured independently, so a near match between the two numbers is coincidence. Detected from telemetry, independent of whether a ticket was filed.',
  mostDowntime: 'Total hours each link or device was actually down from a fault in this window, biggest total first, summed from failure episodes on the 5-minute link rollups (an episode is IS-IS down or 10% or more loss lasting at least 10 minutes). This counts hours, not a percentage of the window. It differs from "Least available", which is a percent of the window and counts intentional maintenance drain as down; here maintenance is excluded and only real faults count, so the two rank different links and devices. These bars are uncapped totals, so they can exceed the per-failure 24h cap used in the failure summary.',
  devicesAffected:
    'Count of distinct devices that had at least one real fault in this window: a link they terminate was IS-IS down or saw 10% or more packet loss for at least 10 minutes, derived from the 5-minute link rollups. Maintenance drains are not counted, so this can be lower than the total number of active devices. A device counts once no matter how many failures it had.',
}

export function NetworkHealthReportingPage() {
  const [params, setParams] = useSearchParams()
  const contributor = params.get('contributor') ?? ''
  const days = Number(params.get('days')) || 30
  const start = params.get('start') ?? ''
  const end = params.get('end') ?? ''

  const range: NetworkHealthParams = start && end ? { start, end } : { days }

  const setRange = (next: { days?: number; start?: string; end?: string }) =>
    setParams((p) => {
      const n = new URLSearchParams(p)
      n.delete('days')
      n.delete('start')
      n.delete('end')
      if (next.start && next.end) {
        n.set('start', next.start)
        n.set('end', next.end)
      } else if (next.days && next.days !== 30) {
        n.set('days', String(next.days))
      }
      return n
    })

  const control = <NetworkHealthFilterBar days={days} start={start} end={end} onRange={setRange} />

  // A contributor re-scopes the whole one-pager (same NetworkView, filtered).
  return <NetworkView range={range} control={control} onRange={setRange} contributor={contributor} />
}

// Shared scope navigation: switch between the whole network and a contributor
// scope, preserving the window params. Available to any panel without
// prop-drilling.
function useScopeNav() {
  const [, setParams] = useSearchParams()
  const set = (key: 'contributor' | '', value: string) =>
    setParams((p) => {
      const n = new URLSearchParams(p)
      n.delete('contributor')
      if (key) n.set(key, value)
      return n
    })
  return {
    openContributor: (v: string) => set('contributor', v),
    toNetwork: () => set('', ''),
  }
}

// Outbound navigation to the standalone device/link detail pages. These are
// plain react-router Links (NOT the removed in-page scope drill-down): a device
// or link NAME anywhere on the report links out to /dz/devices/:pk or
// /dz/links/:pk. Subtle affordance (hover underline), keeps whatever truncation
// classes the call site passes on the anchor. When pk is missing it degrades to
// a plain span so the name still renders.
type EntityLinkProps = { pk?: string; className?: string; title?: string; children: React.ReactNode }

function EntityLink({ kind, pk, className, title, children }: EntityLinkProps & { kind: 'link' | 'device' }) {
  const cls = className ?? ''
  if (!pk) return <span className={cls} title={title}>{children}</span>
  const base = kind === 'link' ? '/dz/links/' : '/dz/devices/'
  return (
    <Link to={base + pk} title={title} className={`hover:underline hover:text-foreground transition-colors ${cls}`}>
      {children}
    </Link>
  )
}

function LinkLink(p: EntityLinkProps) {
  return <EntityLink kind="link" {...p} />
}

function DeviceLink(p: EntityLinkProps) {
  return <EntityLink kind="device" {...p} />
}

// --- Network scope ---

// One react-query hook per data-source group. Every group is keyed identically
// ([nh, group, range, contributor]) so the window preset/custom range and the
// contributor scope re-key ALL groups at once, each re-fetching independently.
// keepPreviousData holds the last window's values (dimmed) during a range change
// instead of flashing every panel back to a skeleton.
function useGroup<T>(
  group: string,
  fetcher: (p: NetworkHealthParams, c: string) => Promise<T>,
  range: NetworkHealthParams,
  contributor: string,
): UseQueryResult<T> {
  return useQuery({
    queryKey: ['nh', group, range, contributor],
    queryFn: () => fetcher(range, contributor),
    staleTime: 60_000,
    placeholderData: keepPreviousData,
  })
}

// --- Network scope ---

function NetworkView({
  range,
  control,
  onRange,
  contributor,
}: {
  range: NetworkHealthParams
  control: React.ReactNode
  onRange: (next: { days?: number; start?: string; end?: string }) => void
  contributor?: string
}) {
  const { toNetwork } = useScopeNav()
  const scoped = !!contributor
  const c = contributor ?? ''

  // Each data-source group fetches + caches on its own, so the page shell renders
  // immediately and every panel shows its own loading state until its group
  // resolves. A slow or failed group only affects its own panels.
  const overviewQ = useGroup('overview', fetchNHOverview, range, c)
  const availQ = useGroup('availability', fetchNHAvailability, range, c)
  const latencyQ = useGroup('latency', fetchNHLatency, range, c)
  const capacityQ = useGroup('capacity', fetchNHCapacity, range, c)
  const outagesQ = useGroup('outages', fetchNHOutages, range, c)
  const drainQ = useGroup('drain', fetchNHDrain, range, c)
  const ticketsQ = useGroup('tickets', fetchNHTickets, range, c)
  const impactfulQ = useGroup('impactful', fetchNHImpactful, range, c)

  // Undrain timing loads on its own slow endpoint (recovery-health query); the
  // Drain panel renders its fast fields immediately and fills the undrain
  // MiniStats in when this resolves.
  const deferredQ = useGroup('deferred', fetchNetworkHealthDeferred, range, c)
  const deferred = deferredQ.data
  const deferredPending = deferredQ.isPending

  return (
    <Scroll>
      {scoped && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground mb-4">
          <button onClick={toNetwork} className="inline-flex items-center gap-1 hover:text-foreground">
            <ArrowLeft className="h-4 w-4" /> Whole network
          </button>
          <span className="text-muted-foreground/50">/</span>
          <span className="text-foreground font-medium">Contributor: {contributor}</span>
        </div>
      )}
      <PageHeader
        icon={BarChart3}
        title={scoped ? (contributor as string) : 'Network Health Reporting'}
        subtitle={
          <span className="text-muted-foreground text-sm">
            {scoped ? 'Contributor view · ' : ''}
            {overviewQ.data?.window.label ?? ''}
          </span>
        }
        actions={control}
      />

      {/* TOP OVERVIEW: headline tiles cross-read three groups (overview, outages,
          impactful) and each tile shows its own loading. */}
      <HeadlineStrip overview={overviewQ} outages={outagesQ} impactful={impactfulQ} />
      <KeyFacts overview={overviewQ} impactful={impactfulQ} capacity={capacityQ} latency={latencyQ} scoped={scoped} />
      <GroupBoundary query={overviewQ} title="Throughput over time" info={DEFS.throughputChart} height={300} sources={['throughput_ts']}>
        {(o) => <TrendsSection points={o.throughput_ts} onRange={onRange} />}
      </GroupBoundary>

      <SectionBand
        title="Performance"
        subtitle="The latency the network committed to versus what it delivered, and which links and devices were least available."
      />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <GroupBoundary query={availQ} title="Least available links" info={DEFS.availabilityLink} sources={['link_availability']}>
          {(g) => <LinkAvailabilityPanel rows={g.link_availability ?? []} />}
        </GroupBoundary>
        <GroupBoundary query={availQ} title="Least available devices" info={DEFS.availabilityDevice} sources={['device_availability']}>
          {(g) => <DeviceAvailabilityPanel rows={g.device_availability ?? []} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        <GroupBoundary query={latencyQ} title="Committed vs measured latency" info={DEFS.perfLinks} height={200} sources={['latency_links']}>
          {(g) => <PerfLinksPanel rows={g.latency_links ?? []} />}
        </GroupBoundary>
      </div>

      <SectionBand
        title="Capacity"
        subtitle="Which links and devices are filling up, and where DIA uplinks stand against their committed rate."
      />
      <div className="mb-6">
        <GroupBoundary query={capacityQ} title="Links carrying the most traffic" info={DEFS.topLinks} sources={['top_links']}>
          {(g) => <TopLinksPanel rows={g.top_links ?? []} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        <GroupBoundary query={capacityQ} title="Fullest devices (user slots)" info={DEFS.deviceSlots} sources={['device_slots']}>
          {(g) => <DeviceSlotsPanel rows={g.device_slots ?? []} />}
        </GroupBoundary>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <GroupBoundary query={capacityQ} title="Fullest links (capacity planning)" info={DEFS.capacityLinks} sources={['fullest_links']}>
          {(g) => <CapacityPanel rows={g.capacity_links ?? []} />}
        </GroupBoundary>
        <GroupBoundary query={capacityQ} title="DIA interfaces (capacity vs bandwidth)" info={DEFS.dia} sources={['dia_interfaces']}>
          {(g) => <DiaPanel rows={g.dia_interfaces ?? []} />}
        </GroupBoundary>
      </div>

      <SectionBand
        title="Operations"
        subtitle="DoubleZero is a routed mesh. When a link fails, traffic almost always reroutes over another path, so the usual effect is added latency, not lost service. A genuine outage (traffic with nowhere to go) happens only when a device loses every path (see the device panel below). Read the counts below as link and device failures to fix, not as time the network was down."
      />
      {/* MIXED groups under one band: outages, tickets and drain interleave, so
          each panel gates on its own group (never the whole band). */}
      <div className="mb-6">
        <GroupBoundary query={outagesQ} title="Link failures over time" info={DEFS.outagesChart} sources={['outages_ts']}>
          {(g) => <OutagesOverTimePanel rows={g.outages_ts ?? []} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        <GroupBoundary query={outagesQ} title="Failure summary" info={DEFS.outageSummary} sources={['outage_summary']}>
          {(g) => <OutageSummaryStrip summary={g.outage_summary} prev={g.prev?.outage_summary} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        {/* The incidents figures include outage-to-ticket coverage, so they also
            rest on the outage list the tickets group scans. */}
        <GroupBoundary query={ticketsQ} title="Incidents" info={DEFS.opsMgmt} sources={['ops_tickets', 'outage_list']}>
          {(g) => <IncidentsStrip tickets={g.ops_tickets} prev={g.prev} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        <GroupBoundary query={ticketsQ} title="Maintenance" info={DEFS.maintenance} sources={['ops_tickets']}>
          {(g) => <MaintenanceStrip tickets={g.ops_tickets} prev={g.prev} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        <GroupBoundary query={outagesQ} title="Most failure time" info={DEFS.mostDowntime} sources={['downtime_links', 'downtime_devices']}>
          {(g) => <MostDowntimePanel links={g.downtime_links ?? []} devices={g.downtime_devices ?? []} />}
        </GroupBoundary>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <GroupBoundary query={drainQ} title="Drain & undrain timing" info={DEFS.drainTiming} sources={['contrib_link_pks', 'link_down_events', 'status_changes']}>
          {(g) => (
            <DrainTimingPanel dt={g.drain_timing} prev={g.prev} deferred={deferred} deferredLoading={deferredPending} />
          )}
        </GroupBoundary>
        <GroupBoundary query={outagesQ} title="Link failures by duration" info={DEFS.durationHist} sources={['reliability', 'degraded_links']}>
          {(g) => <ReliabilityPanel rel={g.reliability} prev={g.prev?.reliability} />}
        </GroupBoundary>
      </div>
      <div className="mb-6">
        <GroupBoundary query={outagesQ} title="Interface errors & carrier flaps" info={DEFS.hotspots} sources={['error_hotspots']}>
          {(g) => <HotspotsPanel rows={g.error_hotspots ?? []} />}
        </GroupBoundary>
      </div>

      {overviewQ.data && <Footer window={overviewQ.data.window} generatedAt={overviewQ.data.generated_at} />}
    </Scroll>
  )
}

// --- Progressive-loading primitives ---
//
// A group payload lists the panel queries that failed in `degraded` (the backend
// panel names, see nhPanels in api/handlers/network_health.go). The fields those
// panels fill hold a zero value, so any panel reading one of them must render as
// unavailable rather than draw that zero as a measurement. `sources` names the
// panels one rendered panel reads, so a failed query degrades only the panels
// that depend on it.
export function panelDegraded(degraded: string[] | undefined, sources: string[] | undefined): boolean {
  if (!degraded?.length || !sources?.length) return false
  return sources.some((s) => degraded.includes(s))
}

// GroupBoundary renders loading/error/data for one group query. Loading shows a
// muted skeleton inside the same bordered section the real panel uses (so layout
// doesn't jump); a resolved-but-errored payload (data.error) reads as
// unavailable, matching the old data.error check, as does a payload whose
// `degraded` names one of this panel's sources. Only one branch renders at a
// time, so a panel's own SectionTitle never double-renders with the shell's.
export function GroupBoundary<T extends { error?: string; degraded?: string[] }>({
  query,
  title,
  info,
  height = 120,
  sources,
  children,
}: {
  query: UseQueryResult<T>
  title?: string
  info?: string
  height?: number
  // Backend panel names this panel's data comes from. Several call sites share
  // one group query, so the check is per panel, not per query.
  sources?: string[]
  children: (data: T) => React.ReactNode
}) {
  if (query.isPending) return <PanelLoading title={title} info={info} height={height} />
  if (query.error || !query.data || query.data.error)
    return <PanelUnavailable title={title} info={info} message={query.data?.error} />
  if (panelDegraded(query.data.degraded, sources)) return <PanelUnavailable title={title} info={info} />
  // On a window change keepPreviousData keeps the prior data visible; dim it
  // while the new window is in flight.
  //
  // h-full + [&>section]:h-full: this wrapper is the grid item in the paired
  // two-column rows, so it stretches to the row height; forcing the panel's own
  // <section> to fill it keeps side-by-side cards equal height (in single-column
  // rows the parent has no fixed height, so both are a no-op).
  return (
    <div className={`h-full [&>section]:h-full ${query.isPlaceholderData ? 'opacity-60 transition-opacity' : ''}`}>
      {children(query.data)}
    </div>
  )
}

function PanelShell({
  title,
  info,
  children,
}: {
  title?: string
  info?: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-lg border border-border p-4">
      {title && <SectionTitle title={title} info={info} />}
      {children}
    </section>
  )
}

function PanelLoading({
  title,
  info,
  height = 120,
}: {
  title?: string
  info?: string
  height?: number
}) {
  return (
    <PanelShell title={title} info={info}>
      <div className="animate-pulse space-y-2" style={{ minHeight: height }} aria-busy="true">
        <div className="h-3 w-1/3 rounded bg-muted" />
        <div className="h-3 w-2/3 rounded bg-muted" />
        <div className="h-3 w-1/2 rounded bg-muted" />
      </div>
      <div className="mt-2 inline-flex items-center gap-2 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" /> loading...
      </div>
    </PanelShell>
  )
}

function PanelUnavailable({
  title,
  info,
  message,
}: {
  title?: string
  info?: string
  message?: string
}) {
  return (
    <PanelShell title={title} info={info}>
      <div className="flex items-center gap-2 text-sm text-muted-foreground py-6">
        <AlertCircle className="h-4 w-4 text-amber-500" />
        <span>Couldn't load this section.{message ? ` ${message}` : ''}</span>
      </div>
    </PanelShell>
  )
}

// A muted headline tile shown when a tile's own group fails to load. Mirrors the
// StatCard container so the headline row keeps its shape.
function UnavailableTile({ label, info }: { label: string; info?: string }) {
  return (
    <div className="text-center rounded-[0.3rem] bg-muted/50 p-2 lg:p-4">
      <div className="text-2xl lg:text-3xl font-medium tabular-nums tracking-tight mb-1 text-muted-foreground">—</div>
      <div className="text-sm text-muted-foreground inline-flex items-center gap-1">
        {label}
        {info && <InfoTip text={info} />}
      </div>
    </div>
  )
}

// One headline tile bound to its own group query. StatCard's built-in
// value===undefined skeleton covers the pending state; a failed group shows a
// muted "—" tile instead of spinning forever.
function HeadlineStat<T extends { error?: string; degraded?: string[] }>({
  q,
  pick,
  delta,
  label,
  info,
  format,
  decimals,
  goodDirection,
  sources,
  unavailable,
}: {
  q: UseQueryResult<T>
  pick: (d: T) => number | undefined
  delta?: (d: T) => number | undefined
  label: string
  info?: string
  format: 'number' | 'bandwidth'
  decimals?: number
  goodDirection?: 'up' | 'down' | 'neutral'
  // Backend panel names this tile's figure comes from, so a failed panel that
  // did not set the group error shows the muted tile instead of its zero.
  sources?: string[]
  // Some groups return a successful payload that is still unusable (e.g. a
  // scoped impactful query that could not compute), flagged by their own field.
  // Render the muted tile for those too, so a failed compute does not show 0.
  unavailable?: (d: T) => boolean | undefined
}) {
  if (q.error || q.data?.error) return <UnavailableTile label={label} info={info} />
  if (panelDegraded(q.data?.degraded, sources)) return <UnavailableTile label={label} info={info} />
  if (q.data && unavailable?.(q.data)) return <UnavailableTile label={label} info={info} />
  const value = q.data ? pick(q.data) : undefined
  const d = q.data && delta ? delta(q.data) : undefined
  return (
    <StatCard
      label={label}
      value={value}
      format={format}
      decimals={decimals}
      delta={d}
      goodDirection={goodDirection}
      info={info}
    />
  )
}

// --- Shared panels ---

function HeadlineStrip({
  overview,
  outages,
  impactful,
}: {
  overview: UseQueryResult<NHOverview>
  outages: UseQueryResult<NHOutagesGroup>
  impactful: UseQueryResult<NHImpactful>
}) {
  const days = overview.data?.window.days
  return (
    <>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-2 lg:grid-cols-4">
        <HeadlineStat
          q={overview}
          label="Peak throughput"
          format="bandwidth"
          decimals={1}
          goodDirection="up"
          info={DEFS.peak}
          sources={['throughput_ts']}
          pick={(o) => o.headline.peak_bps}
          delta={(o) => deltaOrUndef(o.headline.deltas, 'peak_bps')}
        />
        <HeadlineStat
          q={outages}
          label="Link failures"
          format="number"
          goodDirection="down"
          info={DEFS.outages}
          sources={['reliability']}
          pick={(g) => g.outage_count}
          delta={(g) => g.outage_count_delta ?? undefined}
        />
        <HeadlineStat
          q={impactful}
          label="Traffic-weighted failure hours"
          format="number"
          decimals={1}
          goodDirection="down"
          info={DEFS.impactfulDowntime}
          sources={['impactful']}
          pick={(g) => g.impactful_downtime_hours}
          delta={(g) => g.impactful_downtime_delta ?? undefined}
          unavailable={(g) => g.unavailable}
        />
        <HeadlineStat
          q={overview}
          label="Active links"
          format="number"
          goodDirection="neutral"
          info={DEFS.activeLinks}
          sources={['active_links']}
          pick={(o) => o.headline.active_links}
          delta={(o) => deltaOrUndef(o.headline.deltas, 'active_links')}
        />
      </div>
      {days !== undefined && (
        <div className="text-xs text-muted-foreground mb-3 inline-flex items-center gap-1">
          Coloured +/- values are the change versus the previous {days} days (green = better, red = worse) <InfoTip text={DEFS.trend} />
        </div>
      )}
    </>
  )
}

// Both payloads carry the window the server resolved. On a range change
// keepPreviousData can hand back the previous window's impactful figure while
// overview already reports the new one, which would divide the old failure hours
// by the new window length. Compares start and end, not days: two different
// custom ranges can share a day count.
export function sameNHWindow(
  a: NetworkHealthWindow | undefined,
  b: NetworkHealthWindow | undefined,
): boolean {
  return !!a && !!b && a.start === b.start && a.end === b.end
}

// Traffic-weighted availability, derived on the frontend from the Overview active
// link count and the traffic-weighted failure hours (same formula the server used:
// 1 - impactful_hours / (active_links x window_hours)). Null unless both payloads
// describe the same window, so the two sides of the ratio always agree.
export function deriveAvailability(o: NHOverview | undefined, imp: NHImpactful | undefined): number | null {
  if (!o || !imp || imp.unavailable) return null
  if (!sameNHWindow(o.window, imp.window)) return null
  const windowHours = o.window.days * 24
  const activeLinks = o.headline.active_links
  if (activeLinks > 0 && windowHours > 0) {
    let avail = 100 * (1 - imp.impactful_downtime_hours / (activeLinks * windowHours))
    if (avail < 0) avail = 0
    return Math.round(avail * 1000) / 1000
  }
  return null
}

// Label for the traffic-weighted availability stat. A window mismatch reads as
// loading only while it can still resolve on its own: a fetch is in flight, or
// keepPreviousData is holding the previous window's payload (stale). A mismatch
// between two settled payloads is not transient: a repeatedly failing impactful
// refresh makes the worker keep its last good blob, whose window then stays
// behind the overview's for as long as the failure lasts, so that case reads as
// the dash. The dash also covers the cases with genuinely no figure to derive:
// no active links, or an impactful payload that could not compute.
export function availabilityText(
  o: NHOverview | undefined,
  imp: NHImpactful | undefined,
  stale: boolean,
  fetching = false,
): string {
  const a = deriveAvailability(o, imp)
  if (a !== null) return `${a}%`
  if (fetching && o && imp && !imp.unavailable && !sameNHWindow(o.window, imp.window)) return 'loading...'
  return stale ? 'loading...' : '—'
}

// A single inline fact that renders its own group's loading/unavailable state.
// sources names the backend panels the fact is read from, so a panel that failed
// without setting the group error reads as unavailable rather than as its zero.
function statText<T extends { error?: string; degraded?: string[] }>(
  q: UseQueryResult<T>,
  fmt: (d: T) => string,
  sources?: string[],
): string {
  if (q.isPending) return 'loading...'
  if (q.error || !q.data || q.data.error) return 'unavailable'
  if (panelDegraded(q.data.degraded, sources)) return 'unavailable'
  return fmt(q.data)
}

function KeyFacts({
  overview,
  impactful,
  capacity,
  latency,
  scoped,
}: {
  overview: UseQueryResult<NHOverview>
  impactful: UseQueryResult<NHImpactful>
  capacity: UseQueryResult<NHCapacityGroup>
  latency: UseQueryResult<NHLatencyGroup>
  scoped: boolean
}) {
  const o = overview.data
  // Traffic-weighted availability cross-reads two groups (overview + impactful);
  // show loading until both resolve and unavailable if either failed. It divides
  // by the active link count, so a failed active_links panel makes it unavailable
  // too.
  const availValue =
    overview.isPending || impactful.isPending
      ? 'loading...'
      : overview.error || impactful.error || o?.error || impactful.data?.error || panelDegraded(o?.degraded, ['active_links'])
        ? 'unavailable'
        : availabilityText(
            o,
            impactful.data,
            overview.isPlaceholderData || impactful.isPlaceholderData,
            overview.isFetching || impactful.isFetching,
          )

  // The IS-IS and freshness labels read the same figures their values do, so a
  // failed panel drops them to the bare label rather than "all healthy" / "0/0".
  const isisOk = !panelDegraded(o?.degraded, ['isis'])
  const freshOk = !panelDegraded(o?.degraded, ['freshness'])

  const freshInfo =
    o && freshOk
      ? `${DEFS.freshness} The feed last advanced at ${o.freshness.feed_max} UTC; on the cached view the "behind" minutes are frozen at cache time, so compute the live lag from that timestamp.`
      : DEFS.freshness

  return (
    <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-muted-foreground mb-8">
      <Stat label="traffic-weighted availability" value={availValue} info={DEFS.availability} />
      <Stat
        label="of seat capacity used"
        value={statText(capacity, (g) => (g.capacity.util_pct === null ? '—' : `${g.capacity.util_pct}%`), ['seat_capacity'])}
        info={DEFS.capacity}
      />
      <Stat
        label="links within committed RTT"
        value={statText(latency, (g) => (g.sla.total === 0 ? '—' : `${g.sla.within} of ${g.sla.total}`), ['sla'])}
        info={DEFS.sla}
      />
      <Stat label="active devices" value={statText(overview, (g) => g.headline.active_devices.toLocaleString(), ['active_devices'])} info={DEFS.activeDevices} />
      {!scoped && (
        <>
          <Stat label="active metros" value={statText(overview, (g) => g.headline.active_metros.toLocaleString(), ['metros'])} info={DEFS.activeMetros} />
          <Stat label="less latency jitter than internet" value={statText(overview, (g) => `${g.headline.jitter_improve_pct}%`, ['latency_vs_internet'])} info={DEFS.jitter} />
          <Stat label="avg packet loss on DoubleZero" value={statText(overview, (g) => `${g.headline.dz_loss_pct}%`, ['latency_vs_internet'])} info={DEFS.loss} />
          <Stat
            label={
              o && isisOk
                ? `IS-IS devices ${o.isis.overloaded === 0 && o.isis.unreachable === 0 ? 'all healthy' : 'flagged'}`
                : 'IS-IS devices'
            }
            value={statText(overview, (g) => `${g.isis.overloaded} overloaded, ${g.isis.unreachable} unreachable of ${g.isis.devices}`, ['isis'])}
            info={DEFS.isis}
          />
          <Stat
            label={o && freshOk ? `behind · ${o.freshness.devices_fresh}/${o.freshness.devices_active} devices reporting` : 'telemetry freshness'}
            value={statText(overview, (g) => `telemetry ${Math.round(g.freshness.lag_seconds / 60)}m`, ['freshness'])}
            info={freshInfo}
          />
        </>
      )}
    </div>
  )
}

// OUTAGES: the plain outage facts detected from telemetry (counts, hours,
// breadth), independent of any ticket. Ticket data lives in the separate
// INCIDENTS and MAINTENANCE sections below, so no metric is shown twice. Same
// visual language as HeadlineStrip/KeyFacts (StatCard grid).
export function OutageSummaryStrip({
  summary,
  prev,
}: {
  summary: NetworkHealthOutageSummary | null | undefined
  prev?: NetworkHealthOutageSummary | null
}) {
  // A null summary means the server could not compute this panel: a successful
  // query always returns a struct, zeroed for a quiet window. Render the same
  // unavailable frame GroupBoundary uses, so the panel does not vanish (this
  // strip is a GroupBoundary child, and returning null drops its whole section).
  if (!summary) return <PanelUnavailable title="Failure summary" info={DEFS.outageSummary} />
  return (
    <section className="rounded-lg border border-border p-4 mb-6">
      <SectionTitle title="Failure summary" info={DEFS.outageSummary} />
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3 mt-3">
        <StatCard label="Link failures" value={summary.link_outages} format="number" delta={pctChange(summary.link_outages, prev?.link_outages)} goodDirection="down" />
        <StatCard label="Failure hours" value={summary.outage_hours} format="number" decimals={0} delta={pctChange(summary.outage_hours, prev?.outage_hours)} goodDirection="down" />
        <StatCard label="Links affected" value={summary.links_affected} format="number" delta={pctChange(summary.links_affected, prev?.links_affected)} goodDirection="down" />
        <StatCard label="Devices affected" value={summary.devices_affected} format="number" delta={pctChange(summary.devices_affected, prev?.devices_affected)} goodDirection="down" info={DEFS.devicesAffected} />
      </div>
    </section>
  )
}

// Grade for the self-reported share: green when the contributor files most of
// its own incidents, amber in the middle, red when it rarely does. A self-fact
// on the entity's own view, not a cross-contributor ranking.
function selfReportedTone(pct: number): 'good' | 'warn' | 'bad' {
  if (pct >= 90) return 'good'
  if (pct >= 70) return 'warn'
  return 'bad'
}

// INCIDENTS: incident tickets only. Leads with the graded self-reported share
// (the one deliberate self-fact grade on the page, see the file header), then
// incident count + severity, timing (filed/resolved), outage-to-ticket
// coverage, the root-cause breakdown, and the contributor-actionable list of
// outages that still have no ticket.
function IncidentsStrip({ tickets, prev }: { tickets: NetworkHealthTickets | null | undefined; prev?: NetworkHealthTickets | null }) {
  if (!tickets) return null
  const sr = tickets.self_reported_pct
  return (
    <section className="rounded-lg border border-border p-4 mb-6">
      <SectionTitle title="Incidents" info={DEFS.opsMgmt} />
      <div className="flex flex-wrap items-center gap-x-6 gap-y-1 text-xs text-muted-foreground mt-1 mb-3">
        <span className="inline-flex items-center gap-1">
          <span className="font-medium tabular-nums text-foreground">{tickets.incidents}</span> incidents
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="font-medium tabular-nums text-amber-600 dark:text-amber-400">{tickets.sev1}</span> sev1
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="font-medium tabular-nums text-amber-600 dark:text-amber-400">{tickets.sev2}</span> sev2
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="font-medium tabular-nums text-foreground">{tickets.sev3}</span> sev3
        </span>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
        {sr === null ? (
          <MiniStat label="Self-reported by contributor" value="unavailable" info={DEFS.selfReported} />
        ) : (
          <MiniStat
            label="Self-reported by contributor"
            value={`${sr.toFixed(0)}%`}
            tone={selfReportedTone(sr)}
            delta={ppDelta(sr, prev?.self_reported_pct)}
            goodDirection="up"
            deltaUnit="pp"
            info={DEFS.selfReported}
          />
        )}
        <MiniStat label="Median time to file" value={timeToFileLabel(tickets.response_p50_min)} delta={pctChange(tickets.response_p50_min, prev?.response_p50_min)} goodDirection="down" info={DEFS.opsResponse} />
        <MiniStat label="Median time to resolve" value={minutes(tickets.resolution_p50_min)} delta={pctChange(tickets.resolution_p50_min, prev?.resolution_p50_min)} goodDirection="down" info={DEFS.opsResolution} />
        <MiniStat
          label="Failures with a ticket"
          value={`${tickets.outages_with_ticket} / ${tickets.outage_count}`}
          delta={pctChange(tickets.outages_with_ticket, prev?.outages_with_ticket)}
          goodDirection="up"
          info={DEFS.opsMgmt}
        />
        <MiniStat
          label="Failures without a ticket"
          value={`${tickets.outages_no_ticket}${tickets.no_ticket_share_pct === null ? '' : ` (${tickets.no_ticket_share_pct}%)`}`}
          delta={pctChange(tickets.outages_no_ticket, prev?.outages_no_ticket)}
          goodDirection="down"
          info={DEFS.opsNoTicket}
        />
      </div>
      <RootCauseBreakdown causes={tickets.root_causes ?? []} />
      <NoTicketOutages tickets={tickets} />
    </section>
  )
}

// Contributor-actionable call-out (plan B5): the outages that still have no
// matching ticket are the clearest "file these" list. Leads with the count and
// share, then lists each affected link with its start time and duration. Each
// link_code links out to its detail page when the pk resolved; otherwise it is
// plain text.
function NoTicketOutages({ tickets }: { tickets: NetworkHealthTickets }) {
  if (tickets.outages_no_ticket <= 0) return null
  const share = tickets.no_ticket_share_pct === null ? '' : ` (${tickets.no_ticket_share_pct}% of failures)`
  const list = tickets.no_ticket_outages ?? []
  return (
    <div className="mt-4 border-t border-border pt-4">
      <div className="text-xs text-muted-foreground/70 mb-1 inline-flex items-center gap-1">
        Failures without a ticket <InfoTip text={DEFS.opsNoTicket} />
      </div>
      <p className="text-xs text-muted-foreground mb-2">
        <span className="font-medium tabular-nums text-foreground">{tickets.outages_no_ticket}</span> link failure
        {tickets.outages_no_ticket === 1 ? '' : 's'} detected from telemetry have no matching ticket{share}. File a
        ticket for each so it is recorded.
      </p>
      {list.length > 0 && (
        <ul className="space-y-1">
          {list.map((o, i) => (
            <li
              key={`${o.link_code}-${o.start_ts}-${i}`}
              className="grid grid-cols-[1fr_auto] items-baseline gap-3 text-xs"
            >
              <LinkLink pk={o.link_pk} className="block max-w-[280px] truncate font-medium" title={o.link_code}>
                {o.link_code}
              </LinkLink>
              <span className="text-right tabular-nums text-muted-foreground whitespace-nowrap">
                {fmtT(o.start_ts)} · {o.hours.toLocaleString(undefined, { maximumFractionDigits: 1 })} h
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

// MAINTENANCE: scheduled maintenance tickets, counts only. Timing and response
// are intentionally not shown (maintenance is filed ahead of the work, so
// time-to-file is not meaningful); see DEFS.maintenance.
function MaintenanceStrip({ tickets, prev }: { tickets: NetworkHealthTickets | null | undefined; prev?: NetworkHealthTickets | null }) {
  if (!tickets || tickets.maintenance === 0) return null
  const planned = Math.max(0, tickets.maintenance - tickets.closed_maintenance)
  const plannedPrev = prev ? Math.max(0, prev.maintenance - prev.closed_maintenance) : undefined
  return (
    <section className="rounded-lg border border-border p-4 mb-6">
      <SectionTitle title="Maintenance" info={DEFS.maintenance} />
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mt-3">
        <StatCard label="Scheduled maintenance" value={tickets.maintenance} format="number" info={DEFS.maintenance} />
        <StatCard label="Completed" value={tickets.closed_maintenance} format="number" delta={pctChange(tickets.closed_maintenance, prev?.closed_maintenance)} goodDirection="neutral" />
        <StatCard label="Planned" value={planned} format="number" delta={pctChange(planned, plannedPrev)} goodDirection="neutral" />
      </div>
      <p className="text-xs text-muted-foreground mt-3">
        Timing and response are not tracked for scheduled maintenance because it is filed ahead of the work.
      </p>
    </section>
  )
}

// Display labels for the root-cause enum. Covers every token the backend
// publishes (nhRootCauseTokens in api/handlers/network_health.go) plus the
// "other" catch-all it maps unrecognised upstream values to. Passthrough for any
// future value.
const ROOT_CAUSE_LABELS: Record<string, string> = {
  self_resolved: 'Self-resolved',
  network_external: 'External network',
  fiber_cut: 'Fiber cut',
  configuration: 'Configuration',
  hardware: 'Hardware',
  carrier: 'Carrier',
  false_positive: 'False positive',
  duplicate: 'Duplicate',
  software: 'Software',
  dz_managed: 'DoubleZero managed',
  human_error: 'Human error',
  other: 'Other',
}

export function rootCauseLabel(cause: string): string {
  return ROOT_CAUSE_LABELS[cause] ?? cause
}

// Incident root-cause breakdown. Real causes render as labeled horizontal bars
// scaled to the largest count; self-resolved incidents (cleared on their own,
// no operator action) are shown as a separate muted line so they do not read
// as a fault category.
export function RootCauseBreakdown({ causes }: { causes: NetworkHealthRootCause[] }) {
  if (causes.length === 0) return null
  const real = causes.filter((c) => c.cause !== 'self_resolved')
  const selfResolved = causes.find((c) => c.cause === 'self_resolved')
  const max = Math.max(1, ...real.map((c) => c.count))
  return (
    <div className="mt-4 border-t border-border pt-4">
      <div className="text-xs text-muted-foreground/70 mb-2 inline-flex items-center gap-1">
        Incident root causes <InfoTip text={DEFS.rootCauses} />
      </div>
      {real.length === 0 ? (
        <p className="text-xs text-muted-foreground">No root causes recorded.</p>
      ) : (
        <div className="space-y-1.5">
          {real.map((c) => (
            <div key={c.cause} className="grid grid-cols-[130px_1fr_72px] items-center gap-3 text-xs">
              <span className="truncate text-muted-foreground" title={rootCauseLabel(c.cause)}>
                {rootCauseLabel(c.cause)}
              </span>
              <div className="h-3 rounded-sm bg-muted/30 overflow-hidden">
                <div className="h-full" style={{ width: `${(100 * c.count) / max}%`, background: BAR.amber }} />
              </div>
              <span className="text-right tabular-nums text-muted-foreground whitespace-nowrap">
                {c.count}{c.pct === null ? '' : ` · ${c.pct}%`}
              </span>
            </div>
          ))}
        </div>
      )}
      {selfResolved && (
        <div className="mt-2 text-xs text-muted-foreground/60 inline-flex items-center gap-1">
          <span className="font-medium tabular-nums">{selfResolved.count}</span>
          {rootCauseLabel(selfResolved.cause)} (cleared with no operator action)
        </div>
      )}
    </div>
  )
}

export function DrainTimingPanel({
  dt,
  prev,
  deferred,
  deferredLoading,
}: {
  dt: NetworkHealthDrainTiming | undefined
  prev?: NetworkHealthDrainTiming | null
  deferred?: NetworkHealthDeferred
  deferredLoading?: boolean
}) {
  if (!dt) return null
  // Undrain timing arrives from a separate, slower query. While it loads the two
  // undrain MiniStats show a muted "loading..." and the summary line omits the
  // matched-undrain clause.
  const undrainPending = deferredLoading || !deferred
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Drain & undrain timing" info={DEFS.drainTiming} />
      <p className="text-xs text-muted-foreground mb-4">
        {dt.drains} drains, {dt.undrains} undrains; {dt.events_with_drain} of {dt.outage_count} link failures
        had a matching drain
        {undrainPending
          ? '.'
          : deferred!.undrain_unavailable
            ? '. Undrain timing is unavailable this window.'
            : dt.undrains > 0
              ? `; ${deferred!.matched_undrains} of ${dt.undrains} undrains matched a recovered link.`
              : '.'}
      </p>
      <div className="grid grid-cols-2 gap-3">
        <MiniStat label="Median time to drain" value={minutes(dt.time_to_drain_p50_min)} delta={pctChange(dt.time_to_drain_p50_min, prev?.time_to_drain_p50_min)} goodDirection="down" info={DEFS.timeToDrain} />
        <MiniStat label="Max time to drain" value={minutes(dt.time_to_drain_max_min)} delta={pctChange(dt.time_to_drain_max_min, prev?.time_to_drain_max_min)} goodDirection="down" info={DEFS.timeToDrain} />
        <MiniStat label="Median time drained" value={minutes(dt.time_drained_p50_min)} delta={pctChange(dt.time_drained_p50_min, prev?.time_drained_p50_min)} goodDirection="down" info={DEFS.timeDrained} />
        <MiniStat label="Max time drained" value={minutes(dt.time_drained_max_min)} delta={pctChange(dt.time_drained_max_min, prev?.time_drained_max_min)} goodDirection="down" info={DEFS.timeDrained} />
        <MiniStat
          label="Median time to undrain"
          value={undrainPending ? 'loading...' : deferredUndrainValue(dt.undrains, deferred!, deferred!.time_to_undrain_p50_min)}
          muted={undrainPending}
          delta={undrainPending ? undefined : pctChange(deferred!.time_to_undrain_p50_min, deferred!.prev?.time_to_undrain_p50_min)}
          goodDirection="down"
          info={DEFS.timeToUndrain}
        />
        <MiniStat
          label="Max time to undrain"
          value={undrainPending ? 'loading...' : deferredUndrainValue(dt.undrains, deferred!, deferred!.time_to_undrain_max_min)}
          muted={undrainPending}
          delta={undrainPending ? undefined : pctChange(deferred!.time_to_undrain_max_min, deferred!.prev?.time_to_undrain_max_min)}
          goodDirection="down"
          info={DEFS.timeToUndrain}
        />
        <MiniStat
          label="Failures drained within 30 min"
          value={dt.drain_within_30m_pct === null ? '—' : `${dt.drain_within_30m_pct}%`}
          delta={ppDelta(dt.drain_within_30m_pct, prev?.drain_within_30m_pct)}
          goodDirection="up"
          deltaUnit="pp"
          info={DEFS.drainWithin30m}
        />
      </div>
    </section>
  )
}

function ReliabilityPanel({
  rel,
  prev,
}: {
  rel: NetworkHealthReliability | undefined
  prev?: NetworkHealthReliabilityPrev | null
}) {
  if (!rel || !rel.duration_histogram) return null
  const hist = rel.duration_histogram
  const data = [
    { label: '≤5m', count: hist.flap_le5m },
    { label: '5–15m', count: hist.short_5_15m },
    { label: '15–60m', count: hist.medium_15_60m },
    { label: '1–24h', count: hist.sustained_1_24h },
    { label: '>24h', count: hist.chronic_gt24h },
  ]
  // vs-prior deltas on the two headline figures below (fewer failures / less
  // failure time is better, so goodDirection is "down"); each hides when the prior
  // is missing or the change is zero.
  const outageDelta = pctChange(rel.outage_count, prev?.outage_count)
  const downtimeDelta = pctChange(rel.capped_downtime_hours, prev?.capped_downtime_hours)
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Link failures by duration" info={DEFS.durationHist} />
      <p className="text-xs text-muted-foreground mb-1">
        How long each link failure lasted, not which link or device carried the most hours (see Most failure time above).
        This shows all failure episodes by length, including brief flaps under 5 minutes, so it counts more
        episodes than the headline link-failure count above (which counts only sustained failures of 10 minutes or more).
      </p>
      <p className="text-xs text-muted-foreground mb-4">
        {rel.outage_count.toLocaleString()} link failures
        {outageDelta !== undefined && outageDelta !== 0 && (
          <span className={`ml-1 ${deltaColorClass(outageDelta, 'down')}`}>{formatDelta(outageDelta)}</span>
        )}{' '}
        on {rel.distinct_links} links ·{' '}
        {rel.capped_downtime_hours.toLocaleString()} failure hours
        {downtimeDelta !== undefined && downtimeDelta !== 0 && (
          <span className={`ml-1 ${deltaColorClass(downtimeDelta, 'down')}`}>{formatDelta(downtimeDelta)}</span>
        )}{' '}
        ·{' '}
        <span className="inline-flex items-center gap-1">
          {rel.degraded_links} links saw loss without going down <InfoTip text={DEFS.degraded} />
        </span>
      </p>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis dataKey="label" tickLine={false} axisLine={false} tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }} dy={6} />
          <YAxis tickLine={false} axisLine={false} tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }} width={28} allowDecimals={false} />
          <ReTooltip
            cursor={{ fill: 'var(--muted)', opacity: 0.4 }}
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null
              return (
                <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                  <div className="text-muted-foreground mb-1">{label}</div>
                  <div className="font-semibold text-foreground">{payload[0].value} failures</div>
                </div>
              )
            }}
          />
          <Bar dataKey="count" fill={BAR.amber} fillOpacity={0.85} radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </section>
  )
}

// --- Trends + operator panels ---

function TrendsSection({
  points,
  onRange,
}: {
  points: NetworkHealthTsPoint[]
  onRange: (next: { days?: number; start?: string; end?: string }) => void
}) {
  const tput = points.map((p) => ({ t: p.t, avg: p.avg_bps / 1e9, max: p.max_bps / 1e9 }))
  // In-chart drag-to-zoom: press inside the chart and drag; on release, zoom the
  // whole report to the selected time range. left/right are the `t` values (RFC3339)
  // under the cursor (recharts activeLabel).
  const [dragLeft, setDragLeft] = useState<string | null>(null)
  const [dragRight, setDragRight] = useState<string | null>(null)
  const endDrag = () => {
    if (dragLeft && dragRight && dragLeft !== dragRight) {
      const [s, e] = dragLeft < dragRight ? [dragLeft, dragRight] : [dragRight, dragLeft]
      onRange({ start: s, end: e })
    }
    setDragLeft(null)
    setDragRight(null)
  }
  return (
    <div className="mb-6">
      <section className="rounded-lg border border-border p-4">
        <SectionTitle title="Throughput over time" info={DEFS.throughputChart} />
        <p className="mb-3 text-xs text-muted-foreground">
          Drag across the chart to zoom the whole report to that time range, then see what carried traffic
          and what broke in that window. Use the presets in the filter bar to reset.
        </p>
        <ChartLegend items={[[BAR.blue, 'Average'], [BAR.slate, 'Peak']]} />
        <ChartFrame empty={tput.length === 0} height={260}>
          <AreaChart
            data={tput}
            margin={{ top: 4, right: 8, left: 0, bottom: 0 }}
            onMouseDown={(e) => {
              const lbl = (e as { activeLabel?: string } | null)?.activeLabel
              if (lbl) {
                setDragLeft(lbl)
                setDragRight(null)
              }
            }}
            onMouseMove={(e) => {
              if (!dragLeft) return
              const lbl = (e as { activeLabel?: string } | null)?.activeLabel
              if (lbl) setDragRight(lbl)
            }}
            onMouseUp={endDrag}
            style={{ userSelect: 'none' }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
            <XAxis dataKey="t" tickFormatter={fmtT} tickLine={false} axisLine={false} minTickGap={40} tick={AXIS_TICK} />
            <YAxis tickLine={false} axisLine={false} width={36} tick={AXIS_TICK} unit=" G" />
            <ReTooltip content={makeChartTip('Gbps')} />
            <Area type="monotone" dataKey="avg" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.15} strokeWidth={1.5} name="avg" isAnimationActive={false} />
            <Line type="monotone" dataKey="max" stroke="#64748b" dot={false} strokeWidth={1} name="peak" isAnimationActive={false} />
            {dragLeft && dragRight && (
              <ReferenceArea x1={dragLeft} x2={dragRight} strokeOpacity={0.3} fill="#3b82f6" fillOpacity={0.12} />
            )}
          </AreaChart>
        </ChartFrame>
      </section>
    </div>
  )
}

function OutagesOverTimePanel({ rows }: { rows: NetworkHealthCountPoint[] }) {
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Link failures over time" info={DEFS.outagesChart} />
      <p className="text-xs text-muted-foreground mb-3">
        When link failures started, bucketed over the window. A different cut from "Most failure time" (total hours per
        link or device) and "Link failures by duration" (how long each failure ran), shown further down.
      </p>
      <ChartFrame empty={rows.length === 0}>
        <BarChart data={rows} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis dataKey="t" tickFormatter={fmtT} tickLine={false} axisLine={false} minTickGap={40} tick={AXIS_TICK} />
          <YAxis tickLine={false} axisLine={false} width={28} allowDecimals={false} tick={AXIS_TICK} />
          <ReTooltip content={makeChartTip('failures')} />
          <Bar dataKey="count" fill={BAR.amber} fillOpacity={0.85} radius={[3, 3, 0, 0]} />
        </BarChart>
      </ChartFrame>
    </section>
  )
}

// Horizontal bar chart of links ranked by a single fact (traffic, utilization).
// Neutral: these are link facts, not contributor rankings.
function LinkBarChart({
  rows,
  kind,
  unit,
  color = BAR.blue,
  emptyLabel = 'No links in this window.',
  labelWidth = 52,
}: {
  rows: { label: string; value: number; sub: string; pk: string; endLabel: string }[]
  kind: 'link' | 'device'
  unit?: string
  color?: string
  emptyLabel?: string
  labelWidth?: number
}) {
  const Tick = useEntityAxisTick(rows, kind)
  if (rows.length === 0) {
    return <div className="flex h-[120px] items-center justify-center text-sm text-muted-foreground">{emptyLabel}</div>
  }
  const height = rows.length * 30 + 24
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={rows} layout="vertical" margin={{ top: 4, right: labelWidth, left: 8, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
        <XAxis type="number" tickLine={false} axisLine={false} tick={AXIS_TICK} unit={unit} />
        <YAxis type="category" dataKey="label" width={160} interval={0} tickLine={false} axisLine={false} tick={<Tick />} />
        <ReTooltip
          cursor={{ fill: 'var(--muted)', opacity: 0.3 }}
          content={(raw: unknown) => {
            const p = raw as { active?: boolean; payload?: Array<{ payload?: { label: string; value: number; sub: string } }> }
            const d = p.active && p.payload?.length ? p.payload[0].payload : undefined
            if (!d) return null
            return (
              <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                <div className="font-semibold text-foreground">{d.label}</div>
                {d.sub && <div className="text-muted-foreground">{d.sub}</div>}
                <div className="tabular-nums mt-1">
                  {d.value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                  {unit ?? ''}
                </div>
              </div>
            )
          }}
        />
        <Bar
          dataKey="value"
          fill={color}
          radius={[0, 3, 3, 0]}
          isAnimationActive={false}
        >
          <LabelList
            dataKey="endLabel"
            position="right"
            offset={6}
            fontSize={11}
            fill="var(--foreground)"
            className="tabular-nums"
          />
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// Horizontal grouped-bar chart: two or three facts per link on one shared axis
// (all series are the same unit here). The third series `c` is optional and
// only rendered when at least one row supplies it (e.g. peak utilization on the
// capacity panel); the DIA panel omits it and stays a 2-bar chart. `note` adds
// a small clarifying line to the tooltip.
function GroupedBarChart({ rows, kind, unit, refLine, labels = ['P50', 'P99', 'Peak'], note }: {
  rows: { label: string; a: number; b: number; c?: number; sub: string; pk: string }[]
  kind: 'link' | 'device'
  unit?: string; refLine?: number; labels?: [string, string, string]; note?: string
}) {
  const Tick = useEntityAxisTick(rows, kind)
  if (rows.length === 0) return <div className="flex h-[120px] items-center justify-center text-sm text-muted-foreground">No data in this window.</div>
  const hasC = rows.some((r) => r.c !== undefined)
  const height = rows.length * (hasC ? 58 : 44) + 24
  const fmt = (v: number) => `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}${unit ?? ''}`
  const legendItems: Array<[string, string]> = [[BAR.blue, labels[0]], [BAR.amber, labels[1]]]
  if (hasC) legendItems.push([BAR.slate, labels[2]])
  return (
    <>
    <ChartLegend items={legendItems} />
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={rows} layout="vertical" margin={{ top: 4, right: 56, left: 8, bottom: 0 }} barGap={2}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
        <XAxis type="number" tickLine={false} axisLine={false} tick={AXIS_TICK} unit={unit} />
        <YAxis type="category" dataKey="label" width={160} interval={0} tickLine={false} axisLine={false} tick={<Tick />} />
        <ReTooltip
          cursor={{ fill: 'var(--muted)', opacity: 0.3 }}
          content={(raw: unknown) => {
            const p = raw as { active?: boolean; payload?: Array<{ payload?: { label: string; sub: string; a: number; b: number; c?: number } }> }
            const d = p.active && p.payload?.length ? p.payload[0].payload : undefined
            if (!d) return null
            return (
              <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
                <div className="font-semibold text-foreground">{d.label}</div>
                {d.sub && <div className="text-muted-foreground">{d.sub}</div>}
                <div className="tabular-nums mt-1">{labels[0]} {fmt(d.a)}</div>
                <div className="tabular-nums">{labels[1]} {fmt(d.b)}</div>
                {d.c !== undefined && <div className="tabular-nums">{labels[2]} {fmt(d.c)}</div>}
                {note && d.c !== undefined && (
                  <div className="text-muted-foreground mt-1 max-w-[220px]">{note}</div>
                )}
              </div>
            )
          }}
        />
        {refLine !== undefined && <ReferenceLine x={refLine} stroke={BAR.slate} strokeDasharray="4 4" strokeOpacity={0.7} />}
        <Bar dataKey="a" name={labels[0]} fill={BAR.blue} fillOpacity={0.85} radius={[0,2,2,0]} isAnimationActive={false}>
          <LabelList dataKey="a" position="right" offset={6} formatter={(v) => fmt(Number(v))} fontSize={10} fill={BAR.blue} className="tabular-nums" />
        </Bar>
        <Bar dataKey="b" name={labels[1]} fill={BAR.amber} fillOpacity={0.85} radius={[0,2,2,0]} isAnimationActive={false}>
          <LabelList dataKey="b" position="right" offset={6} formatter={(v) => fmt(Number(v))} fontSize={10} fill={BAR.amber} className="tabular-nums" />
        </Bar>
        {hasC && (
          <Bar dataKey="c" name={labels[2]} fill={BAR.slate} fillOpacity={0.85} radius={[0,2,2,0]} isAnimationActive={false}>
            <LabelList dataKey="c" position="right" offset={6} formatter={(v) => fmt(Number(v))} fontSize={10} fill={BAR.slate} className="tabular-nums" />
          </Bar>
        )}
      </BarChart>
    </ResponsiveContainer>
    </>
  )
}

// Reusable color legend: a wrapping row of dot + label pairs. Used by every
// colored panel (availability segments, capacity bars, device slots, throughput).
function ChartLegend({ items }: { items: Array<[string, string]> }) {
  return (
    <div className="flex flex-wrap items-center gap-4 text-[11px] text-muted-foreground mb-3">
      {items.map(([color, label]) => (
        <span key={label} className="inline-flex items-center gap-1.5">
          <span className="inline-block h-2 w-2 rounded-full" style={{ background: color }} />
          {label}
        </span>
      ))}
    </div>
  )
}

// Legend for the 3-way availability segments, shared by the link and device
// availability panels. The middle (drained) label is parametrized so the device
// panel can spell out that it only turns this color when ALL links are drained.
function AvailabilityLegend({ drainedLabel, faultLabel }: { drainedLabel: string; faultLabel: string }) {
  return (
    <ChartLegend
      items={[
        [BAR.blue, 'Available'],
        [BAR.slate, drainedLabel],
        [BAR.amber, faultLabel],
      ]}
    />
  )
}

// Segmented per-entity meter: Available / Drained / Outage shares of the
// window, summing to 100%. Reuses the segmented-meter div pattern from
// DeviceSlotsPanel (not recharts), since this is a per-entity time split, not
// a comparable-magnitude bar chart. Rows are pre-sorted least-available-first
// by the backend.
function AvailabilityRows({ rows, drainedLabel, faultLabel, kind }: { rows: NetworkHealthAvailability[]; drainedLabel: string; faultLabel: string; kind: 'link' | 'device' }) {
  if (rows.length === 0) {
    return <p className="text-xs text-muted-foreground">No data in this window.</p>
  }
  return (
    <div className="space-y-2">
      {rows.map((r) => (
        <div
          key={r.pk}
          className="grid grid-cols-[140px_1fr_92px] items-center gap-3 text-xs"
        >
          <EntityLink kind={kind} pk={r.pk} className="truncate text-muted-foreground" title={r.metros}>
            {r.code}
          </EntityLink>
          <Tooltip content={<AvailabilityTooltip r={r} drainedLabel={drainedLabel} faultLabel={faultLabel} />} className="bg-card shadow-xl">
            <div className="flex h-3 rounded-sm bg-muted/30 overflow-hidden">
              <div className="h-full" style={{ width: `${Math.max(0, r.avail_pct)}%`, background: BAR.blue }} />
              <div className="h-full" style={{ width: `${Math.max(0, r.drained_pct)}%`, background: BAR.slate }} />
              <div className="h-full" style={{ width: `${Math.max(0, r.outage_pct)}%`, background: BAR.amber }} />
            </div>
          </Tooltip>
          <span className="text-right tabular-nums text-muted-foreground whitespace-nowrap">{r.avail_pct}% avail</span>
        </div>
      ))}
    </div>
  )
}

// Full available/drained/outage breakdown (percent and hours) shown on hover
// over an AvailabilityRows bar. The row label only shows the availability
// percent (the bar is the visual signal), so this is where the other two
// segments' numbers live.
function AvailabilityTooltip({ r, drainedLabel, faultLabel }: { r: NetworkHealthAvailability; drainedLabel: string; faultLabel: string }) {
  const segs: Array<[string, string, number, number]> = [
    [BAR.blue, 'Available', r.avail_pct, r.avail_hours],
    [BAR.slate, drainedLabel, r.drained_pct, r.drained_hours],
    [BAR.amber, faultLabel, r.outage_pct, r.outage_hours],
  ]
  return (
    <div className="min-w-[170px]">
      <div className="font-semibold text-foreground mb-1">{r.code}</div>
      {segs.map(([color, label, pct, hours]) => (
        <div key={label} className="flex items-center justify-between gap-4 tabular-nums">
          <span className="inline-flex items-center gap-1.5 text-muted-foreground">
            <span className="inline-block h-2 w-2 rounded-full" style={{ background: color }} />
            {label}
          </span>
          <span>
            {pct}% · {hours}h
          </span>
        </div>
      ))}
    </div>
  )
}

function LinkAvailabilityPanel({ rows }: { rows: NetworkHealthAvailability[] }) {
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Least available links" info={DEFS.availabilityLink} />
      <p className="text-xs text-muted-foreground mb-3">
        Available / maintenance (drained) / down (fault) over the window, as a percent of time. Intentional
        maintenance drain counts as unavailable but is not a fault. Least available first.
      </p>
      <AvailabilityLegend drainedLabel="Maintenance (drained)" faultLabel="Down (fault)" />
      <AvailabilityRows rows={rows} drainedLabel="Maintenance (drained)" faultLabel="Down (fault)" kind="link" />
    </section>
  )
}

function DeviceAvailabilityPanel({ rows }: { rows: NetworkHealthAvailability[] }) {
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Least available devices" info={DEFS.availabilityDevice} />
      <p className="text-xs text-muted-foreground mb-3">
        Device reachability, the page's true-outage signal: a device is unreachable only when it has no working
        path at all. Available when at least one link is working, unreachable when a fault leaves every link down,
        maintenance when every link was intentionally drained with no fault. One link down while others work does
        not lower the device. Ranked by unreachable time.
      </p>
      <AvailabilityLegend drainedLabel="Maintenance (all links drained)" faultLabel="Unreachable (fault)" />
      <AvailabilityRows rows={rows} drainedLabel="Maintenance (all links drained)" faultLabel="Unreachable (fault)" kind="device" />
    </section>
  )
}

// Links and devices ranked by total fault-hours over the window, counted from
// the 5-minute link rollups (full retention). Amber bars mark the down (fault)
// dimension, matching the availability panels' fault color.
function MostDowntimePanel({
  links,
  devices,
}: {
  links: NetworkHealthDowntimeRow[]
  devices: NetworkHealthDowntimeRow[]
}) {
  const linkData = links.map((l) => ({ label: l.code, value: l.hours, sub: `${l.metros ?? ''} · ${l.outages} failures`, pk: l.pk, endLabel: `${l.hours.toLocaleString(undefined, { maximumFractionDigits: 1 })} h` }))
  const deviceData = devices.map((d) => ({ label: d.code, value: d.hours, sub: `${d.metros ?? ''} · ${d.outages} failures`, pk: d.pk, endLabel: `${d.hours.toLocaleString(undefined, { maximumFractionDigits: 1 })} h` }))
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Most failure time" info={DEFS.mostDowntime} />
      <p className="text-xs text-muted-foreground mb-3">
        Total hours each link or device was actually down from a fault, biggest total first. This is a count of
        hours, not a percentage of the window like "Least available" above. Maintenance is excluded, and only
        faults lasting 10 or more minutes count.
      </p>
      <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Links</h3>
      <LinkBarChart rows={linkData} kind="link" unit=" h" color={BAR.amber} emptyLabel="No link failures in this window." />
      <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2 mt-6">Devices</h3>
      <LinkBarChart
        rows={deviceData}
        kind="device"
        unit=" h"
        color={BAR.amber}
        emptyLabel="No device failures in this window."
      />
    </section>
  )
}

function TopLinksPanel({ rows }: { rows: NetworkHealthTrafficLink[] }) {
  const data = rows.map((l) => ({
    label: l.link_code,
    value: l.avg_gbps,
    sub: `${l.side_a_metro} ↔ ${l.side_z_metro} · peak ${l.max_gbps} G`,
    pk: l.link_pk,
    endLabel: `${l.avg_gbps.toLocaleString(undefined, { maximumFractionDigits: 2 })} / ${l.max_gbps.toLocaleString(undefined, { maximumFractionDigits: 2 })} G`,
  }))
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Links carrying the most traffic" info={DEFS.topLinks} />
      <p className="text-xs text-muted-foreground mb-3">Average Gbps over the window. Label shows avg / peak.</p>
      <LinkBarChart rows={data} kind="link" unit=" G" labelWidth={124} />
    </section>
  )
}

function CapacityPanel({ rows }: { rows: NetworkHealthCapacityLink[] }) {
  const data = rows.map((l) => ({
    label: l.link_code,
    a: l.p50_util,
    b: l.p99_util,
    c: l.util_pct,
    sub: `${l.side_a_metro} ↔ ${l.side_z_metro} · ${l.peak_gbps}/${l.bandwidth_gbps} G`,
    pk: l.link_pk,
  }))
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Fullest links (capacity planning)" info={DEFS.capacityLinks} />
      <p className="text-xs text-muted-foreground mb-3">
        P50 (typical), P99, and peak utilization of provisioned bandwidth. Dashed line = 100%. Port-channel subinterface links are not measured yet.
      </p>
      <GroupedBarChart
        rows={data}
        kind="link"
        unit="%"
        refLine={100}
        labels={['P50', 'P99', 'Peak']}
        note="Peak is the combined both-directions 5-minute max; P50/P99 are the busier single direction, so Peak sits above P99."
      />
    </section>
  )
}

// The server nulls a row's percentage when the row has no denominator (a device
// with max_users 0, a DIA interface with no port speed). Those rows render as
// unknown, never as 0 or as a full bar.
export function hasKnownDenominator(pct: number | null | undefined, denom: number): boolean {
  return pct !== null && pct !== undefined && denom > 0
}

// One device-slot row: seats in use, and the three segment widths as a share of
// the max_users cap. widths is null when the device has no cap set, where a
// share of the cap has no meaning.
export function deviceSlotRow(d: NetworkHealthDeviceSlots): {
  known: boolean
  used: number
  widths: { unicast: string; sub: string; pub: string } | null
} {
  const used = d.unicast + d.mcast_sub + d.mcast_pub
  if (!hasKnownDenominator(d.used_pct, d.max_users)) return { known: false, used, widths: null }
  const seg = (n: number) => `${((100 * n) / d.max_users).toFixed(1)}%`
  return {
    known: true,
    used,
    widths: { unicast: seg(d.unicast), sub: seg(d.mcast_sub), pub: seg(d.mcast_pub) },
  }
}

// Segmented per-device meter: user slots in use (unicast, multicast sub,
// multicast pub) vs the device's max_users cap.
export function DeviceSlotsPanel({ rows }: { rows: NetworkHealthDeviceSlots[] }) {
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Fullest devices (user slots)" info={DEFS.deviceSlots} />
      <p className="text-xs text-muted-foreground mb-3">Used vs max_users.</p>
      <ChartLegend items={[[BAR.blue, 'Unicast'], [BAR.amber, 'Multicast subscriber'], [BAR.slate, 'Multicast publisher']]} />
      <div className="space-y-2">
        {rows.map((d) => {
          const { known, used, widths } = deviceSlotRow(d)
          const seats = `Unicast ${d.unicast}, Multicast subscriber ${d.mcast_sub}, Multicast publisher ${d.mcast_pub}.`
          return (
            <div
              key={d.pk}
              className="grid grid-cols-[130px_1fr_128px] items-center gap-3 text-xs"
            >
              <DeviceLink pk={d.pk} className="truncate text-muted-foreground">{d.code}</DeviceLink>
              <div
                className="relative h-3 rounded-sm bg-muted/30 overflow-hidden"
                title={known ? `${seats} ${used} of ${d.max_users} max.` : `${seats} No max_users cap set.`}
              >
                {widths && (
                  <>
                    <div className="absolute inset-y-0" style={{ left: 0, width: widths.unicast, background: BAR.blue }} title={`Unicast ${d.unicast}`} />
                    <div className="absolute inset-y-0" style={{ left: widths.unicast, width: widths.sub, background: BAR.amber }} title={`Multicast subscriber ${d.mcast_sub}`} />
                    <div
                      className="absolute inset-y-0"
                      style={{ left: `calc(${widths.unicast} + ${widths.sub})`, width: widths.pub, background: BAR.slate }}
                      title={`Multicast publisher ${d.mcast_pub}`}
                    />
                  </>
                )}
              </div>
              <span className="text-right tabular-nums text-muted-foreground whitespace-nowrap">
                <span style={{ color: BAR.blue }}>{d.unicast}</span>
                {' / '}
                <span style={{ color: BAR.amber }}>{d.mcast_sub}</span>
                {' / '}
                <span style={{ color: BAR.slate }}>{d.mcast_pub}</span>
                {' of '}
                {known ? d.max_users : <span className="text-muted-foreground/60">no cap</span>}
              </span>
            </div>
          )
        })}
        {rows.length === 0 && <p className="text-xs text-muted-foreground">No devices in this window.</p>}
      </div>
    </section>
  )
}

// DIA rows split by whether the server could compute a utilization for them. An
// interface whose port speed is missing has no denominator and arrives with
// util_pct null; charting it as 0% would draw a busy interface as the idlest
// one. Unknown rows are listed instead of dropped, because the server already
// applied its row limit and a dropped row is not replaced by the next one.
export function splitDiaRows(rows: NetworkHealthDiaInterface[]): {
  measured: NetworkHealthDiaInterface[]
  unknown: NetworkHealthDiaInterface[]
} {
  const measured: NetworkHealthDiaInterface[] = []
  const unknown: NetworkHealthDiaInterface[] = []
  for (const d of rows) {
    if (hasKnownDenominator(d.util_pct, d.port_gbps)) measured.push(d)
    else unknown.push(d)
  }
  return { measured, unknown }
}

// Direct Internet Access interfaces: P50/P99 outbound utilization as a percent
// of provisioned capacity (CIR when set, else port speed), so headroom is
// obvious without having to hold an unknown committed rate in your head.
// Fullest first (by P99 utilization). The absolute Gbps and which capacity
// figure applied still surface in the row sub / hover tooltip.
function DiaPanel({ rows }: { rows: NetworkHealthDiaInterface[] }) {
  const { measured, unknown } = splitDiaRows(rows)
  const data = measured
    .map((d) => {
      const cirNote = d.cir_gbps > 0 ? ` · CIR ${d.cir_gbps} G` : ''
      return {
        label: `${d.device} ${d.intf}`,
        a: (d.p50_gbps / d.port_gbps) * 100,
        b: d.util_pct!,
        sub: `P99 ${d.p99_gbps.toFixed(1)} G of ${d.port_gbps.toFixed(1)} G port speed${cirNote}`,
        pk: d.device_pk,
      }
    })
    .sort((x, y) => y.b - x.b)
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="DIA interfaces (capacity vs bandwidth)" info={DEFS.dia} />
      <p className="text-xs text-muted-foreground mb-3">
        P50 (typical) vs P99 (peak) outbound utilization, as a share of interface bandwidth (port speed).
        Dashed line = 100%. The committed rate (CIR) is shown alongside for reference. Port speed is a
        snapshot refreshed roughly monthly, so a recently changed interface may lag.
      </p>
      <GroupedBarChart rows={data} kind="device" unit="%" refLine={100} />
      {unknown.length > 0 && (
        <div className="mt-3 border-t border-border pt-3">
          <p className="text-xs text-muted-foreground mb-2">
            Port speed not reported, so utilization cannot be computed for these interfaces.
          </p>
          <ul className="space-y-1">
            {unknown.map((d) => (
              <li
                key={`${d.device_pk}-${d.intf}`}
                className="grid grid-cols-[1fr_auto] items-baseline gap-3 text-xs text-muted-foreground"
              >
                <span className="truncate">
                  {d.device} {d.intf}
                </span>
                <span className="text-right tabular-nums whitespace-nowrap">
                  P99 {d.p99_gbps.toFixed(1)} G
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  )
}

function PerfLinksPanel({ rows }: { rows: NetworkHealthPerfLink[] }) {
  const [sort, setSort] = useState<Sort>({ key: 'drift_ms', dir: 'desc' })
  const sorted = useMemo(() => sortRows(rows, sort), [rows, sort])
  const paged = usePaged(sorted)
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Committed vs measured latency" info={DEFS.perfLinks} />
      <p className="text-xs text-muted-foreground mb-4">
        Links with an onchain committed round-trip time, compared to what was measured this window.
        Plain measurements, sortable, no pass/fail. A committed value tagged "override" reflects an active
        IS-IS delay override rather than the raw onchain commitment.
      </p>
      <Table className="[&_th]:h-8 [&_th]:px-2 [&_td]:px-2 [&_td]:py-1 [&_th]:whitespace-nowrap [&_td]:whitespace-nowrap">
        <TableHeader>
          <TableRow>
            <SortableHead label="Link" sortKey="link_code" sort={sort} setSort={setSort} />
            <TableHead>Metros</TableHead>
            <SortableHead label="Committed (ms)" sortKey="committed_ms" sort={sort} setSort={setSort} align="right" info={DEFS.linkCommitted} />
            <SortableHead label="Measured avg (ms)" sortKey="measured_avg_ms" sort={sort} setSort={setSort} align="right" info={DEFS.linkMeasured} />
            <SortableHead label="Measured max (ms)" sortKey="measured_max_ms" sort={sort} setSort={setSort} align="right" info={DEFS.linkMeasured} />
            <SortableHead label="Over committed %" sortKey="over_committed_pct" sort={sort} setSort={setSort} align="right" info={DEFS.linkOverCommitted} />
            <SortableHead label="Drift (ms)" sortKey="drift_ms" sort={sort} setSort={setSort} align="right" info={DEFS.linkDrift} />
            <SortableHead label="Drift (%)" sortKey="drift_pct" sort={sort} setSort={setSort} align="right" info={DEFS.linkDrift} />
          </TableRow>
        </TableHeader>
        <TableBody>
          {paged.pageRows.map((l) => {
            const driftClass = l.drift_ms > 0 ? 'text-amber-500' : 'text-muted-foreground'
            return (
              <TableRow key={l.link_pk}>
                <TableCell className="font-medium"><LinkLink pk={l.link_pk} className="block max-w-[240px] truncate" title={l.link_code}>{l.link_code}</LinkLink></TableCell>
                <TableCell className="text-muted-foreground">
                  {l.side_a_metro} ↔ {l.side_z_metro}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  <span className="inline-flex items-center justify-end gap-1.5">
                    {l.overridden && l.raw_committed_ms != null && (
                      <span
                        className="rounded bg-muted px-1 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground"
                        title={`IS-IS delay override in effect. Onchain committed ${l.raw_committed_ms.toLocaleString(undefined, { maximumFractionDigits: 2 })} ms, overridden to ${l.committed_ms.toLocaleString(undefined, { maximumFractionDigits: 2 })} ms (the value the network routes on). Committed, over-committed %, and drift use the override.`}
                      >
                        override
                      </span>
                    )}
                    {l.committed_ms.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                  </span>
                </TableCell>
                <TableCell className="text-right tabular-nums">{l.measured_avg_ms.toLocaleString(undefined, { maximumFractionDigits: 2 })}</TableCell>
                <TableCell className="text-right tabular-nums">{l.measured_max_ms.toLocaleString(undefined, { maximumFractionDigits: 2 })}</TableCell>
                <TableCell className="text-right tabular-nums text-muted-foreground">{l.over_committed_pct}%</TableCell>
                <TableCell className={`text-right tabular-nums ${driftClass}`}>{l.drift_ms.toLocaleString(undefined, { maximumFractionDigits: 2 })}</TableCell>
                <TableCell className={`text-right tabular-nums ${driftClass}`}>{l.drift_pct}%</TableCell>
              </TableRow>
            )
          })}
          {sorted.length === 0 && (
            <TableRow>
              <TableCell className="text-muted-foreground text-sm" colSpan={8}>
                No links with a committed RTT in this window.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
      <PageControls {...paged} />
    </section>
  )
}

function HotspotsPanel({ rows }: { rows: NetworkHealthErrorHotspot[] }) {
  return (
    <section className="rounded-lg border border-border p-4">
      <SectionTitle title="Interface errors & carrier flaps" info={DEFS.hotspots} />
      <p className="text-xs text-muted-foreground mb-3">
        Physical-layer noise on device interfaces, not logged failures. A leading signal that can show up before a
        failure does.
      </p>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Device</TableHead>
            <TableHead className="text-right">Errors + discards</TableHead>
            <TableHead className="text-right">Carrier transitions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((d) => (
            <TableRow key={d.device_pk}>
              <TableCell className="font-medium"><DeviceLink pk={d.device_pk}>{d.device_code || d.device_pk.slice(0, 8)}</DeviceLink></TableCell>
              <TableCell className="text-right tabular-nums">{d.errors.toLocaleString()}</TableCell>
              <TableCell className="text-right tabular-nums">{d.carrier_flaps.toLocaleString()}</TableCell>
            </TableRow>
          ))}
          {rows.length === 0 && (
            <TableRow>
              <TableCell className="text-muted-foreground text-sm" colSpan={3}>
                No interface errors or carrier flaps in this window.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </section>
  )
}

const AXIS_TICK = { fontSize: 11, fill: 'var(--muted-foreground)' }

// A Recharts YAxis category tick that renders the entity label as SVG text and,
// when the row carries a pk, links out to the entity detail page on click.
// Recharts injects x, y and payload (payload.value = the label, payload.index =
// the row index in data order), so we map the tick back to its row's pk. Rows
// without a pk render as plain, non-interactive text. This replaces the old
// under-chart ChartLinkList: the bar's own label is now the link.
function useEntityAxisTick(rows: { pk?: string }[], kind: 'link' | 'device') {
  const navigate = useNavigate()
  const base = kind === 'link' ? '/dz/links/' : '/dz/devices/'
  return function EntityAxisTick(props: { x?: number; y?: number; payload?: { value?: string | number; index?: number } }) {
    const { x = 0, y = 0, payload } = props
    const label = String(payload?.value ?? '')
    const pk = payload?.index != null ? rows[payload.index]?.pk : undefined
    const attrs = { x, y, dy: 4, textAnchor: 'end' as const, fontSize: 11 }
    if (!pk) return <text {...attrs} fill="var(--muted-foreground)">{label}</text>
    return (
      <text
        {...attrs}
        fill="var(--muted-foreground)"
        className="cursor-pointer hover:underline hover:fill-[var(--foreground)]"
        onClick={() => navigate(base + pk)}
      >
        {label}
      </text>
    )
  }
}

function ChartFrame({ children, empty, height = 200 }: { children: React.ReactElement; empty: boolean; height?: number }) {
  if (empty) {
    return (
      <div className="flex items-center justify-center text-sm text-muted-foreground" style={{ height }}>
        No data in this window.
      </div>
    )
  }
  return (
    <ResponsiveContainer width="100%" height={height}>
      {children}
    </ResponsiveContainer>
  )
}

function makeChartTip(unit?: string) {
  return (raw: unknown) => {
    const { active, payload, label } = (raw ?? {}) as {
      active?: boolean
      payload?: Array<{ name?: string; value?: number; color?: string }>
      label?: string | number
    }
    if (!active || !payload?.length) return null
    return (
      <div className="bg-card border border-border rounded-lg px-3 py-2 text-xs shadow-xl">
        <div className="text-muted-foreground mb-1">{fmtT(String(label))}</div>
        {payload.map((p, i) => (
          <div key={i} className="font-medium" style={{ color: p.color }}>
            {typeof p.value === 'number' ? p.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : p.value}
            {unit ? ` ${unit}` : ''}
            {p.name ? ` ${p.name}` : ''}
          </div>
        ))}
      </div>
    )
  }
}

function fmtT(t: string): string {
  const d = new Date(t)
  if (isNaN(d.getTime())) return t
  return `${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`
}




// --- Small building blocks ---

// Top filter bar: scope selector (whole network / a contributor) + time-range
// presets + a two-click calendar range picker. Reads scope from the URL and
// re-scopes every panel through useScopeNav; the range flows through onRange
// (same contract as the old TimeRangeControl).
function NetworkHealthFilterBar({
  days,
  start,
  end,
  onRange,
}: {
  days: number
  start: string
  end: string
  onRange: (next: { days?: number; start?: string; end?: string }) => void
}) {
  const [params] = useSearchParams()
  const { openContributor, toNetwork } = useScopeNav()
  const [scopeOpen, setScopeOpen] = useState(false)
  const [calOpen, setCalOpen] = useState(false)

  const scopeContributor = params.get('contributor') ?? ''
  const scopeActive = !!scopeContributor
  const scopeLabel = scopeContributor ? `Contributor: ${scopeContributor}` : 'Whole network'

  // Contributor list for the scope dropdown. Only fetched on the network view,
  // where it shares NetworkView's cached query (no extra request). On scoped
  // views the fetch is skipped and the menu just offers "Whole network".
  const range: NetworkHealthParams = start && end ? { start, end } : { days }
  const { data: net } = useQuery({
    queryKey: ['nh', 'overview', range, ''],
    queryFn: () => fetchNHOverview(range, ''),
    staleTime: 60_000,
    placeholderData: keepPreviousData,
    enabled: !scopeActive,
  })
  const contributors = (net?.contributors ?? []).map((c) => c.code)

  const lastMonth = getLastMonth()
  const isCustom = !!(start && end)
  const isLastMonth = isCustom && start === lastMonth.start && end === lastMonth.end
  const active = isLastMonth ? 'month' : isCustom ? 'custom' : days === 7 ? '7d' : '30d'

  const chip = (key: string, label: string, onClick: () => void) => (
    <button
      key={key}
      onClick={onClick}
      className={`px-3 py-1 text-sm ${active === key ? 'bg-muted font-medium' : 'text-muted-foreground hover:bg-muted/50'}`}
    >
      {label}
    </button>
  )

  return (
    <div className="flex flex-wrap items-center gap-2">
      <div className="relative">
        <button
          onClick={() => {
            if (scopeActive) {
              toNetwork()
              return
            }
            setScopeOpen((o) => !o)
            setCalOpen(false)
          }}
          className="inline-flex items-center gap-1 rounded-md border border-border px-3 py-1 text-sm hover:bg-muted/50"
        >
          {scopeActive && <ArrowLeft className="h-3.5 w-3.5" />}
          {scopeActive ? 'Whole network' : scopeLabel}
          {!scopeActive && <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />}
        </button>
        {scopeOpen && !scopeActive && (
          <>
            <div className="fixed inset-0 z-10" onClick={() => setScopeOpen(false)} />
            <div className="absolute left-0 z-20 mt-1 max-h-72 w-56 overflow-auto rounded-md border border-border bg-card py-1 text-sm shadow-xl">
              <button
                className={`block w-full px-3 py-1.5 text-left hover:bg-muted ${!scopeActive ? 'bg-muted font-medium' : ''}`}
                onClick={() => {
                  setScopeOpen(false)
                  toNetwork()
                }}
              >
                Whole network
              </button>
              {contributors.length > 0 && (
                <div className="px-3 py-1 text-xs text-muted-foreground">Contributors</div>
              )}
              {contributors.map((c) => (
                <button
                  key={c}
                  className={`block w-full px-3 py-1.5 text-left hover:bg-muted ${c === scopeContributor ? 'bg-muted font-medium' : ''}`}
                  onClick={() => {
                    setScopeOpen(false)
                    openContributor(c)
                  }}
                >
                  {c}
                </button>
              ))}
            </div>
          </>
        )}
      </div>

      <div className="relative">
        <div className="inline-flex overflow-hidden rounded-md border border-border">
          {chip('7d', '7d', () => {
            setCalOpen(false)
            onRange({ days: 7 })
          })}
          {chip('30d', '30d', () => {
            setCalOpen(false)
            onRange({ days: 30 })
          })}
          {chip('month', 'Last month', () => {
            setCalOpen(false)
            onRange(lastMonth)
          })}
          {chip('custom', 'Custom', () => {
            setCalOpen((o) => !o)
            setScopeOpen(false)
          })}
        </div>
        {calOpen && (
          <>
            <div className="fixed inset-0 z-10" onClick={() => setCalOpen(false)} />
            <div className="absolute right-0 z-20 mt-1">
              <RangeCalendar
                startStr={start}
                endStr={end}
                onApply={(s, e) => {
                  setCalOpen(false)
                  onRange({ start: s, end: e })
                }}
              />
            </div>
          </>
        )}
      </div>
    </div>
  )
}

// Two-click date-range picker: first click sets the start, second sets the end
// (swapping if earlier), Apply emits YYYY-MM-DD strings. Built on the existing
// maintenance-calendar date helpers so no date library is added.
function RangeCalendar({
  startStr,
  endStr,
  onApply,
}: {
  startStr: string
  endStr: string
  onApply: (start: string, end: string) => void
}) {
  const [from, setFrom] = useState<Date | null>(parseISODate(startStr))
  const [to, setTo] = useState<Date | null>(parseISODate(endStr))
  const [anchor, setAnchor] = useState<Date>(parseISODate(startStr) ?? startOfDay(new Date()))

  const monthDays = getDays('month', anchor)
  const lead = (monthDays[0].getDay() + 6) % 7 // Monday-first leading blanks
  const canApply = !!(from && to && from < to)

  const clickDay = (d: Date) => {
    if (!from || (from && to)) {
      setFrom(d)
      setTo(null)
    } else if (d < from) {
      setTo(from)
      setFrom(d)
    } else {
      setTo(d)
    }
  }

  return (
    <div className="w-64 rounded-md border border-border bg-card p-3 shadow-xl">
      <div className="mb-2 flex items-center justify-between">
        <button
          className="rounded px-2 py-1 text-muted-foreground hover:bg-muted"
          onClick={() => setAnchor(new Date(anchor.getFullYear(), anchor.getMonth() - 1, 1))}
        >
          ‹
        </button>
        <div className="text-sm font-medium">{formatDateRange('month', anchor, monthDays)}</div>
        <button
          className="rounded px-2 py-1 text-muted-foreground hover:bg-muted"
          onClick={() => setAnchor(new Date(anchor.getFullYear(), anchor.getMonth() + 1, 1))}
        >
          ›
        </button>
      </div>
      <div className="mb-1 grid grid-cols-7 gap-0.5 text-center text-[10px] text-muted-foreground">
        {['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'].map((d) => (
          <div key={d}>{d}</div>
        ))}
      </div>
      <div className="grid grid-cols-7 gap-0.5">
        {Array.from({ length: lead }).map((_, i) => (
          <div key={`b${i}`} />
        ))}
        {monthDays.map((d) => {
          const endpoint = (from && isSameDay(d, from)) || (to && isSameDay(d, to))
          const within = from && to && d > from && d < to
          return (
            <button
              key={d.getTime()}
              onClick={() => clickDay(d)}
              className={`flex h-8 items-center justify-center rounded text-sm ${
                endpoint ? 'bg-primary text-primary-foreground' : within ? 'bg-muted' : 'hover:bg-muted'
              }`}
            >
              {d.getDate()}
            </button>
          )
        })}
      </div>
      <div className="mt-3 flex items-center justify-between text-xs">
        <span className="text-muted-foreground tabular-nums">
          {from ? formatISODate(from) : '—'} → {to ? formatISODate(to) : '—'}
        </span>
        <button
          disabled={!canApply}
          onClick={() => {
            if (from && to) onApply(formatISODate(from), formatISODate(to))
          }}
          className="rounded-md bg-primary px-3 py-1 text-primary-foreground disabled:opacity-40"
        >
          Apply
        </button>
      </div>
    </div>
  )
}

// A top-level section band that groups panels (General / Performance / Capacity /
// Operations). Neutral, factual heading; no judgment.
function SectionBand({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div className="mt-4 mb-3 border-b border-border pb-1.5">
      <h2 className="text-base font-semibold">{title}</h2>
      {subtitle && <p className="text-xs text-muted-foreground mt-0.5">{subtitle}</p>}
    </div>
  )
}

function SectionTitle({ title, info }: { title: string; info?: string }) {
  return (
    <h2 className="text-sm font-medium mb-1 inline-flex items-center gap-1">
      {title}
      {info && <InfoTip text={info} />}
    </h2>
  )
}

function InfoTip({ text }: { text: string }) {
  return (
    <Tooltip content={text}>
      <span className="cursor-help text-muted-foreground/50 hover:text-foreground inline-flex align-middle">
        <Info className="h-3.5 w-3.5" />
      </span>
    </Tooltip>
  )
}

// A compact stat tile. Value is a pre-formatted string. Optional tone grades the
// value (green/amber/red), matching StatCard's tone. Optional delta shows the
// change versus the prior window next to the value, colored by goodDirection and
// formatted as a percent change ('pct') or percentage points ('pp') for rate
// metrics. No delta renders when it is undefined or exactly zero.
function MiniStat({
  label,
  value,
  info,
  tone,
  delta,
  goodDirection = 'neutral',
  deltaUnit = 'pct',
  muted = false,
}: {
  label: string
  value: string
  info?: string
  tone?: 'good' | 'warn' | 'bad'
  delta?: number
  goodDirection?: 'up' | 'down' | 'neutral'
  deltaUnit?: 'pct' | 'pp'
  muted?: boolean
}) {
  const showDelta = delta !== undefined && delta !== 0
  return (
    <div className="rounded-md bg-muted/50 p-3">
      <div className="text-xl font-medium tabular-nums inline-flex items-baseline gap-2">
        <span className={muted ? 'text-sm font-normal text-muted-foreground' : tone ? toneColorClass(tone) : ''}>{value}</span>
        {showDelta && (
          <span className={`text-xs font-normal ${deltaColorClass(delta, goodDirection)}`}>
            {formatDelta(delta, deltaUnit)}
          </span>
        )}
      </div>
      <div className="text-xs text-muted-foreground inline-flex items-center gap-1">
        {label}
        {info && <InfoTip text={info} />}
      </div>
    </div>
  )
}

function Stat({ label, value, info }: { label: string; value: string; info?: string }) {
  return (
    <span className="inline-flex items-center gap-1">
      <span className="text-foreground font-medium tabular-nums">{value}</span> {label}
      {info && <InfoTip text={info} />}
    </span>
  )
}

function SortableHead({
  label,
  sortKey,
  sort,
  setSort,
  align = 'left',
  info,
}: {
  label: string
  sortKey: string
  sort: Sort
  setSort: (s: Sort) => void
  align?: 'left' | 'right'
  info?: string
}) {
  const active = sort.key === sortKey
  const arrow = active ? (sort.dir === 'asc' ? ' ↑' : ' ↓') : ''
  return (
    <TableHead className={align === 'right' ? 'text-right' : ''}>
      <span className="inline-flex items-center gap-1">
        {align === 'right' && info && <InfoTip text={info} />}
        <button
          className="hover:text-foreground transition-colors"
          onClick={() => setSort({ key: sortKey, dir: active && sort.dir === 'asc' ? 'desc' : 'asc' })}
        >
          {label}
          {arrow}
        </button>
        {align !== 'right' && info && <InfoTip text={info} />}
      </span>
    </TableHead>
  )
}

const PAGE_SIZE = 10

// Client-side pagination for a list table. Page 1 is the top of the (already
// sorted) list, so it doubles as a "top 10" view; Prev/Next reach the rest.
function usePaged<T>(rows: T[], pageSize = PAGE_SIZE) {
  const [page, setPage] = useState(0)
  useEffect(() => setPage(0), [rows])
  const total = rows.length
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  const clamped = Math.min(page, totalPages - 1)
  const pageRows = rows.slice(clamped * pageSize, clamped * pageSize + pageSize)
  return { page: clamped, setPage, pageRows, total, totalPages, pageSize }
}

function PageControls({
  page,
  setPage,
  total,
  totalPages,
  pageSize,
}: {
  page: number
  setPage: (n: number) => void
  total: number
  totalPages: number
  pageSize: number
}) {
  if (total <= pageSize) return null
  const from = page * pageSize + 1
  const to = Math.min((page + 1) * pageSize, total)
  return (
    <div className="mt-3 flex items-center justify-between text-xs text-muted-foreground">
      <span className="tabular-nums">
        {from}–{to} of {total}
      </span>
      <div className="inline-flex items-center gap-1">
        <button
          disabled={page === 0}
          onClick={() => setPage(page - 1)}
          className="rounded border border-border px-2 py-0.5 hover:bg-muted disabled:opacity-40"
        >
          Prev
        </button>
        <span className="px-1 tabular-nums">
          {page + 1}/{totalPages}
        </span>
        <button
          disabled={page >= totalPages - 1}
          onClick={() => setPage(page + 1)}
          className="rounded border border-border px-2 py-0.5 hover:bg-muted disabled:opacity-40"
        >
          Next
        </button>
      </div>
    </div>
  )
}

function Scroll({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">{children}</div>
    </div>
  )
}

function Footer({ window, generatedAt }: { window: { start: string; end: string }; generatedAt: string }) {
  return (
    <p className="text-xs text-muted-foreground mt-8">
      {window.start} to {window.end} UTC · generated {generatedAt}.
    </p>
  )
}

// --- helpers ---

function deltaOrUndef(deltas: NetworkHealthDeltas | undefined, key: keyof NetworkHealthDeltas): number | undefined {
  const v = deltas?.[key]
  return v === null || v === undefined ? undefined : v
}

// Percent change of a current value versus its prior-window value, for a "vs
// prior" delta on a count/duration metric. Mirrors the server pctDelta guards:
// returns undefined when the prior is missing OR zero, so we never divide by
// zero or show a delta against a period with no baseline.
function pctChange(cur: number | null | undefined, prior: number | null | undefined): number | undefined {
  if (cur == null || prior == null || prior === 0) return undefined
  return ((cur - prior) / prior) * 100
}

// Percentage-POINT delta (cur - prior) for rate metrics that are themselves
// percentages (self-reported share, drained-within-30m), where a percent change
// of a percent would be misleading. Undefined when either side is missing.
function ppDelta(cur: number | null | undefined, prior: number | null | undefined): number | undefined {
  if (cur == null || prior == null) return undefined
  return cur - prior
}

function minutes(v: number | null | undefined): string {
  if (v == null) return '—'
  if (v >= 120) return `${(v / 60).toFixed(1)} h`
  return `${v} min`
}

// Undrain timing label that always states a reason instead of a bare dash:
// "unavailable" when the recovery signal could not be read, "no undrains" when
// nothing was returned to service, "no matched undrains" when undrains happened
// but none paired with a recovered link, otherwise the minutes value.
// Undrain-availability comes from the deferred query, but the undrains count is
// already known from the main drain-timing payload, so it is passed in.
function deferredUndrainValue(undrains: number, d: NetworkHealthDeferred, v: number | null | undefined): string {
  if (d.undrain_unavailable) return 'unavailable'
  if (undrains === 0) return 'no undrains'
  if (d.matched_undrains === 0) return 'no matched undrains'
  return minutes(v)
}

// Ops-management "median time to file" is server-rounded to whole minutes, so
// a near-instant ticket rounds down to 0 and a bare "0 min" reads as broken
// data rather than "filed almost immediately". Show "<1 min" instead of "0 min".
function timeToFileLabel(v: number | null | undefined): string {
  if (v == null) return '—'
  if (v === 0) return '<1 min'
  return minutes(v)
}

function getLastMonth(): { start: string; end: string } {
  const now = new Date()
  const firstThis = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1))
  const firstPrev = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1))
  return { start: firstPrev.toISOString().slice(0, 10), end: firstThis.toISOString().slice(0, 10) }
}

function sortRows<T>(rows: T[], sort: Sort): T[] {
  return [...rows].sort((a, b) => {
    const av = (a as Record<string, unknown>)[sort.key]
    const bv = (b as Record<string, unknown>)[sort.key]
    let c: number
    if (typeof av === 'string' || typeof bv === 'string') {
      c = String(av).localeCompare(String(bv))
    } else {
      c = (av as number) - (bv as number)
    }
    return sort.dir === 'asc' ? c : -c
  })
}
