import { useMemo, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock3,
  RefreshCw,
  WifiOff,
} from 'lucide-react'

import { PageHeader } from './page-header'
import { Button } from '@/components/ui/button'
import { useEnv } from '@/contexts/EnvContext'
import {
  fetchNetworkState,
  type BGPStateSummary,
  type InterfaceFamilySummary,
  type ISISStateSummary,
  type NetworkStateResponse,
  type TelemetryFreshness,
} from '@/lib/api'
import { cn } from '@/lib/utils'

function formatCount(value: number | undefined): string {
  return new Intl.NumberFormat().format(value ?? 0)
}

function formatCompact(value: number | undefined): string {
  const n = value ?? 0
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return formatCount(n)
}

function formatSeconds(seconds: number | undefined): string {
  if (seconds === undefined || seconds < 0) return 'No rows'
  if (seconds < 60) return `${seconds}s stale`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m stale`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h stale`
  return `${Math.floor(seconds / 86400)}d stale`
}

function formatTimeAgo(isoString: string | undefined): string {
  if (!isoString) return 'No samples'
  const date = new Date(isoString)
  const diffSecs = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000))
  if (diffSecs < 60) return `${diffSecs}s ago`
  if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`
  if (diffSecs < 86400) return `${Math.floor(diffSecs / 3600)}h ago`
  return `${Math.floor(diffSecs / 86400)}d ago`
}

function tableLabel(table: string): string {
  return table.replaceAll('_', ' ')
}

function gapLabel(gap: string): string {
  const labels: Record<string, string> = {
    raw_telemetry_states_not_incidents: 'Raw telemetry states, not incident status',
    system_state_stdout_only: 'System state is still stdout-only',
    telemetry_empty: 'No telemetry rows in this environment',
    mainnet_beta_telemetry_pilot_not_flowing: 'Mainnet-beta gNMI pilot is not flowing yet',
    isis_overload_bit_empty: 'ISIS overload-bit telemetry is empty',
  }
  return labels[gap] ?? gap.replaceAll('_', ' ')
}

function stateBadgeClass(state: string): string {
  const normalized = state.toUpperCase()
  if (normalized === 'UP' || normalized === 'ESTABLISHED') {
    return 'bg-green-500/10 text-green-700 dark:text-green-400 border-green-500/20'
  }
  if (normalized === 'ACTIVE' || normalized === 'CONNECT' || normalized === 'UNKNOWN') {
    return 'bg-amber-500/10 text-amber-700 dark:text-amber-400 border-amber-500/20'
  }
  if (normalized === 'DOWN' || normalized === 'IDLE') {
    return 'bg-red-500/10 text-red-700 dark:text-red-400 border-red-500/20'
  }
  return 'bg-muted text-muted-foreground border-border'
}

function OverviewMetric({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="px-4 py-3 sm:px-5 sm:py-4">
      <div className="text-xs text-muted-foreground uppercase tracking-wide">{label}</div>
      <div className="mt-1 text-2xl font-medium tabular-nums">{value}</div>
      {detail && <div className="mt-1 text-xs text-muted-foreground">{detail}</div>}
    </div>
  )
}

function Section({
  title,
  description,
  children,
}: {
  title: string
  description?: string
  children: ReactNode
}) {
  return (
    <section className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-4 py-3 border-b border-border">
        <h2 className="text-sm font-medium">{title}</h2>
        {description && <p className="mt-1 text-xs text-muted-foreground">{description}</p>}
      </div>
      {children}
    </section>
  )
}

function EmptyRows({ message }: { message: string }) {
  return (
    <div className="px-4 py-8 text-center text-sm text-muted-foreground">
      {message}
    </div>
  )
}

function FreshnessTable({ rows }: { rows: TelemetryFreshness[] }) {
  return (
    <Section title="Telemetry freshness" description="Raw table volume and latest sample time from normalized gNMI tables.">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-3 font-medium">Table</th>
              <th className="px-4 py-3 font-medium text-right">Rows</th>
              <th className="px-4 py-3 font-medium text-right">Devices</th>
              <th className="px-4 py-3 font-medium">Last seen</th>
              <th className="px-4 py-3 font-medium">Age</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(row => (
              <tr key={row.table} className="border-b border-border/60 last:border-0">
                <td className="px-4 py-3 font-mono text-xs">{tableLabel(row.table)}</td>
                <td className="px-4 py-3 text-right tabular-nums">{formatCount(row.rows)}</td>
                <td className="px-4 py-3 text-right tabular-nums">{formatCount(row.devices)}</td>
                <td className="px-4 py-3 text-muted-foreground whitespace-nowrap">{formatTimeAgo(row.last_seen)}</td>
                <td className="px-4 py-3 text-muted-foreground whitespace-nowrap">{formatSeconds(row.seconds_stale)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Section>
  )
}

function InterfaceTable({ families }: { families: InterfaceFamilySummary[] }) {
  return (
    <Section title="Interfaces" description="Latest interface snapshots grouped by family.">
      {families.length === 0 ? (
        <EmptyRows message="No latest interface telemetry found." />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">Family</th>
                <th className="px-4 py-3 font-medium text-right">Interfaces</th>
                <th className="px-4 py-3 font-medium text-right">Devices</th>
                <th className="px-4 py-3 font-medium text-right">Admin up</th>
                <th className="px-4 py-3 font-medium text-right">Oper up</th>
                <th className="px-4 py-3 font-medium text-right">Oper down</th>
              </tr>
            </thead>
            <tbody>
              {families.map(family => (
                <tr key={family.family} className="border-b border-border/60 last:border-0">
                  <td className="px-4 py-3 capitalize">{family.family.replaceAll('_', ' ')}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(family.interfaces)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(family.devices)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(family.admin_up)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(family.oper_up)}</td>
                  <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">{formatCount(family.oper_down)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  )
}

function BGPTable({ states }: { states: BGPStateSummary[] }) {
  return (
    <Section title="BGP neighbors" description="Latest raw OpenConfig session states.">
      {states.length === 0 ? (
        <EmptyRows message="No latest BGP neighbor telemetry found." />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">State</th>
                <th className="px-4 py-3 font-medium text-right">Neighbors</th>
                <th className="px-4 py-3 font-medium text-right">Devices</th>
                <th className="px-4 py-3 font-medium text-right">Internal</th>
                <th className="px-4 py-3 font-medium text-right">External</th>
              </tr>
            </thead>
            <tbody>
              {states.map(state => (
                <tr key={state.state} className="border-b border-border/60 last:border-0">
                  <td className="px-4 py-3">
                    <span className={cn('inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium', stateBadgeClass(state.state))}>
                      {state.state}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.neighbors)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.devices)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.internal_neighbors)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.external_neighbors)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  )
}

function ISISTable({ states }: { states: ISISStateSummary[] }) {
  return (
    <Section title="ISIS adjacencies" description="Latest raw adjacency states by device and system ID.">
      {states.length === 0 ? (
        <EmptyRows message="No latest ISIS adjacency telemetry found." />
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">State</th>
                <th className="px-4 py-3 font-medium text-right">Adjacencies</th>
                <th className="px-4 py-3 font-medium text-right">Devices</th>
                <th className="px-4 py-3 font-medium text-right">Systems</th>
              </tr>
            </thead>
            <tbody>
              {states.map(state => (
                <tr key={state.state} className="border-b border-border/60 last:border-0">
                  <td className="px-4 py-3">
                    <span className={cn('inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium', stateBadgeClass(state.state))}>
                      {state.state}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.adjacencies)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.devices)}</td>
                  <td className="px-4 py-3 text-right tabular-nums">{formatCount(state.systems)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  )
}

function OpticsPanel({ data }: { data: NetworkStateResponse['optics'] }) {
  const items = [
    ['Lanes', data.lanes],
    ['Devices', data.devices],
    ['Interfaces', data.interfaces],
    ['Threshold rows', data.threshold_rows],
    ['Devices with thresholds', data.devices_with_thresholds],
    ['Interfaces with thresholds', data.interfaces_with_thresholds],
  ] as const

  return (
    <Section title="Optics" description="Latest transceiver lane and threshold volume.">
      <div className="grid grid-cols-2 gap-px bg-border">
        {items.map(([label, value]) => (
          <div key={label} className="bg-card px-4 py-3">
            <div className="text-xs text-muted-foreground">{label}</div>
            <div className="mt-1 text-lg font-medium tabular-nums">{formatCount(value)}</div>
          </div>
        ))}
      </div>
    </Section>
  )
}

function KnownGaps({ env, gaps }: { env: string; gaps: string[] }) {
  const mainnetPilot = gaps.includes('mainnet_beta_telemetry_pilot_not_flowing')

  return (
    <section className="rounded-lg border border-border bg-card p-4">
      <div className="flex items-start gap-3">
        <div className="mt-0.5 rounded-full bg-muted p-1.5 text-muted-foreground">
          {mainnetPilot ? <WifiOff className="h-4 w-4" /> : <AlertCircle className="h-4 w-4" />}
        </div>
        <div className="min-w-0">
          <h2 className="text-sm font-medium">Known gaps</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {mainnetPilot
              ? `No gNMI telemetry is flowing for ${env} yet. The endpoint is healthy, but the pilot is not currently producing rows.`
              : 'These are expected product and collection gaps. They are not incident status.'}
          </p>
          {gaps.length > 0 && (
            <ul className="mt-3 flex flex-wrap gap-2">
              {gaps.map(gap => (
                <li key={gap} className="rounded-full border border-border bg-background px-2.5 py-1 text-xs text-muted-foreground">
                  {gapLabel(gap)}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </section>
  )
}

function NetworkStateSkeleton() {
  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1600px] mx-auto px-4 sm:px-8 py-8">
        <div className="h-9 w-56 animate-pulse rounded bg-muted mb-6" />
        <div className="h-28 animate-pulse rounded-lg bg-muted mb-6" />
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <div className="h-80 animate-pulse rounded-lg bg-muted" />
          <div className="h-80 animate-pulse rounded-lg bg-muted" />
          <div className="h-64 animate-pulse rounded-lg bg-muted" />
          <div className="h-64 animate-pulse rounded-lg bg-muted" />
        </div>
      </div>
    </div>
  )
}

export function NetworkStatePage() {
  const { env } = useEnv()
  const { data, isLoading, isFetching, error, refetch } = useQuery({
    queryKey: ['network-state', env],
    queryFn: fetchNetworkState,
    refetchInterval: 60_000,
    staleTime: 30_000,
  })

  const summary = useMemo(() => {
    const freshness = data?.freshness ?? []
    const families = data?.interfaces.families ?? []
    const bgpStates = data?.bgp.states ?? []
    const isisStates = data?.isis.states ?? []
    return {
      totalRows: freshness.reduce((sum, row) => sum + row.rows, 0),
      telemetryDevices: Math.max(0, ...freshness.map(row => row.devices)),
      interfaces: families.reduce((sum, row) => sum + row.interfaces, 0),
      bgpNeighbors: bgpStates.reduce((sum, row) => sum + row.neighbors, 0),
      isisAdjacencies: isisStates.reduce((sum, row) => sum + row.adjacencies, 0),
      opticsLanes: data?.optics.lanes ?? 0,
      latestSeen: freshness
        .filter(row => row.last_seen)
        .sort((a, b) => new Date(b.last_seen!).getTime() - new Date(a.last_seen!).getTime())[0]?.last_seen,
    }
  }, [data])

  if (isLoading) return <NetworkStateSkeleton />

  if (error || !data) {
    return (
      <div className="flex-1 flex items-center justify-center px-4">
        <div className="max-w-md text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load network state</div>
          <div className="text-sm text-muted-foreground mb-4">
            {error instanceof Error ? error.message : 'Unknown error'}
          </div>
          <Button variant="outline" onClick={() => refetch()}>
            <RefreshCw className="h-4 w-4 mr-2" />
            Try again
          </Button>
        </div>
      </div>
    )
  }

  const telemetryEmpty = data.known_gaps.includes('telemetry_empty')

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1600px] mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Activity}
          title="Network State"
          subtitle={
            <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
              <span>{data.env}</span>
              <span aria-hidden="true">·</span>
              <Clock3 className="h-3.5 w-3.5" />
              <span>fetched {formatTimeAgo(data.fetched_at)}</span>
            </span>
          }
          actions={
            <>
              {data.cache_status && (
                <span className="inline-flex items-center rounded-full border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground">
                  Cache {data.cache_status.toLowerCase()}
                </span>
              )}
              <Button variant="outline" size="sm" onClick={() => refetch()} disabled={isFetching}>
                <RefreshCw className={cn('h-3.5 w-3.5 mr-2', isFetching && 'animate-spin')} />
                Refresh
              </Button>
            </>
          }
        />

        {telemetryEmpty && (
          <div className="mb-6 rounded-lg border border-border bg-card p-4">
            <div className="flex items-start gap-3">
              <div className="mt-0.5 rounded-full bg-muted p-1.5 text-muted-foreground">
                <WifiOff className="h-4 w-4" />
              </div>
              <div>
                <div className="text-sm font-medium">No telemetry rows yet</div>
                <p className="mt-1 text-sm text-muted-foreground">
                  The telemetry schema is reachable for {data.env}, but no normalized gNMI rows are present.
                </p>
              </div>
            </div>
          </div>
        )}

        <div className="mb-6 rounded-lg border border-border bg-card overflow-hidden">
          <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 divide-x divide-y md:divide-y-0 divide-border">
            <OverviewMetric label="Telemetry devices" value={formatCount(summary.telemetryDevices)} detail={`${formatCompact(summary.totalRows)} rows`} />
            <OverviewMetric label="Interfaces" value={formatCount(summary.interfaces)} detail={`${formatCount(data.interfaces.families.length)} families`} />
            <OverviewMetric label="BGP neighbors" value={formatCount(summary.bgpNeighbors)} detail={`${formatCount(data.bgp.states.length)} states`} />
            <OverviewMetric label="ISIS adjacencies" value={formatCount(summary.isisAdjacencies)} detail={`${formatCount(data.isis.states.length)} states`} />
            <OverviewMetric label="Optics lanes" value={formatCount(summary.opticsLanes)} detail={`${formatCount(data.optics.interfaces)} interfaces`} />
            <OverviewMetric label="Latest sample" value={formatTimeAgo(summary.latestSeen)} detail="Across telemetry tables" />
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <div className="xl:col-span-2">
            <FreshnessTable rows={data.freshness} />
          </div>
          <InterfaceTable families={data.interfaces.families} />
          <div className="space-y-6">
            <BGPTable states={data.bgp.states} />
            <ISISTable states={data.isis.states} />
          </div>
          <OpticsPanel data={data.optics} />
          <KnownGaps env={data.env} gaps={data.known_gaps} />
        </div>

        <div className="mt-6 flex items-center gap-2 text-xs text-muted-foreground">
          <CheckCircle2 className="h-3.5 w-3.5" />
          <span>Raw gNMI observations only. Health and incident decisions live on the Status pages.</span>
        </div>
      </div>
    </div>
  )
}
