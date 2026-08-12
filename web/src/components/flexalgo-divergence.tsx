import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, CheckCircle2, Loader2 } from 'lucide-react'
import { fetchAlgoDivergence } from '@/lib/api'
import type { AlgoDivergenceResponse } from '@/lib/api'
import { divergenceHeadline, excludedForText } from './flexalgo-divergence-text'

/**
 * What the unicast topology costs against algo 0.
 *
 * Unicast forwarding uses only the links tagged into a topology and not
 * drained. Multicast uses every activated link. So each link outside the
 * unicast set is one multicast can use and unicast cannot, and every metro
 * pair whose best path crossed that link now has two different latencies
 * depending on the traffic type.
 *
 * The links are the cause and the pairs are the cost. A link turned up
 * without a tag looks like nothing on any other page until you see how far
 * it moved the routes underneath it.
 */
export function FlexAlgoDivergence() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['algo-divergence'],
    queryFn: fetchAlgoDivergence,
    staleTime: 60_000,
  })

  if (isLoading) {
    return (
      <Section>
        <div className="flex items-center gap-2 px-4 py-6 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Comparing unicast and multicast paths
        </div>
      </Section>
    )
  }

  // A failed comparison must not read as a clean network. It says nothing.
  // The summary check also covers a page-cache payload written before this
  // shape existed, which the worker fills from a separate process.
  if (error || !data?.summary) {
    return (
      <Section>
        <div className="px-4 py-6 text-sm text-muted-foreground">
          Could not compare unicast and multicast paths.
        </div>
      </Section>
    )
  }

  return (
    <Section>
      <Headline data={data} />
      {(data.excludedLinks ?? []).length > 0 && (
        <>
          <ExcludedLinksTable data={data} />
          <PairsTable data={data} />
        </>
      )}
    </Section>
  )
}

function Section({ children }: { children: React.ReactNode }) {
  return (
    <div className="mt-10">
      <h2 className="text-lg font-semibold mb-1">Unicast vs multicast</h2>
      <p className="text-sm text-muted-foreground mb-4">
        Unicast follows the flex-algo topology. Multicast follows algo 0 and uses every
        activated link. Where the two link sets differ, so does the latency.
      </p>
      <div className="border border-border rounded-lg overflow-hidden">{children}</div>
    </div>
  )
}

function Headline({ data }: { data: AlgoDivergenceResponse }) {
  const clean = data.summary.excludedLinks === 0
  return (
    <div className="flex items-start gap-2.5 px-4 py-3 bg-muted/30 border-b border-border last:border-b-0">
      {clean ? (
        <CheckCircle2 className="h-4 w-4 mt-0.5 shrink-0 text-green-600 dark:text-green-400" />
      ) : (
        <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0 text-amber-500" />
      )}
      <div className="text-sm">{divergenceHeadline(data.summary)}</div>
    </div>
  )
}

function ExcludedLinksTable({ data }: { data: AlgoDivergenceResponse }) {
  return (
    <div className="overflow-x-auto border-b border-border">
      <table className="w-full min-w-[640px]">
        <caption className="px-4 pt-3 pb-2 text-left text-sm font-medium">
          Links outside the unicast topology
        </caption>
        <thead>
          <tr className="text-left text-sm text-muted-foreground border-y border-border bg-muted/30">
            <th className="px-4 py-2.5 font-medium">Link</th>
            <th className="px-4 py-2.5 font-medium">Route</th>
            <th className="px-4 py-2.5 font-medium text-right">Contracted RTT</th>
            <th className="px-4 py-2.5 font-medium">Reason</th>
            <th className="px-4 py-2.5 font-medium">Out for</th>
          </tr>
        </thead>
        <tbody>
          {(data.excludedLinks ?? []).map((l) => (
            <tr key={l.code} className="border-b border-border last:border-b-0">
              <td className="px-4 py-2.5 text-sm font-mono">{l.code}</td>
              <td className="px-4 py-2.5 text-sm text-muted-foreground">
                {l.fromMetro} — {l.toMetro}
              </td>
              <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                {l.rttMs.toFixed(2)} ms
              </td>
              <td className="px-4 py-2.5 text-sm">
                {l.drained ? 'drained' : 'no topology tag'}
                {!l.everIncluded && (
                  <span className="text-muted-foreground"> · never in the topology</span>
                )}
              </td>
              <td
                className="px-4 py-2.5 text-sm tabular-nums"
                title={
                  l.everIncluded
                    ? l.excludedAt
                    : `${l.excludedAt} is the oldest snapshot held, not the moment the link left the topology`
                }
              >
                {excludedForText(l)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PairsTable({ data }: { data: AlgoDivergenceResponse }) {
  if ((data.pairs ?? []).length === 0) {
    return (
      <div className="px-4 py-6 text-sm text-muted-foreground">
        No metro pair changes its best path, so the excluded links cost nothing today.
      </div>
    )
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px]">
        <caption className="px-4 pt-3 pb-2 text-left text-sm font-medium">
          Metro pairs where unicast is slower
        </caption>
        <thead>
          <tr className="text-left text-sm text-muted-foreground border-y border-border bg-muted/30">
            <th className="px-4 py-2.5 font-medium">Pair</th>
            <th className="px-4 py-2.5 font-medium text-right">Multicast</th>
            <th className="px-4 py-2.5 font-medium text-right">Unicast</th>
            <th className="px-4 py-2.5 font-medium text-right">Unicast pays</th>
            <th className="px-4 py-2.5 font-medium">Unicast path</th>
          </tr>
        </thead>
        <tbody>
          {(data.pairs ?? []).map((p) => (
            <tr key={`${p.fromMetro}|${p.toMetro}`} className="border-b border-border last:border-b-0">
              <td className="px-4 py-2.5 text-sm">
                {p.fromMetro} — {p.toMetro}
              </td>
              <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                {p.multicastMs.toFixed(2)} ms
              </td>
              {p.unicastReachable ? (
                <>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                    {p.unicastMs.toFixed(2)} ms
                  </td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right text-amber-600 dark:text-amber-400">
                    +{p.deltaMs.toFixed(2)} ms ({p.deltaPct.toFixed(1)}%)
                  </td>
                  <td className="px-4 py-2.5 text-sm text-muted-foreground">
                    {p.unicastPath.join(' — ')}
                  </td>
                </>
              ) : (
                <td
                  colSpan={3}
                  className="px-4 py-2.5 text-sm text-red-600 dark:text-red-400"
                >
                  No unicast path at all
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
