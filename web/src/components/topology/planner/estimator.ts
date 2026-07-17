import { haversineKm } from './geo'
import type { TopologyResponse } from '@/lib/api'

const ROUTE_FACTOR = 1.4
const RTT_NS_PER_KM = 10_000 // ~10us/km round trip

// Mirrors api unsetLatencyNs (api/handlers/planner_graph.go): a committed_rtt_ns of
// exactly 1e9 means "latency not set / provisioning", never a real measurement.
export const UNSET_LATENCY_NS = 1_000_000_000

export function greatCircleLatencyNs(km: number): number {
  return Math.round(km * ROUTE_FACTOR * RTT_NS_PER_KM)
}

// Measured RTT (ns) of an existing link whose endpoints sit in the two metros.
// The 1e9 unset sentinel and non-positive values are never returned as a
// comparable, so a copied estimate can never emit the forbidden sentinel.
export function findComparableRttNs(
  topology: TopologyResponse,
  metroAPk: string,
  metroBPk: string
): number | null {
  const metroOf = new Map(topology.devices.map((d) => [d.pk, d.metro_pk]))
  for (const link of topology.links) {
    const ma = metroOf.get(link.side_a_pk)
    const mb = metroOf.get(link.side_z_pk)
    if (!ma || !mb) continue
    const match =
      (ma === metroAPk && mb === metroBPk) || (ma === metroBPk && mb === metroAPk)
    if (!match) continue
    const committed = link.committed_rtt_ns
    let rtt = 0
    if (committed > 0 && committed !== UNSET_LATENCY_NS) {
      rtt = committed
    } else if (link.latency_us > 0) {
      rtt = link.latency_us * 1000
    }
    if (rtt > 0 && rtt !== UNSET_LATENCY_NS) return rtt
  }
  return null
}

export function estimateLatencyNs(input: {
  topology: TopologyResponse
  metroAPk: string
  metroBPk: string
  manualNs?: number
}): { latencyNs: number; source: 'copied' | 'great_circle' | 'manual' } {
  const { topology, metroAPk, metroBPk, manualNs } = input
  if (manualNs && manualNs > 0) return { latencyNs: manualNs, source: 'manual' }

  const copied = findComparableRttNs(topology, metroAPk, metroBPk)
  if (copied) return { latencyNs: copied, source: 'copied' }

  const a = topology.metros.find((m) => m.pk === metroAPk)
  const b = topology.metros.find((m) => m.pk === metroBPk)
  if (a && b) {
    const km = haversineKm(a.latitude, a.longitude, b.latitude, b.longitude)
    return { latencyNs: greatCircleLatencyNs(km), source: 'great_circle' }
  }
  // Unresolvable input: one or both metro pks are not in the topology. Surface it
  // rather than emitting a 0 latency mislabelled as a manual override, which would
  // be dropped by the downstream impact engine.
  throw new Error(
    `estimateLatencyNs: unknown metro pk (metroAPk=${metroAPk}, metroBPk=${metroBPk})`
  )
}
