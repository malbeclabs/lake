import type { EdgeMulticastChannelInstance, GapEpisode } from '@/lib/api'

/**
 * Union of the gap episodes across one publisher line's channel instances.
 *
 * A publisher line can carry MANY instances — the sports market-by-price publisher carries one per
 * league, about thirty — so a strip per instance would be thirty strips. The union answers the
 * question the line is actually asked instead: was this path losing at this second, on anything.
 *
 * Instances with `gaps_measured` false contribute nothing, and must not: they come from a plane with
 * no gap marker, so their absence of episodes is an absence of measurement rather than a clean run.
 */
export function mergeGapEpisodes(instances: EdgeMulticastChannelInstance[]): GapEpisode[] {
  const sorted = instances
    .filter((i) => i.gaps_measured)
    .flatMap((i) => i.gap_episodes ?? [])
    .slice()
    .sort((a, b) => a.start - b.start)

  const out: GapEpisode[] = []
  for (const e of sorted) {
    const last = out[out.length - 1]
    // Touching counts as contiguous, not just overlapping: two runs that meet at a second
    // boundary are one outage, and drawing them as two would inflate the episode count.
    if (last && e.start <= last.start + last.seconds) {
      last.seconds = Math.max(last.start + last.seconds, e.start + e.seconds) - last.start
      continue
    }
    out.push({ ...e })
  }
  return out
}
