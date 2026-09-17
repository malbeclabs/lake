import type { EdgeScoreboardNode } from '@/lib/api'

// `dz_edge` is the server-computed aggregate of the other three on a shared
// per-host denominator, so it is Edge's figure and none of them is an opponent.
const DZ_FEEDS = new Set(['dz_edge', 'dz_root', 'dz', 'dz_retransmit'])

const TURBINE = 'turbine'

/**
 * Head-to-head win rate against Turbine: of the shreds Edge and Turbine
 * contested, the share Edge arrived with first. Not Edge's share of *all*
 * shreds, which moves when an unrelated feed appears or goes quiet. Averaged per
 * recording node and unweighted, the way the node table reads them.
 *
 * A node only votes on a matchup it measured, and the test is whether Turbine
 * has a row there, NOT whether the denominator is non-zero: a node that never
 * saw Turbine would otherwise divide by Edge's own share and record a perfect
 * 100%, so every recorder missing the feed would inflate the headline. Turbine
 * measured and winning nothing is a real shutout and counts.
 *
 * There is deliberately no commercial-feed twin of this. The scoreboard payload
 * has carried no commercial feed since Jito was retired, so the only measurement
 * of that matchup is the daily rollup behind /api/dz/shreds/competitors — a
 * different window and grain. A second definition here would quietly disagree
 * with the tile and the chart that both read the rollup.
 */
export function edgeWinRateVsTurbine(nodes: EdgeScoreboardNode[]): number | null {
  let sum = 0
  let counted = 0

  for (const node of nodes) {
    const edge = node.feeds['dz_edge']?.win_rate_pct ?? 0
    const turbine = node.feeds[TURBINE]
    if (!turbine) continue
    if (edge + turbine.win_rate_pct <= 0) continue
    sum += (edge / (edge + turbine.win_rate_pct)) * 100
    counted++
  }

  return counted > 0 ? sum / counted : null
}

/** Feeds that are neither ours nor Turbine, if the payload ever carries any. */
export function commercialFeedKeys(nodes: EdgeScoreboardNode[]): string[] {
  const keys = new Set<string>()
  for (const node of nodes) {
    for (const feed of Object.keys(node.feeds)) {
      if (DZ_FEEDS.has(feed) || feed === TURBINE) continue
      keys.add(feed)
    }
  }
  return [...keys].sort()
}
