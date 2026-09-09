import type { EdgeScoreboardNode } from '@/lib/api'

// `dz_edge` is the server-computed aggregate of the other three on a shared
// per-host denominator, so it is Edge's figure and none of them is an opponent.
const DZ_FEEDS = new Set(['dz_edge', 'dz_root', 'dz', 'dz_retransmit'])

// The public fallback, which nobody buys — not one of the commercial feeds.
const PUBLIC_FEED = 'turbine'

export type EdgeHeadToHead = {
  vsCommercial: number | null
  vsTurbine: number | null
}

/**
 * Head-to-head win rate: of the shreds DoubleZero Edge and one opponent
 * contested, the share Edge arrived with first. Deliberately not Edge's share of
 * *all* shreds, which is a win rate against nobody in particular and moves when
 * an unrelated feed appears or goes quiet. Averaged per recording node and
 * unweighted, the way the node table reads them.
 *
 * A node only votes on a matchup it measured, and the test is whether the
 * opponent has a row there, NOT whether the denominator is non-zero: a node that
 * never saw Turbine would otherwise divide by Edge's own share and record a
 * perfect 100%, so every recorder missing a feed would inflate the headline that
 * names it. A measured opponent that won nothing is a real shutout and counts.
 */
export function edgeHeadToHead(nodes: EdgeScoreboardNode[]): EdgeHeadToHead {
  let commercialSum = 0
  let commercialNodes = 0
  let turbineSum = 0
  let turbineNodes = 0

  for (const node of nodes) {
    const edge = node.feeds['dz_edge']?.win_rate_pct ?? 0
    const turbineStats = node.feeds[PUBLIC_FEED]

    let commercial = 0
    let sawCommercial = false
    for (const [feed, stats] of Object.entries(node.feeds)) {
      if (DZ_FEEDS.has(feed) || feed === PUBLIC_FEED) continue
      sawCommercial = true
      commercial += stats.win_rate_pct
    }

    if (sawCommercial && edge + commercial > 0) {
      commercialSum += (edge / (edge + commercial)) * 100
      commercialNodes++
    }
    if (turbineStats && edge + turbineStats.win_rate_pct > 0) {
      turbineSum += (edge / (edge + turbineStats.win_rate_pct)) * 100
      turbineNodes++
    }
  }

  return {
    vsCommercial: commercialNodes > 0 ? commercialSum / commercialNodes : null,
    vsTurbine: turbineNodes > 0 ? turbineSum / turbineNodes : null,
  }
}
