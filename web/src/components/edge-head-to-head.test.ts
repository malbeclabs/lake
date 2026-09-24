import { describe, expect, it } from 'vitest'

import type { EdgeScoreboardNode } from '@/lib/api'
import { commercialFeedKeys, edgeWinRateVsTurbine } from './edge-head-to-head'

function node(host: string, rates: Record<string, number>): EdgeScoreboardNode {
  const feeds: EdgeScoreboardNode['feeds'] = {}
  for (const [feed, pct] of Object.entries(rates)) {
    feeds[feed] = { shreds_won: 0, total_shreds: 0, win_rate_pct: pct, lead_times: [] }
  }
  return {
    host,
    location: '',
    metro_name: '',
    latitude: 0,
    longitude: 0,
    feeds,
    stake_sol: 0,
    validators: 0,
    total_slots: 0,
    slots_observed: 0,
    dz_leader_slots: 0,
    last_updated: '',
  }
}

describe('edgeWinRateVsTurbine', () => {
  it('reports the contested share, not the share of every shred', () => {
    // Edge is 60% of all shreds, but took three of every four it contested.
    expect(edgeWinRateVsTurbine([node('a', { dz_edge: 60, turbine: 20, jito: 20 })])).toBeCloseTo(75, 6)
  })

  it('never counts a DoubleZero sub-feed as an opponent', () => {
    // Counting them again would race Edge against itself.
    const h = edgeWinRateVsTurbine([
      node('a', { dz_edge: 80, dz: 50, dz_root: 20, dz_retransmit: 10, turbine: 20 }),
    ])
    expect(h).toBeCloseTo(80, 6)
  })

  it('averages per node rather than pooling, so a quiet vantage still counts once', () => {
    const h = edgeWinRateVsTurbine([
      node('a', { dz_edge: 90, turbine: 10 }),
      node('b', { dz_edge: 50, turbine: 50 }),
    ])
    expect(h).toBeCloseTo(70, 6)
  })

  it('skips a node that never saw Turbine', () => {
    const h = edgeWinRateVsTurbine([
      node('a', { dz_edge: 50, turbine: 50 }),
      node('b', { dz_edge: 90 }),
    ])
    expect(h).toBeCloseTo(50, 6)
  })

  it('does not score a win against an opponent it never measured', () => {
    // Dividing by Edge's own share here records 100% for a matchup nobody saw.
    // This is the live shape: every node carries dz_* and turbine and nothing else.
    expect(edgeWinRateVsTurbine([node('a', { dz_edge: 90 })])).toBeNull()
  })

  it('counts a measured opponent that won nothing as a real shutout', () => {
    expect(edgeWinRateVsTurbine([node('a', { dz_edge: 90, turbine: 0 })])).toBe(100)
  })

  it('reports null rather than zero when nothing was contested', () => {
    expect(edgeWinRateVsTurbine([])).toBeNull()
    expect(edgeWinRateVsTurbine([node('a', { dz_edge: 0, turbine: 0 })])).toBeNull()
  })

  it('reports a shutout in either direction', () => {
    expect(edgeWinRateVsTurbine([node('a', { dz_edge: 100, turbine: 0 })])).toBe(100)
    expect(edgeWinRateVsTurbine([node('a', { dz_edge: 0, turbine: 100 })])).toBe(0)
  })
})

describe('commercialFeedKeys', () => {
  it('is empty for the live payload shape, which carries no commercial feed', () => {
    expect(commercialFeedKeys([node('a', { dz: 1, dz_edge: 99, dz_root: 1, dz_retransmit: 1, turbine: 1 })]))
      .toEqual([])
  })

  it('names them across nodes when the payload does carry them', () => {
    expect(
      commercialFeedKeys([
        node('a', { dz_edge: 90, jito: 5, turbine: 5 }),
        node('b', { dz_edge: 90, pipe: 10 }),
      ])
    ).toEqual(['jito', 'pipe'])
  })
})
