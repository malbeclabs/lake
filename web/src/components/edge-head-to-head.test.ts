import { describe, expect, it } from 'vitest'

import type { EdgeScoreboardNode } from '@/lib/api'
import { edgeHeadToHead } from './edge-head-to-head'

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

describe('edgeHeadToHead', () => {
  it('reports the contested share, not the share of every shred', () => {
    // Edge is 60% of all shreds, but took three of every four it contested.
    const h = edgeHeadToHead([node('a', { dz_edge: 60, turbine: 20, jito: 20 })])
    expect(h.vsTurbine).toBeCloseTo(75, 6)
    expect(h.vsCommercial).toBeCloseTo(75, 6)
  })

  it('races every paid feed together, and Turbine alone', () => {
    const h = edgeHeadToHead([node('a', { dz_edge: 60, jito: 15, pipe: 5, turbine: 20 })])
    expect(h.vsCommercial).toBeCloseTo(75, 6)
    expect(h.vsTurbine).toBeCloseTo(75, 6)
  })

  it('never counts a DoubleZero sub-feed as an opponent', () => {
    // Counting them again would race Edge against itself.
    const withSubs = edgeHeadToHead([
      node('a', { dz_edge: 80, dz: 50, dz_root: 20, dz_retransmit: 10, turbine: 20 }),
    ])
    expect(withSubs.vsTurbine).toBeCloseTo(80, 6)
    expect(withSubs.vsCommercial).toBeNull()
  })

  it('averages per node rather than pooling, so a quiet vantage still counts once', () => {
    const h = edgeHeadToHead([
      node('a', { dz_edge: 90, turbine: 10 }),
      node('b', { dz_edge: 50, turbine: 50 }),
    ])
    expect(h.vsTurbine).toBeCloseTo(70, 6)
  })

  it('skips a node for a matchup it has no data for', () => {
    // Node b never saw Turbine, but still has an opinion about Jito.
    const h = edgeHeadToHead([
      node('a', { dz_edge: 50, turbine: 50, jito: 50 }),
      node('b', { dz_edge: 90, jito: 10 }),
    ])
    expect(h.vsTurbine).toBeCloseTo(50, 6)
    expect(h.vsCommercial).toBeCloseTo(70, 6)
  })

  it('reports null rather than zero when nothing was contested', () => {
    expect(edgeHeadToHead([])).toEqual({ vsCommercial: null, vsTurbine: null })
    expect(edgeHeadToHead([node('a', { dz_edge: 0, turbine: 0 })])).toEqual({
      vsCommercial: null,
      vsTurbine: null,
    })
  })

  it('does not score a win against an opponent it never measured', () => {
    // Dividing by Edge's own share here records 100% for a matchup nobody saw.
    expect(edgeHeadToHead([node('a', { dz_edge: 90 })])).toEqual({
      vsCommercial: null,
      vsTurbine: null,
    })
  })

  it('counts a measured opponent that won nothing as a real shutout', () => {
    const h = edgeHeadToHead([node('a', { dz_edge: 90, turbine: 0, jito: 0 })])
    expect(h.vsTurbine).toBe(100)
    expect(h.vsCommercial).toBe(100)
  })

  it('reports a shutout in either direction', () => {
    expect(edgeHeadToHead([node('a', { dz_edge: 100, turbine: 0 })]).vsTurbine).toBe(100)
    expect(edgeHeadToHead([node('a', { dz_edge: 0, turbine: 100 })]).vsTurbine).toBe(0)
  })
})
