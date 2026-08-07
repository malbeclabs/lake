import { describe, expect, it } from 'vitest'
import { orientPath, pairKeyOf, resolveRoute, routeFigures } from './routes-page'
import type { MetroPathLatency } from '@/lib/api'

/** A fully-measured route; individual tests override the fields they care about. */
function latency(over: Partial<MetroPathLatency> = {}): MetroPathLatency {
  return {
    fromMetroPK: 'p1',
    fromMetroCode: 'tyo',
    toMetroPK: 'p2',
    toMetroCode: 'lon',
    pathLatencyMs: 200,
    measuredLatencyMs: 210.5,
    measuredP95Ms: 220.25,
    measuredJitterMs: 0.029,
    partiallyCommitted: false,
    pathMetros: ['tyo', 'fra', 'lon'],
    hopCount: 3,
    bottleneckBwGbps: 100,
    internetLatencyMs: 259.76,
    internetP95Ms: 272.06,
    internetJitterMs: 2.568,
    improvementPct: 23,
    measuredImprovementPct: 19,
    ...over,
  }
}

describe('routeFigures', () => {
  it('shows every figure on a fully-measured route', () => {
    const f = routeFigures(latency())
    expect(f.tiles.map((t) => [t.label, t.value])).toEqual([
      ['DoubleZero mean', '210.50 ms'],
      ['DoubleZero p95', '220.25 ms'],
      // 3 dp: 2 dp would flatten a typical sub-0.1 ms jitter to one significant figure.
      ['DoubleZero jitter', '0.029 ms'],
      ['Internet mean', '259.76 ms'],
      ['Internet p95', '272.06 ms'],
      ['Internet jitter', '2.568 ms'],
    ])
    expect(f.improvementPct).toBe(19)
    expect(f.footnote).toBeNull()
  })

  // The mean is a commitment on these routes, so it must be labelled as one, and
  // an improvement computed from it must not be shown at all.
  it('marks the mean as contracted and withholds improvement when partiallyCommitted', () => {
    const f = routeFigures(latency({ partiallyCommitted: true, measuredP95Ms: 0, measuredJitterMs: 0 }))
    expect(f.tiles[0]).toEqual({ label: 'DoubleZero mean (contracted)', value: '210.50 ms' })
    expect(f.tiles[1].value).toBeNull()
    expect(f.tiles[2].value).toBeNull()
    expect(f.improvementPct).toBeNull()
    expect(f.footnote).toContain('withheld')
  })

  // The internet side carries no partiallyCommitted-style flag, so 0 is the only
  // signal that a figure is absent. It must never print as "0.00 ms".
  it('renders an unmeasured internet figure as absent, not as zero', () => {
    const f = routeFigures(latency({ internetP95Ms: 0, internetJitterMs: 0 }))
    expect(f.tiles[4].value).toBeNull()
    expect(f.tiles[5].value).toBeNull()
  })
})

describe('orientPath', () => {
  // The API emits both directions of a pair and builds its slice from a Go map,
  // so an undirected lookup returns either one at random. The displayed path must
  // still read from the origin the customer picked, on every load.
  it('keeps a path that already starts at the route origin', () => {
    expect(orientPath(['tyo', 'fra', 'lon'], 'tyo', 'tyo')).toEqual(['tyo', 'fra', 'lon'])
  })

  it('reverses a path that arrived the other way round', () => {
    expect(orientPath(['lon', 'fra', 'tyo'], 'lon', 'tyo')).toEqual(['tyo', 'fra', 'lon'])
  })

  it('compares case-insensitively and does not mutate the input', () => {
    const path = ['TYO', 'fra', 'lon']
    expect(orientPath(path, 'TYO', 'tyo')).toEqual(['TYO', 'fra', 'lon'])
    expect(path).toEqual(['TYO', 'fra', 'lon'])
  })
})

describe('resolveRoute', () => {
  it('keys an on-net route lexicographically, whichever way round it was picked', () => {
    expect(resolveRoute({ from: 'tyo', to: 'lon' }).pairKey).toBe('lon-tyo')
    expect(resolveRoute({ from: 'lon', to: 'tyo' }).pairKey).toBe('lon-tyo')
  })

  // The property Task 6 verifies: both the DoubleZero and the public-internet
  // figures are looked up with this single key, so switching the on-ramp moves
  // both sides together and they cannot come from different anchors.
  it('folds an off-net anchor into the pair key', () => {
    expect(resolveRoute({ from: 'ohio', to: 'lon' }).pairKey).toBe('chi-lon')
    expect(resolveRoute({ from: 'ohio', to: 'lon', fromAnchor: 'pit' }).pairKey).toBe('lon-pit')
  })

  it('reports Zurich as unavailable with its note and no key', () => {
    const r = resolveRoute({ from: 'zurich', to: 'lon' })
    expect(r.unavailable).toBe(true)
    expect(r.pairKey).toBeNull()
    expect(r.notes[0]).toContain('no presence in Zurich')
  })

  it('reports an anchor that collides with the other endpoint as unavailable', () => {
    expect(resolveRoute({ from: 'ohio', to: 'chi' }).unavailable).toBe(true)
  })

  // Reachable only by URL. The cancellation argument in the Ohio note is false
  // here — the two access legs differ — so the route must be refused, not shown
  // with the note printed twice.
  it('refuses a route with the same off-net endpoint at both ends', () => {
    const r = resolveRoute({ from: 'ohio', to: 'ohio', fromAnchor: 'pit', toAnchor: 'chi' })
    expect(r.unavailable).toBe(true)
    expect(r.pairKey).toBeNull()
    expect(r.notes).toHaveLength(1)
    expect(r.notes[0]).toContain('both ends')
  })

  it('is case-insensitive', () => {
    expect(pairKeyOf('TYO', 'lon')).toBe('lon-tyo')
    expect(resolveRoute({ from: 'LON', to: 'lon' }).unavailable).toBe(true)
  })
})
