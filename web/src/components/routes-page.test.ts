import { describe, expect, it } from 'vitest'
import {
  MAX_CELLS,
  MAX_CITIES,
  cellFor,
  cellPairKey,
  formatCells,
  formatCities,
  isComparable,
  isToggleGesture,
  meshPairs,
  orientPath,
  pairKeyOf,
  parseCells,
  parseCities,
  resolveRoute,
  routeFigures,
  shadeFor,
  summariseMatrix,
  toggleCell,
} from './routes-page'
import type { MatrixCell, SelectedCity } from './routes-page'
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

/**
 * What `/api/topology/metro-path-latency` returned from the page_cache on
 * staging on 2026-08-12, after the web deploy and before the worker refreshed:
 * exactly these keys and no others. Written as a cast because the point of the
 * test is a payload the type says cannot happen, arriving anyway.
 */
function stalePayload(): MetroPathLatency {
  return {
    fromMetroPK: 'p1',
    fromMetroCode: 'dfw',
    toMetroPK: 'p2',
    toMetroCode: 'fra',
    pathLatencyMs: 109.41,
    hopCount: 7,
    bottleneckBwGbps: 10,
    internetLatencyMs: 124.74,
    improvementPct: 12.29,
  } as MetroPathLatency
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

  it('reports the milliseconds saved on a fully-measured route', () => {
    const f = routeFigures(latency())
    expect(f.internetMeasured).toBe(true)
    expect(f.savedMs).toBeCloseTo(49.26, 5)
  })

  // The saving is the improvement in another unit, so it must be withheld
  // wherever the percentage is — a matrix cell and a KPI card read it directly.
  it('withholds the saving whenever the improvement is withheld', () => {
    expect(routeFigures(latency({ partiallyCommitted: true })).savedMs).toBeNull()
    expect(routeFigures(latency({ internetLatencyMs: 0 })).savedMs).toBeNull()
  })

  it('reports an internet side with no samples as unmeasured', () => {
    expect(routeFigures(latency({ internetLatencyMs: 0 })).internetMeasured).toBe(false)
  })

  // Every figure blanks rather than throwing on a payload that predates the
  // measured fields. This is not hypothetical: the default request is answered
  // from the page_cache table, and a web deploy reaches readers before the next
  // worker refresh does. On staging that window put `undefined.toFixed()` into
  // a matrix cell and the error boundary took the whole table with it.
  it('blanks every figure on a payload that predates the measured fields', () => {
    const f = routeFigures(stalePayload())
    expect(f.tiles.map((t) => t.value)).toEqual([null, null, null, '124.74 ms', null, null])
    expect(f.improvementPct).toBeNull()
    expect(f.savedMs).toBeNull()
    expect(f.footnote).toBeNull()
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

describe('parseCities', () => {
  it('parses a plain list and round-trips it', () => {
    const { cities, requested } = parseCities('dub,fra,lon')
    expect(cities).toEqual([{ id: 'dub' }, { id: 'fra' }, { id: 'lon' }])
    expect(requested).toBe(3)
    expect(formatCities(cities)).toBe('dub,fra,lon')
  })

  it('carries an off-net anchor through the token', () => {
    const { cities } = parseCities('lon,ohio@pit')
    expect(cities[1]).toEqual({ id: 'ohio', anchor: 'pit' })
    expect(formatCities(cities)).toBe('lon,ohio@pit')
  })

  // Same rule as parseRouteToken: a corrupted shared link drops the token rather
  // than rendering a plausible-but-wrong location.
  it('drops a malformed token instead of coercing it', () => {
    const { cities, requested } = parseCities('lon,ohio@,@pit,fra')
    expect(cities).toEqual([{ id: 'lon' }, { id: 'fra' }])
    // requested still counts them, so the page can say the selection shrank.
    expect(requested).toBe(4)
  })

  it('folds a repeated location, because a city cannot hold two anchors in one grid', () => {
    const { cities, requested } = parseCities('lon,LON,ohio@pit,ohio@chi')
    expect(cities).toEqual([{ id: 'lon' }, { id: 'ohio', anchor: 'pit' }])
    expect(requested).toBe(4)
  })

  it('caps the selection and reports that it asked for more', () => {
    const raw = Array.from({ length: MAX_CITIES + 3 }, (_, i) => `m${i}`).join(',')
    const { cities, requested } = parseCities(raw)
    expect(cities).toHaveLength(MAX_CITIES)
    expect(requested).toBe(MAX_CITIES + 3)
  })

  it('is empty for a missing param', () => {
    expect(parseCities(null)).toEqual({ cities: [], requested: 0 })
  })
})

describe('meshPairs', () => {
  it('enumerates every unordered pair once, in axis order', () => {
    expect(meshPairs(['a', 'b', 'c'])).toEqual([
      ['a', 'b'],
      ['a', 'c'],
      ['b', 'c'],
    ])
  })

  it('has no pairs below two locations', () => {
    expect(meshPairs(['a'])).toEqual([])
    expect(meshPairs([])).toEqual([])
  })

  it('grows quadratically, which is why the selection is capped', () => {
    expect(meshPairs(Array.from({ length: 6 }, (_, i) => i))).toHaveLength(15)
  })
})

describe('cellFor', () => {
  const route = resolveRoute({ from: 'tyo', to: 'lon' })

  it('states the improvement and the saving on a measured pair', () => {
    expect(cellFor(route, latency(), false, false)).toEqual({
      kind: 'improvement',
      pct: 19,
      savedMs: 259.76 - 210.5,
    })
  })

  // Three separate facts. A request failure reported as "no path" would tell a
  // customer DoubleZero cannot reach between two of its own metros.
  it('separates a failed request from a pending one and from a genuine absence', () => {
    expect(cellFor(route, null, true, true)).toEqual({ kind: 'error' })
    expect(cellFor(route, null, true, false)).toEqual({ kind: 'loading' })
    expect(cellFor(route, null, false, false)).toEqual({ kind: 'no-path' })
  })

  it('keeps figures it already holds when a background refetch fails', () => {
    expect(cellFor(route, latency(), false, true).kind).toBe('improvement')
  })

  it('marks a pair with no public-internet samples as unmeasured', () => {
    expect(cellFor(route, latency({ internetLatencyMs: 0 }), false, false)).toEqual({
      kind: 'not-measured',
    })
  })

  // Not 'withheld': nothing was suppressed here, the figure has not arrived.
  // The cell that crashed on staging was this one.
  it('reads a payload predating the measured fields as still loading', () => {
    expect(cellFor(route, stalePayload(), false, false)).toEqual({ kind: 'loading' })
  })

  // Distinct from 'not-measured': there is an internet figure here, it is the
  // DoubleZero side that is a commitment rather than an observation.
  it('withholds rather than fabricating on a partiallyCommitted route', () => {
    expect(cellFor(route, latency({ partiallyCommitted: true }), false, false)).toEqual({
      kind: 'withheld',
    })
  })

  it('reports Zurich as unavailable with its note, at every column', () => {
    const cell = cellFor(resolveRoute({ from: 'zurich', to: 'lon' }), null, false, false)
    expect(cell.kind).toBe('unavailable')
    expect(cell.kind === 'unavailable' && cell.note).toContain('no presence in Zurich')
  })
})

describe('summariseMatrix', () => {
  const improvement = (pct: number, savedMs: number | null): MatrixCell => ({
    kind: 'improvement',
    pct,
    savedMs,
  })

  it('counts pairs, averages the measured ones, and names the best', () => {
    const s = summariseMatrix([
      { label: 'LON↔TYO', cell: improvement(44, 115) },
      { label: 'DUB↔FRA', cell: improvement(20, 35) },
      { label: 'DUB↔ZRH', cell: { kind: 'unavailable', note: null } },
    ])
    expect(s.pairs).toBe(3)
    expect(s.withPath).toBe(2)
    expect(s.avgPct).toBe(32)
    expect(s.avgSavedMs).toBe(75)
    expect(s.best).toEqual({ label: 'LON↔TYO', kind: 'improvement', pct: 44, savedMs: 115 })
  })

  // The point of the whole suppression chain: a contracted route is carried by
  // DoubleZero, and that is all the KPI cards may say about it.
  it('lets a withheld route count as carried but not move any average', () => {
    const s = summariseMatrix([
      { label: 'LON↔TYO', cell: improvement(20, 40) },
      { label: 'FRA↔OHIO', cell: { kind: 'withheld' } },
      { label: 'DUB↔FRA', cell: { kind: 'not-measured' } },
    ])
    expect(s.withPath).toBe(3)
    expect(s.avgPct).toBe(20)
    expect(s.avgSavedMs).toBe(40)
    expect(s.best?.label).toBe('LON↔TYO')
  })

  it('averages nothing rather than zero when no pair is comparable', () => {
    const s = summariseMatrix([
      { label: 'DUB↔ZRH', cell: { kind: 'unavailable', note: null } },
      { label: 'DUB↔FRA', cell: { kind: 'no-path' } },
    ])
    expect(s.withPath).toBe(0)
    expect(s.avgPct).toBeNull()
    expect(s.avgSavedMs).toBeNull()
    expect(s.best).toBeNull()
  })

  it('reports a slower route honestly rather than hiding it', () => {
    const s = summariseMatrix([{ label: 'DUB↔FRA', cell: improvement(-4, -2) }])
    expect(s.avgPct).toBe(-4)
    expect(s.best?.pct).toBe(-4)
  })
})

describe('summariseMatrix pending and failed', () => {
  // The cell-level rule, one layer up: a request that failed must not total to
  // "0 pairs where DoubleZero has a path", which reads to a customer as
  // DoubleZero carrying none of their routes.
  it('counts a failed pair as failed, never as a pair without a path', () => {
    const s = summariseMatrix([
      { label: 'LON↔TYO', cell: { kind: 'error' } },
      { label: 'DUB↔FRA', cell: { kind: 'error' } },
    ])
    expect(s.failed).toBe(2)
    expect(s.pending).toBe(0)
    expect(s.withPath).toBe(0)
    expect(s.avgPct).toBeNull()
    expect(s.best).toBeNull()
  })

  it('counts a pending pair as pending, and keeps it out of every total', () => {
    const s = summariseMatrix([
      { label: 'LON↔TYO', cell: { kind: 'loading' } },
      { label: 'DUB↔FRA', cell: { kind: 'loading' } },
    ])
    expect(s.pending).toBe(2)
    expect(s.failed).toBe(0)
    expect(s.withPath).toBe(0)
  })

  // A part-loaded grid still has an unknown in it, so the caller has something
  // to gate on even when some pairs did resolve.
  it('reports the unknowns alongside the pairs that did resolve', () => {
    const s = summariseMatrix([
      { label: 'LON↔TYO', cell: { kind: 'improvement', pct: 20, savedMs: 40 } },
      { label: 'DUB↔FRA', cell: { kind: 'error' } },
      { label: 'DUB↔LON', cell: { kind: 'loading' } },
    ])
    expect(s.pairs).toBe(3)
    expect(s.withPath).toBe(1)
    expect(s.failed).toBe(1)
    expect(s.pending).toBe(1)
  })

  it('reports no unknowns on a fully resolved grid', () => {
    const s = summariseMatrix([
      { label: 'LON↔TYO', cell: { kind: 'improvement', pct: 20, savedMs: 40 } },
      { label: 'DUB↔ZRH', cell: { kind: 'unavailable', note: null } },
    ])
    expect(s.pending).toBe(0)
    expect(s.failed).toBe(0)
  })
})

describe('shadeFor', () => {
  // A route DoubleZero makes slower must never be shaded as a win.
  it('shades a slower or level route away from the green ramp', () => {
    expect(shadeFor(-0.1)).toContain('red')
    expect(shadeFor(0)).toContain('red')
    expect(shadeFor(0.1)).toContain('emerald')
  })

  it('deepens with the improvement and never lightens', () => {
    const weights = [0.1, 9.9, 10, 19.9, 20, 29.9, 30, 39.9, 40, 100].map((pct) =>
      Number(shadeFor(pct).split('/')[1]),
    )
    for (let i = 1; i < weights.length; i++) {
      expect(weights[i]).toBeGreaterThanOrEqual(weights[i - 1])
    }
    expect(weights[weights.length - 1]).toBeGreaterThan(weights[0])
  })
})

describe('parseCities case folding', () => {
  // An inbox or a hand edit that uppercases a shared link must not turn an
  // off-net location into an unknown metro code, whose row would then read
  // "no path" — asserting DoubleZero cannot reach a place it has only ever said
  // it does not serve.
  it('folds an uppercased token back onto its off-net endpoint', () => {
    const { cities } = parseCities('LON,ZURICH,Ohio@PIT')
    expect(cities).toEqual([{ id: 'lon' }, { id: 'zurich' }, { id: 'ohio', anchor: 'pit' }])
    expect(resolveRoute({ from: cities[1].id, to: 'lon' }).notes[0]).toContain('Zurich')
    expect(resolveRoute({ from: cities[2].id, to: 'lon', fromAnchor: cities[2].anchor }).pairKey).toBe(
      'lon-pit',
    )
  })
})

describe('parseCells', () => {
  const cities: SelectedCity[] = [{ id: 'lon' }, { id: 'tyo' }, { id: 'fra' }, { id: 'ohio' }]

  it('round-trips a comparison set', () => {
    const { cells, requested } = parseCells('lon-tyo,fra-ohio', cities)
    expect(cells).toEqual([
      { from: 'lon', to: 'tyo' },
      { from: 'fra', to: 'ohio' },
    ])
    expect(requested).toBe(2)
    expect(formatCells(cells)).toBe('lon-tyo,fra-ohio')
  })

  // The matrix is symmetric, so the mirrored cell is the same route. Two cards
  // would report one pair's figures twice under two headings.
  it('folds a mirrored cell onto the route it names, keeping the first orientation', () => {
    const { cells, requested } = parseCells('lon-tyo,tyo-lon', cities)
    expect(cells).toEqual([{ from: 'lon', to: 'tyo' }])
    expect(requested).toBe(2)
  })

  // The anchor a location carries has to reach the pair key, or the grid cell
  // reads pit-lon while the card under it reads chi-lon, under one heading. The
  // default anchor cannot show this: it would still fold if the anchor were
  // dropped entirely, so the assertion is on a non-default one.
  it('threads each location’s own anchor into the pair a cell names', () => {
    const cells: SelectedCity[] = [{ id: 'ohio', anchor: 'pit' }, { id: 'lon' }]
    expect(cellPairKey({ from: 'ohio', to: 'lon' }, cells)).toBe('lon-pit')
    expect(cellPairKey({ from: 'ohio', to: 'lon' }, [{ id: 'ohio' }, { id: 'lon' }])).toBe('chi-lon')
    expect(parseCells('ohio-lon', cells).cells).toEqual([{ from: 'ohio', to: 'lon' }])
  })

  // An off-net location on-ramped at a metro that is also in the mesh is that
  // metro: one route, one card, however the reader reaches it.
  it('folds an off-net cell onto its on-ramp metro cell', () => {
    const withChi: SelectedCity[] = [...cities, { id: 'chi' }]
    expect(cellPairKey({ from: 'ohio', to: 'lon' }, withChi)).toBe('chi-lon')
    expect(parseCells('ohio-lon,chi-lon', withChi).cells).toEqual([{ from: 'ohio', to: 'lon' }])
  })

  // The anchor belongs to the location, once, in ?cities=. Half-honouring it
  // here would render CHI under a URL that says PIT.
  it('refuses a cell token carrying an anchor rather than applying a different one', () => {
    const withAnchor: SelectedCity[] = [{ id: 'ohio', anchor: 'chi' }, { id: 'lon' }]
    const { cells, requested } = parseCells('ohio@pit-lon', withAnchor)
    expect(cells).toEqual([])
    // Counted, so the shortfall is stated rather than silently absent.
    expect(requested).toBe(1)
  })

  it('drops a malformed token instead of coercing it', () => {
    const { cells, requested } = parseCells('lon-tyo,lon-tyo-fra,,fra-lon', cities)
    expect(cells).toEqual([
      { from: 'lon', to: 'tyo' },
      { from: 'fra', to: 'lon' },
    ])
    expect(requested).toBe(3)
  })

  it('drops a cell naming a location outside the mesh', () => {
    expect(parseCells('lon-sao,lon-tyo', cities).cells).toEqual([{ from: 'lon', to: 'tyo' }])
  })

  // Nothing to compare: Zurich has no committed coverage, and a cell whose ends
  // resolve to one metro is not a route.
  it('drops a cell with nothing to compare', () => {
    const withZurich: SelectedCity[] = [...cities, { id: 'zurich' }, { id: 'chi' }]
    expect(parseCells('lon-zurich', withZurich).cells).toEqual([])
    expect(parseCells('ohio-chi', withZurich).cells).toEqual([])
    expect(parseCells('lon-lon', withZurich).cells).toEqual([])
  })

  it('caps the set at the pairs one series request accepts, and says it asked for more', () => {
    const many: SelectedCity[] = Array.from({ length: MAX_CELLS + 2 }, (_, i) => ({ id: `m${i}` }))
    const raw = many.slice(1).map((c) => `m0-${c.id}`).join(',')
    const { cells, requested } = parseCells(raw, many)
    expect(cells).toHaveLength(MAX_CELLS)
    expect(requested).toBe(MAX_CELLS + 1)
    expect(requested).toBeGreaterThan(cells.length)
  })

  it('is empty for a missing param', () => {
    expect(parseCells(null, cities)).toEqual({ cells: [], requested: 0 })
  })
})

describe('toggleCell', () => {
  const cities: SelectedCity[] = [{ id: 'lon' }, { id: 'tyo' }, { id: 'fra' }]

  it('adds a cell that is not in the set', () => {
    expect(toggleCell([], { from: 'lon', to: 'tyo' }, cities)).toEqual([{ from: 'lon', to: 'tyo' }])
  })

  it('removes the route when its mirror is toggled', () => {
    const set = [{ from: 'lon', to: 'tyo' }]
    expect(toggleCell(set, { from: 'tyo', to: 'lon' }, cities)).toEqual([])
  })

  it('keeps the orientation of the cells already in the set', () => {
    const set = [{ from: 'lon', to: 'tyo' }]
    expect(toggleCell(set, { from: 'fra', to: 'lon' }, cities)).toEqual([
      { from: 'lon', to: 'tyo' },
      { from: 'fra', to: 'lon' },
    ])
  })

  it('refuses to grow past the cap', () => {
    const many: SelectedCity[] = Array.from({ length: MAX_CELLS + 2 }, (_, i) => ({ id: `m${i}` }))
    const full = many.slice(1, MAX_CELLS + 1).map((c) => ({ from: 'm0', to: c.id }))
    expect(full).toHaveLength(MAX_CELLS)
    const extra = { from: 'm0', to: `m${MAX_CELLS + 1}` }
    expect(toggleCell(full, extra, many)).toBe(full)
    // Still lets one already in the set out, which is how a user makes room.
    expect(toggleCell(full, full[0], many)).toHaveLength(MAX_CELLS - 1)
  })

  it('ignores a cell with nothing to compare', () => {
    const withZurich: SelectedCity[] = [...cities, { id: 'zurich' }]
    const set = [{ from: 'lon', to: 'tyo' }]
    expect(toggleCell(set, { from: 'lon', to: 'zurich' }, withZurich)).toBe(set)
  })
})

describe('isComparable', () => {
  it('opens a cell that carries figures', () => {
    expect(isComparable({ kind: 'improvement', pct: 20, savedMs: 40 })).toBe(true)
    expect(isComparable({ kind: 'withheld' })).toBe(true)
  })

  it('refuses a cell with nothing to compare, and one whose state is not known yet', () => {
    expect(isComparable({ kind: 'unavailable', note: null })).toBe(false)
    expect(isComparable({ kind: 'no-path' })).toBe(false)
    expect(isComparable({ kind: 'not-measured' })).toBe(false)
    expect(isComparable({ kind: 'loading' })).toBe(false)
    expect(isComparable({ kind: 'error' })).toBe(false)
    expect(isComparable({ kind: 'diagonal' })).toBe(false)
  })
})

describe('isToggleGesture', () => {
  // One gesture for all three: a matrix has no linear range for Shift to extend,
  // so distinguishing them would only punish reaching for the wrong modifier.
  it('treats meta, ctrl and shift alike, and a plain click as a plain click', () => {
    const plain = { metaKey: false, ctrlKey: false, shiftKey: false }
    expect(isToggleGesture(plain)).toBe(false)
    expect(isToggleGesture({ ...plain, metaKey: true })).toBe(true)
    expect(isToggleGesture({ ...plain, ctrlKey: true })).toBe(true)
    expect(isToggleGesture({ ...plain, shiftKey: true })).toBe(true)
  })
})
