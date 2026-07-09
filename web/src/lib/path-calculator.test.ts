import { describe, it, expect } from 'vitest'
import { buildLocationOptions, filterLocations, parseEndpointKind, pickBestPair, filterPairsForDevice } from './path-calculator'
import type { MetroDevicePairPath } from '@/lib/api'

const metros = [
  { pk: 'm-lon', code: 'lon', name: 'London' },
  { pk: 'm-nyc', code: 'nyc', name: 'New York' },
]
const devices = [
  { pk: 'd-2', code: 'ny-dz01', status: 'activated', deviceType: 'switch', metroPK: 'm-nyc' },
  { pk: 'd-1', code: 'ld-dz01', status: 'activated', deviceType: 'switch', metroPK: 'm-lon' },
]

describe('buildLocationOptions', () => {
  it('lists metros first (by code), then devices (by code)', () => {
    const opts = buildLocationOptions(metros, devices)
    expect(opts.map((o) => `${o.kind}:${o.code}`)).toEqual([
      'metro:lon',
      'metro:nyc',
      'device:ld-dz01',
      'device:ny-dz01',
    ])
    expect(opts[2].metroPK).toBe('m-lon')
  })
})

describe('filterLocations', () => {
  const opts = buildLocationOptions(metros, devices)

  it('returns nothing for an empty query', () => {
    expect(filterLocations(opts, '   ')).toEqual([])
  })

  it('matches metros by name and ranks metros before devices', () => {
    const r = filterLocations(opts, 'new')
    expect(r).toHaveLength(1)
    expect(r[0]).toMatchObject({ kind: 'metro', code: 'nyc' })
  })

  it('floats an exact code match to the top within its kind', () => {
    const withDup = buildLocationOptions(
      [{ pk: 'm-lo', code: 'lo', name: 'Loota' }, { pk: 'm-lon', code: 'lon', name: 'London' }],
      [],
    )
    const r = filterLocations(withDup, 'lo')
    expect(r[0].code).toBe('lo')
  })

  it('excludes the other endpoint and respects the limit', () => {
    const r = filterLocations(opts, 'dz01', 'd-1', 1)
    expect(r).toHaveLength(1)
    expect(r[0].pk).toBe('d-2')
  })
})

describe('parseEndpointKind', () => {
  it('returns metro only for the literal "metro", else device (back-compat)', () => {
    expect(parseEndpointKind('metro')).toBe('metro')
    expect(parseEndpointKind('device')).toBe('device')
    expect(parseEndpointKind(null)).toBe('device')
    expect(parseEndpointKind('')).toBe('device')
  })
})

function pair(
  over: Partial<Omit<MetroDevicePairPath, 'bestPath'>> & { bestPath?: Partial<MetroDevicePairPath['bestPath']> },
): MetroDevicePairPath {
  return {
    sourceDevicePK: 's',
    sourceDeviceCode: 'src',
    targetDevicePK: 't',
    targetDeviceCode: 'tgt',
    ...over,
    bestPath: { path: [], totalMetric: 0, hopCount: 0, ...(over.bestPath || {}) },
  } as MetroDevicePairPath
}

describe('pickBestPair', () => {
  it('returns null for no pairs', () => {
    expect(pickBestPair([])).toBeNull()
  })

  it('prefers the lowest measured latency when present', () => {
    const best = pickBestPair([
      pair({ sourceDevicePK: 'a', bestPath: { measuredLatencyMs: 30, hopCount: 2 } }),
      pair({ sourceDevicePK: 'b', bestPath: { measuredLatencyMs: 12, hopCount: 4 } }),
    ])
    expect(best?.sourceDevicePK).toBe('b')
  })

  it('falls back to fewest hops, then ISIS metric, when no measured latency', () => {
    const best = pickBestPair([
      pair({ sourceDevicePK: 'a', bestPath: { hopCount: 3, totalMetric: 100 } }),
      pair({ sourceDevicePK: 'b', bestPath: { hopCount: 2, totalMetric: 900 } }),
      pair({ sourceDevicePK: 'c', bestPath: { hopCount: 2, totalMetric: 400 } }),
    ])
    expect(best?.sourceDevicePK).toBe('c')
  })
})

describe('filterPairsForDevice', () => {
  const pairs = [
    pair({ sourceDevicePK: 'a', targetDevicePK: 'x' }),
    pair({ sourceDevicePK: 'a', targetDevicePK: 'y' }),
    pair({ sourceDevicePK: 'b', targetDevicePK: 'x' }),
  ]

  it('filters by target device (metro→device query)', () => {
    expect(filterPairsForDevice(pairs, { targetDevicePK: 'x' })).toHaveLength(2)
  })

  it('filters by source device (device→metro query)', () => {
    expect(filterPairsForDevice(pairs, { sourceDevicePK: 'a' })).toHaveLength(2)
  })

  it('returns all pairs when no device constraint given (metro→metro)', () => {
    expect(filterPairsForDevice(pairs, {})).toHaveLength(3)
  })
})
