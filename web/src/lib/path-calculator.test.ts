import { describe, it, expect } from 'vitest'
import { buildLocationOptions, filterLocations, parseEndpointKind } from './path-calculator'

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
