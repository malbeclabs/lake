import { describe, expect, it } from 'vitest'
import {
  OFF_NET_ENDPOINTS,
  formatCityToken,
  formatRouteToken,
  parseCityToken,
  parseRouteToken,
  resolveEndpoint,
} from './route-anchors'

describe('route-anchors', () => {
  it('resolves an on-net metro to itself', () => {
    expect(resolveEndpoint('lon')).toEqual({ metroCode: 'lon', offNet: null, anchor: null })
  })

  it('resolves Ohio to its default anchor', () => {
    const r = resolveEndpoint('ohio')
    expect(r.metroCode).toBe('chi')
    expect(r.anchor).toBe('chi')
    expect(r.offNet?.label).toContain('Ohio')
  })

  it('honours an explicit anchor override', () => {
    expect(resolveEndpoint('ohio', 'pit').metroCode).toBe('pit')
  })

  it('ignores an anchor that is not a candidate', () => {
    expect(resolveEndpoint('ohio', 'sao').metroCode).toBe('chi')
  })

  // Zurich has no DoubleZero presence and no committed coverage, so it resolves
  // to nothing rather than being silently substituted with Frankfurt.
  it('resolves Zurich to N/A with no anchor', () => {
    const r = resolveEndpoint('zurich')
    expect(r.metroCode).toBeNull()
    expect(r.anchor).toBeNull()
    expect(r.offNet?.defaultAnchor).toBeNull()
  })

  it('round-trips a plain route token', () => {
    expect(parseRouteToken('tyo-lon')).toEqual({ from: 'tyo', to: 'lon' })
    expect(formatRouteToken('tyo', 'lon')).toBe('tyo-lon')
  })

  it('round-trips a route token carrying an anchor', () => {
    expect(parseRouteToken('ohio@pit-lon')).toEqual({ from: 'ohio', to: 'lon', fromAnchor: 'pit' })
    expect(formatRouteToken('ohio', 'lon', 'pit')).toBe('ohio@pit-lon')
  })

  it('round-trips an anchor on the destination side', () => {
    expect(parseRouteToken('tyo-ohio@pit')).toEqual({ from: 'tyo', to: 'ohio', toAnchor: 'pit' })
    expect(formatRouteToken('tyo', 'ohio', undefined, 'pit')).toBe('tyo-ohio@pit')
  })

  it('round-trips anchors on both sides', () => {
    expect(parseRouteToken('ohio@pit-ohio@chi')).toEqual({
      from: 'ohio',
      to: 'ohio',
      fromAnchor: 'pit',
      toAnchor: 'chi',
    })
    expect(formatRouteToken('ohio', 'ohio', 'pit', 'chi')).toBe('ohio@pit-ohio@chi')
  })

  it('rejects malformed route tokens instead of guessing', () => {
    expect(parseRouteToken('')).toBeNull()
    expect(parseRouteToken('tyo-lon-extra')).toBeNull()
    expect(parseRouteToken('-lon')).toBeNull()
    expect(parseRouteToken('tyo-')).toBeNull()
    expect(parseRouteToken('ohio@-lon')).toBeNull()
    expect(parseRouteToken('ohio@pit@x-lon')).toBeNull()
  })

  it('round-trips a location token, with and without an anchor', () => {
    expect(parseCityToken('lon')).toEqual({ id: 'lon' })
    expect(parseCityToken('ohio@pit')).toEqual({ id: 'ohio', anchor: 'pit' })
    expect(formatCityToken('lon')).toBe('lon')
    expect(formatCityToken('ohio', 'pit')).toBe('ohio@pit')
  })

  // Every id lookup downstream is case-sensitive, so a link an inbox uppercased
  // has to fold back onto the same location rather than become an unknown one.
  it('case-folds a location token', () => {
    expect(parseCityToken('ZURICH')).toEqual({ id: 'zurich' })
    expect(parseCityToken('Ohio@PIT')).toEqual({ id: 'ohio', anchor: 'pit' })
    expect(resolveEndpoint('OHIO'.toLowerCase()).metroCode).toBe('chi')
    expect(parseRouteToken('OHIO@PIT-LON')).toEqual({ from: 'ohio', to: 'lon', fromAnchor: 'pit' })
  })

  it('rejects a malformed location token instead of guessing', () => {
    expect(parseCityToken('')).toBeNull()
    expect(parseCityToken('   ')).toBeNull()
    expect(parseCityToken('ohio@')).toBeNull()
    expect(parseCityToken('@pit')).toBeNull()
    expect(parseCityToken('ohio@pit@x')).toBeNull()
  })

  it('every off-net entry carries a short axis label', () => {
    for (const e of OFF_NET_ENDPOINTS) {
      expect(e.short).toMatch(/^[A-Z0-9]{2,6}$/)
    }
  })

  it('every off-net entry lists its default among its candidates', () => {
    for (const e of OFF_NET_ENDPOINTS) {
      if (e.defaultAnchor) {
        expect(e.candidateAnchors).toContain(e.defaultAnchor)
      }
    }
  })
})
