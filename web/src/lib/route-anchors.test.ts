import { describe, expect, it } from 'vitest'
import {
  OFF_NET_ENDPOINTS,
  formatRouteToken,
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

  it('every off-net entry lists its default among its candidates', () => {
    for (const e of OFF_NET_ENDPOINTS) {
      if (e.defaultAnchor) {
        expect(e.candidateAnchors).toContain(e.defaultAnchor)
      }
    }
  })
})
