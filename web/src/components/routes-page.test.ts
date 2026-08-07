import { describe, expect, it } from 'vitest'
import { orientPath, pairKeyOf, resolveRoute } from './routes-page'

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

  it('is case-insensitive', () => {
    expect(pairKeyOf('TYO', 'lon')).toBe('lon-tyo')
    expect(resolveRoute({ from: 'LON', to: 'lon' }).unavailable).toBe(true)
  })
})
