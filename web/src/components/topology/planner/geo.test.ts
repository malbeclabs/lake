import { describe, it, expect } from 'vitest'
import {
  calculateDevicePosition,
  calculateCurvedPath,
  haversineKm,
  nearestKeyWithin,
} from './geo'

describe('calculateDevicePosition', () => {
  it('returns the metro centre for a single device', () => {
    expect(calculateDevicePosition(40, -74, 0, 1)).toEqual([-74, 40])
  })
  it('offsets devices around the metro when there is more than one', () => {
    const pos = calculateDevicePosition(40, -74, 1, 3)
    expect(pos[0]).not.toBe(-74)
    expect(pos[1]).not.toBe(40)
  })
})

describe('calculateCurvedPath', () => {
  it('starts at start and ends at end', () => {
    const pts = calculateCurvedPath([0, 0], [10, 10])
    expect(pts[0]).toEqual([0, 0])
    expect(pts[pts.length - 1][0]).toBeCloseTo(10, 5)
    expect(pts[pts.length - 1][1]).toBeCloseTo(10, 5)
  })
})

describe('haversineKm', () => {
  it('computes ~111 km for one degree of longitude at the equator', () => {
    expect(haversineKm(0, 0, 0, 1)).toBeCloseTo(111.19, 0)
  })
  it('is zero for identical points', () => {
    expect(haversineKm(51.5, -0.1, 51.5, -0.1)).toBe(0)
  })
})

describe('nearestKeyWithin', () => {
  const positions = new Map<string, [number, number]>([
    ['a', [0, 0]],
    ['b', [10, 10]],
  ])
  it('returns the closest key inside the threshold', () => {
    expect(nearestKeyWithin(0.2, 0.2, positions, 1)).toBe('a')
  })
  it('returns null when nothing is within the threshold', () => {
    expect(nearestKeyWithin(5, 5, positions, 1)).toBeNull()
  })
})
