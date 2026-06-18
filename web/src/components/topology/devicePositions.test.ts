import { describe, it, expect } from 'vitest'
import {
  computeDevicePositions,
  FANOUT_RADIUS_DEG,
  FACILITY_JITTER_DEG,
  type PositionableDevice,
  type PositionableMetro,
} from './devicePositions'

const metroMap = new Map<string, PositionableMetro>([
  ['metro-nyc', { latitude: 40.779, longitude: -74.072 }],
])

function dev(pk: string, over: Partial<PositionableDevice> = {}): PositionableDevice {
  return { pk, metro_pk: 'metro-nyc', latitude: 0, longitude: 0, ...over }
}

function dist(a: [number, number], b: [number, number]): number {
  return Math.hypot(a[0] - b[0], a[1] - b[1])
}

describe('computeDevicePositions — fanout mode', () => {
  it('places a lone device at the metro center', () => {
    const pos = computeDevicePositions([dev('d1')], metroMap, 'fanout')
    expect(pos.get('d1')).toEqual([-74.072, 40.779])
  })

  it('spreads co-metro devices within the (small) fanout radius and keeps them distinct', () => {
    const devices = [dev('d1'), dev('d2'), dev('d3'), dev('d4')]
    const pos = computeDevicePositions(devices, metroMap, 'fanout')
    const center: [number, number] = [-74.072, 40.779]
    const coords = devices.map(d => pos.get(d.pk)!)
    // every marker stays close to the metro center (latitude offset bounded by the radius)
    for (const c of coords) {
      expect(Math.abs(c[1] - center[1])).toBeLessThanOrEqual(FANOUT_RADIUS_DEG + 1e-9)
      expect(c).not.toEqual(center)
    }
    // markers are distinct from one another
    const keys = new Set(coords.map(c => `${c[0]},${c[1]}`))
    expect(keys.size).toBe(4)
  })
})

describe('computeDevicePositions — facility mode', () => {
  it('anchors a lone device exactly at its facility coordinates', () => {
    const pos = computeDevicePositions(
      [dev('d1', { latitude: 40.7968, longitude: -74.03088 })],
      metroMap,
      'facility',
    )
    expect(pos.get('d1')).toEqual([-74.03088, 40.7968])
  })

  it('jitters co-located devices around the shared facility, within the jitter radius', () => {
    const fac = { latitude: 40.7968, longitude: -74.03088 }
    const devices = [
      dev('d1', fac),
      dev('d2', fac),
      dev('d3', fac),
    ]
    const pos = computeDevicePositions(devices, metroMap, 'facility')
    const anchor: [number, number] = [fac.longitude, fac.latitude]
    const coords = devices.map(d => pos.get(d.pk)!)
    for (const c of coords) {
      // stays within the small jitter radius of the real facility (not flung away)
      expect(dist(c, anchor)).toBeLessThanOrEqual(FACILITY_JITTER_DEG / Math.cos(fac.latitude * Math.PI / 180) + 1e-6)
    }
    const keys = new Set(coords.map(c => `${c[0]},${c[1]}`))
    expect(keys.size).toBe(3)
  })

  it('falls back to the metro fanout when a device has no facility coordinates', () => {
    // facility coords 0/0 => missing; should NOT end up at [0,0]
    const pos = computeDevicePositions([dev('d1')], metroMap, 'facility')
    expect(pos.get('d1')).toEqual([-74.072, 40.779])
  })
})
