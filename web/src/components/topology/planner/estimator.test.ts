import { describe, it, expect } from 'vitest'
import { greatCircleLatencyNs, findComparableRttNs, estimateLatencyNs } from './estimator'
import type { TopologyResponse } from '@/lib/api'

const topo = {
  metros: [
    { pk: 'mA', code: 'nyc', name: 'New York', latitude: 40.7, longitude: -74 },
    { pk: 'mB', code: 'lon', name: 'London', latitude: 51.5, longitude: -0.1 },
    { pk: 'mC', code: 'fra', name: 'Frankfurt', latitude: 50.1, longitude: 8.7 },
  ],
  devices: [
    { pk: 'dA', metro_pk: 'mA' },
    { pk: 'dB', metro_pk: 'mB' },
  ],
  links: [
    { pk: 'L1', side_a_pk: 'dA', side_z_pk: 'dB', committed_rtt_ns: 70_000_000, latency_us: 70_000 },
  ],
  validators: [],
} as unknown as TopologyResponse

describe('greatCircleLatencyNs', () => {
  it('applies the 1.4 route factor and 10us/km RTT', () => {
    expect(greatCircleLatencyNs(1000)).toBe(14_000_000)
  })
})

describe('findComparableRttNs', () => {
  it('returns the measured RTT of a same-metro-pair link (order independent)', () => {
    expect(findComparableRttNs(topo, 'mB', 'mA')).toBe(70_000_000)
  })
  it('returns null when no link connects the metro pair', () => {
    expect(findComparableRttNs(topo, 'mA', 'mC')).toBeNull()
  })
})

// Same-metro-pair link whose committed_rtt_ns is the 1e9 "latency not set" sentinel
// (api unsetLatencyNs) and which has no measured latency to fall back to.
const topoSentinel = {
  metros: [
    { pk: 'mA', code: 'nyc', name: 'New York', latitude: 40.7, longitude: -74 },
    { pk: 'mC', code: 'fra', name: 'Frankfurt', latitude: 50.1, longitude: 8.7 },
  ],
  devices: [
    { pk: 'dA', metro_pk: 'mA' },
    { pk: 'dC', metro_pk: 'mC' },
  ],
  links: [
    { pk: 'L9', side_a_pk: 'dA', side_z_pk: 'dC', committed_rtt_ns: 1_000_000_000, latency_us: 0 },
  ],
  validators: [],
} as unknown as TopologyResponse

describe('findComparableRttNs sentinel handling', () => {
  it('does not treat the 1e9 unset sentinel as a comparable', () => {
    expect(findComparableRttNs(topoSentinel, 'mA', 'mC')).toBeNull()
  })
})

describe('estimateLatencyNs', () => {
  it('never copies the 1e9 sentinel; falls back to great_circle', () => {
    const r = estimateLatencyNs({ topology: topoSentinel, metroAPk: 'mA', metroBPk: 'mC' })
    expect(r.source).toBe('great_circle')
    expect(r.latencyNs).toBeGreaterThan(0)
    expect(r.latencyNs).not.toBe(1_000_000_000)
  })
  it('throws on an unknown metro pk rather than emitting a 0 latency', () => {
    expect(() =>
      estimateLatencyNs({ topology: topoSentinel, metroAPk: 'mA', metroBPk: 'mZ' })
    ).toThrow()
  })
  it('prefers a copied same-metro-pair RTT', () => {
    const r = estimateLatencyNs({ topology: topo, metroAPk: 'mA', metroBPk: 'mB' })
    expect(r).toEqual({ latencyNs: 70_000_000, source: 'copied' })
  })
  it('falls back to great-circle when no comparable link exists', () => {
    const r = estimateLatencyNs({ topology: topo, metroAPk: 'mA', metroBPk: 'mC' })
    expect(r.source).toBe('great_circle')
    expect(r.latencyNs).toBeGreaterThan(0)
  })
  it('uses a manual override when provided', () => {
    const r = estimateLatencyNs({ topology: topo, metroAPk: 'mA', metroBPk: 'mB', manualNs: 5_000_000 })
    expect(r).toEqual({ latencyNs: 5_000_000, source: 'manual' })
  })
})
