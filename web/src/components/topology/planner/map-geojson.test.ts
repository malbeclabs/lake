import { describe, it, expect } from 'vitest'
import {
  buildDevicePositions,
  buildLinkFeatures,
  buildRemoveDeviceSnapshot,
} from './map-geojson'
import { buildDraft } from './draft'
import type { TopologyResponse } from '@/lib/api'

function baseline(): TopologyResponse {
  return {
    metros: [
      { pk: 'mA', code: 'nyc', name: 'NYC', latitude: 40.7, longitude: -74 },
      { pk: 'mB', code: 'lon', name: 'LON', latitude: 51.5, longitude: -0.1 },
    ],
    devices: [
      { pk: 'dA', code: 'nyc-x1', metro_pk: 'mA', contributor_pk: 'c1' },
      { pk: 'dB', code: 'lon-x1', metro_pk: 'mB', contributor_pk: 'c2' },
    ],
    links: [
      { pk: 'L1', code: 'nyc-lon1', side_a_pk: 'dA', side_z_pk: 'dB', latency_us: 70_000, link_type: 'WAN' },
    ],
    validators: [],
  } as unknown as TopologyResponse
}

describe('buildDevicePositions', () => {
  it('places a lone metro device at the metro centre', () => {
    const pos = buildDevicePositions(buildDraft(baseline(), []))
    expect(pos.get('dA')).toEqual([-74, 40.7])
  })
})

describe('buildLinkFeatures', () => {
  it('emits one feature per link carrying its change colour and pk', () => {
    const draft = buildDraft(baseline(), [])
    const positions = buildDevicePositions(draft)
    const fc = buildLinkFeatures(draft, positions, true, null)
    expect(fc.features).toHaveLength(1)
    expect(fc.features[0].properties?.pk).toBe('L1')
    expect(fc.features[0].properties?.color).toBeTruthy()
  })

  it('marks the selected link', () => {
    const draft = buildDraft(baseline(), [])
    const positions = buildDevicePositions(draft)
    const fc = buildLinkFeatures(draft, positions, true, 'L1')
    expect(fc.features[0].properties?.isSelected).toBe(1)
  })

  it('drops links whose endpoints have no position', () => {
    const draft = buildDraft(baseline(), [])
    const fc = buildLinkFeatures(draft, new Map(), true, null)
    expect(fc.features).toHaveLength(0)
  })
})

describe('buildRemoveDeviceSnapshot', () => {
  it('stores the metro code, not the metro pk', () => {
    const draft = buildDraft(baseline(), [])
    const dev = draft.deviceByKey.get('dA')!
    const snap = buildRemoveDeviceSnapshot(draft, dev)
    expect(snap.device_code).toBe('nyc-x1')
    expect(snap.metro_code).toBe('nyc')
  })

  it('falls back to the metro pk when the metro is missing', () => {
    const draft = buildDraft(baseline(), [])
    const dev = { ...draft.deviceByKey.get('dA')!, metro_pk: 'unknown' }
    const snap = buildRemoveDeviceSnapshot(draft, dev)
    expect(snap.metro_code).toBe('unknown')
  })
})
