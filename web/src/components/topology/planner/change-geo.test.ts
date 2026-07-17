import { describe, it, expect } from 'vitest'
import { changedEntitiesBounds, changeGeoTargetById } from './change-geo'
import type { DraftTopology, DraftDevice, DraftLink } from './draft'
import type { TopologyMetro } from '@/lib/api'

// changedEntitiesBounds delegates to collectChangedEntities, which computes device
// positions via buildDevicePositions (metro-anchored, not a plain positions map), so
// the bounds fixture needs real metros + device.metro_pk, not stubbed coordinates.
function draftWithMetros(metros: TopologyMetro[], devices: Partial<DraftDevice>[], links: Partial<DraftLink>[] = []): DraftTopology {
  const fullDevices = devices.map((d) => ({ changeState: 'unchanged', ...d }) as DraftDevice)
  const fullLinks = links.map((l) => ({ changeState: 'unchanged', ...l }) as DraftLink)
  return {
    metros,
    devices: fullDevices,
    links: fullLinks,
    deviceByKey: new Map(fullDevices.map((d) => [d.pk, d])),
    linkByKey: new Map(fullLinks.map((l) => [l.pk, l])),
  }
}

// changeGeoTargetById takes an explicit positions map, so its fixtures can skip
// metros entirely and stub coordinates directly.
function draftForTarget(devices: Partial<DraftDevice>[], links: Partial<DraftLink>[] = []): DraftTopology {
  return draftWithMetros([], devices, links)
}

describe('changedEntitiesBounds', () => {
  it('returns null when the plan has no changed entities', () => {
    const d = draftWithMetros(
      [{ pk: 'm1', code: 'nyc', name: 'NYC', latitude: 20, longitude: 10 }],
      [{ pk: 'd1', metro_pk: 'm1', changeState: 'unchanged' }]
    )
    expect(changedEntitiesBounds(d)).toBeNull()
  })

  it('bounds a mix of changed devices and changed-link endpoints', () => {
    const metros: TopologyMetro[] = [
      { pk: 'm1', code: 'nyc', name: 'NYC', latitude: 20, longitude: 10 },
      { pk: 'm2', code: 'lon', name: 'LON', latitude: 0, longitude: -5 },
      { pk: 'm3', code: 'fra', name: 'FRA', latitude: -10, longitude: 30 },
    ]
    const d = draftWithMetros(
      metros,
      [
        { pk: 'd1', metro_pk: 'm1', changeState: 'added' }, // changed device, at (10, 20)
        { pk: 'd2', metro_pk: 'm2', changeState: 'unchanged' }, // only touched via the link below
        { pk: 'd3', metro_pk: 'm3', changeState: 'unchanged' },
      ],
      [{ pk: 'l1', side_a_pk: 'd2', side_z_pk: 'd3', changeState: 'removed' }]
    )
    const bounds = changedEntitiesBounds(d)
    expect(bounds).not.toBeNull()
    const [[west, south], [east, north]] = bounds!
    // Covers d1 (10,20) plus the link's endpoints d2 (-5,0) and d3 (30,-10).
    expect(west).toBeCloseTo(-5, 1)
    expect(east).toBeCloseTo(30, 1)
    expect(south).toBeCloseTo(-10, 1)
    expect(north).toBeCloseTo(20, 1)
  })
})

describe('changeGeoTargetById', () => {
  const positions = new Map<string, [number, number]>([
    ['d1', [10, 20]],
    ['d2', [-5, 0]],
    ['d3', [30, -10]],
  ])

  it('resolves a device change to the device position', () => {
    const d = draftForTarget([{ pk: 'd1', changeState: 'added', changeId: 'c1' }])
    expect(changeGeoTargetById(d, positions, 'c1')).toEqual([10, 20])
  })

  it('resolves a link change to the midpoint of its endpoints', () => {
    const d = draftForTarget(
      [],
      [{ pk: 'l1', side_a_pk: 'd2', side_z_pk: 'd3', changeState: 'added', changeId: 'c2' }]
    )
    expect(changeGeoTargetById(d, positions, 'c2')).toEqual([12.5, -5])
  })

  it('returns null for an unknown change id', () => {
    const d = draftForTarget([{ pk: 'd1', changeState: 'added', changeId: 'c1' }])
    expect(changeGeoTargetById(d, positions, 'does-not-exist')).toBeNull()
  })

  it('returns null when a link endpoint has no resolvable position', () => {
    const d = draftForTarget(
      [],
      [{ pk: 'l1', side_a_pk: 'd2', side_z_pk: 'missing', changeState: 'added', changeId: 'c3' }]
    )
    expect(changeGeoTargetById(d, positions, 'c3')).toBeNull()
  })
})
