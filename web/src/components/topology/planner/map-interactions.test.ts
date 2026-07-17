import { describe, it, expect } from 'vitest'
import { resolveMapClick, computeRubberBand } from './map-interactions'

describe('resolveMapClick', () => {
  const positions = new Map<string, [number, number]>([
    ['dev-a', [0, 0]],
    ['dev-b', [10, 10]],
    ['dev-c', [11, 11]],
  ])

  it('add-device tool always places a device, even with a link feature under the click', () => {
    const action = resolveMapClick({
      tool: 'add-device',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 0,
      lat: 0,
      positions,
      linkFeaturePk: 'link-1',
    })
    expect(action).toEqual({ kind: 'place-device' })
  })

  it('remove-device tool never selects or deselects a link', () => {
    const action = resolveMapClick({
      tool: 'remove-device',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 0.05,
      lat: 0.05,
      positions,
      linkFeaturePk: 'link-1',
    })
    expect(action).toEqual({ kind: 'ignore' })
  })

  it('add-link tool never selects or deselects a link, even with a link feature under the click', () => {
    const action = resolveMapClick({
      tool: 'add-link',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 0.05,
      lat: 0.05,
      positions,
      linkFeaturePk: 'link-1',
    })
    expect(action.kind).not.toBe('select-link')
    expect(action.kind).not.toBe('deselect-link')
  })

  it('add-link tool snaps the first click to the nearest device as the source', () => {
    const action = resolveMapClick({
      tool: 'add-link',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 0.1,
      lat: 0.1,
      positions,
      linkFeaturePk: null,
    })
    expect(action).toEqual({ kind: 'add-link-pick', deviceKey: 'dev-a' })
  })

  it('add-link tool snaps the second click to the nearest OTHER device as the target', () => {
    const action = resolveMapClick({
      tool: 'add-link',
      addLinkSource: 'dev-a',
      addLinkTarget: null,
      lng: 10.1,
      lat: 10.1,
      positions,
      linkFeaturePk: null,
    })
    expect(action).toEqual({ kind: 'add-link-pick', deviceKey: 'dev-b' })
  })

  it('excludes the already-picked source from target candidates, even if it is closest', () => {
    const action = resolveMapClick({
      tool: 'add-link',
      addLinkSource: 'dev-a',
      addLinkTarget: null,
      lng: 0,
      lat: 0,
      positions,
      linkFeaturePk: null,
    })
    // Every other device is far outside the snap radius of (0,0), so with dev-a
    // excluded there is nothing left to snap to.
    expect(action).toEqual({ kind: 'ignore' })
  })

  it('ignores further clicks once a target is already chosen', () => {
    const action = resolveMapClick({
      tool: 'add-link',
      addLinkSource: 'dev-a',
      addLinkTarget: 'dev-b',
      lng: 11,
      lat: 11,
      positions,
      linkFeaturePk: null,
    })
    expect(action).toEqual({ kind: 'ignore' })
  })

  it('ignores a click with no device within the snap radius', () => {
    const action = resolveMapClick({
      tool: 'add-link',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 50,
      lat: 50,
      positions,
      linkFeaturePk: null,
    })
    expect(action).toEqual({ kind: 'ignore' })
  })

  it('select tool selects the link under the click', () => {
    const action = resolveMapClick({
      tool: 'select',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 0,
      lat: 0,
      positions,
      linkFeaturePk: 'link-1',
    })
    expect(action).toEqual({ kind: 'select-link', linkPk: 'link-1' })
  })

  it('select tool deselects when the click hits no link', () => {
    const action = resolveMapClick({
      tool: 'select',
      addLinkSource: null,
      addLinkTarget: null,
      lng: 0,
      lat: 0,
      positions,
      linkFeaturePk: null,
    })
    expect(action).toEqual({ kind: 'deselect-link' })
  })
})

describe('computeRubberBand', () => {
  const positions = new Map<string, [number, number]>([['dev-a', [0, 0]]])

  it('is empty outside add-link tool', () => {
    const fc = computeRubberBand({
      tool: 'select',
      addLinkSource: 'dev-a',
      addLinkTarget: null,
      cursor: [1, 1],
      positions,
    })
    expect(fc.features).toHaveLength(0)
  })

  it('is empty until a source is picked', () => {
    const fc = computeRubberBand({
      tool: 'add-link',
      addLinkSource: null,
      addLinkTarget: null,
      cursor: [1, 1],
      positions,
    })
    expect(fc.features).toHaveLength(0)
  })

  it('follows the cursor once a source is picked', () => {
    const fc = computeRubberBand({
      tool: 'add-link',
      addLinkSource: 'dev-a',
      addLinkTarget: null,
      cursor: [1, 1],
      positions,
    })
    expect(fc.features).toHaveLength(1)
    expect(fc.features[0].geometry).toEqual({
      type: 'LineString',
      coordinates: [
        [0, 0],
        [1, 1],
      ],
    })
  })

  it('stops following the cursor once a target is chosen', () => {
    const fc = computeRubberBand({
      tool: 'add-link',
      addLinkSource: 'dev-a',
      addLinkTarget: 'dev-b',
      cursor: [1, 1],
      positions,
    })
    expect(fc.features).toHaveLength(0)
  })
})
