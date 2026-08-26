import { describe, it, expect, vi, afterEach } from 'vitest'
import { basemapTileUrl, createBasemapStyle } from './basemap'
import * as api from './api'

const ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

afterEach(() => {
  vi.restoreAllMocks()
})

describe('basemapTileUrl', () => {
  it('appends the key when one is configured', () => {
    expect(basemapTileUrl(true, 'abc123')).toBe(
      'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png?key=abc123',
    )
  })

  it('URL-encodes the key', () => {
    expect(basemapTileUrl(false, 'a b&c')).toBe(
      'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png?key=a%20b%26c',
    )
  })

  it('matches the pre-key URL exactly when no key is configured', () => {
    expect(basemapTileUrl(true)).toBe('https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png')
    expect(basemapTileUrl(false, '')).toBe(
      'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
    )
  })

  it('leaves the {z}/{x}/{y} placeholders intact', () => {
    expect(basemapTileUrl(true, 'k')).toContain('/{z}/{x}/{y}.png')
  })
})

describe('createBasemapStyle', () => {
  it('reads the key from the startup config', () => {
    vi.spyOn(api, 'getCachedConfig').mockReturnValue({ cartoApiKey: 'k' })
    expect(createBasemapStyle(true).sources.carto).toMatchObject({
      tiles: ['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png?key=k'],
    })
  })

  it('still renders a map when no config landed', () => {
    vi.spyOn(api, 'getCachedConfig').mockReturnValue(null)
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(createBasemapStyle(false).sources.carto).toMatchObject({
      tiles: ['https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'],
    })
  })

  // Pins the source id, layer id, tile size and attribution the three map
  // components used before they shared this module.
  it('keeps the source and layer the components were built against', () => {
    vi.spyOn(api, 'getCachedConfig').mockReturnValue({ cartoApiKey: 'k' })
    const style = createBasemapStyle(true)
    expect(style.version).toBe(8)
    expect(style.sources.carto).toMatchObject({
      type: 'raster',
      tileSize: 256,
      attribution: ATTRIBUTION,
    })
    expect(style.layers).toEqual([
      { id: 'carto-tiles', type: 'raster', source: 'carto', minzoom: 0, maxzoom: 22 },
    ])
  })
})
