import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { basemapTileUrl, createBasemapStyle, setBasemapApiKey } from './basemap'
import { setEnv } from './api'

const ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

const tilesOf = (isDark: boolean) =>
  (createBasemapStyle(isDark).sources.carto as { tiles: string[] }).tiles

beforeEach(() => {
  setBasemapApiKey(undefined)
})

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
  it('uses the key snapshotted at startup', () => {
    setBasemapApiKey('k')
    expect(tilesOf(true)).toEqual(['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png?key=k'])
  })

  it('still renders a map when no key was configured', () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    expect(tilesOf(false)).toEqual(['https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'])
  })

  // Regression: the key used to be read live from the config cache, which
  // setEnv nulls. EnvProvider calls setEnv during its first-render initializer
  // for any ?env= URL and does not reload, so every map on such a load rendered
  // the watermark this module exists to prevent.
  it('keeps the key after setEnv invalidates the config cache', () => {
    setBasemapApiKey('k')
    setEnv('testnet')
    expect(tilesOf(true)).toEqual(['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png?key=k'])
  })

  // Pins the source id, layer id, tile size and attribution the three map
  // components used before they shared this module.
  it('keeps the source and layer the components were built against', () => {
    setBasemapApiKey('k')
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
