import type { StyleSpecification } from 'maplibre-gl'
import { getCachedConfig } from '@/lib/api'

// CARTO basemap tiles. Every map surface reads its style from here so the URL —
// host, style names, and the API key — lives in exactly one place. Changing the
// host means changing the CSP `connect-src` in api/main.go with it.

const ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

/**
 * Tile URL template for the light or dark basemap.
 *
 * Takes the key as an argument rather than reading config, so it stays pure and
 * testable. Without a key CARTO returns HTTP 200 with "API KEY REQUIRED" burned
 * into the PNG — a valid tile as far as MapLibre is concerned, so the failure is
 * visible only in the rendered map.
 */
export function basemapTileUrl(isDark: boolean, apiKey?: string): string {
  const style = isDark ? 'dark_all' : 'light_all'
  const url = `https://a.basemaps.cartocdn.com/${style}/{z}/{x}/{y}.png`
  return apiKey ? `${url}?key=${encodeURIComponent(apiKey)}` : url
}

let warnedMissingKey = false

/** MapLibre style for the CARTO basemap, keyed from the startup config. */
export function createBasemapStyle(isDark: boolean): StyleSpecification {
  const apiKey = getCachedConfig()?.cartoApiKey
  if (!apiKey && !warnedMissingKey) {
    warnedMissingKey = true
    console.warn(
      'No CARTO API key: /api/config carried none (set CARTO_API_KEY on the API) or the startup config fetch failed. Basemap tiles will render an "API KEY REQUIRED" watermark.',
    )
  }
  return {
    version: 8,
    sources: {
      carto: {
        type: 'raster',
        tiles: [basemapTileUrl(isDark, apiKey)],
        tileSize: 256,
        attribution: ATTRIBUTION,
      },
    },
    layers: [{ id: 'carto-tiles', type: 'raster', source: 'carto', minzoom: 0, maxzoom: 22 }],
  }
}
