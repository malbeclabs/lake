import type { StyleSpecification } from 'maplibre-gl'

// CARTO basemap tiles. Every map surface reads its style from here so the URL —
// host, style names, and the API key — lives in exactly one place. Changing the
// host means changing the CSP `connect-src` in api/main.go with it.

const ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

/**
 * Tile URL template for the light or dark basemap.
 *
 * Takes the key as an argument rather than reading module state, so it stays
 * pure and testable. Without a key CARTO returns HTTP 200 with "API KEY
 * REQUIRED" burned into the PNG — a valid tile as far as MapLibre is concerned,
 * so the failure is visible only in the rendered map.
 */
export function basemapTileUrl(isDark: boolean, apiKey?: string): string {
  const style = isDark ? 'dark_all' : 'light_all'
  const url = `https://a.basemaps.cartocdn.com/${style}/{z}/{x}/{y}.png`
  return apiKey ? `${url}?key=${encodeURIComponent(apiKey)}` : url
}

let basemapApiKey: string | undefined
let warnedMissingKey = false

/**
 * Snapshot the CARTO key from the startup config. Called once from main.tsx
 * before React mounts.
 *
 * Deliberately a snapshot rather than a read of the config cache: `setEnv`
 * nulls that cache, and `EnvProvider` calls it during its first-render
 * initializer for any `?env=` URL without reloading. Reading it live left every
 * map on such a load unkeyed — and permanently so, since the style is memoized
 * on the theme. The key is the same for every environment, so it has nothing to
 * re-fetch.
 */
export function setBasemapApiKey(key: string | undefined): void {
  basemapApiKey = key
}

/** MapLibre style for the CARTO basemap, keyed from the startup config. */
export function createBasemapStyle(isDark: boolean): StyleSpecification {
  if (!basemapApiKey && !warnedMissingKey) {
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
        tiles: [basemapTileUrl(isDark, basemapApiKey)],
        tileSize: 256,
        attribution: ATTRIBUTION,
      },
    },
    layers: [{ id: 'carto-tiles', type: 'raster', source: 'carto', minzoom: 0, maxzoom: 22 }],
  }
}
