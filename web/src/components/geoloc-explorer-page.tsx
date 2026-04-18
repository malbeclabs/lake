import { useMemo, useState, useCallback } from 'react'
import MapGL, { Source, Layer } from 'react-map-gl/maplibre'
import type { MapLayerMouseEvent } from 'react-map-gl/maplibre'
import type { StyleSpecification } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useQuery } from '@tanstack/react-query'
import { useTheme } from '@/hooks/use-theme'
import { Loader2, AlertCircle } from 'lucide-react'
import { fetchGeolocExplorer } from '@/lib/api'
import type { GeolocExplorerOffset } from '@/lib/api'

/* ------------------------------------------------------------------ */
/*  Map style                                                         */
/* ------------------------------------------------------------------ */

function createMapStyle(isDark: boolean): StyleSpecification {
  const tileUrl = isDark
    ? 'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
    : 'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'
  return {
    version: 8,
    sources: {
      carto: {
        type: 'raster',
        tiles: [tileUrl],
        tileSize: 256,
        attribution:
          '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      },
    },
    layers: [
      { id: 'carto-tiles', type: 'raster', source: 'carto', minzoom: 0, maxzoom: 22 },
    ],
  }
}

/* ------------------------------------------------------------------ */
/*  RTT helpers                                                       */
/* ------------------------------------------------------------------ */

const SPEED_OF_LIGHT = 299_792_458
const FIBER_FACTOR = 0.6

function rttToRadiusMeters(rttNs: number): number {
  return (FIBER_FACTOR * SPEED_OF_LIGHT * rttNs) / (2 * 1e9)
}

/* ------------------------------------------------------------------ */
/*  Great-circle polygon                                              */
/* ------------------------------------------------------------------ */

function createCirclePolygon(
  centerLng: number,
  centerLat: number,
  radiusMeters: number,
  points = 32,
): GeoJSON.Feature<GeoJSON.Polygon> {
  const coords: [number, number][] = []
  const earthRadius = 6_371_000
  const angularRadius = radiusMeters / earthRadius
  const centerLatRad = (centerLat * Math.PI) / 180
  const centerLngRad = (centerLng * Math.PI) / 180

  for (let i = 0; i <= points; i++) {
    const bearing = (2 * Math.PI * i) / points
    const lat = Math.asin(
      Math.sin(centerLatRad) * Math.cos(angularRadius) +
        Math.cos(centerLatRad) * Math.sin(angularRadius) * Math.cos(bearing),
    )
    const lng =
      centerLngRad +
      Math.atan2(
        Math.sin(bearing) * Math.sin(angularRadius) * Math.cos(centerLatRad),
        Math.cos(angularRadius) - Math.sin(centerLatRad) * Math.sin(lat),
      )
    coords.push([(lng * 180) / Math.PI, (lat * 180) / Math.PI])
  }

  return {
    type: 'Feature',
    properties: {},
    geometry: { type: 'Polygon', coordinates: [coords] },
  }
}

/* ------------------------------------------------------------------ */
/*  Component                                                         */
/* ------------------------------------------------------------------ */

export function GeolocExplorerPage() {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'

  const [hoverInfo, setHoverInfo] = useState<{
    x: number; y: number; probeCode: string
    targetIP: string; rttNs: number; measuredRttNs: number
  } | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['geoloc-explorer'],
    queryFn: () => fetchGeolocExplorer(),
    refetchInterval: 30_000,
  })

  const offsets = data?.offsets ?? []
  const mapStyle = useMemo(() => createMapStyle(isDark), [isDark])

  // Probe points (deduplicated by sender_pubkey)
  const probePointsGeoJSON = useMemo(() => {
    const seen = new Map<string, GeolocExplorerOffset>()
    for (const o of offsets) {
      if (!seen.has(o.sender_pubkey)) seen.set(o.sender_pubkey, o)
    }
    return {
      type: 'FeatureCollection' as const,
      features: Array.from(seen.values()).map((o) => ({
        type: 'Feature' as const,
        properties: {
          probe_code: o.probe_code || o.sender_pubkey.slice(0, 8),
          sender_pubkey: o.sender_pubkey,
        },
        geometry: { type: 'Point' as const, coordinates: [o.lng, o.lat] },
      })),
    }
  }, [offsets])

  // Geoprobe RTT circles (from ref_measured_rtt_ns[0])
  const geoProbeCirclesGeoJSON = useMemo(() => {
    const features: GeoJSON.Feature<GeoJSON.Polygon>[] = []
    for (const o of offsets) {
      if (o.ref_measured_rtt_ns && o.ref_measured_rtt_ns.length > 0) {
        const radius = rttToRadiusMeters(o.ref_measured_rtt_ns[0])
        if (radius > 0 && radius < 5_000_000) {
          const circle = createCirclePolygon(o.lng, o.lat, radius)
          circle.properties = {
            probe_code: o.probe_code || o.sender_pubkey.slice(0, 8),
            target_ip: o.target_ip,
            rtt_ns: o.ref_measured_rtt_ns[0],
            measured_rtt_ns: o.measured_rtt_ns,
            type: 'geoprobe',
          }
          features.push(circle)
        }
      }
    }
    return { type: 'FeatureCollection' as const, features }
  }, [offsets])

  // Target RTT circles (from rtt_ns)
  const targetCirclesGeoJSON = useMemo(() => {
    const features: GeoJSON.Feature<GeoJSON.Polygon>[] = []
    for (const o of offsets) {
      if (o.rtt_ns > 0) {
        const radius = rttToRadiusMeters(o.rtt_ns)
        if (radius > 0 && radius < 5_000_000) {
          const circle = createCirclePolygon(o.lng, o.lat, radius)
          circle.properties = {
            probe_code: o.probe_code || o.sender_pubkey.slice(0, 8),
            target_ip: o.target_ip,
            rtt_ns: o.rtt_ns,
            measured_rtt_ns: o.measured_rtt_ns,
            type: 'target',
          }
          features.push(circle)
        }
      }
    }
    return { type: 'FeatureCollection' as const, features }
  }, [offsets])

  const onHover = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features?.[0]
    if (feature) {
      setHoverInfo({
        x: event.point.x,
        y: event.point.y,
        probeCode: (feature.properties?.probe_code as string) ?? '',
        targetIP: (feature.properties?.target_ip as string) ?? '',
        rttNs: Number(feature.properties?.rtt_ns ?? 0),
        measuredRttNs: Number(feature.properties?.measured_rtt_ns ?? 0),
      })
    } else {
      setHoverInfo(null)
    }
  }, [])

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load explorer data</div>
          <div className="text-sm text-muted-foreground">
            {(error as Error)?.message || 'Unknown error'}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 relative">
      <MapGL
        initialViewState={{ longitude: 0, latitude: 20, zoom: 2 }}
        style={{ width: '100%', height: '100%' }}
        mapStyle={mapStyle}
        interactiveLayerIds={['geoprobe-circles-fill', 'target-circles-fill']}
        onMouseMove={onHover}
        onMouseLeave={() => setHoverInfo(null)}
      >
        {/* Target RTT circles (rendered first, behind geoprobe circles) */}
        <Source id="target-circles" type="geojson" data={targetCirclesGeoJSON}>
          <Layer id="target-circles-fill" type="fill"
            paint={{ 'fill-color': '#f97316', 'fill-opacity': 0.08 }} />
          <Layer id="target-circles-line" type="line"
            paint={{ 'line-color': '#f97316', 'line-width': 1, 'line-opacity': 0.4 }} />
        </Source>

        {/* Geoprobe RTT circles */}
        <Source id="geoprobe-circles" type="geojson" data={geoProbeCirclesGeoJSON}>
          <Layer id="geoprobe-circles-fill" type="fill"
            paint={{ 'fill-color': '#3b82f6', 'fill-opacity': 0.1 }} />
          <Layer id="geoprobe-circles-line" type="line"
            paint={{ 'line-color': '#3b82f6', 'line-width': 1, 'line-opacity': 0.5 }} />
        </Source>

        {/* Probe point markers */}
        <Source id="probe-points" type="geojson" data={probePointsGeoJSON}>
          <Layer id="probe-points-circle" type="circle"
            paint={{
              'circle-radius': 5,
              'circle-color': '#22c55e',
              'circle-stroke-width': 2,
              'circle-stroke-color': isDark ? '#1a1a2e' : '#ffffff',
            }} />
          <Layer id="probe-points-label" type="symbol"
            layout={{
              'text-field': ['get', 'probe_code'],
              'text-size': 11,
              'text-offset': [0, 1.5],
              'text-anchor': 'top',
            }}
            paint={{
              'text-color': isDark ? '#e2e8f0' : '#1e293b',
              'text-halo-color': isDark ? '#0f172a' : '#ffffff',
              'text-halo-width': 1,
            }} />
        </Source>
      </MapGL>

      {/* Empty state overlay */}
      {offsets.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <div className="bg-card/90 backdrop-blur-sm border border-border rounded-lg px-6 py-4 text-center">
            <div className="text-sm text-muted-foreground">No location offset data available</div>
          </div>
        </div>
      )}

      {/* Hover tooltip */}
      {hoverInfo && (
        <div
          className="absolute z-10 pointer-events-none bg-popover border border-border rounded-lg px-3 py-2 shadow-lg text-sm"
          style={{ left: hoverInfo.x + 12, top: hoverInfo.y - 12 }}
        >
          <div className="font-medium">{hoverInfo.probeCode}</div>
          {hoverInfo.targetIP && (
            <div className="text-muted-foreground">Target: {hoverInfo.targetIP}</div>
          )}
          <div className="text-muted-foreground">
            RTT: {(hoverInfo.rttNs / 1000).toFixed(1)} us
            {' · '}
            Radius: {(rttToRadiusMeters(hoverInfo.rttNs) / 1000).toFixed(1)} km
          </div>
        </div>
      )}
    </div>
  )
}
