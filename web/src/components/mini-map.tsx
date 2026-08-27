import { useMemo } from 'react'
import MapGL, { Marker } from 'react-map-gl/maplibre'
import 'maplibre-gl/dist/maplibre-gl.css'
import { useTheme } from '@/hooks/use-theme'
import { createBasemapStyle } from '@/lib/basemap'

interface MiniMapProps {
  lat: number
  lng: number
  zoom?: number
  googleMapsHref?: string
}

export function MiniMap({ lat, lng, zoom = 7, googleMapsHref }: MiniMapProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const mapStyle = useMemo(() => createBasemapStyle(isDark), [isDark])

  return (
    <div className="relative w-full h-full">
      <MapGL
        initialViewState={{ longitude: lng, latitude: lat, zoom }}
        mapStyle={mapStyle}
        style={{ width: '100%', height: '100%' }}
        attributionControl={false}
        interactive={false}
      >
        <Marker longitude={lng} latitude={lat} anchor="bottom">
          <svg viewBox="0 0 24 24" className="w-6 h-6 drop-shadow" fill="currentColor">
            <path
              className="text-red-500"
              fill="currentColor"
              d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5S10.62 6.5 12 6.5s2.5 1.12 2.5 2.5S13.38 11.5 12 11.5z"
            />
          </svg>
        </Marker>
      </MapGL>
      {googleMapsHref && (
        <a
          href={googleMapsHref}
          target="_blank"
          rel="noopener noreferrer"
          className="absolute inset-0"
          aria-label="Open in Google Maps"
        />
      )}
    </div>
  )
}
