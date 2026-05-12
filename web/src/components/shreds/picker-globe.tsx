import { useCallback, useEffect, useRef, useState } from 'react'
import Globe from 'react-globe.gl'
import type { GlobeInstance } from 'react-globe.gl'
import { Loader2 } from 'lucide-react'
import { useTheme } from '@/hooks/use-theme'

export interface PickerGlobeMetro {
  code: string
  name: string
  latitude: number
  longitude: number
  seatsFree: number
}

interface PickerGlobeProps {
  metros: PickerGlobeMetro[]
  selectedMetro: string | null
  onSelectMetro: (code: string) => void
}

// Minimal 3D globe used by the shreds picker. Renders one prominent pin per
// metro (color-coded by seat availability) and routes clicks back to the
// parent picker UX. Intentionally light on features compared to
// `<TopologyGlobe>` — no links, validators, overlays, control bar.
export function PickerGlobe({ metros, selectedMetro, onSelectMetro }: PickerGlobeProps) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const containerRef = useRef<HTMLDivElement>(null)
  const globeRef = useRef<GlobeInstance | null>(null)
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })
  const [texturesLoaded, setTexturesLoaded] = useState(false)

  // Preload textures so theme changes don't flash a network fetch.
  useEffect(() => {
    const urls = ['/textures/earth-day.jpg', '/textures/earth-night.jpg', '/textures/night-sky.jpg']
    let loaded = 0
    for (const url of urls) {
      const img = new Image()
      img.onload = img.onerror = () => { if (++loaded >= urls.length) setTexturesLoaded(true) }
      img.src = url
    }
  }, [])

  // Track container size for the Globe's explicit width/height props.
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(entries => {
      for (const e of entries) {
        setDimensions({ width: e.contentRect.width, height: e.contentRect.height })
      }
    })
    ro.observe(el)
    setDimensions({ width: el.clientWidth, height: el.clientHeight })
    return () => ro.disconnect()
  }, [])

  // Initial camera + slow auto-rotation. Stops on first user drag (handled by
  // OrbitControls — react-globe.gl listens for `start` events internally).
  const globeRefCb = useCallback((instance: GlobeInstance | null) => {
    globeRef.current = instance
    if (!instance) return
    instance.pointOfView({ lat: 30, lng: 10, altitude: 2.2 }, 0)
    const controls = instance.controls() as unknown as { autoRotate: boolean; autoRotateSpeed: number }
    controls.autoRotate = true
    controls.autoRotateSpeed = 0.3
  }, [])

  const globeImageUrl = isDark ? '/textures/earth-night.jpg' : '/textures/earth-day.jpg'

  const buildPin = useCallback((d: object) => {
    const m = d as PickerGlobeMetro
    const isSelected = m.code === selectedMetro
    const hasSeats = m.seatsFree > 0
    const dotBg = isSelected ? '#3b82f6' : hasSeats ? '#f97316' : '#6b7280'
    const ringBorder = isSelected ? '#ffffff' : 'rgba(255,255,255,0.7)'
    const glow = hasSeats ? '0 0 14px rgba(249,115,22,0.7)' : 'none'

    const wrap = document.createElement('div')
    wrap.style.cssText = 'transform: translate(-50%, -50%); pointer-events: auto; cursor: pointer; user-select: none;'
    wrap.title = `${m.name} — ${m.seatsFree} seat${m.seatsFree !== 1 ? 's' : ''} free`
    wrap.innerHTML = `
      <div style="display:flex; flex-direction:column; align-items:center; gap:3px;">
        <div style="
          width:14px; height:14px; border-radius:50%;
          background:${dotBg};
          border:2px solid ${ringBorder};
          box-shadow:0 0 0 3px rgba(0,0,0,0.35), ${glow};
        "></div>
        <div style="
          padding:1px 6px; border-radius:6px;
          background:rgba(0,0,0,0.75);
          color:white; font-size:10px; font-weight:600;
          font-family: ui-sans-serif, system-ui, sans-serif;
          letter-spacing:0.04em; white-space:nowrap;
          border:1px solid rgba(255,255,255,0.15);
        ">${m.code}</div>
      </div>
    `
    wrap.addEventListener('click', e => {
      e.stopPropagation()
      // Stop auto-rotation so the user can read the drawer / see the pin they
      // just clicked without it sliding away.
      const controls = globeRef.current?.controls() as unknown as { autoRotate: boolean } | undefined
      if (controls) controls.autoRotate = false
      onSelectMetro(m.code)
    })
    return wrap
  }, [selectedMetro, onSelectMetro])

  return (
    <div ref={containerRef} className="absolute inset-0 bg-black">
      {(!texturesLoaded || dimensions.width === 0) && (
        <div className="absolute inset-0 z-10 flex items-center justify-center bg-black">
          <Loader2 className="h-6 w-6 animate-spin text-white/60" />
        </div>
      )}
      {texturesLoaded && dimensions.width > 0 && dimensions.height > 0 && (
        <Globe
          ref={globeRefCb}
          width={dimensions.width}
          height={dimensions.height}
          globeImageUrl={globeImageUrl}
          backgroundImageUrl="/textures/night-sky.jpg"
          showAtmosphere
          atmosphereColor={isDark ? '#1a73e8' : '#6baadb'}
          atmosphereAltitude={0.18}
          animateIn
          htmlElementsData={metros}
          htmlLat={(d: object) => (d as PickerGlobeMetro).latitude}
          htmlLng={(d: object) => (d as PickerGlobeMetro).longitude}
          htmlAltitude={0.01}
          htmlElement={buildPin}
        />
      )}
    </div>
  )
}
