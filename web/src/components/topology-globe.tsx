import { useMemo, useEffect, useRef, useState, useCallback } from 'react'
import Globe from 'react-globe.gl'
import type { GlobeInstance } from 'react-globe.gl'
import type { TopologyMetro, TopologyDevice, TopologyLink, TopologyValidator } from '@/lib/api'
import { useTopology, TopologyControlBar, TopologyPanel, MetroDetails } from '@/components/topology'
import type { MetroInfo } from '@/components/topology'

interface TopologyGlobeProps {
  metros: TopologyMetro[]
  devices: TopologyDevice[]
  links: TopologyLink[]
  validators: TopologyValidator[]
}

interface MetroPoint {
  id: string
  code: string
  name: string
  lat: number
  lng: number
  radius: number
  deviceCount: number
}

interface MetroArc {
  id: string
  startLat: number
  startLng: number
  endLat: number
  endLng: number
  linkCount: number
  avgLatencyUs: number
  totalBandwidthBps: number
}

// Log-scale radius for metro points based on device count
function metroRadius(deviceCount: number): number {
  if (deviceCount <= 0) return 0.3
  // Range: 0.3 (1 device) to 1.2 (many devices)
  return 0.3 + Math.min(0.9, Math.log2(deviceCount + 1) * 0.2)
}

// Arc stroke width based on link count
function arcStroke(linkCount: number): number {
  // Range: 0.2 (1 link) to 1.5 (many links)
  return 0.2 + Math.min(1.3, Math.log2(linkCount + 1) * 0.3)
}

// Arc dash animation speed based on avg latency
// Lower latency = faster animation (shorter time = faster dashes)
// Range: ~600ms (very low latency) to ~3000ms (high latency)
function arcAnimateTime(avgLatencyUs: number): number {
  const latencyMs = avgLatencyUs / 1000
  // Clamp to reasonable range: 1ms to 200ms
  const clamped = Math.max(1, Math.min(200, latencyMs))
  // Map logarithmically: low latency → fast (600ms), high latency → slow (3000ms)
  return 600 + (Math.log(clamped) / Math.log(200)) * 2400
}

export function TopologyGlobe({ metros, devices, links }: TopologyGlobeProps) {
  const { selection, setSelection, openPanel, panel } = useTopology()
  const globeRef = useRef<GlobeInstance | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 })
  const interactionTimer = useRef<ReturnType<typeof setTimeout>>(undefined)

  // Track dimensions with ResizeObserver
  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0]
      if (entry) {
        setDimensions({
          width: entry.contentRect.width,
          height: entry.contentRect.height,
        })
      }
    })

    observer.observe(el)
    // Set initial size
    setDimensions({ width: el.clientWidth, height: el.clientHeight })

    return () => observer.disconnect()
  }, [])

  // Enable auto-rotation after globe mounts
  useEffect(() => {
    const globe = globeRef.current
    if (!globe) return
    const timer = setTimeout(() => {
      const controls = globe.controls()
      controls.autoRotate = true
      controls.autoRotateSpeed = 0.3
    }, 1000)
    return () => clearTimeout(timer)
  }, [])

  // Pause auto-rotation on interaction, resume after 5s
  const handleInteraction = useCallback(() => {
    const globe = globeRef.current
    if (!globe) return
    const controls = globe.controls()
    controls.autoRotate = false
    if (interactionTimer.current) clearTimeout(interactionTimer.current)
    interactionTimer.current = setTimeout(() => {
      controls.autoRotate = true
    }, 5000)
  }, [])

  // Attach mouse/touch listeners for interaction detection
  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    el.addEventListener('pointerdown', handleInteraction)
    el.addEventListener('wheel', handleInteraction)

    return () => {
      el.removeEventListener('pointerdown', handleInteraction)
      el.removeEventListener('wheel', handleInteraction)
    }
  }, [handleInteraction])

  // Build device-to-metro mapping
  const deviceMetroMap = useMemo(() => {
    const map = new Map<string, string>()
    for (const d of devices) {
      map.set(d.pk, d.metro_pk)
    }
    return map
  }, [devices])

  // Build metro lookup
  const metroLookup = useMemo(() => {
    const map = new Map<string, TopologyMetro>()
    for (const m of metros) {
      map.set(m.pk, m)
    }
    return map
  }, [metros])

  // Count devices per metro
  const deviceCountByMetro = useMemo(() => {
    const counts = new Map<string, number>()
    for (const d of devices) {
      counts.set(d.metro_pk, (counts.get(d.metro_pk) ?? 0) + 1)
    }
    return counts
  }, [devices])

  // Metro points
  const metroPoints: MetroPoint[] = useMemo(() => {
    return metros.map(m => ({
      id: m.pk,
      code: m.code,
      name: m.name,
      lat: m.latitude,
      lng: m.longitude,
      radius: metroRadius(deviceCountByMetro.get(m.pk) ?? 0),
      deviceCount: deviceCountByMetro.get(m.pk) ?? 0,
    }))
  }, [metros, deviceCountByMetro])

  // Metro arcs: aggregate links by metro pair (skip intra-metro)
  const metroArcs: MetroArc[] = useMemo(() => {
    const arcMap = new Map<string, { linkCount: number; totalLatency: number; totalBandwidth: number }>()

    for (const link of links) {
      const metroA = deviceMetroMap.get(link.side_a_pk)
      const metroB = deviceMetroMap.get(link.side_z_pk)
      if (!metroA || !metroB || metroA === metroB) continue

      // Canonical key (sorted to avoid duplicates)
      const key = metroA < metroB ? `${metroA}:${metroB}` : `${metroB}:${metroA}`
      const existing = arcMap.get(key) ?? { linkCount: 0, totalLatency: 0, totalBandwidth: 0 }
      existing.linkCount++
      existing.totalLatency += link.latency_us
      existing.totalBandwidth += link.bandwidth_bps
      arcMap.set(key, existing)
    }

    const arcs: MetroArc[] = []
    for (const [key, data] of arcMap) {
      const [metroPkA, metroPkB] = key.split(':')
      const mA = metroLookup.get(metroPkA)
      const mB = metroLookup.get(metroPkB)
      if (!mA || !mB) continue

      arcs.push({
        id: key,
        startLat: mA.latitude,
        startLng: mA.longitude,
        endLat: mB.latitude,
        endLng: mB.longitude,
        linkCount: data.linkCount,
        avgLatencyUs: data.totalLatency / data.linkCount,
        totalBandwidthBps: data.totalBandwidth,
      })
    }
    return arcs
  }, [links, deviceMetroMap, metroLookup])

  // Build selected metro info for details panel
  const selectedMetroInfo: MetroInfo | null = useMemo(() => {
    if (selection?.type !== 'metro') return null
    const m = metroLookup.get(selection.id)
    if (!m) return null
    return {
      pk: m.pk,
      code: m.code,
      name: m.name,
      deviceCount: deviceCountByMetro.get(m.pk) ?? 0,
    }
  }, [selection, metroLookup, deviceCountByMetro])

  // Handle metro click
  const handlePointClick = useCallback((point: object) => {
    const p = point as MetroPoint
    setSelection({ type: 'metro', id: p.id })
    openPanel('details')
  }, [setSelection, openPanel])

  // Point color: selected = blue, default = cyan
  const getPointColor = useCallback((point: object) => {
    const p = point as MetroPoint
    if (selection?.type === 'metro' && selection.id === p.id) return '#3b82f6'
    return '#00ffcc'
  }, [selection])

  // Point label
  const getPointLabel = useCallback((point: object) => {
    const p = point as MetroPoint
    return `<div style="background:rgba(0,0,0,0.8);padding:4px 8px;border-radius:4px;font-size:12px;color:#fff">
      <b>${p.code}</b> — ${p.name}<br/>
      ${p.deviceCount} device${p.deviceCount !== 1 ? 's' : ''}
    </div>`
  }, [])

  // Arc color: teal-to-blue gradient
  const getArcColor = useCallback((_arc: object) => {
    return ['rgba(0,255,204,0.6)', 'rgba(59,130,246,0.6)']
  }, [])

  // Arc label
  const getArcLabel = useCallback((arc: object) => {
    const a = arc as MetroArc
    const [pkA, pkB] = a.id.split(':')
    const mA = metroLookup.get(pkA)
    const mB = metroLookup.get(pkB)
    const latencyMs = (a.avgLatencyUs / 1000).toFixed(1)
    return `<div style="background:rgba(0,0,0,0.8);padding:4px 8px;border-radius:4px;font-size:12px;color:#fff">
      <b>${mA?.code ?? '?'} — ${mB?.code ?? '?'}</b><br/>
      ${a.linkCount} link${a.linkCount !== 1 ? 's' : ''} · ${latencyMs}ms avg
    </div>`
  }, [metroLookup])

  // Selection ring data
  const ringsData = useMemo(() => {
    if (selection?.type !== 'metro') return []
    const metro = metroLookup.get(selection.id)
    if (!metro) return []
    return [{ lat: metro.latitude, lng: metro.longitude }]
  }, [selection, metroLookup])

  return (
    <div ref={containerRef} className="absolute inset-0 bg-black">
      {dimensions.width > 0 && dimensions.height > 0 && (
        <Globe
          ref={globeRef}
          width={dimensions.width}
          height={dimensions.height}
          globeImageUrl="//unpkg.com/three-globe/example/img/earth-night.jpg"
          backgroundImageUrl="//unpkg.com/three-globe/example/img/night-sky.png"
          showAtmosphere={true}
          atmosphereColor="#1a73e8"
          atmosphereAltitude={0.2}
          animateIn={true}
          // Points layer (metros)
          pointsData={metroPoints}
          pointLat="lat"
          pointLng="lng"
          pointRadius="radius"
          pointColor={getPointColor}
          pointLabel={getPointLabel}
          pointAltitude={0.01}
          pointResolution={12}
          onPointClick={handlePointClick}
          // Arcs layer (inter-metro links)
          arcsData={metroArcs}
          arcStartLat="startLat"
          arcStartLng="startLng"
          arcEndLat="endLat"
          arcEndLng="endLng"
          arcColor={getArcColor}
          arcStroke={(d: object) => arcStroke((d as MetroArc).linkCount)}
          arcDashLength={0.4}
          arcDashGap={0.2}
          arcDashAnimateTime={(d: object) => arcAnimateTime((d as MetroArc).avgLatencyUs)}
          arcAltitudeAutoScale={0.3}
          arcLabel={getArcLabel}
          // Selection ring
          ringsData={ringsData}
          ringColor={() => '#3b82f6'}
          ringMaxRadius={3}
          ringPropagationSpeed={2}
          ringRepeatPeriod={1000}
        />
      )}

      {/* Control bar */}
      <TopologyControlBar />

      {/* Panel for metro details */}
      {panel.isOpen && panel.content === 'details' && selectedMetroInfo && (
        <TopologyPanel title={selectedMetroInfo.code}>
          <MetroDetails metro={selectedMetroInfo} />
        </TopologyPanel>
      )}
    </div>
  )
}
