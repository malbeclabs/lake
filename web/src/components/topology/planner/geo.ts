// Pure geometry helpers for the planner map. Isolated copies of the position
// and curve math used by topology-map.tsx (which does not export them).

export function calculateDevicePosition(
  metroLat: number,
  metroLng: number,
  deviceIndex: number,
  totalDevices: number
): [number, number] {
  if (totalDevices <= 1) return [metroLng, metroLat]
  const radius = 0.3
  const angle = (2 * Math.PI * deviceIndex) / totalDevices
  const latOffset = radius * Math.cos(angle)
  const lngOffset = (radius * Math.sin(angle)) / Math.cos((metroLat * Math.PI) / 180)
  return [metroLng + lngOffset, metroLat + latOffset]
}

export function calculateCurvedPath(
  start: [number, number],
  end: [number, number],
  curveOffset = 0.15
): [number, number][] {
  const startLng = start[0]
  let endLng = end[0]
  const lngDelta = endLng - startLng
  if (Math.abs(lngDelta) > 180) {
    endLng = lngDelta > 0 ? endLng - 360 : endLng + 360
  }
  const midLng = (startLng + endLng) / 2
  const midLat = (start[1] + end[1]) / 2
  const dx = endLng - startLng
  const dy = end[1] - start[1]
  const length = Math.sqrt(dx * dx + dy * dy)
  if (length === 0) return [start, end]
  const controlLng = midLng - (dy / length) * curveOffset * length
  const controlLat = midLat + (dx / length) * curveOffset * length
  const points: [number, number][] = []
  const segments = 20
  for (let i = 0; i <= segments; i++) {
    const t = i / segments
    const lng =
      (1 - t) * (1 - t) * startLng + 2 * (1 - t) * t * controlLng + t * t * endLng
    const lat =
      (1 - t) * (1 - t) * start[1] + 2 * (1 - t) * t * controlLat + t * t * end[1]
    points.push([lng, lat])
  }
  return points
}

export function haversineKm(
  aLat: number,
  aLng: number,
  bLat: number,
  bLng: number
): number {
  const R = 6371
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(bLat - aLat)
  const dLng = toRad(bLng - aLng)
  const s =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLng / 2) ** 2
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s)))
}

// Nearest map key to a lng/lat, using degree distance, within maxDeg. For snap.
export function nearestKeyWithin(
  lng: number,
  lat: number,
  positions: Map<string, [number, number]>,
  maxDeg: number
): string | null {
  let best: string | null = null
  let bestDist = maxDeg
  for (const [key, [plng, plat]] of positions) {
    const d = Math.sqrt((plng - lng) ** 2 + (plat - lat) ** 2)
    if (d <= bestDist) {
      bestDist = d
      best = key
    }
  }
  return best
}
