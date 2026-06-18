// Shared device map/globe positioning.
//
// Device markers are positioned one of two ways (#652):
//   - 'fanout'   (default): synthetic spread around the metro centroid, kept tight
//                 so co-located devices stay separated without landing in the ocean.
//   - 'facility' (toggle):  anchored at the device's real facility coordinates, with a
//                 small jitter so devices sharing a facility remain individually clickable.
//                 Devices with no facility coordinates fall back to the metro fanout.
//
// Returns positions as [lng, lat] tuples (GeoJSON order); the globe adapts to {lat, lng}.

export type DevicePositionMode = 'fanout' | 'facility'

export interface PositionableDevice {
  pk: string
  metro_pk: string
  // Facility coordinates; 0/0 means unknown (no facility coords available).
  latitude: number
  longitude: number
}

export interface PositionableMetro {
  latitude: number
  longitude: number
}

// Fanout spread around the metro centroid (~3 mi). Small enough that coastal
// metros no longer fling markers offshore, large enough to separate devices.
export const FANOUT_RADIUS_DEG = 0.04

// Jitter around a shared facility for co-located devices (~0.5 mi).
export const FACILITY_JITTER_DEG = 0.008

function hasFacilityCoords(d: PositionableDevice): boolean {
  return d.latitude !== 0 || d.longitude !== 0
}

// Distribute `total` points evenly on a circle of `radius` degrees around a center.
// A lone point sits exactly on the center. Returns [lng, lat].
function radialOffset(
  centerLat: number,
  centerLng: number,
  index: number,
  total: number,
  radius: number,
): [number, number] {
  if (total <= 1) {
    return [centerLng, centerLat]
  }
  const angle = (2 * Math.PI * index) / total
  const latOffset = radius * Math.cos(angle)
  // Correct for longitude compression away from the equator.
  const lngOffset = (radius * Math.sin(angle)) / Math.cos((centerLat * Math.PI) / 180)
  return [centerLng + lngOffset, centerLat + latOffset]
}

// Spread devices around their metro centroids and write the results into `out`.
function fanoutByMetro(
  devices: PositionableDevice[],
  metroMap: Map<string, PositionableMetro>,
  out: Map<string, [number, number]>,
): void {
  const byMetro = new Map<string, PositionableDevice[]>()
  for (const d of devices) {
    const group = byMetro.get(d.metro_pk)
    if (group) group.push(d)
    else byMetro.set(d.metro_pk, [d])
  }
  for (const [metroPk, group] of byMetro) {
    const metro = metroMap.get(metroPk)
    if (!metro) continue
    group.forEach((d, i) => {
      out.set(d.pk, radialOffset(metro.latitude, metro.longitude, i, group.length, FANOUT_RADIUS_DEG))
    })
  }
}

// Compute [lng, lat] positions for every device, keyed by device pk.
export function computeDevicePositions(
  devices: PositionableDevice[],
  metroMap: Map<string, PositionableMetro>,
  mode: DevicePositionMode,
): Map<string, [number, number]> {
  const positions = new Map<string, [number, number]>()

  if (mode === 'facility') {
    // Devices with real facility coords are jittered around the shared facility.
    const byFacility = new Map<string, PositionableDevice[]>()
    const missing: PositionableDevice[] = []
    for (const d of devices) {
      if (!hasFacilityCoords(d)) {
        missing.push(d)
        continue
      }
      const key = `${d.latitude},${d.longitude}`
      const group = byFacility.get(key)
      if (group) group.push(d)
      else byFacility.set(key, [d])
    }
    for (const group of byFacility.values()) {
      const { latitude, longitude } = group[0]
      group.forEach((d, i) => {
        positions.set(d.pk, radialOffset(latitude, longitude, i, group.length, FACILITY_JITTER_DEG))
      })
    }
    // Devices without facility coords fall back to the metro fanout.
    fanoutByMetro(missing, metroMap, positions)
    return positions
  }

  fanoutByMetro(devices, metroMap, positions)
  return positions
}
