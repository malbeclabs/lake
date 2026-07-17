import { calculateDevicePosition, calculateCurvedPath } from './geo'
import { linkChangeStyle } from './change-styles'
import type { DraftTopology, DraftDevice } from './draft'
import type { RefSnapshot } from '@/lib/api'

// Human-readable identity captured when staging a remove_device change. ref_snapshot
// must hold codes (not pks) so it stays readable after the pk is removed; resolve the
// metro's code from the draft, falling back to the pk only if the metro is missing.
export function buildRemoveDeviceSnapshot(
  draft: DraftTopology,
  dev: DraftDevice
): RefSnapshot {
  return {
    device_code: dev.code,
    metro_code: draft.metros.find((m) => m.pk === dev.metro_pk)?.code ?? dev.metro_pk,
  }
}

// Device positions keyed by device pk / local_ref, ringed around their metro.
export function buildDevicePositions(draft: DraftTopology): Map<string, [number, number]> {
  const metroByPk = new Map(draft.metros.map((m) => [m.pk, m]))
  const byMetro = new Map<string, string[]>()
  for (const d of draft.devices) {
    if (!d.metro_pk) continue
    const arr = byMetro.get(d.metro_pk) ?? []
    arr.push(d.pk)
    byMetro.set(d.metro_pk, arr)
  }
  const positions = new Map<string, [number, number]>()
  for (const [metroPk, deviceKeys] of byMetro) {
    const metro = metroByPk.get(metroPk)
    if (!metro) continue
    deviceKeys.forEach((key, i) => {
      positions.set(
        key,
        calculateDevicePosition(metro.latitude, metro.longitude, i, deviceKeys.length)
      )
    })
  }
  return positions
}

export function buildLinkFeatures(
  draft: DraftTopology,
  positions: Map<string, [number, number]>,
  isDark: boolean,
  selectedKey: string | null
): GeoJSON.FeatureCollection {
  const features: GeoJSON.Feature[] = []
  for (const link of draft.links) {
    const start = positions.get(link.side_a_pk)
    const end = positions.get(link.side_z_pk)
    if (!start || !end) continue
    const style = linkChangeStyle(link.changeState, isDark)
    const key = link.localRef ?? link.pk
    features.push({
      type: 'Feature',
      properties: {
        pk: key,
        code: link.code,
        color: style.color,
        weight: style.weight,
        opacity: style.opacity,
        useDash: style.dashed,
        isSelected: (selectedKey === key ? 1 : 0) as number,
        latencyUs: link.latency_us ?? 0,
      },
      geometry: {
        type: 'LineString',
        coordinates: calculateCurvedPath(start, end),
      },
    })
  }
  return { type: 'FeatureCollection', features }
}
