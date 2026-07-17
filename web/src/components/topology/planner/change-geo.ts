// Pure geometry helpers for map camera navigation: fitting the view to a plan's
// changed region on open, and resolving a single change to a fly-to target.
import type { DraftTopology } from './draft'
import { collectChangedEntities } from './plan-preview'

// A maplibre-compatible bounds tuple [[west, south], [east, north]] (lng/lat).
export type LngLatBoundsTuple = [[number, number], [number, number]]

// Bounding box over every changed device + changed-link endpoint, or null when the
// plan touches nothing (so the caller can skip the fit). Reuses collectChangedEntities
// so the fit region matches exactly what the preview/map draw as "changed".
export function changedEntitiesBounds(draft: DraftTopology): LngLatBoundsTuple | null {
  const { devices, links } = collectChangedEntities(draft)
  const lngs: number[] = []
  const lats: number[] = []
  for (const d of devices) {
    lngs.push(d.lng)
    lats.push(d.lat)
  }
  for (const l of links) {
    lngs.push(l.a.lng, l.z.lng)
    lats.push(l.a.lat, l.z.lat)
  }
  if (lngs.length === 0) return null
  return [
    [Math.min(...lngs), Math.min(...lats)],
    [Math.max(...lngs), Math.max(...lats)],
  ]
}

// The [lng, lat] the map should fly to for one change, resolved by matching the
// change id to the draft entity it created/modified: a device change -> the device
// position; a link change -> the midpoint of the link's two endpoint positions.
// Returns null if the change's entity or its position can't be resolved (e.g. a
// removed entity whose metro is gone) so the caller can no-op gracefully.
export function changeGeoTargetById(
  draft: DraftTopology,
  positions: Map<string, [number, number]>,
  changeId: string
): [number, number] | null {
  const device = draft.devices.find((d) => d.changeId === changeId)
  if (device) {
    return positions.get(device.pk) ?? null
  }

  const link = draft.links.find((l) => l.changeId === changeId)
  if (link) {
    const a = positions.get(link.side_a_pk)
    const z = positions.get(link.side_z_pk)
    if (!a || !z) return null
    return [(a[0] + z[0]) / 2, (a[1] + z[1]) / 2]
  }

  return null
}
