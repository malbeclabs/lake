// Pure decision logic for PlannerMap's map-click and rubber-band behavior, split out
// so the tool-routing rules (which map clicks are allowed to select a link, how
// add-link source/target picks snap to a device) can be unit tested without
// mounting react-map-gl/maplibre-gl, which isn't practical in jsdom.
import { nearestKeyWithin, SNAP_RADIUS_DEG } from './geo'
import type { PlannerTool } from './planner-reducer'

export type MapClickAction =
  | { kind: 'place-device' }
  | { kind: 'add-link-pick'; deviceKey: string }
  | { kind: 'select-link'; linkPk: string }
  | { kind: 'deselect-link' }
  | { kind: 'ignore' }

// Decide what a click on the map should do for the active tool. This is the single
// gate that keeps tool actions (add-device / add-link / remove-device) from ALSO
// selecting or deselecting a link underneath the click -- only the plain
// select/explore tool ever touches link selection.
export function resolveMapClick(params: {
  tool: PlannerTool
  addLinkSource: string | null
  addLinkTarget: string | null
  lng: number
  lat: number
  positions: Map<string, [number, number]>
  linkFeaturePk: string | null
}): MapClickAction {
  const { tool, addLinkSource, addLinkTarget, lng, lat, positions, linkFeaturePk } = params

  if (tool === 'add-device') return { kind: 'place-device' }

  if (tool === 'add-link') {
    // The confirm form is already up once both ends are picked -- a further map
    // click must not reopen or overwrite the in-progress pick.
    if (addLinkTarget) return { kind: 'ignore' }
    // Geo-snap to the nearest device (same helper + radius as endpoint-drag
    // reattachment) instead of requiring a pixel-perfect hit on the device dot.
    // Once a source is picked, exclude it from candidates so a second click near
    // it can never re-pick it as the target (mirrors the drag-end exclusion of a
    // link's own endpoints).
    let candidates = positions
    if (addLinkSource) {
      candidates = new Map(positions)
      candidates.delete(addLinkSource)
    }
    const snapped = nearestKeyWithin(lng, lat, candidates, SNAP_RADIUS_DEG)
    return snapped ? { kind: 'add-link-pick', deviceKey: snapped } : { kind: 'ignore' }
  }

  if (tool === 'remove-device') {
    // Removal is staged from the device marker's own click handler, which knows the
    // exact device pk -- a map click here must never also select a link.
    return { kind: 'ignore' }
  }

  return linkFeaturePk ? { kind: 'select-link', linkPk: linkFeaturePk } : { kind: 'deselect-link' }
}

// Rubber-band line shown while picking an add-link source/target. Empty once a
// target is already chosen (the confirm form is up) so the dashed line stops
// following the cursor instead of sticking around under the form.
export function computeRubberBand(params: {
  tool: PlannerTool
  addLinkSource: string | null
  addLinkTarget: string | null
  cursor: [number, number] | null
  positions: Map<string, [number, number]>
}): GeoJSON.FeatureCollection {
  const { tool, addLinkSource, addLinkTarget, cursor, positions } = params
  if (tool !== 'add-link' || !addLinkSource || addLinkTarget || !cursor) {
    return { type: 'FeatureCollection', features: [] }
  }
  const start = positions.get(addLinkSource)
  if (!start) return { type: 'FeatureCollection', features: [] }
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        properties: {},
        geometry: { type: 'LineString', coordinates: [start, cursor] },
      },
    ],
  }
}
