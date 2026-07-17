// Pure geometry helpers for the plan landing preview thumbnails. These render a
// plan's changes as a tiny SVG (no MapLibre instance -- browsers cap WebGL
// contexts around ~16, and a landing page can list far more plans than that).
import type { PlanChange, TopologyResponse } from '@/lib/api'
import { buildDraft, type DraftTopology, type EntityChangeState } from './draft'
import { buildDevicePositions } from './map-geojson'

export interface ChangedDevice {
  key: string
  lat: number
  lng: number
  state: EntityChangeState
}

export interface ChangedLink {
  key: string
  state: EntityChangeState
  a: { lat: number; lng: number }
  z: { lat: number; lng: number }
}

export interface ChangedEntities {
  devices: ChangedDevice[]
  links: ChangedLink[]
}

// Collects the devices and links a plan actually touches: added/removed/modified
// devices, plus BOTH endpoints of every added/removed/modified link (even when an
// endpoint device itself is unchanged, e.g. an existing device on one end of a
// newly added link) so the preview always anchors on real map positions.
// Device positions reuse buildDevicePositions (ringed around their metro), the
// same layout PlannerMap draws, so a thumbnail matches the full map's geometry.
export function collectChangedEntities(draft: DraftTopology): ChangedEntities {
  const positions = buildDevicePositions(draft)

  const devices: ChangedDevice[] = []
  for (const d of draft.devices) {
    if (d.changeState === 'unchanged') continue
    const pos = positions.get(d.pk)
    if (!pos) continue
    devices.push({ key: d.pk, lng: pos[0], lat: pos[1], state: d.changeState })
  }

  const links: ChangedLink[] = []
  for (const l of draft.links) {
    if (l.changeState === 'unchanged') continue
    const a = positions.get(l.side_a_pk)
    const z = positions.get(l.side_z_pk)
    if (!a || !z) continue
    links.push({
      key: l.pk,
      state: l.changeState,
      a: { lng: a[0], lat: a[1] },
      z: { lng: z[0], lat: z[1] },
    })
  }

  return { devices, links }
}

export interface BBox {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}

const DEFAULT_PADDING_DEG = 2

// Lat/lng bounding box over every changed device + changed-link endpoint,
// expanded by paddingDeg on each side so points don't sit flush on the thumbnail
// edge. Returns null when there is nothing to show (plan has no live changes).
export function changedEntitiesBBox(
  entities: ChangedEntities,
  paddingDeg: number = DEFAULT_PADDING_DEG
): BBox | null {
  const lats: number[] = []
  const lngs: number[] = []
  for (const d of entities.devices) {
    lats.push(d.lat)
    lngs.push(d.lng)
  }
  for (const l of entities.links) {
    lats.push(l.a.lat, l.z.lat)
    lngs.push(l.a.lng, l.z.lng)
  }
  if (lats.length === 0) return null

  return {
    minLat: Math.min(...lats) - paddingDeg,
    maxLat: Math.max(...lats) + paddingDeg,
    minLng: Math.min(...lngs) - paddingDeg,
    maxLng: Math.max(...lngs) + paddingDeg,
  }
}

// Simple equirectangular projection (x from lng, y from lat) of a point into a
// viewW x viewH box, uniformly scaled to fit bbox (so a wide bbox doesn't stretch
// into a square) and centered within the box. North is up: higher latitude maps
// to a smaller y.
export function projectPoint(
  lat: number,
  lng: number,
  bbox: BBox,
  viewW: number,
  viewH: number
): [number, number] {
  const latSpan = bbox.maxLat - bbox.minLat || 1
  const lngSpan = bbox.maxLng - bbox.minLng || 1
  const scale = Math.min(viewW / lngSpan, viewH / latSpan)
  const offsetX = (viewW - lngSpan * scale) / 2
  const offsetY = (viewH - latSpan * scale) / 2
  const x = offsetX + (lng - bbox.minLng) * scale
  const y = offsetY + (bbox.maxLat - lat) * scale
  return [x, y]
}

export interface PreviewDevicePoint {
  key: string
  x: number
  y: number
  state: EntityChangeState
}

export interface PreviewLinkLine {
  key: string
  x1: number
  y1: number
  x2: number
  y2: number
  state: EntityChangeState
}

export interface PreviewMetroLabel {
  code: string
  x: number
  y: number
}

export interface PreviewContext {
  devices: { x: number; y: number }[]
  links: { x1: number; y1: number; x2: number; y2: number }[]
}

export interface PlanPreviewGeometry {
  devices: PreviewDevicePoint[]
  links: PreviewLinkLine[]
  metros: PreviewMetroLabel[]
  context: PreviewContext
}

// The metros the plan's changes touch: the metro of every changed device, plus the
// metros of both endpoint devices of every changed link. Deduped by metro pk.
// Position is the metro's own lat/lng (draft.metros), NOT the ringed device
// position, so the label sits on the city rather than on one specific device.
export function collectChangedMetros(
  draft: DraftTopology
): { pk: string; code: string; lat: number; lng: number }[] {
  const metroPks = new Set<string>()
  for (const d of draft.devices) {
    if (d.changeState === 'unchanged') continue
    if (d.metro_pk) metroPks.add(d.metro_pk)
  }
  for (const l of draft.links) {
    if (l.changeState === 'unchanged') continue
    const a = draft.deviceByKey.get(l.side_a_pk)
    const z = draft.deviceByKey.get(l.side_z_pk)
    if (a?.metro_pk) metroPks.add(a.metro_pk)
    if (z?.metro_pk) metroPks.add(z.metro_pk)
  }

  const result: { pk: string; code: string; lat: number; lng: number }[] = []
  for (const pk of metroPks) {
    const metro = draft.metros.find((m) => m.pk === pk)
    if (!metro) continue
    result.push({ pk: metro.pk, code: metro.code, lat: metro.latitude, lng: metro.longitude })
  }
  return result
}

// Bound on the number of context devices/links projected into a preview, so a huge
// baseline topology doesn't blow up the SVG node count for a single thumbnail.
const MAX_CONTEXT_ENTITIES = 80

function inBBox(lat: number, lng: number, bbox: BBox): boolean {
  return lat >= bbox.minLat && lat <= bbox.maxLat && lng >= bbox.minLng && lng <= bbox.maxLng
}

// Full pipeline: baseline topology + a plan's changes -> ready-to-draw SVG
// geometry sized to viewW x viewH. Returns null when the plan has no pending
// changes to preview (buildDraft only applies 'pending' changes, per SC-8), so
// callers can render a neutral "empty plan" placeholder instead.
export function buildPlanPreview(
  baseline: TopologyResponse,
  changes: PlanChange[],
  viewW: number,
  viewH: number
): PlanPreviewGeometry | null {
  const draft = buildDraft(baseline, changes)
  const entities = collectChangedEntities(draft)
  const bbox = changedEntitiesBBox(entities)
  if (!bbox) return null

  const positions = buildDevicePositions(draft)

  const contextDevices: PreviewContext['devices'] = []
  for (const d of draft.devices) {
    if (d.changeState !== 'unchanged') continue
    if (contextDevices.length >= MAX_CONTEXT_ENTITIES) break
    const pos = positions.get(d.pk)
    if (!pos) continue
    const [lng, lat] = pos
    if (!inBBox(lat, lng, bbox)) continue
    const [x, y] = projectPoint(lat, lng, bbox, viewW, viewH)
    contextDevices.push({ x, y })
  }

  const contextLinks: PreviewContext['links'] = []
  for (const l of draft.links) {
    if (l.changeState !== 'unchanged') continue
    if (contextLinks.length >= MAX_CONTEXT_ENTITIES) break
    const a = positions.get(l.side_a_pk)
    const z = positions.get(l.side_z_pk)
    if (!a || !z) continue
    if (!inBBox(a[1], a[0], bbox) || !inBBox(z[1], z[0], bbox)) continue
    const [x1, y1] = projectPoint(a[1], a[0], bbox, viewW, viewH)
    const [x2, y2] = projectPoint(z[1], z[0], bbox, viewW, viewH)
    contextLinks.push({ x1, y1, x2, y2 })
  }

  return {
    devices: entities.devices.map((d) => {
      const [x, y] = projectPoint(d.lat, d.lng, bbox, viewW, viewH)
      return { key: d.key, x, y, state: d.state }
    }),
    links: entities.links.map((l) => {
      const [x1, y1] = projectPoint(l.a.lat, l.a.lng, bbox, viewW, viewH)
      const [x2, y2] = projectPoint(l.z.lat, l.z.lng, bbox, viewW, viewH)
      return { key: l.key, x1, y1, x2, y2, state: l.state }
    }),
    metros: collectChangedMetros(draft).map((m) => {
      const [x, y] = projectPoint(m.lat, m.lng, bbox, viewW, viewH)
      return { code: m.code, x, y }
    }),
    context: { devices: contextDevices, links: contextLinks },
  }
}
