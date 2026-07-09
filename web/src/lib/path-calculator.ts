import type { MetroDevicePairPath, SinglePath } from '@/lib/api'

export type LocationKind = 'metro' | 'device'

export interface LocationOption {
  kind: LocationKind
  pk: string
  code: string
  name?: string // metros only
  status?: string // devices only
  deviceType?: string // devices only
  metroPK?: string // devices only — resolves mixed metro/device queries
}

export interface MetroInput {
  pk: string
  code: string
  name?: string
}

export interface DeviceInput {
  pk: string
  code: string
  status: string
  deviceType: string
  metroPK?: string
}

// Merge metros and devices into one option list: metros first (by code), then devices (by code).
export function buildLocationOptions(
  metros: MetroInput[],
  devices: DeviceInput[],
): LocationOption[] {
  const metroOpts: LocationOption[] = metros
    .map((m) => ({ kind: 'metro' as const, pk: m.pk, code: m.code, name: m.name }))
    .sort((a, b) => a.code.localeCompare(b.code))
  const deviceOpts: LocationOption[] = devices
    .map((d) => ({
      kind: 'device' as const,
      pk: d.pk,
      code: d.code,
      status: d.status,
      deviceType: d.deviceType,
      metroPK: d.metroPK,
    }))
    .sort((a, b) => a.code.localeCompare(b.code))
  return [...metroOpts, ...deviceOpts]
}

// Filter + rank options for the typeahead. Metros rank before devices; within a
// kind, an exact code match sorts first, then alphabetical by code.
export function filterLocations(
  options: LocationOption[],
  query: string,
  excludePK?: string,
  limit = 30,
): LocationOption[] {
  const q = query.trim().toLowerCase()
  if (!q) return []
  const matched = options.filter((o) => {
    if (o.pk === excludePK) return false
    if (o.code.toLowerCase().includes(q)) return true
    if (o.kind === 'metro' && o.name?.toLowerCase().includes(q)) return true
    return false
  })
  const kindRank = (o: LocationOption) => (o.kind === 'metro' ? 0 : 1)
  const exactRank = (o: LocationOption) => (o.code.toLowerCase() === q ? 0 : 1)
  matched.sort((a, b) => {
    if (kindRank(a) !== kindRank(b)) return kindRank(a) - kindRank(b)
    if (exactRank(a) !== exactRank(b)) return exactRank(a) - exactRank(b)
    return a.code.localeCompare(b.code)
  })
  return matched.slice(0, limit)
}

// Parse the URL endpoint-kind param; anything other than 'metro' is a device (back-compat).
export function parseEndpointKind(raw: string | null): LocationKind {
  return raw === 'metro' ? 'metro' : 'device'
}

// Re-exported for Task 2 consumers; kept here so all path-calc logic lives in one module.
export type { MetroDevicePairPath, SinglePath }
