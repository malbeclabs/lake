import type { ActionFilter } from '@/lib/api'

export type Category = 'state_change' | 'packet_loss' | 'interface_carrier' | 'interface_errors' | 'interface_discards'
export type EntityType = 'device' | 'link' | 'metro' | 'contributor' | 'user' | 'validator' | 'gossip_node'
export type DZFilter = 'on_dz' | 'off_dz' | 'all'
export type MinStakeOption = '0' | '0.01' | '0.05' | '0.1' | '0.5' | '1' | '1.5' | '2'

export const ALL_ACTIONS: ActionFilter[] = ['added', 'removed', 'changed', 'alerting', 'resolved']
export const ALL_DZ_ENTITIES: EntityType[] = ['device', 'link', 'metro', 'contributor', 'user']
export const ALL_SOLANA_ENTITIES: EntityType[] = ['validator', 'gossip_node']
export const ALL_ENTITY_TYPES: EntityType[] = [...ALL_DZ_ENTITIES, ...ALL_SOLANA_ENTITIES]
export const DEFAULT_ENTITY_TYPES: EntityType[] = ALL_ENTITY_TYPES.filter(e => e !== 'gossip_node')
export const ALL_CATEGORIES: Category[] = ['state_change', 'packet_loss', 'interface_carrier', 'interface_errors', 'interface_discards']

export const presets: { label: string; params: Record<string, string> }[] = [
  { label: 'Links added', params: { range: '7d', entities: 'link', categories: 'state_change', actions: 'added', dz: 'on_dz' } },
  { label: 'Devices added', params: { range: '7d', entities: 'device', categories: 'state_change', actions: 'added', dz: 'on_dz' } },
  { label: 'Validator connections', params: { range: '7d', entities: 'validator', categories: 'state_change', actions: 'added,removed', dz: 'on_dz', min_stake: '0.01' } },
  { label: 'DZ stake changes', params: { range: '7d', entities: 'validator', categories: 'state_change', actions: 'added,removed,changed,alerting,resolved', dz: 'on_dz', min_stake: '0.01' } },
  { label: 'Link/device updates', params: { range: '24h', entities: 'device,link', categories: 'state_change', actions: 'changed', dz: 'on_dz' } },
  { label: 'Link ops', params: { range: '24h', entities: 'link,device', categories: 'packet_loss,interface_carrier,interface_errors,interface_discards', dz: 'on_dz' } },
  { label: 'Device ops', params: { range: '24h', entities: 'device', categories: 'interface_carrier,interface_errors,interface_discards', dz: 'on_dz' } },
]

// Helper to parse a comma-separated URL param into a Set, with validation
export function parseSetParam<T extends string>(param: string | null, allValues: T[], defaultValues: T[]): Set<T> {
  if (!param) return new Set(defaultValues)
  const values = param.split(',').filter((v): v is T => allValues.includes(v as T))
  return values.length > 0 ? new Set(values) : new Set(defaultValues)
}

// Helper to serialize a Set to comma-separated string, returning undefined if it matches default
export function serializeSetParam<T extends string>(set: Set<T>, defaultValues: T[]): string | undefined {
  const defaultSet = new Set(defaultValues)
  const isDefault = set.size === defaultSet.size && [...set].every(v => defaultSet.has(v))
  if (isDefault) return undefined
  return Array.from(set).join(',')
}

export function formatTimeAgo(timestamp: string): string {
  const now = new Date()
  const then = new Date(timestamp)
  const diffMs = now.getTime() - then.getTime()
  const diffMins = Math.floor(diffMs / 60000)
  const diffHours = Math.floor(diffMs / 3600000)
  const diffDays = Math.floor(diffMs / 86400000)

  if (diffMins < 1) return 'just now'
  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  return then.toLocaleDateString()
}

// Get the date string for grouping
export function getDateKey(timestamp: string): string {
  return new Date(timestamp).toDateString()
}
