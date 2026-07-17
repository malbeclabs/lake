import type { EntityChangeState } from './draft'

const ADDED = '#10b981' // emerald
const MODIFIED = '#f59e0b' // amber
const REMOVED = '#9ca3af' // grey

export interface LinkStyle {
  color: string
  weight: number
  opacity: number
  dashed: boolean
  struck: boolean
}

export function linkChangeStyle(state: EntityChangeState, isDark: boolean): LinkStyle {
  const base = isDark ? '#64748b' : '#94a3b8'
  switch (state) {
    case 'added':
      return { color: ADDED, weight: 4, opacity: 0.95, dashed: false, struck: false }
    case 'modified':
      return { color: MODIFIED, weight: 4, opacity: 0.95, dashed: false, struck: false }
    case 'removed':
      return { color: REMOVED, weight: 3, opacity: 0.4, dashed: true, struck: true }
    default:
      return { color: base, weight: 3, opacity: 0.85, dashed: false, struck: false }
  }
}

export interface DeviceStyle {
  color: string
  opacity: number
  ring: boolean
  struck: boolean
}

export function deviceChangeStyle(state: EntityChangeState, isDark: boolean): DeviceStyle {
  const base = isDark ? '#93c5fd' : '#2563eb'
  switch (state) {
    case 'added':
      return { color: ADDED, opacity: 1, ring: true, struck: false }
    case 'modified':
      return { color: MODIFIED, opacity: 1, ring: true, struck: false }
    case 'removed':
      return { color: REMOVED, opacity: 0.4, ring: false, struck: true }
    default:
      return { color: base, opacity: 0.9, ring: false, struck: false }
  }
}

export const CHANGE_LEGEND: { state: EntityChangeState; label: string; color: string }[] = [
  { state: 'added', label: 'Added', color: ADDED },
  { state: 'modified', label: 'Modified', color: MODIFIED },
  { state: 'removed', label: 'Removed', color: REMOVED },
]
