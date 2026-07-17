import type { PlanChange } from '@/lib/api'

export function changeSummary(change: PlanChange): string {
  const s = change.ref_snapshot ?? {}
  const p = change.payload ?? {}
  switch (change.op_type) {
    case 'remove_link':
      return `Remove link ${s.link_code ?? change.ref_link_pk ?? '?'}`
    case 'remove_device':
      return `Remove device ${s.device_code ?? change.ref_device_pk ?? '?'}`
    case 'add_device':
      return `Add device ${p.code ?? s.device_code ?? '?'}`
    case 'add_link':
      return `Add link ${s.link_code ?? '?'}`
    case 'move_link_end':
      return `Move link ${s.link_code ?? change.ref_link_pk ?? '?'} → ${s.device_code ?? '?'}`
    default:
      return change.op_type
  }
}
