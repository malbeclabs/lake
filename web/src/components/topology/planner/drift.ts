import type { PlanChange, DriftStatus, TopologyResponse } from '@/lib/api'

// Compare one change against the current live topology.
export function computeChangeDrift(
  baseline: TopologyResponse,
  change: PlanChange
): DriftStatus {
  const deviceByPk = new Map(baseline.devices.map((d) => [d.pk, d]))
  const linkByPk = new Map(baseline.links.map((l) => [l.pk, l]))
  const p = change.payload ?? {}

  switch (change.op_type) {
    case 'remove_device':
      return deviceByPk.has(change.ref_device_pk ?? '') ? 'pending' : 'already_done'

    case 'remove_link':
      return linkByPk.has(change.ref_link_pk ?? '') ? 'pending' : 'already_done'

    case 'move_link_end': {
      const link = linkByPk.get(change.ref_link_pk ?? '')
      if (!link) return 'broken'
      const target = change.new_device_pk ?? ''
      // A real target-device pk (new_device_pk) that has vanished from live
      // topology is broken. A same-plan local_ref target (new_device_ref, with
      // no new_device_pk) refers to a device this plan adds, so it isn't a
      // live-topology lookup — don't mark those broken here.
      if (target && !deviceByPk.has(target)) return 'broken'
      const current = p.side === 'a' ? link.side_a_pk : link.side_z_pk
      return current === target ? 'already_done' : 'pending'
    }

    case 'add_device': {
      const code = p.code ?? change.ref_snapshot.device_code
      const exists = baseline.devices.some((d) => code && d.code === code)
      return exists ? 'already_done' : 'pending'
    }

    case 'add_link': {
      // Endpoint pks that reference existing devices (not sibling local_refs).
      const aPk = p.side_a_device_pk
      const zPk = p.side_z_device_pk
      const aMissing = !!aPk && !deviceByPk.has(aPk)
      const zMissing = !!zPk && !deviceByPk.has(zPk)
      if (aMissing || zMissing) return 'broken'
      const exists = baseline.links.some(
        (l) =>
          (l.side_a_pk === aPk && l.side_z_pk === zPk) ||
          (l.side_a_pk === zPk && l.side_z_pk === aPk)
      )
      return exists ? 'already_done' : 'pending'
    }

    default:
      return 'pending'
  }
}

export function annotateDrift(
  baseline: TopologyResponse,
  changes: PlanChange[]
): Map<string, DriftStatus> {
  const m = new Map<string, DriftStatus>()
  for (const c of changes) m.set(c.id, computeChangeDrift(baseline, c))
  return m
}

export function driftLabel(d: DriftStatus): string {
  switch (d) {
    case 'already_done':
      return 'Already done'
    case 'broken':
      return 'Broken'
    default:
      return 'Pending'
  }
}
