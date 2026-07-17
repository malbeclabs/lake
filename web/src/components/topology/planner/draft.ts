import type {
  TopologyResponse,
  TopologyDevice,
  TopologyLink,
  TopologyMetro,
  PlanChange,
} from '@/lib/api'

export type EntityChangeState = 'unchanged' | 'added' | 'removed' | 'modified'

export interface DraftDevice extends TopologyDevice {
  changeState: EntityChangeState
  localRef?: string
  changeId?: string
}

export interface DraftLink extends TopologyLink {
  changeState: EntityChangeState
  localRef?: string
  changeId?: string
  modifiedFields?: string[]
}

export interface DraftTopology {
  metros: TopologyMetro[]
  devices: DraftDevice[]
  links: DraftLink[]
  deviceByKey: Map<string, DraftDevice> // key = pk or local_ref
  linkByKey: Map<string, DraftLink>
}

function blankDevice(over: Partial<DraftDevice>): DraftDevice {
  return {
    pk: '', code: '', status: 'planned', device_type: '',
    metro_pk: '', contributor_pk: '', contributor_code: '',
    user_count: 0, unicast_users_count: 0, multicast_subscribers_count: 0,
    multicast_publishers_count: 0, max_unicast_users: 0, max_multicast_subscribers: 0,
    max_multicast_publishers: 0, validator_count: 0, stake_sol: 0, stake_share: 0,
    interfaces: [], changeState: 'added', ...over,
  }
}

function blankLink(over: Partial<DraftLink>): DraftLink {
  return {
    pk: '', code: '', status: 'planned', link_type: 'WAN', bandwidth_bps: 0,
    side_a_pk: '', side_a_code: '', side_a_iface_name: '', side_a_ip: '',
    side_z_pk: '', side_z_code: '', side_z_iface_name: '', side_z_ip: '',
    contributor_pk: '', contributor_code: '',
    side_a_contributor_pk: '', side_a_contributor_code: '',
    side_z_contributor_pk: '', side_z_contributor_code: '',
    latency_us: 0, jitter_us: 0, latency_a_to_z_us: 0, jitter_a_to_z_us: 0,
    latency_z_to_a_us: 0, jitter_z_to_a_us: 0, loss_percent: 0, sample_count: 0,
    in_bps: 0, out_bps: 0, committed_rtt_ns: 0, isis_delay_override_ns: 0,
    changeState: 'added', ...over,
  }
}

// Browser mirror of the Go applyChanges: baseline + ordered patch -> annotated draft.
export function buildDraft(
  baseline: TopologyResponse,
  changes: PlanChange[]
): DraftTopology {
  const deviceByKey = new Map<string, DraftDevice>()
  const linkByKey = new Map<string, DraftLink>()

  for (const d of baseline.devices) {
    deviceByKey.set(d.pk, { ...d, changeState: 'unchanged' })
  }
  for (const l of baseline.links) {
    linkByKey.set(l.pk, { ...l, changeState: 'unchanged' })
  }

  // SC-8: draft membership = live baseline + only pending changes. 'done' is
  // already reflected in the live baseline (re-applying would double-count the
  // edge/device); 'skipped'/'superseded' are excluded outright.
  const applied = [...changes]
    .filter((c) => c.state === 'pending')
    .sort((a, b) => a.seq - b.seq)

  const resolve = (pk?: string | null, ref?: string | null): string =>
    (pk && pk.length ? pk : ref) ?? ''

  for (const c of applied) {
    const p = c.payload ?? {}
    switch (c.op_type) {
      case 'add_device': {
        const key = c.local_ref ?? c.id
        deviceByKey.set(
          key,
          blankDevice({
            pk: key,
            localRef: c.local_ref ?? undefined,
            changeId: c.id,
            code: p.code ?? c.ref_snapshot.device_code ?? key,
            metro_pk: p.metro_pk ?? '',
            contributor_pk: p.contributor_pk ?? '',
            device_type: p.device_type ?? '',
          })
        )
        break
      }
      case 'remove_device': {
        const dev = deviceByKey.get(c.ref_device_pk ?? '')
        if (dev) dev.changeState = dev.changeState === 'added' ? 'added' : 'removed'
        if (dev) dev.changeId = c.id
        break
      }
      case 'add_link': {
        const key = c.local_ref ?? c.id
        const aKey = resolve(p.side_a_device_pk, p.side_a_ref)
        const zKey = resolve(p.side_z_device_pk, p.side_z_ref)
        // Derive endpoint code + contributor from the resolved device (an existing
        // baseline device or a sibling add_device keyed by local_ref) so the draft
        // renders the correct ownership, mirroring Go addLinkEdge's contributor
        // derivation from g.Nodes[key].
        const aDev = deviceByKey.get(aKey)
        const zDev = deviceByKey.get(zKey)
        linkByKey.set(
          key,
          blankLink({
            pk: key,
            localRef: c.local_ref ?? undefined,
            changeId: c.id,
            code: c.ref_snapshot.link_code ?? key,
            link_type: p.link_type ?? 'WAN',
            side_a_pk: aKey,
            side_z_pk: zKey,
            side_a_code: aDev?.code ?? '',
            side_z_code: zDev?.code ?? '',
            side_a_contributor_pk: aDev?.contributor_pk ?? '',
            side_a_contributor_code: aDev?.contributor_code ?? '',
            side_z_contributor_pk: zDev?.contributor_pk ?? '',
            side_z_contributor_code: zDev?.contributor_code ?? '',
            side_a_iface_name: p.side_a_iface_name ?? '',
            side_z_iface_name: p.side_z_iface_name ?? '',
            side_a_ip: p.side_a_ip ?? '',
            side_z_ip: p.side_z_ip ?? '',
            bandwidth_bps: p.bandwidth_bps ?? 0,
            // Display latency stays committed_rtt_ns / latency_us; the routing metric
            // override rides isis_delay_override_ns (used when set, else committed_rtt_ns),
            // mirroring latencyToMetric(latency_ns, metric_override_ns). The integer
            // µs routing metric itself is computed server-side by the impact engine.
            latency_us: p.latency_ns ? Math.round(p.latency_ns / 1000) : 0,
            committed_rtt_ns: p.latency_ns ?? 0,
            isis_delay_override_ns: p.metric_override_ns ?? 0,
            link_topologies: p.link_topologies,
          })
        )
        break
      }
      case 'remove_link': {
        const link = linkByKey.get(c.ref_link_pk ?? '')
        if (link) {
          link.changeState = link.changeState === 'added' ? 'added' : 'removed'
          link.changeId = c.id
        }
        break
      }
      case 'move_link_end': {
        const link = linkByKey.get(c.ref_link_pk ?? '')
        if (!link) break
        // SC-1: the target existing-device pk is the new_device_pk column; only the
        // temp reference (new_device_ref) lives in payload.
        const newKey = resolve(c.new_device_pk, p.new_device_ref)
        // Refresh the moved side's code + contributor from the resolved device so a
        // moved (e.g. DZX) link renders the new ownership, not stale baseline values.
        const newDev = deviceByKey.get(newKey)
        const modified: string[] = []
        if (p.side === 'a') {
          link.side_a_pk = newKey
          link.side_a_code = newDev?.code ?? ''
          link.side_a_contributor_pk = newDev?.contributor_pk ?? ''
          link.side_a_contributor_code = newDev?.contributor_code ?? ''
          modified.push('side_a_pk')
          if (p.new_iface_name) link.side_a_iface_name = p.new_iface_name
          if (p.new_ip) link.side_a_ip = p.new_ip
        } else {
          link.side_z_pk = newKey
          link.side_z_code = newDev?.code ?? ''
          link.side_z_contributor_pk = newDev?.contributor_pk ?? ''
          link.side_z_contributor_code = newDev?.contributor_code ?? ''
          modified.push('side_z_pk')
          if (p.new_iface_name) link.side_z_iface_name = p.new_iface_name
          if (p.new_ip) link.side_z_ip = p.new_ip
        }
        if (p.latency_ns) {
          link.latency_us = Math.round(p.latency_ns / 1000)
          link.committed_rtt_ns = p.latency_ns
          modified.push('latency_us')
        }
        // Routing metric override rides isis_delay_override_ns (see add_link); display
        // latency above is untouched. Mirrors latencyToMetric(latency_ns, metric_override_ns).
        if (p.metric_override_ns) {
          link.isis_delay_override_ns = p.metric_override_ns
          modified.push('isis_delay_override_ns')
        }
        if (p.bandwidth_bps) {
          link.bandwidth_bps = p.bandwidth_bps
          modified.push('bandwidth_bps')
        }
        if (link.changeState !== 'added') link.changeState = 'modified'
        link.changeId = c.id
        link.modifiedFields = [...(link.modifiedFields ?? []), ...modified]
        break
      }
    }
  }

  return {
    metros: baseline.metros,
    devices: [...deviceByKey.values()],
    links: [...linkByKey.values()],
    deviceByKey,
    linkByKey,
  }
}
