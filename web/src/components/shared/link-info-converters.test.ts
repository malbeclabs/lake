import { describe, it, expect } from 'vitest'
import { topologyLinkToLinkInfo, topologyLinkToInfo } from './link-info-converters'
import type { TopologyLink } from '@/lib/api'

const makeTopologyLink = (overrides: Partial<TopologyLink> = {}): TopologyLink => ({
  pk: 'link-pk',
  code: 'mrs001-dz002:sin001-dz002',
  status: 'activated',
  link_type: 'WAN',
  bandwidth_bps: 10_000_000_000,
  side_a_pk: 'device-a-pk',
  side_a_code: 'mrs001-dz002',
  side_a_iface_name: 'Port-Channel1000.2030',
  side_a_ip: '172.16.0.226',
  side_z_pk: 'device-z-pk',
  side_z_code: 'sin001-dz002',
  side_z_iface_name: 'Port-Channel1000.2030',
  side_z_ip: '172.16.0.227',
  contributor_pk: 'contributor-pk',
  contributor_code: 'jump_',
  side_a_contributor_pk: '',
  side_a_contributor_code: '',
  side_z_contributor_pk: '',
  side_z_contributor_code: '',
  latency_us: 138_480,
  jitter_us: 24,
  latency_a_to_z_us: 138_480,
  jitter_a_to_z_us: 24,
  latency_z_to_a_us: 138_500,
  jitter_z_to_a_us: 29,
  loss_percent: 0,
  sample_count: 100,
  in_bps: 0,
  out_bps: 0,
  committed_rtt_ns: 138_500_000,
  isis_delay_override_ns: 0,
  ...overrides,
})

describe('topologyLinkToLinkInfo', () => {
  it('preserves topology membership and drain state', () => {
    const info = topologyLinkToLinkInfo(
      makeTopologyLink({ link_topologies: ['unicast-default'], unicast_drained: true })
    )
    expect(info.linkTopologies).toEqual(['unicast-default'])
    expect(info.unicastDrained).toBe(true)
  })

  it('maps core fields from the topology API shape', () => {
    const info = topologyLinkToLinkInfo(makeTopologyLink())
    expect(info.pk).toBe('link-pk')
    expect(info.code).toBe('mrs001-dz002:sin001-dz002')
    expect(info.linkType).toBe('WAN')
    expect(info.bandwidthBps).toBe(10_000_000_000)
    expect(info.deviceACode).toBe('mrs001-dz002')
    expect(info.deviceZCode).toBe('sin001-dz002')
    expect(info.interfaceAIP).toBe('172.16.0.226')
    expect(info.interfaceZIP).toBe('172.16.0.227')
    expect(info.latencyAtoZUs).toBe(138_480)
    expect(info.latencyZtoAUs).toBe(138_500)
    expect(info.committedRttNs).toBe(138_500_000)
    expect(info.health).toBeUndefined()
  })

  it('attaches health when provided', () => {
    const info = topologyLinkToLinkInfo(makeTopologyLink(), {
      status: 'healthy',
      committedRttNs: 138_500_000,
      slaRatio: 0.98,
      lossPct: 0,
    })
    expect(info.health).toEqual({
      status: 'healthy',
      committedRttNs: 138_500_000,
      slaRatio: 0.98,
      lossPct: 0,
    })
  })

  it('falls back to device codes when the link code is empty', () => {
    const info = topologyLinkToLinkInfo(makeTopologyLink({ code: '' }))
    expect(info.code).toBe('mrs001-dz002 — sin001-dz002')
  })

  // Regression: the map/globe/graph drawers showed a hardcoded "default"
  // topology chip because each view's inline builder dropped link_topologies
  // on the way to LinkInfoContent.
  it('keeps topology membership through the drawer conversion chain', () => {
    const data = topologyLinkToInfo(
      topologyLinkToLinkInfo(makeTopologyLink({ link_topologies: ['unicast-default'] }))
    )
    expect(data.linkTopologies).toEqual(['unicast-default'])
  })
})
