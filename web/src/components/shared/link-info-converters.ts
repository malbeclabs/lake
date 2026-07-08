import type { LinkDetail, TopologyLink } from '@/lib/api'
import type { LinkInfo } from '@/components/topology/types'
import type { LinkInfoData } from './LinkInfoContent'

/**
 * Convert LinkDetail (from /api/dz/links/:pk) to shared LinkInfoData
 */
export function linkDetailToInfo(link: LinkDetail): LinkInfoData {
  return {
    pk: link.pk,
    code: link.code,
    status: link.status,
    linkType: link.link_type,
    bandwidthBps: link.bandwidth_bps,
    sideAPk: link.side_a_pk,
    sideACode: link.side_a_code,
    sideAMetro: link.side_a_metro,
    sideAIfaceName: link.side_a_iface_name,
    sideAIP: link.side_a_ip,
    sideZPk: link.side_z_pk,
    sideZCode: link.side_z_code,
    sideZMetro: link.side_z_metro,
    sideZIfaceName: link.side_z_iface_name,
    sideZIP: link.side_z_ip,
    contributorPk: link.contributor_pk,
    contributorCode: link.contributor_code,
    sideAContributorPk: link.side_a_contributor_pk,
    sideAContributorCode: link.side_a_contributor_code,
    sideZContributorPk: link.side_z_contributor_pk,
    sideZContributorCode: link.side_z_contributor_code,
    inBps: link.in_bps,
    outBps: link.out_bps,
    utilizationIn: link.utilization_in,
    utilizationOut: link.utilization_out,
    latencyUs: link.latency_us,
    jitterUs: link.jitter_us,
    latencyAtoZUs: link.latency_a_to_z_us,
    jitterAtoZUs: link.jitter_a_to_z_us,
    latencyZtoAUs: link.latency_z_to_a_us,
    jitterZtoAUs: link.jitter_z_to_a_us,
    lossPercent: link.loss_percent,
    peakInBps: link.peak_in_bps,
    peakOutBps: link.peak_out_bps,
    committedRttNs: link.committed_rtt_ns,
    isisDelayOverrideNs: link.isis_delay_override_ns,
    linkTopologies: link.link_topologies ?? [],
    unicastDrained: link.unicast_drained ?? false,
  }
}

/**
 * Convert TopologyLink (from /api/topology) to the shared LinkInfo consumed
 * by the map/globe/graph link drawers. Single mapping point so API fields
 * (e.g. link_topologies) reach every view.
 */
export function topologyLinkToLinkInfo(
  link: TopologyLink,
  health?: { status: string; committedRttNs: number; slaRatio: number; lossPct: number }
): LinkInfo {
  return {
    pk: link.pk,
    code: link.code || `${link.side_a_code || 'Unknown'} — ${link.side_z_code || 'Unknown'}`,
    status: link.status,
    linkType: link.link_type || 'unknown',
    bandwidthBps: link.bandwidth_bps ?? 0,
    latencyUs: link.latency_us ?? 0,
    jitterUs: link.jitter_us ?? 0,
    latencyAtoZUs: link.latency_a_to_z_us ?? 0,
    jitterAtoZUs: link.jitter_a_to_z_us ?? 0,
    latencyZtoAUs: link.latency_z_to_a_us ?? 0,
    jitterZtoAUs: link.jitter_z_to_a_us ?? 0,
    lossPercent: link.loss_percent ?? 0,
    inBps: link.in_bps ?? 0,
    outBps: link.out_bps ?? 0,
    deviceAPk: link.side_a_pk || '',
    deviceACode: link.side_a_code || 'Unknown',
    interfaceAName: link.side_a_iface_name || '',
    interfaceAIP: link.side_a_ip || '',
    deviceZPk: link.side_z_pk || '',
    deviceZCode: link.side_z_code || 'Unknown',
    interfaceZName: link.side_z_iface_name || '',
    interfaceZIP: link.side_z_ip || '',
    contributorPk: link.contributor_pk || '',
    contributorCode: link.contributor_code || '',
    sideAContributorPk: link.side_a_contributor_pk || '',
    sideAContributorCode: link.side_a_contributor_code || '',
    sideZContributorPk: link.side_z_contributor_pk || '',
    sideZContributorCode: link.side_z_contributor_code || '',
    sampleCount: link.sample_count ?? 0,
    committedRttNs: link.committed_rtt_ns ?? 0,
    isisDelayOverrideNs: link.isis_delay_override_ns ?? 0,
    linkTopologies: link.link_topologies,
    unicastDrained: link.unicast_drained,
    health: health
      ? {
          status: health.status,
          committedRttNs: health.committedRttNs,
          slaRatio: health.slaRatio,
          lossPct: health.lossPct,
        }
      : undefined,
  }
}

/**
 * Convert topology LinkInfo to shared LinkInfoData
 */
export function topologyLinkToInfo(link: {
  pk: string
  code: string
  status: string
  linkType: string
  bandwidthBps: number
  latencyUs: number
  jitterUs: number
  latencyAtoZUs: number
  jitterAtoZUs: number
  latencyZtoAUs: number
  jitterZtoAUs: number
  lossPercent: number
  inBps: number
  outBps: number
  deviceAPk: string
  deviceACode: string
  interfaceAName: string
  interfaceAIP: string
  deviceZPk: string
  deviceZCode: string
  interfaceZName: string
  interfaceZIP: string
  contributorPk: string
  contributorCode: string
  sideAContributorPk: string
  sideAContributorCode: string
  sideZContributorPk: string
  sideZContributorCode: string
  committedRttNs: number
  isisDelayOverrideNs: number
  linkTopologies?: string[]
  unicastDrained?: boolean
}): LinkInfoData {
  return {
    pk: link.pk,
    code: link.code,
    status: link.status,
    linkType: link.linkType,
    bandwidthBps: link.bandwidthBps,
    sideAPk: link.deviceAPk,
    sideACode: link.deviceACode,
    sideAMetro: '', // Not available from topology
    sideAIfaceName: link.interfaceAName,
    sideAIP: link.interfaceAIP,
    sideZPk: link.deviceZPk,
    sideZCode: link.deviceZCode,
    sideZMetro: '', // Not available from topology
    sideZIfaceName: link.interfaceZName,
    sideZIP: link.interfaceZIP,
    contributorPk: link.contributorPk,
    contributorCode: link.contributorCode,
    sideAContributorPk: link.sideAContributorPk,
    sideAContributorCode: link.sideAContributorCode,
    sideZContributorPk: link.sideZContributorPk,
    sideZContributorCode: link.sideZContributorCode,
    inBps: link.inBps,
    outBps: link.outBps,
    utilizationIn: link.bandwidthBps > 0 ? (link.inBps / link.bandwidthBps) * 100 : 0,
    utilizationOut: link.bandwidthBps > 0 ? (link.outBps / link.bandwidthBps) * 100 : 0,
    latencyUs: link.latencyUs,
    jitterUs: link.jitterUs,
    latencyAtoZUs: link.latencyAtoZUs,
    jitterAtoZUs: link.jitterAtoZUs,
    latencyZtoAUs: link.latencyZtoAUs,
    jitterZtoAUs: link.jitterZtoAUs,
    lossPercent: link.lossPercent,
    committedRttNs: link.committedRttNs,
    isisDelayOverrideNs: link.isisDelayOverrideNs,
    linkTopologies: link.linkTopologies ?? [],
    unicastDrained: link.unicastDrained ?? false,
  }
}
