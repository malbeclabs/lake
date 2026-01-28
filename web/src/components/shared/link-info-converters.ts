import type { LinkDetail } from '@/lib/api'
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
  }
}

/**
 * Convert topology LinkInfo to shared LinkInfoData
 */
export function topologyLinkToInfo(link: {
  pk: string
  code: string
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
}): LinkInfoData {
  return {
    pk: link.pk,
    code: link.code,
    status: 'activated', // Topology links are always activated
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
  }
}
