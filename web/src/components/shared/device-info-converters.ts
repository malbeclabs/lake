import type { DeviceDetail, DeviceValidatorStats, TopologyDevice } from '@/lib/api'
import type { DeviceInfoData } from './DeviceInfoContent'

/**
 * Convert DeviceDetail (from /api/dz/devices/:pk) to shared DeviceInfoData.
 * Validator stats are fetched separately and passed in when available.
 */
export function deviceDetailToInfo(
  device: DeviceDetail,
  metroName?: string,
  validatorStats?: DeviceValidatorStats,
): DeviceInfoData {
  return {
    pk: device.pk,
    code: device.code,
    deviceType: device.device_type,
    status: device.status,
    metroPk: device.metro_pk,
    metroName: metroName || device.metro_name || device.metro_code,
    contributorPk: device.contributor_pk,
    contributorCode: device.contributor_code,
    userCount: device.current_users,
    maxUsers: device.max_users,
    unicastUsersCount: device.unicast_users_count ?? 0,
    multicastSubscribersCount: device.multicast_subscribers_count ?? 0,
    multicastPublishersCount: device.multicast_publishers_count ?? 0,
    maxUnicastUsers: device.max_unicast_users ?? 0,
    maxMulticastSubscribers: device.max_multicast_subscribers ?? 0,
    maxMulticastPublishers: device.max_multicast_publishers ?? 0,
    validatorCount: validatorStats ? validatorStats.validator_count : null,
    stakeSol: validatorStats ? validatorStats.stake_sol : null,
    stakeShare: validatorStats ? validatorStats.stake_share : null,
    interfaces: device.interfaces || [],
  }
}

/**
 * Convert TopologyDevice (from /api/topology) to shared DeviceInfoData
 */
export function topologyDeviceToInfo(device: TopologyDevice, metroName: string): DeviceInfoData {
  return {
    pk: device.pk,
    code: device.code,
    deviceType: device.device_type,
    status: device.status,
    metroPk: device.metro_pk,
    metroName: metroName,
    contributorPk: device.contributor_pk,
    contributorCode: device.contributor_code,
    userCount: device.user_count,
    maxUsers: 0,
    unicastUsersCount: device.unicast_users_count ?? 0,
    multicastSubscribersCount: device.multicast_subscribers_count ?? 0,
    multicastPublishersCount: device.multicast_publishers_count ?? 0,
    maxUnicastUsers: device.max_unicast_users ?? 0,
    maxMulticastSubscribers: device.max_multicast_subscribers ?? 0,
    maxMulticastPublishers: device.max_multicast_publishers ?? 0,
    validatorCount: device.validator_count,
    stakeSol: device.stake_sol,
    stakeShare: device.stake_share,
    interfaces: device.interfaces || [],
  }
}
