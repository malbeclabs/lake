import type { DeviceDetail, TopologyDevice } from '@/lib/api'
import type { DeviceInfoData } from './DeviceInfoContent'

/**
 * Convert DeviceDetail (from /api/dz/devices/:pk) to shared DeviceInfoData
 */
export function deviceDetailToInfo(device: DeviceDetail, metroName?: string): DeviceInfoData {
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
    validatorCount: device.validator_count,
    stakeSol: device.stake_sol,
    stakeShare: device.stake_share,
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
    validatorCount: device.validator_count,
    stakeSol: device.stake_sol,
    stakeShare: device.stake_share,
    interfaces: device.interfaces || [],
  }
}
