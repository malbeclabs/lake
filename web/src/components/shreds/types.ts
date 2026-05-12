// Latency types from `doublezero latency --json` output.

export interface LatencyEntry {
  device_pk: string
  device_code: string
  device_ip: string
  min_latency_ns: number
  max_latency_ns: number
  avg_latency_ns: number
  reachable: boolean
}

// Grouped by device_pk — best (min) avg latency across all IPs for that device.
export interface DeviceLatency {
  device_pk: string
  device_code: string
  avg_latency_ns: number
  reachable: boolean
}
