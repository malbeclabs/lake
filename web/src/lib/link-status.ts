import type { LinkHourStatus } from '@/lib/api'

const LATENCY_WARNING_PCT = 20
const LATENCY_CRITICAL_PCT = 50

function hasInterfaceIssues(hour: LinkHourStatus): boolean {
  return (
    (hour.side_a_in_errors ?? 0) > 0 ||
    (hour.side_a_out_errors ?? 0) > 0 ||
    (hour.side_a_in_fcs_errors ?? 0) > 0 ||
    (hour.side_z_in_errors ?? 0) > 0 ||
    (hour.side_z_out_errors ?? 0) > 0 ||
    (hour.side_z_in_fcs_errors ?? 0) > 0 ||
    (hour.side_a_in_discards ?? 0) > 0 ||
    (hour.side_a_out_discards ?? 0) > 0 ||
    (hour.side_z_in_discards ?? 0) > 0 ||
    (hour.side_z_out_discards ?? 0) > 0 ||
    (hour.side_a_carrier_transitions ?? 0) > 0 ||
    (hour.side_z_carrier_transitions ?? 0) > 0
  )
}

export function getEffectiveStatus(hour: LinkHourStatus, committedRttUs?: number): string {
  // ISIS down overrides everything — link has no adjacency
  if (hour.isis_down) {
    return 'down'
  }

  // Keep original status if not healthy
  if (hour.status !== 'healthy') {
    return hour.status
  }

  // Check for high latency (>= 50% over SLA = critical/unhealthy, >= 20% = warning/degraded)
  if (committedRttUs && committedRttUs > 0 && hour.avg_latency_us > 0) {
    const latencyOveragePct = ((hour.avg_latency_us - committedRttUs) / committedRttUs) * 100
    if (latencyOveragePct >= LATENCY_CRITICAL_PCT) {
      return 'unhealthy'
    }
    if (latencyOveragePct >= LATENCY_WARNING_PCT) {
      return 'degraded'
    }
  }

  // If marked as healthy but has interface issues, downgrade to degraded
  if (hasInterfaceIssues(hour)) {
    return 'degraded'
  }

  return hour.status
}
