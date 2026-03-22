import type { LinkHourStatus } from '@/lib/api'

// getEffectiveStatus returns the link's effective status for a given hour.
// Classification is now done server-side; this function just passes through
// the backend status, with ISIS down override for backwards compatibility.
export function getEffectiveStatus(hour: LinkHourStatus): string {
  if (hour.isis_down) {
    return 'down'
  }
  return hour.status
}
