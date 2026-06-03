import { useMemo, useState } from 'react'
import type { ShredDevice } from '@/lib/api'
import type { DeviceLatency } from './types'

interface DevicePickerProps {
  devices: ShredDevice[]
  selected: ShredDevice | null
  onSelect: (d: ShredDevice) => void
  latencyMap?: Map<string, DeviceLatency>
}

export function DevicePicker({ devices, selected, onSelect, latencyMap }: DevicePickerProps) {
  const [search, setSearch] = useState('')

  const sortedDevices = useMemo(() => {
    if (!latencyMap) return devices
    return [...devices].sort((a, b) => {
      const la = latencyMap.get(a.device_key)
      const lb = latencyMap.get(b.device_key)
      const aReachable = la?.reachable ?? false
      const bReachable = lb?.reachable ?? false
      if (aReachable && !bReachable) return -1
      if (!aReachable && bReachable) return 1
      if (aReachable && bReachable) return la!.avg_latency_ns - lb!.avg_latency_ns
      return 0
    })
  }, [devices, latencyMap])

  // Top 5 reachable devices: first = "recommended", rest = "next-best"
  const latencyRanks = useMemo(() => {
    if (!latencyMap) return null
    const ranks = new Map<string, 'recommended' | 'next-best'>()
    let count = 0
    for (const d of sortedDevices) {
      if (latencyMap.get(d.device_key)?.reachable) {
        ranks.set(d.device_key, count === 0 ? 'recommended' : 'next-best')
        ++count
      }
    }
    return ranks
  }, [sortedDevices, latencyMap])

  const filtered = useMemo(() => {
    if (!search) return sortedDevices
    const needle = search.toLowerCase()
    return sortedDevices.filter(
      d =>
        d.device_code.toLowerCase().includes(needle) ||
        d.metro_code.toLowerCase().includes(needle),
    )
  }, [sortedDevices, search])

  return (
    <div>
      <input
        type="text"
        value={search}
        onChange={e => setSearch(e.target.value)}
        placeholder="Search devices or metros..."
        className="w-full px-3 py-2 text-sm border border-border rounded-lg bg-background mb-3 focus:outline-none focus:ring-2 focus:ring-primary/50"
      />
      <div className="border border-border rounded-lg overflow-hidden max-h-80 overflow-y-auto">
        <table className="w-full">
          <thead className="sticky top-0 bg-card">
            <tr className="text-xs text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-2.5 font-medium">Device</th>
              <th className="px-4 py-2.5 font-medium">Metro</th>
              {latencyMap && <th className="px-4 py-2.5 font-medium text-right">Latency</th>}
              <th className="px-4 py-2.5 font-medium text-right">Price / Epoch</th>
              <th className="px-4 py-2.5 font-medium text-right">Available Seats</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(d => {
              const isSelected = selected?.device_key === d.device_key
              const hasSeats = d.available_seats > 0
              const latency = latencyMap?.get(d.device_key)
              const rank = latencyRanks?.get(d.device_key)
              return (
                <tr
                  key={d.device_key}
                  onClick={() => hasSeats && onSelect(d)}
                  className={`border-b border-border last:border-b-0 transition-colors ${
                    isSelected
                      ? 'bg-primary/10 border-primary/20'
                      : hasSeats
                        ? 'hover:bg-muted cursor-pointer'
                        : 'opacity-50'
                  }`}
                >
                  <td className="px-4 py-2.5 text-sm font-mono">
                    <span>{d.device_code || d.device_key.slice(0, 8)}</span>
                    {rank === 'recommended' && (
                      <span className="ml-2 inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-green-500/15 text-green-600 dark:text-green-400">
                        Recommended
                      </span>
                    )}
                    {rank === 'next-best' && (
                      <span className="ml-2 inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium bg-blue-500/10 text-blue-600 dark:text-blue-400">
                        Next best
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-sm">{d.metro_code}</td>
                  {latencyMap && (
                    <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                      {latency?.reachable
                        ? <span>{(latency.avg_latency_ns / 1e6).toFixed(3)}ms</span>
                        : latency
                          ? <span className="text-red-400 text-xs">unreachable</span>
                          : <span className="text-muted-foreground">—</span>
                      }
                    </td>
                  )}
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                    ${d.total_price_dollars}
                  </td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                    {d.available_seats > 0 ? (
                      <span>{d.available_seats}</span>
                    ) : (
                      <span className="text-red-500">Full</span>
                    )}
                  </td>
                </tr>
              )
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={latencyMap ? 5 : 4} className="px-4 py-8 text-center text-muted-foreground text-sm">
                  No devices found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
