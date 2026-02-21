import { Monitor, X } from 'lucide-react'
import { useTopology } from '../TopologyContext'

// Device type colors (must match topology-graph.tsx and topology-map.tsx)
// Avoid green/red (status colors) and blue/purple (link colors)
const DEVICE_TYPE_COLORS: Record<string, { light: string; dark: string }> = {
  hybrid: { light: '#ca8a04', dark: '#eab308' },    // yellow
  transit: { light: '#ea580c', dark: '#f97316' },   // orange
  edge: { light: '#0891b2', dark: '#22d3ee' },      // cyan
  default: { light: '#6b7280', dark: '#9ca3af' },   // gray
}

const DEVICE_TYPES = ['hybrid', 'transit', 'edge']

interface DeviceTypeOverlayPanelProps {
  isDark: boolean
  deviceCounts?: Record<string, number>
}

export function DeviceTypeOverlayPanel({ isDark, deviceCounts }: DeviceTypeOverlayPanelProps) {
  const { toggleOverlay } = useTopology()

  return (
    <div className="p-3 text-xs">
      <div className="flex items-center justify-between mb-2">
        <span className="font-medium flex items-center gap-1.5">
          <Monitor className="h-3.5 w-3.5 text-blue-500" />
          Device Types
        </span>
        <button
          onClick={() => toggleOverlay('deviceType')}
          className="p-1 hover:bg-[var(--muted)] rounded"
          title="Close"
        >
          <X className="h-3 w-3" />
        </button>
      </div>

      <div className="space-y-1.5">
        {DEVICE_TYPES.map((type) => {
          const colors = DEVICE_TYPE_COLORS[type] || DEVICE_TYPE_COLORS.default
          const count = deviceCounts?.[type] ?? 0
          return (
            <div key={type} className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div
                  className="w-4 h-4 rounded-full"
                  style={{
                    backgroundColor: isDark ? colors.dark : colors.light,
                  }}
                />
                <span className="capitalize">{type}</span>
              </div>
              {deviceCounts && (
                <span className="text-muted-foreground">{count}</span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
