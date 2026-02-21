import { Cable, X } from 'lucide-react'
import { useTopology } from '../TopologyContext'

// Link type colors (distinct from device type colors: yellow, orange, cyan)
// Avoid green/red as those indicate status
// eslint-disable-next-line react-refresh/only-export-components
export const LINK_TYPE_COLORS: Record<string, { light: string; dark: string }> = {
  WAN: { light: '#6b7280', dark: '#9ca3af' },           // gray - inter-metro wide area
  DZX: { light: '#7c3aed', dark: '#a78bfa' },           // purple - local exchange
  'Inter-Metro': { light: '#6b7280', dark: '#9ca3af' }, // gray - aggregated inter-metro
  default: { light: '#6b7280', dark: '#9ca3af' },       // gray
}

interface LinkTypeOverlayPanelProps {
  isDark: boolean
  linkCounts?: Record<string, number>
}

export function LinkTypeOverlayPanel({ isDark, linkCounts }: LinkTypeOverlayPanelProps) {
  const { toggleOverlay } = useTopology()

  // Get all link types from counts, or use defaults
  const linkTypes = linkCounts
    ? Object.keys(linkCounts).filter(type => type !== 'Inter-Metro').sort()
    : ['WAN', 'DZX']

  return (
    <div className="p-3 text-xs">
      <div className="flex items-center justify-between mb-2">
        <span className="font-medium flex items-center gap-1.5">
          <Cable className="h-3.5 w-3.5 text-blue-500" />
          Link Types
        </span>
        <button
          onClick={() => toggleOverlay('linkType')}
          className="p-1 hover:bg-[var(--muted)] rounded"
          title="Close"
        >
          <X className="h-3 w-3" />
        </button>
      </div>

      <div className="space-y-1.5">
        {linkTypes.map((type) => {
          const colors = LINK_TYPE_COLORS[type] || LINK_TYPE_COLORS.default
          const count = linkCounts?.[type] ?? 0
          return (
            <div key={type} className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div
                  className="w-6 h-1 rounded-full"
                  style={{
                    backgroundColor: isDark ? colors.dark : colors.light,
                  }}
                />
                <span>{type}</span>
              </div>
              {linkCounts && (
                <span className="text-muted-foreground">{count}</span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
