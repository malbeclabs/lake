import { useState } from 'react'
import { GitCompare, ChevronDown, ChevronRight } from 'lucide-react'
import type { TopologyCompareResponse, TopologyDiscrepancy } from '@/lib/api'
import { useTopology } from '../TopologyContext'

interface ComparePanelProps {
  data: TopologyCompareResponse | null
  isLoading: boolean
}

function StatusSection({ discrepancies, type, label, color, lineStyle }: {
  discrepancies: TopologyDiscrepancy[]
  type: 'matched' | 'missing_isis' | 'extra_isis'
  label: string
  color: string
  lineStyle?: React.CSSProperties
}) {
  const [expanded, setExpanded] = useState(false)
  const { setSelection } = useTopology()
  const filtered = type === 'matched' ? [] : discrepancies.filter(d => d.type === type)
  const count = type === 'matched' ? 0 : filtered.length // matched count handled by parent

  const hasItems = filtered.length > 0
  const isExpandable = hasItems

  return (
    <div>
      <div
        role={isExpandable ? 'button' : undefined}
        onClick={isExpandable ? () => setExpanded(!expanded) : undefined}
        className={`flex items-center gap-1.5 py-0.5 ${isExpandable ? 'cursor-pointer hover:text-foreground' : ''}`}
      >
        {isExpandable ? (
          expanded ? <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" /> : <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />
        ) : (
          <div className="w-3" />
        )}
        <div className="w-4 h-0.5 shrink-0" style={{ backgroundColor: color, ...lineStyle }} />
        <span className="flex-1" style={{ color: count > 0 ? color : undefined }}>{label}</span>
        <span className="font-medium" style={{ color: count > 0 ? color : undefined }}>{count}</span>
      </div>
      {expanded && hasItems && (
        <div className="mt-0.5 space-y-px">
          {filtered.map((d, i) => (
            <button
              key={i}
              className="flex items-center w-full text-left pl-[4.5rem] py-0.5 rounded hover:bg-[var(--accent)] text-muted-foreground hover:text-foreground transition-colors truncate"
              title={d.details}
              onClick={() => {
                if (d.linkPK) {
                  setSelection({ type: 'link', id: d.linkPK })
                } else {
                  setSelection({ type: 'device', id: d.deviceAPK })
                }
              }}
            >
              {d.linkCode || `${d.deviceACode} → ${d.deviceBCode}`}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

export function ComparePanel({ data, isLoading }: ComparePanelProps) {
  return (
    <div className="p-3 text-xs">
      <div className="flex items-center gap-1.5 mb-3">
        <GitCompare className="h-3.5 w-3.5 text-blue-500" />
        <span className="font-medium">ISIS Health</span>
      </div>

      {isLoading && (
        <div className="text-muted-foreground">Loading comparison...</div>
      )}

      {data && !data.error && (
        <div className="space-y-3">
          {/* Summary stats */}
          <div className="space-y-1.5">
            <div className="flex justify-between">
              <span className="text-muted-foreground">Configured Links</span>
              <span className="font-medium">{data.configuredLinks}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-muted-foreground">ISIS Adjacencies</span>
              <span className="font-medium">{data.isisAdjacencies}</span>
            </div>
          </div>

          {/* Status breakdown with inline legend */}
          <div className="pt-2 border-t border-[var(--border)] space-y-0.5">
            <div className="flex items-center gap-1.5 py-0.5">
              <div className="w-3" />
              <div className="w-4 h-0.5 shrink-0 bg-green-500" />
              <span className="flex-1 text-green-500">Matched</span>
              <span className="font-medium text-green-500">{data.matchedLinks}</span>
            </div>
            <StatusSection
              discrepancies={data.discrepancies}
              type="missing_isis"
              label="Missing ISIS"
              color="#ef4444"
              lineStyle={{ borderTop: '2px dashed #ef4444', backgroundColor: 'transparent' }}
            />
            <StatusSection
              discrepancies={data.discrepancies}
              type="extra_isis"
              label="Extra adjacency"
              color="#f59e0b"
            />
          </div>
        </div>
      )}

      {data?.error && (
        <div className="text-destructive">{data.error}</div>
      )}
    </div>
  )
}
