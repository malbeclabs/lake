import { useState } from 'react'
import { GitCompare, AlertTriangle, ChevronDown, ChevronRight } from 'lucide-react'
import type { TopologyCompareResponse, TopologyDiscrepancy } from '@/lib/api'
import { useTopology } from '../TopologyContext'

interface ComparePanelProps {
  data: TopologyCompareResponse | null
  isLoading: boolean
}

function DiscrepancySection({ discrepancies, type, label, dotColor }: {
  discrepancies: TopologyDiscrepancy[]
  type: 'missing_isis' | 'extra_isis'
  label: string
  dotColor: string
}) {
  const [expanded, setExpanded] = useState(true)
  const { setSelection } = useTopology()
  const filtered = discrepancies.filter(d => d.type === type)
  if (filtered.length === 0) return null

  return (
    <div>
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1.5 w-full text-left text-muted-foreground hover:text-foreground"
      >
        {expanded ? <ChevronDown className="h-3 w-3 shrink-0" /> : <ChevronRight className="h-3 w-3 shrink-0" />}
        <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: dotColor }} />
        <span>{filtered.length} {label}</span>
      </button>
      {expanded && (
        <div className="mt-1 space-y-px">
          {filtered.map((d, i) => (
            <button
              key={i}
              className="flex items-center gap-1.5 w-full text-left pl-7 py-0.5 rounded hover:bg-[var(--accent)] text-muted-foreground hover:text-foreground transition-colors"
              title={d.details}
              onClick={() => {
                if (d.linkPK) {
                  setSelection({ type: 'link', id: d.linkPK })
                } else {
                  setSelection({ type: 'device', id: d.deviceAPK })
                }
              }}
            >
              <span className="truncate">
                {d.linkCode || `${d.deviceACode} → ${d.deviceBCode}`}
              </span>
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
            <div className="flex justify-between">
              <span className="text-muted-foreground">Matched</span>
              <span className="font-medium text-green-500">{data.matchedLinks}</span>
            </div>
          </div>

          {/* Discrepancy list */}
          {data.discrepancies.length > 0 && (
            <div className="pt-2 border-t border-[var(--border)]">
              <div className="flex items-center gap-1.5 mb-2">
                <AlertTriangle className="h-3.5 w-3.5 text-amber-500" />
                <span className="font-medium">{data.discrepancies.length} Issues</span>
              </div>
              <div className="space-y-2">
                <DiscrepancySection
                  discrepancies={data.discrepancies}
                  type="missing_isis"
                  label="missing ISIS"
                  dotColor="#ef4444"
                />
                <DiscrepancySection
                  discrepancies={data.discrepancies}
                  type="extra_isis"
                  label="extra adjacencies"
                  dotColor="#f59e0b"
                />
              </div>
            </div>
          )}

          {data.discrepancies.length === 0 && (
            <div className="pt-2 border-t border-[var(--border)] text-green-500 flex items-center gap-1.5">
              <div className="w-2 h-2 rounded-full bg-green-500" />
              All links healthy
            </div>
          )}

          {/* Edge legend */}
          <div className="pt-2 border-t border-[var(--border)]">
            <div className="text-muted-foreground mb-1.5">Edge Colors</div>
            <div className="space-y-1">
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 bg-green-500" />
                <span>Matched</span>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 bg-red-500" style={{ borderTop: '2px dashed #ef4444' }} />
                <span>Missing ISIS</span>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="w-4 h-0.5 bg-amber-500" />
                <span>Extra adjacency</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {data?.error && (
        <div className="text-destructive">{data.error}</div>
      )}
    </div>
  )
}
