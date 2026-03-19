import { useState } from 'react'
import { GitCompare, ChevronDown, ChevronRight } from 'lucide-react'
import type { TopologyCompareResponse, TopologyDiscrepancy } from '@/lib/api'
import { useTopology } from '../TopologyContext'

interface ComparePanelProps {
  data: TopologyCompareResponse | null
  isLoading: boolean
}

const STATUS_ORDER: Record<string, number> = {
  'activated': 0,
  'provisioning': 1,
  'soft-drained': 2,
  'hard-drained': 3,
}

const STATUS_LABELS: Record<string, string> = {
  'activated': 'Activated',
  'provisioning': 'Provisioning',
  'soft-drained': 'Soft-drained',
  'hard-drained': 'Hard-drained',
}

const STATUS_COLORS: Record<string, string> = {
  'activated': 'text-foreground',
  'provisioning': 'text-blue-500',
  'soft-drained': 'text-yellow-500',
  'hard-drained': 'text-orange-500',
}

function sortDiscrepancies(discrepancies: TopologyDiscrepancy[]): TopologyDiscrepancy[] {
  return [...discrepancies].sort((a, b) => {
    const aOrder = STATUS_ORDER[a.linkStatus ?? 'activated'] ?? 99
    const bOrder = STATUS_ORDER[b.linkStatus ?? 'activated'] ?? 99
    if (aOrder !== bOrder) return aOrder - bOrder
    return (a.linkCode ?? '').localeCompare(b.linkCode ?? '')
  })
}

function DiscrepancyItem({ d }: { d: TopologyDiscrepancy }) {
  const { setSelection, setHoveredDiscrepancyKey } = useTopology()
  const status = d.linkStatus
  const statusLabel = status && status !== 'activated' ? STATUS_LABELS[status] : null
  const statusColor = status ? STATUS_COLORS[status] : ''
  const hoverKey = `${d.deviceAPK}|${d.deviceBPK}`

  return (
    <button
      className="flex items-center gap-1.5 w-full text-left py-0.5 px-1 rounded text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
      title={d.details}
      onClick={() => {
        setHoveredDiscrepancyKey(null)
        // Drained/provisioning links aren't in the topology data, so select a device instead
        if (d.linkPK && d.linkStatus === 'activated') {
          setSelection({ type: 'link', id: d.linkPK })
        } else {
          setSelection({ type: 'device', id: d.deviceAPK })
        }
      }}
      onMouseEnter={() => setHoveredDiscrepancyKey(hoverKey)}
      onMouseLeave={() => setHoveredDiscrepancyKey(null)}
    >
      <span className="truncate flex-1">{d.linkCode || `${d.deviceACode} → ${d.deviceBCode}`}</span>
      {statusLabel && <span className={`shrink-0 ${statusColor}`}>{statusLabel.toLowerCase()}</span>}
    </button>
  )
}

function StatusRow({ count, label, color, discrepancies }: {
  count: number
  label: string
  color: string
  discrepancies?: TopologyDiscrepancy[]
}) {
  const [expanded, setExpanded] = useState(true)

  const hasItems = discrepancies && discrepancies.length > 0
  const sorted = hasItems ? sortDiscrepancies(discrepancies) : []

  return (
    <div>
      <div
        role={hasItems ? 'button' : undefined}
        onClick={hasItems ? () => setExpanded(!expanded) : undefined}
        className={`flex items-center gap-2 py-1 ${hasItems ? 'cursor-pointer hover:bg-muted -mx-1.5 px-1.5 rounded' : ''}`}
      >
        <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
        <span className="flex-1">{label}</span>
        <span className="font-medium tabular-nums" style={{ color }}>{count}</span>
        {hasItems && (
          expanded ? <ChevronDown className="h-3 w-3 text-muted-foreground" /> : <ChevronRight className="h-3 w-3 text-muted-foreground" />
        )}
      </div>
      {expanded && hasItems && (
        <div className="ml-4 mt-0.5 mb-1 border-l border-[var(--border)] pl-2 space-y-px">
          {sorted.map((d, i) => <DiscrepancyItem key={i} d={d} />)}
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

          {/* Status breakdown */}
          <div className="pt-2 border-t border-[var(--border)] space-y-0.5">
            <StatusRow
              count={data.matchedLinks}
              label="Matched"
              color="#22c55e"
            />
            <StatusRow
              count={data.discrepancies.filter(d => d.type === 'partial_isis').length}
              label="Partial ISIS"
              color="#ef4444"
              discrepancies={data.discrepancies.filter(d => d.type === 'partial_isis')}
            />
            <StatusRow
              count={data.discrepancies.filter(d => d.type === 'missing_isis').length}
              label="Missing ISIS"
              color="#f59e0b"
              discrepancies={data.discrepancies.filter(d => d.type === 'missing_isis')}
            />
            <StatusRow
              count={data.discrepancies.filter(d => d.type === 'extra_isis').length}
              label="Extra adjacency"
              color="#8b5cf6"
              discrepancies={data.discrepancies.filter(d => d.type === 'extra_isis')}
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
