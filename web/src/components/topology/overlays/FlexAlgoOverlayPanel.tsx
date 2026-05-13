import { Network, X } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useTopology } from '../TopologyContext'
import { fetchTopologies } from '@/lib/api'

interface FlexAlgoOverlayPanelProps {
  isDark: boolean
}

export function FlexAlgoOverlayPanel({ isDark }: FlexAlgoOverlayPanelProps) {
  void isDark // reserved for future theming
  const {
    toggleOverlay,
    flexAlgoTopology, setFlexAlgoTopology,
    flexAlgoFilterDefault, setFlexAlgoFilterDefault,
    flexAlgoFilterDrained, setFlexAlgoFilterDrained,
  } = useTopology()

  const { data } = useQuery({
    queryKey: ['topologies'],
    queryFn: fetchTopologies,
    staleTime: 60_000,
  })
  const topologies = data?.topologies
  const totalLinkCount = data?.total_link_count ?? 0
  const untaggedLinkCount = data?.untagged_link_count ?? 0
  const drainedLinkCount = data?.drained_link_count ?? 0

  // Determine what legend to show based on current state
  const showTopologyLegend = flexAlgoTopology !== null && !flexAlgoFilterDefault && !flexAlgoFilterDrained
  const showDefaultFilterLegend = flexAlgoTopology === null && flexAlgoFilterDefault && !flexAlgoFilterDrained
  const showDrainedFilterLegend = flexAlgoTopology === null && !flexAlgoFilterDefault && flexAlgoFilterDrained
  const showUnionFilterLegend = flexAlgoTopology === null && flexAlgoFilterDefault && flexAlgoFilterDrained
  const showTopologyDrainedLegend = flexAlgoTopology !== null && flexAlgoFilterDrained

  return (
    <div className="p-3 text-xs">
      <div className="flex items-center justify-between mb-2">
        <span className="font-medium flex items-center gap-1.5">
          <Network className="h-3.5 w-3.5 text-purple-500" />
          Flex-Algo
        </span>
        <button
          onClick={() => toggleOverlay('flexAlgo')}
          className="p-1 hover:bg-[var(--muted)] rounded"
          title="Close"
        >
          <X className="h-3 w-3" />
        </button>
      </div>

      {/* Section 1 — Topology view (radio buttons) */}
      <div className="text-muted-foreground mb-2">
        Topology view
      </div>

      <div className="space-y-1.5">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="flexAlgoTopology"
            checked={flexAlgoTopology === null}
            onChange={() => setFlexAlgoTopology(null)}
            className="accent-purple-500"
          />
          <span>All links</span>
          <span className="text-muted-foreground ml-auto">{totalLinkCount}</span>
        </label>

        {topologies?.map((t) => (
          <label key={t.pk} className="flex items-center gap-2 cursor-pointer">
            <input
              type="radio"
              name="flexAlgoTopology"
              checked={flexAlgoTopology === t.name}
              onChange={() => setFlexAlgoTopology(t.name)}
              className="accent-green-500"
            />
            <span>{t.name}</span>
            <span className="text-muted-foreground ml-auto">{t.link_count}</span>
          </label>
        ))}
      </div>

      {/* Section 2 — Filters (checkboxes) */}
      <hr className="border-[var(--border)] my-3" />

      <div className="text-muted-foreground mb-2">
        Filters
      </div>

      <div className="space-y-1.5">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={flexAlgoFilterDefault}
            onChange={(e) => setFlexAlgoFilterDefault(e.target.checked)}
            className="accent-cyan-500"
          />
          <span>Only default links</span>
          <span className="text-muted-foreground ml-auto">{untaggedLinkCount}</span>
        </label>

        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={flexAlgoFilterDrained}
            onChange={(e) => setFlexAlgoFilterDrained(e.target.checked)}
            className="accent-amber-500"
          />
          <span className="text-amber-500">Only drained links</span>
          <span className="text-muted-foreground ml-auto">{drainedLinkCount}</span>
        </label>
      </div>

      {/* Dynamic legend */}
      {(showTopologyLegend || showDefaultFilterLegend || showDrainedFilterLegend || showUnionFilterLegend || showTopologyDrainedLegend) && (
        <>
          <hr className="border-[var(--border)] my-2" />
          <div className="space-y-1">
            {showTopologyLegend && (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-green-500 rounded" />
                  <span>in topology</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 rounded" style={{ borderBottom: '2px dashed #f59e0b' }} />
                  <span className="text-amber-500">drained</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
                  <span className="text-muted-foreground opacity-50">excluded</span>
                </div>
              </>
            )}
            {showDefaultFilterLegend && (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-cyan-500 rounded" />
                  <span>default</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
                  <span className="text-muted-foreground opacity-50">has named topology</span>
                </div>
              </>
            )}
            {showDrainedFilterLegend && (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 rounded" style={{ borderBottom: '2px dashed #f59e0b' }} />
                  <span className="text-amber-500">drained</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
                  <span className="text-muted-foreground opacity-50">not drained</span>
                </div>
              </>
            )}
            {showUnionFilterLegend && (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-cyan-500 rounded" />
                  <span>default</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 rounded" style={{ borderBottom: '2px dashed #f59e0b' }} />
                  <span className="text-amber-500">drained</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
                  <span className="text-muted-foreground opacity-50">other</span>
                </div>
              </>
            )}
            {showTopologyDrainedLegend && (
              <>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 rounded" style={{ borderBottom: '2px dashed #f59e0b' }} />
                  <span className="text-amber-500">drained member</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
                  <span className="text-muted-foreground opacity-50">other</span>
                </div>
              </>
            )}
          </div>
        </>
      )}
    </div>
  )
}
