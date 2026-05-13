import { Network, X } from 'lucide-react'
import { useQuery } from '@tanstack/react-query'
import { useTopology } from '../TopologyContext'
import { fetchTopologies } from '@/lib/api'

interface FlexAlgoOverlayPanelProps {
  isDark: boolean
}

export function FlexAlgoOverlayPanel({ isDark }: FlexAlgoOverlayPanelProps) {
  void isDark // reserved for future theming
  const { toggleOverlay, selectedFlexAlgoTopology, setSelectedFlexAlgoTopology } = useTopology()

  const { data } = useQuery({
    queryKey: ['topologies'],
    queryFn: fetchTopologies,
    staleTime: 60_000,
  })
  const topologies = data?.topologies
  const totalLinkCount = data?.total_link_count ?? 0
  const drainedLinkCount = data?.drained_link_count ?? 0

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

      <div className="text-muted-foreground mb-3">
        Filter links by topology membership. Non-member links are dimmed.
      </div>

      <div className="space-y-1.5">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="flexAlgoTopology"
            checked={selectedFlexAlgoTopology === null}
            onChange={() => setSelectedFlexAlgoTopology(null)}
            className="accent-purple-500"
          />
          <span>All links (algo 0)</span>
          <span className="text-muted-foreground ml-auto">{totalLinkCount}</span>
        </label>

        {topologies?.map((t) => (
          <label key={t.pk} className="flex items-center gap-2 cursor-pointer">
            <input
              type="radio"
              name="flexAlgoTopology"
              checked={selectedFlexAlgoTopology === t.name}
              onChange={() => setSelectedFlexAlgoTopology(t.name)}
              className="accent-green-500"
            />
            <span>{t.name}</span>
            <span className="text-muted-foreground ml-auto">{t.link_count}</span>
          </label>
        ))}

        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="flexAlgoTopology"
            checked={selectedFlexAlgoTopology === '__multicast_only__'}
            onChange={() => setSelectedFlexAlgoTopology('__multicast_only__')}
            className="accent-cyan-500"
          />
          <span>Multicast only</span>
          <span className="text-muted-foreground ml-auto">{totalLinkCount - (topologies?.reduce((sum, t) => sum + t.link_count, 0) ?? 0)}</span>
        </label>

        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="flexAlgoTopology"
            checked={selectedFlexAlgoTopology === '__unicast_drained__'}
            onChange={() => setSelectedFlexAlgoTopology('__unicast_drained__')}
            className="accent-amber-500"
          />
          <span className="text-amber-500">Unicast drained</span>
          <span className="text-muted-foreground ml-auto">{drainedLinkCount}</span>
        </label>
      </div>

      {selectedFlexAlgoTopology && selectedFlexAlgoTopology !== '__multicast_only__' && (
        <>
          <hr className="border-[var(--border)] my-2" />
          <div className="space-y-1">
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
          </div>
        </>
      )}

      {selectedFlexAlgoTopology === '__unicast_drained__' && (
        <>
          <hr className="border-[var(--border)] my-2" />
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <div className="w-5 h-0.5 rounded" style={{ borderBottom: '2px dashed #f59e0b' }} />
              <span className="text-amber-500">drained</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
              <span className="text-muted-foreground opacity-50">not drained</span>
            </div>
          </div>
        </>
      )}

      {selectedFlexAlgoTopology === '__multicast_only__' && (
        <>
          <hr className="border-[var(--border)] my-2" />
          <div className="space-y-1">
            <div className="flex items-center gap-2">
              <div className="w-5 h-0.5 bg-cyan-500 rounded" />
              <span>multicast only</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-5 h-0.5 bg-gray-400 rounded opacity-30" />
              <span className="text-muted-foreground opacity-50">has unicast topology</span>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
