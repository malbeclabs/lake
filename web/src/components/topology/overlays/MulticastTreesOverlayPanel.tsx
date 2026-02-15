import { useState, useEffect, useMemo } from 'react'
import { Radio, X } from 'lucide-react'
import { useTopology } from '../TopologyContext'
import {
  fetchMulticastGroups,
  type MulticastGroupListItem,
  type MulticastGroupDetail,
  type TopologyValidator,
} from '@/lib/api'

// Colors for multicast publishers — exported so map/globe/graph views use the same palette
// eslint-disable-next-line react-refresh/only-export-components
export const MULTICAST_PUBLISHER_COLORS = [
  { light: '#9333ea', dark: '#a855f7' },  // purple
  { light: '#2563eb', dark: '#3b82f6' },  // blue
  { light: '#16a34a', dark: '#22c55e' },  // green
  { light: '#ea580c', dark: '#f97316' },  // orange
  { light: '#0891b2', dark: '#06b6d4' },  // cyan
  { light: '#dc2626', dark: '#ef4444' },  // red
  { light: '#ca8a04', dark: '#eab308' },  // yellow
  { light: '#db2777', dark: '#ec4899' },  // pink
]

interface MulticastTreesOverlayPanelProps {
  isDark: boolean
  selectedGroup: string | null  // Single selected group code
  onSelectGroup: (code: string | null) => void
  groupDetails: Map<string, MulticastGroupDetail>  // Cached group details
  // Publisher/subscriber filtering
  enabledPublishers: Set<string>  // device PKs of enabled publishers
  enabledSubscribers: Set<string>  // device PKs of enabled subscribers
  onTogglePublisher: (devicePK: string) => void
  onToggleSubscriber: (devicePK: string) => void
  // Publisher color map for consistent colors
  publisherColorMap: Map<string, number>
  // Dim other links toggle
  dimOtherLinks: boolean
  onToggleDimOtherLinks: () => void
  // Animate flow toggle
  animateFlow: boolean
  onToggleAnimateFlow: () => void
  // Validators overlay
  validators: TopologyValidator[]
  showTreeValidators: boolean
  onToggleShowTreeValidators: () => void
}

export function MulticastTreesOverlayPanel({
  isDark,
  selectedGroup,
  onSelectGroup,
  groupDetails,
  enabledPublishers,
  enabledSubscribers,
  onTogglePublisher,
  onToggleSubscriber,
  publisherColorMap,
  dimOtherLinks,
  onToggleDimOtherLinks,
  animateFlow,
  onToggleAnimateFlow,
  validators,
  showTreeValidators,
  onToggleShowTreeValidators,
}: MulticastTreesOverlayPanelProps) {
  const { toggleOverlay } = useTopology()
  const [groups, setGroups] = useState<MulticastGroupListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Fetch groups on mount
  useEffect(() => {
    setError(null)
    fetchMulticastGroups()
      .then(setGroups)
      .catch(err => {
        console.error('Failed to fetch multicast groups:', err)
        setError('Failed to load multicast groups. The database table may not exist yet.')
      })
      .finally(() => setLoading(false))
  }, [])

  // Compute member counts from loaded details (more accurate than group list counts)
  const getMemberCounts = (group: MulticastGroupListItem) => {
    const detail = groupDetails.get(group.code)
    if (detail?.members) {
      const pubs = detail.members.filter(m => m.mode === 'P' || m.mode === 'P+S').length
      const subs = detail.members.filter(m => m.mode === 'S' || m.mode === 'P+S').length
      return { pubs, subs }
    }
    return { pubs: group.publisher_count, subs: group.subscriber_count }
  }

  // Map device_pk -> validator for enriching member entries
  const validatorByDevice = useMemo(() => {
    const map = new Map<string, TopologyValidator>()
    for (const v of validators) {
      // First validator per device wins (most have 1:1 mapping)
      if (!map.has(v.device_pk)) map.set(v.device_pk, v)
    }
    return map
  }, [validators])

  const formatStake = (sol: number) => {
    if (sol >= 1e6) return `${(sol / 1e6).toFixed(1)}M SOL`
    if (sol >= 1e3) return `${(sol / 1e3).toFixed(0)}k SOL`
    return `${sol.toFixed(0)} SOL`
  }

  return (
    <div className="p-3 text-xs">
      <div className="flex items-center justify-between mb-2">
        <span className="font-medium flex items-center gap-1.5">
          <Radio className="h-3.5 w-3.5 text-purple-500" />
          Multicast
        </span>
        <button
          onClick={() => toggleOverlay('multicastTrees')}
          className="p-1 hover:bg-[var(--muted)] rounded"
          title="Close"
        >
          <X className="h-3 w-3" />
        </button>
      </div>

      {loading && (
        <div className="text-muted-foreground">Loading groups...</div>
      )}

      {!loading && error && (
        <div className="text-red-500 text-xs">{error}</div>
      )}

      {!loading && !error && groups.length === 0 && (
        <div className="text-muted-foreground">No multicast groups found</div>
      )}

      {!loading && !error && groups.length > 0 && (
        <div className="space-y-3">
          {/* Groups list */}
          <div className="space-y-1">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1.5">
              Groups
            </div>
            <div className="space-y-0.5">
              {groups.map((group) => {
                const isSelected = selectedGroup === group.code
                const detail = groupDetails.get(group.code)
                const { pubs, subs } = getMemberCounts(group)

                return (
                  <div key={group.pk}>
                    <button
                      onClick={() => onSelectGroup(isSelected ? null : group.code)}
                      className={`w-full flex items-center gap-2 px-2 py-1.5 rounded cursor-pointer transition-colors ${
                        isSelected ? 'bg-purple-500/20 text-purple-500' : 'hover:bg-[var(--muted)]'
                      }`}
                    >
                      <div className={`w-3 h-3 rounded-full border-2 flex-shrink-0 flex items-center justify-center ${
                        isSelected ? 'border-purple-500' : 'border-[var(--border)]'
                      }`}>
                        {isSelected && <div className="w-1.5 h-1.5 rounded-full bg-purple-500" />}
                      </div>
                      <span className="font-medium">{group.code}</span>
                      <span className="text-muted-foreground text-[10px] ml-auto">
                        {pubs} pub / {subs} sub
                      </span>
                    </button>

                    {/* Show details when selected */}
                    {isSelected && (
                      <div className="ml-4 mt-1 mb-2 pl-2 border-l border-[var(--border)] text-[10px] space-y-1.5">
                        <div className="text-muted-foreground">
                          IP: {group.multicast_ip}
                        </div>
                        {detail ? (
                          <>
                            {detail.members.filter(m => m.mode === 'P' || m.mode === 'P+S').length > 0 && (
                              <div>
                                <div className="text-muted-foreground uppercase tracking-wider mb-0.5">Publishers</div>
                                {detail.members
                                  .filter(m => m.mode === 'P' || m.mode === 'P+S')
                                  .map(m => {
                                    const pubColorIndex = publisherColorMap.get(m.device_pk) ?? 0
                                    const pubColor = MULTICAST_PUBLISHER_COLORS[pubColorIndex % MULTICAST_PUBLISHER_COLORS.length]
                                    const colorStyle = isDark ? pubColor.dark : pubColor.light
                                    const isEnabled = enabledPublishers.has(m.device_pk)
                                    const validator = validatorByDevice.get(m.device_pk)
                                    return (
                                      <div
                                        key={m.user_pk}
                                        className={`py-0.5 cursor-pointer hover:bg-[var(--muted)] rounded px-1 -mx-1 ${!isEnabled ? 'opacity-40' : ''}`}
                                        onClick={() => onTogglePublisher(m.device_pk)}
                                      >
                                        <div className="flex items-center gap-1">
                                          <input
                                            type="checkbox"
                                            checked={isEnabled}
                                            onChange={() => {}}
                                            className="h-2.5 w-2.5 rounded border-[var(--border)] flex-shrink-0"
                                          />
                                          <div
                                            className="w-3 h-3 rounded-full flex-shrink-0"
                                            style={{ backgroundColor: colorStyle }}
                                          />
                                          {validator ? (
                                            <>
                                              <span className="font-mono" title={validator.vote_pubkey}>
                                                {validator.vote_pubkey.slice(0, 4)}..
                                              </span>
                                              <span className="text-muted-foreground ml-auto flex-shrink-0">
                                                {formatStake(validator.stake_sol ?? 0)}
                                              </span>
                                            </>
                                          ) : (
                                            <>
                                              {m.owner_pubkey ? (
                                                <span className="font-mono" title={m.owner_pubkey}>
                                                  {m.owner_pubkey.slice(0, 4)}..
                                                </span>
                                              ) : (
                                                <span>{m.device_code}</span>
                                              )}
                                            </>
                                          )}
                                        </div>
                                        <div className="flex items-center gap-1 ml-6 text-muted-foreground">
                                          <span>{m.device_code}</span>
                                          <span>({m.metro_code})</span>
                                        </div>
                                      </div>
                                    )
                                  })}
                              </div>
                            )}
                            {detail.members.filter(m => m.mode === 'S' || m.mode === 'P+S').length > 0 && (
                              <div>
                                <div className="text-muted-foreground uppercase tracking-wider mb-0.5">Subscribers</div>
                                {detail.members
                                  .filter(m => m.mode === 'S' || m.mode === 'P+S')
                                  .map(m => {
                                    const isEnabled = enabledSubscribers.has(m.device_pk)
                                    const validator = validatorByDevice.get(m.device_pk)
                                    return (
                                      <div
                                        key={m.user_pk + '-sub'}
                                        className={`py-0.5 cursor-pointer hover:bg-[var(--muted)] rounded px-1 -mx-1 ${!isEnabled ? 'opacity-40' : ''}`}
                                        onClick={() => onToggleSubscriber(m.device_pk)}
                                      >
                                        <div className="flex items-center gap-1">
                                          <input
                                            type="checkbox"
                                            checked={isEnabled}
                                            onChange={() => {}}
                                            className="h-2.5 w-2.5 rounded border-[var(--border)] flex-shrink-0"
                                          />
                                          <div className="w-3 h-3 rounded-full bg-red-500 flex-shrink-0" />
                                          {validator ? (
                                            <>
                                              <span className="font-mono" title={validator.vote_pubkey}>
                                                {validator.vote_pubkey.slice(0, 4)}..
                                              </span>
                                              <span className="text-muted-foreground ml-auto flex-shrink-0">
                                                {formatStake(validator.stake_sol ?? 0)}
                                              </span>
                                            </>
                                          ) : (
                                            <>
                                              {m.owner_pubkey ? (
                                                <span className="font-mono" title={m.owner_pubkey}>
                                                  {m.owner_pubkey.slice(0, 4)}..
                                                </span>
                                              ) : (
                                                <span>{m.device_code}</span>
                                              )}
                                            </>
                                          )}
                                        </div>
                                        <div className="flex items-center gap-1 ml-6 text-muted-foreground">
                                          <span>{m.device_code}</span>
                                          <span>({m.metro_code})</span>
                                        </div>
                                      </div>
                                    )
                                  })}
                              </div>
                            )}
                          </>
                        ) : (
                          <div className="text-muted-foreground">Loading members...</div>
                        )}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>

          {/* Options */}
          <div className="pt-2 border-t border-[var(--border)]">
            <div className="flex items-center justify-between">
              <span className="text-[10px] text-muted-foreground">Show validators</span>
              <button
                onClick={onToggleShowTreeValidators}
                className={`relative inline-flex h-4 w-7 items-center rounded-full transition-colors ${
                  showTreeValidators ? 'bg-purple-500' : 'bg-[var(--muted)]'
                }`}
              >
                <span
                  className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${
                    showTreeValidators ? 'translate-x-3.5' : 'translate-x-0.5'
                  }`}
                />
              </button>
            </div>
            <div className="flex items-center justify-between mt-1.5">
              <span className="text-[10px] text-muted-foreground">Dim other links</span>
              <button
                onClick={onToggleDimOtherLinks}
                className={`relative inline-flex h-4 w-7 items-center rounded-full transition-colors ${
                  dimOtherLinks ? 'bg-purple-500' : 'bg-[var(--muted)]'
                }`}
              >
                <span
                  className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${
                    dimOtherLinks ? 'translate-x-3.5' : 'translate-x-0.5'
                  }`}
                />
              </button>
            </div>
            <div className="flex items-center justify-between mt-1.5">
              <span className="text-[10px] text-muted-foreground">Animate flow</span>
              <button
                onClick={onToggleAnimateFlow}
                className={`relative inline-flex h-4 w-7 items-center rounded-full transition-colors ${
                  animateFlow ? 'bg-purple-500' : 'bg-[var(--muted)]'
                }`}
              >
                <span
                  className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${
                    animateFlow ? 'translate-x-3.5' : 'translate-x-0.5'
                  }`}
                />
              </button>
            </div>
          </div>

          {/* Legend */}
          <div className="pt-2 border-t border-[var(--border)]">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1.5">
              Legend
            </div>
            <div className="space-y-1.5 text-[10px]">
              <div className="flex items-center gap-2">
                <div className="flex gap-0.5">
                  {MULTICAST_PUBLISHER_COLORS.slice(0, 4).map((c, i) => (
                    <div
                      key={i}
                      className="w-2 h-2 rounded-full"
                      style={{ backgroundColor: isDark ? c.dark : c.light }}
                    />
                  ))}
                </div>
                <span>Publisher (each has unique color)</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-red-500 flex-shrink-0" />
                <span>Subscriber (destination)</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-6 h-0.5 bg-purple-500 rounded" />
                <span>Tree path</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: isDark ? '#a855f7' : '#7c3aed' }} />
                <span>Validator</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
