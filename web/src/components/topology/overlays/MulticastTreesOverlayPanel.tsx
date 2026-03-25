import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import { useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import { Radio, X, ChevronDown, ChevronRight, Settings2, User, Server, BarChart3, Info, Search, Loader2, RefreshCw } from 'lucide-react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { useTopology } from '../TopologyContext'
import { EntityLink } from '../EntityLink'
import { formatBandwidth } from '../utils'
import {
  fetchMulticastGroups,
  fetchMulticastGroupTraffic,
  type MulticastGroupListItem,
  type MulticastGroupDetail,
  type MulticastMember,
  type TopologyValidator,
} from '@/lib/api'

// Colors for multicast publishers — exported so map/globe/graph views use the same palette
// eslint-disable-next-line react-refresh/only-export-components
export const MULTICAST_PUBLISHER_COLORS = [
  { light: '#7c5cbf', dark: '#a78bda' },  // soft purple
  { light: '#4a8fe7', dark: '#6ba8f2' },  // soft blue
  { light: '#3dad6f', dark: '#5ec98d' },  // soft green
  { light: '#d4854a', dark: '#e8a06e' },  // soft orange
  { light: '#2ba3a8', dark: '#4fc5ca' },  // soft teal
  { light: '#d46a7e', dark: '#e88d9e' },  // soft rose
  { light: '#c4a23d', dark: '#dbbe5c' },  // soft gold
  { light: '#c45fa0', dark: '#da82b8' },  // soft magenta
]

interface MulticastTreesOverlayPanelProps {
  isDark: boolean
  selectedGroup: string | null  // Single selected group code
  onSelectGroup: (code: string | null) => void
  groupDetails: Map<string, MulticastGroupDetail>  // Cached group details
  // Publisher/subscriber filtering
  enabledPublishers: Set<string>  // user PKs of enabled publishers
  enabledSubscribers: Set<string>  // user PKs of enabled subscribers
  onSetEnabledPublishers: (updater: Set<string> | ((prev: Set<string>) => Set<string>)) => void
  onSetEnabledSubscribers: (updater: Set<string> | ((prev: Set<string>) => Set<string>)) => void
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
  // Combine segments toggle
  combineSegments: boolean
  onToggleCombineSegments: () => void
  // Hover coordination with map/globe/graph
  onHoverMember: (devicePK: string | null) => void
}

function Toggle({ enabled, onToggle }: { enabled: boolean; onToggle: () => void }) {
  return (
    <button
      onClick={onToggle}
      className={`relative inline-flex h-4 w-7 items-center rounded-full transition-colors ${
        enabled ? 'bg-purple-500' : 'bg-[var(--muted)]'
      }`}
    >
      <span
        className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${
          enabled ? 'translate-x-3.5' : 'translate-x-0.5'
        }`}
      />
    </button>
  )
}

function SelectionHint() {
  return (
    <div className="relative group flex-shrink-0">
      <Info className="h-3 w-3 text-muted-foreground/50 group-hover:text-muted-foreground cursor-help" />
      <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-1 hidden group-hover:block z-50 pointer-events-none">
        <div className="bg-[var(--popover)] text-[var(--popover-foreground)] border border-[var(--border)] rounded-md px-2 py-1.5 text-[10px] leading-relaxed whitespace-nowrap shadow-md">
          <div><strong>Click</strong> — solo select</div>
          <div><strong>{navigator.platform.includes('Mac') ? 'Cmd' : 'Ctrl'}+click</strong> — toggle</div>
          <div><strong>Shift+click</strong> — range select</div>
        </div>
      </div>
    </div>
  )
}

function shortenPubkey(pk: string, chars = 6): string {
  if (pk.length <= chars * 2 + 2) return pk
  return `${pk.slice(0, chars)}..${pk.slice(-chars)}`
}


function formatStake(sol: number): string {
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(1)}M SOL`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(0)}k SOL`
  return `${sol.toFixed(0)} SOL`
}

function formatSlotDelta(slotDelta: number): string {
  const seconds = Math.abs(slotDelta) * 0.4
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`
  return `${(seconds / 3600).toFixed(1)}h`
}

function leaderTimingText(member: MulticastMember): string | null {
  if (!member.current_slot) return null
  if (member.is_leader) return 'Leading now'
  const parts: string[] = []
  if (member.last_leader_slot != null) {
    parts.push(`Leader ${formatSlotDelta(member.current_slot - member.last_leader_slot)} ago`)
  }
  if (member.next_leader_slot != null) {
    parts.push(`Next in ${formatSlotDelta(member.next_leader_slot - member.current_slot)}`)
  }
  return parts.length > 0 ? parts.join(' · ') : null
}

interface MemberRowProps {
  member: MulticastMember
  isEnabled: boolean
  isHovered: boolean
  onClick: (e: React.MouseEvent) => void
  onMouseEnter: () => void
  onMouseLeave: () => void
  accentColor: string
}

function MemberRow({ member, isEnabled, isHovered, onClick, onMouseEnter, onMouseLeave, accentColor }: MemberRowProps) {
  const isValidator = !!member.node_pubkey
  return (
    <div
      className={`py-1.5 pr-2 pl-1.5 cursor-pointer rounded-md transition-all select-none border-l-2 ${
        isHovered ? 'bg-[var(--muted)]' : 'bg-[var(--muted)]/50'
      } ${!isEnabled ? 'opacity-55' : ''}`}
      style={{ borderLeftColor: isEnabled ? accentColor : 'transparent' }}
      onClick={(e) => {
        // Don't toggle when clicking a link
        if ((e.target as HTMLElement).closest('a')) return
        onClick(e)
      }}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="flex items-center gap-1.5">
        {isValidator ? (
          <Server className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        ) : (
          <User className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        )}
        <div className="flex-1 min-w-0">
          <EntityLink
            to={`/dz/users/${member.user_pk}`}
            className="font-mono text-[10px]"
            title={member.user_pk}
          >
            {shortenPubkey(member.user_pk)}
          </EntityLink>
        </div>
        <div className="flex items-center gap-1.5 flex-shrink-0 ml-auto text-[10px] text-muted-foreground">
          {member.is_leader && (
            <span className="px-1 py-0 rounded-full bg-amber-500/20 text-amber-500 font-medium text-[9px]">
              LEADER
            </span>
          )}
          {member.stake_sol > 0 && (
            <span>{formatStake(member.stake_sol)}</span>
          )}
        </div>
      </div>
      {(member.device_code || member.is_leader || leaderTimingText(member)) && (
        <div className="flex items-center gap-1.5 ml-4.5 mt-0.5 text-[10px] text-muted-foreground">
          {(() => {
            const timing = leaderTimingText(member)
            return timing ? <span className={member.is_leader ? 'text-amber-500' : ''}>{timing}</span> : null
          })()}
          {member.device_code && (
            <EntityLink
              to={`/dz/devices/${member.device_pk}`}
              className="hover:underline"
              title={member.device_code}
            >
              {member.device_code}
            </EntityLink>
          )}
        </div>
      )}
    </div>
  )
}

export function MulticastTreesOverlayPanel({
  isDark,
  selectedGroup,
  onSelectGroup,
  groupDetails,
  enabledPublishers,
  enabledSubscribers,
  onSetEnabledPublishers,
  onSetEnabledSubscribers,
  publisherColorMap,
  dimOtherLinks,
  onToggleDimOtherLinks,
  animateFlow,
  onToggleAnimateFlow,
  validators: _validators, // eslint-disable-line @typescript-eslint/no-unused-vars
  showTreeValidators,
  onToggleShowTreeValidators,
  combineSegments,
  onToggleCombineSegments,
  onHoverMember,
}: MulticastTreesOverlayPanelProps) {
  const { toggleOverlay } = useTopology()
  const [groups, setGroups] = useState<MulticastGroupListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'publishers' | 'subscribers'>('publishers')
  const [publisherSearch, setPublisherSearch] = useState('')
  const [subscriberSearch, setSubscriberSearch] = useState('')
  const [groupsOpen, setGroupsOpen] = useState(true)
  const [membersOpen, setMembersOpen] = useState(true)
  const [optionsOpen, setOptionsOpen] = useState(true)

  // Hover state: which user_pk is hovered in the member list (or from traffic chart)
  const [hoveredUserPK, setHoveredUserPK] = useState<string | null>(null)

  // Click state for solo/cmd/shift selection
  const [lastClickedPubIndex, setLastClickedPubIndex] = useState<number | null>(null)
  const [lastClickedSubIndex, setLastClickedSubIndex] = useState<number | null>(null)

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


  // Get selected group detail and split members
  const selectedDetail = selectedGroup ? groupDetails.get(selectedGroup) : null
  const selectedGroupItem = selectedGroup ? groups.find(g => g.code === selectedGroup) : null

  const publishers = useMemo(() =>
    selectedDetail?.members.filter(m => m.mode === 'P' || m.mode === 'P+S') ?? [],
    [selectedDetail]
  )

  const subscribers = useMemo(() =>
    selectedDetail?.members.filter(m => m.mode === 'S' || m.mode === 'P+S') ?? [],
    [selectedDetail]
  )

  // Filter members by search query (matches shortened pubkey, device_code, metro_code)
  const filterMembers = useCallback((members: MulticastMember[], query: string) => {
    if (!query) return members
    const q = query.toLowerCase()
    return members.filter(m => {
      const shortKey = shortenPubkey(m.user_pk).toLowerCase()
      const fullKey = m.user_pk.toLowerCase()
      const device = (m.device_code || '').toLowerCase()
      const metro = (m.metro_code || '').toLowerCase()
      return shortKey.includes(q) || fullKey.includes(q) || device.includes(q) || metro.includes(q)
    })
  }, [])

  // Group members by metro
  const groupByMetro = (members: MulticastMember[]) => {
    const map = new Map<string, MulticastMember[]>()
    for (const m of members) {
      const key = m.metro_code || 'Unknown'
      const list = map.get(key) ?? []
      list.push(m)
      map.set(key, list)
    }
    return [...map.entries()].sort((a, b) => b[1].length - a[1].length)
  }

  const publishersByMetro = useMemo(() => groupByMetro(filterMembers(publishers, publisherSearch)), [publishers, publisherSearch, filterMembers])
  const subscribersByMetro = useMemo(() => groupByMetro(filterMembers(subscribers, subscriberSearch)), [subscribers, subscriberSearch, filterMembers])

  // Build ordered user_pk arrays from metro-grouped render order (for shift-click range selection)
  const orderedPublisherUserPKs = useMemo(() =>
    publishersByMetro.flatMap(([, members]) => members.map(m => m.user_pk)),
    [publishersByMetro]
  )
  const orderedSubscriberUserPKs = useMemo(() =>
    subscribersByMetro.flatMap(([, members]) => members.map(m => m.user_pk)),
    [subscribersByMetro]
  )

  // Build userPK -> devicePK lookup
  const userPKToDevicePK = useMemo(() => {
    const map = new Map<string, string>()
    if (selectedDetail?.members) {
      for (const m of selectedDetail.members) {
        if (!map.has(m.user_pk)) map.set(m.user_pk, m.device_pk)
      }
    }
    return map
  }, [selectedDetail])

  // When hoveredUserPK changes, derive devicePK and call onHoverMember
  useEffect(() => {
    if (!hoveredUserPK) {
      onHoverMember(null)
      return
    }
    const devicePK = userPKToDevicePK.get(hoveredUserPK) ?? null
    onHoverMember(devicePK)
  }, [hoveredUserPK, userPKToDevicePK, onHoverMember])

  // Derive tunnelId from hoveredUserPK for traffic chart coordination
  const hoveredTunnelId = useMemo(() => {
    if (!hoveredUserPK || !selectedDetail?.members) return null
    const member = selectedDetail.members.find(m => m.user_pk === hoveredUserPK && m.tunnel_id > 0)
    return member?.tunnel_id ?? null
  }, [hoveredUserPK, selectedDetail])

  // Solo/cmd/shift click handler for publishers
  const handlePublisherClick = useCallback((userPK: string, index: number, event: React.MouseEvent) => {
    if (event.shiftKey && lastClickedPubIndex !== null) {
      const start = Math.min(lastClickedPubIndex, index)
      const end = Math.max(lastClickedPubIndex, index)
      onSetEnabledPublishers(prev => {
        const next = new Set(prev)
        for (let i = start; i <= end; i++) {
          next.add(orderedPublisherUserPKs[i])
        }
        return next
      })
    } else if (event.ctrlKey || event.metaKey) {
      onSetEnabledPublishers(prev => {
        const next = new Set(prev)
        if (next.has(userPK)) next.delete(userPK)
        else next.add(userPK)
        return next
      })
    } else {
      // Solo click: if already solo-selected, show all; otherwise solo-select
      const isSolo = enabledPublishers.size === 1 && enabledPublishers.has(userPK)
      if (isSolo) {
        onSetEnabledPublishers(new Set(publishers.map(m => m.user_pk)))
      } else {
        onSetEnabledPublishers(new Set([userPK]))
      }
    }
    setLastClickedPubIndex(index)
  }, [lastClickedPubIndex, orderedPublisherUserPKs, enabledPublishers, publishers, onSetEnabledPublishers])

  // Solo/cmd/shift click handler for subscribers
  const handleSubscriberClick = useCallback((userPK: string, index: number, event: React.MouseEvent) => {
    if (event.shiftKey && lastClickedSubIndex !== null) {
      const start = Math.min(lastClickedSubIndex, index)
      const end = Math.max(lastClickedSubIndex, index)
      onSetEnabledSubscribers(prev => {
        const next = new Set(prev)
        for (let i = start; i <= end; i++) {
          next.add(orderedSubscriberUserPKs[i])
        }
        return next
      })
    } else if (event.ctrlKey || event.metaKey) {
      onSetEnabledSubscribers(prev => {
        const next = new Set(prev)
        if (next.has(userPK)) next.delete(userPK)
        else next.add(userPK)
        return next
      })
    } else {
      const isSolo = enabledSubscribers.size === 1 && enabledSubscribers.has(userPK)
      if (isSolo) {
        onSetEnabledSubscribers(new Set(subscribers.map(m => m.user_pk)))
      } else {
        onSetEnabledSubscribers(new Set([userPK]))
      }
    }
    setLastClickedSubIndex(index)
  }, [lastClickedSubIndex, orderedSubscriberUserPKs, enabledSubscribers, subscribers, onSetEnabledSubscribers])

  // Callback for traffic chart legend hover -> set hoveredUserPK
  const handleTrafficChartHoverUserPK = useCallback((userPK: string | null) => {
    setHoveredUserPK(userPK)
  }, [])

  return (
    <div className="p-3 text-xs">
      {/* Header */}
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
          {/* Groups list — collapsible */}
          <div>
            <button
              onClick={() => setGroupsOpen(o => !o)}
              className="flex items-center gap-1.5 text-[10px] text-muted-foreground uppercase tracking-wider w-full hover:text-foreground transition-colors mb-1.5"
            >
              <Radio className="h-3 w-3" />
              Groups
              {groupsOpen ? <ChevronDown className="h-3 w-3 ml-auto" /> : <ChevronRight className="h-3 w-3 ml-auto" />}
            </button>
            {groupsOpen && (
              <div className="space-y-0.5">
                {groups.map((group) => {
                  const isSelected = selectedGroup === group.code
                  const { pubs, subs } = getMemberCounts(group)

                  return (
                    <button
                      key={group.pk}
                      onClick={() => {
                        const nextSelected = isSelected ? null : group.code
                        onSelectGroup(nextSelected)
                        setGroupsOpen(!nextSelected)
                        setPublisherSearch('')
                        setSubscriberSearch('')
                      }}
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
                  )
                })}
              </div>
            )}
          </div>

          {/* Selected group detail */}
          {selectedGroup && (
            <div className="border-t border-[var(--border)] pt-3">
              {/* Summary header */}
              {selectedGroupItem && (
                <div className="mb-3">
                  <div className="font-medium text-sm">{selectedGroupItem.code}</div>
                  <div className="text-[10px] text-muted-foreground mt-0.5">
                    {selectedGroupItem.multicast_ip}
                  </div>
                </div>
              )}

              {selectedDetail ? (
                <>
                  {/* Collapsible members section */}
                  <div className="border-t border-[var(--border)] pt-2">
                    <button
                      onClick={() => setMembersOpen(o => !o)}
                      className="flex items-center gap-1.5 text-[10px] text-muted-foreground uppercase tracking-wider w-full hover:text-foreground transition-colors"
                    >
                      <User className="h-3 w-3" />
                      Members
                      {membersOpen ? <ChevronDown className="h-3 w-3 ml-auto" /> : <ChevronRight className="h-3 w-3 ml-auto" />}
                    </button>
                    {membersOpen && (
                      <div className="mt-2">
                        {/* Tabs */}
                        <div className="flex border-b border-[var(--border)] mb-2">
                          <button
                            onClick={() => { setActiveTab('publishers'); setPublisherSearch(''); setSubscriberSearch('') }}
                            className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors -mb-px ${
                              activeTab === 'publishers'
                                ? 'border-purple-500 text-purple-500'
                                : 'border-transparent text-muted-foreground hover:text-foreground'
                            }`}
                          >
                            Publishers ({publishers.length})
                          </button>
                          <button
                            onClick={() => { setActiveTab('subscribers'); setPublisherSearch(''); setSubscriberSearch('') }}
                            className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors -mb-px ${
                              activeTab === 'subscribers'
                                ? 'border-purple-500 text-purple-500'
                                : 'border-transparent text-muted-foreground hover:text-foreground'
                            }`}
                          >
                            Subscribers ({subscribers.length})
                          </button>
                        </div>

                        {/* Publishers tab */}
                        {activeTab === 'publishers' && (
                          <div className="space-y-2">
                            {publishers.length > 1 && (
                              <div className="relative">
                                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground pointer-events-none" />
                                <input
                                  type="text"
                                  value={publisherSearch}
                                  onChange={e => setPublisherSearch(e.target.value)}
                                  placeholder="Search publishers..."
                                  className="w-full text-[10px] bg-[var(--muted)] border border-[var(--border)] rounded-md pl-6 pr-6 py-1 text-foreground placeholder:text-muted-foreground/50 outline-none focus:border-purple-500/50"
                                />
                                {publisherSearch && (
                                  <button
                                    onClick={() => setPublisherSearch('')}
                                    className="absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 hover:bg-[var(--border)] rounded"
                                  >
                                    <X className="h-2.5 w-2.5 text-muted-foreground" />
                                  </button>
                                )}
                              </div>
                            )}
                            {publishers.length > 1 && (
                              <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
                                <button
                                  onClick={() => onSetEnabledPublishers(new Set(publishers.map(m => m.user_pk)))}
                                  className="hover:text-foreground transition-colors"
                                >
                                  all
                                </button>
                                {' / '}
                                <button
                                  onClick={() => onSetEnabledPublishers(new Set())}
                                  className="hover:text-foreground transition-colors"
                                >
                                  none
                                </button>
                                <SelectionHint />
                              </div>
                            )}
                            {publishers.length === 0 && (
                              <div className="text-muted-foreground text-[10px] py-2">No publishers</div>
                            )}
                            <div className="max-h-[300px] overflow-y-auto space-y-2">
                              {publishersByMetro.map(([metro, members]) => (
                                <MetroGroup
                                  key={metro}
                                  metro={metro}
                                  members={members}

                                  enabledMembers={enabledPublishers}
                                  onMemberClick={handlePublisherClick}
                                  orderedUserPKs={orderedPublisherUserPKs}

                                  hoveredUserPK={hoveredUserPK}
                                  onHoverUserPK={setHoveredUserPK}

                                  keySuffix="-pub"
                                  accentColorForMember={(m) => {
                                    const pubColorIndex = publisherColorMap.get(m.device_pk) ?? 0
                                    const pubColor = MULTICAST_PUBLISHER_COLORS[pubColorIndex % MULTICAST_PUBLISHER_COLORS.length]
                                    return isDark ? pubColor.dark : pubColor.light
                                  }}
                                />
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Subscribers tab */}
                        {activeTab === 'subscribers' && (
                          <div className="space-y-2">
                            {subscribers.length > 1 && (
                              <div className="relative">
                                <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground pointer-events-none" />
                                <input
                                  type="text"
                                  value={subscriberSearch}
                                  onChange={e => setSubscriberSearch(e.target.value)}
                                  placeholder="Search subscribers..."
                                  className="w-full text-[10px] bg-[var(--muted)] border border-[var(--border)] rounded-md pl-6 pr-6 py-1 text-foreground placeholder:text-muted-foreground/50 outline-none focus:border-purple-500/50"
                                />
                                {subscriberSearch && (
                                  <button
                                    onClick={() => setSubscriberSearch('')}
                                    className="absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 hover:bg-[var(--border)] rounded"
                                  >
                                    <X className="h-2.5 w-2.5 text-muted-foreground" />
                                  </button>
                                )}
                              </div>
                            )}
                            {subscribers.length > 1 && (
                              <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
                                <button
                                  onClick={() => onSetEnabledSubscribers(new Set(subscribers.map(m => m.user_pk)))}
                                  className="hover:text-foreground transition-colors"
                                >
                                  all
                                </button>
                                {' / '}
                                <button
                                  onClick={() => onSetEnabledSubscribers(new Set())}
                                  className="hover:text-foreground transition-colors"
                                >
                                  none
                                </button>
                                <SelectionHint />
                              </div>
                            )}
                            {subscribers.length === 0 && (
                              <div className="text-muted-foreground text-[10px] py-2">No subscribers</div>
                            )}
                            <div className="max-h-[300px] overflow-y-auto space-y-2">
                              {subscribersByMetro.map(([metro, members]) => (
                                <MetroGroup
                                  key={metro}
                                  metro={metro}
                                  members={members}

                                  enabledMembers={enabledSubscribers}
                                  onMemberClick={handleSubscriberClick}
                                  orderedUserPKs={orderedSubscriberUserPKs}

                                  hoveredUserPK={hoveredUserPK}
                                  onHoverUserPK={setHoveredUserPK}

                                  keySuffix="-sub"
                                  accentColorForMember={() => '#14b8a6'}
                                />
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </>
              ) : (
                <div className="text-muted-foreground text-xs py-2">Loading members...</div>
              )}
            </div>
          )}

          {/* Traffic chart — collapsible */}
          {selectedGroup && selectedDetail && (
            <MulticastTrafficChartSection
              groupCode={selectedGroup}
              members={selectedDetail.members}
              isDark={isDark}
              publisherColorMap={publisherColorMap}
              activeTab={activeTab}
              enabledMembers={activeTab === 'publishers' ? enabledPublishers : enabledSubscribers}
              hoveredTunnelId={hoveredTunnelId}
              onHoverUserPK={handleTrafficChartHoverUserPK}
            />
          )}

          {/* Options — collapsible */}
          <div className="border-t border-[var(--border)] pt-2">
            <button
              onClick={() => setOptionsOpen(o => !o)}
              className="flex items-center gap-1.5 text-[10px] text-muted-foreground uppercase tracking-wider w-full hover:text-foreground transition-colors"
            >
              <Settings2 className="h-3 w-3" />
              Options
              {optionsOpen ? <ChevronDown className="h-3 w-3 ml-auto" /> : <ChevronRight className="h-3 w-3 ml-auto" />}
            </button>
            {optionsOpen && (
              <div className="mt-2 space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Show validators</span>
                  <Toggle enabled={showTreeValidators} onToggle={onToggleShowTreeValidators} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Dim other links</span>
                  <Toggle enabled={dimOtherLinks} onToggle={onToggleDimOtherLinks} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Animate flow</span>
                  <Toggle enabled={animateFlow} onToggle={onToggleAnimateFlow} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Combine segments</span>
                  <Toggle enabled={combineSegments} onToggle={onToggleCombineSegments} />
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const TRAFFIC_TIME_RANGES = ['1h', '3h', '6h', '12h', '24h', '3d', '7d', '14d', '30d'] as const
const BUCKET_OPTIONS = ['auto', '10 SECOND', '30 SECOND', '1 MINUTE', '5 MINUTE', '10 MINUTE', '15 MINUTE', '30 MINUTE', '1 HOUR', '4 HOUR'] as const
const BUCKET_LABELS: Record<string, string> = {
  'auto': 'Auto', '10 SECOND': '10s', '30 SECOND': '30s', '1 MINUTE': '1m', '5 MINUTE': '5m',
  '10 MINUTE': '10m', '15 MINUTE': '15m', '30 MINUTE': '30m', '1 HOUR': '1h', '4 HOUR': '4h',
}
function resolveOverlayAutoBucket(timeRange: string): string {
  switch (timeRange) {
    case '1h': return '10 SECOND'
    case '3h': return '30 SECOND'
    case '6h': return '1 MINUTE'
    case '12h': return '10 MINUTE'
    case '24h': return '15 MINUTE'
    case '3d': return '30 MINUTE'
    case '7d': return '4 HOUR'
    default: return '5 MINUTE'
  }
}

function formatPps(pps: number): string {
  if (pps === 0) return '—'
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(1)} Gpps`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)} Mpps`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(1)} Kpps`
  return `${pps.toFixed(0)} pps`
}

function formatAxisPps(pps: number): string {
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(1)}G`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)}M`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(0)}K`
  return `${pps.toFixed(0)}`
}

function formatAxisBps(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)}T`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)}G`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)}M`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)}K`
  return `${bps.toFixed(0)}`
}

/** Color palette for traffic chart lines — same set used by the main traffic page */
const TRAFFIC_COLORS = [
  '#7c5cbf', // soft purple
  '#4a8fe7', // soft blue
  '#3dad6f', // soft green
  '#d4854a', // soft orange
  '#2ba3a8', // soft teal
  '#c4a23d', // soft gold
  '#c45fa0', // soft magenta
  '#6ba8f2', // soft sky
]

/** Collapsible traffic chart for a selected multicast group */
function MulticastTrafficChartSection({
  groupCode,
  members,
  activeTab,
  enabledMembers,
  hoveredTunnelId,
  onHoverUserPK,
}: {
  groupCode: string
  members: MulticastMember[]
  isDark: boolean
  publisherColorMap: Map<string, number>
  activeTab: 'publishers' | 'subscribers'
  enabledMembers: Set<string>
  hoveredTunnelId: number | null
  onHoverUserPK: (userPK: string | null) => void
}) {
  const queryClient = useQueryClient()
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [open, setOpen] = useState(true)
  const [timeRange, setTimeRange] = useState<string>('1h')
  const [metric, setMetric] = useState<'throughput' | 'packets'>('throughput')
  const [bucket, setBucket] = useState<string>('auto')

  const effectiveBucket = bucket === 'auto' ? resolveOverlayAutoBucket(timeRange) : bucket
  const autoBucketLabel = BUCKET_LABELS[resolveOverlayAutoBucket(timeRange)] || '5m'

  const { data: trafficData, isFetching } = useQuery({
    queryKey: ['multicast-traffic', groupCode, timeRange, effectiveBucket],
    queryFn: () => fetchMulticastGroupTraffic(groupCode, timeRange, effectiveBucket),
    refetchInterval: 30000,
    enabled: open,
    placeholderData: keepPreviousData,
  })

  // Build tunnel info lookup from members: tunnel_id -> { code, mode, userPk }
  const tunnelInfo = useMemo(() => {
    const map = new Map<number, { code: string; mode: string; userPk: string }>()
    for (const m of members) {
      if (m.tunnel_id > 0 && !map.has(m.tunnel_id)) {
        const effectiveMode = m.mode === 'P+S' ? 'P' : m.mode
        map.set(m.tunnel_id, {
          code: m.device_code || m.device_pk.slice(0, 8),
          mode: effectiveMode,
          userPk: m.user_pk,
        })
      }
    }
    return map
  }, [members])

  // Transform traffic data: two lines per tunnel (inbound + outbound from user perspective).
  // Device out_bps = user inbound (positive), device in_bps = user outbound (negative).
  // Filtered by active tab (publishers or subscribers).
  const { chartData, tunnelIds } = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return { chartData: [], tunnelIds: [] as number[] }

    const showPubs = activeTab === 'publishers'
    const tunnels = new Set<number>()
    const timeMap = new Map<string, Record<string, string | number>>()

    for (const p of trafficData) {
      const isPub = p.mode === 'P'
      if (isPub !== showPubs) continue

      tunnels.add(p.tunnel_id)

      let row = timeMap.get(p.time)
      if (!row) {
        row = { time: p.time } as Record<string, string | number>
        timeMap.set(p.time, row)
      }
      // From user perspective: device out = user inbound, device in = user outbound
      if (metric === 'throughput') {
        row[`t${p.tunnel_id}_in`] = p.out_bps
        row[`t${p.tunnel_id}_out`] = -p.in_bps
      } else {
        row[`t${p.tunnel_id}_in`] = p.out_pps
        row[`t${p.tunnel_id}_out`] = -p.in_pps
      }
    }

    // Fill missing tunnels with 0 so Recharts renders continuous lines
    for (const row of timeMap.values()) {
      for (const tid of tunnels) {
        if (!(`t${tid}_in` in row)) row[`t${tid}_in`] = 0
        if (!(`t${tid}_out` in row)) row[`t${tid}_out`] = 0
      }
    }

    const data = [...timeMap.values()].sort((a, b) =>
      String(a.time).localeCompare(String(b.time))
    )
    return { chartData: data, tunnelIds: [...tunnels].sort((a, b) => a - b) }
  }, [trafficData, activeTab, metric])

  // Assign a unique color per tunnel from the palette
  const getTunnelColor = (tunnelId: number) => {
    const idx = tunnelIds.indexOf(tunnelId)
    return TRAFFIC_COLORS[idx % TRAFFIC_COLORS.length]
  }

  // Track hovered chart index for legend table values
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const [localHoveredSeries, setLocalHoveredSeries] = useState<number | null>(null)

  // Merge local legend hover with member list hover (hoveredTunnelId from parent)
  const effectiveHoveredSeries = localHoveredSeries ?? hoveredTunnelId

  // Derive visible series from enabled members — member list is the source of truth
  const visibleSeries = useMemo(() => {
    const visible = new Set<number>()
    for (const tid of tunnelIds) {
      const info = tunnelInfo.get(tid)
      if (info && enabledMembers.has(info.userPk)) {
        visible.add(tid)
      }
    }
    return visible
  }, [tunnelIds, tunnelInfo, enabledMembers])


  // Values to display in the legend: hovered point or latest
  const displayValues = useMemo(() => {
    if (chartData.length === 0) return new Map<number, { inBps: number; outBps: number }>()
    const row = hoveredIdx !== null && hoveredIdx < chartData.length
      ? chartData[hoveredIdx]
      : chartData[chartData.length - 1]
    const map = new Map<number, { inBps: number; outBps: number }>()
    for (const tid of tunnelIds) {
      map.set(tid, {
        inBps: (row[`t${tid}_in`] as number) ?? 0,
        outBps: Math.abs((row[`t${tid}_out`] as number) ?? 0),
      })
    }
    return map
  }, [chartData, tunnelIds, hoveredIdx])

  // Build uPlot columnar data from chartData
  const { uplotData, uplotSeries, serisTidMap } = useMemo(() => {
    if (chartData.length === 0 || tunnelIds.length === 0) {
      return { uplotData: [[]] as uPlot.AlignedData, uplotSeries: [] as uPlot.Series[], serisTidMap: [] as number[] }
    }

    const timestamps = chartData.map(row => new Date(row.time as string).getTime() / 1000)
    const splinePaths = uPlot.paths.spline?.()
    const dataArrays: (number | null)[][] = [timestamps]
    const series: uPlot.Series[] = [{}]
    const tidMap: number[] = [] // maps uPlot series index (offset by 1) to tunnel ID

    for (const tid of tunnelIds) {
      const color = getTunnelColor(tid)

      dataArrays.push(chartData.map(row => (row[`t${tid}_in`] as number) ?? null))
      series.push({
        label: `t${tid}_in`,
        stroke: color,
        width: 1.5,
        alpha: visibleSeries.has(tid) ? 1 : 0,
        points: { show: false },
        paths: splinePaths,
      } as uPlot.Series)
      tidMap.push(tid)

      dataArrays.push(chartData.map(row => (row[`t${tid}_out`] as number) ?? null))
      series.push({
        label: `t${tid}_out`,
        stroke: color,
        width: 1.5,
        dash: [4, 2],
        alpha: visibleSeries.has(tid) ? 1 : 0,
        points: { show: false },
        paths: splinePaths,
      } as uPlot.Series)
      tidMap.push(tid)
    }

    return { uplotData: dataArrays as uPlot.AlignedData, uplotSeries: series, serisTidMap: tidMap }
  }, [chartData, tunnelIds, visibleSeries, getTunnelColor])

  // Create/update uPlot chart
  useEffect(() => {
    if (!chartRef.current || !open || uplotData[0].length === 0) {
      plotRef.current?.destroy()
      plotRef.current = null
      return
    }

    plotRef.current?.destroy()

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 176,
      series: uplotSeries,
      scales: { x: { time: true }, y: { auto: true } },
      axes: [
        { stroke: 'rgba(128,128,128,0.4)', grid: { stroke: 'rgba(128,128,128,0.06)' } },
        {
          stroke: 'rgba(128,128,128,0.4)',
          grid: { stroke: 'rgba(128,128,128,0.06)' },
          values: (_: uPlot, vals: number[]) => vals.map(v =>
            metric === 'throughput' ? formatAxisBps(Math.abs(v)) : formatAxisPps(Math.abs(v))
          ),
          size: 45,
        },
      ],
      cursor: {
        points: { size: 10, width: 2 },
      },
      hooks: {
        setCursor: [(u: uPlot) => {
          setHoveredIdx(u.cursor.idx ?? null)
        }],
      },
      legend: { show: false },
    }

    plotRef.current = new uPlot(opts, uplotData, chartRef.current)

    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        plotRef.current?.setSize({ width: entry.contentRect.width, height: 176 })
      }
    })
    ro.observe(chartRef.current)

    return () => {
      ro.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [uplotData, uplotSeries, open, metric])

  // TODO: restore hover dimming — currently disabled because alpha sync
  // conflicts with chart recreation when visibleSeries changes.
  void effectiveHoveredSeries
  void serisTidMap

  return (
    <div className="border-t border-[var(--border)] pt-2">
      <div className="flex items-center gap-1.5">
        <button
          onClick={() => setOpen(o => !o)}
          className="flex items-center gap-1.5 text-[10px] text-muted-foreground uppercase tracking-wider hover:text-foreground transition-colors"
        >
          <BarChart3 className="h-3 w-3" />
          Traffic ({activeTab})
          {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        </button>
        {open && (
          isFetching ? (
            <Loader2 className="h-3 w-3 animate-spin text-muted-foreground ml-1" />
          ) : (
            <button
              onClick={(e) => { e.stopPropagation(); queryClient.invalidateQueries({ queryKey: ['multicast-traffic', groupCode] }) }}
              className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground ml-1"
              title="Refresh"
            >
              <RefreshCw className="h-3 w-3" />
            </button>
          )
        )}
        {open && (
          <div className="flex gap-1 ml-auto" onClick={e => e.stopPropagation()}>
            <select
              value={metric}
              onChange={e => setMetric(e.target.value as 'throughput' | 'packets')}
              className="text-[10px] bg-transparent border border-border rounded px-1 py-0.5 text-foreground cursor-pointer"
            >
              <option value="throughput">bps</option>
              <option value="packets">pps</option>
            </select>
            <select
              value={bucket}
              onChange={e => setBucket(e.target.value)}
              className="text-[10px] bg-transparent border border-border rounded px-1 py-0.5 text-foreground cursor-pointer"
            >
              {BUCKET_OPTIONS.map(b => (
                <option key={b} value={b}>{b === 'auto' ? `Auto (${autoBucketLabel})` : BUCKET_LABELS[b] || b}</option>
              ))}
            </select>
            <select
              value={timeRange}
              onChange={e => setTimeRange(e.target.value)}
              className="text-[10px] bg-transparent border border-border rounded px-1 py-0.5 text-foreground cursor-pointer"
            >
              {TRAFFIC_TIME_RANGES.map(r => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>
        )}
      </div>
      {open && (
        <div className="mt-2 group/chart">
          <div className="h-0.5 w-full overflow-hidden rounded-full mb-1">
            {isFetching && (
              <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
            )}
          </div>

          {!trafficData && !isFetching && (
            <div className="text-[10px] text-muted-foreground py-4 text-center">No traffic data</div>
          )}

          {(trafficData || isFetching) && (
            <div>
              {/* Chart */}
              <div className="relative" style={{ minHeight: 176 }}>
                <span className="absolute top-0.5 right-1 text-[8px] text-muted-foreground/50 pointer-events-none z-10">▲ In</span>
                <span className="absolute bottom-4 right-1 text-[8px] text-muted-foreground/50 pointer-events-none z-10">▼ Out</span>
                <div ref={chartRef} className="w-full" />
              </div>

              {/* Legend table */}
              <div className="mt-2 text-[10px]">
                <div className="flex items-center gap-2 px-1 py-0.5 text-muted-foreground/60 font-medium">
                  <div className="w-2" />
                  <div className="flex-1 min-w-0">Device</div>
                  <div className="text-right">↓ In</div>
                  <div className="text-right">↑ Out</div>
                </div>
                <div className="max-h-[200px] overflow-y-auto">
                {tunnelIds.map((tid) => {
                  const info = tunnelInfo.get(tid)
                  const vals = displayValues.get(tid)
                  const isVisible = visibleSeries.has(tid)
                  const isHighlighted = hoveredTunnelId === tid
                  return (
                    <div
                      key={tid}
                      className={`flex items-center gap-2 px-1 py-0.5 rounded select-none transition-colors ${
                        isHighlighted ? 'bg-muted/80' : 'hover:bg-muted/60'
                      } ${!isVisible ? 'opacity-55' : ''}`}
                      onMouseEnter={() => {
                        if (isVisible) {
                          setLocalHoveredSeries(tid)
                          // Coordinate back to member list
                          if (info?.userPk) onHoverUserPK(info.userPk)
                        }
                      }}
                      onMouseLeave={() => {
                        setLocalHoveredSeries(null)
                        onHoverUserPK(null)
                      }}
                    >
                      <div className="w-2 h-2 rounded-sm flex-shrink-0" style={{ backgroundColor: !isVisible ? 'var(--muted-foreground)' : getTunnelColor(tid) }} />
                      <div className="flex-1 min-w-0 text-foreground truncate font-mono">
                        {info?.code ?? `t${tid}`} <span className="text-muted-foreground">t{tid}</span>
                        {info?.userPk && <span className="text-muted-foreground"> · {shortenPubkey(info.userPk, 4)}</span>}
                      </div>
                      <div className="text-right font-mono tabular-nums text-foreground">
                        {vals && isVisible ? (metric === 'throughput' ? formatBandwidth(vals.inBps) : formatPps(vals.inBps)) : '—'}
                      </div>
                      <div className="text-right font-mono tabular-nums text-muted-foreground">
                        {vals && isVisible ? (metric === 'throughput' ? formatBandwidth(vals.outBps) : formatPps(vals.outBps)) : '—'}
                      </div>
                    </div>
                  )
                })}
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

/** Collapsible metro group for members */
function MetroGroup({
  metro,
  members,
  enabledMembers,
  onMemberClick,
  orderedUserPKs,
  hoveredUserPK,
  onHoverUserPK,
  keySuffix,
  accentColorForMember,
}: {
  metro: string
  members: MulticastMember[]
  enabledMembers: Set<string>
  onMemberClick: (userPK: string, index: number, event: React.MouseEvent) => void
  orderedUserPKs: string[]
  hoveredUserPK: string | null
  onHoverUserPK: (userPK: string | null) => void
  keySuffix: string
  accentColorForMember: (m: MulticastMember) => string
}) {
  const [open, setOpen] = useState(true)

  return (
    <div>
      <button
        onClick={() => setOpen(o => !o)}
        className="flex items-center gap-1.5 text-[10px] text-muted-foreground w-full hover:text-foreground transition-colors py-0.5"
      >
        {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        <span className="px-1 py-0 rounded bg-[var(--muted)] text-[9px] font-medium">{metro}</span>
        <span className="ml-auto text-muted-foreground/50">{members.length}</span>
      </button>
      {open && (
        <div className="space-y-1 mt-1 ml-1">
          {members.map(m => {
            const orderedIndex = orderedUserPKs.indexOf(m.user_pk)
            return (
              <MemberRow
                key={m.user_pk + keySuffix}
                member={m}
                isEnabled={enabledMembers.has(m.user_pk)}
                isHovered={hoveredUserPK === m.user_pk}
                onClick={(e) => onMemberClick(m.user_pk, orderedIndex, e)}
                onMouseEnter={() => onHoverUserPK(m.user_pk)}
                onMouseLeave={() => onHoverUserPK(null)}
                accentColor={accentColorForMember(m)}
              />
            )
          })}
        </div>
      )}
    </div>
  )
}
