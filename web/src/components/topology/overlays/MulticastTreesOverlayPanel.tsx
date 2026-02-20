import { useState, useEffect, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Radio, X, ChevronDown, ChevronRight, Settings2, User, Server, BarChart3 } from 'lucide-react'
import { AreaChart, Area, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid, ReferenceLine } from 'recharts'
import { useTopology } from '../TopologyContext'
import { EntityLink } from '../EntityLink'
import { formatTrafficRate, formatBandwidth } from '../utils'
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
  onSetAllPublishers: (enabled: boolean) => void
  onSetAllSubscribers: (enabled: boolean) => void
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

function shortenPubkey(pk: string, chars = 6): string {
  if (pk.length <= chars * 2 + 2) return pk
  return `${pk.slice(0, chars)}..${pk.slice(-chars)}`
}

function formatPps(pps: number): string {
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)}M pps`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(1)}k pps`
  return `${Math.round(pps)} pps`
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
  onToggle: () => void
  colorDot: React.ReactNode
}

function MemberRow({ member, isEnabled, onToggle, colorDot }: MemberRowProps) {
  const isValidator = !!member.node_pubkey
  return (
    <div
      className={`py-1.5 px-2 cursor-pointer rounded-md bg-[var(--muted)]/50 transition-opacity ${!isEnabled ? 'opacity-40' : ''}`}
      onClick={(e) => {
        // Don't toggle when clicking a link
        if ((e.target as HTMLElement).closest('a')) return
        onToggle()
      }}
    >
      <div className="flex items-center gap-1.5">
        <input
          type="checkbox"
          checked={isEnabled}
          onChange={() => {}}
          className="h-2.5 w-2.5 rounded border-[var(--border)] flex-shrink-0"
        />
        {colorDot}
        {isValidator ? (
          <Server className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        ) : (
          <User className="h-3 w-3 text-muted-foreground flex-shrink-0" />
        )}
        <div className="flex-1 min-w-0">
          <EntityLink
            to={`/dz/users/${member.user_pk}`}
            className="font-mono text-xs"
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
          {member.traffic_bps > 0 && (
            <span>{formatTrafficRate(member.traffic_bps)}{member.traffic_pps > 0 ? ` ${formatPps(member.traffic_pps)}` : ''}</span>
          )}
          {member.stake_sol > 0 && (
            <span>{formatStake(member.stake_sol)}</span>
          )}
        </div>
      </div>
      <div className="flex items-center gap-1.5 ml-6 mt-0.5 text-[10px] text-muted-foreground">
        {(() => {
          const timing = leaderTimingText(member)
          return timing ? <span className={member.is_leader ? 'text-amber-500' : ''}>{timing}</span> : null
        })()}
        {member.vote_pubkey && (
          <EntityLink
            to={`/solana/validators/${member.vote_pubkey}`}
            className="font-mono hover:underline"
            title={member.vote_pubkey}
          >
            {shortenPubkey(member.vote_pubkey, 4)}
          </EntityLink>
        )}
        {member.device_code && (
          <EntityLink
            to={`/dz/devices/${member.device_pk}`}
            className="hover:underline"
            title={member.device_code}
          >
            {member.device_code}
          </EntityLink>
        )}
        {member.tunnel_id > 0 && (
          <span className="text-muted-foreground/60">t{member.tunnel_id}</span>
        )}
      </div>
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
  onTogglePublisher,
  onToggleSubscriber,
  onSetAllPublishers,
  onSetAllSubscribers,
  publisherColorMap,
  dimOtherLinks,
  onToggleDimOtherLinks,
  animateFlow,
  onToggleAnimateFlow,
  validators: _validators,
  showTreeValidators,
  onToggleShowTreeValidators,
}: MulticastTreesOverlayPanelProps) {
  const { toggleOverlay } = useTopology()
  const [groups, setGroups] = useState<MulticastGroupListItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'publishers' | 'subscribers'>('publishers')
  const [optionsOpen, setOptionsOpen] = useState(true)

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

  const publishersByMetro = useMemo(() => groupByMetro(publishers), [publishers])
  const subscribersByMetro = useMemo(() => groupByMetro(subscribers), [subscribers])

  // Check if all are enabled for select/deselect all
  const allPublishersEnabled = publishers.length > 0 && publishers.every(m => enabledPublishers.has(m.device_pk))
  const allSubscribersEnabled = subscribers.length > 0 && subscribers.every(m => enabledSubscribers.has(m.device_pk))

  return (
    <div className="p-3 text-xs">
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <span className="font-medium flex items-center gap-1.5 text-sm">
          <Radio className="h-4 w-4 text-purple-500" />
          Multicast
        </span>
        <button
          onClick={() => toggleOverlay('multicastTrees')}
          className="p-1 hover:bg-[var(--muted)] rounded"
          title="Close"
        >
          <X className="h-3.5 w-3.5" />
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
          <div>
            <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1.5">
              Groups
            </div>
            <div className="space-y-0.5">
              {groups.map((group) => {
                const isSelected = selectedGroup === group.code
                const { pubs, subs } = getMemberCounts(group)

                return (
                  <button
                    key={group.pk}
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
                )
              })}
            </div>
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
                  {/* Tabs */}
                  <div className="flex border-b border-[var(--border)] mb-2">
                    <button
                      onClick={() => setActiveTab('publishers')}
                      className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors -mb-px ${
                        activeTab === 'publishers'
                          ? 'border-purple-500 text-purple-500'
                          : 'border-transparent text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      Publishers ({publishers.length})
                    </button>
                    <button
                      onClick={() => setActiveTab('subscribers')}
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
                        <button
                          onClick={() => onSetAllPublishers(!allPublishersEnabled)}
                          className="text-[10px] text-muted-foreground hover:text-foreground transition-colors"
                        >
                          {allPublishersEnabled ? 'Deselect all' : 'Select all'}
                        </button>
                      )}
                      {publishers.length > 0 && !publishers.some(m => m.is_leader) && publishers.some(m => m.current_slot > 0) && (
                        <div className="text-muted-foreground text-[10px] py-1 italic">No DZ validator is currently leader</div>
                      )}
                      {publishers.length === 0 && (
                        <div className="text-muted-foreground text-[10px] py-2">No publishers</div>
                      )}
                      {publishersByMetro.map(([metro, members]) => (
                        <MetroGroup
                          key={metro}
                          metro={metro}
                          members={members}

                          enabledMembers={enabledPublishers}
                          onToggleMember={onTogglePublisher}
                        
                          keySuffix="-pub"
                          colorDotForMember={(m) => {
                            const pubColorIndex = publisherColorMap.get(m.device_pk) ?? 0
                            const pubColor = MULTICAST_PUBLISHER_COLORS[pubColorIndex % MULTICAST_PUBLISHER_COLORS.length]
                            const colorStyle = isDark ? pubColor.dark : pubColor.light
                            return (
                              <div
                                className="w-3 h-3 rounded-full flex-shrink-0"
                                style={{ backgroundColor: colorStyle }}
                              />
                            )
                          }}
                        />
                      ))}
                    </div>
                  )}

                  {/* Subscribers tab */}
                  {activeTab === 'subscribers' && (
                    <div className="space-y-2">
                      {subscribers.length > 1 && (
                        <button
                          onClick={() => onSetAllSubscribers(!allSubscribersEnabled)}
                          className="text-[10px] text-muted-foreground hover:text-foreground transition-colors"
                        >
                          {allSubscribersEnabled ? 'Deselect all' : 'Select all'}
                        </button>
                      )}
                      {subscribers.length === 0 && (
                        <div className="text-muted-foreground text-[10px] py-2">No subscribers</div>
                      )}
                      {subscribersByMetro.map(([metro, members]) => (
                        <MetroGroup
                          key={metro}
                          metro={metro}
                          members={members}

                          enabledMembers={enabledSubscribers}
                          onToggleMember={onToggleSubscriber}
                        
                          keySuffix="-sub"
                          colorDotForMember={() => (
                            <div className="w-3 h-3 rounded-full bg-red-500 flex-shrink-0" />
                          )}
                        />
                      ))}
                    </div>
                  )}
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
              </div>
            )}
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

const TRAFFIC_TIME_RANGES = ['1h', '6h', '12h', '24h'] as const

function formatTime(timeStr: string): string {
  const d = new Date(timeStr)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

function formatAxisBps(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)}T`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)}G`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)}M`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)}K`
  return `${bps.toFixed(0)}`
}

/** Collapsible traffic chart for a selected multicast group */
function MulticastTrafficChartSection({
  groupCode,
  members,
  isDark,
  publisherColorMap,
}: {
  groupCode: string
  members: MulticastMember[]
  isDark: boolean
  publisherColorMap: Map<string, number>
}) {
  const [open, setOpen] = useState(false)
  const [timeRange, setTimeRange] = useState<string>('1h')

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['multicast-traffic', groupCode, timeRange],
    queryFn: () => fetchMulticastGroupTraffic(groupCode, timeRange),
    refetchInterval: 30000,
    enabled: open,
  })

  // Build device code lookup and color map from members
  const deviceInfo = useMemo(() => {
    const map = new Map<string, { code: string; mode: string }>()
    for (const m of members) {
      if (!map.has(m.device_pk)) {
        const effectiveMode = m.mode === 'P+S' ? 'P' : m.mode
        map.set(m.device_pk, { code: m.device_code || m.device_pk.slice(0, 8), mode: effectiveMode })
      }
    }
    return map
  }, [members])

  // Transform traffic data: publishers Rx (positive), subscribers Tx (negative)
  const { chartData, pubKeys, subKeys } = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return { chartData: [], pubKeys: [] as string[], subKeys: [] as string[] }

    const pubs = new Set<string>()
    const subs = new Set<string>()
    const timeMap = new Map<string, Record<string, string | number>>()

    for (const p of trafficData) {
      const info = deviceInfo.get(p.device_pk)
      const isPub = info?.mode === 'P' || p.mode === 'P'
      if (isPub) pubs.add(p.device_pk)
      else subs.add(p.device_pk)

      let row = timeMap.get(p.time)
      if (!row) {
        row = { time: p.time } as Record<string, string | number>
        timeMap.set(p.time, row)
      }
      // Publishers: positive in_bps; Subscribers: negative out_bps
      const bps = isPub ? p.in_bps : -p.out_bps
      row[p.device_pk] = ((row[p.device_pk] as number) ?? 0) + bps
    }

    const data = [...timeMap.values()].sort((a, b) =>
      String(a.time).localeCompare(String(b.time))
    )
    return { chartData: data, pubKeys: [...pubs], subKeys: [...subs] }
  }, [trafficData, deviceInfo])

  // Assign colors: publishers get their palette color, subscribers get a muted tone
  const getColor = (devicePK: string) => {
    const info = deviceInfo.get(devicePK)
    if (info?.mode === 'P') {
      const colorIdx = publisherColorMap.get(devicePK) ?? 0
      const c = MULTICAST_PUBLISHER_COLORS[colorIdx % MULTICAST_PUBLISHER_COLORS.length]
      return isDark ? c.dark : c.light
    }
    // Subscriber — use red tones
    return isDark ? '#ef4444' : '#dc2626'
  }

  return (
    <div className="border-t border-[var(--border)] pt-2">
      <button
        onClick={() => setOpen(o => !o)}
        className="flex items-center gap-1.5 text-[10px] text-muted-foreground uppercase tracking-wider w-full hover:text-foreground transition-colors"
      >
        <BarChart3 className="h-3 w-3" />
        Traffic
        {open ? <ChevronDown className="h-3 w-3 ml-auto" /> : <ChevronRight className="h-3 w-3 ml-auto" />}
      </button>
      {open && (
        <div className="mt-2">
          {/* Time range pills */}
          <div className="flex gap-1 mb-2">
            {TRAFFIC_TIME_RANGES.map(r => (
              <button
                key={r}
                onClick={() => setTimeRange(r)}
                className={`px-1.5 py-0.5 text-[10px] rounded ${
                  timeRange === r
                    ? 'bg-purple-500/20 text-purple-500 font-medium'
                    : 'text-muted-foreground hover:text-foreground hover:bg-[var(--muted)]'
                }`}
              >
                {r}
              </button>
            ))}
          </div>

          {isLoading && (
            <div className="text-[10px] text-muted-foreground py-4 text-center">Loading...</div>
          )}

          {!isLoading && chartData.length === 0 && (
            <div className="text-[10px] text-muted-foreground py-4 text-center">No traffic data</div>
          )}

          {!isLoading && chartData.length > 0 && (
            <div className="relative">
              <span className="absolute top-0 left-[38px] text-[9px] text-muted-foreground/60 pointer-events-none z-10">▲ Publishers</span>
              <span className="absolute bottom-4 left-[38px] text-[9px] text-muted-foreground/60 pointer-events-none z-10">▼ Subscribers</span>
              <div className="h-[200px]">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />
                    <XAxis
                      dataKey="time"
                      tick={{ fontSize: 8 }}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={formatTime}
                    />
                    <YAxis
                      tick={{ fontSize: 8 }}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(v) => formatAxisBps(Math.abs(v))}
                      width={35}
                    />
                    <ReferenceLine y={0} stroke="var(--border)" strokeWidth={1} />
                    <RechartsTooltip
                      contentStyle={{
                        backgroundColor: 'var(--card)',
                        border: '1px solid var(--border)',
                        borderRadius: '6px',
                        fontSize: '10px',
                      }}
                      labelFormatter={formatTime}
                      formatter={(value, name) => {
                        const v = value as number
                        const info = deviceInfo.get(String(name))
                        const role = info?.mode === 'P' ? 'pub' : 'sub'
                        return [formatBandwidth(Math.abs(v)), `${info?.code ?? String(name).slice(0, 8)} (${role})`]
                      }}
                    />
                    {pubKeys.map(dk => (
                      <Area
                        key={dk}
                        type="monotone"
                        dataKey={dk}
                        stroke={getColor(dk)}
                        fill={getColor(dk)}
                        fillOpacity={0.2}
                        strokeWidth={1.5}
                        dot={false}
                        name={dk}
                      />
                    ))}
                    {subKeys.map(dk => (
                      <Area
                        key={dk}
                        type="monotone"
                        dataKey={dk}
                        stroke={getColor(dk)}
                        fill={getColor(dk)}
                        fillOpacity={0.1}
                        strokeWidth={1.5}
                        strokeDasharray="4 2"
                        dot={false}
                        name={dk}
                      />
                    ))}
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              {/* Legend */}
              {(pubKeys.length > 0 || subKeys.length > 0) && (
                <div className="flex flex-wrap gap-x-3 gap-y-1 mt-1.5">
                  {pubKeys.map(dk => {
                    const info = deviceInfo.get(dk)
                    return (
                      <div key={dk} className="flex items-center gap-1 text-[10px] text-muted-foreground">
                        <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: getColor(dk) }} />
                        <span>{info?.code ?? dk.slice(0, 8)} (pub)</span>
                      </div>
                    )
                  })}
                  {subKeys.map(dk => {
                    const info = deviceInfo.get(dk)
                    return (
                      <div key={dk} className="flex items-center gap-1 text-[10px] text-muted-foreground">
                        <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: getColor(dk) }} />
                        <span>{info?.code ?? dk.slice(0, 8)} (sub)</span>
                      </div>
                    )
                  })}
                </div>
              )}
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
  onToggleMember,
  keySuffix,
  colorDotForMember,
}: {
  metro: string
  members: MulticastMember[]
  enabledMembers: Set<string>
  onToggleMember: (devicePK: string) => void
  keySuffix: string
  colorDotForMember: (m: MulticastMember) => React.ReactNode
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
        <span className="ml-auto">{members.length}</span>
      </button>
      {open && (
        <div className="space-y-1 mt-1 ml-1">
          {members.map(m => (
            <MemberRow
              key={m.user_pk + keySuffix}
              member={m}
              isEnabled={enabledMembers.has(m.device_pk)}
              onToggle={() => onToggleMember(m.device_pk)}
              colorDot={colorDotForMember(m)}
            />
          ))}
        </div>
      )}
    </div>
  )
}
