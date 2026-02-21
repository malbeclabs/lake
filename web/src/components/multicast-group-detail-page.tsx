import { useState, useMemo } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Radio, AlertCircle, ArrowLeft } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid, ReferenceLine } from 'recharts'
import { fetchMulticastGroup, fetchMulticastGroupTraffic, type MulticastMember } from '@/lib/api'
import { formatBandwidth } from '@/components/topology/utils'
import { useDocumentTitle } from '@/hooks/use-document-title'

function formatBps(bps: number): string {
  if (bps === 0) return '—'
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)} Tbps`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)} Gbps`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)} Mbps`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)} Kbps`
  return `${bps.toFixed(0)} bps`
}

function formatAxisBps(bps: number): string {
  if (bps >= 1e12) return `${(bps / 1e12).toFixed(1)}T`
  if (bps >= 1e9) return `${(bps / 1e9).toFixed(1)}G`
  if (bps >= 1e6) return `${(bps / 1e6).toFixed(1)}M`
  if (bps >= 1e3) return `${(bps / 1e3).toFixed(1)}K`
  return `${bps.toFixed(0)}`
}

function formatPps(pps: number): string {
  if (pps === 0) return '—'
  if (pps >= 1e9) return `${(pps / 1e9).toFixed(1)} Gpps`
  if (pps >= 1e6) return `${(pps / 1e6).toFixed(1)} Mpps`
  if (pps >= 1e3) return `${(pps / 1e3).toFixed(1)} Kpps`
  return `${pps.toFixed(0)} pps`
}

type TrafficMetric = 'throughput' | 'packets'

function formatStake(sol: number): string {
  if (sol === 0) return '—'
  if (sol >= 1e6) return `${(sol / 1e6).toFixed(2)}M SOL`
  if (sol >= 1e3) return `${(sol / 1e3).toFixed(1)}K SOL`
  return `${sol.toFixed(0)} SOL`
}

function formatTime(timeStr: string): string {
  const d = new Date(timeStr)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
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

const statusColors: Record<string, string> = {
  active: 'text-green-600 dark:text-green-400',
  activated: 'text-muted-foreground',
  provisioning: 'text-blue-600 dark:text-blue-400',
  suspended: 'text-red-600 dark:text-red-400',
  pending: 'text-amber-600 dark:text-amber-400',
}

const TRAFFIC_COLORS = [
  '#9333ea', '#2563eb', '#16a34a', '#ea580c', '#0891b2', '#ca8a04', '#db2777', '#5bc0de',
]

const TIME_RANGES = ['1h', '6h', '12h', '24h'] as const
const BUCKET_OPTIONS = ['auto', '2s', '10s', '30s', '1m', '2m', '5m', '10m'] as const

function MulticastTrafficChart({ groupCode, members, activeTab }: {
  groupCode: string
  members: MulticastMember[]
  activeTab: 'publishers' | 'subscribers'
}) {
  const [timeRange, setTimeRange] = useState<string>('1h')
  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<string>('auto')

  const autoBucketLabel: Record<string, string> = { '1h': '30s', '6h': '2m', '12h': '5m', '24h': '10m' }

  const bucketSeconds = bucket === 'auto' ? undefined : bucket.endsWith('m')
    ? String(parseInt(bucket) * 60)
    : String(parseInt(bucket))

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['multicast-traffic', groupCode, timeRange, bucket],
    queryFn: () => fetchMulticastGroupTraffic(groupCode, timeRange, bucketSeconds),
    refetchInterval: 30000,
  })

  const tunnelInfo = useMemo(() => {
    const map = new Map<number, { code: string; mode: string }>()
    for (const m of members) {
      if (m.tunnel_id > 0 && !map.has(m.tunnel_id)) {
        const effectiveMode = m.mode === 'P+S' ? 'P' : m.mode
        map.set(m.tunnel_id, {
          code: m.device_code || m.device_pk.slice(0, 8),
          mode: effectiveMode,
        })
      }
    }
    return map
  }, [members])

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
      if (metric === 'throughput') {
        row[`t${p.tunnel_id}_in`] = p.out_bps
        row[`t${p.tunnel_id}_out`] = -p.in_bps
      } else {
        row[`t${p.tunnel_id}_in`] = p.out_pps
        row[`t${p.tunnel_id}_out`] = -p.in_pps
      }
    }

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

  const getTunnelColor = (tunnelId: number) => {
    const idx = tunnelIds.indexOf(tunnelId)
    return TRAFFIC_COLORS[idx % TRAFFIC_COLORS.length]
  }

  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

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

  const fmtValue = metric === 'throughput' ? formatBps : formatPps
  const fmtAxis = (v: number) => formatAxisBps(Math.abs(v))

  return (
    <div className="border border-border rounded-lg p-4 bg-card">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">Traffic ({activeTab})</h3>
        <div className="flex items-center gap-2">
          <select
            value={metric}
            onChange={e => setMetric(e.target.value as TrafficMetric)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            <option value="throughput">bps</option>
            <option value="packets">pps</option>
          </select>
          <select
            value={bucket}
            onChange={e => setBucket(e.target.value)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {BUCKET_OPTIONS.map(b => (
              <option key={b} value={b}>{b === 'auto' ? `auto (${autoBucketLabel[timeRange] || '30s'})` : b}</option>
            ))}
          </select>
          <select
            value={timeRange}
            onChange={e => setTimeRange(e.target.value)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {TIME_RANGES.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin mr-2" />
          Loading traffic data...
        </div>
      )}

      {!isLoading && chartData.length === 0 && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          No traffic data available
        </div>
      )}

      {!isLoading && chartData.length > 0 && (
        <div>
          <div className="h-56 relative">
            <span className="absolute top-0 left-0 text-[10px] text-muted-foreground/60 pointer-events-none z-10">▲ In</span>
            <span className="absolute bottom-5 left-0 text-[10px] text-muted-foreground/60 pointer-events-none z-10">▼ Out</span>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={chartData}
                onMouseMove={(state) => {
                  if (state?.activeTooltipIndex != null) setHoveredIdx(Number(state.activeTooltipIndex))
                }}
                onMouseLeave={() => setHoveredIdx(null)}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" opacity={0.5} />
                <XAxis
                  dataKey="time"
                  tick={{ fontSize: 9 }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={formatTime}
                />
                <YAxis
                  tick={{ fontSize: 9 }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={fmtAxis}
                  width={45}
                />
                <ReferenceLine y={0} stroke="var(--border)" strokeWidth={1} />
                <RechartsTooltip
                  content={() => null}
                  cursor={{ stroke: 'var(--muted-foreground)', strokeWidth: 1, strokeDasharray: '4 2' }}
                />
                {tunnelIds.map(tid => (
                  <Line
                    key={`${tid}_in`}
                    type="monotone"
                    dataKey={`t${tid}_in`}
                    stroke={getTunnelColor(tid)}
                    strokeWidth={1.5}
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
                {tunnelIds.map(tid => (
                  <Line
                    key={`${tid}_out`}
                    type="monotone"
                    dataKey={`t${tid}_out`}
                    stroke={getTunnelColor(tid)}
                    strokeWidth={1.5}
                    strokeDasharray="4 2"
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
          {tunnelIds.length > 0 && (
            <div className="mt-2 text-xs">
              <div className="grid gap-x-4 gap-y-0.5" style={{ gridTemplateColumns: 'auto 1fr 1fr 1fr' }}>
                <div />
                <div className="text-muted-foreground font-medium">Device</div>
                <div className="text-muted-foreground font-medium text-right">Inbound</div>
                <div className="text-muted-foreground font-medium text-right">Outbound</div>
                {tunnelIds.map((tid, i) => {
                  const info = tunnelInfo.get(tid)
                  const vals = displayValues.get(tid)
                  return (
                    <div key={tid} className="contents">
                      <div className="flex items-center">
                        <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: TRAFFIC_COLORS[i % TRAFFIC_COLORS.length] }} />
                      </div>
                      <div className="text-foreground truncate font-mono">
                        {info?.code ?? `t${tid}`}
                      </div>
                      <div className="text-right tabular-nums">{vals ? fmtValue(vals.inBps) : '—'}</div>
                      <div className="text-right tabular-nums">{vals ? fmtValue(vals.outBps) : '—'}</div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export function MulticastGroupDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()
  const [activeTab, setActiveTab] = useState<'publishers' | 'subscribers'>('publishers')

  const { data: group, isLoading, error } = useQuery({
    queryKey: ['multicast-group', pk],
    queryFn: () => fetchMulticastGroup(pk!),
    enabled: !!pk,
  })

  useDocumentTitle(group?.code || 'Multicast Group')

  const publishers = useMemo(() =>
    group?.members.filter(m => m.mode === 'P' || m.mode === 'P+S') ?? [],
    [group]
  )

  const subscribers = useMemo(() =>
    group?.members.filter(m => m.mode === 'S' || m.mode === 'P+S') ?? [],
    [group]
  )

  const activeMembers = activeTab === 'publishers' ? publishers : subscribers

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !group) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Multicast group not found</div>
          <button
            onClick={() => navigate('/dz/multicast-groups')}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to multicast groups
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-4 sm:px-8 py-8">
        {/* Back button */}
        <button
          onClick={() => navigate('/dz/multicast-groups')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to multicast groups
        </button>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <Radio className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium font-mono">{group.code}</h1>
            <div className="text-sm text-muted-foreground font-mono">{group.multicast_ip}</div>
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Status</h3>
            <div className={`text-sm capitalize ${statusColors[group.status] || ''}`}>{group.status}</div>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Max Bandwidth</h3>
            <div className="text-sm">{group.max_bandwidth > 0 ? formatBandwidth(group.max_bandwidth) : '—'}</div>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Publishers</h3>
            <div className="text-sm">{publishers.length}</div>
          </div>

          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Subscribers</h3>
            <div className="text-sm">{subscribers.length}</div>
          </div>
        </div>

        {/* Members table */}
        <div className="border border-border rounded-lg bg-card mb-6">
          <div className="flex border-b border-border">
            <button
              onClick={() => setActiveTab('publishers')}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors -mb-px ${
                activeTab === 'publishers'
                  ? 'border-purple-500 text-purple-500'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              Publishers ({publishers.length})
            </button>
            <button
              onClick={() => setActiveTab('subscribers')}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors -mb-px ${
                activeTab === 'subscribers'
                  ? 'border-purple-500 text-purple-500'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              Subscribers ({subscribers.length})
            </button>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-4 py-3 font-medium">User</th>
                  <th className="px-4 py-3 font-medium">Device</th>
                  <th className="px-4 py-3 font-medium">Metro</th>
                  <th className="px-4 py-3 font-medium">DZ IP</th>
                  <th className="px-4 py-3 font-medium text-right">Tunnel</th>
                  <th className="px-4 py-3 font-medium text-right">Stake</th>
                  <th className="px-4 py-3 font-medium">Leader Schedule</th>
                </tr>
              </thead>
              <tbody>
                {activeMembers.map((member) => (
                  <tr
                    key={member.user_pk}
                    className="border-b border-border last:border-b-0 hover:bg-muted transition-colors"
                  >
                    <td className="px-4 py-3">
                      <Link
                        to={`/dz/users/${member.user_pk}`}
                        className="text-blue-600 dark:text-blue-400 hover:underline font-mono text-sm"
                      >
                        {member.user_pk.slice(0, 8)}...{member.user_pk.slice(-4)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {member.device_pk ? (
                        <Link
                          to={`/dz/devices/${member.device_pk}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline font-mono"
                        >
                          {member.device_code || member.device_pk.slice(0, 8)}
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {member.metro_pk ? (
                        <Link
                          to={`/dz/metros/${member.metro_pk}`}
                          className="text-blue-600 dark:text-blue-400 hover:underline"
                        >
                          {member.metro_name || member.metro_code}
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm font-mono text-muted-foreground">
                      {member.dz_ip || '—'}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground font-mono">
                      {member.tunnel_id > 0 ? member.tunnel_id : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm tabular-nums text-right text-muted-foreground">
                      {member.stake_sol > 0 ? formatStake(member.stake_sol) : '—'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {member.is_leader ? (
                        <span className="inline-flex items-center px-1.5 py-0.5 rounded-full bg-amber-500/15 text-amber-500 font-medium text-xs">
                          Leading now
                        </span>
                      ) : (
                        (() => {
                          const timing = leaderTimingText(member)
                          return timing ? (
                            <span className="text-muted-foreground">{timing}</span>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )
                        })()
                      )}
                    </td>
                  </tr>
                ))}
                {activeMembers.length === 0 && (
                  <tr>
                    <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
                      No {activeTab} found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Traffic chart */}
        {pk && group.members.length > 0 && (
          <MulticastTrafficChart
            groupCode={pk}
            members={group.members}
            activeTab={activeTab}
          />
        )}
      </div>
    </div>
  )
}
