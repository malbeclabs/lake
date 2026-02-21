import { useState, useMemo } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Users, AlertCircle, ArrowLeft } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid, ReferenceLine } from 'recharts'
import { fetchUser, fetchUserTraffic, fetchUserMulticastGroups } from '@/lib/api'
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

const TUNNEL_COLORS = [
  '#2563eb', '#9333ea', '#16a34a', '#ea580c', '#0891b2', '#dc2626', '#ca8a04', '#db2777',
]

const TIME_RANGES = ['1h', '6h', '12h', '24h'] as const

function formatTime(timeStr: string): string {
  const d = new Date(timeStr)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

const BUCKET_OPTIONS = ['auto', '2s', '10s', '30s', '1m', '2m', '5m', '10m'] as const

function UserTrafficChart({ userPk }: { userPk: string }) {
  const [timeRange, setTimeRange] = useState<string>('1h')
  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<string>('auto')

  // Default bucket sizes per time range
  const autoBucketLabel: Record<string, string> = { '1h': '30s', '6h': '2m', '12h': '5m', '24h': '10m' }

  // Convert bucket label to seconds for the API
  const bucketSeconds = bucket === 'auto' ? undefined : bucket.endsWith('m')
    ? String(parseInt(bucket) * 60)
    : String(parseInt(bucket))

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['user-traffic', userPk, timeRange, bucket],
    queryFn: () => fetchUserTraffic(userPk, timeRange, bucketSeconds),
    refetchInterval: 30000,
  })

  // Transform data: inbound (positive), outbound (negative), per tunnel
  // Device in = user sending (outbound), device out = user receiving (inbound)
  const { chartData, tunnelIds } = useMemo(() => {
    if (!trafficData || trafficData.length === 0) return { chartData: [], tunnelIds: [] }

    const tunnelSet = new Set<number>()
    const timeMap = new Map<string, Record<string, string | number>>()

    for (const p of trafficData) {
      tunnelSet.add(p.tunnel_id)
      let row = timeMap.get(p.time)
      if (!row) {
        row = { time: p.time }
        timeMap.set(p.time, row)
      }
      if (metric === 'throughput') {
        row[`t${p.tunnel_id}_in`] = p.in_bps
        row[`t${p.tunnel_id}_out`] = -p.out_bps
      } else {
        row[`t${p.tunnel_id}_in`] = p.in_pps
        row[`t${p.tunnel_id}_out`] = -p.out_pps
      }
    }

    const ids = [...tunnelSet].sort((a, b) => a - b)
    const data = [...timeMap.values()].sort((a, b) =>
      String(a.time).localeCompare(String(b.time))
    )
    return { chartData: data, tunnelIds: ids }
  }, [trafficData, metric])

  const fmtValue = metric === 'throughput' ? formatBps : formatPps
  const fmtAxis = (v: number) => formatAxisBps(Math.abs(v))

  // Track hovered chart index for legend table
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  // Values to display in legend: hovered or latest
  const displayValues = useMemo(() => {
    if (chartData.length === 0) return new Map<number, { inVal: number; outVal: number }>()
    const row = hoveredIdx !== null && hoveredIdx < chartData.length
      ? chartData[hoveredIdx]
      : chartData[chartData.length - 1]
    const map = new Map<number, { inVal: number; outVal: number }>()
    for (const tid of tunnelIds) {
      map.set(tid, {
        inVal: (row[`t${tid}_in`] as number) ?? 0,
        outVal: Math.abs((row[`t${tid}_out`] as number) ?? 0),
      })
    }
    return map
  }, [chartData, tunnelIds, hoveredIdx])

  return (
    <div className="border border-border rounded-lg p-4 bg-card col-span-full">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">Traffic History</h3>
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
            <span className="absolute top-0 left-0 text-[10px] text-muted-foreground/60 pointer-events-none z-10">▲ rx</span>
            <span className="absolute bottom-5 left-0 text-[10px] text-muted-foreground/60 pointer-events-none z-10">▼ tx</span>
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
                {tunnelIds.map((tid, i) => (
                  <Line
                    key={`${tid}_in`}
                    type="monotone"
                    dataKey={`t${tid}_in`}
                    stroke={TUNNEL_COLORS[i % TUNNEL_COLORS.length]}
                    strokeWidth={1.5}
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
                {tunnelIds.map((tid, i) => (
                  <Line
                    key={`${tid}_out`}
                    type="monotone"
                    dataKey={`t${tid}_out`}
                    stroke={TUNNEL_COLORS[i % TUNNEL_COLORS.length]}
                    strokeWidth={1.5}
                    strokeDasharray="4 2"
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
          {/* Legend table */}
          {tunnelIds.length > 0 && (
            <div className="mt-2 text-xs">
              <div className="grid gap-x-4 gap-y-0.5" style={{ gridTemplateColumns: 'auto 1fr 1fr' }}>
                <div className="text-muted-foreground font-medium">Tunnel</div>
                <div className="text-muted-foreground font-medium text-right">Inbound</div>
                <div className="text-muted-foreground font-medium text-right">Outbound</div>
                {tunnelIds.map((tid, i) => {
                  const vals = displayValues.get(tid)
                  return (
                    <div key={tid} className="contents">
                      <div className="flex items-center gap-1.5">
                        <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: TUNNEL_COLORS[i % TUNNEL_COLORS.length] }} />
                        <span className="text-muted-foreground">{tid}</span>
                      </div>
                      <div className="text-right tabular-nums">{fmtValue(vals?.inVal ?? 0)}</div>
                      <div className="text-right tabular-nums">{fmtValue(vals?.outVal ?? 0)}</div>
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

const statusColors: Record<string, string> = {
  activated: 'text-muted-foreground',
  provisioning: 'text-blue-600 dark:text-blue-400',
  'soft-drained': 'text-amber-600 dark:text-amber-400',
  drained: 'text-amber-600 dark:text-amber-400',
  suspended: 'text-red-600 dark:text-red-400',
  pending: 'text-amber-600 dark:text-amber-400',
}

export function UserDetailPage() {
  const { pk } = useParams<{ pk: string }>()
  const navigate = useNavigate()

  const { data: user, isLoading, error } = useQuery({
    queryKey: ['user', pk],
    queryFn: () => fetchUser(pk!),
    enabled: !!pk,
  })

  const { data: multicastGroups } = useQuery({
    queryKey: ['user-multicast-groups', pk],
    queryFn: () => fetchUserMulticastGroups(pk!),
    enabled: !!pk,
  })

  useDocumentTitle(user?.pk ? `${user.pk.slice(0, 8)}...${user.pk.slice(-4)}` : 'User')

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !user) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">User not found</div>
          <button
            onClick={() => navigate('/dz/users')}
            className="text-sm text-muted-foreground hover:text-foreground"
          >
            Back to users
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
          onClick={() => navigate('/dz/users')}
          className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to users
        </button>

        {/* Header */}
        <div className="flex items-center gap-3 mb-8">
          <Users className="h-8 w-8 text-muted-foreground" />
          <div>
            <h1 className="text-2xl font-medium font-mono">{user.pk.slice(0, 8)}...{user.pk.slice(-4)}</h1>
            <div className="text-sm text-muted-foreground">{user.kind || 'Unknown type'}</div>
          </div>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* Identity */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Identity</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Status</dt>
                <dd className={`text-sm capitalize ${statusColors[user.status] || ''}`}>{user.status}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Kind</dt>
                <dd className="text-sm">{user.kind || '—'}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Owner Pubkey</dt>
                <dd className="text-sm">
                  <Link to={`/dz/users?search=owner:${user.owner_pubkey}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
                    {user.owner_pubkey.slice(0, 6)}...{user.owner_pubkey.slice(-4)}
                  </Link>
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Client IP</dt>
                <dd className="text-sm">
                  {user.client_ip ? (
                    <Link to={`/dz/users?search=ip:${user.client_ip}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
                      {user.client_ip}
                    </Link>
                  ) : '—'}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">DZ IP</dt>
                <dd className="text-sm font-mono">{user.dz_ip || '—'}</dd>
              </div>
              {user.tunnel_id > 0 && (
                <div className="flex justify-between">
                  <dt className="text-sm text-muted-foreground">Tunnel ID</dt>
                  <dd className="text-sm font-mono">{user.tunnel_id}</dd>
                </div>
              )}
            </dl>
          </div>

          {/* Location */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Location</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Device</dt>
                <dd className="text-sm">
                  {user.device_pk ? (
                    <Link to={`/dz/devices/${user.device_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
                      {user.device_code}
                    </Link>
                  ) : '—'}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Metro</dt>
                <dd className="text-sm">
                  {user.metro_pk ? (
                    <Link to={`/dz/metros/${user.metro_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
                      {user.metro_name || user.metro_code}
                    </Link>
                  ) : '—'}
                </dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Contributor</dt>
                <dd className="text-sm">
                  {user.contributor_pk ? (
                    <Link to={`/dz/contributors/${user.contributor_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline">
                      {user.contributor_code}
                    </Link>
                  ) : '—'}
                </dd>
              </div>
            </dl>
          </div>

          {/* Solana Info */}
          {user.node_pubkey && (
            <div className="border border-border rounded-lg p-4 bg-card">
              <h3 className="text-sm font-medium text-muted-foreground mb-3">Solana</h3>
              <dl className="space-y-2">
                <div className="flex justify-between">
                  <dt className="text-sm text-muted-foreground">Node Pubkey</dt>
                  <dd className="text-sm">
                        <Link to={`/solana/gossip-nodes/${user.node_pubkey}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
                          {user.node_pubkey.slice(0, 6)}...{user.node_pubkey.slice(-4)}
                        </Link>
                      </dd>
                </div>
                {user.is_validator && (
                  <>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Vote Account</dt>
                      <dd className="text-sm">
                        <Link to={`/solana/validators/${user.vote_pubkey}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
                          {user.vote_pubkey.slice(0, 6)}...{user.vote_pubkey.slice(-4)}
                        </Link>
                      </dd>
                    </div>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Stake</dt>
                      <dd className="text-sm">{formatStake(user.stake_sol)}</dd>
                    </div>
                    <div className="flex justify-between">
                      <dt className="text-sm text-muted-foreground">Stake Weight</dt>
                      <dd className="text-sm">{user.stake_weight_pct > 0 ? `${user.stake_weight_pct.toFixed(2)}%` : '—'}</dd>
                    </div>
                  </>
                )}
              </dl>
            </div>
          )}

          {/* Multicast Groups */}
          {multicastGroups && multicastGroups.length > 0 && (
            <div className="border border-border rounded-lg p-4 bg-card">
              <h3 className="text-sm font-medium text-muted-foreground mb-3">Multicast Groups</h3>
              <div className="space-y-2">
                {multicastGroups.map(g => (
                  <div key={g.group_pk} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      <Link to={`/dz/multicast-groups/${g.group_pk}`} className="text-blue-600 dark:text-blue-400 hover:underline font-mono">
                        {g.group_code}
                      </Link>
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                        g.mode === 'P' ? 'bg-purple-500/15 text-purple-500' :
                        g.mode === 'S' ? 'bg-blue-500/15 text-blue-500' :
                        'bg-amber-500/15 text-amber-500'
                      }`}>
                        {g.mode === 'P' ? 'Publisher' : g.mode === 'S' ? 'Subscriber' : 'Pub + Sub'}
                      </span>
                    </div>
                    <span className="text-xs text-muted-foreground font-mono">{g.multicast_ip}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Traffic Chart */}
          {pk && <UserTrafficChart userPk={pk} />}
        </div>
      </div>
    </div>
  )
}
