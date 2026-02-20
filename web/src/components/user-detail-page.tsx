import { useState, useMemo } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Loader2, Users, AlertCircle, ArrowLeft, Check } from 'lucide-react'
import { AreaChart, Area, XAxis, YAxis, ResponsiveContainer, Tooltip as RechartsTooltip, CartesianGrid, ReferenceLine } from 'recharts'
import { fetchUser, fetchUserTraffic } from '@/lib/api'
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

function UserTrafficChart({ userPk }: { userPk: string }) {
  const [timeRange, setTimeRange] = useState<string>('1h')

  const { data: trafficData, isLoading } = useQuery({
    queryKey: ['user-traffic', userPk, timeRange],
    queryFn: () => fetchUserTraffic(userPk, timeRange),
    refetchInterval: 30000,
  })

  // Transform data: inbound (positive), outbound (negative), per tunnel
  // Device in_bps = user sending (outbound), device out_bps = user receiving (inbound)
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
      row[`t${p.tunnel_id}_in`] = p.out_bps   // device out = user inbound (positive)
      row[`t${p.tunnel_id}_out`] = -p.in_bps   // device in = user outbound (negative)
    }

    const ids = [...tunnelSet].sort((a, b) => a - b)
    const data = [...timeMap.values()].sort((a, b) =>
      String(a.time).localeCompare(String(b.time))
    )
    return { chartData: data, tunnelIds: ids }
  }, [trafficData])

  return (
    <div className="border border-border rounded-lg p-4 bg-card col-span-full">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">Traffic History</h3>
        <div className="flex gap-1">
          {TIME_RANGES.map(r => (
            <button
              key={r}
              onClick={() => setTimeRange(r)}
              className={`px-2 py-0.5 text-xs rounded ${
                timeRange === r
                  ? 'bg-blue-500/20 text-blue-500 font-medium'
                  : 'text-muted-foreground hover:text-foreground hover:bg-[var(--muted)]'
              }`}
            >
              {r}
            </button>
          ))}
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
        <div className="relative">
          <span className="absolute top-0 left-[48px] text-[10px] text-muted-foreground/60 pointer-events-none z-10">▲ Inbound</span>
          <span className="absolute bottom-5 left-[48px] text-[10px] text-muted-foreground/60 pointer-events-none z-10">▼ Outbound</span>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
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
                  tickFormatter={(v) => formatAxisBps(Math.abs(v))}
                  width={45}
                />
                <ReferenceLine y={0} stroke="var(--border)" strokeWidth={1} />
                <RechartsTooltip
                  contentStyle={{
                    backgroundColor: 'var(--card)',
                    border: '1px solid var(--border)',
                    borderRadius: '6px',
                    fontSize: '11px',
                  }}
                  labelFormatter={formatTime}
                  formatter={(value, name) => {
                    const v = value as number
                    const dir = v >= 0 ? 'Inbound' : 'Outbound'
                    const tunnelNum = String(name).match(/\d+/)?.[0]
                    return [formatBps(Math.abs(v)), `Tunnel ${tunnelNum} ${dir}`]
                  }}
                />
                {tunnelIds.map((tid, i) => (
                  <Area
                    key={`${tid}_in`}
                    type="monotone"
                    dataKey={`t${tid}_in`}
                    stroke={TUNNEL_COLORS[i % TUNNEL_COLORS.length]}
                    fill={TUNNEL_COLORS[i % TUNNEL_COLORS.length]}
                    fillOpacity={0.2}
                    strokeWidth={1.5}
                    dot={false}
                    name={`t${tid}_in`}
                  />
                ))}
                {tunnelIds.map((tid, i) => (
                  <Area
                    key={`${tid}_out`}
                    type="monotone"
                    dataKey={`t${tid}_out`}
                    stroke={TUNNEL_COLORS[i % TUNNEL_COLORS.length]}
                    fill={TUNNEL_COLORS[i % TUNNEL_COLORS.length]}
                    fillOpacity={0.1}
                    strokeWidth={1.5}
                    strokeDasharray="4 2"
                    dot={false}
                    name={`t${tid}_out`}
                  />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </div>
          {/* Legend */}
          {tunnelIds.length > 0 && (
            <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2">
              {tunnelIds.map((tid, i) => (
                <div key={tid} className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <div
                    className="w-3 h-3 rounded-sm"
                    style={{ backgroundColor: TUNNEL_COLORS[i % TUNNEL_COLORS.length] }}
                  />
                  <span>Tunnel {tid}</span>
                </div>
              ))}
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground ml-2">
                <div className="w-4 h-0.5 bg-current rounded" />
                <span>Inbound</span>
              </div>
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <div className="w-4 h-0.5 bg-current rounded" style={{ borderTop: '2px dashed currentColor', height: 0 }} />
                <span>Outbound</span>
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
          <span className={`ml-4 capitalize ${statusColors[user.status] || ''}`}>
            {user.status}
          </span>
        </div>

        {/* Info grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* Identity */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Identity</h3>
            <dl className="space-y-2">
              <div>
                <dt className="text-sm text-muted-foreground">Owner Pubkey</dt>
                <dd className="text-sm font-mono break-all">{user.owner_pubkey}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Kind</dt>
                <dd className="text-sm">{user.kind || '—'}</dd>
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

          {/* Traffic */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Traffic</h3>
            <dl className="space-y-2">
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Inbound</dt>
                <dd className="text-sm">{formatBps(user.in_bps)}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-sm text-muted-foreground">Outbound</dt>
                <dd className="text-sm">{formatBps(user.out_bps)}</dd>
              </div>
            </dl>
          </div>

          {/* Validator Info */}
          <div className="border border-border rounded-lg p-4 bg-card">
            <h3 className="text-sm font-medium text-muted-foreground mb-3">Validator</h3>
            <dl className="space-y-2">
              <div className="flex justify-between items-center">
                <dt className="text-sm text-muted-foreground">Is Validator</dt>
                <dd className="text-sm">
                  {user.is_validator ? (
                    <Check className="h-4 w-4 text-green-600 dark:text-green-400" />
                  ) : '—'}
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
                </>
              )}
            </dl>
          </div>

          {/* Traffic Chart */}
          {pk && <UserTrafficChart userPk={pk} />}
        </div>
      </div>
    </div>
  )
}
