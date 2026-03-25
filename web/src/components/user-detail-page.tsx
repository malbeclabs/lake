import { useState, useMemo, useRef, useEffect } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, Users, AlertCircle, ArrowLeft, RefreshCw } from 'lucide-react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { fetchUser, fetchUserTraffic, fetchUserMulticastGroups } from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'
import { useTheme } from '@/hooks/use-theme'

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

const TIME_RANGES = ['1h', '3h', '6h', '12h', '24h', '3d', '7d', '14d', '30d'] as const

const BUCKET_OPTIONS = ['auto', '10 SECOND', '30 SECOND', '1 MINUTE', '5 MINUTE', '10 MINUTE', '15 MINUTE', '30 MINUTE', '1 HOUR', '4 HOUR', '12 HOUR', '1 DAY'] as const
const BUCKET_LABELS: Record<string, string> = {
  'auto': 'Auto',
  '10 SECOND': '10s', '30 SECOND': '30s', '1 MINUTE': '1m', '5 MINUTE': '5m',
  '10 MINUTE': '10m', '15 MINUTE': '15m', '30 MINUTE': '30m', '1 HOUR': '1h',
  '4 HOUR': '4h', '12 HOUR': '12h', '1 DAY': '1d',
}

type AggMethod = 'max' | 'avg' | 'min' | 'p50' | 'p90' | 'p95' | 'p99'
const AGG_OPTIONS: AggMethod[] = ['max', 'p99', 'p95', 'p90', 'p50', 'avg', 'min']
const AGG_LABELS: Record<AggMethod, string> = {
  max: 'Max', p99: 'P99', p95: 'P95', p90: 'P90', p50: 'P50', avg: 'Avg', min: 'Min',
}

function resolveAutoBucket(timeRange: string): string {
  switch (timeRange) {
    case '1h': return '10 SECOND'
    case '3h': return '30 SECOND'
    case '6h': return '1 MINUTE'
    case '12h': return '10 MINUTE'
    case '24h': return '15 MINUTE'
    case '3d': return '30 MINUTE'
    case '7d': return '4 HOUR'
    case '14d': return '12 HOUR'
    case '30d': return '1 DAY'
    default: return '5 MINUTE'
  }
}

function UserTrafficChart({ userPk }: { userPk: string }) {
  const queryClient = useQueryClient()
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  const chartRef = useRef<HTMLDivElement>(null)
  const plotRef = useRef<uPlot | null>(null)
  const [timeRange, setTimeRange] = useState<string>('24h')
  const [metric, setMetric] = useState<TrafficMetric>('throughput')
  const [bucket, setBucket] = useState<string>('auto')
  const [agg, setAgg] = useState<AggMethod>('max')
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)

  const effectiveBucket = bucket === 'auto' ? resolveAutoBucket(timeRange) : bucket
  const autoBucketLabel = BUCKET_LABELS[resolveAutoBucket(timeRange)] || '5m'

  const { data: trafficData, isFetching } = useQuery({
    queryKey: ['user-traffic', userPk, timeRange, effectiveBucket, agg],
    queryFn: () => fetchUserTraffic(userPk, timeRange, effectiveBucket, agg),
    refetchInterval: 30000,
  })

  const fmtValue = metric === 'throughput' ? formatBps : formatPps
  const fmtValueRef = useRef(fmtValue)
  fmtValueRef.current = fmtValue

  // Transform data for uPlot: timestamps + rx/tx per tunnel
  const { uplotData, tunnelIds, uplotSeries } = useMemo(() => {
    if (!trafficData || trafficData.length === 0) {
      return { uplotData: null, tunnelIds: [] as number[], uplotSeries: [] as uPlot.Series[] }
    }

    const tunnelSet = new Set<number>()
    const timeMap = new Map<number, Map<number, { inVal: number; outVal: number }>>()

    for (const p of trafficData) {
      tunnelSet.add(p.tunnel_id)
      const ts = new Date(p.time + 'Z').getTime() / 1000
      if (!timeMap.has(ts)) timeMap.set(ts, new Map())
      const entry = timeMap.get(ts)!
      entry.set(p.tunnel_id, {
        inVal: metric === 'throughput' ? p.in_bps : p.in_pps,
        outVal: metric === 'throughput' ? p.out_bps : p.out_pps,
      })
    }

    const ids = [...tunnelSet].sort((a, b) => a - b)
    const timestamps = [...timeMap.keys()].sort((a, b) => a - b)

    const splinePaths = uPlot.paths.spline?.()
    const dataArrays: (number | null)[][] = [timestamps]
    const seriesConfigs: uPlot.Series[] = [{}]

    for (let i = 0; i < ids.length; i++) {
      const tid = ids[i]
      const color = TUNNEL_COLORS[i % TUNNEL_COLORS.length]
      const rxVals: (number | null)[] = []
      const txVals: (number | null)[] = []

      for (const ts of timestamps) {
        const entry = timeMap.get(ts)?.get(tid)
        rxVals.push(entry ? entry.inVal : null)
        txVals.push(entry ? -entry.outVal : null)
      }

      dataArrays.push(rxVals)
      seriesConfigs.push({
        label: `Tunnel ${tid} Rx`,
        stroke: color,
        width: 1.5,
        points: { show: false },
        paths: splinePaths,
      })
      dataArrays.push(txVals)
      seriesConfigs.push({
        label: `Tunnel ${tid} Tx`,
        stroke: color,
        width: 1.5,
        dash: [4, 2],
        points: { show: false },
        paths: splinePaths,
      })
    }

    return {
      uplotData: dataArrays as uPlot.AlignedData,
      tunnelIds: ids,
      uplotSeries: seriesConfigs,
    }
  }, [trafficData, metric])

  // Create/update chart
  useEffect(() => {
    if (!chartRef.current || !uplotData) {
      plotRef.current?.destroy()
      plotRef.current = null
      return
    }

    plotRef.current?.destroy()

    const axisStroke = isDark ? 'rgba(255,255,255,0.65)' : 'rgba(0,0,0,0.65)'

    const opts: uPlot.Options = {
      width: chartRef.current.offsetWidth,
      height: 224,
      series: uplotSeries,
      scales: {
        x: { time: true },
        y: { auto: true },
      },
      axes: [
        { stroke: axisStroke, grid: { stroke: 'rgba(128,128,128,0.06)' } },
        {
          stroke: axisStroke,
          grid: { stroke: 'rgba(128,128,128,0.06)' },
          values: (_: uPlot, vals: number[]) => vals.map(v => formatAxisBps(Math.abs(v))),
          size: 60,
        },
      ],
      cursor: {
        points: { size: 12, width: 2 },
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
        plotRef.current?.setSize({ width: entry.contentRect.width, height: 224 })
      }
    })
    ro.observe(chartRef.current)

    return () => {
      ro.disconnect()
      plotRef.current?.destroy()
      plotRef.current = null
    }
  }, [uplotData, uplotSeries, isDark, timeRange])

  // Values to display in legend: hovered or latest
  const displayValues = useMemo(() => {
    if (!uplotData || tunnelIds.length === 0) return new Map<number, { inVal: number; outVal: number }>()
    const timestamps = uplotData[0] as number[]
    const idx = hoveredIdx != null && hoveredIdx < timestamps.length ? hoveredIdx : timestamps.length - 1
    const map = new Map<number, { inVal: number; outVal: number }>()
    for (let i = 0; i < tunnelIds.length; i++) {
      const rxArr = uplotData[1 + i * 2] as (number | null)[]
      const txArr = uplotData[2 + i * 2] as (number | null)[]
      map.set(tunnelIds[i], {
        inVal: (rxArr?.[idx] as number) ?? 0,
        outVal: Math.abs((txArr?.[idx] as number) ?? 0),
      })
    }
    return map
  }, [uplotData, tunnelIds, hoveredIdx])

  // Format hovered timestamp
  const hoveredTime = useMemo(() => {
    if (!uplotData) return undefined
    const timestamps = uplotData[0] as number[]
    if (timestamps.length === 0) return undefined
    const idx = hoveredIdx != null && hoveredIdx < timestamps.length ? hoveredIdx : timestamps.length - 1
    const d = new Date(timestamps[idx] * 1000)
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false })
  }, [uplotData, hoveredIdx])

  return (
    <div className="border border-border rounded-lg p-4 bg-card col-span-full group/chart">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-muted-foreground">Traffic History</h3>
          {isFetching ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
          ) : (
            <button
              onClick={() => queryClient.invalidateQueries({ queryKey: ['user-traffic', userPk] })}
              className="opacity-0 group-hover/chart:opacity-100 transition-opacity text-muted-foreground hover:text-foreground"
              title="Refresh"
            >
              <RefreshCw className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
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
            value={agg}
            onChange={e => setAgg(e.target.value as AggMethod)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {AGG_OPTIONS.map(a => (
              <option key={a} value={a}>{AGG_LABELS[a]}</option>
            ))}
          </select>
          <select
            value={bucket}
            onChange={e => setBucket(e.target.value)}
            className="text-xs bg-transparent border border-border rounded px-1.5 py-1 text-foreground cursor-pointer"
          >
            {BUCKET_OPTIONS.map(b => (
              <option key={b} value={b}>{b === 'auto' ? `Auto (${autoBucketLabel})` : BUCKET_LABELS[b] || b}</option>
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

      {/* Shimmer bar */}
      <div className="h-0.5 w-full overflow-hidden rounded-full mb-2">
        {isFetching && (
          <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
        )}
      </div>

      {!uplotData && !isFetching && (
        <div className="flex items-center justify-center h-56 text-sm text-muted-foreground">
          No traffic data available
        </div>
      )}

      {(uplotData || isFetching) && (
        <div>
          <div className="relative" style={{ minHeight: 224 }}>
            <div ref={chartRef} className="w-full" />
            <span className="absolute top-1 right-3 text-[10px] text-muted-foreground/50 pointer-events-none">▲ Rx (in)</span>
            <span className="absolute bottom-8 right-3 text-[10px] text-muted-foreground/50 pointer-events-none">▼ Tx (out)</span>
          </div>
          {/* Legend table */}
          {tunnelIds.length > 0 && (
            <div className="mt-2 text-xs">
              <div className="grid gap-x-4 gap-y-0.5" style={{ gridTemplateColumns: 'auto 1fr 1fr' }}>
                <div className="text-muted-foreground font-medium">Tunnel</div>
                <div className="text-muted-foreground font-medium text-right">Rx (in)</div>
                <div className="text-right">
                  {hoveredTime && <span className="text-[10px] text-muted-foreground">{hoveredTime}</span>}
                  {!hoveredTime && <span className="text-muted-foreground font-medium">Tx (out)</span>}
                </div>
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
