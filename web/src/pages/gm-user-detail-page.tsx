import { useMemo, useRef, useCallback } from 'react'
import { useParams, useSearchParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { ArrowLeft, Loader2, AlertCircle } from 'lucide-react'
import uPlot from 'uplot'
import { fetchGMUser, type GMTimePoint } from '@/lib/api'
import { StatCard } from '@/components/stat-card'
import { useUPlotChart } from '@/hooks/use-uplot-chart'
import { useTheme } from '@/hooks/use-theme'
import { useDocumentTitle } from '@/hooks/use-document-title'

const RANGE_OPTIONS = [
  { label: '1h', value: '1h' },
  { label: '3h', value: '3h' },
  { label: '6h', value: '6h' },
  { label: '12h', value: '12h' },
  { label: '24h', value: '24h' },
  { label: '3d', value: '3d' },
  { label: '7d', value: '7d' },
]

function truncatePubkey(pk: string, len = 8): string {
  if (pk.length <= len * 2) return pk
  return `${pk.slice(0, len)}...${pk.slice(-len)}`
}

function timePointsToUPlotData(points: GMTimePoint[]): uPlot.AlignedData {
  if (points.length === 0) return [[], [], []]
  const ts = points.map(p => new Date(p.time).getTime() / 1000)
  const dz = points.map(p => p.dz_value)
  const pi = points.map(p => p.pi_value)
  return [ts, dz, pi]
}

function DualLineChart({ data, height, yLabel, yFormat }: {
  data: uPlot.AlignedData
  height: number
  yLabel: string
  yFormat?: (v: number) => string
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const { resolvedTheme } = useTheme()
  const dzColor = resolvedTheme === 'dark' ? '#22d3ee' : '#0891b2'
  const piColor = resolvedTheme === 'dark' ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.3)'

  const series: uPlot.Series[] = useMemo(() => [
    {},
    { label: 'DoubleZero', stroke: dzColor, width: 2 },
    { label: 'Public Internet', stroke: piColor, width: 2, dash: [6, 4] },
  ], [dzColor, piColor])

  const axes: uPlot.Axis[] = useMemo(() => [
    {},
    {
      label: yLabel,
      values: (_u: uPlot, vals: number[]) => vals.map(v => yFormat ? yFormat(v) : String(v)),
    },
  ], [yLabel, yFormat])

  useUPlotChart({ containerRef, data, series, height, axes })

  return (
    <div>
      <div className="flex items-center gap-4 mb-2 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: dzColor }} />
          DoubleZero
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-0.5 rounded" style={{ background: piColor, borderTop: `2px dashed ${piColor}`, height: 0 }} />
          Public Internet
        </span>
      </div>
      <div ref={containerRef} className="w-full" />
    </div>
  )
}

export function GMUserDetailPage() {
  const { id } = useParams<{ id: string }>()
  const [searchParams, setSearchParams] = useSearchParams()

  const range = searchParams.get('range') || '24h'

  const setParam = useCallback((key: string, value: string) => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev)
      next.set(key, value)
      return next
    })
  }, [setSearchParams])

  const { data: detail, isLoading, error } = useQuery({
    queryKey: ['gm-user', id, range],
    queryFn: () => fetchGMUser(id!, range),
    enabled: !!id,
  })

  useDocumentTitle(detail ? truncatePubkey(detail.user_pubkey) : 'User')

  const rttData = useMemo(() => timePointsToUPlotData(detail?.rtt_ts ?? []), [detail])
  const availData = useMemo(() => timePointsToUPlotData(detail?.availability_ts ?? []), [detail])

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !detail) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center space-y-3">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto" />
          <p className="text-muted-foreground">Failed to load user</p>
          <Link to="/gm/users" className="text-sm text-blue-500 hover:underline">Back to users</Link>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-[1200px] mx-auto px-6 py-6">
        {/* Back link */}
        <Link to="/gm/users" className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground mb-6">
          <ArrowLeft className="h-4 w-4" />
          Back to users
        </Link>

        {/* Header */}
        <div className="flex flex-wrap items-start justify-between gap-4 mb-6">
          <div>
            <h1 className="text-2xl font-medium font-mono">{truncatePubkey(detail.user_pubkey, 12)}</h1>
            <div className="flex items-center gap-3 mt-1 text-sm text-muted-foreground">
              {detail.metro && <span>{detail.metro}</span>}
              {detail.city && <span>{detail.city}</span>}
              {detail.country && <span>{detail.country}</span>}
              {detail.asn_org && <span>{detail.asn_org}</span>}
              {detail.target_ip && <span className="font-mono">{detail.target_ip}</span>}
            </div>
            {detail.dzd_metro && (
              <div className="mt-1">
                <span className="px-2 py-0.5 rounded-full text-xs bg-cyan-500/10 text-cyan-500 border border-cyan-500/20">
                  DZ: {detail.dzd_metro}
                </span>
              </div>
            )}
          </div>

          {/* Controls */}
          <div className="flex items-center gap-2">
            <div className="flex items-center rounded-md border border-border overflow-hidden text-sm">
              {RANGE_OPTIONS.map(opt => (
                <button
                  key={opt.value}
                  onClick={() => setParam('range', opt.value)}
                  className={`px-2.5 py-1.5 transition-colors ${range === opt.value ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Stat cards */}
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4 mb-6">
          <StatCard label="DZ Availability" value={detail.dz_availability_pct} format="percent" decimals={1} />
          <StatCard label="PI Availability" value={detail.pi_availability_pct} format="percent" decimals={1} />
          <StatCard label="DZ RTT (ms)" value={detail.dz_rtt_ms} format="number" decimals={1} />
          <StatCard label="PI RTT (ms)" value={detail.pi_rtt_ms} format="number" decimals={1} />
          <StatCard label="RTT Delta (ms)" value={detail.rtt_delta_ms} format="number" decimals={1} />
          <StatCard label="Packet Loss" value={detail.packet_loss_pct} format="percent" decimals={2} />
        </div>

        {/* Charts */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
          <div className="border border-border rounded-lg p-4">
            <h3 className="text-sm font-medium mb-3">RTT (ms)</h3>
            <DualLineChart data={rttData} height={200} yLabel="ms" yFormat={v => `${v.toFixed(1)}`} />
          </div>
          <div className="border border-border rounded-lg p-4">
            <h3 className="text-sm font-medium mb-3">Availability (%)</h3>
            <DualLineChart data={availData} height={200} yLabel="%" yFormat={v => `${v.toFixed(0)}%`} />
          </div>
        </div>

        {/* Metro breakdown */}
        {detail.metro_breakdown.length > 0 && (
          <div className="border border-border rounded-lg overflow-hidden">
            <div className="px-4 py-3 border-b border-border bg-muted/50">
              <h3 className="text-sm font-medium">By Source Metro</h3>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left">
                  <th className="px-4 py-2.5 font-medium">Metro</th>
                  <th className="px-4 py-2.5 font-medium text-right">DZ RTT</th>
                  <th className="px-4 py-2.5 font-medium text-right">PI RTT</th>
                  <th className="px-4 py-2.5 font-medium text-right">DZ Avail</th>
                  <th className="px-4 py-2.5 font-medium text-right">PI Avail</th>
                  <th className="px-4 py-2.5 font-medium text-right">Probes</th>
                </tr>
              </thead>
              <tbody>
                {detail.metro_breakdown.map(m => (
                  <tr key={m.source_metro} className="border-b border-border last:border-0">
                    <td className="px-4 py-2.5">{m.source_metro}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{m.dz_rtt_ms ? `${m.dz_rtt_ms.toFixed(1)} ms` : '-'}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{m.pi_rtt_ms ? `${m.pi_rtt_ms.toFixed(1)} ms` : '-'}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{m.dz_avail_pct ? `${m.dz_avail_pct.toFixed(1)}%` : '-'}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{m.pi_avail_pct ? `${m.pi_avail_pct.toFixed(1)}%` : '-'}</td>
                    <td className="px-4 py-2.5 text-right tabular-nums">{m.probe_count.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
