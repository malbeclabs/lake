import { useEffect, useMemo, useState, useCallback } from 'react'
import { Trophy } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchHyperliquidScoreboard,
  type HyperliquidScoreboardResponse,
} from '@/lib/api'

const WINDOWS = ['1h', '24h', '7d'] as const

const DZ_COLOR = '#34d399' // emerald-400 — DoubleZero

function pct(n: number): string {
  return `${n.toFixed(1)}%`
}

// DoubleZero's lead over a competitor. Sub-second in ms, larger in seconds.
function lead(n: number): string {
  if (n >= 1000) return `${(n / 1000).toFixed(2)} s`
  return `${n.toFixed(n < 10 ? 1 : 0)} ms`
}

// Three-quarter-arc win-rate gauge — mirrors the edge scoreboard's WinRateGauge.
function WinGauge({ value }: { value: number }) {
  const size = 160
  const r = 65
  const c = size / 2
  const circ = 2 * Math.PI * r
  const arc = circ * 0.75
  const gap = circ - arc
  const fill = arc * (Math.min(100, Math.max(0, value)) / 100)
  return (
    <div className="relative flex items-center justify-center shrink-0" style={{ width: size, height: size }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="absolute inset-0">
        <circle
          cx={c} cy={c} r={r} fill="none" strokeWidth={4}
          stroke="currentColor" className="text-muted-foreground/25"
          strokeDasharray={`${arc} ${gap}`} strokeLinecap="round"
          transform={`rotate(-225, ${c}, ${c})`}
        />
        <circle
          cx={c} cy={c} r={r} fill="none" strokeWidth={4}
          stroke={DZ_COLOR}
          strokeDasharray={`${fill} ${circ - fill}`} strokeLinecap="round"
          transform={`rotate(-225, ${c}, ${c})`}
          style={{ transition: 'stroke-dasharray 0.5s ease-out' }}
        />
      </svg>
      <div className="z-10 flex flex-col items-center">
        <div className="text-2xl font-semibold tabular-nums">{value.toFixed(1)}%</div>
        <div className="mt-0.5 text-center text-xs text-muted-foreground">DoubleZero<br />Win Rate</div>
      </div>
    </div>
  )
}

function WinBar({ value }: { value: number }) {
  return (
    <div className="h-1 overflow-hidden rounded-full bg-muted-foreground/25">
      <div
        className="h-full rounded-full transition-all duration-500"
        style={{ width: `${Math.min(100, Math.max(0, value))}%`, backgroundColor: DZ_COLOR }}
      />
    </div>
  )
}

export function HyperliquidScoreboardPage() {
  const [timeWindow, setTimeWindow] = useState<(typeof WINDOWS)[number]>('1h')
  const [data, setData] = useState<HyperliquidScoreboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [now, setNow] = useState(() => Date.now())

  const load = useCallback(async () => {
    try {
      setData(await fetchHyperliquidScoreboard(timeWindow))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [timeWindow])

  useEffect(() => {
    let active = true
    const run = () => { void load() }
    run()
    const poll = setInterval(run, 15000)
    const tick = setInterval(() => active && setNow(Date.now()), 5000)
    return () => { active = false; clearInterval(poll); clearInterval(tick) }
  }, [load])

  const freshness = useMemo(() => {
    if (!data?.generated_at) return null
    const age = Math.round((now - new Date(data.generated_at).getTime()) / 1000)
    if (age < 5) return 'just now'
    if (age < 60) return `${age}s ago`
    return `${Math.round(age / 60)}m ago`
  }, [data?.generated_at, now])

  // Global competitor set drives the per-vantage table columns (stable order).
  const competitorCols = data?.competitors ?? []

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-8">
        <PageHeader
          icon={Trophy}
          title="Hyperliquid Scoreboard"
          subtitle={
            <span className="flex items-center gap-2 text-xs text-muted-foreground/50">
              <span>last {data?.window ?? timeWindow}</span>
              {freshness && <span>· updated {freshness}</span>}
            </span>
          }
          actions={
            <div className="inline-flex overflow-hidden rounded-md border border-border text-sm">
              {WINDOWS.map((w) => (
                <button
                  key={w}
                  type="button"
                  onClick={() => setTimeWindow(w)}
                  className={
                    w === timeWindow
                      ? 'bg-emerald-500/10 px-3 py-1.5 font-medium text-emerald-400'
                      : 'px-3 py-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground'
                  }
                >
                  {w}
                </button>
              ))}
            </div>
          }
        />

        {error && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-red-500">{error}</div>
        )}
        {!data && !error && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">Loading…</div>
        )}

        {data && (
          <>
            {/* Hero stats — 3 columns: description+stats | metrics | gauge */}
            <div className="mb-8 flex flex-col rounded-lg border border-border bg-card lg:flex-row">
              {/* Left: description + summary stats */}
              <div className="flex min-w-0 flex-1 flex-col justify-between p-4 sm:p-6">
                <p className="text-sm leading-relaxed text-muted-foreground">
                  Scoreboard benchmarks Hyperliquid best-bid/offer delivery speed across DoubleZero and
                  competing providers, comparing who delivers each order-book update first across {data.nodes.length} vantage points.
                </p>
                <div className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-3 border-t border-border pt-4">
                  <div>
                    <div className="mb-1 text-xs text-muted-foreground">Head-to-head races</div>
                    <div className="text-xl font-semibold tabular-nums sm:text-2xl">{data.total_races.toLocaleString()}</div>
                  </div>
                  <div className="sm:border-l sm:border-border sm:pl-6">
                    <div className="mb-1 text-xs text-muted-foreground">Vantage points</div>
                    <div className="text-xl font-semibold tabular-nums sm:text-2xl">{data.nodes.length}</div>
                  </div>
                </div>
              </div>

              {/* Middle: win rate + per-competitor leads */}
              <div className="flex min-w-0 flex-1 flex-col justify-center gap-4 border-t border-border p-4 sm:p-6 lg:border-l lg:border-t-0">
                <div className="pb-2">
                  <div className="mb-1.5 flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">DoubleZero Win Rate</span>
                    <span className="ml-4 shrink-0 text-sm font-medium tabular-nums">{pct(data.dz_win_share_pct)}</span>
                  </div>
                  <WinBar value={data.dz_win_share_pct} />
                </div>
                {data.competitors.map((c) => (
                  <div key={c.feed}>
                    <div className="flex items-center justify-between">
                      <span className="text-sm text-muted-foreground">DoubleZero vs {c.label}</span>
                      <span className="ml-4 shrink-0 text-sm font-medium tabular-nums">
                        <span className="mr-1 text-xs font-normal text-muted-foreground">p50:</span>
                        <span className="text-emerald-500">+{lead(c.lead_p50_ms)}</span>
                      </span>
                    </div>
                    <div className="mt-0.5 text-xs text-muted-foreground">p95: +{lead(c.lead_p95_ms)}</div>
                  </div>
                ))}
              </div>

              {/* Right: gauge */}
              <div className="flex shrink-0 items-center justify-center border-t border-border px-6 py-6 sm:px-8 lg:border-l lg:border-t-0 lg:py-0">
                <WinGauge value={data.dz_win_share_pct} />
              </div>
            </div>

            {/* Per-vantage table */}
            <div className="mb-6 overflow-hidden rounded-lg border border-border bg-card">
              <div className="overflow-x-auto">
                <table className="min-w-full">
                  <thead>
                    <tr className="border-b border-border text-left text-sm text-muted-foreground">
                      <th className="whitespace-nowrap px-3 py-3 font-medium sm:px-4">Vantage</th>
                      <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">DZ Win Rate %</th>
                      {competitorCols.map((c) => (
                        <th key={c.feed} className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">
                          vs {c.label}
                          <span className="block text-xs font-normal">p50 (p95)</span>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.nodes.length === 0 ? (
                      <tr>
                        <td colSpan={2 + competitorCols.length} className="px-4 py-12 text-center text-muted-foreground">
                          No data available for the selected time window.
                        </td>
                      </tr>
                    ) : (
                      data.nodes.map((n) => {
                        const byFeed = new Map(n.competitors.map((c) => [c.feed, c]))
                        return (
                          <tr key={n.measurement_node_id} className="border-b border-border transition-colors last:border-b-0 hover:bg-muted/50">
                            <td className="px-3 py-3 sm:px-4">
                              <div className="text-sm font-medium uppercase">{n.location_code}</div>
                              <div className="text-xs text-muted-foreground">{n.measurement_node_id}</div>
                              <div className="text-xs text-muted-foreground">{n.total_races.toLocaleString()} races</div>
                            </td>
                            <td className="px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                              <div className="mb-1.5">{pct(n.dz_win_share_pct)}</div>
                              <WinBar value={n.dz_win_share_pct} />
                            </td>
                            {competitorCols.map((col) => {
                              const c = byFeed.get(col.feed)
                              return (
                                <td key={col.feed} className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                                  {c ? (
                                    <>
                                      <span className="text-emerald-500">+{lead(c.lead_p50_ms)}</span>{' '}
                                      <span className="text-muted-foreground">(+{lead(c.lead_p95_ms)})</span>
                                    </>
                                  ) : (
                                    '—'
                                  )}
                                </td>
                              )
                            })}
                          </tr>
                        )
                      })
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Recent races */}
            <div className="mb-6 overflow-hidden rounded-lg border border-border bg-card">
              <div className="border-b border-border px-4 py-3 text-sm font-medium text-muted-foreground">Recent races</div>
              {data.recent_races.length === 0 ? (
                <div className="flex flex-col items-center gap-1 px-4 py-10 text-center">
                  <div className="text-sm text-muted-foreground">No races in the last couple of minutes.</div>
                  <div className="max-w-md text-xs text-muted-foreground/70">
                    The live feed populates from the continuously-updating production source.
                  </div>
                </div>
              ) : (
                <table className="min-w-full">
                  <tbody>
                    {data.recent_races.map((r, i) => (
                      <tr key={`${r.symbol}-${r.event_ts}-${i}`} className="border-b border-border text-sm last:border-b-0 hover:bg-muted/50">
                        <td className="px-4 py-2 font-medium">{r.symbol}</td>
                        <td className="px-4 py-2">
                          {r.is_dz ? (
                            <span className="text-emerald-500">DoubleZero</span>
                          ) : (
                            <span className="text-muted-foreground">{r.runner_up_label}</span>
                          )}
                          <span className="text-muted-foreground/60"> · {r.winner_feed}</span>
                        </td>
                        <td className="whitespace-nowrap px-4 py-2 text-right tabular-nums text-muted-foreground">
                          +{lead(r.lead_ms)} <span className="text-muted-foreground/60">vs {r.runner_up_label}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
