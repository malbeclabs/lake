import { useEffect, useMemo, useState, useCallback } from 'react'
import { Trophy } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchHyperliquidScoreboard,
  type HyperliquidScoreboardResponse,
  type HyperliquidRace,
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

// Recent-races grid: two sections of top symbols by volume. Only some symbols have competitor
// feeds (currently BTC/ETH); the rest render as DoubleZero-exclusive coverage.
const RECENT_SECTIONS = [
  { title: 'Hyperliquid Perpetual Futures', symbols: ['BTC', 'ETH', 'SOL', 'HYPE'] },
  { title: 'Hyperliquid HIP-3 DEX Perpetual Futures', symbols: ['xyz:SP500', 'xyz:XYZ100', 'xyz:MU', 'xyz:SKHX'] },
] as const

function symbolDisplay(sym: string): string {
  return sym.startsWith('xyz:') ? sym.slice(4) : sym
}

function fmtPrice(p: number): string {
  if (p >= 1000) return `$${Math.round(p).toLocaleString()}`
  return `$${p.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
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

// Competitor colors for the win-rate bar — warm hues contrasting with DoubleZero green.
const COMPETITOR_COLORS = ['#fbbf24', '#fb923c', '#ef4444', '#ec4899', '#a855f7'] // amber, orange, red, pink, purple
function competitorColor(i: number): string {
  return COMPETITOR_COLORS[i % COMPETITOR_COLORS.length]
}

// Win-rate bar: DoubleZero's share in green, the remaining loss split by competitor color.
function WinRateBar({ dzPct, segments }: { dzPct: number; segments: { label: string; pct: number; color: string }[] }) {
  return (
    <div className="flex h-1.5 overflow-hidden rounded-full bg-muted-foreground/25">
      <div
        className="h-full transition-all duration-500"
        style={{ width: `${Math.max(0, Math.min(100, dzPct))}%`, backgroundColor: DZ_COLOR }}
        title={`DoubleZero ${pct(dzPct)}`}
      />
      {segments.map((s) => (
        <div
          key={s.label}
          className="h-full transition-all duration-500"
          style={{ width: `${Math.max(0, s.pct)}%`, backgroundColor: s.color }}
          title={`${s.label} ${pct(s.pct)}`}
        />
      ))}
    </div>
  )
}

// relAge returns a short "just now" / "12s ago" / "3m ago" / "2h ago" string for a ms timestamp.
function relAge(tsMs: number, nowMs: number): string {
  const age = Math.round((nowMs - tsMs) / 1000)
  if (age < 5) return 'just now'
  if (age < 60) return `${age}s ago`
  if (age < 3600) return `${Math.round(age / 60)}m ago`
  return `${Math.round(age / 3600)}h ago`
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

  // Win-rate bar segments: each competitor's share of all comparisons where it beat DoubleZero
  // (so the green DZ share + the colored competitor segments fill the bar to ~100%).
  const lossTotal = data?.total_races ?? 0
  const lossSegments = (data?.competitors ?? []).map((c, i) => ({
    label: c.label,
    pct: lossTotal > 0 ? (c.races * (1 - c.dz_win_pct / 100) / lossTotal) * 100 : 0,
    color: competitorColor(i),
  }))

  // Recent races grouped by symbol for the per-symbol grid.
  const racesBySymbol = useMemo(() => {
    const m: Record<string, HyperliquidRace[]> = {}
    for (const r of data?.recent_races ?? []) (m[r.symbol] ??= []).push(r)
    return m
  }, [data?.recent_races])

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
                  {data.composite_latency && (
                    <div className="w-full">
                      <div className="mb-1.5 text-xs text-muted-foreground">
                        Composite feed latency{' '}
                        <span className="text-muted-foreground/60">({data.composite_latency.window})</span>
                      </div>
                      <div className="flex gap-5">
                        {([
                          ['p50', data.composite_latency.p50_ms],
                          ['p90', data.composite_latency.p90_ms],
                          ['p99', data.composite_latency.p99_ms],
                        ] as const).map(([label, v]) => (
                          <div key={label}>
                            <div className="text-xl font-semibold tabular-nums sm:text-2xl">
                              {Math.round(v)}
                              <span className="ml-1 text-xs font-normal text-muted-foreground">ms</span>
                            </div>
                            <div className="text-xs text-muted-foreground">{label}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Middle: win rate + per-competitor leads */}
              <div className="flex min-w-0 flex-1 flex-col justify-center gap-4 border-t border-border p-4 sm:p-6 lg:border-l lg:border-t-0">
                <div className="pb-2">
                  <div className="mb-1.5 flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">DoubleZero Win Rate</span>
                    <span className="ml-4 shrink-0 text-sm font-medium tabular-nums">{pct(data.dz_win_share_pct)}</span>
                  </div>
                  <WinRateBar dzPct={data.dz_win_share_pct} segments={lossSegments} />
                </div>
                {data.competitors.map((c, i) => (
                  <div key={c.feed}>
                    <div className="flex items-center justify-between">
                      <span className="flex items-center gap-1.5 text-sm text-muted-foreground">
                        <span className="inline-block h-2 w-2 shrink-0 rounded-full" style={{ backgroundColor: competitorColor(i) }} />
                        DoubleZero vs {c.label}
                      </span>
                      <span className="ml-4 shrink-0 text-sm font-medium tabular-nums">
                        <span className="mr-1 text-xs font-normal text-muted-foreground">p50:</span>
                        +{lead(c.lead_p50_ms)}
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
                                      <span>+{lead(c.lead_p50_ms)}</span>{' '}
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

            {/* Recent races — per-symbol grid, split into native perps and HIP-3 DEX perps */}
            {RECENT_SECTIONS.map((section) => (
              <div key={section.title} className="mb-6 overflow-hidden rounded-lg border border-border bg-card">
                <div className="border-b border-border px-4 py-3 text-sm font-medium text-muted-foreground">
                  {section.title}
                </div>
                <div className="grid grid-cols-2 gap-px bg-border sm:grid-cols-4">
                  {section.symbols.map((sym) => {
                    const races = racesBySymbol[sym] ?? []
                    const price = data.prices?.[sym]
                    const lastTs = races.length
                      ? Math.max(...races.map((r) => new Date(r.event_ts).getTime()))
                      : null
                    return (
                      <div key={sym} className="bg-card p-3">
                        <div className="mb-2 flex items-baseline justify-between gap-1">
                          <span className="text-sm font-semibold">{symbolDisplay(sym)}</span>
                          {price != null && (
                            <span className="text-xs tabular-nums text-muted-foreground">{fmtPrice(price)}</span>
                          )}
                        </div>
                        {lastTs != null && (
                          <div className="mb-1.5 text-[11px] tabular-nums text-muted-foreground/50">
                            updated {relAge(lastTs, now)}
                          </div>
                        )}
                        {races.length === 0 ? (
                          <div className="flex flex-col gap-1">
                            <span className="inline-flex w-fit items-center rounded bg-muted px-1.5 py-0.5 text-[11px] font-medium text-muted-foreground">
                              Competitor feeds pending
                            </span>
                            <span className="text-[11px] text-muted-foreground/60">No head-to-head races yet</span>
                          </div>
                        ) : (
                          <div className="space-y-1">
                            {races.map((r, i) => {
                              const won = r.is_dz
                              const comp = won ? r.runner_up_label : r.winner_label
                              return (
                                <div key={i} className="flex items-baseline justify-between gap-1.5 text-xs">
                                  <span className="flex min-w-0 items-baseline gap-1 truncate">
                                    <span className={`shrink-0 font-semibold ${won ? 'text-emerald-500' : 'text-rose-400'}`}>
                                      {won ? '▲ DoubleZero' : `▼ ${comp}`}
                                    </span>
                                    <span className="truncate text-muted-foreground/50">vs {won ? comp : 'DZ'}</span>
                                  </span>
                                  <span className={`shrink-0 tabular-nums ${won ? 'text-emerald-500' : 'text-rose-400'}`}>
                                    +{lead(r.lead_ms)}
                                  </span>
                                </div>
                              )
                            })}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            ))}
          </>
        )}
      </div>
    </div>
  )
}
