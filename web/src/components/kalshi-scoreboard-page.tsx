import { useEffect, useMemo, useState, useCallback } from 'react'
import { Trophy } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchKalshiScoreboard,
  type KalshiScoreboardResponse,
  type KalshiRace,
} from '@/lib/api'

const WINDOWS = ['1h', '24h', '7d'] as const

const DZ_COLOR = '#34d399' // emerald-400 — DoubleZero

function pct(n: number): string {
  return `${n.toFixed(1)}%`
}

// DoubleZero's lead over a competing feed. Sub-second in ms, larger in seconds.
function lead(n: number): string {
  if (n >= 1000) return `${(n / 1000).toFixed(2)} s`
  return `${n.toFixed(n < 10 ? 1 : 0)} ms`
}

// Recent-races grid: the perps tickers the edge feed carries. Kept in sync with
// kalshiRecentRaceSymbols in api/handlers/kalshi_scoreboard.go — a symbol listed here but not
// there simply renders empty.
const RECENT_SECTIONS = [
  {
    title: 'Kalshi Perpetual Futures',
    symbols: ['KXBTCPERP', 'KXETHPERP', 'KXSOLPERP', 'KXHYPEPERP'],
  },
  {
    title: 'Kalshi Perpetual Futures (cont.)',
    symbols: ['KXXRPPERP', 'KXDOGEPERP', 'KXLTCPERP', 'KXLINKPERP'],
  },
] as const

// KXBTCPERP -> BTC. Anything not matching the venue's ticker convention is shown as-is.
function symbolDisplay(sym: string): string {
  const m = /^KX(.+)PERP\d*$/.exec(sym)
  return m ? m[1] : sym
}

// Vantage-point facility metadata (keyed by location_code), plus the row display order. cmh is
// the near-venue baseline recorder; was and dub were added as the capture fleet grew. An
// unlisted metro still renders — it falls back to its raw code and sorts last.
const VANTAGE_INFO: Record<string, { facility: string; city: string; order: number }> = {
  cmh: { facility: 'AWS us-east-2', city: 'Columbus, OH', order: 0 },
  was: { facility: 'AWS us-east-1', city: 'Ashburn, VA', order: 1 },
  dub: { facility: 'AWS eu-west-1', city: 'Dublin, IE', order: 2 },
}
function vantageOrder(locationCode: string): number {
  return VANTAGE_INFO[locationCode]?.order ?? 99
}

function fmtPrice(p: number): string {
  if (p >= 1000) return `$${Math.round(p).toLocaleString()}`
  return `$${p.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

// Three-quarter-arc win-rate gauge — mirrors the Hyperliquid scoreboard's WinGauge.
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

// Competing-feed colors for the win-rate bar — warm hues contrasting with DoubleZero green.
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

export function KalshiScoreboardPage() {
  const [timeWindow, setTimeWindow] = useState<(typeof WINDOWS)[number]>('24h')
  const [data, setData] = useState<KalshiScoreboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [now, setNow] = useState(() => Date.now())

  const load = useCallback(async () => {
    try {
      setData(await fetchKalshiScoreboard(timeWindow))
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

  // Win-rate bar segments: each competing feed's share of all comparisons where it beat
  // DoubleZero (so the green DZ share + the colored segments fill the bar to ~100%).
  const lossTotal = data?.total_races ?? 0
  const lossSegments = (data?.competitors ?? []).map((c, i) => ({
    label: c.label,
    pct: lossTotal > 0 ? (c.races * (1 - c.dz_win_pct / 100) / lossTotal) * 100 : 0,
    color: competitorColor(i),
  }))

  // Recent races grouped by symbol for the per-symbol grid.
  const racesBySymbol = useMemo(() => {
    const m: Record<string, KalshiRace[]> = {}
    for (const r of data?.recent_races ?? []) (m[r.symbol] ??= []).push(r)
    return m
  }, [data?.recent_races])

  // No configured feeds is the expected state before an operator seeds the allow-list, and the
  // expected local-dev state. Say so rather than rendering a scoreboard of zeroes.
  const unconfigured = data != null && data.competitors.length === 0 && data.total_races === 0

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-8">
        <PageHeader
          icon={Trophy}
          title="Kalshi Scoreboard"
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
        {unconfigured && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
            No comparison feeds are configured for this environment yet, so there is nothing to race.
          </div>
        )}

        {data && !unconfigured && (
          <>
            {/* Hero — path latency per feed. This is the headline because it never pairs the
                two feeds: each is measured against the venue's own timestamp for the same
                update, so the number does not depend on the race pairing at all. */}
            <div className="mb-6 rounded-lg border border-border bg-card p-4 sm:p-6">
              <p className="text-sm leading-relaxed text-muted-foreground">
                Path latency from the venue's own timestamp to arrival, per feed at each vantage
                point — how long each path takes to deliver the same order-book update. Compare
                rows within a vantage: the two paths end in the same place, so only there is the
                difference a property of the path rather than of the location. Window:{' '}
                {data.path_latency?.window ?? '24h'}.
              </p>

              {data.path_latency && data.path_latency.feeds.length > 0 ? (
                <div className="mt-5 overflow-x-auto">
                  <table className="min-w-full">
                    <thead>
                      <tr className="border-b border-border text-left text-xs text-muted-foreground">
                        <th className="whitespace-nowrap py-2 pr-4 font-medium">Vantage</th>
                        <th className="whitespace-nowrap px-4 py-2 font-medium">Feed</th>
                        <th className="whitespace-nowrap px-4 py-2 text-right font-medium">p50</th>
                        <th className="whitespace-nowrap px-4 py-2 text-right font-medium">p90</th>
                        <th className="whitespace-nowrap px-4 py-2 text-right font-medium">p99</th>
                        <th className="whitespace-nowrap py-2 pl-4 text-right font-medium">Samples</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.path_latency.feeds.map((f) => (
                        <tr key={`${f.location_code}:${f.feed}`} className="border-b border-border last:border-b-0">
                          <td className="whitespace-nowrap py-3 pr-4">
                            <div className="text-sm font-medium uppercase">{f.location_code}</div>
                            <div className="text-xs text-muted-foreground">
                              {VANTAGE_INFO[f.location_code]?.city ?? ''}
                            </div>
                          </td>
                          <td className="whitespace-nowrap px-4 py-3">
                            <span className="flex items-center gap-1.5 text-sm font-medium">
                              <span
                                className="inline-block h-2 w-2 shrink-0 rounded-full"
                                style={{ backgroundColor: f.is_dz ? DZ_COLOR : COMPETITOR_COLORS[0] }}
                              />
                              {f.label}
                            </span>
                            <span className="text-xs text-muted-foreground">{f.feed}</span>
                          </td>
                          {([f.p50_ms, f.p90_ms, f.p99_ms] as const).map((v, i) => (
                            <td
                              key={i}
                              className={`whitespace-nowrap px-4 py-3 text-right tabular-nums ${
                                i === 0 ? 'text-xl font-semibold sm:text-2xl' : 'text-sm text-muted-foreground'
                              }`}
                            >
                              {lead(v)}
                            </td>
                          ))}
                          <td className="whitespace-nowrap py-3 pl-4 text-right text-sm tabular-nums text-muted-foreground">
                            {f.samples.toLocaleString()}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="mt-5 text-sm text-muted-foreground">
                  Path latency is computed on a background refresh and is not available yet.
                </div>
              )}
            </div>

            {/* Race win rate — kept, but below the headline and with its caveat attached. */}
            <div className="mb-6 flex flex-col rounded-lg border border-border bg-card lg:flex-row">
              <div className="flex min-w-0 flex-1 flex-col justify-between p-4 sm:p-6">
                <div>
                  <div className="text-sm font-medium">Delivery race</div>
                  <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                    Which feed delivered each update first, and by how much.
                  </p>
                  <p className="mt-3 rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-xs leading-relaxed text-muted-foreground">
                    <span className="font-medium text-amber-500">Read this as a distribution, not as a lead.</span>{' '}
                    The venue's public feed delivers on a batched cadence of roughly a second, so the
                    gap between the two arrival times largely measures that cadence rather than a path
                    advantage. A margin that clusters tightly is a fixed offset, not a race. The path
                    latency above is the comparison that does not depend on the pairing.
                  </p>
                </div>
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

              {/* Middle: win rate + per-competitor margins */}
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
                      [...data.nodes]
                        .sort((a, b) => vantageOrder(a.location_code) - vantageOrder(b.location_code))
                        .map((n) => {
                          const byFeed = new Map(n.competitors.map((c) => [c.feed, c]))
                          const info = VANTAGE_INFO[n.location_code]
                          return (
                            <tr key={n.measurement_node_id} className="border-b border-border transition-colors last:border-b-0 hover:bg-muted/50">
                              <td className="px-3 py-3 sm:px-4">
                                <div
                                  className="cursor-default"
                                  title={[
                                    info?.facility ?? n.location_code.toUpperCase(),
                                    info?.city,
                                    n.measurement_node_id,
                                    `${n.total_races.toLocaleString()} races`,
                                  ]
                                    .filter(Boolean)
                                    .join('\n')}
                                >
                                  <div className="text-sm font-medium uppercase">{n.location_code}</div>
                                  <div className="text-xs text-muted-foreground">{info?.facility ?? n.measurement_node_id}</div>
                                  <div className="text-xs text-muted-foreground/70">{n.total_races.toLocaleString()} races</div>
                                </div>
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

            {/* Recent races — per-symbol grid */}
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
                              No recent races
                            </span>
                            <span className="text-[11px] text-muted-foreground/60">Both feeds must carry this market</span>
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
