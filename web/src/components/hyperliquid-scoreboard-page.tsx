import { Fragment, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties } from 'react'
import { Trophy } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchHyperliquidScoreboard,
  HyperliquidScoreboardPendingError,
  type HyperliquidScoreboardResponse,
} from '@/lib/api'

// The public Hyperliquid scoreboard. Margins are signed — positive means DoubleZero
// delivered the book first — so a cell below 50% win rate reports a negative median.
// See hyperliquid_scoreboard.go for why this does not agree with the internal board.

const DZ_COLOR = '#34d399'
const FIELD_COLOR = '#d99a3c'

const METRICS = [
  { key: 'win_pct', label: 'Win rate', hint: 'share delivered first' },
  { key: 'p50_ms', label: 'Median', hint: 'typical margin' },
  { key: 'p95_ms', label: '95th pct', hint: 'best 5% of races' },
  { key: 'p99_ms', label: '99th pct', hint: 'best 1% of races' },
] as const

type MetricKey = (typeof METRICS)[number]['key']

// The worker recomputes once a day, just after 00:00 UTC, so age alone says nothing — a
// payload is only behind once a midnight has passed without it being rewritten. The grace
// period is the refresh's own window: without it the page goes amber every night between
// midnight and whenever the cycle picks the entry up.
const REFRESH_GRACE_MS = 30 * 60 * 1000
function isBehind(asOf: number, now: number): boolean {
  const d = new Date(now)
  const midnight = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())
  return asOf < midnight && now - midnight > REFRESH_GRACE_MS
}

function pct(v: number): string {
  return `${v.toFixed(1)}%`
}

// Never a bare "+" when negative: a competitor-first cell has to read as a deficit.
function ms(v: number): string {
  const sign = v < 0 ? '−' : '+'
  const a = Math.abs(v)
  return a >= 1000 ? `${sign}${(a / 1000).toFixed(1)} s` : `${sign}${Math.round(a)} ms`
}

function showMetric(metric: MetricKey, v: number): string {
  return metric === 'win_pct' ? pct(v) : ms(v)
}

function fmtCount(n: number): string {
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e8 ? 0 : 1)}M`
  if (n >= 1e3) return `${Math.round(n / 1e3)}K`
  return String(Math.round(n))
}

function WinGauge({ value }: { value: number }) {
  const r = 65
  const circ = 2 * Math.PI * r
  const arc = circ * 0.75
  const fill = arc * (Math.min(100, Math.max(0, value)) / 100)
  return (
    <div className="relative flex shrink-0 items-center justify-center" style={{ width: 132, height: 132 }}>
      <svg width={132} height={132} viewBox="0 0 160 160" className="absolute inset-0" aria-hidden="true">
        <circle
          cx={80} cy={80} r={r} fill="none" strokeWidth={7} stroke="var(--muted)"
          strokeDasharray={`${arc.toFixed(1)} ${(circ - arc).toFixed(1)}`}
          strokeLinecap="round" transform="rotate(-225, 80, 80)"
        />
        <circle
          cx={80} cy={80} r={r} fill="none" strokeWidth={7} stroke={DZ_COLOR}
          strokeDasharray={`${fill.toFixed(1)} ${(circ - fill).toFixed(1)}`}
          strokeLinecap="round" transform="rotate(-225, 80, 80)"
          style={{ transition: 'stroke-dasharray 1.1s cubic-bezier(.2,.75,.3,1)' }}
        />
      </svg>
      <span className="font-mono text-2xl font-semibold tabular-nums">{pct(value)}</span>
    </div>
  )
}

// DoubleZero sits at zero and every other feed is placed by how long after it that feed's
// copy of the same update landed. One linear scale, so bar lengths compare across rows.
function ArrivalChart({ data }: { data: HyperliquidScoreboardResponse }) {
  const [grown, setGrown] = useState(false)
  const rows = useMemo(() => {
    const r = [
      { label: 'DoubleZero', dz: true, v: 0 },
      ...data.feeds.map((f) => ({ label: f.label, dz: false, v: f.p50_ms })),
    ]
    return r.sort((a, b) => a.v - b.v)
  }, [data.feeds])

  const top = Math.max(50, Math.ceil(Math.max(...rows.map((r) => r.v)) / 50) * 50)
  const step = top / 4

  useEffect(() => {
    setGrown(false)
    const id = requestAnimationFrame(() => setGrown(true))
    return () => cancelAnimationFrame(id)
  }, [rows])

  return (
    <div className="flex flex-col gap-2">
      {rows.map((r, i) => (
        <div key={r.label} className="grid items-center gap-3" style={{ gridTemplateColumns: '104px minmax(0,1fr) 58px' }}>
          <span className={`flex items-center gap-2 whitespace-nowrap text-xs ${r.dz ? 'font-medium' : 'text-muted-foreground'}`}>
            <span className="inline-block h-2.5 w-2.5 shrink-0" style={{ background: r.dz ? DZ_COLOR : FIELD_COLOR }} />
            {r.label}
          </span>
          <div className="hl-arr">
            <div className="hl-arr-axis" />
            {[0, 1, 2, 3, 4].map((k) => (
              <span key={k} className="hl-arr-tick" style={{ left: `${k * 25}%` }} />
            ))}
            {r.dz ? (
              <div className="hl-arr-dot" />
            ) : (
              <div
                className="hl-arr-bar"
                data-grown={grown ? '1' : '0'}
                style={{ width: `${((r.v / top) * 100).toFixed(1)}%`, transitionDelay: `${i * 70}ms` }}
              />
            )}
          </div>
          <span
            className={`text-right font-mono text-xs tabular-nums ${r.dz ? '' : 'text-muted-foreground'}`}
            style={r.dz ? { color: DZ_COLOR } : undefined}
          >
            {r.dz ? 'first' : ms(r.v)}
          </span>
        </div>
      ))}
      <div className="grid gap-3" style={{ gridTemplateColumns: '104px minmax(0,1fr) 58px' }}>
        <span />
        <div className="relative h-4">
          {[0, 1, 2, 3, 4].map((k) => (
            <span
              key={k}
              className="absolute top-0 whitespace-nowrap font-mono text-[10px] tabular-nums text-muted-foreground"
              style={{ left: `${k * 25}%`, transform: `translateX(${k === 0 ? '0' : k === 4 ? '-100%' : '-50%'})` }}
            >
              {k * step}
              {k === 4 ? ' ms' : ''}
            </span>
          ))}
        </div>
        <span />
      </div>
    </div>
  )
}

// The fill is scaled within its own column, so a cell compares against the other feeds at that
// site. One scale across the whole table would flatten everything but the slowest feed — the
// margin metrics span 12 ms to 36 s.
function MatrixCell({
  value, max, metric, who, where, highlighted, onHover,
}: {
  value: number; max: number; metric: MetricKey; who: string; where: string
  highlighted: boolean; onHover: (where: string) => void
}) {
  const [w, setW] = useState(0)
  useEffect(() => {
    const id = requestAnimationFrame(() => setW(max > 0 ? Math.max(4, (value / max) * 100) : 0))
    return () => cancelAnimationFrame(id)
  }, [value, max, metric])
  return (
    <td
      className={`relative whitespace-nowrap px-4 py-3 text-right ${highlighted ? 'hl-hl' : ''}`}
      onMouseEnter={() => onHover(where)}
      title={`${who} at ${where} — DoubleZero ${showMetric(metric, value)}`}
    >
      <span className="hl-cellfill" style={{ width: `${w}%` }} />
      <span className="relative font-mono text-sm tabular-nums">{showMetric(metric, value)}</span>
    </td>
  )
}

function FeedMatrix({ data }: { data: HyperliquidScoreboardResponse }) {
  // Whatever the API measured, not a fixed three. data.sites and each feed's sites come from
  // one server-side list, so the positional index below stays aligned.
  const siteCols = data.sites.map((s) => s.label)
  const [metric, setMetric] = useState<MetricKey>('win_pct')
  const [pill, setPill] = useState({ left: 0, width: 0 })
  const tabsRef = useRef<HTMLDivElement>(null)

  useLayoutEffect(() => {
    const on = tabsRef.current?.querySelector<HTMLElement>('[aria-selected="true"]')
    if (on) setPill({ left: on.offsetLeft, width: on.offsetWidth })
  }, [metric])

  const [hoverCol, setHoverCol] = useState<string | null>(null)
  const colCls = (c: string) => (hoverCol === c ? 'hl-hl' : '')

  const colMax = useMemo(
    () => data.sites.map((_, k) => Math.max(0, ...data.feeds.map((f) => f.sites[k]?.[metric] ?? 0))),
    [data.feeds, data.sites, metric],
  )

  return (
    <div className="mb-4 overflow-hidden rounded-lg border border-border bg-card">
      <div className="border-b border-border px-4 py-3 text-sm font-medium text-muted-foreground">
        Feeds we race, by recording site
      </div>
      <div
        ref={tabsRef}
        className="hl-tabwrap grid grid-cols-2 border-b border-border sm:grid-cols-4"
        role="tablist"
        aria-label="Metric shown in the table"
      >
        {METRICS.map((m, i) => (
          <button
            key={m.key}
            type="button"
            role="tab"
            aria-selected={m.key === metric}
            tabIndex={m.key === metric ? 0 : -1}
            onClick={() => setMetric(m.key)}
            onKeyDown={(e) => {
              const d = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0
              if (!d) return
              e.preventDefault()
              const next = METRICS[(i + d + METRICS.length) % METRICS.length]
              setMetric(next.key)
              tabsRef.current?.querySelectorAll<HTMLElement>('[role="tab"]')[
                (i + d + METRICS.length) % METRICS.length
              ]?.focus()
            }}
            className="hl-tab px-4 py-2.5 text-left transition-colors"
          >
            <span className="hl-tab-label block text-sm font-medium">{m.label}</span>
            <span className="block text-xs text-muted-foreground">{m.hint}</span>
          </button>
        ))}
        <span className="hl-tabpill" aria-hidden="true" style={{ left: pill.left, width: pill.width }} />
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] table-fixed" onMouseLeave={() => setHoverCol(null)}>
          <thead>
            <tr className="border-b border-border text-left text-sm text-muted-foreground">
              {/* Only the label column is sized; table-fixed splits the rest evenly. */}
              <th className="whitespace-nowrap px-4 py-3 font-medium" style={{ width: '20%' }}>Feed</th>
              {siteCols.map((c) => (
                <th key={c} className={`whitespace-nowrap px-4 py-3 text-right font-medium ${colCls(c)}`}>
                  {c}
                </th>
              ))}
              <th className="whitespace-nowrap px-4 py-3 text-right font-medium">All sites</th>
            </tr>
          </thead>
          <tbody>
            {data.feeds.map((f) => (
              <tr key={f.label} className="hl-rowhover border-b border-border transition-colors">
                <td className="px-4 py-3">
                  <span className="flex items-center gap-2 whitespace-nowrap text-sm font-medium">
                    <span className="inline-block h-2.5 w-2.5 shrink-0" style={{ background: FIELD_COLOR }} />
                    {f.label}
                  </span>
                </td>
                {f.sites.map((s, k) => (
                  <MatrixCell
                    key={s.code}
                    value={s[metric]}
                    max={colMax[k]}
                    metric={metric}
                    who={f.label}
                    where={siteCols[k]}
                    highlighted={hoverCol === siteCols[k]}
                    onHover={setHoverCol}
                  />
                ))}
                <td className="whitespace-nowrap px-4 py-3 text-right font-mono text-sm tabular-nums text-muted-foreground">
                  {showMetric(metric, f[metric])}
                </td>
              </tr>
            ))}
            <tr className="hl-grouprow">
              <td className="px-4 py-3 text-sm font-medium">All feeds</td>
              {data.sites.map((s, k) => (
                <td
                  key={s.code}
                  className={`whitespace-nowrap px-4 py-3 text-right font-mono text-sm font-medium tabular-nums ${colCls(siteCols[k])}`}
                  onMouseEnter={() => setHoverCol(siteCols[k])}
                >
                  {showMetric(metric, s[metric])}
                </td>
              ))}
              <td className="whitespace-nowrap px-4 py-3 text-right font-mono text-sm font-medium tabular-nums">
                {showMetric(metric, data.all[metric])}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}

function MarketTable({ data }: { data: HyperliquidScoreboardResponse }) {
  return (
    <div className="mb-4 overflow-hidden rounded-lg border border-border bg-card">
      <div className="flex flex-wrap items-baseline justify-between gap-2 border-b border-border px-4 py-3">
        <span className="text-sm font-medium text-muted-foreground">By market</span>
        <span className="text-xs text-muted-foreground">How far ahead DoubleZero finished</span>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full">
          <thead>
            <tr className="border-b border-border text-left text-sm text-muted-foreground">
              <th className="whitespace-nowrap px-4 py-3 font-medium">Market / contract type</th>
              <th className="whitespace-nowrap px-4 py-3 text-right font-medium" style={{ width: 150 }}>
                DoubleZero first
              </th>
              <th className="whitespace-nowrap px-4 py-3 text-right font-medium">Median</th>
              <th className="whitespace-nowrap px-4 py-3 text-right font-medium">95th</th>
              <th className="whitespace-nowrap px-4 py-3 text-right font-medium">99th</th>
            </tr>
          </thead>
          <tbody>
            {data.markets.map((m) => (
              <Fragment key={m.name}>
                <tr className="hl-grouprow border-b border-border">
                  <td className="px-4 py-2 text-sm font-medium">{m.name}</td>
                  <td colSpan={4} className="px-4 py-2 text-right font-mono text-xs tabular-nums text-muted-foreground">
                    {m.carried} instruments carried
                  </td>
                </tr>
                {m.cats.map((c) => {
                  const delta = c.win_pct - data.all.win_pct
                  return (
                    <tr key={`${m.name}-${c.name}`} className="hl-rowhover border-b border-border transition-colors">
                      <td className="py-3 pl-9 pr-4">
                        <div className="text-sm">{c.name}</div>
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-right" style={{ width: 150 }}>
                        <div className="font-mono text-sm tabular-nums">{pct(c.win_pct)}</div>
                        <div
                          className="font-mono text-[11px] tabular-nums"
                          style={{ color: delta >= 0 ? DZ_COLOR : 'var(--muted-foreground)' }}
                        >
                          {delta >= 0 ? '+' : '−'}
                          {Math.abs(delta).toFixed(1)} pt
                        </div>
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-right font-mono text-sm tabular-nums">
                        {ms(c.p50_ms)}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-right font-mono text-sm tabular-nums text-muted-foreground">
                        {ms(c.p95_ms)}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-right font-mono text-sm tabular-nums text-muted-foreground">
                        {ms(c.p99_ms)}
                      </td>
                    </tr>
                  )
                })}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function HyperliquidScoreboardPage() {
  const [data, setData] = useState<HyperliquidScoreboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)
  const [now, setNow] = useState(() => Date.now())

  const load = useCallback(async () => {
    try {
      setData(await fetchHyperliquidScoreboard())
      setError(null)
      setPending(false)
    } catch (e) {
      // A cold cache is not a failure: the poll below picks it up once the worker writes.
      if (e instanceof HyperliquidScoreboardPendingError) {
        setPending(true)
        setError(null)
        return
      }
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [])

  useEffect(() => {
    let active = true
    const run = () => { void load() }
    run()
    const poll = setInterval(run, 60000)
    const tick = setInterval(() => active && setNow(Date.now()), 5000)
    return () => { active = false; clearInterval(poll); clearInterval(tick) }
  }, [load])

  const freshness = useMemo(() => {
    if (!data?.as_of) return null
    const asOf = new Date(data.as_of).getTime()
    const age = Math.round((now - asOf) / 1000)
    const stale = isBehind(asOf, now)
    if (age < 60) return { text: 'just now', stale }
    if (age < 3600) return { text: `${Math.round(age / 60)}m ago`, stale }
    return { text: `${Math.round(age / 3600)}h ago`, stale }
  }, [data?.as_of, now])

  return (
    <div className="flex-1 overflow-auto" style={{ ['--hl-dz' as string]: DZ_COLOR, ['--hl-field' as string]: FIELD_COLOR }}>
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-8">
        <PageHeader
          icon={Trophy}
          title="Hyperliquid Scoreboard"
          subtitle={
            <span className="flex items-center gap-2 text-xs text-muted-foreground/50">
              <span>{data?.window_label ?? 'last 24 hours'}</span>
              {freshness && (
                <>
                  <span>·</span>
                  <span
                    className={`inline-block h-1.5 w-1.5 rounded-full ${freshness.stale ? 'bg-amber-500' : 'hl-pulse'}`}
                    style={freshness.stale ? undefined : { background: DZ_COLOR }}
                  />
                  <span className={freshness.stale ? 'text-amber-600 dark:text-amber-400' : undefined}>
                    updated {freshness.text}
                    {freshness.stale ? ' — no update since midnight UTC' : ''}
                  </span>
                </>
              )}
            </span>
          }
        />

        {error && <div className="rounded-lg border border-border bg-card p-6 text-sm text-red-500">{error}</div>}

        {!error && !data && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
            {pending ? 'Scoreboard is being computed. This page updates itself.' : 'Loading…'}
          </div>
        )}

        {!error && data && data.races === 0 && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
            No races recorded in this window.
          </div>
        )}

        {!error && data && data.races > 0 && (
          <>
            <div className="mb-4 rounded-lg border border-border bg-card">
              <div className="flex flex-col lg:flex-row">
                <div className="flex min-w-0 shrink-0 items-center gap-4 p-4 sm:gap-5 sm:p-5 lg:w-[42%]">
                  <WinGauge value={data.all.win_pct} />
                  <div className="min-w-0">
                    <div className="text-lg font-medium leading-snug" style={{ textWrap: 'balance' } as CSSProperties}>
                      DoubleZero delivers the order book first.
                    </div>
                    <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                      Each update, raced against the venue's public API and every commercial feed we buy.
                    </p>
                  </div>
                </div>
                <div className="flex min-w-0 flex-1 flex-col justify-center gap-2.5 border-t border-border p-4 sm:p-5 lg:border-l lg:border-t-0">
                  <div className="flex items-baseline justify-between gap-3">
                    <span className="text-sm font-medium">One update, every feed racing</span>
                    <span className="font-mono text-xs text-muted-foreground">median, after DoubleZero</span>
                  </div>
                  <ArrivalChart data={data} />
                </div>
              </div>

              <div
                className="grid grid-cols-2 gap-px border-t border-border sm:grid-cols-4"
                style={{ background: 'var(--border)' }}
              >
                <div className="bg-card px-4 py-3">
                  <div className="text-xs text-muted-foreground">Updates raced</div>
                  <div className="font-mono text-xl font-semibold tabular-nums">{fmtCount(data.races)}</div>
                </div>
                <div className="bg-card px-4 py-3">
                  <div className="text-xs text-muted-foreground">Instruments carried</div>
                  <div className="font-mono text-xl font-semibold tabular-nums">{data.instruments}</div>
                </div>
                <div className="bg-card px-4 py-3">
                  <div className="text-xs text-muted-foreground">Recording sites</div>
                  <div className="font-mono text-xl font-semibold tabular-nums">{data.site_count}</div>
                </div>
                <div className="bg-card px-4 py-3">
                  <div className="text-xs text-muted-foreground">Feeds raced against</div>
                  <div className="font-mono text-xl font-semibold tabular-nums">{data.feed_count}</div>
                </div>
              </div>
            </div>

            <FeedMatrix data={data} />

            <MarketTable data={data} />
          </>
        )}
      </div>
    </div>
  )
}
