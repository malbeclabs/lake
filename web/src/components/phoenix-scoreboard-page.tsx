import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { CSSProperties, PointerEvent, ReactNode } from 'react'
import { Trophy } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchPhoenixScoreboard,
  type PhoenixScoreboardBucket,
  type PhoenixScoreboardResponse,
} from '@/lib/api'

// The public Phoenix scoreboard: DoubleZero's top-of-book feed raced against Phoenix's own,
// both recorded on one host. Leads are signed — positive means DoubleZero delivered first.
// Built on the Hyperliquid scoreboard's layout and classes.

const DZ_COLOR = '#34d399'
const FIELD_COLOR = '#d99a3c'
const LOSS_COLOR = '#f87171'

const BUCKET_MS = 15 * 60 * 1000

const METRICS = [
  { key: 'win_pct', label: 'Win rate', hint: 'share delivered first', desc: 'Share of updates DoubleZero delivered first' },
  { key: 'p50_ms', label: 'Median', hint: 'typical margin', desc: 'Typical DoubleZero lead over the Phoenix feed' },
  { key: 'p95_ms', label: '95th pct', hint: 'best 5% of races', desc: 'Lead in the best 5% of races' },
  { key: 'p99_ms', label: '99th pct', hint: 'best 1% of races', desc: 'Lead in the best 1% of races' },
] as const

type MetricKey = (typeof METRICS)[number]['key']

const LEAD_PHRASE: Record<Exclude<MetricKey, 'win_pct'>, string> = {
  p50_ms: 'Median',
  p95_ms: '95th percentile',
  p99_ms: '99th percentile',
}

// Stale only once the scheduled refresh (plus a grace period for the worker cycle) has passed.
const REFRESH_GRACE_MS = 30 * 60 * 1000

function prefersCalm(): boolean {
  return typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches
}

function hhmm(ms: number): string {
  return new Date(ms).toISOString().slice(11, 16)
}

function dayHhmm(ms: number): string {
  const d = new Date(ms)
  return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })} ${hhmm(ms)}`
}

function pct(v: number): string {
  return `${v.toFixed(1)}%`
}

function pct2(v: number): string {
  return `${v.toFixed(2)}%`
}

function count(n: number): string {
  return Math.round(n).toLocaleString('en-US')
}

// Always milliseconds, so every lead on the page reads on one scale. Never a bare "+" when
// negative: a venue-first margin has to read as a deficit.
function ms(v: number): string {
  const sign = v < 0 ? '−' : '+'
  return `${sign}${absMs(v)}`
}

function absMs(v: number): string {
  const a = Math.abs(v)
  return a > 0 && a < 10 ? `${a.toFixed(1)} ms` : `${count(a)} ms`
}

function leadLabel(v: number): string {
  return v < 0 ? `Phoenix ahead by ${absMs(v)}` : `DoubleZero ahead by ${absMs(v)}`
}

function show(metric: MetricKey, v: number): string {
  return metric === 'win_pct' ? pct2(v) : ms(v)
}

// Counts from `from` to `to` over `duration`, restarting whenever `to` changes.
function useCountUp(to: number, { duration = 1200, delay = 0 } = {}): { value: number; settled: boolean } {
  const shown = useRef<number | null>(null)
  const [state, setState] = useState({ value: prefersCalm() ? to : 0, settled: prefersCalm() })
  useEffect(() => {
    const from = shown.current ?? 0
    const wait = shown.current === null ? delay : 0
    if (prefersCalm() || from === to) {
      shown.current = to
      setState({ value: to, settled: true })
      return
    }
    let raf = 0
    const t0 = performance.now() + wait
    const step = (now: number) => {
      const t = Math.min(1, Math.max(0, (now - t0) / duration))
      const value = from + (to - from) * (1 - Math.pow(1 - t, 3))
      shown.current = value
      setState((prev) => ({ value, settled: prev.settled || t >= 1 }))
      if (t < 1) raf = requestAnimationFrame(step)
    }
    raf = requestAnimationFrame(step)
    return () => cancelAnimationFrame(raf)
  }, [to, duration, delay])
  return state
}

function CountUp({ to, format, delay, pop }: {
  to: number; format: (v: number) => string; delay?: number; pop?: boolean
}) {
  const { value, settled } = useCountUp(to, { delay })
  return <span className={pop && settled && !prefersCalm() ? 'phx-pop' : undefined}>{format(value)}</span>
}

function WinGauge({ value }: { value: number }) {
  const r = 65
  const circ = 2 * Math.PI * r
  const arc = circ * 0.75
  const [shown, setShown] = useState(prefersCalm() ? value : 0)
  useEffect(() => {
    const id = requestAnimationFrame(() => setShown(value))
    return () => cancelAnimationFrame(id)
  }, [value])
  const fill = arc * (Math.min(100, Math.max(0, shown)) / 100)
  return (
    <div className="relative flex shrink-0 items-center justify-center" style={{ width: 132, height: 132 }}>
      <svg width={132} height={132} viewBox="0 0 160 160" className="absolute inset-0" aria-hidden="true">
        <circle
          cx={80} cy={80} r={r} fill="none" strokeWidth={7} stroke="var(--muted)"
          strokeDasharray={`${arc.toFixed(1)} ${(circ - arc).toFixed(1)}`}
          strokeLinecap="round" transform="rotate(-225, 80, 80)"
        />
        <circle
          className="phx-breathe"
          cx={80} cy={80} r={r} fill="none" strokeWidth={7} stroke={DZ_COLOR}
          strokeDasharray={`${fill.toFixed(1)} ${(circ - fill).toFixed(1)}`}
          strokeLinecap="round" transform="rotate(-225, 80, 80)"
          style={{ transition: 'stroke-dasharray 1.1s cubic-bezier(.2,.75,.3,1)' }}
        />
      </svg>
      <span className="font-mono text-2xl font-semibold tabular-nums">
        <CountUp to={value} format={pct} />
      </span>
    </div>
  )
}

// DoubleZero sits at zero and the Phoenix feed is placed by how long after it the same update
// landed, at the median.
function ArrivalChart({ lagMs }: { lagMs: number }) {
  const [grown, setGrown] = useState(false)
  useEffect(() => {
    setGrown(false)
    const id = requestAnimationFrame(() => setGrown(true))
    return () => cancelAnimationFrame(id)
  }, [lagMs])

  // Signed, so the axis has to be able to start left of zero: a Phoenix feed that beat
  // DoubleZero is drawn left of the dot, in the loss colour.
  const lo = Math.min(0, Math.floor(lagMs / 50) * 50)
  const hi = Math.max(lo + 50, 0, Math.ceil(lagMs / 50) * 50)
  const span = hi - lo
  const step = span / 4
  const pctOf = (v: number) => ((v - lo) / span) * 100
  const zeroPct = pctOf(0)
  const rows = [
    { label: 'DoubleZero', dz: true, v: 0 },
    { label: 'Phoenix feed', dz: false, v: lagMs },
  ].sort((a, b) => a.v - b.v)
  const cols = { gridTemplateColumns: '104px minmax(0,1fr) 80px' }

  return (
    <div className="flex flex-col gap-2">
      {rows.map((r) => (
        <div key={r.label} className="grid items-center gap-3" style={cols}>
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
              <div className="hl-arr-dot phx-sonar" style={{ left: `${zeroPct}%`, marginLeft: -7, zIndex: 1 }} />
            ) : (
              <div
                className="hl-arr-bar"
                data-grown={grown ? '1' : '0'}
                style={{
                  left: `${Math.min(zeroPct, pctOf(r.v)).toFixed(1)}%`,
                  width: `${Math.abs(pctOf(r.v) - zeroPct).toFixed(1)}%`,
                  transformOrigin: r.v < 0 ? 'right center' : 'left center',
                  background: r.v < 0 ? 'var(--hl-loss)' : undefined,
                }}
              />
            )}
          </div>
          <span
            className={`text-right font-mono text-xs tabular-nums ${r.dz ? '' : 'text-muted-foreground'}`}
            style={r.dz ? { color: DZ_COLOR } : undefined}
          >
            {r.v === rows[0].v ? 'first' : `+${absMs(r.v - rows[0].v)}`}
          </span>
        </div>
      ))}
      <div className="grid gap-3" style={cols}>
        <span />
        <div className="relative h-4">
          {[0, 1, 2, 3, 4].map((k) => (
            <span
              key={k}
              className="absolute top-0 whitespace-nowrap font-mono text-[10px] tabular-nums text-muted-foreground"
              style={{ left: `${k * 25}%`, transform: `translateX(${k === 0 ? '0' : k === 4 ? '-100%' : '-50%'})` }}
            >
              {count(lo + k * step)}
              {k === 4 ? ' ms' : ''}
            </span>
          ))}
        </div>
        <span />
      </div>
    </div>
  )
}

// Monotone cubic through every point (harmonic-mean tangents): smooth, and never swings past
// the measured values the way an ordinary spline does.
function smoothPath(p: [number, number][]): string {
  if (p.length === 1) return `M${p[0][0].toFixed(1)},${p[0][1].toFixed(1)}`
  const n = p.length
  const d: number[] = []
  for (let i = 0; i < n - 1; i++) d.push((p[i + 1][1] - p[i][1]) / (p[i + 1][0] - p[i][0]))
  const m: number[] = [d[0]]
  for (let i = 1; i < n - 1; i++) m.push(d[i - 1] * d[i] <= 0 ? 0 : (2 * d[i - 1] * d[i]) / (d[i - 1] + d[i]))
  m.push(d[n - 2])
  let out = `M${p[0][0].toFixed(1)},${p[0][1].toFixed(1)}`
  for (let i = 0; i < n - 1; i++) {
    const h = (p[i + 1][0] - p[i][0]) / 3
    out +=
      `C${(p[i][0] + h).toFixed(1)},${(p[i][1] + m[i] * h).toFixed(1)} ` +
      `${(p[i + 1][0] - h).toFixed(1)},${(p[i + 1][1] - m[i + 1] * h).toFixed(1)} ` +
      `${p[i + 1][0].toFixed(1)},${p[i + 1][1].toFixed(1)}`
  }
  return out
}

// Splits the series where a window is missing, so an idle stretch reads as a gap rather than
// as a line drawn straight across it.
function runs(buckets: PhoenixScoreboardBucket[]): PhoenixScoreboardBucket[][] {
  const out: PhoenixScoreboardBucket[][] = []
  let prev = -Infinity
  for (const b of buckets) {
    const t = Date.parse(b.start)
    if (t - prev > BUCKET_MS || out.length === 0) out.push([])
    out[out.length - 1].push(b)
    prev = t
  }
  return out
}

function winDomain(buckets: PhoenixScoreboardBucket[]): { lo: number; hi: number; ticks: number[] } {
  const min = Math.min(100, ...buckets.map((b) => b.win_pct))
  const lo = min >= 95 ? 95 : Math.floor(min / 5) * 5
  const step = (100 - lo) / 5
  return { lo, hi: 100, ticks: [0, 1, 2, 3, 4, 5].map((i) => lo + i * step) }
}

function leadDomain(values: number[]): { lo: number; hi: number; ticks: number[] } {
  const max = Math.max(0, ...values)
  const min = Math.min(0, ...values)
  const unit = [250, 500, 1000, 2000, 5000].find((u) => Math.ceil(max / u) - Math.floor(min / u) <= 6) ?? 5000
  const lo = Math.floor(min / unit) * unit
  const hi = Math.max(unit, Math.ceil(max / unit) * unit)
  const ticks: number[] = []
  for (let v = lo; v <= hi; v += unit) ticks.push(v)
  return { lo, hi, ticks }
}

const CHART_H = 260
// Monospace glyph width at the 11px axis size, used to fit the y-axis gutter to its labels.
const AXIS_CHAR_PX = 6.8

function TrendChart({ data, metric }: { data: PhoenixScoreboardResponse; metric: MetricKey }) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const lineRef = useRef<SVGPathElement>(null)
  const [width, setWidth] = useState(0)
  const [hover, setHover] = useState<number | null>(null)
  const calm = prefersCalm()

  useLayoutEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setWidth(el.clientWidth))
    ro.observe(el)
    setWidth(el.clientWidth)
    return () => ro.disconnect()
  }, [])

  const win = metric === 'win_pct'
  const t0 = Date.parse(data.window_start)
  const t1 = Math.max(t0 + BUCKET_MS, Date.parse(data.window_end) - BUCKET_MS)
  const { lo, hi, ticks } = win ? winDomain(data.buckets) : leadDomain(data.buckets.map((b) => b[metric]))
  const tickLabel = (v: number) => (win ? `${v}%` : `${v < 0 ? '−' : ''}${count(Math.abs(v))} ms`)
  // The gutter is sized to the widest label, so "4,000 ms" is never cut at the left edge.
  const widest = Math.max(...ticks.map((v) => tickLabel(v).length))
  const PAD = { l: Math.ceil(widest * AXIS_CHAR_PX) + 16, r: 16, t: 14, b: 24 }
  const iw = Math.max(1, width - PAD.l - PAD.r)
  const ih = CHART_H - PAD.t - PAD.b
  const x = (t: number) => PAD.l + ((t - t0) / (t1 - t0)) * iw
  const y = (v: number) => PAD.t + ih - ((Math.max(lo, Math.min(hi, v)) - lo) / (hi - lo)) * ih
  const base = win ? y(lo) : y(0)

  const segments = useMemo(
    () =>
      runs(data.buckets).map((run) => {
        const pts = run.map((b) => [x(Date.parse(b.start)), y(b[metric])] as [number, number])
        const curve = smoothPath(pts)
        return {
          curve,
          area: `${curve}L${pts[pts.length - 1][0].toFixed(1)},${base}L${pts[0][0].toFixed(1)},${base}Z`,
        }
      }),
    // x and y are rebuilt every render from these inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [data.buckets, metric, width],
  )
  const cometPath = useMemo(() => segments.map((s) => s.curve).join(' '), [segments])

  // The draw-in runs on a metric change, not on a resize.
  const [drawKey, setDrawKey] = useState(0)
  useEffect(() => setDrawKey((k) => k + 1), [metric])
  useLayoutEffect(() => {
    const el = lineRef.current
    if (el) el.style.setProperty('--phx-len', String(Math.ceil(el.getTotalLength())))
  }, [drawKey, segments])

  const hours = useMemo(() => {
    const out: number[] = []
    const first = Math.ceil(t0 / (3 * 3600 * 1000)) * 3 * 3600 * 1000
    for (let t = first; t <= t1; t += 3 * 3600 * 1000) out.push(t)
    return out
  }, [t0, t1])

  const last = data.buckets[data.buckets.length - 1]
  const hovered = hover !== null ? data.buckets[hover] : null

  const onMove = (e: PointerEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect()
    const t = t0 + ((e.clientX - rect.left - PAD.l) / iw) * (t1 - t0)
    let best = 0
    for (let i = 1; i < data.buckets.length; i++) {
      if (Math.abs(Date.parse(data.buckets[i].start) - t) < Math.abs(Date.parse(data.buckets[best].start) - t)) best = i
    }
    setHover(best)
  }

  const hx = hovered ? x(Date.parse(hovered.start)) : 0
  const tipLeft = hovered ? (hx + 14 + 196 > width ? hx - 14 - 196 : hx + 14) : 0

  return (
    <div ref={wrapRef} className="relative">
      {width > 0 && (
        <svg
          width={width}
          height={CHART_H}
          viewBox={`0 0 ${width} ${CHART_H}`}
          className="block touch-pan-y overflow-visible"
          role="img"
          aria-label={`${METRICS.find((m) => m.key === metric)?.label} every 15 minutes`}
          onPointerMove={onMove}
          onPointerLeave={() => setHover(null)}
        >
          <defs>
            <linearGradient id="px-area" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0" stopColor={DZ_COLOR} stopOpacity={win ? 0.45 : 0.38} />
              <stop offset="1" stopColor={DZ_COLOR} stopOpacity={win ? 0.12 : 0.04} />
            </linearGradient>
            <filter id="px-glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3" />
            </filter>
          </defs>
          {ticks.map((v) => (
            <g key={v}>
              <line x1={PAD.l} x2={width - PAD.r} y1={y(v)} y2={y(v)} stroke="var(--border)" />
              <text x={PAD.l - 8} y={y(v) + 3} textAnchor="end" className="fill-muted-foreground font-mono text-[11px]">
                {tickLabel(v)}
              </text>
            </g>
          ))}
          {hours.map((t) => (
            <g key={t}>
              <line x1={x(t)} x2={x(t)} y1={PAD.t + ih} y2={PAD.t + ih + 4} stroke="var(--border)" />
              <text
                x={x(t)}
                y={CHART_H - 6}
                textAnchor={x(t) > width - PAD.r - 18 ? 'end' : x(t) < PAD.l + 18 ? 'start' : 'middle'}
                className="fill-muted-foreground font-mono text-[11px]"
              >
                {hhmm(t)}
              </text>
            </g>
          ))}
          <g key={`area-${drawKey}`} className="phx-fade">
            {segments.map((s, i) => (
              <path key={i} d={s.area} fill="url(#px-area)" />
            ))}
          </g>
          {!win && (
            <line
              x1={PAD.l} x2={width - PAD.r} y1={y(data.all[metric])} y2={y(data.all[metric])}
              stroke={FIELD_COLOR} strokeWidth={1.5} strokeDasharray="4 4" opacity={0.8}
            />
          )}
          {segments.length === 1 ? (
            <path
              key={`line-${drawKey}`} ref={lineRef} className="phx-draw" d={segments[0].curve}
              fill="none" stroke={DZ_COLOR} strokeWidth={2.25} strokeLinejoin="round" strokeLinecap="round"
            />
          ) : (
            segments.map((s, i) => (
              <path
                key={`line-${drawKey}-${i}`} className="phx-fade" d={s.curve}
                fill="none" stroke={DZ_COLOR} strokeWidth={2.25} strokeLinejoin="round" strokeLinecap="round"
              />
            ))
          )}
          <text
            x={PAD.l + 14}
            y={base - 34}
            className="phx-inlabel text-sm font-semibold"
            style={{ fill: !win && data.all[metric] < 0 ? 'var(--hl-loss)' : 'var(--hl-dz)' }}
          >
            {win ? `DoubleZero first · ${pct(data.all.win_pct)}` : leadLabel(data.all[metric])}
          </text>
          <text x={PAD.l + 14} y={base - 16} className="phx-inlabel fill-foreground text-xs font-medium">
            {win
              ? `Venue first on ${pct2(data.races ? (100 * data.venue_wins) / data.races : 0)} of updates`
              : `${LEAD_PHRASE[metric]} across ${count(data.races)} updates`}
          </text>
          {!calm && data.buckets.length > 1 && (
            <g key={`comet-${drawKey}`} opacity={0}>
              <animate attributeName="opacity" to="1" begin="0.9s" dur="0.4s" fill="freeze" />
              <circle r={7} fill={DZ_COLOR} filter="url(#px-glow)" opacity={0.8}>
                <animateMotion dur="7s" begin="0.9s" repeatCount="indefinite" path={cometPath} />
              </circle>
              <circle r={3} fill="var(--card)" stroke={DZ_COLOR} strokeWidth={2}>
                <animateMotion dur="7s" begin="0.9s" repeatCount="indefinite" path={cometPath} />
              </circle>
            </g>
          )}
          {last && (
            <>
              {!calm && (
                <circle cx={x(Date.parse(last.start))} cy={y(last[metric])} r={4} fill="none" stroke={DZ_COLOR} strokeWidth={2}>
                  <animate attributeName="r" values="4;14" dur="2s" repeatCount="indefinite" />
                  <animate attributeName="opacity" values=".8;0" dur="2s" repeatCount="indefinite" />
                </circle>
              )}
              <circle cx={x(Date.parse(last.start))} cy={y(last[metric])} r={4} fill={DZ_COLOR} stroke="var(--card)" strokeWidth={2} />
            </>
          )}
          {hovered && (
            <g pointerEvents="none">
              <line x1={hx} x2={hx} y1={PAD.t} y2={PAD.t + ih} stroke="var(--muted-foreground)" strokeDasharray="2 3" />
              <circle cx={hx} cy={y(hovered[metric])} r={5} fill={DZ_COLOR} stroke="var(--card)" strokeWidth={2} />
            </g>
          )}
        </svg>
      )}
      {hovered && (
        <div
          className="pointer-events-none absolute top-2 z-10 w-[196px] rounded-md border border-border bg-card px-3 py-2 text-xs shadow-lg"
          style={{ left: tipLeft }}
        >
          <div className="mb-1 font-mono text-muted-foreground">
            {dayHhmm(Date.parse(hovered.start))}–{hhmm(Date.parse(hovered.start) + BUCKET_MS)} UTC
          </div>
          <TipRow label="Raced" value={count(hovered.races)} />
          <TipRow label="DoubleZero first" value={count(hovered.dz_wins)} color={DZ_COLOR} />
          <TipRow label="Venue first" value={count(hovered.venue_wins)} color={hovered.venue_wins ? LOSS_COLOR : undefined} />
          {METRICS.map((m) => (
            <TipRow key={m.key} label={m.label} value={show(m.key, hovered[m.key])} strong={m.key === metric} />
          ))}
        </div>
      )}
    </div>
  )
}

function TipRow({ label, value, color, strong }: { label: string; value: string; color?: string; strong?: boolean }) {
  return (
    <div className="flex justify-between gap-4">
      <span className={strong ? 'font-medium text-foreground' : 'text-muted-foreground'}>{label}</span>
      <span className="font-mono tabular-nums" style={color ? { color } : undefined}>{value}</span>
    </div>
  )
}

function TrendCard({ data }: { data: PhoenixScoreboardResponse }) {
  const [metric, setMetric] = useState<MetricKey>('win_pct')
  const [pill, setPill] = useState({ left: 0, width: 0 })
  const tabsRef = useRef<HTMLDivElement>(null)

  useLayoutEffect(() => {
    const place = () => {
      const on = tabsRef.current?.querySelector<HTMLElement>('[aria-selected="true"]')
      if (on) setPill({ left: on.offsetLeft, width: on.offsetWidth })
    }
    place()
    window.addEventListener('resize', place)
    return () => window.removeEventListener('resize', place)
  }, [metric])

  const m = METRICS.find((x) => x.key === metric)!

  return (
    <div className="phx-rise mb-4 overflow-hidden rounded-lg border border-border bg-card" style={{ animationDelay: '120ms' }}>
      <div className="flex flex-wrap justify-between gap-3 border-b border-border px-4 py-3 text-sm font-medium text-muted-foreground">
        <span>Every 15 minutes</span>
        <span className="font-mono text-xs font-normal">
          {dayHhmm(Date.parse(data.window_start))} – {dayHhmm(Date.parse(data.window_end))} UTC
        </span>
      </div>
      <div
        ref={tabsRef}
        className="hl-tabwrap grid grid-cols-2 border-b border-border sm:grid-cols-4"
        role="tablist"
        aria-label="Metric shown in the chart"
      >
        {METRICS.map((x, i) => (
          <button
            key={x.key}
            type="button"
            role="tab"
            aria-selected={x.key === metric}
            tabIndex={x.key === metric ? 0 : -1}
            onClick={() => setMetric(x.key)}
            onKeyDown={(e) => {
              const d = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0
              if (!d) return
              e.preventDefault()
              const next = (i + d + METRICS.length) % METRICS.length
              setMetric(METRICS[next].key)
              tabsRef.current?.querySelectorAll<HTMLElement>('[role="tab"]')[next]?.focus()
            }}
            className="hl-tab px-4 py-2.5 text-left transition-colors"
          >
            <span className="hl-tab-label block text-sm font-medium">{x.label}</span>
            <span className="block text-xs text-muted-foreground">{x.hint}</span>
          </button>
        ))}
        <span className="hl-tabpill" aria-hidden="true" style={{ left: pill.left, width: pill.width }} />
      </div>
      <div className="px-4 pb-2 pt-4">
        <div className="mb-2 flex flex-wrap items-baseline justify-between gap-3">
          <span className="text-sm text-muted-foreground">{m.desc}</span>
          <span>
            <span className="text-xs text-muted-foreground">24h </span>
            <span className="font-mono text-xl font-semibold tabular-nums">
              <CountUp key={metric} to={data.all[metric]} format={(v) => show(metric, v)} />
            </span>
          </span>
        </div>
        <TrendChart data={data} metric={metric} />
      </div>
      <div className="flex flex-wrap gap-4 px-4 pb-3.5 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-0.5 w-3.5" style={{ background: DZ_COLOR }} />
          {metric === 'win_pct' ? 'Win rate per window' : `${m.label} lead per window`}
        </span>
        {metric !== 'win_pct' && (
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-0.5 w-3.5 opacity-70" style={{ background: FIELD_COLOR }} />
            24h {m.label.toLowerCase()}
          </span>
        )}
      </div>
    </div>
  )
}

function Stat({ label, children, delay, title }: { label: string; children: ReactNode; delay: number; title?: string }) {
  return (
    <div className="phx-rise bg-card px-4 py-3" style={{ animationDelay: `${delay}ms` }} title={title}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="font-mono text-xl font-semibold tabular-nums">{children}</div>
    </div>
  )
}

function Freshness({ asOf, nextRefreshAt }: { asOf: string; nextRefreshAt: string }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 5000)
    return () => clearInterval(tick)
  }, [])
  const age = Math.round((now - Date.parse(asOf)) / 1000)
  const next = Date.parse(nextRefreshAt)
  const stale = now - next > REFRESH_GRACE_MS
  const text = age < 60 ? 'just now' : age < 3600 ? `${Math.round(age / 60)}m ago` : `${Math.round(age / 3600)}h ago`
  return (
    <>
      <span>·</span>
      <span
        className={`inline-block h-1.5 w-1.5 rounded-full ${stale ? 'bg-amber-500' : 'hl-pulse'}`}
        style={stale ? undefined : { background: DZ_COLOR }}
      />
      <span className={stale ? 'text-amber-600 dark:text-amber-400' : undefined}>
        updated {text}
        {stale ? ` — missed the ${hhmm(next)} UTC refresh` : ''}
      </span>
    </>
  )
}

export function PhoenixScoreboardPage() {
  const [data, setData] = useState<PhoenixScoreboardResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [site, setSite] = useState<string | undefined>(undefined)

  const load = useCallback(async () => {
    try {
      setData(await fetchPhoenixScoreboard(site))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [site])

  useEffect(() => {
    void load()
    const poll = setInterval(() => void load(), 60000)
    return () => clearInterval(poll)
  }, [load])

  return (
    <div
      className="flex-1 overflow-auto"
      style={{
        ['--hl-dz' as string]: DZ_COLOR,
        ['--hl-field' as string]: FIELD_COLOR,
        ['--hl-loss' as string]: LOSS_COLOR,
      }}
    >
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-8">
        <PageHeader
          icon={Trophy}
          title="Phoenix Scoreboard"
          subtitle={
            <span className="flex items-center gap-2 text-xs text-muted-foreground/50">
              <span>last 24 hours</span>
              {data?.as_of && data.next_refresh_at && (
                <Freshness asOf={data.as_of} nextRefreshAt={data.next_refresh_at} />
              )}
              {error && data && (
                <>
                  <span>·</span>
                  <span className="text-amber-600 dark:text-amber-400">couldn't refresh, showing the last loaded board</span>
                </>
              )}
            </span>
          }
          actions={
            data && data.sites.length > 1 ? (
              <div className="flex overflow-hidden rounded-md border border-border text-xs" role="group" aria-label="Recording site">
                {data.sites.map((x) => (
                  <button
                    key={x.code}
                    type="button"
                    aria-pressed={x.code === data.site}
                    onClick={() => setSite(x.code)}
                    className={`border-l border-border px-3 py-1.5 first:border-l-0 transition-colors ${
                      x.code === data.site ? 'bg-muted font-medium text-foreground' : 'text-muted-foreground hover:bg-muted/50'
                    }`}
                  >
                    {x.label}
                  </button>
                ))}
              </div>
            ) : undefined
          }
        />

        {error && !data && <div className="rounded-lg border border-border bg-card p-6 text-sm text-red-500">{error}</div>}

        {!error && !data && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">Loading…</div>
        )}

        {data && data.races === 0 && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
            No races recorded in this window.
          </div>
        )}

        {data && data.races > 0 && (
          <>
            <div className="phx-rise mb-4 rounded-lg border border-border bg-card">
              <div className="flex flex-col lg:flex-row">
                <div className="flex min-w-0 shrink-0 items-center gap-4 p-4 sm:gap-5 sm:p-5 lg:w-[42%]">
                  <WinGauge value={data.all.win_pct} />
                  <div className="min-w-0">
                    <div className="text-xl font-semibold leading-snug" style={{ textWrap: 'balance' } as CSSProperties}>
                      DoubleZero is first on{' '}
                      <span style={{ color: DZ_COLOR }}>
                        <CountUp to={data.all.win_pct} format={pct} />
                      </span>{' '}
                      of updates.
                    </div>
                    <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                      A typical update arrives{' '}
                      {data.all.p50_ms === 0
                        ? 'at the same time as on'
                        : `${absMs(data.all.p50_ms)} ${data.all.p50_ms < 0 ? 'later' : 'sooner'} than on`}{' '}
                      Phoenix's own feed. Both
                      are recorded on the same host in {data.site_label}.
                    </p>
                  </div>
                </div>
                <div className="flex min-w-0 flex-1 flex-col justify-center gap-2.5 border-t border-border p-4 sm:p-5 lg:border-l lg:border-t-0">
                  <div className="flex items-baseline justify-end gap-3">
                    <span className="font-mono text-xs text-muted-foreground">
                      median, after {data.all.p50_ms < 0 ? 'Phoenix' : 'DoubleZero'}
                    </span>
                  </div>
                  <ArrivalChart lagMs={data.all.p50_ms} />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-px border-t border-border sm:grid-cols-4" style={{ background: 'var(--border)' }}>
                <Stat
                  label="Updates raced"
                  delay={60}
                  title="Updates both recorders saw, where the later copy arrived within 5 seconds"
                >
                  <CountUp to={data.races} format={count} delay={250} pop />
                </Stat>
                <Stat label="DoubleZero first" delay={120}>
                  <span style={{ color: DZ_COLOR }}>
                    <CountUp to={data.dz_wins} format={count} delay={350} pop />
                  </span>
                </Stat>
                <Stat label="Venue first" delay={180}>
                  <CountUp to={data.venue_wins} format={count} delay={450} />{' '}
                  <span className="text-xs font-normal text-muted-foreground">{pct2((100 * data.venue_wins) / data.races)}</span>
                </Stat>
                <Stat label="Recording site" delay={240}>
                  {data.site_label}
                </Stat>
              </div>
            </div>

            <TrendCard data={data} />
          </>
        )}
      </div>
    </div>
  )
}
