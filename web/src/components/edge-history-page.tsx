import { Fragment, useMemo, useState } from 'react'
import type { PointerEvent } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { AlertCircle, ChevronDown, ChevronRight, History, Loader2 } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchEdgeHistory,
  type EdgeHistoryFeed,
  type EdgeHistoryRange,
  type EdgeHistoryRecorder,
  type EdgeHistoryResponse,
} from '@/lib/api'

// Edge History: what the multicast pcap warehouse holds for each feed — since when each
// recorder has captured it, how much, and where the capture has holes. One row per
// (feed, recorder host); a host rebuilt on a new address is one row, its addresses listed
// under it, and the rebuild shows as a gap.
//
// A gap is a stretch of at least min_gap_seconds with no packet between one pcap file and
// the next. Consecutive files of a healthy capture abut, and on the quietest feeds the hole
// between them stays under ten seconds, so anything the page calls a gap is a capture that
// was not running — or one whose file a restarted recorder overwrote in the bucket.

const RANGES: { key: EdgeHistoryRange; label: string }[] = [
  { key: '7d', label: '7 days' },
  { key: '30d', label: '30 days' },
  { key: '90d', label: '90 days' },
  { key: 'all', label: 'All' },
]

type ColorMode = 'coverage' | 'volume'

// Coverage states use the status palette, which reads in both themes and is never reused for
// a series; every state is also named in the legend and the tooltip, so colour never carries
// it alone. Captured is not "good" green: it is the page's normal state and should recede.
const CAPTURED = '#3987e5'
const PARTIAL = '#fab219'
const MISSING = '#d03b3b'

// One-hue sequential ramp for volume, lightest = least.
const VOLUME_RAMP = ['#cde2fb', '#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95', '#0d366b']

function coverageColor(c: number): string | undefined {
  if (c < 0) return undefined
  if (c >= 1) return CAPTURED
  if (c <= 0) return MISSING
  return PARTIAL
}

function coverageLabel(c: number): string {
  if (c < 0) return 'Not recording'
  if (c >= 1) return 'Captured'
  if (c <= 0) return 'Missing'
  return `Partial — ${(c * 100).toFixed(2)}% captured`
}

// Log scale against the row's busiest bucket: feeds on one page differ by three orders of
// magnitude, and within a row a linear scale would paint every quiet hour as empty.
function volumeColor(bytes: number, rowMax: number): string | undefined {
  if (bytes <= 0 || rowMax <= 0) return undefined
  const t = rowMax <= 1 ? 1 : Math.log(bytes) / Math.log(rowMax)
  const i = Math.min(VOLUME_RAMP.length - 1, Math.max(0, Math.round(t * (VOLUME_RAMP.length - 1))))
  return VOLUME_RAMP[i]
}

function formatBytes(n: number): string {
  if (n <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1000)))
  const v = n / 1000 ** i
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`
}

function formatDuration(secs: number): string {
  if (secs < 90) return `${Math.round(secs)}s`
  const m = secs / 60
  if (m < 90) return `${Math.round(m)}m`
  const h = m / 60
  if (h < 48) return `${h.toFixed(h < 10 ? 1 : 0)}h`
  const d = h / 24
  return `${d.toFixed(d < 10 ? 1 : 0)}d`
}

function formatCount(n: number): string {
  return n.toLocaleString('en-US')
}

function day(iso: string | number): string {
  return new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' })
}

function dayTime(iso: string | number): string {
  const d = new Date(iso)
  return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' })} ${d.toISOString().slice(11, 16)}`
}

function bucketLabel(startMs: number, bucketSecs: number): string {
  if (bucketSecs >= 86400) return `${day(startMs)} (UTC)`
  return `${dayTime(startMs)}–${new Date(startMs + bucketSecs * 1000).toISOString().slice(11, 16)} UTC`
}

function siteLabel(rec: EdgeHistoryRecorder): string {
  return rec.site.toUpperCase()
}

interface Axis {
  startMs: number
  bucketSecs: number
  n: number
}

// Month starts for day buckets, day starts for hour buckets — whichever the window spans
// fewer than a dozen or so of, thinned to fit.
function axisTicks(axis: Axis): { at: number; label: string }[] {
  const endMs = axis.startMs + axis.n * axis.bucketSecs * 1000
  const ticks: { at: number; label: string }[] = []
  const d = new Date(axis.startMs)
  if (axis.bucketSecs >= 86400) {
    let t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1)
    while (t < endMs) {
      const m = new Date(t)
      ticks.push({
        at: (t - axis.startMs) / (endMs - axis.startMs),
        label: m.toLocaleDateString('en-US', { month: 'short', timeZone: 'UTC', ...(m.getUTCMonth() === 0 ? { year: 'numeric' } : {}) }),
      })
      t = Date.UTC(m.getUTCFullYear(), m.getUTCMonth() + 1, 1)
    }
    if (ticks.length <= 2) {
      // A 30-day window crosses one month boundary at most; label weeks instead.
      ticks.length = 0
      for (let t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1); t < endMs; t += 7 * 86400_000) {
        ticks.push({ at: (t - axis.startMs) / (endMs - axis.startMs), label: day(t).replace(/, \d{4}$/, '') })
      }
    }
  } else {
    for (let t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1); t < endMs; t += 86400_000) {
      ticks.push({ at: (t - axis.startMs) / (endMs - axis.startMs), label: day(t).replace(/, \d{4}$/, '') })
    }
  }
  const maxTicks = 12
  if (ticks.length > maxTicks) {
    const step = Math.ceil(ticks.length / maxTicks)
    return ticks.filter((_, i) => i % step === 0)
  }
  return ticks
}

function AxisRow({ axis }: { axis: Axis }) {
  const ticks = useMemo(() => axisTicks(axis), [axis])
  return (
    <div className="relative h-4 text-[10px] text-muted-foreground select-none">
      {ticks.map((t) => (
        <span key={t.at} className="absolute -translate-x-1/2 whitespace-nowrap" style={{ left: `${t.at * 100}%` }}>
          {t.label}
        </span>
      ))}
    </div>
  )
}

interface Hover {
  x: number
  y: number
  lines: string[]
}

function Timeline({
  rec,
  axis,
  mode,
  onHover,
}: {
  rec: EdgeHistoryRecorder
  axis: Axis
  mode: ColorMode
  onHover: (h: Hover | null) => void
}) {
  const rowMax = useMemo(() => Math.max(0, ...rec.bucket_bytes), [rec.bucket_bytes])
  // Consecutive buckets of one colour are drawn as one rect: abutting rects anti-alias into
  // hairline stripes at fractional widths, which read as gaps that are not there.
  const runs = useMemo(() => {
    const out: { x: number; w: number; fill: string }[] = []
    rec.bucket_coverage.forEach((c, i) => {
      const fill = mode === 'coverage' ? coverageColor(c) : volumeColor(rec.bucket_bytes[i] ?? 0, rowMax)
      if (!fill) return
      const last = out[out.length - 1]
      if (last && last.fill === fill && last.x + last.w === i) last.w++
      else out.push({ x: i, w: 1, fill })
    })
    return out
  }, [rec.bucket_coverage, rec.bucket_bytes, mode, rowMax])

  const onMove = (e: PointerEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    const i = Math.floor(((e.clientX - box.left) / box.width) * axis.n)
    if (i < 0 || i >= axis.n) {
      onHover(null)
      return
    }
    const start = axis.startMs + i * axis.bucketSecs * 1000
    const c = rec.bucket_coverage[i] ?? -1
    const lines = [bucketLabel(start, axis.bucketSecs), coverageLabel(c)]
    if (c >= 0 || (rec.bucket_bytes[i] ?? 0) > 0) {
      lines.push(formatBytes(rec.bucket_bytes[i] ?? 0))
    }
    onHover({ x: e.clientX, y: box.top, lines })
  }

  return (
    <svg
      className="block w-full h-4 cursor-crosshair"
      viewBox={`0 0 ${axis.n} 1`}
      preserveAspectRatio="none"
      onPointerMove={onMove}
      onPointerLeave={() => onHover(null)}
      role="img"
      aria-label={`${rec.recorder} capture timeline`}
    >
      <rect x={0} y={0} width={axis.n} height={1} className="fill-muted" />
      {runs.map((r) => (
        <rect key={r.x} x={r.x} y={0} width={r.w} height={1} fill={r.fill} />
      ))}
    </svg>
  )
}

function Legend({ mode }: { mode: ColorMode }) {
  const swatch = (color: string | undefined, label: string) => (
    <span className="flex items-center gap-1.5">
      <span className={`inline-block h-3 w-3 rounded-sm ${color ? '' : 'bg-muted'}`} style={color ? { background: color } : undefined} />
      {label}
    </span>
  )
  if (mode === 'volume') {
    return (
      <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          less
          <span className="flex">
            {VOLUME_RAMP.map((c) => (
              <span key={c} className="inline-block h-3 w-3" style={{ background: c }} />
            ))}
          </span>
          more — bytes per bucket, log scale against each row's busiest
        </span>
        {swatch(undefined, 'Nothing stored')}
      </div>
    )
  }
  return (
    <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
      {swatch(CAPTURED, 'Captured')}
      {swatch(PARTIAL, 'Partial — a gap inside the bucket')}
      {swatch(MISSING, 'Missing — the whole bucket is a gap')}
      {swatch(undefined, 'Not recording — before the first or after the last capture')}
    </div>
  )
}

function Segmented<T extends string>({
  value,
  options,
  onChange,
}: {
  value: T
  options: { key: T; label: string }[]
  onChange: (v: T) => void
}) {
  return (
    <div className="inline-flex rounded-md border border-border p-0.5 text-sm">
      {options.map((o) => (
        <button
          key={o.key}
          type="button"
          onClick={() => onChange(o.key)}
          className={`px-2.5 py-1 rounded ${value === o.key ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}

function StatusCell({ rec, now }: { rec: EdgeHistoryRecorder; now: number }) {
  if (rec.live) {
    return (
      <span className="inline-flex items-center gap-1.5 text-xs">
        <span className="h-2 w-2 rounded-full" style={{ background: '#0ca30c' }} />
        recording
      </span>
    )
  }
  const stoppedFor = (now - new Date(rec.last_ts).getTime()) / 1000
  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground" title={`Last packet ${dayTime(rec.last_ts)} UTC`}>
      <span className="h-2 w-2 rounded-full bg-muted-foreground/50" />
      stopped {formatDuration(stoppedFor)} ago
    </span>
  )
}

function coverageTone(pct: number): string {
  if (pct >= 99.9) return ''
  if (pct >= 95) return 'text-amber-600 dark:text-amber-400'
  return 'text-red-600 dark:text-red-400'
}

function RecorderDetail({ rec, minGap }: { rec: EdgeHistoryRecorder; minGap: number }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 text-xs py-3">
      <div>
        <div className="font-medium mb-1.5">Instances</div>
        <table className="w-full">
          <thead className="text-muted-foreground">
            <tr>
              <th className="text-left font-normal pr-3">Public IP</th>
              <th className="text-left font-normal pr-3">From</th>
              <th className="text-left font-normal pr-3">To</th>
              <th className="text-right font-normal">Files</th>
            </tr>
          </thead>
          <tbody className="font-mono">
            {rec.instances.map((inst) => (
              <tr key={inst.ip}>
                <td className="pr-3">{inst.ip}</td>
                <td className="pr-3">{dayTime(inst.first_ts)}</td>
                <td className="pr-3">{dayTime(inst.last_ts)}</td>
                <td className="text-right">{formatCount(inst.files)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="mt-3 text-muted-foreground">
          {formatCount(rec.files)} files · {formatCount(rec.packets)} packets · {formatBytes(rec.bytes)}
          {rec.overwritten > 0 && (
            <div className="mt-1 text-amber-600 dark:text-amber-400">
              {formatCount(rec.overwritten)} file{rec.overwritten === 1 ? '' : 's'} overwritten in the bucket — a restarted
              recorder reused the key, so that capture is no longer stored.
            </div>
          )}
        </div>
      </div>
      <div>
        <div className="font-medium mb-1.5">
          Gaps of {formatDuration(minGap)} or more
          {rec.gap_count > rec.gaps.length && (
            <span className="font-normal text-muted-foreground"> — longest {rec.gaps.length} of {formatCount(rec.gap_count)}</span>
          )}
        </div>
        {rec.gaps.length === 0 ? (
          <div className="text-muted-foreground">None in this window.</div>
        ) : (
          <div className="max-h-56 overflow-auto">
            <table className="w-full">
              <thead className="text-muted-foreground sticky top-0 bg-background">
                <tr>
                  <th className="text-left font-normal pr-3">From (UTC)</th>
                  <th className="text-left font-normal pr-3">To</th>
                  <th className="text-right font-normal">Length</th>
                </tr>
              </thead>
              <tbody className="font-mono">
                {rec.gaps.map((g) => (
                  <tr key={g.start}>
                    <td className="pr-3">{dayTime(g.start)}</td>
                    <td className="pr-3">{dayTime(g.end)}</td>
                    <td className="text-right">{formatDuration(g.seconds)}</td>
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

const GRID = 'grid grid-cols-[7rem_6.5rem_7.5rem_5rem_6rem_4.5rem_minmax(16rem,1fr)] gap-x-3 items-center'

function FeedSection({
  feed,
  axis,
  mode,
  now,
  minGap,
  expanded,
  onToggle,
  onHover,
}: {
  feed: EdgeHistoryFeed
  axis: Axis
  mode: ColorMode
  now: number
  minGap: number
  expanded: Set<string>
  onToggle: (key: string) => void
  onHover: (h: Hover | null) => void
}) {
  return (
    <div className="border border-border rounded-lg mb-4">
      <div className={`${GRID} px-3 pt-3 pb-1`}>
        <div className="col-span-6 flex flex-wrap items-baseline gap-x-3 gap-y-0.5 min-w-0">
          <span className="font-medium truncate">{feed.code || feed.multicast_group}</span>
          {feed.code && <span className="font-mono text-xs text-muted-foreground">{feed.multicast_group}</span>}
          <span className="text-xs text-muted-foreground">
            since {day(feed.first_ts)} · {formatBytes(feed.bytes)} · {formatCount(feed.files)} files
          </span>
        </div>
        <AxisRow axis={axis} />
      </div>
      <div className={`${GRID} px-3 pb-1 text-[11px] uppercase tracking-wide text-muted-foreground`}>
        <span>Recorder</span>
        <span>Since</span>
        <span>Status</span>
        <span className="text-right">Coverage</span>
        <span className="text-right">Gaps</span>
        <span className="text-right">Size</span>
        <span />
      </div>
      {feed.recorders.map((rec) => {
        const key = `${feed.multicast_group}|${rec.recorder}`
        const open = expanded.has(key)
        return (
          <Fragment key={key}>
            <button
              type="button"
              onClick={() => onToggle(key)}
              className={`${GRID} w-full text-left px-3 py-1.5 text-sm border-t border-border hover:bg-muted/40`}
              aria-expanded={open}
            >
              <span className="flex items-center gap-1 min-w-0" title={rec.recorder}>
                {open ? <ChevronDown className="h-3.5 w-3.5 shrink-0" /> : <ChevronRight className="h-3.5 w-3.5 shrink-0" />}
                <span className="font-medium">{siteLabel(rec)}</span>
                {rec.instances.length > 1 && (
                  <span className="text-[10px] text-muted-foreground" title="Rebuilt on a new public IP">
                    ×{rec.instances.length}
                  </span>
                )}
              </span>
              <span className="text-xs text-muted-foreground">{day(rec.first_ts)}</span>
              <StatusCell rec={rec} now={now} />
              <span className={`text-right tabular-nums ${coverageTone(rec.coverage_pct)}`}>
                {/* Truncated, not rounded: 99.95% must not print as 100. */}
                {(Math.floor(rec.coverage_pct * 100) / 100).toFixed(2)}%
              </span>
              <span className="text-right tabular-nums text-xs">
                {rec.gap_count === 0 ? (
                  <span className="text-muted-foreground">none</span>
                ) : (
                  <span title={`${formatCount(rec.gap_count)} gaps, ${formatDuration(rec.gap_seconds)} in total`}>
                    {formatCount(rec.gap_count)} · {formatDuration(rec.gap_seconds)}
                  </span>
                )}
              </span>
              <span className="text-right tabular-nums text-xs">
                {formatBytes(rec.bytes)}
                {rec.overwritten > 0 && (
                  <span className="text-amber-600 dark:text-amber-400" title={`${rec.overwritten} files overwritten in the bucket`}>
                    {' '}
                    ⚠
                  </span>
                )}
              </span>
              <Timeline rec={rec} axis={axis} mode={mode} onHover={onHover} />
            </button>
            {open && (
              <div className="px-3 border-t border-border bg-muted/20">
                <RecorderDetail rec={rec} minGap={minGap} />
              </div>
            )}
          </Fragment>
        )
      })}
    </div>
  )
}

function Summary({ data }: { data: EdgeHistoryResponse }) {
  const recorders = new Set(data.feeds.flatMap((f) => f.recorders.map((r) => r.recorder))).size
  const stat = (label: string, value: string) => (
    <div>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg tabular-nums">{value}</div>
    </div>
  )
  return (
    <div className="flex flex-wrap gap-x-8 gap-y-3 mb-5">
      {stat('Stored', formatBytes(data.bytes))}
      {stat('Files', formatCount(data.files))}
      {stat('Feeds', formatCount(data.feeds.length))}
      {stat('Recorders', formatCount(recorders))}
      {stat('Earliest capture', data.feeds.length ? day(Math.min(...data.feeds.map((f) => new Date(f.first_ts).getTime()))) : '—')}
      {data.overwritten > 0 && stat('Overwritten files', formatCount(data.overwritten))}
    </div>
  )
}

export function EdgeHistoryPage() {
  const [range, setRange] = useState<EdgeHistoryRange>('all')
  const [mode, setMode] = useState<ColorMode>('coverage')
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const [hover, setHover] = useState<Hover | null>(null)

  const { data, isLoading, error, isFetching } = useQuery({
    queryKey: ['edge-history', range],
    queryFn: () => fetchEdgeHistory(range),
    refetchInterval: 5 * 60_000,
    placeholderData: keepPreviousData,
  })

  // Judged against when the payload was computed, not the wall clock; only read once data exists.
  const now = data?.window_end ? new Date(data.window_end).getTime() : 0
  const axis = useMemo<Axis | null>(() => {
    if (!data?.window_start || !data.bucket_seconds) return null
    const n = data.feeds[0]?.recorders[0]?.bucket_coverage.length ?? 0
    return n > 0 ? { startMs: new Date(data.window_start).getTime(), bucketSecs: data.bucket_seconds, n } : null
  }, [data])

  const toggle = (key: string) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load capture history</div>
          <div className="text-sm text-muted-foreground">{error?.message || 'Unknown error'}</div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={History}
          title="Edge History"
          subtitle={
            <span className="text-sm text-muted-foreground">
              Multicast captures stored in the pcap warehouse
              {data.as_of && <> · indexed {dayTime(data.as_of)} UTC</>}
              {isFetching && <Loader2 className="inline h-3.5 w-3.5 ml-2 animate-spin" />}
            </span>
          }
          actions={
            <>
              <Segmented value={mode} options={[{ key: 'coverage', label: 'Coverage' }, { key: 'volume', label: 'Volume' }]} onChange={setMode} />
              <Segmented value={range} options={RANGES} onChange={setRange} />
            </>
          }
        />

        {data.feeds.length === 0 || !axis ? (
          <div className="text-sm text-muted-foreground border border-border rounded-lg p-8 text-center">
            No captures indexed{range === 'all' ? '' : ' in this window'}.
          </div>
        ) : (
          <>
            <Summary data={data} />
            <div className="mb-3">
              <Legend mode={mode} />
            </div>
            <div className="overflow-x-auto">
              <div className="min-w-[56rem]">
                {data.feeds.map((feed) => (
                  <FeedSection
                    key={feed.multicast_group}
                    feed={feed}
                    axis={axis}
                    mode={mode}
                    now={now}
                    minGap={data.min_gap_seconds}
                    expanded={expanded}
                    onToggle={toggle}
                    onHover={setHover}
                  />
                ))}
              </div>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              A gap is {formatDuration(data.min_gap_seconds)} or more with no packet between one capture file and the next.
              Coverage is the share of each recorder&apos;s span in this window, from its first packet to its last (or now,
              while recording), not lost to a gap. Times are UTC.
            </p>
          </>
        )}
      </div>
      {hover && (
        <div
          className="pointer-events-none fixed z-50 rounded-md border border-border bg-popover px-2 py-1.5 text-xs shadow-md"
          style={{ left: hover.x + 12, top: hover.y - 8, transform: 'translateY(-100%)' }}
        >
          {hover.lines.map((l, i) => (
            <div key={i} className={i === 0 ? 'font-medium' : 'text-muted-foreground'}>
              {l}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
