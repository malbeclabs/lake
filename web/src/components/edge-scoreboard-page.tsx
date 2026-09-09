import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams, Link } from 'react-router-dom'
import { Trophy, Loader2, ArrowRight } from 'lucide-react'

import {
  fetchEdgeScoreboard,
  type EdgeScoreboardNode,
} from '@/lib/api'
import { cn } from '@/lib/utils'
import { FEED_COLORS } from '@/lib/feed-colors'
import { Tooltip } from '@/components/ui/tooltip'
import { PageHeader } from './page-header'
import { edgeWinRateVsTurbine } from './edge-head-to-head'
import { formatDay } from './shreds-competitor-day'
import { ShredsCompetitorChart, useShredsCompetitors } from './shreds-competitor-chart'
import { ShredsNowLeading } from './shreds-now-leading'

function useAnimatedNumber(target: number | undefined, duration = 500) {
  const [current, setCurrent] = useState<number | undefined>(undefined)
  const prevRef = useRef<number | undefined>(undefined)
  useEffect(() => {
    if (target === undefined) return
    const start = prevRef.current ?? target
    const startTime = performance.now()
    const animate = (time: number) => {
      const elapsed = time - startTime
      const progress = Math.min(elapsed / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3)
      setCurrent(start + (target - start) * eased)
      if (progress < 1) requestAnimationFrame(animate)
      else prevRef.current = target
    }
    requestAnimationFrame(animate)
  }, [target, duration])
  return current
}

const VALID_WINDOWS = ['1h', '24h', '3d', '7d'] as const
type TimeWindow = (typeof VALID_WINDOWS)[number]

function isValidWindow(v: string | null): v is TimeWindow {
  return v !== null && (VALID_WINDOWS as readonly string[]).includes(v)
}

function formatPct(v: number): string {
  return v >= 100 ? '100%' : `${v.toFixed(1)}%`
}

function formatMs(v: number): string {
  if (v < 0.1) return '<0.1ms'
  if (v >= 1000) return `${(v / 1000).toFixed(1)}s`
  return `${v.toFixed(1)}ms`
}


function windowLabel(w: TimeWindow): string {
  const labels: Record<TimeWindow, string> = {
    '1h': 'past 1 hour',
    '24h': 'past 24 hours',
    '3d': 'past 3 days',
    '7d': 'past 7 days',
  }
  return labels[w] ?? w
}

function formatStake(sol: number): string {
  if (sol >= 1_000_000) return `${(sol / 1_000_000).toFixed(1)}M SOL`
  if (sol >= 1_000) return `${(sol / 1_000).toFixed(0)}K SOL`
  return `${sol.toFixed(0)} SOL`
}




// AnimatedStat renders an animated numeric value using a format function.
// Defined as a component (not inline) so it can be used inside loops.
function AnimatedStat({ value, fmt }: { value: number; fmt: (v: number) => string }) {
  const animated = useAnimatedNumber(value) ?? value
  return <>{fmt(animated)}</>
}

function HeadlineTile({
  label,
  swatch,
  value,
  detail,
}: {
  label: string
  swatch?: string
  value: React.ReactNode
  detail: string
}) {
  return (
    <div className="bg-card px-4 py-4 sm:px-5 sm:py-5 flex flex-col gap-1 min-w-0 transition-colors hover:bg-muted/30">
      {/* Wraps rather than truncates, and every tile reserves both lines so the
          figures stay on one baseline across the row. */}
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground flex items-start gap-1.5 min-h-[26px]">
        {swatch && <span className="inline-block w-2 h-2 rounded-sm shrink-0 mt-[3px]" style={{ backgroundColor: swatch }} />}
        <span>{label}</span>
      </div>
      <div className="text-2xl sm:text-[27px] font-semibold tabular-nums leading-tight tracking-tight">{value}</div>
      <div className="text-xs text-muted-foreground">{detail}</div>
    </div>
  )
}

const FEED_LABELS: Record<string, string> = {
  dz_edge: 'DZ Edge',
  dz_root: 'DZ Edge turbine-root',
  dz: 'DZ Edge Leaders',
  dz_retransmit: 'DZ Edge Retransmits',
  jito: 'Jito Shredstream',
  turbine: 'Turbine',
  pipe: 'Pipe',
  other: 'Other',
}

type FeedSegment = { key: string; pct: number; rawPct: number; color: string }

function StackedBar({ segments, children, popoverSide = 'top', dzTotalPct }: { segments: FeedSegment[]; children?: React.ReactNode; popoverSide?: 'top' | 'bottom' | 'right'; dzTotalPct?: number }) {
  const [hover, setHover] = useState(false)
  const popoverClass = popoverSide === 'right'
    ? 'left-full top-1/2 -translate-y-1/2 ml-2'
    : popoverSide === 'bottom'
    ? 'top-full left-0 mt-2'
    : 'bottom-full left-0 mb-2'
  return (
    <div className="relative" onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)}>
      {children}
      <div className="h-1 rounded-full bg-muted-foreground/25 overflow-hidden">
        <div className="flex h-full">
          {segments.map(({ key, pct, color }) => (
            <div key={key} className="h-full transition-all duration-500" style={{ width: `${pct}%`, backgroundColor: color }} />
          ))}
        </div>
      </div>
      {hover && segments.length > 0 && (() => {
        const dzSegs = segments.filter(s => DZ_FEED_KEYS.has(s.key))
        const otherSegs = segments.filter(s => !DZ_FEED_KEYS.has(s.key))
        const dzBySubKey = new Map(dzSegs.map(s => [s.key, s]))
        // Granular-DZ mode: we're breaking DZ into Leaders/Root/Retransmits (delivery order).
        // Always show all three, synthesizing a 0% entry for whichever is missing so the layout
        // stays stable across rows (e.g. a node with no retransmit feed still shows "Retransmits 0.00%").
        const hasGranularDz = dzBySubKey.has('dz_root') || dzBySubKey.has('dz') || dzBySubKey.has('dz_retransmit')
        const groupDz = hasGranularDz || dzSegs.length > 1
        const dzDisplaySegs: FeedSegment[] = hasGranularDz
          ? [
              dzBySubKey.get('dz') ?? { key: 'dz', pct: 0, rawPct: 0, color: FEED_COLORS.dz },
              dzBySubKey.get('dz_root') ?? { key: 'dz_root', pct: 0, rawPct: 0, color: FEED_COLORS.dz_root },
              dzBySubKey.get('dz_retransmit') ?? { key: 'dz_retransmit', pct: 0, rawPct: 0, color: FEED_COLORS.dz_retransmit },
            ]
          : dzSegs
        const dzSubLabels: Record<string, string> = { dz_edge: 'Edge', dz_root: 'turbine-root', dz: 'Leaders', dz_retransmit: 'Retransmits' }
        const flatSegs = groupDz ? otherSegs : segments
        return (
          <div className={cn('absolute z-30 bg-popover border border-border rounded-lg shadow-lg px-3 py-2 text-xs whitespace-nowrap', popoverClass)}>
            {groupDz && (
              <>
                <div className="flex items-center gap-2 py-0.5 font-medium">
                  <span>DZ Edge</span>
                  <span className="ml-auto pl-4 tabular-nums">{(dzTotalPct ?? dzSegs.reduce((s, seg) => s + seg.rawPct, 0)).toFixed(1)}%</span>
                </div>
                {dzDisplaySegs.map(({ key, rawPct, color }) => (
                  <div key={key} className="flex items-center gap-2 py-0.5 pl-3">
                    <div className="w-1.5 h-1.5 rounded-full shrink-0" style={{ backgroundColor: color }} />
                    <span className="text-muted-foreground">{dzSubLabels[key] ?? key}</span>
                    <span className="ml-auto pl-4 tabular-nums">{rawPct.toFixed(key === 'dz_retransmit' ? 2 : 1)}%</span>
                  </div>
                ))}
                {otherSegs.length > 0 && <div className="border-t border-border my-1.5" />}
              </>
            )}
            {flatSegs.map(({ key, rawPct, color }) => (
              <div key={key} className="flex items-center gap-2 py-0.5">
                <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: color }} />
                <span className="text-muted-foreground">{FEED_LABELS[key] ?? key}</span>
                <span className="ml-auto pl-4 tabular-nums font-medium">{rawPct.toFixed(1)}%</span>
              </div>
            ))}
          </div>
        )
      })()}
    </div>
  )
}

// Feeds considered "DZ" for simplified view grouping.
const DZ_FEED_KEYS = new Set(['dz_edge', 'dz_root', 'dz', 'dz_retransmit'])

// Map a raw feed name to the key used in the chart/bar data.
// The API returns 'dz_edge' (server-computed aggregate of dz + retransmit), 'dz' (Leaders),
// and 'dz_retransmit' (pre-aggregated regional retransmit feeds).
// Simplified mode: all DZ feeds → 'dz_edge'.
// Granular mode: skip 'dz_edge' (redundant with components); show 'dz' and 'dz_retransmit'.
function feedKeyForMode(feed: string, granular: boolean): string | null {
  if (granular) {
    if (feed === 'dz_edge') return null  // skip aggregate — components shown instead
    return feed in FEED_COLORS ? feed : null
  }
  if (DZ_FEED_KEYS.has(feed)) return 'dz_edge'
  if (feed in FEED_COLORS) return 'other'
  return null
}

// Priority for feed ordering in chart (lower = rendered first / bottom of stack).
function feedSortPriority(f: string): number {
  if (f === 'dz_edge') return 0
  if (f === 'dz') return 1
  if (f === 'dz_root') return 1.5
  if (f === 'dz_retransmit') return 2
  if (f === 'jito') return 5
  if (f === 'turbine') return 6
  if (f === 'pipe') return 7
  if (f === 'other') return 10
  return 8
}


function NodePopover({ node }: { node: EdgeScoreboardNode }) {
  const hasGossip = !!node.gossip_pubkey
  return (
    <div className="bg-popover border border-border rounded-lg shadow-xl text-xs whitespace-nowrap text-left text-foreground min-w-[160px] overflow-hidden">
      {node.metro_name && (
        <div className="px-3 py-2 font-medium text-foreground border-b border-border bg-muted/40">{node.metro_name}</div>
      )}
      <div className="px-3 py-2 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5">
        <span className="text-muted-foreground">Host</span>
        <span className="font-mono">{node.host}</span>
        {node.gossip_ip && <>
          <span className="text-muted-foreground">IP</span>
          <span className="font-mono">{node.gossip_ip}</span>
        </>}
        {node.asn_org && <>
          <span className="text-muted-foreground">Org</span>
          <span>{node.asn_org}</span>
        </>}
        {node.asn != null && node.asn > 0 && <>
          <span className="text-muted-foreground">ASN</span>
          <span>AS{node.asn}</span>
        </>}
        {node.city && <>
          <span className="text-muted-foreground">Location</span>
          <span>{node.city}{node.country ? `, ${node.country}` : ''}</span>
        </>}
        {hasGossip && <>
          <span className="text-muted-foreground">Pubkey</span>
          <span className="font-mono">{node.gossip_pubkey!.slice(0, 8)}…{node.gossip_pubkey!.slice(-4)}</span>
        </>}
      </div>
    </div>
  )
}

// nodeDisplayLabel returns a disambiguated label for a node. When multiple nodes
// share the same metro location (e.g. "ams-mn-bm1" and "ams-mn-bm2" both map to "AMS"),
// appends the trailing index from the host name so the UI shows "AMS-1" / "AMS-2".
function nodeDisplayLabel(node: EdgeScoreboardNode, nodes: EdgeScoreboardNode[]): string {
  const hasDuplicate = nodes.some(n => n.host !== node.host && n.location === node.location)
  if (!hasDuplicate) return node.location
  const suffix = node.host.split('-').pop()?.match(/\d+$/)?.[0]
  return suffix ? `${node.location}-${suffix}` : node.host
}





// liveEdge=0 means "no anchor" — uses the buffer's newest slot (non-live mode).


export function EdgeScoreboardPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const rawWindow = searchParams.get('window')
  const activeWindow: TimeWindow = isValidWindow(rawWindow) ? rawWindow : '24h'

  const leadersOnly = searchParams.get('leaders_only') !== 'false'

  // Still read by the node table's per-feed breakdown.
  const granular = searchParams.get('granular') === '1'

  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 5_000)
    return () => clearInterval(id)
  }, [])

  const [showLoader, setShowLoader] = useState(false)
  const [showShimmer, setShowShimmer] = useState(false)

  const showTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['edge-scoreboard', activeWindow, leadersOnly],
    queryFn: () => fetchEdgeScoreboard(activeWindow, leadersOnly),
    refetchInterval: 30_000,
    staleTime: 15_000,
    placeholderData: keepPreviousData,
  })

  // Derive recent slots snapshot synchronously from `data` so both charts update in the same
  // render cycle. Using state+useEffect caused a one-render lag; keepPreviousData already
  // handles the transition — data never goes blank — so the generated_at guard is not needed.
  const stableRecent = useMemo(() => {
    if (!data?.recent_slots?.length) return null
    return { slots: data.recent_slots, leaders: data.slot_leaders }
  }, [data])

  const freshness = useMemo(() => {
    if (!data?.generated_at) return null
    const ageSec = Math.round((now - new Date(data.generated_at).getTime()) / 1000)
    if (ageSec < 5) return 'just now'
    if (ageSec < 60) return `${ageSec}s ago`
    return `${Math.round(ageSec / 60)}m ago`
  }, [data?.generated_at, now])

  const setLeadersOnly = (v: boolean) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (!v) p.set('leaders_only', 'false')
      else p.delete('leaders_only')
      return p
    })
  }

  const vsTurbine = useMemo(
    () => (data?.nodes?.length ? edgeWinRateVsTurbine(data.nodes) : null),
    [data?.nodes]
  )

  // The commercial-feed matchup exists only in the daily rollup — the live
  // payload has carried no commercial feed since Jito was retired.
  const { data: competitorDays } = useShredsCompetitors()
  const latestDay = competitorDays?.length ? competitorDays[competitorDays.length - 1] : undefined

  // Sort nodes by stake weight descending
  const sortedNodes = useMemo(() => {
    if (!data?.nodes) return []
    return [...data.nodes].sort((a, b) => a.host.localeCompare(b.host))
  }, [data?.nodes])

  useEffect(() => {
    if (!isLoading) {
      setShowLoader(false)
      return
    }
    const t = setTimeout(() => setShowLoader(true), 200)
    return () => clearTimeout(t)
  }, [isLoading])

  // Show shimmer while fetching, debounced 200ms so instant cache hits skip it entirely.
  // Shimmer stays on until isFetching clears — no fixed duration.
  useEffect(() => {
    if (!isFetching) {
      if (showTimerRef.current) { clearTimeout(showTimerRef.current); showTimerRef.current = null }
      setShowShimmer(false)
      return
    }
    showTimerRef.current = setTimeout(() => {
      showTimerRef.current = null
      setShowShimmer(true)
    }, 200)
    return () => {
      if (showTimerRef.current) { clearTimeout(showTimerRef.current); showTimerRef.current = null }
    }
  }, [isFetching])

  const animPublishingCount = useAnimatedNumber(data?.publishing_count)
  const animPublishingStakePct = useAnimatedNumber(data?.publishing_stake_pct)
  const animVsCommercial = useAnimatedNumber(latestDay?.win_typical_pct)
  const animVsTurbine = useAnimatedNumber(vsTurbine ?? undefined)

  if (isLoading && showLoader && !data) return (
    <div className="flex-1 flex items-center justify-center bg-background">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
    </div>
  )

  if (error) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-500 mb-2">Failed to load edge scoreboard</div>
          <div className="text-sm text-muted-foreground">
            {error instanceof Error ? error.message : 'Unknown error'}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={Trophy}
          title="Shreds Scoreboard"
          subtitle={
            <span className="text-xs text-muted-foreground/50 flex items-center gap-2">
              <span>{windowLabel(activeWindow)}</span>
              {freshness && <span>· updated {freshness}</span>}
            </span>
          }
          actions={
            <div className="flex flex-wrap items-center gap-3 sm:gap-4">
              <div className="flex flex-wrap items-center gap-1.5 text-xs">
                {([
                  [false, 'All Slots', 'Shred arrival rates across all observed slots.'] as const,
                  [true, 'DZ Edge Leaders', 'Scoped to slots where the scheduled leader is publishing shreds via DZ Edge.'] as const,
                ]).map(([v, label, tooltip]) => (
                  <Tooltip key={String(v)} content={tooltip}>
                    <button
                      type="button"
                      onClick={() => setLeadersOnly(v)}
                      className={cn(
                        'px-2.5 py-1 rounded-md border transition-colors',
                        leadersOnly === v
                          ? 'border-emerald-500/60 bg-emerald-500/10 text-emerald-400'
                          : 'border-border text-muted-foreground hover:bg-muted hover:text-foreground'
                      )}
                    >
                      {label}
                    </button>
                  </Tooltip>
                ))}
              </div>
              <a
                href="https://docs.malbeclabs.com/Edge%20Subscriber%20Connection/"
                target="_blank"
                rel="noopener noreferrer"
                className="group inline-flex items-center gap-2 rounded-md bg-emerald-500 px-4 py-2 text-sm font-medium text-white shadow-[0_0_0_1px_rgba(16,185,129,0.5),0_4px_14px_-2px_rgba(16,185,129,0.45)] transition-all hover:bg-emerald-600 hover:shadow-[0_0_0_1px_rgba(16,185,129,0.6),0_6px_20px_-2px_rgba(16,185,129,0.6)]"
              >
                Subscribe Now
                <ArrowRight size={14} className="transition-transform group-hover:translate-x-0.5" />
              </a>
            </div>
          }
        />

        {/* Loading shimmer */}
        <div className="h-0.5 w-full overflow-hidden rounded-full mb-4">
          {showShimmer && (
            <div className="h-full w-1/3 bg-muted-foreground/40 animate-[shimmer_1.5s_ease-in-out_infinite] rounded-full" />
          )}
        </div>


        {data && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-px mb-8 bg-border border border-border rounded-lg overflow-hidden">
            <HeadlineTile
              label="Win rate vs commercial feeds"
              swatch={FEED_COLORS.dz_edge}
              value={latestDay ? formatPct(animVsCommercial ?? latestDay.win_typical_pct) : '—'}
              detail={latestDay ? `${formatDay(latestDay.day)} · median per leader slot` : 'no completed days yet'}
            />
            <HeadlineTile
              label="Win rate vs Turbine"
              swatch={FEED_COLORS.turbine}
              value={vsTurbine === null ? '—' : formatPct(animVsTurbine ?? vsTurbine)}
              detail={`${windowLabel(activeWindow)} · share of first arrivals`}
            />
            <HeadlineTile
              label="Validators publishing shreds"
              value={
                <>
                  {Math.round(animPublishingCount ?? data.publishing_count).toLocaleString()}
                  <span className="text-base font-medium text-muted-foreground"> / {data.publisher_count.toLocaleString()}</span>
                </>
              }
              detail={
                data.publisher_count > 0
                  ? `${formatPct((data.publishing_count / data.publisher_count) * 100)} of registered publishers`
                  : 'no registered publishers'
              }
            />
            <HeadlineTile
              label="Stake publishing shreds"
              value={formatPct(animPublishingStakePct ?? data.publishing_stake_pct)}
              detail="of network stake"
            />
          </div>
        )}

        {/* Daily competitor win rate — its own payload and its own cadence (one
            point per closed UTC day), so it neither waits on nor blocks the
            live-tailing charts above. */}
        <ShredsCompetitorChart />

        <ShredsNowLeading slots={stableRecent?.slots ?? []} leaders={stableRecent?.leaders} />

        {/* Node detail table */}
        <div className="border border-border rounded-lg overflow-hidden bg-card mb-6">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="text-sm text-left text-muted-foreground border-b border-border">
                  <th className="px-3 sm:px-4 py-3 font-medium whitespace-nowrap">Node</th>
                  <th className="px-3 sm:px-4 py-3 font-medium text-right whitespace-nowrap">DZ Edge Win Rate %</th>
                  <th className="px-3 sm:px-4 py-3 font-medium text-right whitespace-nowrap">vs Jito Shredstream<span className="block font-normal text-xs">p50 (p95)</span></th>
                  <th className="px-3 sm:px-4 py-3 font-medium text-right whitespace-nowrap">vs Turbine<span className="block font-normal text-xs">p50 (p95)</span></th>
                </tr>
              </thead>
              <tbody>
                {sortedNodes.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-12 text-center text-muted-foreground">
                      No data available for the selected time window.
                    </td>
                  </tr>
                ) : (
                  sortedNodes.map((node) => (
                    <NodeRow key={node.host} node={node} label={nodeDisplayLabel(node, data?.nodes ?? [])} granular={granular} />
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>


      </div>
    </div>
  )
}

function NodeRow({ node, label, granular }: { node: EdgeScoreboardNode; label: string; granular: boolean }) {
  const [fixedPos, setFixedPos] = useState<{ top: number; left: number } | null>(null)
  const cellRef = useRef<HTMLDivElement>(null)
  const dz = node.feeds['dz']
  const dzEdge = node.feeds['dz_edge']
  const edgeFirstArrival = dzEdge?.win_rate_pct ?? 0

  // Build lead time lookup: loser_feed -> { p50, p95 }.
  // Prefer dz_edge (dz + retransmit combined, matches the win-rate framing);
  // fall back to dz-only for older API responses.
  const dzLeadByFeed: Record<string, { p50: number; p95: number }> = {}
  const leadSource = dzEdge?.lead_times?.length ? dzEdge.lead_times : dz?.lead_times
  if (leadSource) {
    for (const lt of leadSource) {
      dzLeadByFeed[lt.loser_feed] = { p50: lt.p50_ms, p95: lt.p95_ms }
    }
  }

  // Per-feed-key segments for the stacked bar.
  // `rawPct` = server-provided win_rate_pct on the shared per-host denominator.
  // `pct` = visual width, normalized so the bar always fills 100%.
  const feedBarSegments = useMemo(() => {
    const accumulated: Record<string, number> = {}
    const hasDzEdge = 'dz_edge' in node.feeds
    for (const [feedName, stats] of Object.entries(node.feeds)) {
      if (!granular && hasDzEdge && (feedName !== 'dz_edge' && DZ_FEED_KEYS.has(feedName))) continue
      const key = feedKeyForMode(feedName, granular)
      if (!key) continue
      accumulated[key] = (accumulated[key] ?? 0) + stats.win_rate_pct
    }
    const total = Object.values(accumulated).reduce((s, v) => s + v, 0)
    const scale = total > 0 ? 100 / total : 1
    return Object.entries(accumulated)
      .sort(([a], [b]) => feedSortPriority(a) - feedSortPriority(b))
      .map(([key, pct]) => ({ key, pct: pct * scale, rawPct: pct, color: FEED_COLORS[key] ?? '#6b7280' }))
  }, [node.feeds, granular])

  const hasGossip = !!node.gossip_pubkey

  return (
    <tr className="border-b border-border last:border-b-0 hover:bg-muted/50 transition-colors">
      <td className="px-3 sm:px-4 py-3">
        <div ref={cellRef} className="relative" onMouseEnter={() => {
          if (cellRef.current) {
            const r = cellRef.current.getBoundingClientRect()
            setFixedPos({ top: r.top + r.height / 2, left: r.right + 8 })
          }
        }} onMouseLeave={() => setFixedPos(null)}>
          {hasGossip ? (
            <Link to={`/solana/gossip-nodes/${node.gossip_pubkey}`} state={{ back: { to: '/dz/shreds/scoreboard', label: 'Shreds Scoreboard' } }} className="text-sm font-medium hover:text-[#10b981] transition-colors">
              {label}
            </Link>
          ) : (
            <div className="text-sm font-medium">{label}</div>
          )}
          <div className="text-xs text-muted-foreground">{node.metro_name}</div>
          {node.stake_sol > 0 && <div className="text-xs text-muted-foreground">{formatStake(node.stake_sol)} staked</div>}
          {fixedPos && (
            <div style={{ position: 'fixed', top: fixedPos.top, left: fixedPos.left, transform: 'translateY(-50%)', zIndex: 50 }}>
              <NodePopover node={node} />
            </div>
          )}
        </div>
      </td>
      <td className="px-3 sm:px-4 py-3 text-right tabular-nums text-sm">
        {dz ? (
          <StackedBar segments={feedBarSegments} popoverSide="right" dzTotalPct={edgeFirstArrival}>
            <div className="mb-1.5">{formatPct(edgeFirstArrival)}</div>
          </StackedBar>
        ) : '—'}
      </td>
      {['jito', 'turbine'].map(f => {
        const lt = dzLeadByFeed[f]
        return (
          <td key={f} className="px-3 sm:px-4 py-3 text-right tabular-nums text-sm whitespace-nowrap">
            {lt ? <><AnimatedStat value={lt.p50} fmt={formatMs} /> <span className="text-muted-foreground">(<AnimatedStat value={lt.p95} fmt={formatMs} />)</span></> : '—'}
          </td>
        )
      })}
    </tr>
  )
}
