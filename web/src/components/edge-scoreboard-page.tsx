import { useState, useMemo, useRef, useEffect } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { Trophy, Loader2, ArrowRight } from 'lucide-react'

import {
  fetchEdgeScoreboard,
} from '@/lib/api'
import { FEED_COLORS } from '@/lib/feed-colors'
import { PageHeader } from './page-header'
import { edgeWinRateVsTurbine } from './edge-head-to-head'
import { formatDay } from './shreds-competitor-day'
import { ShredsCompetitorChart } from './shreds-competitor-chart'
import { useShredsCompetitors } from './use-shreds-competitors'
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



function windowLabel(w: TimeWindow): string {
  const labels: Record<TimeWindow, string> = {
    '1h': 'past 1 hour',
    '24h': 'past 24 hours',
    '3d': 'past 3 days',
    '7d': 'past 7 days',
  }
  return labels[w] ?? w
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













// liveEdge=0 means "no anchor" — uses the buffer's newest slot (non-live mode).


export function EdgeScoreboardPage() {
  const [searchParams] = useSearchParams()

  const rawWindow = searchParams.get('window')
  const activeWindow: TimeWindow = isValidWindow(rawWindow) ? rawWindow : '24h'

  const LEADERS_ONLY = true


  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 5_000)
    return () => clearInterval(id)
  }, [])

  const [showLoader, setShowLoader] = useState(false)
  const [showShimmer, setShowShimmer] = useState(false)

  const showTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const { data, isLoading, isFetching, error } = useQuery({
    queryKey: ['edge-scoreboard', activeWindow, LEADERS_ONLY],
    queryFn: () => fetchEdgeScoreboard(activeWindow, LEADERS_ONLY),
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


  const vsTurbine = useMemo(
    () => (data?.nodes?.length ? edgeWinRateVsTurbine(data.nodes) : null),
    [data?.nodes]
  )

  // The commercial-feed matchup exists only in the daily rollup — the live
  // payload has carried no commercial feed since Jito was retired.
  const { data: competitorDays } = useShredsCompetitors()
  const latestDay = competitorDays?.length ? competitorDays[competitorDays.length - 1] : undefined

  const measurement = useMemo(() => {
    if (!data?.nodes?.length) return null
    const metros = [...new Set(data.nodes.map((n) => n.metro_name).filter(Boolean))].sort()
    return { nodes: data.nodes.length, metros }
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
              detail={`${windowLabel(activeWindow)} · first arrivals in leader slots`}
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

        {/* What the Recording Nodes table was really for. Its own columns
            worked against it: "Stake" was the stake of the validators a node
            observed, which reads as stake belonging to the node. */}
        {measurement && (
          <p
            className="text-xs text-muted-foreground/70 mb-6"
            title={`Recording in ${measurement.metros.join(', ')}`}
          >
            Win rates are measured at {measurement.nodes} geographically distributed recording
            {measurement.nodes === 1 ? ' node' : ' nodes'} across {measurement.metros.length}{' '}
            {measurement.metros.length === 1 ? 'metro' : 'metros'}.
          </p>
        )}


      </div>
    </div>
  )
}
