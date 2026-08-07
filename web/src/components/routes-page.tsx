/* eslint-disable react-refresh/only-export-components */
// Customer-facing route latency page. Deliberately separate from the internal
// /performance/path-latency matrix: this one answers "how much faster is my
// route", so it shows a handful of user-chosen routes with their measured
// figures and a shareable URL, not an all-pairs grid.
import { useCallback, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { ArrowRight, Loader2, Plus, Route as RouteIcon, X } from 'lucide-react'
import { fetchFacilitiesByMetro, fetchMetroPathLatency, fetchMetros, fetchRouteSeries } from '@/lib/api'
import type { MetroPathLatency } from '@/lib/api'
import {
  OFF_NET_ENDPOINTS,
  formatRouteToken,
  parseRouteToken,
  resolveEndpoint,
} from '@/lib/route-anchors'
import { Sparkline } from '@/components/shared/sparkline'
import { PageHeader } from '@/components/page-header'
import { ErrorState } from '@/components/ui/error-state'
import { cn } from '@/lib/utils'

/** Matches maxRouteSeriesPairs on the route-series endpoint. */
const MAX_ROUTES = 10

// Copied rather than exported from path-latency-page.tsx: that page is a
// separate, internal artefact and must not grow an export surface for this one.
type ImprovementTier = 'great' | 'good' | 'neutral' | 'bad' | 'none'

function getImprovementTier(pct: number | null): ImprovementTier {
  if (pct === null) return 'none'
  if (pct > 20) return 'great'
  if (pct > 0) return 'good'
  if (pct >= -10) return 'neutral'
  return 'bad'
}

const TIER_PCT_TEXT = {
  great: 'text-green-700 dark:text-green-400',
  good: 'text-emerald-700 dark:text-emerald-400',
  neutral: 'text-amber-700 dark:text-amber-400',
  bad: 'text-red-700 dark:text-red-400',
  none: 'text-muted-foreground',
} as const

export type SelectedRoute = {
  from: string // metro code or off-net id
  to: string
  fromAnchor?: string
  toAnchor?: string
}

export type ResolvedRoute = {
  route: SelectedRoute
  fromMetro: string | null
  toMetro: string | null
  /** Non-empty when either endpoint is off-net; drives the note and anchor picker. */
  notes: string[]
  /** True when a figure cannot be produced at all (e.g. Zurich). */
  unavailable: boolean
  /** Pair key for the latency + series lookups, lexicographically ordered. */
  pairKey: string | null
}

/**
 * Lexicographic, case-folded pair key. Every lookup — measured figures, series,
 * and the `pairs=` query the series endpoint parses — goes through this one
 * function, so a route's DoubleZero and public-internet figures are read with
 * the same key and cannot come from different anchors.
 */
export function pairKeyOf(a: string, b: string): string {
  return [a.toLowerCase(), b.toLowerCase()].sort().join('-')
}

export function resolveRoute(route: SelectedRoute): ResolvedRoute {
  const from = resolveEndpoint(route.from, route.fromAnchor)
  const to = resolveEndpoint(route.to, route.toAnchor)

  // Same off-net location at both ends — the UI blocks this, but a URL can ask
  // for it (?routes=ohio@pit-ohio@chi). Its note argues the access leg cancels
  // between the two paths, which is untrue here because the two legs differ, and
  // the card would report pit↔chi under an Ohio→Ohio heading. Refuse instead.
  if (from.offNet && from.offNet === to.offNet) {
    return {
      route,
      fromMetro: from.metroCode,
      toMetro: to.metroCode,
      notes: [`${from.offNet.label} is at both ends of this route, so there is nothing to compare.`],
      unavailable: true,
      pairKey: null,
    }
  }

  const notes = [from.offNet?.note, to.offNet?.note].filter(Boolean) as string[]

  // Either side unresolvable, or both sides land on the same metro (e.g. an
  // anchor colliding with the other endpoint) — there is no route to report.
  const unavailable =
    !from.metroCode ||
    !to.metroCode ||
    from.metroCode.toLowerCase() === to.metroCode.toLowerCase()

  const pairKey = unavailable ? null : pairKeyOf(from.metroCode as string, to.metroCode as string)

  return { route, fromMetro: from.metroCode, toMetro: to.metroCode, notes, unavailable, pairKey }
}

function fmtMs(ms: number, dp = 2): string {
  return `${ms.toFixed(dp)} ms`
}

/** 0 means the figure was never measured, so render it as absent, not as zero. */
function orAbsent(ms: number, dp = 2): string | null {
  return ms > 0 ? fmtMs(ms, dp) : null
}

export const CONTRACTED_NOTE =
  'One or more hops had no recent measurements, so this route shows its contracted latency ' +
  'rather than a measured one. The improvement figure is withheld, because it would be ' +
  'comparing a commitment against a measurement.'

export type RouteFigures = {
  tiles: { label: string; value: string | null }[]
  improvementPct: number | null
  footnote: string | null
}

/**
 * Decides what every figure on a route card shows.
 *
 * All the suppression rules live here because they are not uniform, and getting
 * one wrong prints something that is not a measurement:
 *
 *  - `partiallyCommitted` means a hop had no recent samples and the API
 *    substituted the contracted figure. p95 and jitter arrive as 0 and are
 *    blanked. The mean is real, but it is a commitment rather than an
 *    observation, so it is shown with a label that says so. The improvement is
 *    withheld entirely — a percentage comparing a commitment against a measured
 *    internet figure is not a claim we can stand behind.
 *  - the public-internet side has no equivalent flag, so 0 is the only signal
 *    that one of its figures is absent.
 *
 * Jitter renders at 3 dp because a typical per-hop figure is around 0.03 ms,
 * which 2 dp flattens to a single significant figure.
 */
export function routeFigures(l: MetroPathLatency): RouteFigures {
  const partial = l.partiallyCommitted
  return {
    tiles: [
      {
        label: partial ? 'DoubleZero mean (contracted)' : 'DoubleZero mean',
        value: orAbsent(l.measuredLatencyMs),
      },
      { label: 'DoubleZero p95', value: partial ? null : orAbsent(l.measuredP95Ms) },
      { label: 'DoubleZero jitter', value: partial ? null : orAbsent(l.measuredJitterMs, 3) },
      { label: 'Internet mean', value: orAbsent(l.internetLatencyMs) },
      { label: 'Internet p95', value: orAbsent(l.internetP95Ms) },
      { label: 'Internet jitter', value: orAbsent(l.internetJitterMs, 3) },
    ],
    improvementPct: partial ? null : l.measuredImprovementPct,
    footnote: partial ? CONTRACTED_NOTE : null,
  }
}

/**
 * Orients an API path so it reads from the origin the customer picked.
 *
 * The latency lookup is keyed undirected (see `pairKeyOf`) — that is what stops
 * a route's DoubleZero and internet figures coming from different anchors — but
 * the API emits both directions of every pair and builds its slice by ranging a
 * Go map, so which one wins the lookup is chance. Without this, the same shared
 * link renders "tyo ─ fra ─ lon" on one load and "lon ─ fra ─ tyo" on the next.
 * Figures are direction-symmetric, so only the display order needs fixing.
 */
export function orientPath(pathMetros: string[], apiFrom: string, routeFrom: string): string[] {
  return apiFrom.toLowerCase() === routeFrom.toLowerCase() ? pathMetros : [...pathMetros].reverse()
}

/** One measured figure, or an em dash when the figure is absent. */
function Stat({ label, value }: { label: string; value: string | null }) {
  return (
    <div className="bg-muted/50 rounded-lg p-2.5">
      <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">{label}</div>
      <div
        className={cn(
          'text-base font-semibold tabular-nums',
          value === null && 'text-muted-foreground font-normal',
        )}
      >
        {value ?? '—'}
      </div>
    </div>
  )
}

/**
 * Collapsed list of the facilities in a metro. Shown only when a metro has more
 * than one, where "London" alone would be ambiguous.
 */
function FacilityDisclosure({ metroPk, metroLabel }: { metroPk: string; metroLabel: string }) {
  const { data } = useQuery({
    queryKey: ['facilities-by-metro', metroPk],
    queryFn: () => fetchFacilitiesByMetro(metroPk),
    staleTime: 300000,
  })

  const facilities = data?.items ?? []
  if (facilities.length < 2) return null

  return (
    <details className="text-xs text-muted-foreground">
      <summary className="cursor-pointer hover:text-foreground transition-colors">
        {metroLabel} — {facilities.length} facilities
      </summary>
      <div className="mt-1 pl-4">
        <div className="text-foreground">
          {metroLabel} {facilities.map((f) => f.code).join(', ')}
        </div>
        <div className="mt-0.5">
          Latency is measured metro to metro. It is not broken out per facility, so the figures
          above apply to the metro as a whole rather than to any one of these buildings.
        </div>
      </div>
    </details>
  )
}

/** Anchor picker for an off-net endpoint that has committed coverage nearby. */
function AnchorPicker({
  candidates,
  value,
  onChange,
}: {
  candidates: string[]
  value: string | null
  onChange: (anchor: string) => void
}) {
  return (
    <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
      On-ramp
      <select
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value)}
        className="px-2 py-1 text-xs border border-border rounded-md bg-background hover:bg-muted transition-colors"
      >
        {candidates.map((c) => (
          <option key={c} value={c}>
            {c.toUpperCase()}
          </option>
        ))}
      </select>
    </label>
  )
}

function RouteCard({
  resolved,
  labelFor,
  metroPkFor,
  latency,
  latencyPending,
  latencyError,
  series,
  seriesPending,
  seriesError,
  onRemove,
  onAnchorChange,
}: {
  resolved: ResolvedRoute
  labelFor: (id: string) => string
  metroPkFor: (metroCode: string | null) => string | null
  latency: MetroPathLatency | null
  latencyPending: boolean
  latencyError: boolean
  series: { dz: number[]; internet: number[] } | null
  seriesPending: boolean
  seriesError: boolean
  onRemove: () => void
  onAnchorChange: (side: 'from' | 'to', anchor: string) => void
}) {
  const { route, notes, unavailable, fromMetro, toMetro } = resolved
  const fromOffNet = OFF_NET_ENDPOINTS.find((e) => e.id === route.from)
  const toOffNet = OFF_NET_ENDPOINTS.find((e) => e.id === route.to)

  const figures = latency ? routeFigures(latency) : null
  const tier = getImprovementTier(figures?.improvementPct ?? null)

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2 font-medium">
          <span>{labelFor(route.from)}</span>
          <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0" />
          <span>{labelFor(route.to)}</span>
        </div>
        <div className="flex items-center gap-2">
          {fromOffNet && fromOffNet.candidateAnchors.length > 0 && (
            <AnchorPicker
              candidates={fromOffNet.candidateAnchors}
              value={fromMetro}
              onChange={(a) => onAnchorChange('from', a)}
            />
          )}
          {toOffNet && toOffNet.candidateAnchors.length > 0 && (
            <AnchorPicker
              candidates={toOffNet.candidateAnchors}
              value={toMetro}
              onChange={(a) => onAnchorChange('to', a)}
            />
          )}
          <button
            onClick={onRemove}
            aria-label="Remove route"
            className="p-1 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      {notes.length > 0 && (
        <div className="mt-2 space-y-1">
          {notes.map((n) => (
            <p key={n} className="text-xs text-muted-foreground">
              {n}
            </p>
          ))}
        </div>
      )}

      {unavailable ? (
        <div className="mt-3 text-sm text-muted-foreground">
          <span className="text-base font-semibold text-foreground">N/A</span>
          {notes.length === 0 && (
            <span className="ml-2">
              Both ends of this route resolve to the same metro, so there is nothing to compare.
            </span>
          )}
        </div>
      ) : !latency && latencyError ? (
        // Distinct from "no path": an unreachable endpoint must never be reported
        // to a customer as DoubleZero having no route between two of its metros.
        // Guarded on !latency so a failed background refetch does not hide figures
        // we already hold.
        <div className="mt-3 text-sm text-muted-foreground">
          Couldn&apos;t load latency data. Try again in a moment.
        </div>
      ) : !latency && latencyPending ? (
        <div className="mt-3 flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading latency…
        </div>
      ) : !latency || !figures ? (
        <div className="mt-3 text-sm text-muted-foreground">
          No DoubleZero path between these metros.
        </div>
      ) : (
        <>
          <div className="mt-3 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2">
            {figures.tiles.map((t) => (
              <Stat key={t.label} label={t.label} value={t.value} />
            ))}
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-x-6 gap-y-2">
            <div>
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                Improvement
              </div>
              <div className={cn('text-lg font-bold tabular-nums', TIER_PCT_TEXT[tier])}>
                {figures.improvementPct !== null
                  ? `${figures.improvementPct > 0 ? '+' : ''}${figures.improvementPct.toFixed(1)}%`
                  : '—'}
              </div>
            </div>
            <div>
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                Hops
              </div>
              <div className="text-lg font-bold tabular-nums">{latency.hopCount}</div>
            </div>
            <div className="min-w-0">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                Path
              </div>
              <div className="font-mono text-sm truncate">
                {orientPath(latency.pathMetros, latency.fromMetroCode, fromMetro ?? '').join(' ─ ')}
              </div>
            </div>
            <div className="ml-auto">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1">
                7 days
              </div>
              {/* Sparkline renders "no history" for two empty series. Reaching that
                  while the query is in flight or has failed would tell a customer
                  DoubleZero has no measurement history, so both states are named. */}
              {!series && (seriesPending || seriesError) ? (
                <div
                  className="flex items-center justify-center text-xs text-muted-foreground"
                  style={{ width: 220, height: 40 }}
                >
                  {seriesError ? (
                    'history unavailable'
                  ) : (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  )}
                </div>
              ) : (
                <Sparkline dz={series?.dz ?? []} internet={series?.internet ?? []} />
              )}
            </div>
          </div>

          <div className="mt-3 space-y-1">
            {figures.footnote && (
              <p className="text-xs text-muted-foreground">{figures.footnote}</p>
            )}
            {[fromMetro, toMetro].map((code) => {
              const pk = metroPkFor(code)
              if (!pk || !code) return null
              return <FacilityDisclosure key={code} metroPk={pk} metroLabel={labelFor(code)} />
            })}
          </div>
        </>
      )}
    </div>
  )
}

export function RoutesPage() {
  // --- URL state -----------------------------------------------------------
  const [searchParams, setSearchParams] = useSearchParams()

  const { routes, truncated } = useMemo(() => {
    const raw = searchParams.get('routes')
    if (!raw) return { routes: [] as SelectedRoute[], truncated: false }
    // parseRouteToken returns null for a malformed token. Drop those rather than
    // coercing — a corrupted shared link must not render as a plausible-but-wrong
    // route, which is the same failure the Zurich N/A rule exists to prevent.
    const parsed = raw
      .split(',')
      .filter(Boolean)
      .map(parseRouteToken)
      .filter((r): r is NonNullable<ReturnType<typeof parseRouteToken>> => r !== null)
    // Truncation is reported, not silent: otherwise the recipient of a shared
    // link sees a different set of routes than the sender and cannot tell.
    return { routes: parsed.slice(0, MAX_ROUTES), truncated: parsed.length > MAX_ROUTES }
  }, [searchParams])

  const setRoutes = useCallback(
    (next: SelectedRoute[]) => {
      const tokens = next.map((r) => formatRouteToken(r.from, r.to, r.fromAnchor, r.toAnchor))
      // Replace the whole param so a shared link reproduces the view exactly, and
      // replace the history entry so Back leaves the page rather than walking
      // back through every intermediate route list.
      setSearchParams(tokens.length ? { routes: tokens.join(',') } : {}, { replace: true })
    },
    [setSearchParams],
  )

  // --- Data ----------------------------------------------------------------
  const {
    data: metroData,
    isLoading: metrosLoading,
    error: metrosError,
  } = useQuery({
    queryKey: ['metros', 'all', 500],
    queryFn: () => fetchMetros(500),
    staleTime: 300000,
  })

  const {
    data: latencyData,
    isPending: latencyPending,
    error: latencyQueryError,
  } = useQuery({
    queryKey: ['metro-path-latency', 'latency'],
    queryFn: () => fetchMetroPathLatency('latency'),
    staleTime: 60000,
  })

  // The endpoint also returns 200 with an `error` body on a backend failure, so
  // check both. Either way the card must not fall through to "no path".
  const latencyError = Boolean(latencyQueryError) || Boolean(latencyData?.error)

  const resolved = useMemo(() => routes.map(resolveRoute), [routes])

  const pairKeys = useMemo(
    () => [...new Set(resolved.map((r) => r.pairKey).filter((k): k is string => k !== null))],
    [resolved],
  )

  const {
    data: seriesData,
    isPending: seriesIsPending,
    error: seriesQueryError,
  } = useQuery({
    queryKey: ['route-series', pairKeys.join(',')],
    queryFn: () => fetchRouteSeries(pairKeys),
    enabled: pairKeys.length > 0,
    staleTime: 300000,
  })

  // A disabled query reports isPending, so gate on there being something to fetch.
  const seriesError = Boolean(seriesQueryError) || Boolean(seriesData?.error)
  const seriesPending = pairKeys.length > 0 && seriesIsPending && !seriesError

  const latencyByPair = useMemo(() => {
    const map = new Map<string, MetroPathLatency>()
    for (const p of latencyData?.paths ?? []) {
      map.set(pairKeyOf(p.fromMetroCode, p.toMetroCode), p)
    }
    return map
  }, [latencyData])

  const seriesByPair = useMemo(() => {
    const map = new Map<string, { dz: number[]; internet: number[] }>()
    for (const s of seriesData?.series ?? []) {
      map.set(pairKeyOf(s.fromMetroCode, s.toMetroCode), {
        dz: s.points.map((p) => p.dzMs),
        internet: s.points.map((p) => p.internetMs),
      })
    }
    return map
  }, [seriesData])

  const metros = useMemo(() => metroData?.items ?? [], [metroData])

  const metroByCode = useMemo(
    () => new Map(metros.map((m) => [m.code.toLowerCase(), m])),
    [metros],
  )

  const labelFor = useCallback(
    (id: string) => {
      const offNet = OFF_NET_ENDPOINTS.find((e) => e.id === id)
      if (offNet) return offNet.label
      const metro = metroByCode.get(id.toLowerCase())
      return metro ? `${metro.name} (${metro.code.toUpperCase()})` : id.toUpperCase()
    },
    [metroByCode],
  )

  const metroPkFor = useCallback(
    (metroCode: string | null) => metroByCode.get((metroCode ?? '').toLowerCase())?.pk ?? null,
    [metroByCode],
  )

  // --- Selection -----------------------------------------------------------
  const [origin, setOrigin] = useState('')
  const [destination, setDestination] = useState('')

  const atLimit = routes.length >= MAX_ROUTES
  const canAdd = Boolean(origin) && Boolean(destination) && origin !== destination && !atLimit

  const addRoute = useCallback(() => {
    if (!canAdd) return
    setRoutes([...routes, { from: origin, to: destination }])
  }, [canAdd, routes, origin, destination, setRoutes])

  const options = useMemo(
    () => ({
      onNet: metros.map((m) => ({ value: m.code, label: `${m.name} (${m.code.toUpperCase()})` })),
      offNet: OFF_NET_ENDPOINTS.map((e) => ({ value: e.id, label: e.label })),
    }),
    [metros],
  )

  if (metrosError) {
    return (
      <div className="flex-1 flex items-center justify-center bg-background">
        <ErrorState
          title="Failed to load metros"
          message={metrosError instanceof Error ? metrosError.message : 'Unknown error'}
        />
      </div>
    )
  }

  return (
    <div className="flex-1 flex flex-col bg-background overflow-y-auto">
      <div className="px-6 py-4">
        <PageHeader icon={RouteIcon} title="Route Latency" />

        <p className="mt-2 text-sm text-muted-foreground">
          Round-trip latency between metros, measured over the DoubleZero network and the public
          internet.
        </p>
        <p className="mt-1 text-xs text-muted-foreground max-w-4xl">
          Figures are averages over the last 24 hours; the spark lines show the last 7 days by the
          hour. The public-internet figures are measured end to end. The DoubleZero p95 and jitter
          are sums of each hop&apos;s own p95 and mean jitter — percentiles and jitter do not add,
          so those two figures are higher than what a packet actually sees. The bias runs against
          DoubleZero.
        </p>

        {/* Selection */}
        <div className="flex flex-wrap items-end gap-2 mt-4">
          <EndpointSelect
            label="Origin"
            value={origin}
            onChange={setOrigin}
            options={options}
            loading={metrosLoading}
          />
          <EndpointSelect
            label="Destination"
            value={destination}
            onChange={setDestination}
            options={options}
            loading={metrosLoading}
          />
          <button
            onClick={addRoute}
            disabled={!canAdd}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-border bg-background hover:bg-muted/50 rounded-md transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Plus className="h-3 w-3" />
            Add route
          </button>
          {(atLimit || truncated) && (
            <span className="text-xs text-muted-foreground pb-1.5">
              Showing the maximum of {MAX_ROUTES} routes
              {truncated ? '; this link asked for more.' : '. Remove one to add another.'}
            </span>
          )}
        </div>

        {/* Routes */}
        <div className="mt-4 space-y-3">
          {resolved.length === 0 ? (
            <div className="text-sm text-muted-foreground py-8 text-center">
              {metrosLoading ? (
                <Loader2 className="h-5 w-5 animate-spin mx-auto" />
              ) : (
                'Pick an origin and a destination to compare a route.'
              )}
            </div>
          ) : (
            resolved.map((r, i) => (
              <RouteCard
                key={`${formatRouteToken(r.route.from, r.route.to, r.route.fromAnchor, r.route.toAnchor)}#${i}`}
                resolved={r}
                labelFor={labelFor}
                metroPkFor={metroPkFor}
                latency={r.pairKey ? (latencyByPair.get(r.pairKey) ?? null) : null}
                latencyPending={latencyPending}
                latencyError={latencyError}
                series={r.pairKey ? (seriesByPair.get(r.pairKey) ?? null) : null}
                seriesPending={seriesPending}
                seriesError={seriesError}
                onRemove={() => setRoutes(routes.filter((_, idx) => idx !== i))}
                onAnchorChange={(side, anchor) =>
                  setRoutes(
                    routes.map((route, idx) =>
                      idx === i
                        ? { ...route, [side === 'from' ? 'fromAnchor' : 'toAnchor']: anchor }
                        : route,
                    ),
                  )
                }
              />
            ))
          )}
        </div>
      </div>
    </div>
  )
}

function EndpointSelect({
  label,
  value,
  onChange,
  options,
  loading,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  options: { onNet: { value: string; label: string }[]; offNet: { value: string; label: string }[] }
  loading: boolean
}) {
  return (
    <label className="flex flex-col gap-1 text-xs text-muted-foreground">
      {label}
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={loading}
        className="min-w-[14rem] px-3 py-1.5 text-xs border border-border rounded-md bg-background hover:bg-muted transition-colors disabled:opacity-50"
      >
        <option value="">Select a location…</option>
        <optgroup label="DoubleZero metros">
          {options.onNet.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </optgroup>
        <optgroup label="Not yet on DoubleZero">
          {options.offNet.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </optgroup>
      </select>
    </label>
  )
}
