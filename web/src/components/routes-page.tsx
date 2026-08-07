/* eslint-disable react-refresh/only-export-components */
// Customer-facing route latency page. Deliberately separate from the internal
// /performance/path-latency matrix: that one grids every metro pair on the
// network, this one grids only the locations a customer picked and answers
// "how much faster is my mesh", with a shareable URL.
import { useCallback, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import { ArrowRight, Loader2, Route as RouteIcon, X } from 'lucide-react'
import { fetchFacilitiesByMetro, fetchMetroPathLatency, fetchMetros, fetchRouteSeries } from '@/lib/api'
import type { MetroPathLatency } from '@/lib/api'
import {
  OFF_NET_ENDPOINTS,
  formatCityToken,
  formatRouteToken,
  parseCityToken,
  parseRouteToken,
  resolveEndpoint,
} from '@/lib/route-anchors'
import { Sparkline } from '@/components/shared/sparkline'
import { PageHeader } from '@/components/page-header'
import { ErrorState } from '@/components/ui/error-state'
import { cn } from '@/lib/utils'

/**
 * Presentational cap. The mesh is quadratic — 12 locations is 66 pairs, which is
 * about as much as a reader can still scan. It is not the route-series cap: the
 * series is fetched for the selected cell alone, one pair at a time.
 */
export const MAX_CITIES = 12

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
  /**
   * Milliseconds saved against the public internet. Null wherever the
   * improvement is withheld, so a matrix cell and a KPI card cannot print a
   * saving for a route whose percentage we refuse to state.
   */
  savedMs: number | null
  /** False when the public internet has no samples for this pair in the window. */
  internetMeasured: boolean
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
  const internetMeasured = l.internetLatencyMs > 0
  const improvementPct = partial ? null : l.measuredImprovementPct
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
    improvementPct,
    savedMs:
      improvementPct !== null && internetMeasured && l.measuredLatencyMs > 0
        ? l.internetLatencyMs - l.measuredLatencyMs
        : null,
    internetMeasured,
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

// --- Matrix model ----------------------------------------------------------

/** A location the customer picked, with the on-ramp chosen for it if it is off-net. */
export type SelectedCity = { id: string; anchor?: string }

/**
 * Reads the `?cities=` list. Returns how many tokens the link asked for as well
 * as the ones kept, because a malformed token is dropped and a repeat is folded:
 * the recipient of a shared link must be told the selection shrank rather than
 * quietly seeing a smaller mesh than the sender.
 */
export function parseCities(raw: string | null): { cities: SelectedCity[]; requested: number } {
  if (!raw) return { cities: [], requested: 0 }
  const tokens = raw.split(',').filter(Boolean)
  const seen = new Set<string>()
  const cities: SelectedCity[] = []
  for (const token of tokens) {
    const parsed = parseCityToken(token)
    if (!parsed) continue
    const key = parsed.id.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    cities.push(parsed)
  }
  return { cities: cities.slice(0, MAX_CITIES), requested: tokens.length }
}

export function formatCities(cities: SelectedCity[]): string {
  return cities.map((c) => formatCityToken(c.id, c.anchor)).join(',')
}

/** Every unordered pair of a selection, each once — the mesh the KPI cards sum over. */
export function meshPairs<T>(items: T[]): [T, T][] {
  const out: [T, T][] = []
  for (let i = 0; i < items.length; i++) {
    for (let j = i + 1; j < items.length; j++) out.push([items[i], items[j]])
  }
  return out
}

/**
 * What one cell of the mesh states.
 *
 * The four ways a cell can hold no percentage are four different facts, and a
 * customer reading the grid will ask which one applies, so none of them
 * collapse into a shared blank:
 *
 *  - `unavailable` — the location has no committed DoubleZero coverage (Zurich),
 *    or both ends resolve to one metro. Carries the note that says which.
 *  - `no-path` — DoubleZero cannot route between the two metros.
 *  - `not-measured` — the public internet had no samples for the pair.
 *  - `withheld` — `partiallyCommitted`; a percentage here would compare a
 *    commitment against a measurement.
 *
 * `loading` and `error` are separate again: a failed request must never render
 * as DoubleZero having no route.
 */
export type MatrixCell =
  | { kind: 'diagonal' }
  | { kind: 'unavailable'; note: string | null }
  | { kind: 'loading' }
  | { kind: 'error' }
  | { kind: 'no-path' }
  | { kind: 'not-measured' }
  | { kind: 'withheld' }
  | { kind: 'improvement'; pct: number; savedMs: number | null }

/**
 * Classifies one cell. Every suppression decision is read off `routeFigures` —
 * the cell re-derives none of them, so the grid and the card below it cannot
 * disagree about what counts as a measurement.
 */
export function cellFor(
  resolved: ResolvedRoute,
  latency: MetroPathLatency | null,
  pending: boolean,
  error: boolean,
): MatrixCell {
  if (resolved.unavailable) return { kind: 'unavailable', note: resolved.notes[0] ?? null }
  // Guarded on !latency so a failed background refetch does not blank figures we hold.
  if (!latency && error) return { kind: 'error' }
  if (!latency && pending) return { kind: 'loading' }
  if (!latency) return { kind: 'no-path' }

  const figures = routeFigures(latency)
  if (!figures.internetMeasured) return { kind: 'not-measured' }
  if (figures.improvementPct === null) return { kind: 'withheld' }
  return { kind: 'improvement', pct: figures.improvementPct, savedMs: figures.savedMs }
}

export type MatrixSummary = {
  pairs: number
  /** Pairs DoubleZero carries, measured or not. */
  withPath: number
  avgSavedMs: number | null
  avgPct: number | null
  best: { label: string; pct: number; savedMs: number | null } | null
}

/**
 * Totals for the KPI cards. Only `improvement` cells carry a percentage, so a
 * withheld or unmeasured pair moves no average and can never be the best route —
 * it is counted as carried by DoubleZero, which is a fact, and nothing more.
 */
export function summariseMatrix(entries: { label: string; cell: MatrixCell }[]): MatrixSummary {
  const improved = entries.flatMap((e) =>
    e.cell.kind === 'improvement' ? [{ label: e.label, ...e.cell }] : [],
  )
  const saved = improved.map((e) => e.savedMs).filter((ms): ms is number => ms !== null)
  const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null)

  return {
    pairs: entries.length,
    withPath: entries.filter((e) =>
      ['improvement', 'withheld', 'not-measured'].includes(e.cell.kind),
    ).length,
    avgSavedMs: mean(saved),
    avgPct: mean(improved.map((e) => e.pct)),
    best: improved.reduce<MatrixSummary['best']>(
      (best, e) => (best === null || e.pct > best.pct ? e : best),
      null,
    ),
  }
}

/**
 * Cell shading, by improvement rather than by absolute latency: 25 metros carry
 * activated links and the graph is connected, so DoubleZero carrying a route is
 * the norm across the mesh and shading that fact would spend the whole range on
 * a constant.
 */
export function shadeFor(pct: number): string {
  if (pct <= 0) return 'bg-red-500/20'
  if (pct < 10) return 'bg-emerald-500/10'
  if (pct < 20) return 'bg-emerald-500/20'
  if (pct < 30) return 'bg-emerald-500/30'
  if (pct < 40) return 'bg-emerald-500/45'
  return 'bg-emerald-500/60'
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
            aria-label="Clear selected route"
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
                {figures.improvementPct != null
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

  // parseCities drops a malformed or repeated token rather than coercing it, and
  // reports how many the link asked for so a shrunken selection can be stated.
  const { cities, requested } = useMemo(
    () => parseCities(searchParams.get('cities')),
    [searchParams],
  )

  const selectedCell = useMemo(() => {
    const parsed = parseRouteToken(searchParams.get('cell') ?? '')
    if (!parsed) return null
    // A cell naming a location outside the mesh is dropped on the same rule: a
    // shared link must not open on a pair this grid does not show.
    const inMesh = (id: string) => cities.some((c) => c.id.toLowerCase() === id.toLowerCase())
    if (!inMesh(parsed.from) || !inMesh(parsed.to)) return null
    if (parsed.from.toLowerCase() === parsed.to.toLowerCase()) return null
    return { from: parsed.from, to: parsed.to }
  }, [searchParams, cities])

  const setUrl = useCallback(
    (nextCities: SelectedCity[], cell: { from: string; to: string } | null) => {
      const params: Record<string, string> = {}
      if (nextCities.length) params.cities = formatCities(nextCities)
      if (cell) params.cell = formatRouteToken(cell.from, cell.to)
      // Replace the whole query so a shared link reproduces the view exactly, and
      // replace the history entry so Back leaves the page rather than walking
      // back through every intermediate selection.
      setSearchParams(params, { replace: true })
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
  // check both. Either way a cell must not fall through to "no path".
  const latencyError = Boolean(latencyQueryError) || Boolean(latencyData?.error)

  const latencyByPair = useMemo(() => {
    const map = new Map<string, MetroPathLatency>()
    for (const p of latencyData?.paths ?? []) {
      map.set(pairKeyOf(p.fromMetroCode, p.toMetroCode), p)
    }
    return map
  }, [latencyData])

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

  // --- Mesh ----------------------------------------------------------------

  // Axes are alphabetical by city name, matching the picker, so a reader can find
  // a location in the grid without first learning the order it was added in.
  const axis = useMemo(
    () => [...cities].sort((a, b) => labelFor(a.id).localeCompare(labelFor(b.id))),
    [cities, labelFor],
  )

  const grid = useMemo(
    () =>
      axis.map((row, i) =>
        axis.map((col, j) => {
          if (i === j) {
            return { resolved: null, cell: { kind: 'diagonal' } as MatrixCell }
          }
          // Row is the origin and column the destination, which is the direction
          // the reader is scanning — orientPath in the card below uses it.
          const resolved = resolveRoute({
            from: row.id,
            to: col.id,
            fromAnchor: row.anchor,
            toAnchor: col.anchor,
          })
          const latency = resolved.pairKey ? (latencyByPair.get(resolved.pairKey) ?? null) : null
          return { resolved, cell: cellFor(resolved, latency, latencyPending, latencyError) }
        }),
      ),
    [axis, latencyByPair, latencyPending, latencyError],
  )

  const summary = useMemo(
    () =>
      summariseMatrix(
        meshPairs(axis.map((_, i) => i)).map(([i, j]) => ({
          label: `${shortLabel(axis[i].id)}↔${shortLabel(axis[j].id)}`,
          cell: grid[i][j].cell,
        })),
      ),
    [axis, grid],
  )

  const selectedIndex = useMemo(() => {
    if (!selectedCell) return null
    const at = (id: string) => axis.findIndex((c) => c.id.toLowerCase() === id.toLowerCase())
    const i = at(selectedCell.from)
    const j = at(selectedCell.to)
    return i < 0 || j < 0 || i === j ? null : { i, j }
  }, [selectedCell, axis])

  const selected = selectedIndex ? grid[selectedIndex.i][selectedIndex.j].resolved : null
  const selectedLatency = selected?.pairKey ? (latencyByPair.get(selected.pairKey) ?? null) : null

  // Fetched one pair at a time, for the selected cell only: the grid shows no
  // sparklines, so the route-series cap of 10 pairs is never approached.
  const selectedPairKey = selected?.pairKey ?? null
  const {
    data: seriesData,
    isPending: seriesIsPending,
    error: seriesQueryError,
  } = useQuery({
    queryKey: ['route-series', selectedPairKey],
    queryFn: () => fetchRouteSeries([selectedPairKey as string]),
    enabled: selectedPairKey !== null,
    staleTime: 300000,
  })

  // A disabled query reports isPending, so gate on there being something to fetch.
  const seriesError = Boolean(seriesQueryError) || Boolean(seriesData?.error)
  const seriesPending = selectedPairKey !== null && seriesIsPending && !seriesError

  const series = useMemo(() => {
    const s = (seriesData?.series ?? []).find(
      (x) => pairKeyOf(x.fromMetroCode, x.toMetroCode) === selectedPairKey,
    )
    if (!s) return null
    return { dz: s.points.map((p) => p.dzMs), internet: s.points.map((p) => p.internetMs) }
  }, [seriesData, selectedPairKey])

  // --- Selection -----------------------------------------------------------
  const atLimit = cities.length >= MAX_CITIES

  const addCity = useCallback(
    (id: string) => {
      if (!id || atLimit) return
      setUrl([...cities, { id }], selectedCell)
    },
    [atLimit, cities, selectedCell, setUrl],
  )

  const removeCity = useCallback(
    (id: string) => {
      const gone = (other: string) => other.toLowerCase() === id.toLowerCase()
      const stillShown =
        selectedCell && !gone(selectedCell.from) && !gone(selectedCell.to) ? selectedCell : null
      setUrl(
        cities.filter((c) => !gone(c.id)),
        stillShown,
      )
    },
    [cities, selectedCell, setUrl],
  )

  // One anchor per city rather than per route: the same city cannot sensibly have
  // two on-ramps in one grid, and both sides of every pair it appears in move
  // together, so the customer's access leg still cancels.
  const setAnchor = useCallback(
    (id: string, anchor: string) => {
      setUrl(
        cities.map((c) => (c.id === id ? { ...c, anchor } : c)),
        selectedCell,
      )
    },
    [cities, selectedCell, setUrl],
  )

  const options = useMemo(() => {
    const chosen = new Set(cities.map((c) => c.id.toLowerCase()))
    // Sorted by the label, which leads with the city name — a reader looking for
    // Dublin should not have to know it is DUB.
    const byLabel = (a: { label: string }, b: { label: string }) => a.label.localeCompare(b.label)
    return {
      onNet: metros
        .filter((m) => !chosen.has(m.code.toLowerCase()))
        .map((m) => ({ value: m.code, label: `${m.name} (${m.code.toUpperCase()})` }))
        .sort(byLabel),
      offNet: OFF_NET_ENDPOINTS.filter((e) => !chosen.has(e.id.toLowerCase()))
        .map((e) => ({ value: e.id, label: e.label }))
        .sort(byLabel),
    }
  }, [metros, cities])

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
          DoubleZero against the public internet, across your locations. Pick the locations you
          care about and every route between them is compared.
        </p>
        <p className="mt-1 text-xs text-muted-foreground max-w-4xl">
          Figures cover the last 24 hours; the selected route also shows the last 7 days by the
          hour. The public-internet figures are measured end to end. The DoubleZero p95 and jitter
          are sums of each hop&apos;s own p95 and mean jitter — percentiles and jitter do not add,
          so those two figures are higher than what a packet actually sees. The bias runs against
          DoubleZero.
        </p>

        {/* Selection */}
        <div className="flex flex-wrap items-center gap-2 mt-4">
          <EndpointSelect
            value=""
            onChange={addCity}
            options={options}
            loading={metrosLoading}
            disabled={atLimit}
          />
          {/* Chips follow the axis order, so the picker, the chips and the grid all
              read alphabetically. */}
          {axis.map((c) => {
            const offNet = OFF_NET_ENDPOINTS.find((e) => e.id === c.id)
            return (
              <span
                key={c.id}
                className="flex items-center gap-2 pl-2.5 pr-1 py-1 text-xs border border-border rounded-md bg-muted/40"
              >
                {labelFor(c.id)}
                {offNet && offNet.candidateAnchors.length > 0 && (
                  <AnchorPicker
                    candidates={offNet.candidateAnchors}
                    value={resolveEndpoint(c.id, c.anchor).metroCode}
                    onChange={(a) => setAnchor(c.id, a)}
                  />
                )}
                <button
                  onClick={() => removeCity(c.id)}
                  aria-label={`Remove ${labelFor(c.id)}`}
                  className="p-0.5 rounded text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </span>
            )
          })}
          {(atLimit || requested > cities.length) && (
            <span className="text-xs text-muted-foreground">
              {requested > cities.length
                ? `This link asked for ${requested} locations; showing ${cities.length}.`
                : `Showing the maximum of ${MAX_CITIES} locations. Remove one to add another.`}
            </span>
          )}
        </div>

        {axis.length < 2 ? (
          <div className="text-sm text-muted-foreground py-8 text-center">
            {metrosLoading ? (
              <Loader2 className="h-5 w-5 animate-spin mx-auto" />
            ) : (
              'Add two or more locations to compare every route between them.'
            )}
          </div>
        ) : (
          <>
            <SummaryCards summary={summary} locations={axis.length} />

            <LatencyMatrix
              axis={axis}
              grid={grid}
              selected={selectedIndex}
              onSelect={(i, j) => setUrl(cities, { from: axis[i].id, to: axis[j].id })}
            />

            <Legend />

            <OffNetNotes axis={axis} />

            <div className="mt-4">
              {selected ? (
                <RouteCard
                  resolved={selected}
                  labelFor={labelFor}
                  metroPkFor={metroPkFor}
                  latency={selectedLatency}
                  latencyPending={latencyPending}
                  latencyError={latencyError}
                  series={series}
                  seriesPending={seriesPending}
                  seriesError={seriesError}
                  onRemove={() => setUrl(cities, null)}
                  onAnchorChange={(side, anchor) =>
                    setAnchor(side === 'from' ? selected.route.from : selected.route.to, anchor)
                  }
                />
              ) : (
                <p className="text-sm text-muted-foreground text-center py-6">
                  Select a cell to see the full breakdown for that route.
                </p>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

/** Axis label: a metro shows its code, an off-net location its own short form. */
function shortLabel(id: string): string {
  return (OFF_NET_ENDPOINTS.find((e) => e.id === id)?.short ?? id).toUpperCase()
}

function SummaryCards({ summary, locations }: { summary: MatrixSummary; locations: number }) {
  const cards: { label: string; value: string; lines: string[] }[] = [
    {
      label: 'Pairs shown',
      value: String(summary.pairs),
      lines: [`${locations} locations`],
    },
    {
      label: 'On DoubleZero',
      value: `${summary.withPath} pairs`,
      lines: ['where DoubleZero has a path'],
    },
    {
      label: 'Average faster',
      value: summary.avgSavedMs !== null ? `${summary.avgSavedMs.toFixed(1)} ms` : '—',
      lines: [
        summary.avgPct !== null ? `${summary.avgPct.toFixed(1)}% mean reduction` : 'nothing to average',
      ],
    },
    {
      label: 'Best route',
      value: summary.best ? `${summary.best.pct.toFixed(1)}%` : '—',
      lines: summary.best
        ? [
            summary.best.label,
            summary.best.savedMs !== null ? `${summary.best.savedMs.toFixed(1)} ms saved` : '',
          ].filter(Boolean)
        : ['no measured comparison'],
    },
  ]

  return (
    <div className="mt-4 grid grid-cols-2 lg:grid-cols-4 gap-2">
      {cards.map((c) => (
        <div key={c.label} className="bg-card border border-border rounded-lg p-3">
          <div className="text-[10px] text-muted-foreground uppercase tracking-wider">{c.label}</div>
          <div className="mt-1 text-xl font-bold tabular-nums">{c.value}</div>
          {c.lines.map((l) => (
            <div key={l} className="text-xs text-muted-foreground">
              {l}
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

const CELL_NOTE: Record<Exclude<MatrixCell['kind'], 'improvement' | 'unavailable'>, string> = {
  diagonal: 'same location',
  loading: 'loading…',
  error: 'latency data could not be loaded',
  'no-path': 'DoubleZero has no path between these metros',
  'not-measured': 'no public-internet measurements for this pair in the window',
  withheld: 'improvement withheld: a hop reports its contracted figure, not a measurement',
}

function CellBody({ cell }: { cell: MatrixCell }) {
  switch (cell.kind) {
    case 'improvement':
      return (
        <>
          <span className="text-sm font-semibold tabular-nums">{cell.pct.toFixed(1)}%</span>
          <span className="text-[10px] text-muted-foreground tabular-nums">
            {cell.savedMs !== null ? `${cell.savedMs.toFixed(1)} ms` : '—'}
          </span>
        </>
      )
    case 'unavailable':
      return <span className="text-xs text-muted-foreground">N/A</span>
    case 'loading':
      return <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
    case 'diagonal':
      return <span className="text-muted-foreground/40">·</span>
    default:
      return (
        <span className="text-[10px] leading-tight text-muted-foreground px-1 text-center">
          {CELL_TEXT[cell.kind]}
        </span>
      )
  }
}

/** Spelled out rather than derived from the kind: this is text a customer reads. */
const CELL_TEXT: Record<'no-path' | 'not-measured' | 'withheld' | 'error', string> = {
  'no-path': 'no path',
  'not-measured': 'not measured',
  withheld: 'withheld',
  error: 'load failed',
}

const CELL_BOX = 'w-24 h-14 flex flex-col items-center justify-center gap-0.5 border border-border/40'

function LatencyMatrix({
  axis,
  grid,
  selected,
  onSelect,
}: {
  axis: SelectedCity[]
  grid: { resolved: ResolvedRoute | null; cell: MatrixCell }[][]
  selected: { i: number; j: number } | null
  onSelect: (i: number, j: number) => void
}) {
  return (
    // The grid scrolls inside this box; the page body never scrolls sideways.
    <div className="mt-4 max-w-full overflow-x-auto">
      <table className="border-separate border-spacing-0">
        <caption className="caption-top text-left text-[11px] text-muted-foreground pb-2">
          Latency reduction on DoubleZero against the public internet, row to column: percentage,
          and milliseconds saved.
        </caption>
        <thead>
          <tr>
            <th className="sticky left-0 z-10 bg-background" />
            {axis.map((c) => (
              <th
                key={c.id}
                scope="col"
                className="px-1 pb-1 text-[11px] font-medium text-muted-foreground"
              >
                {shortLabel(c.id)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {axis.map((row, i) => (
            <tr key={row.id}>
              <th
                scope="row"
                className="sticky left-0 z-10 bg-background pr-2 text-right text-[11px] font-medium text-muted-foreground"
              >
                {shortLabel(row.id)}
              </th>
              {axis.map((col, j) => {
                const { cell } = grid[i][j]
                if (cell.kind === 'diagonal') {
                  return (
                    <td key={col.id} className="p-0">
                      <div className={cn(CELL_BOX, 'bg-muted/30')}>
                        <CellBody cell={cell} />
                      </div>
                    </td>
                  )
                }
                const isSelected = selected?.i === i && selected?.j === j
                const note =
                  cell.kind === 'improvement'
                    ? `${cell.pct.toFixed(1)}% faster on DoubleZero`
                    : cell.kind === 'unavailable'
                      ? (cell.note ?? 'no figure to report')
                      : CELL_NOTE[cell.kind]
                return (
                  <td key={col.id} className="p-0">
                    <button
                      onClick={() => onSelect(i, j)}
                      title={`${shortLabel(row.id)} → ${shortLabel(col.id)} — ${note}`}
                      aria-pressed={isSelected}
                      className={cn(
                        CELL_BOX,
                        'w-full transition-colors hover:brightness-110',
                        cell.kind === 'improvement' ? shadeFor(cell.pct) : 'bg-muted/10',
                        isSelected && 'outline-2 -outline-offset-2 outline-foreground',
                      )}
                    >
                      <CellBody cell={cell} />
                    </button>
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/**
 * Where an off-net location's figures actually come from, stated beside the grid
 * rather than only inside the card. An OHIO row of ordinary-looking numbers is
 * taken at its on-ramp, and a reader scanning the mesh has to be told that
 * without having to click a cell first.
 */
function OffNetNotes({ axis }: { axis: SelectedCity[] }) {
  const present = axis.flatMap((c) => OFF_NET_ENDPOINTS.filter((e) => e.id === c.id))
  if (present.length === 0) return null
  return (
    <div className="mt-2 space-y-1 max-w-4xl">
      {present.map((e) => (
        <p key={e.id} className="text-xs text-muted-foreground">
          <span className="font-medium text-foreground">{e.short}</span> — {e.note}
        </p>
      ))}
    </div>
  )
}

/** Swatches come from shadeFor, so the key cannot drift from the cells. */
function Legend() {
  return (
    <div className="mt-3 flex flex-wrap items-center gap-x-6 gap-y-2 text-[11px] text-muted-foreground">
      <span className="flex items-center gap-1">
        slower
        {[-1, 5, 15, 25, 35, 45].map((pct) => (
          <span key={pct} className={cn('w-5 h-3 border border-border/40', shadeFor(pct))} />
        ))}
        40%+ faster on DoubleZero
      </span>
      <span>no path — DoubleZero cannot route between the two metros</span>
      <span>not measured — no public-internet samples in the window</span>
      <span>withheld — a hop reports a contracted figure, so no percentage is claimed</span>
      <span>N/A — no committed DoubleZero coverage at that location</span>
    </div>
  )
}

function EndpointSelect({
  value,
  onChange,
  options,
  loading,
  disabled,
}: {
  value: string
  onChange: (v: string) => void
  options: { onNet: { value: string; label: string }[]; offNet: { value: string; label: string }[] }
  loading: boolean
  disabled: boolean
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      disabled={loading || disabled}
      aria-label="Add a location"
      className="min-w-[14rem] px-3 py-1.5 text-xs border border-border rounded-md bg-background hover:bg-muted transition-colors disabled:opacity-50"
    >
      <option value="">Add a location…</option>
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
  )
}
