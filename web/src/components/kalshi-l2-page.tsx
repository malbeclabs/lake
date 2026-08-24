import { useCallback, useEffect, useMemo, useState } from 'react'
import { Layers } from 'lucide-react'
import { PageHeader } from './page-header'
import {
  fetchKalshiL2Completeness,
  fetchKalshiL2Coverage,
  type KalshiL2CompletenessResponse,
  type KalshiL2CoverageResponse,
  type KalshiL2Day,
  type KalshiL2Lane,
} from '@/lib/api'

// Section order. Lanes arrive pre-sorted from the API; this only groups them into cards and
// keeps a stable order for categories. An unrecognised category still renders, appended last,
// so a lane can never be silently dropped by the UI.
const CATEGORY_ORDER = ['Perps', 'Football', 'Basketball', 'Baseball', 'Hockey', 'Soccer', 'Other']

// A lane whose last message is this much older than the payload is called out. It is not
// necessarily a fault — a league out of season has nothing to publish — so it is labelled
// "quiet", not "down".
//
// Measured against the payload's own generated_at, NOT wall clock. This view is served
// cache-first from an entry the background refresher rewrites every 10 minutes, so by wall
// clock a perfectly healthy lane is routinely five to ten minutes stale and every row would
// render quiet for most of each cycle — an alarm that is always on, which is the same as no
// alarm at all.
const QUIET_AFTER_MS = 5 * 60 * 1000

function rate(n: number): string {
  if (n === 0) return '0'
  if (n < 1) return n.toFixed(2)
  if (n < 100) return n.toFixed(1)
  return Math.round(n).toLocaleString()
}

function relAge(tsMs: number, nowMs: number): string {
  const age = Math.round((nowMs - tsMs) / 1000)
  if (age < 5) return 'just now'
  if (age < 60) return `${age}s ago`
  if (age < 3600) return `${Math.round(age / 60)}m ago`
  return `${Math.round(age / 3600)}h ago`
}

// The FIX/WS split is per publisher host: publisher index = channel_id / 100, instrument set =
// channel_id % 100. Sports lanes each carry their own instrument set, so the raw id is the
// honest label here rather than a guessed transport name.
function channelLabel(channelID: number): string {
  return `ch ${channelID}`
}

function LaneRow({ lane, asOf }: { lane: KalshiL2Lane; asOf: number }) {
  const lastSeen = new Date(lane.last_seen).getTime()
  const quiet = lane.seen && asOf - lastSeen > QUIET_AFTER_MS
  // Books, not messages: gap_messages is a recovery DURATION scaled by the message rate, so
  // it cannot be compared between lanes and must never be shown as a fault count. The
  // measurement behind that is on KalshiL2Lane (api/handlers/kalshi_l2_coverage.go).
  const faults = lane.gap_books + lane.resets
  // The duration half, kept but labelled as what it is: the share of messages that arrived
  // while their book was un-anchored.
  const unanchoredPct = lane.messages > 0 ? (100 * lane.gap_messages) / lane.messages : 0
  return (
    <tr className="border-b border-border transition-colors last:border-b-0 hover:bg-muted/50">
      <td className="px-3 py-3 sm:px-4">
        <div className="text-sm font-medium">{lane.label}</div>
        <div className="text-xs text-muted-foreground">
          {lane.source} · {channelLabel(lane.channel_id)} · {lane.location_code}
        </div>
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        {lane.seen ? (
          <>
            <span className={quiet ? 'text-amber-500' : 'text-muted-foreground'}>
              {relAge(lastSeen, asOf)}
            </span>
            {quiet && <div className="text-[11px] text-amber-500/70">quiet</div>}
          </>
        ) : (
          // Neutral, not amber: a roster lane with nothing in the window is most often a league
          // out of season, which is not a fault. A lane that WAS publishing and stopped is the
          // failure this page exists to catch, and that one is flagged as quiet above.
          <>
            <span className="text-muted-foreground">—</span>
            <div className="text-[11px] text-muted-foreground/70">no data in window</div>
          </>
        )}
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        {rate(lane.level_updates_per_sec)}
        <span className="ml-1 text-xs text-muted-foreground">/s</span>
        <div className="text-[11px] text-muted-foreground/70">{rate(lane.messages_per_sec)}/s all msgs</div>
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        {lane.instruments.toLocaleString()}
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        {lane.depth_p50.toFixed(1)}
        <span className="ml-1 text-muted-foreground">({lane.depth_p95.toFixed(1)})</span>
        <div className="text-[11px] text-muted-foreground/70">max {lane.depth_max.toLocaleString()}</div>
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        {lane.snapshot_cycles.toLocaleString()}
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        <span className={faults > 0 ? 'text-amber-500' : ''}>{faults.toLocaleString()}</span>
        <div className="text-[11px] text-muted-foreground/70">
          {lane.gap_books.toLocaleString()} gapped · {lane.resets.toLocaleString()} reset ·{' '}
          {lane.clears.toLocaleString()} clear
        </div>
        {lane.gap_messages > 0 && (
          <div className="text-[11px] text-muted-foreground/70">
            {unanchoredPct.toFixed(1)}% un-anchored
          </div>
        )}
      </td>
    </tr>
  )
}

// UTC, because the day is a UTC calendar day (the table's partition key) and the browser's
// local day would relabel every row for anyone west of Greenwich.
function utcTime(iso: string): string {
  const d = new Date(iso)
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`
}

// A day is only sellable as history if every book that moved stayed anchored and had a snapshot
// inside the day to replay from. Today is neither: it is still being written, so it is labelled
// rather than judged.
//
// A clean verdict is NOT a claim that the capture was up all day. Nothing in the data separates
// a closed market from a dead socket, so no coverage figure is derived from it — see the header
// of api/handlers/kalshi_l2_completeness.go. First and last message are shown for that reason.
function verdictOf(day: KalshiL2Day, today: string): { label: string; className: string } {
  if (day.day >= today) return { label: 'in progress', className: 'text-muted-foreground' }
  if (day.gapped_instruments > 0 || day.unanchored_instruments > 0) {
    return { label: 'incomplete', className: 'text-amber-500' }
  }
  return { label: 'replayable', className: 'text-emerald-500' }
}

function CompletenessTable({ data }: { data: KalshiL2CompletenessResponse }) {
  const today = useMemo(() => new Date(data.generated_at).toISOString().slice(0, 10), [data.generated_at])
  return (
    <div className="mb-6 overflow-hidden rounded-lg border border-border bg-card">
      <div className="border-b border-border px-4 py-3">
        <div className="text-sm font-medium text-muted-foreground">Daily completeness</div>
        <p className="mt-1 text-xs leading-relaxed text-muted-foreground/70">
          Whether each day's captured levels can be replayed into a book, which is what makes a
          day usable as history. A gapped book has a hole in its delta stream that no recorder of
          that lane can fill. A book with no snapshot in the day cannot start a replay from that
          day alone. Last {data.day_count} days.
        </p>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full">
          <thead>
            <tr className="border-b border-border text-left text-sm text-muted-foreground">
              <th className="whitespace-nowrap px-3 py-3 font-medium sm:px-4">Day</th>
              <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Lanes</th>
              <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Instruments</th>
              <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">
                Gapped
                <span className="block text-xs font-normal">books</span>
              </th>
              <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">
                No snapshot
                <span className="block text-xs font-normal">books</span>
              </th>
              <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Messages</th>
              <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">
                First / last
                <span className="block text-xs font-normal">UTC</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {data.days.map((day) => {
              const verdict = verdictOf(day, today)
              return (
                <tr key={day.day} className="border-b border-border transition-colors last:border-b-0 hover:bg-muted/50">
                  <td className="whitespace-nowrap px-3 py-3 sm:px-4">
                    <div className="text-sm font-medium tabular-nums">{day.day}</div>
                    <div className={`text-[11px] ${verdict.className}`}>{verdict.label}</div>
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                    {day.lanes.toLocaleString()}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                    {day.instruments.toLocaleString()}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                    <span className={day.gapped_instruments > 0 ? 'text-amber-500' : ''}>
                      {day.gapped_instruments.toLocaleString()}
                    </span>
                    {day.gap_lanes.length > 0 && (
                      <div className="text-[11px] text-muted-foreground/70">{day.gap_lanes.join(', ')}</div>
                    )}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                    <span className={day.unanchored_instruments > 0 ? 'text-amber-500' : ''}>
                      {day.unanchored_instruments.toLocaleString()}
                    </span>
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                    {day.messages.toLocaleString()}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
                    {utcTime(day.first_message)} – {utcTime(day.last_message)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function KalshiL2Page() {
  const [data, setData] = useState<KalshiL2CoverageResponse | null>(null)
  const [days, setDays] = useState<KalshiL2CompletenessResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [now, setNow] = useState(() => Date.now())

  const load = useCallback(async () => {
    try {
      setData(await fetchKalshiL2Coverage())
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [])

  // Loaded separately and its failure is not this page's error: the coverage view is what the
  // page is for, and it must still render when the heavier daily query times out. A failed poll
  // keeps the payload it already had — the table is a catalog of finished days, so the last one
  // read is still true, and blanking it would turn a blip into a missing section.
  const loadDays = useCallback(async () => {
    try {
      setDays(await fetchKalshiL2Completeness())
    } catch {
      // keep the previous payload
    }
  }, [])

  // The coverage view is a live 15-minute average and polls. The daily table is not: the server
  // recomputes it hourly, so re-fetching it every 30s would only re-serve the same cached row.
  useEffect(() => { void loadDays() }, [loadDays])

  useEffect(() => {
    let active = true
    void load()
    const poll = setInterval(() => { void load() }, 30000)
    const tick = setInterval(() => active && setNow(Date.now()), 5000)
    return () => { active = false; clearInterval(poll); clearInterval(tick) }
  }, [load])

  const sections = useMemo(() => {
    const byCategory = new Map<string, KalshiL2Lane[]>()
    for (const lane of data?.lanes ?? []) {
      const list = byCategory.get(lane.category)
      if (list) list.push(lane)
      else byCategory.set(lane.category, [lane])
    }
    const known = CATEGORY_ORDER.filter((c) => byCategory.has(c))
    const extra = [...byCategory.keys()].filter((c) => !CATEGORY_ORDER.includes(c)).sort()
    return [...known, ...extra].map((category) => ({ category, lanes: byCategory.get(category) ?? [] }))
  }, [data?.lanes])

  // asOf is when the payload was computed, not now: every staleness judgement below is made
  // relative to the data's own clock (see QUIET_AFTER_MS). With no payload clock we fall back to
  // the ticking `now` state rather than reading Date.now() during render.
  const asOf = useMemo(
    () => (data?.generated_at ? new Date(data.generated_at).getTime() : now),
    [data?.generated_at, now],
  )

  const totals = useMemo(() => {
    const lanes = data?.lanes ?? []

    // Instruments are counted per instrument SET, not per lane. A lane is one (source,
    // channel_id) pair and instrument_id is unique only within a channel, so the API cannot
    // collapse the arms — but the two publisher arms of one source carry the SAME instruments
    // (channel_id / 100 is the publisher index, channel_id % 100 the instrument set), so
    // summing lanes reports twice the real coverage for every dual-arm lane. Take the widest
    // arm within each set, then sum the sets.
    const perSet = new Map<string, number>()
    for (const l of lanes) {
      const key = `${l.source}:${l.channel_id % 100}`
      perSet.set(key, Math.max(perSet.get(key) ?? 0, l.instruments))
    }

    // Every tile below is per LANE, while a row is per (lane, vantage): the same stream
    // recorded at two vantages is two rows, so summing rows would report the recording
    // fan-out as if it were traffic. Rates take the widest vantage rather than the sum —
    // each vantage sees the same publisher, so the largest is the best observation of it
    // and a lossy vantage cannot inflate the total. Counts are of distinct lanes.
    const ratePerLane = new Map<string, number>()
    const seenByLane = new Map<string, { seen: boolean; lastSeen: number }>()
    for (const l of lanes) {
      const key = `${l.source}:${l.channel_id}`
      ratePerLane.set(key, Math.max(ratePerLane.get(key) ?? 0, l.level_updates_per_sec))
      // A lane counts as seen if ANY vantage heard it, and its freshness is the newest
      // vantage: one dead recorder must not make a live lane read as quiet.
      const prev = seenByLane.get(key)
      const lastSeen = l.seen ? new Date(l.last_seen).getTime() : 0
      seenByLane.set(key, {
        seen: (prev?.seen ?? false) || l.seen,
        lastSeen: Math.max(prev?.lastSeen ?? 0, lastSeen),
      })
    }

    // A lane that never published in the window is not the same thing as one that went silent:
    // most of the roster is out of season at any time, and folding them together would leave
    // this tile reading ~20 year-round, which is an alarm that is always on. Only lanes that
    // were heard from and then stopped are "quiet" — the same rule LaneRow uses. Never-seen
    // lanes get their own count so a lane that never started after a deploy is still visible.
    const byLane = [...seenByLane.values()]
    return {
      lanes: seenByLane.size,
      instruments: [...perSet.values()].reduce((sum, n) => sum + n, 0),
      updatesPerSec: [...ratePerLane.values()].reduce((sum, n) => sum + n, 0),
      quiet: byLane.filter((l) => l.seen && asOf - l.lastSeen > QUIET_AFTER_MS).length,
      neverSeen: byLane.filter((l) => !l.seen).length,
    }
  }, [data?.lanes, asOf])

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-8">
        <PageHeader
          icon={Layers}
          title="Kalshi Sports L2"
          subtitle={
            <span className="flex items-center gap-2 text-xs text-muted-foreground/50">
              <span>last {data?.window_minutes ?? 15} min</span>
              {data?.generated_at && <span>· computed {relAge(asOf, now)}</span>}
            </span>
          }
        />

        {error && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-red-500">{error}</div>
        )}
        {!data && !error && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">Loading…</div>
        )}

        {data && data.lanes.length === 0 && (
          <div className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
            No market-by-price messages recorded in the last {data.window_minutes} minutes.
          </div>
        )}

        {days && days.days.length > 0 && <CompletenessTable data={days} />}

        {data && data.lanes.length > 0 && (
          <>
            <div className="mb-8 rounded-lg border border-border bg-card p-4 sm:p-6">
              <p className="text-sm leading-relaxed text-muted-foreground">
                Coverage of the market-by-price lanes DoubleZero's Kalshi edge publisher carries.
                These markets have no public comparison feed, so this reports what the lanes are
                delivering — update rates, instrument counts, real book depth, and faults — rather
                than a head-to-head race. Rates are averaged over the last {data.window_minutes} minutes.
              </p>
              <div className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-3 border-t border-border pt-4">
                <div>
                  <div className="mb-1 text-xs text-muted-foreground">Lanes</div>
                  <div className="text-xl font-semibold tabular-nums sm:text-2xl">{totals.lanes}</div>
                </div>
                <div className="sm:border-l sm:border-border sm:pl-6">
                  <div className="mb-1 text-xs text-muted-foreground">Instruments</div>
                  <div className="text-xl font-semibold tabular-nums sm:text-2xl">{totals.instruments.toLocaleString()}</div>
                </div>
                <div className="sm:border-l sm:border-border sm:pl-6">
                  <div className="mb-1 text-xs text-muted-foreground">Level updates</div>
                  <div className="text-xl font-semibold tabular-nums sm:text-2xl">
                    {rate(totals.updatesPerSec)}
                    <span className="ml-1 text-xs font-normal text-muted-foreground">/s</span>
                  </div>
                </div>
                <div className="sm:border-l sm:border-border sm:pl-6">
                  <div className="mb-1 text-xs text-muted-foreground">Quiet lanes</div>
                  <div className={`text-xl font-semibold tabular-nums sm:text-2xl ${totals.quiet > 0 ? 'text-amber-500' : ''}`}>
                    {totals.quiet}
                  </div>
                  {totals.neverSeen > 0 && (
                    <div className="mt-0.5 text-[11px] text-muted-foreground/70">
                      {totals.neverSeen} never seen in window
                    </div>
                  )}
                </div>
              </div>
            </div>

            {sections.map((section) => (
              <div key={section.category} className="mb-6 overflow-hidden rounded-lg border border-border bg-card">
                <div className="border-b border-border px-4 py-3 text-sm font-medium text-muted-foreground">
                  {section.category}
                </div>
                <div className="overflow-x-auto">
                  <table className="min-w-full">
                    <thead>
                      <tr className="border-b border-border text-left text-sm text-muted-foreground">
                        <th className="whitespace-nowrap px-3 py-3 font-medium sm:px-4">Lane</th>
                        <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Last message</th>
                        <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">
                          Level updates
                          <span className="block text-xs font-normal">per second</span>
                        </th>
                        <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Instruments</th>
                        <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">
                          Book depth
                          <span className="block text-xs font-normal">p50 (p95)</span>
                        </th>
                        <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Snapshots</th>
                        <th className="whitespace-nowrap px-3 py-3 text-right font-medium sm:px-4">Faults</th>
                      </tr>
                    </thead>
                    <tbody>
                      {section.lanes.map((lane) => (
                        <LaneRow
                          key={`${lane.source}:${lane.channel_id}:${lane.measurement_node_id}`}
                          lane={lane}
                          asOf={asOf}
                        />
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </>
        )}
      </div>
    </div>
  )
}
