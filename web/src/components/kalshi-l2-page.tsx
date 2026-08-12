import { useCallback, useEffect, useMemo, useState } from 'react'
import { Layers } from 'lucide-react'
import { PageHeader } from './page-header'
import { fetchKalshiL2Coverage, type KalshiL2CoverageResponse, type KalshiL2Lane } from '@/lib/api'

// Section order. Lanes arrive pre-sorted from the API; this only groups them into cards and
// keeps a stable order for categories. An unrecognised category still renders, appended last,
// so a lane can never be silently dropped by the UI.
const CATEGORY_ORDER = ['Perps', 'Football', 'Basketball', 'Baseball', 'Hockey', 'Soccer', 'Other']

// A lane whose last message is older than this is called out. It is not necessarily a fault —
// a league out of season has nothing to publish — so it is labelled "quiet", not "down".
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

function LaneRow({ lane, now }: { lane: KalshiL2Lane; now: number }) {
  const lastSeen = new Date(lane.last_seen).getTime()
  const quiet = now - lastSeen > QUIET_AFTER_MS
  const faults = lane.gaps + lane.resets
  return (
    <tr className="border-b border-border transition-colors last:border-b-0 hover:bg-muted/50">
      <td className="px-3 py-3 sm:px-4">
        <div className="text-sm font-medium">{lane.label}</div>
        <div className="text-xs text-muted-foreground">
          {lane.source} · {channelLabel(lane.channel_id)} · {lane.location_code}
        </div>
      </td>
      <td className="whitespace-nowrap px-3 py-3 text-right text-sm tabular-nums sm:px-4">
        <span className={quiet ? 'text-amber-500' : 'text-muted-foreground'}>
          {relAge(lastSeen, now)}
        </span>
        {quiet && <div className="text-[11px] text-amber-500/70">quiet</div>}
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
          {lane.gaps.toLocaleString()} gap · {lane.resets.toLocaleString()} reset · {lane.clears.toLocaleString()} clear
        </div>
      </td>
    </tr>
  )
}

export function KalshiL2Page() {
  const [data, setData] = useState<KalshiL2CoverageResponse | null>(null)
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

  useEffect(() => {
    let active = true
    const run = () => { void load() }
    run()
    const poll = setInterval(run, 30000)
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

  const totals = useMemo(() => {
    const lanes = data?.lanes ?? []
    return {
      lanes: lanes.length,
      instruments: lanes.reduce((sum, l) => sum + l.instruments, 0),
      updatesPerSec: lanes.reduce((sum, l) => sum + l.level_updates_per_sec, 0),
      quiet: lanes.filter((l) => now - new Date(l.last_seen).getTime() > QUIET_AFTER_MS).length,
    }
  }, [data?.lanes, now])

  return (
    <div className="flex-1 overflow-auto">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-8">
        <PageHeader
          icon={Layers}
          title="Kalshi Sports L2"
          subtitle={
            <span className="flex items-center gap-2 text-xs text-muted-foreground/50">
              <span>last {data?.window_minutes ?? 15} min</span>
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
                        <LaneRow key={`${lane.source}:${lane.channel_id}`} lane={lane} now={now} />
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
