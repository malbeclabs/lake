import { useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'

import type { EdgeScoreboardLeader, EdgeScoreboardSlotRace } from '@/lib/api'
import { FEED_COLORS } from '@/lib/feed-colors'
import { leaderName, leaderPlace, leaderRuns, type LeaderRun } from './shreds-leader-runs'

// One leader holds four slots at ~400ms each, so a run is about 1.6s of real
// time. Advancing that fast is unreadable; 2.6s is slow enough to read a name
// and still keeps the panel visibly moving.
const RUN_MS = 2600
const TICK_MS = 65

const TAPE_ROWS = 6

/**
 * Win rates against Turbine sit in the high 90s and the spread between the best
 * and worst leader is often under four points, so a 0–100 scale draws every bar
 * as the same full block. The floor is taken from the data in view instead of a
 * constant: a fixed one is either too low to separate today's values or too high
 * to hold tomorrow's. Never above 99, so a genuinely uniform window still reads
 * as "all high" rather than manufacturing contrast out of rounding noise.
 */
function barScale(values: (number | null)[]): (pct: number | null) => number {
  const seen = values.filter((v): v is number => v !== null)
  const floor = seen.length ? Math.min(99, Math.floor(Math.min(...seen) * 2) / 2 - 0.5) : 0
  const span = 100 - floor
  return (pct) => {
    if (pct === null) return 0
    if (span <= 0) return 1
    return Math.min(1, Math.max(0, (pct - floor) / span))
  }
}

function fmt(pct: number | null): string {
  return pct === null ? '—' : `${pct.toFixed(1)}%`
}

export function ShredsNowLeading({
  slots,
  leaders,
}: {
  slots: EdgeScoreboardSlotRace[]
  leaders?: Record<string, EdgeScoreboardLeader>
}) {
  const runs = useMemo(() => leaderRuns(slots, leaders), [slots, leaders])
  const scale = useMemo(
    () => barScale(runs.flatMap((r) => [r.winPct, ...r.slots.map((s) => s.winPct)])),
    [runs]
  )

  const [cursor, setCursor] = useState(0)
  const [held, setHeld] = useState(false)
  const [elapsed, setElapsed] = useState(0)

  // Park at the newest run whenever the buffer changes, so a refetch lands the
  // panel on live data rather than wherever the cursor had wandered to.
  useEffect(() => {
    setCursor(runs.length - 1)
    setElapsed(0)
  }, [runs.length])

  useEffect(() => {
    if (runs.length < 2) return
    const id = setInterval(() => {
      if (held) return
      setElapsed((e) => {
        if (e + TICK_MS < RUN_MS) return e + TICK_MS
        // Walk backwards through history, then snap back to live.
        setCursor((c) => (c <= 0 ? runs.length - 1 : c - 1))
        return 0
      })
    }, TICK_MS)
    return () => clearInterval(id)
  }, [runs.length, held])

  const current: LeaderRun | undefined = runs[cursor] ?? runs[runs.length - 1]
  const tape = current ? runs.slice(Math.max(0, cursor + 1), cursor + 1 + TAPE_ROWS) : []
  const isLive = cursor === runs.length - 1

  // Count the win rate up from the previous card's figure on every change.
  const [shown, setShown] = useState<number | null>(null)
  const fromRef = useRef<number | null>(null)
  useEffect(() => {
    const to = current?.winPct ?? null
    if (to === null) {
      setShown(null)
      return
    }
    const from = fromRef.current ?? to
    const start = performance.now()
    let raf = 0
    const step = (t: number) => {
      const k = Math.min(1, (t - start) / 420)
      const eased = 1 - Math.pow(1 - k, 3)
      setShown(from + (to - from) * eased)
      if (k < 1) raf = requestAnimationFrame(step)
      else fromRef.current = to
    }
    raf = requestAnimationFrame(step)
    return () => cancelAnimationFrame(raf)
  }, [current?.key, current?.winPct])

  if (!current) return null

  const first = current.slots[0].slot
  const last = current.slots[current.slots.length - 1].slot
  const place = leaderPlace(current)

  return (
    <div className="border border-border rounded-lg bg-card overflow-hidden mb-6">
      <div className="flex items-baseline justify-between gap-3 px-4 py-3 flex-wrap">
        <h2 className="text-sm font-semibold flex items-center gap-2">
          <span className="relative flex items-center">
            {isLive && !held && (
              <span className="absolute inline-flex h-2 w-2 rounded-full bg-emerald-400 opacity-75 motion-safe:animate-ping" />
            )}
            <span className={`relative inline-flex rounded-full h-2 w-2 ${isLive ? 'bg-emerald-500' : 'bg-emerald-500/30'}`} />
          </span>
          Now Leading
        </h2>
        <span className="text-xs text-muted-foreground">
          {held ? 'held' : isLive ? 'live · hover to hold' : 'recent slots · hover to hold'}
        </span>
      </div>

      <div className="h-0.5 bg-muted-foreground/15">
        <div
          className="h-full bg-emerald-500/60"
          style={{ width: `${(elapsed / RUN_MS) * 100}%` }}
        />
      </div>

      <div
        key={current.key}
        onMouseEnter={() => setHeld(true)}
        onMouseLeave={() => setHeld(false)}
        className="relative overflow-hidden grid grid-cols-1 sm:grid-cols-[1fr_auto] gap-5 items-center px-4 py-5 motion-safe:animate-[fadeIn_.35s_ease-out]"
      >
        <div className="min-w-0">
          {current.leader?.pubkey ? (
            <Link
              to={`/solana/gossip-nodes/${current.leader.pubkey}`}
              state={{ back: { to: '/dz/shreds/scoreboard', label: 'Shreds Scoreboard' } }}
              className="block text-lg sm:text-xl font-semibold tracking-tight truncate hover:text-emerald-400 transition-colors"
            >
              {leaderName(current)}
            </Link>
          ) : (
            <div className="text-lg sm:text-xl font-semibold tracking-tight truncate">{leaderName(current)}</div>
          )}
          <div className="text-xs text-muted-foreground truncate mt-0.5">
            {[place || 'location unknown', current.leader?.asn_org].filter(Boolean).join(' · ')}
          </div>
          <div className="text-[11px] font-mono text-muted-foreground/70 truncate mt-1.5">{current.pubkey}</div>
        </div>

        <div className="flex items-end gap-4 shrink-0">
          <div className="flex items-end gap-1 h-11" aria-hidden="true">
            {current.slots.map((s, i) => (
              <div
                key={s.slot}
                title={`Slot ${s.slot.toLocaleString()} · ${fmt(s.winPct)} vs Turbine`}
                className="w-3 rounded-t-sm motion-safe:animate-[growUp_.45s_cubic-bezier(.2,.85,.25,1)_both] origin-bottom"
                style={{
                  height: `${Math.max(3, scale(s.winPct) * 44)}px`,
                  backgroundColor: FEED_COLORS.dz_edge,
                  opacity: 0.85,
                  animationDelay: `${i * 60}ms`,
                }}
              />
            ))}
          </div>
          <div className="text-right">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Win rate vs Turbine</div>
            <div className="text-2xl font-semibold tabular-nums leading-tight" style={{ color: FEED_COLORS.dz_edge }}>
              {fmt(shown ?? current.winPct)}
            </div>
            <div className="text-[10px] font-mono text-muted-foreground tabular-nums">
              {first === last ? `slot ${first.toLocaleString()}` : `slots ${first.toLocaleString()}–${last.toLocaleString()}`}
            </div>
          </div>
        </div>
      </div>

      {tape.length > 0 && (
        <>
          <div className="flex items-baseline justify-between gap-2 px-4 pt-3 pb-2 border-t border-border text-[10px] uppercase tracking-wider text-muted-foreground">
            <span>Previous leaders</span>
            <span>win rate vs Turbine</span>
          </div>
          {tape.map((run) => (
            <div
              key={run.key}
              className="grid grid-cols-[1fr_auto] sm:grid-cols-[1fr_110px_90px_56px] gap-3 items-center px-4 py-1.5 border-t border-border/50 text-xs hover:bg-muted/30 transition-colors"
            >
              <span className="truncate text-muted-foreground">{leaderName(run)}</span>
              <span className="hidden sm:block font-mono tabular-nums text-[11px] text-muted-foreground/60">
                {run.slots[0].slot.toLocaleString()}
              </span>
              <span className="hidden sm:block h-1 rounded-full bg-muted-foreground/20 overflow-hidden">
                <span
                  className="block h-full rounded-full"
                  style={{ width: `${Math.max(2, scale(run.winPct) * 100)}%`, backgroundColor: FEED_COLORS.dz_edge, opacity: 0.75 }}
                />
              </span>
              <span className="font-mono tabular-nums text-right">{fmt(run.winPct)}</span>
            </div>
          ))}
        </>
      )}
    </div>
  )
}
