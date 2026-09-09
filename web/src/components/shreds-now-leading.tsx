import { useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'

import type { EdgeScoreboardLeader, EdgeScoreboardSlotRace } from '@/lib/api'
import { FEED_COLORS } from '@/lib/feed-colors'
import { leaderName, leaderPlace, leaderRuns, type LeaderRun } from './shreds-leader-runs'

// A leader holds four slots at ~400ms each, so a turn is ~1.6s of chain time.
// The panel advances at that cadence so one card really is one leader's turn:
// the buffer holds ~80s of slots and the payload refetches every 30s, so
// playing forward at chain speed arrives at the newest run about when the next
// refetch lands, and the panel stays level with the chain on its own.
const RUN_MS = 1600
const TICK_MS = 50

const TAPE_ROWS = 6

// How far back playback restarts from. The buffer holds ~50 runs and the payload
// refetches every 30s, so replaying the trailing ~20 (about 32s of chain time)
// keeps runway ahead of the cursor at all times and keeps the content recent.
const WINDOW_RUNS = 20

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

  // The cursor is a SLOT, not an index. Every refetch rebuilds the runs array
  // with new entries on the end and old ones dropped off the front, so an index
  // silently points at a different leader afterwards. A slot number survives
  // that, and when it finally ages out of the buffer the lookup fails and the
  // panel snaps to live — which is the right answer for a cursor that old.
  const [cursorSlot, setCursorSlot] = useState<number | null>(null)
  const [held, setHeld] = useState(false)
  const [elapsed, setElapsed] = useState(0)

  const index = useMemo(() => {
    if (cursorSlot === null) return runs.length - 1
    const found = runs.findIndex((r) => r.slots[0].slot === cursorSlot)
    return found === -1 ? runs.length - 1 : found
  }, [runs, cursorSlot])

  const runsRef = useRef(runs)
  runsRef.current = runs
  const indexRef = useRef(index)
  indexRef.current = index
  const heldRef = useRef(held)
  heldRef.current = held

  useEffect(() => {
    const id = setInterval(() => {
      if (heldRef.current) return
      setElapsed((e) => {
        if (e + TICK_MS < RUN_MS) return e + TICK_MS
        const rs = runsRef.current
        // Forward in chain order, and back to the top of the trailing window on
        // reaching the newest run. Pinning to the newest instead leaves the card
        // motionless between refetches, which is a live panel that never moves.
        const next = rs[indexRef.current + 1] ?? rs[Math.max(0, rs.length - WINDOW_RUNS)]
        setCursorSlot(next ? next.slots[0].slot : null)
        return 0
      })
    }, TICK_MS)
    return () => clearInterval(id)
  }, [])

  const current: LeaderRun | undefined = runs[index]
  const tape = runs.slice(Math.max(0, index - TAPE_ROWS), index).reverse()
  const isLive = index >= runs.length - 1
  const behind = Math.max(0, runs.length - 1 - index)

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
        <span className="text-xs text-muted-foreground tabular-nums">
          {held
            ? 'held — release to resume'
            : isLive
              ? 'live · hover to hold'
              : `${(behind * (RUN_MS / 1000)).toFixed(0)}s behind · catching up`}
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
        className="relative overflow-hidden grid grid-cols-1 sm:grid-cols-[1fr_auto] gap-5 items-center px-4 py-5"
      >
        <span
          aria-hidden="true"
          className="pointer-events-none absolute inset-0 motion-safe:animate-[leaderSweep_.85s_cubic-bezier(.4,0,.2,1)] motion-reduce:hidden"
          style={{ background: 'linear-gradient(100deg, transparent 32%, rgba(16,185,129,.13) 50%, transparent 68%)' }}
        />
        <div className="min-w-0 motion-safe:animate-[fadeIn_.42s_cubic-bezier(.2,.8,.2,1)_both]">
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
          <div className="text-xs text-muted-foreground truncate mt-0.5 motion-safe:animate-[fadeIn_.42s_cubic-bezier(.2,.8,.2,1)_.05s_both]">
            {[place || 'location unknown', current.leader?.asn_org].filter(Boolean).join(' · ')}
          </div>
          <div className="text-[11px] font-mono text-muted-foreground/70 truncate mt-1.5 motion-safe:animate-[fadeIn_.42s_cubic-bezier(.2,.8,.2,1)_.1s_both]">{current.pubkey}</div>
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
          <div className="text-right motion-safe:animate-[fadeIn_.42s_cubic-bezier(.2,.8,.2,1)_.08s_both]">
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
          {tape.map((run, i) => (
            <div
              key={run.key}
              className={`${i === 0 ? 'motion-safe:animate-[slideDown_.45s_cubic-bezier(.2,.8,.2,1)_both] ' : ''}grid grid-cols-[1fr_auto] sm:grid-cols-[1fr_110px_90px_56px] gap-3 items-center px-4 py-1.5 border-t border-border/50 text-xs hover:bg-muted/30 transition-colors`}
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
