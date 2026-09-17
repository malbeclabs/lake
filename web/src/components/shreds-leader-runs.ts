import type { EdgeScoreboardLeader, EdgeScoreboardSlotRace } from '@/lib/api'

// `dz_edge` is the server's aggregate of the other three on a shared per-host
// denominator, so it is used INSTEAD of them, never alongside — summing both
// counts DoubleZero's shreds twice.
const DZ_EDGE = 'dz_edge'
const DZ_PARTS = ['dz', 'dz_root', 'dz_retransmit']
const TURBINE = 'turbine'

export type LeaderSlot = { slot: number; winPct: number | null }

export type LeaderRun = {
  key: string
  pubkey: string
  leader?: EdgeScoreboardLeader
  slots: LeaderSlot[]
  /** Mean of the slots that had a reading, or null when none did. */
  winPct: number | null
}

/**
 * Per-slot win rate against Turbine, summed over every recording node that saw
 * the slot. Summing rather than averaging ratios weights a slot by how many
 * vantages actually recorded it, so a slot one node saw does not carry the same
 * authority as one all eight saw.
 */
export function slotWinRatesVsTurbine(races: EdgeScoreboardSlotRace[]): Map<number, number> {
  const totals = new Map<number, { edge: number; turbine: number }>()
  const perHost = new Map<string, { edge: number; parts: number; turbine: number }>()

  for (const r of races) {
    const k = `${r.slot}:${r.host}`
    let h = perHost.get(k)
    if (!h) {
      h = { edge: 0, parts: 0, turbine: 0 }
      perHost.set(k, h)
    }
    if (r.feed === DZ_EDGE) h.edge += r.win_pct
    else if (DZ_PARTS.includes(r.feed)) h.parts += r.win_pct
    else if (r.feed === TURBINE) h.turbine += r.win_pct
  }

  for (const [k, h] of perHost) {
    const slot = Number(k.slice(0, k.indexOf(':')))
    let t = totals.get(slot)
    if (!t) {
      t = { edge: 0, turbine: 0 }
      totals.set(slot, t)
    }
    t.edge += h.edge > 0 ? h.edge : h.parts
    t.turbine += h.turbine
  }

  const out = new Map<number, number>()
  for (const [slot, t] of totals) {
    if (t.edge + t.turbine <= 0) continue
    out.set(slot, (t.edge / (t.edge + t.turbine)) * 100)
  }
  return out
}

/**
 * Consecutive slots grouped into the leader runs they belong to, oldest first.
 *
 * A Solana leader holds four consecutive slots, but the grouping keys on the
 * leader's pubkey rather than counting to four: the buffer starts mid-run and
 * can be missing slots, and a fixed stride would then split one leader's run
 * across two cards and attribute the tail to whoever came next.
 *
 * Slots with no known leader are dropped rather than grouped under a shared
 * blank key, which would otherwise fuse unrelated runs into one long card.
 */
export function leaderRuns(
  races: EdgeScoreboardSlotRace[],
  leaders: Record<string, EdgeScoreboardLeader> | undefined
): LeaderRun[] {
  if (!leaders) return []

  const winRates = slotWinRatesVsTurbine(races)
  const slotNums = [...new Set(races.map((r) => r.slot))].sort((a, b) => a - b)

  const runs: LeaderRun[] = []
  for (const slot of slotNums) {
    const leader = leaders[String(slot)]
    if (!leader?.pubkey) continue

    const winPct = winRates.get(slot) ?? null
    const last = runs[runs.length - 1]
    if (last && last.pubkey === leader.pubkey && slot - last.slots[last.slots.length - 1].slot <= 4) {
      last.slots.push({ slot, winPct })
    } else {
      runs.push({
        key: `${leader.pubkey}:${slot}`,
        pubkey: leader.pubkey,
        leader,
        slots: [{ slot, winPct }],
      } as LeaderRun)
    }
  }

  for (const run of runs) {
    const seen = run.slots.map((s) => s.winPct).filter((v): v is number => v !== null)
    run.winPct = seen.length ? seen.reduce((a, b) => a + b, 0) / seen.length : null
  }
  return runs
}

/** "Jupiter", or a short pubkey when the validator publishes no name. */
export function leaderName(run: LeaderRun): string {
  const name = run.leader?.name?.trim()
  if (name) return name
  return `${run.pubkey.slice(0, 4)}…${run.pubkey.slice(-4)}`
}

export function leaderPlace(run: LeaderRun): string {
  return [run.leader?.city, run.leader?.country].filter(Boolean).join(', ')
}
