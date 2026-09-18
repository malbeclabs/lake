import type { EdgeMulticastRecorderLoss } from '@/lib/api'

// The loss strip's text, out of the component file so it can be tested without driving a hover to
// assert a string — and it has to be: the caveats that live only here are what a reader needs to
// not misread the number beside them.

/** The axis gap episodes are drawn on: the payload's own clock and how wide its window is. */
export type GapWindow = { endMs: number; secs: number }

// The rule set's five verdicts, worst attribution first: the reader's question, not the order the
// rules are tested in. `publisher` is the finding; `recorder` is the line that says the number
// above it is not a publisher finding at all.
const GAP_VERDICT_ORDER = ['publisher', 'path', 'upstream', 'recorder', 'unverifiable'] as const

export function verdictSplit(byVerdict?: Record<string, number>): string {
  if (!byVerdict) return ''
  return GAP_VERDICT_ORDER.filter((v) => (byVerdict[v] ?? 0) > 0)
    .map((v) => `${v} ${byVerdict[v].toLocaleString()}`)
    .join(' · ')
}

// One recorder's row on the recorder leg, where the numbers are absolute rather than relative to
// what the other recorders happened to receive.
//
// Exported for its own tests: it is the only place the caveats live — a hole in the archive, and a
// clean row that has no reference to be a share of — and reaching them through the rendered
// tooltip would mean driving a hover to assert a string.
export function recorderRowDetail(r: EdgeMulticastRecorderLoss, win: GapWindow, windowEnd: string): string {
  const rate =
    r.reference_seqs > 0
      ? ` · ${((r.missing / r.reference_seqs) * 1e6).toFixed(1)} ppm`
      : ''
  // A node with no gap row has no reference either — `reference_seqs` comes off the gap rows, and a
  // clean node reaches the strip from the coverage half alone. So the line the whole comparison
  // rests on, "cmh lost 0" beside "was lost 267", described itself as `0 of 0 sequence numbers the
  // publisher sent`, which reads as nothing having been measured. What coverage does have for it is
  // the datagram count.
  const lines =
    r.reference_seqs > 0
      ? [
          `${r.node}: ${r.missing.toLocaleString()} of ${r.reference_seqs.toLocaleString()} ` +
            `sequence numbers the publisher sent${rate}`,
        ]
      : [`${r.node}: no gap over this window · ${(r.datagrams ?? 0).toLocaleString()} datagrams recorded`]
  // The subtraction, shown rather than asserted. This is the thing the peer comparison could only
  // infer: a loss every recorder shares reads as the publisher's there, and here it is a number
  // this recorder admitted losing.
  if ((r.admitted ?? 0) > 0) {
    lines.push(
      `${(r.missing_raw ?? r.missing).toLocaleString()} missing less ` +
        `${(r.admitted ?? 0).toLocaleString()} this recorder admits dropping`,
    )
  }
  const split = verdictSplit(r.missing_by_verdict)
  if (split) lines.push(split)
  if (r.missing === 0) {
    lines.push('recorded every sequence number the publisher sent')
  } else {
    lines.push(
      `${(r.runs ?? (r.episodes ?? []).length).toLocaleString()} run(s) of missing sequence ` +
        `numbers over the ${Math.round(win.secs / 60)}m to ${windowEnd}Z`,
    )
  }
  // Said on BOTH readings, and it used to be said only on the clean one. A hole in the archive
  // under a loss that was found makes the figure above a FLOOR — the runs that would have been
  // carried by the segments we do not hold cannot be counted, so what is printed is what the
  // segments we do hold happened to contain. Printed with no caveat, a floor reads as a
  // measurement, and a node with a hole and a loss is the one place the two readings differ.
  if (r.unverifiable) {
    lines.push(
      r.missing === 0
        ? 'the archive has a hole over this window, so "nothing missing" here is unverified'
        : 'the archive has a hole over this window, so this is a floor on the loss, not all of it',
    )
  }
  // A mark places a run; it does not size it. The count above is the quantity, and a run of
  // seconds would say as much about how busy the feed was as about what was lost.
  if ((r.episodes ?? []).length > 0) {
    lines.push('marks place each run, and never its size — the count above is the loss')
  }
  return lines.join('\n')
}
