import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import type { EdgeMulticastSequenceHealth } from '@/lib/api'

import { TooltipProvider } from '@/components/ui/tooltip'

import { recorderRowDetail } from '@/lib/edge-multicast-loss'

import { RecorderLossTimeline } from './edge-multicast-page'

// `recorder_gaps_unavailable` says the recorder rows EXIST and the read of them failed, so the peer
// comparison is standing in. It is the one signal that distinguishes a degraded strip from an
// absent measurement, and it was rendered only in the footer under a drawn strip.
//
// That made it invisible in precisely the case it is about. Every market-by-price group is recorded
// at a single node, so the peer leg there has no peer and the component returns early — printing
// `one vantage, nothing to compare`, which is a permanent property of the group, while saying
// nothing about the leg that does measure loss from one vantage having just failed.

const WINDOW = { endMs: 1_700_000_000_000, secs: 900 }

function sequence(over: Partial<EdgeMulticastSequenceHealth>): EdgeMulticastSequenceHealth {
  return { status: 'ok', instances: [], ...over } as EdgeMulticastSequenceHealth
}

function recorder(node: string) {
  return { node, location_code: node.slice(0, 3), missing: 0, reference_seqs: 1000 }
}

describe('RecorderLossTimeline', () => {
  it('says the recorder read failed on a single vantage, where the peer leg cannot report', () => {
    render(
      <RecorderLossTimeline
        sequence={sequence({
          recorder_loss_source: 'peers',
          recorder_loss: [recorder('cmh-rec1')],
          recorder_gaps_unavailable: true,
        })}
        window={WINDOW}
      />,
    )

    expect(screen.getByText(/one vantage, nothing to compare/)).toBeInTheDocument()
    expect(screen.getByText(/recorder rows unavailable/)).toBeInTheDocument()
  })

  // The guard must not become an always-on warning: one recorder with the recorder leg healthy is
  // the ordinary state of every sports group, and nothing failed there.
  it('does not claim a failure on a single vantage when nothing failed', () => {
    render(
      <RecorderLossTimeline
        sequence={sequence({
          recorder_loss_source: 'peers',
          recorder_loss: [recorder('cmh-rec1')],
        })}
        window={WINDOW}
      />,
    )

    expect(screen.getByText(/one vantage, nothing to compare/)).toBeInTheDocument()
    expect(screen.queryByText(/recorder rows unavailable/)).not.toBeInTheDocument()
  })

  // Both legs down. "not measured" is the right summary and the note is the cause, so the row
  // carries both rather than leaving an operator to guess which measurement is missing.
  it('keeps the note beside "not measured" when both legs failed', () => {
    render(
      <RecorderLossTimeline
        sequence={sequence({
          recorder_loss_unavailable: true,
          recorder_gaps_unavailable: true,
        })}
        window={WINDOW}
      />,
    )

    expect(screen.getByText(/not measured/)).toBeInTheDocument()
    expect(screen.getByText(/recorder rows unavailable/)).toBeInTheDocument()
  })

  // The peer leg's other no-strip return: no recorder rows at all for the line. Same guard, same
  // reason — and it is the state the page is in today, before any environment has the tables.
  it('says the recorder read failed when the peer leg has no rows either', () => {
    render(
      <RecorderLossTimeline
        sequence={sequence({ recorder_gaps_unavailable: true })}
        window={WINDOW}
      />,
    )

    expect(screen.getByText(/no peer to compare/)).toBeInTheDocument()
    expect(screen.getByText(/recorder rows unavailable/)).toBeInTheDocument()
  })

  // The leg is named per line, so one group can read `recorder rows` while the next still reads
  // `peer comparison`. Which bottom row renders follows the line's own source and nothing else:
  // `pub` is an attribution the comparison cannot make, and `2+` is the inference the recorder
  // rows replace.
  it('renders the bottom row the line\'s own leg produces', () => {
    const recorderFed = render(
      <TooltipProvider>
        <RecorderLossTimeline
        sequence={sequence({
          recorder_loss_source: 'recorder',
          recorder_loss: [{ ...recorder('was-rec1'), missing: 267, reference_seqs: 300_000 }],
          recorder_loss_publisher: [{ start: 1_699_999_000, seconds: 1 }],
        })}
          window={WINDOW}
        />
      </TooltipProvider>,
    )
    const recorderRow = within(recorderFed.container)
    expect(recorderRow.getByText('pub')).toBeInTheDocument()
    expect(recorderRow.getByText(/recorder rows/)).toBeInTheDocument()
    expect(recorderRow.queryByText('2+')).not.toBeInTheDocument()

    const peerFed = render(
      <TooltipProvider>
        <RecorderLossTimeline
        sequence={sequence({
          recorder_loss_source: 'peers',
          recorder_loss: [recorder('was-rec1'), recorder('cmh-rec1')],
        })}
          window={WINDOW}
        />
      </TooltipProvider>,
    )
    const peerRow = within(peerFed.container)
    expect(peerRow.getByText('2+')).toBeInTheDocument()
    expect(peerRow.getByText(/peer comparison/)).toBeInTheDocument()
    expect(peerRow.queryByText('pub')).not.toBeInTheDocument()
  })

  // `unverifiable` is a hole in the archive, and it was consulted only where the loss was ZERO —
  // the one branch where it is the milder statement. Under a loss that WAS found it makes the
  // figure a floor: the runs the missing segments would have carried cannot be counted, so the
  // number is what the segments we hold happened to contain. With no caveat, a floor reads as a
  // measurement.
  it('says a loss measured over a hole in the archive is a floor', () => {
    const detail = recorderRowDetail(
      {
        node: 'was-rec1',
        location_code: 'was',
        missing: 267,
        reference_seqs: 300_000,
        runs: 2,
        unverifiable: true,
      },
      WINDOW,
      '12:00:00',
    )

    expect(detail).toMatch(/floor on the loss, not all of it/)
    expect(detail).toMatch(/267 of 300,000/)
  })

  // The same flag on a clean row keeps the weaker sentence it always had: there, the hole is why
  // "nothing missing" is unverified rather than why a number is a floor.
  it('keeps the unverified wording on a clean row', () => {
    const detail = recorderRowDetail(
      { node: 'cmh-rec1', missing: 0, reference_seqs: 1000, unverifiable: true },
      WINDOW,
      '12:00:00',
    )

    expect(detail).toMatch(/"nothing missing" here is unverified/)
    expect(detail).not.toMatch(/floor on the loss/)
  })

  // A clean node has no gap row, so it has no reference either — it reaches the strip from the
  // coverage half alone. The row it fills is the whole point of the comparison ("cmh lost 0"
  // beside "was lost 267") and it described itself as `0 of 0 sequence numbers the publisher
  // sent`, which reads as nothing having been measured.
  it('gives a clean row the figure coverage has for it', () => {
    const detail = recorderRowDetail(
      { node: 'cmh-rec1', location_code: 'cmh', missing: 0, reference_seqs: 0, datagrams: 299_733 },
      WINDOW,
      '12:00:00',
    )

    expect(detail).toMatch(/299,733 datagrams recorded/)
    expect(detail).not.toMatch(/0 of 0/)
  })
})
