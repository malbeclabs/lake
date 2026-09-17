import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { EdgeMulticastSequenceHealth } from '@/lib/api'

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

  // On the RECORDER leg a line the rows carry nothing for renders nothing, and the note cannot
  // apply: a failed read leaves those rows empty, so the selection falls to the peer comparison
  // and one of the branches above is what renders.
  it('draws nothing for a recorder-leg line with no rows', () => {
    const { container } = render(
      <RecorderLossTimeline
        sequence={sequence({ recorder_loss_source: 'recorder' })}
        window={WINDOW}
      />,
    )
    expect(container.firstChild).toBeNull()
  })
})
